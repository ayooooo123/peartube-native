//! Port of PearTube's `src/identify.js`: what a link is (a media file, or a page `ubit` can open), the release name
//! parsed out of it, and the Cinemeta suggestion that becomes the `imdb:tt…` id MediaStorm searches for.
//!
//! The link is licked with a HEAD first: a page name hides behind `?filename=`, `content-disposition` or a path that
//! already looks like media, so the path alone would read a 60 GB MKV as a web page. Pages go through `ubit info`,
//! and `ubit get` drains them into a private scratch hole with live NDJSON progress. Both subprocesses are the app's
//! own argv with `UBIT_CONFIG_DIR` when a config directory is set; the app's statuses and messages are kept, so a
//! missing `ubit` still answers 501 `UBIT_MISSING` and DRM still answers 451.

use std::collections::HashSet;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::sync::Notify;
use unicode_normalization::UnicodeNormalization;
use url::Url;

use crate::app_settings::{js_number, js_string, js_truthy};
use crate::art::{self, SendOptions};
use crate::protocol::{MEDIA_EXTENSIONS, is_js_space, js_trim, js_trim_end, truncate_utf16};

/// `ubit info` gives up after this long.
const INFO_TIMEOUT: Duration = Duration::from_millis(45_000);
/// `ubit get` is armed with at least this much before it says anything, and four times the stall while muxing.
const STARTUP_TIMEOUT: Duration = Duration::from_millis(45_000);
const PROCESS_TIMEOUT: Duration = Duration::from_millis(120_000);
/// The longest error line the app reports.
const MAX_ERROR_CHARS: usize = 200;

/// A failure the worklet throws: the message, plus the status and code it answers with when the app tags one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentifyError {
    message: String,
    status: Option<u16>,
    code: Option<&'static str>,
}

impl IdentifyError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
            code: None,
        }
    }

    fn tagged(message: impl Into<String>, status: Option<u16>) -> Self {
        Self {
            status,
            ..Self::new(message)
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// The HTTP status the app answers with, when it tags one.
    pub fn status(&self) -> Option<u16> {
        self.status
    }

    /// The error code the app tags, when it tags one.
    pub fn code(&self) -> Option<&'static str> {
        self.code
    }
}

impl std::fmt::Display for IdentifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for IdentifyError {}

impl From<art::HttpError> for IdentifyError {
    fn from(error: art::HttpError) -> Self {
        Self::new(error.message())
    }
}

/// `Install ubit to add page links`, with the status and code the app tags it with.
fn missing_ubit() -> IdentifyError {
    IdentifyError {
        status: Some(501),
        code: Some("UBIT_MISSING"),
        ..IdentifyError::new("Install ubit to add page links")
    }
}

/// A JS number in JSON: integral values serialize without a fractional part, and NaN never reaches the output.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JsNumber(pub f64);

impl Serialize for JsNumber {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let number = self.0;
        if number.is_finite() && number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0 {
            serializer.serialize_i64(number as i64)
        } else {
            serializer.serialize_f64(number)
        }
    }
}

/// `Number(value)` for the JSON shapes the app meets; `undefined` is NaN.
fn js_num(value: Option<&Value>) -> f64 {
    match value {
        None => f64::NAN,
        Some(Value::Number(number)) => number.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(text)) => js_number(text),
        Some(Value::Bool(flag)) => f64::from(u8::from(*flag)),
        Some(Value::Null) => 0.0,
        Some(_) => f64::NAN,
    }
}

/// `Number(x) || null`.
fn opt_number(value: Option<&Value>) -> Option<JsNumber> {
    let number = js_num(value);
    (number != 0.0 && number.is_finite()).then_some(JsNumber(number))
}

/// `String(number)`: integrals have no `.0`.
fn number_text(number: f64) -> String {
    if number.is_finite() && number.fract() == 0.0 && number.abs() < 1e21 {
        format!("{number:.0}")
    } else {
        number.to_string()
    }
}

/// The release name parsed out of a link, in the app's field order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParsedName {
    pub title: String,
    pub year: Option<u32>,
    pub season: Option<u32>,
    pub episode: Option<u32>,
    pub resolution: String,
    pub codec: String,
    pub audio: String,
    pub ext: String,
}

/// `NOISE` and the other tag lists are matched by hand: a boundary is a non-word byte on either side.
fn is_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn starts_word(bytes: &[u8], at: usize) -> bool {
    at == 0 || !is_word(bytes[at - 1])
}

fn ends_word(bytes: &[u8], end: usize) -> bool {
    end >= bytes.len() || !is_word(bytes[end])
}

fn literal(bytes: &[u8], at: usize, text: &str) -> bool {
    bytes.get(at..at + text.len()) == Some(text.as_bytes())
}

fn literal_ci(bytes: &[u8], at: usize, text: &str) -> bool {
    bytes
        .get(at..at + text.len())
        .is_some_and(|window| window.eq_ignore_ascii_case(text.as_bytes()))
}

/// `^error:` on a line that may start with a multi-byte character.
fn starts_error(line: &str) -> bool {
    literal_ci(line.as_bytes(), 0, "error:")
}

fn digit_run(bytes: &[u8], at: usize, max: usize) -> usize {
    let mut width = 0;
    while width < max && bytes.get(at + width).is_some_and(u8::is_ascii_digit) {
        width += 1;
    }
    width
}

fn digits_value(bytes: &[u8], at: usize, width: usize) -> u32 {
    bytes[at..at + width]
        .iter()
        .fold(0, |value, byte| value * 10 + u32::from(byte - b'0'))
}

/// The width of one JS whitespace character at `at`: the app's `\s?` is a single character.
fn space_width(bytes: &[u8], at: usize) -> usize {
    match bytes.get(at) {
        Some(byte) if byte.is_ascii() => usize::from(is_js_space(char::from(*byte))),
        Some(_) => std::str::from_utf8(&bytes[at..])
            .ok()
            .and_then(|text| text.chars().next())
            .filter(|ch| is_js_space(*ch))
            .map_or(0, char::len_utf8),
        None => 0,
    }
}

/// `\bS(\d{1,2})\s?E(\d{1,3})\b`, leftmost match.
fn find_episode(low: &[u8]) -> Option<(usize, u32, u32)> {
    for at in 0..low.len() {
        if low[at] != b's' || !starts_word(low, at) {
            continue;
        }
        let seasons = digit_run(low, at + 1, 2);
        for width in (1..=seasons).rev() {
            let mut cursor = at + 1 + width + space_width(low, at + 1 + width);
            if low.get(cursor) != Some(&b'e') {
                continue;
            }
            cursor += 1;
            let episodes = digit_run(low, cursor, 3);
            for episode_width in (1..=episodes).rev() {
                if ends_word(low, cursor + episode_width) {
                    return Some((
                        at,
                        digits_value(low, at + 1, width),
                        digits_value(low, cursor, episode_width),
                    ));
                }
            }
        }
    }
    None
}

/// `\b(\d{1,2})x(\d{2,3})\b`, leftmost match.
fn find_by_x(low: &[u8]) -> Option<(usize, u32, u32)> {
    for at in 0..low.len() {
        if !low[at].is_ascii_digit() || !starts_word(low, at) {
            continue;
        }
        let seasons = digit_run(low, at, 2);
        for width in (1..=seasons).rev() {
            let cursor = at + width;
            if low.get(cursor) != Some(&b'x') {
                continue;
            }
            let episodes = digit_run(low, cursor + 1, 3);
            for episode_width in (2..=episodes).rev() {
                if ends_word(low, cursor + 1 + episode_width) {
                    return Some((
                        at,
                        digits_value(low, at, width),
                        digits_value(low, cursor + 1, episode_width),
                    ));
                }
            }
        }
    }
    None
}

/// `(19[2-9]\d|20[0-4]\d)` at `at`, with a word boundary after it.
fn year_at(low: &[u8], at: usize) -> Option<u32> {
    let century = match (low.get(at)?, low.get(at + 1)?) {
        (b'1', b'9') => (b'2'..=b'9').contains(low.get(at + 2)?),
        (b'2', b'0') => (b'0'..=b'4').contains(low.get(at + 2)?),
        _ => false,
    };
    if !century || !low.get(at + 3)?.is_ascii_digit() || !ends_word(low, at + 4) {
        return None;
    }
    Some(digits_value(low, at, 4))
}

/// Every `[([]?\b(19…|20…)\b[)\]]?` match, so the last one wins the way `matchAll().pop()` does.
fn find_year(low: &[u8]) -> Option<(usize, u32)> {
    let mut last = None;
    for at in 0..low.len() {
        match low[at] {
            b'(' | b'[' => {
                if let Some(year) = year_at(low, at + 1) {
                    last = Some((at, year));
                }
            }
            _ if low[at].is_ascii_digit() && starts_word(low, at) => {
                if let Some(year) = year_at(low, at) {
                    last = Some((at, year));
                }
            }
            _ => {}
        }
    }
    last
}

/// The `NOISE` alternation, tried at one word start.
fn noise_at(low: &[u8], at: usize) -> bool {
    const PLAIN: [&str; 32] = [
        "2160p", "1080p", "720p", "576p", "480p", "4k", "uhd", "bluray", "blu-ray", "bdrip",
        "brrip", "webrip", "hdtv", "dvdrip", "remux", "proper", "repack", "extended", "unrated",
        "x264", "x265", "hevc", "avc", "av1", "xvid", "divx", "aac", "dts", "truehd", "atmos",
        "flac", "opus",
    ];
    let matched = |end: usize| ends_word(low, end);
    for word in PLAIN {
        if literal(low, at, word) && matched(at + word.len()) {
            return true;
        }
    }
    for (prefix, end) in [("mp3", 3), ("ac3", 3), ("dv", 2), ("dd", 2), ("ddp", 3)] {
        if literal(low, at, prefix) && matched(at + end) {
            return true;
        }
    }
    for (prefix, end) in [
        ("hdr10", 5),
        ("hdr1", 4),
        ("web-dl", 6),
        ("webdl", 5),
        ("web", 3),
        ("eac-3", 5),
        ("eac3", 4),
        ("ac-3", 4),
    ] {
        if literal(low, at, prefix) && matched(at + end) {
            return true;
        }
    }
    for tail in ["264", "265"] {
        if (literal(low, at, "h") && literal(low, at + 1, tail) && matched(at + 4))
            || (literal(low, at, "h.") && literal(low, at + 2, tail) && matched(at + 5))
        {
            return true;
        }
    }
    false
}

fn find_noise(low: &[u8]) -> Option<usize> {
    (0..low.len()).find(|at| starts_word(low, *at) && noise_at(low, *at))
}

/// The first capture of one of the app's tag regexes, at one word start.
fn tag_end(low: &[u8], at: usize, tag: Tag) -> Option<usize> {
    const RESOLUTION: [&str; 6] = ["2160p", "1080p", "720p", "576p", "480p", "4k"];
    const CODEC: [&str; 10] = [
        "h.264", "h.265", "x264", "x265", "hevc", "avc", "av1", "vp9", "xvid", "divx",
    ];
    const AUDIO: [&str; 16] = [
        "eac-3", "eac3", "ac-3", "ac3", "ddp5.1", "dd5.1", "ddp", "dd", "dts-hd", "dts", "truehd",
        "atmos", "aac", "flac", "opus", "mp3",
    ];
    let tags: &[&str] = match tag {
        Tag::Resolution => &RESOLUTION,
        Tag::Codec => &CODEC,
        Tag::Audio => &AUDIO,
    };
    for word in tags {
        if literal(low, at, word) && ends_word(low, at + word.len()) {
            return Some(at + word.len());
        }
    }
    None
}

#[derive(Clone, Copy)]
enum Tag {
    Resolution,
    Codec,
    Audio,
}

fn pick(words: &str, low: &[u8], tag: Tag) -> String {
    for at in 0..low.len() {
        if starts_word(low, at)
            && let Some(end) = tag_end(low, at, tag)
        {
            return words[at..end].to_ascii_lowercase();
        }
    }
    String::new()
}

/// `MEDIA_FILE`'s suffix, split off: the stem and the lowercased extension.
fn split_media_ext(name: &str) -> Option<(&str, String)> {
    let dot = name.rfind('.')?;
    let ext = name[dot + 1..].to_ascii_lowercase();
    MEDIA_EXTENSIONS
        .contains(&ext.as_str())
        .then(|| (&name[..dot], ext))
}

fn strip_media_ext(name: &str) -> &str {
    split_media_ext(name).map_or(name, |(stem, _)| stem)
}

/// `stem.replace(/[._]+/g, ' ').replace(/\s+/g, ' ').trim()`.
fn words_of(stem: &str) -> String {
    let mut out = String::with_capacity(stem.len());
    let mut space = false;
    for ch in stem.chars() {
        if ch == '.' || ch == '_' || is_js_space(ch) {
            space = !out.is_empty();
        } else {
            if space {
                out.push(' ');
                space = false;
            }
            out.push(ch);
        }
    }
    out
}

/// `title.slice(0, cut).replace(/[[(-]+\s*$/, '').trim()`.
fn title_prefix(words: &str) -> String {
    let trimmed = js_trim_end(words);
    js_trim(trimmed.trim_end_matches(&['[', '(', '-'][..])).to_owned()
}

/// `parseName`: the release name at the end of a link, or the whole link when it is not one.
pub fn parse_name(raw: &str) -> ParsedName {
    let mut name = raw.to_owned();
    if let Ok(url) = Url::parse(&name) {
        let last = url.path().rsplit('/').next().unwrap_or("");
        let segment = if last.is_empty() {
            name.clone()
        } else {
            last.to_owned()
        };
        if let Some(decoded) = decode_uri_component(&segment) {
            name = decoded;
        }
    }
    let ext = split_media_ext(&name).map_or(String::new(), |(_, ext)| ext);
    let words = words_of(strip_media_ext(strip_media_ext(&name)));
    let low = words.to_ascii_lowercase();
    let low = low.as_bytes();

    let episode = find_episode(low).or_else(|| find_by_x(low));
    let year = find_year(low);
    let mut marks: Vec<usize> = [episode.map(|(at, _, _)| at), year.map(|(at, _)| at)]
        .into_iter()
        .flatten()
        .filter(|at| *at > 0)
        .collect();
    if marks.is_empty() {
        // A year or episode marks where the title ends; release tags only when neither is there.
        marks = find_noise(low).filter(|at| *at > 0).into_iter().collect();
    }
    let cut = marks.iter().min().copied().unwrap_or(words.len());
    let title = match title_prefix(&words[..cut]) {
        title if !title.is_empty() => title,
        _ => words.clone(),
    };
    ParsedName {
        title,
        year: year.map(|(_, value)| value),
        season: episode.map(|(_, season, _)| season),
        episode: episode.map(|(_, _, number)| number),
        resolution: pick(&words, low, Tag::Resolution),
        codec: pick(&words, low, Tag::Codec),
        audio: pick(&words, low, Tag::Audio),
        ext,
    }
}

/// `encodeURIComponent`: the unreserved set the app leaves alone.
fn encode_uri_component(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z'
            | b'a'..=b'z'
            | b'0'..=b'9'
            | b'-'
            | b'_'
            | b'.'
            | b'!'
            | b'~'
            | b'*'
            | b'\''
            | b'('
            | b')' => out.push(char::from(byte)),
            byte => out.push_str(&format!("%{byte:02X}")),
        }
    }
    out
}

/// `decodeURIComponent`, or nothing when the escapes are malformed.
fn decode_uri_component(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] != b'%' {
            out.push(bytes[at]);
            at += 1;
            continue;
        }
        let hex = bytes.get(at + 1..at + 3)?;
        if !hex.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
        at += 3;
    }
    String::from_utf8(out).ok()
}

fn find_ci(haystack: &str, needle: &str) -> Option<usize> {
    let hay = haystack.as_bytes();
    let needle = needle.as_bytes();
    hay.windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

fn header_of<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

/// `filename*=UTF-8''…` from a content disposition.
fn disposition_star(disposition: &str) -> Option<String> {
    let start = find_ci(disposition, "filename*=")? + "filename*=".len();
    let rest = disposition.get(start..)?;
    if !rest.get(..7)?.eq_ignore_ascii_case("utf-8''") {
        return None;
    }
    let value = rest.get(7..)?;
    let end = value.find(';').unwrap_or(value.len());
    (!value[..end].is_empty()).then(|| value[..end].to_owned())
}

/// `filename="…"` from a content disposition.
fn disposition_quoted(disposition: &str) -> Option<String> {
    let start = find_ci(disposition, "filename=\"")? + "filename=\"".len();
    let value = disposition.get(start..)?;
    let end = value.find('"')?;
    (!value[..end].is_empty()).then(|| value[..end].to_owned())
}

/// `fileName`: the media name a reply carries, preferring the query, then the disposition, then the path.
///
/// A bad link and a malformed escape are errors, exactly as in the app.
pub fn file_name(raw: &str, headers: &[(String, String)]) -> Result<String, IdentifyError> {
    let url = Url::parse(raw).map_err(|_| IdentifyError::new("Invalid URL"))?;
    let disposition = header_of(headers, "content-disposition").unwrap_or("");
    let named = disposition_star(disposition).and_then(|raw| decode_uri_component(&raw));
    let from_query = url
        .query_pairs()
        .find(|(key, _)| key == "filename")
        .map(|(_, value)| value.into_owned());
    let path = url.path().rsplit('/').next().unwrap_or("");
    let from_path =
        decode_uri_component(path).ok_or_else(|| IdentifyError::new("URI malformed"))?;
    Ok([from_query, named, disposition_quoted(disposition)]
        .into_iter()
        .flatten()
        .find(|name| crate::protocol::is_media_file(name))
        .unwrap_or(from_path))
}

/// A link the app recognised: a direct media file, or a page `ubit` can read.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum Link {
    File {
        name: String,
        size: Option<JsNumber>,
        #[serde(rename = "contentType")]
        content_type: Option<String>,
    },
    Page {
        name: String,
        site: String,
        uploader: String,
        duration: Option<JsNumber>,
        thumbnail: Option<String>,
        size: Option<JsNumber>,
        resolution: String,
        /// The URL MediaStorm announces the page under; the app strips it from the reported link.
        #[serde(skip)]
        page_id: String,
    },
}

impl Link {
    /// `link.kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::File { .. } => "file",
            Self::Page { .. } => "page",
        }
    }

    /// The name the release-name parser reads.
    pub fn name(&self) -> &str {
        match self {
            Self::File { name, .. } | Self::Page { name, .. } => name,
        }
    }

    /// The resolution the page reported, empty for a file.
    pub fn resolution(&self) -> &str {
        match self {
            Self::File { .. } => "",
            Self::Page { resolution, .. } => resolution,
        }
    }

    /// The URL MediaStorm announces a page under; a file has none.
    pub fn page_id(&self) -> Option<&str> {
        match self {
            Self::File { .. } => None,
            Self::Page { page_id, .. } => Some(page_id),
        }
    }
}

/// How `probe` may run `ubit`.
#[derive(Debug, Clone)]
pub struct ProbeOptions {
    pub config_dir: String,
    pub ubit: String,
    pub ubit_timeout: Duration,
}

impl Default for ProbeOptions {
    fn default() -> Self {
        Self {
            config_dir: String::new(),
            ubit: "ubit".to_owned(),
            ubit_timeout: INFO_TIMEOUT,
        }
    }
}

fn ubit_command(program: &str, args: &[&str], config_dir: &str) -> Command {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // The app copies the whole environment and adds the config directory; an inherited environment is that copy.
    if !config_dir.is_empty() {
        command.env("UBIT_CONFIG_DIR", config_dir);
    }
    command
}

/// `tagStatus`: DRM is 451, otherwise the status the message names.
pub fn tag_status(message: &str) -> Option<u16> {
    let bytes = message.as_bytes();
    let has_drm = (0..bytes.len()).any(|at| {
        starts_word(bytes, at) && literal_ci(bytes, at, "drm") && ends_word(bytes, at + "drm".len())
    });
    if has_drm {
        return Some(451);
    }
    let mut at = 0;
    while at < bytes.len() {
        if !bytes[at].is_ascii_digit() {
            at += 1;
            continue;
        }
        let start = at;
        while at < bytes.len() && bytes[at].is_ascii_digit() {
            at += 1;
        }
        if starts_word(bytes, start) && ends_word(bytes, at) {
            match &message[start..at] {
                "401" => return Some(401),
                "403" => return Some(403),
                "404" => return Some(404),
                "410" => return Some(410),
                "451" => return Some(451),
                _ => {}
            }
        }
    }
    None
}

/// `error: …` stripped, trimmed, and cut to the app's report length; the app's fallback when nothing is left.
fn error_line(raw: &str, fallback: &str) -> String {
    let raw = js_trim(raw);
    let lines: Vec<&str> = raw.split('\n').collect();
    let found = lines
        .iter()
        .map(|line| js_trim(line))
        .find(|line| starts_error(line));
    let line = match found {
        Some(line) => line,
        None => match lines.first().copied().filter(|line| !line.is_empty()) {
            Some(line) => line,
            None => return fallback.to_owned(),
        },
    };
    let stripped = if starts_error(line) {
        line["error:".len()..].trim_start_matches(is_js_space)
    } else {
        line
    };
    let cut = truncate_utf16(js_trim(stripped), MAX_ERROR_CHARS);
    if cut.is_empty() {
        fallback.to_owned()
    } else {
        cut
    }
}

/// The message `ubit info` fails with: the first `error:` line, else the first line, else the app's default.
fn info_error_message(stderr: &str) -> String {
    error_line(stderr, "This page has no video ubit can read")
}

/// The message `ubit get` fails with.
fn download_error_message(raw: &str) -> String {
    error_line(raw, "ubit failed to download this page")
}

/// What `ubit info` answered, from the recorded streams: a status-tagged failure, or the parsed page.
pub fn ubit_info_result(stdout: &[u8], stderr: &[u8], code: i32) -> Result<Value, IdentifyError> {
    if code != 0 {
        let message = info_error_message(&String::from_utf8_lossy(stderr));
        return Err(IdentifyError::tagged(message.clone(), tag_status(&message)));
    }
    let info: Value = serde_json::from_slice(stdout)
        .ok()
        .filter(Value::is_object)
        .ok_or_else(|| IdentifyError::new("ubit returned no metadata"))?;
    if info.get("isDRM").is_some_and(js_truthy) {
        return Err(IdentifyError::tagged(
            "This video is DRM-protected",
            Some(451),
        ));
    }
    Ok(info)
}

/// Runs `ubit info` and reads both streams to the end, killing it at the timeout the way the app's timer does.
pub async fn ubit_info(
    program: &str,
    url: &str,
    config_dir: &str,
    timeout: Duration,
) -> Result<Value, IdentifyError> {
    let mut command = ubit_command(program, &["info", url, "--json"], config_dir);
    let mut child = command.spawn().map_err(|_| missing_ubit())?;
    let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take()) else {
        return Err(missing_ubit());
    };
    let mut out = Vec::new();
    let mut err = Vec::new();
    let mut out_chunk = [0u8; 8192];
    let mut err_chunk = [0u8; 8192];
    let mut out_done = false;
    let mut err_done = false;
    let mut deadline = Box::pin(tokio::time::sleep(timeout));
    while !(out_done && err_done) {
        tokio::select! {
            _ = &mut deadline => {
                let _ = child.start_kill();
                deadline = Box::pin(tokio::time::sleep(Duration::from_secs(3600)));
            }
            read = stdout.read(&mut out_chunk), if !out_done => match read {
                Ok(0) | Err(_) => out_done = true,
                Ok(read) => out.extend_from_slice(&out_chunk[..read]),
            },
            read = stderr.read(&mut err_chunk), if !err_done => match read {
                Ok(0) | Err(_) => err_done = true,
                Ok(read) => err.extend_from_slice(&err_chunk[..read]),
            },
        }
    }
    let status = child.wait().await.map_err(|_| missing_ubit())?;
    ubit_info_result(&out, &err, status.code().unwrap_or(1))
}

/// `probe`: HEAD the link, then hand pages to `ubit info`.
pub async fn probe(url: &str, opts: &ProbeOptions) -> Result<Link, IdentifyError> {
    let head = art::send(
        url,
        SendOptions {
            method: hyper::Method::HEAD,
            ..Default::default()
        },
    )
    .await
    .ok();
    let content_type = head
        .as_ref()
        .and_then(|response| response.header("content-type"))
        .unwrap_or("")
        .to_lowercase();
    let final_url = head
        .as_ref()
        .map_or_else(|| url.to_owned(), |response| response.url.clone());
    let name = file_name(
        &final_url,
        head.as_ref().map_or(&[][..], |response| &response.headers),
    )?;
    let looks_media = content_type.starts_with("video/")
        || content_type.contains("matroska")
        || (crate::protocol::is_media_file(&name) && !content_type.starts_with("text/"));
    if let Some(head) = &head {
        if head.status < 400 && looks_media {
            let size = head
                .header("content-length")
                .map(js_number)
                .filter(|size| *size != 0.0 && size.is_finite())
                .map(JsNumber);
            return Ok(Link::File {
                name,
                size,
                content_type: (!content_type.is_empty()).then(|| content_type.clone()),
            });
        }
        if head.status >= 400 && crate::protocol::is_media_file(&name) {
            return Err(IdentifyError::new(format!(
                "The link answered {}",
                head.status
            )));
        }
    }
    let info = ubit_info(&opts.ubit, url, &opts.config_dir, opts.ubit_timeout).await?;
    let formats = info
        .get("formats")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let tallest = formats
        .iter()
        .map(|format| js_num(format.get("height")))
        .fold(0.0f64, f64::max)
        .max(0.0);
    let height = if tallest != 0.0 {
        tallest
    } else {
        let reported = js_num(info.get("height"));
        if reported.is_finite() {
            reported.max(0.0)
        } else {
            0.0
        }
    };
    let largest = formats
        .iter()
        .map(|format| js_num(format.get("filesize")))
        .fold(0.0f64, f64::max)
        .max(0.0);
    let size =
        opt_number(info.get("filesize")).or_else(|| (largest != 0.0).then_some(JsNumber(largest)));
    Ok(Link::Page {
        name: string_field(&info, "title"),
        site: string_field(&info, "site"),
        uploader: match string_field(&info, "author") {
            empty if empty.is_empty() => string_field(&info, "uploader"),
            author => author,
        },
        duration: opt_number(info.get("duration")),
        thumbnail: info
            .get("thumbnail")
            .and_then(Value::as_str)
            .filter(|thumbnail| !thumbnail.is_empty())
            .map(str::to_owned),
        size,
        resolution: if height != 0.0 {
            format!("{}p", number_text(height))
        } else {
            String::new()
        },
        page_id: info
            .get("webpageURL")
            .filter(|value| js_truthy(value))
            .map_or_else(|| url.to_owned(), js_string),
    })
}

/// A truthy string field, empty when the app would report `''`.
fn string_field(info: &Value, field: &str) -> String {
    info.get(field)
        .filter(|value| js_truthy(value))
        .map_or_else(String::new, js_string)
}

/// One Cinemeta suggestion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Candidate {
    pub imdb: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub year: Option<u32>,
    pub poster: Option<String>,
}

/// The id and title a suggestion is announced under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Entry {
    pub id: String,
    pub title: String,
}

/// `norm`: lowercased, NFKD, non-alphanumerics collapsed, a leading article gone.
pub fn norm(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut in_run = false;
    for ch in text.to_lowercase().nfkd() {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            out.push(ch);
            in_run = false;
        } else if !in_run {
            out.push(' ');
            in_run = true;
        }
    }
    for article in ["the ", "a ", "an "] {
        if let Some(rest) = out.strip_prefix(article) {
            return js_trim(rest).to_owned();
        }
    }
    js_trim(&out).to_owned()
}

/// A suggestion is taken without asking only when the title matches exactly and the year (if known) agrees.
pub fn strong(candidate: &Candidate, parsed: &ParsedName) -> bool {
    if norm(&candidate.name) != norm(&parsed.title) {
        return false;
    }
    match parsed.year {
        None => true,
        Some(year) => candidate.kind == "series" || candidate.year == Some(year),
    }
}

/// The id and title an entry is announced under for a chosen candidate, in the shapes MediaStorm searches for.
pub fn candidate_entry(candidate: &Candidate, parsed: &ParsedName) -> Entry {
    if candidate.kind == "series"
        && let (Some(season), Some(episode)) = (parsed.season, parsed.episode)
    {
        let episode = format!("s{season:02}e{episode:02}");
        return Entry {
            id: format!("imdb:{}:{episode}", candidate.imdb),
            title: format!("{} {}", candidate.name, episode.to_uppercase()),
        };
    }
    Entry {
        id: format!("imdb:{}", candidate.imdb),
        title: match candidate.year {
            Some(year) => format!("{} {year}", candidate.name),
            None => candidate.name.clone(),
        },
    }
}

/// `clip:` and sixteen hex characters of the page id, or of the link itself for a file.
pub fn clip_id(page_id: Option<&str>, url: &str) -> String {
    let digest = Sha256::digest(page_id.unwrap_or(url).as_bytes());
    let hex: String = digest
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("clip:{hex}")
}

/// The suggestions in a Cinemeta search answer, filtered to related titles and ranked.
pub fn search_candidates(body: &str, kind: &str, title: &str, year: Option<u32>) -> Vec<Candidate> {
    let metas = serde_json::from_str::<Value>(body)
        .ok()
        .and_then(|body| body.get("metas").and_then(Value::as_array).cloned())
        .unwrap_or_default();
    let mut candidates = Vec::new();
    for meta in &metas {
        let imdb = match meta.get("imdb_id").filter(|value| js_truthy(value)) {
            Some(value) => js_string(value),
            None => string_field(meta, "id"),
        };
        if !art::is_imdb_id(&imdb) {
            continue;
        }
        let release = match meta.get("releaseInfo").filter(|value| js_truthy(value)) {
            Some(value) => js_string(value),
            None => string_field(meta, "year"),
        };
        let first_four: String = release.chars().take(4).collect();
        let parsed_year = js_num(Some(&Value::String(first_four)));
        candidates.push(Candidate {
            imdb,
            kind: kind.to_owned(),
            name: string_field(meta, "name"),
            year: (parsed_year.is_finite() && parsed_year >= 1.0 && parsed_year.fract() == 0.0)
                .then_some(parsed_year as u32),
            poster: meta
                .get("poster")
                .and_then(Value::as_str)
                .map(str::to_owned),
        });
    }
    // Only names sharing at least half their words with the parsed title; exact title first, then same year,
    // then Cinemeta's own order (a stable sort keeps it).
    let want = norm(title);
    let words: HashSet<&str> = want.split(' ').collect();
    let related = |candidate: &Candidate| {
        let normalized = norm(&candidate.name);
        let theirs: Vec<&str> = normalized.split(' ').collect();
        let shared = theirs.iter().filter(|word| words.contains(*word)).count();
        shared as f64 / theirs.len().max(words.len()) as f64 >= 0.5
    };
    let mut ranked: Vec<Candidate> = candidates.into_iter().filter(related).collect();
    let score = |candidate: &Candidate| {
        u8::from(norm(&candidate.name) == want) * 2
            + u8::from(year.is_some_and(|year| candidate.year == Some(year)))
    };
    ranked.sort_by_key(|candidate| std::cmp::Reverse(score(candidate)));
    ranked.truncate(6);
    ranked
}

/// `cinemetaSearch`: up to six related suggestions, or none when the search fails.
pub async fn cinemeta_search(
    base: &str,
    title: &str,
    year: Option<u32>,
    season: Option<u32>,
) -> Result<Vec<Candidate>, IdentifyError> {
    if base.is_empty() || title.is_empty() {
        return Ok(Vec::new());
    }
    let kind = if season.is_some_and(|season| season != 0) {
        "series"
    } else {
        "movie"
    };
    let response = art::send(
        &format!(
            "{base}/catalog/{kind}/top/search={}.json",
            encode_uri_component(title)
        ),
        SendOptions {
            timeout: Duration::from_millis(8000),
            ..Default::default()
        },
    )
    .await?;
    if response.status != 200 {
        return Ok(Vec::new());
    }
    Ok(search_candidates(&response.body, kind, title, year))
}

/// A finished `ubit get`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Downloaded {
    pub path: String,
    pub size: Option<i64>,
}

/// One NDJSON progress event.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DownloadProgress {
    pub bytes: JsNumber,
    pub total: Option<JsNumber>,
}

/// What one line of `ubit get`'s NDJSON means.
#[derive(Debug, Clone, PartialEq)]
pub enum LineEvent {
    Progress {
        bytes: f64,
        total: Option<f64>,
    },
    Process,
    Done {
        path: Option<String>,
        size: f64,
    },
    Error(String),
    /// Valid JSON the app re-arms the stall timer for.
    Ignored,
    /// Empty or unparseable: the app does not touch the timer.
    Unparsed,
}

/// `parseLine`: one NDJSON line of `ubit get`.
pub fn parse_progress_line(line: &str) -> LineEvent {
    let trimmed = js_trim(line);
    if trimmed.is_empty() {
        return LineEvent::Unparsed;
    }
    let Ok(event) = serde_json::from_str::<Value>(trimmed) else {
        return LineEvent::Unparsed;
    };
    match event.get("event").and_then(Value::as_str) {
        Some("progress") => {
            let total = js_num(event.get("total"));
            LineEvent::Progress {
                bytes: js_num(event.get("downloaded")),
                total: (total > 0.0).then_some(total),
            }
        }
        Some("process") => LineEvent::Process,
        Some("done") => LineEvent::Done {
            path: event.get("path").and_then(Value::as_str).map(str::to_owned),
            size: js_num(event.get("size")),
        },
        Some("error") => match event.get("error").filter(|value| js_truthy(value)) {
            Some(error) => LineEvent::Error(js_string(error)),
            None => LineEvent::Ignored,
        },
        _ => LineEvent::Ignored,
    }
}

/// What a line did to the stall timer, and whether it should be reported.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LineOutcome {
    pub progress: Option<DownloadProgress>,
    /// The new stall deadline, when the app re-armed the timer.
    pub timeout: Option<Duration>,
}

/// The state `ubit get`'s NDJSON builds up, kept apart from the process so recordings replay offline.
#[derive(Debug, Clone)]
pub struct DownloadState {
    done_path: Option<String>,
    done_size: f64,
    event_error: Option<String>,
    pub timed_out: bool,
    startup: Duration,
}

impl DownloadState {
    pub fn new(stall: Duration) -> Self {
        Self {
            done_path: None,
            done_size: f64::NAN,
            event_error: None,
            timed_out: false,
            startup: stall.max(STARTUP_TIMEOUT),
        }
    }

    /// The deadline the process starts with.
    pub fn timeout(&self) -> Duration {
        self.startup
    }

    /// `parseLine`, with the timer arming it does.
    pub fn line(&mut self, line: &str, stall: Duration) -> LineOutcome {
        match parse_progress_line(line) {
            LineEvent::Progress { bytes, total } => LineOutcome {
                progress: Some(DownloadProgress {
                    bytes: JsNumber(if bytes == 0.0 || !bytes.is_finite() {
                        0.0
                    } else {
                        bytes
                    }),
                    total: total.map(JsNumber),
                }),
                timeout: Some(stall),
            },
            LineEvent::Process => LineOutcome {
                progress: None,
                timeout: Some(stall.saturating_mul(4).max(PROCESS_TIMEOUT)),
            },
            LineEvent::Done { path, size } => {
                self.done_path = path;
                self.done_size = size;
                LineOutcome::default()
            }
            LineEvent::Error(error) => {
                self.event_error = Some(error);
                LineOutcome::default()
            }
            LineEvent::Ignored => LineOutcome {
                progress: None,
                timeout: Some(stall),
            },
            LineEvent::Unparsed => LineOutcome::default(),
        }
    }

    /// What `ubit get` answered: `Source stalled`, the event or stderr message, or the file it wrote.
    pub fn finish(&self, code: i32, stderr: &[u8]) -> Result<Downloaded, IdentifyError> {
        if self.timed_out {
            return Err(IdentifyError::new("Source stalled"));
        }
        let Some(path) = self.done_path.clone().filter(|_| code == 0) else {
            let raw = match self.event_error.clone() {
                Some(error) if !error.is_empty() => error,
                _ => String::from_utf8_lossy(stderr).into_owned(),
            };
            let message = download_error_message(&raw);
            return Err(IdentifyError::tagged(message.clone(), tag_status(&message)));
        };
        Ok(Downloaded {
            path,
            size: (self.done_size.is_finite()
                && self.done_size >= 0.0
                && self.done_size.fract() == 0.0
                && self.done_size <= 9_007_199_254_740_991.0)
                .then_some(self.done_size as i64),
        })
    }
}

/// `AbortSignal` as the app passes it to `downloadPage`: aborting kills the running `ubit get`.
#[derive(Debug, Clone, Default)]
pub struct Controller {
    aborted: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl Controller {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn abort(&self) {
        self.aborted.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    pub fn is_aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    async fn aborted(&self) {
        while !self.is_aborted() {
            self.notify.notified().await;
        }
    }
}

/// How `download_page` runs `ubit get`.
pub struct DownloadOptions {
    pub config_dir: String,
    pub stall_timeout: Duration,
    pub ubit: String,
    pub controller: Option<Controller>,
    pub on_progress: Option<Box<dyn FnMut(DownloadProgress) + Send>>,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            config_dir: String::new(),
            stall_timeout: Duration::from_millis(30_000),
            ubit: "ubit".to_owned(),
            controller: None,
            on_progress: None,
        }
    }
}

/// `downloadPage`: drain a page into `out_dir` with live progress, cancelling or timing out a stalled source.
pub async fn download_page(
    page_url: &str,
    out_dir: &str,
    mut opts: DownloadOptions,
) -> Result<Downloaded, IdentifyError> {
    let controller = opts.controller.clone();
    if controller.as_ref().is_some_and(Controller::is_aborted) {
        return Err(IdentifyError::new("Cancelled"));
    }
    let stall = opts.stall_timeout;
    let ubit = if opts.ubit.is_empty() {
        "ubit".to_owned()
    } else {
        opts.ubit.clone()
    };
    let args = [
        "get",
        page_url,
        "--no-library",
        "--no-playlist",
        "--json",
        "-o",
        out_dir,
    ];
    let mut child = ubit_command(&ubit, &args, &opts.config_dir)
        .spawn()
        .map_err(|_| missing_ubit())?;
    let (Some(mut stdout), Some(mut stderr)) = (child.stdout.take(), child.stderr.take()) else {
        return Err(missing_ubit());
    };

    let mut state = DownloadState::new(stall);
    let mut stderr_bytes = Vec::new();
    let mut pending: Vec<u8> = Vec::new();
    let mut out_chunk = [0u8; 8192];
    let mut err_chunk = [0u8; 8192];
    let mut stderr_done = false;
    let mut cancelled = false;
    let mut deadline = Box::pin(tokio::time::sleep(state.timeout()));

    let mut abort_signal = Box::pin({
        let controller = controller.clone();
        async move {
            match controller {
                Some(controller) => controller.aborted().await,
                None => std::future::pending().await,
            }
        }
    });

    loop {
        tokio::select! {
            _ = &mut deadline => {
                state.timed_out = true;
                let _ = child.start_kill();
                deadline = Box::pin(tokio::time::sleep(Duration::from_secs(3600)));
            }
            _ = &mut abort_signal, if !cancelled => {
                cancelled = true;
                let _ = child.start_kill();
            }
            read = stdout.read(&mut out_chunk) => {
                let read = match read {
                    Ok(0) | Err(_) => break,
                    Ok(read) => read,
                };
                pending.extend_from_slice(&out_chunk[..read]);
                while let Some(end) = pending.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = pending.drain(..=end).collect();
                    let outcome = state.line(&String::from_utf8_lossy(&line[..line.len() - 1]), stall);
                    if let Some(progress) = outcome.progress
                        && let Some(on_progress) = opts.on_progress.as_mut()
                    {
                        on_progress(progress);
                    }
                    if let Some(timeout) = outcome.timeout {
                        deadline = Box::pin(tokio::time::sleep(timeout));
                    }
                }
                if controller.as_ref().is_some_and(Controller::is_aborted) && !cancelled {
                    cancelled = true;
                    let _ = child.start_kill();
                }
            }
            read = stderr.read(&mut err_chunk), if !stderr_done => match read {
                Ok(0) | Err(_) => stderr_done = true,
                Ok(read) => {
                    stderr_bytes.extend_from_slice(&err_chunk[..read]);
                    deadline = Box::pin(tokio::time::sleep(stall));
                }
            },
        }
    }
    if !pending.is_empty() {
        let outcome = state.line(&String::from_utf8_lossy(&pending), stall);
        if let Some(progress) = outcome.progress
            && let Some(on_progress) = opts.on_progress.as_mut()
        {
            on_progress(progress);
        }
    }
    let status = child.wait().await.map_err(|_| missing_ubit())?;
    if controller.as_ref().is_some_and(Controller::is_aborted) {
        return Err(IdentifyError::new("Cancelled"));
    }
    state.finish(status.code().unwrap_or(1), &stderr_bytes)
}

/// The plain entry a link is announced as when no suggestion is taken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Plain {
    pub id: String,
    pub title: String,
}

/// A suggestion with the id and title it is announced under.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Match {
    #[serde(flatten)]
    pub candidate: Candidate,
    pub id: String,
    pub title: String,
}

/// What `identify` answers.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Identification {
    pub link: Link,
    pub parsed: ParsedName,
    pub candidates: Vec<Match>,
    #[serde(rename = "match")]
    pub matched: Option<String>,
    pub plain: Plain,
}

/// How `identify` reaches Cinemeta and `ubit`.
#[derive(Debug, Clone)]
pub struct IdentifyOptions {
    pub cinemeta_url: String,
    pub config_dir: String,
    pub ubit: String,
}

impl Default for IdentifyOptions {
    fn default() -> Self {
        Self {
            cinemeta_url: String::new(),
            config_dir: String::new(),
            ubit: "ubit".to_owned(),
        }
    }
}

/// `identify`: what the link is, the release name inside it, and the suggestion to announce it under.
pub async fn identify(url: &str, opts: &IdentifyOptions) -> Result<Identification, IdentifyError> {
    let link = probe(
        url,
        &ProbeOptions {
            config_dir: opts.config_dir.clone(),
            ubit: opts.ubit.clone(),
            ubit_timeout: INFO_TIMEOUT,
        },
    )
    .await?;
    let mut parsed = parse_name(link.name());
    if !link.resolution().is_empty() {
        parsed.resolution = link.resolution().to_owned();
    }
    // Page titles are free text; only ones that read like a release (a year or an episode) get suggestions.
    let lookup = link.kind() == "file" || parsed.year.is_some() || parsed.season.is_some();
    let candidates = if lookup {
        cinemeta_search(
            &opts.cinemeta_url,
            &parsed.title,
            parsed.year,
            parsed.season,
        )
        .await
        .unwrap_or_default()
    } else {
        Vec::new()
    };
    let episode = match (parsed.season, parsed.episode) {
        (Some(season), Some(number)) => format!(" S{season:02}E{number:02}"),
        _ => String::new(),
    };
    let plain_title = {
        let name = if link.kind() == "page" {
            link.name().to_owned()
        } else {
            format!("{}{episode}", parsed.title)
        };
        if name.is_empty() {
            parsed.title.clone()
        } else {
            name
        }
    };
    let plain = Plain {
        id: clip_id(link.page_id(), url),
        title: plain_title,
    };
    let candidates: Vec<Match> = candidates
        .into_iter()
        .map(|candidate| {
            let entry = candidate_entry(&candidate, &parsed);
            Match {
                candidate,
                id: entry.id,
                title: entry.title,
            }
        })
        .collect();
    let matched = candidates
        .iter()
        .find(|candidate| strong(&candidate.candidate, &parsed))
        .map(|candidate| candidate.id.clone());
    Ok(Identification {
        link,
        parsed,
        candidates,
        matched,
        plain,
    })
}
