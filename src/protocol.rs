//! PearTube's wire formats: what peers write into a channel, how a video's blocks are laid out, and
//! every `peartube:` link, ported from the app's `src/protocol.js`.
//!
//! Strings are parsed with JavaScript semantics (UTF-16 lengths, JS whitespace, `toWellFormed`,
//! `encodeURIComponent`/`decodeURIComponent`), because the same values cross the IPC boundary and
//! must round-trip into the app.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value;

/// Roles a channel grant may name.
pub const ROLES: [&str; 3] = ["owner", "mod", "writer"];
/// Maximum title length, in UTF-16 code units.
pub const MAX_TITLE: usize = 300;
/// Blob block size used by PearTube's uniform blob layout.
pub const BLOB_BLOCK_SIZE: u64 = 512 * 1024;
/// Extensions PearTube treats as media files (lowercase).
pub const MEDIA_EXTENSIONS: [&str; 21] = [
    "mp4", "m4v", "mkv", "webm", "avi", "mov", "ts", "m2ts", "mts", "mpg", "mpeg", "wmv", "flv",
    "ogv", "3gp", "ogg", "flac", "mp3", "opus", "wav", "aac",
];
/// Port of a node's HTTP API.
pub const API_PORT: u16 = 8174;
/// Port of a TV receiver's pairing/command server.
pub const RECEIVER_PORT: u16 = 8176;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// `ID`: `kind:name` with an optional `:sNNeNN(N)` episode suffix, lowercase only.
pub fn is_id(s: &str) -> bool {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return false;
    }
    let name = |p: &str| {
        !p.is_empty()
            && p.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    };
    if !name(parts[0]) || !name(parts[1]) {
        return false;
    }
    parts.len() == 2 || is_episode_part(parts[2])
}

fn is_episode_part(p: &str) -> bool {
    let b = p.as_bytes();
    (b.len() == 6 || b.len() == 7)
        && b[0] == b's'
        && b[3] == b'e'
        && b[1..3].iter().all(u8::is_ascii_digit)
        && b[4..].iter().all(u8::is_ascii_digit)
}

fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// `HEX64`: exactly 64 lowercase hex characters.
pub fn is_hex64(s: &str) -> bool {
    is_hex(s, 64)
}

/// `HEX128`: exactly 128 lowercase hex characters.
pub fn is_hex128(s: &str) -> bool {
    is_hex(s, 128)
}

/// `HEX144`: exactly 144 lowercase hex characters.
pub fn is_hex144(s: &str) -> bool {
    is_hex(s, 144)
}

/// `MEDIA_FILE`: the file name ends in a media extension, case-insensitively.
pub fn is_media_file(name: &str) -> bool {
    let Some(dot) = name.rfind('.') else {
        return false;
    };
    let ext = name[dot + 1..].to_lowercase();
    MEDIA_EXTENSIONS.contains(&ext.as_str())
}

/// The byte layout of a blob, as `isUniform512` sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobSize {
    pub byte_length: u64,
    pub block_length: u64,
}

/// `isUniform512`: the blob fills whole 512 KiB blocks, or is empty.
pub fn is_uniform_512(blob: Option<&BlobSize>) -> bool {
    let Some(blob) = blob else {
        return false;
    };
    if blob.byte_length == 0 {
        return true;
    }
    blob.block_length > 0
        && (blob.block_length - 1).saturating_mul(BLOB_BLOCK_SIZE) < blob.byte_length
        && blob.byte_length <= blob.block_length.saturating_mul(BLOB_BLOCK_SIZE)
}

/// What a catalog entry is: an episode of a series, or a film.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EntryInfo {
    pub season: Option<u32>,
    pub episode: Option<u32>,
    #[serde(rename = "seriesKey")]
    pub series_key: String,
    #[serde(rename = "baseTitle")]
    pub base_title: String,
    pub year: Option<String>,
}

/// `describeEntry`.
pub fn describe_entry(id: &str, title: &str) -> EntryInfo {
    let episode = episode_suffix(id);
    let raw = js_trim(title).to_owned();
    let (base_title, year) = trailing_year(&raw).unwrap_or_else(|| (raw.clone(), None));
    EntryInfo {
        season: episode.map(|(s, _, _)| s),
        episode: episode.map(|(_, e, _)| e),
        series_key: match episode {
            Some((_, _, start)) => id[..start].to_lowercase(),
            None => raw.to_lowercase(),
        },
        base_title,
        year,
    }
}

/// Returns `(season, episode, byte index of the ':')` of a trailing `:sNNeNN(N)` suffix.
fn episode_suffix(id: &str) -> Option<(u32, u32, usize)> {
    let len = id.len();
    let check = |start: usize, digits: usize| -> Option<(u32, u32)> {
        if start == 0 || !id.is_char_boundary(start) || !id.is_char_boundary(start + 1) {
            return None;
        }
        let rest = id.get(start + 1..)?;
        let b = rest.as_bytes();
        if b.len() != 4 + digits
            || !b[0].eq_ignore_ascii_case(&b's')
            || !b[3].eq_ignore_ascii_case(&b'e')
        {
            return None;
        }
        let num = |s: &[u8]| {
            if s.iter().all(u8::is_ascii_digit) {
                Some(s.iter().fold(0u32, |n, d| n * 10 + u32::from(d - b'0')))
            } else {
                None
            }
        };
        Some((num(&b[1..3])?, num(&b[4..])?))
    };
    if len >= 8
        && let Some((s, e)) = check(len - 8, 3)
    {
        return Some((s, e, len - 8));
    }
    if len >= 7
        && let Some((s, e)) = check(len - 7, 2)
    {
        return Some((s, e, len - 7));
    }
    None
}

/// `^(.*\S)\s*\((19|20)(\d{2})\)$` then `^(.*\S)\s+(19|20)(\d{2})$`, with JS `.` (no line
/// terminators) and JS `\s`.
fn trailing_year(raw: &str) -> Option<(String, Option<String>)> {
    if let Some(core) = raw.strip_suffix(')')
        && let Some((head, year)) = split_year(core)
        && let Some(head) = head.strip_suffix('(')
    {
        let base = js_trim_end(head);
        if !base.is_empty() && !contains_line_terminator(base) {
            return Some((base.to_owned(), Some(year)));
        }
    }
    let (head, year) = split_year(raw)?;
    if head.is_empty() || !head.ends_with(is_js_space) {
        return None;
    }
    let base = js_trim_end(head);
    if base.is_empty() || contains_line_terminator(base) {
        return None;
    }
    Some((base.to_owned(), Some(year)))
}

fn split_year(s: &str) -> Option<(&str, String)> {
    let at = s.len().checked_sub(4)?;
    if !s.is_char_boundary(at) {
        return None;
    }
    let (head, year) = s.split_at(at);
    let b = year.as_bytes();
    if b.len() != 4
        || !b.iter().all(u8::is_ascii_digit)
        || !(year.starts_with("19") || year.starts_with("20"))
    {
        return None;
    }
    Some((head, year.to_owned()))
}

fn contains_line_terminator(s: &str) -> bool {
    s.contains(['\n', '\r', '\u{2028}', '\u{2029}'])
}

/// `str(v, re)`: a string that matches the validator.
fn str_matches(v: Option<&Value>, f: fn(&str) -> bool) -> bool {
    v.and_then(Value::as_str).is_some_and(f)
}

/// A JSON number that is an integer in JavaScript's safe range.
fn safe_int(v: &Value) -> Option<i64> {
    let n = v.as_i64()?;
    (-9_007_199_254_740_991..=9_007_199_254_740_991)
        .contains(&n)
        .then_some(n)
}

fn safe_uint(v: Option<&Value>) -> Option<u64> {
    let n = safe_int(v?)?;
    (n >= 0).then_some(n as u64)
}

/// `cleanText`: falsy inputs fall back, then the first 300 UTF-16 units, made well-formed.
pub fn clean_text(value: Option<&str>, fallback: &str) -> String {
    let s = match value {
        Some(v) if !v.is_empty() => v,
        _ => fallback,
    };
    truncate_utf16(s, MAX_TITLE)
}

/// `cleanName`.
pub fn clean_name(value: Option<&str>, fallback: &str) -> String {
    let s = match value {
        Some(v) if !v.is_empty() => v,
        _ => fallback,
    };
    let out = truncate_utf16(js_trim(s), 80);
    if out.is_empty() {
        fallback.to_owned()
    } else {
        out
    }
}

/// A blob pointer inside a video announcement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BlobPointer {
    #[serde(rename = "blockOffset")]
    pub block_offset: u64,
    #[serde(rename = "blockLength")]
    pub block_length: u64,
    #[serde(rename = "byteOffset")]
    pub byte_offset: u64,
    #[serde(rename = "byteLength")]
    pub byte_length: u64,
}

/// The entry fields `formatWatchUri` reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct WatchEntry {
    pub id: String,
    pub title: String,
    pub sha256: Option<String>,
    pub blobs: String,
    /// `entry.blob || {}`.
    pub blob: Option<BlobPointer>,
}

/// `formatWatchUri`: a read-only watch link. Only channel attribution is ever included.
pub fn format_watch_uri(entry: &WatchEntry, source: &str, source_kind: &str) -> String {
    let mut q = vec![
        format!("id={}", encode_uri_component(&entry.id)),
        format!(
            "title={}",
            encode_uri_component(&clean_text(Some(&entry.title), &entry.id))
        ),
    ];
    if entry.sha256.as_deref().is_some_and(is_hex64) {
        q.push(format!("sha256={}", entry.sha256.as_deref().unwrap_or("")));
    }
    if source_kind == "channel" && is_hex64(source) {
        q.push(format!("source={source}"));
        q.push("kind=channel".to_owned());
    }
    let b = entry.blob.unwrap_or_default();
    format!(
        "peartube://watch/{}:{}:{}:{}:{}?{}",
        entry.blobs,
        b.block_offset,
        b.block_length,
        b.byte_offset,
        b.byte_length,
        q.join("&")
    )
}

/// Input of `formatReceiverUri`; every field defaults to empty/false.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ReceiverUriInput {
    pub public_key: String,
    pub device_id: String,
    pub name: String,
    pub token: String,
    pub host: String,
    pub port: f64,
    pub p2p: bool,
}

/// `formatReceiverUri`.
pub fn format_receiver_uri(input: &ReceiverUriInput) -> String {
    let mut target = if !input.public_key.is_empty() {
        input.public_key.clone()
    } else if !input.device_id.is_empty() {
        input.device_id.clone()
    } else if !input.host.is_empty() && input.port != 0.0 && !input.port.is_nan() {
        format!("{}:{}", input.host, js_number_string(input.port))
    } else {
        String::new()
    };
    target = target.to_lowercase();
    let mut q = Vec::new();
    if !input.name.is_empty() {
        q.push(format!(
            "name={}",
            encode_uri_component(&clean_name(Some(&input.name), "PearTube TV"))
        ));
    }
    if !input.token.is_empty() {
        q.push(format!("token={}", encode_uri_component(&input.token)));
    }
    if !input.host.is_empty() {
        q.push(format!("host={}", encode_uri_component(&input.host)));
    }
    if input.port > 0.0 {
        q.push(format!("port={}", js_number_string(input.port)));
    }
    q.push(format!("p2p={}", u8::from(input.p2p)));
    format!("peartube://receiver/{target}?{}", q.join("&"))
}

/// The result of `parseUri`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum ParsedUri {
    Source {
        key: String,
        kind: &'static str,
        alias: String,
    },
    Access {
        channel: String,
        epoch: u64,
        #[serde(rename = "epochKey")]
        epoch_key: String,
        alias: String,
    },
    Watch {
        #[serde(rename = "sourceKey")]
        source_key: Option<String>,
        #[serde(rename = "sourceKind")]
        source_kind: &'static str,
        entry: ParsedEntry,
    },
    Receiver {
        #[serde(rename = "publicKey")]
        public_key: String,
        #[serde(rename = "deviceId")]
        device_id: String,
        host: String,
        port: JsNumber,
        token: String,
        name: String,
        p2p: bool,
    },
}

/// The announcement `parseUri` builds for a watch link.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ParsedEntry {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub id: String,
    pub title: String,
    pub size: u64,
    pub sha256: String,
    pub blobs: String,
    pub blob: BlobPointer,
}

/// A `parseUri` failure; the message matches the app's thrown `Error`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UriError(String);

impl UriError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for UriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for UriError {}

type Query = HashMap<String, String>;

/// `parseUri`: a PearTube link or raw 64-hex key, as the one thing it asks for.
pub fn parse_uri(input: &str) -> Result<ParsedUri, UriError> {
    let raw = js_trim(input);
    let lower = raw.to_lowercase();
    if is_hex64(&lower) {
        return Ok(ParsedUri::Source {
            key: lower,
            kind: "channel",
            alias: String::new(),
        });
    }
    let (verb, body, query) =
        split_uri(raw).ok_or_else(|| UriError::new("Invalid PearTube URI or 64-hex key"))?;
    let body = js_trim(body);
    let q = parse_query(query);
    let verb = verb.to_ascii_lowercase();
    match verb.as_str() {
        "channel" | "tracker" | "add" | "source" => {
            let key = body.to_lowercase();
            if !is_hex64(&key) {
                return Err(UriError::new("Channel key must be 64 hex characters"));
            }
            Ok(ParsedUri::Source {
                key,
                kind: "channel",
                alias: clean_text(first_truthy(&q, &["name", "alias"]), ""),
            })
        }
        "receiver" | "cast" => parse_receiver(body, &q),
        "access" => parse_access(body, &q),
        "watch" => parse_watch(body, &q),
        other => Err(UriError::new(format!(
            "Unsupported PearTube URI action: {other}"
        ))),
    }
}

/// `^peartube:(?:\/\/)?([a-z]+)\/([^?#]+)(?:\?([^#]*))?`, case-insensitively on the scheme.
fn split_uri(raw: &str) -> Option<(&str, &str, &str)> {
    let bytes = raw.as_bytes();
    if bytes.len() < 9 || !bytes[..9].eq_ignore_ascii_case(b"peartube:") {
        return None;
    }
    let mut i = 9;
    if bytes.get(i) == Some(&b'/') && bytes.get(i + 1) == Some(&b'/') {
        i += 2;
    }
    let start = i;
    while bytes.get(i).is_some_and(u8::is_ascii_alphabetic) {
        i += 1;
    }
    if i == start || bytes.get(i) != Some(&b'/') {
        return None;
    }
    let verb = &raw[start..i];
    let body_start = i + 1;
    let mut j = body_start;
    while !matches!(bytes.get(j), Some(&b'?') | Some(&b'#') | None) {
        j += 1;
    }
    if j == body_start {
        return None;
    }
    let body = &raw[body_start..j];
    let query = if bytes.get(j) == Some(&b'?') {
        let q_start = j + 1;
        let mut k = q_start;
        while !matches!(bytes.get(k), Some(&b'#') | None) {
            k += 1;
        }
        &raw[q_start..k]
    } else {
        ""
    };
    Some((verb, body, query))
}

fn first_truthy<'a>(q: &'a Query, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .filter_map(|k| q.get(*k))
        .find(|v| !v.is_empty())
        .map(String::as_str)
}

fn parse_query(raw: &str) -> Query {
    let mut out = Query::new();
    if raw.is_empty() {
        return out;
    }
    for part in raw.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = match part.find('=') {
            Some(eq) => (&part[..eq], &part[eq + 1..]),
            None => (part, ""),
        };
        let key = percent_decode(k).unwrap_or_else(|| k.to_owned());
        let value = percent_decode(&v.replace('+', " ")).unwrap_or_else(|| v.to_owned());
        out.insert(key, value);
    }
    out
}

fn parse_receiver(body: &str, q: &Query) -> Result<ParsedUri, UriError> {
    let clean = js_trim(body).to_lowercase();
    let pk = q.get("pk").map(|s| s.to_lowercase()).unwrap_or_default();
    let public_key = if is_hex64(&clean) {
        clean.clone()
    } else if is_hex64(&pk) {
        pk
    } else {
        String::new()
    };
    let hp = if public_key.is_empty() && clean.contains(':') {
        clean.as_str()
    } else {
        ""
    };
    let host = match q.get("host") {
        Some(host) if !host.is_empty() => host.clone(),
        _ if !hp.is_empty() => hp[..hp.rfind(':').unwrap_or(0)].to_owned(),
        _ => String::new(),
    };
    let port_text = match q.get("port") {
        Some(port) if !port.is_empty() => port.clone(),
        _ if !hp.is_empty() => hp[hp.rfind(':').unwrap_or(0) + 1..].to_owned(),
        _ => "0".to_owned(),
    };
    let port = js_number(&port_text);
    let port = if port == 0.0 || port.is_nan() {
        0.0
    } else {
        port
    };
    let device_id = if public_key.is_empty() && hp.is_empty() {
        clean
            .chars()
            .filter(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '-')
            .take(64)
            .collect()
    } else {
        truncate_utf16(q.get("id").map(String::as_str).unwrap_or(""), 64)
    };
    if public_key.is_empty() && device_id.is_empty() && (host.is_empty() || port <= 0.0) {
        return Err(UriError::new(
            "Receiver URI needs a 64-hex key, device ID, or host:port",
        ));
    }
    Ok(ParsedUri::Receiver {
        p2p: q.get("p2p").is_some_and(|p| p == "1") || !public_key.is_empty(),
        public_key,
        device_id,
        host,
        port: JsNumber(port),
        token: q.get("token").cloned().unwrap_or_default(),
        name: clean_name(first_truthy(q, &["name", "alias"]), "Paired TV"),
    })
}

fn parse_access(body: &str, q: &Query) -> Result<ParsedUri, UriError> {
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 3
        || !is_hex64(&parts[0].to_lowercase())
        || !is_hex64(&parts[2].to_lowercase())
    {
        return Err(UriError::new("Malformed peartube://access pass"));
    }
    let epoch = js_number(parts[1]);
    if !(epoch.is_finite() && epoch.fract() == 0.0 && (1.0..=MAX_SAFE_INTEGER).contains(&epoch)) {
        return Err(UriError::new("Malformed peartube://access epoch"));
    }
    Ok(ParsedUri::Access {
        channel: parts[0].to_lowercase(),
        epoch: epoch as u64,
        epoch_key: parts[2].to_lowercase(),
        alias: clean_text(first_truthy(q, &["name", "alias"]), ""),
    })
}

fn parse_watch(body: &str, q: &Query) -> Result<ParsedUri, UriError> {
    let parts: Vec<&str> = body.split(':').collect();
    if parts.len() != 5 || !is_hex64(&parts[0].to_lowercase()) {
        return Err(UriError::new("Malformed peartube://watch blob pointer"));
    }
    let blobs = parts[0].to_lowercase();
    let nums: Vec<f64> = parts[1..].iter().map(|p| js_number(p)).collect();
    let safe = |n: &f64| n.is_finite() && n.fract() == 0.0 && (0.0..=MAX_SAFE_INTEGER).contains(n);
    if !nums.iter().all(safe) || nums[1] <= 0.0 {
        return Err(UriError::new("Malformed peartube://watch block bounds"));
    }
    let [block_offset, block_length, byte_offset, byte_length] =
        [nums[0], nums[1], nums[2], nums[3]];
    let sha_raw = q.get("sha256").map(String::as_str).unwrap_or("");
    let sha_lower = sha_raw.to_lowercase();
    let sha256 = if !sha_raw.is_empty() && is_hex64(&sha_lower) {
        sha_lower
    } else {
        "0".repeat(64)
    };
    let basis = if sha_raw.is_empty() { &blobs } else { sha_raw };
    let fallback_id = format!("clip:{}", truncate_utf16(&basis.to_lowercase(), 12));
    let id = match q.get("id") {
        Some(id) => {
            let lower = id.to_lowercase();
            if is_id(&lower) { lower } else { fallback_id }
        }
        None => fallback_id,
    };
    let source = q.get("source").map(String::as_str).unwrap_or("");
    let source_lower = source.to_lowercase();
    let source_key = if q.get("kind").map(String::as_str) != Some("tracker")
        && !source.is_empty()
        && is_hex64(&source_lower)
    {
        Some(source_lower)
    } else {
        None
    };
    Ok(ParsedUri::Watch {
        source_key,
        source_kind: "channel",
        entry: ParsedEntry {
            kind: "announce",
            title: clean_text(q.get("title").map(String::as_str), &id),
            id,
            size: byte_length as u64,
            sha256,
            blobs,
            blob: BlobPointer {
                block_offset: block_offset as u64,
                block_length: block_length as u64,
                byte_offset: byte_offset as u64,
                byte_length: byte_length as u64,
            },
        },
    })
}

/// `decodeOp`: a channel op as JSON, if it passes every field check. The parsed object is returned
/// unchanged, including unknown fields.
pub fn decode_op(value: impl AsRef<[u8]>) -> Option<Value> {
    let text = String::from_utf8_lossy(value.as_ref());
    let mut op: Value = serde_json::from_str(&text).ok()?;
    canonicalize_numbers(&mut op);
    if op.get("claim").is_some() && !str_matches(op.get("claim"), is_hex128) {
        return None;
    }
    match op.get("type").and_then(Value::as_str)? {
        "init" => {
            if op.get("owner").is_some() && !str_matches(op.get("owner"), is_hex64) {
                return None;
            }
            if let Some(epoch) = op.get("epoch")
                && safe_uint(Some(epoch)).is_none_or(|e| e < 1)
            {
                return None;
            }
            Some(op)
        }
        "owner" if str_matches(op.get("owner"), is_hex64) => Some(op),
        "epoch" if safe_uint(op.get("epoch")).is_some_and(|e| e >= 1) => Some(op),
        "claim" if str_matches(op.get("sig"), is_hex128) => Some(op),
        "grant" if str_matches(op.get("writer"), is_hex64) && valid_role(op.get("role")) => {
            Some(op)
        }
        "revoke" if str_matches(op.get("writer"), is_hex64) => Some(op),
        "remove" if op.get("key").is_some_and(Value::is_string) => Some(op),
        "source" => {
            let kind = op.get("kind").and_then(Value::as_str);
            if !str_matches(op.get("sourceKey"), is_hex64)
                || !matches!(kind, Some("tracker" | "channel"))
            {
                return None;
            }
            let title = op.get("title").and_then(Value::as_str)?;
            let len = utf16_len(title);
            (1..=MAX_TITLE).contains(&len).then_some(op)
        }
        "announce" => {
            if !str_matches(op.get("id"), is_id)
                || !str_matches(op.get("blobs"), is_hex64)
                || !str_matches(op.get("sha256"), is_hex64)
                || op
                    .get("title")
                    .and_then(Value::as_str)
                    .is_none_or(|t| utf16_len(t) > MAX_TITLE)
                || safe_uint(op.get("size")).is_none()
            {
                return None;
            }
            if let Some(epoch) = op.get("epoch")
                && safe_uint(Some(epoch)).is_none_or(|e| e < 1)
            {
                return None;
            }
            if op.get("wrappedKey").is_some() && !str_matches(op.get("wrappedKey"), is_hex144) {
                return None;
            }
            let blob = op.get("blob").filter(|b| b.is_object());
            let field = |k: &str| blob.and_then(|b| b.get(k)).and_then(|v| safe_uint(Some(v)));
            if field("blockOffset").is_none()
                || field("blockLength").is_none()
                || field("byteOffset").is_none()
                || field("byteLength").is_none()
            {
                return None;
            }
            Some(op)
        }
        _ => None,
    }
}

/// `ROLES.has(role || 'writer')`, including JavaScript falsiness.
fn valid_role(role: Option<&Value>) -> bool {
    match role {
        None | Some(Value::Null) | Some(Value::Bool(false)) => true,
        Some(Value::String(s)) => s.is_empty() || ROLES.contains(&s.as_str()),
        Some(Value::Number(n)) => n.as_f64() == Some(0.0),
        _ => false,
    }
}

/// Turns integral JSON numbers that JS treats as integers into integers, so output and
/// `Number.isSafeInteger` checks match `JSON.parse`.
fn canonicalize_numbers(v: &mut Value) {
    match v {
        Value::Number(n) => {
            if let Some(f) = n.as_f64() {
                if f == 0.0 {
                    *v = Value::from(0);
                } else if f.is_finite() && f.fract() == 0.0 && f.abs() <= MAX_SAFE_INTEGER {
                    *v = Value::from(f as i64);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(canonicalize_numbers),
        Value::Object(o) => o.values_mut().for_each(canonicalize_numbers),
        _ => {}
    }
}

/// Channel view keys that are not video entries.
pub fn is_system_view_key(key: &str) -> bool {
    ["peer/", "source/", "meta/", "writer/", "revoked/"]
        .iter()
        .any(|p| key.starts_with(p))
}

/// Lowercase hex, as `b4a.toString(buf, 'hex')`.
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[usize::from(b >> 4)] as char);
        out.push(HEX[usize::from(b & 0xf)] as char);
    }
    out
}

/// Lowercase hex, as `b4a.from(hex, 'hex')`; `None` for odd lengths or non-hex digits.
pub(crate) fn hex_decode(s: &str) -> Option<Vec<u8>> {
    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks(2) {
        out.push(nibble(pair[0])? * 16 + nibble(pair[1])?);
    }
    Some(out)
}

/// A JSON number serialized the way `JSON.stringify` prints it for the values ports can take.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct JsNumber(pub f64);

impl Serialize for JsNumber {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = self.0;
        if !v.is_finite() {
            s.serialize_none()
        } else if v.fract() == 0.0 && v.abs() <= MAX_SAFE_INTEGER {
            s.serialize_i64(v as i64)
        } else {
            s.serialize_f64(v)
        }
    }
}

fn js_number_string(n: f64) -> String {
    if n.is_nan() {
        return "NaN".to_owned();
    }
    if n.is_infinite() {
        return if n > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if n == 0.0 {
        return "0".to_owned();
    }
    // shortcut: exponent formats (`1e+21`) differ from Rust's; ports are small integers in practice.
    if n.fract() == 0.0 && n.abs() < 1e21 {
        format!("{n:.0}")
    } else {
        n.to_string()
    }
}

/// `encodeURIComponent`.
pub fn encode_uri_component(s: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        match b {
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
            | b')' => out.push(b as char),
            _ => {
                out.push('%');
                out.push(HEX[usize::from(b >> 4)] as char);
                out.push(HEX[usize::from(b & 0xf)] as char);
            }
        }
    }
    out
}

/// `decodeURIComponent`, as `Option` instead of a throw.
fn percent_decode(s: &str) -> Option<String> {
    fn hex(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            b'A'..=b'F' => Some(b - b'A' + 10),
            _ => None,
        }
    }
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex(*bytes.get(i + 1)?)?;
            let lo = hex(*bytes.get(i + 2)?)?;
            out.push(hi * 16 + lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// JavaScript's `\s` (WhiteSpace + LineTerminator + U+FEFF).
pub(crate) fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

pub(crate) fn js_trim(s: &str) -> &str {
    js_trim_end(js_trim_start(s))
}

pub(crate) fn js_trim_start(s: &str) -> &str {
    s.trim_start_matches(is_js_space)
}

pub(crate) fn js_trim_end(s: &str) -> &str {
    s.trim_end_matches(is_js_space)
}

/// JavaScript `String.prototype.length`.
pub(crate) fn utf16_len(s: &str) -> usize {
    s.encode_utf16().count()
}

/// JS `slice(0, max)` followed by `toWellFormed`.
pub(crate) fn truncate_utf16(s: &str, max: usize) -> String {
    let units: Vec<u16> = s.encode_utf16().collect();
    if units.len() <= max {
        return s.to_owned();
    }
    char::decode_utf16(units[..max].iter().copied())
        .map(|r| r.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect()
}

/// `Number(string)`.
pub(crate) fn js_number(s: &str) -> f64 {
    let t = js_trim(s);
    if t.is_empty() {
        return 0.0;
    }
    match t {
        "Infinity" | "+Infinity" => return f64::INFINITY,
        "-Infinity" => return f64::NEG_INFINITY,
        _ => {}
    }
    if t.len() > 2 {
        let radix = match &t[..2] {
            "0x" | "0X" => Some(16),
            "0o" | "0O" => Some(8),
            "0b" | "0B" => Some(2),
            _ => None,
        };
        if let Some(radix) = radix {
            let mut v = 0.0;
            let digits = &t[2..];
            if digits.is_empty() {
                return f64::NAN;
            }
            for c in digits.chars() {
                let Some(d) = c.to_digit(radix) else {
                    return f64::NAN;
                };
                v = v * f64::from(radix) + f64::from(d);
            }
            return v;
        }
    }
    let body = t.strip_prefix(['+', '-']).unwrap_or(t);
    let numeric = body.bytes().all(|b| {
        b.is_ascii_digit() || b == b'.' || b == b'e' || b == b'E' || b == b'+' || b == b'-'
    });
    if !numeric || !body.bytes().any(|b| b.is_ascii_digit()) {
        return f64::NAN;
    }
    t.parse::<f64>().unwrap_or(f64::NAN)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validators() {
        assert!(is_id("clip:abc"));
        assert!(is_id("series:name:s01e02"));
        assert!(is_id("series:name:s01e234"));
        assert!(!is_id("clip:ABC"));
        assert!(!is_id("clip:abc:s1e02"));
        assert!(!is_id("a:b:s01e02x"));
        assert!(is_hex64(&"0".repeat(64)));
        assert!(!is_hex64(&"0".repeat(63)));
        assert!(!is_hex64(&"A".repeat(64)));
        assert!(is_media_file("Film.MP4"));
        assert!(is_media_file("a.b/c.mp3"));
        assert!(!is_media_file("a.mp4.txt"));
        assert!(!is_media_file("mp4"));
        assert!(is_system_view_key("writer/abc"));
        assert!(!is_system_view_key("clip/abc"));
    }

    #[test]
    fn uniform() {
        let b = |byte_length, block_length| BlobSize {
            byte_length,
            block_length,
        };
        assert!(!is_uniform_512(None));
        assert!(is_uniform_512(Some(&b(0, 0))));
        assert!(is_uniform_512(Some(&b(1, 1))));
        assert!(is_uniform_512(Some(&b(BLOB_BLOCK_SIZE, 1))));
        assert!(!is_uniform_512(Some(&b(BLOB_BLOCK_SIZE + 1, 1))));
        assert!(is_uniform_512(Some(&b(BLOB_BLOCK_SIZE + 1, 2))));
        assert!(!is_uniform_512(Some(&b(1, 0))));
    }

    #[test]
    fn js_numbers() {
        assert_eq!(js_number("8176"), 8176.0);
        assert_eq!(js_number(" 0x10 "), 16.0);
        assert_eq!(js_number("2.5"), 2.5);
        assert_eq!(js_number(""), 0.0);
        assert!(js_number("inf").is_nan());
        assert!(js_number("1x").is_nan());
        assert!(js_number("+-1").is_nan());
    }

    #[test]
    fn utf16_slicing_well_forms() {
        let s = format!("{}😀", "a".repeat(299));
        // 299 + 2 units: the cut splits the surrogate pair and leaves one U+FFFD.
        let out = clean_text(Some(&s), "");
        assert_eq!(out.chars().count(), 300);
        assert!(out.ends_with('\u{fffd}'));
        assert_eq!(utf16_len(&out), 300);
    }
}
