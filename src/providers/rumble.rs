//! Rumble provider for PearTube peers and relays, ported from `src/providers/rumble.js`:
//!
//!   - Searches Rumble via `https://rumble.com/search/video?q=<query>` through the guarded fetch.
//!   - Parses video result cards (permalink, title, channel, duration, cached thumbnail URL).
//!   - Saves chosen videos via `ubit` into `<staging>/<Title> [Rumble]/`, or with `audio: true`
//!     into `<staging>/<Artist> - <Album> [Rumble]/NN - Title.<ext>` pinned to an `rmb:` release,
//!     then triggers a staging scan.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use super::staging::{
    ALBUM_PIN, Cancelled, Place, PlaceAndScan, PlaceError, free_path, note_mirrored, placed_here,
    safe_segment, split_ext,
};
use super::youtube::{Backend, Fetch, Picture, ProgressFn, YoutubeError};
use crate::acquire::RandomSource;
use crate::app_settings::{js_string_or_empty, js_truthy};
use crate::identify::{Controller, js_num};
use crate::protocol::{
    MAX_TITLE, encode_uri_component, is_media_file, js_trim, js_trim_end, truncate_utf16,
};
use crate::tags::album_key;

pub const NAME: &str = "rumble";
pub const DESCRIPTION: &str =
    "Finds videos on Rumble and saves video or audio into the staging folder";
pub const RUMBLE_SEARCH_CAP: usize = 20;
const MAX_TRACKS: usize = 100;
const JOBS_MOST: usize = 48;
const JOBS_KEPT: usize = 32;
const THUMB_URLS_KEPT: usize = 256;
const THUMBS_KEPT: usize = 64;

pub struct Options {
    pub storage: String,
    pub staging_root: Option<String>,
    pub staging_label: String,
    pub backend: Option<Arc<dyn Backend>>,
    pub fetch: Option<Arc<dyn Fetch>>,
    pub search_base: String,
    pub site_origin: String,
    pub search_timeout_ms: f64,
    pub onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    pub random: Arc<dyn RandomSource>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Options {
    pub fn new(
        storage: impl Into<String>,
        backend: Option<Arc<dyn Backend>>,
        fetch: Option<Arc<dyn Fetch>>,
        random: Arc<dyn RandomSource>,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            storage: storage.into(),
            staging_root: None,
            staging_label: "storage".to_owned(),
            backend,
            fetch,
            search_base: "https://rumble.com/search/video".to_owned(),
            site_origin: "https://rumble.com".to_owned(),
            search_timeout_ms: 20_000.0,
            onchange: None,
            random,
            now_ms,
        }
    }
}

fn is_rumble_id(text: &str) -> bool {
    let bytes = text.as_bytes();
    (3..=33).contains(&bytes.len())
        && (bytes[0] == b'v' || bytes[0] == b'V')
        && bytes[1..].iter().all(u8::is_ascii_alphanumeric)
}

fn is_http_url(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn js_round(number: f64) -> f64 {
    (number + 0.5).floor()
}

pub fn rumble_release(artist: &str, album: &str, year: Option<u32>) -> String {
    let digest = Sha256::digest(album_key(Some(artist), Some(album), year).as_bytes());
    let mut hex = String::with_capacity(20);
    for byte in &digest[..10] {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("rmb:{hex}")
}

fn decode_html(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;
    while let Some(ch) = rest.chars().next() {
        if ch == '&'
            && let Some(semi) = rest.find(';')
            && let Some(entity) = rest.get(1..semi)
        {
            let replaced = match entity {
                "amp" => Some("&".to_owned()),
                "lt" => Some("<".to_owned()),
                "gt" => Some(">".to_owned()),
                "quot" => Some("\"".to_owned()),
                "apos" | "#39" => Some("'".to_owned()),
                _ if entity.starts_with("#x") || entity.starts_with("#X") => entity
                    .get(2..)
                    .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    .and_then(char::from_u32)
                    .map(|c| c.to_string()),
                _ if entity.starts_with('#') => entity
                    .get(1..)
                    .and_then(|num| num.parse::<u32>().ok())
                    .and_then(char::from_u32)
                    .map(|c| c.to_string()),
                _ => None,
            };
            if let Some(text) = replaced {
                out.push_str(&text);
                rest = rest.get(semi + 1..).unwrap_or("");
                continue;
            }
        }
        out.push(ch);
        rest = rest.get(ch.len_utf8()..).unwrap_or("");
    }
    out
}

fn strip_tags(raw: &str) -> String {
    let mut plain = String::with_capacity(raw.len());
    let mut in_tag = false;
    for ch in raw.chars() {
        if ch == '<' {
            in_tag = true;
            plain.push(' ');
        } else if ch == '>' {
            in_tag = false;
        } else if !in_tag {
            plain.push(ch);
        }
    }
    let decoded = decode_html(&plain);
    decoded.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub fn parse_duration(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if s.is_empty() {
        return None;
    }
    let parts: Vec<u64> = s
        .split(':')
        .map(|p| p.trim().parse::<u64>().ok())
        .collect::<Option<Vec<_>>>()?;
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let total = parts.into_iter().fold(0u64, |acc, n| acc * 60 + n);
    (total > 0).then_some(total)
}

fn find_attr_value<'a>(tag: &'a str, attr: &str) -> Option<&'a str> {
    let low = tag.to_ascii_lowercase();
    let needle = format!("{attr}=");
    let mut offset = 0;
    while let Some(rel) = low.get(offset..).and_then(|s| s.find(&needle)) {
        let pos = offset + rel;
        let boundary_ok = pos == 0
            || !low.as_bytes()[pos - 1].is_ascii_alphanumeric() && low.as_bytes()[pos - 1] != b'-';
        offset = pos + needle.len();
        if !boundary_ok {
            continue;
        }
        let rest = tag.get(offset..)?;
        let quote = *rest.as_bytes().first()?;
        if quote != b'"' && quote != b'\'' {
            continue;
        }
        let end = rest.get(1..)?.find(quote as char)?;
        return rest.get(1..1 + end);
    }
    None
}

fn extract_rumble_link(chunk: &str, site_origin: &str) -> Option<(String, String)> {
    let base = Url::parse(site_origin).ok()?;
    let low = chunk.to_ascii_lowercase();
    let mut offset = 0;
    while let Some(rel) = low.get(offset..).and_then(|s| s.find("href=")) {
        let pos = offset + rel + 5;
        offset = pos;
        let Some(rest) = chunk.get(pos..) else {
            continue;
        };
        let Some(&quote) = rest
            .as_bytes()
            .first()
            .filter(|&&b| b == b'"' || b == b'\'')
        else {
            continue;
        };
        let Some(end) = rest.get(1..).and_then(|s| s.find(quote as char)) else {
            continue;
        };
        let Some(raw_href) = rest.get(1..1 + end) else {
            continue;
        };
        let Ok(resolved) = base.join(raw_href) else {
            continue;
        };
        let path = resolved.path();
        if !path.to_ascii_lowercase().ends_with(".html") {
            continue;
        }
        let stem = path.rsplit('/').next().unwrap_or("");
        let id_part = stem.split(['-', '.']).next().unwrap_or("");
        if is_rumble_id(id_part) {
            return Some((id_part.to_ascii_lowercase(), resolved.into()));
        }
    }
    None
}

fn tag_inner<'a>(chunk: &'a str, tag: &str) -> Option<&'a str> {
    let low = chunk.to_ascii_lowercase();
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let start = low.find(&open)?;
    let gt = low.get(start..)?.find('>')? + start + 1;
    let end = low.get(gt..)?.find(&close)? + gt;
    chunk.get(gt..end)
}

fn find_class_element<'a>(chunk: &'a str, class_fragments: &[&str]) -> Option<(&'a str, &'a str)> {
    let low = chunk.to_ascii_lowercase();
    let mut cursor = 0;
    while let Some(rel) = low.get(cursor..).and_then(|s| s.find('<')) {
        let start = cursor + rel;
        let Some(gt_rel) = low.get(start..).and_then(|s| s.find('>')) else {
            break;
        };
        let gt = start + gt_rel;
        cursor = gt + 1;
        let Some(open_tag) = chunk.get(start..=gt) else {
            continue;
        };
        let Some(class_val) = find_attr_value(open_tag, "class") else {
            continue;
        };
        let class_low = class_val.to_ascii_lowercase();
        if !class_fragments.iter().any(|f| class_low.contains(f)) {
            continue;
        }
        let name: String = open_tag
            .get(1..)
            .unwrap_or("")
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase();
        let close = format!("</{name}>");
        let inner = low
            .get(cursor..)
            .and_then(|s| s.find(&close))
            .and_then(|end| chunk.get(cursor..cursor + end))
            .unwrap_or("");
        return Some((open_tag, inner));
    }
    None
}

fn parse_card(
    chunk: &str,
    site_origin: &str,
    on_thumb: &mut Option<&mut dyn FnMut(String, String)>,
) -> Option<Value> {
    let (rumble_id, url) = extract_rumble_link(chunk, site_origin)?;
    let h3_title = tag_inner(chunk, "h3").map(strip_tags).unwrap_or_default();
    let link_title =
        find_class_element(chunk, &["video-item--a", "thumbnail__link", "title__link"])
            .and_then(|(tag, _)| find_attr_value(tag, "title"))
            .map(decode_html)
            .unwrap_or_default();
    let img_tag = {
        let low = chunk.to_ascii_lowercase();
        low.find("<img").and_then(|s| {
            low.get(s..)
                .and_then(|tail| tail.find('>'))
                .and_then(|e| chunk.get(s..=s + e))
        })
    };
    let img_alt = img_tag
        .and_then(|tag| find_attr_value(tag, "alt"))
        .map(decode_html)
        .unwrap_or_default();
    let raw_title = if !h3_title.is_empty() {
        h3_title
    } else if !link_title.trim().is_empty() {
        link_title.trim().to_owned()
    } else {
        img_alt.trim().to_owned()
    };
    let title = truncate_utf16(&raw_title, 300);
    let channel = find_class_element(chunk, &["video-item--by-a", "channel__name", "ellipsis-1"])
        .map(|(_, inner)| strip_tags(inner))
        .filter(|s| !s.is_empty())
        .or_else(|| {
            tag_inner(chunk, "address")
                .map(strip_tags)
                .filter(|s| !s.is_empty())
        })
        .map(|s| truncate_utf16(&s, 120));
    let length = find_class_element(chunk, &["duration"]).and_then(|(tag, inner)| {
        find_attr_value(tag, "data-value")
            .and_then(parse_duration)
            .or_else(|| parse_duration(&strip_tags(inner)))
    });
    if let Some(tag) = img_tag
        && let Some(raw_src) =
            find_attr_value(tag, "data-src").or_else(|| find_attr_value(tag, "src"))
        && let Ok(base) = Url::parse(site_origin)
        && let Ok(resolved) = base.join(&decode_html(raw_src))
    {
        let thumb_url: String = resolved.into();
        if is_http_url(&thumb_url)
            && let Some(cb) = on_thumb.as_mut()
        {
            cb(rumble_id.clone(), thumb_url);
        }
    }
    Some(json!({
        "title": if title.is_empty() { rumble_id.as_str() } else { title.as_str() },
        "channel": channel,
        "length": length,
        "rumbleId": rumble_id,
        "url": url,
        "thumbnail": format!("/v1/providers/rumble/thumb/{rumble_id}"),
        "video": true,
    }))
}

fn push_card(rows: &mut Vec<Value>, seen: &mut Vec<String>, card: Option<Value>) {
    if let Some(c) = card {
        let id = c["rumbleId"].as_str().unwrap_or_default().to_owned();
        if !id.is_empty() && !seen.contains(&id) {
            seen.push(id);
            rows.push(c);
        }
    }
}

fn floor_boundary(text: &str, mut index: usize) -> usize {
    index = index.min(text.len());
    while index > 0 && !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_boundary(text: &str, mut index: usize) -> usize {
    index = index.min(text.len());
    while index < text.len() && !text.is_char_boundary(index) {
        index += 1;
    }
    index
}

pub fn parse_search_html(
    html: &str,
    limit: usize,
    site_origin: &str,
    mut on_thumb: Option<&mut dyn FnMut(String, String)>,
) -> Vec<Value> {
    let low = html.to_ascii_lowercase();
    let mut rows = Vec::new();
    let mut seen: Vec<String> = Vec::new();

    let mut offsets = Vec::new();
    for marker in ["video-listing-entry", "videostream", "video-item"] {
        let mut cur = 0;
        while let Some(rel) = low.get(cur..).and_then(|s| s.find(marker)) {
            let pos = cur + rel;
            cur = pos + marker.len();
            if let Some(tag_start) = low.get(..pos).and_then(|s| s.rfind('<')) {
                offsets.push(tag_start);
            }
        }
        if !offsets.is_empty() {
            break;
        }
    }
    offsets.sort_unstable();
    offsets.dedup();
    for (idx, &start) in offsets.iter().enumerate() {
        let end = offsets.get(idx + 1).copied().unwrap_or(html.len());
        if let Some(slice) = html.get(start..end) {
            push_card(
                &mut rows,
                &mut seen,
                parse_card(slice, site_origin, &mut on_thumb),
            );
        }
        if rows.len() >= limit {
            return rows;
        }
    }
    if rows.is_empty() {
        let mut cur = 0;
        while let Some(rel) = low.get(cur..).and_then(|s| s.find("<a ")) {
            let start = cur + rel;
            cur = start + 3;
            let lo = floor_boundary(html, start.saturating_sub(400));
            let hi = ceil_boundary(html, (start + 900).min(html.len()));
            if let Some(slice) = html.get(lo..hi) {
                push_card(
                    &mut rows,
                    &mut seen,
                    parse_card(slice, site_origin, &mut on_thumb),
                );
            }
            if rows.len() >= limit {
                break;
            }
        }
    }
    rows
}

#[derive(Debug, Clone)]
struct Item {
    url: String,
    title: String,
    track: u32,
    rumble_id: String,
}

fn file_of(item: &Item, audio: bool, ext: &str) -> String {
    if audio {
        let number = format!("{:02}", item.track);
        format!(
            "{}{ext}",
            safe_segment(&format!("{number} - {}", item.title), &number)
        )
    } else {
        format!("{}{ext}", safe_segment(&item.title, "Video"))
    }
}

struct Job {
    id: String,
    controller: Controller,
    record: Mutex<JobRecord>,
}

struct JobRecord {
    entry_id: String,
    title: String,
    audio: bool,
    status: &'static str,
    phase: &'static str,
    created: u64,
    file: String,
    file_index: usize,
    file_count: usize,
    speed: u64,
    bytes: u64,
    total: Option<u64>,
    error: Option<String>,
    skipped: Vec<Value>,
    note: Option<String>,
    progress_at: u64,
    done: Option<(u64, Vec<Value>)>,
}

impl Job {
    fn record(&self) -> MutexGuard<'_, JobRecord> {
        self.record.lock().expect("rumble job")
    }

    fn status(&self) -> &'static str {
        self.record().status
    }

    fn view(&self, now: u64) -> Map<String, Value> {
        let record = self.record();
        let mut out = Map::new();
        out.insert("jobId".into(), json!(self.id));
        out.insert("id".into(), json!(record.entry_id));
        out.insert("title".into(), json!(record.title));
        out.insert("plugin".into(), json!(NAME));
        out.insert("source".into(), json!(NAME));
        out.insert("audio".into(), json!(record.audio));
        out.insert("channel".into(), json!(true));
        out.insert("private".into(), json!(false));
        out.insert("status".into(), json!(record.status));
        out.insert("phase".into(), json!(record.phase));
        out.insert("created".into(), json!(record.created));
        out.insert("peer".into(), Value::Null);
        out.insert("queuePlace".into(), Value::Null);
        out.insert("file".into(), json!(record.file));
        out.insert("fileIndex".into(), json!(record.file_index));
        out.insert("fileCount".into(), json!(record.file_count));
        out.insert("speed".into(), json!(record.speed));
        out.insert("bytes".into(), json!(record.bytes));
        out.insert("total".into(), json!(record.total));
        out.insert("error".into(), json!(record.error));
        out.insert("skipped".into(), json!(record.skipped));
        out.insert("note".into(), json!(record.note));
        if let Some((size, sha256s)) = &record.done {
            out.insert("size".into(), json!(size));
            out.insert("sha256s".into(), json!(sha256s));
        }
        let idle = matches!(record.status, "queued" | "transferring")
            .then(|| now.saturating_sub(record.progress_at));
        out.insert("idleMs".into(), json!(idle));
        out
    }
}

fn skipped_note(skipped: &[Value], count: usize) -> String {
    let said: Vec<String> = skipped
        .iter()
        .map(|entry| {
            format!(
                "{} ({})",
                entry["title"].as_str().unwrap_or_default(),
                entry["error"].as_str().unwrap_or_default()
            )
        })
        .collect();
    truncate_utf16(
        &format!(
            "{} of {count} items not saved: {}",
            skipped.len(),
            said.join("; ")
        ),
        600,
    )
}

type Pending = Shared<BoxFuture<'static, Option<Picture>>>;

struct State {
    jobs: Vec<Arc<Job>>,
    closed: bool,
    chain: Shared<BoxFuture<'static, ()>>,
    last_search_at: Option<u64>,
    last_result_count: Option<usize>,
    last_error: Option<String>,
    thumb_urls: Vec<(String, String)>,
    thumbs: Vec<(String, u64, Pending)>,
    thumb_marks: u64,
}

struct Inner {
    storage: String,
    staging_root: String,
    staging_label: String,
    backend: Option<Arc<dyn Backend>>,
    fetch: Option<Arc<dyn Fetch>>,
    search_base: String,
    site_origin: String,
    search_timeout_ms: f64,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    random: Arc<dyn RandomSource>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<State>,
}

struct Plan {
    audio: bool,
    artist: String,
    entry_id: String,
    title: String,
    folder_name: String,
    items: Vec<Item>,
}

#[derive(Clone)]
pub struct Provider(Arc<Inner>);

impl Provider {
    pub fn new(options: Options) -> Self {
        let staging_root = options.staging_root.unwrap_or_else(|| {
            Path::new(&options.storage)
                .join("staging")
                .to_string_lossy()
                .into_owned()
        });
        let provider = Self(Arc::new(Inner {
            storage: options.storage,
            staging_root,
            staging_label: options.staging_label,
            backend: options.backend,
            fetch: options.fetch,
            search_base: options.search_base,
            site_origin: options.site_origin,
            search_timeout_ms: options.search_timeout_ms,
            onchange: options.onchange,
            random: options.random,
            now_ms: options.now_ms,
            state: Mutex::new(State {
                jobs: Vec::new(),
                closed: false,
                chain: futures_util::future::ready(()).boxed().shared(),
                last_search_at: None,
                last_result_count: None,
                last_error: None,
                thumb_urls: Vec::new(),
                thumbs: Vec::new(),
                thumb_marks: 0,
            }),
        }));
        if provider.0.backend.is_some() {
            let _ = std::fs::remove_dir_all(provider.work_root());
        }
        provider
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().expect("rumble state")
    }

    fn now(&self) -> u64 {
        (self.0.now_ms)()
    }

    fn changed(&self) {
        if let Some(onchange) = &self.0.onchange {
            onchange();
        }
    }

    fn work_root(&self) -> PathBuf {
        Path::new(&self.0.storage).join("rumble-work")
    }

    fn err(status: u16, message: impl Into<String>) -> YoutubeError {
        YoutubeError {
            message: message.into(),
            status: Some(status),
        }
    }

    fn backend(&self) -> Result<Arc<dyn Backend>, YoutubeError> {
        self.0
            .backend
            .clone()
            .ok_or_else(|| Self::err(503, "Rumble is not available on this device"))
    }

    fn fetch(&self) -> Result<Arc<dyn Fetch>, YoutubeError> {
        self.0
            .fetch
            .clone()
            .ok_or_else(|| Self::err(503, "Rumble is not available on this device"))
    }

    fn resolve_item_url(&self, item: &Value) -> Option<String> {
        if let Some(url) = item.get("url").and_then(Value::as_str).map(str::trim)
            && is_http_url(url)
        {
            return Some(url.to_owned());
        }
        let rid = item
            .get("rumbleId")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        if is_rumble_id(rid) {
            return Some(format!(
                "{}/{}.html",
                self.0.site_origin.trim_end_matches('/'),
                rid.to_ascii_lowercase()
            ));
        }
        None
    }

    pub async fn search(&self, payload: &Value) -> Result<Value, YoutubeError> {
        let fetch = self.fetch()?;
        let query = truncate_utf16(js_trim(&js_string_or_empty(payload.get("query"))), 200);
        if query.is_empty() {
            return Err(Self::err(400, "query is required"));
        }
        let most = RUMBLE_SEARCH_CAP as f64;
        let wanted = match payload.get("limit") {
            None => most,
            given => Some(js_num(given).trunc())
                .filter(|n| *n != 0.0 && !n.is_nan())
                .unwrap_or(most),
        };
        let cap = wanted.clamp(1.0, most) as usize;
        let timeout = match payload.get("timeoutMs") {
            None => self.0.search_timeout_ms,
            given => Some(js_num(given))
                .filter(|n| *n != 0.0 && !n.is_nan())
                .unwrap_or(self.0.search_timeout_ms),
        }
        .clamp(1000.0, 45_000.0) as u64;

        let sep = if self.0.search_base.contains('?') {
            '&'
        } else {
            '?'
        };
        let url = format!(
            "{}{sep}q={}",
            self.0.search_base,
            encode_uri_component(&query)
        );
        let reply = match fetch
            .fetch_opts(&url, timeout, 4 * 1024 * 1024, Some("text/html"))
            .await
        {
            Ok(reply) if (200..300).contains(&reply.status) => reply,
            Ok(reply) => {
                let msg = format!("Rumble answered {}", reply.status);
                self.state().last_error = Some(msg.clone());
                self.changed();
                return Err(Self::err(
                    if reply.status == 429 { 429 } else { 502 },
                    format!("Rumble search failed: {msg}"),
                ));
            }
            Err(error) => {
                let said = truncate_utf16(&error, 200);
                self.state().last_error = Some(said.clone());
                self.changed();
                return Err(Self::err(502, format!("Rumble search failed: {said}")));
            }
        };
        let html = String::from_utf8_lossy(&reply.body);
        let mut thumbs = Vec::new();
        let candidates = parse_search_html(
            &html,
            cap + 1,
            &self.0.site_origin,
            Some(&mut |id: String, url: String| thumbs.push((id, url))),
        );
        {
            let mut state = self.state();
            for (id, thumb_url) in thumbs {
                state.thumb_urls.retain(|(seen, _)| seen != &id);
                state.thumb_urls.push((id, thumb_url));
                while state.thumb_urls.len() > THUMB_URLS_KEPT {
                    state.thumb_urls.remove(0);
                }
            }
            state.last_error = None;
            state.last_search_at = Some(self.now());
            state.last_result_count = Some(candidates.len());
        }
        self.changed();
        let truncated = candidates.len() > cap;
        let shown: Vec<Value> = candidates.into_iter().take(cap).collect();
        Ok(json!({ "candidates": shown, "cap": RUMBLE_SEARCH_CAP, "truncated": truncated }))
    }

    fn trim_jobs(state: &mut State) {
        if state.jobs.len() <= JOBS_MOST {
            return;
        }
        let mut left = state.jobs.len();
        state.jobs.retain(|job| {
            let over = left > JOBS_KEPT && matches!(job.status(), "done" | "failed" | "cancelled");
            if over {
                left -= 1;
            }
            !over
        });
    }

    pub async fn mirror(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, YoutubeError> {
        let backend = self.backend()?;
        let audio = payload.get("audio").is_some_and(js_truthy);
        let named = |value: Option<&Value>, fallback: &str| {
            truncate_utf16(&safe_segment(&js_string_or_empty(value), fallback), 120)
        };
        let year = payload
            .get("year")
            .filter(|v| !v.is_null())
            .map(|v| js_trim(&js_string_or_empty(Some(v))).to_owned())
            .filter(|t| {
                t.len() == 4
                    && t.bytes().all(|b| b.is_ascii_digit())
                    && ["18", "19", "20"].iter().any(|p| t.starts_with(p))
            })
            .and_then(|t| t.parse::<u32>().ok());

        let (raw_items, artist, entry_id, title, folder_name) = if audio {
            let (artist, album, raw_items) =
                if let Some(arr) = payload.get("tracks").and_then(Value::as_array) {
                    let artist = named(payload.get("artist"), "");
                    let album = named(payload.get("album"), "");
                    if artist.is_empty() || album.is_empty() {
                        return Err(Self::err(400, "artist and album are required"));
                    }
                    (artist, album, arr.clone())
                } else {
                    let raw_title = named(payload.get("title"), "");
                    if raw_title.is_empty() {
                        return Err(Self::err(400, "title is required"));
                    }
                    let artist = named(payload.get("artist"), "Rumble");
                    let album = match payload.get("album").filter(|v| js_truthy(v)) {
                        Some(v) => named(Some(v), &raw_title),
                        None => raw_title.clone(),
                    };
                    (
                        artist,
                        album,
                        vec![json!({
                            "title": raw_title,
                            "url": payload.get("url").cloned().unwrap_or(Value::Null),
                            "rumbleId": payload.get("rumbleId").cloned().unwrap_or(Value::Null),
                            "track": 1,
                        })],
                    )
                };
            if raw_items.len() > MAX_TRACKS {
                return Err(Self::err(
                    400,
                    format!("An album takes at most {MAX_TRACKS} tracks"),
                ));
            }
            let dated = year.map(|y| format!(" ({y})")).unwrap_or_default();
            let folder = truncate_utf16(
                &safe_segment(&format!("{artist} - {album}{dated}"), "Album"),
                128,
            );
            (
                raw_items,
                artist.clone(),
                rumble_release(&artist, &album, year),
                truncate_utf16(&format!("{artist} · {album}"), MAX_TITLE),
                format!("{} [Rumble]", js_trim_end(&folder)),
            )
        } else {
            let raw_items = payload
                .get("tracks")
                .and_then(Value::as_array)
                .filter(|a| !a.is_empty())
                .cloned()
                .unwrap_or_else(|| {
                    vec![json!({
                        "title": payload.get("title").cloned().unwrap_or(Value::Null),
                        "url": payload.get("url").cloned().unwrap_or(Value::Null),
                        "rumbleId": payload.get("rumbleId").cloned().unwrap_or(Value::Null),
                    })]
                });
            let primary_title = named(
                payload
                    .get("title")
                    .filter(|v| js_truthy(v))
                    .or_else(|| raw_items.first().and_then(|v| v.get("title"))),
                "",
            );
            if primary_title.is_empty() {
                return Err(Self::err(400, "title is required"));
            }
            let rid = payload
                .get("rumbleId")
                .or_else(|| raw_items.first().and_then(|v| v.get("rumbleId")))
                .and_then(Value::as_str)
                .map(|s| s.trim().to_ascii_lowercase())
                .unwrap_or_default();
            let entry_id = if is_rumble_id(&rid) {
                format!("rmb:{rid}")
            } else {
                let seed = raw_items
                    .first()
                    .and_then(|i| self.resolve_item_url(i))
                    .unwrap_or_else(|| primary_title.clone());
                let digest = Sha256::digest(seed.as_bytes());
                let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
                format!("clip:{hex}")
            };
            let folder = truncate_utf16(&safe_segment(&primary_title, "Video"), 128);
            (
                raw_items,
                String::new(),
                entry_id,
                truncate_utf16(&primary_title, MAX_TITLE),
                format!("{} [Rumble]", js_trim_end(&folder)),
            )
        };

        let mut items = Vec::new();
        let mut seen_nums = HashSet::new();
        for (i, entry) in raw_items.iter().enumerate() {
            let Some(url) = self.resolve_item_url(entry) else {
                return Err(Self::err(
                    400,
                    format!("Item {} has no valid URL or rumbleId", i + 1),
                ));
            };
            let item_title = named(
                entry
                    .get("title")
                    .filter(|v| js_truthy(v))
                    .or(Some(&Value::String(title.clone()))),
                "",
            );
            if item_title.is_empty() {
                return Err(Self::err(400, format!("Item {} has no title", i + 1)));
            }
            let given = js_num(entry.get("track")).trunc();
            let track = if (1.0..=999.0).contains(&given) {
                given as u32
            } else {
                i as u32 + 1
            };
            if audio && !seen_nums.insert(track) {
                return Err(Self::err(400, format!("Two tracks are numbered {track}")));
            }
            let rid = entry
                .get("rumbleId")
                .and_then(Value::as_str)
                .map(|s| s.trim().to_ascii_lowercase())
                .filter(|s| is_rumble_id(s))
                .unwrap_or_default();
            items.push(Item {
                url,
                title: item_title,
                track,
                rumble_id: rid,
            });
        }
        if items.is_empty() {
            return Err(Self::err(400, "At least one video is required"));
        }

        let default_ext = if audio { ".m4a" } else { ".mp4" };
        let plan = Plan {
            audio,
            artist,
            entry_id,
            title,
            folder_name,
            items,
        };
        let now = self.now();
        let job = Arc::new(Job {
            id: self.0.random.uuid_v4(),
            controller: Controller::new(),
            record: Mutex::new(JobRecord {
                entry_id: plan.entry_id.clone(),
                title: plan.title.clone(),
                audio: plan.audio,
                status: "queued",
                phase: "queued",
                created: now,
                file: file_of(&plan.items[0], plan.audio, default_ext),
                file_index: 1,
                file_count: plan.items.len(),
                speed: 0,
                bytes: 0,
                total: None,
                error: None,
                skipped: Vec::new(),
                note: None,
                progress_at: now,
                done: None,
            }),
        });
        let (turn_over, over) = tokio::sync::oneshot::channel::<()>();
        let before = {
            let mut state = self.state();
            Self::trim_jobs(&mut state);
            state.jobs.push(job.clone());
            let this = async move {
                let _ = over.await;
            };
            std::mem::replace(&mut state.chain, this.boxed().shared())
        };
        self.changed();
        let save = tokio::spawn({
            let (provider, job) = (self.clone(), job.clone());
            async move {
                let _turn_over = turn_over;
                before.await;
                provider.save(&backend, &job, &plan, place_and_scan).await
            }
        });
        if payload.get("wait") == Some(&Value::Bool(true)) {
            return save.await.unwrap_or_else(|e| {
                Err(YoutubeError {
                    message: e.to_string(),
                    status: None,
                })
            });
        }
        Ok(json!({ "queued": true, "job": job.view(self.now()) }))
    }

    fn stopped(&self, job: &Job) -> bool {
        job.controller.is_aborted() || self.state().closed
    }

    async fn save(
        &self,
        backend: &Arc<dyn Backend>,
        job: &Arc<Job>,
        plan: &Plan,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, YoutubeError> {
        let mut work_dir: Option<PathBuf> = None;
        let outcome = self
            .download_and_place(backend, job, plan, place_and_scan, &mut work_dir)
            .await;
        let result = outcome.map_err(|failure| {
            let message = {
                let stopped = self.stopped(job);
                let mut record = job.record();
                if stopped {
                    record.status = "cancelled";
                    record.phase = "cancelled";
                } else {
                    record.status = "failed";
                    record.phase = "failed";
                    record.error = Some(truncate_utf16(&failure.message, 300));
                }
                record.speed = 0;
                record.error.clone()
            };
            self.changed();
            Self::err(
                failure.status.unwrap_or(502),
                message.unwrap_or_else(|| "Cancelled".to_owned()),
            )
        });
        if let Some(dir) = work_dir {
            let _ = std::fs::remove_dir_all(dir);
        }
        result
    }

    async fn download_and_place(
        &self,
        backend: &Arc<dyn Backend>,
        job: &Arc<Job>,
        plan: &Plan,
        place_and_scan: Option<PlaceAndScan>,
        work_dir: &mut Option<PathBuf>,
    ) -> Result<Value, YoutubeError> {
        let io = |e: std::io::Error| YoutubeError {
            message: e.to_string(),
            status: None,
        };
        if self.stopped(job) {
            return Err(YoutubeError {
                message: "Cancelled".to_owned(),
                status: None,
            });
        }
        let work_root = self.work_root();
        std::fs::create_dir_all(&work_root).map_err(io)?;
        let work = make_work_dir(&work_root, &*self.0.random).map_err(io)?;
        *work_dir = Some(work.clone());

        let default_ext = if plan.audio { ".m4a" } else { ".mp4" };
        let mut got: Vec<(Item, PathBuf, String)> = Vec::new();
        let mut done_bytes = 0u64;
        for (i, item) in plan.items.iter().enumerate() {
            if self.stopped(job) {
                return Err(YoutubeError {
                    message: "Cancelled".to_owned(),
                    status: None,
                });
            }
            let started = self.now();
            {
                let mut record = job.record();
                record.status = "transferring";
                record.phase = "downloading";
                record.file = file_of(item, plan.audio, default_ext);
                record.file_index = i + 1;
                record.speed = 0;
                record.progress_at = started;
            }
            self.changed();
            let out_dir = work.join((i + 1).to_string());
            std::fs::create_dir(&out_dir).map_err(io)?;
            let on_progress: ProgressFn = Box::new({
                let (provider, job) = (self.clone(), job.clone());
                let (mut rate_at, mut rate_bytes) = (started, 0u64);
                move |bytes: u64| {
                    let now = provider.now();
                    let measured = {
                        let mut record = job.record();
                        if done_bytes + bytes > record.bytes {
                            record.progress_at = now;
                        }
                        record.bytes = record.bytes.max(done_bytes + bytes);
                        let elapsed = now.saturating_sub(rate_at);
                        if elapsed >= 1000 {
                            let rate =
                                (bytes as f64 - rate_bytes as f64) / (elapsed as f64 / 1000.0);
                            let smoothed = if record.speed > 0 {
                                record.speed as f64 * 0.5 + rate * 0.5
                            } else {
                                rate
                            };
                            record.speed = js_round(smoothed).max(0.0) as u64;
                            rate_at = now;
                            rate_bytes = bytes;
                        }
                        elapsed >= 1000
                    };
                    if measured {
                        provider.changed();
                    }
                }
            });
            let sent = backend
                .download_media(
                    &item.url,
                    &out_dir,
                    plan.audio,
                    job.controller.clone(),
                    on_progress,
                )
                .await
                .map_err(|error| error.message)
                .and_then(|path| {
                    let name = path
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let ext = split_ext(&name).1.to_lowercase();
                    if !is_media_file(&format!("x{ext}")) {
                        let said = if ext.is_empty() { "no extension" } else { &ext };
                        return Err(format!(
                            "Rumble sent a file this relay does not import ({said})"
                        ));
                    }
                    let size = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
                    if size == 0 {
                        return Err("Rumble sent an empty file".to_owned());
                    }
                    Ok((path, ext, size))
                });
            match sent {
                Ok((path, ext, size)) => {
                    got.push((item.clone(), path, ext));
                    done_bytes += size;
                    job.record().bytes = done_bytes;
                }
                Err(message) => {
                    if self.stopped(job) {
                        return Err(YoutubeError {
                            message: "Cancelled".to_owned(),
                            status: None,
                        });
                    }
                    let reason = truncate_utf16(&message, 200);
                    job.record().skipped.push(json!({
                        "track": item.track,
                        "title": item.title,
                        "error": reason,
                    }));
                }
            }
        }
        let count = plan.items.len();
        if got.is_empty() {
            let first = job.record().skipped.first().cloned().unwrap_or_default();
            let (title, error) = (
                first["title"].as_str().unwrap_or_default(),
                first["error"].as_str().unwrap_or_default(),
            );
            return Err(if count == 1 {
                Self::err(502, format!("Rumble did not send “{title}”: {error}"))
            } else {
                Self::err(
                    502,
                    format!("Rumble sent none of the {count} items. First reason: {error}"),
                )
            });
        }
        {
            let mut record = job.record();
            if !record.skipped.is_empty() {
                record.note = Some(skipped_note(&record.skipped, count));
            }
        }
        let folder_name = &plan.folder_name;
        let final_dir = Path::new(&self.0.staging_root).join(folder_name);
        let placed: Arc<Mutex<Vec<String>>> = Arc::default();
        let place: Place = Box::new({
            let (provider, job, placed) = (self.clone(), job.clone(), placed.clone());
            let (audio, entry_id, title, artist) = (
                plan.audio,
                plan.entry_id.clone(),
                plan.title.clone(),
                plan.artist.clone(),
            );
            let (folder_name, got) = (folder_name.clone(), got.clone());
            move || {
                let io = |e: std::io::Error| PlaceError {
                    message: e.to_string(),
                    status: None,
                };
                if audio {
                    let pin_path = final_dir.join(ALBUM_PIN);
                    let pinned = std::fs::read_to_string(&pin_path)
                        .ok()
                        .and_then(|t| {
                            serde_json::from_str::<Value>(&crate::ipc::well_formed(&t)).ok()
                        })
                        .and_then(|pin| pin.get("release").cloned())
                        .filter(js_truthy);
                    let taken = match pinned {
                        Some(pinned) => pinned.as_str() != Some(entry_id.as_str()),
                        None => std::fs::read_dir(&final_dir)
                            .map(|entries| {
                                entries.flatten().any(|entry| {
                                    let name = entry.file_name();
                                    let name = name.to_string_lossy();
                                    !name.starts_with('.') && !name.starts_with('@')
                                })
                            })
                            .unwrap_or(false),
                    };
                    if taken {
                        return Err(PlaceError {
                            message: format!(
                                "The folder “{folder_name}” already holds other files"
                            ),
                            status: Some(409),
                        });
                    }
                    std::fs::create_dir_all(&final_dir).map_err(io)?;
                    let pin = json!({ "release": entry_id, "title": title, "artist": artist });
                    std::fs::write(&pin_path, format!("{pin}\n")).map_err(io)?;
                } else {
                    std::fs::create_dir_all(&final_dir).map_err(io)?;
                }
                let mut names = placed.lock().expect("placed names");
                for (item, path, ext) in &got {
                    let dest = free_path(&final_dir, &file_of(item, audio, ext));
                    if std::fs::rename(path, &dest).is_err() {
                        std::fs::copy(path, &dest).map_err(io)?;
                    }
                    if !audio {
                        let sidecar_id = if !item.rumble_id.is_empty() {
                            format!("rmb:{}", item.rumble_id)
                        } else {
                            entry_id.clone()
                        };
                        let sidecar = PathBuf::from(format!("{}.json", dest.display()));
                        let meta = json!({ "id": sidecar_id, "title": item.title });
                        std::fs::write(&sidecar, format!("{meta}\n")).map_err(io)?;
                    }
                    let name = dest
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    if !names.contains(&name) {
                        names.push(name);
                    }
                }
                note_mirrored(&final_dir, &names).map_err(io)?;
                drop(names);
                job.record().phase = "importing";
                provider.changed();
                Ok(())
            }
        });
        {
            let mut record = job.record();
            record.status = "announcing";
            record.phase = "waiting";
            record.speed = 0;
            record.progress_at = self.now();
        }
        self.changed();
        let scan = match &place_and_scan {
            Some(host) => {
                let stopped: Cancelled = Arc::new({
                    let (provider, job) = (self.clone(), job.clone());
                    move || provider.stopped(&job)
                });
                host(place, stopped).await
            }
            None => place().map(|()| json!({ "scanned": 0, "ingested": 0, "items": [] })),
        }
        .map_err(|e| YoutubeError {
            message: e.message,
            status: e.status,
        })?;
        let placed = std::mem::take(&mut *placed.lock().expect("placed names"));
        let own = |row: &&Value| placed_here(row, &placed, folder_name);
        let rows = |key: &str| -> &[Value] {
            scan.get(key)
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default()
        };
        let failed = rows("failed");
        let refused_import: Vec<&Value> = failed.iter().filter(own).collect();
        for row in &refused_import {
            let file = row["file"].as_str().unwrap_or_default();
            let item = got
                .iter()
                .map(|(it, _, ext)| (it, file_of(it, plan.audio, ext)))
                .find(|(_, name)| name == file)
                .map(|(it, _)| it);
            let error = truncate_utf16(&js_string_or_empty(row.get("error")), 160);
            job.record().skipped.push(json!({
                "track": item.map(|it| it.track),
                "title": item.map_or(file, |it| it.title.as_str()),
                "error": format!("Not imported: {error}"),
            }));
        }
        let imported = placed.len() - refused_import.len();
        if imported == 0 {
            let error = refused_import
                .first()
                .map(|r| js_string_or_empty(r.get("error")))
                .unwrap_or_default();
            return Err(Self::err(
                500,
                format!("The relay could not import the saved media: {error}"),
            ));
        }
        let items = rows("items");
        let sha256s: Vec<Value> = items
            .iter()
            .filter(own)
            .map(|item| item.get("sha256").cloned().unwrap_or(Value::Null))
            .collect();
        let skipped = {
            let mut record = job.record();
            if !record.skipped.is_empty() {
                record.note = Some(skipped_note(&record.skipped, count));
            }
            record.status = "done";
            record.phase = "done";
            record.bytes = done_bytes;
            record.total = Some(done_bytes);
            record.done = Some((done_bytes, sha256s));
            record.skipped.clone()
        };
        self.changed();
        let ingested = scan.get("ingested").filter(|c| js_truthy(c));
        Ok(json!({
            "jobId": job.id,
            "id": plan.entry_id,
            "folder": folder_name,
            "files": imported,
            "bytes": done_bytes,
            "ingested": ingested.cloned().unwrap_or(json!(0)),
            "items": items,
            "skipped": skipped,
        }))
    }

    pub fn list_jobs(&self) -> Vec<Value> {
        let now = self.now();
        self.state()
            .jobs
            .iter()
            .map(|job| Value::Object(job.view(now)))
            .collect()
    }

    pub fn get_job(&self, job_id: &str) -> Option<Value> {
        let now = self.now();
        let job = self.state().jobs.iter().find(|j| j.id == job_id).cloned()?;
        Some(Value::Object(job.view(now)))
    }

    pub fn cancel_job(&self, job_id: &str) -> Option<Value> {
        let now = self.now();
        let job = self.state().jobs.iter().find(|j| j.id == job_id).cloned()?;
        let cancellable = {
            let record = job.record();
            matches!(record.status, "queued" | "transferring")
                || (record.status == "announcing" && record.phase == "waiting")
        };
        if cancellable {
            job.controller.abort();
            {
                let mut record = job.record();
                record.status = "cancelled";
                record.phase = "cancelled";
            }
            self.changed();
            return Some(Value::Object(job.view(now)));
        }
        if job.status() != "announcing" {
            self.state().jobs.retain(|other| other.id != job_id);
            self.changed();
            let mut view = job.view(now);
            view.insert("status".into(), json!("removed"));
            return Some(Value::Object(view));
        }
        Some(Value::Object(job.view(now)))
    }

    pub fn info(&self) -> Value {
        let can_search = self.0.fetch.is_some();
        let can_save = self.0.backend.is_some();
        let available = can_search && can_save;
        let state = self.state();
        let active_jobs = state
            .jobs
            .iter()
            .filter(|j| matches!(j.status(), "queued" | "transferring" | "announcing"))
            .count();
        let message = if !available {
            "Rumble search needs ubit."
        } else {
            state.last_error.as_deref().unwrap_or("")
        };
        json!({
            "status": if available { "ready" } else { "unavailable" },
            "message": message,
            "canSave": can_save,
            "configured": available,
            "available": available,
            "activeJobs": active_jobs,
            "searchCap": RUMBLE_SEARCH_CAP,
            "staging": self.0.staging_label,
            "lastSearchAt": state.last_search_at,
            "lastResultCount": state.last_result_count,
        })
    }

    pub async fn thumbnail(&self, rumble_id: &str) -> Option<Picture> {
        let fetch = self.0.fetch.clone()?;
        let id = rumble_id.to_ascii_lowercase();
        if !is_rumble_id(&id) {
            return None;
        }
        let thumb_url = self
            .state()
            .thumb_urls
            .iter()
            .find(|(seen, _)| seen == &id)
            .map(|(_, url)| url.clone())?;
        let pending = {
            let mut state = self.state();
            if let Some((_, _, existing)) = state.thumbs.iter().find(|(seen, _, _)| seen == &id) {
                existing.clone()
            } else {
                state.thumb_marks += 1;
                let mark = state.thumb_marks;
                let provider = self.clone();
                let id_owned = id.clone();
                let request: Pending = async move {
                    let reply = match fetch.fetch(&thumb_url).await {
                        Ok(reply) => reply,
                        Err(_) => {
                            provider.evict_thumb(&id_owned, mark);
                            return None;
                        }
                    };
                    if reply.status != 200 || reply.body.is_empty() {
                        return None;
                    }
                    let ct = reply.content_type.as_deref().unwrap_or("");
                    let media = ct.split(';').next().unwrap_or("").trim();
                    let ok = ["image/jpeg", "image/webp", "image/png", "image/gif"]
                        .iter()
                        .any(|t| media.eq_ignore_ascii_case(t));
                    ok.then(|| Picture {
                        content_type: media.to_owned(),
                        bytes: Bytes::from(reply.body),
                    })
                }
                .boxed()
                .shared();
                state.thumbs.push((id, mark, request.clone()));
                while state.thumbs.len() > THUMBS_KEPT {
                    let _ = state.thumbs.remove(0);
                }
                request
            }
        };
        pending.await
    }

    fn evict_thumb(&self, id: &str, mark: u64) {
        self.state()
            .thumbs
            .retain(|(seen, m, _)| !(seen == id && *m == mark));
    }

    pub async fn run(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, YoutubeError> {
        if self.0.backend.is_none() || self.0.fetch.is_none() {
            return Ok(json!({ "skipped": true, "status": "unavailable" }));
        }
        if payload.get("tracks").and_then(Value::as_array).is_some()
            || payload.get("url").and_then(Value::as_str).is_some()
            || payload.get("rumbleId").and_then(Value::as_str).is_some()
        {
            return self.mirror(payload, place_and_scan).await;
        }
        if payload.get("query").and_then(Value::as_str).is_some() {
            return self.search(payload).await;
        }
        let mut out = Map::new();
        out.insert("ok".into(), json!(true));
        if let Value::Object(info) = self.info() {
            out.extend(info);
        }
        Ok(Value::Object(out))
    }

    pub async fn stop(&self) {
        let (jobs, chain) = {
            let mut state = self.state();
            state.closed = true;
            (state.jobs.clone(), state.chain.clone())
        };
        for job in jobs {
            let mut record = job.record();
            let waiting = record.status == "announcing" && record.phase == "waiting";
            if matches!(record.status, "queued" | "transferring") || waiting {
                job.controller.abort();
                if waiting {
                    record.status = "cancelled";
                    record.phase = "cancelled";
                }
            }
        }
        chain.await;
    }
}

fn make_work_dir(root: &Path, random: &dyn RandomSource) -> std::io::Result<PathBuf> {
    let bytes = random.bytes(6);
    let mut hex = String::with_capacity(12);
    for b in bytes {
        hex.push_str(&format!("{b:02x}"));
    }
    let dir = root.join(format!("job-{hex}"));
    std::fs::create_dir(&dir)?;
    Ok(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rumble_cards_and_durations() {
        let html = r#"
        <li class="video-listing-entry">
          <a class="video-item--a" href="/v5abc12-great-talk.html">
            <img src="https://sp.rmbl.ws/s8/1/a.jpg" alt="Great Talk" />
          </a>
          <span class="video-item--duration" data-value="12:34"></span>
          <h3 class="video-item--title">Great &amp; Clear Talk</h3>
          <div class="video-item--by-a">Channel One</div>
        </li>"#;
        let mut thumb = None;
        let rows = parse_search_html(
            html,
            20,
            "https://rumble.com",
            Some(&mut |id: String, url: String| thumb = Some((id, url))),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["rumbleId"], "v5abc12");
        assert_eq!(rows[0]["title"], "Great & Clear Talk");
        assert_eq!(rows[0]["channel"], "Channel One");
        assert_eq!(rows[0]["length"], 754);
        assert_eq!(rows[0]["url"], "https://rumble.com/v5abc12-great-talk.html");
        assert_eq!(
            thumb,
            Some((
                "v5abc12".to_owned(),
                "https://sp.rmbl.ws/s8/1/a.jpg".to_owned()
            ))
        );
    }
}
