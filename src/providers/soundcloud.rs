//! SoundCloud provider for PearTube peers and relays, ported from `src/providers/soundcloud.js`:
//!
//!   - Scrapes a public SoundCloud `client_id` from `https://soundcloud.com` and its
//!     `sndcdn.com/assets/*.js` bundles through the guarded fetch, caches it in memory (re-scraping
//!     once on 401/403), and searches `https://api-v2.soundcloud.com/search/tracks`.
//!   - Saves chosen tracks' audio via `ubit` (`download_media(..., audio: true)`) into
//!     `<staging>/<Artist> - <Album> (<Year>) [SoundCloud]/NN - Title.<ext>`, pinned to an `sc:`
//!     release, then triggers a staging scan.
//!   - Serves result thumbnails through the local API so the browser never contacts SoundCloud's
//!     image host.

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

pub const NAME: &str = "soundcloud";
pub const DESCRIPTION: &str =
    "Finds music on SoundCloud and saves its audio as albums in the staging folder";
pub const SOUNDCLOUD_SEARCH_CAP: usize = 20;
pub const SOUNDCLOUD_BITRATE: u32 = 128;
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
    pub site_base: String,
    pub api_base: String,
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
            site_base: "https://soundcloud.com".to_owned(),
            api_base: "https://api-v2.soundcloud.com".to_owned(),
            search_timeout_ms: 20_000.0,
            onchange: None,
            random,
            now_ms,
        }
    }
}

fn is_track_id(text: &str) -> bool {
    (1..=64).contains(&text.len())
        && text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn is_http_url(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn js_round(number: f64) -> f64 {
    (number + 0.5).floor()
}

/// The `sc:` release an album saved from SoundCloud is pinned to.
pub fn soundcloud_release(artist: &str, album: &str, year: Option<u32>) -> String {
    let digest = Sha256::digest(album_key(Some(artist), Some(album), year).as_bytes());
    let mut hex = String::with_capacity(20);
    for byte in &digest[..10] {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("sc:{hex}")
}

fn is_client_id_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b'-'
}

/// Extract `client_id:"..."`, `client_id='...'`, or `client_id=...` from HTML/JS text.
pub fn extract_client_id(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let needle = b"client_id";
    let mut pos = 0;
    while pos + needle.len() < bytes.len() {
        let Some(rel) = text.get(pos..)?.find("client_id") else {
            break;
        };
        let mut idx = pos + rel + needle.len();
        pos = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() || (bytes[idx] != b':' && bytes[idx] != b'=') {
            continue;
        }
        idx += 1;
        while idx < bytes.len() && bytes[idx].is_ascii_whitespace() {
            idx += 1;
        }
        if idx >= bytes.len() {
            break;
        }
        let quote = (bytes[idx] == b'"' || bytes[idx] == b'\'').then_some(bytes[idx]);
        if quote.is_some() {
            idx += 1;
        }
        let start = idx;
        while idx < bytes.len() && is_client_id_char(bytes[idx]) {
            idx += 1;
        }
        let len = idx - start;
        if !(16..=64).contains(&len) {
            continue;
        }
        if let Some(q) = quote
            && bytes.get(idx) != Some(&q)
        {
            continue;
        }
        if let Some(id) = text.get(start..idx) {
            return Some(id.to_owned());
        }
    }
    None
}

/// Extract SoundCloud JS asset script URLs from HTML.
fn extract_script_urls(html: &str, site_base: &str) -> Vec<String> {
    let Ok(base) = Url::parse(site_base) else {
        return Vec::new();
    };
    let lower = html.to_ascii_lowercase();
    let mut urls = Vec::new();
    let mut seen = HashSet::new();
    let mut cursor = 0;
    while let Some(rel) = lower.get(cursor..).and_then(|s| s.find("<script")) {
        let tag_start = cursor + rel;
        let Some(tag_end_rel) = lower.get(tag_start..).and_then(|s| s.find('>')) else {
            break;
        };
        let tag_end = tag_start + tag_end_rel;
        cursor = tag_end + 1;
        let (Some(tag), Some(tag_low)) =
            (html.get(tag_start..tag_end), lower.get(tag_start..tag_end))
        else {
            continue;
        };
        let Some(src_pos) = tag_low.find("src=") else {
            continue;
        };
        let Some(after) = tag.get(src_pos + 4..) else {
            continue;
        };
        let bytes = after.as_bytes();
        let Some(&quote) = bytes.first().filter(|&&b| b == b'"' || b == b'\'') else {
            continue;
        };
        let Some(end_q) = after.get(1..).and_then(|s| s.find(quote as char)) else {
            continue;
        };
        let Some(raw) = after.get(1..1 + end_q) else {
            continue;
        };
        let raw_path = raw.split('?').next().unwrap_or(raw);
        if !raw_path.to_ascii_lowercase().ends_with(".js") {
            continue;
        }
        let Ok(resolved) = base.join(raw) else {
            continue;
        };
        let href: String = resolved.into();
        let href_low = href.to_ascii_lowercase();
        if !href_low.contains("sndcdn.com/assets/") && !href_low.contains("/assets/") {
            continue;
        }
        if seen.insert(href.clone()) {
            urls.push(href);
        }
    }
    urls
}

fn artwork_url_for(entry: &Value) -> Option<String> {
    let raw = entry
        .get("artwork_url")
        .and_then(Value::as_str)
        .filter(|s| is_http_url(s))
        .or_else(|| {
            entry
                .get("user")
                .and_then(|u| u.get("avatar_url"))
                .and_then(Value::as_str)
                .filter(|s| is_http_url(s))
        })?;
    if raw.to_ascii_lowercase().contains("sndcdn.com") {
        for ext in [".jpg", ".jpeg", ".png", ".webp"] {
            let suffix = format!("-large{ext}");
            if raw.len() >= suffix.len()
                && raw[raw.len() - suffix.len()..].eq_ignore_ascii_case(&suffix)
            {
                let stem = &raw[..raw.len() - suffix.len()];
                let orig_ext = &raw[raw.len() - ext.len()..];
                return Some(format!("{stem}-t300x300{orig_ext}"));
            }
        }
    }
    Some(raw.to_owned())
}

pub fn normalize_results(
    data: &Value,
    limit: usize,
    mut on_thumb: Option<&mut dyn FnMut(String, String)>,
) -> Vec<Value> {
    let list = data
        .get("collection")
        .and_then(Value::as_array)
        .or_else(|| data.get("tracks").and_then(Value::as_array))
        .or_else(|| data.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut rows = Vec::new();
    let mut seen: Vec<String> = Vec::new();
    for entry in list {
        let raw_id = entry
            .get("id")
            .or_else(|| entry.get("trackId"))
            .map(|v| match v {
                Value::String(s) => s.trim().to_owned(),
                Value::Number(n) => n.to_string(),
                _ => String::new(),
            })
            .unwrap_or_default();
        if !is_track_id(&raw_id) || seen.contains(&raw_id) {
            continue;
        }
        let url = entry
            .get("permalink_url")
            .and_then(Value::as_str)
            .filter(|s| is_http_url(s))
            .or_else(|| {
                entry
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|s| is_http_url(s))
            });
        let Some(url) = url else {
            continue;
        };
        seen.push(raw_id.clone());
        let artist = [
            entry.get("user").and_then(|u| u.get("username")),
            entry.get("user").and_then(|u| u.get("full_name")),
            entry.get("artist"),
            entry
                .get("publisher_metadata")
                .and_then(|m| m.get("artist")),
        ]
        .into_iter()
        .flatten()
        .find(|v| js_truthy(v))
        .map(|v| truncate_utf16(js_trim(&js_string_or_empty(Some(v))), 120))
        .filter(|s| !s.is_empty());
        let raw_sec = js_num(entry.get("length"));
        let raw_ms = js_num(entry.get("duration").or_else(|| entry.get("full_duration")));
        let length = if raw_sec.is_finite() && raw_sec > 0.0 {
            Some(js_round(raw_sec) as u64)
        } else if raw_ms.is_finite() && raw_ms > 0.0 {
            Some(js_round(raw_ms / 1000.0) as u64)
        } else {
            None
        };
        if let Some(art) = artwork_url_for(entry)
            && let Some(cb) = on_thumb.as_mut()
        {
            cb(raw_id.clone(), art);
        }
        let title = truncate_utf16(js_trim(&js_string_or_empty(entry.get("title"))), 300);
        let mut row = Map::new();
        row.insert(
            "title".into(),
            json!(if title.is_empty() {
                raw_id.as_str()
            } else {
                title.as_str()
            }),
        );
        row.insert("artist".into(), json!(artist));
        row.insert("length".into(), json!(length));
        row.insert("size".into(), Value::Null);
        row.insert("bitrate".into(), json!(SOUNDCLOUD_BITRATE));
        row.insert("sample_rate".into(), Value::Null);
        row.insert("bit_depth".into(), Value::Null);
        row.insert("lossy".into(), json!(true));
        row.insert("trackId".into(), json!(raw_id));
        row.insert("url".into(), json!(url));
        row.insert("channel".into(), json!(artist));
        row.insert(
            "thumbnail".into(),
            json!(format!("/v1/providers/soundcloud/thumb/{raw_id}")),
        );
        rows.push(Value::Object(row));
        if rows.len() >= limit {
            break;
        }
    }
    rows
}

#[derive(Debug, Clone)]
struct Track {
    url: String,
    title: String,
    track: u32,
}

fn file_of(track: &Track, ext: &str) -> String {
    let number = format!("{:02}", track.track);
    format!(
        "{}{ext}",
        safe_segment(&format!("{number} - {}", track.title), &number)
    )
}

struct Job {
    id: String,
    controller: Controller,
    record: Mutex<JobRecord>,
}

struct JobRecord {
    release: String,
    title: String,
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
        self.record.lock().expect("soundcloud job")
    }

    fn status(&self) -> &'static str {
        self.record().status
    }

    fn view(&self, now: u64) -> Map<String, Value> {
        let record = self.record();
        let mut out = Map::new();
        out.insert("jobId".into(), json!(self.id));
        out.insert("id".into(), json!(record.release));
        out.insert("title".into(), json!(record.title));
        out.insert("plugin".into(), json!(NAME));
        out.insert("source".into(), json!(NAME));
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

fn skipped_note(skipped: &[Value], tracks: usize) -> String {
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
            "{} of {tracks} tracks not saved: {}",
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
    client_id: Option<String>,
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
    site_base: String,
    api_base: String,
    search_timeout_ms: f64,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    random: Arc<dyn RandomSource>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<State>,
}

struct Plan {
    artist: String,
    release: String,
    album_title: String,
    folder_name: String,
    tracks: Vec<Track>,
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
            site_base: options.site_base,
            api_base: options.api_base,
            search_timeout_ms: options.search_timeout_ms,
            onchange: options.onchange,
            random: options.random,
            now_ms: options.now_ms,
            state: Mutex::new(State {
                jobs: Vec::new(),
                closed: false,
                chain: futures_util::future::ready(()).boxed().shared(),
                client_id: None,
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
        self.0.state.lock().expect("soundcloud state")
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
        Path::new(&self.0.storage).join("soundcloud-work")
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
            .ok_or_else(|| Self::err(503, "SoundCloud is not available on this device"))
    }

    fn fetch(&self) -> Result<Arc<dyn Fetch>, YoutubeError> {
        self.0
            .fetch
            .clone()
            .ok_or_else(|| Self::err(503, "SoundCloud is not available on this device"))
    }

    async fn scrape_client_id(
        &self,
        fetch: &Arc<dyn Fetch>,
        timeout_ms: u64,
    ) -> Result<String, YoutubeError> {
        let reply = fetch
            .fetch_opts(
                &self.0.site_base,
                timeout_ms,
                4 * 1024 * 1024,
                Some("text/html"),
            )
            .await
            .map_err(|e| Self::err(502, e))?;
        if !(200..300).contains(&reply.status) {
            return Err(Self::err(
                502,
                format!("SoundCloud homepage answered {}", reply.status),
            ));
        }
        let html = String::from_utf8_lossy(&reply.body);
        if let Some(id) = extract_client_id(&html) {
            return Ok(id);
        }
        let scripts = extract_script_urls(&html, &self.0.site_base);
        for src in scripts.iter().rev().take(8) {
            let Ok(res) = fetch
                .fetch_opts(src, timeout_ms, 6 * 1024 * 1024, None)
                .await
            else {
                continue;
            };
            if res.status != 200 {
                continue;
            }
            let body = String::from_utf8_lossy(&res.body);
            if let Some(id) = extract_client_id(&body) {
                return Ok(id);
            }
        }
        Err(Self::err(502, "Could not find a SoundCloud client_id"))
    }

    async fn query_api(
        &self,
        query: &str,
        limit: usize,
        timeout_ms: u64,
    ) -> Result<Value, YoutubeError> {
        let fetch = self.fetch()?;
        let base = self.0.api_base.trim_end_matches('/');
        for attempt in 0..2 {
            let cached = self.state().client_id.clone();
            let client_id = match cached {
                Some(id) => id,
                None => {
                    let scraped = self.scrape_client_id(&fetch, timeout_ms).await?;
                    self.state().client_id = Some(scraped.clone());
                    scraped
                }
            };
            let url = format!(
                "{base}/search/tracks?q={}&client_id={}&limit={limit}",
                encode_uri_component(query),
                encode_uri_component(&client_id),
            );
            let res = fetch
                .fetch_opts(&url, timeout_ms, 2 * 1024 * 1024, Some("application/json"))
                .await
                .map_err(|e| Self::err(502, e))?;
            if (res.status == 401 || res.status == 403) && attempt == 0 {
                self.state().client_id = None;
                continue;
            }
            if !(200..300).contains(&res.status) {
                let status = if res.status == 429 { 429 } else { 502 };
                return Err(Self::err(
                    status,
                    format!("SoundCloud API answered {}", res.status),
                ));
            }
            return serde_json::from_slice::<Value>(&res.body)
                .map_err(|_| Self::err(502, "SoundCloud returned invalid JSON"));
        }
        Err(Self::err(502, "SoundCloud refused the client_id"))
    }

    pub async fn search(&self, payload: &Value) -> Result<Value, YoutubeError> {
        let _ = self.fetch()?;
        let query = truncate_utf16(js_trim(&js_string_or_empty(payload.get("query"))), 200);
        if query.is_empty() {
            return Err(Self::err(400, "query is required"));
        }
        let most = SOUNDCLOUD_SEARCH_CAP as f64;
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

        let data = match self.query_api(&query, cap + 1, timeout).await {
            Ok(data) => data,
            Err(error) => {
                let said = truncate_utf16(&error.message, 200);
                self.state().last_error = Some(said.clone());
                self.changed();
                return Err(Self::err(
                    error.status.unwrap_or(502),
                    format!("SoundCloud search failed: {said}"),
                ));
            }
        };
        let mut thumbs = Vec::new();
        let candidates = normalize_results(
            &data,
            cap + 1,
            Some(&mut |id: String, art: String| thumbs.push((id, art))),
        );
        {
            let mut state = self.state();
            for (id, art) in thumbs {
                state.thumb_urls.retain(|(seen, _)| seen != &id);
                state.thumb_urls.push((id, art));
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
        Ok(json!({ "candidates": shown, "cap": SOUNDCLOUD_SEARCH_CAP, "truncated": truncated }))
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
        let named = |value: Option<&Value>, fallback: &str| {
            truncate_utf16(&safe_segment(&js_string_or_empty(value), fallback), 120)
        };
        let is_single = payload.get("tracks").and_then(Value::as_array).is_none()
            && payload.get("url").and_then(Value::as_str).is_some();
        let (artist, album, raw_list): (String, String, Vec<Value>) = if is_single {
            let single_title = named(payload.get("title"), "");
            if single_title.is_empty() {
                return Err(Self::err(400, "title is required"));
            }
            let artist = named(payload.get("artist"), "SoundCloud");
            let album = match payload.get("album").filter(|v| js_truthy(v)) {
                Some(v) => named(Some(v), &single_title),
                None => single_title.clone(),
            };
            (
                artist,
                album,
                vec![json!({
                    "trackId": payload.get("trackId").cloned().unwrap_or(json!("")),
                    "url": payload.get("url").cloned().unwrap_or(json!("")),
                    "title": single_title,
                    "track": 1,
                    "disc": 1,
                })],
            )
        } else {
            let artist = named(payload.get("artist"), "");
            let album = named(payload.get("album"), "");
            if artist.is_empty() || album.is_empty() {
                return Err(Self::err(400, "artist and album are required"));
            }
            let list = payload
                .get("tracks")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            (artist, album, list)
        };
        if raw_list.len() > MAX_TRACKS {
            return Err(Self::err(
                400,
                format!("An album takes at most {MAX_TRACKS} tracks"),
            ));
        }
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
        let mut tracks = Vec::new();
        let mut seen_nums = HashSet::new();
        for (i, entry) in raw_list.iter().enumerate() {
            let url = js_trim(&js_string_or_empty(entry.get("url"))).to_owned();
            if !is_http_url(&url) {
                return Err(Self::err(400, format!("Track {} has no valid URL", i + 1)));
            }
            let title = named(entry.get("title"), "");
            if title.is_empty() {
                return Err(Self::err(400, format!("Track {} has no title", i + 1)));
            }
            let given_track = js_num(entry.get("track")).trunc();
            let track = if (1.0..=999.0).contains(&given_track) {
                given_track as u32
            } else {
                i as u32 + 1
            };
            let given_disc = js_num(entry.get("disc")).trunc();
            let disc = if (1.0..=99.0).contains(&given_disc) {
                given_disc as u32
            } else {
                1
            };
            if !seen_nums.insert((disc, track)) {
                return Err(Self::err(400, format!("Two tracks are numbered {track}")));
            }
            tracks.push(Track { url, title, track });
        }
        if tracks.is_empty() {
            return Err(Self::err(400, "tracks must list at least one track"));
        }
        let dated = year.map(|y| format!(" ({y})")).unwrap_or_default();
        let folder = truncate_utf16(
            &safe_segment(&format!("{artist} - {album}{dated}"), "Album"),
            128,
        );
        let plan = Plan {
            release: soundcloud_release(&artist, &album, year),
            album_title: truncate_utf16(&format!("{artist} · {album}"), MAX_TITLE),
            folder_name: format!("{} [SoundCloud]", js_trim_end(&folder)),
            artist,
            tracks,
        };
        let now = self.now();
        let job = Arc::new(Job {
            id: self.0.random.uuid_v4(),
            controller: Controller::new(),
            record: Mutex::new(JobRecord {
                release: plan.release.clone(),
                title: plan.album_title.clone(),
                status: "queued",
                phase: "queued",
                created: now,
                file: file_of(&plan.tracks[0], ".mp3"),
                file_index: 1,
                file_count: plan.tracks.len(),
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

        let mut got: Vec<(Track, PathBuf, String)> = Vec::new();
        let mut done_bytes = 0u64;
        for (i, track) in plan.tracks.iter().enumerate() {
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
                record.file = file_of(track, ".mp3");
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
                    &track.url,
                    &out_dir,
                    true,
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
                            "SoundCloud sent a file this relay does not import ({said})"
                        ));
                    }
                    let size = std::fs::metadata(&path).map_err(|e| e.to_string())?.len();
                    if size == 0 {
                        return Err("SoundCloud sent an empty file".to_owned());
                    }
                    Ok((path, ext, size))
                });
            match sent {
                Ok((path, ext, size)) => {
                    got.push((track.clone(), path, ext));
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
                        "track": track.track,
                        "title": track.title,
                        "error": reason,
                    }));
                }
            }
        }
        let count = plan.tracks.len();
        if got.is_empty() {
            let first = job.record().skipped.first().cloned().unwrap_or_default();
            let (title, error) = (
                first["title"].as_str().unwrap_or_default(),
                first["error"].as_str().unwrap_or_default(),
            );
            return Err(if count == 1 {
                Self::err(502, format!("SoundCloud did not send “{title}”: {error}"))
            } else {
                Self::err(
                    502,
                    format!("SoundCloud sent none of the {count} tracks. First reason: {error}"),
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
            let (release, title, artist) = (
                plan.release.clone(),
                plan.album_title.clone(),
                plan.artist.clone(),
            );
            let (folder_name, got) = (folder_name.clone(), got.clone());
            move || {
                let io = |e: std::io::Error| PlaceError {
                    message: e.to_string(),
                    status: None,
                };
                let pin_path = final_dir.join(ALBUM_PIN);
                let pinned = std::fs::read_to_string(&pin_path)
                    .ok()
                    .and_then(|t| serde_json::from_str::<Value>(&crate::ipc::well_formed(&t)).ok())
                    .and_then(|pin| pin.get("release").cloned())
                    .filter(js_truthy);
                let taken = match pinned {
                    Some(pinned) => pinned.as_str() != Some(release.as_str()),
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
                        message: format!("The folder “{folder_name}” already holds other files"),
                        status: Some(409),
                    });
                }
                std::fs::create_dir_all(&final_dir).map_err(io)?;
                let pin = json!({ "release": release, "title": title, "artist": artist });
                std::fs::write(&pin_path, format!("{pin}\n")).map_err(io)?;
                let mut names = placed.lock().expect("placed names");
                for (track, path, ext) in &got {
                    let dest = free_path(&final_dir, &file_of(track, ext));
                    if std::fs::rename(path, &dest).is_err() {
                        std::fs::copy(path, &dest).map_err(io)?;
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
            let track = got
                .iter()
                .map(|(t, _, ext)| (t, file_of(t, ext)))
                .find(|(_, name)| name == file)
                .map(|(t, _)| t);
            let error = truncate_utf16(&js_string_or_empty(row.get("error")), 160);
            job.record().skipped.push(json!({
                "track": track.map(|t| t.track),
                "title": track.map_or(file, |t| t.title.as_str()),
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
                format!("The relay could not import the saved tracks: {error}"),
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
            "id": plan.release,
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
            "SoundCloud search needs ubit."
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
            "searchCap": SOUNDCLOUD_SEARCH_CAP,
            "bitrate": SOUNDCLOUD_BITRATE,
            "staging": self.0.staging_label,
            "lastSearchAt": state.last_search_at,
            "lastResultCount": state.last_result_count,
        })
    }

    pub async fn thumbnail(&self, track_id: &str) -> Option<Picture> {
        let fetch = self.0.fetch.clone()?;
        if !is_track_id(track_id) {
            return None;
        }
        let art_url = self
            .state()
            .thumb_urls
            .iter()
            .find(|(id, _)| id == track_id)
            .map(|(_, url)| url.clone())?;
        let pending = {
            let mut state = self.state();
            if let Some((_, _, existing)) = state.thumbs.iter().find(|(id, _, _)| id == track_id) {
                existing.clone()
            } else {
                state.thumb_marks += 1;
                let mark = state.thumb_marks;
                let provider = self.clone();
                let id_owned = track_id.to_owned();
                let request: Pending = async move {
                    let reply = match fetch.fetch(&art_url).await {
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
                state
                    .thumbs
                    .push((track_id.to_owned(), mark, request.clone()));
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
    fn extracts_client_id_from_js_bundle() {
        assert_eq!(
            extract_client_id(r#"foo({client_id:"AbCdEf0123456789xyz"})"#).as_deref(),
            Some("AbCdEf0123456789xyz")
        );
        assert_eq!(
            extract_client_id(
                "https://api-v2.soundcloud.com/x?client_id=1234567890abcdef1234&limit=5"
            )
            .as_deref(),
            Some("1234567890abcdef1234")
        );
    }

    #[test]
    fn normalizes_tracks_and_artwork() {
        let data = json!({
            "collection": [{
                "id": 987654,
                "title": "Midnight Rain",
                "permalink_url": "https://soundcloud.com/nova/midnight-rain",
                "duration": 215000,
                "artwork_url": "https://i1.sndcdn.com/artworks-1-large.jpg",
                "user": { "username": "Nova" }
            }]
        });
        let mut got_thumb = None;
        let rows = normalize_results(
            &data,
            20,
            Some(&mut |id: String, url: String| got_thumb = Some((id, url))),
        );
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["trackId"], "987654");
        assert_eq!(rows[0]["title"], "Midnight Rain");
        assert_eq!(rows[0]["artist"], "Nova");
        assert_eq!(rows[0]["length"], 215);
        assert_eq!(
            rows[0]["thumbnail"],
            "/v1/providers/soundcloud/thumb/987654"
        );
        assert_eq!(
            got_thumb,
            Some((
                "987654".to_owned(),
                "https://i1.sndcdn.com/artworks-1-t300x300.jpg".to_owned()
            ))
        );
    }
}
