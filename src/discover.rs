//! Creator discovery and feed controller (`src/discover.js` parity).
//!
//! Persists followed external creators (YouTube, SoundCloud, Rumble) privately in
//! `<storage>/discover.json` (`0600`), fetches public feeds through SSRF-guarded HTTP,
//! proxies thumbnails via `/v1/discover/thumb/:token`, and bridges on-demand browsing
//! through `MirrorHost::search_sources`.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use hyper::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use url::Url;

use crate::art::{SendOptions, send_bytes};
use crate::http::{DiscoverBridge, HttpError};
use crate::mirror::MirrorHost;
use crate::node::api::SearchOptions;
use crate::node::core::CoreNode;
use crate::providers::youtube::Picture;
use crate::providers::{rumble, soundcloud};

pub const DISCOVER_SOURCES: &[&str] = &["youtube", "soundcloud", "rumble"];
const MAX_CREATORS: usize = 200;
const FEED_TTL_MS: u64 = 10 * 60 * 1000;
const FETCH_TIMEOUT: Duration = Duration::from_secs(12);
const THUMB_TIMEOUT: Duration = Duration::from_secs(8);
const THUMB_MAX_BYTES: usize = 512 * 1024;
const THUMB_URLS_MAX: usize = 512;
const THUMB_CACHE_MAX: usize = 256;
const BROWSER_UA: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SavedCreator {
    id: String,
    source: String,
    #[serde(default)]
    handle: String,
    name: String,
    url: String,
    #[serde(default)]
    channel_id: String,
    #[serde(default)]
    avatar_url: String,
    #[serde(default)]
    followed_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SavedFile {
    version: u32,
    #[serde(default)]
    creators: Vec<SavedCreator>,
}

#[derive(Debug, Clone)]
struct CachedFeed {
    at: u64,
    items: Vec<Value>,
}

#[derive(Default)]
struct State {
    creators: Vec<SavedCreator>,
    thumb_urls: Vec<(String, String)>,
    thumb_cache: Vec<(String, Picture)>,
    feed_cache: HashMap<String, CachedFeed>,
    sc_client_id: Option<String>,
}

impl State {
    fn register_thumb(&mut self, raw_url: &str) -> String {
        let trimmed = raw_url.trim();
        if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
            return String::new();
        }
        let token = sha256_hex(trimmed);
        let token = token.get(..20).unwrap_or(&token).to_owned();
        if let Some(pos) = self.thumb_urls.iter().position(|(k, _)| k == &token) {
            let _ = self.thumb_urls.remove(pos);
        }
        self.thumb_urls.push((token.clone(), trimmed.to_owned()));
        while self.thumb_urls.len() > THUMB_URLS_MAX {
            let _ = self.thumb_urls.remove(0);
        }
        format!("/v1/discover/thumb/{token}")
    }

    fn thumb_url(&self, token: &str) -> Option<String> {
        self.thumb_urls
            .iter()
            .find(|(k, _)| k == token)
            .map(|(_, v)| v.clone())
    }

    fn cached_thumb(&self, token: &str) -> Option<Picture> {
        self.thumb_cache
            .iter()
            .find(|(k, _)| k == token)
            .map(|(_, p)| p.clone())
    }

    fn insert_thumb(&mut self, token: String, pic: Picture) {
        if let Some(pos) = self.thumb_cache.iter().position(|(k, _)| k == &token) {
            let _ = self.thumb_cache.remove(pos);
        }
        self.thumb_cache.push((token, pic));
        while self.thumb_cache.len() > THUMB_CACHE_MAX {
            let _ = self.thumb_cache.remove(0);
        }
    }

    fn hydrate_creator(&mut self, creator: &SavedCreator) -> Value {
        let avatar = if creator.avatar_url.is_empty() {
            String::new()
        } else {
            self.register_thumb(&creator.avatar_url)
        };
        json!({
            "id": creator.id,
            "source": creator.source,
            "handle": creator.handle,
            "name": if creator.name.is_empty() { &creator.id } else { &creator.name },
            "url": creator.url,
            "channelId": creator.channel_id,
            "avatar": avatar,
            "followedAt": creator.followed_at,
        })
    }

    fn list_hydrated(&mut self) -> Vec<Value> {
        let snapshot = self.creators.clone();
        snapshot.iter().map(|c| self.hydrate_creator(c)).collect()
    }
}

pub type GetNode = Arc<dyn Fn() -> Option<Arc<CoreNode>> + Send + Sync>;
pub type GetMirror = Arc<dyn Fn() -> Option<Arc<MirrorHost<CoreNode>>> + Send + Sync>;

pub struct DiscoverHost {
    file: PathBuf,
    allow_private_urls: bool,
    get_node: GetNode,
    get_mirror: GetMirror,
    state: Mutex<State>,
}

impl DiscoverHost {
    pub fn open(
        storage: impl AsRef<Path>,
        allow_private_urls: bool,
        get_node: GetNode,
        get_mirror: GetMirror,
    ) -> Arc<Self> {
        let file = storage.as_ref().join("discover.json");
        let creators = load_saved(&file);
        let mut state = State::default();
        for c in &creators {
            if !c.avatar_url.is_empty() {
                let _ = state.register_thumb(&c.avatar_url);
            }
        }
        state.creators = creators;
        Arc::new(Self {
            file,
            allow_private_urls,
            get_node,
            get_mirror,
            state: Mutex::new(state),
        })
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn persist_creators(&self, creators: &[SavedCreator]) {
        save_creators(&self.file, creators);
    }

    async fn fetch_text(
        &self,
        url: &str,
        max_bytes: u64,
        browser_headers: bool,
    ) -> Result<(u16, String), HttpError> {
        let mut headers = Vec::new();
        if browser_headers {
            headers.push(("user-agent".to_owned(), BROWSER_UA.to_owned()));
            headers.push(("accept-language".to_owned(), "en-US,en;q=0.9".to_owned()));
            headers.push((
                "accept".to_owned(),
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8".to_owned(),
            ));
        }
        let reply = send_bytes(
            url,
            SendOptions {
                method: Method::GET,
                headers,
                timeout: FETCH_TIMEOUT,
                max_bytes: Some(max_bytes),
                redirects: 0,
                allow_private: self.allow_private_urls,
            },
        )
        .await
        .map_err(|e| HttpError::new(e.status().unwrap_or(502), e.to_string()))?;
        let text = String::from_utf8(reply.body)
            .unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned());
        Ok((reply.status, text))
    }

    async fn ensure_sc_client_id(&self, force_refresh: bool) -> Result<String, HttpError> {
        if !force_refresh && let Some(cached) = self.lock_state().sc_client_id.clone() {
            return Ok(cached);
        }
        let (status, html) = self
            .fetch_text("https://soundcloud.com", 4 * 1024 * 1024, false)
            .await?;
        if !(200..300).contains(&status) {
            return Err(HttpError::new(
                502,
                format!("SoundCloud homepage answered {status}"),
            ));
        }
        if let Some(cid) = soundcloud::extract_client_id(&html) {
            self.lock_state().sc_client_id = Some(cid.clone());
            return Ok(cid);
        }
        for script_url in extract_sc_scripts(&html, "https://soundcloud.com")
            .into_iter()
            .rev()
            .take(8)
        {
            let Ok((s_status, body)) = self.fetch_text(&script_url, 6 * 1024 * 1024, false).await
            else {
                continue;
            };
            if s_status != 200 {
                continue;
            }
            if let Some(cid) = soundcloud::extract_client_id(&body) {
                self.lock_state().sc_client_id = Some(cid.clone());
                return Ok(cid);
            }
        }
        Err(HttpError::new(502, "Could not find a SoundCloud client_id"))
    }

    async fn sc_api_get(&self, path_and_query: &str) -> Result<Value, HttpError> {
        for attempt in 0..2 {
            let cid = self.ensure_sc_client_id(attempt > 0).await?;
            let sep = if path_and_query.contains('?') {
                '&'
            } else {
                '?'
            };
            let url = format!(
                "https://api-v2.soundcloud.com{path_and_query}{sep}client_id={}",
                percent_encode(&cid)
            );
            let (status, body) = self.fetch_text(&url, 2 * 1024 * 1024, false).await?;
            if (status == 401 || status == 403) && attempt == 0 {
                self.lock_state().sc_client_id = None;
                continue;
            }
            if !(200..300).contains(&status) {
                return Err(HttpError::new(502, format!("SoundCloud answered {status}")));
            }
            return serde_json::from_str(&body)
                .map_err(|_| HttpError::new(502, "SoundCloud returned invalid JSON"));
        }
        Err(HttpError::new(502, "SoundCloud authorization failed"))
    }

    async fn resolve_youtube(
        &self,
        raw_url: &str,
        initial_channel_id: String,
        initial_handle: String,
        fallback_name: &str,
    ) -> Result<SavedCreator, HttpError> {
        let mut channel_id = initial_channel_id;
        let mut handle = initial_handle;
        let mut name = fallback_name.to_owned();
        let mut avatar_url = String::new();

        if let Ok(parsed) = Url::parse(raw_url) {
            let path = parsed.path();
            if let Some(rest) = path.strip_prefix("/channel/") {
                let candidate = rest.split('/').next().unwrap_or("");
                if is_yt_channel_id(candidate) {
                    channel_id = candidate.to_owned();
                }
            }
            if let Some(rest) = path.strip_prefix("/@") {
                let slug = rest.split('/').next().unwrap_or("");
                if !slug.is_empty() {
                    handle = format!("@{slug}");
                }
            }
        }

        if channel_id.is_empty() {
            let is_watch = raw_url.contains("/watch") || raw_url.contains("youtu.be/");
            let (status, html) = self.fetch_text(raw_url, 4 * 1024 * 1024, true).await?;
            if !(200..300).contains(&status) {
                return Err(HttpError::new(502, format!("YouTube answered {status}")));
            }
            channel_id = extract_youtube_channel_id(&html, is_watch).ok_or_else(|| {
                HttpError::new(400, "Could not find a YouTube channel ID for that link")
            })?;
            if !is_watch {
                if let Some(og_title) = meta_content(&html, "og:title") {
                    name = og_title;
                }
                if let Some(og_img) = meta_content(&html, "og:image") {
                    avatar_url = og_img;
                }
            } else if let Some(author) = extract_video_details_author(&html) {
                name = author;
            }
        }

        if name.is_empty() {
            let feed_url = format!(
                "https://www.youtube.com/feeds/videos.xml?channel_id={}",
                percent_encode(&channel_id)
            );
            if let Ok((status, xml)) = self.fetch_text(&feed_url, 1024 * 1024, false).await
                && (200..300).contains(&status)
            {
                let (feed_title, _) = parse_youtube_feed_xml(&xml, None, None);
                if !feed_title.is_empty() {
                    name = feed_title;
                }
            }
        }

        let display = if !name.is_empty() {
            name
        } else if !handle.is_empty() {
            handle.clone()
        } else {
            channel_id.clone()
        };
        Ok(SavedCreator {
            id: format!("youtube:{channel_id}"),
            source: "youtube".to_owned(),
            handle,
            name: truncate_chars(&display, 120),
            url: format!("https://www.youtube.com/channel/{channel_id}"),
            channel_id,
            avatar_url,
            followed_at: now_ms(),
        })
    }

    async fn resolve_soundcloud(
        &self,
        raw_url: &str,
        fallback_name: &str,
    ) -> Result<SavedCreator, HttpError> {
        let resolved = self
            .sc_api_get(&format!("/resolve?url={}", percent_encode(raw_url)))
            .await?;
        let user = if resolved.get("kind").and_then(Value::as_str) == Some("user") {
            &resolved
        } else {
            resolved.get("user").unwrap_or(&Value::Null)
        };
        let uid = match user.get("id") {
            Some(Value::Number(n)) => n.to_string(),
            Some(Value::String(s)) => s.trim().to_owned(),
            _ => String::new(),
        };
        if uid.is_empty() {
            return Err(HttpError::new(
                400,
                "Could not resolve a SoundCloud artist from that link",
            ));
        }
        let permalink = user
            .get("permalink")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();
        let name = user
            .get("username")
            .or_else(|| user.get("full_name"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or(if fallback_name.is_empty() {
                &uid
            } else {
                fallback_name
            });
        let url = user
            .get("permalink_url")
            .and_then(Value::as_str)
            .filter(|u| u.starts_with("http://") || u.starts_with("https://"))
            .map(str::to_owned)
            .unwrap_or_else(|| {
                let slug = if permalink.is_empty() {
                    &uid
                } else {
                    &permalink
                };
                format!("https://soundcloud.com/{slug}")
            });
        let avatar_url = user
            .get("avatar_url")
            .and_then(Value::as_str)
            .map(sc_artwork)
            .unwrap_or_default();
        Ok(SavedCreator {
            id: format!("soundcloud:{uid}"),
            source: "soundcloud".to_owned(),
            handle: if permalink.is_empty() {
                String::new()
            } else {
                format!("@{permalink}")
            },
            name: truncate_chars(name, 120),
            url,
            channel_id: uid,
            avatar_url,
            followed_at: now_ms(),
        })
    }

    async fn resolve_rumble(
        &self,
        raw_url: &str,
        fallback_name: &str,
    ) -> Result<SavedCreator, HttpError> {
        let parsed = Url::parse(raw_url).map_err(|_| {
            HttpError::new(
                400,
                "Use a Rumble channel link like https://rumble.com/c/<channel>",
            )
        })?;
        let mut segs = parsed.path().split('/').filter(|s| !s.is_empty());
        let kind = segs.next().unwrap_or("").to_ascii_lowercase();
        let slug = segs.next().unwrap_or("").to_owned();
        if !matches!(kind.as_str(), "c" | "user") || slug.is_empty() {
            return Err(HttpError::new(
                400,
                "Use a Rumble channel link like https://rumble.com/c/<channel>",
            ));
        }
        let canonical = format!("https://rumble.com/{kind}/{slug}");
        let mut name = if fallback_name.is_empty() {
            slug.clone()
        } else {
            fallback_name.to_owned()
        };
        let mut avatar_url = String::new();
        if let Ok((status, html)) = self.fetch_text(&canonical, 2 * 1024 * 1024, true).await
            && (200..300).contains(&status)
        {
            if let Some(og) = meta_content(&html, "og:title") {
                let cleaned = og
                    .strip_suffix(" - Rumble")
                    .or_else(|| og.strip_suffix("- Rumble"))
                    .unwrap_or(&og)
                    .trim();
                if !cleaned.is_empty() {
                    name = cleaned.to_owned();
                }
            }
            if let Some(img) = meta_content(&html, "og:image") {
                avatar_url = img;
            }
        }
        Ok(SavedCreator {
            id: format!("rumble:{kind}/{}", slug.to_ascii_lowercase()),
            source: "rumble".to_owned(),
            handle: slug.clone(),
            name: truncate_chars(&name, 120),
            url: canonical,
            channel_id: format!("{kind}/{slug}"),
            avatar_url,
            followed_at: now_ms(),
        })
    }

    async fn fetch_creator_items(&self, creator: &SavedCreator) -> Result<Vec<Value>, String> {
        if creator.source == "youtube" {
            let feed_url = format!(
                "https://www.youtube.com/feeds/videos.xml?channel_id={}",
                percent_encode(&creator.channel_id)
            );
            let (status, xml) = self
                .fetch_text(&feed_url, 1024 * 1024, false)
                .await
                .map_err(|e| e.message().to_owned())?;
            if !(200..300).contains(&status) {
                return Err(format!("YouTube feed answered {status}"));
            }
            let mut thumb_pairs = Vec::new();
            let (_, mut items) =
                parse_youtube_feed_xml(&xml, Some(creator), Some(&mut thumb_pairs));
            let mut state = self.lock_state();
            for (idx, raw_url) in thumb_pairs {
                if let Some(item) = items.get_mut(idx) {
                    item["thumbnail"] = Value::String(state.register_thumb(&raw_url));
                }
            }
            return Ok(items);
        }

        if creator.source == "soundcloud" {
            let data = self
                .sc_api_get(&format!(
                    "/users/{}/tracks?limit=12",
                    percent_encode(&creator.channel_id)
                ))
                .await
                .map_err(|e| e.message().to_owned())?;
            let list = data
                .get("collection")
                .and_then(Value::as_array)
                .or_else(|| data.as_array())
                .cloned()
                .unwrap_or_default();
            let mut state = self.lock_state();
            let mut items = Vec::new();
            for entry in list {
                let track_id = match entry.get("id") {
                    Some(Value::Number(n)) => n.to_string(),
                    Some(Value::String(s)) => s.trim().to_owned(),
                    _ => String::new(),
                };
                let url = entry
                    .get("permalink_url")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim()
                    .to_owned();
                if track_id.is_empty()
                    || !(url.starts_with("http://") || url.starts_with("https://"))
                {
                    continue;
                }
                let title = truncate_chars(
                    entry
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or(&track_id)
                        .trim(),
                    300,
                );
                let artist = truncate_chars(
                    entry
                        .pointer("/user/username")
                        .and_then(Value::as_str)
                        .unwrap_or(&creator.name)
                        .trim(),
                    120,
                );
                let dur_sec = entry
                    .get("duration")
                    .or_else(|| entry.get("full_duration"))
                    .and_then(Value::as_u64)
                    .filter(|&ms| ms > 0)
                    .map(|ms| (ms + 500) / 1000);
                let pub_ms = entry
                    .get("created_at")
                    .and_then(Value::as_str)
                    .and_then(parse_iso_timestamp_ms)
                    .unwrap_or(0);
                let raw_art = entry
                    .get("artwork_url")
                    .or_else(|| entry.pointer("/user/avatar_url"))
                    .and_then(Value::as_str)
                    .map(sc_artwork)
                    .unwrap_or_default();
                let thumb = if raw_art.is_empty() {
                    String::new()
                } else {
                    state.register_thumb(&raw_art)
                };
                let acq = acquire_id_for("soundcloud", &url);
                items.push(json!({
                    "id": format!("soundcloud:{track_id}"),
                    "source": "soundcloud",
                    "kind": "audio",
                    "itemId": track_id,
                    "title": title,
                    "creatorId": creator.id,
                    "creatorName": artist,
                    "creatorUrl": creator.url,
                    "url": url,
                    "thumbnail": thumb,
                    "duration": dur_sec,
                    "publishedAt": pub_ms,
                    "acquireId": acq,
                    "audioOnly": true,
                    "music": { "title": title, "artist": artist },
                    "inLibrary": false,
                }));
            }
            return Ok(items);
        }

        let (status, body) = self
            .fetch_text(&creator.url, 2 * 1024 * 1024, true)
            .await
            .map_err(|e| e.message().to_owned())?;
        if !(200..300).contains(&status) {
            return Err(format!("Rumble answered {status}"));
        }
        let mut thumbs: HashMap<String, String> = HashMap::new();
        let mut on_thumb = |id: String, url: String| {
            thumbs.insert(id, url);
        };
        let rows = rumble::parse_search_html(&body, 15, "https://rumble.com", Some(&mut on_thumb));
        let mut state = self.lock_state();
        let mut items = Vec::new();
        for row in rows {
            let rmb_id = row
                .get("rumbleId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            let url = row
                .get("url")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned();
            if rmb_id.is_empty() || url.is_empty() {
                continue;
            }
            let title = row
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or(&rmb_id)
                .to_owned();
            let creator_name = row
                .get("channel")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(&creator.name)
                .to_owned();
            let thumb = thumbs
                .get(&rmb_id)
                .map(|u| state.register_thumb(u))
                .unwrap_or_default();
            let acq = acquire_id_for("rumble", &url);
            items.push(json!({
                "id": format!("rumble:{rmb_id}"),
                "source": "rumble",
                "kind": "video",
                "itemId": rmb_id,
                "title": title,
                "creatorId": creator.id,
                "creatorName": creator_name,
                "creatorUrl": creator.url,
                "url": url,
                "thumbnail": thumb,
                "duration": row.get("length").cloned().unwrap_or(Value::Null),
                "publishedAt": 0,
                "acquireId": acq,
                "audioOnly": false,
                "music": Value::Null,
                "inLibrary": false,
            }));
        }
        Ok(items)
    }

    async fn library_matcher(&self) -> (HashSet<String>, HashSet<String>) {
        let mut ids = HashSet::new();
        let mut titles = HashSet::new();
        let Some(node) = (self.get_node)() else {
            return (ids, titles);
        };
        let entries = node.catalog().search(None, SearchOptions::default()).await;
        for e in entries {
            if !e.id.is_empty() {
                ids.insert(e.id.to_ascii_lowercase());
            }
            let raw_title = e
                .music
                .as_ref()
                .and_then(|m| m.title.as_deref())
                .filter(|s| !s.is_empty())
                .unwrap_or(&e.title);
            let n = norm_text(raw_title);
            if !n.is_empty() {
                titles.insert(n);
            }
        }
        (ids, titles)
    }
}

impl DiscoverBridge for DiscoverHost {
    fn list_creators(&self) -> Value {
        let creators = self.lock_state().list_hydrated();
        json!({ "creators": creators })
    }

    fn follow_creator<'a>(&'a self, payload: Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let raw_url = payload
                .get("url")
                .or_else(|| payload.get("handle"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_owned();
            let explicit_source = payload
                .get("source")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            let fallback_name = truncate_chars(
                payload
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim(),
                120,
            );
            let explicit_channel = payload
                .get("channelId")
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_owned();

            let mut record = if DISCOVER_SOURCES.contains(&explicit_source.as_str())
                && !explicit_channel.is_empty()
                && !raw_url.is_empty()
            {
                SavedCreator {
                    id: format!("{explicit_source}:{explicit_channel}"),
                    source: explicit_source,
                    handle: truncate_chars(
                        payload
                            .get("handle")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .trim(),
                        80,
                    ),
                    name: if fallback_name.is_empty() {
                        explicit_channel.clone()
                    } else {
                        fallback_name
                    },
                    url: raw_url,
                    channel_id: explicit_channel,
                    avatar_url: payload
                        .get("avatarUrl")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    followed_at: now_ms(),
                }
            } else {
                let detected = detect_source_from_input(&raw_url).ok_or_else(|| {
                    HttpError::new(
                        400,
                        "Paste a YouTube (@handle or channel link), SoundCloud artist link, or Rumble channel link",
                    )
                })?;
                match detected.source {
                    "youtube" => {
                        self.resolve_youtube(
                            &detected.url,
                            detected.channel_id,
                            detected.handle,
                            &fallback_name,
                        )
                        .await?
                    }
                    "soundcloud" => {
                        self.resolve_soundcloud(&detected.url, &fallback_name)
                            .await?
                    }
                    _ => self.resolve_rumble(&detected.url, &fallback_name).await?,
                }
            };

            let (creator_json, all_creators, snapshot) = {
                let mut state = self.lock_state();
                if let Some(existing) = state.creators.iter_mut().find(|c| c.id == record.id) {
                    if existing.followed_at > 0 {
                        record.followed_at = existing.followed_at;
                    }
                    *existing = record.clone();
                } else {
                    if state.creators.len() >= MAX_CREATORS {
                        return Err(HttpError::new(
                            400,
                            format!("You can follow at most {MAX_CREATORS} creators"),
                        ));
                    }
                    state.creators.insert(0, record.clone());
                }
                state.feed_cache.remove(&record.id);
                let snapshot = state.creators.clone();
                let creator_json = state.hydrate_creator(&record);
                let all_creators = state.list_hydrated();
                (creator_json, all_creators, snapshot)
            };
            self.persist_creators(&snapshot);
            Ok(json!({ "creator": creator_json, "creators": all_creators }))
        })
    }

    fn unfollow_creator(&self, raw_id: &str) -> Value {
        let decoded = percent_decode(raw_id.trim());
        let (removed, all_creators, snapshot) = {
            let mut state = self.lock_state();
            let before = state.creators.len();
            state.creators.retain(|c| c.id != decoded);
            state.feed_cache.remove(&decoded);
            let removed = state.creators.len() != before;
            let snapshot = if removed {
                Some(state.creators.clone())
            } else {
                None
            };
            let all_creators = state.list_hydrated();
            (removed, all_creators, snapshot)
        };
        if let Some(snapshot) = snapshot {
            self.persist_creators(&snapshot);
        }
        json!({ "removed": removed, "creators": all_creators })
    }

    fn feed<'a>(
        &'a self,
        source_filter: String,
        creator_filter: String,
        refresh: bool,
        limit: usize,
    ) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let src = source_filter.trim().to_ascii_lowercase();
            let cid = creator_filter.trim().to_owned();
            let cap = limit.clamp(1, 100);
            let selected: Vec<SavedCreator> = {
                let state = self.lock_state();
                state
                    .creators
                    .iter()
                    .filter(|c| {
                        (src.is_empty() || c.source == src) && (cid.is_empty() || c.id == cid)
                    })
                    .cloned()
                    .collect()
            };

            let t_now = now_ms();
            let mut errors: Vec<Value> = Vec::new();
            let mut merged: Vec<Value> = Vec::new();
            let mut seen: HashSet<String> = HashSet::new();

            for creator in selected {
                let cached = self.lock_state().feed_cache.get(&creator.id).cloned();
                let batch = if !refresh
                    && let Some(c) = cached.as_ref()
                    && t_now.saturating_sub(c.at) < FEED_TTL_MS
                {
                    c.items.clone()
                } else {
                    match self.fetch_creator_items(&creator).await {
                        Ok(items) => {
                            self.lock_state().feed_cache.insert(
                                creator.id.clone(),
                                CachedFeed {
                                    at: t_now,
                                    items: items.clone(),
                                },
                            );
                            items
                        }
                        Err(msg) => {
                            errors.push(json!({
                                "creatorId": creator.id,
                                "source": creator.source,
                                "name": creator.name,
                                "message": truncate_chars(&msg, 200),
                            }));
                            cached.map(|c| c.items).unwrap_or_default()
                        }
                    }
                };
                for item in batch {
                    let id = item
                        .get("id")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned();
                    if id.is_empty() || !seen.insert(id) {
                        continue;
                    }
                    merged.push(item);
                }
            }

            let (lib_ids, lib_titles) = self.library_matcher().await;
            for item in &mut merged {
                let acq = item
                    .get("acquireId")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                let t = norm_text(item.get("title").and_then(Value::as_str).unwrap_or(""));
                let in_lib = (!acq.is_empty() && lib_ids.contains(&acq))
                    || (!t.is_empty() && lib_titles.contains(&t));
                item["inLibrary"] = Value::Bool(in_lib);
            }
            merged.sort_by(|a, b| {
                let pa = a.get("publishedAt").and_then(Value::as_u64).unwrap_or(0);
                let pb = b.get("publishedAt").and_then(Value::as_u64).unwrap_or(0);
                pb.cmp(&pa).then_with(|| {
                    let ta = a.get("title").and_then(Value::as_str).unwrap_or("");
                    let tb = b.get("title").and_then(Value::as_str).unwrap_or("");
                    ta.cmp(tb)
                })
            });
            merged.truncate(cap);

            let creators_json = self.lock_state().list_hydrated();
            Ok(json!({
                "creators": creators_json,
                "items": merged,
                "errors": errors,
                "updatedAt": t_now,
            }))
        })
    }

    fn browse<'a>(&'a self, payload: Value) -> BoxFuture<'a, Result<Value, HttpError>> {
        Box::pin(async move {
            let query = truncate_chars(
                payload
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .trim(),
                200,
            );
            if query.is_empty() {
                return Err(HttpError::new(400, "query is required"));
            }
            let mirror = (self.get_mirror)().ok_or_else(|| {
                HttpError::new(503, "Source search is not available on this device")
            })?;
            let picked: Vec<String> = payload
                .get("sources")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(|s| s.trim().to_ascii_lowercase())
                        .filter(|s| DISCOVER_SOURCES.contains(&s.as_str()))
                        .collect()
                })
                .unwrap_or_default();
            let active_sources: Vec<String> = if picked.is_empty() {
                DISCOVER_SOURCES.iter().map(|s| (*s).to_owned()).collect()
            } else {
                picked
            };
            let limit = payload
                .get("limit")
                .and_then(Value::as_u64)
                .unwrap_or(12)
                .clamp(1, 20);

            let search_req = json!({
                "query": query,
                "sources": active_sources,
                "limit": limit,
            });
            let res = mirror
                .search_sources(search_req, None)
                .await
                .map_err(|e| HttpError::new(e.status.unwrap_or(500), e.message))?;

            let (lib_ids, lib_titles) = self.library_matcher().await;
            let (followed_urls, followed_names): (HashSet<String>, HashSet<String>) = {
                let state = self.lock_state();
                let urls = state
                    .creators
                    .iter()
                    .map(|c| c.url.to_ascii_lowercase())
                    .collect();
                let names = state
                    .creators
                    .iter()
                    .map(|c| format!("{}:{}", c.source, norm_text(&c.name)))
                    .collect();
                (urls, names)
            };

            let mut items = Vec::new();
            let mut creators_out: Vec<Value> = Vec::new();
            let mut seen_creators: HashSet<String> = HashSet::new();

            for row in res.results {
                let source = row
                    .get("source")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let url = row
                    .get("url")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                if !DISCOVER_SOURCES.contains(&source.as_str()) || url.is_empty() {
                    continue;
                }
                let item_id = row
                    .get("videoId")
                    .or_else(|| row.get("trackId"))
                    .or_else(|| row.get("rumbleId"))
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| {
                        let h = sha256_hex(&url);
                        h.get(..12).unwrap_or(&h).to_owned()
                    });
                let title = truncate_chars(
                    row.get("title")
                        .and_then(Value::as_str)
                        .unwrap_or(&item_id)
                        .trim(),
                    300,
                );
                let is_audio = source == "soundcloud";
                let mut creator_name = truncate_chars(
                    row.get("artist")
                        .or_else(|| row.get("channel"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .trim(),
                    120,
                );
                let mut creator_url = String::new();
                if source == "soundcloud" {
                    if let Ok(parsed) = Url::parse(&url)
                        && let Some(seg) = parsed.path().split('/').find(|s| !s.is_empty())
                    {
                        creator_url = format!("https://soundcloud.com/{seg}");
                        if creator_name.is_empty() {
                            creator_name = seg.to_owned();
                        }
                    }
                } else if source == "youtube" {
                    creator_url = url.clone();
                }
                if creator_name.is_empty() {
                    creator_name = match source.as_str() {
                        "youtube" => "YouTube".to_owned(),
                        "soundcloud" => "SoundCloud".to_owned(),
                        _ => "Rumble".to_owned(),
                    };
                }
                let creator_id = if creator_url.is_empty() {
                    String::new()
                } else {
                    let h = sha256_hex(&creator_url);
                    format!("{source}:{}", h.get(..12).unwrap_or(&h))
                };
                let acq = acquire_id_for(&source, &url);
                let in_lib = lib_ids.contains(&acq.to_ascii_lowercase())
                    || lib_titles.contains(&norm_text(&title));
                let thumb = row
                    .get("thumbnail")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let music = if is_audio {
                    json!({ "title": title, "artist": creator_name })
                } else {
                    Value::Null
                };
                items.push(json!({
                    "id": format!("{source}:{item_id}"),
                    "source": source,
                    "kind": if is_audio { "audio" } else { "video" },
                    "itemId": item_id,
                    "title": title,
                    "creatorId": creator_id,
                    "creatorName": creator_name,
                    "creatorUrl": creator_url,
                    "url": url,
                    "thumbnail": thumb,
                    "duration": row.get("length").cloned().unwrap_or(Value::Null),
                    "publishedAt": 0,
                    "acquireId": acq,
                    "audioOnly": is_audio,
                    "music": music,
                    "inLibrary": in_lib,
                }));

                let c_key = format!("{source}:{}", norm_text(&creator_name));
                if !creator_url.is_empty()
                    && creator_name != "YouTube"
                    && creator_name != "Rumble"
                    && creators_out.len() < 12
                    && seen_creators.insert(c_key.clone())
                {
                    let followed = followed_urls.contains(&creator_url.to_ascii_lowercase())
                        || followed_names.contains(&c_key);
                    creators_out.push(json!({
                        "id": creator_id,
                        "source": source,
                        "handle": "",
                        "name": creator_name,
                        "url": creator_url,
                        "channelId": "",
                        "avatar": thumb,
                        "followedAt": 0,
                        "followed": followed,
                    }));
                }
            }

            Ok(json!({
                "query": query,
                "sources": active_sources,
                "creators": creators_out,
                "items": items,
                "errors": res.summary.errors,
            }))
        })
    }

    fn thumb<'a>(&'a self, token: &'a str) -> BoxFuture<'a, Result<Option<Picture>, HttpError>> {
        Box::pin(async move {
            if !is_thumb_token(token) {
                return Ok(None);
            }
            if let Some(cached) = self.lock_state().cached_thumb(token) {
                return Ok(Some(cached));
            }
            let Some(url) = self.lock_state().thumb_url(token) else {
                return Ok(None);
            };
            let Ok(reply) = send_bytes(
                &url,
                SendOptions {
                    method: Method::GET,
                    headers: Vec::new(),
                    timeout: THUMB_TIMEOUT,
                    max_bytes: Some(THUMB_MAX_BYTES as u64),
                    redirects: 0,
                    allow_private: self.allow_private_urls,
                },
            )
            .await
            else {
                return Ok(None);
            };
            if reply.status != 200 || reply.body.is_empty() || reply.body.len() > THUMB_MAX_BYTES {
                return Ok(None);
            }
            let raw_ct = reply
                .header("content-type")
                .unwrap_or("")
                .split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase();
            if !matches!(
                raw_ct.as_str(),
                "image/jpeg" | "image/png" | "image/webp" | "image/gif"
            ) {
                return Ok(None);
            }
            let pic = Picture {
                content_type: raw_ct,
                bytes: Bytes::from(reply.body),
            };
            self.lock_state()
                .insert_thumb(token.to_owned(), pic.clone());
            Ok(Some(pic))
        })
    }
}

struct DetectedInput {
    source: &'static str,
    url: String,
    channel_id: String,
    handle: String,
}

fn detect_source_from_input(raw: &str) -> Option<DetectedInput> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with('@') && !trimmed.chars().any(char::is_whitespace) {
        return Some(DetectedInput {
            source: "youtube",
            url: format!("https://www.youtube.com/{trimmed}"),
            channel_id: String::new(),
            handle: trimmed.to_owned(),
        });
    }
    if is_yt_channel_id(trimmed) {
        return Some(DetectedInput {
            source: "youtube",
            url: format!("https://www.youtube.com/channel/{trimmed}"),
            channel_id: trimmed.to_owned(),
            handle: String::new(),
        });
    }
    let parsed = Url::parse(trimmed).ok()?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return None;
    }
    let host = parsed.host_str()?.to_ascii_lowercase();
    let host = host.strip_prefix("www.").unwrap_or(&host);
    let host = host.strip_prefix("m.").unwrap_or(host);
    let source = match host {
        "youtube.com" | "youtu.be" | "music.youtube.com" => "youtube",
        "soundcloud.com" | "on.soundcloud.com" => "soundcloud",
        "rumble.com" => "rumble",
        _ => return None,
    };
    Some(DetectedInput {
        source,
        url: parsed.as_str().to_owned(),
        channel_id: String::new(),
        handle: String::new(),
    })
}

pub fn parse_youtube_feed_xml(
    xml: &str,
    creator: Option<&SavedCreator>,
    mut thumb_out: Option<&mut Vec<(usize, String)>>,
) -> (String, Vec<Value>) {
    let head = xml.split("<entry").next().unwrap_or("");
    let feed_title = xml_tag(head, "title");
    let mut items = Vec::new();
    let mut seen = HashSet::new();

    for part in xml.split("<entry").skip(1) {
        let Some(gt) = part.find('>') else {
            continue;
        };
        let Some(after) = part.get(gt + 1..) else {
            continue;
        };
        let entry = after.split("</entry>").next().unwrap_or(after);
        let video_id = xml_tag(entry, "yt:videoId");
        if !is_yt_video_id(&video_id) || !seen.insert(video_id.clone()) {
            continue;
        }
        let media_title = xml_tag(entry, "media:title");
        let title_raw = if !media_title.is_empty() {
            media_title
        } else {
            let t = xml_tag(entry, "title");
            if t.is_empty() { video_id.clone() } else { t }
        };
        let title = truncate_chars(&title_raw, 300);
        let author_tag = xml_tag(entry, "name");
        let author = if !author_tag.is_empty() {
            author_tag
        } else if let Some(c) = creator {
            c.name.clone()
        } else if !feed_title.is_empty() {
            feed_title.clone()
        } else {
            "YouTube creator".to_owned()
        };
        let pub_raw = {
            let p = xml_tag(entry, "published");
            if !p.is_empty() {
                p
            } else {
                xml_tag(entry, "updated")
            }
        };
        let pub_ms = parse_iso_timestamp_ms(&pub_raw).unwrap_or(0);
        let url = format!("https://www.youtube.com/watch?v={video_id}");
        let raw_thumb = {
            let attr = xml_attr(entry, "media:thumbnail", "url");
            if !attr.is_empty() {
                attr
            } else {
                format!("https://i.ytimg.com/vi/{video_id}/hqdefault.jpg")
            }
        };
        let chan_id = {
            let tag = xml_tag(entry, "yt:channelId");
            if !tag.is_empty() {
                tag
            } else {
                creator.map(|c| c.channel_id.clone()).unwrap_or_default()
            }
        };
        let creator_id = creator.map(|c| c.id.clone()).unwrap_or_else(|| {
            if chan_id.is_empty() {
                String::new()
            } else {
                format!("youtube:{chan_id}")
            }
        });
        let creator_url = creator.map(|c| c.url.clone()).unwrap_or_else(|| {
            if chan_id.is_empty() {
                String::new()
            } else {
                format!("https://www.youtube.com/channel/{chan_id}")
            }
        });
        let idx = items.len();
        if let Some(out) = thumb_out.as_mut() {
            out.push((idx, raw_thumb));
        }
        let acq = acquire_id_for("youtube", &url);
        items.push(json!({
            "id": format!("youtube:{video_id}"),
            "source": "youtube",
            "kind": "video",
            "itemId": video_id,
            "title": title,
            "creatorId": creator_id,
            "creatorName": truncate_chars(&author, 120),
            "creatorUrl": creator_url,
            "url": url,
            "thumbnail": "",
            "duration": Value::Null,
            "publishedAt": pub_ms,
            "acquireId": acq,
            "audioOnly": false,
            "music": Value::Null,
            "inLibrary": false,
        }));
    }
    (feed_title, items)
}

fn extract_youtube_channel_id(html: &str, is_watch: bool) -> Option<String> {
    if !is_watch {
        if let Some(cid) = find_uc_after(html, "youtube.com/channel/") {
            return Some(cid);
        }
        if let Some(cid) = find_uc_after(html, "\"externalId\":\"") {
            return Some(cid);
        }
    }
    if let Some(pos) = html.find("\"videoDetails\"")
        && let Some(slice) = html.get(pos..html.len().min(pos + 2400))
        && let Some(cid) = find_uc_after(slice, "\"channelId\":\"")
    {
        return Some(cid);
    }
    find_uc_after(html, "\"channelId\":\"").or_else(|| find_uc_after(html, "\"browseId\":\""))
}

fn extract_video_details_author(html: &str) -> Option<String> {
    let pos = html.find("\"videoDetails\"")?;
    let slice = html.get(pos..html.len().min(pos + 2400))?;
    let key = "\"author\":\"";
    let start = slice.find(key)? + key.len();
    let rest = slice.get(start..)?;
    let end = rest.find('"')?;
    let raw = rest.get(..end)?.trim();
    (!raw.is_empty()).then(|| raw.to_owned())
}

fn find_uc_after(haystack: &str, needle: &str) -> Option<String> {
    let mut cur = 0;
    while let Some(rel) = haystack.get(cur..).and_then(|s| s.find(needle)) {
        let start = cur + rel + needle.len();
        if let Some(candidate) = haystack.get(start..start + 24)
            && is_yt_channel_id(candidate)
        {
            return Some(candidate.to_owned());
        }
        cur = start;
    }
    None
}

fn meta_content(html: &str, prop: &str) -> Option<String> {
    let low = html.to_ascii_lowercase();
    let needle = format!("\"{}\"", prop.to_ascii_lowercase());
    let mut cur = 0;
    while let Some(rel) = low.get(cur..).and_then(|s| s.find(&needle)) {
        let pos = cur + rel;
        cur = pos + needle.len();
        let Some(tag_start) = low.get(..pos).and_then(|s| s.rfind("<meta")) else {
            continue;
        };
        let Some(tag_end_rel) = low.get(pos..).and_then(|s| s.find('>')) else {
            continue;
        };
        let Some(tag) = html.get(tag_start..pos + tag_end_rel + 1) else {
            continue;
        };
        if let Some(val) = attr_value(tag, "content")
            && !val.is_empty()
        {
            return Some(decode_entities(&val));
        }
    }
    None
}

fn attr_value(tag: &str, name: &str) -> Option<String> {
    let low = tag.to_ascii_lowercase();
    let needle = format!("{}=", name.to_ascii_lowercase());
    let rel = low.find(&needle)?;
    let rest = tag.get(rel + needle.len()..)?;
    let quote = rest.as_bytes().first().copied()?;
    if quote != b'"' && quote != b'\'' {
        return None;
    }
    let inner = rest.get(1..)?;
    let end = inner.find(quote as char)?;
    inner.get(..end).map(|s| s.trim().to_owned())
}

fn xml_tag(block: &str, tag: &str) -> String {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let Some(start) = block.find(&open) else {
        return String::new();
    };
    let Some(after_open) = block.get(start + open.len()..) else {
        return String::new();
    };
    let Some(gt) = after_open.find('>') else {
        return String::new();
    };
    let Some(rest) = after_open.get(gt + 1..) else {
        return String::new();
    };
    let Some(end) = rest.find(&close) else {
        return String::new();
    };
    decode_entities(rest.get(..end).unwrap_or(""))
        .trim()
        .to_owned()
}

fn xml_attr(block: &str, tag: &str, attr: &str) -> String {
    let open = format!("<{tag}");
    let Some(start) = block.find(&open) else {
        return String::new();
    };
    let Some(rest) = block.get(start..) else {
        return String::new();
    };
    let Some(gt) = rest.find('>') else {
        return String::new();
    };
    let Some(slice) = rest.get(..gt + 1) else {
        return String::new();
    };
    attr_value(slice, attr)
        .map(|s| decode_entities(&s))
        .unwrap_or_default()
}

fn decode_entities(raw: &str) -> String {
    let cleaned = raw
        .strip_prefix("<![CDATA[")
        .and_then(|s| s.strip_suffix("]]>"))
        .unwrap_or(raw);
    cleaned
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'")
}

fn extract_sc_scripts(html: &str, site_base: &str) -> Vec<String> {
    let Ok(base) = Url::parse(site_base) else {
        return Vec::new();
    };
    let low = html.to_ascii_lowercase();
    let mut urls = Vec::new();
    let mut seen = HashSet::new();
    let mut cur = 0;
    while let Some(rel) = low.get(cur..).and_then(|s| s.find("<script")) {
        let start = cur + rel;
        let Some(end_rel) = low.get(start..).and_then(|s| s.find('>')) else {
            break;
        };
        cur = start + end_rel + 1;
        let Some(tag) = html.get(start..cur) else {
            continue;
        };
        let Some(src) = attr_value(tag, "src") else {
            continue;
        };
        let Ok(resolved) = base.join(&src) else {
            continue;
        };
        let href = resolved.as_str();
        let low_href = href.to_ascii_lowercase();
        if (low_href.contains("sndcdn.com/assets/") || low_href.contains("/assets/"))
            && low_href.contains(".js")
            && seen.insert(href.to_owned())
        {
            urls.push(href.to_owned());
        }
    }
    urls
}

fn sc_artwork(raw: &str) -> String {
    let trimmed = raw.trim();
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return String::new();
    }
    if trimmed.to_ascii_lowercase().contains("sndcdn.com") {
        for ext in [".jpg", ".jpeg", ".png", ".webp"] {
            let suffix = format!("-large{ext}");
            if let Some(stem) = trimmed.strip_suffix(&suffix) {
                return format!("{stem}-t300x300{ext}");
            }
        }
    }
    trimmed.to_owned()
}

fn load_saved(file: &Path) -> Vec<SavedCreator> {
    let Ok(raw) = fs::read_to_string(file) else {
        return Vec::new();
    };
    let Ok(parsed) = serde_json::from_str::<SavedFile>(&raw) else {
        return Vec::new();
    };
    parsed
        .creators
        .into_iter()
        .filter(|c| {
            DISCOVER_SOURCES.contains(&c.source.as_str()) && !c.id.is_empty() && !c.url.is_empty()
        })
        .collect()
}

fn save_creators(file: &Path, creators: &[SavedCreator]) {
    if let Some(parent) = file.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let payload = SavedFile {
        version: 1,
        creators: creators.to_vec(),
    };
    let Ok(mut json) = serde_json::to_string_pretty(&payload) else {
        return;
    };
    json.push('\n');
    let tmp = file.with_extension(format!("json.tmp.{}", std::process::id()));
    #[cfg(unix)]
    let write_ok = {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .and_then(|mut f| f.write_all(json.as_bytes()))
            .is_ok()
    };
    #[cfg(not(unix))]
    let write_ok = fs::write(&tmp, json.as_bytes()).is_ok();
    if write_ok {
        let _ = fs::rename(&tmp, file);
    } else {
        let _ = fs::remove_file(&tmp);
    }
}

fn acquire_id_for(source: &str, url: &str) -> String {
    let hex = sha256_hex(url);
    if source == "soundcloud" {
        format!("sc:{}:s01e01", hex.get(..12).unwrap_or(&hex))
    } else {
        format!("clip:{}", hex.get(..16).unwrap_or(&hex))
    }
}

fn sha256_hex(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut out, "{b:02x}");
    }
    out
}

fn is_yt_channel_id(s: &str) -> bool {
    s.len() == 24
        && s.starts_with("UC")
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn is_yt_video_id(s: &str) -> bool {
    s.len() == 11
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn is_thumb_token(s: &str) -> bool {
    s.len() == 20 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

fn norm_text(s: &str) -> String {
    let mut out = String::new();
    let mut prev_space = true;
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }
    out.trim().to_owned()
}

fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let Ok(hex) = std::str::from_utf8(&bytes[i + 1..i + 3])
            && let Ok(val) = u8::from_str_radix(hex, 16)
        {
            out.push(val);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Minimal ISO-8601 (`YYYY-MM-DDTHH:MM:SSZ` or `+00:00`) parser -> epoch ms.
fn parse_iso_timestamp_ms(raw: &str) -> Option<u64> {
    let s = raw.trim();
    if s.len() < 19 {
        return None;
    }
    let y: i64 = s.get(0..4)?.parse().ok()?;
    let m: i64 = s.get(5..7)?.parse().ok()?;
    let d: i64 = s.get(8..10)?.parse().ok()?;
    let hh: i64 = s.get(11..13)?.parse().ok()?;
    let mm: i64 = s.get(14..16)?.parse().ok()?;
    let ss: i64 = s.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || hh > 23 || mm > 59 || ss > 60 {
        return None;
    }
    let days = days_from_civil(y, m, d);
    let secs = days * 86_400 + hh * 3_600 + mm * 60 + ss;
    u64::try_from(secs).ok().map(|v| v * 1000)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = m + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn youtube_atom_xml_parses_entries() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns:yt="http://www.youtube.com/xml/schemas/2015" xmlns:media="http://search.yahoo.com/mrss/">
  <title>Lofi Girl</title>
  <entry>
    <yt:videoId>nx32Ue0AAA0</yt:videoId>
    <yt:channelId>UCSJ4gkVC6NrvII8umztf0Ow</yt:channelId>
    <title>rain &amp; chill</title>
    <author><name>Lofi Girl</name></author>
    <published>2026-11-18T19:00:10+00:00</published>
    <media:group><media:thumbnail url="https://i3.ytimg.com/vi/nx32Ue0AAA0/hqdefault.jpg"/></media:group>
  </entry>
</feed>"#;
        let mut thumbs = Vec::new();
        let (title, items) = parse_youtube_feed_xml(xml, None, Some(&mut thumbs));
        assert_eq!(title, "Lofi Girl");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["id"], "youtube:nx32Ue0AAA0");
        assert_eq!(items[0]["title"], "rain & chill");
        assert_eq!(items[0]["audioOnly"], false);
        assert!(items[0]["publishedAt"].as_u64().unwrap_or(0) > 1_700_000_000_000);
        assert_eq!(thumbs.len(), 1);
    }

    #[test]
    fn youtube_handle_prefers_canonical_channel_id() {
        let html = r#"<link rel="canonical" href="https://www.youtube.com/channel/UCSJ4gkVC6NrvII8umztf0Ow">
            {"channelId":"UCWRONG00000000000000000","externalId":"UCSJ4gkVC6NrvII8umztf0Ow"}"#;
        assert_eq!(
            extract_youtube_channel_id(html, false).as_deref(),
            Some("UCSJ4gkVC6NrvII8umztf0Ow")
        );
    }

    #[test]
    fn discover_creators_persist_across_reopen() {
        let dir = tempfile::tempdir().expect("tempdir");
        let host = DiscoverHost::open(dir.path(), false, Arc::new(|| None), Arc::new(|| None));
        let rt = tokio::runtime::Runtime::new().expect("rt");
        let added = rt
            .block_on(host.follow_creator(json!({
                "source": "youtube",
                "channelId": "UCSJ4gkVC6NrvII8umztf0Ow",
                "url": "https://www.youtube.com/channel/UCSJ4gkVC6NrvII8umztf0Ow",
                "name": "Lofi Girl"
            })))
            .expect("follow");
        assert_eq!(added["creator"]["id"], "youtube:UCSJ4gkVC6NrvII8umztf0Ow");

        let reopened = DiscoverHost::open(dir.path(), false, Arc::new(|| None), Arc::new(|| None));
        let listed = reopened.list_creators();
        assert_eq!(listed["creators"].as_array().map(Vec::len), Some(1));
        let removed = reopened.unfollow_creator("youtube%3AUCSJ4gkVC6NrvII8umztf0Ow");
        assert_eq!(removed["removed"], true);
        assert_eq!(removed["creators"].as_array().map(Vec::len), Some(0));
    }
}
