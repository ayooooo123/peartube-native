//! YouTube provider for PearTube relays, ported from `src/providers/youtube.js`:
//!
//!   - Searches YouTube through `ubit` (`info ytsearch:<query>`), one page, no account, no cookies.
//!   - Saves chosen videos' audio as the tracks of an album the caller names, into
//!     `<staging>/<Artist> - <Album> (<Year>) [YouTube]/NN - Title.m4a`, pinned to a `yt:` release,
//!     then has the host import it.
//!   - Every copy is lossy (AAC about 128 kbps, stream-copied, never re-wrapped): rows say so, and
//!     the `yt:` id keeps a saved album from answering for an `mb:`/`discogs:`/`album:` release.
//!   - Jobs use the Soulseek provider's shape (`/v1/jobs`), without a peer, a queue place or a
//!     total size.
//!
//! `ubit` and the relay's guarded fetch stand behind [`Backend`] and [`Fetch`]. A host without a
//! backend (the apps' engine cannot run `ubit`) reports `unavailable`. [`Provider`] knows nothing
//! of the node: the mirror host registers it as its `youtube` plugin and hands it its
//! `placeAndScan`.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use bytes::Bytes;
use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use super::staging::{
    ALBUM_PIN, Cancelled, Place, PlaceAndScan, PlaceError, free_path, note_mirrored, placed_here,
    safe_segment, split_ext,
};
use crate::acquire::RandomSource;
use crate::app_settings::{js_string, js_string_or_empty, js_truthy};
use crate::identify::{self, Controller, DownloadOptions, DownloadProgress, IdentifyError, js_num};
use crate::protocol::{MAX_TITLE, is_media_file, js_trim, js_trim_end, truncate_utf16, utf16_len};
use crate::tags::album_key;

/// The plugin's name on the host.
pub const NAME: &str = "youtube";
pub const DESCRIPTION: &str =
    "Finds music on YouTube and saves its audio (AAC, lossy) as albums in the staging folder";
/// One page of results is all `ubit` returns.
pub const YOUTUBE_SEARCH_CAP: usize = 20;
/// Nominal rate of the AAC stream `ubit get -a` copies (itag 140; measured 129.5 kbps).
pub const YOUTUBE_BITRATE: u32 = 128;
const MAX_TRACKS: usize = 100;
// Empty answers in a row before the provider says the extractor may be out of date: when YouTube
// changes its result layout, `ubit` finds nothing and that cannot be told from a query with no
// results.
const EMPTY_STREAK: u32 = 3;
// Finished jobs are dropped once more than 48 are listed, down to 32.
const JOBS_MOST: usize = 48;
const JOBS_KEPT: usize = 32;
const THUMBS_KEPT: usize = 64;

/// A request the provider refuses or could not finish, with the HTTP status the app's error
/// carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YoutubeError {
    pub message: String,
    pub status: Option<u16>,
}

impl YoutubeError {
    fn status(status: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: Some(status),
        }
    }

    fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }

    fn cancelled() -> Self {
        Self::plain("Cancelled")
    }
}

impl std::fmt::Display for YoutubeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for YoutubeError {}

/// What `ubit` could not do, with the HTTP status its message names when it names one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendError {
    pub message: String,
    pub status: Option<u16>,
}

pub type BackendFuture<T> = Pin<Box<dyn Future<Output = Result<T, BackendError>> + Send>>;

/// The bytes of the file a download has so far.
pub type ProgressFn = Box<dyn FnMut(u64) + Send>;

/// The relay's `ubitInfo` and `downloadPage` (`src/identify.js`), as the app passes them in.
pub trait Backend: Send + Sync {
    /// `ubitInfo(query, { timeoutMs })`.
    fn info(&self, query: &str, timeout_ms: f64) -> BackendFuture<Value>;
    /// `downloadPage(url, outDir, { audio: true, controller, onProgress })`: the file it left in
    /// `out_dir`. Aborting `controller` ends it at once.
    fn download(
        &self,
        url: &str,
        out_dir: &Path,
        controller: Controller,
        on_progress: ProgressFn,
    ) -> BackendFuture<PathBuf>;
}

/// A reply of the relay's guarded fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub status: u16,
    pub content_type: Option<String>,
    pub body: Vec<u8>,
}

/// The relay's guarded fetch (`send` in `src/art.js` with `binary`), for result pictures.
pub trait Fetch: Send + Sync {
    fn fetch(&self, url: &str) -> Pin<Box<dyn Future<Output = Result<Fetched, String>> + Send>>;
}

/// A result's picture, as the relay serves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Picture {
    pub content_type: String,
    pub bytes: Bytes,
}

/// The real `ubit`: `ubitInfo` and `downloadPage` with the relay's config folder, as
/// `bin/relay.js` hands them in.
pub struct Ubit {
    pub config_dir: String,
    /// `UBIT_BIN`; empty runs the baked-in engine.
    pub ubit: String,
}

impl Backend for Ubit {
    fn info(&self, query: &str, timeout_ms: f64) -> BackendFuture<Value> {
        let (ubit, query, config_dir) =
            (self.ubit.clone(), query.to_owned(), self.config_dir.clone());
        Box::pin(async move {
            let timeout = std::time::Duration::from_millis(timeout_ms as u64);
            identify::ubit_info(&ubit, &query, &config_dir, timeout)
                .await
                .map_err(ubit_error)
        })
    }

    fn download(
        &self,
        url: &str,
        out_dir: &Path,
        controller: Controller,
        mut on_progress: ProgressFn,
    ) -> BackendFuture<PathBuf> {
        let (ubit, url, config_dir) = (self.ubit.clone(), url.to_owned(), self.config_dir.clone());
        let out_dir = out_dir.to_string_lossy().into_owned();
        Box::pin(async move {
            let saved = identify::download_page(
                &url,
                &out_dir,
                DownloadOptions {
                    config_dir,
                    ubit,
                    controller: Some(controller),
                    on_progress: Some(Box::new(move |progress: DownloadProgress| {
                        on_progress(progress.bytes.0 as u64);
                    })),
                    audio: true,
                    ..DownloadOptions::default()
                },
            )
            .await
            .map_err(ubit_error)?;
            Ok(PathBuf::from(saved.path))
        })
    }
}

fn ubit_error(error: IdentifyError) -> BackendError {
    BackendError {
        message: error.message().to_owned(),
        status: error.status(),
    }
}

/// `createYoutubePlugin`'s options.
pub struct Options {
    pub storage: String,
    /// The folder saves land in; `<storage>/staging` when the host names none.
    pub staging_root: Option<String>,
    pub staging_label: String,
    /// `None` on a host that cannot run `ubit`: the plugin then reports `unavailable`.
    pub backend: Option<Arc<dyn Backend>>,
    pub fetch: Option<Arc<dyn Fetch>>,
    pub search_timeout_ms: f64,
    pub onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    pub random: Arc<dyn RandomSource>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Options {
    pub fn new(
        storage: impl Into<String>,
        backend: Option<Arc<dyn Backend>>,
        random: Arc<dyn RandomSource>,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            storage: storage.into(),
            staging_root: None,
            staging_label: "storage".to_owned(),
            backend,
            fetch: None,
            search_timeout_ms: 20_000.0,
            onchange: None,
            random,
            now_ms,
        }
    }
}

/// `[A-Za-z0-9_-]{11}`.
fn is_video_id(text: &str) -> bool {
    text.len() == 11
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

/// `Math.round`: a half goes up.
fn js_round(number: f64) -> f64 {
    (number + 0.5).floor()
}

/// YouTube's refusal of a whole network exit (`/not a bot/i`). Its age gate ("Sign in to confirm
/// your age") is one video's refusal and must not match.
fn is_bot_check(reason: &str) -> bool {
    reason.to_ascii_lowercase().contains("not a bot")
}

/// The `yt:` release an album saved from YouTube is pinned to (`youtubeRelease`): the same hash
/// the importer gives an untagged folder as `album:<hex>`, so the two name one album without
/// sharing an id.
pub fn youtube_release(artist: &str, album: &str, year: Option<u32>) -> String {
    let digest = Sha256::digest(album_key(Some(artist), Some(album), year).as_bytes());
    let mut hex = String::with_capacity(20);
    for byte in &digest[..10] {
        hex.push_str(&format!("{byte:02x}"));
    }
    format!("yt:{hex}")
}

/// `normalizeYoutubeResults`: one row for each entry with a video id not seen before, `limit` at
/// most.
pub fn normalize_results(info: &Value, limit: usize) -> Vec<Value> {
    let mut rows: Vec<Value> = Vec::new();
    let mut seen: Vec<&str> = Vec::new();
    let entries = info.get("entries").and_then(Value::as_array);
    for entry in entries.into_iter().flatten() {
        let Some(video_id) = entry.get("id").and_then(Value::as_str) else {
            continue;
        };
        if !is_video_id(video_id) || seen.contains(&video_id) {
            continue;
        }
        seen.push(video_id);
        let title = truncate_utf16(js_trim(&js_string_or_empty(entry.get("title"))), 300);
        let length = js_num(entry.get("duration"));
        // `ubit` gives no uploader for search results today, so this is null until it does.
        let channel = ["author", "channel", "uploader"]
            .into_iter()
            .filter_map(|key| entry.get(key))
            .find(|named| js_truthy(named))
            .map(|named| truncate_utf16(js_trim(&js_string(named)), 120))
            .filter(|name| !name.is_empty());
        let mut row = Map::new();
        row.insert(
            "title".into(),
            json!(if title.is_empty() {
                video_id
            } else {
                title.as_str()
            }),
        );
        row.insert(
            "length".into(),
            if length.is_finite() && length > 0.0 {
                json!(js_round(length) as u64)
            } else {
                Value::Null
            },
        );
        row.insert("size".into(), Value::Null);
        row.insert("bitrate".into(), json!(YOUTUBE_BITRATE));
        row.insert("sample_rate".into(), Value::Null);
        row.insert("bit_depth".into(), Value::Null);
        row.insert("lossy".into(), json!(true));
        row.insert("videoId".into(), json!(video_id));
        row.insert(
            "url".into(),
            json!(format!("https://www.youtube.com/watch?v={video_id}")),
        );
        row.insert("channel".into(), json!(channel));
        // Served by this relay (`thumbnail` below), so the owner's browser never contacts
        // YouTube's image host.
        row.insert(
            "thumbnail".into(),
            json!(format!("/v1/providers/youtube/thumb/{video_id}")),
        );
        rows.push(Value::Object(row));
        if rows.len() >= limit {
            break;
        }
    }
    rows
}

/// One track of a save, as the caller named it.
#[derive(Debug, Clone)]
struct Track {
    video_id: String,
    title: String,
    track: u32,
}

/// `NN - Title<ext>`.
fn file_of(track: &Track, ext: &str) -> String {
    let number = format!("{:02}", track.track);
    format!(
        "{}{ext}",
        safe_segment(&format!("{number} - {}", track.title), &number)
    )
}

/// One save, as `/v1/jobs` lists it.
struct Job {
    id: String,
    /// Aborting it ends the running download; the save then ends as cancelled.
    controller: Controller,
    record: Mutex<JobRecord>,
}

struct JobRecord {
    release: String,
    title: String,
    /// `queued` (behind another save), `transferring`, `announcing`, `done`, `failed` or
    /// `cancelled`.
    status: &'static str,
    phase: &'static str,
    created: u64,
    file: String,
    file_index: usize,
    file_count: usize,
    speed: u64,
    bytes: u64,
    /// YouTube does not state a size before the download, so there is never a percentage for the
    /// album.
    total: Option<u64>,
    error: Option<String>,
    /// Tracks that could not be saved while the rest of the album was: `{ track, title, error }`.
    skipped: Vec<Value>,
    note: Option<String>,
    progress_at: u64,
    /// The size and the sha256 of each file the import added, once the save is done.
    done: Option<(u64, Vec<Value>)>,
}

impl Job {
    fn record(&self) -> MutexGuard<'_, JobRecord> {
        self.record.lock().expect("youtube job")
    }

    fn status(&self) -> &'static str {
        self.record().status
    }

    /// `publicJob`: `idleMs` is how long a waiting or transferring job has gone without a new byte.
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

/// What the tracks a save could not take say on its job (`note`).
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
    /// In the order they were made.
    jobs: Vec<Arc<Job>>,
    closed: bool,
    /// `ubit` holds one connection at a time, so saves run one after another across jobs: this
    /// ends when the last save asked for has ended.
    chain: Shared<BoxFuture<'static, ()>>,
    last_search_at: Option<u64>,
    last_result_count: Option<usize>,
    empty_streak: u32,
    last_error: Option<String>,
    /// YouTube's bot check refuses every download from a network exit it distrusts (hosting
    /// ranges, some VPNs) while search keeps working. It is the network's state, not one
    /// video's: set by a refused save, cleared by a saved one.
    saves_blocked: bool,
    /// A few result pictures, oldest first: the video id, a mark that tells one request for it
    /// from the next, and the request.
    thumbs: Vec<(String, u64, Pending)>,
    thumb_marks: u64,
}

struct Inner {
    storage: String,
    staging_root: String,
    staging_label: String,
    backend: Option<Arc<dyn Backend>>,
    fetch: Option<Arc<dyn Fetch>>,
    search_timeout_ms: f64,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    random: Arc<dyn RandomSource>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<State>,
}

/// What `mirror` settled before the save started.
struct Plan {
    artist: String,
    release: String,
    album_title: String,
    folder_name: String,
    tracks: Vec<Track>,
}

/// The YouTube plugin (`createYoutubePlugin`'s returned object).
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
                empty_streak: 0,
                last_error: None,
                saves_blocked: false,
                thumbs: Vec::new(),
                thumb_marks: 0,
            }),
        }));
        // Whatever a killed relay left half-downloaded.
        if provider.0.backend.is_some() {
            let _ = std::fs::remove_dir_all(provider.work_root());
        }
        provider
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().expect("youtube state")
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
        Path::new(&self.0.storage).join("youtube-work")
    }

    fn backend(&self) -> Result<Arc<dyn Backend>, YoutubeError> {
        self.0
            .backend
            .clone()
            .ok_or_else(|| YoutubeError::status(503, "YouTube is not available on this device"))
    }

    /// `search({ query, limit, timeoutMs })`: one page of results, `limit` rows at most.
    pub async fn search(&self, payload: &Value) -> Result<Value, YoutubeError> {
        let backend = self.backend()?;
        let query = truncate_utf16(js_trim(&js_string_or_empty(payload.get("query"))), 200);
        if query.is_empty() {
            return Err(YoutubeError::status(400, "query is required"));
        }
        // A number that is missing, zero or not a number stands for the default.
        let or_default = |value: Option<&Value>, default: f64| match value {
            None => default,
            given => Some(js_num(given))
                .filter(|number| *number != 0.0 && !number.is_nan())
                .unwrap_or(default),
        };
        let most = YOUTUBE_SEARCH_CAP as f64;
        let wanted = match payload.get("limit") {
            None => most,
            given => Some(js_num(given).trunc())
                .filter(|number| *number != 0.0 && !number.is_nan())
                .unwrap_or(most),
        };
        let cap = wanted.clamp(1.0, most) as usize;
        let timeout =
            or_default(payload.get("timeoutMs"), self.0.search_timeout_ms).clamp(1000.0, 45_000.0);
        // One row past the cap, to know whether rows were left out.
        let candidates = match backend.info(&format!("ytsearch:{query}"), timeout).await {
            Ok(info) => {
                self.state().last_error = None;
                normalize_results(&info, cap + 1)
            }
            Err(error) => {
                if !error
                    .message
                    .to_ascii_lowercase()
                    .contains("no youtube results")
                {
                    let said = truncate_utf16(&error.message, 200);
                    self.state().last_error = Some(said.clone());
                    self.changed();
                    return Err(YoutubeError::status(
                        error.status.unwrap_or(502),
                        format!("YouTube search failed: {said}"),
                    ));
                }
                self.state().last_error = None;
                Vec::new()
            }
        };
        {
            let mut state = self.state();
            state.last_search_at = Some(self.now());
            state.last_result_count = Some(candidates.len());
            state.empty_streak = if !candidates.is_empty() || utf16_len(&query) <= 3 {
                0
            } else {
                state.empty_streak + 1
            };
        }
        self.changed();
        let truncated = candidates.len() > cap;
        let shown: Vec<Value> = candidates.into_iter().take(cap).collect();
        Ok(json!({ "candidates": shown, "cap": YOUTUBE_SEARCH_CAP, "truncated": truncated }))
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

    /// `mirror({ artist, album, year, tracks, wait })`: saves the tracks' audio as one album.
    /// Artist and album come from the caller: a video title is not album metadata, and guessing
    /// it here would file albums under whatever an uploader typed. With `wait` the answer is the
    /// finished save; without, `{ queued, job }` at once while the save goes on.
    pub async fn mirror(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, YoutubeError> {
        let backend = self.backend()?;
        let named = |value: Option<&Value>| {
            truncate_utf16(&safe_segment(&js_string_or_empty(value), ""), 120)
        };
        let (artist, album) = (named(payload.get("artist")), named(payload.get("album")));
        if artist.is_empty() || album.is_empty() {
            return Err(YoutubeError::status(400, "artist and album are required"));
        }
        // 1800 to 2099, as text or as a number.
        let year = match payload.get("year") {
            None | Some(Value::Null) => String::new(),
            Some(given) => js_string(given),
        };
        let year = Some(js_trim(&year))
            .filter(|text| {
                text.len() == 4
                    && text.bytes().all(|byte| byte.is_ascii_digit())
                    && ["18", "19", "20"]
                        .iter()
                        .any(|start| text.starts_with(start))
            })
            .and_then(|text| text.parse::<u32>().ok());
        let raw = payload.get("tracks").and_then(Value::as_array);
        let raw = raw.map(Vec::as_slice).unwrap_or_default();
        if raw.len() > MAX_TRACKS {
            return Err(YoutubeError::status(
                400,
                format!("An album takes at most {MAX_TRACKS} tracks"),
            ));
        }
        let mut tracks: Vec<Track> = Vec::new();
        for (i, entry) in raw.iter().enumerate() {
            let video_id = js_string_or_empty(entry.get("videoId"));
            if !is_video_id(&video_id) {
                return Err(YoutubeError::status(
                    400,
                    format!("Track {} has no valid video id", i + 1),
                ));
            }
            let title = named(entry.get("title"));
            if title.is_empty() {
                return Err(YoutubeError::status(
                    400,
                    format!("Track {} has no title", i + 1),
                ));
            }
            let given = js_num(entry.get("track")).trunc();
            let track = if (1.0..=999.0).contains(&given) {
                given as u32
            } else {
                i as u32 + 1
            };
            if tracks.iter().any(|other| other.track == track) {
                return Err(YoutubeError::status(
                    400,
                    format!("Two tracks are numbered {track}"),
                ));
            }
            tracks.push(Track {
                video_id,
                title,
                track,
            });
        }
        if tracks.is_empty() {
            return Err(YoutubeError::status(
                400,
                "tracks must list at least one video",
            ));
        }

        let dated = year.map(|year| format!(" ({year})")).unwrap_or_default();
        // Its own folder, never the one a lossless copy of the same album sits in: the pin would
        // re-file those tracks under `yt:`. The name is cut before the marker, so a long one
        // cannot lose it and land on the other folder.
        let folder = truncate_utf16(
            &safe_segment(&format!("{artist} - {album}{dated}"), "Album"),
            128,
        );
        let plan = Plan {
            release: youtube_release(&artist, &album, year),
            album_title: truncate_utf16(&format!("{artist} · {album}"), MAX_TITLE),
            folder_name: format!("{} [YouTube]", js_trim_end(&folder)),
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
                file: file_of(&plan.tracks[0], ".m4a"),
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
        // The save takes its place behind the saves asked for before it here, not when its task
        // first runs. The next save starts when this one's task ends, however it ends.
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

        // The save goes on when the caller stops waiting for it.
        let save = tokio::spawn({
            let (provider, job) = (self.clone(), job.clone());
            async move {
                let _turn_over = turn_over;
                before.await;
                provider.save(&backend, &job, &plan, place_and_scan).await
            }
        });
        if payload.get("wait") == Some(&Value::Bool(true)) {
            return save
                .await
                .unwrap_or_else(|error| Err(YoutubeError::plain(error.to_string())));
        }
        Ok(json!({ "queued": true, "job": job.view(self.now()) }))
    }

    fn stopped(&self, job: &Job) -> bool {
        job.controller.is_aborted() || self.state().closed
    }

    /// One save from its first download to its import; the job follows it.
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
            YoutubeError::status(
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
        let io = |error: std::io::Error| YoutubeError::plain(error.to_string());
        if self.stopped(job) {
            return Err(YoutubeError::cancelled());
        }
        let work_root = self.work_root();
        std::fs::create_dir_all(&work_root).map_err(io)?;
        let work = make_work_dir(&work_root, &*self.0.random).map_err(io)?;
        *work_dir = Some(work.clone());
        // Tracks wait in the work folder until the last one is in: a cancelled save leaves nothing
        // to import. Each is the track, the file `ubit` left and that file's extension.
        let mut got: Vec<(Track, PathBuf, String)> = Vec::new();
        let mut done_bytes = 0u64;
        let mut refused: Option<&'static str> = None;
        for (i, track) in plan.tracks.iter().enumerate() {
            if self.stopped(job) {
                return Err(YoutubeError::cancelled());
            }
            let skip =
                |error: &str| json!({ "track": track.track, "title": track.title, "error": error });
            if let Some(refused) = refused {
                // Every other track would be refused the same way; asking again only deepens the
                // block.
                job.record().skipped.push(skip(refused));
                continue;
            }
            let started = self.now();
            {
                let mut record = job.record();
                record.status = "transferring";
                record.phase = "downloading";
                record.file = file_of(track, ".m4a");
                record.file_index = i + 1;
                record.speed = 0;
                record.progress_at = started;
            }
            self.changed();
            let out_dir = work.join((i + 1).to_string());
            std::fs::create_dir(&out_dir).map_err(io)?;
            // Speed is measured from the bytes received over at least a second, smoothed.
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
            let url = format!("https://www.youtube.com/watch?v={}", track.video_id);
            let sent = backend
                .download(&url, &out_dir, job.controller.clone(), on_progress)
                .await
                .map_err(|error| error.message)
                .and_then(|path| {
                    let name = path
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    let ext = split_ext(&name).1.to_lowercase();
                    if !is_media_file(&format!("x{ext}")) {
                        let said = if ext.is_empty() { "no extension" } else { &ext };
                        return Err(format!(
                            "YouTube sent a file this relay does not import ({said})"
                        ));
                    }
                    let size = std::fs::metadata(&path)
                        .map_err(|error| error.to_string())?
                        .len();
                    if size == 0 {
                        return Err("YouTube sent an empty file".to_owned());
                    }
                    Ok((path, ext, size))
                });
            match sent {
                Ok((path, ext, size)) => {
                    got.push((track.clone(), path, ext));
                    done_bytes += size;
                    job.record().bytes = done_bytes;
                    self.state().saves_blocked = false;
                }
                Err(message) => {
                    if self.stopped(job) {
                        return Err(YoutubeError::cancelled());
                    }
                    let reason = truncate_utf16(&message, 200);
                    if is_bot_check(&reason) {
                        self.state().saves_blocked = true;
                        refused = Some("YouTube is refusing downloads from this network");
                    }
                    // One video YouTube will not hand over (removed, age-gated, blocked here) does
                    // not lose the album.
                    job.record().skipped.push(skip(refused.unwrap_or(&reason)));
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
            return Err(if refused.is_some() {
                YoutubeError::status(
                    503,
                    "YouTube is refusing downloads from this relay's network (its bot check). Search still works; saving needs another network.",
                )
            } else if count == 1 {
                YoutubeError::status(502, format!("YouTube did not send “{title}”: {error}"))
            } else {
                YoutubeError::status(
                    502,
                    format!("YouTube sent none of the {count} tracks. First reason: {error}"),
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
        // Runs only once no other import holds the folder (`place_and_scan`), in one step with
        // this album's own import: until then nothing is in the import folder, so a save that is
        // cancelled or gives up while it waits leaves nothing for a later scan to add.
        let place: Place = Box::new({
            let (provider, job, placed) = (self.clone(), job.clone(), placed.clone());
            let (release, title, artist) = (
                plan.release.clone(),
                plan.album_title.clone(),
                plan.artist.clone(),
            );
            let (folder_name, got) = (folder_name.clone(), got.clone());
            move || {
                let io = |error: std::io::Error| PlaceError {
                    message: error.to_string(),
                    status: None,
                };
                let pin_path = final_dir.join(ALBUM_PIN);
                let pinned = std::fs::read_to_string(&pin_path)
                    .ok()
                    .and_then(|text| {
                        serde_json::from_str::<Value>(&crate::ipc::well_formed(&text)).ok()
                    })
                    .and_then(|pin| pin.get("release").cloned())
                    .filter(js_truthy);
                // Only a folder this album's own pin already names, or an empty one, is taken: a
                // pin written into a folder of the owner's files would re-file them under `yt:`.
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
                    // Across file systems a rename fails: copy.
                    if std::fs::rename(path, &dest).is_err() {
                        std::fs::copy(path, &dest).map_err(io)?;
                    }
                    let name = dest
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
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
        // `waiting` (for another import to finish; Cancel still works) then `importing` (this
        // album's own files).
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
            // A bare provider has no host to import for it: the files are only placed.
            None => place().map(|()| json!({ "scanned": 0, "ingested": 0, "items": [] })),
        }
        .map_err(|error| YoutubeError {
            message: error.message,
            status: error.status,
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
                .map(|(track, _, ext)| (track, file_of(track, ext)))
                .find(|(_, name)| name == file)
                .map(|(track, _)| track);
            let error = truncate_utf16(&js_string(&row["error"]), 160);
            job.record().skipped.push(json!({
                "track": track.map(|track| track.track),
                "title": track.map_or(file, |track| track.title.as_str()),
                "error": format!("Not imported: {error}"),
            }));
        }
        let imported = placed.len() - refused_import.len();
        if imported == 0 {
            let error = refused_import
                .first()
                .map(|row| js_string(&row["error"]))
                .unwrap_or_default();
            return Err(YoutubeError::status(
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
        let ingested = scan.get("ingested").filter(|count| js_truthy(count));
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

    fn job(&self, job_id: &str) -> Option<Arc<Job>> {
        self.state()
            .jobs
            .iter()
            .find(|job| job.id == job_id)
            .cloned()
    }

    pub fn list_jobs(&self) -> Vec<Value> {
        let jobs = self.state().jobs.clone();
        let now = self.now();
        jobs.iter()
            .map(|job| Value::Object(job.view(now)))
            .collect()
    }

    pub fn get_job(&self, job_id: &str) -> Option<Value> {
        let job = self.job(job_id)?;
        Some(Value::Object(job.view(self.now())))
    }

    /// Cancels a save that waits or downloads, or that waits for another import with nothing
    /// placed yet; one that is over is removed from the list instead (`status: "removed"`), and
    /// one whose own import has started is left alone.
    pub fn cancel_job(&self, job_id: &str) -> Option<Value> {
        let job = self.job(job_id)?;
        let status = {
            let mut record = job.record();
            if matches!(record.status, "queued" | "transferring")
                || (record.status == "announcing" && record.phase == "waiting")
            {
                record.status = "cancelled";
                record.phase = "cancelled";
                None
            } else {
                Some(record.status)
            }
        };
        match status {
            None => {
                job.controller.abort();
                self.changed();
                Some(Value::Object(job.view(self.now())))
            }
            Some("announcing") => Some(Value::Object(job.view(self.now()))),
            Some(_) => {
                self.state().jobs.retain(|other| other.id != job_id);
                self.changed();
                let mut view = job.view(self.now());
                view.insert("status".into(), json!("removed"));
                Some(Value::Object(view))
            }
        }
    }

    /// `info()`: what `/v1/plugins` shows for the plugin.
    pub fn info(&self) -> Value {
        let available = self.0.backend.is_some();
        let state = self.state();
        let degraded = state.saves_blocked || state.empty_streak >= EMPTY_STREAK;
        let active = state
            .jobs
            .iter()
            .filter(|job| matches!(job.status(), "queued" | "transferring" | "announcing"))
            .count();
        let message = if !available {
            "YouTube search needs the relay.".to_owned()
        } else if state.saves_blocked {
            "YouTube is refusing downloads from this network. Search still works.".to_owned()
        } else if degraded {
            format!(
                "YouTube search returned nothing {} times; it may be out of date.",
                state.empty_streak
            )
        } else {
            state.last_error.clone().unwrap_or_default()
        };
        json!({
            "status": if !available { "unavailable" } else if degraded { "degraded" } else { "ready" },
            "message": message,
            // false = a save was refused by YouTube's bot check on this network and none has
            // worked since.
            "canSave": available && !state.saves_blocked,
            "configured": available,
            "available": available,
            "activeJobs": active,
            "searchCap": YOUTUBE_SEARCH_CAP,
            "bitrate": YOUTUBE_BITRATE,
            "staging": self.0.staging_label,
            "lastSearchAt": state.last_search_at,
            "lastResultCount": state.last_result_count,
            "emptyStreak": state.empty_streak,
        })
    }

    /// A result's picture, fetched by the relay so the owner's browser never contacts YouTube's
    /// image host. A few are kept in memory; `None` when there is none.
    pub async fn thumbnail(&self, video_id: &str) -> Option<Picture> {
        let fetch = self.0.fetch.clone()?;
        if !is_video_id(video_id) {
            return None;
        }
        let pending = {
            let mut state = self.state();
            let kept = state.thumbs.iter().find(|(id, _, _)| id == video_id);
            match kept {
                Some((_, _, pending)) => pending.clone(),
                None => {
                    state.thumb_marks += 1;
                    let mark = state.thumb_marks;
                    let request =
                        fetch.fetch(&format!("https://i.ytimg.com/vi/{video_id}/mqdefault.jpg"));
                    let (inner, id) = (Arc::downgrade(&self.0), video_id.to_owned());
                    let pending = async move {
                        match request.await {
                            Ok(reply) => picture_of(reply),
                            // A failed request is not "no picture": forget it, so the next look
                            // asks again.
                            Err(_) => {
                                forget_thumb(&inner, &id, mark);
                                None
                            }
                        }
                    }
                    .boxed()
                    .shared();
                    state
                        .thumbs
                        .push((video_id.to_owned(), mark, pending.clone()));
                    while state.thumbs.len() > THUMBS_KEPT {
                        let _oldest = state.thumbs.remove(0);
                    }
                    pending
                }
            }
        };
        pending.await
    }

    /// Ends the provider: every save that has placed nothing yet is cancelled, and the answer
    /// comes when the last of them has ended.
    pub async fn stop(&self) {
        let chain = {
            let mut state = self.state();
            state.closed = true;
            for job in &state.jobs {
                let mut record = job.record();
                // A save waiting for another import has placed nothing: closing ends it as a
                // cancel, as Cancel would.
                let waiting = record.status == "announcing" && record.phase == "waiting";
                if matches!(record.status, "queued" | "transferring") || waiting {
                    job.controller.abort();
                    if waiting {
                        record.status = "cancelled";
                        record.phase = "cancelled";
                    }
                }
            }
            state.chain.clone()
        };
        // Each job above ends at its next step (a killed download, the wait's next check), so
        // this is short; it keeps a job from writing after the host has closed.
        chain.await;
    }

    /// The plugin's `run`: a save or a search by what the payload holds, else `info`.
    pub async fn run(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, YoutubeError> {
        if self.0.backend.is_none() {
            return Ok(json!({ "skipped": true, "status": "unavailable" }));
        }
        if payload.get("tracks").is_some_and(Value::is_array) {
            return self.mirror(payload, place_and_scan).await;
        }
        if payload.get("query").is_some_and(Value::is_string) {
            return self.search(payload).await;
        }
        let mut out = Map::new();
        out.insert("ok".into(), json!(true));
        if let Value::Object(info) = self.info() {
            out.extend(info);
        }
        Ok(Value::Object(out))
    }
}

/// A reply as a picture: only a whole answer that says it is a JPEG, WebP or PNG.
fn picture_of(reply: Fetched) -> Option<Picture> {
    let content_type = reply.content_type?;
    let image = matches!(
        content_type.as_str(),
        "image/jpeg" | "image/webp" | "image/png"
    );
    (reply.status == 200 && !reply.body.is_empty() && image).then(|| Picture {
        content_type,
        bytes: reply.body.into(),
    })
}

/// Drops the request marked `mark` for `video_id`, when it is still the one that is kept.
fn forget_thumb(inner: &Weak<Inner>, video_id: &str, mark: u64) {
    if let Some(inner) = inner.upgrade() {
        inner
            .state
            .lock()
            .expect("youtube state")
            .thumbs
            .retain(|(id, kept, _)| !(id == video_id && *kept == mark));
    }
}

/// A new folder `job-<random>` under `root` that only this user can read.
fn make_work_dir(root: &Path, random: &dyn RandomSource) -> std::io::Result<PathBuf> {
    let mut name = String::from("job-");
    for byte in random.bytes(3) {
        name.push_str(&format!("{byte:02x}"));
    }
    let dir = root.join(name);
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&dir)?;
    Ok(dir)
}
