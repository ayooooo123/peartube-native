//! Mirror plugin host for PearTube relays, ported from `src/mirror.js`.
//!
//! Built-in plugins: `http-acquire` (the durable acquirer as a plugin), `channel-backfill`, `blob-rechunk`,
//! `watch-dir` and `feed-mirror`. `p2p-replicate` and `hiverelay-seed` are not built here: replication needs
//! block-range downloads off a remote core, which `node::Node` deliberately does not expose, and the HiveRelay
//! seeder owns its own module. Both register through `MirrorHost::register` exactly as the app's plugins do.
//!
//! The filesystem and remote-feed fetches sit behind small traits so a test can script them.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

use crate::acquire::{AcquireError, Acquirer, AddJob, JobSource, PutStreamFactory, SourceSpec};
use crate::art;
use crate::node::Node;
use crate::node::api::{PutMeta, SearchOptions};
use crate::protocol::{MAX_TITLE, is_hex64, is_id, is_media_file};

/// What a mirror plugin run may ask its host for, as `ctxFor(name)` in the app.
pub struct Ctx<N: Node> {
    pub node: Arc<N>,
    pub storage: String,
    pub acquirer: Option<Acquirer<N>>,
    pub mirror_channel: bool,
    files: Arc<dyn MirrorFiles>,
    factory: Arc<dyn PutStreamFactory<N>>,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl<N: Node + 'static> Ctx<N>
where
    N::Error: From<AcquireError>,
{
    /// `has`: an active or done acquire job counts, otherwise the catalog must hold a local (or sha-matched) entry.
    pub async fn has(&self, id: Option<&str>, sha256: Option<&str>) -> Result<bool, MirrorError> {
        if let Some(acquirer) = &self.acquirer {
            let active = acquirer.list().iter().any(|job| {
                let status = job
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let status = matches!(status, "queued" | "running" | "announcing" | "done");
                let same_id =
                    id.is_some_and(|id| job.get("id").and_then(Value::as_str) == Some(id));
                let same_sha = sha256
                    .is_some_and(|sha| job.get("sha256").and_then(Value::as_str) == Some(sha));
                status && (same_id || same_sha)
            });
            if active {
                return Ok(true);
            }
        }
        let entries = self
            .node
            .search(id, SearchOptions::default())
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        Ok(entries.iter().any(|entry| {
            id.is_none_or(|id| entry.id == id)
                && sha256.is_none_or(|sha| entry.sha256 == sha)
                && (entry.local || sha256.is_none())
        }))
    }

    /// `enqueue`: hand a job to the acquirer, tagged with the plugin that asked for it.
    pub fn enqueue(&self, spec: Enqueue) -> Result<Value, MirrorError> {
        let acquirer = self
            .acquirer
            .as_ref()
            .ok_or_else(|| MirrorError::plain("HTTP acquirer unavailable on this peer"))?;
        let job = acquirer
            .add(AddJob {
                id: spec.id,
                title: spec.title,
                source: spec.source,
                channel: Some(spec.channel),
                private: false,
                plugin: Some(spec.plugin),
            })
            .map_err(|error| MirrorError::plain(error.message()))?;
        self.changed();
        Ok(job)
    }

    /// `ingestStream`: `put`, then `append`, then tell listeners.
    pub async fn ingest_stream(
        &self,
        id: &str,
        title: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        private: bool,
    ) -> Result<Value, MirrorError> {
        let meta = PutMeta {
            id: id.to_owned(),
            title: title.to_owned(),
            private_entry: Some(private),
            epoch: None,
            store: None,
            expected_size: None,
            extra: Map::new(),
        };
        let op = self
            .node
            .put(meta, self.factory.reader(JobSource::stream(reader)))
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        let op = serde_json::to_value(op).map_err(|error| MirrorError::plain(error.to_string()))?;
        self.node
            .append(op.clone())
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        self.changed();
        Ok(op)
    }

    /// `repost`: announce a catalog entry this relay knows, optionally pinning and/or writing it to the tracker.
    pub async fn repost(
        &self,
        entry: &Value,
        pin: bool,
        channel: bool,
        tracker: bool,
    ) -> Result<Value, MirrorError> {
        let mut op = Map::new();
        op.insert("type".into(), json!("announce"));
        for key in ["id", "title", "size", "sha256", "blobs", "blob"] {
            if let Some(value) = entry.get(key) {
                op.insert(key.to_owned(), value.clone());
            }
        }
        if let Some(epoch) = entry.get("epoch").filter(|value| !value.is_null()) {
            op.insert("epoch".into(), epoch.clone());
        }
        if let Some(wrapped) = entry.get("wrappedKey").filter(|value| !value.is_null()) {
            op.insert("wrappedKey".into(), wrapped.clone());
        }
        let op = Value::Object(op);
        if tracker || channel {
            self.node
                .append(op.clone())
                .await
                .map_err(|error| MirrorError::plain(error.to_string()))?;
        }
        if pin {
            self.node
                .warm(&op)
                .await
                .map_err(|error| MirrorError::plain(error.to_string()))?;
        }
        self.changed();
        Ok(op)
    }

    /// `publishDirectory`: add a source to this relay's channel.
    pub async fn publish_directory(
        &self,
        source_key: &str,
        title: &str,
    ) -> Result<Value, MirrorError> {
        let published = self
            .node
            .publish_source(source_key, title)
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        self.changed();
        Ok(published)
    }

    pub fn files(&self) -> &Arc<dyn MirrorFiles> {
        &self.files
    }

    fn changed(&self) {
        if let Some(onchange) = &self.onchange {
            onchange();
        }
    }
}

/// What `enqueue` takes.
pub struct Enqueue {
    pub id: String,
    pub title: String,
    pub source: SourceSpec,
    pub channel: bool,
    pub plugin: String,
}

/// A plugin failure; `status` is the HTTP status `run` reports (404 for an unknown plugin).
#[derive(Debug, Clone, PartialEq)]
pub struct MirrorError {
    pub message: String,
    pub status: Option<u16>,
}

impl MirrorError {
    pub fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
        }
    }

    pub fn with_status(message: impl Into<String>, status: u16) -> Self {
        Self {
            message: message.into(),
            status: Some(status),
        }
    }
}

impl std::fmt::Display for MirrorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for MirrorError {}

/// One registered plugin. The app's plugins are plain objects; this is the same shape with types.
pub trait MirrorPlugin<N: Node>: Send + Sync {
    fn name(&self) -> &str;
    fn description(&self) -> &str {
        ""
    }
    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>>;
    fn poll<'a>(
        &'a self,
        _ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
    fn stop<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async {})
    }
    fn info(&self) -> Value {
        Value::Object(Map::new())
    }
}

/// One directory entry, as `statSync` reports it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileStat {
    pub is_file: bool,
    pub size: u64,
    pub mtime_ms: f64,
}

/// The files a mirror host touches.
pub trait MirrorFiles: Send + Sync {
    fn exists(&self, path: &str) -> bool;
    fn mkdirs(&self, path: &str);
    /// Names inside `dir`, unsorted; callers sort as the app does.
    fn list_dir(&self, dir: &str) -> Vec<String>;
    fn stat(&self, path: &str) -> Option<FileStat>;
    fn read(&self, path: &str) -> Option<String>;
    fn write_atomic(&self, path: &str, text: &str) -> Result<(), String>;
    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String>;
}

/// The real filesystem.
pub struct LocalMirrorFiles;

impl MirrorFiles for LocalMirrorFiles {
    fn exists(&self, path: &str) -> bool {
        Path::new(path).exists()
    }

    fn mkdirs(&self, path: &str) {
        let _ = std::fs::create_dir_all(path);
    }

    fn list_dir(&self, dir: &str) -> Vec<String> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    fn stat(&self, path: &str) -> Option<FileStat> {
        let meta = std::fs::metadata(path).ok()?;
        let mtime_ms = meta
            .modified()
            .ok()
            .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|elapsed| elapsed.as_secs_f64() * 1000.0)
            .unwrap_or(0.0);
        Some(FileStat {
            is_file: meta.is_file(),
            size: meta.len(),
            mtime_ms,
        })
    }

    fn read(&self, path: &str) -> Option<String> {
        std::fs::read_to_string(path).ok()
    }

    fn write_atomic(&self, path: &str, text: &str) -> Result<(), String> {
        use std::io::Write;
        let tmp = format!("{path}.tmp");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut handle = options.open(&tmp).map_err(|error| error.to_string())?;
        handle
            .write_all(text.as_bytes())
            .map_err(|error| error.to_string())?;
        std::fs::rename(&tmp, path).map_err(|error| error.to_string())
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String> {
        let file = std::fs::File::open(path).map_err(|error| error.to_string())?;
        Ok(Box::new(tokio::fs::File::from_std(file)))
    }
}

/// `send` as the feed mirror uses it: one buffered reply for a feed URL.
pub trait FeedFetch: Send + Sync {
    fn get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> Pin<Box<dyn Future<Output = Result<art::HttpResponse, art::HttpError>> + Send>>;
}

/// The real feed fetcher: 60 s, 8 MiB, JSON accept.
pub struct ArtFeed;

impl FeedFetch for ArtFeed {
    fn get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> Pin<Box<dyn Future<Output = Result<art::HttpResponse, art::HttpError>> + Send>> {
        let url = url.to_owned();
        Box::pin(async move {
            let mut all = vec![("accept".to_owned(), "application/json".to_owned())];
            all.extend(headers);
            art::send(
                &url,
                art::SendOptions {
                    headers: all,
                    timeout: Duration::from_millis(60_000),
                    max_bytes: Some(8 * 1024 * 1024),
                    ..art::SendOptions::default()
                },
            )
            .await
        })
    }
}

/// `fetchJson`: a 2xx JSON reply, or an error naming the feed by host and path (never its query or credentials).
pub async fn fetch_json(
    feed: &dyn FeedFetch,
    url: &str,
    headers: Vec<(String, String)>,
) -> Result<Value, MirrorError> {
    let response = feed
        .get(url, headers)
        .await
        .map_err(|error| MirrorError::plain(error.message().to_owned()))?;
    if !(200..300).contains(&response.status) {
        let named = url::Url::parse(&response.url)
            .map(|parsed| format!("{}{}", parsed.host_str().unwrap_or_default(), parsed.path()))
            .unwrap_or_else(|_| response.url.clone());
        return Err(MirrorError::plain(format!(
            "Feed {named} answered {}",
            response.status
        )));
    }
    serde_json::from_str(&response.body)
        .map_err(|error| MirrorError::plain(format!("Feed JSON: {error}")))
}

/// Truncate to PearTube's `MAX_TITLE` UTF-16 units, as JS `slice` does.
fn title_slice(text: &str) -> String {
    let mut units = 0;
    let mut out = String::new();
    for ch in text.chars() {
        let width = ch.len_utf16();
        if units + width > MAX_TITLE {
            break;
        }
        units += width;
        out.push(ch);
    }
    out
}

fn sha256_hex(text: &str) -> String {
    let digest = Sha256::digest(text.as_bytes());
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// `path.basename(path, path.extname(path))`.
fn stem(file_path: &str) -> String {
    let name = Path::new(file_path)
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    match name.rfind('.') {
        Some(index) if index > 0 => name[..index].to_owned(),
        _ => name,
    }
}

fn ascii_word(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// First `tt` + 6..=10 digits at a word boundary, as the lowercased match text.
fn find_imdb(stem: &str) -> Option<String> {
    let bytes = stem.as_bytes();
    let lower = stem.to_ascii_lowercase();
    let bytes_lower = lower.as_bytes();
    for index in 0..bytes.len() {
        if index > 0 && ascii_word(bytes[index - 1]) {
            continue;
        }
        if bytes_lower.get(index) != Some(&b't') || bytes_lower.get(index + 1) != Some(&b't') {
            continue;
        }
        let mut end = index + 2;
        while end < bytes_lower.len() && bytes_lower[end].is_ascii_digit() {
            end += 1;
        }
        if (6..=10).contains(&(end - index - 2)) {
            return Some(lower[index..end].to_owned());
        }
    }
    None
}

/// First `sNN(eNN)` at a word boundary, as `(NN, NN)`.
fn find_episode(stem: &str) -> Option<(String, String)> {
    let bytes = stem.as_bytes();
    let lower = stem.to_ascii_lowercase();
    let bytes_lower = lower.as_bytes();
    for index in 0..bytes.len() {
        if index > 0 && ascii_word(bytes[index - 1]) {
            continue;
        }
        if bytes_lower.get(index) != Some(&b's') {
            continue;
        }
        let Some(season) = bytes_lower.get(index + 1..index + 3) else {
            continue;
        };
        if !season.iter().all(u8::is_ascii_digit) || bytes_lower.get(index + 3) != Some(&b'e') {
            continue;
        }
        let mut end = index + 4;
        while end < bytes_lower.len() && bytes_lower[end].is_ascii_digit() {
            end += 1;
        }
        if !(2..=3).contains(&(end - index - 4)) {
            continue;
        }
        return Some((
            lower[index + 1..index + 3].to_owned(),
            lower[index + 4..end].to_owned(),
        ));
    }
    None
}

/// `inferMediaMeta`: the ID a file name (or its sidecar) names.
pub fn infer_media_meta(file_path: &str, sidecar: Option<&Value>) -> (String, String) {
    let stem = stem(file_path);
    if let Some(id) = sidecar
        .and_then(|sidecar| sidecar.get("id"))
        .and_then(Value::as_str)
    {
        let id = id.to_lowercase();
        if is_id(&id) {
            let title = sidecar
                .and_then(|sidecar| sidecar.get("title"))
                .map(|value| match value {
                    Value::String(text) => text.clone(),
                    other => other.to_string(),
                })
                .filter(|title| !title.is_empty())
                .unwrap_or_else(|| stem.clone());
            return (id, title_slice(&title));
        }
    }
    if let Some(imdb) = find_imdb(&stem) {
        let episode = find_episode(&stem);
        let clean = |with_episode: bool| {
            let mut text = remove_pattern(&stem, &imdb);
            if with_episode && let Some((season, episode)) = &episode {
                text = remove_pattern(&text, &format!("s{season}e{episode}"));
            }
            let text = text
                .chars()
                .map(|ch| {
                    if matches!(ch, '.' | '_' | '-') {
                        ' '
                    } else {
                        ch
                    }
                })
                .collect::<String>();
            let text = collapse_spaces(&text);
            if text.is_empty() { stem.clone() } else { text }
        };
        if let Some((season, episode)) = &episode {
            return (format!("imdb:{imdb}:s{season}e{episode}"), clean(true));
        }
        return (format!("imdb:{imdb}"), clean(false));
    }
    let slug: String = stem
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    let suffix = if slug.len() >= 4 {
        slug.chars().take(24).collect()
    } else {
        sha256_hex(&stem)[..12].to_owned()
    };
    let title = collapse_spaces(
        &stem
            .chars()
            .map(|ch| if matches!(ch, '.' | '_') { ' ' } else { ch })
            .collect::<String>(),
    );
    let title = if title.is_empty() { stem } else { title };
    (format!("clip:{suffix}"), title)
}

/// Remove every case-insensitive occurrence of `needle`.
fn remove_pattern(text: &str, needle: &str) -> String {
    let lower = text.to_lowercase();
    let needle_lower = needle.to_lowercase();
    let mut out = String::with_capacity(text.len());
    let mut cursor = 0;
    while let Some(found) = lower[cursor..].find(&needle_lower) {
        let start = cursor + found;
        out.push_str(&text[cursor..start]);
        cursor = start + needle_lower.len();
    }
    out.push_str(&text[cursor..]);
    out
}

fn collapse_spaces(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut space = false;
    for ch in text.trim().chars() {
        if ch == ' ' {
            space = true;
        } else {
            if space && !out.is_empty() {
                out.push(' ');
            }
            space = false;
            out.push(ch);
        }
    }
    out
}

/// A per-entry migration callback (`onMigrate(oldTag, target)`).
pub type MigrationCallback = Box<dyn Fn(&str, Value) + Send + Sync>;

/// How the backend turns `rechunkLegacyBlobs`' options (with their callbacks) into its own type.
pub struct RechunkOptions {
    pub id: Option<String>,
    pub foreground: bool,
    /// `None` is the app's `Infinity`.
    pub limit: Option<u64>,
    pub on_migrate: MigrationCallback,
    pub on_progress: Box<dyn Fn(Value) + Send + Sync>,
}

/// Builds the backend's opaque rechunk options.
pub trait RechunkOptionsFactory<N: Node>: Send + Sync {
    fn make(&self, options: RechunkOptions) -> N::RechunkOptions;
}

/// `createMirrorHost`'s options.
pub struct MirrorOptions {
    pub storage: String,
    pub mirror_channel: bool,
    pub rechunk: bool,
    pub watch_dir: Option<String>,
    pub feeds: Vec<String>,
    pub plugins_dir: Option<String>,
    pub poll_interval_ms: u64,
    pub onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    pub onstarting: Option<Arc<dyn Fn() + Send + Sync>>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Default for MirrorOptions {
    fn default() -> Self {
        Self {
            storage: String::new(),
            mirror_channel: true,
            rechunk: false,
            watch_dir: None,
            feeds: Vec::new(),
            plugins_dir: None,
            poll_interval_ms: 60_000,
            onchange: None,
            onstarting: None,
            now_ms: Arc::new(system_now_ms),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct Stats {
    runs: u64,
    last_run_at: Option<u64>,
    last_result: Option<Value>,
    last_error: Option<String>,
}

impl Stats {
    fn to_json(&self) -> Value {
        json!({
            "runs": self.runs,
            "lastRunAt": self.last_run_at,
            "lastResult": self.last_result.clone().unwrap_or(Value::Null),
            "lastError": self.last_error.clone().map_or(Value::Null, Value::String),
        })
    }
}

/// Shared state the `watch-dir` plugin keeps between runs.
pub struct WatchState {
    ledger: Mutex<Map<String, Value>>,
    state_file: String,
    scanning: AtomicBool,
    watch_dir: Mutex<Option<String>>,
    closed: Arc<AtomicBool>,
    files: Arc<dyn MirrorFiles>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl WatchState {
    fn save(&self) {
        let text = {
            let ledger = self.ledger.lock().expect("mirror ledger");
            Value::Object(ledger.clone()).to_string()
        };
        let _ = self.files.write_atomic(&self.state_file, &text);
    }

    async fn scan<N>(&self, dir: Option<&str>, ctx: &Ctx<N>) -> Result<Value, MirrorError>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let Some(dir) = dir else {
            return Ok(empty_scan());
        };
        if !self.files.exists(dir) {
            return Ok(empty_scan());
        }
        if self.scanning.swap(true, Ordering::SeqCst) {
            let mut out = empty_scan();
            if let Some(map) = out.as_object_mut() {
                map.insert("busy".into(), json!(true));
            }
            return Ok(out);
        }
        let result = self.scan_inner(dir, ctx).await;
        self.scanning.store(false, Ordering::SeqCst);
        result
    }

    async fn scan_inner<N>(&self, dir: &str, ctx: &Ctx<N>) -> Result<Value, MirrorError>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let root = std::path::absolute(dir).unwrap_or_else(|_| std::path::PathBuf::from(dir));
        let mut names = self.files.list_dir(&root.to_string_lossy());
        names.sort();
        let mut scanned = 0u64;
        let mut ingested = Vec::new();
        for name in names {
            if self.closed.load(Ordering::SeqCst) {
                break;
            }
            if !is_media_file(&name) {
                continue;
            }
            let full = root.join(&name).to_string_lossy().into_owned();
            let Some(stat) = self.files.stat(&full) else {
                continue;
            };
            if !stat.is_file || stat.size == 0 {
                continue;
            }
            scanned += 1;
            let mtime = stat.mtime_ms.trunc() as i64;
            let previous = self
                .ledger
                .lock()
                .expect("mirror ledger")
                .get(&name)
                .cloned();
            if let Some(previous) = &previous
                && previous.get("size").and_then(Value::as_u64) == Some(stat.size)
                && previous.get("mtimeMs").and_then(Value::as_f64) == Some(mtime as f64)
            {
                continue;
            }
            let sidecar_path = format!("{full}.json");
            let sidecar = if self.files.exists(&sidecar_path) {
                self.files
                    .read(&sidecar_path)
                    .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            } else {
                None
            };
            let (id, title) = infer_media_meta(&full, sidecar.as_ref());
            let reader = self.files.open_read(&full).map_err(MirrorError::plain)?;
            let op = ctx.ingest_stream(&id, &title, reader, false).await?;
            let mut entry = Map::new();
            entry.insert("size".into(), json!(stat.size));
            entry.insert("mtimeMs".into(), json!(mtime));
            entry.insert("id".into(), op.get("id").cloned().unwrap_or(Value::Null));
            entry.insert(
                "sha256".into(),
                op.get("sha256").cloned().unwrap_or(Value::Null),
            );
            entry.insert("ingestedAt".into(), json!((self.now_ms)()));
            self.ledger
                .lock()
                .expect("mirror ledger")
                .insert(name.clone(), Value::Object(entry));
            self.save();
            ingested.push(json!({
                "file": name,
                "id": op.get("id").cloned().unwrap_or(Value::Null),
                "title": op.get("title").cloned().unwrap_or(Value::Null),
                "size": op.get("size").cloned().unwrap_or(Value::Null),
                "sha256": op.get("sha256").cloned().unwrap_or(Value::Null),
            }));
        }
        Ok(json!({
            "scanned": scanned,
            "ingested": ingested.len(),
            "items": ingested,
        }))
    }
}

fn empty_scan() -> Value {
    json!({"scanned": 0, "ingested": 0, "items": []})
}

struct HttpAcquirePlugin<N: Node> {
    acquirer: Option<Acquirer<N>>,
}

impl<N: Node + 'static> MirrorPlugin<N> for HttpAcquirePlugin<N>
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "http-acquire"
    }

    fn description(&self) -> &str {
        "Durable HTTP/HTTPS stream acquirer backed by jobs.json"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let id = payload
                .get("id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let title = payload
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let channel = payload
                .get("channel")
                .and_then(Value::as_bool)
                .unwrap_or(ctx.mirror_channel);
            let dedupe = payload
                .get("dedupe")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if dedupe && !id.is_empty() && ctx.has(Some(&id), None).await? {
                return Ok(json!({"skipped": true, "id": id, "reason": "already_present"}));
            }
            let source = payload.get("source").cloned().unwrap_or(Value::Null);
            let Some(url) = source.get("url").and_then(Value::as_str) else {
                return Err(MirrorError::plain("source.url must be http(s)"));
            };
            let headers = source
                .get("headers")
                .and_then(Value::as_object)
                .map(|headers| {
                    headers
                        .iter()
                        .map(|(key, value)| {
                            (key.clone(), value.as_str().unwrap_or_default().to_owned())
                        })
                        .collect()
                })
                .unwrap_or_default();
            let job = ctx.enqueue(Enqueue {
                id,
                title,
                source: SourceSpec {
                    url: url.to_owned(),
                    headers,
                    page: source.get("page").and_then(Value::as_bool).unwrap_or(false),
                    private: source
                        .get("private")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                },
                channel,
                plugin: self.name().to_owned(),
            })?;
            Ok(json!({"queued": true, "job": job}))
        })
    }

    fn info(&self) -> Value {
        let list = self
            .acquirer
            .as_ref()
            .map(Acquirer::list)
            .unwrap_or_default();
        let count = |wanted: &[&str]| {
            list.iter()
                .filter(|job| {
                    job.get("status")
                        .and_then(Value::as_str)
                        .is_some_and(|status| wanted.contains(&status))
                })
                .count()
        };
        json!({
            "totalJobs": list.len(),
            "activeJobs": count(&["queued", "running", "announcing"]),
            "doneJobs": count(&["done"]),
        })
    }
}

struct ChannelBackfillPlugin;

impl<N: Node + 'static> MirrorPlugin<N> for ChannelBackfillPlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "channel-backfill"
    }

    fn description(&self) -> &str {
        "Backfills previously saved local tracker blobs into this relay's single-writer channel with zero byte duplication"
    }

    fn run<'a>(
        &'a self,
        _payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let result = ctx
                .node
                .sync_local_channel()
                .await
                .map_err(|error| MirrorError::plain(error.to_string()))?;
            Ok(serde_json::to_value(result).unwrap_or(Value::Null))
        })
    }
}

struct BlobRechunkPlugin<N: Node> {
    acquirer: Option<Acquirer<N>>,
    progress: Arc<Mutex<Value>>,
    factory: Arc<dyn RechunkOptionsFactory<N>>,
}

impl<N: Node + 'static> MirrorPlugin<N> for BlobRechunkPlugin<N>
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "blob-rechunk"
    }

    fn description(&self) -> &str {
        "Re-chunks legacy variable-block local blobs into uniform 512 KiB blocks with SHA-256 verification and space reclamation"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            if self
                .progress
                .lock()
                .expect("rechunk progress")
                .get("running")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                let mut progress = self.progress.lock().expect("rechunk progress").clone();
                if let Some(map) = progress.as_object_mut() {
                    map.insert("busy".into(), json!(true));
                }
                return Ok(progress);
            }
            if let Some(map) = self
                .progress
                .lock()
                .expect("rechunk progress")
                .as_object_mut()
            {
                map.insert("running".into(), json!(true));
            }
            let id = payload
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .map(str::to_owned);
            let foreground = payload
                .get("foreground")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let limit = payload
                .get("limit")
                .and_then(Value::as_f64)
                .filter(|limit| limit.is_finite() && *limit > 0.0)
                .map(|limit| limit as u64);
            let acquirer = self.acquirer.clone();
            let progress = self.progress.clone();
            let options = self.factory.make(RechunkOptions {
                id,
                foreground,
                limit,
                on_migrate: Box::new(move |old_tag, target| {
                    if let Some(acquirer) = &acquirer {
                        acquirer.repoint_blob(old_tag, &target);
                    }
                }),
                on_progress: Box::new(move |update| {
                    let mut map = Map::new();
                    map.insert("running".into(), json!(true));
                    if let Some(update) = update.as_object() {
                        for (key, value) in update {
                            if key != "running" {
                                map.insert(key.clone(), value.clone());
                            }
                        }
                    }
                    *progress.lock().expect("rechunk progress") = Value::Object(map);
                }),
            });
            let result = ctx.node.rechunk_legacy_blobs(options).await;
            if let Some(map) = self
                .progress
                .lock()
                .expect("rechunk progress")
                .as_object_mut()
            {
                map.insert("running".into(), json!(false));
            }
            let result = result.map_err(|error| MirrorError::plain(error.to_string()))?;
            let value = serde_json::to_value(result).unwrap_or(Value::Null);
            let mut done = value.as_object().cloned().unwrap_or_default();
            done.insert("running".into(), json!(false));
            done.insert("currentId".into(), Value::Null);
            done.insert("currentTitle".into(), Value::Null);
            *self.progress.lock().expect("rechunk progress") = Value::Object(done);
            Ok(value)
        })
    }

    fn info(&self) -> Value {
        self.progress.lock().expect("rechunk progress").clone()
    }
}

struct WatchDirPlugin {
    state: Arc<WatchState>,
}

impl<N: Node + 'static> MirrorPlugin<N> for WatchDirPlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "watch-dir"
    }

    fn description(&self) -> &str {
        "Ingests media files dropped into a local folder (PEARTUBE_WATCH_DIR) once, tracking fingerprints in mirror-watch.json"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let dir = payload
                .get("dir")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| self.state.watch_dir.lock().expect("watch dir").clone());
            self.state.scan(dir.as_deref(), ctx).await
        })
    }

    fn poll<'a>(
        &'a self,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let dir = self.state.watch_dir.lock().expect("watch dir").clone();
            match dir {
                Some(dir) => Ok(Some(self.state.scan(Some(&dir), ctx).await?)),
                None => Ok(None),
            }
        })
    }

    fn info(&self) -> Value {
        let tracked = self.state.ledger.lock().expect("mirror ledger").len();
        let watching = self.state.watch_dir.lock().expect("watch dir").is_some();
        json!({"watching": watching, "trackedFiles": tracked})
    }
}

struct FeedMirrorPlugin {
    feeds: Arc<Mutex<Vec<String>>>,
    feed: Arc<dyn FeedFetch>,
}

impl FeedMirrorPlugin {
    async fn mirror_items<N>(
        &self,
        items: &[Value],
        headers: &[(String, String)],
        ctx: &Ctx<N>,
    ) -> Result<Value, MirrorError>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let mut enqueued = 0u64;
        let mut skipped = 0u64;
        let mut jobs = Vec::new();
        for item in items {
            if !item.is_object() {
                continue;
            }
            let id = match item.get("id") {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(text)) => text.to_lowercase(),
                Some(other) => other.to_string().to_lowercase(),
            };
            if !is_id(&id) {
                continue;
            }
            let sha256 = item
                .get("sha256")
                .and_then(Value::as_str)
                .filter(|sha| sha.bytes().all(|b| b.is_ascii_hexdigit()))
                .map(str::to_lowercase)
                .filter(|sha| is_hex64(sha));
            if ctx.has(Some(&id), sha256.as_deref()).await? {
                skipped += 1;
                continue;
            }
            let url = item
                .get("source")
                .and_then(|source| source.get("url"))
                .or_else(|| item.get("url"))
                .or_else(|| item.get("streamUrl"))
                .and_then(Value::as_str);
            let Some(url) =
                url.filter(|url| url.starts_with("http://") || url.starts_with("https://"))
            else {
                skipped += 1;
                continue;
            };
            let mut merged = headers.to_vec();
            let extra = item
                .get("source")
                .and_then(|source| source.get("headers"))
                .or_else(|| item.get("headers"))
                .and_then(Value::as_object);
            if let Some(extra) = extra {
                for (key, value) in extra {
                    let value = value.as_str().unwrap_or_default().to_owned();
                    match merged.iter_mut().find(|(seen, _)| seen == key) {
                        Some(slot) => slot.1 = value,
                        None => merged.push((key.clone(), value)),
                    }
                }
            }
            let title = item
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or(&id)
                .to_owned();
            let job = ctx.enqueue(Enqueue {
                id,
                title,
                source: SourceSpec {
                    url: url.to_owned(),
                    headers: merged,
                    page: false,
                    private: false,
                },
                channel: ctx.mirror_channel,
                plugin: "feed-mirror".to_owned(),
            })?;
            enqueued += 1;
            jobs.push(job);
        }
        Ok(json!({"enqueued": enqueued, "skipped": skipped, "jobs": jobs}))
    }

    async fn run_urls<N>(
        &self,
        urls: &[String],
        headers: Vec<(String, String)>,
        ctx: &Ctx<N>,
    ) -> Result<Value, MirrorError>
    where
        N: Node + 'static,
        N::Error: From<AcquireError>,
    {
        let mut enqueued = 0u64;
        let mut skipped = 0u64;
        for url in urls {
            let data = fetch_json(&*self.feed, url, headers.clone()).await?;
            let list = if let Some(list) = data.as_array() {
                list.clone()
            } else {
                data.get("items")
                    .and_then(Value::as_array)
                    .or_else(|| data.get("results").and_then(Value::as_array))
                    .cloned()
                    .unwrap_or_default()
            };
            let result = self.mirror_items(&list, &headers, ctx).await?;
            enqueued += result.get("enqueued").and_then(Value::as_u64).unwrap_or(0);
            skipped += result.get("skipped").and_then(Value::as_u64).unwrap_or(0);
        }
        Ok(json!({"feeds": urls.len(), "enqueued": enqueued, "skipped": skipped}))
    }
}

impl<N: Node + 'static> MirrorPlugin<N> for FeedMirrorPlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        "feed-mirror"
    }

    fn description(&self) -> &str {
        "Mirrors remote JSON manifests or peer PearTube relay /v1/entries catalogs with idempotent deduplication"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let headers = payload
                .get("headers")
                .and_then(Value::as_object)
                .map(|headers| {
                    headers
                        .iter()
                        .map(|(key, value)| {
                            (key.clone(), value.as_str().unwrap_or_default().to_owned())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if let Some(items) = payload.get("items").and_then(Value::as_array) {
                return self.mirror_items(items, &headers, ctx).await;
            }
            let urls: Vec<String> = match payload.get("url").and_then(Value::as_str) {
                Some(url) => vec![url.to_owned()],
                None => self.feeds.lock().expect("feeds").clone(),
            };
            self.run_urls(&urls, headers, ctx).await
        })
    }

    fn poll<'a>(
        &'a self,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            if self.feeds.lock().expect("feeds").is_empty() {
                return Ok(None);
            }
            Ok(Some(self.run(Value::Null, ctx).await?))
        })
    }

    fn info(&self) -> Value {
        json!({"feeds": self.feeds.lock().expect("feeds").len()})
    }
}

/// One registered mirror plugin, with the name it was registered under.
pub type Plugin<N> = Arc<dyn MirrorPlugin<N>>;

/// `createMirrorHost`'s returned handle.
pub struct MirrorHost<N: Node> {
    pub node: Arc<N>,
    storage: String,
    plugins: Mutex<Vec<(String, Plugin<N>)>>,
    stats: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
    acquirer: Option<Acquirer<N>>,
    watch_state: Arc<WatchState>,
    watch_dir: Arc<Mutex<Option<String>>>,
    feeds: Arc<Mutex<Vec<String>>>,
    rechunk_progress: Arc<Mutex<Value>>,
    closed: Arc<AtomicBool>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    poll_task: Mutex<Option<JoinHandle<()>>>,
    ctx: Ctx<N>,
}

impl<N: Node + 'static> MirrorHost<N>
where
    N::Error: From<AcquireError>,
{
    /// `createMirrorHost`: register the built-ins, then run the startup backfill, folder import, feeds and timer.
    pub async fn start(
        node: Arc<N>,
        acquirer: Option<Acquirer<N>>,
        files: Arc<dyn MirrorFiles>,
        factory: Arc<dyn PutStreamFactory<N>>,
        rechunk_factory: Arc<dyn RechunkOptionsFactory<N>>,
        feed: Arc<dyn FeedFetch>,
        opts: MirrorOptions,
    ) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let watch_state = Arc::new(WatchState {
            ledger: Mutex::new(load_ledger(&*files, &opts.storage)),
            state_file: format!("{}/mirror-watch.json", opts.storage),
            scanning: AtomicBool::new(false),
            watch_dir: Mutex::new(opts.watch_dir.clone()),
            closed: closed.clone(),
            files: files.clone(),
            now_ms: opts.now_ms.clone(),
        });
        let watch_dir = Arc::new(Mutex::new(opts.watch_dir.clone()));
        let feeds = Arc::new(Mutex::new(opts.feeds.clone()));
        let rechunk_progress = Arc::new(Mutex::new(json!({
            "running": false,
            "total": 0,
            "migrated": 0,
            "deduped": 0,
            "remaining": 0,
            "migratedBytes": 0,
            "currentId": null,
            "currentTitle": null,
        })));
        let ctx = Ctx {
            node: node.clone(),
            storage: opts.storage.clone(),
            acquirer: acquirer.clone(),
            mirror_channel: opts.mirror_channel,
            files: files.clone(),
            factory,
            onchange: opts.onchange.clone(),
        };
        let host = Self {
            node,
            storage: opts.storage.clone(),
            plugins: Mutex::new(Vec::new()),
            stats: Arc::new(Mutex::new(std::collections::HashMap::new())),
            acquirer: acquirer.clone(),
            watch_state,
            watch_dir,
            feeds,
            rechunk_progress,
            closed,
            now_ms: opts.now_ms.clone(),
            onchange: opts.onchange.clone(),
            tasks: Mutex::new(Vec::new()),
            poll_task: Mutex::new(None),
            ctx,
        };
        host.register(Arc::new(HttpAcquirePlugin {
            acquirer: acquirer.clone(),
        }));
        host.register(Arc::new(ChannelBackfillPlugin));
        host.register(Arc::new(BlobRechunkPlugin {
            acquirer,
            progress: host.rechunk_progress.clone(),
            factory: rechunk_factory.clone(),
        }));
        host.register(Arc::new(WatchDirPlugin {
            state: host.watch_state.clone(),
        }));
        host.register(Arc::new(FeedMirrorPlugin {
            feeds: host.feeds.clone(),
            feed,
        }));

        if let Some(onstarting) = &opts.onstarting {
            onstarting();
        }
        if opts.mirror_channel {
            let _ = host.run("channel-backfill", Value::Null).await;
        }
        if opts.rechunk {
            host.spawn_run("blob-rechunk", Value::Null);
        } else if !host.is_closed() {
            // Count legacy blobs without moving any, so the UI offers the migration only when there is something.
            let progress = host.rechunk_progress.clone();
            let node = host.node.clone();
            let options = rechunk_factory.make(RechunkOptions {
                id: None,
                foreground: false,
                limit: Some(0),
                on_migrate: Box::new(|_, _| {}),
                on_progress: Box::new(|_| {}),
            });
            let handle = tokio::spawn(async move {
                if let Ok(Value::Object(result)) = node
                    .rechunk_legacy_blobs(options)
                    .await
                    .map(|result| serde_json::to_value(result).unwrap_or(Value::Null))
                {
                    let mut progress = progress.lock().expect("rechunk progress");
                    if !progress
                        .get("running")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                        && let Some(map) = progress.as_object_mut()
                    {
                        map.extend(result);
                        map.insert("running".into(), json!(false));
                    }
                }
            });
            host.tasks.lock().expect("mirror tasks").push(handle);
        }
        if let Some(dir) = opts.watch_dir.clone().filter(|_| !host.is_closed()) {
            files.mkdirs(&dir);
            let _ = host.run("watch-dir", Value::Null).await;
        }
        if !host.feeds.lock().expect("feeds").is_empty() && !host.is_closed() {
            host.spawn_run("feed-mirror", Value::Null);
        }
        host.start_poll_timer(opts.poll_interval_ms);
        host
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// `register`: the name must be non-empty; a re-registration replaces the plugin and keeps its stats.
    pub fn register(&self, plugin: Arc<dyn MirrorPlugin<N>>) {
        let name = plugin.name().trim().to_owned();
        assert!(!name.is_empty(), "Mirror plugin requires a non-empty name");
        {
            let mut plugins = self.plugins.lock().expect("mirror plugins");
            match plugins.iter_mut().find(|(seen, _)| *seen == name) {
                Some(slot) => slot.1 = plugin,
                None => plugins.push((name.clone(), plugin)),
            }
        }
        self.stats
            .lock()
            .expect("mirror stats")
            .entry(name)
            .or_default();
    }

    fn plugin(&self, name: &str) -> Option<Arc<dyn MirrorPlugin<N>>> {
        self.plugins
            .lock()
            .expect("mirror plugins")
            .iter()
            .find(|(seen, _)| seen == name)
            .map(|(_, plugin)| plugin.clone())
    }

    /// `run`: count the run, keep its last result or error, and surface the error unchanged.
    pub async fn run(&self, name: &str, payload: Value) -> Result<Value, MirrorError> {
        let Some(plugin) = self.plugin(name) else {
            return Err(MirrorError::with_status(
                format!("Unknown mirror plugin: {name}"),
                404,
            ));
        };
        {
            let mut stats = self.stats.lock().expect("mirror stats");
            let stat = stats.entry(name.to_owned()).or_default();
            stat.runs += 1;
            stat.last_run_at = Some((self.now_ms)());
            stat.last_error = None;
        }
        match plugin.run(payload, &self.ctx).await {
            Ok(result) => {
                self.stats
                    .lock()
                    .expect("mirror stats")
                    .entry(name.to_owned())
                    .or_default()
                    .last_result = Some(result.clone());
                if let Some(onchange) = &self.onchange {
                    onchange();
                }
                Ok(result)
            }
            Err(error) => {
                self.stats
                    .lock()
                    .expect("mirror stats")
                    .entry(name.to_owned())
                    .or_default()
                    .last_error = Some(error.message.clone());
                Err(error)
            }
        }
    }

    fn spawn_run(&self, name: &str, payload: Value) {
        let Some(plugin) = self.plugin(name) else {
            return;
        };
        let stats = self.stats.clone();
        let ctx = Ctx {
            node: self.ctx.node.clone(),
            storage: self.ctx.storage.clone(),
            acquirer: self.ctx.acquirer.clone(),
            mirror_channel: self.ctx.mirror_channel,
            files: self.ctx.files.clone(),
            factory: self.ctx.factory.clone(),
            onchange: self.ctx.onchange.clone(),
        };
        let now_ms = self.now_ms.clone();
        let name = name.to_owned();
        let handle = tokio::spawn(async move {
            {
                let mut stats = stats.lock().expect("mirror stats");
                let stat = stats.entry(name.clone()).or_default();
                stat.runs += 1;
                stat.last_run_at = Some((now_ms)());
                stat.last_error = None;
            }
            let outcome = plugin.run(payload, &ctx).await;
            let mut stats = stats.lock().expect("mirror stats");
            let stat = stats.entry(name).or_default();
            match outcome {
                Ok(result) => stat.last_result = Some(result),
                Err(error) => stat.last_error = Some(error.message),
            }
        });
        self.tasks.lock().expect("mirror tasks").push(handle);
    }

    fn start_poll_timer(&self, poll_interval_ms: u64) {
        if poll_interval_ms == 0 || self.is_closed() {
            return;
        }
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        let stats = self.stats.clone();
        let closed = self.closed.clone();
        let now_ms = self.now_ms.clone();
        let template = Ctx {
            node: self.ctx.node.clone(),
            storage: self.ctx.storage.clone(),
            acquirer: self.ctx.acquirer.clone(),
            mirror_channel: self.ctx.mirror_channel,
            files: self.ctx.files.clone(),
            factory: self.ctx.factory.clone(),
            onchange: self.ctx.onchange.clone(),
        };
        // Keep the app's polling interval: the plugin list is small and each poll is cheap.
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                if closed.load(Ordering::SeqCst) {
                    return;
                }
                let ctx = Ctx {
                    node: template.node.clone(),
                    storage: template.storage.clone(),
                    acquirer: template.acquirer.clone(),
                    mirror_channel: template.mirror_channel,
                    files: template.files.clone(),
                    factory: template.factory.clone(),
                    onchange: template.onchange.clone(),
                };
                for (name, plugin) in &plugins {
                    match plugin.poll(&ctx).await {
                        Ok(Some(result)) => {
                            let mut stats = stats.lock().expect("mirror stats");
                            let stat = stats.entry(name.clone()).or_default();
                            stat.last_run_at = Some((now_ms)());
                            stat.last_result = Some(result);
                        }
                        Ok(None) => {}
                        Err(error) => {
                            stats
                                .lock()
                                .expect("mirror stats")
                                .entry(name.clone())
                                .or_default()
                                .last_error = Some(error.message);
                        }
                    }
                }
            }
        });
        *self.poll_task.lock().expect("poll task") = Some(handle);
    }

    /// `/v1/plugins`: name, description, live info and run stats for every registered plugin.
    pub fn list(&self) -> Vec<Value> {
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        let stats = self.stats.lock().expect("mirror stats");
        plugins
            .iter()
            .map(|(name, plugin)| {
                let mut out = Map::new();
                out.insert("name".into(), json!(name));
                out.insert("description".into(), json!(plugin.description()));
                if let Value::Object(info) = plugin.info() {
                    for (key, value) in info {
                        out.insert(key, value);
                    }
                }
                if let Value::Object(fields) =
                    stats.get(name).cloned().unwrap_or_default().to_json()
                {
                    for (key, value) in fields {
                        out.insert(key, value);
                    }
                }
                Value::Object(out)
            })
            .collect()
    }

    /// `configure`: the watch folder and followed feeds take effect without a restart.
    ///
    /// `replicate*` keys belong to the unported `p2p-replicate` plugin and are ignored here.
    pub fn configure(&self, watch_dir: Option<Option<String>>, feeds: Option<Vec<String>>) {
        if let Some(dir) = watch_dir
            && dir != *self.watch_dir.lock().expect("watch dir")
        {
            *self.watch_dir.lock().expect("watch dir") = dir.clone();
            if let Some(dir) = dir {
                self.ctx.files.mkdirs(&dir);
                self.spawn_run("watch-dir", Value::Null);
            }
        }
        if let Some(next) = feeds {
            let added: Vec<String> = {
                let mut configured = self.feeds.lock().expect("feeds");
                let added = next
                    .iter()
                    .filter(|url| !configured.contains(url))
                    .cloned()
                    .collect();
                *configured = next;
                added
            };
            for url in added {
                self.spawn_run("feed-mirror", json!({ "url": url }));
            }
        }
        if let Some(onchange) = &self.onchange {
            onchange();
        }
    }

    /// Wait for the work the host started in the background (feeds, rechunk, configure-driven scans).
    pub async fn wait_idle(&self) {
        loop {
            let handle = {
                let mut tasks = self.tasks.lock().expect("mirror tasks");
                tasks.retain(|handle| !handle.is_finished());
                tasks.pop()
            };
            match handle {
                Some(handle) => {
                    let _ = handle.await;
                }
                None => return,
            }
        }
    }

    /// `close`: stop polling, let every plugin stop, and refuse new work.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(handle) = self.poll_task.lock().expect("poll task").take() {
            handle.abort();
        }
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        for (_, plugin) in plugins {
            plugin.stop().await;
        }
        let handles: Vec<JoinHandle<()>> =
            self.tasks.lock().expect("mirror tasks").drain(..).collect();
        for handle in handles {
            handle.abort();
        }
    }

    pub fn acquirer(&self) -> Option<&Acquirer<N>> {
        self.acquirer.as_ref()
    }

    pub fn rechunk_progress(&self) -> Value {
        self.rechunk_progress
            .lock()
            .expect("rechunk progress")
            .clone()
    }

    pub fn storage(&self) -> &str {
        &self.storage
    }
}

fn load_ledger(files: &dyn MirrorFiles, storage: &str) -> Map<String, Value> {
    let path = format!("{storage}/mirror-watch.json");
    files
        .read(&path)
        .and_then(|text| serde_json::from_str::<Value>(&text).ok())
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

/// The clock a host uses for stats and the ledger.
pub fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
