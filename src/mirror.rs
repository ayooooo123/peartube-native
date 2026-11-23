//! Mirror plugin host for PearTube relays, ported from `src/mirror.js`.
//!
//! Built-in plugins: `http-acquire` (the durable acquirer as a plugin), `channel-backfill`, `blob-rechunk`,
//! `watch-dir` ([`watch`]: the import folder, where a folder of music files becomes one album), `feed-mirror`
//! and `soulseek` (the provider in [`crate::providers::soulseek`], which mirrors albums into the folder
//! `watch-dir` sweeps). `p2p-replicate` (`crate::replicate`) and `hiverelay-seed` (`crate::hiverelay::seeder`)
//! own their modules: replication needs block-range downloads off a remote core, and the seeder needs the
//! Corestore and the node's connections. The caller passes them to [`MirrorHost::start`] and they register
//! as the app's plugins do.
//!
//! The filesystem and remote-feed fetches sit behind small traits so a test can script them.

pub mod sources;
pub mod watch;

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

use self::watch::{WatchDirPlugin, WatchState};
use crate::acquire::{
    AcquireError, Acquirer, AddJob, JobSource, OsRandom, PutStreamFactory, RandomSource, SourceSpec,
};
use crate::art;
use crate::node::Node;
use crate::node::api::{PutMeta, SearchOptions};
use crate::protocol::{is_hex64, is_id};
use crate::providers::soulseek::client::Backend;
use crate::providers::soulseek::{self, Provider, SoulseekError};
use crate::providers::staging::PlaceAndScan;
use crate::providers::youtube;

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

impl<N: Node> Clone for Ctx<N> {
    fn clone(&self) -> Self {
        Self {
            node: self.node.clone(),
            storage: self.storage.clone(),
            acquirer: self.acquirer.clone(),
            mirror_channel: self.mirror_channel,
            files: self.files.clone(),
            factory: self.factory.clone(),
            onchange: self.onchange.clone(),
        }
    }
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
                let status = matches!(
                    status,
                    "queued" | "copying" | "running" | "announcing" | "done"
                );
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
                existing: spec.existing,
            })
            .map_err(|error| MirrorError::plain(error.message()))?;
        self.changed();
        Ok(job)
    }

    /// `ingestStream`: `put`, then `append`, then tell listeners. `music` is a track's own name and
    /// artist, when the file is one.
    pub async fn ingest_stream(
        &self,
        id: &str,
        title: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        private: bool,
        music: Option<Value>,
    ) -> Result<Value, MirrorError> {
        let mut extra = Map::new();
        extra.insert("music".to_owned(), music.unwrap_or(Value::Null));
        let meta = PutMeta {
            id: id.to_owned(),
            title: title.to_owned(),
            private_entry: Some(private),
            epoch: None,
            store: None,
            expected_size: None,
            extra,
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
    pub existing: Option<Value>,
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
    /// The plugin as a provider, when it is one.
    fn provider(&self) -> Option<&dyn MirrorProvider<N>> {
        None
    }
}

/// What a provider call answers.
pub type ProviderFuture<'a> = Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>>;

/// A plugin that finds media somewhere else and mirrors it in: the `search`, `browse`, `mirror` and
/// job calls the app's provider plugins carry, behind `/v1/providers/<name>/…` and `/v1/jobs`.
pub trait MirrorProvider<N: Node>: Send + Sync {
    fn search<'a>(&'a self, payload: Value, ctx: &'a Ctx<N>) -> ProviderFuture<'a>;
    fn browse<'a>(&'a self, payload: Value, ctx: &'a Ctx<N>) -> ProviderFuture<'a>;
    fn mirror<'a>(&'a self, payload: Value, ctx: &'a Ctx<N>) -> ProviderFuture<'a>;
    fn list_jobs(&self) -> Vec<Value>;
    fn get_job(&self, job_id: &str) -> Option<Value>;
    fn cancel_job(&self, job_id: &str) -> Option<Value>;
    /// A result's picture, served by the relay (`thumbnail(id)`). A provider without pictures has
    /// none.
    fn thumbnail<'a>(
        &'a self,
        _id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<youtube::Picture>> + Send + 'a>> {
        Box::pin(async { None })
    }
}

/// One directory entry, as `statSync` reports it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FileStat {
    pub is_file: bool,
    pub size: u64,
    pub mtime_ms: f64,
}

/// One name in a folder. `is_dir` is the entry's own kind: a link to a folder is not a folder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

/// The files a mirror host touches.
pub trait MirrorFiles: Send + Sync {
    fn exists(&self, path: &str) -> bool;
    fn mkdirs(&self, path: &str);
    /// What is inside `dir`, unsorted; callers sort as the app does. Empty when it cannot be read.
    fn list_entries(&self, dir: &str) -> Vec<DirEntry>;
    fn stat(&self, path: &str) -> Option<FileStat>;
    fn read(&self, path: &str) -> Option<String>;
    /// The first `max` bytes of a file.
    fn read_head(&self, path: &str, max: usize) -> Option<Vec<u8>>;
    fn write(&self, path: &str, text: &str) -> Result<(), String>;
    fn write_atomic(&self, path: &str, text: &str) -> Result<(), String>;
    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String>;
    /// Removes a file; `Ok(false)` when there was none.
    fn remove_file(&self, path: &str) -> Result<bool, String>;
    /// Removes a folder that is empty.
    fn remove_dir(&self, path: &str);
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

    fn list_entries(&self, dir: &str) -> Vec<DirEntry> {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .flatten()
                    .map(|entry| DirEntry {
                        name: entry.file_name().to_string_lossy().into_owned(),
                        is_dir: entry.file_type().is_ok_and(|kind| kind.is_dir()),
                    })
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

    fn read_head(&self, path: &str, max: usize) -> Option<Vec<u8>> {
        use std::io::Read;
        let mut head = Vec::new();
        std::fs::File::open(path)
            .ok()?
            .take(max as u64)
            .read_to_end(&mut head)
            .ok()?;
        Some(head)
    }

    fn write(&self, path: &str, text: &str) -> Result<(), String> {
        std::fs::write(path, text).map_err(|error| error.to_string())
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

    fn remove_file(&self, path: &str) -> Result<bool, String> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.to_string()),
        }
    }

    fn remove_dir(&self, path: &str) {
        let _ = std::fs::remove_dir(path);
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
pub struct ArtFeed {
    pub allow_private_urls: bool,
}

impl Default for ArtFeed {
    fn default() -> Self {
        Self {
            allow_private_urls: crate::ssrf::private_urls_allowed(),
        }
    }
}

impl FeedFetch for ArtFeed {
    fn get(
        &self,
        url: &str,
        headers: Vec<(String, String)>,
    ) -> Pin<Box<dyn Future<Output = Result<art::HttpResponse, art::HttpError>> + Send>> {
        let url = url.to_owned();
        let allow_private = self.allow_private_urls;
        Box::pin(async move {
            let mut all = vec![("accept".to_owned(), "application/json".to_owned())];
            all.extend(headers);
            art::send(
                &url,
                art::SendOptions {
                    headers: all,
                    timeout: Duration::from_millis(60_000),
                    max_bytes: Some(8 * 1024 * 1024),
                    allow_private,
                    ..art::SendOptions::default()
                },
            )
            .await
        })
    }
}

/// The host's `fetchBytes`: the guarded fetch with the reply's bytes, 8 s and 512 KiB at most, for
/// the pictures of a provider's results.
pub struct ArtPictures {
    pub allow_private_urls: bool,
}

impl Default for ArtPictures {
    fn default() -> Self {
        Self {
            allow_private_urls: crate::ssrf::private_urls_allowed(),
        }
    }
}

impl youtube::Fetch for ArtPictures {
    fn fetch(
        &self,
        url: &str,
    ) -> Pin<Box<dyn Future<Output = Result<youtube::Fetched, String>> + Send>> {
        let url = url.to_owned();
        let allow_private = self.allow_private_urls;
        Box::pin(async move {
            let reply = art::send_bytes(
                &url,
                art::SendOptions {
                    timeout: Duration::from_millis(8000),
                    max_bytes: Some(512 * 1024),
                    allow_private,
                    ..art::SendOptions::default()
                },
            )
            .await
            .map_err(|error| error.message().to_owned())?;
            Ok(youtube::Fetched {
                status: reply.status,
                content_type: reply.header("content-type").map(str::to_owned),
                body: reply.body,
            })
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

/// The Soulseek provider's settings (`slskUser` to `slskShareMaxFiles`), and what makes its clients.
pub struct SoulseekSettings {
    pub user: String,
    pub pass: String,
    /// `host:port`; empty is the provider's own default.
    pub server: String,
    /// Peers connect back to this TCP port; 0 lets the system pick one each start.
    pub listen_port: u16,
    pub share: bool,
    pub share_max_files: u32,
    /// `None` on a host with no Soulseek client: the plugin then reports `unavailable`.
    pub backend: Option<Arc<dyn Backend>>,
    pub random: Arc<dyn RandomSource>,
}

impl Default for SoulseekSettings {
    fn default() -> Self {
        Self {
            user: String::new(),
            pass: String::new(),
            server: String::new(),
            listen_port: soulseek::DEFAULT_LISTEN_PORT,
            share: false,
            share_max_files: soulseek::DEFAULT_SHARE_MAX_FILES,
            backend: None,
            random: Arc::new(OsRandom),
        }
    }
}

/// The YouTube provider's ways out: the relay's `ubit` calls (`youtube: { info, download }`) and
/// the host's guarded fetch for result pictures. Without a backend the plugin reports
/// `unavailable`.
#[derive(Clone)]
pub struct YoutubeSettings {
    pub backend: Option<Arc<dyn youtube::Backend>>,
    pub fetch: Arc<dyn youtube::Fetch>,
    pub random: Arc<dyn RandomSource>,
}

impl Default for YoutubeSettings {
    fn default() -> Self {
        Self {
            backend: None,
            fetch: Arc::new(ArtPictures::default()),
            random: Arc::new(OsRandom),
        }
    }
}

/// `createMirrorHost`'s options.
pub struct MirrorOptions {
    pub storage: String,
    pub mirror_channel: bool,
    pub rechunk: bool,
    pub watch_dir: Option<String>,
    pub feeds: Vec<String>,
    pub soulseek: SoulseekSettings,
    pub youtube: YoutubeSettings,
    pub plugins_dir: Option<String>,
    pub poll_interval_ms: u64,
    /// How long a provider's finished save waits for another import to finish before it gives up
    /// with nothing placed.
    pub import_wait_ms: u64,
    /// Plugins to run once the host is up, by name, as `relay.js` runs `hiverelay-seed`.
    pub start_runs: Vec<String>,
    pub onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Runs once, after every plugin has stopped: detach the relay's traffic listener and stop
    /// its gate.
    pub stop_traffic: Option<Box<dyn FnOnce() + Send + Sync>>,
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
            soulseek: SoulseekSettings::default(),
            youtube: YoutubeSettings::default(),
            plugins_dir: None,
            poll_interval_ms: 60_000,
            import_wait_ms: 120_000,
            start_runs: Vec::new(),
            onchange: None,
            stop_traffic: None,
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
                existing: None,
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
            "activeJobs": count(&["queued", "copying", "running", "announcing"]),
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
        let mut by_sha: Option<std::collections::HashMap<String, crate::node::api::Entry>> = None;
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
            let existing = if let Some(sha) = &sha256 {
                if by_sha.is_none() {
                    let mut map = std::collections::HashMap::new();
                    for entry in ctx
                        .node
                        .search(None, SearchOptions::default())
                        .await
                        .map_err(|e| MirrorError::plain(e.to_string()))?
                    {
                        if !entry.sha256.is_empty()
                            && !entry.private_entry
                            && !entry.locked
                            && entry.epoch == 0
                        {
                            map.insert(entry.sha256.clone(), entry);
                        }
                    }
                    by_sha = Some(map);
                }
                let candidate = by_sha.as_ref().and_then(|m| m.get(sha)).cloned();
                if let Some(entry) = candidate {
                    let op = ctx.node.find_entry(&entry.key).await.ok().flatten();
                    op.filter(|op| {
                        op.get("blobs").is_some_and(|v| !v.is_null())
                            && op.get("blob").is_some_and(|v| !v.is_null())
                            && op.get("wrappedKey").is_none_or(Value::is_null)
                            && op.get("epoch").and_then(Value::as_u64).unwrap_or(0) == 0
                    })
                    .map(|mut op| {
                        if let Some(map) = op.as_object_mut() {
                            map.insert("key".into(), json!(entry.key));
                        }
                        op
                    })
                } else {
                    None
                }
            } else {
                None
            };
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
                existing,
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

/// The `soulseek` plugin: the provider behind the host's plugin and provider calls. Its mirrors land
/// in the folder `watch-dir` sweeps, placed and imported in one step once no other import holds it.
struct SoulseekPlugin {
    provider: Provider,
    watch: Arc<WatchState>,
    /// Where mirrors land.
    staging_root: String,
}

/// The host's `placeAndScan` as a provider whose saves land in `root` is given it.
fn place_and_scan<N>(watch: &Arc<WatchState>, ctx: &Ctx<N>, root: &str) -> PlaceAndScan
where
    N: Node + 'static,
    N::Error: From<AcquireError>,
{
    let (watch, ctx, root) = (watch.clone(), ctx.clone(), root.to_owned());
    Arc::new(move |place, cancelled| {
        let (watch, ctx, root) = (watch.clone(), ctx.clone(), root.clone());
        Box::pin(async move {
            let prefix = watch.prefix_of(&root);
            watch
                .place_and_scan(&root, &ctx, prefix, place, cancelled)
                .await
        })
    })
}

fn provider_error(error: SoulseekError) -> MirrorError {
    MirrorError {
        message: error.message,
        status: error.status,
    }
}

fn unknown_provider(name: &str) -> MirrorError {
    MirrorError::with_status(format!("Unknown provider: {name}"), 404)
}

impl<N: Node + 'static> MirrorPlugin<N> for SoulseekPlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        soulseek::NAME
    }

    fn description(&self) -> &str {
        soulseek::DESCRIPTION
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let host = place_and_scan(&self.watch, ctx, &self.staging_root);
            self.provider
                .run(&payload, Some(host))
                .await
                .map_err(provider_error)
        })
    }

    fn stop<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.provider.stop() })
    }

    fn info(&self) -> Value {
        self.provider.info()
    }

    fn provider(&self) -> Option<&dyn MirrorProvider<N>> {
        Some(self)
    }
}

impl<N: Node + 'static> MirrorProvider<N> for SoulseekPlugin
where
    N::Error: From<AcquireError>,
{
    fn search<'a>(&'a self, payload: Value, _ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async move { self.provider.search(&payload).await.map_err(provider_error) })
    }

    fn browse<'a>(&'a self, payload: Value, _ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async move { self.provider.browse(&payload).await.map_err(provider_error) })
    }

    fn mirror<'a>(&'a self, payload: Value, ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async move {
            let host = place_and_scan(&self.watch, ctx, &self.staging_root);
            self.provider
                .mirror(&payload, Some(host))
                .await
                .map_err(provider_error)
        })
    }

    fn list_jobs(&self) -> Vec<Value> {
        self.provider.list_jobs()
    }

    fn get_job(&self, job_id: &str) -> Option<Value> {
        self.provider.get_job(job_id)
    }

    fn cancel_job(&self, job_id: &str) -> Option<Value> {
        self.provider.cancel_job(job_id)
    }
}

/// The `youtube` plugin: the provider behind the host's plugin and provider calls. Its saves land
/// where the Soulseek provider's mirrors land, placed and imported in the same way.
struct YoutubePlugin {
    provider: youtube::Provider,
    watch: Arc<WatchState>,
    /// Where saves land.
    staging_root: String,
}

fn youtube_error(error: youtube::YoutubeError) -> MirrorError {
    MirrorError {
        message: error.message,
        status: error.status,
    }
}

impl<N: Node + 'static> MirrorPlugin<N> for YoutubePlugin
where
    N::Error: From<AcquireError>,
{
    fn name(&self) -> &str {
        youtube::NAME
    }

    fn description(&self) -> &str {
        youtube::DESCRIPTION
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        ctx: &'a Ctx<N>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            let host = place_and_scan(&self.watch, ctx, &self.staging_root);
            self.provider
                .run(&payload, Some(host))
                .await
                .map_err(youtube_error)
        })
    }

    fn stop<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(self.provider.stop())
    }

    fn info(&self) -> Value {
        self.provider.info()
    }

    fn provider(&self) -> Option<&dyn MirrorProvider<N>> {
        Some(self)
    }
}

impl<N: Node + 'static> MirrorProvider<N> for YoutubePlugin
where
    N::Error: From<AcquireError>,
{
    fn search<'a>(&'a self, payload: Value, _ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async move { self.provider.search(&payload).await.map_err(youtube_error) })
    }

    /// The app's plugin has no `browse`: the host answers as for a provider it does not know.
    fn browse<'a>(&'a self, _payload: Value, _ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async { Err(unknown_provider(youtube::NAME)) })
    }

    fn mirror<'a>(&'a self, payload: Value, ctx: &'a Ctx<N>) -> ProviderFuture<'a> {
        Box::pin(async move {
            let host = place_and_scan(&self.watch, ctx, &self.staging_root);
            self.provider
                .mirror(&payload, Some(host))
                .await
                .map_err(youtube_error)
        })
    }

    fn list_jobs(&self) -> Vec<Value> {
        self.provider.list_jobs()
    }

    fn get_job(&self, job_id: &str) -> Option<Value> {
        self.provider.get_job(job_id)
    }

    fn cancel_job(&self, job_id: &str) -> Option<Value> {
        self.provider.cancel_job(job_id)
    }

    fn thumbnail<'a>(
        &'a self,
        id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Option<youtube::Picture>> + Send + 'a>> {
        Box::pin(self.provider.thumbnail(id))
    }
}

/// What `configure` may change while the host runs; `None` leaves a value as it is.
#[derive(Debug, Clone, Default)]
pub struct MirrorConfig {
    pub feeds: Option<Vec<String>>,
    pub slsk_user: Option<String>,
    pub slsk_pass: Option<String>,
    pub slsk_server: Option<String>,
}

/// One registered mirror plugin, with the name it was registered under.
pub type Plugin<N> = Arc<dyn MirrorPlugin<N>>;

/// What the start runs read: [`MirrorHost::open`] keeps it for [`MirrorHost::startup`].
struct Startup<N: Node> {
    mirror_channel: bool,
    rechunk: bool,
    watch_dir: Option<String>,
    staging_dir: String,
    start_runs: Vec<String>,
    poll_interval_ms: u64,
    files: Arc<dyn MirrorFiles>,
    rechunk_factory: Arc<dyn RechunkOptionsFactory<N>>,
}

/// `createMirrorHost`'s returned handle.
pub struct MirrorHost<N: Node> {
    pub node: Arc<N>,
    storage: String,
    plugins: Mutex<Vec<(String, Plugin<N>)>>,
    stats: Arc<Mutex<std::collections::HashMap<String, Stats>>>,
    acquirer: Option<Acquirer<N>>,
    watch_state: Arc<WatchState>,
    soulseek: Provider,
    feeds: Arc<Mutex<Vec<String>>>,
    rechunk_progress: Arc<Mutex<Value>>,
    closed: Arc<AtomicBool>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    poll_task: Mutex<Option<JoinHandle<()>>>,
    stop_traffic: Mutex<Option<Box<dyn FnOnce() + Send + Sync>>>,
    startup: Mutex<Option<Startup<N>>>,
    ctx: Ctx<N>,
}

impl<N: Node + 'static> MirrorHost<N>
where
    N::Error: From<AcquireError>,
{
    /// `createMirrorHost` up to `onstarting`: the host with its plugins registered. It runs
    /// nothing until [`MirrorHost::startup`], and it can be closed before that or while that runs.
    #[allow(clippy::too_many_arguments)] // the reference's host arguments, plus the relay's plugins
    pub fn open(
        node: Arc<N>,
        acquirer: Option<Acquirer<N>>,
        files: Arc<dyn MirrorFiles>,
        factory: Arc<dyn PutStreamFactory<N>>,
        rechunk_factory: Arc<dyn RechunkOptionsFactory<N>>,
        feed: Arc<dyn FeedFetch>,
        // Plugins the caller built (the relay's `hiverelay-seed`), registered before polling starts.
        extra: Vec<Arc<dyn MirrorPlugin<N>>>,
        opts: MirrorOptions,
    ) -> Self {
        let closed = Arc::new(AtomicBool::new(false));
        let staging_dir = watch::staging_dir(&opts.storage);
        let watch_state = Arc::new(WatchState {
            ledger: Mutex::new(watch::load_ledger(&*files, &opts.storage)),
            state_file: format!("{}/mirror-watch.json", opts.storage),
            scanning: AtomicBool::new(false),
            import_wait_ms: opts.import_wait_ms,
            watch_dir: Mutex::new(opts.watch_dir.clone()),
            staging_dir: staging_dir.clone(),
            closed: closed.clone(),
            files: files.clone(),
            now_ms: opts.now_ms.clone(),
        });
        // Soulseek mirrors land in the import folder the relay already sweeps, so a mirrored album
        // sits beside (and joins) the albums imported from there, under the same ledger keys. A
        // read-only import folder cannot take them; they go to `<storage>/staging`, which
        // `watch-dir` sweeps too under the `staging:` prefix.
        let staging_root = match opts.watch_dir.as_deref() {
            Some(dir) if watch::writable_dir(&*files, dir, (opts.now_ms)()) => watch::resolved(dir),
            _ => staging_dir.clone(),
        };
        let staged = staging_root == staging_dir;
        eprintln!(
            "{}",
            json!({
                "msg": "soulseek staging root",
                "root": staging_root,
                "sweptAs": if staged { "staging:" } else { "watch-dir" },
            })
        );
        let staging_label = match (staged, &opts.watch_dir) {
            (false, _) => "import folder",
            (true, Some(_)) => "storage (import folder is read-only)",
            (true, None) => "storage",
        };
        let soulseek = Provider::new(soulseek::Options {
            staging_root: Some(staging_root.clone()),
            staging_label: staging_label.to_owned(),
            user: opts.soulseek.user.clone(),
            pass: opts.soulseek.pass.clone(),
            server: opts.soulseek.server.clone(),
            listen_port: opts.soulseek.listen_port,
            share: opts.soulseek.share,
            share_max_files: opts.soulseek.share_max_files,
            onchange: opts.onchange.clone(),
            ..soulseek::Options::new(
                opts.storage.clone(),
                opts.soulseek.backend.clone(),
                opts.soulseek.random.clone(),
                opts.now_ms.clone(),
            )
        });
        let youtube = youtube::Provider::new(youtube::Options {
            staging_root: Some(staging_root.clone()),
            staging_label: staging_label.to_owned(),
            fetch: Some(opts.youtube.fetch.clone()),
            onchange: opts.onchange.clone(),
            ..youtube::Options::new(
                opts.storage.clone(),
                opts.youtube.backend.clone(),
                opts.youtube.random.clone(),
                opts.now_ms.clone(),
            )
        });
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
            soulseek: soulseek.clone(),
            feeds,
            rechunk_progress,
            closed,
            now_ms: opts.now_ms.clone(),
            onchange: opts.onchange.clone(),
            tasks: Mutex::new(Vec::new()),
            poll_task: Mutex::new(None),
            stop_traffic: Mutex::new(opts.stop_traffic),
            startup: Mutex::new(Some(Startup {
                mirror_channel: opts.mirror_channel,
                rechunk: opts.rechunk,
                watch_dir: opts.watch_dir,
                staging_dir,
                start_runs: opts.start_runs,
                poll_interval_ms: opts.poll_interval_ms,
                files,
                rechunk_factory: rechunk_factory.clone(),
            })),
            ctx,
        };
        host.register(Arc::new(HttpAcquirePlugin {
            acquirer: acquirer.clone(),
        }));
        host.register(Arc::new(ChannelBackfillPlugin));
        host.register(Arc::new(BlobRechunkPlugin {
            acquirer,
            progress: host.rechunk_progress.clone(),
            factory: rechunk_factory,
        }));
        host.register(Arc::new(WatchDirPlugin {
            state: host.watch_state.clone(),
        }));
        host.register(Arc::new(FeedMirrorPlugin {
            feeds: host.feeds.clone(),
            feed,
        }));
        for plugin in extra {
            host.register(plugin);
        }
        host.register(Arc::new(SoulseekPlugin {
            provider: soulseek,
            watch: host.watch_state.clone(),
            staging_root: staging_root.clone(),
        }));
        host.register(Arc::new(YoutubePlugin {
            provider: youtube,
            watch: host.watch_state.clone(),
            staging_root,
        }));
        host
    }

    /// The rest of `createMirrorHost`: the startup backfill, the folder import, the feeds, the
    /// start runs and the poll timer. The backfill runs even when the host is already closed, as
    /// the app's does; the work after it starts only on a host that is not closed, and the folder
    /// import ends between files. Only the first call does the work.
    pub async fn startup(&self) {
        let Some(startup) = self.startup.lock().expect("mirror startup").take() else {
            return;
        };
        if startup.mirror_channel {
            let _ = self.run("channel-backfill", Value::Null).await;
        }
        if startup.rechunk && !self.is_closed() {
            self.spawn_run("blob-rechunk", Value::Null);
        } else if !self.is_closed() {
            // Count legacy blobs without moving any, so the UI offers the migration only when there is something.
            let progress = self.rechunk_progress.clone();
            let node = self.node.clone();
            let options = startup.rechunk_factory.make(RechunkOptions {
                id: None,
                foreground: false,
                limit: Some(0),
                on_migrate: Box::new(|_, _| {}),
                on_progress: Box::new(|_| {}),
            });
            self.background(async move {
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
        }
        if let Some(dir) = startup.watch_dir.as_deref().filter(|_| !self.is_closed()) {
            startup.files.mkdirs(dir);
            let _ = self.run("watch-dir", Value::Null).await;
        }
        let own_staging = startup
            .watch_dir
            .as_deref()
            .is_none_or(|dir| watch::resolved(dir) != watch::resolved(&startup.staging_dir));
        if startup.files.exists(&startup.staging_dir) && own_staging && !self.is_closed() {
            self.watch_state
                .scan(Some(&startup.staging_dir), &self.ctx, "staging:")
                .await;
        }
        if !self.feeds.lock().expect("feeds").is_empty() && !self.is_closed() {
            self.spawn_run("feed-mirror", Value::Null);
        }
        for name in &startup.start_runs {
            if !self.is_closed() {
                self.spawn_run(name, Value::Null);
            }
        }
        self.start_poll_timer(startup.poll_interval_ms);
    }

    /// `createMirrorHost`: [`MirrorHost::open`], then [`MirrorHost::startup`].
    #[allow(clippy::too_many_arguments)] // as `open`
    pub async fn start(
        node: Arc<N>,
        acquirer: Option<Acquirer<N>>,
        files: Arc<dyn MirrorFiles>,
        factory: Arc<dyn PutStreamFactory<N>>,
        rechunk_factory: Arc<dyn RechunkOptionsFactory<N>>,
        feed: Arc<dyn FeedFetch>,
        extra: Vec<Arc<dyn MirrorPlugin<N>>>,
        opts: MirrorOptions,
    ) -> Self {
        let host = Self::open(
            node,
            acquirer,
            files,
            factory,
            rechunk_factory,
            feed,
            extra,
            opts,
        );
        host.startup().await;
        host
    }

    /// Closed hosts start no new work; a caller checks this before work of its own on the host.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Starts a background task that a close ends. A closed host starts none: its close may
    /// already have ended the tasks it knew.
    fn background(&self, task: impl Future<Output = ()> + Send + 'static) {
        let mut tasks = self.tasks.lock().expect("mirror tasks");
        if !self.is_closed() {
            tasks.push(tokio::spawn(task));
        }
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
        let ctx = self.ctx.clone();
        let now_ms = self.now_ms.clone();
        let name = name.to_owned();
        self.background(async move {
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
    }

    fn start_poll_timer(&self, poll_interval_ms: u64) {
        // Checked under the lock a close takes the timer from: the close then finds the timer,
        // or this finds the host closed.
        let mut timer = self.poll_task.lock().expect("poll task");
        if poll_interval_ms == 0 || self.is_closed() {
            return;
        }
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        let stats = self.stats.clone();
        let closed = self.closed.clone();
        let now_ms = self.now_ms.clone();
        let ctx = self.ctx.clone();
        // Keep the app's polling interval: the plugin list is small and each poll is cheap.
        *timer = Some(tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
                if closed.load(Ordering::SeqCst) {
                    return;
                }
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
        }));
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
                out.insert("searchable".into(), json!(plugin.provider().is_some()));
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

    /// `configure`: followed feeds and the Soulseek sign-in take effect without a restart. The import
    /// folder is fixed at start.
    ///
    /// `replicate*` keys go to `crate::replicate::Replicator::configure`.
    pub fn configure(&self, config: MirrorConfig) {
        if let Some(next) = config.feeds {
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
        if config.slsk_user.is_some() || config.slsk_pass.is_some() || config.slsk_server.is_some()
        {
            self.soulseek.configure(
                config.slsk_user.as_deref(),
                config.slsk_pass.as_deref(),
                config.slsk_server.as_deref(),
            );
        }
        if let Some(onchange) = &self.onchange {
            onchange();
        }
    }

    /// Stop polling and start every plugin's stop before waiting. A save that is waiting for
    /// an import must receive its stop even if the importer's stop is still pending.
    /// Each call runs the stops again. The first call that finishes releases the traffic gate.
    pub async fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        if let Some(handle) = self.poll_task.lock().expect("poll task").take() {
            handle.abort();
        }
        let plugins: Vec<_> = self
            .plugins
            .lock()
            .expect("mirror plugins")
            .iter()
            .map(|(_, plugin)| plugin.clone())
            .collect();
        futures_util::future::join_all(plugins.iter().map(|plugin| plugin.stop())).await;
        let traffic = self.stop_traffic.lock().expect("traffic gate").take();
        if let Some(stop) = traffic {
            stop();
        }
        for handle in self.tasks.lock().expect("mirror tasks").drain(..) {
            handle.abort();
        }
    }

    /// `searchProvider(name, payload)`.
    pub async fn search_provider(&self, name: &str, payload: Value) -> Result<Value, MirrorError> {
        let plugin = self.plugin(name);
        match plugin.as_deref().and_then(MirrorPlugin::provider) {
            Some(provider) => provider.search(payload, &self.ctx).await,
            None => Err(unknown_provider(name)),
        }
    }

    /// `browseProvider(name, payload)`.
    pub async fn browse_provider(&self, name: &str, payload: Value) -> Result<Value, MirrorError> {
        let plugin = self.plugin(name);
        match plugin.as_deref().and_then(MirrorPlugin::provider) {
            Some(provider) => provider.browse(payload, &self.ctx).await,
            None => Err(unknown_provider(name)),
        }
    }

    /// `mirrorProvider(name, payload)`.
    pub async fn mirror_provider(&self, name: &str, payload: Value) -> Result<Value, MirrorError> {
        let plugin = self.plugin(name);
        match plugin.as_deref().and_then(MirrorPlugin::provider) {
            Some(provider) => provider.mirror(payload, &self.ctx).await,
            None => Err(unknown_provider(name)),
        }
    }

    /// `providerThumb(name, id)`: a result's picture from the provider `name`, when it has one.
    pub async fn provider_thumb(&self, name: &str, id: &str) -> Option<youtube::Picture> {
        let plugin = self.plugin(name)?;
        plugin.provider()?.thumbnail(id).await
    }

    /// `listProviderJobs()`: every provider's mirrors, for `/v1/jobs`.
    pub fn list_provider_jobs(&self) -> Vec<Value> {
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        plugins
            .iter()
            .filter_map(|(_, plugin)| plugin.provider())
            .flat_map(|provider| provider.list_jobs())
            .collect()
    }

    /// `getProviderJob(jobId)`.
    pub fn get_provider_job(&self, job_id: &str) -> Option<Value> {
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        plugins
            .iter()
            .filter_map(|(_, plugin)| plugin.provider())
            .find_map(|provider| provider.get_job(job_id))
    }

    /// `cancelProviderJob(jobId)`.
    pub fn cancel_provider_job(&self, job_id: &str) -> Option<Value> {
        let plugins = self.plugins.lock().expect("mirror plugins").clone();
        plugins
            .iter()
            .filter_map(|(_, plugin)| plugin.provider())
            .find_map(|provider| provider.cancel_job(job_id))
    }

    /// `forgetImported(gone)`: after the owner removed announces, the files this relay mirrored for
    /// them go too. `gone` is each removed announce's `(id, sha256)`; the answer says `deleted` or
    /// `kept` for each `id/sha256` it found a file for.
    pub async fn forget_imported(&self, gone: &[(String, String)]) -> Vec<(String, &'static str)> {
        self.watch_state.forget_imported(gone).await
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

/// The clock a host uses for stats and the ledger.
pub fn system_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
