//! The Rust acquirer and mirror host must reproduce the app's own calls and JSON for the same scripted sources.
//!
//! The oracles are `tests/js/acquire-mirror-acquire.mjs` and `tests/js/acquire-mirror-mirror.mjs`, which run the
//! copied app modules (`fixtures/gen/acquire.js`, `fixtures/gen/mirror.js`) against a scripted node, an in-memory
//! filesystem, scripted HTTP feeds and a fake `ubit`. Here the same scenarios drive fake implementations of every
//! seam; the two traces are compared as JSON.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peartube_native::acquire::{
    AcquireError, AcquireOptions, Acquirer, AddJob, HttpOpen, JobFiles, JobSource, PageDownloader,
    PageOptions, PutStreamFactory, RandomSource, SourceError, SourceRequest, SourceResponse,
    SourceSpec,
};
use peartube_native::mirror::{FileStat, MirrorFiles};
use peartube_native::node::api::{Announce, Entry, PutMeta, SearchOptions, SyncResult};
use peartube_native::node::{Node, PutSource};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

/// The node's error type in these tests; `AcquireError` is already `Send + Sync + 'static`.
type TestError = AcquireError;

fn js_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"))
}

/// Run one of the scenario harnesses and return its JSON trace.
fn js_trace(script: &str) -> Value {
    let output = std::process::Command::new("node")
        .arg(js_dir().join(script))
        .output()
        .unwrap_or_else(|error| panic!("run {script}: {error}"));
    assert!(
        output.status.success(),
        "{} failed: {}",
        script,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{script} did not print JSON: {error}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// A scripted `SourceResponse` body.
type BoxReader = Box<dyn AsyncRead + Send + Unpin>;

/// One `SourceResponse` body, delivered chunk by chunk so progress reports match the app's per-chunk reports.
struct BytesReader {
    chunks: VecDeque<Vec<u8>>,
}

impl BytesReader {
    fn boxed(chunks: Vec<Vec<u8>>) -> BoxReader {
        Box::new(Self {
            chunks: chunks.into(),
        })
    }
}

impl AsyncRead for BytesReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        match self.chunks.pop_front() {
            None => std::task::Poll::Ready(Ok(())),
            Some(chunk) => {
                let take = chunk.len().min(buf.remaining());
                buf.put_slice(&chunk[..take]);
                std::task::Poll::Ready(Ok(()))
            }
        }
    }
}

// ---------------------------------------------------------------- filesystem

#[derive(Default)]
struct Files {
    files: Mutex<BTreeMap<String, Vec<u8>>>,
    dirs: Mutex<BTreeSet<String>>,
    writes: Mutex<Vec<Value>>,
    temp: Mutex<u64>,
}

impl Files {
    fn new() -> Arc<Self> {
        let files = Self::default();
        files
            .dirs
            .lock()
            .unwrap()
            .extend(["/work".to_owned(), "/data".to_owned()]);
        Arc::new(files)
    }

    fn set(&self, path: &str, text: &str) {
        self.files
            .lock()
            .unwrap()
            .insert(path.to_owned(), text.as_bytes().to_vec());
    }

    fn read(&self, path: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(path).cloned()
    }

    fn writes(&self) -> Vec<Value> {
        self.writes.lock().unwrap().clone()
    }

    fn children(&self, dir: &str) -> Vec<String> {
        let prefix = format!("{}/", dir.trim_end_matches('/'));
        let mut names = BTreeSet::new();
        for key in self.files.lock().unwrap().keys() {
            if let Some(rest) = key.strip_prefix(&prefix)
                && let Some(first) = rest.split('/').next()
                && !first.is_empty()
            {
                names.insert(first.to_owned());
            }
        }
        for folder in self.dirs.lock().unwrap().iter() {
            if let Some(rest) = folder.strip_prefix(&prefix)
                && let Some(first) = rest.split('/').next()
                && !first.is_empty()
            {
                names.insert(first.to_owned());
            }
        }
        names.into_iter().collect()
    }
}

impl JobFiles for Files {
    fn read_jobs(&self) -> Option<String> {
        self.read("/work/jobs.json")
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }

    fn write_jobs(&self, json: &str) -> Result<(), String> {
        self.writes
            .lock()
            .unwrap()
            .push(json!(["/work/jobs.json.tmp", 384]));
        self.set("/work/jobs.json.tmp", json);
        self.files
            .lock()
            .unwrap()
            .insert("/work/jobs.json".to_owned(), json.as_bytes().to_vec());
        self.files.lock().unwrap().remove("/work/jobs.json.tmp");
        Ok(())
    }

    fn clean_temp(&self) {
        self.files
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with("/work/ubit-dl-"));
    }

    fn make_temp_dir(&self) -> Result<String, String> {
        let mut temp = self.temp.lock().unwrap();
        *temp += 1;
        let dir = format!("/work/ubit-dl-{:06}", *temp);
        self.dirs.lock().unwrap().insert(dir.clone());
        Ok(dir)
    }

    fn remove_dir(&self, path: &str) {
        self.files
            .lock()
            .unwrap()
            .retain(|key, _| !key.starts_with(&format!("{path}/")));
        self.dirs.lock().unwrap().remove(path);
    }

    fn file_size(&self, path: &str) -> Option<u64> {
        self.read(path).map(|bytes| bytes.len() as u64)
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String> {
        match self.read(path) {
            Some(bytes) => Ok(BytesReader::boxed(vec![bytes])),
            None => Err(format!("ENOENT: {path}")),
        }
    }

    fn config_dir(&self) -> Option<String> {
        Some("/work/ubit".to_owned())
    }
}

impl MirrorFiles for Files {
    fn exists(&self, path: &str) -> bool {
        self.files.lock().unwrap().contains_key(path) || self.dirs.lock().unwrap().contains(path)
    }

    fn mkdirs(&self, path: &str) {
        self.dirs.lock().unwrap().insert(path.to_owned());
    }

    fn list_dir(&self, dir: &str) -> Vec<String> {
        self.children(dir)
    }

    fn stat(&self, path: &str) -> Option<FileStat> {
        if let Some(bytes) = self.read(path) {
            return Some(FileStat {
                is_file: true,
                size: bytes.len() as u64,
                mtime_ms: 1000.5,
            });
        }
        self.dirs
            .lock()
            .unwrap()
            .contains(path)
            .then_some(FileStat {
                is_file: false,
                size: 0,
                mtime_ms: 1000.5,
            })
    }

    fn read(&self, path: &str) -> Option<String> {
        Files::read(self, path).map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
    }

    fn write_atomic(&self, path: &str, text: &str) -> Result<(), String> {
        self.set(path, text);
        Ok(())
    }

    fn open_read(&self, path: &str) -> Result<Box<dyn AsyncRead + Send + Unpin>, String> {
        JobFiles::open_read(self, path)
    }
}

// ---------------------------------------------------------------- randoms

struct Random {
    uuids: AtomicU64,
    blocks: AtomicU64,
}

impl Random {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            uuids: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
        })
    }
}

impl RandomSource for Random {
    fn uuid_v4(&self) -> String {
        let n = self.uuids.fetch_add(1, Ordering::SeqCst) + 1;
        format!("00000000-0000-4000-8000-{n:012}")
    }

    fn bytes(&self, len: usize) -> Vec<u8> {
        let n = self.blocks.fetch_add(1, Ordering::SeqCst) + 1;
        (0..len).map(|i| (n * 31 + i as u64) as u8).collect()
    }
}

// ---------------------------------------------------------------- HTTP

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    chunks: Vec<Vec<u8>>,
}

struct Http {
    plan: Mutex<VecDeque<Reply>>,
    calls: Arc<Mutex<Vec<Value>>>,
}

impl Http {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            plan: Mutex::new(VecDeque::new()),
            calls: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn plan(&self, replies: Vec<Reply>) {
        self.plan.lock().unwrap().extend(replies);
    }

    fn calls(&self) -> Vec<Value> {
        self.calls.lock().unwrap().clone()
    }
}

fn reply(status: u16, headers: &[(&str, &str)], chunks: &[&str]) -> Reply {
    Reply {
        status,
        headers: headers
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
        chunks: chunks
            .iter()
            .map(|chunk| chunk.as_bytes().to_vec())
            .collect(),
    }
}

impl HttpOpen for Http {
    fn open(
        &self,
        request: SourceRequest,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<SourceResponse, SourceError>> + Send>>
    {
        let mut headers = Map::new();
        for (key, value) in &request.headers {
            headers.insert(key.clone(), json!(value));
        }
        self.calls
            .lock()
            .unwrap()
            .push(json!({"url": request.url, "headers": headers}));
        let plan = self.plan.lock().unwrap().pop_front().unwrap_or(Reply {
            status: 500,
            headers: Vec::new(),
            chunks: Vec::new(),
        });
        Box::pin(async move {
            Ok(SourceResponse {
                status: plan.status,
                headers: plan.headers,
                url: request.url,
                body: BytesReader::boxed(plan.chunks),
            })
        })
    }
}

// ---------------------------------------------------------------- pages

struct PagePlan {
    size: Option<i64>,
    body: String,
    progress: Vec<(f64, Option<f64>)>,
}

struct Pages {
    plan: Mutex<VecDeque<PagePlan>>,
    files: Arc<Files>,
}

impl Pages {
    fn new(files: Arc<Files>) -> Arc<Self> {
        Arc::new(Self {
            plan: Mutex::new(VecDeque::new()),
            files,
        })
    }
}

impl PageDownloader for Pages {
    fn download(
        &self,
        _url: &str,
        dir: &str,
        opts: PageOptions,
    ) -> Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<peartube_native::identify::Downloaded, AcquireError>,
                > + Send,
        >,
    > {
        let plan = self.plan.lock().unwrap().pop_front();
        let files = self.files.clone();
        let dir = dir.to_owned();
        Box::pin(async move {
            let Some(plan) = plan else {
                return Err(AcquireError::Message("Source stalled".into()));
            };
            if let Some(mut on_progress) = opts.on_progress {
                for (bytes, total) in plan.progress {
                    on_progress(peartube_native::identify::DownloadProgress {
                        bytes: peartube_native::identify::JsNumber(bytes),
                        total: total.map(peartube_native::identify::JsNumber),
                    });
                }
            }
            let path = format!("{dir}/clip.mp4");
            files.set(&path, &plan.body);
            Ok(peartube_native::identify::Downloaded {
                path,
                size: plan.size,
            })
        })
    }
}

// ---------------------------------------------------------------- put source

enum Stream {
    Job(JobSource<TestError>),
    Plain(JobSource<TestError>),
}

struct BoxedSource(Stream);

impl PutSource for BoxedSource {
    type Error = TestError;

    async fn open(
        &mut self,
        offset: u64,
        meta: &mut PutMeta,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>, Self::Error> {
        match &mut self.0 {
            Stream::Job(job) | Stream::Plain(job) => {
                Ok(Box::into_pin(job.open(offset, meta).await?))
            }
        }
    }

    fn on_data(&mut self, bytes: u64) {
        match &mut self.0 {
            Stream::Job(job) | Stream::Plain(job) => job.on_data(bytes),
        }
    }

    fn on_hash_state(&mut self, state: &[u8], bytes: u64) {
        match &mut self.0 {
            Stream::Job(job) | Stream::Plain(job) => job.on_hash_state(state, bytes),
        }
    }
}

struct Factory;

impl PutStreamFactory<FakeNode> for Factory {
    fn resumable(&self, source: JobSource<TestError>) -> BoxedSource {
        BoxedSource(Stream::Job(source))
    }

    fn reader(&self, source: JobSource<TestError>) -> BoxedSource {
        BoxedSource(Stream::Plain(source))
    }
}

// ---------------------------------------------------------------- node

#[derive(Clone, Copy, Default)]
struct PutPlan {
    start: u64,
    hash: Option<u64>,
}

type MigrationFn = Arc<dyn Fn(&str, Value) + Send + Sync>;
type ProgressFn = Arc<dyn Fn(Value) + Send + Sync>;

#[derive(Clone)]
struct FakeRechunkOptions {
    id: Option<String>,
    foreground: bool,
    limit: Option<u64>,
    on_migrate: MigrationFn,
    on_progress: ProgressFn,
}

struct FakeNode {
    calls: Arc<Mutex<Vec<Value>>>,
    put_plan: Mutex<VecDeque<PutPlan>>,
    entries: Mutex<Vec<Value>>,
    backfill: Mutex<SyncResult>,
    rechunk_progress: Mutex<Vec<Value>>,
    rechunk_migrate: Mutex<Vec<Value>>,
    rechunk_result: Mutex<Value>,
    rechunk_wait: Mutex<Option<Arc<tokio::sync::Notify>>>,
}

impl FakeNode {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            put_plan: Mutex::new(VecDeque::new()),
            entries: Mutex::new(Vec::new()),
            backfill: Mutex::new(SyncResult {
                backfilled: 0,
                total: 0,
            }),
            rechunk_progress: Mutex::new(Vec::new()),
            rechunk_migrate: Mutex::new(Vec::new()),
            rechunk_result: Mutex::new(
                json!({"total": 0, "migrated": 0, "deduped": 0, "remaining": 0, "migratedBytes": 0}),
            ),
            rechunk_wait: Mutex::new(None),
        })
    }

    fn push(&self, call: Value) {
        self.calls.lock().unwrap().push(call);
    }
}

struct NoSwarm;

impl peartube_native::node::api::Swarm for NoSwarm {
    type Error = TestError;

    async fn suspend(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn resume(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| ())
    }
}

static SWARM: NoSwarm = NoSwarm;

impl Node for FakeNode {
    type Error = TestError;
    type Store = ();
    type Core = ();
    type Channel = ();
    type Swarm = NoSwarm;
    type Dht = ();
    type Connection = ();
    type ConnectionSubscription = ();
    type Lan = ();
    type BlobServer = ();
    type PutSource = BoxedSource;
    type RechunkOptions = FakeRechunkOptions;

    fn store(&self) -> &Self::Store {
        &()
    }
    fn channel(&self) -> &Self::Channel {
        &()
    }
    fn swarm(&self) -> &Self::Swarm {
        &SWARM
    }
    fn dht(&self) -> &Self::Dht {
        &()
    }
    fn blob_server(&self) -> &Self::BlobServer {
        &()
    }
    fn private_routing(&self) -> bool {
        false
    }
    fn bootstrap(&self) -> Option<Value> {
        None
    }
    fn relay_through(&self) -> Option<Value> {
        None
    }
    fn peer_key(&self) -> String {
        String::new()
    }
    fn closing(&self) -> bool {
        false
    }
    fn peer_connections(&self) -> Vec<Self::Connection> {
        Vec::new()
    }
    fn on_connection(
        &self,
        _callback: Box<dyn Fn(Self::Connection) + Send + Sync>,
    ) -> Self::ConnectionSubscription {
    }
    fn onchange(&self) -> tokio::sync::watch::Receiver<u64> {
        tokio::sync::watch::channel(0).1
    }

    async fn find_entry(&self, _key: &str) -> Result<Option<Value>, Self::Error> {
        unimplemented!("acquire/mirror do not read raw entries")
    }

    async fn blob_core(&self, _key: &str) -> Result<Self::Core, Self::Error> {
        unimplemented!("replication is out of scope")
    }

    async fn seedable_cores(
        &self,
        _include_cached: bool,
    ) -> Result<Vec<peartube_native::node::api::SeedableCore>, Self::Error> {
        unimplemented!("hiverelay owns seeding")
    }

    async fn has_lan(&self) -> bool {
        false
    }

    async fn lan_endpoint(&self) -> Option<peartube_native::node::api::LanEndpoint> {
        None
    }

    async fn dial_lan_peer(&self, _endpoint: peartube_native::node::api::LanEndpoint) {}

    async fn join_peer(&self, _key: &str, _lan_first: bool) {}

    async fn evict_behind(
        &self,
        _blobs: &str,
        _blob: peartube_native::node::api::Blob,
        _options: Value,
    ) -> Result<u64, Self::Error> {
        Ok(0)
    }

    async fn refresh_block_count(&self, _core: &Self::Core) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn search(
        &self,
        id: Option<&str>,
        _options: SearchOptions,
    ) -> Result<Vec<Entry>, Self::Error> {
        self.push(json!({"method": "search", "id": id}));
        let entries = self.entries.lock().unwrap().clone();
        Ok(entries
            .iter()
            .filter(|entry| id.is_none_or(|id| entry.get("id").and_then(Value::as_str) == Some(id)))
            .filter_map(|entry| serde_json::from_value(entry.clone()).ok())
            .collect())
    }

    async fn put(
        &self,
        mut meta: PutMeta,
        mut source: BoxedSource,
    ) -> Result<Announce, Self::Error> {
        let plan = self
            .put_plan
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_default();
        let is_job = matches!(source.0, Stream::Job(_));
        // The app records the put before touching the source, so an open failure still leaves the call.
        let meta_before = serde_json::to_value(&meta)
            .map_err(|error| AcquireError::Message(error.to_string()))?;
        self.push(json!({
            "method": "put",
            "meta": meta_before,
            "start": if is_job { json!(plan.start) } else { Value::Null },
            "progress": [],
        }));
        let index = self.calls.lock().unwrap().len() - 1;
        if let Some(hash) = plan.hash {
            source.on_hash_state(b"checkpoint", hash);
        }
        let mut reader = source.open(plan.start, &mut meta).await?;
        let mut bytes = Vec::new();
        let mut progress = Vec::new();
        let mut chunk = vec![0u8; 65536];
        loop {
            let read = reader
                .read(&mut chunk)
                .await
                .map_err(|error| AcquireError::Message(error.to_string()))?;
            if read == 0 {
                break;
            }
            bytes.extend_from_slice(&chunk[..read]);
            let total = bytes.len() as u64 + if is_job { plan.start } else { 0 };
            source.on_data(total);
            progress.push(json!(total));
            self.calls.lock().unwrap()[index]["progress"] = Value::Array(progress.clone());
        }
        let meta = serde_json::to_value(&meta)
            .map_err(|error| AcquireError::Message(error.to_string()))?;
        self.calls.lock().unwrap()[index]["meta"] = meta.clone();
        let announce = announce_for_json(&meta, &bytes);
        serde_json::from_value(announce).map_err(|error| AcquireError::Message(error.to_string()))
    }

    async fn drop_store(&self, name: &str) -> Result<(), Self::Error> {
        self.push(json!({"method": "dropStore", "name": name}));
        Ok(())
    }

    async fn append(&self, op: Value) -> Result<(), Self::Error> {
        self.push(json!({"method": "append", "op": op}));
        Ok(())
    }

    async fn publish(
        &self,
        _meta: PutMeta,
        _source: Self::PutSource,
    ) -> Result<peartube_native::node::api::Published, Self::Error> {
        unimplemented!("the mirror host publishes through append")
    }

    async fn publish_channel(
        &self,
        _meta_or_op: Value,
        _source: Option<Self::PutSource>,
    ) -> Result<peartube_native::node::api::Published, Self::Error> {
        unimplemented!("the mirror host publishes through append")
    }

    async fn remove(&self, _key: &str) -> Result<(), Self::Error> {
        unimplemented!("the mirror host does not remove entries")
    }

    async fn publish_source(&self, source_key: &str, title: &str) -> Result<Value, Self::Error> {
        self.push(json!({"method": "publishSource", "sourceKey": source_key, "title": title}));
        Ok(json!({"type": "source", "sourceKey": source_key, "kind": "channel", "title": title}))
    }

    async fn sync_local_channel(&self) -> Result<SyncResult, Self::Error> {
        self.push(json!({"method": "syncLocalChannel"}));
        Ok(self.backfill.lock().unwrap().clone())
    }

    async fn rechunk_legacy_blobs(
        &self,
        options: Self::RechunkOptions,
    ) -> Result<peartube_native::node::api::RechunkResult, Self::Error> {
        self.push(json!({
            "method": "rechunk",
            "id": options.id,
            "foreground": options.foreground,
            "limit": options.limit,
        }));
        for update in self.rechunk_progress.lock().unwrap().clone() {
            (options.on_progress)(update);
        }
        for target in self.rechunk_migrate.lock().unwrap().clone() {
            let old = target
                .get("old")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            (options.on_migrate)(&old, target.get("target").cloned().unwrap_or(Value::Null));
        }
        let wait = self.rechunk_wait.lock().unwrap().clone();
        if let Some(notify) = wait {
            notify.notified().await;
        }
        serde_json::from_value(self.rechunk_result.lock().unwrap().clone())
            .map_err(|error| AcquireError::Message(error.to_string()))
    }

    async fn unpublish_source(&self, _key: &str) -> Result<Value, Self::Error> {
        unimplemented!("the mirror host does not unpublish")
    }

    async fn published_sources(
        &self,
    ) -> Result<Vec<peartube_native::node::api::PublishedSource>, Self::Error> {
        Ok(Vec::new())
    }

    async fn add_source(
        &self,
        _spec: peartube_native::node::api::SourceSpec,
        _wake: bool,
    ) -> Result<peartube_native::node::api::Source, Self::Error> {
        unimplemented!("feeding sources is the http server's job")
    }

    async fn remove_source(
        &self,
        _key: &str,
    ) -> Result<peartube_native::node::api::Removed, Self::Error> {
        unimplemented!("feeding sources is the http server's job")
    }

    async fn list_sources(&self) -> Vec<peartube_native::node::api::Source> {
        Vec::new()
    }

    async fn focus_source(&self, _key: &str) -> Result<bool, Self::Error> {
        Ok(false)
    }

    async fn set_player_active(&self, _active: bool) -> peartube_native::node::api::OkResult {
        peartube_native::node::api::OkResult { ok: true }
    }

    async fn active_source_count(&self) -> usize {
        0
    }

    async fn overlay_topic_count(&self) -> usize {
        0
    }

    async fn discovered_sources(
        &self,
    ) -> Result<Vec<peartube_native::node::api::DiscoveredSource>, Self::Error> {
        Ok(Vec::new())
    }

    async fn open_uri(
        &self,
        _uri: &str,
        _join_source: bool,
    ) -> Result<peartube_native::node::api::OpenUri, Self::Error> {
        unimplemented!("the mirror host does not play media")
    }

    async fn warm(&self, entry: &Value) -> Result<(), Self::Error> {
        self.push(json!({"method": "warm", "entry": entry}));
        Ok(())
    }

    async fn identity(
        &self,
        _reveal_mnemonic: bool,
    ) -> Result<peartube_native::node::api::Identity, Self::Error> {
        unimplemented!("the mirror host does not read identities")
    }

    async fn grant_writer(
        &self,
        _writer: &str,
        _role: &str,
        _alias: &str,
    ) -> Result<peartube_native::node::api::Grant, Self::Error> {
        unimplemented!("the mirror host does not manage writers")
    }

    async fn revoke_writer(
        &self,
        _writer: &str,
        _purge_entries: bool,
    ) -> Result<peartube_native::node::api::Revoke, Self::Error> {
        unimplemented!("the mirror host does not manage writers")
    }

    async fn claim_with_mnemonic(
        &self,
        _mnemonic: &str,
        _alias: &str,
    ) -> Result<peartube_native::node::api::Claim, Self::Error> {
        unimplemented!("the mirror host does not claim")
    }

    async fn rotate_epoch(&self) -> Result<peartube_native::node::api::Epoch, Self::Error> {
        unimplemented!("the mirror host does not rotate epochs")
    }

    async fn set_allowed_peers(&self, _peers: Option<Vec<String>>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn clear_cache(
        &self,
        _key: Option<&str>,
        _keep_blobs_hex: Option<&str>,
    ) -> Result<peartube_native::node::api::CacheCleared, Self::Error> {
        Ok(peartube_native::node::api::CacheCleared {
            cleared_bytes: 0,
            reclaimed_bytes: 0,
        })
    }

    async fn set_cache_max_bytes(&self, bytes: f64) -> u64 {
        bytes as u64
    }

    async fn enforce_cache_ceiling(
        &self,
        _keep_hex: Option<&str>,
    ) -> Result<peartube_native::node::api::CacheCeiling, Self::Error> {
        Ok(peartube_native::node::api::CacheCeiling {
            evicted: 0,
            cached_bytes: 0,
        })
    }

    async fn gc_storage(
        &self,
        _full: bool,
        _streaming: bool,
    ) -> Result<peartube_native::node::api::StorageGc, Self::Error> {
        self.push(json!({"method": "gcStorage"}));
        Ok(peartube_native::node::api::StorageGc {
            before_bytes: 0,
            after_bytes: 0,
            reclaimed_bytes: 0,
        })
    }

    async fn status(&self) -> peartube_native::node::api::Status {
        unimplemented!("the mirror host only reads status while replicating")
    }

    async fn stream_url(&self, _entry: &Announce) -> String {
        String::new()
    }

    async fn http_viewing(&self) -> bool {
        false
    }

    async fn mark_bulk_peer(&self, _key: &str, _on: bool) {}

    async fn set_lan(&self, _lan: Option<Self::Lan>) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn lan_stats(&self) -> Option<Value> {
        None
    }

    async fn close(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

fn announce_for_json(meta: &Value, bytes: &[u8]) -> Value {
    let id = meta.get("id").and_then(Value::as_str).unwrap_or_default();
    let title = meta
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let size = bytes.len() as u64;
    json!({
        "type": "announce",
        "id": id,
        "title": title,
        "size": size,
        "sha256": sha256_hex(bytes),
        "blobs": format!("blobs:{}", &sha256_hex(id.as_bytes())[..8]),
        "blob": {
            "blockOffset": 0,
            "blockLength": 1.max(size.div_ceil(65536)),
            "byteOffset": 0,
            "byteLength": size,
        },
    })
}

// ---------------------------------------------------------------- acquire scenarios

const NOW: u64 = 1_700_000_000_000;

struct Harness {
    node: Arc<FakeNode>,
    files: Arc<Files>,
    http: Arc<Http>,
    pages: Arc<Pages>,
    acquirer: Option<Acquirer<FakeNode>>,
    done: Arc<Mutex<Vec<Value>>>,
}

impl Harness {
    fn new(path: AcquirePath) -> Self {
        Self::with_jobs(path, None)
    }

    fn with_jobs(path: AcquirePath, jobs: Option<Value>) -> Self {
        let node = FakeNode::new();
        let files = Files::new();
        if let Some(jobs) = jobs {
            files.set("/work/jobs.json", &jobs.to_string());
        }
        let http = Http::new();
        let random = Random::new();
        let pages = Pages::new(files.clone());
        let done = Arc::new(Mutex::new(Vec::new()));
        let done_sink = done.clone();
        let opts = AcquireOptions {
            concurrency: path.concurrency(),
            retry_delay: Duration::ZERO,
            ubit_config_dir: Some("/work/ubit".to_owned()),
            now_ms: Arc::new(|| NOW),
            on_done: Some(Arc::new(move |job, op| {
                done_sink
                    .lock()
                    .unwrap()
                    .push(json!({"method": "onDone", "job": job, "op": op}));
            })),
            ..AcquireOptions::default()
        };
        let acquirer = Acquirer::new(
            node.clone(),
            files.clone(),
            pages.clone(),
            http.clone(),
            Arc::new(Factory),
            random.clone(),
            opts,
        );
        Self {
            node,
            files,
            http,
            pages,
            acquirer: Some(acquirer),
            done,
        }
    }

    fn trace(&self) -> Value {
        let acquirer = self.acquirer.as_ref().unwrap();
        let mut calls = self.node.calls.lock().unwrap().clone();
        calls.extend(self.done.lock().unwrap().clone());
        let saved = self
            .files
            .read("/work/jobs.json")
            .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap())
            .unwrap_or(Value::Array(Vec::new()));
        json!({
            "calls": calls,
            "list": acquirer.list(),
            "saved": saved,
            "modes": self.files.writes(),
            "http": self.http.calls(),
        })
    }
}

fn add_film(harness: &Harness, id: &str, title: &str, url: &str, headers: &[(&str, &str)]) {
    let acquirer = harness.acquirer.as_ref().unwrap();
    acquirer
        .add(AddJob {
            id: id.to_owned(),
            title: title.to_owned(),
            source: SourceSpec {
                url: url.to_owned(),
                headers: headers
                    .iter()
                    .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
                    .collect(),
                page: false,
                private: false,
            },
            channel: None,
            private: false,
            plugin: None,
        })
        .unwrap();
}

async fn settle(harness: &Harness) {
    harness.acquirer.as_ref().unwrap().wait_idle().await;
}

#[tokio::test]
async fn acquirer_matches_the_app() {
    let oracle = js_trace("acquire-mirror-acquire.mjs");
    let mut traces = Map::new();

    {
        let harness = Harness::new(AcquirePath::Acquirer);
        harness.http.plan(vec![reply(
            200,
            &[
                ("content-length", "7"),
                ("etag", "\"strong\""),
                ("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
            ],
            &["abc", "defg"],
        )]);
        harness.node.put_plan.lock().unwrap().push_back(PutPlan {
            start: 0,
            hash: Some(3),
        });
        add_film(
            &harness,
            "imdb:tt1234567",
            "Film",
            "http://source.test/film.mp4",
            &[("authorization", "Bearer secret")],
        );
        settle(&harness).await;
        traces.insert("download".into(), harness.trace());
    }

    {
        let harness = Harness::with_jobs(
            AcquirePath::Acquirer,
            Some(json!([{
                "jobId": "aaaaaaaa-0000-4000-8000-000000000001",
                "id": "imdb:tt1234567",
                "title": "Film",
                "plugin": "http-acquire",
                "channel": false,
                "private": false,
                "status": "running",
                "created": NOW,
                "store": "video:resume",
                "source": {"url": "http://source.test/film.mp4", "headers": {}, "page": false},
                "total": 10,
                "bytes": 4,
                "etag": "\"v1\"",
                "lastModified": null,
                "hashState": null,
                "hashBytes": 0
            }])),
        );
        harness.http.plan(vec![reply(
            206,
            &[("content-range", "bytes 4-9/10"), ("etag", "\"v1\"")],
            &["ghij", "kl"],
        )]);
        harness.node.put_plan.lock().unwrap().push_back(PutPlan {
            start: 4,
            hash: None,
        });
        settle(&harness).await;
        traces.insert("resume".into(), harness.trace());
    }

    {
        let harness = Harness::new(AcquirePath::Acquirer);
        harness.http.plan(vec![reply(404, &[], &[])]);
        add_film(
            &harness,
            "imdb:tt7654321",
            "Gone",
            "http://source.test/gone.mp4",
            &[],
        );
        settle(&harness).await;
        traces.insert("terminal".into(), harness.trace());
    }

    {
        let harness = Harness::new(AcquirePath::Acquirer);
        harness.http.plan(vec![
            reply(302, &[("location", "http://cdn.test/film.mp4")], &[]),
            reply(200, &[("content-length", "3")], &["xyz"]),
        ]);
        add_film(
            &harness,
            "imdb:tt1111111",
            "Moved",
            "http://source.test/film.mp4",
            &[("authorization", "Bearer secret"), ("cookie", "a=b")],
        );
        settle(&harness).await;
        traces.insert("redirect".into(), harness.trace());
    }

    {
        let harness = Harness::new(AcquirePath::Acquirer);
        harness.pages.plan.lock().unwrap().push_back(PagePlan {
            size: Some(7),
            body: "page-7".to_owned(),
            progress: vec![(3.0, Some(7.0))],
        });
        harness
            .acquirer
            .as_ref()
            .unwrap()
            .add(AddJob {
                id: "imdb:tt2222222".to_owned(),
                title: "Page".to_owned(),
                source: SourceSpec {
                    url: "http://page.test/watch/2".to_owned(),
                    headers: Vec::new(),
                    page: true,
                    private: false,
                },
                channel: None,
                private: false,
                plugin: None,
            })
            .unwrap();
        settle(&harness).await;
        traces.insert("page".into(), harness.trace());
    }

    {
        let harness = Harness::with_jobs(
            AcquirePath::Acquirer,
            Some(json!([{
                "jobId": "bbbbbbbb-0000-4000-8000-000000000002",
                "id": "imdb:tt3333333",
                "title": "Done",
                "plugin": "http-acquire",
                "channel": false,
                "private": false,
                "status": "done",
                "created": NOW,
                "store": "video:done",
                "source": null,
                "total": 4,
                "bytes": 4,
                "announce": {"type": "announce", "id": "imdb:tt3333333", "title": "Done", "size": 4, "sha256": "a".repeat(64), "blobs": "AAAA", "blob": {"blockOffset": 5, "blockLength": 1, "byteOffset": 0, "byteLength": 4}},
                "size": 4,
                "sha256": "a".repeat(64),
                "hashState": null,
                "hashBytes": 0
            }])),
        );
        settle(&harness).await;
        let acquirer = harness.acquirer.as_ref().unwrap();
        harness.node.push(json!({
            "method": "repoint",
            "updated": acquirer.repoint_blob(
                "AAAA:5",
                &json!({"blobs": "BBBB", "blob": {"blockOffset": 7, "blockLength": 1, "byteOffset": 0, "byteLength": 4}}),
            ),
        }));
        harness.node.push(json!({
            "method": "cancel",
            "result": acquirer.cancel("bbbbbbbb-0000-4000-8000-000000000002"),
        }));
        harness
            .node
            .push(json!({"method": "cancel", "result": acquirer.cancel("nope")}));
        harness.node.push(json!({
            "method": "get",
            "result": acquirer.get("bbbbbbbb-0000-4000-8000-000000000002"),
        }));
        traces.insert("cancel".into(), harness.trace());
    }

    compare(&Value::Object(traces), &oracle);
}

fn compare(rust: &Value, js: &Value) {
    let rust = rust.as_object().unwrap();
    for (name, expected) in js.as_object().unwrap() {
        let actual = rust
            .get(name)
            .unwrap_or_else(|| panic!("missing scenario {name}"));
        for (field, value) in expected.as_object().unwrap() {
            assert_eq!(
                actual.get(field),
                Some(value),
                "scenario {name}.{field}\nrust: {}\njs:   {}",
                actual.get(field).unwrap_or(&Value::Null),
                value
            );
        }
    }
}

/// How a harness is driven; the mirror's own harness keeps jobs queued for its dedupe checks.
#[derive(Clone, Copy)]
enum AcquirePath {
    Acquirer,
}

impl AcquirePath {
    fn concurrency(self) -> usize {
        match self {
            Self::Acquirer => 1,
        }
    }
}

// ---------------------------------------------------------------- mirror

use peartube_native::art;
use peartube_native::mirror::{
    FeedFetch, MirrorHost, MirrorOptions, RechunkOptions, RechunkOptionsFactory,
};

struct RechunkFactory;

impl RechunkOptionsFactory<FakeNode> for RechunkFactory {
    fn make(&self, options: RechunkOptions) -> FakeRechunkOptions {
        FakeRechunkOptions {
            id: options.id,
            foreground: options.foreground,
            limit: options.limit,
            on_migrate: Arc::from(options.on_migrate),
            on_progress: Arc::from(options.on_progress),
        }
    }
}

struct Feeds {
    plan: Mutex<BTreeMap<String, Value>>,
}

impl Feeds {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            plan: Mutex::new(BTreeMap::new()),
        })
    }

    fn set(&self, url: &str, body: Value) {
        self.plan.lock().unwrap().insert(url.to_owned(), body);
    }
}

impl FeedFetch for Feeds {
    fn get(
        &self,
        url: &str,
        _headers: Vec<(String, String)>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<art::HttpResponse, art::HttpError>> + Send>>
    {
        let body = self.plan.lock().unwrap().get(url).cloned();
        let url = url.to_owned();
        Box::pin(async move {
            let Some(body) = body else {
                return Ok(art::HttpResponse {
                    status: 404,
                    headers: Vec::new(),
                    url,
                    body: String::new(),
                });
            };
            Ok(art::HttpResponse {
                status: 200,
                headers: Vec::new(),
                url,
                body: body.to_string(),
            })
        })
    }
}

struct MirrorHarness {
    node: Arc<FakeNode>,
    files: Arc<Files>,
    acquirer: Acquirer<FakeNode>,
    host: MirrorHost<FakeNode>,
}

impl MirrorHarness {
    async fn new(seed_jobs: Value) -> Self {
        Self::with_feeds(seed_jobs, Feeds::new()).await
    }

    async fn with_feeds(seed_jobs: Value, feeds: Arc<Feeds>) -> Self {
        let node = FakeNode::new();
        let files = Files::new();
        files.mkdirs("/data");
        if !seed_jobs.as_array().is_none_or(Vec::is_empty) {
            files.set("/work/jobs.json", &seed_jobs.to_string());
        }
        let random = Random::new();
        let opts = AcquireOptions {
            concurrency: 0,
            retry_delay: Duration::ZERO,
            now_ms: Arc::new(|| NOW),
            ..AcquireOptions::default()
        };
        let acquirer = Acquirer::new(
            node.clone(),
            files.clone(),
            Pages::new(files.clone()),
            Http::new(),
            Arc::new(Factory),
            random,
            opts,
        );
        let host = MirrorHost::start(
            node.clone(),
            Some(acquirer.clone()),
            files.clone(),
            Arc::new(Factory),
            Arc::new(RechunkFactory),
            feeds,
            Vec::new(),
            MirrorOptions {
                storage: "/data".to_owned(),
                mirror_channel: false,
                rechunk: false,
                watch_dir: None,
                feeds: Vec::new(),
                plugins_dir: Some("/data/plugins".to_owned()),
                poll_interval_ms: 0,
                now_ms: Arc::new(|| NOW),
                ..MirrorOptions::default()
            },
        )
        .await;
        host.wait_idle().await;
        Self {
            node,
            files,
            acquirer,
            host,
        }
    }

    fn trace(&self) -> Value {
        let ledger = self
            .files
            .read("/data/mirror-watch.json")
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or(Value::Null);
        let jobs = self
            .files
            .read("/work/jobs.json")
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let list: Vec<Value> = self
            .host
            .list()
            .into_iter()
            .filter(|plugin| {
                let name = plugin
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                [
                    "http-acquire",
                    "channel-backfill",
                    "blob-rechunk",
                    "watch-dir",
                    "feed-mirror",
                ]
                .contains(&name)
            })
            .collect();
        json!({
            "calls": self.node.calls.lock().unwrap().clone(),
            "list": list,
            "ledger": ledger,
            "jobs": jobs,
            "acquirerList": self.acquirer.list(),
        })
    }
}

fn local_entry(id: &str, title: &str) -> Value {
    json!({
        "key": format!("k:{id}"),
        "id": id,
        "title": title,
        "size": 4,
        "sha256": "c".repeat(64),
        "local": true,
        "cachedBytes": 4,
        "private": false,
        "epoch": 0,
        "locked": false,
        "source": "channel",
        "sourceKind": "channel",
        "season": null,
        "episode": null,
        "seriesKey": id,
        "baseTitle": title,
        "year": null,
        "streamUrl": "",
        "magnetUrl": ""
    })
}

#[tokio::test]
async fn mirror_host_matches_the_app() {
    let oracle = js_trace("acquire-mirror-mirror.mjs");
    let mut traces = Map::new();

    {
        let harness = MirrorHarness::new(Value::Array(Vec::new())).await;
        *harness.node.backfill.lock().unwrap() = SyncResult {
            backfilled: 2,
            total: 3,
        };
        let result = harness
            .host
            .run("channel-backfill", Value::Null)
            .await
            .unwrap();
        let mut trace = harness.trace();
        trace
            .as_object_mut()
            .unwrap()
            .insert("result".into(), result);
        traces.insert("backfill".into(), trace);
        harness.host.close().await;
    }

    {
        let feeds = Feeds::new();
        feeds.set(
            "http://feed.test/v1/entries",
            json!([
                {"id": "IMDB:TT1234567", "title": "Film", "url": "http://cdn/film.mp4"},
                {"id": "imdb:tt1234567", "title": "Dup"},
                {"id": "imdb:tt2222222", "sha256": "b".repeat(64), "source": {"url": "http://cdn/b.mp4", "headers": {"authorization": "Bearer b"}}},
                {"id": "not an id"},
                {"id": "clip:x", "url": "ftp://cdn/x.mp4"},
                {"id": "imdb:tt3333333", "url": "http://cdn/local.mp4"}
            ]),
        );
        let harness = MirrorHarness::with_feeds(Value::Array(Vec::new()), feeds).await;
        harness
            .node
            .entries
            .lock()
            .unwrap()
            .push(local_entry("imdb:tt3333333", "Local"));
        let result = harness
            .host
            .run(
                "feed-mirror",
                json!({"url": "http://feed.test/v1/entries", "headers": {"x-token": "t"}}),
            )
            .await
            .unwrap();
        let mut trace = harness.trace();
        trace
            .as_object_mut()
            .unwrap()
            .insert("result".into(), result);
        traces.insert("feed".into(), trace);
        harness.host.close().await;
    }

    {
        let harness = MirrorHarness::new(Value::Array(Vec::new())).await;
        harness
            .node
            .entries
            .lock()
            .unwrap()
            .push(local_entry("imdb:tt4444444", "Manifest Local"));
        let result = harness
            .host
            .run(
                "feed-mirror",
                json!({
                    "headers": {"x-token": "m"},
                    "items": [
                        {"id": "imdb:tt4444444", "title": "Present"},
                        {"id": "imdb:tt5555555", "title": "New", "source": {"url": "http://cdn/new.mp4", "headers": {"x-item": "i"}}},
                        {"id": "imdb:tt6666666", "title": "Bad", "source": {"url": "/relative"}},
                        "not an object"
                    ]
                }),
            )
            .await
            .unwrap();
        let mut trace = harness.trace();
        trace
            .as_object_mut()
            .unwrap()
            .insert("result".into(), result);
        traces.insert("manifest".into(), trace);
        harness.host.close().await;
    }

    {
        let harness = MirrorHarness::new(Value::Array(Vec::new())).await;
        harness.files.mkdirs("/data/import");
        harness.files.set("/data/import/Some.Film.2019.mkv", "film");
        harness.files.set("/data/import/Show.S01E02.mkv", "episode");
        harness
            .files
            .set("/data/import/Movie.tt1234567.S01E02.1080p.mkv", "movie");
        harness.files.set("/data/import/Clip.mkv", "clip");
        harness.files.set(
            "/data/import/Clip.mkv.json",
            &json!({"id": "imdb:tt7654321", "title": "Sidecar Title"}).to_string(),
        );
        harness.files.set("/data/import/notes.txt", "ignore me");
        harness.files.set("/data/import/empty.mp4", "");
        let first = harness
            .host
            .run("watch-dir", json!({"dir": "/data/import"}))
            .await
            .unwrap();
        let second = harness
            .host
            .run("watch-dir", json!({"dir": "/data/import"}))
            .await
            .unwrap();
        harness
            .files
            .set("/data/import/Show.S01E02.mkv", "episode, now longer");
        let third = harness
            .host
            .run("watch-dir", json!({"dir": "/data/import"}))
            .await
            .unwrap();
        let mut trace = harness.trace();
        trace
            .as_object_mut()
            .unwrap()
            .insert("runs".into(), json!([first, second, third]));
        traces.insert("watch".into(), trace);
        harness.host.close().await;
    }

    {
        let harness = MirrorHarness::new(json!([{
            "jobId": "cccccccc-0000-4000-8000-000000000003",
            "id": "imdb:tt7777777",
            "title": "Done",
            "plugin": "http-acquire",
            "channel": false,
            "private": false,
            "status": "done",
            "created": NOW,
            "store": "video:rechunk",
            "source": null,
            "total": 4,
            "bytes": 4,
            "announce": {"type": "announce", "id": "imdb:tt7777777", "title": "Done", "size": 4, "sha256": "d".repeat(64), "blobs": "AAAA", "blob": {"blockOffset": 5, "blockLength": 1, "byteOffset": 0, "byteLength": 4}},
            "size": 4,
            "sha256": "d".repeat(64),
            "hashState": null,
            "hashBytes": 0
        }]))
        .await;
        *harness.node.rechunk_progress.lock().unwrap() = vec![
            json!({"total": 2, "migrated": 0, "deduped": 0, "remaining": 2, "migratedBytes": 0, "currentId": "imdb:tt7777777", "currentTitle": "Done"}),
            json!({"total": 2, "migrated": 1, "deduped": 0, "remaining": 1, "migratedBytes": 4, "currentId": "imdb:tt8888888", "currentTitle": "Other"}),
        ];
        *harness.node.rechunk_migrate.lock().unwrap() = vec![json!({
            "old": "AAAA:5",
            "target": {"blobs": "BBBB", "blob": {"blockOffset": 7, "blockLength": 1, "byteOffset": 0, "byteLength": 4}}
        })];
        *harness.node.rechunk_result.lock().unwrap() =
            json!({"total": 2, "migrated": 1, "deduped": 1, "remaining": 1, "migratedBytes": 4});
        let result = harness
            .host
            .run(
                "blob-rechunk",
                json!({"id": "imdb:tt7777777", "foreground": true, "limit": 5}),
            )
            .await
            .unwrap();
        let mut trace = harness.trace();
        trace
            .as_object_mut()
            .unwrap()
            .insert("result".into(), result);
        traces.insert("rechunk".into(), trace);
        harness.host.close().await;
    }

    {
        let harness = MirrorHarness::new(Value::Array(Vec::new())).await;
        let error = harness
            .host
            .run("nope", Value::Null)
            .await
            .map(|_| Value::Null)
            .unwrap_or_else(|error| json!({"message": error.message, "status": error.status}));
        let mut trace = harness.trace();
        trace.as_object_mut().unwrap().insert("error".into(), error);
        traces.insert("list".into(), trace);
        harness.host.close().await;
    }

    compare(&Value::Object(traces), &oracle);
}
