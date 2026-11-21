//! The Rust acquirer and mirror host must reproduce the app's own calls and JSON for the same scripted sources.
//!
//! The oracles are `tests/js/acquire-mirror-acquire.mjs` and `tests/js/acquire-mirror-mirror.mjs`, which run the
//! app's own `src/acquire.js` and `src/mirror.js` (from `PEARTUBE_APP`, else the sibling `../peartube`) against a
//! scripted node, an in-memory filesystem, scripted HTTP feeds and a fake `ubit`. Here the same scenarios drive fake
//! implementations of every seam; the two traces are compared as JSON.
//!
//! `tests/js/acquire-mirror-library.mjs` runs the app's mirror host on real files instead: the import folder,
//! album folders, what the owner removed, and the Soulseek provider on a scripted network. Its sessions are in
//! `tests/js/acquire-mirror-library.json`, and its trace is compared as JSON text.

#[path = "support/files.rs"]
mod files;
#[path = "support/soulseek_network.rs"]
mod network;
#[path = "support/trace.rs"]
mod trace;

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
use peartube_native::mirror::{DirEntry, FileStat, MirrorFiles};
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

    fn list_entries(&self, dir: &str) -> Vec<DirEntry> {
        let base = dir.trim_end_matches('/');
        self.children(dir)
            .into_iter()
            .map(|name| {
                let full = format!("{base}/{name}");
                let below = format!("{full}/");
                let is_dir = self.dirs.lock().unwrap().contains(&full)
                    || self
                        .files
                        .lock()
                        .unwrap()
                        .keys()
                        .any(|key| key.starts_with(&below));
                DirEntry { name, is_dir }
            })
            .collect()
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

    fn read_head(&self, path: &str, max: usize) -> Option<Vec<u8>> {
        Files::read(self, path).map(|bytes| bytes[..bytes.len().min(max)].to_vec())
    }

    fn write(&self, path: &str, text: &str) -> Result<(), String> {
        self.set(path, text);
        Ok(())
    }

    fn write_atomic(&self, path: &str, text: &str) -> Result<(), String> {
        self.set(path, text);
        Ok(())
    }

    fn remove_file(&self, path: &str) -> Result<bool, String> {
        Ok(self.files.lock().unwrap().remove(path).is_some())
    }

    fn remove_dir(&self, path: &str) {
        self.dirs.lock().unwrap().remove(path);
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
    /// Held after the source's first response (where the job's validators are saved) so a test can read
    /// `jobs.json` mid-transfer.
    put_gate: Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    entries: Mutex<Vec<Value>>,
    backfill: Mutex<SyncResult>,
    rechunk_progress: Mutex<Vec<Value>>,
    rechunk_migrate: Mutex<Vec<Value>>,
    rechunk_result: Mutex<Value>,
    rechunk_wait: Mutex<Option<Arc<tokio::sync::Notify>>>,
    /// The ids `put` refuses once it has read the file.
    refuse: Mutex<Vec<String>>,
    /// While set, every `put` waits on it once it has read its file, so the import that asked stays
    /// under way; `parked` counts them.
    held: Mutex<Option<Arc<tokio::sync::watch::Sender<bool>>>>,
    parked: std::sync::atomic::AtomicUsize,
}

impl FakeNode {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            put_plan: Mutex::new(VecDeque::new()),
            put_gate: Mutex::new(None),
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
            refuse: Mutex::new(Vec::new()),
            held: Mutex::new(None),
            parked: std::sync::atomic::AtomicUsize::new(0),
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

/// The fake has no store to suspend.
struct NoStore;

impl peartube_native::node::api::StoreSuspension for NoStore {
    fn suspend(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn resume(&self) -> std::io::Result<()> {
        Ok(())
    }
    fn close(&self) {}
}

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
    fn store_suspension(&self) -> Arc<dyn peartube_native::node::api::StoreSuspension> {
        Arc::new(NoStore)
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
        Ok(None)
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
        let gate = self.put_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire().await;
        }
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
        let hold = self
            .held
            .lock()
            .unwrap()
            .as_ref()
            .map(|free| free.subscribe());
        if let Some(mut free) = hold {
            self.parked.fetch_add(1, Ordering::SeqCst);
            let _ = free.wait_for(|free| *free).await;
            self.parked.fetch_sub(1, Ordering::SeqCst);
        }
        let id = meta["id"].as_str().unwrap_or_default();
        if self
            .refuse
            .lock()
            .unwrap()
            .iter()
            .any(|refused| refused == id)
        {
            return Err(AcquireError::Message(format!("refused {id}")));
        }
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

    async fn is_source_peer(&self, _key: &str) -> bool {
        false
    }

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
            existing: None,
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
                existing: None,
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

// ---------------------------------------------------------------- restore, durability, cancellation

async fn until(mut check: impl FnMut() -> bool) {
    for _ in 0..100_000 {
        if check() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("condition never held");
}

fn saved_jobs(harness: &Harness) -> Vec<Value> {
    harness
        .files
        .read("/work/jobs.json")
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default()
}

fn seeded_job(job_id: &str, status: &str, store: &str, private: bool) -> Value {
    json!({
        "jobId": job_id,
        "id": "imdb:tt4444444",
        "title": "Seeded",
        "plugin": "http-acquire",
        "channel": false,
        "private": private,
        "status": status,
        "created": NOW,
        "store": store,
        "source": {"url": "http://source.test/film.mp4", "headers": {}, "page": false},
        "total": 10,
        "bytes": 4,
        "etag": "\"v1\"",
        "lastModified": null,
        "hashState": null,
        "hashBytes": 0
    })
}

/// A job left `announcing` (its announce saved, its source dropped) re-appends and finishes instead of reading
/// through the missing source.
#[tokio::test]
async fn an_announcing_job_restores_and_finishes() {
    let announce = json!({
        "type": "announce", "id": "imdb:tt4444444", "title": "Seeded", "size": 4,
        "sha256": "a".repeat(64), "blobs": "AAAA",
        "blob": {"blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 4}
    });
    let mut job = seeded_job(
        "cccccccc-0000-4000-8000-000000000003",
        "announcing",
        "video:announced",
        false,
    );
    job["source"] = Value::Null;
    job["announce"] = announce.clone();
    let harness = Harness::with_jobs(AcquirePath::Acquirer, Some(json!([job])));
    tokio::time::timeout(Duration::from_secs(10), settle(&harness))
        .await
        .expect("an announcing job must not block startup");

    let trace = harness.trace();
    let calls = trace["calls"].as_array().expect("calls");
    let appends: Vec<&Value> = calls
        .iter()
        .filter(|call| call["method"] == "append")
        .collect();
    assert_eq!(appends.len(), 1, "the saved announce is appended once");
    assert_eq!(appends[0]["op"], announce);
    assert!(
        calls.iter().all(|call| call["method"] != "put"),
        "no source is read"
    );
    assert!(trace["http"].as_array().expect("http").is_empty());
    assert_eq!(trace["list"][0]["status"], "done");
    assert_eq!(trace["saved"][0]["status"], "done");
    assert_eq!(trace["saved"][0]["sha256"], "a".repeat(64));
    assert_eq!(trace["saved"][0]["hashState"], Value::Null);
    assert_eq!(trace["saved"][0]["hashBytes"], 0);
}

/// A job with neither an announce nor a source can never run: it fails, drops its half-written core and does
/// not leave the acquirer waiting on it.
#[tokio::test]
async fn a_job_with_no_source_and_no_announce_fails_instead_of_hanging() {
    let mut job = seeded_job(
        "cccccccc-0000-4000-8000-000000000004",
        "running",
        "video:orphan",
        false,
    );
    job["source"] = Value::Null;
    let harness = Harness::with_jobs(AcquirePath::Acquirer, Some(json!([job])));
    tokio::time::timeout(Duration::from_secs(10), settle(&harness))
        .await
        .expect("a sourceless job must not leave the acquirer busy");

    let trace = harness.trace();
    let calls = trace["calls"].as_array().expect("calls");
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "dropStore" && call["name"] == "video:orphan"),
        "the dead job's core is dropped: {calls:?}"
    );
    assert_eq!(trace["list"][0]["status"], "failed");
    assert!(trace["list"][0]["error"].is_string());
    assert!(calls.iter().all(|call| call["method"] != "put"));
}

/// The first response's size and validators are in `jobs.json` before the transfer ends, so a crash resumes
/// with the range and validators the source already answered with.
#[tokio::test]
async fn the_first_response_size_and_validators_are_durable_at_once() {
    let harness = Harness::new(AcquirePath::Acquirer);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *harness.node.put_gate.lock().unwrap() = Some(gate.clone());
    harness.http.plan(vec![reply(
        200,
        &[
            ("content-length", "7"),
            ("etag", "\"strong\""),
            ("last-modified", "Wed, 01 Jan 2025 00:00:00 GMT"),
        ],
        &["abc", "defg"],
    )]);
    add_film(
        &harness,
        "imdb:tt5555555",
        "Film",
        "http://source.test/film.mp4",
        &[],
    );
    until(|| {
        saved_jobs(&harness)
            .first()
            .and_then(|job| job.get("etag"))
            .is_some_and(|etag| etag.as_str() == Some("\"strong\""))
    })
    .await;
    let saved = saved_jobs(&harness);
    assert_eq!(saved[0]["total"], 7, "size persisted before the body");
    assert_eq!(saved[0]["etag"], "\"strong\"");
    assert_eq!(saved[0]["lastModified"], "Wed, 01 Jan 2025 00:00:00 GMT");
    gate.add_permits(1);
    settle(&harness).await;
    assert_eq!(harness.trace()["list"][0]["status"], "done");
}

/// A private job written before per-job keys existed gets one before its first attempt, and no key or
/// checkpoint shows in the public job view.
#[tokio::test]
async fn a_legacy_private_job_gets_a_key_before_its_first_attempt() {
    let harness = Harness::with_jobs(
        AcquirePath::Acquirer,
        Some(json!([seeded_job(
            "cccccccc-0000-4000-8000-000000000005",
            "queued",
            "video:legacy-private",
            true
        )])),
    );
    harness
        .http
        .plan(vec![reply(200, &[("content-length", "3")], &["abc"])]);
    settle(&harness).await;

    let trace = harness.trace();
    let calls = trace["calls"].as_array().expect("calls");
    let put = calls
        .iter()
        .find(|call| call["method"] == "put")
        .expect("put was called");
    let key = put["meta"]["storeKey"]
        .as_str()
        .expect("the private put carries a per-job key");
    assert_eq!(key.len(), 64);
    assert!(key.bytes().all(|b| b.is_ascii_hexdigit()));
    assert_eq!(trace["saved"][0]["storeKey"], key, "the key is durable");
    let public = &trace["list"][0];
    for field in [
        "storeKey",
        "etag",
        "lastModified",
        "hashState",
        "hashBytes",
        "source",
        "announce",
    ] {
        assert!(public.get(field).is_none(), "{field} leaks: {public}");
    }
}

/// Cancelling a job that is not running drops its half-written core; a running one drops it in its own
/// failure path.
#[tokio::test]
async fn cancelling_a_queued_job_drops_its_core() {
    let harness = Harness::new(AcquirePath::Acquirer);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    *harness.node.put_gate.lock().unwrap() = Some(gate.clone());
    harness
        .http
        .plan(vec![reply(200, &[("content-length", "3")], &["abc"])]);
    add_film(
        &harness,
        "imdb:tt6666666",
        "Busy",
        "http://source.test/busy.mp4",
        &[],
    );
    until(|| {
        harness
            .node
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call["method"] == "put")
    })
    .await;
    let queued = harness
        .acquirer
        .as_ref()
        .unwrap()
        .add(AddJob {
            id: "imdb:tt7777777".into(),
            title: "Waiting".into(),
            source: SourceSpec {
                url: "http://source.test/waiting.mp4".into(),
                headers: Vec::new(),
                page: false,
                private: false,
            },
            channel: None,
            private: false,
            plugin: None,
            existing: None,
        })
        .expect("add");
    let store = queued["store"].as_str().expect("store").to_owned();
    let cancelled = harness
        .acquirer
        .as_ref()
        .unwrap()
        .cancel(queued["jobId"].as_str().expect("jobId"))
        .expect("queued job");
    assert_eq!(cancelled["status"], "cancelled");
    until(|| {
        harness
            .node
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call["method"] == "dropStore" && call["name"] == store.as_str())
    })
    .await;
    gate.add_permits(1);
    settle(&harness).await;
}

/// A dismissed dead-lettered job takes its half-written core with it.
#[tokio::test]
async fn cancelling_a_failed_job_drops_its_core() {
    let mut job = seeded_job(
        "cccccccc-0000-4000-8000-000000000006",
        "failed",
        "video:deadtail",
        false,
    );
    job["source"] = Value::Null;
    let harness = Harness::with_jobs(AcquirePath::Acquirer, Some(json!([job])));
    tokio::time::timeout(Duration::from_secs(10), settle(&harness))
        .await
        .expect("a failed job is not run");
    let removed = harness
        .acquirer
        .as_ref()
        .unwrap()
        .cancel("cccccccc-0000-4000-8000-000000000006")
        .expect("failed job");
    assert_eq!(removed["status"], "removed");
    assert!(harness.acquirer.as_ref().unwrap().list().is_empty());
    until(|| {
        harness
            .node
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|call| call["method"] == "dropStore" && call["name"] == "video:deadtail")
    })
    .await;
}

/// A strong ETag that rotated under a resumed core marks the bytes on disk as another encode, whatever the
/// range says.
#[tokio::test]
async fn a_rotated_etag_drops_the_half_written_core() {
    let harness = Harness::with_jobs(
        AcquirePath::Acquirer,
        Some(json!([seeded_job(
            "cccccccc-0000-4000-8000-000000000008",
            "running",
            "video:reencoded",
            false
        )])),
    );
    harness.node.put_plan.lock().unwrap().push_back(PutPlan {
        start: 4,
        hash: None,
    });
    harness.http.plan(vec![reply(
        206,
        &[("content-range", "bytes 4-9/10"), ("etag", "\"v2\"")],
        &["abcdefghij"],
    )]);
    tokio::time::timeout(Duration::from_secs(10), settle(&harness))
        .await
        .expect("the restart path settles");
    let trace = harness.trace();
    let calls = trace["calls"].as_array().expect("calls");
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "dropStore" && call["name"] == "video:reencoded"),
        "the other encode's core is dropped: {calls:?}"
    );
    assert_eq!(trace["list"][0]["status"], "failed");
}

/// A source that changed under a resumed core (here a Content-Range that does not start where the core ends)
/// drops the bytes of the other encode and retries from zero, rather than resuming on top of them forever.
#[tokio::test]
async fn a_changed_source_drops_the_half_written_core() {
    let harness = Harness::with_jobs(
        AcquirePath::Acquirer,
        Some(json!([seeded_job(
            "cccccccc-0000-4000-8000-000000000007",
            "running",
            "video:rotated",
            false
        )])),
    );
    harness.node.put_plan.lock().unwrap().push_back(PutPlan {
        start: 4,
        hash: None,
    });
    harness.http.plan(vec![reply(
        206,
        &[("content-range", "bytes 0-9/10"), ("etag", "\"v1\"")],
        &["abcdefghij"],
    )]);
    tokio::time::timeout(Duration::from_secs(10), settle(&harness))
        .await
        .expect("the restart path settles");
    let trace = harness.trace();
    let calls = trace["calls"].as_array().expect("calls");
    assert!(
        calls
            .iter()
            .any(|call| call["method"] == "dropStore" && call["name"] == "video:rotated"),
        "the other encode's core is dropped: {calls:?}"
    );
    assert_eq!(trace["list"][0]["status"], "failed");
    let request = &trace["http"][0]["headers"];
    assert_eq!(request["range"], "bytes=4-");
    assert_eq!(request["if-range"], "\"v1\"");
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
        Self::with_options(seed_jobs, Feeds::new(), None).await
    }

    async fn with_watch_dir(dir: &str) -> Self {
        Self::with_options(Value::Array(Vec::new()), Feeds::new(), Some(dir.to_owned())).await
    }

    async fn with_feeds(seed_jobs: Value, feeds: Arc<Feeds>) -> Self {
        Self::with_options(seed_jobs, feeds, None).await
    }

    async fn with_options(seed_jobs: Value, feeds: Arc<Feeds>, watch_dir: Option<String>) -> Self {
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
                watch_dir,
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
                    "soulseek",
                    "youtube",
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
        harness.host.close().await.unwrap();
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
        harness.host.close().await.unwrap();
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
        harness.host.close().await.unwrap();
    }

    {
        let harness = MirrorHarness::with_watch_dir("/data/import").await;
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
        harness.host.close().await.unwrap();
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
        harness.host.close().await.unwrap();
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
        harness.host.close().await.unwrap();
    }

    compare(&Value::Object(traces), &oracle);
}

// ---------------------------------------------------------------- library

#[cfg(unix)]
mod library {
    use std::collections::HashMap;
    use std::future::Future;
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use peartube_native::mirror::close::{CloseError, Closing};
    use peartube_native::mirror::{
        Ctx, LocalMirrorFiles, MirrorConfig, MirrorError, MirrorHost, MirrorOptions, MirrorPlugin,
        MirrorProvider, ProviderFuture, SoulseekSettings,
    };
    use serde_json::{Map, Value, json};

    use super::files::{Labels, tree};
    use super::network::{FakeBackend, Network, World, flac};
    use super::{Factory, FakeNode, Feeds, NOW, RechunkFactory, js_trace};

    /// A file's modification time when a step names none, in seconds.
    const MTIME: u64 = 1_600_000_000;

    /// The ledger file's text, with the time of every file written during the run (a download) named `<now>`:
    /// only those are later than the scripted clock.
    fn steady(text: &str) -> String {
        let mark = "\"mtimeMs\": ";
        let mut out = String::new();
        let mut rest = text;
        while let Some(at) = rest.find(mark) {
            let (head, tail) = rest.split_at(at + mark.len());
            let digits = tail.bytes().take_while(u8::is_ascii_digit).count();
            out.push_str(head);
            match tail[..digits].parse::<u64>() {
                Ok(ms) if ms >= NOW => out.push_str("\"<now>\""),
                _ => out.push_str(&tail[..digits]),
            }
            rest = &tail[digits..];
        }
        out + rest
    }

    fn set_mode(path: &Path, mode: u32) {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
    }

    /// A step that changes the files under `root`; false when the step is something else.
    fn file_step(root: &Path, step: &Value) -> bool {
        if let Some(path) = step.get("write").and_then(Value::as_str) {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let bytes = match step.get("flac") {
                Some(tags) => flac(
                    &serde_json::from_value::<Vec<(String, String)>>(tags.clone()).unwrap(),
                    step.get("pad").and_then(Value::as_u64).unwrap_or(0) as usize,
                ),
                None => step["text"].as_str().unwrap().as_bytes().to_vec(),
            };
            std::fs::write(&path, bytes).unwrap();
            let at = step.get("mtime").and_then(Value::as_u64).unwrap_or(MTIME);
            std::fs::File::options()
                .write(true)
                .open(&path)
                .unwrap()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(at))
                .unwrap();
        } else if let Some(link) = step.get("link").and_then(Value::as_str) {
            let target = root.join(step["to"].as_str().unwrap());
            std::os::unix::fs::symlink(target, root.join(link)).unwrap();
        } else if let Some(path) = step.get("remove").and_then(Value::as_str) {
            std::fs::remove_file(root.join(path)).unwrap();
        } else {
            return false;
        }
        true
    }

    fn outcome(result: Result<Value, MirrorError>) -> Value {
        match result {
            Ok(value) => json!({ "result": value }),
            Err(error) => json!({ "error": { "message": error.message, "status": error.status } }),
        }
    }

    /// The source declared in `searchPlugins`. A held search receives one permit after the
    /// deadline; its completion proves that timing out the caller did not cancel the provider.
    struct ScriptedSource {
        spec: Value,
        release: Arc<tokio::sync::Notify>,
        finished: Arc<tokio::sync::Notify>,
    }

    impl MirrorPlugin<FakeNode> for ScriptedSource {
        fn name(&self) -> &str {
            self.spec["name"].as_str().unwrap()
        }
        fn provider(&self) -> Option<&dyn MirrorProvider<FakeNode>> {
            Some(self)
        }
        fn run<'a>(&'a self, _: Value, _: &'a Ctx<FakeNode>) -> ProviderFuture<'a> {
            unreachable!()
        }
    }

    impl MirrorProvider<FakeNode> for ScriptedSource {
        fn search<'a>(&'a self, payload: Value, _: &'a Ctx<FakeNode>) -> ProviderFuture<'a> {
            Box::pin(async move {
                let plan = self.spec["queries"]
                    .get(payload["query"].as_str().unwrap())
                    .unwrap_or(&self.spec);
                if let Some(ms) = plan["delay"].as_u64() {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                if plan["hold"] == true {
                    self.release.notified().await;
                    self.finished.notify_one();
                }
                if let Some(error) = plan["error"].as_str() {
                    return Err(MirrorError {
                        message: error.repeat(plan["repeat"].as_u64().unwrap_or(1) as usize),
                        status: plan["status"].as_u64().map(|s| s as u16),
                    });
                }
                Ok(if plan["args"] == true {
                    json!({ "candidates": [payload] })
                } else {
                    plan["reply"].clone()
                })
            })
        }
        fn browse<'a>(&'a self, _: Value, _: &'a Ctx<FakeNode>) -> ProviderFuture<'a> {
            unreachable!()
        }
        fn mirror<'a>(&'a self, _: Value, _: &'a Ctx<FakeNode>) -> ProviderFuture<'a> {
            unreachable!()
        }
        fn list_jobs(&self) -> Vec<Value> {
            Vec::new()
        }
        fn get_job(&self, _: &str) -> Option<Value> {
            None
        }
        fn cancel_job(&self, _: &str) -> Option<Value> {
            None
        }
    }

    /// A plugin registered before the provider whose stop does not finish at once. It is in no
    /// compared output. With it, a host that stopped its plugins one after another has not reached
    /// the provider when `close` is first polled, which is what the app's own test checks "in the
    /// tick `close()` was called".
    struct SlowStop;

    impl MirrorPlugin<FakeNode> for SlowStop {
        fn name(&self) -> &str {
            "slow-stop"
        }

        fn run<'a>(
            &'a self,
            _payload: Value,
            _ctx: &'a Ctx<FakeNode>,
        ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
            Box::pin(async { Ok(Value::Null) })
        }

        fn stop(&self) -> peartube_native::mirror::close::Stop<'_> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(())
            })
        }
    }

    struct StopProbe {
        name: String,
        release: tokio::sync::watch::Receiver<bool>,
        started: Arc<std::sync::Mutex<Vec<String>>>,
        finished: Arc<std::sync::Mutex<Vec<String>>>,
        host: std::sync::Weak<MirrorHost<FakeNode>>,
        from_stop: Arc<std::sync::Mutex<Option<Closing>>>,
    }

    impl MirrorPlugin<FakeNode> for StopProbe {
        fn name(&self) -> &str {
            &self.name
        }
        fn run<'a>(&'a self, _: Value, _: &'a Ctx<FakeNode>) -> ProviderFuture<'a> {
            unreachable!()
        }
        fn stop(&self) -> peartube_native::mirror::close::Stop<'_> {
            let first = {
                let mut started = self.started.lock().unwrap();
                let first = !started.contains(&self.name);
                started.push(self.name.clone());
                first
            };
            if self.name == "reenter" && first {
                let close = self.host.upgrade().unwrap().close();
                *self.from_stop.lock().unwrap() = Some(close);
            }
            if self.name == "throw" {
                panic!("not async");
            }
            Box::pin(async move {
                if matches!(self.name.as_str(), "slow" | "late failure" | "reenter") {
                    self.release.clone().wait_for(|open| *open).await.unwrap();
                }
                self.finished.lock().unwrap().push(self.name.clone());
                match self.name.as_str() {
                    "fail" => Err(MirrorError::with_status("disk gone", 507)),
                    "late failure" => Err(MirrorError::plain("late failure")),
                    "slow" | "reenter" => Ok(()),
                    other => panic!("unknown stop {other}"),
                }
            })
        }
    }

    fn stop_reason(error: &CloseError) -> Value {
        let one = |e: &MirrorError| json!({ "message": e.message, "status": e.status });
        match error {
            CloseError::Plugin(e) => one(e),
            CloseError::Several(errors) => json!({
                "message": error.to_string(),
                "errors": errors.iter().map(one).collect::<Vec<_>>(),
            }),
        }
    }

    /// Compare the host before the slow stop is released and once close returns. The app runs
    /// its real gate; here the relay's detach/stop callback records when it releases that resource.
    /// A promise settles with nobody awaiting it, so the Rust close must finish the same way.
    #[tokio::test]
    async fn shutdown_matches_all_settled_on_the_app() {
        use futures_util::{FutureExt, future::join_all};
        use std::sync::{Mutex, atomic::AtomicBool};
        let oracle = js_trace("mirror-close.mjs");
        for case in oracle.as_array().unwrap() {
            let storage = tempfile::tempdir().unwrap();
            let listening = Arc::new(AtomicBool::new(true));
            let released = Arc::new(tokio::sync::Notify::new());
            let host = Arc::new(
                MirrorHost::start(
                    FakeNode::new(),
                    None,
                    Arc::new(LocalMirrorFiles),
                    Arc::new(Factory),
                    Arc::new(RechunkFactory),
                    Feeds::new(),
                    Vec::new(),
                    MirrorOptions {
                        storage: storage.path().display().to_string(),
                        poll_interval_ms: 0,
                        mirror_channel: false,
                        rechunk: false,
                        stop_traffic: Some(Box::new({
                            let (live, released) = (listening.clone(), released.clone());
                            move || {
                                live.store(false, Ordering::SeqCst);
                                released.notify_one();
                            }
                        })),
                        ..MirrorOptions::default()
                    },
                )
                .await,
            );
            let (release, gate) = tokio::sync::watch::channel(false);
            let started = Arc::new(Mutex::new(Vec::new()));
            let finished = Arc::new(Mutex::new(Vec::new()));
            let from_stop = Arc::new(Mutex::new(None));
            for n in case["stops"].as_array().unwrap() {
                host.register(Arc::new(StopProbe {
                    name: n.as_str().unwrap().into(),
                    release: gate.clone(),
                    started: started.clone(),
                    finished: finished.clone(),
                    host: Arc::downgrade(&host),
                    from_stop: from_stop.clone(),
                }));
            }
            let first = host.close();
            let at_return = started.lock().unwrap().clone();
            let mut calls = vec![first, host.close()];
            calls.extend(from_stop.lock().unwrap().take());
            let settled = calls
                .iter()
                .filter(|c| (**c).clone().now_or_never().is_some())
                .count();
            assert_eq!(
                json!({ "settled": settled, "listening": listening.load(Ordering::SeqCst),
                "started": *started.lock().unwrap(), "atReturn": at_return }),
                case["before"],
                "before release: {}",
                case["stops"]
            );
            release.send_replace(true);
            // No caller polls its handle between the release and the end of the close.
            tokio::time::timeout(Duration::from_secs(5), released.notified())
                .await
                .expect("the close finishes with no caller waiting");
            let mut results = tokio::time::timeout(Duration::from_secs(5), join_all(calls))
                .await
                .expect("every caller gets the result");
            results.push(host.close().await);
            let same = results.iter().all(|r| r == &results[0]);
            assert_eq!(json!(same), case["same"]);
            assert_eq!(json!(results.len()), case["callers"]);
            assert_eq!(
                json!(results[0].as_ref().err().map(stop_reason)),
                case["error"]
            );
            assert_eq!(json!(*started.lock().unwrap()), case["started"]);
            assert_eq!(json!(*finished.lock().unwrap()), case["finished"]);
            assert_eq!(json!(listening.load(Ordering::SeqCst)), case["listening"]);
        }
    }

    /// One session of the scenario: the mirror host on real files in a temporary folder.
    async fn run_session(network: &Arc<Network>, session: &Value) -> Value {
        let name = session["name"].as_str().unwrap();
        let root = std::env::temp_dir().join(format!(
            "pt-library-rust-{}-{}",
            std::process::id(),
            name.replace(' ', "-")
        ));
        let _ = std::fs::remove_dir_all(&root);
        let storage = root.join("storage");
        // The import folder is `import`, or the folder a session names.
        let import = root.join(session["watchDir"].as_str().unwrap_or("import"));
        let staging = storage.join("staging");
        std::fs::create_dir_all(&storage).unwrap();
        let watching = session["watchDir"] != false;
        if watching {
            std::fs::create_dir_all(&import).unwrap();
        }
        for step in session["before"].as_array().unwrap() {
            assert!(file_step(&root, step), "not a file step {step}");
        }
        let read_only = session["readOnly"] == true;
        if read_only {
            set_mode(&import, 0o555);
        }

        let node = FakeNode::new();
        let world = World::new(network.clone(), None, None);
        // The clock stands still unless a `clock` step moves it.
        let clock = Arc::new(AtomicU64::new(NOW));
        let sign_in = |key: &str| {
            session["soulseek"][key]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        };
        let host = MirrorHost::start(
            node.clone(),
            None,
            Arc::new(LocalMirrorFiles),
            Arc::new(Factory),
            Arc::new(RechunkFactory),
            Feeds::new(),
            vec![Arc::new(SlowStop)],
            MirrorOptions {
                storage: storage.to_string_lossy().into_owned(),
                mirror_channel: false,
                rechunk: false,
                watch_dir: watching.then(|| import.to_string_lossy().into_owned()),
                soulseek: SoulseekSettings {
                    user: sign_in("slskUser"),
                    pass: sign_in("slskPass"),
                    backend: Some(Arc::new(FakeBackend(world.clone()))),
                    random: Arc::new(Labels::default()),
                    ..SoulseekSettings::default()
                },
                plugins_dir: Some(storage.join("plugins").to_string_lossy().into_owned()),
                poll_interval_ms: 0,
                import_wait_ms: session["importWaitMs"].as_u64().unwrap_or(120_000),
                now_ms: Arc::new({
                    let clock = clock.clone();
                    move || clock.load(Ordering::SeqCst)
                }),
                ..MirrorOptions::default()
            },
        )
        .await;
        host.wait_idle().await;
        let release = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(tokio::sync::Notify::new());
        for spec in session["searchPlugins"].as_array().into_iter().flatten() {
            host.register(Arc::new(ScriptedSource {
                spec: spec.clone(),
                release: release.clone(),
                finished: finished.clone(),
            }));
        }
        let host = Arc::new(host);
        // Calls a `begin` step started and no step has taken the outcome of yet.
        let mut pending: HashMap<String, tokio::task::JoinHandle<Value>> = HashMap::new();
        let mut closed = false;

        let ledger_file = storage.join("mirror-watch.json");
        let ledger = || std::fs::read_to_string(&ledger_file).ok();
        let mut out: Vec<Value> = Vec::new();
        let mut put = |step: Value, value: Value| {
            let calls: Vec<Value> = std::mem::take(&mut *node.calls.lock().unwrap())
                .into_iter()
                .filter(|call| matches!(call["method"].as_str(), Some("put" | "append")))
                .collect();
            out.push(json!({
                "step": step,
                "calls": calls,
                "network": world.take_log(),
                "value": value,
            }));
        };
        put(json!("start"), Value::Null);
        for (i, step) in session["steps"].as_array().unwrap().iter().enumerate() {
            let payload = || step.get("payload").cloned().unwrap_or(Value::Null);
            let value = if file_step(&root, step) {
                Value::Null
            } else if let Some(label) = step.get("begin").and_then(Value::as_str) {
                // A run or a provider call that is not waited for: a later `end` takes its outcome.
                // Its task does not start before this step is recorded.
                let (host, payload) = (host.clone(), payload());
                let text = |key: &str| step.get(key).and_then(Value::as_str).map(str::to_owned);
                let (run, call, provider) = (text("run"), text("provider"), text("name"));
                let task = tokio::spawn(async move {
                    let provider = provider.unwrap_or_default();
                    outcome(match (run, call.as_deref()) {
                        (Some(plugin), _) => host.run(&plugin, payload).await,
                        (None, Some("search")) => host.search_provider(&provider, payload).await,
                        (None, Some("browse")) => host.browse_provider(&provider, payload).await,
                        (None, Some("mirror")) => host.mirror_provider(&provider, payload).await,
                        (None, other) => panic!("unknown provider call {other:?}"),
                    })
                });
                pending.insert(label.to_owned(), task);
                json!({ "started": label })
            } else if let Some(label) = step.get("end").and_then(Value::as_str) {
                pending
                    .remove(label)
                    .expect("a call that was begun")
                    .await
                    .expect("the call ran to its end")
            } else if let Some(want) = step.get("until") {
                // Waits, in real time, until as many announces are parked on a hold or a job reads as
                // the step says. A state that never comes stops the run here, where the step is known.
                let mut reached = false;
                for _ in 0..2000 {
                    reached = match want.get("job").and_then(Value::as_u64) {
                        None => {
                            Some(node.parked.load(Ordering::SeqCst) as u64)
                                == want["parked"].as_u64()
                        }
                        Some(n) => host
                            .get_provider_job(&format!("job-{n}"))
                            .is_some_and(|job| {
                                job["status"] == want["status"]
                                    && (want.get("phase").is_none()
                                        || job["phase"] == want["phase"])
                            }),
                    };
                    if reached {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert!(reached, "session “{name}”, step {i}: never reached {want}");
                Value::Null
            } else if step.get("hold").is_some() {
                *node.held.lock().unwrap() = Some(Arc::new(tokio::sync::watch::channel(false).0));
                Value::Null
            } else if step.get("release").is_some() {
                if let Some(free) = node.held.lock().unwrap().take() {
                    free.send_replace(true);
                }
                Value::Null
            } else if let Some(ms) = step.get("clock").and_then(Value::as_u64) {
                clock.fetch_add(ms, Ordering::SeqCst);
                Value::Null
            } else if step.get("close").is_some() {
                // What every provider job reads when `close` has been polled once (every plugin's stop
                // has started and none has been waited for: the app's "tick `close()` was called in"),
                // and once the host has closed.
                let view = || -> Value {
                    host.list_provider_jobs()
                        .iter()
                        .map(|job| {
                            json!({
                                "jobId": job["jobId"],
                                "status": job["status"],
                                "phase": job["phase"],
                                "error": job["error"],
                            })
                        })
                        .collect()
                };
                let mut closing = Box::pin(host.close());
                let done =
                    std::future::poll_fn(|cx| std::task::Poll::Ready(closing.as_mut().poll(cx)))
                        .await;
                let at_once = view();
                match done {
                    std::task::Poll::Ready(result) => result.unwrap(),
                    std::task::Poll::Pending => closing.await.unwrap(),
                }
                closed = true;
                json!({ "atOnce": at_once, "closed": view() })
            } else if let Some(ids) = step.get("refuse") {
                *node.refuse.lock().unwrap() = serde_json::from_value(ids.clone()).unwrap();
                Value::Null
            } else if let Some(plugin) = step.get("run").and_then(Value::as_str) {
                outcome(host.run(plugin, payload()).await)
            } else if let Some(plugins) = step.get("together").and_then(Value::as_array) {
                // Runs asked for while another is under way.
                let runs = plugins
                    .iter()
                    .map(|plugin| host.run(plugin.as_str().unwrap(), Value::Null));
                futures_util::future::join_all(runs)
                    .await
                    .into_iter()
                    .map(outcome)
                    .collect()
            } else if let Some(next) = step.get("configure") {
                let text = |key: &str| next.get(key).and_then(Value::as_str).map(str::to_owned);
                host.configure(MirrorConfig {
                    feeds: None,
                    slsk_user: text("slskUser"),
                    slsk_pass: text("slskPass"),
                    slsk_server: text("slskServer"),
                });
                Value::Null
            } else if let Some(query) = step.get("searchSources") {
                let mut parts = Vec::new();
                let answer = host
                    .search_sources(
                        query.clone(),
                        Some(&mut |part| {
                            parts.push(serde_json::to_value(part).unwrap());
                        }),
                    )
                    .await;
                let mut record = outcome(answer.map(|v| serde_json::to_value(v).unwrap()));
                record["parts"] = Value::Array(parts);
                record
            } else if step.get("releaseSearch").is_some() {
                release.notify_one();
                let resumed = tokio::time::timeout(Duration::from_secs(1), finished.notified())
                    .await
                    .is_ok();
                json!({ "finishedLate": usize::from(resumed) })
            } else if let Some(call) = step.get("provider").and_then(Value::as_str) {
                let provider = step["name"].as_str().unwrap();
                outcome(match call {
                    "search" => host.search_provider(provider, payload()).await,
                    "browse" => host.browse_provider(provider, payload()).await,
                    "mirror" => host.mirror_provider(provider, payload()).await,
                    other => panic!("unknown provider call {other}"),
                })
            } else if step.get("jobs").is_some() {
                Value::Array(host.list_provider_jobs())
            } else if let Some(n) = step.get("job").and_then(Value::as_u64) {
                json!(host.get_provider_job(&format!("job-{n}")))
            } else if let Some(n) = step.get("cancel").and_then(Value::as_u64) {
                json!(host.cancel_provider_job(&format!("job-{n}")))
            } else if let Some(wanted) = step.get("forget").and_then(Value::as_array) {
                // An announce is named by its ledger key, by its place in the ledger, or outright.
                let rows: Map<String, Value> = ledger()
                    .map(|text| serde_json::from_str(&text).unwrap())
                    .unwrap_or_default();
                let gone: Vec<(String, String)> = wanted
                    .iter()
                    .map(|want| {
                        let row = match (want.as_str(), want.get("row").and_then(Value::as_u64)) {
                            (Some(key), _) => &rows[key],
                            (None, Some(place)) => rows.values().nth(place as usize).unwrap(),
                            (None, None) => want,
                        };
                        let text = |key: &str| row[key].as_str().unwrap().to_owned();
                        (text("id"), text("sha256"))
                    })
                    .collect();
                json!(host.forget_imported(&gone).await)
            } else if step.get("list").is_some() {
                host.list()
                    .into_iter()
                    .filter(|plugin| {
                        matches!(
                            plugin["name"].as_str(),
                            Some("watch-dir" | "soulseek" | "youtube")
                        )
                    })
                    .collect()
            } else if step.get("tree").is_some() {
                json!({
                    "import": import.exists().then(|| tree(&import)),
                    "staging": staging.exists().then(|| tree(&staging)),
                })
            } else if step.get("ledger").is_some() {
                json!(ledger().map(|text| steady(&text)))
            } else {
                panic!("unknown step {step}");
            };
            put(json!(i), value);
        }
        if !closed {
            host.close().await.unwrap();
        }
        if read_only {
            set_mode(&import, 0o755);
        }
        let _ = std::fs::remove_dir_all(&root);
        let text = json!({ "name": name, "trace": out }).to_string();
        let root = serde_json::to_string(&root.to_string_lossy()).unwrap();
        serde_json::from_str(&text.replace(&root[1..root.len() - 1], "<root>")).unwrap()
    }

    /// Every session runs to its end before the test fails, so one run names all that differs.
    #[tokio::test]
    async fn the_library_matches_the_app() {
        let oracle = js_trace("acquire-mirror-library.mjs");
        let scenario: Value =
            serde_json::from_str(include_str!("js/acquire-mirror-library.json")).unwrap();
        let network = Network::load();
        let sessions = scenario["sessions"].as_array().unwrap();
        let expected = oracle["sessions"].as_array().unwrap();
        assert_eq!(expected.len(), sessions.len());
        let mut records = 0;
        let mut differs: Vec<String> = Vec::new();
        for (session, expected) in sessions.iter().zip(expected) {
            let actual = run_session(&network, session).await;
            assert_eq!(actual["name"], expected["name"]);
            let (actual, expected) = (
                actual["trace"].as_array().unwrap(),
                expected["trace"].as_array().unwrap(),
            );
            for (actual, expected) in actual.iter().zip(expected) {
                if let Some(difference) = super::trace::first_difference(actual, expected, "") {
                    differs.push(format!(
                        "session “{}”, step {}: {difference}",
                        session["name"].as_str().unwrap(),
                        expected["step"]
                    ));
                }
                records += 1;
            }
            if actual.len() != expected.len() {
                differs.push(format!(
                    "session “{}”: {} records here, {} in the app",
                    session["name"].as_str().unwrap(),
                    actual.len(),
                    expected.len()
                ));
            }
        }
        assert!(
            differs.is_empty(),
            "{} records differ from the app:\n  {}",
            differs.len(),
            differs.join("\n  ")
        );
        assert!(records > 150, "{records} records compared");
    }
}
