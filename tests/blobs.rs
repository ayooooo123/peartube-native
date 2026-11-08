//! Parity and behaviour tests for [`Blobs`].
//!
//! `fixtures/blobs-reference.json` is generated from the app's own `src/node/blobs.js` by
//! `tests/js/blobs-fixtures.mjs`, running the same scenarios against real hypercore/hyperblobs.
//! Every recorded value here must come out identical: channel ops, core keys, announce fields,
//! plain and encrypted core blocks, and the cache ceiling's decisions for a scripted sequence.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use corestore::{Corestore, GetOptions as StoreGetOptions};
use hypercore::GetOptions;
use peartube_native::node::PutSource;
use peartube_native::node::api::{Announce, PutMeta};
use peartube_native::node::blobs::{
    BlobHost, BlobStreams, Blobs, BlobsError, ChannelView, Compact, ContentSwarm, RechunkOptions,
    SendFuture,
};
use serde_json::{Map, Value, json};
use tokio::io::{AsyncRead, AsyncWriteExt};

const FIXTURES: &str = include_str!("../fixtures/blobs-reference.json");
const BLOCK: usize = 512 * 1024;

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/blobs-reference.json")
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex32(s: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).expect("hex");
    }
    out
}

/// Same deterministic bytes as the JS fixture generator.
fn data(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

// ---------------------------------------------------------------------------------------------
// Stubs

// The stubs are owned handles, not `Arc<T>`: `Arc` is not a fundamental type, so a test crate
// cannot implement this crate's traits for it.
#[derive(Default)]
struct SwarmState {
    joined: Mutex<Vec<[u8; 32]>>,
    left: Mutex<Vec<[u8; 32]>>,
}

#[derive(Clone, Default)]
struct TestSwarm(Arc<SwarmState>);

impl TestSwarm {
    fn joined(&self) -> Vec<[u8; 32]> {
        lock(&self.0.joined).clone()
    }

    fn left(&self) -> Vec<[u8; 32]> {
        lock(&self.0.left).clone()
    }
}

impl ContentSwarm for TestSwarm {
    fn join(&self, key: [u8; 32]) {
        lock(&self.0.joined).push(key);
    }
    fn leave(&self, key: [u8; 32]) {
        lock(&self.0.left).push(key);
    }
    fn down_bps(&self) -> f64 {
        0.0
    }
    fn up_bps(&self) -> f64 {
        0.0
    }
    fn inflight_range(&self) -> Option<(u32, u32)> {
        None
    }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Default)]
struct ChannelState {
    writer: [u8; 32],
    primary: [u8; 32],
    ops: Mutex<Vec<(Vec<u8>, Vec<u8>)>>,
    secondary: Mutex<HashSet<String>>,
    has_entries: AtomicBool,
}

#[derive(Clone, Default)]
struct TestChannel(Arc<ChannelState>);

impl TestChannel {
    fn new(primary: [u8; 32], writer: [u8; 32]) -> Self {
        let state = ChannelState {
            writer,
            primary,
            ..Default::default()
        };
        Self(Arc::new(state))
    }

    fn push(&self, op: &Value) {
        let kind = op.get("type").and_then(Value::as_str).unwrap_or("op");
        let id = op.get("id").and_then(Value::as_str).unwrap_or("");
        let key = format!("{kind}/{}/{id}", hex(&self.0.writer));
        lock(&self.0.ops).push((key.into_bytes(), op.to_string().into_bytes()));
    }

    fn appended(&self) -> Vec<Value> {
        lock(&self.0.ops)
            .iter()
            .map(|(_, value)| serde_json::from_slice(value).expect("op json"))
            .collect()
    }

    fn raw_ops(&self) -> Vec<String> {
        lock(&self.0.ops)
            .iter()
            .map(|(_, value)| String::from_utf8_lossy(value).into_owned())
            .collect()
    }

    fn op_count(&self) -> usize {
        lock(&self.0.ops).len()
    }

    fn writer_hex(&self) -> String {
        hex(&self.0.writer)
    }
}

impl ChannelView for TestChannel {
    type Error = std::io::Error;

    fn local_writer(&self) -> [u8; 32] {
        self.0.writer
    }
    fn primary_key(&self) -> [u8; 32] {
        self.0.primary
    }
    fn current_epoch(&self) -> SendFuture<'_, u64> {
        Box::pin(async { 1 })
    }
    fn local_role(&self) -> SendFuture<'_, String> {
        Box::pin(async { "owner".to_owned() })
    }
    fn update(&self) -> SendFuture<'_, Result<(), Self::Error>> {
        Box::pin(async { Ok(()) })
    }
    fn entries(&self) -> SendFuture<'_, Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>> {
        let entries = lock(&self.0.ops).clone();
        Box::pin(async move { Ok(entries) })
    }
    fn append<'a>(&'a self, op: &Value) -> SendFuture<'a, Result<(), Self::Error>> {
        self.push(op);
        Box::pin(async { Ok(()) })
    }
    fn add_secondary_writer(&self, writer_hex: &str) {
        lock(&self.0.secondary).insert(writer_hex.to_owned());
    }
    fn mark_has_entries(&self) {
        self.0.has_entries.store(true, Ordering::Release);
    }
    fn sync_writer_cores(&self) -> SendFuture<'_, ()> {
        Box::pin(async {})
    }
}

#[derive(Default)]
struct HostState {
    clock: AtomicU64,
    closing: AtomicBool,
    vault: AtomicBool,
    epoch_key: Mutex<Option<[u8; 32]>>,
    announcements: Mutex<HashMap<String, Announce>>,
    stored: Mutex<HashSet<[u8; 32]>>,
    db_bytes: AtomicU64,
    compactions: Mutex<Vec<Compact>>,
    storage: Mutex<Option<Corestore>>,
}

#[derive(Clone, Default)]
struct TestHost(Arc<HostState>);

impl TestHost {
    fn new(clock: u64) -> Self {
        let state = HostState {
            clock: AtomicU64::new(clock),
            ..Default::default()
        };
        state.vault.store(true, Ordering::Release);
        Self(Arc::new(state))
    }

    fn tick(&self, millis: u64) -> u64 {
        self.0.clock.fetch_add(millis, Ordering::AcqRel) + millis
    }

    fn set_epoch_key(&self, key: [u8; 32]) {
        *lock(&self.0.epoch_key) = Some(key);
    }

    fn compactions(&self) -> Vec<Compact> {
        lock(&self.0.compactions).clone()
    }
}

impl BlobHost for TestHost {
    fn closing(&self) -> bool {
        self.0.closing.load(Ordering::Acquire)
    }
    fn update(&self) {}
    fn now_ms(&self) -> u64 {
        self.0.clock.load(Ordering::Acquire)
    }
    fn has_vault(&self) -> bool {
        self.0.vault.load(Ordering::Acquire)
    }
    fn epoch_key(&self, _channel_hex: &str, _epoch: u64) -> Option<[u8; 32]> {
        *lock(&self.0.epoch_key)
    }
    fn access_key(&self, _channel_hex: &str, _epoch: u64) -> Option<[u8; 32]> {
        None
    }
    fn find_announce(&self, key: &str) -> SendFuture<'_, Option<Announce>> {
        let found = lock(&self.0.announcements).get(key).cloned();
        Box::pin(async move { found })
    }
    fn ephemeral_announces(&self) -> SendFuture<'_, Vec<Announce>> {
        Box::pin(async { Vec::new() })
    }
    fn refresh_catalog(&self) -> SendFuture<'_, ()> {
        Box::pin(async {})
    }
    fn mark_stored(&self, discovery_key: [u8; 32]) {
        lock(&self.0.stored).insert(discovery_key);
    }
    fn db_disk_bytes(&self) -> u64 {
        self.0.db_bytes.load(Ordering::Acquire)
    }
    fn free_bytes(&self) -> Option<u64> {
        None
    }
    fn flush(&self) -> SendFuture<'_, ()> {
        let storage = lock(&self.0.storage).clone();
        Box::pin(async move {
            if let Some(store) = storage {
                tokio::task::spawn_blocking(move || store.store().flush())
                    .await
                    .unwrap()
                    .unwrap();
            }
        })
    }
    fn compact(&self, policy: Compact) -> SendFuture<'_, ()> {
        lock(&self.0.compactions).push(policy);
        let storage = lock(&self.0.storage).clone();
        Box::pin(async move {
            if let Some(store) = storage {
                tokio::task::spawn_blocking(move || store.store().compact(false))
                    .await
                    .unwrap()
                    .unwrap();
            }
        })
    }
}

/// A scripted view of the HTTP server's streams.
#[derive(Default)]
struct TestStreams {
    progress: Mutex<HashMap<[u8; 32], u64>>,
    streaming: Mutex<HashSet<[u8; 32]>>,
    aborted: Mutex<Vec<[u8; 32]>>,
}

impl BlobStreams for TestStreams {
    fn streaming_keys(&self) -> HashSet<[u8; 32]> {
        lock(&self.streaming).clone()
    }
    fn stream_progress(&self) -> HashMap<[u8; 32], u64> {
        lock(&self.progress).clone()
    }
    fn abort_streams(&self, only: Option<[u8; 32]>, _except: Option<[u8; 32]>) {
        if let Some(key) = only {
            lock(&self.aborted).push(key);
        }
    }
    fn link(&self, key: &[u8; 32], blob: &hyperblobs::BlobId, mime: &str) -> String {
        format!(
            "http://127.0.0.1:0/?key={}&blob={}:{}:{}:{}&type={mime}",
            hex(key),
            blob.block_offset,
            blob.block_length,
            blob.byte_offset,
            blob.byte_length
        )
    }
}

/// A source that serves `data` from the requested offset.
struct BufferSource {
    data: Vec<u8>,
}

impl BufferSource {
    fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl PutSource for BufferSource {
    type Error = std::io::Error;

    async fn open(
        &mut self,
        offset: u64,
        _meta: &mut PutMeta,
    ) -> Result<std::pin::Pin<Box<dyn AsyncRead + Send>>, Self::Error> {
        let rest = self.data[offset as usize..].to_vec();
        let (reader, mut writer) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let _ = writer.write_all(&rest).await;
            let _ = writer.shutdown().await;
        });
        Ok(Box::pin(reader))
    }

    fn on_data(&mut self, _bytes: u64) {}
    fn on_hash_state(&mut self, _state: &[u8], _bytes: u64) {}
}

type TestBlobs = Blobs<TestChannel, TestSwarm, TestHost>;

struct Harness {
    _dir: tempfile::TempDir,
    store: Corestore,
    channel: TestChannel,
    swarm: TestSwarm,
    host: TestHost,
    blobs: TestBlobs,
}

async fn harness(primary: [u8; 32], writer: [u8; 32], clock: u64) -> Harness {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = Corestore::open_with_primary_key(dir.path(), primary).expect("corestore");
    let channel = TestChannel::new(primary, writer);
    let swarm = TestSwarm::default();
    let host = TestHost::new(clock);
    let blobs = Blobs::open(
        dir.path().to_path_buf(),
        store.clone(),
        channel.clone(),
        swarm.clone(),
        host.clone(),
        false,
    )
    .await
    .expect("blobs");
    Harness {
        _dir: dir,
        store,
        channel,
        swarm,
        host,
        blobs,
    }
}

fn primary_from(fixtures: &Value) -> [u8; 32] {
    unhex32(fixtures["primaryKey"].as_str().expect("primaryKey"))
}

fn put_meta(id: &str, title: &str, store: &str) -> PutMeta {
    PutMeta {
        id: id.into(),
        title: title.into(),
        private_entry: None,
        epoch: None,
        store: Some(store.into()),
        expected_size: None,
        extra: Map::new(),
    }
}

fn digest(bytes: &[u8]) -> String {
    use sha2::Digest;
    hex(sha2::Sha256::digest(bytes).as_slice())
}

fn strings(values: &Value) -> Vec<String> {
    values
        .as_array()
        .expect("array")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_owned())
        .collect()
}

async fn raw_blocks(store: &Corestore, key: &[u8; 32]) -> Vec<String> {
    let session = store
        .get(StoreGetOptions::key(*key))
        .await
        .expect("open core");
    let mut out = Vec::new();
    for index in 0..session.length() {
        let block = session
            .core()
            .get_raw_local(index)
            .expect("raw block")
            .expect("local block");
        out.push(digest(&block));
    }
    session.close();
    out
}

// ---------------------------------------------------------------------------------------------
// Ingest parity

#[tokio::test]
async fn fresh_ingest_matches_the_app() {
    let fixtures = fixtures();
    let primary = primary_from(&fixtures);
    let writer = unhex32(fixtures["localKey"].as_str().expect("localKey"));
    let fresh = &fixtures["fresh"];
    let size = fresh["size"].as_u64().expect("size") as usize;
    let expected = fresh["announce"].clone();

    let h = harness(primary, writer, 1_000_000).await;
    let address = h
        .blobs
        .start_server("127.0.0.1:0".parse().unwrap(), 96 * 1024 * 1024)
        .await
        .expect("server");
    let mut meta = put_meta(
        expected["id"].as_str().expect("id"),
        expected["title"].as_str().expect("title"),
        "video:fixture-fresh",
    );
    meta.expected_size = Some(Some(size as u64));
    let published = h
        .blobs
        .publish(meta, BufferSource::new(data(size)))
        .await
        .expect("publish");

    assert_eq!(
        serde_json::to_value(&published.announce).expect("announce json"),
        expected,
        "announce"
    );
    assert_eq!(published.key, fresh["key"].as_str().expect("key"));
    assert_eq!(
        published.channel,
        fresh["channel"].as_str().expect("channel")
    );
    assert_eq!(
        published.magnet_url,
        fresh["magnetUrl"].as_str().expect("magnetUrl")
    );
    assert!(
        published
            .stream_url
            .starts_with(&format!("http://{address}/")),
        "{}",
        published.stream_url
    );
    assert_eq!(
        published
            .stream_url
            .replacen(&format!("http://{address}/"), "", 1),
        fresh["streamUrl"].as_str().expect("streamUrl"),
        "range link"
    );

    let key = unhex32(&published.announce.blobs);
    assert_eq!(
        raw_blocks(&h.store, &key).await,
        strings(&fresh["core"]["blocks"]),
        "raw blocks"
    );
    let recorded: Vec<Value> = strings(&fresh["ops"])
        .iter()
        .map(|text| serde_json::from_str(text).expect("op"))
        .collect();
    let appended: Vec<Value> = h
        .channel
        .raw_ops()
        .iter()
        .map(|text| serde_json::from_str(text).expect("op"))
        .collect();
    assert_eq!(appended, recorded, "channel ops");
    assert!(h.blobs.is_owned_blob(&key));
    assert!(
        h.blobs.open_core(&key).is_none(),
        "the write session is closed at rest"
    );
    assert_eq!(h.swarm.joined().first(), Some(&key));
    h.blobs.stop_server().await;
}

#[tokio::test]
async fn encrypted_ingest_matches_the_app() {
    let fixtures = fixtures();
    let primary = primary_from(&fixtures);
    let writer = unhex32(fixtures["localKey"].as_str().expect("localKey"));
    let epoch_key = unhex32(fixtures["epochKey"].as_str().expect("epochKey"));
    let video_key = unhex32(fixtures["videoKey"].as_str().expect("videoKey"));
    let encrypted = &fixtures["encrypted"];
    let size = encrypted["size"].as_u64().expect("size") as usize;
    let expected = encrypted["announce"].clone();

    let h = harness(primary, writer, 1_000_000).await;
    h.host.set_epoch_key(epoch_key);
    let mut meta = put_meta(
        expected["id"].as_str().expect("id"),
        expected["title"].as_str().expect("title"),
        "video:fixture-encrypted",
    );
    meta.private_entry = Some(true);
    meta.epoch = Some(1);
    meta.extra.insert("storeKey".into(), json!(hex(&video_key)));
    h.blobs
        .start_server("127.0.0.1:0".parse().unwrap(), 96 * 1024 * 1024)
        .await
        .expect("server");
    let published = h
        .blobs
        .publish(meta, BufferSource::new(data(size)))
        .await
        .expect("publish");
    let announce = published.announce.clone();

    assert_eq!(announce.epoch, Some(1));
    let wrapped = announce.wrapped_key.clone().expect("wrapped key");
    assert_eq!(wrapped.len(), 144, "nonce || MAC || ciphertext, hex");
    assert_eq!(h.blobs.resolve_video_key(&announce), Some(video_key));
    assert_eq!(
        h.blobs
            .resolve_blob_encryption_key(&unhex32(&announce.blobs)),
        Some(video_key)
    );
    assert_eq!(
        encrypted["unwrapped"].as_str().expect("unwrapped"),
        hex(&video_key)
    );

    let mut got = serde_json::to_value(&announce).expect("json");
    let mut want = expected.clone();
    // The wrapped key carries a random nonce; everything else is byte-for-byte the app's output.
    got.as_object_mut().unwrap().remove("wrappedKey");
    want.as_object_mut().unwrap().remove("wrappedKey");
    assert_eq!(got, want, "announce");

    let key = unhex32(&announce.blobs);
    assert_eq!(
        raw_blocks(&h.store, &key).await,
        strings(&encrypted["core"]["blocks"]),
        "encrypted raw blocks"
    );
    let decrypted: Vec<String> = {
        let mut opts = StoreGetOptions::key(key);
        opts.encryption_key = Some(video_key);
        let session = h.store.get(opts).await.expect("open with key");
        let mut out = Vec::new();
        for index in 0..session.length() {
            let block = session
                .get(index, GetOptions::default())
                .await
                .unwrap()
                .unwrap();
            out.push(digest(&block));
        }
        session.close();
        out
    };
    assert_eq!(
        decrypted,
        strings(&encrypted["core"]["plain"]),
        "decrypted blocks"
    );
    h.blobs.stop_server().await;
}

#[tokio::test]
async fn resumed_ingest_finishes_the_core() {
    let fixtures = fixtures();
    let primary = primary_from(&fixtures);
    let writer = unhex32(fixtures["localKey"].as_str().expect("localKey"));
    let size = 3 * BLOCK + 1000;
    let payload = data(size);

    let h = harness(primary, writer, 1_000_000).await;
    let mut meta = put_meta("youtube:resumed", "Resumed", "video:fixture-resumed");
    meta.expected_size = Some(Some(size as u64));

    // A restart leaves the first block on disk and the job's expected size unchanged.
    let core = h
        .store
        .get(StoreGetOptions::name("video:fixture-resumed"))
        .await
        .expect("core");
    core.append(&[&payload[..BLOCK]]).await.expect("prefix");
    core.close();

    let announce = h
        .blobs
        .put(meta.clone(), BufferSource::new(payload.clone()))
        .await
        .expect("resumed put");
    assert_eq!(announce.size, size as u64);
    assert_eq!(announce.blob.block_offset, 0);
    assert_eq!(announce.blob.block_length, 4);
    assert_eq!(announce.blob.byte_length, size as u64);
    let key = unhex32(&announce.blobs);
    let session = h.store.get(StoreGetOptions::key(key)).await.expect("open");
    let mut joined = Vec::new();
    for index in 0..session.length() {
        let block = session
            .get(index, GetOptions::default())
            .await
            .unwrap()
            .unwrap();
        joined.extend_from_slice(&block);
    }
    assert_eq!(joined, payload);
    session.close();

    // Nothing left to fetch: the core is already complete.
    let again = h
        .blobs
        .put(meta, BufferSource::new(Vec::new()))
        .await
        .expect("complete");
    assert_eq!(again.sha256, announce.sha256);
    assert_eq!(again.size, announce.size);
}

#[tokio::test]
async fn truncated_transfer_is_rejected() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let mut meta = put_meta("youtube:short", "Short", "video:short");
    meta.expected_size = Some(Some(5000));
    let err = h
        .blobs
        .put(meta, BufferSource::new(vec![1u8; 4000]))
        .await
        .expect_err("truncated");
    assert!(
        matches!(
            err,
            BlobsError::Truncated {
                expected: 5000,
                got: 4000,
                ..
            }
        ),
        "{err}"
    );
    // An unnamed core is purged so a half file never lies on disk.
    assert!(
        h.blobs
            .open_core_keys()
            .iter()
            .all(|k| h.blobs.slot_keys().contains(k))
    );
}

// ---------------------------------------------------------------------------------------------
// Cache ceiling

/// Copies one core into a viewer store over an in-memory replication link, so the viewer's core
/// has blocks but no secret key — the state a downloaded film is in.
async fn replicate_core(
    writer_store: &Corestore,
    viewer_store: &Corestore,
    name: &str,
    blocks: &[Vec<u8>],
) -> [u8; 32] {
    let writer = writer_store
        .get(StoreGetOptions::name(name))
        .await
        .expect("writer core");
    writer.append(blocks).await.expect("append");
    let viewer = viewer_store
        .get(StoreGetOptions::key(*writer.key()))
        .await
        .expect("viewer core");
    let ((ar, aw), (br, bw)) = protomux::memory::duplex(256);
    let handshake = Some([7u8; 64]);
    let ma = protomux::Mux::new(
        ar,
        aw,
        protomux::StreamInfo {
            is_initiator: true,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    );
    let mb = protomux::Mux::new(
        br,
        bw,
        protomux::StreamInfo {
            is_initiator: false,
            handshake_hash: handshake,
            remote_public_key: None,
        },
    );
    writer.core().replicate(&ma);
    viewer.core().replicate(&mb);
    tokio::time::timeout(Duration::from_secs(10), viewer.update(None))
        .await
        .expect("update")
        .expect("update ok");
    for index in 0..blocks.len() as u64 {
        tokio::time::timeout(
            Duration::from_secs(10),
            viewer.get(index, GetOptions::default()),
        )
        .await
        .expect("download")
        .expect("download ok")
        .expect("block");
    }
    assert!(!viewer.writable(), "a downloaded core stays read-only");
    let key = *viewer.key();
    viewer.close();
    writer.close();
    key
}

#[tokio::test]
async fn cache_ceiling_matches_the_app() {
    let fixtures = fixtures();
    let primary = primary_from(&fixtures);
    let writer = unhex32(fixtures["localKey"].as_str().expect("localKey"));
    let trim = &fixtures["cacheTrim"];
    let budget = trim["budget"].as_u64().expect("budget");
    let names: Vec<String> = trim["cores"]
        .as_array()
        .expect("cores")
        .iter()
        .map(|c| c["name"].as_str().expect("name").to_owned())
        .collect();
    let expected_keys: Vec<[u8; 32]> = trim["cores"]
        .as_array()
        .expect("cores")
        .iter()
        .map(|c| unhex32(c["key"].as_str().expect("key")))
        .collect();

    let h = harness(primary, writer, 1_000_000).await;
    let writer_dir = tempfile::tempdir().expect("writer dir");
    let writer_store =
        Corestore::open_with_primary_key(writer_dir.path(), primary).expect("writer store");
    let blocks = vec![data(600 * 1024), data(600 * 1024)];
    let mut keys = Vec::new();
    for name in &names {
        keys.push(replicate_core(&writer_store, &h.store, name, &blocks).await);
    }
    writer_store.close().await;
    assert_eq!(keys, expected_keys, "core keys derive like the app's");

    for key in &keys {
        h.host.tick(1000);
        h.blobs.blob_core(key).await.expect("blob core");
        // The scripted clock decides recency, exactly as the fixture's patched Date.now does.
    }

    let streams = Arc::new(TestStreams::default());
    for pair in trim["progress"].as_array().expect("progress") {
        lock(&streams.progress).insert(
            unhex32(pair[0].as_str().unwrap()),
            pair[1].as_u64().unwrap(),
        );
    }
    for key in trim["streaming"].as_array().expect("streaming") {
        lock(&streams.streaming).insert(unhex32(key.as_str().unwrap()));
    }
    let keep = trim["keep"].as_str().map(unhex32);
    let installed: Arc<dyn BlobStreams> = streams.clone();
    h.blobs.install_streams(installed);
    h.blobs.set_cache_max_bytes(budget as f64);
    let ceiling = h.blobs.enforce_cache_ceiling(keep).await;

    assert_eq!(
        ceiling.evicted,
        trim["result"]["evicted"].as_u64().expect("evicted")
    );
    assert_eq!(
        ceiling.cached_bytes,
        trim["result"]["cachedBytes"].as_u64().expect("cachedBytes")
    );
    for (key, blocks) in trim["remaining"].as_object().expect("remaining") {
        let key32 = unhex32(key);
        let session = h
            .store
            .get(StoreGetOptions::key(key32))
            .await
            .expect("core");
        let left = if session.length() > 0 {
            (0..session.length()).filter(|i| session.has(*i)).count()
        } else {
            0
        };
        session.close();
        assert_eq!(
            left as u64,
            blocks.as_u64().expect("blocks"),
            "remaining blocks for {key}"
        );
    }
    // An evicted core leaves the swarm; the one being served stays.
    let mut left = h.swarm.left();
    left.sort();
    let mut expected_left: Vec<[u8; 32]> = trim["remaining"]
        .as_object()
        .expect("remaining")
        .iter()
        .filter(|(_, blocks)| blocks.as_u64() == Some(0))
        .map(|(key, _)| unhex32(key))
        .collect();
    expected_left.sort();
    assert_eq!(left, expected_left, "every evicted core leaves the swarm");

    // The film being served is never touched.
    let streaming = unhex32(
        trim["streaming"].as_array().expect("streaming")[0]
            .as_str()
            .expect("streaming key"),
    );
    assert!(h.blobs.open_core(&streaming).is_some());
}

#[tokio::test]
#[ignore = "needs the JS app's installed modules; CACHE_FREEZE_BUDGET scales the 64 MiB budget"]
async fn http_stream_survives_cache_ceiling() {
    use hyperblobs::BlobId;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    use tokio::net::TcpStream;
    use tokio::process::Command;
    use tokio::time::{Instant, timeout};

    let budget = std::env::var("CACHE_FREEZE_BUDGET")
        .ok()
        .map(|n| n.parse::<u64>().expect("byte budget"))
        .unwrap_or(64 * 1024 * 1024);
    let block_size = std::env::var("CACHE_FREEZE_BLOCK_BYTES")
        .ok()
        .map(|n| n.parse::<usize>().expect("block size"))
        .unwrap_or(BLOCK);
    let h = harness([0x71; 32], [0x72; 32], 1_000_000).await;
    *lock(&h.host.0.storage) = Some(h.store.clone());
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new("node")
        .arg("tests/js/cache-ceiling.mjs")
        .arg(dir.path())
        .arg(budget.to_string())
        .arg(block_size.to_string())
        .env(
            "PEARTUBE_APP",
            std::env::var("PEARTUBE_APP").unwrap_or_else(|_| {
                format!("{}/projects/peartube", std::env::var("HOME").unwrap())
            }),
        )
        .env("HOME", dir.path())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    let info: Value = serde_json::from_str(
        &timeout(Duration::from_secs(120), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let key = unhex32(info["cores"][1]["key"].as_str().unwrap());
    let old_key = unhex32(info["cores"][0]["key"].as_str().unwrap());
    let viewer = h.blobs.blob_core(&key).await.unwrap();
    let old = h.blobs.blob_core(&old_key).await.unwrap();
    let socket = TcpStream::connect(("127.0.0.1", info["port"].as_u64().unwrap() as u16))
        .await
        .unwrap();
    let stream = peeroxide_dht::secret_stream::SecretStream::new(
        true,
        socket,
        peeroxide_dht::noise::generate_keypair(),
    )
    .await
    .unwrap();
    let mux = protomux::Mux::from_secret_stream(stream);
    h.store.replicate(&mux);
    mux.ready();
    timeout(Duration::from_secs(10), viewer.update(None))
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(10), old.update(None))
        .await
        .unwrap()
        .unwrap();
    timeout(
        Duration::from_secs(120),
        old.download(0, Some(old.length()), true),
    )
    .await
    .expect("old cache download")
    .unwrap();
    h.blobs.sync_block_counter(&old).await;
    let length = viewer.length();
    let addr = h
        .blobs
        .start_server("127.0.0.1:0".parse().unwrap(), 96 * 1024 * 1024)
        .await
        .unwrap();
    let blob = BlobId {
        block_offset: 0,
        block_length: length,
        byte_offset: 0,
        byte_length: length * block_size as u64,
    };
    let entry: Announce = serde_json::from_value(json!({
        "type": "announce", "id": "clip:cache-freeze", "title": "Cache freeze",
        "size": blob.byte_length, "sha256": hex(&[0; 32]), "blobs": hex(&key),
        "blob": { "blockOffset": 0, "blockLength": length, "byteOffset": 0,
            "byteLength": blob.byte_length }
    }))
    .unwrap();
    let url = h.blobs.stream_url(&entry);
    let path = format!("/?{}", url.split_once("/?").unwrap().1);
    h.blobs.set_cache_max_bytes(budget as f64);
    let started = Instant::now();
    let mut socket = TcpStream::connect(addr).await.unwrap();
    socket
        .write_all(
            format!(
                "GET {path} HTTP/1.1\r\nHost: x\r\nRange: bytes=0-\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let mut reader = BufReader::new(socket);
    let mut head = String::new();
    timeout(Duration::from_secs(10), async {
        loop {
            assert!(
                reader.read_line(&mut head).await.unwrap() > 0,
                "HTTP headers"
            );
            if head.ends_with("\r\n\r\n") {
                break;
            }
        }
    })
    .await
    .expect("HTTP headers stalled");
    assert!(head.starts_with("HTTP/1.1 206"), "{head}");
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let blobs = h.blobs.clone();
    let ceiler = tokio::spawn(async move {
        let mut trimmed = 0;
        loop {
            tokio::select! {
                _ = stopped.changed() => return trimmed,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            let before = blobs.cached_total_bytes().await;
            let started = Instant::now();
            let ceiling = blobs.enforce_cache_ceiling(Some(key)).await;
            trimmed += ceiling.evicted;
            eprintln!(
                "cache-freeze: before={before} cached={} evicted={} pass_ms={}",
                ceiling.cached_bytes,
                ceiling.evicted,
                started.elapsed().as_millis()
            );
        }
    });
    let mut last_chunk = Instant::now();
    let mut max_gap = Duration::ZERO;
    let mut max_read = Duration::ZERO;
    let mut block = vec![0; block_size];
    for index in 0..length {
        let read_started = Instant::now();
        timeout(Duration::from_secs(10), reader.read_exact(&mut block))
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "HTTP freeze at block {index}: cached={}, has={}, peers={}",
                    h.blobs.local_blocks(&key) * block_size as u64,
                    viewer.has(index),
                    viewer.core().peer_count()
                )
            })
            .unwrap();
        max_read = max_read.max(read_started.elapsed());
        max_gap = max_gap.max(last_chunk.elapsed());
        last_chunk = Instant::now();
        let mut x = index as u32 + 1;
        for word in block.as_chunks::<4>().0 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            assert_eq!(u32::from_le_bytes(*word), x, "wrong block {index}");
        }
        // Keep the headless consumer below 8 MiB/s so the test doesn't saturate the host.
        tokio::time::sleep(Duration::from_secs_f64(
            block_size as f64 / (8.0 * 1024.0 * 1024.0),
        ))
        .await;
    }
    stop.send(true).unwrap();
    let trimmed = timeout(Duration::from_secs(60), ceiler)
        .await
        .unwrap()
        .unwrap();
    assert!(trimmed > 0, "the run must cross the ceiling");
    let cleared = h.store.get(StoreGetOptions::key(old_key)).await.unwrap();
    assert!(
        !cleared.has(0),
        "the ceiling must evict the older cached film"
    );
    cleared.close();
    assert!(
        !viewer.has(length / 4),
        "the ceiling must also trim spent playing blocks"
    );
    eprintln!(
        "cache-freeze: exact={} trimmed={trimmed} elapsed={:?} max_gap_ms={} max_read_ms={}",
        blob.byte_length,
        started.elapsed(),
        max_gap.as_millis(),
        max_read.as_millis()
    );
    h.blobs.stop_server().await;
    viewer.close();
    old.close();
    h.store.close().await;
    drop(child.stdin.take());
    timeout(Duration::from_secs(10), child.wait())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn store_wakeup_does_not_pin_an_owned_blob_session() {
    let h = harness([0x31; 32], [0x32; 32], 1_000_000).await;
    let owned = h
        .blobs
        .put(
            put_meta("clip:wakeup", "Wakeup", "video:wakeup"),
            BufferSource::new(data(BLOCK)),
        )
        .await
        .expect("put");
    let session = h
        .store
        .get(StoreGetOptions::key(unhex32(&owned.blobs)))
        .await
        .expect("open owned core");
    let core = session.core().clone();
    h.blobs.on_core_wake(&core);
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
    session.close();
    assert_eq!(
        core.session_count(),
        0,
        "the wake observer must not prevent core GC"
    );
}

// ---------------------------------------------------------------------------------------------
// Clearing

#[tokio::test]
async fn clear_cache_wipes_cached_cores_but_keeps_owned_ones() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let owned = h
        .blobs
        .put(
            put_meta("youtube:owned", "Owned", "video:owned"),
            BufferSource::new(data(BLOCK)),
        )
        .await
        .expect("put");
    let owned_key = unhex32(&owned.blobs);
    let _ = h.blobs.set_cache_max_bytes(1024.0 * 1024.0);

    let writer_dir = tempfile::tempdir().expect("writer dir");
    let writer_store =
        Corestore::open_with_primary_key(writer_dir.path(), primary).expect("writer store");
    let cached = replicate_core(&writer_store, &h.store, "cached", &[data(64 * 1024)]).await;
    writer_store.close().await;
    h.blobs.blob_core(&cached).await.expect("blob core");

    let cleared = h.blobs.clear_cache(None, None).await.expect("clear");
    assert!(cleared.cleared_bytes > 0);
    assert_eq!(h.blobs.local_blocks(&cached), 0);
    assert!(h.blobs.is_owned_blob(&owned_key));
    assert!(
        !h.host.compactions().is_empty(),
        "a clear asks the engine to compact"
    );
    // The ledger forgets the cleared core and keeps reporting the owned one.
    assert_eq!(h.blobs.owned(&owned_key).map(|m| m.length), Some(1));
}

// ---------------------------------------------------------------------------------------------
// Legacy migration

#[tokio::test]
async fn rechunk_moves_a_legacy_slot_span_into_its_own_core() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let slot = h.blobs.slot_keys()[0];

    // A legacy variable-block span in the shared slot: 64 KiB blocks, so not uniform 512 KiB.
    let payload = data(3 * BLOCK);
    let slot_session = h.store.get(StoreGetOptions::key(slot)).await.expect("slot");
    let legacy = hyperblobs::Blobs::new(slot_session.core().clone(), 64 * 1024);
    slot_session.close();
    let id = legacy.put(&payload).await.expect("legacy put");
    let announce = Announce {
        kind: "announce".into(),
        id: "youtube:legacy".into(),
        title: "Legacy".into(),
        size: payload.len() as u64,
        sha256: digest(&payload),
        blobs: hex(&slot),
        blob: peartube_native::node::api::Blob {
            block_offset: id.block_offset,
            block_length: id.block_length,
            byte_offset: id.byte_offset,
            byte_length: id.byte_length,
            extra: Map::new(),
        },
        epoch: None,
        wrapped_key: None,
        extra: Map::new(),
    };
    h.channel
        .append(&serde_json::to_value(&announce).expect("json"))
        .await
        .expect("append");
    let before = h.channel.op_count();

    let result = h
        .blobs
        .rechunk_legacy_blobs(RechunkOptions::default())
        .await
        .expect("rechunk");
    assert_eq!(result.total, 1);
    assert_eq!(result.migrated, 1);
    assert_eq!(result.deduped, 0);
    assert_eq!(result.migrated_bytes, payload.len() as u64);
    assert_eq!(result.remaining, 0);

    let ops = h.channel.appended();
    assert_eq!(ops.len(), before + 2, "one announce, one remove");
    let moved = &ops[before];
    assert_ne!(moved["blobs"].as_str().unwrap(), hex(&slot));
    assert_eq!(moved["blob"]["blockOffset"], 0);
    assert_eq!(ops[before + 1]["type"], "remove");
    assert_eq!(
        ops[before + 1]["key"].as_str().unwrap(),
        format!("announce/{}/youtube:legacy", h.channel.writer_hex()),
        "a remove points at the channel view key"
    );

    // The new core holds the same bytes as one uniform 512 KiB block per piece.
    let key = unhex32(moved["blobs"].as_str().unwrap());
    let session = h
        .store
        .get(StoreGetOptions::key(key))
        .await
        .expect("new core");
    assert_eq!(session.length(), 3);
    let mut joined = Vec::new();
    for index in 0..session.length() {
        joined.extend_from_slice(
            &session
                .get(index, GetOptions::default())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert_eq!(joined, payload);
    session.close();
    // The legacy span is spent.
    let slot_session = h.store.get(StoreGetOptions::key(slot)).await.expect("slot");
    assert!(!slot_session.has(id.block_offset));
    slot_session.close();
}

#[tokio::test]
async fn rechunk_dedupes_identical_payloads() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let slot = h.blobs.slot_keys()[0];
    let payload = data(2 * BLOCK);

    let slot_session = h.store.get(StoreGetOptions::key(slot)).await.expect("slot");
    let legacy = hyperblobs::Blobs::new(slot_session.core().clone(), 64 * 1024);
    slot_session.close();
    let mut offsets = Vec::new();
    for _ in 0..2 {
        let id = legacy.put(&payload).await.expect("legacy put");
        offsets.push(id);
    }
    let sha = digest(&payload);
    for (index, id) in offsets.iter().enumerate() {
        let announce = Announce {
            kind: "announce".into(),
            id: format!("youtube:legacy{index}"),
            title: "Legacy".into(),
            size: payload.len() as u64,
            sha256: sha.clone(),
            blobs: hex(&slot),
            blob: peartube_native::node::api::Blob {
                block_offset: id.block_offset,
                block_length: id.block_length,
                byte_offset: id.byte_offset,
                byte_length: id.byte_length,
                extra: Map::new(),
            },
            epoch: None,
            wrapped_key: None,
            extra: Map::new(),
        };
        h.channel
            .append(&serde_json::to_value(&announce).expect("json"))
            .await
            .expect("append");
    }
    let result = h
        .blobs
        .rechunk_legacy_blobs(RechunkOptions::default())
        .await
        .expect("rechunk");
    assert_eq!(result.total, 2);
    assert_eq!(result.migrated, 2);
    assert_eq!(result.deduped, 1, "the second span reuses the first core");
    let ops = h.channel.appended();
    let announces: Vec<&Value> = ops.iter().filter(|op| op["type"] == "announce").collect();
    let last_two: Vec<&Value> = announces.iter().rev().take(2).copied().collect();
    assert_eq!(last_two[0]["blobs"], last_two[1]["blobs"]);
}

// ---------------------------------------------------------------------------------------------
// Legacy channel migration and warm-up

#[tokio::test]
async fn sync_local_channel_backfills_a_legacy_bee() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let legacy_store = h.store.namespace("channel");
    let bee = hyperbee::Bee::open(&legacy_store, None).await.expect("bee");
    let announce = Announce {
        kind: "announce".into(),
        id: "youtube:bee".into(),
        title: "From the bee".into(),
        size: 10,
        sha256: hex(&[0x55; 32]),
        blobs: hex(&[0x66; 32]),
        blob: peartube_native::node::api::Blob {
            block_offset: 0,
            block_length: 1,
            byte_offset: 0,
            byte_length: 10,
            extra: Map::new(),
        },
        epoch: None,
        wrapped_key: None,
        extra: Map::new(),
    };
    let mut batch = bee.batch();
    batch.put(
        b"entry/youtube:bee".as_slice(),
        serde_json::to_vec(&announce).expect("json"),
    );
    batch.flush().await.expect("flush");

    let result = h.blobs.sync_local_channel().await.expect("sync");
    assert_eq!(result.backfilled, 1);
    assert_eq!(result.total, 1);
    assert_eq!(h.channel.appended()[0]["id"], "youtube:bee");
    // Idempotent: the second pass finds the entry already there.
    let again = h.blobs.sync_local_channel().await.expect("sync again");
    assert_eq!(again.backfilled, 0);
    assert_eq!(again.total, 1);
}

#[tokio::test]
async fn warm_joins_the_swarm_and_registers_ranges() {
    let primary = [0x11; 32];
    let h = harness(primary, [0x22; 32], 1_000_000).await;
    let announce = Announce {
        kind: "announce".into(),
        id: "youtube:warm".into(),
        title: "Warm".into(),
        size: 4 * BLOCK as u64,
        sha256: hex(&[0x55; 32]),
        blobs: hex(&unhex32(&"77".repeat(32))),
        blob: peartube_native::node::api::Blob {
            block_offset: 0,
            block_length: 4,
            byte_offset: 0,
            byte_length: 4 * BLOCK as u64,
            extra: Map::new(),
        },
        epoch: None,
        wrapped_key: None,
        extra: Map::new(),
    };
    h.blobs.warm(&announce).await.expect("warm");
    let key = unhex32(&announce.blobs);
    assert_eq!(h.swarm.joined(), vec![key]);
    assert!(h.blobs.open_core(&key).is_some());
    // A second warm for the same span re-joins (as the JS does) and keeps one range.
    h.blobs.warm(&announce).await.expect("warm again");
    assert_eq!(h.swarm.joined(), vec![key, key]);
}

#[test]
fn blobs_error_names_the_missing_vault() {
    let err = BlobsError::VaultRequired;
    assert!(err.to_string().contains("12-word recovery vault"));
}
