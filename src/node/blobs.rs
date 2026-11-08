//! Per-video blob cores, the ingest pipeline, blob encryption and the download cache, ported from
//! the app's `src/node/blobs.js`.
//!
//! Every video gets its own 512 KiB-piece Hypercore, closed at rest. Legacy shared slots
//! (`blobs`, `blobs-1`, …) stay readable and migratable in place. The cache ledger, its ceiling and
//! the sliding-window trim behind a playhead are the same estimates `status()` reports.
//!
//! Three seams keep this testable: [`ChannelView`] (the channel the announce ops land in),
//! [`ContentSwarm`] (join/leave and the replication window) and [`BlobHost`] (catalog, vault keys,
//! store compaction, clock and closing flag). The blob HTTP server is real; [`BlobStreams`] lets a
//! caller install one that lives elsewhere (or a scripted one in tests).
//!
//! Deliberate differences from the JS:
//!
//! - No checkpointed SHA-256 state (`sodium-native`'s exportable state). A resumed ingest re-reads
//!   the committed prefix off the core and folds it into the hasher before the tail arrives, which
//!   is the same byte order without the state blob, so `PutSource::on_hash_state` is never called.
//! - `PutSource` errors carry no `restart` flag: a named core is always kept across a failed ingest
//!   and an unnamed one is always purged. A caller that must drop a named core calls `drop_store`.
//! - hypercore-rs has no `upload` event, so serving-driven read-ahead (`warmAhead`, `bulkPeers`
//!   filtering) has no trigger; `mark_bulk_peer`/`set_player_active` keep their shape.
//! - The HTTP resolver receives only a key, so `info.head`/`info.range`-driven warmth and
//!   `lastHttpViewAt`'s head exclusion cannot be reproduced.
//! - No peer `inflight` introspection, so the ceiling cannot skip a core that a peer is fetching.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use byte_stream::server::{BlobServer, Resolve, ResolveFuture, Resolved, ServerOptions};
use bytes::{Bytes, BytesMut};
use corestore::{Corestore, GetOptions as StoreGetOptions};
use hyperbee::{Bee, RangeOptions};
use hyperblobs::{BlobId, Blobs as Hyperblobs};
use hypercore::{CoreError, CoreEvent, GetOptions, RequestBag, Session};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};

use crate::mnemonic::{unwrap_video_key, wrap_video_key};
use crate::node::PutSource;
use crate::node::api::{
    Announce, Blob, CacheCeiling, CacheCleared, Published, PutMeta, RechunkResult, StorageGc,
    SyncResult,
};
use crate::protocol::{
    BLOB_BLOCK_SIZE, BlobSize, clean_text, decode_op, format_watch_uri, is_hex64,
    is_system_view_key, is_uniform_512,
};

/// Never spend the last of the disk on the cache, whatever the user contributes.
const CACHE_DISK_RESERVE: u64 = 2 * 1024 * 1024 * 1024;
/// Cached blob spans remembered per core before the slice cache is dropped.
const MAX_SLICES: usize = 512;
/// Warm read-ahead ranges kept at once (JS: 8, oldest dropped).
const MAX_WARM_RANGES: usize = 8;
/// Open cached blob cores besides the slots.
const MAX_OPEN_CORES: usize = 24;
const MAX_OPEN_CORES_WORKLET: usize = 6;
/// Sessions kept for owned cores answering HTTP, and how long each lingers.
const MAX_HTTP_OWNED: usize = 4;
const HTTP_OWNED_TTL: Duration = Duration::from_secs(30);
/// Recency of a film still considered "being watched" when the ceiling runs without a keep hint.
const RECENT_WINDOW_MS: u64 = 10 * 60 * 1000;
/// Debounce for persisting the LRU ledger (one file write per burst, not per block).
const LRU_WRITE_DELAY: Duration = Duration::from_secs(2);
/// First ceiling pass after a cache budget is set waits for the catalog walk.
const CEILING_BOOT_DELAY: Duration = Duration::from_secs(5);
const CEILING_DELAY: Duration = Duration::from_secs(2);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn hex(bytes: &[u8]) -> String {
    crate::protocol::hex_encode(bytes)
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let raw = crate::protocol::hex_decode(s)?;
    raw.try_into().ok()
}

/// JS `Math.round(numerator / denominator * scale)` for the cache estimate.
fn scaled(numerator: u64, denominator: u64, scale: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    let v = (numerator as f64 / denominator as f64) * scale as f64;
    v.round().min(u64::MAX as f64) as u64
}

/// Everything the blob store can fail with.
#[derive(Debug)]
pub enum BlobsError {
    InvalidId(String),
    VaultRequired,
    Truncated {
        id: String,
        expected: u64,
        got: u64,
    },
    MissingLocalBlock,
    HashOutOfStep {
        id: String,
        hashed: u64,
        size: u64,
    },
    InvalidAnnounce,
    Sha256Mismatch {
        id: String,
        expected: String,
        got: String,
    },
    Closing,
    Core(CoreError),
    Store(corestore::Error),
    Bee(hyperbee::Error),
    Channel(String),
    Source(String),
    Io(std::io::Error),
    Json(serde_json::Error),
}

impl std::fmt::Display for BlobsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidId(id) => write!(f, "Invalid id {id}"),
            Self::VaultRequired => {
                write!(
                    f,
                    "12-word recovery vault required to encrypt private channel media"
                )
            }
            Self::Truncated { id, expected, got } => {
                write!(
                    f,
                    "Truncated transfer for {id}: expected {expected} bytes, got {got}"
                )
            }
            Self::MissingLocalBlock => write!(f, "Missing local block during rechunk"),
            Self::HashOutOfStep { id, hashed, size } => {
                write!(f, "Hash is out of step for {id}: {hashed} of {size} bytes")
            }
            Self::InvalidAnnounce => write!(f, "Invalid channel announce payload"),
            Self::Sha256Mismatch { id, expected, got } => {
                write!(
                    f,
                    "SHA-256 mismatch rechunking {id}: expected {expected}, got {got}"
                )
            }
            Self::Closing => write!(f, "Node closing"),
            Self::Core(e) => write!(f, "{e}"),
            Self::Store(e) => write!(f, "{e}"),
            Self::Bee(e) => write!(f, "{e}"),
            Self::Channel(e) => write!(f, "{e}"),
            Self::Source(e) => write!(f, "{e}"),
            Self::Io(e) => write!(f, "{e}"),
            Self::Json(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BlobsError {}

impl From<CoreError> for BlobsError {
    fn from(e: CoreError) -> Self {
        Self::Core(e)
    }
}

impl From<corestore::Error> for BlobsError {
    fn from(e: corestore::Error) -> Self {
        Self::Store(e)
    }
}

impl From<hyperbee::Error> for BlobsError {
    fn from(e: hyperbee::Error) -> Self {
        Self::Bee(e)
    }
}

impl From<std::io::Error> for BlobsError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<serde_json::Error> for BlobsError {
    fn from(e: serde_json::Error) -> Self {
        Self::Json(e)
    }
}

/// A boxed future every seam returns, so a host can be driven from a spawned task.
pub type SendFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// What the blob store reads from the node besides the channel: the catalog, the vault's epoch
/// keys, the storage engine's maintenance calls, the clock and the closing flag.
pub trait BlobHost: Send + Sync {
    fn closing(&self) -> bool;
    /// Something the app shows changed (the JS `ctx.update()`).
    fn update(&self);
    /// Wall clock in milliseconds; tests script it so LRU order is deterministic.
    fn now_ms(&self) -> u64;
    fn has_vault(&self) -> bool;
    /// `idVault.epochKey(channelHex, epoch)`.
    fn epoch_key(&self, channel_hex: &str, epoch: u64) -> Option<[u8; 32]>;
    /// An access key accepted for `channelHex:epoch`, if one was imported.
    fn access_key(&self, channel_hex: &str, epoch: u64) -> Option<[u8; 32]>;
    /// `ctx.catalog.findEntry(key)`.
    fn find_announce(&self, key: &str) -> SendFuture<'_, Option<Announce>>;
    /// Entries a direct watch link added that the catalog does not persist.
    fn ephemeral_announces(&self) -> SendFuture<'_, Vec<Announce>>;
    /// Walk the whole catalog once, so a cache pass sees blocks that survived a restart.
    fn refresh_catalog(&self) -> SendFuture<'_, ()>;
    /// `ctx.storage.storedDisc.add(hex)`.
    fn mark_stored(&self, discovery_key: [u8; 32]);
    fn db_disk_bytes(&self) -> u64;
    /// Bytes the filesystem can spare, or `None` when unknown (unbounded).
    fn free_bytes(&self) -> Option<u64>;
    fn flush(&self) -> SendFuture<'_, ()>;
    fn compact(&self, policy: Compact) -> SendFuture<'_, ()>;
}

/// How the storage engine should compact. The engine maps this to RocksDB exactly as the JS
/// describes: forced relocation (`exclusive`, blob GC policy 1, age cutoff 1.0) only for a full
/// wipe with no local blobs left, non-exclusive SST compaction (policy 2, bottommost 0) otherwise.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Compact {
    pub full: bool,
    pub streaming: bool,
    pub has_local_blobs: bool,
}

/// The channel the announce ops land in, as far as blobs are concerned.
/// The view's keys and values (`channel.view.createReadStream()`).
pub type ViewEntries = Vec<(Vec<u8>, Vec<u8>)>;

pub trait ChannelView: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn local_writer(&self) -> [u8; 32];
    fn primary_key(&self) -> [u8; 32];
    fn current_epoch(&self) -> SendFuture<'_, u64>;
    fn local_role(&self) -> SendFuture<'_, String>;
    fn update(&self) -> SendFuture<'_, Result<(), Self::Error>>;
    /// `channel.view.createReadStream()`: every view key and value, newest first.
    fn entries(&self) -> SendFuture<'_, Result<ViewEntries, Self::Error>>;
    fn append(&self, op: &Value) -> SendFuture<'_, Result<(), Self::Error>>;
    fn add_secondary_writer(&self, writer_hex: &str);
    fn mark_has_entries(&self);
    fn sync_writer_cores(&self) -> SendFuture<'_, ()>;
}

/// Blob swarm membership and the replication window, as `ctx.net` exposes them.
pub trait ContentSwarm: Send + Sync {
    fn join(&self, key: [u8; 32]);
    fn leave(&self, key: [u8; 32]);
    fn down_bps(&self) -> f64;
    fn up_bps(&self) -> f64;
    fn inflight_range(&self) -> Option<(u32, u32)>;
}

/// The live HTTP range server's view of streams, for the cache ceiling.
pub trait BlobStreams: Send + Sync {
    fn streaming_keys(&self) -> HashSet<[u8; 32]>;
    fn stream_progress(&self) -> HashMap<[u8; 32], u64>;
    fn abort_streams(&self, only: Option<[u8; 32]>, except: Option<[u8; 32]>);
    fn link(&self, key: &[u8; 32], blob: &BlobId, mime: &str) -> String;
}

struct ServerStreams(Arc<BlobServer>);

impl BlobStreams for ServerStreams {
    fn streaming_keys(&self) -> HashSet<[u8; 32]> {
        self.0.streaming_keys()
    }
    fn stream_progress(&self) -> HashMap<[u8; 32], u64> {
        self.0.stream_progress()
    }
    fn abort_streams(&self, only: Option<[u8; 32]>, except: Option<[u8; 32]>) {
        self.0.abort_streams(only, except);
    }
    fn link(&self, key: &[u8; 32], blob: &BlobId, mime: &str) -> String {
        self.0.link(key, blob, mime)
    }
}

/// A video's blocks in its own core.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedMeta {
    pub length: u64,
    pub byte_length: u64,
}

/// What the ledger knows about a core this node only has parts of.
#[derive(Clone, Debug, Default)]
pub struct CachedMeta {
    pub length: u64,
    pub byte_length: u64,
    pub blocks: u64,
    pub slices: HashMap<(u64, u64), u64>,
}

/// A video encrypted with a wrapped key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EncryptedMeta {
    pub source: [u8; 32],
    pub epoch: u64,
    pub wrapped_key: String,
}

/// Options for [`Blobs::evict_behind`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EvictOptions {
    pub playhead_byte: u64,
    pub keep_behind_bytes: u64,
    pub max_bytes: Option<u64>,
}

impl Default for EvictOptions {
    fn default() -> Self {
        Self {
            playhead_byte: 0,
            keep_behind_bytes: 64 * 1024 * 1024,
            max_bytes: None,
        }
    }
}

/// Migration progress, the shape the app's `onProgress` sees.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MigrationProgress {
    pub total: u64,
    pub migrated: u64,
    pub deduped: u64,
    pub remaining: u64,
    pub migrated_bytes: u64,
    pub current_id: Option<String>,
    pub current_title: Option<String>,
}

/// A rechunk job's progress callback.
pub type MigrationProgressHook = dyn FnMut(&MigrationProgress) + Send;
/// A rechunk job's per-entry callback (`onMigrate(oldTag, target, sample)`).
pub type MigrationHook = dyn FnMut(&str, &Announce, &Announce) + Send;

/// [`Blobs::rechunk_legacy_blobs`] options.
#[derive(Default)]
pub struct RechunkOptions {
    pub id: Option<String>,
    pub foreground: bool,
    pub limit: Option<u64>,
    pub on_progress: Option<Box<MigrationProgressHook>>,
    pub on_migrate: Option<Box<MigrationHook>>,
}

struct Slot {
    session: Arc<Session>,
    key: [u8; 32],
}

struct CoreEntry {
    session: Arc<Session>,
    seq: u64,
}

#[derive(Default)]
struct Cores {
    map: HashMap<[u8; 32], CoreEntry>,
    next_seq: u64,
}

#[derive(Default)]
struct Ledger {
    owned: HashMap<[u8; 32], OwnedMeta>,
    owned_by_discovery: HashMap<[u8; 32], [u8; 32]>,
    cached: HashMap<[u8; 32], CachedMeta>,
    cached_by_discovery: HashMap<[u8; 32], [u8; 32]>,
    encrypted: HashMap<[u8; 32], EncryptedMeta>,
    unlocked: HashMap<[u8; 32], [u8; 32]>,
    last_used: HashMap<[u8; 32], (u64, u64)>,
    discs: HashMap<[u8; 32], [u8; 32]>,
    absent: HashSet<[u8; 32]>,
    next_use_seq: u64,
}

#[derive(Default)]
struct Ceilings {
    max_bytes: u64,
    growth: u64,
    timer: bool,
}

#[derive(Default)]
struct Warm {
    ranges: Vec<(String, Vec<RequestBag>)>,
}

struct HttpOwned {
    session: Arc<Session>,
    generation: u64,
}

#[derive(Default)]
struct BlockCounter {
    blocks: AtomicU64,
    slices: Mutex<HashMap<(u64, u64), u64>>,
}

impl BlockCounter {
    fn blocks(&self) -> u64 {
        self.blocks.load(Ordering::Relaxed)
    }
    fn remember(&self, tag: (u64, u64), blocks: u64) {
        let mut slices = lock(&self.slices);
        if slices.len() >= MAX_SLICES {
            slices.clear();
        }
        slices.insert(tag, blocks);
    }
}

struct Inner<C: ChannelView, S: ContentSwarm, H: BlobHost> {
    store: Corestore,
    storage: PathBuf,
    channel: C,
    swarm: S,
    host: H,
    worklet: bool,
    slots: Vec<Slot>,
    slot_set: HashSet<[u8; 32]>,
    cores: Mutex<Cores>,
    ledger: Mutex<Ledger>,
    ceilings: Mutex<Ceilings>,
    warm: Mutex<Warm>,
    block_counts: Mutex<HashMap<[u8; 32], Arc<BlockCounter>>>,
    http_owned: Mutex<HashMap<[u8; 32], HttpOwned>>,
    http_generation: AtomicU64,
    evicted_to: Mutex<HashMap<[u8; 32], u64>>,
    bulk_peers: Mutex<HashSet<String>>,
    player_active: AtomicBool,
    streams: Mutex<Option<Arc<dyn BlobStreams>>>,
    gc_queue: tokio::sync::Mutex<()>,
    ceiling_pass: tokio::sync::Mutex<()>,
    lru_pending: AtomicBool,
    last_http_view_at: AtomicU64,
    server_stop: tokio::sync::Mutex<Option<Arc<BlobServer>>>,
}

/// Per-video blob cores, ingest, encryption and the download cache.
pub struct Blobs<C: ChannelView, S: ContentSwarm, H: BlobHost> {
    inner: Arc<Inner<C, S, H>>,
}

impl<C: ChannelView, S: ContentSwarm, H: BlobHost> Clone for Blobs<C, S, H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<C, S, H> Blobs<C, S, H>
where
    C: ChannelView + 'static,
    S: ContentSwarm + 'static,
    H: BlobHost + 'static,
{
    /// Opens the legacy slots, then hydrates the cores this writer owns.
    pub async fn open(
        storage: PathBuf,
        store: Corestore,
        channel: C,
        swarm: S,
        host: H,
        worklet: bool,
    ) -> Result<Self, BlobsError> {
        let mut slots: Vec<Slot> = Vec::new();
        let shared = store.get(StoreGetOptions::name("blobs")).await?;
        slots.push(Slot {
            key: *shared.key(),
            session: Arc::new(shared),
        });
        for i in 1..64 {
            let session = store
                .get(StoreGetOptions::name(format!("blobs-{i}")))
                .await?;
            if session.length() == 0 {
                session.close();
                if i >= 4 {
                    break;
                }
                continue;
            }
            slots.push(Slot {
                key: *session.key(),
                session: Arc::new(session),
            });
        }
        let slot_set: HashSet<[u8; 32]> = slots.iter().map(|s| s.key).collect();
        let inner = Arc::new(Inner {
            store,
            storage,
            channel,
            swarm,
            host,
            worklet,
            slots,
            slot_set,
            cores: Mutex::new(Cores::default()),
            ledger: Mutex::new(Ledger::default()),
            ceilings: Mutex::new(Ceilings::default()),
            warm: Mutex::new(Warm::default()),
            block_counts: Mutex::new(HashMap::new()),
            http_owned: Mutex::new(HashMap::new()),
            http_generation: AtomicU64::new(0),
            evicted_to: Mutex::new(HashMap::new()),
            bulk_peers: Mutex::new(HashSet::new()),
            player_active: AtomicBool::new(false),
            streams: Mutex::new(None),
            gc_queue: tokio::sync::Mutex::new(()),
            ceiling_pass: tokio::sync::Mutex::new(()),
            lru_pending: AtomicBool::new(false),
            last_http_view_at: AtomicU64::new(0),
            server_stop: tokio::sync::Mutex::new(None),
        });
        let blobs = Self { inner };
        let slot_sessions: Vec<Arc<Session>> = blobs
            .inner
            .slots
            .iter()
            .map(|s| s.session.clone())
            .collect();
        for session in slot_sessions {
            let key = *session.key();
            blobs.ensure_block_counter(&session).await;
            let mut cores = lock(&blobs.inner.cores);
            cores.next_seq += 1;
            let seq = cores.next_seq;
            cores.map.insert(key, CoreEntry { session, seq });
        }
        blobs.load_lru();
        blobs.hydrate_owned().await?;
        Ok(blobs)
    }

    fn from_inner(inner: Arc<Inner<C, S, H>>) -> Self {
        Self { inner }
    }

    pub fn channel(&self) -> &C {
        &self.inner.channel
    }

    pub fn host(&self) -> &H {
        &self.inner.host
    }

    pub fn blobs_key(&self) -> [u8; 32] {
        self.inner.slots.first().map(|s| s.key).unwrap_or_default()
    }

    pub fn slot_keys(&self) -> Vec<[u8; 32]> {
        self.inner.slots.iter().map(|s| s.key).collect()
    }

    pub fn is_owned_blob(&self, key: &[u8; 32]) -> bool {
        self.inner.slot_set.contains(key) || lock(&self.inner.ledger).owned.contains_key(key)
    }

    pub fn owned(&self, key: &[u8; 32]) -> Option<OwnedMeta> {
        lock(&self.inner.ledger).owned.get(key).copied()
    }

    pub fn owned_blobs(&self) -> HashMap<[u8; 32], OwnedMeta> {
        lock(&self.inner.ledger).owned.clone()
    }

    pub fn cached_blobs(&self) -> HashMap<[u8; 32], CachedMeta> {
        lock(&self.inner.ledger).cached.clone()
    }

    pub fn open_core(&self, key: &[u8; 32]) -> Option<Arc<Session>> {
        lock(&self.inner.cores)
            .map
            .get(key)
            .map(|e| e.session.clone())
    }

    pub fn open_core_keys(&self) -> Vec<[u8; 32]> {
        lock(&self.inner.cores).map.keys().copied().collect()
    }

    pub async fn inflight_blocks(&self) -> u64 {
        let sessions: Vec<_> = lock(&self.inner.cores).map.values()
            .map(|entry| entry.session.clone()).collect();
        let mut count = 0;
        for session in sessions {
            count += session.core().inflight_requests().await as u64;
        }
        count
    }

    pub fn last_http_view_at(&self) -> u64 {
        self.inner.last_http_view_at.load(Ordering::Relaxed)
    }

    pub fn absent_disc(&self) -> HashSet<[u8; 32]> {
        lock(&self.inner.ledger).absent.clone()
    }

    pub fn clear_absent_disc(&self, discovery_key: &[u8; 32]) {
        lock(&self.inner.ledger).absent.remove(discovery_key);
    }

    pub fn blob_disc(&self, key: &[u8; 32]) -> [u8; 32] {
        if let Some(d) = lock(&self.inner.ledger).discs.get(key) {
            return *d;
        }
        let d = hypercore::crypto::discovery_key(key);
        lock(&self.inner.ledger).discs.insert(*key, d);
        d
    }

    fn register_owned(&self, key: [u8; 32], meta: OwnedMeta) {
        let disc = hypercore::crypto::discovery_key(&key);
        let mut ledger = lock(&self.inner.ledger);
        ledger.owned.insert(key, meta);
        ledger.owned_by_discovery.insert(disc, key);
    }

    fn drop_owned(&self, key: &[u8; 32]) {
        let disc = hypercore::crypto::discovery_key(key);
        {
            let mut ledger = lock(&self.inner.ledger);
            ledger.owned.remove(key);
            ledger.owned_by_discovery.remove(&disc);
            ledger.cached.remove(key);
        }
        let http = lock(&self.inner.http_owned).remove(key);
        if let Some(rec) = http {
            rec.session.close();
        }
    }

    fn load_lru(&self) {
        let path = self.inner.storage.join("cache-lru.json");
        let Ok(text) = std::fs::read_to_string(&path) else {
            return;
        };
        let Ok(saved) = serde_json::from_str::<Map<String, Value>>(&text) else {
            return;
        };
        let mut ledger = lock(&self.inner.ledger);
        for (k, v) in saved {
            let Some(at) = v.as_u64() else { continue };
            if !is_hex64(&k) {
                continue;
            }
            let Some(key) = parse_hex32(&k) else { continue };
            ledger.next_use_seq += 1;
            let seq = ledger.next_use_seq;
            ledger.last_used.insert(key, (at, seq));
        }
    }

    fn write_lru(&self) {
        let entries: Map<String, Value> = {
            let ledger = lock(&self.inner.ledger);
            let mut out = Map::new();
            for (key, (at, _)) in &ledger.last_used {
                out.insert(hex(key), Value::from(*at));
            }
            out
        };
        let path = self.inner.storage.join("cache-lru.json");
        let text = Value::Object(entries).to_string();
        write_private(&path, text.as_bytes());
    }

    fn touch_cached(&self, key: Option<[u8; 32]>) {
        let Some(key) = key else { return };
        if self.inner.host.closing() {
            return;
        }
        let at = self.inner.host.now_ms();
        {
            let mut ledger = lock(&self.inner.ledger);
            ledger.next_use_seq += 1;
            let seq = ledger.next_use_seq;
            ledger.last_used.insert(key, (at, seq));
        }
        if self.inner.lru_pending.swap(true, Ordering::AcqRel) {
            return;
        }
        let inner = self.inner.clone();
        tokio::spawn(async move {
            tokio::time::sleep(LRU_WRITE_DELAY).await;
            inner.lru_pending.store(false, Ordering::Release);
            Blobs::from_inner(inner).write_lru();
        });
    }

    /// `recordCached`: the ledger entry the estimate reads for a core this node does not own.
    pub fn record_cached(&self, key: [u8; 32], length: u64, byte_length: u64, blocks: u64) {
        self.record_cached_with(key, length, byte_length, blocks, None);
    }

    fn record_cached_with(
        &self,
        key: [u8; 32],
        length: u64,
        byte_length: u64,
        blocks: u64,
        prev_slices: Option<HashMap<(u64, u64), u64>>,
    ) {
        if self.is_owned_blob(&key) {
            return;
        }
        let disc = hypercore::crypto::discovery_key(&key);
        let mut ledger = lock(&self.inner.ledger);
        let slices = prev_slices.unwrap_or_else(|| {
            ledger
                .cached
                .get(&key)
                .filter(|m| m.length == length && m.blocks == blocks)
                .map(|m| m.slices.clone())
                .unwrap_or_default()
        });
        ledger.cached.insert(
            key,
            CachedMeta {
                length,
                byte_length,
                blocks,
                slices,
            },
        );
        ledger.discs.entry(key).or_insert(disc);
        if blocks > 0 {
            ledger.cached_by_discovery.insert(disc, key);
        } else {
            ledger.cached_by_discovery.remove(&disc);
        }
    }

    /// `resolveVideoKey`: unwraps and remembers a video's key from its announce.
    pub fn resolve_video_key(&self, entry: &Announce) -> Option<[u8; 32]> {
        let wrapped = entry.wrapped_key.as_ref()?;
        let epoch = entry.epoch.unwrap_or(1);
        let blobs = parse_hex32(&entry.blobs)?;
        let source = self.inner.channel.primary_key();
        {
            let mut ledger = lock(&self.inner.ledger);
            ledger.encrypted.insert(
                blobs,
                EncryptedMeta {
                    source,
                    epoch,
                    wrapped_key: wrapped.clone(),
                },
            );
            if let Some(key) = ledger.unlocked.get(&blobs) {
                return Some(*key);
            }
        }
        let source_hex = hex(&source);
        let mut candidates = Vec::new();
        if let Some(ek) = self.inner.host.epoch_key(&source_hex, epoch) {
            candidates.push(ek);
        }
        if let Some(ak) = self.inner.host.access_key(&source_hex, epoch) {
            candidates.push(ak);
        }
        for ek in candidates {
            let Some(vk) = unwrap_video_key(wrapped, &ek) else {
                continue;
            };
            let Ok(key) = <[u8; 32]>::try_from(vk.as_slice()) else {
                continue;
            };
            lock(&self.inner.ledger).unlocked.insert(blobs, key);
            return Some(key);
        }
        None
    }

    /// The encryption key for a blob, if one was unwrapped already.
    pub fn resolve_blob_encryption_key(&self, key: &[u8; 32]) -> Option<[u8; 32]> {
        let (unlocked, meta) = {
            let ledger = lock(&self.inner.ledger);
            (
                ledger.unlocked.get(key).copied(),
                ledger.encrypted.get(key).cloned(),
            )
        };
        if let Some(k) = unlocked {
            return Some(k);
        }
        let meta = meta?;
        let entry = Announce {
            kind: "announce".into(),
            id: String::new(),
            title: String::new(),
            size: 0,
            sha256: String::new(),
            blobs: hex(key),
            blob: Blob {
                block_offset: 0,
                block_length: 0,
                byte_offset: 0,
                byte_length: 0,
                extra: Map::new(),
            },
            epoch: Some(meta.epoch),
            wrapped_key: Some(meta.wrapped_key),
            extra: Map::new(),
        };
        self.resolve_video_key(&entry)
    }

    fn is_warming(&self, key: &[u8; 32]) -> bool {
        let prefix = format!("{}:", hex(key));
        lock(&self.inner.warm)
            .ranges
            .iter()
            .any(|(tag, _)| tag.starts_with(&prefix))
    }

    fn forget_warm(&self, key: &[u8; 32]) {
        let prefix = format!("{}:", hex(key));
        let dropped: Vec<Vec<RequestBag>> = {
            let mut warm = lock(&self.inner.warm);
            let mut kept = Vec::with_capacity(warm.ranges.len());
            let mut dropped = Vec::new();
            for (tag, bags) in warm.ranges.drain(..) {
                if tag.starts_with(&prefix) {
                    dropped.push(bags);
                } else {
                    kept.push((tag, bags));
                }
            }
            warm.ranges = kept;
            dropped
        };
        for bags in dropped {
            destroy_bags(&bags);
        }
    }

    /// Opens a core session by key with this node's replication window and blob key.
    async fn open_key(&self, key: [u8; 32], inactive: bool) -> Result<Session, BlobsError> {
        let mut opts = StoreGetOptions::key(key);
        opts.inactive = inactive;
        opts.encryption_key = self.resolve_blob_encryption_key(&key);
        opts.inflight_range = self.inner.swarm.inflight_range();
        Ok(self.inner.store.get(opts).await?)
    }

    /// The per-core local block counter, with its download/append listener running.
    async fn ensure_block_counter(&self, session: &Arc<Session>) -> Arc<BlockCounter> {
        let key = *session.key();
        if let Some(counter) = lock(&self.inner.block_counts).get(&key) {
            return counter.clone();
        }
        let counter = Arc::new(BlockCounter::default());
        {
            let mut map = lock(&self.inner.block_counts);
            if let Some(existing) = map.get(&key) {
                return existing.clone();
            }
            map.insert(key, counter.clone());
        }
        counter
            .blocks
            .store(count_local(session, 0, session.length()), Ordering::Relaxed);
        let inner = self.inner.clone();
        let listener = session.clone();
        let shared = counter.clone();
        tokio::spawn(async move {
            let counter = shared;
            let mut events = listener.events();
            loop {
                let event = match events.recv().await {
                    Ok(event) => event,
                    Err(_) => break,
                };
                match event {
                    CoreEvent::Download { byte_length, .. } => {
                        counter.blocks.fetch_add(1, Ordering::Relaxed);
                        lock(&counter.slices).clear();
                        let blobs = Blobs::from_inner(inner.clone());
                        blobs.record_cached(
                            key,
                            listener.length(),
                            content_bytes(&listener),
                            counter.blocks(),
                        );
                        blobs.touch_cached(Some(key));
                        blobs.grew(byte_length);
                    }
                    CoreEvent::Append { .. } => {
                        let blocks = count_local(&listener, 0, listener.length());
                        counter.blocks.store(blocks, Ordering::Relaxed);
                        lock(&counter.slices).clear();
                        Blobs::from_inner(inner.clone()).record_cached(
                            key,
                            listener.length(),
                            content_bytes(&listener),
                            blocks,
                        );
                    }
                    CoreEvent::Close => break,
                    _ => {}
                }
            }
        });
        counter
    }

    fn grew(&self, byte_length: u64) {
        let (max, step) = {
            let mut ceilings = lock(&self.inner.ceilings);
            ceilings.growth += if byte_length > 0 {
                byte_length
            } else {
                1 << 19
            };
            let max = ceilings.max_bytes;
            (max, (256u64 * 1024 * 1024).max(max / 50))
        };
        if max == 0 {
            return;
        }
        let mut ceilings = lock(&self.inner.ceilings);
        if ceilings.growth >= step {
            ceilings.growth = 0;
            drop(ceilings);
            self.schedule_ceiling(None);
        }
    }

    /// `syncBlockCounter`: recount the whole core and drop cached spans.
    pub async fn sync_block_counter(&self, session: &Session) {
        let blocks = count_local(session, 0, session.length());
        let key = *session.key();
        if let Some(counter) = lock(&self.inner.block_counts).get(&key).cloned() {
            lock(&counter.slices).clear();
            counter.blocks.store(blocks, Ordering::Relaxed);
        }
    }

    /// The local block count for a core, as last observed.
    pub fn local_blocks(&self, key: &[u8; 32]) -> u64 {
        lock(&self.inner.block_counts)
            .get(key)
            .map(|c| c.blocks())
            .unwrap_or(0)
    }

    /// `hydrateOwned`: registers every core this writer announced that is already on disk.
    pub async fn hydrate_owned(&self) -> Result<(), BlobsError> {
        let channel = &self.inner.channel;
        let primary = channel.primary_key();
        let primary_hex = hex(&primary);
        let writer_hex = hex(&channel.local_writer());
        let mut candidates: Vec<[u8; 32]> = Vec::new();
        let mut seen = HashSet::new();
        for (raw_key, value) in channel
            .entries()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?
        {
            let key = String::from_utf8_lossy(&raw_key).into_owned();
            if let Some(w) = key.strip_prefix("writer/") {
                if w != primary_hex {
                    channel.add_secondary_writer(w);
                }
                continue;
            }
            if key.starts_with("source/") {
                channel.mark_has_entries();
                continue;
            }
            if is_system_view_key(&key) {
                continue;
            }
            channel.mark_has_entries();
            let Some(op) = decode_op(&value) else {
                continue;
            };
            if op.get("type").and_then(Value::as_str) != Some("announce") {
                continue;
            }
            let Some(announce) = announce_from_op(&op) else {
                continue;
            };
            if announce.wrapped_key.is_some() {
                self.resolve_video_key(&announce);
            }
            let Some(blobs) = parse_hex32(&announce.blobs) else {
                continue;
            };
            if key.split('/').nth(1) == Some(&writer_hex)
                && !self.inner.slot_set.contains(&blobs)
                && seen.insert(blobs)
            {
                candidates.push(blobs);
            }
        }
        channel.sync_writer_cores().await;
        for key in candidates {
            if self.is_owned_blob(&key) {
                continue;
            }
            let session = match self.open_key(key, true).await {
                Ok(session) => session,
                Err(_) => continue,
            };
            if session.writable() && session.length() > 0 {
                self.register_owned(
                    key,
                    OwnedMeta {
                        length: session.length(),
                        byte_length: content_bytes(&session),
                    },
                );
            }
            session.close();
        }
        Ok(())
    }

    /// `onCoreWake`: remembers a core without keeping a session alive.
    pub fn on_core_wake(&self, core: &hypercore::Core) {
        if self.inner.host.closing() {
            return;
        }
        let disc = *core.discovery_key();
        self.inner.host.mark_stored(disc);
        // Corestore already keeps cores with peers alive; a session here would prevent GC.
        lock(&self.inner.ledger).absent.remove(&disc);
    }

    /// Starts the store watcher that feeds [`Blobs::on_core_wake`].
    pub fn watch_store(&self) -> tokio::task::JoinHandle<()> {
        let mut opened = self.inner.store.watch();
        let blobs = self.clone();
        tokio::spawn(async move {
            while let Some(core) = opened.recv().await {
                blobs.on_core_wake(&core);
            }
        })
    }

    /// The open session for a core, opening it and keeping the ledger and ceiling honest.
    pub async fn blob_core(&self, key: &[u8; 32]) -> Result<Arc<Session>, BlobsError> {
        let existing = {
            let mut cores = lock(&self.inner.cores);
            cores.map.remove(key).map(|entry| {
                cores.next_seq += 1;
                let seq = cores.next_seq;
                cores.map.insert(
                    *key,
                    CoreEntry {
                        session: entry.session.clone(),
                        seq,
                    },
                );
                entry.session
            })
        };
        let session = match existing {
            Some(session) => session,
            None => {
                let slot = self
                    .inner
                    .slots
                    .iter()
                    .find(|s| &s.key == key)
                    .map(|s| s.session.clone());
                let session = match slot {
                    Some(session) => session,
                    None => Arc::new(self.open_key(*key, false).await?),
                };
                let mut cores = lock(&self.inner.cores);
                cores.next_seq += 1;
                let seq = cores.next_seq;
                cores.map.insert(
                    *key,
                    CoreEntry {
                        session: session.clone(),
                        seq,
                    },
                );
                session
            }
        };
        self.ensure_block_counter(&session).await;
        self.record_cached(
            *key,
            session.length(),
            content_bytes(&session),
            self.local_blocks(key),
        );
        self.touch_cached(Some(*key));
        self.evict_idle_blob_cores(Some(*key));
        self.schedule_ceiling(Some(*key));
        Ok(session)
    }

    fn evict_idle_blob_cores(&self, keep: Option<[u8; 32]>) {
        let cap = if self.inner.worklet {
            MAX_OPEN_CORES_WORKLET
        } else {
            MAX_OPEN_CORES
        };
        let limit = self.inner.slot_set.len() + cap;
        let mut victims = Vec::new();
        {
            let cores = lock(&self.inner.cores);
            if cores.map.len() <= limit {
                return;
            }
            let mut ordered: Vec<(&[u8; 32], &CoreEntry)> = cores.map.iter().collect();
            ordered.sort_by_key(|(_, e)| e.seq);
            for (key, entry) in ordered {
                if cores.map.len() - victims.len() <= limit {
                    break;
                }
                if Some(*key) == keep || self.inner.slot_set.contains(key) {
                    continue;
                }
                if self.is_warming(key) {
                    continue;
                }
                self.record_cached(
                    *key,
                    entry.session.length(),
                    content_bytes(&entry.session),
                    self.local_blocks(key),
                );
                victims.push(*key);
            }
        }
        for key in victims {
            let closed = lock(&self.inner.cores).map.remove(&key);
            if let Some(entry) = closed {
                entry.session.close();
            }
        }
    }

    /// `entryCachedBytes`: how much of one entry is on disk, from the counter or the ledger.
    pub async fn entry_cached_bytes(&self, core: Option<&Arc<Session>>, entry: &Announce) -> u64 {
        let block_offset = entry.blob.block_offset;
        let block_length = entry.blob.block_length;
        if block_length == 0 {
            return 0;
        }
        if let Some(session) = core.filter(|c| !c.is_closed()) {
            if session.length() <= block_offset {
                return 0;
            }
            let counter = self.ensure_block_counter(session).await;
            if counter.blocks() == 0 {
                return 0;
            }
            let len = block_length.min(session.length() - block_offset);
            let whole = block_offset == 0 && len >= session.length();
            let mut blocks = if whole {
                Some(counter.blocks())
            } else {
                lock(&counter.slices).get(&(block_offset, len)).copied()
            };
            if blocks.is_none() {
                let counted = count_local(session, block_offset, block_offset + len);
                counter.remember((block_offset, len), counted);
                blocks = Some(counted);
            }
            let blocks = blocks.unwrap_or(0);
            if blocks == 0 {
                return 0;
            }
            return if blocks >= block_length {
                entry.size
            } else {
                entry.size.min(scaled(blocks, block_length, entry.size))
            };
        }
        let key = match parse_hex32(&entry.blobs) {
            Some(key) => key,
            None => return 0,
        };
        let meta = lock(&self.inner.ledger).cached.get(&key).cloned();
        let Some(meta) = meta else { return 0 };
        if meta.blocks == 0 || meta.length <= block_offset {
            return 0;
        }
        let len = block_length.min(meta.length - block_offset);
        let tag = (block_offset, len);
        let mut blocks = if meta.blocks >= meta.length {
            Some(len)
        } else if block_offset == 0 && len >= meta.length {
            Some(meta.blocks)
        } else {
            meta.slices.get(&tag).copied()
        };
        if blocks.is_none() {
            let temporary = self.open_key(key, true).await.ok();
            let counted = match temporary.as_ref() {
                Some(session) => count_local(session, block_offset, block_offset + len),
                None => 0,
            };
            if let Some(session) = temporary {
                session.close();
            }
            let mut ledger = lock(&self.inner.ledger);
            if let Some(m) = ledger.cached.get_mut(&key) {
                if m.slices.len() >= MAX_SLICES {
                    m.slices.clear();
                }
                m.slices.insert(tag, counted);
            }
            blocks = Some(counted);
        }
        let blocks = blocks.unwrap_or(0);
        if blocks == 0 {
            return 0;
        }
        if blocks >= block_length {
            entry.size
        } else {
            entry.size.min(scaled(blocks, block_length, entry.size))
        }
    }

    /// `gcStorage`: flush, compact and report what the database gave back.
    pub async fn gc_storage(&self, full: bool, streaming: bool) -> StorageGc {
        let _queue = self.inner.gc_queue.lock().await;
        if self.inner.host.closing() {
            return StorageGc {
                before_bytes: 0,
                after_bytes: 0,
                reclaimed_bytes: 0,
            };
        }
        let before = self.inner.host.db_disk_bytes();
        self.inner.host.flush().await;
        if !self.inner.host.closing() {
            let has_local_blobs = !lock(&self.inner.ledger).owned.is_empty()
                || self
                    .inner
                    .slots
                    .iter()
                    .any(|s| self.local_blocks(&s.key) > 0);
            self.inner
                .host
                .compact(Compact {
                    full,
                    streaming,
                    has_local_blobs,
                })
                .await;
        }
        let after = self.inner.host.db_disk_bytes();
        StorageGc {
            before_bytes: before,
            after_bytes: after,
            reclaimed_bytes: before.saturating_sub(after),
        }
    }

    /// Whether any channel op or direct watch still points at this blob.
    pub async fn has_live_blob_reference(
        &self,
        key: &[u8; 32],
        block_offset: Option<u64>,
    ) -> Result<bool, BlobsError> {
        let hex_key = hex(key);
        let matches = |announce: &Announce| {
            announce.blobs == hex_key
                && block_offset.is_none_or(|offset| announce.blob.block_offset == offset)
        };
        let channel = &self.inner.channel;
        channel
            .update()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?;
        for (raw_key, value) in channel
            .entries()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?
        {
            let view_key = String::from_utf8_lossy(&raw_key);
            if is_system_view_key(&view_key) {
                continue;
            }
            let Some(op) = decode_op(&value) else {
                continue;
            };
            let Some(announce) = announce_from_op(&op) else {
                continue;
            };
            if matches(&announce) {
                return Ok(true);
            }
        }
        for announce in self.inner.host.ephemeral_announces().await {
            if matches(&announce) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Drops a per-video core no entry points at any more, or clears a slot's span.
    pub async fn gc_unreferenced_slot_blob(&self, entry: &Announce) -> Result<(), BlobsError> {
        let Some(key) = parse_hex32(&entry.blobs) else {
            return Ok(());
        };
        if entry.blob.block_length == 0 && entry.blob.byte_length != 0 {
            return Ok(());
        }
        if self.is_owned_blob(&key) && !self.inner.slot_set.contains(&key) {
            if self.has_live_blob_reference(&key, None).await? {
                return Ok(());
            }
            self.forget_warm(&key);
            self.inner.swarm.leave(key);
            self.drop_owned(&key);
            let open = lock(&self.inner.cores).map.remove(&key);
            if let Some(entry) = open {
                entry.session.close();
            }
            let victim = self.open_key(key, true).await?;
            self.inner.store.purge(victim.discovery_key()).await?;
            victim.close();
            self.gc_storage(false, false).await;
            return Ok(());
        }
        if !self.inner.slot_set.contains(&key) || entry.blob.block_length == 0 {
            return Ok(());
        }
        if self
            .has_live_blob_reference(&key, Some(entry.blob.block_offset))
            .await?
        {
            return Ok(());
        }
        let slot = self
            .inner
            .slots
            .iter()
            .find(|s| s.key == key)
            .map(|s| s.session.clone());
        let Some(slot) = slot else { return Ok(()) };
        let end = slot
            .length()
            .min(entry.blob.block_offset + entry.blob.block_length);
        if end > entry.blob.block_offset {
            slot.clear(entry.blob.block_offset, end).await?;
            self.gc_storage(false, false).await;
        }
        Ok(())
    }

    /// Drains spent blocks behind the playhead, oldest first, and reclaims the space once.
    pub async fn evict_behind(&self, key: &[u8; 32], blob: BlobId, opts: EvictOptions) -> u64 {
        let Some(session) = self.open_core(key).filter(|s| !s.is_closed()) else {
            return 0;
        };
        if blob.block_length <= 16 || self.is_owned_blob(key) || session.writable() {
            return 0;
        }
        let bs = 65536.max(
            (if blob.byte_length > 0 {
                blob.byte_length
            } else {
                BLOB_BLOCK_SIZE
            })
            .div_ceil(blob.block_length.max(1)),
        );
        let playhead = blob.block_offset + opts.playhead_byte / bs;
        let keep_blocks = 16.max(opts.keep_behind_bytes.div_ceil(bs));
        let head_guard = blob.block_offset + 8.max((4 * 1024 * 1024u64).div_ceil(bs));
        let tail_guard = (blob.block_offset + blob.block_length)
            .saturating_sub(4.max((2 * 1024 * 1024u64).div_ceil(bs)));
        let clear_to = tail_guard
            .min(session.length())
            .min(playhead.saturating_sub(keep_blocks));
        if clear_to <= head_guard {
            return 0;
        }
        let max_blocks = 1.max((32 * 1024 * 1024u64) / bs);
        let hint = clear_to.min(
            head_guard.max(
                lock(&self.inner.evicted_to)
                    .get(key)
                    .copied()
                    .unwrap_or(head_guard),
            ),
        );
        let mut start = if hint > head_guard && count_local(&session, head_guard, hint) > 0 {
            head_guard
        } else {
            hint
        };
        let mut cleared = 0u64;
        while start < clear_to
            && !self.inner.host.closing()
            && opts.max_bytes.is_none_or(|max| cleared * bs < max)
        {
            let end = clear_to.min(start + max_blocks);
            let blocks = count_local(&session, start, end);
            if blocks > 0 && session.clear(start, end).await.is_err() {
                break;
            }
            cleared += blocks;
            start = end;
        }
        lock(&self.inner.evicted_to).insert(*key, start.min(clear_to));
        if cleared == 0 {
            return 0;
        }
        self.sync_block_counter(&session).await;
        self.record_cached(
            *key,
            session.length(),
            content_bytes(&session),
            self.local_blocks(key),
        );
        self.gc_storage(false, true).await.reclaimed_bytes
    }

    fn cache_floor_bytes(&self) -> Option<u64> {
        self.inner
            .host
            .free_bytes()
            .map(|bytes| bytes.saturating_sub(CACHE_DISK_RESERVE))
    }

    /// The one estimate the ceiling and `status()` share.
    pub async fn cached_total_bytes(&self) -> u64 {
        let mut total = 0u64;
        let open: Vec<([u8; 32], Arc<Session>)> = {
            let cores = lock(&self.inner.cores);
            cores
                .map
                .iter()
                .filter(|(_, e)| !e.session.is_closed())
                .map(|(k, e)| (*k, e.session.clone()))
                .collect()
        };
        for (key, session) in open {
            if self.is_owned_blob(&key) || session.length() == 0 || content_bytes(&session) == 0 {
                continue;
            }
            self.ensure_block_counter(&session).await;
            let blocks = self.local_blocks(&key);
            self.record_cached(key, session.length(), content_bytes(&session), blocks);
            if blocks > 0 {
                total += content_bytes(&session).min(scaled(
                    blocks,
                    session.length(),
                    content_bytes(&session),
                ));
            }
        }
        let closed: Vec<([u8; 32], CachedMeta)> = {
            let cores = lock(&self.inner.cores);
            lock(&self.inner.ledger)
                .cached
                .iter()
                .filter(|(key, _)| !cores.map.contains_key(*key))
                .map(|(key, meta)| (*key, meta.clone()))
                .collect()
        };
        for (key, meta) in closed {
            if self.is_owned_blob(&key)
                || meta.blocks == 0
                || meta.length == 0
                || meta.byte_length == 0
            {
                continue;
            }
            total += meta
                .byte_length
                .min(scaled(meta.blocks, meta.length, meta.byte_length));
        }
        total
    }

    /// `clearBlobHex`: closes the session, clears every block and fixes the ledger.
    pub async fn clear_blob_hex(&self, key: &[u8; 32]) -> u64 {
        if self.is_owned_blob(key) {
            return 0;
        }
        let is_slot = self.inner.slot_set.contains(key);
        if !is_slot {
            let open = lock(&self.inner.cores).map.remove(key);
            if let Some(entry) = open {
                entry.session.close();
            }
        }
        let core = if is_slot {
            self.open_core(key)
        } else {
            self.open_key(*key, true).await.ok().map(Arc::new)
        };
        let Some(core) = core else { return 0 };
        let result = self.clear_open_core(key, &core, is_slot).await;
        if !is_slot {
            core.close();
        }
        result
    }

    async fn clear_open_core(&self, key: &[u8; 32], core: &Arc<Session>, is_slot: bool) -> u64 {
        if core.writable() || core.length() == 0 {
            return 0;
        }
        let blocks = if is_slot {
            self.ensure_block_counter(core).await;
            self.sync_block_counter(core).await;
            self.local_blocks(key)
        } else {
            count_local(core, 0, core.length())
        };
        let mut cleared = 0u64;
        if blocks > 0 {
            cleared = content_bytes(core).min(scaled(blocks, core.length(), content_bytes(core)));
            let _ = core.clear(0, core.length()).await;
            if is_slot {
                self.sync_block_counter(core).await;
            }
        }
        if let Some(counter) = lock(&self.inner.block_counts).get(key).cloned() {
            counter.blocks.store(0, Ordering::Relaxed);
            lock(&counter.slices).clear();
        }
        self.record_cached(*key, core.length(), content_bytes(core), 0);
        lock(&self.inner.ledger).last_used.remove(key);
        cleared
    }

    /// Runs one ceiling pass; serialised, so two passes never fight over the same victims.
    pub async fn enforce_cache_ceiling(&self, keep_hex: Option<[u8; 32]>) -> CacheCeiling {
        let _pass = self.inner.ceiling_pass.lock().await;
        let budget = self.cache_max_bytes();
        let mut total = self.cached_total_bytes().await;
        if budget == 0 {
            return CacheCeiling {
                evicted: 0,
                cached_bytes: total,
            };
        }
        let limit = match self.cache_floor_bytes() {
            Some(spare) => budget.min(total.saturating_add(spare)),
            None => budget,
        };
        if total <= limit {
            return CacheCeiling {
                evicted: 0,
                cached_bytes: total,
            };
        }
        let target = limit / 10 * 9;
        let (progress, streaming) = match lock(&self.inner.streams).clone() {
            Some(streams) => (streams.stream_progress(), streams.streaming_keys()),
            None => (HashMap::new(), HashSet::new()),
        };
        let freshest = {
            let ledger = lock(&self.inner.ledger);
            ledger
                .last_used
                .iter()
                .max_by_key(|(_, (at, seq))| (*at, u64::MAX - *seq))
                .map(|(key, _)| *key)
        };
        let now = self.inner.host.now_ms();
        let recent = freshest.is_some_and(|key| {
            let ledger = lock(&self.inner.ledger);
            let at = ledger.last_used.get(&key).map(|(at, _)| *at).unwrap_or(0);
            now.saturating_sub(at) < RECENT_WINDOW_MS
        });
        let playing = keep_hex.or(if recent { freshest } else { None });
        let (open_keys, cached_keys, last_used) = {
            let cores = lock(&self.inner.cores);
            let ledger = lock(&self.inner.ledger);
            (
                cores.map.keys().copied().collect::<Vec<[u8; 32]>>(),
                ledger.cached.keys().copied().collect::<Vec<[u8; 32]>>(),
                ledger.last_used.clone(),
            )
        };
        let mut seen: HashSet<[u8; 32]> = open_keys.iter().copied().collect();
        let mut keys = open_keys;
        for key in cached_keys {
            if seen.insert(key) {
                keys.push(key);
            }
        }
        let mut victims: Vec<([u8; 32], u64, u64)> = keys
            .into_iter()
            .filter(|key| !self.is_owned_blob(key))
            .filter(|key| Some(*key) != playing && !streaming.contains(key))
            .filter(|key| !self.is_warming(key))
            .map(|key| {
                let (at, seq) = last_used.get(&key).copied().unwrap_or((0, 0));
                (key, at, seq)
            })
            .collect();
        victims.sort_by_key(|(_, at, seq)| (*at, *seq));
        let mut evicted = 0u64;
        for (key, _, _) in victims {
            if total <= target || self.inner.host.closing() {
                break;
            }
            let freed = self.clear_blob_hex(&key).await;
            if freed > 0 {
                evicted += freed;
                total = total.saturating_sub(freed);
                if !self.inner.slot_set.contains(&key) {
                    self.inner.swarm.leave(key);
                }
            }
        }
        // One film can be larger than the whole budget: keep a window around its playhead.
        if total > target
            && let Some(playing) = playing
            && !self.is_owned_blob(&playing)
            && let Some(core) = self.open_core(&playing)
            && !core.is_closed()
            && core.length() > 16
        {
            let keep_behind = (64 * 1024 * 1024).min((8 * 1024 * 1024).max(budget / 8));
            let before = total;
            self.evict_behind(
                &playing,
                BlobId {
                    block_offset: 0,
                    block_length: core.length(),
                    byte_offset: 0,
                    byte_length: content_bytes(&core),
                },
                EvictOptions {
                    playhead_byte: progress.get(&playing).copied().unwrap_or(0),
                    keep_behind_bytes: keep_behind,
                    max_bytes: Some(total.saturating_sub(target)),
                },
            )
            .await;
            total = self.cached_total_bytes().await;
            evicted += before.saturating_sub(total);
        }
        if evicted > 0 {
            {
                let mut ceilings = lock(&self.inner.ceilings);
                ceilings.timer = false;
            }
            self.gc_storage(false, false).await;
            self.inner.host.update();
        }
        CacheCeiling {
            evicted,
            cached_bytes: total,
        }
    }

    /// Debounces a ceiling pass by two seconds, as the JS does.
    pub fn schedule_ceiling(&self, keep_hex: Option<[u8; 32]>) {
        if self.cache_max_bytes() == 0 || self.inner.host.closing() {
            return;
        }
        {
            let mut ceilings = lock(&self.inner.ceilings);
            if ceilings.timer {
                return;
            }
            ceilings.timer = true;
        }
        let blobs = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(CEILING_DELAY).await;
            {
                let mut ceilings = lock(&blobs.inner.ceilings);
                if !ceilings.timer {
                    return;
                }
                ceilings.timer = false;
            }
            blobs.enforce_cache_ceiling(keep_hex).await;
        });
    }

    pub fn cache_max_bytes(&self) -> u64 {
        lock(&self.inner.ceilings).max_bytes
    }

    /// Sets the contribution and, once, runs a boot pass after the catalog walk.
    pub fn set_cache_max_bytes(&self, bytes: f64) -> u64 {
        let next = if bytes.is_finite() && bytes > 0.0 {
            bytes.floor() as u64
        } else {
            0
        };
        {
            let mut ceilings = lock(&self.inner.ceilings);
            ceilings.max_bytes = next;
            ceilings.growth = 0;
        }
        if next > 0 {
            let blobs = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(CEILING_BOOT_DELAY).await;
                blobs.inner.host.refresh_catalog().await;
                blobs.schedule_ceiling(None);
            });
        }
        next
    }

    /// `clearCache`: one entry, or everything cached but `keep_blobs_hex`.
    pub async fn clear_cache(
        &self,
        key: Option<&str>,
        keep_blobs_hex: Option<[u8; 32]>,
    ) -> Result<CacheCleared, BlobsError> {
        if let Some(key) = key {
            let Some(entry) = self.inner.host.find_announce(key).await else {
                return Ok(CacheCleared {
                    cleared_bytes: 0,
                    reclaimed_bytes: 0,
                });
            };
            let Some(blobs) = parse_hex32(&entry.blobs) else {
                return Ok(CacheCleared {
                    cleared_bytes: 0,
                    reclaimed_bytes: 0,
                });
            };
            if self.is_owned_blob(&blobs) {
                return Ok(CacheCleared {
                    cleared_bytes: 0,
                    reclaimed_bytes: 0,
                });
            }
            if let Some(streams) = lock(&self.inner.streams).clone() {
                streams.abort_streams(Some(blobs), None);
            }
            self.forget_warm(&blobs);
            let is_slot = self.inner.slot_set.contains(&blobs);
            if !is_slot {
                let open = lock(&self.inner.cores).map.remove(&blobs);
                if let Some(open) = open {
                    open.session.close();
                }
            }
            let core = if is_slot {
                self.open_core(&blobs)
            } else {
                self.open_key(blobs, true).await.ok().map(Arc::new)
            };
            let Some(core) = core else {
                return Ok(CacheCleared {
                    cleared_bytes: 0,
                    reclaimed_bytes: 0,
                });
            };
            let mut cleared = 0u64;
            let mut reclaimed = 0u64;
            if !core.is_closed() {
                cleared = self.entry_cached_bytes(Some(&core), &entry).await;
                if cleared > 0
                    && entry.blob.block_length > 0
                    && core.length() > entry.blob.block_offset
                {
                    core.clear(
                        entry.blob.block_offset,
                        core.length()
                            .min(entry.blob.block_offset + entry.blob.block_length),
                    )
                    .await?;
                    if is_slot {
                        self.sync_block_counter(&core).await;
                    }
                    let remaining = if core.length() > 0 {
                        count_local(&core, 0, core.length())
                    } else {
                        0
                    };
                    self.record_cached(blobs, core.length(), content_bytes(&core), remaining);
                    reclaimed = self.gc_storage(false, false).await.reclaimed_bytes;
                    self.inner.host.update();
                }
            }
            if !is_slot {
                core.close();
            }
            return Ok(CacheCleared {
                cleared_bytes: cleared,
                reclaimed_bytes: reclaimed,
            });
        }
        if let Some(streams) = lock(&self.inner.streams).clone() {
            streams.abort_streams(None, keep_blobs_hex);
        }
        {
            let mut warm = lock(&self.inner.warm);
            let prefix = keep_blobs_hex.map(|k| format!("{}:", hex(&k)));
            warm.ranges.retain(|(tag, _)| match &prefix {
                Some(prefix) => tag.starts_with(prefix),
                None => false,
            });
        }
        self.inner.host.refresh_catalog().await;
        let mut cleared = 0u64;
        let all: Vec<[u8; 32]> = {
            let cores = lock(&self.inner.cores);
            let ledger = lock(&self.inner.ledger);
            let mut keys: Vec<[u8; 32]> = cores.map.keys().copied().collect();
            for key in ledger.cached.keys() {
                if !cores.map.contains_key(key) {
                    keys.push(*key);
                }
            }
            keys
        };
        for blobs in all {
            if Some(blobs) == keep_blobs_hex {
                continue;
            }
            cleared += self.clear_blob_hex(&blobs).await;
        }
        let reclaimed = if cleared > 0 {
            self.gc_storage(true, false).await.reclaimed_bytes
        } else {
            0
        };
        self.inner.host.update();
        Ok(CacheCleared {
            cleared_bytes: cleared,
            reclaimed_bytes: reclaimed,
        })
    }

    /// Mints a dedicated 512 KiB-piece core per video and returns the announce op.
    pub async fn put<P: PutSource>(
        &self,
        meta: PutMeta,
        mut source: P,
    ) -> Result<Announce, BlobsError> {
        if !crate::protocol::is_id(&meta.id) {
            return Err(BlobsError::InvalidId(meta.id.clone()));
        }
        let private = meta.private_entry.unwrap_or(false) || meta.epoch.is_some_and(|e| e > 0);
        let mut video_key: Option<[u8; 32]> = None;
        let mut epoch = 0u64;
        let mut wrapped_key: Option<String> = None;
        if private {
            if !self.inner.host.has_vault() {
                return Err(BlobsError::VaultRequired);
            }
            epoch = match meta.epoch {
                Some(e) if e > 0 => e,
                _ => self.inner.channel.current_epoch().await,
            };
            let resumed = meta
                .extra
                .get("storeKey")
                .and_then(Value::as_str)
                .and_then(parse_hex32);
            video_key = Some(resumed.unwrap_or_else(rand::random));
            let source_hex = hex(&self.inner.channel.primary_key());
            let epoch_key = self
                .inner
                .host
                .epoch_key(&source_hex, epoch)
                .ok_or(BlobsError::VaultRequired)?;
            wrapped_key = Some(wrap_video_key(&video_key.unwrap_or_default(), &epoch_key));
        }
        let store_name = meta.store.clone().unwrap_or_else(|| {
            let random: [u8; 16] = rand::random();
            format!("video:{}", hex(&random))
        });
        let named = meta.store.is_some();
        let mut opts = StoreGetOptions::name(store_name.into_bytes());
        opts.inactive = true;
        opts.encryption_key = video_key;
        let session = self.inner.store.get(opts).await?;
        let started = {
            let start = if named { content_bytes(&session) } else { 0 };
            self.ingest(&session, &meta, &mut source, start).await
        };
        match started {
            Ok(announce) => {
                let key = *session.key();
                self.register_owned(
                    key,
                    OwnedMeta {
                        length: session.length(),
                        byte_length: content_bytes(&session),
                    },
                );
                if let Some(video_key) = video_key {
                    let source = self.inner.channel.primary_key();
                    let mut ledger = lock(&self.inner.ledger);
                    ledger.encrypted.insert(
                        key,
                        EncryptedMeta {
                            source,
                            epoch,
                            wrapped_key: wrapped_key.clone().unwrap_or_default(),
                        },
                    );
                    ledger.unlocked.insert(key, video_key);
                }
                session.close();
                self.inner.swarm.join(key);
                let mut announce = announce;
                if video_key.is_some() {
                    announce.epoch = Some(epoch);
                    announce.wrapped_key = wrapped_key;
                }
                Ok(announce)
            }
            Err(err) => {
                if named {
                    session.close();
                    return Err(err);
                }
                self.inner.store.purge(session.discovery_key()).await?;
                session.close();
                Err(err)
            }
        }
    }

    async fn ingest<P: PutSource>(
        &self,
        session: &Session,
        meta: &PutMeta,
        source: &mut P,
        start: u64,
    ) -> Result<Announce, BlobsError> {
        let id = meta.id.clone();
        // Appends are durable, so a named core holds every block an earlier attempt wrote; the
        // hasher re-reads that prefix instead of trusting an external SHA-256 state blob.
        let resumed = start > 0;
        let mut expected = meta.expected_size.flatten();
        let complete = resumed && expected == Some(start);
        let core = session.core().clone();
        let blobs = Hyperblobs::new(core.clone(), BLOB_BLOCK_SIZE as usize);
        let mut sha = Sha256::new();
        let mut size = start;
        let mut hashed = 0u64;
        if start > 0 {
            hashed = hash_committed(session, &mut sha, start).await?;
        }
        if !complete {
            let mut source_meta = meta.clone();
            let mut reader = source
                .open(start, &mut source_meta)
                .await
                .map_err(|e| BlobsError::Source(e.to_string()))?;
            // A resumed job may learn a better size from the source before the tail arrives.
            expected = source_meta.expected_size.flatten();
            let mut writer = blobs.writer().await;
            let mut pending = BytesMut::with_capacity(BLOB_BLOCK_SIZE as usize + 8192);
            let mut chunk = vec![0u8; 256 * 1024];
            let block = BLOB_BLOCK_SIZE as usize;
            loop {
                let read = reader.read(&mut chunk).await.map_err(BlobsError::Io)?;
                if read == 0 {
                    break;
                }
                sha.update(&chunk[..read]);
                hashed += read as u64;
                size += read as u64;
                source.on_data(size);
                pending.extend_from_slice(&chunk[..read]);
                while pending.len() >= block {
                    writer.write(pending.split_to(block).freeze()).await?;
                }
            }
            if !pending.is_empty() {
                writer.write(pending.split().freeze()).await?;
            }
            writer.finish().await?;
        }
        if let Some(expected) = expected
            && size != expected
        {
            return Err(BlobsError::Truncated {
                id,
                expected,
                got: size,
            });
        }
        if hashed != size {
            return Err(BlobsError::HashOutOfStep { id, hashed, size });
        }
        let sha256 = hex(&sha.finalize());
        let key = *session.key();
        Ok(Announce {
            kind: "announce".into(),
            id: meta.id.clone(),
            title: clean_text(Some(&meta.title), &meta.id),
            size,
            sha256,
            blobs: hex(&key),
            blob: Blob {
                block_offset: 0,
                block_length: session.length(),
                byte_offset: 0,
                byte_length: content_bytes(session),
                extra: Map::new(),
            },
            epoch: None,
            wrapped_key: None,
            extra: Map::new(),
        })
    }

    /// `dropStore`: purges a half-written core when its job is cancelled or dead-lettered.
    pub async fn drop_store(&self, name: &str) -> Result<(), BlobsError> {
        if name.is_empty() {
            return Ok(());
        }
        let mut opts = StoreGetOptions::name(name.as_bytes());
        opts.inactive = true;
        let session = self.inner.store.get(opts).await?;
        self.inner.store.purge(session.discovery_key()).await?;
        session.close();
        Ok(())
    }

    fn published(&self, op: Announce) -> Published {
        let writer_hex = hex(&self.inner.channel.local_writer());
        Published {
            key: format!(
                "{}/{}/{}:{}",
                op.id, writer_hex, op.blobs, op.blob.block_offset
            ),
            channel: hex(&self.inner.channel.primary_key()),
            stream_url: self.stream_url(&op),
            magnet_url: format_watch_uri(&watch_entry(&op), "", ""),
            announce: op,
        }
    }

    /// `publish`: ingest, append the announce and return the app-facing record.
    pub async fn publish<P: PutSource>(
        &self,
        meta: PutMeta,
        source: P,
    ) -> Result<Published, BlobsError> {
        let op = self.put(meta, source).await?;
        self.append_op(&op).await?;
        Ok(self.published(op))
    }

    /// `publishChannel`: the same, from a source or an already-built announce op.
    pub async fn publish_channel<P: PutSource>(
        &self,
        meta_or_op: Value,
        source: Option<P>,
    ) -> Result<Published, BlobsError> {
        let op = match source {
            Some(source) => {
                let meta: PutMeta = serde_json::from_value(meta_or_op)?;
                self.put(meta, source).await?
            }
            None => {
                let text = serde_json::to_string(&meta_or_op)?;
                decode_op(text)
                    .and_then(|op| announce_from_op(&op))
                    .ok_or(BlobsError::InvalidAnnounce)?
            }
        };
        self.append_op(&op).await?;
        self.inner
            .swarm
            .join(parse_hex32(&op.blobs).unwrap_or_default());
        Ok(self.published(op))
    }

    async fn append_op(&self, op: &Announce) -> Result<(), BlobsError> {
        let value = serde_json::to_value(op)?;
        self.inner
            .channel
            .append(&value)
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))
    }

    /// `streamUrl`: the Range link for an entry's blob.
    pub fn stream_url(&self, entry: &Announce) -> String {
        let streams = lock(&self.inner.streams).clone();
        let Some(streams) = streams else {
            return String::new();
        };
        let key = parse_hex32(&entry.blobs).unwrap_or_default();
        streams.link(&key, &blob_id(&entry.blob), "application/octet-stream")
    }

    /// `syncLocalChannel`: backfills a legacy single-writer Hyperbee once.
    pub async fn sync_local_channel(&self) -> Result<SyncResult, BlobsError> {
        let legacy_store = self.inner.store.namespace("channel");
        let probe = legacy_store.get(StoreGetOptions::name("bee")).await?;
        let legacy_len = probe.length();
        probe.close();
        self.inner
            .channel
            .update()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?;
        let mut existing = HashSet::new();
        let mut total = 0u64;
        for (raw_key, value) in self
            .inner
            .channel
            .entries()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?
        {
            if is_system_view_key(&String::from_utf8_lossy(&raw_key)) {
                continue;
            }
            let Some(op) = decode_op(&value) else {
                continue;
            };
            if let Some(announce) = announce_from_op(&op) {
                existing.insert(format!(
                    "{}/{}:{}",
                    announce.id, announce.blobs, announce.blob.block_offset
                ));
                total += 1;
            }
        }
        if legacy_len == 0 {
            return Ok(SyncResult {
                backfilled: 0,
                total,
            });
        }
        let bee = Bee::open(&legacy_store, None).await?;
        let mut backfilled = 0u64;
        let mut range = bee.range(RangeOptions {
            gte: Some(Bytes::from_static(b"entry/")),
            lt: Some(Bytes::from_static(b"entry/\xff")),
            ..Default::default()
        });
        loop {
            let entry = match range.next().await {
                Ok(Some(entry)) => entry,
                Ok(None) => break,
                Err(e) => {
                    bee.close().await;
                    return Err(BlobsError::Bee(e));
                }
            };
            let Some(op) = decode_op(&entry.value) else {
                continue;
            };
            let Some(announce) = announce_from_op(&op) else {
                continue;
            };
            let slice = format!(
                "{}/{}:{}",
                announce.id, announce.blobs, announce.blob.block_offset
            );
            if existing.contains(&slice) {
                continue;
            }
            self.append_op(&announce).await?;
            self.inner
                .swarm
                .join(parse_hex32(&announce.blobs).unwrap_or_default());
            existing.insert(slice);
            backfilled += 1;
        }
        bee.close().await;
        if backfilled > 0 {
            self.hydrate_owned().await?;
            self.inner.host.update();
        }
        Ok(SyncResult {
            backfilled,
            total: total + backfilled,
        })
    }

    /// Splits legacy variable-block and multi-entry slot spans into per-video cores.
    pub async fn rechunk_legacy_blobs(
        &self,
        mut opts: RechunkOptions,
    ) -> Result<RechunkResult, BlobsError> {
        let writer_hex = hex(&self.inner.channel.local_writer());
        let role = self.inner.channel.local_role().await;
        let owner = role == "owner";
        let mut uniform_by_hash: HashMap<String, ([u8; 32], BlobId)> = HashMap::new();
        let mut groups: Vec<LegacyGroup> = Vec::new();
        let mut group_at: HashMap<String, usize> = HashMap::new();
        self.inner
            .channel
            .update()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?;
        let entries = self
            .inner
            .channel
            .entries()
            .await
            .map_err(|e| BlobsError::Channel(e.to_string()))?;
        for (raw_key, value) in entries {
            let key = String::from_utf8_lossy(&raw_key).into_owned();
            if is_system_view_key(&key) {
                continue;
            }
            let Some(op) = decode_op(&value) else {
                continue;
            };
            let Some(announce) = announce_from_op(&op) else {
                continue;
            };
            if key.split('/').nth(1) != Some(&writer_hex) && !owner {
                continue;
            }
            let Some(blobs) = parse_hex32(&announce.blobs) else {
                continue;
            };
            let Some(slot) = self
                .inner
                .slots
                .iter()
                .find(|s| s.key == blobs)
                .map(|s| s.session.clone())
            else {
                if self.is_owned_blob(&blobs)
                    && announce.blob.block_offset == 0
                    && is_uniform_512(Some(&BlobSize {
                        byte_length: announce.blob.byte_length,
                        block_length: announce.blob.block_length,
                    }))
                {
                    uniform_by_hash.insert(
                        format!("{}:{}", announce.sha256, announce.size),
                        (blobs, blob_id(&announce.blob)),
                    );
                }
                continue;
            };
            if !slot_has_blocks(&slot, &announce).await {
                continue;
            }
            let old_tag = format!("{}:{}", announce.blobs, announce.blob.block_offset);
            let index = match group_at.get(&old_tag) {
                Some(index) => *index,
                None => {
                    group_at.insert(old_tag.clone(), groups.len());
                    groups.push(LegacyGroup {
                        old_tag,
                        blobs,
                        blob: blob_id(&announce.blob),
                        sha256: announce.sha256.clone(),
                        size: announce.size,
                        ops: Vec::new(),
                    });
                    groups.len() - 1
                }
            };
            groups[index].ops.push((key, announce));
        }
        let queue: Vec<usize> = {
            let mut indexes: Vec<usize> = (0..groups.len()).collect();
            indexes.sort_by_key(|i| groups[*i].size);
            indexes
                .into_iter()
                .filter(|i| match &opts.id {
                    Some(id) => groups[*i].ops.iter().any(|(_, op)| &op.id == id),
                    None => true,
                })
                .collect()
        };
        let total = queue.len() as u64;
        let mut migrated = 0u64;
        let mut deduped = 0u64;
        let mut migrated_bytes = 0u64;
        let report = |opts: &mut RechunkOptions,
                      migrated: u64,
                      deduped: u64,
                      migrated_bytes: u64,
                      current: Option<(&str, &str)>| {
            if let Some(callback) = opts.on_progress.as_mut() {
                callback(&MigrationProgress {
                    total,
                    migrated,
                    deduped,
                    remaining: total.saturating_sub(migrated),
                    migrated_bytes,
                    current_id: current.map(|(id, _)| id.to_owned()),
                    current_title: current.map(|(_, title)| title.to_owned()),
                });
            }
        };
        report(&mut opts, 0, 0, 0, None);
        for index in queue {
            if self.inner.host.closing() || opts.limit.is_some_and(|limit| migrated >= limit) {
                break;
            }
            let group = &groups[index];
            let sample = group.ops.first().map(|(_, op)| op.clone());
            if let Some(sample) = sample.as_ref() {
                let title = clean_text(Some(&sample.title), &sample.id);
                report(
                    &mut opts,
                    migrated,
                    deduped,
                    migrated_bytes,
                    Some((sample.id.as_str(), title.as_str())),
                );
            }
            let Some(sample) = sample else { continue };
            let Some(slot) = self
                .inner
                .slots
                .iter()
                .find(|s| s.key == group.blobs)
                .map(|s| s.session.clone())
            else {
                continue;
            };
            if !slot_has_blocks(&slot, &sample).await {
                continue;
            }
            let hash_key = format!("{}:{}", group.sha256, group.size);
            let mut target = uniform_by_hash
                .get(&hash_key)
                .copied()
                .filter(|(blobs, _)| self.is_owned_blob(blobs));
            if target.is_some() {
                deduped += 1;
            } else {
                let meta = PutMeta {
                    id: sample.id.clone(),
                    title: sample.title.clone(),
                    private_entry: None,
                    epoch: None,
                    store: None,
                    expected_size: None,
                    extra: Map::new(),
                };
                let source =
                    SlotSource::new(self.clone(), slot.clone(), group.blob, opts.foreground);
                let new_op = self.put(meta, source).await?;
                if new_op.sha256 != group.sha256 || new_op.size != group.size {
                    let blobs = parse_hex32(&new_op.blobs).unwrap_or_default();
                    self.drop_owned(&blobs);
                    self.inner.swarm.leave(blobs);
                    let victim = self.open_key(blobs, true).await.ok();
                    if let Some(victim) = victim {
                        self.inner.store.purge(victim.discovery_key()).await?;
                        victim.close();
                    }
                    return Err(BlobsError::Sha256Mismatch {
                        id: sample.id.clone(),
                        expected: group.sha256.clone(),
                        got: new_op.sha256,
                    });
                }
                let made = (
                    parse_hex32(&new_op.blobs).unwrap_or_default(),
                    blob_id(&new_op.blob),
                );
                uniform_by_hash.insert(hash_key, made);
                target = Some(made);
            }
            let Some(target) = target else { continue };
            let target_announce = Announce {
                blob: api_blob(&target.1),
                blobs: hex(&target.0),
                ..sample.clone()
            };
            if let Some(callback) = opts.on_migrate.as_mut() {
                callback(&group.old_tag, &target_announce, &sample);
            }
            for (_, op) in &group.ops {
                let moved = Announce {
                    blobs: hex(&target.0),
                    blob: api_blob(&target.1),
                    ..op.clone()
                };
                self.append_op(&moved).await?;
            }
            for (full_key, _) in &group.ops {
                let remove = json!({ "type": "remove", "key": full_key });
                self.inner
                    .channel
                    .append(&remove)
                    .await
                    .map_err(|e| BlobsError::Channel(e.to_string()))?;
            }
            let clear_end = slot
                .length()
                .min(group.blob.block_offset + group.blob.block_length);
            if clear_end > group.blob.block_offset {
                slot.clear(group.blob.block_offset, clear_end).await?;
                self.sync_block_counter(&slot).await;
            }
            migrated += 1;
            migrated_bytes += group.size;
            report(&mut opts, migrated, deduped, migrated_bytes, None);
        }
        if migrated > 0 {
            self.inner.host.update();
        }
        Ok(RechunkResult {
            total,
            migrated,
            deduped,
            remaining: total.saturating_sub(migrated),
            migrated_bytes,
        })
    }

    /// `warm`: joins the swarm and prefetches the head and the moov/cues tail.
    pub async fn warm(&self, entry: &Announce) -> Result<(), BlobsError> {
        let Some(key) = parse_hex32(&entry.blobs) else {
            return Ok(());
        };
        if entry.blob.block_length == 0 && entry.blob.byte_length == 0 {
            return Ok(());
        }
        self.inner.swarm.join(key);
        let core = self.blob_core(&key).await?;
        let tag = format!("{}:{}", entry.blobs, entry.blob.block_offset);
        {
            let mut warm = lock(&self.inner.warm);
            if warm.ranges.iter().any(|(t, _)| t == &tag) {
                return Ok(());
            }
            if warm.ranges.len() >= MAX_WARM_RANGES {
                let (_, dropped) = warm.ranges.remove(0);
                drop(warm);
                destroy_bags(&dropped);
                warm = lock(&self.inner.warm);
            }
            warm.ranges.push((tag.clone(), Vec::new()));
        }
        self.spawn_warm_ranges(&core, entry, tag);
        Ok(())
    }

    fn spawn_warm_ranges(&self, core: &Session, entry: &Announce, tag: String) {
        let block_offset = entry.blob.block_offset;
        let block_length = entry.blob.block_length;
        let byte_length = entry.blob.byte_length;
        let avg = 4096.max(
            (if byte_length > 0 {
                byte_length
            } else {
                BLOB_BLOCK_SIZE
            })
            .div_ceil(block_length.max(1)),
        );
        let scale = if avg <= 16384 {
            3
        } else if avg <= 131072 {
            2
        } else {
            1
        };
        let end_block = block_offset + block_length;
        let head_end = end_block.min(block_offset + 8 * scale);
        let mut ranges = vec![(block_offset, head_end)];
        if end_block > head_end {
            ranges.push((head_end.max(end_block - 2 * scale), end_block));
        }
        let mut bags = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            let bag = RequestBag::new();
            let download = core.download_with_bag(start, Some(end), true, bag.clone());
            bags.push(bag);
            tokio::spawn(async move {
                let _ = download.await;
            });
        }
        let mut warm = lock(&self.inner.warm);
        if let Some((_, entry)) = warm.ranges.iter_mut().find(|(t, _)| t == &tag) {
            *entry = bags;
        }
    }

    /// Drops every warm range (playback started: the player must not wait behind read-ahead).
    pub fn set_player_active(&self, active: bool) {
        self.inner.player_active.store(active, Ordering::Release);
        if active {
            let ranges = std::mem::take(&mut lock(&self.inner.warm).ranges);
            for (_, bags) in ranges {
                destroy_bags(&bags);
            }
        }
    }

    pub fn mark_bulk_peer(&self, key: &str, on: bool) {
        let mut peers = lock(&self.inner.bulk_peers);
        if on {
            peers.insert(key.to_owned());
        } else {
            peers.remove(key);
        }
    }

    /// Installs the view of live HTTP streams the cache ceiling reads. `start_server` does this
    /// itself; a host whose Range server lives elsewhere (or a scripted test) calls it directly.
    pub fn install_streams(&self, streams: Arc<dyn BlobStreams>) {
        *lock(&self.inner.streams) = Some(streams);
    }

    /// `startServer`: the Range server whose stream progress drives the ceiling.
    pub async fn start_server(
        &self,
        addr: SocketAddr,
        window_bytes: u64,
    ) -> Result<SocketAddr, BlobsError> {
        let weak: Weak<Inner<C, S, H>> = Arc::downgrade(&self.inner);
        let resolve: Resolve = Arc::new(move |key: [u8; 32]| {
            let weak = weak.clone();
            Box::pin(async move {
                let inner = weak.upgrade()?;
                Blobs::from_inner(inner).resolve_http(key).await
            }) as ResolveFuture
        });
        let server = BlobServer::bind(
            addr,
            ServerOptions {
                token: None,
                window_bytes,
            },
            resolve,
        )
        .await
        .map_err(BlobsError::Io)?;
        let local = server.local_addr();
        let server = Arc::new(server);
        *lock(&self.inner.streams) =
            Some(Arc::new(ServerStreams(server.clone())) as Arc<dyn BlobStreams>);
        *self.inner.server_stop.lock().await = Some(server);
        Ok(local)
    }

    /// Stops the Range server, if this store started one.
    pub async fn stop_server(&self) {
        lock(&self.inner.streams).take();
        let server = self.inner.server_stop.lock().await.take();
        if let Some(server) = server {
            match Arc::try_unwrap(server) {
                Ok(server) => server.close().await,
                Err(server) => server.abort_streams(None, None),
            }
        }
    }

    async fn resolve_http(&self, key: [u8; 32]) -> Option<Resolved> {
        let encrypted = lock(&self.inner.ledger).encrypted.contains_key(&key);
        let encryption_key = self.resolve_blob_encryption_key(&key);
        if encrypted && encryption_key.is_none() {
            return None;
        }
        self.inner.swarm.join(key);
        self.inner
            .last_http_view_at
            .store(self.inner.host.now_ms(), Ordering::Relaxed);
        let owned = self.is_owned_blob(&key);
        if !owned {
            // `blob_core` keeps the counter, ledger and ceiling side effects; the response gets
            // its own session so the request can close it independently.
            self.blob_core(&key).await.ok()?;
            let session = self.open_key(key, false).await.ok()?;
            return Some(Resolved { session });
        }
        // The HTTP session lingers for 30 s so a seeking player does not reopen the core per range.
        if !self.inner.host.closing() && self.owned(&key).is_some() {
            self.http_session(&key).await;
        }
        let session = self.open_key(key, true).await.ok()?;
        Some(Resolved { session })
    }

    async fn http_session(&self, key: &[u8; 32]) -> Option<Arc<Session>> {
        let generation = self.inner.http_generation.fetch_add(1, Ordering::AcqRel) + 1;
        if lock(&self.inner.http_owned).len() >= MAX_HTTP_OWNED {
            let oldest = {
                let owned = lock(&self.inner.http_owned);
                let mut keys: Vec<[u8; 32]> = owned.keys().copied().collect();
                keys.sort();
                keys.first().copied()
            };
            if let Some(oldest) = oldest
                && let Some(rec) = lock(&self.inner.http_owned).remove(&oldest)
            {
                rec.session.close();
            }
        }
        let session = Arc::new(self.open_key(*key, true).await.ok()?);
        lock(&self.inner.http_owned).insert(
            *key,
            HttpOwned {
                session: session.clone(),
                generation,
            },
        );
        let inner = self.inner.clone();
        let key = *key;
        tokio::spawn(async move {
            tokio::time::sleep(HTTP_OWNED_TTL).await;
            let rec = {
                let mut owned = lock(&inner.http_owned);
                if owned.get(&key).is_some_and(|r| r.generation == generation) {
                    owned.remove(&key)
                } else {
                    None
                }
            };
            if let Some(rec) = rec {
                rec.session.close();
            }
        });
        Some(session)
    }
}

/// A legacy slot span with every op that points at it.
struct LegacyGroup {
    old_tag: String,
    blobs: [u8; 32],
    blob: BlobId,
    sha256: String,
    size: u64,
    ops: Vec<(String, Announce)>,
}

/// Reads a legacy slot span as the `PutSource` of a migration.
struct SlotSource<C: ChannelView, S: ContentSwarm, H: BlobHost> {
    blobs: Blobs<C, S, H>,
    slot: Arc<Session>,
    blob: BlobId,
    foreground: bool,
}

impl<C, S, H> SlotSource<C, S, H>
where
    C: ChannelView + 'static,
    S: ContentSwarm + 'static,
    H: BlobHost + 'static,
{
    fn new(blobs: Blobs<C, S, H>, slot: Arc<Session>, blob: BlobId, foreground: bool) -> Self {
        Self {
            blobs,
            slot,
            blob,
            foreground,
        }
    }
}

impl<C, S, H> PutSource for SlotSource<C, S, H>
where
    C: ChannelView + 'static,
    S: ContentSwarm + 'static,
    H: BlobHost + 'static,
{
    type Error = BlobsError;

    async fn open(
        &mut self,
        _offset: u64,
        _meta: &mut PutMeta,
    ) -> Result<std::pin::Pin<Box<dyn AsyncRead + Send>>, Self::Error> {
        let (reader, mut writer) = tokio::io::duplex(1024 * 1024);
        let slot = self.slot.clone();
        let blob = self.blob;
        let foreground = self.foreground;
        let inner = self.blobs.inner.clone();
        tokio::spawn(async move {
            let start = blob.block_offset;
            let end = blob.block_offset + blob.block_length;
            let mut index = start;
            'outer: while index < end {
                if inner.host.closing() {
                    break;
                }
                if !foreground
                    && (inner.swarm.down_bps() > 65536.0 || inner.swarm.up_bps() > 65536.0)
                {
                    tokio::time::sleep(Duration::from_millis(1500)).await;
                    continue;
                }
                let batch_end = end.min(index + 64);
                for i in index..batch_end {
                    let block = slot
                        .get(
                            i,
                            GetOptions {
                                wait: false,
                                ..Default::default()
                            },
                        )
                        .await
                        .ok()
                        .flatten();
                    let Some(block) = block else { break 'outer };
                    if writer.write_all(&block).await.is_err() {
                        break 'outer;
                    }
                }
                index = batch_end;
            }
            let _ = writer.shutdown().await;
        });
        Ok(Box::pin(reader))
    }

    fn on_data(&mut self, _bytes: u64) {}

    fn on_hash_state(&mut self, _state: &[u8], _bytes: u64) {}
}

fn destroy_bags(bags: &[RequestBag]) {
    for bag in bags {
        bag.destroy("warm range dropped");
    }
}

fn write_private(path: &std::path::Path, bytes: &[u8]) {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
        {
            let _ = file.write_all(bytes);
        }
    }
    #[cfg(not(unix))]
    {
        let _ = std::fs::write(path, bytes);
    }
}

/// Bytes of content in a core. hypercore-rs counts an encrypted core's per-block padding in
/// `byte_length`, where the JS reports the plaintext length the app links use.
fn content_bytes(session: &Session) -> u64 {
    session
        .byte_length()
        .saturating_sub(session.padding() as u64 * session.length())
}

/// Local blocks in `[start, end)`; hypercore-rs has no range count.
fn count_local(session: &Session, start: u64, end: u64) -> u64 {
    let end = end.min(session.length());
    let mut count = 0;
    for index in start..end {
        if session.has(index) {
            count += 1;
        }
    }
    count
}

async fn slot_has_blocks(slot: &Session, entry: &Announce) -> bool {
    if entry.blob.block_length == 0 {
        return entry.blob.byte_length == 0;
    }
    let end = entry.blob.block_offset + entry.blob.block_length;
    if slot.length() < end {
        return false;
    }
    (entry.blob.block_offset..end).all(|index| slot.has(index))
}

async fn hash_committed(
    session: &Session,
    sha: &mut Sha256,
    bytes: u64,
) -> Result<u64, BlobsError> {
    let mut remaining = bytes;
    let mut index = 0u64;
    while remaining > 0 {
        let block = session
            .get(
                index,
                GetOptions {
                    wait: false,
                    ..Default::default()
                },
            )
            .await?
            .ok_or(BlobsError::MissingLocalBlock)?;
        let take = remaining.min(block.len() as u64) as usize;
        sha.update(&block[..take]);
        remaining -= take as u64;
        index += 1;
    }
    Ok(bytes)
}

fn announce_from_op(op: &Value) -> Option<Announce> {
    if op.get("type").and_then(Value::as_str) != Some("announce") {
        return None;
    }
    serde_json::from_value(op.clone()).ok()
}

fn blob_id(blob: &Blob) -> BlobId {
    BlobId {
        block_offset: blob.block_offset,
        block_length: blob.block_length,
        byte_offset: blob.byte_offset,
        byte_length: blob.byte_length,
    }
}

fn api_blob(blob: &BlobId) -> Blob {
    Blob {
        block_offset: blob.block_offset,
        block_length: blob.block_length,
        byte_offset: blob.byte_offset,
        byte_length: blob.byte_length,
        extra: Map::new(),
    }
}

fn watch_entry(entry: &Announce) -> crate::protocol::WatchEntry {
    crate::protocol::WatchEntry {
        id: entry.id.clone(),
        title: entry.title.clone(),
        sha256: Some(entry.sha256.clone()),
        blobs: entry.blobs.clone(),
        blob: Some(crate::protocol::BlobPointer {
            block_offset: entry.blob.block_offset,
            block_length: entry.blob.block_length,
            byte_offset: entry.blob.byte_offset,
            byte_length: entry.blob.byte_length,
        }),
    }
}
