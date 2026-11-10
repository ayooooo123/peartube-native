//! `src/index.js`: `createNode` — the node the app, the API and the cast controller drive.
//!
//! One `CoreNode` owns the Corestore, the channel, the swarm, the blob store, the followed-channel
//! pool and the catalog, in the reference's opening order, and closes them in the reference's
//! closing order.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};

use autobee::Autobee;
use corestore::Corestore;
use hypercore::core::Session;
use protomux_wakeup::WakeupSwarm;
use serde_json::{Map, Value, json};
use tokio::io::AsyncRead;
use tokio::sync::watch;

use super::Node;
use super::PutSource;
use super::api::Identity as ApiIdentity;
use super::api::PutMeta as BlobPutMeta;
use super::api::*;
use super::apply::{Apply, Mirror};
use super::blobs::{BlobHost, Blobs, BlobsError, Compact, EvictOptions, RechunkOptions};
use super::catalog::{Catalog, CatalogBlobs};
use super::channel::{AppendAuthority, Channel, ChannelError, Hooks};
use super::network::{LanBinding, Network, NetworkSwarm};
use super::sources::{Sources, SourcesDeps};
use super::storage::Storage;
use crate::acquire::JobSource;
use crate::app_settings::{js_string, js_truthy};
use crate::identity::{Channel as IdentityChannel, Identity as IdentityStore, IdentityContext};
use crate::protocol::{hex_encode, is_hex64};
use hyperblobs::BlobId;

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

/// `createNode`'s error: a message the host shows.
#[derive(Debug)]
pub struct NodeError(String);

impl NodeError {
    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for NodeError {}

impl From<BlobsError> for NodeError {
    fn from(error: BlobsError) -> Self {
        Self(error.to_string())
    }
}

impl From<ChannelError> for NodeError {
    fn from(error: ChannelError) -> Self {
        Self(error.to_string())
    }
}

impl From<String> for NodeError {
    fn from(error: String) -> Self {
        Self(error)
    }
}

impl From<corestore::Error> for NodeError {
    fn from(error: corestore::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<std::io::Error> for NodeError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<crate::identity::IdentityError> for NodeError {
    fn from(error: crate::identity::IdentityError) -> Self {
        Self(error.to_string())
    }
}

impl From<crate::protocol::UriError> for NodeError {
    fn from(error: crate::protocol::UriError) -> Self {
        Self(error.to_string())
    }
}

/// The acquirer's failures reach the node as its own error, as they do through the app's `put`.
impl From<crate::acquire::AcquireError> for NodeError {
    fn from(error: crate::acquire::AcquireError) -> Self {
        Self(error.message())
    }
}

/// A publish input: bytes already in memory, a file read from disk, or a resumable source the
/// acquirer and the mirror hand over (`PutStreamFactory`).
pub struct NodeSource {
    bytes: Option<Vec<u8>>,
    file: Option<PathBuf>,
    stream: Option<JobSource<NodeError>>,
    position: u64,
}

impl NodeSource {
    pub fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            bytes: Some(bytes.into()),
            file: None,
            stream: None,
            position: 0,
        }
    }

    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self {
            bytes: None,
            file: Some(path.into()),
            stream: None,
            position: 0,
        }
    }

    /// A source that opens at the offset the core already holds, so a reconnect resumes.
    pub fn resumable(stream: JobSource<NodeError>) -> Self {
        Self {
            bytes: None,
            file: None,
            stream: Some(stream),
            position: 0,
        }
    }
}

impl PutSource for NodeSource {
    type Error = NodeError;

    async fn open(
        &mut self,
        offset: u64,
        meta: &mut BlobPutMeta,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>, NodeError> {
        if let Some(stream) = &mut self.stream {
            let reader = stream.open(offset, meta).await?;
            let boxed: Box<dyn AsyncRead + Send> = reader;
            return Ok(Box::into_pin(boxed));
        }
        let mut cursor = 0u64;
        if let Some(bytes) = &self.bytes {
            cursor = offset.min(bytes.len() as u64);
            let tail = bytes[cursor as usize..].to_vec();
            if meta.expected_size.is_none() {
                meta.expected_size = Some(Some(bytes.len() as u64));
            }
            return Ok(Box::pin(std::io::Cursor::new(tail)));
        }
        let Some(file) = &self.file else {
            return Err(NodeError("no source".into()));
        };
        let mut handle = tokio::fs::File::open(file).await?;
        if meta.expected_size.is_none()
            && let Ok(stat) = handle.metadata().await
        {
            meta.expected_size = Some(Some(stat.len()));
        }
        if offset > 0 {
            use tokio::io::AsyncSeekExt;
            handle.seek(std::io::SeekFrom::Start(offset)).await?;
            cursor = offset;
        }
        self.position = cursor;
        Ok(Box::pin(handle))
    }

    fn on_data(&mut self, bytes: u64) {
        if let Some(stream) = &mut self.stream {
            stream.on_data(bytes);
        }
        self.position = bytes;
    }

    fn on_hash_state(&mut self, state: &[u8], bytes: u64) {
        if let Some(stream) = &mut self.stream {
            stream.on_hash_state(state, bytes);
        }
    }
}

/// `node.put`'s source factory for this node: what the acquirer and the mirror build a source with.
pub struct NodeStreams;

impl crate::acquire::PutStreamFactory<CoreNode> for NodeStreams {
    fn resumable(&self, source: JobSource<NodeError>) -> NodeSource {
        NodeSource::resumable(source)
    }

    fn reader(&self, source: JobSource<NodeError>) -> NodeSource {
        NodeSource::resumable(source)
    }
}

/// The channel handle `identity.rs` and the blob host call back into once it exists.
#[derive(Clone, Default)]
pub struct ChannelHandle(Arc<Mutex<Option<Arc<Channel>>>>);

impl ChannelHandle {
    fn set(&self, channel: Arc<Channel>) {
        *lock(&self.0) = Some(channel);
    }

    pub fn get(&self) -> Option<Arc<Channel>> {
        lock(&self.0).clone()
    }

    fn require(&self) -> Arc<Channel> {
        self.get()
            .unwrap_or_else(|| panic!("channel used before it opened"))
    }
}

impl IdentityChannel for ChannelHandle {
    fn primary_key(&self) -> String {
        self.require().primary_key()
    }

    fn writable(&self) -> bool {
        self.require().writable()
    }

    fn local_key(&self) -> String {
        self.require().local_key()
    }

    async fn append(&mut self, op: Value) {
        let _ = self.require().append(op).await;
    }

    async fn current_channel_epoch(&mut self) -> u64 {
        self.require().current_epoch().await
    }

    async fn local_role(&mut self) -> Option<String> {
        self.require().local_role().await
    }

    async fn list_writers(&mut self) -> Vec<Value> {
        self.require().list_writers().await
    }

    fn update(&mut self) {
        self.require().changed();
    }
}

/// The vault-backed claim source for channel writes.
struct Authority(Weak<Shared>);

impl AppendAuthority for Authority {
    fn vault_channel_key(&self) -> Option<String> {
        let shared = self.0.upgrade()?;
        let vault = lock(&shared.vault);
        vault.as_ref().map(|vault| vault.channel_key.clone())
    }

    fn owner_public_key(&self) -> Option<String> {
        let shared = self.0.upgrade()?;
        let vault = lock(&shared.vault);
        vault.as_ref().map(|vault| vault.owner_public_key.clone())
    }

    fn sign_claim(&self, channel: &str, writer: &str) -> Option<String> {
        let shared = self.0.upgrade()?;
        let vault = lock(&shared.vault);
        vault
            .as_ref()
            .map(|vault| vault.sign_claim(channel, writer))
    }
}

/// `BlobHost` over the node.
pub struct Host(Weak<Shared>);

impl BlobHost for Host {
    fn closing(&self) -> bool {
        self.0
            .upgrade()
            .is_none_or(|shared| shared.closing.load(Ordering::Acquire))
    }

    fn update(&self) {
        if let Some(shared) = self.0.upgrade() {
            shared.changed();
        }
    }

    fn now_ms(&self) -> u64 {
        now_ms()
    }

    fn has_vault(&self) -> bool {
        self.0
            .upgrade()
            .is_some_and(|shared| lock(&shared.vault).is_some())
    }

    fn epoch_key(&self, channel_hex: &str, epoch: u64) -> Option<[u8; 32]> {
        let shared = self.0.upgrade()?;
        let vault = lock(&shared.vault);
        vault
            .as_ref()
            .map(|vault| vault.epoch_key(channel_hex, epoch))
    }

    fn access_key(&self, channel_hex: &str, epoch: u64) -> Option<[u8; 32]> {
        let shared = self.0.upgrade()?;
        let keys = lock(&shared.access_keys);
        let saved = keys.get(&format!("{channel_hex}:{epoch}"))?;
        parse_hex32(&js_string(saved))
    }

    fn find_announce(&self, key: &str) -> super::blobs::SendFuture<'_, Option<Announce>> {
        let shared = self.0.upgrade();
        let key = key.to_owned();
        Box::pin(async move {
            let catalog = shared.as_ref()?.catalog.get()?.clone();
            let value = catalog.find_entry(&key).await?;
            serde_json::from_value(value).ok()
        })
    }

    fn ephemeral_announces(&self) -> super::blobs::SendFuture<'_, Vec<Announce>> {
        let shared = self.0.upgrade();
        Box::pin(async move {
            let Some(catalog) = shared.as_ref().and_then(|shared| shared.catalog.get()) else {
                return Vec::new();
            };
            catalog.ephemeral_announces()
        })
    }

    fn refresh_catalog(&self) -> super::blobs::SendFuture<'_, ()> {
        let shared = self.0.upgrade();
        Box::pin(async move {
            let Some(catalog) = shared.as_ref().and_then(|shared| shared.catalog.get()) else {
                return;
            };
            let _ = catalog.search(None, SearchOptions::default()).await;
        })
    }

    fn mark_stored(&self, discovery_key: [u8; 32]) {
        if let Some(shared) = self.0.upgrade() {
            shared.storage.mark_stored(discovery_key);
        }
    }

    fn db_disk_bytes(&self) -> u64 {
        self.0.upgrade().map_or(0, |shared| {
            dir_bytes(&shared.storage.path().join("corestore"))
        })
    }

    fn free_bytes(&self) -> Option<u64> {
        None
    }

    fn flush(&self) -> super::blobs::SendFuture<'_, ()> {
        let shared = self.0.upgrade();
        Box::pin(async move {
            if let Some(shared) = shared {
                let _ = tokio::task::spawn_blocking(move || shared.storage.flush()).await;
            }
        })
    }

    fn compact(&self, policy: Compact) -> super::blobs::SendFuture<'_, ()> {
        let shared = self.0.upgrade();
        // JS: forced blob relocation only for a full wipe with no local blobs left.
        let relocate = policy.full && !policy.streaming && !policy.has_local_blobs;
        Box::pin(async move {
            if let Some(shared) = shared {
                let _ = tokio::task::spawn_blocking(move || shared.storage.compact(relocate)).await;
            }
        })
    }
}

fn dir_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0;
    for entry in entries.flatten() {
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        total += if metadata.is_dir() {
            dir_bytes(&entry.path())
        } else {
            metadata.len()
        };
    }
    total
}

/// The node's storage and the late-bound halves of its graph.
pub struct Shared {
    pub storage: Storage,
    pub opts: Value,
    memory_window_budget: Option<Arc<tokio::sync::Semaphore>>,
    pub closing: Arc<AtomicBool>,
    /// The node's one wakeup swarm, shared by the channel and every followed
    /// channel: a connection pairs wakeup once, so there can only be one.
    pub wakeup: Arc<WakeupSwarm>,
    pub channel: OnceLock<Arc<Channel>>,
    pub net: OnceLock<Arc<Network>>,
    pub blobs: OnceLock<Blobs<Arc<Channel>, Arc<Network>, Host>>,
    pub sources: OnceLock<Arc<Sources>>,
    pub catalog: OnceLock<Arc<Catalog>>,
    pub identity: tokio::sync::Mutex<Option<IdentityStore<ChannelHandle>>>,
    pub vault: Mutex<Option<crate::mnemonic::Identity>>,
    pub access_keys: Mutex<Map<String, Value>>,
    pub handle: ChannelHandle,
    pub changes: Mutex<Option<watch::Sender<u64>>>,
    pub primary_alias: Mutex<String>,
    pub close_started: Mutex<bool>,
    pub swarm_cell: OnceLock<NetworkSwarm>,
    pub dht: OnceLock<peeroxide_dht::hyperdht::HyperDhtHandle>,
    pub blob_server: OnceLock<std::net::SocketAddr>,
}

impl Shared {
    /// Copies the vault and saved access keys out for the synchronous callers.
    fn refresh_from(&self, identity: &IdentityStore<ChannelHandle>) {
        *lock(&self.vault) = identity.id_vault().cloned();
        *lock(&self.access_keys) = identity.access_keys().clone();
    }

    /// `ctx.update()`'s notification half: tell the app something changed.
    pub fn changed(&self) {
        if let Some(sender) = lock(&self.changes).as_ref() {
            sender.send_modify(|value| *value += 1);
        }
    }
}

/// The real node: `createNode(opts)`.
pub struct CoreNode {
    inner: Arc<Shared>,
}

impl CoreNode {
    pub fn memory_window_budget(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        self.inner.memory_window_budget.clone()
    }

    pub fn memory_cache_bytes(&self) -> Option<u64> {
        self.inner
            .opts
            .get("memoryCacheBytes")
            .and_then(Value::as_u64)
    }

    fn channel(&self) -> Arc<Channel> {
        self.inner.channel.get().cloned().expect("channel open")
    }

    /// The swarm, the connection set and the traffic sample.
    pub fn net(&self) -> Arc<Network> {
        self.inner.net.get().cloned().expect("network open")
    }

    fn blobs(&self) -> &Blobs<Arc<Channel>, Arc<Network>, Host> {
        self.inner.blobs.get().expect("blobs open")
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.inner.catalog.get().cloned().expect("catalog open")
    }

    fn sources(&self) -> Arc<Sources> {
        self.inner.sources.get().cloned().expect("sources open")
    }

    /// `status()`.
    pub async fn status_value(&self) -> Status {
        let net = self.net();
        let blobs = self.blobs();
        let channel = self.channel();
        let sample = net.sample();
        let cached_bytes = blobs.cached_total_bytes().await;
        let mut owned_video_bytes = 0;
        for meta in blobs.owned_blobs().values() {
            owned_video_bytes += meta.byte_length;
        }
        let mut blob_bytes = owned_video_bytes;
        for key in blobs.slot_keys() {
            if let Some(meta) = blobs.owned(&key) {
                blob_bytes += meta.byte_length;
            }
        }
        let (tier, ratio, inflight_max) = self.apply_reciprocity(
            blob_bytes + cached_bytes,
            sample.tx_bytes,
            sample.rx_bytes,
            sample.contributing_peers,
        );
        let writer_hex = channel.local_key();
        let vault_channel = lock(&self.inner.vault)
            .as_ref()
            .map(|vault| vault.channel_key.clone());
        Status {
            tracker: if channel.use_primary() || channel.has_entries() {
                channel.primary_key()
            } else {
                String::new()
            },
            channel: channel.primary_key(),
            writer: writer_hex.clone(),
            writable: channel.writable()
                || writer_hex == channel.primary_key()
                || vault_channel.as_deref() == Some(channel.primary_key().as_str()),
            blobs: hex(&blobs.blobs_key()),
            blob_bytes,
            cached_bytes,
            cache_max_bytes: blobs.cache_max_bytes(),
            // JS `swarm.connections.size`: the swarm's own peers; LAN ones count only in lanPeers.
            peers: net
                .connection_count()
                .saturating_sub(net.lan_connection_count()),
            lan_peers: net.lan_connection_count(),
            down_bps: serde_json::Number::from_f64(sample.down_bps).unwrap_or_else(|| 0.into()),
            up_bps: serde_json::Number::from_f64(sample.up_bps).unwrap_or_else(|| 0.into()),
            rx_bytes: sample.rx_bytes,
            tx_bytes: sample.tx_bytes,
            routes: sample.routes.clone(),
            inflight_blocks: blobs.inflight_blocks().await,
            rtt: serde_json::Number::from(sample.rtt),
            retransmits: sample.retransmits,
            sources: self.list_sources_value(),
            reciprocity: Reciprocity {
                tier,
                seeded_bytes: blob_bytes + cached_bytes,
                contributing_peers: sample.contributing_peers,
                ratio: serde_json::Number::from_f64(ratio).unwrap_or_else(|| 0.into()),
                inflight_max,
            },
        }
    }

    /// `applyReciprocity`: seeders earn a wider window than empty leechers.
    fn apply_reciprocity(
        &self,
        seeded: u64,
        tx: u64,
        rx: u64,
        contributing_peers: u64,
    ) -> (String, f64, u64) {
        let net = self.net();
        let blobs = self.blobs();
        let worklet = self.worklet();
        let any_bytes = blobs
            .owned_blobs()
            .values()
            .any(|meta| meta.byte_length > 0)
            || blobs
                .slot_keys()
                .iter()
                .any(|key| blobs.owned(key).is_some_and(|meta| meta.byte_length > 0));
        let tier = if seeded > 0
            && (tx > 0
                || !blobs.owned_blobs().is_empty()
                || any_bytes
                || seeded >= crate::protocol::BLOB_BLOCK_SIZE)
        {
            "seeder"
        } else if seeded > 0 || contributing_peers > 0 {
            "peer"
        } else {
            "leecher"
        };
        let private = net.private_routing();
        let next = if private {
            match tier {
                "seeder" => (64, 192),
                "peer" => (48, 128),
                _ => (24, 64),
            }
        } else if self.storage().is_mobile_worklet() {
            (6, 8)
        } else if worklet {
            (16, 48)
        } else {
            match tier {
                "seeder" => (16, 48),
                "peer" => (12, 32),
                _ => (8, 24),
            }
        };
        net.set_inflight_range(next);
        let ratio = if rx > 0 {
            ((tx as f64 / rx as f64) * 100.0).round() / 100.0
        } else if tx > 0 {
            1.0
        } else {
            0.0
        };
        (tier.to_owned(), ratio, u64::from(next.1))
    }

    fn list_sources_value(&self) -> Vec<Source> {
        let channel = self.channel();
        let alias = lock(&self.inner.primary_alias).clone();
        let writable = channel.writable()
            || lock(&self.inner.vault)
                .as_ref()
                .is_some_and(|vault| vault.channel_key == channel.primary_key());
        let primary = if channel.use_primary() || channel.has_entries() || !alias.is_empty() {
            Some(Source {
                key: channel.primary_key(),
                kind: "channel".to_owned(),
                alias,
                primary: true,
                writable,
                entries: None,
                active: None,
                hydrated: None,
            })
        } else {
            None
        };
        self.sources().list(primary)
    }

    fn storage(&self) -> &Storage {
        &self.inner.storage
    }

    fn worklet(&self) -> bool {
        self.storage().worklet()
    }

    /// `seedableCores({ includeCached })`.
    pub fn seedable_cores_value(&self, include_cached: bool) -> Vec<SeedableCore> {
        let channel = self.channel();
        let blobs = self.blobs();
        let mut out = Vec::new();
        let (length, byte_length) = channel.bootstrap_len();
        if length > 0 && (channel.has_entries() || !blobs.owned_blobs().is_empty()) {
            out.push(SeedableCore {
                key: channel.primary_key(),
                kind: "channel".to_owned(),
                length,
                byte_length,
            });
        }
        for hex in channel.writer_core_keys() {
            if let Some((length, byte_length)) = channel.writer_core_len(&hex)
                && length > 0
            {
                out.push(SeedableCore {
                    key: hex,
                    kind: "channel".to_owned(),
                    length,
                    byte_length,
                });
            }
        }
        for (key, meta) in blobs.owned_blobs() {
            if meta.length > 0 {
                out.push(SeedableCore {
                    key: hex(&key),
                    kind: "blobs".to_owned(),
                    length: meta.length,
                    byte_length: meta.byte_length,
                });
            }
        }
        for key in blobs.slot_keys() {
            if let Some(meta) = blobs.owned(&key)
                && meta.length > 0
                && blobs.local_blocks(&key) > 0
            {
                out.push(SeedableCore {
                    key: hex(&key),
                    kind: "blobs".to_owned(),
                    length: meta.length,
                    byte_length: meta.byte_length,
                });
            }
        }
        if include_cached {
            for (key, meta) in blobs.cached_blobs() {
                if blobs.is_owned_blob(&key) || meta.length == 0 || meta.byte_length == 0 {
                    continue;
                }
                if meta.blocks >= meta.length {
                    out.push(SeedableCore {
                        key: hex(&key),
                        kind: "blobs".to_owned(),
                        length: meta.length,
                        byte_length: meta.byte_length,
                    });
                }
            }
        }
        out
    }

    /// `close()`: the reference's order (activity, cores, store).
    pub async fn close(&self) -> Result<(), NodeError> {
        {
            let mut closed = lock(&self.inner.close_started);
            if *closed {
                return Ok(());
            }
            *closed = true;
        }
        self.inner.closing.store(true, Ordering::Release);
        if let Some(sources) = self.inner.sources.get() {
            sources.flush();
        }
        if let Some(blobs) = self.inner.blobs.get() {
            blobs.stop_server().await;
        }
        if let Some(net) = self.inner.net.get() {
            net.close().await;
        }
        if let Some(sources) = self.inner.sources.get() {
            for key in sources.keys() {
                if let Some(record) = sources.get(&key) {
                    sources.hibernate(&record).await;
                }
            }
        }
        if let Some(channel) = self.inner.channel.get() {
            channel.close().await;
        }
        // Every bee shares this swarm and none of them owns it.
        self.inner.wakeup.destroy();
        self.inner.storage.close().await;
        Ok(())
    }
}

/// `createNode(opts)`, the reference's opening order and undo discipline.
pub async fn create_node(options: Value) -> Result<Arc<CoreNode>, NodeError> {
    let storage_path = options
        .get("storage")
        .and_then(Value::as_str)
        .ok_or_else(|| NodeError("storage is required".into()))?
        .to_owned();
    let worklet = options.get("worklet").is_some_and(js_truthy);
    let wal = options
        .get("storageWal")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let memory = options
        .get("storageMemory")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let storage = Storage::open(&storage_path, worklet, wal, memory)?;
    let closing = Arc::new(AtomicBool::new(false));
    let handle = ChannelHandle::default();
    let wakeup = Arc::new(WakeupSwarm::new(|_, _| {}));

    let shared: Arc<Shared> = Arc::new_cyclic(|_weak| Shared {
        storage,
        opts: options.clone(),
        memory_window_budget: options.get("memoryCacheBytes").and_then(Value::as_u64).map(
            |bytes| {
                Arc::new(tokio::sync::Semaphore::new(
                    (bytes.saturating_mul(3) / 4)
                        .min(u32::MAX as u64)
                        .min(tokio::sync::Semaphore::MAX_PERMITS as u64)
                        as usize,
                ))
            },
        ),
        closing: closing.clone(),
        wakeup: wakeup.clone(),
        channel: OnceLock::new(),
        net: OnceLock::new(),
        blobs: OnceLock::new(),
        sources: OnceLock::new(),
        catalog: OnceLock::new(),
        identity: tokio::sync::Mutex::new(None),
        vault: Mutex::new(None),
        access_keys: Mutex::new(Map::new()),
        handle: handle.clone(),
        changes: Mutex::new(None),
        primary_alias: Mutex::new(String::new()),
        close_started: Mutex::new(false),
        swarm_cell: OnceLock::new(),
        dht: OnceLock::new(),
        blob_server: OnceLock::new(),
    });
    let node = Arc::new(CoreNode {
        inner: shared.clone(),
    });
    let weak = Arc::downgrade(&shared);

    {
        let (sender, _receiver) = watch::channel(0u64);
        *lock(&shared.changes) = Some(sender);
    }

    let identity = IdentityStore::new(IdentityContext {
        storage: PathBuf::from(&storage_path),
        mnemonic: options
            .get("mnemonic")
            .and_then(Value::as_str)
            .map(|text| text.to_owned()),
        channel: handle.clone(),
    })?;
    {
        let mut slot = shared.identity.lock().await;
        *slot = Some(identity);
        if let Some(identity) = slot.as_ref() {
            shared.refresh_from(identity);
        }
    }

    let tracker_opt = options
        .get("tracker")
        .and_then(Value::as_str)
        .map(|text| text.trim().to_lowercase())
        .filter(|text| !text.is_empty());
    if let Some(target) = &tracker_opt
        && !is_hex64(target)
    {
        return Err(NodeError("Channel key must be 64 hex characters".into()));
    }
    let join_opt = options.get("join").is_some_and(js_truthy);
    let initial_sources: Vec<SourceSpec> = options
        .get("sources")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| serde_json::from_value::<SourceSpec>(item.clone()).ok())
                .collect()
        })
        .unwrap_or_default();

    let tracker_store = shared.storage.store().namespace("tracker");
    let has_pre_mnemonic_local = {
        let mut opts = corestore::GetOptions::name("local".as_bytes());
        opts.inactive = true;
        let probe = tracker_store.get(opts).await?;
        let length = probe.length();
        probe.close();
        length > 0
    };

    let mut founder = None;
    let mut founder_channel: Option<[u8; 32]> = None;
    let mut target_key: Option<[u8; 32]> = tracker_opt.as_deref().and_then(parse_hex32);
    {
        let mut guard = shared.identity.lock().await;
        let identity = guard.as_mut().expect("identity constructed");
        if target_key.is_none() {
            let join_vault = join_opt
                .then(|| identity.id_vault())
                .flatten()
                .map(|vault| vault.channel_key.clone());
            if join_opt && let Some(vault_key) = join_vault {
                target_key = parse_hex32(&vault_key);
            }
        }
        if target_key.is_none() {
            if has_pre_mnemonic_local && identity.founded_key().is_empty() {
                identity.ensure_vault();
            } else {
                identity.ensure_vault();
                let founded = identity.founded_key().to_owned();
                let vault_key = identity
                    .id_vault()
                    .map(|vault| vault.channel_key.clone())
                    .unwrap_or_default();
                if !founded.is_empty() && founded != vault_key {
                    target_key = parse_hex32(&founded);
                } else {
                    founder = identity
                        .id_vault()
                        .map(|vault| vault.genesis_key_pair.clone());
                    // A founder's channel is its genesis core, and a v1 manifest hashes the genesis
                    // key: `channel.key` is the vault's channel key, not the key pair's public key.
                    founder_channel = parse_hex32(&vault_key);
                }
            }
        } else if identity.id_vault().is_none() && !join_opt {
            identity.ensure_vault();
        }
    }

    let primary = match target_key.or(founder_channel) {
        Some(key) => key,
        None => {
            let mut opts = corestore::GetOptions::name("local".as_bytes());
            opts.inactive = true;
            let probe = tracker_store.get(opts).await?;
            let key = *probe.key();
            probe.close();
            key
        }
    };
    let primary_hex = hex_encode(&primary);

    let channel_slot: Arc<Mutex<Option<Arc<Channel>>>> = Arc::new(Mutex::new(None));
    let writer_slot = channel_slot.clone();
    let mirror_slot = weak.clone();
    let apply = Apply::new(primary_hex.clone())
        .on_writer_change(Arc::new(move |writer, added| {
            if let Some(channel) = lock(&writer_slot).clone() {
                channel.writer_changed(writer, added);
            }
        }))
        .with_mirror(Mirror::new(
            Arc::new(move |core_key| {
                if let Some(shared) = mirror_slot.upgrade() {
                    land_core(&shared, core_key);
                }
            }),
            {
                let mirror = weak.clone();
                Arc::new(move || {
                    if let Some(shared) = mirror.upgrade() {
                        shared.changed();
                    }
                })
            },
        ));
    let hooks = Hooks {
        update: {
            let weak = weak.clone();
            Some(Arc::new(move || {
                if let Some(shared) = weak.upgrade() {
                    shared.changed();
                }
            }) as Arc<dyn Fn() + Send + Sync>)
        },
        join_topic: {
            let weak = weak.clone();
            Some(Arc::new(move |id: &str| {
                if let Some(shared) = weak.upgrade()
                    && let Some(net) = shared.net.get()
                    && let Some(channel) = shared.channel.get()
                {
                    net.join_overlay_topic(id, channel.key());
                }
            }) as Arc<dyn Fn(&str) + Send + Sync>)
        },
    };
    let channel = Channel::open(
        tracker_store,
        target_key,
        primary,
        founder,
        apply,
        hooks,
        wakeup.clone(),
        closing.clone(),
        worklet,
    )
    .await?;
    {
        let mut guard = shared.identity.lock().await;
        let identity = guard.as_mut().expect("identity constructed");
        if channel.writable() && identity.founded_key().is_empty() {
            identity.mark_founded(&primary_hex);
        }
        identity.save_identity();
        shared.refresh_from(identity);
    }
    handle.set(channel.clone());
    *lock(&channel_slot) = Some(channel.clone());
    let _ = shared.channel.set(channel.clone());
    channel.set_authority(Arc::new(Authority(weak.clone())));
    // `usePrimaryChannel = Boolean(targetKey) || initialSources.length === 0`
    channel.set_use_primary(target_key.is_some() || initial_sources.is_empty());

    if target_key.is_none() && channel.local_length() == 0 && channel.writable() {
        let owner = lock(&shared.vault)
            .as_ref()
            .map(|vault| vault.owner_public_key.clone());
        let mut op = Map::new();
        op.insert("type".to_owned(), Value::String("init".to_owned()));
        if let Some(owner) = owner {
            op.insert("owner".to_owned(), Value::String(owner));
        }
        op.insert("epoch".to_owned(), json!(1));
        channel.append(Value::Object(op)).await?;
    }

    channel.prime().await;
    let founded_here = {
        let guard = shared.identity.lock().await;
        guard
            .as_ref()
            .is_some_and(|identity| identity.founded_key() == primary_hex)
    };
    if channel.writable() && founded_here && channel.view_get_timeout(b"meta/owner").await.is_none()
    {
        let owner = lock(&shared.vault)
            .as_ref()
            .map(|vault| vault.owner_public_key.clone());
        if let Some(owner) = owner {
            channel
                .append(json!({ "type": "owner", "owner": owner }))
                .await?;
        }
    }

    let bootstrap = super::network::bootstrap_list(options.get("bootstrap"));
    let private_host = options
        .get("privateHost")
        .and_then(Value::as_str)
        .map(|text| text.to_owned());
    let dht_port = options.get("dhtPort").and_then(Value::as_u64).unwrap_or(0) as u16;
    let relay_through = options
        .get("relayThrough")
        .and_then(Value::as_array)
        .and_then(|keys| keys.first())
        .and_then(Value::as_str)
        .and_then(parse_hex32);
    let private_routing = options.get("privateRouting").is_some_and(js_truthy);
    let allowed_peers = options
        .get("allowedPeers")
        .and_then(Value::as_array)
        .map(|keys| {
            keys.iter()
                .map(|key| js_string(key).to_lowercase())
                .collect::<Vec<_>>()
        });
    let net = Network::open(
        shared.storage.store().clone(),
        bootstrap,
        private_host,
        dht_port,
        relay_through,
        allowed_peers,
        private_routing,
        closing.clone(),
    )
    .await?;
    net.attach_wakeup(channel.auto().clone());
    if channel.use_primary() {
        net.join_overlay_topic(&format!("channel:{}", channel.primary_key()), channel.key());
    }
    let _ = shared.net.set(net.clone());

    let host = Host(weak.clone());
    let blobs = Blobs::open(
        PathBuf::from(&storage_path),
        shared.storage.store().clone(),
        channel.clone(),
        net.clone(),
        host,
        worklet,
    )
    .await?;
    let stream_host = options
        .get("streamHost")
        .and_then(Value::as_str)
        .unwrap_or("127.0.0.1");
    let stream_port = options
        .get("streamPort")
        .and_then(Value::as_u64)
        .unwrap_or(0) as u16;
    let window = if shared.storage.is_mobile_worklet() {
        96 * 1024 * 1024
    } else {
        256 * 1024 * 1024
    };
    let addr: std::net::SocketAddr = format!("{stream_host}:{stream_port}")
        .parse()
        .map_err(|_| NodeError("invalid stream host".into()))?;
    let bound = blobs.start_server(addr, window).await?;
    let _ = shared.blob_server.set(bound);
    let _ = shared.blobs.set(blobs.clone());

    let sources = Sources::new(
        Arc::new(SourcesDeps {
            store: shared.storage.store().clone(),
            wakeup: wakeup.clone(),
            closing: closing.clone(),
            update: {
                let weak = weak.clone();
                Arc::new(move || {
                    if let Some(shared) = weak.upgrade() {
                        shared.changed();
                    }
                })
            },
            join_topic: {
                let weak = weak.clone();
                Arc::new(move |id: &str| {
                    if let Some(shared) = weak.upgrade()
                        && let Some(net) = shared.net.get()
                        // `channel:<key>` names the followed source, whose own discovery key is
                        // the topic `lan.js` joins (`bee.key` / `bee.discoveryKey` in sources.js).
                        && let Some(key) = id.strip_prefix("channel:").and_then(parse_hex32)
                    {
                        net.join_overlay_topic(id, key);
                    }
                })
            },
            leave_topic: {
                let weak = weak.clone();
                Arc::new(move |id: String| {
                    let Some(net) = weak.upgrade().and_then(|shared| shared.net.get().cloned())
                    else {
                        return;
                    };
                    tokio::spawn(async move {
                        net.leave_overlay_topic(&id).await;
                    });
                })
            },
            attach_wakeup: {
                let weak = weak.clone();
                Arc::new(move |bee: Arc<Autobee>| {
                    if let Some(shared) = weak.upgrade()
                        && let Some(net) = shared.net.get()
                    {
                        net.attach_wakeup_now(bee);
                    }
                })
            },
            make_apply: {
                let weak = weak.clone();
                Arc::new(move |channel_hex: &str| {
                    let mirror = weak.clone();
                    Apply::new(channel_hex.to_owned()).with_mirror(Mirror::new(
                        Arc::new(move |core_key| {
                            if let Some(shared) = mirror.upgrade() {
                                land_core(&shared, core_key);
                            }
                        }),
                        {
                            let update = weak.clone();
                            Arc::new(move || {
                                if let Some(shared) = update.upgrade() {
                                    shared.changed();
                                }
                            })
                        },
                    ))
                })
            },
            cache_file: PathBuf::from(&storage_path).join("catalog-cache.json"),
            worklet,
            cap: options
                .get("maxActiveSources")
                .and_then(Value::as_u64)
                .map(|value| value.max(1) as usize)
                .unwrap_or(if worklet { 8 } else { 24 }),
            max_active: options.get("maxActiveSources").and_then(Value::as_u64),
        }),
        &initial_sources,
    );
    let _ = shared.sources.set(sources.clone());
    // Wake the first `cap` saved channels, as `createSources` does at boot.
    let mut woken = 0usize;
    for spec in initial_sources.iter().take(sources.cap()) {
        let key = match spec {
            SourceSpec::Uri(text) => text.trim().to_lowercase(),
            SourceSpec::Object(object) => object
                .key
                .clone()
                .or_else(|| object.uri.clone())
                .unwrap_or_default()
                .trim()
                .to_lowercase(),
        };
        if is_hex64(&key) && sources.wake(&key, false).await.is_some() {
            woken += 1;
        }
    }
    let _ = woken;

    let catalog = Catalog::new(
        channel.clone(),
        sources.clone(),
        Arc::new(BlobOps(weak.clone())),
    );
    let _ = shared.catalog.set(catalog.clone());

    // A primary channel is hydrated once the update that follows the swarm's join has run.
    if channel.use_primary() {
        let bee = channel.auto().clone();
        let blobs = blobs.clone();
        let closing = closing.clone();
        tokio::spawn(async move {
            let _ = bee.update().await;
            if !closing.load(Ordering::Acquire) {
                let _ = blobs.hydrate_owned().await;
            }
        });
    }

    net.set_inflight_range(if shared.storage.is_mobile_worklet() {
        (6, 8)
    } else if worklet {
        (16, 48)
    } else {
        (12, 32)
    });

    Ok(node)
}

fn land_core(shared: &Arc<Shared>, key: [u8; 32]) {
    let hex = hex_encode(&key);
    if shared.catalog.get().is_none() && shared.net.get().is_none() {
        return;
    }
    let store = shared.storage.store().clone();
    tokio::spawn(async move {
        let Ok(session) = store.get(corestore::GetOptions::key(key)).await else {
            return;
        };
        let _ = session.download(0, None, false).await;
        drop(hex);
    });
}

/// The blob operations the catalog asks for.
struct BlobOps(Weak<Shared>);

impl BlobOps {
    fn blobs(&self) -> Option<Blobs<Arc<Channel>, Arc<Network>, Host>> {
        self.0.upgrade()?.blobs.get().cloned()
    }
}

impl CatalogBlobs for BlobOps {
    fn owned(&self, blob: &[u8; 32]) -> bool {
        self.blobs().is_some_and(|blobs| blobs.is_owned_blob(blob))
    }

    fn slot_contains(&self, blob: &[u8; 32]) -> bool {
        self.blobs()
            .is_some_and(|blobs| blobs.slot_keys().contains(blob))
    }

    fn slot_keys(&self) -> Vec<[u8; 32]> {
        self.blobs()
            .map_or_else(Vec::new, |blobs| blobs.slot_keys())
    }

    fn open_core(&self, blob: &[u8; 32]) -> bool {
        self.blobs()
            .is_some_and(|blobs| blobs.open_core(blob).is_some())
    }

    fn cached_meta(&self, blob: &[u8; 32]) -> Option<(u64, u64, u64)> {
        self.blobs().and_then(|blobs| {
            blobs
                .cached_blobs()
                .get(blob)
                .map(|meta| (meta.length, meta.byte_length, meta.blocks))
        })
    }

    fn absent(&self, discovery: &[u8; 32]) -> bool {
        self.blobs()
            .is_some_and(|blobs| blobs.absent_disc().contains(discovery))
    }

    fn blob_disc(&self, blob: &[u8; 32]) -> [u8; 32] {
        self.blobs().map_or(*blob, |blobs| blobs.blob_disc(blob))
    }

    fn stored(&self, discovery: &[u8; 32]) -> bool {
        self.0
            .upgrade()
            .is_some_and(|shared| shared.storage.has_stored(discovery))
    }

    fn open_core_session(&self, blob: &[u8; 32]) -> Option<Arc<Session>> {
        self.blobs().and_then(|blobs| blobs.open_core(blob))
    }

    fn probe<'a>(&'a self, blob: &'a [u8; 32]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let Some(shared) = self.0.upgrade() else {
                return;
            };
            let Some(blobs) = shared.blobs.get() else {
                return;
            };
            let discovery = blobs.blob_disc(blob);
            let exists = shared
                .storage
                .store()
                .storage_infos(&[discovery])
                .ok()
                .and_then(|infos| infos.into_iter().next().flatten())
                .is_some();
            if !exists {
                shared.storage.forget_stored(&discovery);
                return;
            }
            let mut opts = corestore::GetOptions::key(*blob);
            opts.inactive = true;
            let Ok(session) = shared.storage.store().get(opts).await else {
                return;
            };
            let length = session.length();
            let byte_length = session.byte_length();
            let blocks = if length > 0 {
                let mut count = 0;
                for index in 0..length {
                    if session.has(index) {
                        count += 1;
                    }
                }
                count
            } else {
                0
            };
            blobs.record_cached(*blob, length, byte_length, blocks);
            session.close();
        })
    }

    fn entry_cached_bytes<'a>(
        &'a self,
        blob: &'a [u8; 32],
        entry: &'a Announce,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>> {
        Box::pin(async move {
            let Some(blobs) = self.blobs() else {
                return 0;
            };
            let core = blobs.open_core(blob);
            blobs.entry_cached_bytes(core.as_ref(), entry).await
        })
    }

    fn resolve_video_key(&self, entry: &Announce) -> bool {
        self.blobs()
            .is_some_and(|blobs| blobs.resolve_video_key(entry).is_some())
    }

    fn join_content(&self, blob: &[u8; 32], evict: bool) {
        let _ = evict;
        if let Some(shared) = self.0.upgrade()
            && let Some(net) = shared.net.get()
        {
            net.join_content(*blob);
        }
    }

    fn join_source_swarm(&self, source: &[u8; 32]) {
        if let Some(net) = self
            .0
            .upgrade()
            .and_then(|shared| shared.net.get().cloned())
            && !net.private_routing()
        {
            net.join_overlay_topic(&format!("source-swarm:{}", hex(source)), *source);
        }
    }

    fn stream_url(&self, entry: &Announce) -> String {
        self.blobs()
            .map_or_else(String::new, |blobs| blobs.stream_url(entry))
    }

    fn warm<'a>(&'a self, entry: &'a Announce) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if let Some(blobs) = self.blobs() {
                let _ = blobs.warm(entry).await;
            }
        })
    }
}

impl Node for CoreNode {
    type Error = NodeError;
    type Store = Corestore;
    type Core = Arc<Session>;
    type Channel = Arc<Channel>;
    type Swarm = NetworkSwarm;
    type Dht = peeroxide_dht::hyperdht::HyperDhtHandle;
    type Connection = super::network::ConnectionHandle;
    type ConnectionSubscription = Box<dyn Fn() + Send + Sync>;
    type Lan = Arc<dyn LanBinding>;
    type BlobServer = std::net::SocketAddr;
    type PutSource = NodeSource;
    type RechunkOptions = RechunkOptions;

    fn store(&self) -> &Self::Store {
        self.storage().store()
    }

    fn channel(&self) -> &Self::Channel {
        self.inner.channel.get().expect("channel open")
    }

    fn swarm(&self) -> &Self::Swarm {
        // The swarm handle is created with the network; `NetworkSwarm` is rebuilt per call in the
        // contract but must be borrowed, so the node keeps one alive beside the network.
        self.inner.swarm_cell.get_or_init(|| NetworkSwarm {
            network: self.net(),
        })
    }

    fn dht(&self) -> &Self::Dht {
        self.inner.dht.get_or_init(|| self.net().dht().clone())
    }

    fn blob_server(&self) -> &Self::BlobServer {
        self.inner
            .blob_server
            .get()
            .expect("the blob server starts with the node")
    }

    fn private_routing(&self) -> bool {
        self.net().private_routing()
    }

    fn bootstrap(&self) -> Option<Value> {
        self.inner.opts.get("bootstrap").cloned()
    }

    fn relay_through(&self) -> Option<Value> {
        self.inner.opts.get("relayThrough").cloned()
    }

    fn peer_key(&self) -> String {
        hex(&self.net().peer_key())
    }

    fn closing(&self) -> bool {
        self.inner.closing.load(Ordering::Acquire)
    }

    fn peer_connections(&self) -> Vec<Self::Connection> {
        self.net().connections()
    }

    fn on_connection(
        &self,
        callback: Box<dyn Fn(Self::Connection) + Send + Sync>,
    ) -> Self::ConnectionSubscription {
        self.net().on_connection(Arc::from(callback))
    }

    fn onchange(&self) -> watch::Receiver<u64> {
        lock(&self.inner.changes)
            .as_ref()
            .map(|sender| sender.subscribe())
            .unwrap_or_else(|| watch::channel(0u64).1)
    }

    async fn find_entry(&self, key: &str) -> Result<Option<Value>, Self::Error> {
        Ok(self.catalog().find_entry(key).await)
    }

    async fn blob_core(&self, key: &str) -> Result<Self::Core, Self::Error> {
        let blob = parse_hex32(key).ok_or_else(|| NodeError("Invalid blob key".into()))?;
        Ok(self.blobs().blob_core(&blob).await?)
    }

    async fn seedable_cores(&self, include_cached: bool) -> Result<Vec<SeedableCore>, Self::Error> {
        Ok(self.seedable_cores_value(include_cached))
    }

    async fn has_lan(&self) -> bool {
        self.net().has_lan()
    }

    async fn lan_endpoint(&self) -> Option<LanEndpoint> {
        self.net().lan_endpoint()
    }

    async fn dial_lan_peer(&self, endpoint: LanEndpoint) {
        self.net().dial_lan_peer(&endpoint);
    }

    async fn join_peer(&self, key: &str, lan_first: bool) {
        self.net().join_peer(key, lan_first);
    }

    async fn evict_behind(
        &self,
        blobs: &str,
        blob: Blob,
        options: Value,
    ) -> Result<u64, Self::Error> {
        let key = parse_hex32(blobs).ok_or_else(|| NodeError("Invalid blob key".into()))?;
        let mut opts = EvictOptions {
            keep_ahead_bytes: self.memory_cache_bytes().map(|limit| limit * 7 / 8),
            ..EvictOptions::default()
        };
        if let Some(value) = options.get("keepAheadBytes").and_then(Value::as_u64) {
            opts.keep_ahead_bytes = Some(value);
        }
        if let Some(value) = options.get("playheadByte").and_then(Value::as_u64) {
            opts.playhead_byte = value;
        }
        if let Some(value) = options.get("keepBehindBytes").and_then(Value::as_u64) {
            opts.keep_behind_bytes = value;
        }
        if let Some(value) = options.get("maxBytes").and_then(Value::as_u64) {
            opts.max_bytes = Some(value);
        }
        let id = BlobId {
            block_offset: blob.block_offset,
            block_length: blob.block_length,
            byte_offset: blob.byte_offset,
            byte_length: blob.byte_length,
        };
        Ok(self.blobs().evict_behind(&key, id, opts).await)
    }

    async fn refresh_block_count(&self, core: &Self::Core) -> Result<(), Self::Error> {
        self.blobs().sync_block_counter(core).await;
        Ok(())
    }

    async fn search(
        &self,
        id: Option<&str>,
        options: SearchOptions,
    ) -> Result<Vec<Entry>, Self::Error> {
        Ok(self.catalog().search(id, options).await)
    }

    async fn put(
        &self,
        meta: super::api::PutMeta,
        source: Self::PutSource,
    ) -> Result<Announce, Self::Error> {
        Ok(self.blobs().put(api_meta(meta), source).await?)
    }

    async fn drop_store(&self, name: &str) -> Result<(), Self::Error> {
        self.blobs().drop_store(name).await?;
        Ok(())
    }

    async fn append(&self, op: Value) -> Result<(), Self::Error> {
        self.channel().append(op).await?;
        Ok(())
    }

    async fn publish(
        &self,
        meta: super::api::PutMeta,
        source: Self::PutSource,
    ) -> Result<Published, Self::Error> {
        Ok(self.blobs().publish(api_meta(meta), source).await?)
    }

    async fn publish_channel(
        &self,
        meta_or_op: Value,
        source: Option<Self::PutSource>,
    ) -> Result<Published, Self::Error> {
        Ok(self.blobs().publish_channel(meta_or_op, source).await?)
    }

    async fn remove(&self, key: &str) -> Result<(), Self::Error> {
        let channel = self.channel();
        let primary = channel.primary_key();
        let clean = key
            .strip_prefix(&format!("channel:{primary}/"))
            .or_else(|| key.strip_prefix(&format!("tracker:{primary}/")))
            .unwrap_or(key);
        let previous = self
            .catalog()
            .find_entry(clean)
            .await
            .and_then(|value| serde_json::from_value::<Announce>(value).ok());
        channel
            .append(json!({ "type": "remove", "key": clean }))
            .await?;
        if let Some(previous) = previous {
            let _ = self.blobs().gc_unreferenced_slot_blob(&previous).await;
        }
        channel.changed();
        Ok(())
    }

    async fn publish_source(&self, source_key: &str, title: &str) -> Result<Value, Self::Error> {
        Ok(self.channel().publish_source(source_key, title).await?)
    }

    async fn sync_local_channel(&self) -> Result<SyncResult, Self::Error> {
        Ok(self.blobs().sync_local_channel().await?)
    }

    async fn rechunk_legacy_blobs(
        &self,
        options: Self::RechunkOptions,
    ) -> Result<RechunkResult, Self::Error> {
        Ok(self.blobs().rechunk_legacy_blobs(options).await?)
    }

    async fn unpublish_source(&self, key: &str) -> Result<Value, Self::Error> {
        Ok(self.channel().unpublish_source(key).await?)
    }

    async fn published_sources(&self) -> Result<Vec<PublishedSource>, Self::Error> {
        let list = self.channel().published_sources().await?;
        Ok(list
            .into_iter()
            .filter_map(|value| serde_json::from_value::<PublishedSource>(value).ok())
            .collect())
    }

    async fn add_source(&self, spec: SourceSpec, wake: bool) -> Result<Source, Self::Error> {
        Ok(self.sources().add(spec, wake).await?)
    }

    async fn remove_source(&self, key: &str) -> Result<Removed, Self::Error> {
        Ok(self.sources().remove(key).await)
    }

    async fn list_sources(&self) -> Vec<Source> {
        self.list_sources_value()
    }

    async fn focus_source(&self, key: &str) -> Result<bool, Self::Error> {
        Ok(self.sources().focus(key).await)
    }

    async fn set_player_active(&self, active: bool) -> OkResult {
        self.blobs().set_player_active(active);
        OkResult { ok: true }
    }

    async fn active_source_count(&self) -> usize {
        self.sources().active_count()
    }

    async fn overlay_topic_count(&self) -> usize {
        self.net().overlay_topic_count()
    }

    async fn discovered_sources(&self) -> Result<Vec<DiscoveredSource>, Self::Error> {
        Ok(self.catalog().discovered_sources().await)
    }

    async fn open_uri(&self, uri: &str, join_source: bool) -> Result<OpenUri, Self::Error> {
        Ok(self.catalog().open_uri(uri, join_source).await?)
    }

    async fn warm(&self, entry: &Value) -> Result<(), Self::Error> {
        let announce: Announce = serde_json::from_value(entry.clone())
            .or_else(|_| {
                entry
                    .get("entry")
                    .cloned()
                    .ok_or(serde_json::Error::io(std::io::Error::other("no entry")))
                    .and_then(serde_json::from_value)
            })
            .map_err(|error| NodeError(error.to_string()))?;
        self.blobs().warm(&announce).await?;
        Ok(())
    }

    async fn identity(&self, reveal_mnemonic: bool) -> Result<ApiIdentity, Self::Error> {
        let value = {
            let mut guard = self.inner.identity.lock().await;
            let identity = guard.as_mut().expect("identity constructed");
            let value = identity.identity(reveal_mnemonic).await?;
            self.inner.refresh_from(identity);
            value
        };
        serde_json::from_value(value).map_err(|error| NodeError(error.to_string()))
    }

    async fn grant_writer(
        &self,
        writer: &str,
        role: &str,
        alias: &str,
    ) -> Result<Grant, Self::Error> {
        let value = self.channel().grant_writer(writer, role, alias).await?;
        serde_json::from_value(value).map_err(|error| NodeError(error.to_string()))
    }

    async fn revoke_writer(
        &self,
        writer: &str,
        purge_entries: bool,
    ) -> Result<Revoke, Self::Error> {
        let value = self.channel().revoke_writer(writer, purge_entries).await?;
        serde_json::from_value(value).map_err(|error| NodeError(error.to_string()))
    }

    async fn claim_with_mnemonic(&self, mnemonic: &str, alias: &str) -> Result<Claim, Self::Error> {
        let value = {
            let mut guard = self.inner.identity.lock().await;
            let identity = guard.as_mut().expect("identity constructed");
            let value = identity.claim_with_mnemonic(mnemonic, alias).await?;
            self.inner.refresh_from(identity);
            value
        };
        serde_json::from_value(value).map_err(|error| NodeError(error.to_string()))
    }

    async fn rotate_epoch(&self) -> Result<Epoch, Self::Error> {
        let value = {
            let mut guard = self.inner.identity.lock().await;
            let identity = guard.as_mut().expect("identity constructed");
            let value = identity.rotate_epoch().await?;
            self.inner.refresh_from(identity);
            value
        };
        serde_json::from_value(value).map_err(|error| NodeError(error.to_string()))
    }

    async fn set_allowed_peers(&self, peers: Option<Vec<String>>) -> Result<(), Self::Error> {
        self.net().set_allowed_peers(peers);
        Ok(())
    }

    async fn clear_cache(
        &self,
        key: Option<&str>,
        keep_blobs_hex: Option<&str>,
    ) -> Result<CacheCleared, Self::Error> {
        let keep = keep_blobs_hex.and_then(parse_hex32);
        Ok(self.blobs().clear_cache(key, keep).await?)
    }

    async fn set_cache_max_bytes(&self, bytes: f64) -> u64 {
        self.blobs().set_cache_max_bytes(bytes)
    }

    async fn enforce_cache_ceiling(
        &self,
        keep_hex: Option<&str>,
    ) -> Result<CacheCeiling, Self::Error> {
        let keep = keep_hex.and_then(parse_hex32);
        Ok(self.blobs().enforce_cache_ceiling(keep).await)
    }

    async fn gc_storage(&self, full: bool, streaming: bool) -> Result<StorageGc, Self::Error> {
        Ok(self.blobs().gc_storage(full, streaming).await)
    }

    async fn status(&self) -> Status {
        self.status_value().await
    }

    async fn stream_url(&self, entry: &Announce) -> String {
        self.blobs().stream_url(entry)
    }

    async fn http_viewing(&self) -> bool {
        now_ms().saturating_sub(self.blobs().last_http_view_at()) < 3_000
    }

    async fn mark_bulk_peer(&self, key: &str, on: bool) {
        self.blobs().mark_bulk_peer(key, on);
    }

    async fn set_lan(&self, lan: Option<Self::Lan>) -> Result<(), Self::Error> {
        self.net().set_lan(lan).await?;
        Ok(())
    }

    async fn lan_stats(&self) -> Option<Value> {
        self.net().lan_stats()
    }

    async fn close(&self) -> Result<(), Self::Error> {
        CoreNode::close(self).await
    }
}

fn api_meta(meta: super::api::PutMeta) -> BlobPutMeta {
    meta
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn primary_catalog_cache_tracks_applied_heads() {
        let dir = tempfile::tempdir().unwrap();
        let node = create_node(json!({
            "storage": dir.path(), "bootstrap": ["127.0.0.1:9"], "worklet": true
        }))
        .await
        .unwrap();
        let announce = |title: &str| {
            json!({
                "type": "announce", "id": "imdb:tt0000001", "title": title, "size": 1,
                "sha256": "33".repeat(32), "blobs": "22".repeat(32),
                "blob": { "blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 1 }
            })
        };
        node.append(announce("Original")).await.unwrap();
        let options = SearchOptions::default();
        let first = node.search(None, options.clone()).await.unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].title, "Original");
        assert_eq!(
            node.search(Some("imdb:tt0000001"), options.clone())
                .await
                .unwrap(),
            first
        );
        node.append(announce("Changed")).await.unwrap();
        let changed = node
            .search(Some("imdb:tt0000001"), options.clone())
            .await
            .unwrap();
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].title, "Changed");
        node.remove(&changed[0].key).await.unwrap();
        assert!(node.search(None, options).await.unwrap().is_empty());
        node.close().await.unwrap();
    }

    #[tokio::test]
    async fn awake_source_does_not_resurrect_deleted_snapshot_entries() {
        let dir = tempfile::tempdir().unwrap();
        let source = "11".repeat(32);
        let blobs = "22".repeat(32);
        let announce = json!({
            "type": "announce", "id": "deleted", "title": "Deleted fixture",
            "size": 1, "sha256": "33".repeat(32), "blobs": blobs,
            "blob": { "blockOffset": 0, "blockLength": 1, "byteOffset": 0, "byteLength": 1 }
        });
        std::fs::write(
            dir.path().join("catalog-cache.json"),
            serde_json::to_vec(&json!({
                "version": 1,
                "channels": {
                    (source.clone()): {
                        "entries": [
                            [format!("deleted/{source}/{blobs}:1"), announce],
                            [format!("deleted/{source}/{blobs}:0"), announce]
                        ],
                        "discovered": []
                    }
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let node = create_node(json!({
            "storage": dir.path(),
            "sources": [source],
            "bootstrap": ["127.0.0.1:9"],
            "worklet": true
        }))
        .await
        .unwrap();
        let record = node.sources().get(&source).unwrap();
        assert!(record.active());
        assert_eq!(record.entries().len(), 2, "keep the hibernation snapshot");
        assert!(
            node.search(None, SearchOptions::default())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            node.search(Some("deleted"), SearchOptions::default())
                .await
                .unwrap()
                .is_empty()
        );

        node.sources().hibernate(&record).await;
        let entries = node.search(None, SearchOptions::default()).await.unwrap();
        assert_eq!(
            entries.len(),
            1,
            "sleeping sources still serve their snapshot"
        );
        assert!(entries[0].key.ends_with(":0"), "JS deduplication ranking");
        node.close().await.unwrap();
    }
}
