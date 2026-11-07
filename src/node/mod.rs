pub mod api;

use api::*;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use tokio::io::AsyncRead;
use tokio::sync::watch;

/// Input may resume at the offset supplied by the backend and update expected size.
pub trait PutSource: Send {
    type Error: std::error::Error + Send + Sync + 'static;
    fn open(&mut self, offset: u64, meta: &mut PutMeta) -> impl Future<Output = Result<Pin<Box<dyn AsyncRead + Send>>, Self::Error>> + Send;
    fn on_data(&mut self, bytes: u64);
    fn on_hash_state(&mut self, state: &[u8], bytes: u64);
}

/// Application-facing operations and native handles returned by `createNode`.
/// Native handles and streams are deliberately not JSON values.
pub trait Node: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Store: Send + Sync;
    type Core: Send + Sync;
    type Channel: Send + Sync;
    /// Suspends and resumes the native swarm; see [`api::Swarm`].
    type Swarm: api::Swarm;
    type Dht: Send + Sync;
    type Connection: Send + Sync;
    type ConnectionSubscription: Send;
    type Lan: Send;
    type BlobServer: Send + Sync;
    /// A stream or resumable stream factory, including its progress/hash callbacks.
    type PutSource: PutSource<Error = Self::Error>;
    /// Rechunk callbacks and options, including non-JSON callback functions.
    type RechunkOptions: Send;

    fn store(&self) -> &Self::Store;
    fn channel(&self) -> &Self::Channel;
    fn swarm(&self) -> &Self::Swarm;
    fn dht(&self) -> &Self::Dht;
    fn blob_server(&self) -> &Self::BlobServer;
    fn private_routing(&self) -> bool;
    fn bootstrap(&self) -> Option<Value>;
    fn relay_through(&self) -> Option<Value>;
    fn peer_key(&self) -> String;
    fn closing(&self) -> bool;
    fn peer_connections(&self) -> Vec<Self::Connection>;
    fn on_connection(&self, callback: Box<dyn Fn(Self::Connection) + Send + Sync>) -> Self::ConnectionSubscription;
    fn onchange(&self) -> watch::Receiver<u64>;

    fn find_entry(&self, key: &str) -> impl Future<Output = Result<Option<Value>, Self::Error>> + Send;
    fn blob_core(&self, key: &str) -> impl Future<Output = Result<Self::Core, Self::Error>> + Send;
    fn seedable_cores(&self, include_cached: bool) -> impl Future<Output = Result<Vec<SeedableCore>, Self::Error>> + Send;
    fn has_lan(&self) -> impl Future<Output = bool> + Send;
    fn lan_endpoint(&self) -> impl Future<Output = Option<LanEndpoint>> + Send;
    fn dial_lan_peer(&self, endpoint: LanEndpoint) -> impl Future<Output = ()> + Send;
    fn join_peer(&self, key: &str, lan_first: bool) -> impl Future<Output = ()> + Send;
    fn evict_behind(&self, blobs: &str, blob: Blob, options: Value) -> impl Future<Output = Result<u64, Self::Error>> + Send;
    fn refresh_block_count(&self, core: &Self::Core) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn search(&self, id: Option<&str>, options: SearchOptions) -> impl Future<Output = Result<Vec<Entry>, Self::Error>> + Send;
    fn put(&self, meta: PutMeta, source: Self::PutSource) -> impl Future<Output = Result<Announce, Self::Error>> + Send;
    fn drop_store(&self, name: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn append(&self, op: Value) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn publish(&self, meta: PutMeta, source: Self::PutSource) -> impl Future<Output = Result<Published, Self::Error>> + Send;
    fn publish_channel(&self, meta_or_op: Value, source: Option<Self::PutSource>) -> impl Future<Output = Result<Published, Self::Error>> + Send;
    fn remove(&self, key: &str) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn publish_source(&self, source_key: &str, title: &str) -> impl Future<Output = Result<Value, Self::Error>> + Send;
    fn sync_local_channel(&self) -> impl Future<Output = Result<SyncResult, Self::Error>> + Send;
    fn rechunk_legacy_blobs(&self, options: Self::RechunkOptions) -> impl Future<Output = Result<RechunkResult, Self::Error>> + Send;
    fn unpublish_source(&self, key: &str) -> impl Future<Output = Result<Value, Self::Error>> + Send;
    fn published_sources(&self) -> impl Future<Output = Result<Vec<PublishedSource>, Self::Error>> + Send;
    fn add_source(&self, spec: SourceSpec, wake: bool) -> impl Future<Output = Result<Source, Self::Error>> + Send;
    fn remove_source(&self, key: &str) -> impl Future<Output = Result<Removed, Self::Error>> + Send;
    fn list_sources(&self) -> impl Future<Output = Vec<Source>> + Send;
    fn focus_source(&self, key: &str) -> impl Future<Output = Result<bool, Self::Error>> + Send;
    fn set_player_active(&self, active: bool) -> impl Future<Output = OkResult> + Send;
    fn active_source_count(&self) -> impl Future<Output = usize> + Send;
    fn overlay_topic_count(&self) -> impl Future<Output = usize> + Send;
    fn discovered_sources(&self) -> impl Future<Output = Result<Vec<DiscoveredSource>, Self::Error>> + Send;
    fn open_uri(&self, uri: &str, join_source: bool) -> impl Future<Output = Result<OpenUri, Self::Error>> + Send;
    fn warm(&self, entry: &Value) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn identity(&self, reveal_mnemonic: bool) -> impl Future<Output = Result<Identity, Self::Error>> + Send;
    fn grant_writer(&self, writer: &str, role: &str, alias: &str) -> impl Future<Output = Result<Grant, Self::Error>> + Send;
    fn revoke_writer(&self, writer: &str, purge_entries: bool) -> impl Future<Output = Result<Revoke, Self::Error>> + Send;
    fn claim_with_mnemonic(&self, mnemonic: &str, alias: &str) -> impl Future<Output = Result<Claim, Self::Error>> + Send;
    fn rotate_epoch(&self) -> impl Future<Output = Result<Epoch, Self::Error>> + Send;
    fn set_allowed_peers(&self, peers: Option<Vec<String>>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn clear_cache(&self, key: Option<&str>, keep_blobs_hex: Option<&str>) -> impl Future<Output = Result<CacheCleared, Self::Error>> + Send;
    fn set_cache_max_bytes(&self, bytes: f64) -> impl Future<Output = u64> + Send;
    fn enforce_cache_ceiling(&self, keep_hex: Option<&str>) -> impl Future<Output = Result<CacheCeiling, Self::Error>> + Send;
    fn gc_storage(&self, full: bool, streaming: bool) -> impl Future<Output = Result<StorageGc, Self::Error>> + Send;
    fn status(&self) -> impl Future<Output = Status> + Send;
    fn stream_url(&self, entry: &Announce) -> impl Future<Output = String> + Send;
    fn http_viewing(&self) -> impl Future<Output = bool> + Send;
    fn mark_bulk_peer(&self, key: &str, on: bool) -> impl Future<Output = ()> + Send;
    fn set_lan(&self, lan: Option<Self::Lan>) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn lan_stats(&self) -> impl Future<Output = Option<Value>> + Send;
    fn close(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub mod blobs;
pub mod catalog;
pub mod channel;
pub mod core;
pub mod factory;
pub mod network;
pub mod sources;
pub mod storage;

pub mod apply;
