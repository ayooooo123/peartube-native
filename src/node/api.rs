use std::future::Future;
use std::pin::Pin;

use futures_util::future::BoxFuture;

use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};

fn present_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Route {
    pub peers: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub down_bps: Number,
    pub up_bps: Number,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Routes {
    pub private: Route,
    pub public: Route,
    pub lan: Route,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Reciprocity {
    pub tier: String,
    pub seeded_bytes: u64,
    pub contributing_peers: u64,
    pub ratio: Number,
    pub inflight_max: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub tracker: String,
    pub channel: String,
    pub writer: String,
    pub writable: bool,
    pub blobs: String,
    pub blob_bytes: u64,
    pub cached_bytes: u64,
    pub cache_max_bytes: u64,
    pub peers: u64,
    pub lan_peers: u64,
    pub down_bps: Number,
    pub up_bps: Number,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub routes: Routes,
    pub inflight_blocks: u64,
    pub rtt: Number,
    pub retransmits: u64,
    pub sources: Vec<Source>,
    pub reciprocity: Reciprocity,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    pub key: String,
    pub kind: String,
    pub alias: String,
    pub primary: bool,
    pub writable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub entries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hydrated: Option<bool>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FollowedSource {
    pub key: String,
    pub kind: String,
    pub alias: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredSource {
    pub key: String,
    pub kind: String,
    pub title: String,
    pub via: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedSource {
    pub key: String,
    pub kind: String,
    pub title: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Blob {
    pub block_offset: u64,
    pub block_length: u64,
    pub byte_offset: u64,
    pub byte_length: u64,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Announce {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    pub title: String,
    pub size: u64,
    pub sha256: String,
    pub blobs: String,
    pub blob: Blob,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wrapped_key: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Entry {
    pub key: String,
    pub id: String,
    pub title: String,
    pub size: u64,
    pub sha256: String,
    pub local: bool,
    pub cached_bytes: u64,
    #[serde(rename = "private")]
    pub private_entry: bool,
    pub epoch: u64,
    pub locked: bool,
    pub source: String,
    pub source_kind: String,
    pub season: Option<u64>,
    pub episode: Option<u64>,
    pub series_key: String,
    pub base_title: String,
    pub year: Option<String>,
    pub stream_url: String,
    pub magnet_url: String,
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SearchOptions {
    #[serde(
        default,
        deserialize_with = "present_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub source: Option<Option<String>>,
    #[serde(
        default,
        deserialize_with = "present_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub query: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<Number>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<Number>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SourceSpec {
    Uri(String),
    Object(SourceObject),
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SourceObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alias: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Removed {
    pub removed: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OkResult {
    pub ok: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeedableCore {
    pub key: String,
    pub kind: String,
    pub length: u64,
    pub byte_length: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LanEndpoint {
    pub host: String,
    pub port: u16,
    pub public_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Writer {
    pub key: String,
    pub role: String,
    pub alias: String,
    #[serde(rename = "self")]
    pub is_self: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Identity {
    pub channel: String,
    pub writer: String,
    pub role: Option<String>,
    pub writable: bool,
    pub mnemonic_set: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mnemonic: Option<String>,
    pub writers: Vec<Writer>,
    pub epoch: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_uri: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    pub granted: bool,
    pub writer: String,
    pub role: String,
    pub alias: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Revoke {
    pub revoked: bool,
    pub writer: String,
    pub purge_entries: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claim {
    pub claimed: bool,
    pub reopen_needed: bool,
    pub channel: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Epoch {
    pub epoch: u64,
    pub epoch_key: String,
    pub access_uri: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCleared {
    pub cleared_bytes: u64,
    pub reclaimed_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheCeiling {
    pub evicted: u64,
    pub cached_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageGc {
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub reclaimed_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncResult {
    pub backfilled: u64,
    pub total: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RechunkResult {
    pub total: u64,
    pub migrated: u64,
    pub deduped: u64,
    pub remaining: u64,
    pub migrated_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PutMeta {
    pub id: String,
    pub title: String,
    #[serde(rename = "private", skip_serializing_if = "Option::is_none")]
    pub private_entry: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<String>,
    #[serde(
        default,
        deserialize_with = "present_nullable",
        skip_serializing_if = "Option::is_none"
    )]
    pub expected_size: Option<Option<u64>>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Published {
    #[serde(flatten)]
    pub announce: Announce,
    pub key: String,
    pub channel: String,
    pub stream_url: String,
    pub magnet_url: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum OpenUri {
    Source {
        source: Source,
    },
    Access {
        channel: String,
        epoch: u64,
        source: Source,
    },
    Watch {
        entry: Entry,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastStatus {
    pub web_receiver_url: Option<String>,
    pub receivers: Vec<Value>,
    pub session: Option<Value>,
    pub receiver_mode: Option<Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReceiverCommand {
    pub seq: Value,
    pub action: String,
    #[serde(flatten)]
    pub fields: serde_json::Map<String, Value>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event")]
pub enum WorkerEvent {
    #[serde(rename = "update")]
    Update,
    #[serde(rename = "sources")]
    Sources { sources: Vec<FollowedSource> },
    #[serde(rename = "cast")]
    Cast { cast: CastStatus },
    #[serde(rename = "receiverCommand")]
    ReceiverCommand { command: ReceiverCommand },
    #[serde(rename = "fatal")]
    Fatal { error: String },
}

/// Creates a node from the options passed to `createNode`.
#[allow(async_fn_in_trait)]
pub trait NodeFactory: Send + Sync {
    type Node: crate::node::Node;
    type Error: std::error::Error + Send + Sync + 'static;
    fn create(
        &self,
        options: Value,
    ) -> impl Future<Output = Result<std::sync::Arc<Self::Node>, Self::Error>> + Send;
}

/// The lifecycle operations required from a node's native swarm handle.
#[allow(async_fn_in_trait)]
pub trait Swarm: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    fn suspend(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn resume(&self) -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Registers the tracker/peer-set change callback (`swarm.on('update')`); the returned
    /// closure removes it.
    fn on_update(
        &self,
        callback: std::sync::Arc<dyn Fn() + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync>;
}

/// Supplies LAN bindings and forwards host Bonjour events.
#[allow(async_fn_in_trait)]
pub trait LanFactory<N: crate::node::Node>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    fn interfaces(
        &self,
    ) -> impl Future<Output = Result<Vec<(String, std::net::IpAddr, bool)>, Self::Error>> + Send;
    fn create(
        &self,
        host: std::net::Ipv4Addr,
        node: &N,
    ) -> impl Future<Output = Result<N::Lan, Self::Error>> + Send;
    fn host_event(&self, event: Value) -> impl Future<Output = ()> + Send;
}

/// The loopback HTTP service used by the worker.
#[allow(async_fn_in_trait)]
pub trait ApiServer<N: crate::node::Node>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    fn start(
        &self,
        node: std::sync::Arc<N>,
        on_sources: std::sync::Arc<dyn Fn(Vec<Source>) + Send + Sync>,
    ) -> impl Future<Output = Result<Option<String>, Self::Error>> + Send;
    /// A notification only queues an event for the API's stream, so it is synchronous.
    fn notify(&self, event: &str);
    fn close(&self) -> impl Future<Output = ()> + Send;
}

/// Monotonic milliseconds: the worker's only clock, so its timers can be driven by a host's.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
    /// Resolves once the clock is at or past `deadline_ms`.
    fn wait_until(&self, deadline_ms: u64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
}

/// `createCastController`'s argument object, minus what the worker owns.
pub struct CastOptions<N: crate::node::Node> {
    pub storage: String,
    /// `getNode`: the node a start opened, if any.
    pub node: std::sync::Arc<dyn Fn() -> Option<std::sync::Arc<N>> + Send + Sync>,
    /// `getLanHost`: the device address last sampled while discovering on the LAN.
    pub get_lan_host: std::sync::Arc<dyn Fn() -> Option<String> + Send + Sync>,
    /// `onchange`: a cast state change the worker should broadcast.
    pub on_change: std::sync::Arc<dyn Fn() + Send + Sync>,
    pub worklet: bool,
}

/// `startLocalReceiver`'s argument object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiverStart {
    pub name: String,
    pub open_auth: bool,
    pub peer_storage: Option<String>,
}

/// The cast controller and TV receiver, as the worker drives them (`src/cast.js`).
pub trait Cast: Send + Sync {
    fn init_hub(&self) -> BoxFuture<'_, ()>;
    fn set_lan_host(&self, host: Option<String>) -> BoxFuture<'_, ()>;
    fn status(&self) -> Value;
    fn pair_receiver<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>>;
    fn remove_receiver(&self, id: &str) -> Value;
    fn cast_play<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>>;
    fn cast_attach<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>>;
    fn cast_control<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>>;
    fn cast_stop<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>>;
    fn start_local_receiver(
        &self,
        options: ReceiverStart,
        on_command: std::sync::Arc<dyn Fn(Value) + Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, String>>;
    fn report_local_receiver_state<'a>(&'a self, patch: &'a Value) -> BoxFuture<'a, Option<Value>>;
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// `createCastController` plus the receiver-storage helpers beside it in `src/cast.js`.
#[allow(async_fn_in_trait)]
pub trait CastFactory<N: crate::node::Node>: Send + Sync {
    fn create(
        &self,
        options: CastOptions<N>,
    ) -> impl Future<Output = Result<std::sync::Arc<dyn Cast>, String>> + Send;
    /// `resolvePeerStorage`: the receiver's own node folder, probed and created if missing.
    fn peer_storage(
        &self,
        candidate: Option<&str>,
        fallback: &str,
    ) -> impl Future<Output = String> + Send;
    /// `reclaimLowDiskReceiverStorage`: purges stale Corestores when the disk is nearly full.
    fn reclaim(&self, storage: &str);
}
