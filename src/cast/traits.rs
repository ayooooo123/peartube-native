//! The narrow seams the app integration fills: the P2P node, the DHT, the LAN blob server and
//! the mDNS adapter. Nothing in the cast port opens sockets for P2P itself.

use std::sync::Arc;

use futures_util::future::BoxFuture;
use hyperblobs::BlobId;
use hypercore::Core;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};

use super::CastError;
use super::state::CastState;

/// A bidirectional byte pipe with the DHT handshake already done (a secret stream).
pub trait Duplex: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Duplex for T {}

/// Why a P2P dial failed; `code` is matched against the app's retryable set.
#[derive(Debug, Clone)]
pub struct DhtError {
    pub code: Option<String>,
    pub message: String,
}

impl DhtError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: Some(code.into()),
            message: message.into(),
        }
    }

    pub fn other(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for DhtError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DhtError {}

/// `dht.connect(publicKey, { relayThrough })`.
pub trait DhtConnect: Send + Sync {
    fn connect(
        &self,
        public_key: [u8; 32],
        relay_through: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<Box<dyn Duplex>, DhtError>>;
}

/// One accepted Noise connection.
pub type NoiseHandler = Arc<dyn Fn(Box<dyn Duplex>) + Send + Sync>;

/// `dht.createServer(onConnection)` plus `server.listen(keyPair)`.
pub trait DhtServer: Send + Sync {
    fn serve(
        &self,
        public_key: [u8; 32],
        handler: NoiseHandler,
    ) -> BoxFuture<'_, Result<(), DhtError>>;
    /// Stops accepting dials (the receiver's `noiseServer.close()`).
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// `peerNode.status()`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PeerStatus {
    pub peers: u64,
    pub lan_peers: u64,
    /// `status().routes`, echoed in `/cast/v1/info` when present.
    #[serde(skip)]
    pub routes: Option<Value>,
}

/// What `openUri` answers with.
#[derive(Debug, Clone, Default)]
pub struct OpenedMedia {
    /// The node's own blob-server link for the video.
    pub stream_url: Option<String>,
    pub entry: Value,
    pub blob: Option<BlobId>,
}

/// `network` in a load command, defaults resolved.
#[derive(Debug, Clone, Default)]
pub struct NetworkConfig {
    pub private_routing: bool,
    pub bootstrap: Option<Value>,
    pub relay_through: Option<Value>,
    pub lan: bool,
    pub source_key: String,
    pub source_kind: String,
}

/// Per-load cache trimming, as the receiver's read-ahead asks for it.
#[derive(Debug, Clone, Copy)]
pub struct EvictOptions {
    pub playhead_byte: u64,
    pub keep_behind_bytes: u64,
    pub max_bytes: u64,
}

/// `clearCache(null, { keepBlobsHex })`; `None` clears everything.
#[derive(Debug, Clone, Default)]
pub struct ClearCacheOptions {
    pub keep_blobs_hex: Option<String>,
}

/// The P2P node a receiver or controller drives, as the cast stack uses it.
pub trait CastPeer: Send + Sync {
    fn memory_cache_bytes(&self) -> Option<u64> {
        None
    }
    /// 64-hex Noise public key (`peerKey`).
    fn peer_key(&self) -> String;
    fn private_routing(&self) -> bool;
    fn has_lan(&self) -> bool;
    fn lan_endpoint(&self) -> Option<Value>;
    fn bootstrap(&self) -> Option<Value>;
    fn relay_through(&self) -> Option<Value>;
    fn status(&self) -> PeerStatus;
    fn join_peer(&self, key: &str, lan_first: bool);
    fn dial_lan_peer(&self, endpoint: &Value);
    fn open_uri(
        &self,
        uri: &str,
        join_source: bool,
    ) -> BoxFuture<'_, Result<Option<OpenedMedia>, CastError>>;
    fn blob_core(&self, blobs_hex: &str) -> BoxFuture<'_, Result<Option<Core>, CastError>>;
    fn clear_cache(&self, opts: ClearCacheOptions) -> BoxFuture<'_, ()>;
    fn evict_behind(
        &self,
        core_key_hex: &str,
        blob: BlobId,
        opts: EvictOptions,
    ) -> BoxFuture<'_, ()>;
    /// One entry by key, as `node.findEntry` answers (the raw announcement).
    fn find_entry(&self, key: &str) -> BoxFuture<'_, Option<Value>>;
    /// `node.search(id)`, the UI entry list.
    fn search(&self, id: &str) -> BoxFuture<'_, Vec<Value>>;
    /// `node.warm(raw)`: start replicating the video's blocks from its raw announcement.
    fn warm(&self, raw: &Value) -> BoxFuture<'_, ()>;
    /// `node.close()`.
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// A LAN blob server, as `createBlobRangeServer` hands back.
pub trait LanBlobServer: Send + Sync {
    /// Allow (or keep allowing) one key through the resolver.
    fn allow(&self, blobs_hex: &str);
    /// `getLink(key, { host, blob, type })`.
    fn get_link(&self, blobs_hex: &str, host: &str, blob: &BlobId, mime: &str) -> String;
    fn close(&self) -> BoxFuture<'_, ()>;
}

/// Binds the sender's token-gated LAN Range server on first use.
pub trait BlobServerFactory: Send + Sync {
    fn ensure(
        &self,
        lan_host: Option<&str>,
    ) -> BoxFuture<'_, Result<Arc<dyn LanBlobServer>, CastError>>;
}

/// Opens (or reuses) the receiver's own P2P node for a load's network configuration.
pub trait PeerFactory: Send + Sync {
    fn ensure(
        &self,
        network: NetworkConfig,
        lan_host: Option<&str>,
    ) -> BoxFuture<'_, Result<Option<Arc<dyn CastPeer>>, CastError>>;
}

/// Called by a receiver for every command it accepts; `update_state` is the state the player
/// should report back through `reportState`.
pub type CommandSink = Arc<dyn Fn(Value) + Send + Sync>;
/// Called whenever the receiver's visible status changed.
pub type ChangeSink = Arc<dyn Fn(&CastState) + Send + Sync>;
