//! The narrow seams the host fills: the P2P node a receiver or controller drives, one peer
//! connection (the Protomux mux the cast channel opens on), and mDNS. Nothing in the cast port
//! opens a socket of its own besides the receiver's loopback player port.

use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use hyperblobs::BlobId;
use hypercore::Core;
use protomux::Mux;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::CastError;
use super::state::CastState;

/// One connection to a peer, as `node.peerConnections()` / `node.onConnection()` hand it out:
/// its Protomux mux (the cast channel opens on it), whether it came over the LAN, and its close.
pub trait PeerConnection: Send + Sync {
    /// Unique among live connections; the receiver keys its per-connection state on it.
    fn id(&self) -> u64;
    /// 64-hex Noise public key of the peer.
    fn remote_public_key(&self) -> String;
    /// `node.isLanConnection(conn)`.
    fn is_lan(&self) -> bool;
    fn destroyed(&self) -> bool;
    /// `Protomux.from(conn)`.
    fn mux(&self) -> Mux;
    /// `conn.once('close')`.
    fn closed(&self) -> BoxFuture<'static, ()>;
    fn close(&self);
}

/// A connection the cast can open its channel on.
pub type PeerConn = Arc<dyn PeerConnection>;

/// Where a receiver's node answers on this LAN, mDNS `h` + `lp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LanAddress {
    pub host: String,
    pub port: u16,
}

/// A node's LAN endpoint, as `node.lanEndpoint()` answers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LanEndpoint {
    pub host: String,
    pub port: u16,
    pub public_key: String,
}

/// `peerNode.status()`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PeerStatus {
    /// Non-LAN peers; callers add `lan_peers` once, as in JS.
    pub peers: u64,
    pub lan_peers: u64,
    /// `status().routes`, echoed in `/cast/v1/info` when present.
    #[serde(skip)]
    pub routes: Option<Value>,
}

impl PeerStatus {
    pub(crate) fn from_routes(routes: crate::node::api::Routes) -> Self {
        Self {
            peers: routes.public.peers + routes.private.peers,
            lan_peers: routes.lan.peers,
            routes: serde_json::to_value(routes).ok(),
        }
    }
}

/// What `openUri` answers with.
#[derive(Debug, Clone, Default)]
pub struct OpenedMedia {
    /// `source`, `access` or `watch`, as `parseUri` classifies the link.
    pub action: String,
    /// The node's own blob-server link for the video.
    pub stream_url: Option<String>,
    pub entry: Value,
    pub blob: Option<BlobId>,
}

/// Per-load cache trimming, as the receiver's read-ahead asks for it.
#[derive(Debug, Clone, Copy)]
pub struct EvictOptions {
    pub playhead_byte: u64,
    pub keep_behind_bytes: u64,
    pub keep_ahead_bytes: Option<u64>,
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
    fn memory_window_budget(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        None
    }
    /// 64-hex Noise public key (`peerKey`).
    fn peer_key(&self) -> String;
    fn private_routing(&self) -> bool;
    fn has_lan(&self) -> bool;
    fn lan_endpoint(&self) -> Option<LanEndpoint>;
    fn status(&self) -> PeerStatus;
    /// `node.joinLanTopic('cast', hash('peartube-cast/v1'))`: peers on this LAN connect here.
    fn join_lan_topic(&self, id: &str, topic: [u8; 32]);
    /// Every live connection, now.
    fn peer_connections(&self) -> Vec<PeerConn>;
    /// `node.onConnection(serve)`; the returned closure unregisters it.
    fn on_connection(
        &self,
        callback: Arc<dyn Fn(PeerConn) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync>;
    /// `node.connectPeer(key, { lan })`: the open connection to that peer, else a LAN dial and a DHT join.
    fn connect_peer(
        &self,
        key: &str,
        lan: Option<LanAddress>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<PeerConn, CastError>>;
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

/// Called by a receiver for every command it accepts; `update_state` is the state the player
/// should report back through `reportState`.
pub type CommandSink = Arc<dyn Fn(Value) + Send + Sync>;
/// Called whenever the receiver's visible status changed.
pub type ChangeSink = Arc<dyn Fn(&CastState) + Send + Sync>;

#[cfg(test)]
mod peer_status_tests {
    use super::*;

    #[test]
    fn lan_peers_are_counted_once() {
        let route = |peers| crate::node::api::Route {
            peers,
            rx_bytes: 0,
            tx_bytes: 0,
            down_bps: 0.into(),
            up_bps: 0.into(),
        };
        let status = PeerStatus::from_routes(crate::node::api::Routes {
            public: route(2),
            private: route(1),
            lan: route(3),
        });
        assert_eq!(status.peers, 3);
        assert_eq!(status.lan_peers, 3);
        assert_eq!(status.peers + status.lan_peers, 6);
    }
}
