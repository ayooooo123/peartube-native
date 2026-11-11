//! The real adapters behind [`crate::node::api::CastFactory`]: the cast stack's seams filled by
//! `CoreNode` and its Protomux connections, as `worker.js` fills them from the app's node.
//!
//! - [`NodePeer`] is the [`CastPeer`] a receiver or controller drives: the node's catalog, blob
//!   cores, connection set, LAN topic and cache trimming.
//! - [`Conn`] is one peer connection as the cast channel opens on it.
//! - [`Service`] is the worker's [`crate::node::api::Cast`]: the controller plus the TV receiver.
//! - [`Factory`] is [`crate::node::api::CastFactory`], with the receiver-storage helpers beside
//!   `createCastController`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use hyperblobs::BlobId;
use protomux::Mux;
use serde_json::{Map, Value, json};

use crate::node::Node;
use crate::node::api::{
    self, Blob, Cast as CastApi, CastFactory, CastOptions, OpenUri, ReceiverStart, SearchOptions,
};
use crate::node::core::CoreNode;
use crate::node::network::ConnectionHandle;
use crate::protocol::RECEIVER_PORT;

use super::CastError;
use super::controller::{CastController, ControllerOptions};
use super::disk;
use super::receiver::ReceiverOptions;
use super::traits::{
    CastPeer, ClearCacheOptions, EvictOptions, LanAddress, LanEndpoint, OpenedMedia, PeerConn,
    PeerConnection, PeerStatus,
};

type NodeGetter = Arc<dyn Fn() -> Option<Arc<CoreNode>> + Send + Sync>;

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    let text = text.trim();
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(out)
}

// ---------------------------------------------------------------------------------------------
// PeerConnection over the node's Protomux connections

/// One connection to a peer, as `node.onConnection()` hands it out.
pub struct Conn {
    handle: ConnectionHandle,
}

impl Conn {
    fn new(handle: ConnectionHandle) -> Self {
        Self { handle }
    }
}

impl PeerConnection for Conn {
    fn id(&self) -> u64 {
        crate::traffic::Connection::id(&self.handle)
    }

    fn remote_public_key(&self) -> String {
        self.handle.remote_public_key()
    }

    fn is_lan(&self) -> bool {
        self.handle.is_lan()
    }

    fn destroyed(&self) -> bool {
        self.handle.destroyed()
    }

    fn mux(&self) -> Mux {
        self.handle.mux().clone()
    }

    fn closed(&self) -> BoxFuture<'static, ()> {
        let mux = self.handle.mux().clone();
        Box::pin(async move { mux.closed().await })
    }

    fn close(&self) {
        self.handle.close();
    }
}

// ---------------------------------------------------------------------------------------------
// CastPeer over CoreNode

/// The node a receiver or controller drives, as [`CastPeer`] sees it.
pub struct NodePeer {
    node: Arc<CoreNode>,
}

impl NodePeer {
    pub fn new(node: Arc<CoreNode>) -> Self {
        Self { node }
    }

    pub fn node(&self) -> &Arc<CoreNode> {
        &self.node
    }
}

impl CastPeer for NodePeer {
    fn memory_cache_bytes(&self) -> Option<u64> {
        self.node.memory_cache_bytes()
    }
    fn memory_window_budget(&self) -> Option<Arc<tokio::sync::Semaphore>> {
        self.node.memory_window_budget()
    }
    fn peer_key(&self) -> String {
        self.node.peer_key()
    }
    fn private_routing(&self) -> bool {
        self.node.private_routing()
    }
    fn has_lan(&self) -> bool {
        self.node.net().has_lan()
    }
    fn lan_endpoint(&self) -> Option<LanEndpoint> {
        let endpoint = self.node.net().lan_endpoint()?;
        Some(LanEndpoint {
            host: endpoint.host,
            port: endpoint.port,
            public_key: endpoint.public_key,
        })
    }
    fn status(&self) -> PeerStatus {
        PeerStatus::from_routes(self.node.net().sample().routes)
    }
    fn join_lan_topic(&self, id: &str, topic: [u8; 32]) {
        self.node.net().join_lan_topic(id, topic);
    }
    fn peer_connections(&self) -> Vec<PeerConn> {
        self.node
            .net()
            .connections()
            .into_iter()
            .map(|handle| Arc::new(Conn::new(handle)) as PeerConn)
            .collect()
    }
    fn on_connection(
        &self,
        callback: Arc<dyn Fn(PeerConn) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        self.node.net().on_connection(Arc::new(move |handle| {
            callback(Arc::new(Conn::new(handle)));
        }))
    }
    fn connect_peer(
        &self,
        key: &str,
        lan: Option<LanAddress>,
        timeout: Duration,
    ) -> BoxFuture<'_, Result<PeerConn, CastError>> {
        let node = Arc::clone(&self.node);
        let key = key.to_lowercase();
        Box::pin(async move {
            if parse_hex32(&key).is_none() {
                return Err(CastError::new("A peer key is 64 hex characters", 0));
            }
            let net = node.net();
            for handle in net.connections() {
                if !handle.destroyed() && handle.remote_public_key() == key {
                    return Ok(Arc::new(Conn::new(handle)) as PeerConn);
                }
            }
            let (tx, rx) = tokio::sync::oneshot::channel::<ConnectionHandle>();
            let tx = Arc::new(Mutex::new(Some(tx)));
            let wanted = key.clone();
            let sender = Arc::clone(&tx);
            let unlisten = net.on_connection(Arc::new(move |handle| {
                if handle.destroyed() || handle.remote_public_key() != wanted {
                    return;
                }
                if let Ok(mut slot) = sender.lock()
                    && let Some(tx) = slot.take()
                {
                    let _ = tx.send(handle);
                }
            }));
            if let Some(lan) = lan.clone() {
                net.dial_lan_peer(&api::LanEndpoint {
                    host: lan.host,
                    port: lan.port,
                    public_key: key.clone(),
                });
            }
            net.join_peer(&key, lan.is_some());
            let result = tokio::time::timeout(timeout, rx).await;
            unlisten();
            match result {
                Ok(Ok(handle)) => Ok(Arc::new(Conn::new(handle)) as PeerConn),
                _ => Err(CastError::new("The peer did not connect", 0)),
            }
        })
    }
    fn open_uri(
        &self,
        uri: &str,
        join_source: bool,
    ) -> BoxFuture<'_, Result<Option<OpenedMedia>, CastError>> {
        let node = Arc::clone(&self.node);
        let uri = uri.to_owned();
        Box::pin(async move {
            match node.open_uri(&uri, join_source).await {
                Ok(OpenUri::Watch { entry }) => {
                    let stream_url = if entry.stream_url.is_empty() {
                        None
                    } else {
                        Some(entry.stream_url.clone())
                    };
                    Ok(Some(OpenedMedia {
                        action: "watch".to_owned(),
                        stream_url,
                        entry: serde_json::to_value(entry)
                            .map_err(|error| CastError::new(error.to_string(), 500))?,
                        blob: None,
                    }))
                }
                Ok(OpenUri::Source { source }) => Ok(Some(OpenedMedia {
                    action: "source".to_owned(),
                    entry: serde_json::to_value(source)
                        .map_err(|error| CastError::new(error.to_string(), 500))?,
                    ..OpenedMedia::default()
                })),
                Ok(OpenUri::Access { source, .. }) => Ok(Some(OpenedMedia {
                    action: "access".to_owned(),
                    entry: serde_json::to_value(source)
                        .map_err(|error| CastError::new(error.to_string(), 500))?,
                    ..OpenedMedia::default()
                })),
                Err(error) => Err(CastError::new(error.to_string(), 500)),
            }
        })
    }
    fn blob_core(
        &self,
        blobs_hex: &str,
    ) -> BoxFuture<'_, Result<Option<hypercore::Core>, CastError>> {
        let node = Arc::clone(&self.node);
        let blobs_hex = blobs_hex.to_owned();
        Box::pin(async move {
            let session = node
                .blob_core(&blobs_hex)
                .await
                .map_err(|error| CastError::new(error.to_string(), 500))?;
            Ok(Some(session.core().clone()))
        })
    }
    fn clear_cache(&self, opts: ClearCacheOptions) -> BoxFuture<'_, ()> {
        let node = Arc::clone(&self.node);
        Box::pin(async move {
            let _ = node.clear_cache(None, opts.keep_blobs_hex.as_deref()).await;
        })
    }
    fn evict_behind(
        &self,
        core_key_hex: &str,
        blob: BlobId,
        opts: EvictOptions,
    ) -> BoxFuture<'_, ()> {
        let node = Arc::clone(&self.node);
        let core_key_hex = core_key_hex.to_owned();
        Box::pin(async move {
            let blob = Blob {
                block_offset: blob.block_offset,
                block_length: blob.block_length,
                byte_offset: blob.byte_offset,
                byte_length: blob.byte_length,
                extra: Map::new(),
            };
            let options = json!({
                "playheadByte": opts.playhead_byte,
                "keepBehindBytes": opts.keep_behind_bytes,
                "maxBytes": opts.max_bytes,
                "keepAheadBytes": opts.keep_ahead_bytes,
            });
            let _ = node.evict_behind(&core_key_hex, blob, options).await;
        })
    }
    fn find_entry(&self, key: &str) -> BoxFuture<'_, Option<Value>> {
        let node = Arc::clone(&self.node);
        let key = key.to_owned();
        Box::pin(async move { node.find_entry(&key).await.ok().flatten() })
    }
    fn search(&self, id: &str) -> BoxFuture<'_, Vec<Value>> {
        let node = Arc::clone(&self.node);
        let id = id.to_owned();
        Box::pin(async move {
            node.search(Some(&id), SearchOptions::default())
                .await
                .unwrap_or_default()
                .into_iter()
                .filter_map(|entry| serde_json::to_value(entry).ok())
                .collect()
        })
    }
    fn warm(&self, raw: &Value) -> BoxFuture<'_, ()> {
        let node = Arc::clone(&self.node);
        let raw = raw.clone();
        Box::pin(async move {
            let _ = node.warm(&raw).await;
        })
    }
    fn close(&self) -> BoxFuture<'_, ()> {
        // The worker owns this node and closes it when the backend shuts down; a receiver or
        // controller must not close the node it shares with the app's API.
        Box::pin(async {})
    }
}

// ---------------------------------------------------------------------------------------------
// The worker's Cast: the controller plus the local receiver

/// The controller and TV receiver the worker drives through [`crate::node::api::Cast`].
pub struct Service {
    controller: CastController,
    storage: PathBuf,
    node: NodeGetter,
    get_lan_host: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    on_change: Arc<dyn Fn() + Send + Sync>,
}

fn cast_error(error: CastError) -> String {
    error.message
}

impl CastApi for Service {
    fn set_lan_host(&self, host: Option<String>) -> BoxFuture<'_, ()> {
        Box::pin(self.controller.set_lan_host(host))
    }

    fn status(&self) -> Value {
        self.controller.status()
    }

    fn pair_receiver<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.controller
                .pair_receiver(params)
                .await
                .map_err(cast_error)
        })
    }

    fn remove_receiver(&self, id: &str) -> Value {
        self.controller.remove_receiver(id)
    }

    fn cast_play<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move { self.controller.cast_play(params).await.map_err(cast_error) })
    }

    fn cast_attach<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.controller
                .cast_attach(params)
                .await
                .map_err(cast_error)
        })
    }

    fn cast_control<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
        Box::pin(async move {
            self.controller
                .cast_control(params)
                .await
                .map_err(cast_error)
        })
    }

    fn cast_stop<'a>(&'a self, params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
        let stop_playback = params.get("stopPlayback").is_none_or(super::state::truthy);
        Box::pin(async move {
            self.controller
                .cast_stop(stop_playback)
                .await
                .map_err(cast_error)
        })
    }

    fn start_local_receiver(
        &self,
        options: ReceiverStart,
        on_command: Arc<dyn Fn(Value) + Send + Sync>,
    ) -> BoxFuture<'_, Result<Value, String>> {
        Box::pin(async move {
            let peer = (self.node)().map(|node| Arc::new(NodePeer::new(node)) as Arc<dyn CastPeer>);
            let change = Arc::clone(&self.on_change);
            let receiver = ReceiverOptions {
                storage: self.storage.clone(),
                peer_storage: options.peer_storage.as_deref().map(PathBuf::from),
                name: options.name,
                port: RECEIVER_PORT,
                lan_host: (self.get_lan_host)(),
                peer,
                mdns: true,
                open_auth: options.open_auth,
                any_port: true,
                on_command: Some(on_command),
                on_change: Some(Arc::new(move |_state| change())),
            };
            self.controller
                .start_local_receiver(receiver)
                .await
                .map_err(cast_error)
        })
    }

    fn report_local_receiver_state<'a>(&'a self, patch: &'a Value) -> BoxFuture<'a, Option<Value>> {
        Box::pin(async move { self.controller.report_local_receiver_state(patch) })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.controller.close())
    }
}

/// `createCastController` plus `startLocalReceiver`, over the backend's real node.
pub struct Factory;

impl CastFactory<CoreNode> for Factory {
    async fn create(&self, options: CastOptions<CoreNode>) -> Result<Arc<dyn CastApi>, String> {
        let storage = PathBuf::from(&options.storage);
        let node = options.node.clone();
        let getter: NodeGetter = {
            let node = Arc::clone(&node);
            Arc::new(move || node())
        };
        let peer_node = Arc::clone(&getter);
        let controller = CastController::start(ControllerOptions {
            storage: storage.clone(),
            get_node: Arc::new(move || {
                peer_node().map(|node| Arc::new(NodePeer::new(node)) as Arc<dyn CastPeer>)
            }),
            get_lan_host: Arc::clone(&options.get_lan_host),
            mdns: true,
            on_change: Some(Arc::clone(&options.on_change)),
        })
        .await
        .map_err(cast_error)?;
        Ok(Arc::new(Service {
            controller,
            storage,
            node: getter,
            get_lan_host: options.get_lan_host,
            on_change: options.on_change,
        }))
    }

    async fn peer_storage(&self, candidate: Option<&str>, fallback: &str) -> String {
        disk::resolve_peer_storage(candidate.map(Path::new), Path::new(fallback))
            .await
            .to_string_lossy()
            .into_owned()
    }

    fn reclaim(&self, storage: &str) {
        disk::reclaim_low_disk_receiver_storage(Path::new(storage));
    }

    fn receiver_key_pair(&self, storage: &str) -> Option<Value> {
        super::receiver::receiver_key_pair_json(Path::new(storage)).ok()
    }
}

impl Service {
    /// The node this service drives, for the host's own bookkeeping.
    pub fn node(&self) -> Option<Arc<CoreNode>> {
        (self.node)()
    }

    /// The controller, for hosts that need the receiver list without a JSON round trip.
    pub fn controller(&self) -> &CastController {
        &self.controller
    }
}
