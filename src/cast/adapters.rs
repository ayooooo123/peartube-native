//! The real adapters behind [`crate::node::api::CastFactory`]: the cast stack's seams filled by
//! `CoreNode`, peeroxide's secret streams and `byte-stream-rs`, as `worker.js` fills them from the
//! app's node, `dht` and `createBlobRangeServer`.
//!
//! - [`NodePeer`] is the [`CastPeer`] a receiver or controller drives: the node's catalog, blob
//!   cores, swarm joins and cache trimming.
//! - [`NodeDht`] / [`NodeDhtServer`] turn a peeroxide [`SecretStream`] into the byte pipe the Noise
//!   RPC protocol runs on.
//! - [`CastBlobServers`] binds the sender's token-gated LAN Range server over the node's Corestore.
//! - [`Factory`] is [`crate::node::api::CastFactory`]: the controller plus the local receiver the
//!   worker starts, and the receiver-storage helpers beside `createCastController`.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use byte_stream::server::{BlobServer, Resolve, ResolveFuture, Resolved, ServerOptions};
use corestore::GetOptions as StoreGetOptions;
use futures_util::future::BoxFuture;
use hyperblobs::BlobId;
use libudx::{UdxAsyncStream, UdxRuntime};
use peeroxide::{JoinOpts, SwarmConfig, SwarmHandle};
use peeroxide_dht::hyperdht::{HyperDhtConfig, KeyPair};
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::rpc::DhtConfig;
use peeroxide_dht::secret_stream::SecretStream;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex as AsyncMutex;

use crate::node::Node;
use crate::node::api::{
    Blob, Cast as CastApi, CastFactory, CastOptions, LanEndpoint, OpenUri, ReceiverStart,
    SearchOptions,
};
use crate::node::core::CoreNode;
use crate::node::network::bootstrap_list;
use crate::protocol::{API_PORT, RECEIVER_PORT};

use super::CastError;
use super::controller::{CastController, ControllerOptions};
use super::disk;
use super::pairing::ReceiverIdentity;
use super::receiver::ReceiverOptions;
use super::state::truthy;
use super::traits::{
    BlobServerFactory, CastPeer, ClearCacheOptions, DhtConnect, DhtError, DhtServer, Duplex,
    EvictOptions, LanBlobServer, NoiseHandler, OpenedMedia, PeerStatus,
};

/// Bytes a pump hands between a secret stream and the local pipe in one go.
const PIPE_BYTES: usize = 64 * 1024;

type NodeGetter = Arc<dyn Fn() -> Option<Arc<CoreNode>> + Send + Sync>;

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

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

/// `dht.isLoopback`: a bootstrap on `127.` keeps the node on the loopback interface.
fn bootstrap_is_loopback(bootstrap: &[String]) -> bool {
    bootstrap.iter().any(|node| {
        let host = node.split('@').next_back().unwrap_or(node);
        host.starts_with("127.")
    })
}

fn relays_of(list: &[String]) -> Vec<Ipv4Peer> {
    list.iter()
        .filter_map(|entry| {
            let address = entry.split('@').next_back().unwrap_or(entry);
            let (host, port) = address.rsplit_once(':')?;
            Some(Ipv4Peer {
                host: host.to_owned(),
                port: port.parse().ok()?,
            })
        })
        .collect()
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

    fn net(&self) -> Arc<crate::node::network::Network> {
        self.node.net()
    }
}

impl CastPeer for NodePeer {
    fn peer_key(&self) -> String {
        self.node.peer_key()
    }

    fn private_routing(&self) -> bool {
        self.node.private_routing()
    }

    fn has_lan(&self) -> bool {
        self.net().has_lan()
    }

    fn lan_endpoint(&self) -> Option<Value> {
        serde_json::to_value(self.net().lan_endpoint()?).ok()
    }

    fn bootstrap(&self) -> Option<Value> {
        self.node.bootstrap()
    }

    fn relay_through(&self) -> Option<Value> {
        self.node.relay_through()
    }

    fn status(&self) -> PeerStatus {
        let net = self.net();
        let sample = net.sample();
        PeerStatus {
            peers: net.connection_count(),
            lan_peers: net.lan_connection_count(),
            routes: serde_json::to_value(sample.routes).ok(),
        }
    }

    fn join_peer(&self, key: &str, lan_first: bool) {
        self.net().join_peer(key, lan_first);
    }

    fn dial_lan_peer(&self, endpoint: &Value) {
        if let Ok(endpoint) = serde_json::from_value::<LanEndpoint>(endpoint.clone()) {
            self.net().dial_lan_peer(&endpoint);
        }
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
                        stream_url,
                        entry: serde_json::to_value(entry)
                            .map_err(|error| CastError::new(error.to_string(), 500))?,
                        blob: None,
                    }))
                }
                Ok(_) => Ok(None),
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
// DHT: peeroxide secret streams over the cast's newline JSON pipe

/// One established secret stream as a byte pipe.
///
/// `SecretStream` frames messages; the RPC protocol is newline JSON over a byte stream, so the
/// pump concatenates frames into a local duplex and frames each write. `shortcut: a write is
/// sent as one message per read chunk (64 KiB), no coalescing; upgrade if the RPC ever sends
/// latency-sensitive small messages in bulk`.
fn secret_stream_pipe(stream: SecretStream<UdxAsyncStream>) -> Box<dyn Duplex> {
    let (mut frames_in, mut frames_out) = stream.into_split();
    let (read_end, mut pump_write) = tokio::io::duplex(PIPE_BYTES);
    let (mut pump_read, write_end) = tokio::io::duplex(PIPE_BYTES);
    tokio::spawn(async move {
        while let Ok(Some(frame)) = frames_in.read().await {
            if pump_write.write_all(&frame).await.is_err() {
                break;
            }
        }
        let _ = pump_write.shutdown().await;
    });
    tokio::spawn(async move {
        let mut buffer = vec![0u8; PIPE_BYTES];
        loop {
            match pump_read.read(&mut buffer).await {
                Ok(0) => {
                    let _ = frames_out.shutdown().await;
                    break;
                }
                Ok(read) => {
                    if frames_out.write(&buffer[..read]).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    Box::new(tokio::io::join(read_end, write_end))
}

/// `dht.connect(publicKey, { relayThrough })` over the node's own HyperDHT handle.
pub struct NodeDht {
    node: NodeGetter,
    runtime: Arc<UdxRuntime>,
}

impl NodeDht {
    pub fn new(node: NodeGetter) -> Result<Self, String> {
        let runtime = UdxRuntime::new().map_err(|error| error.to_string())?;
        Ok(Self {
            node,
            runtime: Arc::new(runtime),
        })
    }
}

impl DhtConnect for NodeDht {
    fn connect(
        &self,
        public_key: [u8; 32],
        relay_through: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<Box<dyn Duplex>, DhtError>> {
        let node = (self.node)();
        let runtime = Arc::clone(&self.runtime);
        Box::pin(async move {
            let node = node.ok_or_else(|| DhtError::other("P2P node is not started"))?;
            let handle = node.dht().clone();
            let key_pair = node.net().key_pair().clone();
            let relays = relay_through
                .map(|list| relays_of(&list))
                .unwrap_or_default();
            let connection = if relays.is_empty() {
                handle.connect(&key_pair, public_key, &runtime).await
            } else {
                handle
                    .connect_with_nodes(&key_pair, public_key, &relays, &runtime)
                    .await
            }
            .map_err(|error| DhtError::other(error.to_string()))?;
            Ok(secret_stream_pipe(connection.stream))
        })
    }
}

/// `dht.createServer(onConnection)` + `server.listen(keyPair)`: a swarm bound to the receiver's own
/// Noise key pair, accepting the secret streams a controller dials.
pub struct NodeDhtServer {
    key_pair: KeyPair,
    bootstrap: Vec<String>,
    host: String,
    swarm: AsyncMutex<Option<SwarmHandle>>,
    tasks: AsyncMutex<Vec<tokio::task::JoinHandle<()>>>,
    closed: AtomicBool,
}

impl NodeDhtServer {
    pub fn new(key_pair: KeyPair, bootstrap: Vec<String>) -> Self {
        let host = if bootstrap_is_loopback(&bootstrap) {
            "127.0.0.1".to_owned()
        } else {
            "0.0.0.0".to_owned()
        };
        Self {
            key_pair,
            bootstrap,
            host,
            swarm: AsyncMutex::new(None),
            tasks: AsyncMutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        }
    }
}

impl DhtServer for NodeDhtServer {
    fn serve(
        &self,
        public_key: [u8; 32],
        handler: NoiseHandler,
    ) -> BoxFuture<'_, Result<(), DhtError>> {
        let expected = self.key_pair.public_key;
        let mut config = SwarmConfig::default();
        config.key_pair = Some(self.key_pair.clone());
        config.dht = HyperDhtConfig::default();
        let mut dht = DhtConfig::default();
        dht.bootstrap = self.bootstrap.clone();
        dht.host = self.host.clone();
        dht.port = 0;
        dht.firewalled = false;
        config.dht.dht = dht;
        Box::pin(async move {
            if expected != public_key {
                return Err(DhtError::other(
                    "The receiver identity key does not match the advertised public key",
                ));
            }
            if self.closed.load(Ordering::SeqCst) {
                return Err(DhtError::other("The receiver DHT server is closed"));
            }
            let (join, swarm, mut incoming) = peeroxide::spawn(config)
                .await
                .map_err(|error| DhtError::other(error.to_string()))?;
            let topic = peeroxide::discovery_key(&public_key);
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = false;
            swarm
                .join(topic, opts)
                .await
                .map_err(|error| DhtError::other(error.to_string()))?;
            let accept = tokio::spawn(async move {
                while let Some(connection) = incoming.recv().await {
                    handler(secret_stream_pipe(connection.peer.stream));
                }
            });
            *self.swarm.lock().await = Some(swarm);
            *self.tasks.lock().await = vec![join, accept];
            Ok(())
        })
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if self.closed.swap(true, Ordering::SeqCst) {
                return;
            }
            for task in self.tasks.lock().await.drain(..) {
                task.abort();
            }
            let swarm = self.swarm.lock().await.take();
            if let Some(swarm) = swarm {
                let _ = swarm.destroy().await;
            }
        })
    }
}

// ---------------------------------------------------------------------------------------------
// The token-gated LAN Range server

/// The sender's LAN Range server, as [`BlobServerFactory`] binds it.
pub struct CastBlobServers {
    node: NodeGetter,
    allowed: Arc<Mutex<HashSet<String>>>,
    server: AsyncMutex<Option<Arc<LanServer>>>,
}

impl CastBlobServers {
    pub fn new(node: NodeGetter) -> Self {
        Self {
            node,
            allowed: Arc::new(Mutex::new(HashSet::new())),
            server: AsyncMutex::new(None),
        }
    }
}

impl BlobServerFactory for CastBlobServers {
    fn ensure(
        &self,
        _lan_host: Option<&str>,
    ) -> BoxFuture<'_, Result<Arc<dyn LanBlobServer>, CastError>> {
        Box::pin(async move {
            if let Some(server) = self.server.lock().await.as_ref() {
                return Ok(Arc::clone(server) as Arc<dyn LanBlobServer>);
            }
            let allowed = Arc::clone(&self.allowed);
            let node = Arc::clone(&self.node);
            let resolve: Resolve = Arc::new(move |key: [u8; 32]| {
                let allowed = Arc::clone(&allowed);
                let node = Arc::clone(&node);
                Box::pin(async move {
                    let hex = hex(&key);
                    if !allowed
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .contains(&hex)
                    {
                        return None;
                    }
                    let node = node()?;
                    // `blob_core` keeps the swarm join, ledger and ceiling side effects; the
                    // response then opens its own session, like the app's `store.session().get()`.
                    node.blob_core(&hex).await.ok()?;
                    let session = node.store().get(StoreGetOptions::key(key)).await.ok()?;
                    Some(Resolved { session })
                }) as ResolveFuture
            });
            let bound = BlobServer::bind(
                SocketAddr::from(([0, 0, 0, 0], 0)),
                ServerOptions::default(),
                resolve,
            )
            .await
            .map_err(|error| CastError::new(error.to_string(), 500))?;
            let server = Arc::new(LanServer {
                server: Mutex::new(Some(bound)),
                allowed: Arc::clone(&self.allowed),
            });
            *self.server.lock().await = Some(Arc::clone(&server));
            Ok(server as Arc<dyn LanBlobServer>)
        })
    }
}

struct LanServer {
    server: Mutex<Option<BlobServer>>,
    allowed: Arc<Mutex<HashSet<String>>>,
}

impl LanBlobServer for LanServer {
    fn allow(&self, blobs_hex: &str) {
        self.allowed
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(blobs_hex.to_lowercase());
    }

    fn get_link(&self, blobs_hex: &str, host: &str, blob: &BlobId, mime: &str) -> String {
        let Some(key) = parse_hex32(blobs_hex) else {
            return String::new();
        };
        let server = self
            .server
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(server) = server.as_ref() else {
            return String::new();
        };
        let port = server.local_addr().port();
        let link = server.link(&key, blob, mime);
        let path = link
            .split_once("://")
            .and_then(|(_, rest)| rest.split_once('/'))
            .map(|(_, path)| path)
            .unwrap_or("");
        format!("http://{host}:{port}/{path}")
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let server = self
                .server
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(server) = server {
                server.close().await;
            }
        })
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
    fn init_hub(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.controller.init_hub().await;
        })
    }

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
        let stop_playback = params.get("stopPlayback").is_some_and(truthy);
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
            let identity = ReceiverIdentity::load_or_create(&self.storage, &options.name)
                .map_err(|error| error.to_string())?;
            let dht_server = identity.seed_bytes().map(|seed| {
                let key_pair = KeyPair::from_seed(seed);
                let bootstrap = (self.node)()
                    .map(|node| bootstrap_list(node.bootstrap().as_ref()))
                    .unwrap_or_default();
                Arc::new(NodeDhtServer::new(key_pair, bootstrap)) as Arc<dyn DhtServer>
            });
            let change = Arc::clone(&self.on_change);
            let receiver = ReceiverOptions {
                storage: self.storage.clone(),
                peer_storage: options.peer_storage.as_deref().map(PathBuf::from),
                name: options.name,
                port: RECEIVER_PORT,
                bind_host: "0.0.0.0".to_owned(),
                lan_host: (self.get_lan_host)(),
                peer: None,
                allow_peer_node: true,
                peer_factory: None,
                dht_server,
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
        Box::pin(self.controller.report_local_receiver_state(patch))
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
        let peer = {
            let node = Arc::clone(&node);
            move || node().map(|node| Arc::new(NodePeer::new(node)) as Arc<dyn CastPeer>)
        };
        let ensure_node: super::controller::EnsureNode = {
            let peer = Arc::new(peer);
            let getter = Arc::clone(&peer);
            Arc::new(move || {
                let getter = Arc::clone(&getter);
                Box::pin(async move { Ok(getter()) })
            })
        };
        let initial_peer = ensure_node().await.map_err(cast_error)?;
        let blob_servers = Arc::new(CastBlobServers::new(Arc::clone(&node)));
        let dht = Arc::new(NodeDht::new(Arc::clone(&node))?);
        let controller = CastController::start(ControllerOptions {
            storage: storage.clone(),
            peer: initial_peer,
            dht: Some(dht),
            ensure_node: Some(ensure_node),
            get_lan_host: Some(Arc::clone(&options.get_lan_host)),
            blob_servers: Some(blob_servers),
            on_change: Some(Arc::clone(&options.on_change)),
            api_port: API_PORT,
            mdns: true,
        })
        .await
        .map_err(cast_error)?;
        Ok(Arc::new(Service {
            controller,
            storage,
            node,
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
}
