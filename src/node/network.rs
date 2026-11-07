//! `src/node/network.js`: the swarm, its topics, replication and the traffic sample.
//!
//! Every accepted connection becomes one Protomux multiplexer over its Noise stream; the
//! Corestore replicates every open core over it (and opens stored cores a peer asks for), and
//! the node's one wakeup swarm opens a topic channel per bee on it. A connection pairs wakeup
//! once, so the swarm is shared: a second swarm on the same mux would take the pairing.
//!
//! The peeroxide swarm has no `joinPeer`: a direct peer dials the DHT itself and wires the
//! resulting stream into the same replication set.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

#[cfg(feature = "peeroxide-dht")]
use crate::private_overlay::dht::{MessageDuplex, MessageRead, MessageWrite, NoiseConnection};
#[cfg(feature = "peeroxide-dht")]
use crate::private_overlay::peeroxide::open_dht;
#[cfg(feature = "peeroxide-dht")]
use crate::private_overlay::{PrivateSwarm, SwarmOptions, create_private_swarm};
use corestore::Corestore;
use futures_util::future::BoxFuture;
use libudx::UdxRuntime;
use peeroxide::{JoinOpts, SwarmConfig, SwarmHandle};
use peeroxide_dht::hyperdht::{HyperDhtConfig, HyperDhtHandle};
use peeroxide_dht::rpc::DhtConfig;
use peeroxide_dht::secret_stream::SecretStream;
use protomux::{FrameRead, FrameWrite, Mux, StreamInfo};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};

use super::api::{LanEndpoint, Route, Routes};

/// Announcements beyond the cap take turns on the DHT.
pub const MAX_CONTENT_TOPICS: usize = 48;
/// How often content beyond the announcement cap gets its turn.
pub const CONTENT_TOPIC_ROTATE_MS: u64 = 20_000;

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
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

fn number(value: f64) -> serde_json::Number {
    serde_json::Number::from_f64(value).unwrap_or_else(|| 0.into())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

/// `bootstrap`: strings (`host:port` or `ip@host:port`) or `{ host, port }` objects.
pub fn bootstrap_list(value: Option<&Value>) -> Vec<String> {
    let Some(Value::Array(entries)) = value else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| match entry {
            Value::String(text) => Some(text.clone()),
            Value::Object(map) => {
                let host = map.get("host")?.as_str()?;
                let port = map.get("port")?.as_u64()?;
                Some(format!("{host}:{port}"))
            }
            _ => None,
        })
        .collect()
}

fn is_loopback(bootstrap: &[String]) -> bool {
    bootstrap.iter().any(|node| {
        let host = node.split('@').next_back().unwrap_or(node);
        host.starts_with("127.")
    })
}

/// `bootstrap: []` means the public network, as npm hyperdht does: an empty list is only ever the
/// caller saying "I have no custom nodes", never "run isolated" (the LAN half passes its own DHT).
pub fn resolve_bootstrap(bootstrap: Vec<String>) -> Vec<String> {
    if !bootstrap.is_empty() {
        return bootstrap;
    }
    peeroxide_dht::hyperdht::DEFAULT_BOOTSTRAP
        .iter()
        .map(|node| (*node).to_owned())
        .collect()
}

/// A LAN binding the host supplies (`opts.lan(swarm.keyPair)`), as the node drives it.
pub trait LanBinding: Send + Sync {
    fn join(&self, topic: [u8; 32]);
    fn leave(&self, topic: [u8; 32]);
    fn destroy(&self) -> BoxFuture<'_, ()>;
    fn host(&self) -> Option<String>;
    fn port(&self) -> Option<u16>;
    fn stats(&self) -> Option<Value>;
    /// `lan.dialDirect(publicKey, host, port)`.
    fn dial_direct(&self, public_key: &str, host: &str, port: u16);
    /// Registers the connection callback; the returned closure unregisters it.
    fn on_connection(
        &self,
        callback: Arc<dyn Fn(LanConnection) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync>;
}

/// One connection a LAN binding accepted.
pub struct LanConnection {
    pub mux: Mux,
    pub remote_public_key: Option<[u8; 32]>,
    pub close: Box<dyn Fn() + Send + Sync>,
    /// The frames this connection carried, so the LAN route can count them.
    pub bytes: WireBytes,
}

struct LanState {
    binding: Arc<dyn LanBinding>,
    closer: Box<dyn Fn() + Send + Sync>,
}

/// A bee's wakeup that every connection must join.
pub trait WakeupSink: Send + Sync {
    fn add_stream(&self, mux: &Mux);
    /// `session.broadcastLookup()`: ask the new peer for the writers it holds.
    fn request_lookup(&self);
}

/// The frames one connection carried, as its route counts them.
///
/// peeroxide's DHT counters never see established UDX streams, so nothing else would; every
/// mux counts what crosses it, and a LAN binding hands its connection's counters over with it.
#[derive(Clone, Default)]
pub struct WireBytes {
    rx: Arc<AtomicU64>,
    tx: Arc<AtomicU64>,
}

impl WireBytes {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rx(&self) -> u64 {
        self.rx.load(Ordering::Relaxed)
    }

    pub fn tx(&self) -> u64 {
        self.tx.load(Ordering::Relaxed)
    }
}

/// The connection's frames, counted: peeroxide's DHT counters never see
/// established UDX streams, so nothing else would count replication bytes.
struct MeteredRead<R>(R, Arc<AtomicU64>);

impl<R: FrameRead> FrameRead for MeteredRead<R> {
    async fn read(&mut self) -> std::io::Result<Option<bytes::Bytes>> {
        let frame = self.0.read().await?;
        if let Some(frame) = &frame {
            self.1.fetch_add(frame.len() as u64, Ordering::Relaxed);
        }
        Ok(frame)
    }
}

struct MeteredWrite<W>(W, Arc<AtomicU64>);

impl<W: FrameWrite> FrameWrite for MeteredWrite<W> {
    async fn write(&mut self, frame: bytes::Bytes) -> std::io::Result<()> {
        self.1.fetch_add(frame.len() as u64, Ordering::Relaxed);
        self.0.write(frame).await
    }

    async fn close(&mut self) -> std::io::Result<()> {
        self.0.close().await
    }
}

/// `Mux::from_secret_stream`, with every frame added to `bytes`.
pub fn metered_mux<T>(stream: SecretStream<T>, bytes: &WireBytes) -> Mux
where
    T: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    ReadHalf<T>: Send,
    WriteHalf<T>: Send,
{
    let info = StreamInfo {
        is_initiator: stream.is_initiator(),
        handshake_hash: Some(*stream.handshake_hash()),
        remote_public_key: Some(*stream.remote_public_key()),
    };
    let (read, write) = stream.into_split();
    Mux::new(
        MeteredRead(read, Arc::clone(&bytes.rx)),
        MeteredWrite(write, Arc::clone(&bytes.tx)),
        info,
    )
}

/// The private overlay handle when this build ships one; nothing is ever stored otherwise.
#[cfg(feature = "peeroxide-dht")]
type PrivateOverlay = PrivateSwarm;
/// Without the feature no overlay is ever created; the stub keeps the plumbing type-checking.
#[cfg(not(feature = "peeroxide-dht"))]
#[derive(Clone)]
#[allow(dead_code)]
struct PrivateOverlay;

#[cfg(not(feature = "peeroxide-dht"))]
#[allow(dead_code)]
impl PrivateOverlay {
    async fn destroy(&self) {}
    async fn suspend(&self) {}
    async fn resume(&self) {}
    async fn leave_source(&self, _id: &str) {}
    fn join_source(&self, _id: &str, _key: &[u8; 32]) {}
    fn join_peer(&self, _key: [u8; 32]) {}
}

/// The private overlay's message halves as a protomux frame transport.
#[cfg(feature = "peeroxide-dht")]
struct FramesRead(Box<dyn MessageRead>);

#[cfg(feature = "peeroxide-dht")]
impl FrameRead for FramesRead {
    async fn read(&mut self) -> std::io::Result<Option<bytes::Bytes>> {
        self.0
            .read_message()
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[cfg(feature = "peeroxide-dht")]
struct FramesWrite(Box<dyn MessageWrite>);

#[cfg(feature = "peeroxide-dht")]
impl FrameWrite for FramesWrite {
    async fn write(&mut self, frame: bytes::Bytes) -> std::io::Result<()> {
        self.0
            .write_message(&frame)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
    }

    async fn close(&mut self) -> std::io::Result<()> {
        self.0.shutdown().await;
        Ok(())
    }
}

/// One private overlay connection as a metered mux over the Noise stream's own messages, the
/// same framing the public connections use (and JS replicates over).
#[cfg(feature = "peeroxide-dht")]
fn private_mux(
    stream: Box<dyn MessageDuplex>,
    is_initiator: bool,
    handshake_hash: Option<[u8; 64]>,
    remote_public_key: [u8; 32],
    bytes: &WireBytes,
) -> Mux {
    let info = StreamInfo {
        is_initiator,
        handshake_hash,
        remote_public_key: Some(remote_public_key),
    };
    let (read, write) = stream.split();
    Mux::new(
        MeteredRead(FramesRead(read), Arc::clone(&bytes.rx)),
        MeteredWrite(FramesWrite(write), Arc::clone(&bytes.tx)),
        info,
    )
}

/// Opens the private overlay with the network's bind and bootstrap.
///
/// Returns `None` when the overlay cannot start; `privateRouting` then fails closed, announcing
/// nothing on the public DHT or the LAN.
#[cfg(feature = "peeroxide-dht")]
async fn start_private(
    network: &Arc<Network>,
    host: String,
    bootstrap: Vec<String>,
    port: u16,
) -> Option<PrivateOverlay> {
    let dht = open_dht(crate::private_overlay::dht::DhtOptions {
        host: Some(host.clone()),
        port,
        bootstrap: bootstrap.clone(),
    })
    .await
    .ok()?;
    let weak = network.weak.clone();
    let hook: crate::private_overlay::swarm::ConnectionHook = Arc::new(move |connection| {
        let Some(network) = weak.upgrade() else {
            (connection.close)();
            return;
        };
        let NoiseConnection {
            remote_public_key: peer,
            is_initiator,
            handshake_hash,
            stream,
            close,
            ..
        } = connection;
        if network.closing.load(Ordering::Acquire) || network.blocked(&peer) {
            close();
            return;
        }
        let bytes = WireBytes {
            rx: Arc::clone(&network.private_rx),
            tx: Arc::clone(&network.private_tx),
        };
        let mux = private_mux(stream, is_initiator, handshake_hash, peer, &bytes);
        let handle = network.wire(
            mux,
            peer,
            false,
            true,
            Box::new(move || close()),
            Vec::new(),
            None,
        );
        network.private_peers.fetch_add(1, Ordering::AcqRel);
        let watched = handle.mux().clone();
        let weak = network.weak.clone();
        tokio::spawn(async move {
            watched.closed().await;
            if let Some(network) = weak.upgrade() {
                network.private_peers.fetch_sub(1, Ordering::AcqRel);
            }
        });
    });
    Some(create_private_swarm(
        dht,
        SwarmOptions {
            host: Some(host),
            bootstrap,
            port,
            allow_loopback: None,
            resolve_address: None,
            key_pair: None,
            firewall: None,
            on_connection: Some(hook),
            on_update: None,
        },
    ))
}

struct BeeWakeup(Arc<autobee::Autobee>);

impl WakeupSink for BeeWakeup {
    fn add_stream(&self, mux: &Mux) {
        self.0.wakeup.add_stream(mux);
    }

    fn request_lookup(&self) {
        self.0.wakeup.request_lookup();
    }
}

struct ConnectionState {
    peer: [u8; 32],
    mux: Mux,
    closed: AtomicBool,
    close: Box<dyn Fn() + Send + Sync>,
    lan: bool,
    private: bool,
    /// The transport pieces the mux does not own (socket handle, direct-dial runtime).
    _keepalive: Vec<Box<dyn Any + Send + Sync>>,
}

/// A connection as the app's non-JSON handle (`Node::Connection`).
#[derive(Clone)]
pub struct ConnectionHandle(Arc<ConnectionState>);

impl ConnectionHandle {
    pub fn remote_public_key(&self) -> String {
        hex(&self.0.peer)
    }
    pub fn destroyed(&self) -> bool {
        self.0.closed.load(Ordering::Acquire) || self.0.mux.is_destroyed()
    }
    pub fn is_lan(&self) -> bool {
        self.0.lan
    }
    pub fn is_private(&self) -> bool {
        self.0.private
    }
    pub fn mux(&self) -> &Mux {
        &self.0.mux
    }
    pub fn close(&self) {
        if !self.0.closed.swap(true, Ordering::AcqRel) {
            (self.0.close)();
        }
    }
}

#[derive(Default, Clone, Copy)]
struct Bucket {
    closed_rx: u64,
    closed_tx: u64,
    down_bps: f64,
    up_bps: f64,
}

impl Bucket {
    fn route(&self, peers: u64, rx: u64, tx: u64) -> Route {
        Route {
            peers,
            rx_bytes: rx,
            tx_bytes: tx,
            down_bps: number(self.down_bps),
            up_bps: number(self.up_bps),
        }
    }
}

/// The traffic sample the app's `status()` shows.
#[derive(Clone, Debug)]
pub struct Sample {
    pub routes: Routes,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub down_bps: f64,
    pub up_bps: f64,
    pub contributing_peers: u64,
    pub rtt: u64,
    pub retransmits: u64,
}

type Listener = Arc<dyn Fn(ConnectionHandle) + Send + Sync>;

/// `createNetwork(ctx)`.
pub struct Network {
    weak: Weak<Network>,
    swarm: SwarmHandle,
    dht: HyperDhtHandle,
    runtime: Arc<UdxRuntime>,
    store: Corestore,
    private_routing: bool,
    /// The private overlay when it started; `privateRouting` fails closed without one.
    private: Mutex<Option<PrivateOverlay>>,
    private_rx: Arc<AtomicU64>,
    private_tx: Arc<AtomicU64>,
    private_peers: Arc<AtomicU64>,
    connections: Mutex<Vec<ConnectionHandle>>,
    listeners: Mutex<Vec<(u64, Listener)>>,
    next_listener: AtomicU64,
    wakeups: Mutex<Vec<Arc<dyn WakeupSink>>>,
    content_topics: Mutex<HashMap<[u8; 32], [u8; 32]>>,
    overlay_topics: Mutex<HashMap<String, [u8; 32]>>,
    allowed: Mutex<Option<HashSet<String>>>,
    lan: Mutex<Option<LanState>>,
    public: Bucket,
    lan_bucket: Bucket,
    /// Frames read and written on the swarm's connections (the public route's bytes).
    wire_rx: Arc<AtomicU64>,
    wire_tx: Arc<AtomicU64>,
    /// Every LAN connection's counters, including closed ones so the route's total only grows;
    /// they are folded into the closed totals when the binding is replaced.
    lan_wires: Arc<Mutex<Vec<WireBytes>>>,
    lan_closed_rx: AtomicU64,
    lan_closed_tx: AtomicU64,
    lan_prev_rx: AtomicU64,
    lan_prev_tx: AtomicU64,
    prev_rx: AtomicU64,
    prev_tx: AtomicU64,
    prev_at: AtomicU64,
    down_bps: Mutex<f64>,
    up_bps: Mutex<f64>,
    inflight: Mutex<Option<(u32, u32)>>,
    closing: Arc<AtomicBool>,
}

impl Network {
    /// Opens the swarm and starts accepting connections.
    #[allow(clippy::too_many_arguments)] // the reference's `createNetwork(ctx)` fields, in order
    pub async fn open(
        store: Corestore,
        bootstrap: Vec<String>,
        base: Option<String>,
        port: u16,
        relay_through: Option<[u8; 32]>,
        allowed_peers: Option<Vec<String>>,
        private_routing: bool,
        closing: Arc<AtomicBool>,
    ) -> Result<Arc<Self>, std::io::Error> {
        let host = base.unwrap_or_else(|| {
            if is_loopback(&bootstrap) {
                "127.0.0.1".to_owned()
            } else {
                "0.0.0.0".to_owned()
            }
        });
        let bootstrap = resolve_bootstrap(bootstrap);
        let private_host = host.clone();
        let private_bootstrap = bootstrap.clone();
        let mut config = SwarmConfig::default();
        config.max_parallel = 16;
        config.relay_through = relay_through;
        config.dht = HyperDhtConfig::default();
        let mut rpc = DhtConfig::default();
        rpc.bootstrap = bootstrap;
        rpc.port = port;
        rpc.host = host;
        rpc.firewalled = false;
        config.dht.dht = rpc;
        let (_task, swarm, mut incoming) = peeroxide::spawn(config)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        let runtime =
            Arc::new(UdxRuntime::new().map_err(|error| std::io::Error::other(error.to_string()))?);
        let allowed = allowed_peers
            .filter(|peers| !peers.is_empty())
            .map(|peers| {
                peers
                    .into_iter()
                    .map(|key| key.to_lowercase())
                    .collect::<HashSet<_>>()
            });
        let dht = swarm.dht().clone();
        let network: Arc<Self> = Arc::new_cyclic(|weak| Self {
            weak: weak.clone(),
            swarm,
            dht,
            runtime,
            store,
            private_routing,
            private: Mutex::new(None),
            private_rx: Arc::new(AtomicU64::new(0)),
            private_tx: Arc::new(AtomicU64::new(0)),
            private_peers: Arc::new(AtomicU64::new(0)),
            connections: Mutex::new(Vec::new()),
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicU64::new(0),
            wakeups: Mutex::new(Vec::new()),
            content_topics: Mutex::new(HashMap::new()),
            overlay_topics: Mutex::new(HashMap::new()),
            allowed: Mutex::new(allowed),
            lan: Mutex::new(None),
            public: Bucket::default(),
            lan_bucket: Bucket::default(),
            wire_rx: Arc::new(AtomicU64::new(0)),
            wire_tx: Arc::new(AtomicU64::new(0)),
            lan_wires: Arc::new(Mutex::new(Vec::new())),
            lan_closed_rx: AtomicU64::new(0),
            lan_closed_tx: AtomicU64::new(0),
            lan_prev_rx: AtomicU64::new(0),
            lan_prev_tx: AtomicU64::new(0),
            prev_rx: AtomicU64::new(0),
            prev_tx: AtomicU64::new(0),
            prev_at: AtomicU64::new(now_ms()),
            down_bps: Mutex::new(0.0),
            up_bps: Mutex::new(0.0),
            inflight: Mutex::new(None),
            closing,
        });
        let weak = network.weak.clone();
        tokio::spawn(async move {
            while let Some(connection) = incoming.recv().await {
                let Some(network) = weak.upgrade() else {
                    break;
                };
                if network.closing.load(Ordering::Acquire) {
                    break;
                }
                network.accept(connection);
            }
        });
        #[cfg(feature = "peeroxide-dht")]
        if private_routing
            && let Some(private) =
                start_private(&network, private_host, private_bootstrap, port).await
        {
            *lock(&network.private) = Some(private);
        }
        Ok(network)
    }

    /// Registers a bee's wakeup before any connection arrives.
    pub fn attach_wakeup(&self, bee: Arc<autobee::Autobee>) {
        lock(&self.wakeups).push(Arc::new(BeeWakeup(bee)));
    }

    /// A followed source's wakeup must ride the connections already open.
    pub fn attach_wakeup_now(&self, bee: Arc<autobee::Autobee>) {
        let sink = Arc::new(BeeWakeup(bee));
        for connection in self.connections() {
            sink.add_stream(connection.mux());
            sink.request_lookup();
        }
        lock(&self.wakeups).push(sink);
    }

    fn blocked(&self, public_key: &[u8; 32]) -> bool {
        let allowed = lock(&self.allowed);
        allowed
            .as_ref()
            .is_some_and(|set| !set.contains(&hex(public_key)))
    }

    fn accept(&self, connection: peeroxide::SwarmConnection) {
        if self.blocked(connection.remote_public_key()) {
            return;
        }
        let peer = *connection.remote_public_key();
        let socket = connection.peer.socket.clone();
        let bytes = WireBytes {
            rx: Arc::clone(&self.wire_rx),
            tx: Arc::clone(&self.wire_tx),
        };
        let mux = metered_mux(connection.peer.stream, &bytes);
        self.wire(
            mux,
            peer,
            false,
            false,
            Box::new(|| {}),
            vec![Box::new(socket)],
            None,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn wire(
        &self,
        mux: Mux,
        peer: [u8; 32],
        lan: bool,
        private: bool,
        close: Box<dyn Fn() + Send + Sync>,
        keepalive: Vec<Box<dyn Any + Send + Sync>>,
        bytes: Option<WireBytes>,
    ) -> ConnectionHandle {
        self.store.replicate(&mux);
        for sink in lock(&self.wakeups).iter() {
            sink.add_stream(&mux);
            // The published Autobee asks on its fast-forward search; a channel that never asks
            // would stall at the head it woke with, so the node asks for every bee as it pairs.
            sink.request_lookup();
        }
        if let Some(bytes) = &bytes {
            // The route's total must not drop when the connection goes: the counters stay.
            lock(&self.lan_wires).push(bytes.clone());
        }
        let handle = ConnectionHandle(Arc::new(ConnectionState {
            peer,
            mux,
            closed: AtomicBool::new(false),
            close,
            lan,
            private,
            _keepalive: keepalive,
        }));
        {
            let mut connections = lock(&self.connections);
            connections.retain(|connection| !connection.destroyed());
            connections.push(handle.clone());
        }
        // A connection the remote closed must leave the list (and drop its transport, which
        // frees the peer's swarm slot) without waiting for the next status poll.
        let weak = self.weak.clone();
        let watched = handle.mux().clone();
        tokio::spawn(async move {
            watched.closed().await;
            if let Some(network) = weak.upgrade() {
                lock(&network.connections).retain(|connection| !connection.destroyed());
            }
        });
        for (_, listener) in lock(&self.listeners).iter() {
            listener(handle.clone());
        }
        // Every protocol has registered its pairer by now; later unknown opens are refused.
        handle.mux().ready();
        handle
    }

    pub fn connections(&self) -> Vec<ConnectionHandle> {
        let mut connections = lock(&self.connections);
        connections.retain(|connection| !connection.destroyed());
        connections.clone()
    }

    pub fn connection_count(&self) -> u64 {
        self.connections().len() as u64
    }

    pub fn lan_connection_count(&self) -> u64 {
        self.connections()
            .iter()
            .filter(|connection| connection.is_lan())
            .count() as u64
    }

    pub fn dht(&self) -> &HyperDhtHandle {
        &self.dht
    }

    pub fn swarm(&self) -> &SwarmHandle {
        &self.swarm
    }

    pub fn peer_key(&self) -> [u8; 32] {
        self.swarm.key_pair().public_key
    }

    /// `swarm.keyPair`: the identity a LAN binding runs its own DHT with.
    pub fn key_pair(&self) -> &peeroxide::KeyPair {
        self.swarm.key_pair()
    }

    pub fn private_routing(&self) -> bool {
        self.private_routing
    }

    /// Whether topics may be announced on the public swarm or the LAN.
    ///
    /// A private-routed node's discovery belongs to the private overlay (`private_overlay::swarm`),
    /// which this build does not ship. Joining the public DHT or the LAN instead would publish the
    /// channel's discovery key, so it announces nothing: `status()` then shows no private route
    /// bytes, because there is no private route to carry them.
    fn can_announce(&self) -> bool {
        !self.private_routing
    }

    /// `setAllowedPeers`: a key not in the set is dropped on arrival.
    pub fn set_allowed_peers(&self, keys: Option<Vec<String>>) {
        let set = keys.filter(|keys| !keys.is_empty()).map(|keys| {
            keys.into_iter()
                .map(|key| key.to_lowercase())
                .collect::<HashSet<_>>()
        });
        *lock(&self.allowed) = set.clone();
        if let Some(set) = set {
            for connection in self.connections() {
                if !set.contains(&connection.remote_public_key()) {
                    connection.close();
                }
            }
        }
    }

    /// `onConnection(fn)`: the returned closure removes the listener.
    pub fn on_connection(&self, callback: Listener) -> Box<dyn Fn() + Send + Sync> {
        let id = self.next_listener.fetch_add(1, Ordering::AcqRel) + 1;
        lock(&self.listeners).push((id, callback));
        let weak = self.weak.clone();
        Box::new(move || {
            if let Some(network) = weak.upgrade() {
                lock(&network.listeners).retain(|(kept, _)| *kept != id);
            }
        })
    }

    /// `joinOverlayTopic`: one discovery per followed channel.
    pub fn join_overlay_topic(&self, id: &str, key: [u8; 32]) {
        if !self.can_announce() {
            // `swarm.joinSource`: the private overlay's rendezvous, not a public topic join.
            if !self.private_routing {
                return;
            }
            let private = { lock(&self.private).clone() };
            let Some(private) = private else {
                return;
            };
            if lock(&self.overlay_topics)
                .insert(id.to_owned(), key)
                .is_some()
            {
                return;
            }
            private.join_source(id, &key);
            return;
        }
        let mut topics = lock(&self.overlay_topics);
        if topics.contains_key(id) {
            return;
        }
        topics.insert(id.to_owned(), key);
        drop(topics);
        let topic = hypercore::crypto::discovery_key(&key);
        let swarm = self.swarm.clone();
        if let Some(lan) = lock(&self.lan).as_ref().map(|state| state.binding.clone()) {
            lan.join(topic);
        }
        let closing = self.closing.clone();
        tokio::spawn(async move {
            if closing.load(Ordering::Acquire) {
                return;
            }
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = true;
            let _ = swarm.join(topic, opts).await;
        });
    }

    pub async fn leave_overlay_topic(&self, id: &str) {
        let key = lock(&self.overlay_topics).remove(id);
        if !self.can_announce() {
            let private = { lock(&self.private).clone() };
            if let Some(private) = private {
                private.leave_source(id).await;
            }
            return;
        }
        if let Some(key) = key {
            let topic = hypercore::crypto::discovery_key(&key);
            let _ = self.swarm.leave(topic).await;
            if let Some(lan) = lock(&self.lan).as_ref().map(|state| state.binding.clone()) {
                lan.leave(topic);
            }
        }
    }

    /// `joinContentSwarm`: the stock Hypercore discoveryKey of a video core.
    pub fn join_content(&self, key: [u8; 32]) {
        if !self.can_announce() {
            return;
        }
        let mut topics = lock(&self.content_topics);
        if topics.contains_key(&key) {
            return;
        }
        if topics.len() >= MAX_CONTENT_TOPICS
            && let Some(victim) = topics.keys().next().copied()
        {
            topics.remove(&victim);
        }
        let discovery = hypercore::crypto::discovery_key(&key);
        topics.insert(key, discovery);
        drop(topics);
        let swarm = self.swarm.clone();
        if let Some(lan) = lock(&self.lan).as_ref().map(|state| state.binding.clone()) {
            lan.join(discovery);
        }
        tokio::spawn(async move {
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = true;
            let _ = swarm.join(discovery, opts).await;
        });
    }

    pub fn leave_content(&self, key: &[u8; 32]) {
        if let Some(discovery) = lock(&self.content_topics).remove(key) {
            let swarm = self.swarm.clone();
            tokio::spawn(async move {
                let _ = swarm.leave(discovery).await;
            });
        }
    }

    pub fn has_content_topic(&self, key: &[u8; 32]) -> bool {
        lock(&self.content_topics).contains_key(key)
    }

    pub fn content_topics(&self) -> Vec<[u8; 32]> {
        lock(&self.content_topics).values().copied().collect()
    }

    pub fn overlay_topic_count(&self) -> usize {
        lock(&self.overlay_topics).len()
    }

    /// `setLan`: swap the LAN binding and register its connections.
    pub async fn set_lan(
        &self,
        binding: Option<Arc<dyn LanBinding>>,
    ) -> Result<(), std::io::Error> {
        let previous = { lock(&self.lan).take() };
        if let Some(previous) = previous {
            (previous.closer)();
            previous.binding.destroy().await;
            self.fold_lan_bytes();
        }
        let Some(binding) = binding else {
            return Ok(());
        };
        if !self.can_announce() || self.closing.load(Ordering::Acquire) {
            binding.destroy().await;
            return Ok(());
        }
        for topic in lock(&self.content_topics).values() {
            binding.join(*topic);
        }
        for key in lock(&self.overlay_topics).values() {
            binding.join(hypercore::crypto::discovery_key(key));
        }
        let weak = self.weak.clone();
        let closer = binding.on_connection(Arc::new(move |connection| {
            let Some(network) = weak.upgrade() else {
                (connection.close)();
                return;
            };
            network.wire(
                connection.mux,
                connection.remote_public_key.unwrap_or_default(),
                true,
                false,
                connection.close,
                Vec::new(),
                Some(connection.bytes),
            );
        }));
        *lock(&self.lan) = Some(LanState { binding, closer });
        Ok(())
    }

    pub fn lan_stats(&self) -> Option<Value> {
        lock(&self.lan)
            .as_ref()
            .and_then(|state| state.binding.stats())
    }

    pub fn lan_endpoint(&self) -> Option<LanEndpoint> {
        let lan = lock(&self.lan)
            .as_ref()
            .map(|state| state.binding.clone())?;
        Some(LanEndpoint {
            host: lan.host()?,
            port: lan.port()?,
            public_key: hex(&self.peer_key()),
        })
    }

    pub fn has_lan(&self) -> bool {
        lock(&self.lan).is_some()
    }

    pub fn dial_lan_peer(&self, endpoint: &LanEndpoint) {
        if let Some(lan) = lock(&self.lan).as_ref().map(|state| state.binding.clone()) {
            lan.dial_direct(&endpoint.public_key, &endpoint.host, endpoint.port);
        }
    }

    /// `joinPeer`: dial a peer by key and wire the resulting stream into replication.
    ///
    /// Returns whether a dial was started. A private-routed node dials nothing: the public DHT is
    /// not its route, and the private overlay that would carry the dial is not in this build.
    pub fn join_peer(&self, key_hex: &str, lan_first: bool) -> bool {
        let Some(key) = parse_hex32(key_hex) else {
            return false;
        };
        if key == self.peer_key() {
            return false;
        }
        if !self.can_announce() {
            // `swarm.joinPeer`: the private overlay dials through the guards, never the DHT.
            let private = { lock(&self.private).clone() };
            let Some(private) = private else {
                return false;
            };
            private.join_peer(key);
            return true;
        }
        if lan_first && self.has_lan() {
            // The LAN dial is already in flight. JS joins through the DHT 1.5 s later rather
            // than not at all: a LAN path that never lands must not cost the peer.
            let weak = self.weak.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                if let Some(network) = weak.upgrade()
                    && !network.closing.load(Ordering::Acquire)
                {
                    network.dial_public(key);
                }
            });
            return true;
        }
        self.dial_public(key);
        true
    }

    /// The DHT half of `joinPeer`: connect by key and wire the stream into replication.
    fn dial_public(&self, key: [u8; 32]) {
        let weak = self.weak.clone();
        let key_pair = self.swarm.key_pair().clone();
        let dht = self.dht.clone();
        let runtime = Arc::clone(&self.runtime);
        let closing = self.closing.clone();
        tokio::spawn(async move {
            if closing.load(Ordering::Acquire) {
                return;
            }
            let connection = match dht.connect(&key_pair, key, &runtime).await {
                Ok(connection) => connection,
                Err(_) => return,
            };
            let Some(network) = weak.upgrade() else {
                return;
            };
            if network.blocked(&key) {
                return;
            }
            let socket = connection.socket.clone();
            let bytes = WireBytes {
                rx: Arc::clone(&network.wire_rx),
                tx: Arc::clone(&network.wire_tx),
            };
            let mux = metered_mux(connection.stream, &bytes);
            network.wire(
                mux,
                key,
                false,
                false,
                Box::new(|| {}),
                vec![Box::new(socket)],
                None,
            );
        });
    }

    pub fn inflight_range(&self) -> Option<(u32, u32)> {
        *lock(&self.inflight)
    }

    pub fn set_inflight_range(&self, range: (u32, u32)) {
        *lock(&self.inflight) = Some(range);
        self.store.set_inflight_range(range);
    }

    pub fn down_bps(&self) -> f64 {
        *lock(&self.down_bps)
    }

    pub fn up_bps(&self) -> f64 {
        *lock(&self.up_bps)
    }

    /// The LAN route's bytes: every connection it ever carried, closed ones included.
    fn lan_totals(&self) -> (u64, u64) {
        let mut rx = self.lan_closed_rx.load(Ordering::Relaxed);
        let mut tx = self.lan_closed_tx.load(Ordering::Relaxed);
        for bytes in lock(&self.lan_wires).iter() {
            rx += bytes.rx();
            tx += bytes.tx();
        }
        (rx, tx)
    }

    /// A replaced LAN binding's counters join the route's closed totals.
    fn fold_lan_bytes(&self) {
        let (rx, tx) = self.lan_totals();
        self.lan_closed_rx.store(rx, Ordering::Relaxed);
        self.lan_closed_tx.store(tx, Ordering::Relaxed);
        lock(&self.lan_wires).clear();
    }

    /// `sample()`: whole-node totals and rates (rates move at most every 500 ms).
    ///
    /// Bytes are counted where they cross the mux ([`metered_mux`]): peeroxide exposes no
    /// per-connection counters and its DHT counters never see established UDX streams. The
    /// swarm's connections are the public route; a private-routed node has no overlay
    /// connection to carry its bytes, so nothing is ever labelled private, and a LAN binding
    /// hands over its connection's counters, so the LAN route counts exactly its peers.
    pub fn sample(&self) -> Sample {
        let now = now_ms();
        let prev_at = self.prev_at.load(Ordering::Relaxed);
        let dt = now.saturating_sub(prev_at);
        let public_rx = self.wire_rx.load(Ordering::Relaxed);
        let public_tx = self.wire_tx.load(Ordering::Relaxed);
        let (lan_rx, lan_tx) = self.lan_totals();
        let private_rx = self.private_rx.load(Ordering::Relaxed);
        let private_tx = self.private_tx.load(Ordering::Relaxed);
        let private_peers = self.private_peers.load(Ordering::Relaxed);
        let (rx, tx) = (
            public_rx + lan_rx + private_rx,
            public_tx + lan_tx + private_tx,
        );
        let peers = self.connection_count();
        let mut public = self.public;
        let mut lan = self.lan_bucket;
        public.closed_rx = public_rx;
        public.closed_tx = public_tx;
        lan.closed_rx = lan_rx;
        lan.closed_tx = lan_tx;
        let prev_rx = self.prev_rx.load(Ordering::Relaxed);
        let prev_tx = self.prev_tx.load(Ordering::Relaxed);
        if dt >= 500 {
            *lock(&self.down_bps) = ((rx.saturating_sub(prev_rx)) as f64 * 1000.0 / dt as f64)
                .round()
                .max(0.0);
            *lock(&self.up_bps) = ((tx.saturating_sub(prev_tx)) as f64 * 1000.0 / dt as f64)
                .round()
                .max(0.0);
            public.down_bps = self.down_bps();
            public.up_bps = self.up_bps();
            let lan_prev_rx = self.lan_prev_rx.load(Ordering::Relaxed);
            let lan_prev_tx = self.lan_prev_tx.load(Ordering::Relaxed);
            lan.down_bps = ((lan_rx.saturating_sub(lan_prev_rx)) as f64 * 1000.0 / dt as f64)
                .round()
                .max(0.0);
            lan.up_bps = ((lan_tx.saturating_sub(lan_prev_tx)) as f64 * 1000.0 / dt as f64)
                .round()
                .max(0.0);
            self.lan_prev_rx.store(lan_rx, Ordering::Relaxed);
            self.lan_prev_tx.store(lan_tx, Ordering::Relaxed);
            self.prev_rx.store(rx, Ordering::Relaxed);
            self.prev_tx.store(tx, Ordering::Relaxed);
            self.prev_at.store(now, Ordering::Relaxed);
        }
        let lan_peers = self.lan_connection_count();
        let public_peers = peers.saturating_sub(lan_peers + private_peers);
        let public_route = public.route(public_peers, public.closed_rx, public.closed_tx);
        // The private overlay's own route; nothing when it never started.
        let private_route = Bucket::default().route(private_peers, private_rx, private_tx);
        let lan_route = lan.route(lan_peers, lan.closed_rx, lan.closed_tx);
        Sample {
            routes: Routes {
                private: private_route,
                public: public_route,
                lan: lan_route,
            },
            rx_bytes: rx,
            tx_bytes: tx,
            down_bps: self.down_bps(),
            up_bps: self.up_bps(),
            contributing_peers: peers,
            rtt: 0,
            retransmits: 0,
        }
    }

    /// `close()`: drop every connection, the LAN binding and the swarm.
    pub async fn close(&self) {
        for connection in self.connections() {
            connection.close();
        }
        lock(&self.connections).clear();
        lock(&self.wakeups).clear();
        let lan = { lock(&self.lan).take() };
        if let Some(state) = lan {
            (state.closer)();
            state.binding.destroy().await;
        }
        let private = { lock(&self.private).take() };
        if let Some(private) = private {
            private.destroy().await;
        }
        let _ = self.swarm.destroy().await;
    }

    /// `flush()`: wait for the joined topics' initial discovery, bounded.
    pub async fn flush(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.swarm.flush()).await;
    }

    /// A subscription that runs an async handler for each connection.
    pub fn watch_connection<F, Fut>(&self, handler: F) -> Box<dyn Fn() + Send + Sync>
    where
        F: Fn(ConnectionHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.on_connection(Arc::new(move |connection| {
            let future = handler(connection);
            tokio::spawn(future);
        }))
    }
}

impl super::blobs::ContentSwarm for Arc<Network> {
    fn join(&self, key: [u8; 32]) {
        Network::join_content(self, key);
    }
    fn leave(&self, key: [u8; 32]) {
        Network::leave_content(self, &key);
    }
    fn down_bps(&self) -> f64 {
        Network::down_bps(self)
    }
    fn up_bps(&self) -> f64 {
        Network::up_bps(self)
    }
    fn inflight_range(&self) -> Option<(u32, u32)> {
        Network::inflight_range(self)
    }
}

/// `Node::Swarm`.
pub struct NetworkSwarm {
    pub network: Arc<Network>,
}

impl super::api::Swarm for NetworkSwarm {
    type Error = std::io::Error;

    async fn suspend(&self) -> Result<(), Self::Error> {
        for connection in self.network.connections() {
            connection.close();
        }
        let private = { lock(&self.network.private).clone() };
        if let Some(private) = private {
            private.suspend().await;
        }
        Ok(())
    }

    async fn resume(&self) -> Result<(), Self::Error> {
        let private = { lock(&self.network.private).clone() };
        if let Some(private) = private {
            private.resume().await;
        }
        Ok(())
    }

    fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The app sends no bootstrap (the normal case): an empty list must reach the public DHT.
    #[test]
    fn empty_bootstrap_means_the_public_network() {
        let public: Vec<String> = peeroxide_dht::hyperdht::DEFAULT_BOOTSTRAP
            .iter()
            .map(|node| (*node).to_owned())
            .collect();
        assert_eq!(resolve_bootstrap(Vec::new()), public);
        assert_eq!(bootstrap_list(None), Vec::<String>::new());
        assert_eq!(
            resolve_bootstrap(bootstrap_list(Some(&serde_json::json!([])))),
            public
        );
        assert_eq!(
            resolve_bootstrap(bootstrap_list(Some(&serde_json::json!(null)))),
            public
        );
    }

    #[test]
    fn an_explicit_bootstrap_is_kept() {
        let given = vec!["127.0.0.1:9".to_owned()];
        assert_eq!(resolve_bootstrap(given.clone()), given);
        assert_eq!(
            resolve_bootstrap(bootstrap_list(Some(&serde_json::json!([
                { "host": "127.0.0.1", "port": 9 }
            ])))),
            given
        );
    }
}
