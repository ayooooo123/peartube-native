//! `src/node/network.js`: the swarm, its topics, replication and the traffic sample.
//!
//! Every accepted connection becomes one Protomux multiplexer over its Noise stream; the
//! Corestore replicates every open core over it (and opens stored cores a peer asks for), the
//! channel's wakeup session rides it, and each followed source attaches its own wakeup.
//!
//! The peeroxide swarm has no `joinPeer`: a direct peer dials the DHT itself and wires the
//! resulting stream into the same replication set.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::Duration;

use corestore::Corestore;
use futures_util::future::BoxFuture;
use libudx::UdxRuntime;
use peeroxide::{JoinOpts, SwarmConfig, SwarmHandle};
use peeroxide_dht::hyperdht::{HyperDhtConfig, HyperDhtHandle};
use peeroxide_dht::rpc::DhtConfig;
use protomux::Mux;
use serde_json::Value;

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
        self.0.closed.load(Ordering::Acquire)
    }
    pub fn is_lan(&self) -> bool {
        self.0.lan
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
        let mux = Mux::from_secret_stream(connection.peer.stream);
        self.wire(mux, peer, false, Box::new(|| {}), vec![Box::new(socket)]);
    }

    fn wire(
        &self,
        mux: Mux,
        peer: [u8; 32],
        lan: bool,
        close: Box<dyn Fn() + Send + Sync>,
        keepalive: Vec<Box<dyn Any + Send + Sync>>,
    ) -> ConnectionHandle {
        self.store.replicate(&mux);
        for sink in lock(&self.wakeups).iter() {
            sink.add_stream(&mux);
            // The published Autobee asks on its fast-forward search; a channel that never asks
            // would stall at the head it woke with, so the node asks for every bee as it pairs.
            sink.request_lookup();
        }
        let handle = ConnectionHandle(Arc::new(ConnectionState {
            peer,
            mux,
            closed: AtomicBool::new(false),
            close,
            lan,
            _keepalive: keepalive,
        }));
        {
            let mut connections = lock(&self.connections);
            connections.retain(|connection| !connection.destroyed());
            connections.push(handle.clone());
        }
        for (_, listener) in lock(&self.listeners).iter() {
            listener(handle.clone());
        }
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
        let lan = lock(&self.lan).as_ref().map(|state| state.binding.clone());
        let closing = self.closing.clone();
        tokio::spawn(async move {
            if closing.load(Ordering::Acquire) {
                return;
            }
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = true;
            let _ = swarm.join(topic, opts).await;
            if let Some(lan) = lan {
                lan.join(topic);
            }
        });
    }

    pub async fn leave_overlay_topic(&self, id: &str) {
        let key = lock(&self.overlay_topics).remove(id);
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
        let lan = lock(&self.lan).as_ref().map(|state| state.binding.clone());
        tokio::spawn(async move {
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = true;
            let _ = swarm.join(discovery, opts).await;
            if let Some(lan) = lan {
                lan.join(discovery);
            }
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
                connection.close,
                Vec::new(),
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
        if !self.can_announce() {
            return false;
        }
        let Some(key) = parse_hex32(key_hex) else {
            return false;
        };
        if key == self.peer_key() {
            return false;
        }
        if lan_first && self.has_lan() {
            return false;
        }
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
            let mux = Mux::from_secret_stream(connection.stream);
            network.wire(mux, key, false, Box::new(|| {}), vec![Box::new(socket)]);
        });
        true
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

    /// `sample()`: whole-node totals and rates (rates move at most every 500 ms).
    ///
    /// The peeroxide connection exposes no per-connection byte counters, so the DHT's wire
    /// counters are attributed to the route that carries traffic; the LAN route counts its peers.
    /// The DHT here is the public swarm: a private-routed node has no overlay connection to carry
    /// its bytes, so nothing is ever labelled private.
    pub fn sample(&self) -> Sample {
        let now = now_ms();
        let prev_at = self.prev_at.load(Ordering::Relaxed);
        let dt = now.saturating_sub(prev_at);
        let (rx, tx) = self.dht.wire_stats();
        let peers = self.connection_count();
        let mut public = self.public;
        let lan = self.lan_bucket;
        public.closed_rx = rx;
        public.closed_tx = tx;
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
            self.prev_rx.store(rx, Ordering::Relaxed);
            self.prev_tx.store(tx, Ordering::Relaxed);
            self.prev_at.store(now, Ordering::Relaxed);
        }
        let public_route = public.route(peers, public.closed_rx, public.closed_tx);
        // No private overlay: the private route carries nothing.
        let private_route = Bucket::default().route(0, 0, 0);
        let lan_route = lan.route(self.lan_connection_count(), lan.closed_rx, lan.closed_tx);
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
        Ok(())
    }

    async fn resume(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}
