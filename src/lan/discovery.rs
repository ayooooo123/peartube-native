//! `lan.js`'s `LanDiscovery`: an isolated, bootstrap-free HyperDHT whose only peers are the ones
//! mDNS handed out. The mDNS half lives in [`super::mdns`]; the DHT half is specified in
//! `docs/port-notes-lan.md`.
//!
//! No announcement is ever issued: topics travel in mDNS TXT only, and a peer is dialled when it
//! shares at least one token. The peer with the smaller public key dials immediately, the larger
//! one after [`DIAL_AFTER`], so a one-way firewall still gets a connection.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::future::BoxFuture;
use libudx::UdxRuntime;
use peeroxide::{JoinOpts, KeyPair, SwarmConfig, SwarmHandle};
use peeroxide_dht::hyperdht::HyperDhtConfig;
use peeroxide_dht::messages::Ipv4Peer;
use peeroxide_dht::rpc::DhtConfig;
use protomux::Mux;
use serde_json::{Value, json};

use super::mdns::{
    Advert, BonjourAdapter, Browse, DiscoveredService, ErrorSink, SelfAddressAdapter, ServiceSink,
    create_records, create_records_with, topic_token,
};
use crate::node::network::{LanBinding, LanConnection, WireBytes, metered_mux};

/// How long the larger public key waits before dialling a peer (`dialAfter`).
pub const DIAL_AFTER: Duration = Duration::from_secs(3);
/// How long the smaller public key waits before redialling a closed peer.
const REDIAL: Duration = Duration::from_millis(500);
/// The server-registration topic: the isolated DHT never announces it.
const SERVER_TOPIC: [u8; 32] = [0u8; 32];

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    out
}

/// A LAN binding failure the status line shows as `failed on <host>: <error>`.
#[derive(Debug)]
pub struct LanError(String);

impl LanError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for LanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LanError {}

/// One `on_connection` subscriber; the node registers exactly one (`setLan`).
type Listener = Arc<dyn Fn(LanConnection) + Send + Sync>;

/// One peer mDNS told us about.
#[derive(Clone)]
struct Peer {
    public_key: [u8; 32],
    host: Ipv4Addr,
    port: u16,
    tokens: Vec<String>,
}

/// The shards one advertisement generation delivered (`_peerShards`).
struct Shards {
    generation: Option<String>,
    total_shards: usize,
    shards: HashMap<usize, Vec<String>>,
    prev_tokens: Vec<String>,
}

struct Inner {
    /// The backend's runtime: mDNS callbacks arrive on mdns-sd's own thread, so nothing here may
    /// call `tokio::spawn` on the ambient context.
    runtime: tokio::runtime::Handle,
    key_pair: KeyPair,
    peer_key: String,
    swarm: SwarmHandle,
    port: AtomicU16,
    adapter: SelfAddressAdapter,
    /// Joined mDNS topic tokens, indexed directly for incoming advertisement lookups.
    topics: Mutex<HashSet<String>>,
    peers: Mutex<HashMap<String, Peer>>,
    shards: Mutex<HashMap<String, Shards>>,
    /// One live connection per peer: id → its serial and mux. Both sides can dial each other at
    /// once (`dialDirect` on the receiver and on the sender), and two muxes between the same pair
    /// never pair their protocols, so the accepted connection replaces our own.
    active: Mutex<HashMap<String, (u64, Mux)>>,
    next_serial: AtomicU64,
    pending: Mutex<HashSet<String>>,
    direct: Mutex<HashSet<String>>,
    timers: Mutex<HashMap<String, tokio::task::JoinHandle<()>>>,
    listeners: Mutex<Vec<(u64, Listener)>>,
    next_listener: AtomicU64,
    ads: Mutex<Vec<Advert>>,
    ad_sig: tokio::sync::Mutex<String>,
    browse: Mutex<Option<Browse>>,
    accept: Mutex<Option<tokio::task::JoinHandle<()>>>,
    destroyed: AtomicBool,
}

/// `createLan({ host, keyPair, adapter })`.
pub struct LanDiscovery {
    inner: Arc<Inner>,
}

impl LanDiscovery {
    /// Opens the isolated DHT, registers its server and starts browsing mDNS.
    pub async fn open(host: Ipv4Addr, key_pair: KeyPair) -> Result<Arc<Self>, LanError> {
        // Each discovery owns its advertisements and browser; closing it must not stop another
        // node's discovery when tests host several nodes in one process.
        Self::open_with_adapter(
            SelfAddressAdapter::new(BonjourAdapter::new(), host),
            key_pair,
        )
        .await
    }

    /// Opens the isolated DHT with the Bonjour backend the host supplies (a device whose
    /// platform daemon is reached over the line protocol, see [`super::host`]).
    pub async fn open_with_adapter(
        adapter: SelfAddressAdapter,
        key_pair: KeyPair,
    ) -> Result<Arc<Self>, LanError> {
        let mut config = SwarmConfig::default();
        config.key_pair = Some(key_pair.clone());
        let mut dht = DhtConfig::default();
        dht.bootstrap = Vec::new();
        dht.host = adapter.host().to_string();
        dht.port = 0;
        dht.ephemeral = Some(false);
        dht.firewalled = false;
        dht.quick_firewall = false;
        let mut hyper = HyperDhtConfig::default();
        hyper.dht = dht;
        config.dht = hyper;
        let (actor, swarm, mut incoming) = peeroxide::spawn(config)
            .await
            .map_err(|error| LanError::new(error.to_string()))?;
        let port = swarm.dht().local_port().await.unwrap_or(0);
        let runtime = tokio::runtime::Handle::current();
        let inner = Arc::new(Inner {
            runtime: runtime.clone(),
            peer_key: hex(&key_pair.public_key),
            key_pair,
            swarm,
            port: AtomicU16::new(port),
            adapter,
            topics: Mutex::new(HashSet::new()),
            peers: Mutex::new(HashMap::new()),
            shards: Mutex::new(HashMap::new()),
            active: Mutex::new(HashMap::new()),
            next_serial: AtomicU64::new(0),
            pending: Mutex::new(HashSet::new()),
            direct: Mutex::new(HashSet::new()),
            timers: Mutex::new(HashMap::new()),
            listeners: Mutex::new(Vec::new()),
            next_listener: AtomicU64::new(1),
            ads: Mutex::new(Vec::new()),
            ad_sig: tokio::sync::Mutex::new(String::new()),
            browse: Mutex::new(None),
            accept: Mutex::new(None),
            destroyed: AtomicBool::new(false),
        });
        // Register the DHT server before `open` reports a port: a peer that dials us in the window
        // between the socket binding and the server registering gets nothing, and an unregistered
        // server has to wait for the next resolve. `lan.js` does the same by `listen()`ing before
        // it advertises.
        {
            let mut opts = JoinOpts::default();
            opts.server = true;
            opts.client = false;
            let _ = inner.swarm.join(SERVER_TOPIC, opts).await;
        }
        let weak = Arc::downgrade(&inner);
        let accept = inner.runtime.spawn(async move {
            while let Some(connection) = incoming.recv().await {
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                Inner::on_incoming(&inner, connection);
            }
            drop(actor);
        });
        *lock(&inner.accept) = Some(accept);

        let up: ServiceSink = {
            let weak = Arc::downgrade(&inner);
            Arc::new(move |service: &DiscoveredService| {
                if let Some(inner) = weak.upgrade() {
                    Inner::on_service(&inner, service);
                }
            })
        };
        let down: ServiceSink = {
            let weak = Arc::downgrade(&inner);
            Arc::new(move |service: &DiscoveredService| {
                if let Some(inner) = weak.upgrade() {
                    Inner::on_service_down(&inner, service);
                }
            })
        };
        let on_error: ErrorSink = Arc::new(|_message: &str| {});
        let browse = match inner.adapter.browse(up, Some(down), Some(on_error)) {
            Ok(browse) => browse,
            Err(error) => {
                inner.destroyed.store(true, Ordering::Release);
                let _ = inner.swarm.destroy().await;
                if let Some(accept) = lock(&inner.accept).take() {
                    accept.abort();
                }
                return Err(LanError::new(error.to_string()));
            }
        };
        *lock(&inner.browse) = Some(browse);
        // `_syncAds` runs on open too: an advertisement without tokens is how a peer first shows up.
        inner.runtime.spawn(Inner::sync_ads(inner.clone()));
        Ok(Arc::new(Self { inner }))
    }
}

impl Inner {
    fn shares_topic(&self, tokens: &[String]) -> bool {
        if tokens.is_empty() {
            return false;
        }
        let topics = lock(&self.topics);
        !topics.is_empty() && tokens.iter().any(|token| topics.contains(token))
    }

    fn cancel(&self, id: &str) {
        if let Some(timer) = lock(&self.timers).remove(id) {
            timer.abort();
        }
    }

    // ------------------------------------------------------------- mDNS

    fn on_service(self: &Arc<Self>, service: &DiscoveredService) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        // Ignore our own advertisement before allocating and validating its topic strings.
        if super::mdns::txt_get(&service.txt, "peerKey").eq_ignore_ascii_case(&self.peer_key) {
            return;
        }
        let Some(record) = service.parse() else {
            return;
        };
        let Some(host) = service.ipv4() else {
            return;
        };
        let id = record.peer_key_hex.clone();
        let previous = lock(&self.peers).get(&id).map(|peer| peer.tokens.clone());
        let tokens = {
            let mut shards = lock(&self.shards);
            let stale = shards
                .get(&id)
                .is_none_or(|state| state.generation != record.generation);
            if stale {
                let prev_tokens = if record.total_shards > 1 {
                    previous.clone().unwrap_or_default()
                } else {
                    Vec::new()
                };
                shards.insert(
                    id.clone(),
                    Shards {
                        generation: record.generation.clone(),
                        total_shards: record.total_shards,
                        shards: HashMap::new(),
                        prev_tokens,
                    },
                );
            }
            let state = shards.get_mut(&id).expect("shard state inserted");
            state.total_shards = record.total_shards;
            state.shards.insert(record.shard, record.tokens.clone());
            if state.shards.len() >= state.total_shards {
                state.prev_tokens.clear();
            }
            let mut tokens: Vec<String> = state.prev_tokens.clone();
            for shard in state.shards.values() {
                for token in shard {
                    if !tokens.contains(token) {
                        tokens.push(token.clone());
                    }
                }
            }
            tokens
        };

        let peer = Peer {
            public_key: record.peer_key,
            host,
            port: record.port,
            tokens,
        };
        lock(&self.peers).insert(id, peer.clone());
        self.consider(peer);
    }

    fn on_service_down(self: &Arc<Self>, service: &DiscoveredService) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let Some(record) = service.parse() else {
            return;
        };
        let id = record.peer_key_hex.clone();
        let remaining = {
            let mut shards = lock(&self.shards);
            let state = match shards.get_mut(&id) {
                Some(state) => state,
                None => return,
            };
            if record.generation.is_some()
                && state.generation.is_some()
                && record.generation != state.generation
            {
                return;
            }
            state.shards.remove(&record.shard);
            if state.shards.is_empty() {
                shards.remove(&id);
                None
            } else {
                let mut tokens: Vec<String> = Vec::new();
                for shard in state.shards.values() {
                    for token in shard {
                        if !tokens.contains(token) {
                            tokens.push(token.clone());
                        }
                    }
                }
                Some(tokens)
            }
        };
        match remaining {
            Some(tokens) => {
                let updated = {
                    let mut peers = lock(&self.peers);
                    peers.get_mut(&id).map(|peer| {
                        peer.tokens = tokens;
                        peer.clone()
                    })
                };
                if let Some(peer) = updated {
                    self.consider(peer);
                }
            }
            None => {
                lock(&self.peers).remove(&id);
                self.cancel(&id);
            }
        }
    }

    /// `_syncAds`: re-advertise only when the port or the token set changed.
    async fn sync_ads(self: Arc<Self>) {
        let mut ad_sig = self.ad_sig.lock().await;
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let mut tokens: Vec<String> = lock(&self.topics).iter().cloned().collect();
        tokens.sort_unstable();
        let port = self.port.load(Ordering::Acquire);
        let shape: Vec<Vec<(String, String)>> =
            create_records_with(&self.peer_key, port, &tokens, "")
                .iter()
                .map(|record| record.txt.clone())
                .collect();
        let signature = format!("{port}{shape:?}");
        if *ad_sig == signature {
            return;
        }
        let mut next = Vec::new();
        for record in create_records(&self.peer_key, port, &tokens) {
            match self.adapter.advertise(&record, None) {
                Ok(advertisement) => next.push(advertisement),
                Err(_) => {
                    for advertisement in &next {
                        advertisement.stop();
                    }
                    return;
                }
            }
        }
        if self.destroyed.load(Ordering::Acquire) {
            for advertisement in &next {
                advertisement.stop();
            }
            return;
        }
        let previous = std::mem::replace(&mut *lock(&self.ads), next);
        *ad_sig = signature;
        for advertisement in &previous {
            advertisement.stop();
        }
    }

    // ------------------------------------------------------------- dialling

    fn on_incoming(self: &Arc<Self>, connection: peeroxide::SwarmConnection) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let peer = *connection.remote_public_key();
        let id = hex(&peer);
        // No token check here. An advertisement is a snapshot: a peer that just joined the topic we
        // share (or that dialled us straight by address, `dialDirect`) still shows its previous
        // token set for a moment, so rejecting on it drops exactly the dial `dialDirect` exists to
        // make. The dialler already has our public key, and no core or wakeup capability comes with
        // a connection: replication still needs the discovery key or the bee's key.
        let socket = connection.peer.socket.clone();
        let bytes = WireBytes::with_transport(connection.peer.stream.abort_handle());
        let mux = metered_mux(connection.peer.stream, &bytes);
        // The swarm retires the connection when its stream closes, so a redial finds the slot
        // free; only the socket must outlive the mux.
        self.wire(id, peer, mux, bytes, true, Box::new(socket));
    }

    fn consider(self: &Arc<Self>, peer: Peer) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let id = hex(&peer.public_key);
        // A connected peer needs no dial decision or topic-intersection scan.
        if lock(&self.active).contains_key(&id) {
            return;
        }
        let direct = lock(&self.direct).contains(&id);
        if !direct && !self.shares_topic(&peer.tokens) {
            self.cancel(&id);
            return;
        }
        if lock(&self.active).contains_key(&id) || lock(&self.pending).contains(&id) {
            return;
        }
        if direct || self.key_pair.public_key < peer.public_key {
            self.dial(peer);
        } else if !lock(&self.timers).contains_key(&id) {
            self.schedule(id, DIAL_AFTER);
        }
    }

    fn dial(self: &Arc<Self>, peer: Peer) {
        let id = hex(&peer.public_key);
        if !lock(&self.pending).insert(id.clone()) {
            return;
        }
        let inner = self.clone();
        self.runtime.spawn(async move {
            let runtime = match UdxRuntime::new() {
                Ok(runtime) => runtime,
                Err(_) => {
                    lock(&inner.pending).remove(&id);
                    return;
                }
            };
            let addresses = [Ipv4Peer {
                host: peer.host.to_string(),
                port: peer.port,
            }];
            let connected = inner
                .swarm
                .dht()
                .connect_with_nodes(&inner.key_pair, peer.public_key, &addresses, &runtime)
                .await;
            let Ok(connection) = connected else {
                lock(&inner.pending).remove(&id);
                // A refused or racing handshake is not the peer going away: the other side may
                // still be binding its socket, so re-arm the dial instead of dropping the peer.
                if !inner.destroyed.load(Ordering::Acquire) {
                    Inner::retry(&inner, peer, DIAL_AFTER);
                }
                return;
            };
            if inner.destroyed.load(Ordering::Acquire) {
                return;
            }
            let socket = connection.socket.clone();
            let bytes = WireBytes::with_transport(connection.stream.abort_handle());
            let mux = metered_mux(connection.stream, &bytes);
            Inner::wire(
                &inner,
                id,
                peer.public_key,
                mux,
                bytes,
                false,
                Box::new((socket, runtime)),
            );
        });
    }

    fn wire(
        self: &Arc<Self>,
        id: String,
        peer: [u8; 32],
        mux: Mux,
        bytes: WireBytes,
        incoming: bool,
        keepalive: Box<dyn std::any::Any + Send + Sync>,
    ) {
        self.cancel(&id);
        lock(&self.pending).remove(&id);
        let serial = self.next_serial.fetch_add(1, Ordering::AcqRel) + 1;
        // Both ends may dial at once (`dialDirect` on the receiver and on the sender). Two muxes
        // between one pair never pair their protocols, so exactly one is kept, and both ends pick
        // it by the same rule: the connection the smaller public key initiated.
        let canonical = incoming == (peer < self.key_pair.public_key);
        {
            let mut active = lock(&self.active);
            if active.contains_key(&id) {
                if !canonical {
                    // Closing it tells the other end, whose rule drops this one too.
                    mux.destroy();
                    return;
                }
                if let Some((_, existing)) = active.get(&id) {
                    existing.destroy();
                }
            }
            active.insert(id.clone(), (serial, mux.clone()));
        }
        let callback = lock(&self.listeners)
            .first()
            .map(|(_, callback)| callback.clone());
        let Some(callback) = callback else {
            lock(&self.active).remove(&id);
            return;
        };

        // A closed mux is the Rust socket's `close` event: re-arm the dial, as `_onSocketClosed` does.
        let watcher = mux.clone();
        let owner = Arc::downgrade(self);
        let watched = id.clone();
        self.runtime.spawn(async move {
            watcher.closed().await;
            if let Some(inner) = owner.upgrade() {
                Inner::on_closed(&inner, &watched, serial);
            }
        });
        // The transport (socket, runtime) must outlive the mux; the close closure owns it.
        let keepalive = Mutex::new(Some(keepalive));
        let close: Box<dyn Fn() + Send + Sync> = {
            let inner = Arc::downgrade(self);
            let id = id.clone();
            Box::new(move || {
                let _ = lock(&keepalive).take();
                if let Some(inner) = inner.upgrade() {
                    Inner::on_closed(&inner, &id, serial);
                }
            })
        };
        // The callback registered every pairer; unknown opens can be rejected from here on.
        let ready = mux.clone();
        callback(LanConnection {
            mux,
            remote_public_key: Some(peer),
            close,
            bytes,
        });
        ready.ready();
    }

    fn on_closed(self: &Arc<Self>, id: &str, serial: u64) {
        {
            let mut active = lock(&self.active);
            // A replaced connection's close must not take the live one's entry.
            if active.get(id).map(|(kept, _)| *kept) != Some(serial) {
                return;
            }
            active.remove(id);
        }
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let peer = lock(&self.peers).get(id).cloned();
        let Some(peer) = peer else {
            return;
        };
        let direct = lock(&self.direct).contains(id);
        if (!direct && !self.shares_topic(&peer.tokens)) || lock(&self.timers).contains_key(id) {
            return;
        }
        let wait = if direct || self.key_pair.public_key < peer.public_key {
            REDIAL
        } else {
            DIAL_AFTER
        };
        self.schedule(id.to_owned(), wait);
    }

    /// Re-arms one failed dial; it stops once the peer is neither advertised nor a direct dial.
    fn retry(self: &Arc<Self>, peer: Peer, wait: Duration) {
        let id = hex(&peer.public_key);
        self.cancel(&id);
        let weak = Arc::downgrade(self);
        let fired = id.clone();
        let task = self.runtime.spawn(async move {
            tokio::time::sleep(wait).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            lock(&inner.timers).remove(&fired);
            if inner.destroyed.load(Ordering::Acquire)
                || lock(&inner.active).contains_key(&fired)
                || lock(&inner.pending).contains(&fired)
            {
                return;
            }
            let known =
                lock(&inner.direct).contains(&fired) || lock(&inner.peers).contains_key(&fired);
            if known {
                // A direct peer dials its address; a discovered one re-reads the table, whose
                // tokens may have changed since this retry was armed.
                let peer = lock(&inner.peers).get(&fired).cloned().unwrap_or(peer);
                inner.dial(peer);
            }
        });
        lock(&self.timers).insert(id, task);
    }

    fn schedule(self: &Arc<Self>, id: String, wait: Duration) {
        self.cancel(&id);
        let weak = Arc::downgrade(self);
        let fired = id.clone();
        let task = self.runtime.spawn(async move {
            tokio::time::sleep(wait).await;
            let Some(inner) = weak.upgrade() else {
                return;
            };
            lock(&inner.timers).remove(&fired);
            if inner.destroyed.load(Ordering::Acquire)
                || lock(&inner.active).contains_key(&fired)
                || lock(&inner.pending).contains(&fired)
            {
                return;
            }
            let peer = lock(&inner.peers).get(&fired).cloned();
            if let Some(peer) = peer
                && (lock(&inner.direct).contains(&fired) || inner.shares_topic(&peer.tokens))
            {
                inner.dial(peer);
            }
        });
        lock(&self.timers).insert(id, task);
    }

    fn dial_direct(self: &Arc<Self>, public_key: &str, host: &str, port: u16) {
        if self.destroyed.load(Ordering::Acquire) {
            return;
        }
        let Ok(host) = host.parse::<Ipv4Addr>() else {
            return;
        };
        if port == 0 {
            return;
        }
        let Some(public_key) = unhex32(public_key) else {
            return;
        };
        let id = hex(&public_key);
        if id == self.peer_key {
            return;
        }
        lock(&self.direct).insert(id.clone());
        let peer = Peer {
            public_key,
            host,
            port,
            tokens: Vec::new(),
        };
        // A direct peer is known by address, not by an advertisement: keeping it in the table is
        // what lets a closed connection redial it (`_onSocketClosed` has no address otherwise).
        lock(&self.peers).insert(id.clone(), peer.clone());
        if lock(&self.active).contains_key(&id) || lock(&self.pending).contains(&id) {
            return;
        }
        self.dial(peer);
    }

    fn stats(&self) -> Value {
        json!({
            "peers": lock(&self.peers).len(),
            "connections": lock(&self.active).len(),
            "topics": lock(&self.topics).len(),
            "direct": lock(&self.direct).len(),
        })
    }
}

fn unhex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(text.get(index * 2..index * 2 + 2)?, 16).ok()?;
    }
    Some(key)
}

impl LanBinding for LanDiscovery {
    fn join(&self, topic: [u8; 32]) {
        let token = topic_token(&topic);
        if !lock(&self.inner.topics).insert(token) {
            return;
        }
        let inner = self.inner.clone();
        self.inner
            .runtime
            .spawn(async move { Inner::sync_ads(inner).await });
        let peers: Vec<Peer> = lock(&self.inner.peers).values().cloned().collect();
        for peer in peers {
            self.inner.consider(peer);
        }
    }

    fn leave(&self, topic: [u8; 32]) {
        let token = topic_token(&topic);
        if !lock(&self.inner.topics).remove(&token) {
            return;
        }
        let inner = self.inner.clone();
        self.inner
            .runtime
            .spawn(async move { Inner::sync_ads(inner).await });
        let peers: Vec<Peer> = lock(&self.inner.peers).values().cloned().collect();
        for peer in peers {
            if !self.inner.shares_topic(&peer.tokens) {
                self.inner.cancel(&hex(&peer.public_key));
            }
        }
    }

    fn destroy(&self) -> BoxFuture<'_, ()> {
        self.inner.destroyed.store(true, Ordering::Release);
        let timers: Vec<_> = lock(&self.inner.timers)
            .drain()
            .map(|(_, timer)| timer)
            .collect();
        for timer in timers {
            timer.abort();
        }
        if let Some(browse) = lock(&self.inner.browse).take() {
            browse.stop();
        }
        let ads: Vec<_> = lock(&self.inner.ads).drain(..).collect();
        for advertisement in ads {
            advertisement.stop();
        }
        lock(&self.inner.pending).clear();
        lock(&self.inner.active).clear();
        let inner = self.inner.clone();
        Box::pin(async move {
            let _ = inner.swarm.destroy().await;
            let accept = lock(&inner.accept).take();
            if let Some(accept) = accept {
                accept.abort();
            }
        })
    }

    fn host(&self) -> Option<String> {
        Some(self.inner.adapter.host().to_string())
    }

    fn port(&self) -> Option<u16> {
        Some(self.inner.port.load(Ordering::Acquire))
    }

    fn stats(&self) -> Option<Value> {
        Some(self.inner.stats())
    }

    fn dial_direct(&self, public_key: &str, host: &str, port: u16) {
        self.inner.dial_direct(public_key, host, port);
    }

    fn on_connection(
        &self,
        callback: Arc<dyn Fn(LanConnection) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        let id = self.inner.next_listener.fetch_add(1, Ordering::AcqRel) + 1;
        lock(&self.inner.listeners).push((id, callback));
        let inner = Arc::downgrade(&self.inner);
        Box::new(move || {
            if let Some(inner) = inner.upgrade() {
                lock(&inner.listeners).retain(|(kept, _)| *kept != id);
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_hex_stays_lowercase_and_zero_padded() {
        assert_eq!(hex(&[0, 1, 15, 16, 127, 255]), "00010f107fff");
        assert_eq!(hex(&[0xab; 32]), "ab".repeat(32));
        assert_eq!(hex(&[]), "");
    }

    #[tokio::test]
    async fn topic_membership_is_idempotent_and_removable() {
        use crate::lan::host::HostAdapter;
        use crate::lan::mdns::Adapter;

        let adapter = SelfAddressAdapter::with_adapter(
            Adapter::Host(Arc::new(HostAdapter::new(|_| {}))),
            Ipv4Addr::LOCALHOST,
        );
        let lan = LanDiscovery::open_with_adapter(adapter, KeyPair::generate())
            .await
            .unwrap();
        let a = [7; 32];
        let b = [8; 32];
        let a_token = topic_token(&a);
        let b_token = topic_token(&b);
        lan.join(a);
        lan.join(a);
        lan.join(b);
        assert_eq!(lan.stats().unwrap()["topics"], 2);
        assert!(lan.inner.shares_topic(std::slice::from_ref(&a_token)));
        lan.leave(a);
        lan.leave(a);
        assert_eq!(lan.stats().unwrap()["topics"], 1);
        assert!(!lan.inner.shares_topic(std::slice::from_ref(&a_token)));
        assert!(lan.inner.shares_topic(std::slice::from_ref(&b_token)));
        lan.leave(b);
        assert_eq!(lan.stats().unwrap()["topics"], 0);
        assert!(!lan.inner.shares_topic(&[a_token, b_token]));
        lan.destroy().await;
    }

    #[tokio::test]
    async fn larger_key_dials_after_the_fallback_delay() {
        use crate::lan::host::HostAdapter;
        use crate::lan::mdns::Adapter;

        let mut keys = [KeyPair::generate(), KeyPair::generate()];
        keys.sort_by_key(|key| key.public_key);
        let [small_key, large_key] = keys;
        let adapter = || {
            SelfAddressAdapter::with_adapter(
                Adapter::Host(Arc::new(HostAdapter::new(|_| {}))),
                Ipv4Addr::LOCALHOST,
            )
        };
        let small = LanDiscovery::open_with_adapter(adapter(), small_key)
            .await
            .unwrap();
        let large = LanDiscovery::open_with_adapter(adapter(), large_key)
            .await
            .unwrap();
        let (connected, mut connections) = tokio::sync::mpsc::unbounded_channel();
        let _small_listener = small.on_connection(Arc::new(|_| {}));
        let _large_listener = large.on_connection(Arc::new(move |connection| {
            let _ = connected.send(connection);
        }));
        let topic = [17; 32];
        small.join(topic);
        large.join(topic);
        let record = create_records(
            &small.inner.peer_key,
            small.port().unwrap(),
            &[topic_token(&topic)],
        )
        .remove(0);
        // Only the larger key sees the other peer: the smaller key cannot initiate.
        Inner::on_service(
            &large.inner,
            &DiscoveredService {
                name: record.name,
                port: record.port,
                addresses: vec![Ipv4Addr::LOCALHOST],
                txt: record.txt,
            },
        );
        assert_eq!(DIAL_AFTER, Duration::from_millis(3000));
        assert!(
            tokio::time::timeout(DIAL_AFTER - Duration::from_millis(100), connections.recv())
                .await
                .is_err(),
            "the larger key must let the smaller key dial first",
        );
        // A re-arming timer never dials, so a generous window still catches that bug.
        let connection = tokio::time::timeout(Duration::from_secs(8), connections.recv()).await;
        large.destroy().await;
        small.destroy().await;
        assert!(
            connection.is_ok_and(|connection| connection.is_some()),
            "the fallback must dial, not re-arm another three-second timer",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn advertisement_updates_cannot_overtake_each_other() {
        use crate::lan::host::HostAdapter;
        use crate::lan::mdns::Adapter;

        let (published, mut records) = tokio::sync::mpsc::unbounded_channel();
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let blocked = gate.clone();
        let first = AtomicBool::new(true);
        let host = Arc::new(HostAdapter::new(move |command| {
            if command["operation"] != "publish" {
                return;
            }
            let _ = published.send(command);
            if first.swap(false, Ordering::AcqRel) {
                let (released, ready) = &*blocked;
                let mut released = lock(released);
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
        }));
        let adapter = SelfAddressAdapter::with_adapter(Adapter::Host(host), Ipv4Addr::LOCALHOST);
        let lan = LanDiscovery::open_with_adapter(adapter, KeyPair::generate())
            .await
            .unwrap();
        let initial = tokio::time::timeout(Duration::from_secs(5), records.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(initial["record"]["txt"]["tc"], "0");
        let topic = [9; 32];
        lock(&lan.inner.topics).insert(topic_token(&topic));
        let update = tokio::spawn(Inner::sync_ads(lan.inner.clone()));
        let overlap = tokio::time::timeout(Duration::from_millis(100), records.recv()).await;
        let overtook = overlap.is_ok();
        *lock(&gate.0) = true;
        gate.1.notify_all();
        update.await.unwrap();
        let latest = match overlap.ok().flatten() {
            Some(record) => record,
            None => tokio::time::timeout(Duration::from_secs(5), records.recv())
                .await
                .unwrap()
                .unwrap(),
        };
        assert!(
            !overtook,
            "a newer advertisement overtook the blocked old one"
        );
        assert_eq!(latest["record"]["txt"]["tc"], "1");
        assert_eq!(latest["record"]["txt"]["t0"], topic_token(&topic));
        lan.destroy().await;
    }

    #[tokio::test]
    async fn binds_the_configured_loopback_interface() {
        let lan = LanDiscovery::open(Ipv4Addr::LOCALHOST, KeyPair::generate())
            .await
            .unwrap();
        let socket = lan
            .inner
            .swarm
            .dht()
            .server_socket()
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            socket.local_addr().await.unwrap().ip(),
            std::net::IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
        lan.destroy().await;
    }
}
