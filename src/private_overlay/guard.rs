//! `createPrivateGuard(...)`: the stateless relay half of the overlay.
//!
//! A guard holds no Corestore state and never touches disk. It announces itself on the four
//! guard topics, accepts endpoint control connections, forwards mutable descriptor writes and
//! rendezvous slots, and splices raw UDX streams between two `/24`-diverse peers.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use futures_util::stream::{FuturesUnordered, StreamExt};
use protomux::Mux;
use tokio::sync::Mutex as AsyncMutex;

use super::dht::{Connection, ConnectionHandler, Dht, RawStream, Server};
use super::rpc::{Handler, Rpc, RpcSender, attach_rpc_channel};
use super::{
    Error, Frame, GUARD_TOPICS, KeyPair, MAX_CARD_BYTES, MAX_CIRCUITS_PER_CONN, MAX_CIRCUITS_TOTAL,
    MAX_ROUTES_PER_CONN, MAX_ROUTES_TOTAL, MAX_RV_SLOTS_PER_TOPIC, MAX_RV_TOPICS, RV_REANNOUNCE_MS,
    Result, diversity_bucket_for_address, now_ms, op, rand_bytes, route_lifetime,
    verify_dht_mutable,
};

pub use super::dht::ResolveAddress;

/// The role a control peer claimed in `OP_HELLO`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// A relay hoping to be spliced through (destination guard).
    Hop,
    /// An endpoint publishing routes and rendezvous slots.
    Endpoint,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Hop => "hop",
            Role::Endpoint => "endpoint",
        }
    }
}

/// Report handed to `onControlPeer`.
#[derive(Debug, Clone)]
pub struct ControlPeer {
    pub role: Role,
    pub remote_public_key: [u8; 32],
    pub remote_host: Option<IpAddr>,
    pub remote_port: u16,
}

/// Guard counters (`guard.stats()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuardStats {
    pub registered_routes: usize,
    pub active_circuits: usize,
    pub rendezvous_topics: usize,
    pub put_mutable_count: u64,
    pub rv_register_count: u64,
}

/// `createPrivateGuard` options.
pub struct GuardOptions {
    pub host: Option<String>,
    pub port: u16,
    pub bootstrap: Vec<String>,
    /// `None` derives the default from [`super::is_local_bootstrap`].
    pub seed: Option<[u8; 32]>,
    pub allow_loopback: Option<bool>,
    pub resolve_address: Option<ResolveAddress>,
    pub announce_guard: bool,
    pub on_control_peer: Option<Arc<dyn Fn(ControlPeer) + Send + Sync>>,
}

impl Default for GuardOptions {
    fn default() -> Self {
        Self {
            host: None,
            port: 0,
            bootstrap: Vec::new(),
            seed: None,
            allow_loopback: None,
            resolve_address: None,
            announce_guard: true,
            on_control_peer: None,
        }
    }
}

struct Route {
    session: Arc<Session>,
    expires_at: u64,
}

struct Slot {
    sealed_card: Vec<u8>,
    expires_at: u64,
}

struct RvEntry {
    topic: [u8; 32],
    slots: Mutex<HashMap<[u8; 16], Slot>>,
    last_announce_at: AtomicU64,
}

struct Session {
    id: u64,
    conn: Arc<dyn Connection>,
    rpc: Mutex<Option<Arc<Rpc>>>,
    role: Mutex<Option<Role>>,
    owned_routes: Mutex<HashSet<[u8; 32]>>,
    owned_rv_slots: Mutex<HashSet<([u8; 32], [u8; 16])>>,
    drops: Mutex<HashMap<u64, Arc<dyn Fn() + Send + Sync>>>,
    circuits: AtomicUsize,
}

impl Session {
    fn rpc(&self) -> Option<Arc<Rpc>> {
        self.rpc.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn role(&self) -> Option<Role> {
        *self.role.lock().unwrap_or_else(|e| e.into_inner())
    }
}

struct HopPeer {
    conn: Arc<dyn Connection>,
    rpc: Mutex<Option<Arc<Rpc>>>,
    drops: Mutex<HashMap<u64, Arc<dyn Fn() + Send + Sync>>>,
}

impl HopPeer {
    fn rpc(&self) -> Option<Arc<Rpc>> {
        self.rpc.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

struct Circuit {
    left: Arc<dyn RawStream>,
    right: Arc<dyn RawStream>,
}

struct Inner {
    dht: Arc<dyn Dht>,
    key_pair: KeyPair,
    allow_loopback: bool,
    resolve_address: Option<ResolveAddress>,
    announce_guard: bool,
    on_control_peer: Option<Arc<dyn Fn(ControlPeer) + Send + Sync>>,
    server: Mutex<Option<Arc<dyn Server>>>,
    server_address: Mutex<Option<SocketAddr>>,
    closing: AtomicBool,
    routes: Mutex<HashMap<[u8; 32], Route>>,
    rv_topics: Mutex<HashMap<[u8; 32], Arc<RvEntry>>>,
    rv_locks: Mutex<HashMap<[u8; 32], Arc<AsyncMutex<()>>>>,
    active_circuits: Mutex<HashMap<u64, Circuit>>,
    sessions: Mutex<HashMap<u64, Arc<Session>>>,
    hop_peers: Mutex<HashMap<[u8; 32], Arc<HopPeer>>>,
    hop_locks: Mutex<HashMap<[u8; 32], Arc<AsyncMutex<()>>>>,
    verified_guards: Mutex<HashMap<[u8; 32], u64>>,
    put_mutable_count: AtomicU64,
    rv_register_count: AtomicU64,
    fail_mutable_puts: AtomicU64,
    next_session_id: AtomicU64,
    next_circuit_id: AtomicU64,
    next_drop_id: AtomicU64,
}

fn u32_at(buf: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes([
        buf[offset],
        buf[offset + 1],
        buf[offset + 2],
        buf[offset + 3],
    ])
}

impl Inner {
    fn closing(&self) -> bool {
        self.closing.load(Ordering::Relaxed)
    }

    /// `bucketOfConn(conn)`.
    fn bucket_of_conn(&self, conn: &Arc<dyn Connection>) -> Option<String> {
        super::dht::bucket_for_conn(self.resolve_address.as_ref(), self.allow_loopback, conn)
    }

    /// `ownDiversityBucket()`.
    fn own_diversity_bucket(&self) -> Option<String> {
        let own = self.dht.remote_address().or_else(|| {
            *self
                .server_address
                .lock()
                .unwrap_or_else(|e| e.into_inner())
        });
        let own: SocketAddr = own?;
        let host = own.ip().to_string();
        let host = match &self.resolve_address {
            Some(resolve) => resolve(Some(&host), Some(own.port()), &self.key_pair.public_key),
            None => host,
        };
        diversity_bucket_for_address(&host, Some(u32::from(own.port())), self.allow_loopback)
    }

    /// `isAdvertisedGuard(pubKey)`, cached for 60s.
    async fn is_advertised_guard(&self, public_key: &[u8; 32]) -> bool {
        if public_key == &self.key_pair.public_key {
            return false;
        }
        if self.cached_guard(public_key) {
            return true;
        }
        let mut lookups = FuturesUnordered::new();
        for topic in GUARD_TOPICS.iter() {
            let topic = *topic;
            let dht = Arc::clone(&self.dht);
            lookups.push(async move {
                tokio::time::timeout(Duration::from_millis(3500), dht.lookup(topic))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .unwrap_or_default()
            });
        }
        while let Some(peers) = lookups.next().await {
            let mut cache = self
                .verified_guards
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let expires_at = now_ms() + 60_000;
            for peer in peers {
                cache.insert(peer, expires_at);
            }
        }
        self.cached_guard(public_key)
    }

    fn cached_guard(&self, public_key: &[u8; 32]) -> bool {
        let cache = self
            .verified_guards
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        cache.get(public_key).is_some_and(|at| *at > now_ms())
    }

    async fn announce_service(&self) {
        if !self.announce_guard || self.closing() {
            return;
        }
        let mut due = FuturesUnordered::new();
        for topic in GUARD_TOPICS.iter() {
            let topic = *topic;
            let dht = Arc::clone(&self.dht);
            let key_pair = self.key_pair.clone();
            due.push(async move {
                let _ = dht.announce(topic, &key_pair).await;
            });
        }
        while due.next().await.is_some() {}
    }

    fn topic_lock(&self, topic: &[u8; 32]) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            self.rv_locks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(*topic)
                .or_default(),
        )
    }

    fn hop_lock(&self, key: &[u8; 32]) -> Arc<AsyncMutex<()>> {
        Arc::clone(
            self.hop_locks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entry(*key)
                .or_default(),
        )
    }

    /// `ensureRvAnnounced(tHex, entry)`.
    async fn ensure_rv_announced(self: &Arc<Self>, topic: [u8; 32], entry: &Arc<RvEntry>) {
        let lock = self.topic_lock(&topic);
        let _guard = lock.lock().await;
        if self.closing() {
            return;
        }
        let current = self
            .rv_topics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&topic)
            .cloned();
        if !current.is_some_and(|c| Arc::ptr_eq(&c, entry)) {
            return;
        }
        if entry
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
        {
            return;
        }
        let now = now_ms();
        if now.saturating_sub(entry.last_announce_at.load(Ordering::Relaxed)) < RV_REANNOUNCE_MS {
            return;
        }
        if self.dht.announce(entry.topic, &self.key_pair).await.is_ok() {
            entry.last_announce_at.store(now, Ordering::Relaxed);
        }
    }

    /// `removeRvSlot(tHex, sHex)`: drop the slot, and the topic with it when the last one goes.
    fn remove_rv_slot(self: &Arc<Self>, topic: [u8; 32], slot: [u8; 16]) {
        let removed = {
            let mut topics = self.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
            let Some(entry) = topics.get(&topic).cloned() else {
                return;
            };
            entry
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&slot);
            let empty = entry
                .slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty();
            if empty {
                topics.remove(&topic);
                Some(entry)
            } else {
                None
            }
        };
        let Some(entry) = removed else {
            return;
        };
        let inner = Arc::clone(self);
        let lock = self.topic_lock(&topic);
        tokio::spawn(async move {
            let _guard = lock.lock().await;
            let still_used = inner
                .rv_topics
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&topic)
                .is_some_and(|e| !e.slots.lock().unwrap_or_else(|e| e.into_inner()).is_empty());
            if !inner.closing() && !still_used {
                let _ = inner.dht.unannounce(entry.topic, &inner.key_pair).await;
            }
        });
    }

    /// `getHopPeer(targetGuardKey)`: one RPC channel per guard, kept alive with keepalive.
    async fn get_hop_peer(self: &Arc<Self>, target: [u8; 32]) -> Result<Arc<HopPeer>> {
        if let Some(current) = self
            .hop_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&target)
            .cloned()
            && !current.rpc().is_some_and(|rpc| rpc.is_closed())
        {
            return Ok(current);
        }
        let lock = self.hop_lock(&target);
        let _guard = lock.lock().await;
        if let Some(current) = self
            .hop_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&target)
            .cloned()
            && !current.rpc().is_some_and(|rpc| rpc.is_closed())
        {
            return Ok(current);
        }
        if self.closing() {
            return Err(Error::Closed("Guard closing"));
        }
        let conn = self
            .dht
            .connect(target, &self.key_pair)
            .await
            .map_err(|_| Error::Rejected("Guard unreachable"))?;
        let entry = Arc::new(HopPeer {
            conn: Arc::clone(&conn),
            rpc: Mutex::new(None),
            drops: Mutex::new(HashMap::new()),
        });
        let mux: Mux = self
            .dht
            .protomux(&conn)
            .await
            .map_err(|_| Error::Rejected("Guard unreachable"))?;
        let rpc = attach_rpc_channel(&mux, Arc::clone(&conn), None)
            .map_err(|_| Error::Rejected("Guard unreachable"))?;
        rpc.request(op::HELLO, op::HELLO, b"hop", 4000)
            .await
            .map_err(|_| Error::Rejected("Guard unreachable"))?;
        *entry.rpc.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&rpc));
        self.hop_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(target, Arc::clone(&entry));
        let inner = Arc::clone(self);
        let watched = Arc::clone(&entry);
        tokio::spawn(async move {
            conn.closed().await;
            {
                let mut peers = inner.hop_peers.lock().unwrap_or_else(|e| e.into_inner());
                if peers.get(&target).is_some_and(|p| Arc::ptr_eq(p, &watched)) {
                    peers.remove(&target);
                }
            }
            let drops: Vec<_> = watched
                .drops
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .cloned()
                .collect();
            for drop in drops {
                drop();
            }
        });
        Ok(entry)
    }

    /// `allocRawStream(dht, ctrlConn, framed)`.
    async fn alloc_raw_stream(
        &self,
        conn: &Arc<dyn Connection>,
        framed: bool,
    ) -> Result<Arc<dyn RawStream>> {
        super::dht::alloc_raw_stream(&self.dht, conn, framed).await
    }

    /// Registers a spliced circuit and its close handlers (`activeCircuits.add(rec)`).
    fn add_circuit(
        self: &Arc<Self>,
        session: &Arc<Session>,
        other_session: Option<&Arc<Session>>,
        left: Arc<dyn RawStream>,
        right: Arc<dyn RawStream>,
        dead_udx: u32,
        dead_via: Arc<Rpc>,
    ) {
        let id = self.next_circuit_id.fetch_add(1, Ordering::Relaxed);
        self.active_circuits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                id,
                Circuit {
                    left: Arc::clone(&left),
                    right: Arc::clone(&right),
                },
            );
        session.circuits.fetch_add(1, Ordering::Relaxed);
        if let Some(other) = other_session {
            other.circuits.fetch_add(1, Ordering::Relaxed);
        }
        let drop_id = self.next_drop_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::clone(self);
        let session_handle = Arc::clone(session);
        let other_handle = other_session.cloned();
        let drop: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let circuit = inner
                .active_circuits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            if circuit.is_none() {
                return;
            }
            session_handle
                .drops
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&drop_id);
            if let Some(other) = &other_handle {
                other
                    .drops
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&drop_id);
                other.circuits.fetch_sub(1, Ordering::Relaxed);
            }
            session_handle.circuits.fetch_sub(1, Ordering::Relaxed);
            dead_via
                .sender()
                .send(op::CIRCUIT_DEAD, 0, &dead_udx.to_be_bytes());
            if let Some(circuit) = circuit {
                circuit.left.destroy();
                circuit.right.destroy();
            }
        });
        session
            .drops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(drop_id, Arc::clone(&drop));
        if let Some(other) = other_session {
            other
                .drops
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(drop_id, Arc::clone(&drop));
        }
        for stream in [left, right] {
            let drop = Arc::clone(&drop);
            tokio::spawn(async move {
                stream.closed().await;
                drop();
            });
        }
    }
}

/// A live guard.
#[derive(Clone)]
pub struct PrivateGuard {
    inner: Arc<Inner>,
    refresh: Arc<AsyncMutex<Option<tokio::task::JoinHandle<()>>>>,
}

/// `createPrivateGuard(...)`; the [`Dht`] must have been opened with the same bind/bootstrap.
pub async fn create_private_guard(dht: Arc<dyn Dht>, opts: GuardOptions) -> Result<PrivateGuard> {
    let allow_loopback = opts
        .allow_loopback
        .unwrap_or_else(|| super::is_local_bootstrap(opts.host.as_deref(), &opts.bootstrap));
    let key_pair = KeyPair::from_seed(opts.seed.unwrap_or_else(rand_bytes));
    let inner = Arc::new(Inner {
        dht,
        key_pair,
        allow_loopback,
        resolve_address: opts.resolve_address,
        announce_guard: opts.announce_guard,
        on_control_peer: opts.on_control_peer,
        server: Mutex::new(None),
        server_address: Mutex::new(None),
        closing: AtomicBool::new(false),
        routes: Mutex::new(HashMap::new()),
        rv_topics: Mutex::new(HashMap::new()),
        rv_locks: Mutex::new(HashMap::new()),
        active_circuits: Mutex::new(HashMap::new()),
        sessions: Mutex::new(HashMap::new()),
        hop_peers: Mutex::new(HashMap::new()),
        hop_locks: Mutex::new(HashMap::new()),
        verified_guards: Mutex::new(HashMap::new()),
        put_mutable_count: AtomicU64::new(0),
        rv_register_count: AtomicU64::new(0),
        fail_mutable_puts: AtomicU64::new(0),
        next_session_id: AtomicU64::new(1),
        next_circuit_id: AtomicU64::new(1),
        next_drop_id: AtomicU64::new(1),
    });
    let handler: ConnectionHandler = {
        let inner = Arc::clone(&inner);
        Arc::new(move |conn| {
            let inner = Arc::clone(&inner);
            tokio::spawn(async move {
                handle_connection(inner, conn).await;
            });
        })
    };
    let server = inner.dht.create_server(handler).await?;
    server.listen(&inner.key_pair).await?;
    *inner.server.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&server));
    *inner
        .server_address
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = server.address();
    inner.announce_service().await;
    let refresh = tokio::spawn({
        let inner = Arc::clone(&inner);
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if inner.closing() {
                    return;
                }
                inner.announce_service().await;
                let now = now_ms();
                inner
                    .routes
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .retain(|_, route| route.expires_at > now);
                let topics: Vec<(Arc<RvEntry>, [u8; 32])> = {
                    let topics = inner.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
                    topics
                        .iter()
                        .map(|(topic, entry)| (Arc::clone(entry), *topic))
                        .collect()
                };
                for (entry, topic) in topics {
                    let expired: Vec<[u8; 16]> = entry
                        .slots
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .iter()
                        .filter(|(_, slot)| slot.expires_at <= now)
                        .map(|(id, _)| *id)
                        .collect();
                    for slot in expired {
                        entry
                            .slots
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&slot);
                    }
                    let empty = entry
                        .slots
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .is_empty();
                    if empty {
                        let removed = inner
                            .rv_topics
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get(&topic)
                            .is_some_and(|current| Arc::ptr_eq(current, &entry));
                        if removed {
                            inner
                                .rv_topics
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .remove(&topic);
                            let unannounce = Arc::clone(&inner);
                            let lock = inner.topic_lock(&topic);
                            let topic_buf = entry.topic;
                            tokio::spawn(async move {
                                let _guard = lock.lock().await;
                                if !unannounce.closing() {
                                    let _ = unannounce
                                        .dht
                                        .unannounce(topic_buf, &unannounce.key_pair)
                                        .await;
                                }
                            });
                        }
                    } else {
                        inner.ensure_rv_announced(topic, &entry).await;
                    }
                }
            }
        }
    });
    Ok(PrivateGuard {
        inner,
        refresh: Arc::new(AsyncMutex::new(Some(refresh))),
    })
}

impl PrivateGuard {
    pub fn public_key(&self) -> [u8; 32] {
        self.inner.key_pair.public_key
    }

    /// `guard.address()`: the listening socket.
    pub fn address(&self) -> Option<SocketAddr> {
        self.inner
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(|server| server.address())
    }

    /// `guard.boundPorts()`.
    pub fn bound_ports(&self) -> Vec<u16> {
        let mut ports: Vec<u16> = Vec::new();
        for port in [
            self.address().map(|a| a.port()),
            self.inner.dht.address().map(|a| a.port()),
            self.inner.dht.local_address().map(|a| a.port()),
        ]
        .into_iter()
        .flatten()
        .filter(|port| *port > 0)
        {
            if !ports.contains(&port) {
                ports.push(port);
            }
        }
        ports
    }

    pub fn stats(&self) -> GuardStats {
        GuardStats {
            registered_routes: self
                .inner
                .routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            active_circuits: self
                .inner
                .active_circuits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            rendezvous_topics: self
                .inner
                .rv_topics
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            put_mutable_count: self.inner.put_mutable_count.load(Ordering::Relaxed),
            rv_register_count: self.inner.rv_register_count.load(Ordering::Relaxed),
        }
    }

    /// `failNextMutablePut(n)`, for exercising the endpoint's publish retry.
    pub fn fail_next_mutable_put(&self, n: u64) {
        self.inner.fail_mutable_puts.fetch_add(n, Ordering::Relaxed);
    }

    pub async fn suspend(&self) -> Result<()> {
        let server = self
            .inner
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(server) = server {
            server.suspend().await?;
        }
        self.inner.dht.suspend().await
    }

    pub async fn resume(&self) -> Result<()> {
        self.inner.dht.resume().await?;
        let server = self
            .inner
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(server) = server {
            server.resume().await?;
        }
        self.inner.announce_service().await;
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        if self.inner.closing.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        if let Some(task) = self.refresh.lock().await.take() {
            task.abort();
        }
        let circuits: Vec<Circuit> = self
            .inner
            .active_circuits
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, circuit)| circuit)
            .collect();
        for circuit in circuits {
            circuit.left.destroy();
            circuit.right.destroy();
        }
        let sessions: Vec<Arc<Session>> = self
            .inner
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, session)| session)
            .collect();
        for session in sessions {
            session.conn.destroy();
        }
        let hops: Vec<Arc<HopPeer>> = self
            .inner
            .hop_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, hop)| hop)
            .collect();
        for hop in hops {
            hop.conn.destroy();
        }
        if self.inner.announce_guard {
            let unannounce = async {
                let mut due = FuturesUnordered::new();
                for topic in GUARD_TOPICS.iter() {
                    let topic = *topic;
                    let dht = Arc::clone(&self.inner.dht);
                    let key_pair = self.inner.key_pair.clone();
                    due.push(async move {
                        let _ = dht.unannounce(topic, &key_pair).await;
                    });
                }
                while due.next().await.is_some() {}
            };
            let _ = tokio::time::timeout(Duration::from_millis(1500), unannounce).await;
        }
        let server = self
            .inner
            .server
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(server) = server {
            let _ = server.close().await;
        }
        let _ = self.inner.dht.destroy().await;
        Ok(())
    }
}

async fn handle_connection(inner: Arc<Inner>, conn: Arc<dyn Connection>) {
    if inner.closing() {
        conn.destroy();
        return;
    }
    let session = Arc::new(Session {
        id: inner.next_session_id.fetch_add(1, Ordering::Relaxed),
        conn: Arc::clone(&conn),
        rpc: Mutex::new(None),
        role: Mutex::new(None),
        owned_routes: Mutex::new(HashSet::new()),
        owned_rv_slots: Mutex::new(HashSet::new()),
        drops: Mutex::new(HashMap::new()),
        circuits: AtomicUsize::new(0),
    });
    inner
        .sessions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(session.id, Arc::clone(&session));
    let handler: Handler = {
        let inner = Arc::clone(&inner);
        let session = Arc::clone(&session);
        Arc::new(move |frame: Frame, send: RpcSender| {
            let inner = Arc::clone(&inner);
            let session = Arc::clone(&session);
            Box::pin(async move {
                // The JS side swallows handler failures (`Promise.resolve(...).catch(() => {})`).
                let _ = handle_request(inner, session, frame, send).await;
            }) as BoxFuture<'static, ()>
        })
    };
    let mux = match inner.dht.protomux(&conn).await {
        Ok(mux) => mux,
        Err(_) => {
            conn.destroy();
            return;
        }
    };
    let rpc = match attach_rpc_channel(&mux, Arc::clone(&conn), Some(handler)) {
        Ok(rpc) => rpc,
        Err(_) => {
            conn.destroy();
            return;
        }
    };
    *session.rpc.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&rpc));
    let inner_closed = Arc::clone(&inner);
    tokio::spawn(async move {
        conn.closed().await;
        inner_closed
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&session.id);
        let drops: Vec<_> = session
            .drops
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        for drop in drops {
            drop();
        }
        let owned: Vec<[u8; 32]> = session
            .owned_routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        {
            let mut routes = inner_closed
                .routes
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            for token in owned {
                if routes
                    .get(&token)
                    .is_some_and(|route| Arc::ptr_eq(&route.session, &session))
                {
                    routes.remove(&token);
                }
            }
        }
        let slots: Vec<([u8; 32], [u8; 16])> = session
            .owned_rv_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        for (topic, slot) in slots {
            inner_closed.remove_rv_slot(topic, slot);
        }
    });
}

async fn handle_request(
    inner: Arc<Inner>,
    session: Arc<Session>,
    frame: Frame,
    send: RpcSender,
) -> Result<()> {
    if inner.closing() {
        return Ok(());
    }
    let payload = frame.payload.clone();
    match frame.op {
        op::HELLO => {
            let wanted = if payload.as_slice() == b"hop" {
                Role::Hop
            } else {
                Role::Endpoint
            };
            if wanted == Role::Hop {
                let advertised = inner
                    .is_advertised_guard(&session.conn.remote_public_key())
                    .await;
                if inner.bucket_of_conn(&session.conn).is_none() || !advertised {
                    session.conn.destroy();
                    return Ok(());
                }
            }
            *session.role.lock().unwrap_or_else(|e| e.into_inner()) = Some(wanted);
            if let Some(on_control_peer) = &inner.on_control_peer {
                on_control_peer(ControlPeer {
                    role: wanted,
                    remote_public_key: session.conn.remote_public_key(),
                    remote_host: session.conn.remote_host(),
                    remote_port: session.conn.remote_port().unwrap_or(0),
                });
            }
            send.send(op::HELLO, frame.id, b"ok");
        }
        op::REGISTER => {
            if session.role() != Some(Role::Endpoint) || payload.len() != 36 {
                send.send(op::REGISTER_ACK, frame.id, &[0]);
                return Ok(());
            }
            let mut token = [0u8; 32];
            token.copy_from_slice(&payload[..32]);
            let ttl = u32_at(&payload, 32);
            let over_limit = {
                let routes = inner.routes.lock().unwrap_or_else(|e| e.into_inner());
                !routes.contains_key(&token)
                    && (routes.len() >= MAX_ROUTES_TOTAL
                        || session
                            .owned_routes
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .len()
                            >= MAX_ROUTES_PER_CONN)
            };
            if over_limit {
                send.send(op::REGISTER_ACK, frame.id, &[0]);
                return Ok(());
            }
            inner
                .routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    token,
                    Route {
                        session: Arc::clone(&session),
                        expires_at: now_ms() + route_lifetime(ttl),
                    },
                );
            session
                .owned_routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(token);
            send.send(op::REGISTER_ACK, frame.id, &[1]);
        }
        op::PUT_MUTABLE => {
            if session.role() != Some(Role::Endpoint) || payload.len() <= 100 {
                send.send(op::PUT_MUTABLE_ACK, frame.id, &[0]);
                return Ok(());
            }
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(&payload[..32]);
            let seq = u32_at(&payload, 32) as u64;
            let mut signature = [0u8; 64];
            signature.copy_from_slice(&payload[36..100]);
            let value = &payload[100..];
            if value.len() > MAX_CARD_BYTES
                || !verify_dht_mutable(&signature, seq, value, &public_key)
            {
                send.send(op::PUT_MUTABLE_ACK, frame.id, &[0]);
                return Ok(());
            }
            let remaining = inner.fail_mutable_puts.load(Ordering::Relaxed);
            if remaining > 0 {
                inner.fail_mutable_puts.fetch_sub(1, Ordering::Relaxed);
                send.send(op::PUT_MUTABLE_ACK, frame.id, &[0]);
                return Ok(());
            }
            match inner
                .dht
                .mutable_put_signed(public_key, seq, signature, value)
                .await
            {
                Ok(()) => {
                    inner.put_mutable_count.fetch_add(1, Ordering::Relaxed);
                    send.send(op::PUT_MUTABLE_ACK, frame.id, &[1]);
                }
                Err(_) => {
                    send.send(op::PUT_MUTABLE_ACK, frame.id, &[0]);
                }
            }
        }
        op::GET_MUTABLE => {
            if session.role() != Some(Role::Endpoint) || payload.len() != 32 {
                send.send(op::GET_MUTABLE_REPLY, frame.id, &[0]);
                return Ok(());
            }
            let mut public_key = [0u8; 32];
            public_key.copy_from_slice(&payload);
            match inner.dht.mutable_get(public_key).await {
                Ok(Some(record)) if !record.value.is_empty() => {
                    let mut reply = Vec::with_capacity(1 + 4 + 64 + record.value.len());
                    reply.push(1);
                    reply.extend_from_slice(&(record.seq as u32).to_be_bytes());
                    reply.extend_from_slice(&record.signature);
                    reply.extend_from_slice(&record.value);
                    send.send(op::GET_MUTABLE_REPLY, frame.id, &reply);
                }
                _ => {
                    send.send(op::GET_MUTABLE_REPLY, frame.id, &[0]);
                }
            }
        }
        op::RV_REGISTER => {
            if session.role() != Some(Role::Endpoint)
                || payload.len() <= 52
                || payload.len() - 52 > MAX_CARD_BYTES
            {
                send.send(op::RV_REGISTER_ACK, frame.id, &[0]);
                return Ok(());
            }
            let mut topic = [0u8; 32];
            topic.copy_from_slice(&payload[..32]);
            let mut slot_id = [0u8; 16];
            slot_id.copy_from_slice(&payload[32..48]);
            let ttl = u32_at(&payload, 48);
            let sealed_card = payload[52..].to_vec();
            let entry = {
                let mut topics = inner.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
                match topics.get(&topic).cloned() {
                    Some(entry) => entry,
                    None => {
                        if topics.len() >= MAX_RV_TOPICS {
                            drop(topics);
                            send.send(op::RV_REGISTER_ACK, frame.id, &[0]);
                            return Ok(());
                        }
                        let entry = Arc::new(RvEntry {
                            topic,
                            slots: Mutex::new(HashMap::new()),
                            last_announce_at: AtomicU64::new(0),
                        });
                        topics.insert(topic, Arc::clone(&entry));
                        entry
                    }
                }
            };
            {
                let mut slots = entry.slots.lock().unwrap_or_else(|e| e.into_inner());
                if !slots.contains_key(&slot_id) && slots.len() >= MAX_RV_SLOTS_PER_TOPIC {
                    drop(slots);
                    send.send(op::RV_REGISTER_ACK, frame.id, &[0]);
                    return Ok(());
                }
                slots.insert(
                    slot_id,
                    Slot {
                        sealed_card,
                        expires_at: now_ms() + route_lifetime(ttl),
                    },
                );
            }
            session
                .owned_rv_slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert((topic, slot_id));
            inner.ensure_rv_announced(topic, &entry).await;
            inner.rv_register_count.fetch_add(1, Ordering::Relaxed);
            send.send(op::RV_REGISTER_ACK, frame.id, &[1]);
        }
        op::RV_UNREGISTER => {
            if session.role() == Some(Role::Endpoint) && payload.len() == 48 {
                let mut topic = [0u8; 32];
                topic.copy_from_slice(&payload[..32]);
                let mut slot_id = [0u8; 16];
                slot_id.copy_from_slice(&payload[32..48]);
                session
                    .owned_rv_slots
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&(topic, slot_id));
                inner.remove_rv_slot(topic, slot_id);
            }
        }
        op::RV_FETCH => {
            if session.role() != Some(Role::Hop) {
                send.send(op::RV_FETCH_REPLY, frame.id, &super::encode_buf_arr(&[]));
                return Ok(());
            }
            let entry = if payload.len() == 32 {
                let mut topic = [0u8; 32];
                topic.copy_from_slice(&payload);
                inner
                    .rv_topics
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&topic)
                    .cloned()
            } else {
                None
            };
            let now = now_ms();
            let mut cards = Vec::new();
            if let Some(entry) = entry {
                let mut slots = entry.slots.lock().unwrap_or_else(|e| e.into_inner());
                slots.retain(|_, slot| {
                    if slot.expires_at <= now {
                        false
                    } else {
                        cards.push(slot.sealed_card.clone());
                        true
                    }
                });
            }
            send.send(op::RV_FETCH_REPLY, frame.id, &super::encode_buf_arr(&cards));
        }
        op::RV_LOOKUP => {
            if session.role() != Some(Role::Endpoint) || payload.len() != 32 {
                send.send(op::RV_LOOKUP_REPLY, frame.id, &super::encode_buf_arr(&[]));
                return Ok(());
            }
            let mut topic = [0u8; 32];
            topic.copy_from_slice(&payload);
            let mut seen: Vec<Vec<u8>> = Vec::new();
            let mut seen_set: HashSet<Vec<u8>> = HashSet::new();
            let now = now_ms();
            let local = inner
                .rv_topics
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&topic)
                .cloned();
            if let Some(entry) = local {
                let mut slots = entry.slots.lock().unwrap_or_else(|e| e.into_inner());
                slots.retain(|_, slot| {
                    if slot.expires_at <= now {
                        return false;
                    }
                    if seen_set.insert(slot.sealed_card.clone()) {
                        seen.push(slot.sealed_card.clone());
                    }
                    true
                });
            }
            let peers = tokio::time::timeout(Duration::from_millis(4000), inner.dht.lookup(topic))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            let mut responders: Vec<[u8; 32]> = Vec::new();
            for peer in peers {
                if peer != inner.key_pair.public_key && !responders.contains(&peer) {
                    responders.push(peer);
                }
                if responders.len() >= 8 {
                    break;
                }
            }
            let mut fetches = FuturesUnordered::new();
            for guard in responders {
                let inner = Arc::clone(&inner);
                fetches.push(async move {
                    let hop = inner.get_hop_peer(guard).await.ok()?;
                    let rpc = hop.rpc()?;
                    rpc.request(op::RV_FETCH, op::RV_FETCH_REPLY, &topic, 4000)
                        .await
                        .ok()
                });
            }
            while let Some(reply) = fetches.next().await {
                let Some(raw) = reply else {
                    continue;
                };
                for card in super::decode_buf_arr(&raw).unwrap_or_default() {
                    if seen.len() >= 64 {
                        break;
                    }
                    if seen_set.insert(card.clone()) {
                        seen.push(card);
                    }
                }
            }
            send.send(op::RV_LOOKUP_REPLY, frame.id, &super::encode_buf_arr(&seen));
        }
        op::DIAL_OPEN => {
            if session.role() != Some(Role::Endpoint) || payload.len() < 68 {
                send.send(op::DIAL_READY, frame.id, &[0; 5]);
                return Ok(());
            }
            let mut target_guard = [0u8; 32];
            target_guard.copy_from_slice(&payload[..32]);
            let mut route_token = [0u8; 32];
            route_token.copy_from_slice(&payload[32..64]);
            let caller_udx = u32_at(&payload, 64);
            let caller_bucket = if payload.len() > 68 {
                String::from_utf8_lossy(&payload[68..]).into_owned()
            } else {
                String::new()
            };
            if caller_udx == 0
                || target_guard == inner.key_pair.public_key
                || inner
                    .active_circuits
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len()
                    >= MAX_CIRCUITS_TOTAL
                || session.circuits.load(Ordering::Relaxed) >= MAX_CIRCUITS_PER_CONN
            {
                send.send(op::DIAL_READY, frame.id, &[0; 5]);
                return Ok(());
            }
            let mut left: Option<Arc<dyn RawStream>> = None;
            let mut right: Option<Arc<dyn RawStream>> = None;
            let session_rpc = session
                .rpc()
                .ok_or(Error::Closed("Control channel closed"))?;
            let result = async {
                let hop = inner.get_hop_peer(target_guard).await?;
                let next_bucket = inner.bucket_of_conn(&hop.conn);
                let own_bucket = inner.own_diversity_bucket();
                let Some(next_bucket) = next_bucket else {
                    return Err(Error::Rejected("Non-diverse hop"));
                };
                if (!caller_bucket.is_empty() && caller_bucket == next_bucket)
                    || own_bucket.as_deref() == Some(next_bucket.as_str())
                {
                    return Err(Error::Rejected("Non-diverse hop"));
                }
                let ga_left = inner.alloc_raw_stream(&session.conn, false).await?;
                left = Some(Arc::clone(&ga_left));
                let ga_right = inner.alloc_raw_stream(&hop.conn, false).await?;
                right = Some(Arc::clone(&ga_right));
                inner
                    .dht
                    .connect_raw_stream(&session.conn, &ga_left, caller_udx)
                    .await?;
                let hop_rpc = hop.rpc().ok_or(Error::Closed("Control channel closed"))?;
                let mut ask = Vec::with_capacity(36);
                ask.extend_from_slice(&route_token);
                ask.extend_from_slice(&ga_right.id().to_be_bytes());
                let reply = hop_rpc
                    .request(op::HOP_OPEN, op::HOP_READY, &ask, 8000)
                    .await?;
                if reply.len() != 5 || reply[0] != 1 {
                    return Err(Error::Rejected("Hop rejected"));
                }
                let remote = u32_at(&reply, 1);
                inner
                    .dht
                    .connect_raw_stream(&hop.conn, &ga_right, remote)
                    .await?;
                ga_left.relay_to(ga_right.as_ref())?;
                ga_right.relay_to(ga_left.as_ref())?;
                inner.add_circuit(
                    &session,
                    None,
                    Arc::clone(&ga_left),
                    Arc::clone(&ga_right),
                    caller_udx,
                    Arc::clone(&session_rpc),
                );
                let mut ready = Vec::with_capacity(5);
                ready.push(1);
                ready.extend_from_slice(&ga_left.id().to_be_bytes());
                send.send(op::DIAL_READY, frame.id, &ready);
                left = None;
                right = None;
                Ok::<(), Error>(())
            }
            .await;
            if let Err(error) = result {
                if let Some(stream) = &left {
                    stream.destroy();
                }
                if let Some(stream) = &right {
                    stream.destroy();
                }
                let non_diverse = matches!(error, Error::Rejected("Non-diverse hop"));
                let body: [u8; 5] = if non_diverse { [2, 0, 0, 0, 0] } else { [0; 5] };
                send.send(op::DIAL_READY, frame.id, &body);
            }
        }
        op::HOP_OPEN => {
            if session.role() != Some(Role::Hop) || payload.len() != 36 {
                send.send(op::HOP_READY, frame.id, &[0; 5]);
                return Ok(());
            }
            let mut route_token = [0u8; 32];
            route_token.copy_from_slice(&payload[..32]);
            let caller_udx = u32_at(&payload, 32);
            let route = inner
                .routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&route_token)
                .map(|route| (Arc::clone(&route.session), route.expires_at));
            let (endpoint_session, expires_at) = match route {
                Some(route) => route,
                None => {
                    send.send(op::HOP_READY, frame.id, &[0; 5]);
                    return Ok(());
                }
            };
            let endpoint_rpc_closed = endpoint_session.rpc().is_none_or(|rpc| rpc.is_closed());
            if caller_udx == 0
                || expires_at <= now_ms()
                || Arc::ptr_eq(&endpoint_session.conn, &session.conn)
                || endpoint_rpc_closed
                || inner
                    .active_circuits
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .len()
                    >= MAX_CIRCUITS_TOTAL
                || endpoint_session.circuits.load(Ordering::Relaxed) >= MAX_CIRCUITS_PER_CONN
            {
                send.send(op::HOP_READY, frame.id, &[0; 5]);
                return Ok(());
            }
            let mut left: Option<Arc<dyn RawStream>> = None;
            let mut right: Option<Arc<dyn RawStream>> = None;
            let result = async {
                let hop_bucket = inner.bucket_of_conn(&session.conn);
                let own_bucket = inner.own_diversity_bucket();
                let endpoint_bucket = inner.bucket_of_conn(&endpoint_session.conn);
                let Some(hop_bucket) = hop_bucket else {
                    return Err(Error::Rejected("Non-diverse hop"));
                };
                if own_bucket.as_deref() == Some(hop_bucket.as_str())
                    || (!inner.allow_loopback
                        && endpoint_bucket.as_deref() == Some(hop_bucket.as_str()))
                {
                    return Err(Error::Rejected("Non-diverse hop"));
                }
                let gb_left = inner.alloc_raw_stream(&session.conn, false).await?;
                left = Some(Arc::clone(&gb_left));
                let gb_right = inner
                    .alloc_raw_stream(&endpoint_session.conn, false)
                    .await?;
                right = Some(Arc::clone(&gb_right));
                inner
                    .dht
                    .connect_raw_stream(&session.conn, &gb_left, caller_udx)
                    .await?;
                let endpoint_rpc = endpoint_session
                    .rpc()
                    .ok_or(Error::Closed("Control channel closed"))?;
                let mut ask = Vec::with_capacity(36);
                ask.extend_from_slice(&route_token);
                ask.extend_from_slice(&gb_right.id().to_be_bytes());
                let reply = endpoint_rpc
                    .request(op::INC_OPEN, op::INC_READY, &ask, 8000)
                    .await?;
                if reply.len() != 5 || reply[0] != 1 {
                    return Err(Error::Rejected("Endpoint rejected"));
                }
                let remote = u32_at(&reply, 1);
                inner
                    .dht
                    .connect_raw_stream(&endpoint_session.conn, &gb_right, remote)
                    .await?;
                gb_left.relay_to(gb_right.as_ref())?;
                gb_right.relay_to(gb_left.as_ref())?;
                inner.add_circuit(
                    &session,
                    Some(&endpoint_session),
                    Arc::clone(&gb_left),
                    Arc::clone(&gb_right),
                    remote,
                    endpoint_rpc,
                );
                let mut ready = Vec::with_capacity(5);
                ready.push(1);
                ready.extend_from_slice(&gb_left.id().to_be_bytes());
                send.send(op::HOP_READY, frame.id, &ready);
                left = None;
                right = None;
                Ok::<(), Error>(())
            }
            .await;
            if result.is_err() {
                if let Some(stream) = &left {
                    stream.destroy();
                }
                if let Some(stream) = &right {
                    stream.destroy();
                }
                send.send(op::HOP_READY, frame.id, &[0; 5]);
            }
        }
        _ => {}
    }
    Ok(())
}
