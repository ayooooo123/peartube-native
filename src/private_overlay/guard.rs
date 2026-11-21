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

use bytes::Bytes;
use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Mutex as AsyncMutex;

use super::dht::{Connection, ConnectionHandler, Dht, RawStream, Server};
use super::{
    Error, GUARD_CHANNEL, GUARD_TOPICS, HELLO_TIMEOUT, KeyPair, MAX_CARDS, MAX_CIRCUITS_PER_CONN,
    MAX_CIRCUITS_TOTAL, MAX_ROUTES_PER_CONN, MAX_ROUTES_TOTAL, MAX_RV_SLOTS_PER_TOPIC,
    MAX_RV_TOPICS, RV_REANNOUNCE_MS, Result, diversity_bucket_for_address, is_card, is_u32, now_ms,
    op, rand_bytes, ready, route_lifetime, verify_dht_mutable,
};
use crate::control_channel::{Control, ControlHandlers, open_channel, serve_channel};
use crate::control_protocol::Body;
use crate::wire::peartube::{
    GuardAck, GuardCards, GuardCircuitDead, GuardDial, GuardGetResult, GuardHello, GuardKey,
    GuardOpen, GuardPutMutable, GuardReady, GuardRecord, GuardRegister, GuardRvRegister,
    GuardRvSlot,
};

pub use super::dht::ResolveAddress;

/// The role a control peer claimed in `hello`.
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
    sealed_card: Bytes,
    session: Arc<Session>,
    expires_at: u64,
}

struct RvEntry {
    topic: [u8; 32],
    slots: Mutex<HashMap<[u8; 16], Slot>>,
    last_announce_at: AtomicU64,
}

/// The circuit teardowns registered with one owner (a channel session or an outgoing hop).
/// `None` once the owner was released: nothing can be registered with it any more.
type Drops = Mutex<Option<HashMap<u64, Arc<dyn Fn() + Send + Sync>>>>;

/// Runs every teardown an owner holds and closes its registry, in one step under its lock, so
/// a circuit admitted at the same time is either torn down here or refused.
fn release_drops(drops: &Drops) {
    let taken = drops
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
        .unwrap_or_default();
    for drop in taken.into_values() {
        drop();
    }
}

/// One guard channel session: what `hello` granted and what its requests reserved (routes,
/// rendezvous slots, circuits) live from channel open to channel close, even when the
/// connection stays. The DHT rate budget belongs to the connection.
struct Session {
    conn: Arc<dyn Connection>,
    rpc: Control,
    role: Mutex<Option<Role>>,
    owned_routes: Mutex<HashSet<[u8; 32]>>,
    owned_rv_slots: Mutex<HashSet<([u8; 32], [u8; 16])>>,
    drops: Drops,
    circuits: AtomicUsize,
    dht_ops: Arc<Mutex<std::collections::VecDeque<u64>>>,
}

impl Session {
    fn role(&self) -> Option<Role> {
        *self.role.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `isClosed(session.rpc)`.
    fn is_closed(&self) -> bool {
        self.rpc.is_closed() || self.conn.is_closed()
    }

    fn allow_dht_op(&self) -> bool {
        let now = now_ms();
        let mut ops = self.dht_ops.lock().unwrap_or_else(|e| e.into_inner());
        while ops
            .front()
            .is_some_and(|&at| now.saturating_sub(at) > 10_000)
        {
            ops.pop_front();
        }
        if ops.len() >= super::MAX_DHT_OPS_PER_10S {
            return false;
        }
        ops.push_back(now);
        true
    }
}

struct HopPeer {
    conn: Arc<dyn Connection>,
    rpc: Control,
    drops: Drops,
}

impl HopPeer {
    /// `isClosed(hop.rpc)`.
    fn is_closed(&self) -> bool {
        self.rpc.is_closed() || self.conn.is_closed()
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
    /// Every accepted connection, with or without an open guard channel; shutdown destroys them.
    conns: Mutex<HashMap<u64, Arc<dyn Connection>>>,
    hop_peers: Mutex<HashMap<[u8; 32], Arc<HopPeer>>>,
    hop_locks: Mutex<HashMap<[u8; 32], Arc<AsyncMutex<()>>>>,
    verified_guards: Mutex<HashMap<[u8; 32], u64>>,
    put_mutable_count: AtomicU64,
    rv_register_count: AtomicU64,
    fail_mutable_puts: AtomicU64,
    next_conn_id: AtomicU64,
    next_circuit_id: AtomicU64,
    next_drop_id: AtomicU64,
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
    fn remove_rv_slot(self: &Arc<Self>, topic: [u8; 32], slot: [u8; 16], session: &Arc<Session>) {
        let removed = {
            let mut topics = self.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
            let Some(entry) = topics.get(&topic).cloned() else {
                return;
            };
            {
                let mut slots = entry.slots.lock().unwrap_or_else(|e| e.into_inner());
                let ours = slots
                    .get(&slot)
                    .is_some_and(|s| Arc::ptr_eq(&s.session, session));
                if !ours {
                    return;
                }
                slots.remove(&slot);
            }
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

    /// `getHopPeer(targetGuardKey)`: one guard channel per guard, opened as `hop`.
    async fn get_hop_peer(self: &Arc<Self>, target: [u8; 32]) -> Result<Arc<HopPeer>> {
        let current = || {
            self.hop_peers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&target)
                .filter(|hop| !hop.is_closed())
                .cloned()
        };
        if let Some(current) = current() {
            return Ok(current);
        }
        let lock = self.hop_lock(&target);
        let _guard = lock.lock().await;
        if let Some(current) = current() {
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
        // A guard that refuses the channel or the hop is not used, and nothing else runs on
        // this connection.
        let opened = async {
            let mux = self.dht.protomux(&conn).await?;
            mux.ready();
            let rpc = open_channel(
                &mux,
                &GUARD_CHANNEL,
                // A hop channel that closes later leaves the guard unusable as a hop: its
                // dedicated connection goes too, and the next use connects again.
                ControlHandlers {
                    on_close: Some({
                        let conn = Arc::clone(&conn);
                        Arc::new(move |_| conn.destroy())
                    }),
                    ..ControlHandlers::default()
                },
                HELLO_TIMEOUT,
            )
            .await?;
            let hello = GuardHello { role: "hop".into() };
            match rpc.call(op::HELLO, hello.into(), HELLO_TIMEOUT).await? {
                Body::GuardAck(GuardAck { status: 1 }) => Ok(rpc),
                _ => Err(Error::Rejected("Guard refused the hop")),
            }
        }
        .await;
        let rpc = match opened {
            Ok(rpc) => rpc,
            Err(error) => {
                conn.destroy();
                return Err(error);
            }
        };
        let entry = Arc::new(HopPeer {
            conn: Arc::clone(&conn),
            rpc,
            drops: Mutex::new(Some(HashMap::new())),
        });
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
            release_drops(&watched.drops);
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

    /// Admits a spliced circuit (`activeCircuits.add(rec)`) and registers one idempotent
    /// teardown with every owner: the session that asked, the route's endpoint session for a
    /// hop, the outgoing hop for a dial. It runs after the last await, under every owner's
    /// registry lock and only while all of them are live, so an owner released meanwhile
    /// refuses it (`false`) and the caller destroys both streams. `circuitDead` goes to the
    /// endpoint: the route's session for a hop, the asking session for a dial.
    fn add_circuit(
        self: &Arc<Self>,
        session: &Arc<Session>,
        other_session: Option<&Arc<Session>>,
        hop: Option<&Arc<HopPeer>>,
        left: Arc<dyn RawStream>,
        right: Arc<dyn RawStream>,
        dead_udx: u32,
    ) -> bool {
        let mut registries: Vec<_> = std::iter::once(&session.drops)
            .chain(other_session.map(|other| &other.drops))
            .chain(hop.map(|hop| &hop.drops))
            .map(|drops| drops.lock().unwrap_or_else(|e| e.into_inner()))
            .collect();
        let live = registries.iter().all(|drops| drops.is_some())
            && !session.is_closed()
            && other_session.is_none_or(|other| !other.is_closed())
            && hop.is_none_or(|hop| !hop.is_closed());
        if !live {
            return false;
        }
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
        let hop_handle = hop.cloned();
        let dead_via = other_session.unwrap_or(session).rpc.clone();
        let drop: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let circuit = inner
                .active_circuits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            let Some(circuit) = circuit else {
                return;
            };
            let owners = std::iter::once(&session_handle.drops)
                .chain(other_handle.as_ref().map(|other| &other.drops))
                .chain(hop_handle.as_ref().map(|hop| &hop.drops));
            for drops in owners {
                if let Some(drops) = drops.lock().unwrap_or_else(|e| e.into_inner()).as_mut() {
                    drops.remove(&drop_id);
                }
            }
            if let Some(other) = &other_handle {
                other.circuits.fetch_sub(1, Ordering::Relaxed);
            }
            session_handle.circuits.fetch_sub(1, Ordering::Relaxed);
            // One event, never retried: a full queue only delays it.
            dead_via.push(
                op::CIRCUIT_DEAD,
                GuardCircuitDead {
                    udx_id: u64::from(dead_udx),
                }
                .into(),
            );
            circuit.left.destroy();
            circuit.right.destroy();
        });
        for drops in registries.iter_mut().filter_map(|drops| drops.as_mut()) {
            drops.insert(drop_id, Arc::clone(&drop));
        }
        std::mem::drop(registries);
        for stream in [left, right] {
            let drop = Arc::clone(&drop);
            tokio::spawn(async move {
                stream.closed().await;
                drop();
            });
        }
        true
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
        conns: Mutex::new(HashMap::new()),
        hop_peers: Mutex::new(HashMap::new()),
        hop_locks: Mutex::new(HashMap::new()),
        verified_guards: Mutex::new(HashMap::new()),
        put_mutable_count: AtomicU64::new(0),
        rv_register_count: AtomicU64::new(0),
        fail_mutable_puts: AtomicU64::new(0),
        next_conn_id: AtomicU64::new(1),
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
                {
                    let mut routes = inner.routes.lock().unwrap_or_else(|e| e.into_inner());
                    routes.retain(|token, route| {
                        if route.expires_at <= now {
                            route
                                .session
                                .owned_routes
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .remove(token);
                            false
                        } else {
                            true
                        }
                    });
                }
                let topics: Vec<(Arc<RvEntry>, [u8; 32])> = {
                    let topics = inner.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
                    topics
                        .iter()
                        .map(|(topic, entry)| (Arc::clone(entry), *topic))
                        .collect()
                };
                for (entry, topic) in topics {
                    let expired: Vec<([u8; 16], Arc<Session>)> = entry
                        .slots
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .iter()
                        .filter(|(_, slot)| slot.expires_at <= now)
                        .map(|(id, slot)| (*id, Arc::clone(&slot.session)))
                        .collect();
                    for (slot, owner) in expired {
                        entry
                            .slots
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&slot);
                        owner
                            .owned_rv_slots
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&(topic, slot));
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
        // Every accepted connection, also one whose guard channel closed or never opened.
        let conns: Vec<Arc<dyn Connection>> = self
            .inner
            .conns
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, conn)| conn)
            .collect();
        for conn in conns {
            conn.destroy();
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
    // Tracked from acceptance until it closes. A connection taken in after shutdown drained the
    // set sees `closing` and goes at once.
    let conn_id = inner.next_conn_id.fetch_add(1, Ordering::Relaxed);
    inner
        .conns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(conn_id, Arc::clone(&conn));
    let forget = {
        let inner = Arc::clone(&inner);
        move || {
            inner
                .conns
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&conn_id);
        }
    };
    if inner.closing() {
        forget();
        conn.destroy();
        return;
    }
    let mux = match inner.dht.protomux(&conn).await {
        Ok(mux) => mux,
        Err(_) => {
            forget();
            conn.destroy();
            return;
        }
    };
    // The DHT rate budget stays with the connection, so reopening the channel does not reset it.
    let dht_ops = Arc::new(Mutex::new(std::collections::VecDeque::new()));
    // This connection's open channel sessions, by channel.
    let channels: Arc<Mutex<HashMap<u64, Arc<Session>>>> = Arc::default();
    let session_of = {
        let channels = Arc::clone(&channels);
        move |control: &Control| {
            channels
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&control.id())
                .cloned()
        }
    };
    // What the guard answers on this connection. Each op checks the session's role, and the
    // bounds of its struct after decode; a refusal is an ack or a ready with status 0.
    let handlers = ControlHandlers {
        handled: SERVED,
        on_request: Some({
            let inner = Arc::clone(&inner);
            let session_of = session_of.clone();
            Arc::new(move |op, body, control| {
                let inner = Arc::clone(&inner);
                let session = session_of(&control);
                Box::pin(async move {
                    let session = session.ok_or("Guard channel closed")?;
                    handle_request(inner, session, op, body).await
                })
            })
        }),
        on_event: Some({
            let inner = Arc::clone(&inner);
            Arc::new(move |_, body, control| {
                if let (Some(session), Body::GuardRvSlot(GuardRvSlot { topic, slot_id })) =
                    (session_of(&control), body)
                {
                    inner.rv_unregister(&session, topic, slot_id);
                }
            })
        }),
        on_open: Some({
            let conn = Arc::clone(&conn);
            let channels = Arc::clone(&channels);
            Arc::new(move |control| {
                let session = Arc::new(Session {
                    conn: Arc::clone(&conn),
                    rpc: control.clone(),
                    role: Mutex::new(None),
                    owned_routes: Mutex::new(HashSet::new()),
                    owned_rv_slots: Mutex::new(HashSet::new()),
                    drops: Mutex::new(Some(HashMap::new())),
                    circuits: AtomicUsize::new(0),
                    dht_ops: Arc::clone(&dht_ops),
                });
                channels
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(control.id(), session);
            })
        }),
        on_close: Some({
            let inner = Arc::clone(&inner);
            let channels = Arc::clone(&channels);
            Arc::new(move |control| {
                let session = channels
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&control.id());
                if let Some(session) = session {
                    inner.release_session(&session);
                }
            })
        }),
    };
    let serving = serve_channel(&mux, &GUARD_CHANNEL, handlers);
    mux.ready();
    tokio::spawn(async move {
        conn.closed().await;
        forget();
        drop(serving);
        let open: Vec<Control> = channels
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .map(|session| session.rpc.clone())
            .collect();
        for control in open {
            control.close();
        }
    });
}

impl Inner {
    /// Releases what one channel session held: its circuits, routes and rendezvous slots.
    fn release_session(self: &Arc<Self>, session: &Arc<Session>) {
        release_drops(&session.drops);
        let owned: Vec<[u8; 32]> = session
            .owned_routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .collect();
        {
            let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
            for token in owned {
                if routes
                    .get(&token)
                    .is_some_and(|route| Arc::ptr_eq(&route.session, session))
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
            self.remove_rv_slot(topic, slot, session);
        }
    }
}

/// What a guard serves (bit n for op n): every request except `incOpen`, and the
/// `rvUnregister` event. `incOpen` and `circuitDead` go from the guard to the endpoint.
const SERVED: u64 = (1 << op::HELLO)
    | (1 << op::REGISTER)
    | (1 << op::PUT_MUTABLE)
    | (1 << op::GET_MUTABLE)
    | (1 << op::RV_REGISTER)
    | (1 << op::RV_UNREGISTER)
    | (1 << op::RV_LOOKUP)
    | (1 << op::RV_FETCH)
    | (1 << op::DIAL_OPEN)
    | (1 << op::HOP_OPEN);

/// A circuit that was not opened, and a dial refused because the next hop is not diverse.
const NOT_READY: GuardReady = GuardReady {
    status: 0,
    udx_id: 0,
};
const NOT_DIVERSE: GuardReady = GuardReady {
    status: 2,
    udx_id: 0,
};
/// `rvFetch` from another guard, during an endpoint's `rvLookup`.
const FETCH_TIMEOUT: Duration = Duration::from_millis(4000);
/// `hopOpen` and `incOpen` while a circuit is spliced.
const CIRCUIT_TIMEOUT: Duration = Duration::from_millis(8000);

fn ack(ok: bool) -> Body {
    GuardAck {
        status: u64::from(ok),
    }
    .into()
}

/// The answer to one request on a session's channel. The envelope has already matched the
/// struct to the op; requests that share `guard-key` are told apart by the op.
async fn handle_request(
    inner: Arc<Inner>,
    session: Arc<Session>,
    op_index: u64,
    body: Body,
) -> Result<Body, String> {
    Ok(match body {
        Body::GuardHello(GuardHello { role }) => return inner.hello(&session, &role).await,
        Body::GuardRegister(v) => ack(inner.register(&session, v)),
        Body::GuardPutMutable(v) => ack(inner.put_mutable(&session, v).await),
        Body::GuardKey(GuardKey { key }) if op_index == op::GET_MUTABLE => {
            inner.get_mutable(&session, key).await.into()
        }
        Body::GuardRvRegister(v) => ack(inner.rv_register(&session, v).await),
        Body::GuardKey(GuardKey { key }) if op_index == op::RV_LOOKUP => {
            inner.rv_lookup(&session, key).await.into()
        }
        Body::GuardKey(GuardKey { key }) if op_index == op::RV_FETCH => {
            inner.rv_fetch(&session, key).into()
        }
        Body::GuardDial(v) => inner.dial_open(&session, v).await.into(),
        Body::GuardOpen(v) if op_index == op::HOP_OPEN => inner.hop_open(&session, v).await.into(),
        _ => return Err("Unknown operation".into()),
    })
}

impl Inner {
    /// `hello`. A hop claim must come from an advertised guard in a bucket of its own; anything
    /// else is cut off. This is the only place a guard destroys a connection (contract D13).
    async fn hello(&self, session: &Arc<Session>, role: &str) -> Result<Body, String> {
        if self.closing() {
            return Err("Guard is closing".into());
        }
        let role = match role {
            "hop" => Role::Hop,
            "endpoint" => Role::Endpoint,
            _ => return Ok(ack(false)),
        };
        if role == Role::Hop
            && (self.bucket_of_conn(&session.conn).is_none()
                || !self
                    .is_advertised_guard(&session.conn.remote_public_key())
                    .await)
        {
            session.conn.destroy();
            return Ok(ack(false));
        }
        // A hop verification can end after the channel went: the released session gets no role
        // and nobody is told of it. The check and the grant are one step with the release.
        {
            let drops = session.drops.lock().unwrap_or_else(|e| e.into_inner());
            if drops.is_none() || session.is_closed() {
                return Ok(ack(false));
            }
            *session.role.lock().unwrap_or_else(|e| e.into_inner()) = Some(role);
        }
        if let Some(on_control_peer) = &self.on_control_peer {
            on_control_peer(ControlPeer {
                role,
                remote_public_key: session.conn.remote_public_key(),
                remote_host: session.conn.remote_host(),
                remote_port: session.conn.remote_port().unwrap_or(0),
            });
        }
        Ok(ack(true))
    }

    /// `register`. A live token belongs to the session that registered it: another session can
    /// neither take it over nor refresh it past the caps. An expired one is free again.
    fn register(&self, session: &Arc<Session>, v: GuardRegister) -> bool {
        if self.closing() || session.role() != Some(Role::Endpoint) || !is_u32(v.ttl_ms) {
            return false;
        }
        let token = v.route_token;
        let now = now_ms();
        let evict_owner = {
            let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
            let cur = routes.get(&token);
            let mine = cur.is_some_and(|r| Arc::ptr_eq(&r.session, session));
            if cur.is_some_and(|cur| !mine && cur.expires_at > now) {
                return false;
            }
            let occupied = routes.len().saturating_sub(usize::from(cur.is_some()));
            let own_count = session
                .owned_routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len();
            if !mine && (occupied >= MAX_ROUTES_TOTAL || own_count >= MAX_ROUTES_PER_CONN) {
                return false;
            }
            let prev_owner = cur.filter(|_| !mine).map(|r| Arc::clone(&r.session));
            routes.insert(
                token,
                Route {
                    session: Arc::clone(session),
                    expires_at: now + route_lifetime(v.ttl_ms as u32),
                },
            );
            prev_owner
        };
        if let Some(prev) = evict_owner {
            prev.owned_routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&token);
        }
        session
            .owned_routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(token);
        true
    }

    /// `putMutable`: forwards the endpoint's own signed record, rate limited.
    async fn put_mutable(&self, session: &Arc<Session>, v: GuardPutMutable) -> bool {
        if self.closing()
            || session.role() != Some(Role::Endpoint)
            || !is_u32(v.seq)
            || v.value.is_empty()
            || !session.allow_dht_op()
        {
            return false;
        }
        // An oversized value has spent its rate slot, as in the released guard.
        if !is_card(&v.value) || !verify_dht_mutable(&v.signature, v.seq, &v.value, &v.public_key) {
            return false;
        }
        // Takes one planned failure, if any is left.
        let mut left = self.fail_mutable_puts.load(Ordering::Relaxed);
        while left > 0 {
            match self.fail_mutable_puts.compare_exchange(
                left,
                left - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return false,
                Err(now) => left = now,
            }
        }
        let put = self
            .dht
            .mutable_put_signed(v.public_key, v.seq, v.signature, &v.value)
            .await;
        if put.is_ok() {
            self.put_mutable_count.fetch_add(1, Ordering::Relaxed);
        }
        put.is_ok()
    }

    /// `getMutable`: "not found" is an absent record.
    async fn get_mutable(&self, session: &Arc<Session>, key: [u8; 32]) -> GuardGetResult {
        if self.closing() || session.role() != Some(Role::Endpoint) || !session.allow_dht_op() {
            return GuardGetResult { record: None };
        }
        let record = match self.dht.mutable_get(key).await {
            Ok(Some(r)) if is_u32(r.seq) && is_card(&r.value) => Some(GuardRecord {
                seq: r.seq,
                signature: r.signature,
                value: Bytes::from(r.value),
            }),
            _ => None,
        };
        GuardGetResult { record }
    }

    /// `rvRegister`. Same rule as routes: a live slot is its registering session's alone.
    async fn rv_register(self: &Arc<Self>, session: &Arc<Session>, v: GuardRvRegister) -> bool {
        if self.closing()
            || session.role() != Some(Role::Endpoint)
            || !is_u32(v.ttl_ms)
            || !is_card(&v.sealed_card)
        {
            return false;
        }
        let GuardRvRegister {
            topic,
            slot_id,
            ttl_ms,
            sealed_card,
        } = v;
        let entry = {
            let mut topics = self.rv_topics.lock().unwrap_or_else(|e| e.into_inner());
            match topics.get(&topic).cloned() {
                Some(entry) => entry,
                None => {
                    if topics.len() >= MAX_RV_TOPICS {
                        return false;
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
        let now = now_ms();
        let evict_owner = {
            let mut slots = entry.slots.lock().unwrap_or_else(|e| e.into_inner());
            let cur = slots.get(&slot_id);
            let mine = cur.is_some_and(|s| Arc::ptr_eq(&s.session, session));
            if cur.is_some_and(|cur| !mine && cur.expires_at > now) {
                return false;
            }
            if cur.is_none() && slots.len() >= MAX_RV_SLOTS_PER_TOPIC {
                return false;
            }
            let prev_owner = cur.filter(|_| !mine).map(|s| Arc::clone(&s.session));
            slots.insert(
                slot_id,
                Slot {
                    // Its own bytes, not a view into the frame it came in.
                    sealed_card: Bytes::copy_from_slice(&sealed_card),
                    session: Arc::clone(session),
                    expires_at: now + route_lifetime(ttl_ms as u32),
                },
            );
            prev_owner
        };
        if let Some(prev) = evict_owner {
            prev.owned_rv_slots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&(topic, slot_id));
        }
        session
            .owned_rv_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((topic, slot_id));
        self.ensure_rv_announced(topic, &entry).await;
        self.rv_register_count.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// The `rvUnregister` event: an endpoint drops its own slot.
    fn rv_unregister(self: &Arc<Self>, session: &Arc<Session>, topic: [u8; 32], slot: [u8; 16]) {
        if session.role() != Some(Role::Endpoint) {
            return;
        }
        session
            .owned_rv_slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(topic, slot));
        self.remove_rv_slot(topic, slot, session);
    }

    /// The live sealed cards of one rendezvous topic, at most [`MAX_CARDS`], pruning expired
    /// slots; `seen` already holds cards that count against the cap. One card per slot, or one
    /// per distinct card with `unique` (`rvLookup`).
    fn live_cards(&self, topic: &[u8; 32], seen: &mut Vec<Bytes>, unique: bool) {
        let entry = self
            .rv_topics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(topic)
            .cloned();
        let Some(entry) = entry else {
            return;
        };
        let now = now_ms();
        entry
            .slots
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|_, slot| {
                if slot.expires_at <= now {
                    return false;
                }
                if seen.len() < MAX_CARDS && !(unique && seen.contains(&slot.sealed_card)) {
                    seen.push(slot.sealed_card.clone());
                }
                true
            });
    }

    /// `rvFetch`: only another guard, on a hop session, reads the slots.
    fn rv_fetch(&self, session: &Arc<Session>, topic: [u8; 32]) -> GuardCards {
        let mut cards = Vec::new();
        if !self.closing() && session.role() == Some(Role::Hop) {
            self.live_cards(&topic, &mut cards, false);
        }
        GuardCards { cards }
    }

    /// `rvLookup`: the local slots, then up to eight other guards announcing the topic.
    async fn rv_lookup(self: &Arc<Self>, session: &Arc<Session>, topic: [u8; 32]) -> GuardCards {
        let mut seen = Vec::new();
        if self.closing() || session.role() != Some(Role::Endpoint) || !session.allow_dht_op() {
            return GuardCards { cards: seen };
        }
        self.live_cards(&topic, &mut seen, true);
        let peers = tokio::time::timeout(Duration::from_millis(4000), self.dht.lookup(topic))
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or_default();
        let mut responders: Vec<[u8; 32]> = Vec::new();
        for peer in peers {
            if peer != self.key_pair.public_key && !responders.contains(&peer) {
                responders.push(peer);
            }
            if responders.len() >= 8 {
                break;
            }
        }
        let mut fetches = FuturesUnordered::new();
        for guard in responders {
            let inner = Arc::clone(self);
            fetches.push(async move {
                let hop = inner.get_hop_peer(guard).await.ok()?;
                let key = GuardKey { key: topic };
                match hop.rpc.call(op::RV_FETCH, key.into(), FETCH_TIMEOUT).await {
                    Ok(Body::GuardCards(GuardCards { cards })) => Some(cards),
                    _ => None,
                }
            });
        }
        while let Some(reply) = fetches.next().await {
            for card in reply.unwrap_or_default() {
                if seen.len() < MAX_CARDS && is_card(&card) && !seen.contains(&card) {
                    seen.push(card);
                }
            }
        }
        GuardCards { cards: seen }
    }

    /// `dialOpen`: the source guard splices the endpoint to a `/24`-diverse destination guard.
    async fn dial_open(self: &Arc<Self>, session: &Arc<Session>, v: GuardDial) -> GuardReady {
        let GuardDial {
            guard_key,
            route_token,
            udx_id,
            bucket: caller_bucket,
        } = v;
        if self.closing() || session.role() != Some(Role::Endpoint) || !is_u32(udx_id) {
            return NOT_READY;
        }
        let caller_udx = udx_id as u32;
        if caller_udx == 0
            || guard_key == self.key_pair.public_key
            || self
                .active_circuits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len()
                >= MAX_CIRCUITS_TOTAL
            || session.circuits.load(Ordering::Relaxed) >= MAX_CIRCUITS_PER_CONN
        {
            return NOT_READY;
        }
        let mut left: Option<Arc<dyn RawStream>> = None;
        let mut right: Option<Arc<dyn RawStream>> = None;
        let result = async {
            let hop = self.get_hop_peer(guard_key).await?;
            let Some(next_bucket) = self.bucket_of_conn(&hop.conn) else {
                return Ok(None);
            };
            if (!caller_bucket.is_empty() && caller_bucket == next_bucket)
                || self.own_diversity_bucket().as_deref() == Some(next_bucket.as_str())
            {
                return Ok(None);
            }
            let ga_left = self.alloc_raw_stream(&session.conn, false).await?;
            left = Some(Arc::clone(&ga_left));
            let ga_right = self.alloc_raw_stream(&hop.conn, false).await?;
            right = Some(Arc::clone(&ga_right));
            self.dht
                .connect_raw_stream(&session.conn, &ga_left, caller_udx)
                .await?;
            let open = GuardOpen {
                route_token,
                udx_id: u64::from(ga_right.id()),
            };
            let remote = match hop
                .rpc
                .call(op::HOP_OPEN, open.into(), CIRCUIT_TIMEOUT)
                .await?
            {
                Body::GuardReady(reply) => ready(&reply),
                _ => None,
            }
            .ok_or(Error::Rejected("Hop rejected"))?;
            self.dht
                .connect_raw_stream(&hop.conn, &ga_right, remote)
                .await?;
            ga_left.relay_to(ga_right.as_ref())?;
            ga_right.relay_to(ga_left.as_ref())?;
            if !self.add_circuit(
                session,
                None,
                Some(&hop),
                Arc::clone(&ga_left),
                Arc::clone(&ga_right),
                caller_udx,
            ) {
                return Err(Error::Closed("A channel of this circuit closed"));
            }
            left = None;
            right = None;
            Ok::<_, Error>(Some(ga_left.id()))
        }
        .await;
        match result {
            Ok(Some(id)) => GuardReady {
                status: 1,
                udx_id: u64::from(id),
            },
            Ok(None) => NOT_DIVERSE,
            Err(_) => {
                for stream in left.iter().chain(right.iter()) {
                    stream.destroy();
                }
                NOT_READY
            }
        }
    }

    /// `hopOpen`: the destination guard splices a hop to the endpoint that owns the route.
    async fn hop_open(self: &Arc<Self>, session: &Arc<Session>, v: GuardOpen) -> GuardReady {
        let GuardOpen {
            route_token,
            udx_id,
        } = v;
        if self.closing() || session.role() != Some(Role::Hop) || !is_u32(udx_id) {
            return NOT_READY;
        }
        let caller_udx = udx_id as u32;
        let route = self
            .routes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&route_token)
            .map(|route| (Arc::clone(&route.session), route.expires_at));
        let Some((endpoint_session, expires_at)) = route else {
            return NOT_READY;
        };
        if caller_udx == 0
            || expires_at <= now_ms()
            || Arc::ptr_eq(&endpoint_session.conn, &session.conn)
            || endpoint_session.is_closed()
            || self
                .active_circuits
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len()
                >= MAX_CIRCUITS_TOTAL
            || endpoint_session.circuits.load(Ordering::Relaxed) >= MAX_CIRCUITS_PER_CONN
        {
            return NOT_READY;
        }
        let mut left: Option<Arc<dyn RawStream>> = None;
        let mut right: Option<Arc<dyn RawStream>> = None;
        let result = async {
            let Some(hop_bucket) = self.bucket_of_conn(&session.conn) else {
                return Err(Error::Rejected("Non-diverse hop"));
            };
            let endpoint_bucket = self.bucket_of_conn(&endpoint_session.conn);
            if self.own_diversity_bucket().as_deref() == Some(hop_bucket.as_str())
                || (!self.allow_loopback && endpoint_bucket.as_deref() == Some(hop_bucket.as_str()))
            {
                return Err(Error::Rejected("Non-diverse hop"));
            }
            let gb_left = self.alloc_raw_stream(&session.conn, false).await?;
            left = Some(Arc::clone(&gb_left));
            let gb_right = self.alloc_raw_stream(&endpoint_session.conn, false).await?;
            right = Some(Arc::clone(&gb_right));
            self.dht
                .connect_raw_stream(&session.conn, &gb_left, caller_udx)
                .await?;
            let open = GuardOpen {
                route_token,
                udx_id: u64::from(gb_right.id()),
            };
            let remote = match endpoint_session
                .rpc
                .call(op::INC_OPEN, open.into(), CIRCUIT_TIMEOUT)
                .await?
            {
                Body::GuardReady(reply) => ready(&reply),
                _ => None,
            }
            .ok_or(Error::Rejected("Endpoint rejected"))?;
            self.dht
                .connect_raw_stream(&endpoint_session.conn, &gb_right, remote)
                .await?;
            gb_left.relay_to(gb_right.as_ref())?;
            gb_right.relay_to(gb_left.as_ref())?;
            if !self.add_circuit(
                session,
                Some(&endpoint_session),
                None,
                Arc::clone(&gb_left),
                Arc::clone(&gb_right),
                remote,
            ) {
                return Err(Error::Closed("A channel of this circuit closed"));
            }
            left = None;
            right = None;
            Ok(gb_left.id())
        }
        .await;
        match result {
            Ok(id) => GuardReady {
                status: 1,
                udx_id: u64::from(id),
            },
            Err(_) => {
                for stream in left.iter().chain(right.iter()) {
                    stream.destroy();
                }
                NOT_READY
            }
        }
    }
}
