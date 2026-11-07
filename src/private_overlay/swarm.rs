//! `createPrivateSwarm(...)`: the endpoint half of the overlay.
//!
//! The endpoint keeps its Noise identity off the public DHT. It discovers two `/24`-diverse
//! guards, publishes a sealed owner-signed route card through the primary one, registers one
//! rendezvous slot per joined source, and reaches peers only through guard-spliced raw streams.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::sync::Mutex as AsyncMutex;

use super::dht::{
    self, Connection, Dht, NoiseConnection, NoiseOpened, PendingNoise, RawStream, ResolveAddress,
};
use super::rpc::{Handler, Rpc, attach_rpc_channel};
use super::{
    Error, Frame, GUARD_TOPICS, KeyPair, ROUTE_TTL_MS, RV_EPOCH_MS, Result, SignedCard,
    decode_and_verify_card, decode_buf_arr, encode_signed_card, now_ms, op, open_box,
    peer_seal_key, rand_bytes, rv_epoch, rv_seal_key, rv_topic, seal_box, sign_dht_mutable,
    verify_dht_mutable,
};
use futures_util::future::BoxFuture;

/// `privateRouting.status()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingState {
    Bootstrapping,
    Ready,
    Unavailable,
    Suspended,
    Destroyed,
}

impl RoutingState {
    pub fn as_str(self) -> &'static str {
        match self {
            RoutingState::Bootstrapping => "BOOTSTRAPPING",
            RoutingState::Ready => "READY",
            RoutingState::Unavailable => "UNAVAILABLE",
            RoutingState::Suspended => "SUSPENDED",
            RoutingState::Destroyed => "DESTROYED",
        }
    }
}

/// `privateRouting.exposureReport()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExposureReport {
    pub hops: usize,
    pub direct_destination_sends: u64,
    pub guards: usize,
    pub transport_port: u16,
}

/// The endpoint-facing view of the transport DHT, as `swarm.dht` exposes it.
#[derive(Clone)]
pub struct PrivateRouting {
    inner: Arc<SwarmInner>,
}

impl PrivateRouting {
    pub fn status(&self) -> RoutingState {
        if self.inner.closing() {
            RoutingState::Destroyed
        } else if self.inner.suspended() {
            RoutingState::Suspended
        } else {
            *self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
        }
    }

    pub fn exposure_report(&self) -> ExposureReport {
        let port = self
            .inner
            .dht
            .address()
            .or_else(|| self.inner.dht.local_address())
            .map(|a| a.port())
            .unwrap_or(0);
        ExposureReport {
            hops: 2,
            direct_destination_sends: 0,
            guards: self
                .inner
                .guards
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            transport_port: port,
        }
    }
}

/// `firewall(remotePublicKey)`: `true` blocks the peer.
pub type Firewall = Arc<dyn Fn(&[u8; 32]) -> bool + Send + Sync>;
/// `swarm.on('connection', fn)`.
pub type ConnectionHook = Arc<dyn Fn(NoiseConnection) + Send + Sync>;
/// `swarm.on('update', fn)`.
pub type UpdateHook = Arc<dyn Fn() + Send + Sync>;

/// `createPrivateSwarm` options.
#[derive(Default)]
pub struct SwarmOptions {
    pub host: Option<String>,
    pub bootstrap: Vec<String>,
    pub port: u16,
    /// `None` derives the default from [`super::is_local_bootstrap`].
    pub allow_loopback: Option<bool>,
    pub resolve_address: Option<ResolveAddress>,
    /// The end-to-end Noise identity; random when omitted.
    pub key_pair: Option<KeyPair>,
    /// `true` blocks a peer (`firewall(remotePublicKey)`).
    pub firewall: Option<Firewall>,
    /// `swarm.on('connection', ...)`.
    pub on_connection: Option<ConnectionHook>,
    /// `swarm.on('update', ...)`: emitted after every connection add/remove.
    pub on_update: Option<UpdateHook>,
}

/// The handle `joinSource`/`join` returns.
#[derive(Clone)]
pub struct SourceHandle {
    pub topic: [u8; 32],
    pub is_server: bool,
    pub is_client: bool,
    inner: Arc<SwarmInner>,
    id: Option<String>,
}

impl SourceHandle {
    /// `handle.refresh()`.
    pub async fn refresh(&self) {
        self.inner.poll_all().await;
    }

    /// `handle.destroy()`.
    pub async fn destroy(&self) {
        if let Some(id) = &self.id {
            self.inner.leave_source(id).await;
        }
    }
}

struct Admitted {
    close: Arc<dyn Fn() + Send + Sync>,
    closed: tokio::sync::watch::Receiver<bool>,
}

struct Guard {
    public_key: [u8; 32],
    bucket: String,
    conn: Arc<dyn Connection>,
    rpc: Mutex<Option<Arc<Rpc>>>,
    streams: Mutex<HashMap<u32, Arc<dyn Fn() + Send + Sync>>>,
}

impl Guard {
    fn rpc(&self) -> Option<Arc<Rpc>> {
        self.rpc.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    fn rpc_closed(&self) -> bool {
        self.rpc().is_none_or(|rpc| rpc.is_closed())
    }
}

struct Source {
    id: String,
    source_key: [u8; 32],
    slot_id: [u8; 16],
    last: Mutex<Option<SourceStamp>>,
    /// Topic at join time; `handle.topic`.
    topic: [u8; 32],
}

struct SourceStamp {
    epoch: String,
    guard: [u8; 32],
    token: [u8; 32],
    next_renewal_at: u64,
}

struct PublishStamp {
    guard: [u8; 32],
    token: [u8; 32],
    epoch: String,
}

impl Source {
    fn stamp(&self) -> Option<(String, [u8; 32], [u8; 32], u64)> {
        self.last
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| (s.epoch.clone(), s.guard, s.token, s.next_renewal_at))
    }

    fn set_stamp(&self, stamp: SourceStamp) {
        *self.last.lock().unwrap_or_else(|e| e.into_inner()) = Some(stamp);
    }
}

struct SwarmInner {
    dht: Arc<dyn Dht>,
    /// End-to-end Noise identity handed to the app.
    key_pair: KeyPair,
    /// Identity used only towards the transport DHT and the guards.
    transport_key_pair: KeyPair,
    allow_loopback: bool,
    resolve_address: Option<ResolveAddress>,
    firewall: Firewall,
    on_connection: Option<ConnectionHook>,
    on_update: Option<UpdateHook>,
    guards: Mutex<Vec<Arc<Guard>>>,
    connections: Mutex<HashMap<u64, Arc<Admitted>>>,
    active_by_peer: Mutex<HashMap<[u8; 32], u64>>,
    dialing: Mutex<HashSet<[u8; 32]>>,
    explicit_peers: Mutex<HashSet<[u8; 32]>>,
    joined: Mutex<HashMap<String, Arc<Source>>>,
    highest_seq: Mutex<HashMap<[u8; 32], u64>>,
    route_token: Mutex<[u8; 32]>,
    card_seq: AtomicU64,
    cached_card: Mutex<Option<Vec<u8>>>,
    published: Mutex<Option<PublishStamp>>,
    next_renewal_at: AtomicU64,
    state: Mutex<RoutingState>,
    suspended: AtomicBool,
    closing: AtomicBool,
    ensure_lock: Arc<AsyncMutex<()>>,
    next_stream_id: AtomicU64,
    poll_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl SwarmInner {
    fn closing(&self) -> bool {
        self.closing.load(Ordering::Relaxed)
    }

    fn suspended(&self) -> bool {
        self.suspended.load(Ordering::Relaxed)
    }

    fn set_state(&self, state: RoutingState) {
        *self.state.lock().unwrap_or_else(|e| e.into_inner()) = state;
    }

    fn route_token(&self) -> [u8; 32] {
        *self.route_token.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn guard_list(&self) -> Vec<Arc<Guard>> {
        self.guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn bucket_of(&self, conn: &Arc<dyn Connection>) -> Option<String> {
        dht::bucket_for_conn(self.resolve_address.as_ref(), self.allow_loopback, conn)
    }

    fn emit_update(&self) {
        if let Some(on_update) = &self.on_update {
            on_update();
        }
    }

    /// `acceptValidCard(card)`.
    fn accept_valid_card(&self, card: &SignedCard) -> bool {
        let mut highest = self.highest_seq.lock().unwrap_or_else(|e| e.into_inner());
        let entry = highest.entry(card.inner_public_key).or_insert(0);
        if card.seq < *entry {
            return false;
        }
        *entry = card.seq;
        true
    }

    /// `admitInnerStream(enc, expectedRemoteKey)`: waits for `enc.opened`, then admits.
    fn admit_inner_stream(self: &Arc<Self>, pending: PendingNoise, expected: Option<[u8; 32]>) {
        let inner = Arc::clone(self);
        let close = Arc::clone(&pending.close);
        let closed = pending.closed.clone();
        tokio::spawn(async move {
            match pending.opened.await {
                Ok(opened) => inner.admit_opened(opened, close, closed, expected),
                Err(_) => close(),
            }
        });
    }

    fn admit_opened(
        self: &Arc<Self>,
        opened: NoiseOpened,
        close: Arc<dyn Fn() + Send + Sync>,
        closed: tokio::sync::watch::Receiver<bool>,
        expected: Option<[u8; 32]>,
    ) {
        let remote = opened.remote_public_key;
        if self.closing()
            || self.suspended()
            || expected.is_some_and(|key| key != remote)
            || remote == self.key_pair.public_key
            || (self.firewall)(&remote)
        {
            close();
            return;
        }
        // Duplicate selection and publication are one JS callback, so keep them atomic here too.
        let mut active = self
            .active_by_peer
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let previous = active.get(&remote).copied();
        let admitted = Arc::new(Admitted {
            close: Arc::clone(&close),
            closed: closed.clone(),
        });
        let mut replaced = None;
        if let Some(previous_id) = previous {
            let previous = self
                .connections
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&previous_id)
                .cloned();
            if let Some(previous) = previous.filter(|p| !*p.closed.borrow()) {
                // The responder wins the duplicate exchange when the remote half sorts lower.
                if opened.is_initiator || remote >= self.key_pair.public_key {
                    drop(active);
                    close();
                    return;
                }
                replaced = Some(previous);
            }
        }
        let id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        self.connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, Arc::clone(&admitted));
        active.insert(remote, id);
        drop(active);
        if let Some(previous) = replaced {
            (previous.close)();
        }
        let inner = Arc::clone(self);
        let mut watcher = closed.clone();
        tokio::spawn(async move {
            let _ = watcher.wait_for(|closed| *closed).await;
            inner
                .connections
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            let mut peers = inner
                .active_by_peer
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if peers.get(&remote).is_some_and(|current| *current == id) {
                peers.remove(&remote);
            }
            drop(peers);
            inner.emit_update();
        });
        if let Some(on_connection) = &self.on_connection {
            on_connection(NoiseConnection {
                remote_public_key: remote,
                is_initiator: opened.is_initiator,
                handshake_hash: opened.handshake_hash,
                stream: opened.stream,
                close,
                closed,
            });
        }
        self.emit_update();
    }

    /// `connectCandidateGuard(pubKey)`. Boxed: the close watcher starts another poll cycle, so the
    /// future would otherwise be recursive.
    fn connect_candidate_guard(
        self: &Arc<Self>,
        public_key: [u8; 32],
    ) -> BoxFuture<'_, Option<Arc<Guard>>> {
        Box::pin(async move {
            let conn = self
                .dht
                .connect(public_key, &self.transport_key_pair)
                .await
                .ok()?;
            if self.closing() || self.suspended() {
                conn.destroy();
                return None;
            }
            let Some(bucket) = self.bucket_of(&conn) else {
                conn.destroy();
                return None;
            };
            let guard = Arc::new(Guard {
                public_key,
                bucket,
                conn: Arc::clone(&conn),
                rpc: Mutex::new(None),
                streams: Mutex::new(HashMap::new()),
            });
            let handler: Handler = {
                let inner = Arc::clone(self);
                let guard = Arc::clone(&guard);
                Arc::new(move |frame: Frame, send| {
                    let inner = Arc::clone(&inner);
                    let guard = Arc::clone(&guard);
                    Box::pin(async move {
                        handle_guard_request(inner, guard, frame, send).await;
                    })
                })
            };
            let mux = match self.dht.protomux(&conn).await {
                Ok(mux) => mux,
                Err(_) => {
                    conn.destroy();
                    return None;
                }
            };
            let rpc = match attach_rpc_channel(&mux, Arc::clone(&conn), Some(handler)) {
                Ok(rpc) => rpc,
                Err(_) => {
                    conn.destroy();
                    return None;
                }
            };
            mux.ready();
            *guard.rpc.lock().unwrap_or_else(|e| e.into_inner()) = Some(Arc::clone(&rpc));
            if rpc
                .request(op::HELLO, op::HELLO, b"endpoint", 4000)
                .await
                .is_err()
            {
                conn.destroy();
                return None;
            }
            let inner = Arc::clone(self);
            let watched = Arc::clone(&guard);
            tokio::spawn(async move {
                conn.closed().await;
                let streams: Vec<Arc<dyn Fn() + Send + Sync>> = watched
                    .streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .drain()
                    .map(|(_, close)| close)
                    .collect();
                for close in streams {
                    close();
                }
                {
                    let mut guards = inner.guards.lock().unwrap_or_else(|e| e.into_inner());
                    if let Some(index) = guards.iter().position(|g| Arc::ptr_eq(g, &watched)) {
                        guards.remove(index);
                    }
                }
                if !inner.closing() && !inner.suspended() {
                    if inner.guards.lock().unwrap_or_else(|e| e.into_inner()).len() < 2 {
                        inner.set_state(RoutingState::Bootstrapping);
                    }
                    let inner = Arc::clone(&inner);
                    tokio::spawn(async move {
                        inner.poll_all().await;
                    });
                }
            });
            Some(guard)
        })
    }

    /// `discoverGuards()`.
    async fn discover_guards(self: &Arc<Self>) -> bool {
        {
            let mut guards = self.guards.lock().unwrap_or_else(|e| e.into_inner());
            guards.retain(|guard| !guard.rpc_closed());
        }
        if self.guards.lock().unwrap_or_else(|e| e.into_inner()).len() >= 2 {
            return true;
        }
        let mut lookups = FuturesUnordered::new();
        for topic in GUARD_TOPICS.iter() {
            let topic = *topic;
            let dht = Arc::clone(&self.dht);
            lookups.push(async move {
                tokio::time::timeout(Duration::from_millis(4000), dht.lookup(topic))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .unwrap_or_default()
            });
        }
        let mut candidates: Vec<[u8; 32]> = Vec::new();
        while let Some(peers) = lookups.next().await {
            let existing = self.guard_list();
            for peer in peers {
                if existing.iter().any(|guard| guard.public_key == peer) {
                    continue;
                }
                if !candidates.contains(&peer) {
                    candidates.push(peer);
                }
            }
        }
        // Fisher-Yates with the same bias the JS shuffle has.
        for i in (1..candidates.len()).rev() {
            let bytes: [u8; 4] = rand_bytes();
            let j = (u32::from_be_bytes(bytes) % (i as u32 + 1)) as usize;
            candidates.swap(i, j);
        }
        let mut used: HashSet<String> = self
            .guard_list()
            .iter()
            .map(|guard| guard.bucket.clone())
            .collect();
        for public_key in candidates {
            if self.guard_list().len() >= 2 || self.closing() || self.suspended() {
                break;
            }
            let Some(guard) = self.connect_candidate_guard(public_key).await else {
                continue;
            };
            if used.contains(&guard.bucket) {
                guard.conn.destroy();
                continue;
            }
            used.insert(guard.bucket.clone());
            self.guards
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(guard);
        }
        self.guard_list().len() >= 2
    }

    /// `publishPresence()`.
    async fn publish_presence(self: &Arc<Self>) -> Result<bool> {
        if self.closing() || self.suspended() {
            return Ok(false);
        }
        let guards = self.guard_list();
        if guards.len() < 2 {
            return Ok(false);
        }
        let primary = Arc::clone(&guards[0]);
        let Some(primary_rpc) = primary.rpc() else {
            return Ok(false);
        };
        let now = now_ms();
        let token = self.route_token();
        let epoch = rv_epoch(now);
        let published = self
            .published
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|stamp| (stamp.guard, stamp.token, stamp.epoch.clone()));
        let need_card_renewal = self
            .cached_card
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none()
            || published
                .as_ref()
                .is_none_or(|(guard, published_token, published_epoch)| {
                    *guard != primary.public_key
                        || *published_token != token
                        || *published_epoch != epoch
                })
            || now >= self.next_renewal_at.load(Ordering::Relaxed);
        if need_card_renewal {
            let mut ask = Vec::with_capacity(36);
            ask.extend_from_slice(&token);
            ask.extend_from_slice(&(ROUTE_TTL_MS as u32).to_be_bytes());
            let ack = primary_rpc
                .request(op::REGISTER, op::REGISTER_ACK, &ask, 5000)
                .await?;
            if ack.first() != Some(&1) {
                return Err(Error::Rejected("Guard rejected route"));
            }
            let seq = self.card_seq.fetch_add(1, Ordering::Relaxed);
            let card = encode_signed_card(
                &self.key_pair,
                &primary.public_key,
                &token,
                seq,
                now + ROUTE_TTL_MS,
            );
            let sealed = seal_box(&card, &peer_seal_key(&self.key_pair.public_key));
            let signature = sign_dht_mutable(seq, &sealed, &self.key_pair);
            let mut put = Vec::with_capacity(32 + 4 + 64 + sealed.len());
            put.extend_from_slice(&self.key_pair.public_key);
            put.extend_from_slice(&(seq as u32).to_be_bytes());
            put.extend_from_slice(&signature);
            put.extend_from_slice(&sealed);
            let ack = primary_rpc
                .request(op::PUT_MUTABLE, op::PUT_MUTABLE_ACK, &put, 8000)
                .await?;
            if ack.first() != Some(&1) {
                return Err(Error::Rejected("Guard failed to publish mutable card"));
            }
            *self.cached_card.lock().unwrap_or_else(|e| e.into_inner()) = Some(card);
            *self.published.lock().unwrap_or_else(|e| e.into_inner()) = Some(PublishStamp {
                guard: primary.public_key,
                token,
                epoch: epoch.clone(),
            });
            self.next_renewal_at
                .store(now + ROUTE_TTL_MS / 2, Ordering::Relaxed);
        }
        let sources = self.joined_list();
        let cached_card = self
            .cached_card
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .unwrap_or_default();
        for source in sources {
            if !need_card_renewal
                && let Some((last_epoch, last_guard, last_token, renewal)) = source.stamp()
                && last_guard == primary.public_key
                && last_epoch == epoch
                && last_token == token
                && now < renewal
            {
                continue;
            }
            let topic = rv_topic(&source.source_key, &epoch);
            let sealed_card = seal_box(&cached_card, &rv_seal_key(&source.source_key));
            let mut ask = Vec::with_capacity(32 + 16 + 4 + sealed_card.len());
            ask.extend_from_slice(&topic);
            ask.extend_from_slice(&source.slot_id);
            ask.extend_from_slice(&(ROUTE_TTL_MS as u32).to_be_bytes());
            ask.extend_from_slice(&sealed_card);
            let ok = primary_rpc
                .request(op::RV_REGISTER, op::RV_REGISTER_ACK, &ask, 6000)
                .await
                .map(|reply| reply.first() == Some(&1))
                .unwrap_or(false);
            if ok {
                source.set_stamp(SourceStamp {
                    epoch: epoch.clone(),
                    guard: primary.public_key,
                    token,
                    next_renewal_at: now + ROUTE_TTL_MS / 2,
                });
            }
        }
        self.set_state(RoutingState::Ready);
        Ok(true)
    }

    fn joined_list(&self) -> Vec<Arc<Source>> {
        self.joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// `ensureGuardsAndPublish()`: one publish cycle at a time; callers that arrive mid-cycle
    /// wait and then run their own (the JS loop re-runs on `ensureDirty`, which is equivalent
    /// for an idempotent publish).
    async fn ensure_guards_and_publish(self: &Arc<Self>) -> bool {
        if self.closing() || self.suspended() {
            return false;
        }
        let lock = Arc::clone(&self.ensure_lock);
        let _guard = lock.lock().await;
        if self.closing() || self.suspended() {
            return false;
        }
        if !self.discover_guards().await {
            self.set_state(RoutingState::Unavailable);
            return false;
        }
        match self.publish_presence().await {
            Ok(ok) => ok,
            Err(_) => {
                self.set_state(RoutingState::Unavailable);
                false
            }
        }
    }

    /// `dialCard(card)`.
    async fn dial_card(self: &Arc<Self>, card: SignedCard) {
        if self.closing() || self.suspended() {
            return;
        }
        let peer = card.inner_public_key;
        if peer == self.key_pair.public_key
            || self
                .active_by_peer
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&peer)
            || !self
                .dialing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(peer)
        {
            return;
        }
        if (self.firewall)(&peer) {
            self.dialing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&peer);
            return;
        }
        if self.guard_list().len() < 2 && !self.ensure_guards_and_publish().await {
            self.dialing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&peer);
            return;
        }
        let candidates: Vec<Arc<Guard>> = self
            .guard_list()
            .into_iter()
            .filter(|guard| !guard.rpc_closed() && guard.public_key != card.guard_public_key)
            .collect();
        for source_guard in candidates {
            let Some(source_rpc) = source_guard.rpc() else {
                continue;
            };
            let mut raw: Option<Arc<dyn RawStream>> = None;
            let result = async {
                let stream = dht::alloc_raw_stream(&self.dht, &source_guard.conn, true).await?;
                raw = Some(Arc::clone(&stream));
                let mut ask = Vec::with_capacity(68 + source_guard.bucket.len());
                ask.extend_from_slice(&card.guard_public_key);
                ask.extend_from_slice(&card.route_token);
                ask.extend_from_slice(&stream.id().to_be_bytes());
                ask.extend_from_slice(source_guard.bucket.as_bytes());
                let reply = source_rpc
                    .request(op::DIAL_OPEN, op::DIAL_READY, &ask, 10_000)
                    .await?;
                let non_diverse = reply.len() == 5 && reply[0] == 2;
                if reply.len() != 5 || reply[0] != 1 {
                    if non_diverse {
                        return Err(Error::Rejected("Non-diverse hop"));
                    }
                    return Err(Error::Rejected("Guard rejected dial"));
                }
                let remote = u32::from_be_bytes([reply[1], reply[2], reply[3], reply[4]]);
                self.dht
                    .connect_raw_stream(&source_guard.conn, &stream, remote)
                    .await?;
                let udx_id = stream.id();
                let pending = self
                    .dht
                    .open_noise(
                        true,
                        Arc::clone(&stream),
                        &self.key_pair,
                        Some(card.inner_public_key),
                        8000,
                    )
                    .await?;
                source_guard
                    .streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(udx_id, Arc::clone(&pending.close));
                let close = Arc::clone(&pending.close);
                let guard = Arc::clone(&source_guard);
                let mut closed = pending.closed.clone();
                tokio::spawn(async move {
                    let _ = closed.wait_for(|closed| *closed).await;
                    guard
                        .streams
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&udx_id);
                    close();
                });
                self.admit_inner_stream(pending, Some(card.inner_public_key));
                raw = None;
                Ok(())
            }
            .await;
            match result {
                Ok(()) => {
                    self.dialing
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .remove(&peer);
                    return;
                }
                Err(Error::Rejected("Non-diverse hop")) => {
                    if let Some(stream) = &raw {
                        stream.destroy();
                    }
                    continue;
                }
                Err(_) => {
                    if let Some(stream) = &raw {
                        stream.destroy();
                    }
                    break;
                }
            }
        }
        self.dialing
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&peer);
    }

    /// `lookupAndDialPeer(pk)`.
    async fn lookup_and_dial_peer(self: &Arc<Self>, public_key: [u8; 32]) {
        if self.closing()
            || self.suspended()
            || self
                .active_by_peer
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&public_key)
            || self
                .dialing
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&public_key)
        {
            return;
        }
        if self.guard_list().len() < 2 && !self.ensure_guards_and_publish().await {
            return;
        }
        let Some(guard) = self.guard_list().first().cloned() else {
            return;
        };
        let Some(rpc) = guard.rpc() else {
            return;
        };
        let Ok(reply) = rpc
            .request(op::GET_MUTABLE, op::GET_MUTABLE_REPLY, &public_key, 6000)
            .await
        else {
            return;
        };
        if reply.len() <= 69 || reply[0] != 1 {
            return;
        }
        let seq = u32::from_be_bytes([reply[1], reply[2], reply[3], reply[4]]) as u64;
        let signature = &reply[5..69];
        let value = &reply[69..];
        if !verify_dht_mutable(signature, seq, value, &public_key) {
            return;
        }
        let Some(plain) = open_box(value, &peer_seal_key(&public_key)) else {
            return;
        };
        let Some(card) = decode_and_verify_card(&plain, now_ms(), Some(&public_key)) else {
            return;
        };
        if self.accept_valid_card(&card) {
            self.dial_card(card).await;
        }
    }

    /// `refreshSourceDiscovery(src)`.
    async fn refresh_source_discovery(self: &Arc<Self>, source: Arc<Source>) {
        if self.closing() || self.suspended() {
            return;
        }
        if self.guard_list().len() < 2 && !self.ensure_guards_and_publish().await {
            return;
        }
        let Some(guard) = self.guard_list().first().cloned() else {
            return;
        };
        let Some(rpc) = guard.rpc() else {
            return;
        };
        let now = now_ms();
        let epoch_now = now / RV_EPOCH_MS;
        let seal_key = rv_seal_key(&source.source_key);
        for epoch in [epoch_now, epoch_now.saturating_sub(1)] {
            let topic = rv_topic(&source.source_key, &epoch.to_string());
            let Ok(raw) = rpc
                .request(op::RV_LOOKUP, op::RV_LOOKUP_REPLY, &topic, 6000)
                .await
            else {
                continue;
            };
            let at = now_ms();
            for sealed in decode_buf_arr(&raw).unwrap_or_default() {
                let Some(plain) = open_box(&sealed, &seal_key) else {
                    continue;
                };
                let Some(card) = decode_and_verify_card(&plain, at, None) else {
                    continue;
                };
                if !self.accept_valid_card(&card) {
                    continue;
                }
                let inner = Arc::clone(self);
                tokio::spawn(async move {
                    inner.dial_card(card).await;
                });
            }
        }
    }

    /// `pollAll()`.
    async fn poll_all(self: &Arc<Self>) {
        if self.closing() || self.suspended() {
            return;
        }
        self.ensure_guards_and_publish().await;
        if self.guard_list().len() < 2 {
            return;
        }
        let mut work: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        for source in self.joined_list() {
            let inner = Arc::clone(self);
            work.push(Box::pin(async move {
                inner.refresh_source_discovery(source).await;
            }));
        }
        let peers: Vec<[u8; 32]> = self
            .explicit_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .copied()
            .collect();
        for peer in peers {
            let inner = Arc::clone(self);
            work.push(Box::pin(async move {
                inner.lookup_and_dial_peer(peer).await;
            }));
        }
        while work.next().await.is_some() {}
    }

    /// `leaveSource(id)`.
    async fn leave_source(self: &Arc<Self>, id: &str) {
        let source = self
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id);
        let Some(source) = source else {
            return;
        };
        let Some((epoch, guard, _, _)) = source.stamp() else {
            return;
        };
        let guard = self
            .guard_list()
            .into_iter()
            .find(|candidate| candidate.public_key == guard);
        let Some(guard) = guard.filter(|guard| !guard.rpc_closed()) else {
            return;
        };
        let Some(rpc) = guard.rpc() else {
            return;
        };
        let topic = rv_topic(&source.source_key, &epoch);
        let mut payload = Vec::with_capacity(48);
        payload.extend_from_slice(&topic);
        payload.extend_from_slice(&source.slot_id);
        rpc.sender().send(op::RV_UNREGISTER, 0, &payload);
    }
}

/// `OP_INC_OPEN` / `OP_CIRCUIT_DEAD` on an endpoint's guard connection.
async fn handle_guard_request(
    inner: Arc<SwarmInner>,
    guard: Arc<Guard>,
    frame: Frame,
    send: super::rpc::RpcSender,
) {
    if frame.op == op::CIRCUIT_DEAD {
        if frame.payload.len() == 4 {
            let id = u32::from_be_bytes([
                frame.payload[0],
                frame.payload[1],
                frame.payload[2],
                frame.payload[3],
            ]);
            let close = guard
                .streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
                .cloned();
            if let Some(close) = close {
                close();
            }
        }
        return;
    }
    if frame.op != op::INC_OPEN {
        return;
    }
    let payload = &frame.payload;
    let accepted = !inner.closing()
        && !inner.suspended()
        && payload.len() == 36
        && payload[..32] == inner.route_token()[..];
    if !accepted {
        send.send(op::INC_READY, frame.id, &[0; 5]);
        return;
    }
    let caller_udx = u32::from_be_bytes([payload[32], payload[33], payload[34], payload[35]]);
    let result = async {
        let stream = dht::alloc_raw_stream(&inner.dht, &guard.conn, true).await?;
        inner
            .dht
            .connect_raw_stream(&guard.conn, &stream, caller_udx)
            .await?;
        let udx_id = stream.id();
        let pending = inner
            .dht
            .open_noise(false, Arc::clone(&stream), &inner.key_pair, None, 8000)
            .await?;
        guard
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(udx_id, Arc::clone(&pending.close));
        // A responder closes the raw stream when the inner stream closes.
        let close = Arc::clone(&pending.close);
        let guard = Arc::clone(&guard);
        let mut closed = pending.closed.clone();
        tokio::spawn(async move {
            let _ = closed.wait_for(|closed| *closed).await;
            guard
                .streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&udx_id);
            close();
            stream.destroy();
        });
        inner.admit_inner_stream(pending, None);
        Ok::<u32, Error>(udx_id)
    }
    .await;
    match result {
        Ok(udx_id) => {
            let mut ready = Vec::with_capacity(5);
            ready.push(1);
            ready.extend_from_slice(&udx_id.to_be_bytes());
            send.send(op::INC_READY, frame.id, &ready);
        }
        Err(_) => {
            send.send(op::INC_READY, frame.id, &[0; 5]);
        }
    }
}

/// A live private swarm.
#[derive(Clone)]
pub struct PrivateSwarm {
    inner: Arc<SwarmInner>,
}

/// `createPrivateSwarm(...)`; the [`Dht`] must have been opened with the same bind/bootstrap.
pub fn create_private_swarm(dht: Arc<dyn Dht>, opts: SwarmOptions) -> PrivateSwarm {
    let allow_loopback = opts
        .allow_loopback
        .unwrap_or_else(|| super::is_local_bootstrap(opts.host.as_deref(), &opts.bootstrap));
    let key_pair = opts
        .key_pair
        .unwrap_or_else(|| KeyPair::from_seed(rand_bytes()));
    let inner = Arc::new(SwarmInner {
        dht,
        key_pair,
        transport_key_pair: KeyPair::from_seed(rand_bytes()),
        allow_loopback,
        resolve_address: opts.resolve_address,
        firewall: opts.firewall.unwrap_or_else(|| Arc::new(|_| false)),
        on_connection: opts.on_connection,
        on_update: opts.on_update,
        guards: Mutex::new(Vec::new()),
        connections: Mutex::new(HashMap::new()),
        active_by_peer: Mutex::new(HashMap::new()),
        dialing: Mutex::new(HashSet::new()),
        explicit_peers: Mutex::new(HashSet::new()),
        joined: Mutex::new(HashMap::new()),
        highest_seq: Mutex::new(HashMap::new()),
        route_token: Mutex::new(rand_bytes()),
        card_seq: AtomicU64::new(1),
        cached_card: Mutex::new(None),
        published: Mutex::new(None),
        next_renewal_at: AtomicU64::new(0),
        state: Mutex::new(RoutingState::Bootstrapping),
        suspended: AtomicBool::new(false),
        closing: AtomicBool::new(false),
        ensure_lock: Arc::new(AsyncMutex::new(())),
        next_stream_id: AtomicU64::new(1),
        poll_task: Mutex::new(None),
    });
    {
        let inner = Arc::clone(&inner);
        tokio::spawn(async move {
            inner.ensure_guards_and_publish().await;
        });
    }
    let poll = tokio::spawn({
        let inner = Arc::clone(&inner);
        async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(4));
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if inner.closing() {
                    return;
                }
                inner.poll_all().await;
            }
        }
    });
    *inner.poll_task.lock().unwrap_or_else(|e| e.into_inner()) = Some(poll);
    PrivateSwarm { inner }
}

impl PrivateSwarm {
    pub fn key_pair(&self) -> &KeyPair {
        &self.inner.key_pair
    }

    pub fn public_key(&self) -> [u8; 32] {
        self.inner.key_pair.public_key
    }

    pub fn private_routing(&self) -> PrivateRouting {
        PrivateRouting {
            inner: Arc::clone(&self.inner),
        }
    }

    /// `swarm.connections.size`.
    pub fn connection_count(&self) -> usize {
        self.inner
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// `await swarm.listening`.
    pub async fn listen(&self) -> bool {
        self.inner.ensure_guards_and_publish().await
    }

    /// `swarm.joinSource(id, sourceKey)`.
    pub fn join_source(&self, id: &str, source_key: &[u8; 32]) -> SourceHandle {
        let existing = self
            .inner
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned();
        let topic = rv_topic(source_key, &rv_epoch(now_ms()));
        if let Some(source) = existing {
            return SourceHandle {
                topic: source.topic,
                is_server: false,
                is_client: true,
                inner: Arc::clone(&self.inner),
                id: Some(source.id.clone()),
            };
        }
        let source = Arc::new(Source {
            id: id.to_owned(),
            source_key: *source_key,
            slot_id: rand_bytes(),
            last: Mutex::new(None),
            topic,
        });
        self.inner
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.to_owned(), Arc::clone(&source));
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            inner.poll_all().await;
        });
        SourceHandle {
            topic,
            is_server: false,
            is_client: true,
            inner: Arc::clone(&self.inner),
            id: Some(id.to_owned()),
        }
    }

    /// `swarm.leaveSource(id)`.
    pub async fn leave_source(&self, id: &str) {
        self.inner.leave_source(id).await;
    }

    /// `swarm.join(topic)`: a discovery-only handle.
    pub fn join(&self, topic: [u8; 32]) -> SourceHandle {
        SourceHandle {
            topic,
            is_server: false,
            is_client: true,
            inner: Arc::clone(&self.inner),
            id: None,
        }
    }

    /// `swarm.leave()`: a no-op in the app, kept for interface parity.
    pub async fn leave(&self) {}

    /// `swarm.joinPeer(publicKey)`.
    pub fn join_peer(&self, public_key: [u8; 32]) {
        if public_key == self.inner.key_pair.public_key {
            return;
        }
        self.inner
            .explicit_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(public_key);
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            inner.lookup_and_dial_peer(public_key).await;
        });
    }

    /// `swarm.leavePeer(publicKey)`.
    pub fn leave_peer(&self, public_key: [u8; 32]) {
        self.inner
            .explicit_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&public_key);
    }

    /// `swarm.status(topic)`.
    pub fn status(&self, topic: [u8; 32]) -> Option<SourceHandle> {
        self.inner
            .joined_list()
            .into_iter()
            .find(|source| source.topic == topic)
            .map(|source| SourceHandle {
                topic: source.topic,
                is_server: false,
                is_client: true,
                inner: Arc::clone(&self.inner),
                id: Some(source.id.clone()),
            })
    }

    /// `swarm.topics()`.
    pub fn topics(&self) -> Vec<SourceHandle> {
        self.inner
            .joined_list()
            .into_iter()
            .map(|source| SourceHandle {
                topic: source.topic,
                is_server: false,
                is_client: true,
                inner: Arc::clone(&self.inner),
                id: Some(source.id.clone()),
            })
            .collect()
    }

    pub async fn suspend(&self) {
        if self.inner.suspended.swap(true, Ordering::Relaxed) || self.inner.closing() {
            return;
        }
        self.inner.set_state(RoutingState::Suspended);
        *self
            .inner
            .published
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        self.inner.next_renewal_at.store(0, Ordering::Relaxed);
        *self
            .inner
            .cached_card
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = None;
        let connections: Vec<Arc<Admitted>> = self
            .inner
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, admitted)| admitted)
            .collect();
        for admitted in connections {
            (admitted.close)();
        }
        self.inner
            .active_by_peer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        let guards: Vec<Arc<Guard>> = self
            .inner
            .guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        for guard in guards {
            guard.conn.destroy();
        }
        let _ = self.inner.dht.suspend().await;
    }

    pub async fn resume(&self) {
        if !self.inner.suspended.load(Ordering::Relaxed) || self.inner.closing() {
            return;
        }
        let _ = self.inner.dht.resume().await;
        *self
            .inner
            .route_token
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = rand_bytes();
        self.inner.suspended.store(false, Ordering::Relaxed);
        self.inner.set_state(RoutingState::Bootstrapping);
        self.inner.poll_all().await;
    }

    /// `swarm.clear()`: leaves every source and forgets explicit peers.
    pub async fn clear(&self) {
        let ids: Vec<String> = self
            .inner
            .joined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        for id in ids {
            self.inner.leave_source(&id).await;
        }
        self.inner
            .explicit_peers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    pub async fn destroy(&self) {
        if self.inner.closing.swap(true, Ordering::Relaxed) {
            return;
        }
        self.inner.set_state(RoutingState::Destroyed);
        if let Some(task) = self
            .inner
            .poll_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            task.abort();
        }
        let connections: Vec<Arc<Admitted>> = self
            .inner
            .connections
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, admitted)| admitted)
            .collect();
        for admitted in connections {
            (admitted.close)();
        }
        self.inner
            .active_by_peer
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        let guards: Vec<Arc<Guard>> = self
            .inner
            .guards
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect();
        for guard in guards {
            guard.conn.destroy();
        }
        let _ = self.inner.dht.destroy().await;
    }
}

/// Options for [`publish_standalone_route_card`].
pub struct StandaloneCardOptions {
    pub host: Option<String>,
    pub bootstrap: Vec<String>,
    pub guard_public_key: [u8; 32],
    pub advertised_key_pair: KeyPair,
    /// Defaults to `advertised_key_pair`.
    pub responder_key_pair: Option<KeyPair>,
    pub on_inner_connection: Option<Arc<dyn Fn(NoiseConnection) + Send + Sync>>,
}

/// `publishStandaloneRouteCard(...)`: one endpoint-less card, used by the diagnostics page.
///
/// Unlike the JS original, the DHT is supplied by the caller (the same seam the guard and swarm
/// use); [`StandaloneRouteCard::close`] therefore only tears down the control connection.
pub async fn publish_standalone_route_card(
    dht: Arc<dyn Dht>,
    opts: StandaloneCardOptions,
) -> Result<StandaloneRouteCard> {
    let transport_key_pair = KeyPair::from_seed(rand_bytes());
    let conn = dht
        .connect(opts.guard_public_key, &transport_key_pair)
        .await?;
    let route_token: [u8; 32] = rand_bytes();
    let responder = opts
        .responder_key_pair
        .unwrap_or_else(|| opts.advertised_key_pair.clone());
    let handler: Handler = {
        let dht = Arc::clone(&dht);
        let conn = Arc::clone(&conn);
        let responder = responder.clone();
        let on_inner_connection = opts.on_inner_connection.clone();
        Arc::new(move |frame: Frame, send| {
            let dht = Arc::clone(&dht);
            let conn = Arc::clone(&conn);
            let responder = responder.clone();
            let on_inner_connection = on_inner_connection.clone();
            Box::pin(async move {
                if frame.op != op::INC_OPEN || frame.payload.len() != 36 {
                    return;
                }
                let caller = u32::from_be_bytes([
                    frame.payload[32],
                    frame.payload[33],
                    frame.payload[34],
                    frame.payload[35],
                ]);
                let result = async {
                    let stream = dht::alloc_raw_stream(&dht, &conn, true).await?;
                    dht.connect_raw_stream(&conn, &stream, caller).await?;
                    let id = stream.id();
                    let pending = dht
                        .open_noise(false, Arc::clone(&stream), &responder, None, 8000)
                        .await?;
                    Ok::<(u32, PendingNoise), Error>((id, pending))
                }
                .await;
                match result {
                    Ok((id, pending)) => {
                        let mut ready = Vec::with_capacity(5);
                        ready.push(1);
                        ready.extend_from_slice(&id.to_be_bytes());
                        send.send(op::INC_READY, frame.id, &ready);
                        let PendingNoise {
                            opened,
                            close,
                            closed,
                        } = pending;
                        tokio::spawn(async move {
                            match opened.await {
                                Ok(opened) => match on_inner_connection {
                                    Some(on_inner_connection) => {
                                        on_inner_connection(NoiseConnection {
                                            remote_public_key: opened.remote_public_key,
                                            is_initiator: opened.is_initiator,
                                            handshake_hash: opened.handshake_hash,
                                            stream: opened.stream,
                                            close,
                                            closed,
                                        })
                                    }
                                    None => close(),
                                },
                                Err(_) => close(),
                            }
                        });
                    }
                    Err(_) => {
                        send.send(op::INC_READY, frame.id, &[0; 5]);
                    }
                }
            })
        })
    };
    let mux = dht.protomux(&conn).await?;
    let rpc = attach_rpc_channel(&mux, Arc::clone(&conn), Some(handler))?;
    mux.ready();
    rpc.request(op::HELLO, op::HELLO, b"endpoint", 4000).await?;
    let mut register = Vec::with_capacity(36);
    register.extend_from_slice(&route_token);
    register.extend_from_slice(&(ROUTE_TTL_MS as u32).to_be_bytes());
    rpc.request(op::REGISTER, op::REGISTER_ACK, &register, 4000)
        .await?;
    let seq = 1u64;
    let card = encode_signed_card(
        &opts.advertised_key_pair,
        &opts.guard_public_key,
        &route_token,
        seq,
        now_ms() + ROUTE_TTL_MS,
    );
    let sealed = seal_box(&card, &peer_seal_key(&opts.advertised_key_pair.public_key));
    let mut put = Vec::with_capacity(32 + 4 + 64 + sealed.len());
    put.extend_from_slice(&opts.advertised_key_pair.public_key);
    put.extend_from_slice(&(seq as u32).to_be_bytes());
    put.extend_from_slice(&sign_dht_mutable(seq, &sealed, &opts.advertised_key_pair));
    put.extend_from_slice(&sealed);
    rpc.request(op::PUT_MUTABLE, op::PUT_MUTABLE_ACK, &put, 6000)
        .await?;
    Ok(StandaloneRouteCard { conn })
}

/// The teardown handle for [`publish_standalone_route_card`].
pub struct StandaloneRouteCard {
    conn: Arc<dyn Connection>,
}

impl StandaloneRouteCard {
    /// Closes the guard connection.
    pub async fn close(&self) {
        self.conn.destroy();
    }
}

/// What [`attempt_direct_hop_bypass`] observes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BypassReport {
    pub endpoint_bypass_accepted: bool,
    pub spoofed_hop_accepted: bool,
}

/// `attemptDirectHopBypass(...)`: the diagnostics page's probe of the guards' role separation.
pub async fn attempt_direct_hop_bypass(
    dht: Arc<dyn Dht>,
    target_peer_public_key: [u8; 32],
) -> Result<BypassReport> {
    let record = dht
        .mutable_get(target_peer_public_key)
        .await?
        .filter(|record| !record.value.is_empty())
        .ok_or(Error::Rejected("Target mutable card not found"))?;
    let plain = open_box(&record.value, &peer_seal_key(&target_peer_public_key))
        .ok_or(Error::Rejected("Target card invalid"))?;
    let card = decode_and_verify_card(&plain, now_ms(), Some(&target_peer_public_key))
        .ok_or(Error::Rejected("Target card invalid"))?;
    // 1. Endpoint session attempting a forbidden OP_HOP_OPEN on the destination guard.
    let mut endpoint_bypass_accepted = false;
    let endpoint_key_pair = KeyPair::from_seed(rand_bytes());
    if let Ok(conn) = dht.connect(card.guard_public_key, &endpoint_key_pair).await {
        if let Ok(mux) = dht.protomux(&conn).await
            && let Ok(rpc) = attach_rpc_channel(&mux, Arc::clone(&conn), None)
        {
            mux.ready();
            let _ = rpc.request(op::HELLO, op::HELLO, b"endpoint", 3000).await;
            if let Ok(raw) = dht::alloc_raw_stream(&dht, &conn, true).await {
                let mut ask = Vec::with_capacity(36);
                ask.extend_from_slice(&card.route_token);
                ask.extend_from_slice(&raw.id().to_be_bytes());
                if let Ok(reply) = rpc.request(op::HOP_OPEN, op::HOP_READY, &ask, 3000).await {
                    endpoint_bypass_accepted = reply.len() == 5 && reply[0] == 1;
                }
                raw.destroy();
            }
        }
        conn.destroy();
    }

    // 2. An unadvertised key pair claiming the 'hop' role on the destination guard.
    let mut spoofed_hop_accepted = false;
    let hop_key_pair = KeyPair::from_seed(rand_bytes());
    if let Ok(conn) = dht.connect(card.guard_public_key, &hop_key_pair).await {
        if let Ok(mux) = dht.protomux(&conn).await
            && let Ok(rpc) = attach_rpc_channel(&mux, Arc::clone(&conn), None)
            && rpc
                .request(op::HELLO, op::HELLO, b"hop", 3000)
                .await
                .is_ok()
            && let Ok(raw) = dht::alloc_raw_stream(&dht, &conn, true).await
        {
            let mut ask = Vec::with_capacity(36);
            ask.extend_from_slice(&card.route_token);
            ask.extend_from_slice(&raw.id().to_be_bytes());
            if let Ok(reply) = rpc.request(op::HOP_OPEN, op::HOP_READY, &ask, 3000).await {
                spoofed_hop_accepted = reply.len() == 5 && reply[0] == 1;
            }
            raw.destroy();
        }
        conn.destroy();
    }
    Ok(BypassReport {
        endpoint_bypass_accepted,
        spoofed_hop_accepted,
    })
}
