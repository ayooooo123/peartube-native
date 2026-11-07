//! Relay upload sharing, ported from PearTube's `src/traffic.js`.
//!
//! A relay detects its own viewers, tells connected PearTube relays over
//! `peartube/bulk-busy` (their backfill waits while any peer is busy), and
//! paces the bulk connections it is told about (HiveRelays) under one budget.
//! Connections come in through [`Connection`]; the gate never owns a socket.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compact_encoding::decode as decode_compact;
use protomux::{ChannelOptions, ChannelSender, Event, Mux};
use serde::Serialize;

pub const BUSY_PROTOCOL: &str = "peartube/bulk-busy";
/// The app ticks every 250 ms; kept so pacing decisions match it. The timer
/// only runs while connections are attached.
pub const TICK_MS: u64 = 250;
/// While viewers stream, paused bulk connections get turns one at a time.
/// Each still gets a turn within `TURN_MS`, inside Protomux's 10 s timeout.
pub const TURN_MS: u64 = 6000;
/// More than 1 Mbps to non-bulk peers, held this long, means someone is watching.
pub const VIEWER_HOLD_MS: u64 = 3000;
/// 1 Mbps for one 250 ms tick (`125_000 / 4`, as in the app).
pub const VIEWER_BYTES_PER_TICK: u64 = 125_000 / 4;
/// While viewers stream, a bulk peer may use this much per tick (0.5 Mbps).
pub const VIEWING_TICK_CAP: f64 = 15_625.0;

/// Wall clock the gate reads; tests substitute a fake one.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis() as u64)
    }
}

/// One transport connection as the gate sees it.
pub trait Connection: Send + Sync {
    /// Unique among live connections; the gate keys its per-connection state on it.
    fn id(&self) -> u64;
    /// Raw bytes handed to the transport so far (viewer and bulk accounting).
    fn raw_bytes_written(&self) -> u64;
    fn pause(&self);
    fn resume(&self);
    fn destroyed(&self) -> bool;
    fn mux(&self) -> Mux;
    fn remote_public_key(&self) -> Option<[u8; 32]> {
        None
    }
}

pub type ViewingHook = Arc<dyn Fn() -> bool + Send + Sync>;
pub type BulkPeerHook = Arc<dyn Fn(&str, bool) + Send + Sync>;

pub struct Options {
    pub upload_mbps: f64,
    pub private_routing: bool,
    /// Run the 250 ms tick timer; tests drive [`TrafficGate::tick`] themselves.
    pub auto_tick: bool,
    pub clock: Arc<dyn Clock>,
    /// The HTTP blob server is serving a viewer right now.
    pub http_viewing: Option<ViewingHook>,
    /// Told when a peer becomes bulk (or stops being bulk) so it can be bucketed.
    pub mark_bulk_peer: Option<BulkPeerHook>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            upload_mbps: 0.0,
            private_routing: false,
            auto_tick: true,
            clock: Arc::new(SystemClock),
            http_viewing: None,
            mark_bulk_peer: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub bulk_upload_mbps: f64,
    pub bulk_peers: usize,
    pub bulk_paused: bool,
    pub serving_viewers: bool,
    pub peers_busy: bool,
}

struct Bulk {
    id: u64,
    conn: Arc<dyn Connection>,
    written: u64,
    last_turn: i64,
}

struct Busy {
    sender: ChannelSender,
    remote: bool,
}

#[derive(Default)]
struct State {
    conns: Vec<(u64, Arc<dyn Connection>)>,
    bulk: Vec<Bulk>,
    viewer_sent: std::collections::HashMap<u64, u64>,
    busy: std::collections::HashMap<u64, Busy>,
    viewers_until: i64,
    viewing: bool,
    bulk_tokens: f64,
    bulk_paused: bool,
    turn: Option<u64>,
}

struct Shared {
    upload_mbps: f64,
    bulk_bytes_per_tick: f64,
    private_routing: bool,
    auto_tick: bool,
    clock: Arc<dyn Clock>,
    http_viewing: Option<ViewingHook>,
    mark_bulk_peer: Option<BulkPeerHook>,
    state: Mutex<State>,
    closed: AtomicBool,
    ticking: AtomicBool,
}

/// Upload sharing for one relay node. Cheap to clone.
#[derive(Clone)]
pub struct TrafficGate {
    shared: Arc<Shared>,
}

impl TrafficGate {
    pub fn new(opts: Options) -> Self {
        let bulk_bytes_per_tick = if opts.upload_mbps > 0.0 {
            opts.upload_mbps * 125_000.0 / 4.0
        } else {
            0.0
        };
        TrafficGate {
            shared: Arc::new(Shared {
                upload_mbps: opts.upload_mbps,
                bulk_bytes_per_tick,
                private_routing: opts.private_routing,
                auto_tick: opts.auto_tick,
                clock: opts.clock,
                http_viewing: opts.http_viewing,
                mark_bulk_peer: opts.mark_bulk_peer,
                state: Mutex::new(State {
                    bulk_tokens: bulk_bytes_per_tick,
                    ..State::default()
                }),
                closed: AtomicBool::new(false),
                ticking: AtomicBool::new(false),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.shared.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    /// Starts tracking a connection and opens the busy channel on it.
    pub fn attach(&self, conn: Arc<dyn Connection>) {
        if self.closed() || self.shared.private_routing || conn.destroyed() {
            return;
        }
        let mux = conn.mux();
        {
            let mut st = self.lock();
            if !st.conns.iter().any(|(id, _)| *id == conn.id()) {
                st.conns.push((conn.id(), conn.clone()));
            }
        }
        if !mux.is_opened(BUSY_PROTOCOL, None) {
            let pairs = mux.pair(BUSY_PROTOCOL, None);
            self.attach_busy(&conn, &mux);
            let gate = self.clone();
            let pair_mux = mux.clone();
            tokio::spawn(async move {
                let mut pairs = pairs;
                while let Some(request) = pairs.recv().await {
                    if gate.closed() {
                        break;
                    }
                    // The peer opened first: answer with our own channel.
                    gate.attach_busy(&conn, &pair_mux);
                    drop(request);
                }
            });
        }
        self.ensure_timer();
    }

    fn attach_busy(&self, conn: &Arc<dyn Connection>, mux: &Mux) {
        if self.closed() || conn.destroyed() || mux.is_opened(BUSY_PROTOCOL, None) {
            return;
        }
        let Some(channel) = mux.open_channel(ChannelOptions::new(BUSY_PROTOCOL)) else {
            return;
        };
        let sender = channel.sender();
        let id = conn.id();
        let gate = self.clone();
        tokio::spawn(async move {
            let mut channel = channel;
            while let Some(event) = channel.recv().await {
                match event {
                    Event::Opened { .. } => gate.on_busy_open(id, sender.clone()),
                    Event::Message(message) => {
                        if let Ok(value) = decode_compact::<u64>(message.data.clone()) {
                            gate.on_busy_message(id, value == 1);
                        }
                    }
                    Event::Closed { .. } => break,
                }
            }
            gate.on_busy_close(id);
        });
    }

    fn on_busy_open(&self, id: u64, sender: ChannelSender) {
        if self.closed() {
            return;
        }
        let mut st = self.lock();
        st.busy.insert(
            id,
            Busy {
                sender: sender.clone(),
                remote: false,
            },
        );
        // It paces itself from here on; release it if the budget had paused it.
        if st.bulk_paused
            && let Some(bulk) = st.bulk.iter().find(|b| b.id == id)
        {
            bulk.conn.resume();
        }
        sender.send(0, &u64::from(st.viewing));
    }

    fn on_busy_message(&self, id: u64, remote: bool) {
        if let Some(rec) = self.lock().busy.get_mut(&id) {
            rec.remote = remote;
        }
    }

    fn on_busy_close(&self, id: u64) {
        self.lock().busy.remove(&id);
    }

    fn ensure_timer(&self) {
        if !self.shared.auto_tick {
            return;
        }
        if self.shared.ticking.swap(true, Ordering::AcqRel) {
            return;
        }
        let gate = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(TICK_MS)).await;
                if gate.closed() {
                    break;
                }
                gate.tick();
                if gate.live_connections() == 0 {
                    break;
                }
            }
            gate.shared.ticking.store(false, Ordering::Release);
        });
    }

    fn live_connections(&self) -> usize {
        self.lock().conns.len()
    }

    /// Keeps bulk peers (HiveRelays and replica relays) off viewers' upload.
    pub fn mark_bulk(&self, conn: Arc<dyn Connection>) {
        if self.closed() || conn.destroyed() {
            return;
        }
        let hook = {
            let mut st = self.lock();
            if st.bulk.iter().any(|b| b.id == conn.id()) {
                return;
            }
            st.bulk.push(Bulk {
                id: conn.id(),
                conn: conn.clone(),
                written: conn.raw_bytes_written(),
                last_turn: 0,
            });
            let paused = st.bulk_paused && !st.busy.contains_key(&conn.id());
            if paused {
                conn.pause();
            }
            conn.remote_public_key().map(|key| (hex(&key), true))
        };
        self.mark_peer(hook);
    }

    /// A publisher (another PearTube relay) opens the same channel but sends
    /// seed requests; HiveRelays never do.
    pub fn unmark_bulk(&self, conn: &Arc<dyn Connection>) {
        let hook = {
            let mut st = self.lock();
            unmark_bulk_state(&mut st, conn.id())
        };
        self.mark_peer(hook);
    }

    fn mark_peer(&self, hook: Option<(String, bool)>) {
        if let (Some((key, bulk)), Some(cb)) = (hook, &self.shared.mark_bulk_peer) {
            cb(&key, bulk);
        }
    }

    /// Any connected relay reports viewers.
    pub fn peers_busy(&self) -> bool {
        self.lock().busy.values().any(|rec| rec.remote)
    }

    pub fn info(&self) -> Info {
        let st = self.lock();
        Info {
            bulk_upload_mbps: self.shared.upload_mbps,
            bulk_peers: st.bulk.len(),
            bulk_paused: st.bulk_paused,
            serving_viewers: st.viewing,
            peers_busy: st.busy.values().any(|rec| rec.remote),
        }
    }

    pub fn tick(&self) {
        if self.closed() {
            return;
        }
        let hooks = {
            let mut st = self.lock();
            tick_state(&self.shared, &mut st)
        };
        for hook in hooks {
            self.mark_peer(Some(hook));
        }
    }

    pub fn stop(&self) {
        self.shared.closed.store(true, Ordering::Release);
        let mut st = self.lock();
        for bulk in st.bulk.drain(..) {
            bulk.conn.resume();
        }
        st.bulk_paused = false;
        st.turn = None;
        st.viewing = false;
        for (_, rec) in st.busy.drain() {
            rec.sender.close();
        }
    }
}

fn unmark_bulk_state(st: &mut State, id: u64) -> Option<(String, bool)> {
    let pos = st.bulk.iter().position(|b| b.id == id)?;
    let bulk = st.bulk.remove(pos);
    let hook = bulk
        .conn
        .remote_public_key()
        .filter(|key| {
            // The peer stays bulk while another of its connections still is.
            !st.bulk
                .iter()
                .any(|other| other.conn.remote_public_key().as_ref() == Some(key))
        })
        .map(|key| (hex(&key), false));
    if st.bulk_paused {
        bulk.conn.resume();
    }
    hook
}

fn tick_state(sh: &Shared, st: &mut State) -> Vec<(String, bool)> {
    let mut hooks = Vec::new();
    st.conns.retain(|(_, conn)| !conn.destroyed());
    let now = sh.clock.now_ms() as i64;

    let viewer_written: Vec<(u64, u64)> = st
        .conns
        .iter()
        .filter(|(id, _)| !st.bulk.iter().any(|b| b.id == *id))
        .map(|(id, conn)| (*id, conn.raw_bytes_written()))
        .collect();
    let mut viewer_bytes = 0u64;
    for (id, written) in viewer_written {
        if let Some(prev) = st.viewer_sent.insert(id, written) {
            viewer_bytes += written.saturating_sub(prev);
        }
    }
    if viewer_bytes >= VIEWER_BYTES_PER_TICK || sh.http_viewing.as_ref().is_some_and(|cb| cb()) {
        st.viewers_until = now + VIEWER_HOLD_MS as i64;
    }
    let now_viewing = now < st.viewers_until;
    if now_viewing != st.viewing {
        st.viewing = now_viewing;
        let value = u64::from(now_viewing);
        for rec in st.busy.values() {
            rec.sender.send(0, &value);
        }
    }
    if sh.bulk_bytes_per_tick == 0.0 {
        return hooks;
    }

    let mut sent = 0u64;
    let mut dead = Vec::new();
    for bulk in st.bulk.iter_mut() {
        if bulk.conn.destroyed() {
            dead.push(bulk.id);
            continue;
        }
        let written = bulk.conn.raw_bytes_written();
        if !st.busy.contains_key(&bulk.id) {
            sent += written.saturating_sub(bulk.written);
        }
        bulk.written = written;
    }
    for id in dead {
        if let Some(hook) = unmark_bulk_state(st, id) {
            hooks.push(hook);
        }
    }

    let tick_budget = if st.viewing {
        sh.bulk_bytes_per_tick.min(VIEWING_TICK_CAP)
    } else {
        sh.bulk_bytes_per_tick
    };
    let next_tokens = (tick_budget * 2.0).min(st.bulk_tokens + tick_budget - sent as f64);
    st.bulk_tokens = if st.viewing {
        (-tick_budget * 24.0).max(next_tokens)
    } else {
        next_tokens
    };
    let pause = st.bulk_tokens < 0.0;

    if let Some(turn) = st.turn.take()
        && let Some(bulk) = st.bulk.iter().find(|b| b.id == turn)
        && st.bulk_paused
    {
        // A turn lasts one tick: the backlog it released is already being served.
        bulk.conn.pause();
    }

    if st.viewing && pause {
        let held: Vec<(u64, Arc<dyn Connection>)> = st
            .bulk
            .iter()
            .filter(|b| !st.busy.contains_key(&b.id))
            .map(|b| (b.id, b.conn.clone()))
            .collect();
        if !st.bulk_paused {
            st.bulk_paused = true;
            let count = held.len() as i64;
            for (i, (id, conn)) in held.iter().enumerate() {
                conn.pause();
                // Spread the first turns over TURN_MS instead of all coming due together.
                st.turn_schedule(*id, now - (i as i64 * TURN_MS as i64 / count));
            }
        }
        let next = held
            .iter()
            .min_by_key(|(id, _)| st.last_turn_of(*id))
            .map(|(id, conn)| (*id, conn.clone()));
        if let Some((id, conn)) = next
            && now - st.last_turn_of(id) >= TURN_MS as i64
        {
            conn.resume();
            st.turn_schedule(id, now);
            st.turn = Some(id);
        }
        return hooks;
    }
    if pause == st.bulk_paused {
        return hooks;
    }
    st.bulk_paused = pause;
    for bulk in &st.bulk {
        if st.busy.contains_key(&bulk.id) {
            continue;
        }
        if pause {
            bulk.conn.pause();
        } else {
            bulk.conn.resume();
        }
    }
    hooks
}

impl State {
    fn turn_schedule(&mut self, id: u64, at: i64) {
        if let Some(bulk) = self.bulk.iter_mut().find(|b| b.id == id) {
            bulk.last_turn = at;
        }
    }

    fn last_turn_of(&self, id: u64) -> i64 {
        self.bulk
            .iter()
            .find(|b| b.id == id)
            .map_or(0, |b| b.last_turn)
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
