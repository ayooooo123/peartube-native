//! The TV receiver, ported from `createReceiver` in `src/cast.js`: identity on disk, the cast
//! control channel on every peer connection, the loopback HTTP server its own player reads, mDNS
//! advertising, and the windowed P2P media side. It opens no server of its own for peers: senders
//! reach it on the connection they already share.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use hyperblobs::BlobId;
use serde_json::{Map, Value, json};
use tokio::task::JoinHandle;

use crate::protocol::{
    BlobPointer, ParsedUri, RECEIVER_PORT, ReceiverUriInput, format_receiver_uri, parse_uri,
};

use super::control::{
    Control, ControlHandlers, RequestHandler, command_from_wire, serve_control, state_to_wire,
};
use super::http::{self, Body, HttpServer, guess_media_mime};
use super::mdns::{self, AdvertiseHandle, AdvertiseInfo};
use super::media::Media;
use super::pairing::{PairLimiter, ReceiverIdentity, random_hex, safe_equal};
use super::state::{self, CastState, num, report_patch, truthy};
use super::traits::{
    CastPeer, ChangeSink, ClearCacheOptions, CommandSink, LanEndpoint, PeerConn, PeerStatus,
};
use super::{CastError, LOW_DISK_BOOT_BYTES, LOW_DISK_STREAM_BYTES, REMEMBERED_STREAMS, disk};
use crate::control_protocol::Body as Payload;
use crate::wire::peartube::CastPaired;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or(0)
}

/// `CAST_LAN_TOPIC`: hash of the cast protocol name. Senders and receivers on one LAN join it, so LAN
/// discovery links their nodes before a cast needs the link.
pub fn cast_lan_topic() -> [u8; 32] {
    hypercore::crypto::hash(&[super::control::PROTOCOL.as_bytes()])
}

/// Options for [`Receiver::start`], the app's `createReceiver` argument object.
pub struct ReceiverOptions {
    pub storage: PathBuf,
    pub peer_storage: Option<PathBuf>,
    pub name: String,
    pub port: u16,
    pub lan_host: Option<String>,
    /// The node the host opened for this receiver (with `receiverKeyPair`).
    pub peer: Option<Arc<dyn CastPeer>>,
    /// Bind an mDNS advertisement (the apps always do; tests stand in for the adapter).
    pub mdns: bool,
    /// No pairing for senders in the same room: any sender on a LAN link may cast.
    pub open_auth: bool,
    pub any_port: bool,
    pub on_command: Option<CommandSink>,
    pub on_change: Option<ChangeSink>,
}

impl Default for ReceiverOptions {
    fn default() -> Self {
        Self {
            storage: PathBuf::new(),
            peer_storage: None,
            name: "PearTube TV".to_owned(),
            port: RECEIVER_PORT,
            lan_host: None,
            peer: None,
            mdns: false,
            open_auth: false,
            any_port: true,
            on_command: None,
            on_change: None,
        }
    }
}

/// A load's resolved media, before it becomes the active stream.
struct Prepared {
    entry: Value,
    mode: &'static str,
    stream_url: String,
}

struct Inner {
    identity: ReceiverIdentity,
    storage: PathBuf,
    peer_root: PathBuf,
    public_key: String,
    open_auth: bool,
    current_host: Mutex<Option<String>>,
    port: AtomicU16,
    state: Mutex<CastState>,
    active: Mutex<Option<Arc<Media>>>,
    streams: Mutex<Vec<Arc<Media>>>,
    seq: AtomicU64,
    controls: Mutex<Vec<Control>>,
    subscribers: Mutex<Vec<Control>>,
    // Per queued subscriber: whether a newer state arrived while waiting for drain.
    backpressured: Mutex<HashMap<u64, bool>>,
    limiter_lan: Mutex<PairLimiter>,
    limiter_far: Mutex<PairLimiter>,
    peer: Arc<dyn CastPeer>,
    on_command: Option<CommandSink>,
    on_change: Option<ChangeSink>,
    last_notify: Mutex<String>,
    qr_cache: Mutex<(String, String)>,
    ad: Mutex<Option<AdvertiseHandle>>,
    ad_sig: Mutex<String>,
    mdns: bool,
    closing: AtomicBool,
    last_loaded_blobs: Mutex<String>,
    evicting: AtomicBool,
    /// `steerStreams`' one-at-a-time guard: a reader's open and the timer never steer twice over.
    steering: AtomicBool,
    last_evict_at: Mutex<Instant>,
    free_cache: Mutex<(Instant, u64)>,
    timers: Mutex<Vec<JoinHandle<()>>>,
    unserve: Mutex<HashMap<u64, super::control::ServeHandle>>,
    stop_serving: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
}

/// The advertisement `refreshMdns` publishes, or `None` when the receiver has no LAN host to
/// name: the app returns without advertising then (`h` falls back to `127.0.0.1` only for an
/// adapter a host injects). `lp` stays `0` when the node's LAN DHT did not start.
fn advertise_info(
    identity: &ReceiverIdentity,
    public_key: &str,
    host: Option<&str>,
    lan_port: u16,
    open_auth: bool,
) -> Option<AdvertiseInfo> {
    let host = host.filter(|host| !host.is_empty())?;
    Some(AdvertiseInfo {
        name: mdns::instance_name(&identity.name, &identity.device_id, &crate::lan::ad_nonce()),
        host: host.parse().ok()?,
        port: if lan_port > 0 {
            lan_port
        } else {
            RECEIVER_PORT
        },
        txt: mdns::advertisement_txt(
            &identity.device_id,
            &identity.name,
            Some(host),
            public_key,
            lan_port,
            open_auth,
        ),
    })
}

impl Inner {
    fn state(&self) -> CastState {
        lock(&self.state).clone()
    }

    fn host(&self) -> Option<String> {
        lock(&self.current_host).clone()
    }

    fn port(&self) -> u16 {
        self.port.load(Ordering::Acquire)
    }

    fn lan_endpoint(&self) -> Option<LanEndpoint> {
        self.peer.lan_endpoint()
    }

    /// `currentFreeBytes`: five-second cache, so status polls do not run `statfs` every tick.
    fn free_bytes(&self) -> u64 {
        let mut cache = lock(&self.free_cache);
        if cache.0.elapsed() < Duration::from_secs(5) {
            return cache.1;
        }
        let free = disk::free_disk_bytes_or_unbounded(&self.peer_root);
        *cache = (Instant::now(), free);
        free
    }

    fn pairing_uri(&self) -> String {
        format_receiver_uri(&ReceiverUriInput {
            public_key: self.public_key.clone(),
            device_id: self.identity.device_id.clone(),
            name: self.identity.name.clone(),
            token: self.identity.pair_token.clone(),
            p2p: true,
            ..ReceiverUriInput::default()
        })
    }

    /// What this device's own screen shows: who the receiver is, how to pair with it, and what
    /// it plays.
    fn info(&self) -> Value {
        let uri = self.pairing_uri();
        let svg = {
            let mut cache = lock(&self.qr_cache);
            if cache.0 != uri {
                *cache = (uri.clone(), super::qr::qr_svg(&uri));
            }
            cache.1.clone()
        };
        let external = self.peer_root.to_string_lossy().starts_with("/mnt/expand/")
            || (self.peer_root != self.storage);
        let mut out = Map::new();
        out.insert("protocol".into(), json!(super::control::PROTOCOL));
        out.insert("deviceId".into(), json!(self.identity.device_id));
        out.insert("name".into(), json!(self.identity.name));
        out.insert("publicKey".into(), json!(self.public_key));
        out.insert(
            "host".into(),
            match self.host() {
                Some(host) if !host.is_empty() => json!(host),
                _ => Value::Null,
            },
        );
        out.insert("port".into(), json!(self.port()));
        out.insert("authRequired".into(), json!(!self.open_auth));
        out.insert(
            "routes".into(),
            self.peer.status().routes.unwrap_or(Value::Null),
        );
        out.insert("storageFreeBytes".into(), json!(self.free_bytes()));
        out.insert("externalStorage".into(), json!(external));
        out.insert("state".into(), state::to_json(&self.state()));
        out.insert(
            "pairCode".into(),
            json!(if self.open_auth {
                "OPEN".to_owned()
            } else {
                self.identity.pair_code.clone()
            }),
        );
        out.insert("pairingUri".into(), json!(uri));
        out.insert("qrSvg".into(), json!(svg));
        Value::Object(out)
    }

    fn notify_local_host(&self, force: bool) {
        let Some(on_change) = self.on_change.as_ref() else {
            return;
        };
        let state = self.state();
        let key = state::notify_key(
            &state,
            self.host().as_deref().unwrap_or(""),
            self.port(),
            &self.identity.pair_code,
        );
        {
            let mut last = lock(&self.last_notify);
            if !force && *last == key {
                return;
            }
            *last = key;
        }
        on_change(&state);
    }

    /// `broadcastState`: every subscriber gets the state; one that wants the sender to wait for
    /// `drain` is pushed the latest state once the connection drains.
    fn broadcast_state(self: &Arc<Self>) {
        let state = self.state();
        let subscribers = lock(&self.subscribers).clone();
        for control in subscribers {
            let mut pending = lock(&self.backpressured);
            if let Some(dirty) = pending.get_mut(&control.id()) {
                *dirty = true;
                continue;
            }
            if control.push(3, state_to_wire(&state).into())
                != crate::control_channel::PushResult::WaitForDrain
            {
                continue;
            }
            pending.insert(control.id(), false);
            let inner = self.clone();
            let id = control.id();
            tokio::spawn(async move {
                loop {
                    control.drained().await;
                    let mut pending = lock(&inner.backpressured);
                    if pending.remove(&id) != Some(true)
                        || control.is_closed()
                        || inner.closing.load(Ordering::Acquire)
                    {
                        break;
                    }
                    if control.push(3, state_to_wire(&inner.state()).into())
                        != crate::control_channel::PushResult::WaitForDrain
                    {
                        break;
                    }
                    pending.insert(id, false);
                }
            });
        }
        self.notify_local_host(false);
    }

    fn state_json(&self, state: &CastState) -> Value {
        state::to_json(state)
    }

    fn update_state(self: &Arc<Self>, patch: Map<String, Value>) -> CastState {
        let mut state = lock(&self.state);
        state::update(&mut state, &patch, now_ms());
        drop(state);
        self.broadcast_state();
        self.state()
    }

    fn remember_stream(&self, media: Arc<Media>) {
        let mut streams = lock(&self.streams);
        streams.insert(0, media);
        while streams.len() > REMEMBERED_STREAMS {
            // `rememberStream` destroys the evicted stream's window; steering no longer visits it.
            if let Some(evicted) = streams.pop() {
                evicted.clear_window();
            }
        }
    }

    fn stream(&self, token: &str) -> Option<Arc<Media>> {
        lock(&self.streams)
            .iter()
            .find(|media| media.token == token)
            .cloned()
    }

    async fn prepare_media_load(self: &Arc<Self>, cmd: &Value) -> Result<Prepared, CastError> {
        *lock(&self.active) = None;
        let entry = load_entry(cmd);
        let magnet = cmd.get("magnetUrl").and_then(Value::as_str).unwrap_or("");
        let watch = if magnet.starts_with("peartube://watch/") {
            match parse_uri(magnet) {
                Ok(ParsedUri::Watch { entry, .. }) => Some(entry),
                _ => None,
            }
        } else {
            None
        };
        let Some(watch) = watch else {
            return Err(CastError::bad_request(
                "A cast load needs a peartube://watch link",
            ));
        };
        let blobs = watch.blobs.clone();
        let free_now = disk::free_disk_bytes_or_unbounded(&self.peer_root);
        let last_loaded = lock(&self.last_loaded_blobs).clone();
        let entry_size = entry.get("size").and_then(Value::as_u64).unwrap_or(0);
        if free_now < LOW_DISK_BOOT_BYTES
            || (!last_loaded.is_empty()
                && last_loaded != blobs
                && free_now < (3 * 1024 * 1024 * 1024).max(entry_size + LOW_DISK_STREAM_BYTES))
        {
            let keep = if free_now < LOW_DISK_STREAM_BYTES {
                None
            } else {
                Some(blobs.clone())
            };
            self.peer
                .clear_cache(ClearCacheOptions {
                    keep_blobs_hex: keep,
                })
                .await;
        }
        *lock(&self.last_loaded_blobs) = blobs.clone();
        self.peer.open_uri(magnet, true).await?;
        let core = self.peer.blob_core(&blobs).await?;
        let token = random_hex(16);
        let media = Arc::new(Media::new(
            token.clone(),
            entry.clone(),
            guess_media_mime(&entry),
            core,
            blobs,
            ptr_to_blob(watch.blob),
            Some(self.peer.clone()),
        ));
        self.remember_stream(media.clone());
        *lock(&self.active) = Some(media);
        Ok(Prepared {
            entry,
            mode: if self.peer.private_routing() {
                "private-p2p"
            } else {
                "p2p"
            },
            stream_url: format!("http://127.0.0.1:{}/cast/v1/p2p/{token}", self.port()),
        })
    }

    async fn apply_command(self: &Arc<Self>, cmd: &Value) -> Result<CastState, CastError> {
        let action = cmd.get("action").map(state::text_value).unwrap_or_default();
        if action.is_empty() {
            return Err(CastError::bad_request("action is required"));
        }
        if action == "status" {
            return Ok(self.state());
        }
        if action == "load" {
            let prepared = self.prepare_media_load(cmd).await?;
            let start_position_ms = cmd
                .get("startPositionMs")
                .map(|value| state::non_negative(Some(value)))
                .unwrap_or(0);
            let active = lock(&self.active).clone();
            let cached_bytes = match active {
                Some(media) => media.cached_bytes().await,
                None => 0,
            };
            let status = self.peer.status();
            let p2p_peers = status.peers + status.lan_peers;
            let mut patch = Map::new();
            patch.insert("status".into(), json!("loading"));
            patch.insert("mode".into(), json!(prepared.mode));
            patch.insert("entry".into(), prepared.entry.clone());
            patch.insert("positionMs".into(), json!(start_position_ms));
            patch.insert("durationMs".into(), Value::Null);
            patch.insert("playing".into(), json!(true));
            patch.insert("buffering".into(), json!(true));
            patch.insert("ended".into(), json!(false));
            patch.insert("p2pPeers".into(), json!(p2p_peers));
            patch.insert("cachedBytes".into(), json!(cached_bytes));
            patch.insert("seeding".into(), json!(cached_bytes > 0));
            patch.insert("tracks".into(), json!([]));
            patch.insert("audio".into(), Value::Null);
            patch.insert("subtitle".into(), Value::Null);
            patch.insert("error".into(), Value::Null);
            self.update_state(patch);
            let full = json!({
                "seq": self.seq.fetch_add(1, Ordering::AcqRel) + 1,
                "action": "load",
                "entry": prepared.entry,
                "mode": prepared.mode,
                "streamUrl": prepared.stream_url,
                "startPositionMs": start_position_ms
            });
            if let Some(on_command) = self.on_command.as_ref() {
                on_command(full);
            }
            return Ok(self.state());
        }
        if action == "stop" {
            // Keep the cached video in Corestore, so this TV remains a normal swarm peer.
            *lock(&self.active) = None;
            let full = json!({
                "seq": self.seq.fetch_add(1, Ordering::AcqRel) + 1,
                "action": "stop"
            });
            if let Some(on_command) = self.on_command.as_ref() {
                on_command(full);
            }
            // `updateState(idleCastState())`: a spread over the old state, so the keys only the
            // player reports (`videoDecoder`, `droppedFrames`) survive the reset.
            let idle = self.update_state(state::idle(now_ms()).into_iter().collect());
            return Ok(idle);
        }
        let state::Plan::Patch(patch) = state::plan_command(&self.state(), cmd)? else {
            return Err(CastError::bad_request(format!(
                "Unsupported cast action: {action}"
            )));
        };
        let mut full = cmd.as_object().cloned().unwrap_or_default();
        full.insert(
            "seq".into(),
            json!(self.seq.fetch_add(1, Ordering::AcqRel) + 1),
        );
        full.insert("action".into(), json!(action));
        if let Some(position_ms) = patch.get("positionMs") {
            full.insert("positionMs".into(), position_ms.clone());
        }
        if let Some(on_command) = self.on_command.as_ref() {
            on_command(Value::Object(full));
        }
        Ok(self.update_state(patch))
    }

    /// `onControl`: pairing and remote control. Any peer that connects to the node can send these,
    /// so each request is checked. An open receiver is open to the room it is in (LAN links), not
    /// to every peer on the internet.
    async fn on_control(
        self: &Arc<Self>,
        message: Payload,
        control: Control,
        on_lan: bool,
    ) -> Result<Payload, String> {
        let trusted = self.open_auth && on_lan;
        if let Payload::CastPair(pair) = &message {
            let peer_key = control
                .mux()
                .stream_info()
                .remote_public_key
                .map(|pk| crate::protocol::hex_encode(&pk))
                .unwrap_or_else(|| if on_lan { "lan".into() } else { "far".into() });
            self.check_pair_limit(on_lan, &peer_key)?;
            let code = Some(pair.code.as_str());
            if !trusted
                && !safe_equal(
                    Some(&super::pairing::normalize_pair_code(code)),
                    Some(&self.identity.pair_code),
                )
            {
                self.record_pair_failure(on_lan, &peer_key);
                return Err("Invalid TV pairing code".to_owned());
            }
            // An open receiver on this LAN needs no token: it answers with an empty one, never the
            // permanent pair token.
            return Ok(CastPaired {
                device_id: super::control::bounded(&self.identity.device_id, 128),
                name: super::control::bounded(&self.identity.name, 80),
                public_key: crate::node::sources::topic_key(&self.public_key)
                    .ok_or("Invalid receiver key")?,
                token: if trusted {
                    String::new()
                } else {
                    super::control::bounded(&self.identity.pair_token, 256)
                },
            }
            .into());
        }
        let token = match &message {
            Payload::CastSubscribe(v) => &v.token,
            Payload::CastCommand(v) => &v.token,
            _ => return Err("Unknown cast message".into()),
        };
        if !trusted && !safe_equal(Some(token), Some(&self.identity.pair_token)) {
            return Err("Unauthorized; pair with this TV first".to_owned());
        }
        if matches!(message, Payload::CastSubscribe(_)) {
            let id = control.id();
            let mut subscribers = lock(&self.subscribers);
            // A subscription belongs to its channel: one that already closed is not kept.
            if !control.is_closed() && !subscribers.iter().any(|kept| kept.id() == id) {
                subscribers.push(control);
            }
            return Ok(state_to_wire(&self.state()).into());
        }
        if let Payload::CastCommand(command) = message {
            let state = self
                .apply_command(&command_from_wire(command))
                .await
                .map_err(|error| error.message)?;
            return Ok(state_to_wire(&state).into());
        }
        unreachable!()
    }

    fn check_pair_limit(&self, on_lan: bool, peer_key: &str) -> Result<(), String> {
        let limiter = if on_lan {
            &self.limiter_lan
        } else {
            &self.limiter_far
        };
        limiter
            .lock()
            .map_err(|_| "Too many pairing attempts; wait 15 seconds".to_owned())?
            .check_peer(peer_key, now_ms())
            .map_err(|error| error.message)
    }

    fn record_pair_failure(&self, on_lan: bool, peer_key: &str) {
        let limiter = if on_lan {
            &self.limiter_lan
        } else {
            &self.limiter_far
        };
        if let Ok(mut limiter) = limiter.lock() {
            limiter.record_peer_failure(peer_key, now_ms());
        }
    }

    /// One sliding read-ahead window ahead of the player, sized in seconds of the stream and
    /// shrunk by the free disk; watched blocks are trimmed only when the disk needs the space.
    async fn steer_read_ahead(self: &Arc<Self>, media: Arc<Media>) {
        if media.core.is_none() || media.blob.is_none() {
            return;
        }
        if self.closing.load(Ordering::Acquire)
            || !lock(&self.streams)
                .iter()
                .any(|kept| Arc::ptr_eq(kept, &media))
        {
            return;
        }
        let free = self.free_bytes();
        let (state, duration) = {
            let state = self.state();
            let duration = state
                .get("durationMs")
                .map(num)
                .filter(|value| *value > 0.0);
            (state, duration)
        };
        let active = lock(&self.active)
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &media));
        // Only the playing load has a duration; another stream's is its own average bitrate.
        let duration_ms = duration.filter(|_| active).map(|value| value as u64);
        let rate = media.rate(duration_ms);
        let playhead = media.lead_byte(&state, duration_ms);
        media.steer_window(free, rate, playhead);
        let Some(plan) = super::media::evict_plan(free, rate, playhead) else {
            return;
        };
        if media.core_key_hex.is_empty() || media.blob.is_none() {
            return;
        }
        let settling = (active && state.get("buffering").is_some_and(truthy))
            || media.last_reader_at().elapsed() < Duration::from_millis(super::media::SETTLING_MS);
        if settling && free >= super::STREAM_FLOOR_BYTES {
            return;
        }
        if !super::media::trim_due(free, *lock(&self.last_evict_at)) {
            return;
        }
        if self.evicting.swap(true, Ordering::AcqRel) {
            return;
        }
        *lock(&self.last_evict_at) = Instant::now();
        let peer = self.peer.clone();
        let core_key = media.core_key_hex.clone();
        let blob = media.blob.expect("blob checked");
        let inner = self.clone();
        tokio::spawn(async move {
            peer.evict_behind(&core_key, blob, plan).await;
            inner.evicting.store(false, Ordering::Release);
        });
    }

    async fn steer_streams(self: &Arc<Self>) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        // `steerStreams`' one-at-a-time guard: a reader that just opened and the timer coincide.
        if self.steering.swap(true, Ordering::AcqRel) {
            return;
        }
        let streams = lock(&self.streams).clone();
        let active = lock(&self.active).clone();
        super::media::steer_newest_first(
            &streams,
            |media| {
                // `steerStreams`' read-rate estimate, taken for every remembered stream.
                media.refresh_rate();
                media.core.is_some()
                    && (lock_media_readers(media)
                        || active.as_ref().is_some_and(|a| Arc::ptr_eq(a, media)))
            },
            |media| self.steer_read_ahead(media),
        )
        .await;
        self.steering.store(false, Ordering::Release);
    }

    /// The status loop's peer/cache counters while something plays.
    async fn refresh_stats(self: &Arc<Self>) {
        if self.closing.load(Ordering::Acquire) {
            return;
        }
        let Some(media) = lock(&self.active).clone() else {
            return;
        };
        let state = self.state();
        if state.get("entry").is_none_or(Value::is_null) {
            return;
        }
        let status = self.peer.status();
        let p2p_peers = status.peers + status.lan_peers;
        let cached_bytes = media.cached_bytes().await;
        let seeding = cached_bytes > 0;
        let unchanged = state.get("p2pPeers").and_then(Value::as_u64) == Some(p2p_peers)
            && state.get("cachedBytes").and_then(Value::as_u64) == Some(cached_bytes)
            && state.get("seeding").is_some_and(truthy) == seeding;
        if unchanged
            || !lock(&self.active)
                .as_ref()
                .is_some_and(|a| Arc::ptr_eq(a, &media))
        {
            return;
        }
        let mut patch = Map::new();
        patch.insert("p2pPeers".into(), json!(p2p_peers));
        patch.insert("cachedBytes".into(), json!(cached_bytes));
        patch.insert("seeding".into(), json!(seeding));
        self.update_state(patch);
    }

    /// `refreshMdns`: publish the receiver under the node's LAN endpoint.
    async fn refresh_mdns(self: &Arc<Self>) {
        let lan = self.lan_endpoint();
        let host = self.host();
        let sig = format!(
            "{}|{}",
            host.clone().unwrap_or_default(),
            lan.as_ref().map_or(0, |lan| lan.port)
        );
        if lock(&self.ad).is_some() && *lock(&self.ad_sig) == sig {
            return;
        }
        let previous = lock(&self.ad).take();
        if let Some(previous) = previous {
            previous.stop();
        }
        if self.closing.load(Ordering::Acquire) || !self.mdns {
            return;
        }
        let lan_port = lan.as_ref().map_or(0, |lan| lan.port);
        let Some(info) = advertise_info(
            &self.identity,
            &self.public_key,
            host.as_deref(),
            lan_port,
            self.open_auth,
        ) else {
            return;
        };
        if let Ok(handle) = mdns::advertise(info) {
            *lock(&self.ad) = Some(handle);
            *lock(&self.ad_sig) = sig;
        }
    }

    fn serve(self: &Arc<Self>, conn: PeerConn) {
        let on_lan = conn.is_lan();
        let inner = self.clone();
        let controls_open = self.clone();
        let controls_close = self.clone();
        let subscribers = self.clone();
        let backpressured = self.clone();
        let request: RequestHandler = Arc::new(move |_, message, control| {
            let inner = inner.clone();
            Box::pin(async move { inner.on_control(message, control, on_lan).await })
        });
        let handlers = ControlHandlers {
            handled: 0b111,
            on_request: Some(request),
            on_event: None,
            on_open: Some(Arc::new(move |control| {
                let mut controls = lock(&controls_open.controls);
                if !controls.iter().any(|kept| kept.id() == control.id()) {
                    controls.push(control);
                }
            })),
            on_close: Some(Arc::new(move |control| {
                let id = control.id();
                lock(&controls_close.controls).retain(|kept| kept.id() != id);
                lock(&subscribers.subscribers).retain(|kept| kept.id() != id);
                lock(&backpressured.backpressured).remove(&id);
            })),
        };
        // One server per connection: a second `serve` (a listener call and the startup scan, or two
        // listener calls) must not replace the first handle, whose drop unpaired the protocol.
        let id = conn.id();
        {
            let mut unserve = lock(&self.unserve);
            if unserve.contains_key(&id) {
                return;
            }
            unserve.insert(id, serve_control(&conn.mux(), handlers));
        }
        let inner = self.clone();
        tokio::spawn(async move {
            conn.closed().await;
            lock(&inner.unserve).remove(&id);
        });
    }

    /// The receiver's own player reads the film here. The port answers this device only.
    fn handle_http(self: &Arc<Self>, req: Request<Incoming>) -> Response<Body> {
        let path = req.uri().path().to_owned();
        let method = req.method().clone();
        let range = req
            .headers()
            .get(hyper::header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let head = method == Method::HEAD;
        let token = path
            .strip_prefix("/cast/v1/p2p/")
            .filter(|token| token.len() == 32 && token.chars().all(|c| c.is_ascii_hexdigit()))
            .map(str::to_owned);
        if let Some(token) = token
            && (method == Method::GET || method == Method::HEAD)
            && let Some(media) = self.stream(&token)
        {
            // `onReader` → `steerStreams()`: a new reader starts the stream's window at once.
            let steering = self.clone();
            tokio::spawn(async move { steering.steer_streams().await });
            return super::media::windowed_response(media, range.as_deref(), head);
        }
        if method == Method::GET && path == "/cast/v1/info" {
            return http::json_response(
                200,
                &json!({
                    "deviceId": self.identity.device_id,
                    "name": self.identity.name,
                    "routes": self.peer.status().routes.unwrap_or(Value::Null),
                    "state": self.state_json(&self.state())
                }),
            );
        }
        http::empty(StatusCode::NOT_FOUND)
    }
}

/// `load_entry`: the entry a load command carries, with the app's defaults.
fn load_entry(cmd: &Value) -> Value {
    let entry = cmd.get("entry").filter(|entry| entry.is_object());
    let id = entry
        .and_then(|entry| entry.get("id"))
        .map(state::text_value)
        .filter(|id| !id.is_empty())
        .unwrap_or_else(|| "stream".to_owned());
    json!({
        "key": entry.and_then(|entry| entry.get("key")).map(state::text_value).unwrap_or_default(),
        "id": id,
        "title": crate::protocol::clean_name(
            Some(&entry.and_then(|entry| entry.get("title")).map(state::text_value).unwrap_or_default()),
            &entry.and_then(|entry| entry.get("id")).map(state::text_value).unwrap_or_else(|| "PearTube Stream".to_owned())
        ),
        "size": entry
            .and_then(|entry| entry.get("size"))
            .map(|size| num(size).max(0.0) as u64)
            .unwrap_or(0),
        "sha256": entry.and_then(|entry| entry.get("sha256")).map(state::text_value).unwrap_or_default()
    })
}

fn ptr_to_blob(pointer: BlobPointer) -> Option<BlobId> {
    (pointer.block_length > 0).then(|| super::media::blob_from_pointer(pointer))
}

fn lock_media_readers(media: &Arc<Media>) -> bool {
    media.reader_count() > 0
}

/// A receiver on `peer`: the node the host opened for it (with `receiverKeyPair`, so pairings last).
pub struct Receiver {
    inner: Arc<Inner>,
    server: tokio::sync::Mutex<Option<HttpServer>>,
}

impl Receiver {
    pub async fn start(options: ReceiverOptions) -> Result<Self, CastError> {
        let peer = options
            .peer
            .clone()
            .ok_or_else(|| CastError::bad_request("A receiver needs a node"))?;
        let identity = ReceiverIdentity::load_or_create(&options.storage, &options.name)
            .map_err(|error| CastError::bad_request(error.to_string()))?;
        let peer_root =
            disk::resolve_peer_storage(options.peer_storage.as_deref(), &options.storage).await;
        let public_key = peer.peer_key();
        peer.join_lan_topic("cast", cast_lan_topic());
        let inner = Arc::new(Inner {
            identity,
            storage: options.storage.clone(),
            peer_root,
            public_key,
            open_auth: options.open_auth,
            current_host: Mutex::new(options.lan_host.clone()),
            port: AtomicU16::new(0),
            state: Mutex::new(state::idle(now_ms())),
            active: Mutex::new(None),
            streams: Mutex::new(Vec::new()),
            seq: AtomicU64::new(0),
            controls: Mutex::new(Vec::new()),
            subscribers: Mutex::new(Vec::new()),
            backpressured: Mutex::new(HashMap::new()),
            limiter_lan: Mutex::new(PairLimiter::default()),
            limiter_far: Mutex::new(PairLimiter::default()),
            peer: peer.clone(),
            on_command: options.on_command.clone(),
            on_change: options.on_change.clone(),
            last_notify: Mutex::new(String::new()),
            qr_cache: Mutex::new((String::new(), String::new())),
            ad: Mutex::new(None),
            ad_sig: Mutex::new(String::new()),
            mdns: options.mdns,
            closing: AtomicBool::new(false),
            last_loaded_blobs: Mutex::new(String::new()),
            evicting: AtomicBool::new(false),
            steering: AtomicBool::new(false),
            last_evict_at: Mutex::new(Instant::now()),
            free_cache: Mutex::new((Instant::now() - Duration::from_secs(10), 0)),
            timers: Mutex::new(Vec::new()),
            unserve: Mutex::new(HashMap::new()),
            stop_serving: Mutex::new(None),
        });

        let handler_inner = inner.clone();
        let handler: http::Handler = Arc::new(move |req, _peer: SocketAddr| {
            let inner = handler_inner.clone();
            Box::pin(async move { inner.handle_http(req) })
        });
        let server = HttpServer::bind(options.port, "127.0.0.1", options.any_port, handler)
            .await
            .map_err(|error| CastError::new(error.to_string(), 500))?;
        inner.port.store(server.port(), Ordering::Release);

        let serve_inner = inner.clone();
        let listener: Arc<dyn Fn(PeerConn) + Send + Sync> = Arc::new(move |conn| {
            serve_inner.serve(conn);
        });
        let stop = peer.on_connection(listener.clone());
        *lock(&inner.stop_serving) = Some(stop);
        for conn in peer.peer_connections() {
            listener(conn);
        }

        inner.refresh_mdns().await;

        {
            let mut timers = lock(&inner.timers);
            let steering = inner.clone();
            timers.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if steering.closing.load(Ordering::Acquire) {
                        return;
                    }
                    steering.steer_streams().await;
                }
            }));
            let stats = inner.clone();
            timers.push(tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    if stats.closing.load(Ordering::Acquire) {
                        return;
                    }
                    stats.refresh_stats().await;
                }
            }));
        }

        Ok(Self {
            inner,
            server: tokio::sync::Mutex::new(Some(server)),
        })
    }

    pub fn port(&self) -> u16 {
        self.inner.port()
    }

    pub fn public_key(&self) -> String {
        self.inner.public_key.clone()
    }

    pub fn device_id(&self) -> String {
        self.inner.identity.device_id.clone()
    }

    pub fn peer_status(&self) -> PeerStatus {
        self.inner.peer.status()
    }

    pub fn info(&self) -> Value {
        self.inner.info()
    }

    pub fn state(&self) -> CastState {
        self.inner.state()
    }

    /// `command(cmd)`: the same path the control channel's `cmd` takes.
    pub async fn command(&self, cmd: &Value) -> Result<CastState, CastError> {
        self.inner.apply_command(cmd).await
    }

    pub fn report_state(&self, patch: &Value) -> CastState {
        if !patch.is_object() {
            return self.inner.state();
        }
        if patch.get("idle").is_some_and(truthy) {
            *lock(&self.inner.active) = None;
            // `updateState(idleCastState())` again: a spread, not a replacement.
            return self
                .inner
                .update_state(state::idle(now_ms()).into_iter().collect());
        }
        let Some(patch) = report_patch(&self.inner.state(), patch) else {
            return self.inner.state();
        };
        if patch.get("ended").is_some_and(truthy) {
            // Playback finished, yet the Corestore keeps seeding the cached video.
            *lock(&self.inner.active) = None;
        }
        self.inner.update_state(patch)
    }

    pub async fn set_host(&self, host: Option<String>) {
        let moved = host != self.inner.host();
        *lock(&self.inner.current_host) = host;
        self.inner.refresh_mdns().await;
        if moved {
            self.inner.notify_local_host(true);
        }
    }

    pub async fn close(&self) {
        if self.inner.closing.swap(true, Ordering::AcqRel) {
            return;
        }
        for timer in std::mem::take(&mut *lock(&self.inner.timers)) {
            timer.abort();
        }
        *lock(&self.inner.active) = None;
        for media in lock(&self.inner.streams).iter() {
            media.clear_window();
        }
        lock(&self.inner.streams).clear();
        if let Some(ad) = lock(&self.inner.ad).take() {
            ad.stop();
        }
        if let Some(stop) = lock(&self.inner.stop_serving).take() {
            stop();
        }
        let serves: Vec<_> = lock(&self.inner.unserve)
            .drain()
            .map(|(_, mut handle)| {
                handle.stop();
                handle
            })
            .collect();
        drop(serves);
        let controls: Vec<Control> = lock(&self.inner.controls).drain(..).collect();
        for control in controls {
            control.close();
        }
        lock(&self.inner.subscribers).clear();
        if let Some(server) = self.server.lock().await.take() {
            server.close().await;
        }
    }
}

/// The identity a host reads before opening a receiver's node: `receiverKeyPair(storage)`.
pub fn receiver_key_pair_json(storage: &Path) -> Result<Value, CastError> {
    let pair = super::pairing::receiver_key_pair(storage)
        .map_err(|error| CastError::bad_request(error.to_string()))?;
    Ok(json!({
        "publicKey": pair.public_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
        "secretKey": pair.secret_key.iter().map(|byte| format!("{byte:02x}")).collect::<String>()
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::records::{DiscoveredReceiver, SavedReceiver, SavedReceivers};

    /// `refreshMdns` returns without an advertisement when the receiver has no LAN host; the
    /// `h` fallback to `127.0.0.1` belongs to an injected adapter, which this port's host does
    /// not pass.
    #[test]
    fn a_receiver_without_a_lan_host_does_not_advertise() {
        let identity = ReceiverIdentity {
            device_id: "0123456789abcdef0123456789abcdef".into(),
            name: "Kitchen TV".into(),
            seed: "ab".repeat(32),
            pair_token: "cd".repeat(16),
            pair_code: "ABC123".into(),
        };
        let key = "ee".repeat(32);
        assert!(advertise_info(&identity, &key, None, 0, false).is_none());
        assert!(advertise_info(&identity, &key, Some(""), 0, false).is_none());

        let info = advertise_info(&identity, &key, Some("192.168.1.9"), 40_000, true)
            .expect("a LAN host is advertised");
        let txt: std::collections::HashMap<_, _> = info.txt.into_iter().collect();
        assert_eq!(info.host.to_string(), "192.168.1.9");
        assert_eq!(info.port, 40_000);
        assert_eq!(txt["h"], "192.168.1.9");
        assert_eq!(txt["lp"], "40000");
        assert_eq!(txt["auth"], "open");
        assert_eq!(txt["pk"], key.as_str());

        // A receiver whose LAN DHT did not start keeps `lp=0` and the receiver's own port.
        let info = advertise_info(&identity, &key, Some("192.168.1.9"), 0, false)
            .expect("still advertised");
        let txt: std::collections::HashMap<_, _> = info.txt.into_iter().collect();
        assert_eq!(info.port, RECEIVER_PORT);
        assert_eq!(txt["lp"], "0");
    }

    fn identity(name: &str) -> ReceiverIdentity {
        ReceiverIdentity {
            device_id: "0123456789abcdef0123456789abcdef".into(),
            name: name.into(),
            seed: "ab".repeat(32),
            pair_token: "cd".repeat(16),
            pair_code: "ABC123".into(),
        }
    }

    /// A receiver that restarts, or moves to another address, advertises again. The app's
    /// responder silently drops an advertisement whose name is taken, and a sender removes a
    /// receiver when the name that it heard last goes down, so no two advertisements share a name.
    #[test]
    fn every_advertisement_goes_out_under_a_new_name() {
        let identity = identity("Kitchen TV");
        let key = "ee".repeat(32);
        let names: std::collections::HashSet<String> = (0..4)
            .map(|_| {
                advertise_info(&identity, &key, Some("192.168.1.9"), 40_000, false)
                    .expect("a LAN host is advertised")
                    .name
            })
            .collect();
        // The nonce is random: four equal draws happen once in 46,656³ runs.
        assert!(
            names.len() > 1,
            "four advertisements shared a name: {names:?}"
        );
        assert!(
            names
                .iter()
                .all(|name| name.starts_with("Kitchen TV (012345-"))
        );
    }

    #[test]
    fn a_sixteen_emoji_name_is_advertised_under_a_label_that_fits() {
        let identity = identity(&"📺".repeat(16));
        let key = "ee".repeat(32);
        let info = advertise_info(&identity, &key, Some("192.168.1.9"), 40_000, false)
            .expect("a LAN host is advertised");
        assert!(
            info.name.len() <= 63,
            "the instance label takes {} bytes",
            info.name.len()
        );
        assert!(info.name.starts_with("📺"));
        assert!(info.name.ends_with(')'));
        let txt: std::collections::HashMap<_, _> = info.txt.into_iter().collect();
        assert_eq!(txt["name"], identity.name);
    }

    #[test]
    fn a_load_entry_defaults_like_the_app() {
        let entry = load_entry(&json!({ "entry": { "title": "A Film" } }));
        assert_eq!(entry["id"], "stream");
        assert_eq!(entry["title"], "A Film");
        assert_eq!(entry["size"], 0);
    }

    #[test]
    fn saved_receivers_survive_a_reload() {
        let dir = std::env::temp_dir().join(format!("cast-records-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut saved = SavedReceivers::load(&dir);
        saved.set(SavedReceiver {
            id: "peer-1".into(),
            name: "Kitchen TV".into(),
            public_key: "aa".repeat(32),
            token: "tok".into(),
        });
        saved.save(&dir).unwrap();
        let again = SavedReceivers::load(&dir);
        assert_eq!(again.get("peer-1").unwrap().token, "tok");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn receiver_lan_listings_carry_the_current_flag() {
        let rows = super::super::records::list_receivers(
            &[DiscoveredReceiver {
                id: "tv".into(),
                name: "TV".into(),
                host: "192.168.1.9".into(),
                public_key: "bb".repeat(32),
                lan: None,
                auth: "code".into(),
                current: false,
            }],
            &SavedReceivers::default(),
        );
        assert!(!rows[0].current);
    }
}
