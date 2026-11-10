//! The TV receiver, ported from `createReceiver` in `src/cast.js`: identity on disk, the LAN HTTP
//! server (`/tv`, the pair/command/info API, token-gated Range routes, SSE), the Noise RPC server,
//! and mDNS advertising. Media prep delegates to the P2P node through [`CastPeer`].

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use hyper::body::Incoming;
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyperblobs::BlobId;
use hypercore::Core;
use hypercore::crypto::KeyPair;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::protocol::{
    BlobPointer, ParsedUri, RECEIVER_PORT, ReceiverUriInput, WatchEntry, format_receiver_uri,
    format_watch_uri, parse_uri,
};

use super::http::{
    Body, Handler, HttpClient, HttpServer, bearer_token, channel_body, collect_body, html_response,
    json_response, preflight, query_param, read_json_body, sse_frame,
};
use super::mdns::{self, AdvertiseHandle, AdvertiseInfo};
use super::media::{Media, evict_plan, splice_response, windowed_response};
use super::pairing::{PairLimiter, ReceiverIdentity, capabilities, random_hex, safe_equal};
use super::records::pair_payload;
use super::rpc::{self, Dispatch, NoiseWriter};
use super::state::{self, CastState, Plan, load_entry, notify_key, num, report_patch, truthy};
use super::traits::{
    CastPeer, ChangeSink, ClearCacheOptions, CommandSink, DhtServer, NetworkConfig, PeerFactory,
    PeerStatus,
};
use super::{CastError, LOW_DISK_BOOT_BYTES, LOW_DISK_STREAM_BYTES, REMEMBERED_STREAMS, disk};

/// Options for [`Receiver::start`], the app's `createReceiver` argument object.
pub struct ReceiverOptions {
    pub storage: PathBuf,
    pub peer_storage: Option<PathBuf>,
    pub name: String,
    pub port: u16,
    pub bind_host: String,
    pub lan_host: Option<String>,
    /// An already-open node; `createReceiver`'s `node`.
    pub peer: Option<Arc<dyn CastPeer>>,
    /// `allowPeerNode`.
    pub allow_peer_node: bool,
    pub peer_factory: Option<Arc<dyn PeerFactory>>,
    /// `node.dht` plus the Noise key pair, when the receiver listens over P2P.
    pub dht_server: Option<Arc<dyn DhtServer>>,
    pub mdns: bool,
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
            bind_host: "0.0.0.0".to_owned(),
            lan_host: None,
            peer: None,
            allow_peer_node: false,
            peer_factory: None,
            dht_server: None,
            mdns: true,
            open_auth: false,
            any_port: true,
            on_command: None,
            on_change: None,
        }
    }
}

struct ManagedPeer {
    sig: String,
    peer: Arc<dyn CastPeer>,
}

/// The receiver's shared state; the HTTP and Noise handlers hold it.
struct Inner {
    identity: ReceiverIdentity,
    storage: PathBuf,
    peer_root: PathBuf,
    web_token: String,
    public_key: String,
    open_auth: bool,
    p2p_capable: bool,
    current_host: Mutex<Option<String>>,
    port: AtomicU16,
    state: Mutex<CastState>,
    active: Mutex<Option<Arc<Media>>>,
    streams: Mutex<VecDeque<Arc<Media>>>,
    seq: AtomicU64,
    sse: Mutex<Vec<mpsc::Sender<Bytes>>>,
    web_sse: Mutex<Vec<mpsc::Sender<Bytes>>>,
    noise_subscribers: Mutex<Vec<NoiseWriter>>,
    limiter: Mutex<PairLimiter>,
    peer: Option<Arc<dyn CastPeer>>,
    peer_factory: Option<Arc<dyn PeerFactory>>,
    managed: tokio::sync::Mutex<Option<ManagedPeer>>,
    on_command: Option<CommandSink>,
    on_change: Option<ChangeSink>,
    last_notify: Mutex<String>,
    qr_cache: Mutex<(String, String)>,
    client: Arc<HttpClient>,
    mdns: Mutex<Option<AdvertiseHandle>>,
    mdns_enabled: bool,
    closing: AtomicBool,
    last_loaded_blobs: Mutex<String>,
    last_evict_at: Mutex<Instant>,
    free_cache: Mutex<(Instant, u64)>,
    timers: Mutex<Vec<JoinHandle<()>>>,
}

impl Inner {
    fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn state(&self) -> CastState {
        self.state.lock().expect("cast state").clone()
    }

    fn host(&self) -> Option<String> {
        self.current_host.lock().expect("cast host").clone()
    }

    fn capabilities(&self) -> Map<String, Value> {
        capabilities(self.p2p_capable)
    }

    fn current_peer(&self) -> Option<Arc<dyn CastPeer>> {
        self.active
            .lock()
            .expect("cast media")
            .as_ref()
            .and_then(|m| m.peer.clone())
            .or_else(|| self.peer.clone())
    }

    /// `currentFreeBytes`: five-second cache, so status polls do not run `df` every tick.
    fn free_bytes(&self) -> u64 {
        let mut cache = self.free_cache.lock().expect("cast free");
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
            host: self.host().unwrap_or_default(),
            port: f64::from(self.port.load(Ordering::Relaxed)),
            p2p: self.p2p_capable,
        })
    }

    fn public_info(&self) -> Value {
        let peer = self.current_peer();
        let status = peer.as_ref().map(|p| p.status());
        let state = self.state();
        let external = self.peer_root.to_string_lossy().starts_with("/mnt/expand/")
            || (self.peer_root != self.storage);
        let mut out = Map::new();
        out.insert("protocol".into(), json!("peartube-cast/v1"));
        out.insert("deviceId".into(), json!(self.identity.device_id));
        out.insert("name".into(), json!(self.identity.name));
        out.insert("publicKey".into(), json!(self.public_key));
        out.insert(
            "swarmKey".into(),
            json!(peer.as_ref().map(|p| p.peer_key()).unwrap_or_default()),
        );
        out.insert(
            "host".into(),
            match self.host() {
                Some(host) if !host.is_empty() => json!(host),
                _ => Value::Null,
            },
        );
        out.insert("port".into(), json!(self.port.load(Ordering::Relaxed)));
        out.insert("capabilities".into(), Value::Object(self.capabilities()));
        out.insert("authRequired".into(), json!(!self.open_auth));
        out.insert(
            "connectedPeers".into(),
            status
                .as_ref()
                .map(|s| json!(s.connected_peers))
                .unwrap_or_else(|| json!([])),
        );
        out.insert(
            "routes".into(),
            status.and_then(|s| s.routes).unwrap_or(Value::Null),
        );
        out.insert("storageFreeBytes".into(), json!(self.free_bytes()));
        out.insert("externalStorage".into(), json!(external));
        out.insert("state".into(), state::to_json(&state));
        Value::Object(out)
    }

    fn detailed_info(&self) -> Value {
        let uri = self.pairing_uri();
        let svg = {
            let mut cache = self.qr_cache.lock().expect("cast qr");
            if cache.0 != uri {
                *cache = (uri.clone(), super::qr::qr_svg(&uri));
            }
            cache.1.clone()
        };
        let mut out = match self.public_info() {
            Value::Object(map) => map,
            _ => Map::new(),
        };
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
        out.insert(
            "listenUrl".into(),
            json!(format!(
                "http://{}:{}",
                self.host().unwrap_or_else(|| "127.0.0.1".to_owned()),
                self.port.load(Ordering::Relaxed)
            )),
        );
        Value::Object(out)
    }

    fn broadcast_state(&self) {
        let state = self.state();
        let frame = sse_frame(&json!({ "event": "state", "state": state::to_json(&state) }));
        {
            let mut clients = self.sse.lock().expect("cast sse");
            clients.retain(|tx| tx.try_send(frame.clone()).is_ok());
        }
        let line = json!({ "event": "state", "state": state::to_json(&state) });
        {
            let subscribers = self.noise_subscribers.lock().expect("cast noise");
            let senders: Vec<NoiseWriter> = subscribers.clone();
            drop(subscribers);
            let live: Vec<NoiseWriter> = senders
                .into_iter()
                .filter(|writer| writer.try_send(line.clone()))
                .collect();
            *self.noise_subscribers.lock().expect("cast noise") = live;
        }
        self.notify_local(false);
    }

    fn notify_local(&self, force: bool) {
        let Some(sink) = self.on_change.as_ref() else {
            return;
        };
        let state = self.state();
        let key = notify_key(
            &state,
            &self.host().unwrap_or_default(),
            self.port.load(Ordering::Relaxed),
            &self.identity.pair_code,
        );
        let mut last = self.last_notify.lock().expect("cast notify");
        if !force && *last == key {
            return;
        }
        *last = key;
        sink(&state);
    }

    fn update_state(&self, patch: &Map<String, Value>) -> CastState {
        let now = self.now();
        let mut state = self.state.lock().expect("cast state");
        state::update(&mut state, patch, now);
        let snapshot = state.clone();
        drop(state);
        self.broadcast_state();
        snapshot
    }

    fn send_web_command(&self, command: &Value) {
        let frame = sse_frame(&json!({ "event": "command", "command": command }));
        let mut clients = self.web_sse.lock().expect("cast web sse");
        clients.retain(|tx| tx.try_send(frame.clone()).is_ok());
    }

    fn authorized(&self, headers: &hyper::HeaderMap, query: Option<&str>) -> bool {
        if self.open_auth {
            return true;
        }
        let supplied = bearer_token(headers, query_param(query, "token").as_deref());
        safe_equal(Some(&supplied), Some(&self.identity.pair_token))
    }

    fn remember_stream(&self, media: Arc<Media>) {
        let mut streams = self.streams.lock().expect("cast streams");
        streams.push_back(media);
        while streams.len() > REMEMBERED_STREAMS {
            streams.pop_front();
        }
    }

    fn stream_by_token(&self, token: &str) -> Option<Arc<Media>> {
        self.streams
            .lock()
            .expect("cast streams")
            .iter()
            .find(|media| media.token == token)
            .cloned()
    }

    async fn ensure_peer_node(
        self: &Arc<Self>,
        network: &NetworkConfig,
    ) -> Option<Arc<dyn CastPeer>> {
        let want_private = network.private_routing;
        let want_bootstrap = network.bootstrap.clone();
        let want_relay = network.relay_through.clone();
        let want_lan = if want_private { network.lan } else { true };
        let source_key = network.source_key.to_lowercase();
        let source_key = if crate::protocol::is_hex64(&source_key) {
            source_key
        } else {
            String::new()
        };
        let source_kind = if network.source_kind == "channel" {
            "channel"
        } else {
            "tracker"
        };

        if let Some(peer) = self.peer.as_ref()
            && peer.private_routing() == want_private
            && same_bootstrap(peer.bootstrap().as_ref(), want_bootstrap.as_ref())
            && same_bootstrap(peer.relay_through().as_ref(), want_relay.as_ref())
            && (!want_private || peer.has_lan() == want_lan)
        {
            return Some(Arc::clone(peer));
        }
        let factory = self.peer_factory.as_ref()?;
        if !self.p2p_capable {
            return None;
        }
        let tracker_for_private =
            if want_private && source_kind == "tracker" && !source_key.is_empty() {
                source_key.clone()
            } else {
                String::new()
            };
        let sig = json!({
            "wantPrivate": want_private,
            "wantBootstrap": want_bootstrap,
            "wantRelayThrough": want_relay,
            "wantLan": want_lan,
            "trackerForPrivate": tracker_for_private,
        })
        .to_string();
        let mut managed = self.managed.lock().await;
        if managed.as_ref().is_some_and(|m| m.sig == sig) {
            return managed.as_ref().map(|m| Arc::clone(&m.peer));
        }
        if let Some(previous) = managed.take() {
            previous.peer.close().await;
        }
        let host = self.host();
        match factory.ensure(network.clone(), host.as_deref()).await {
            Ok(Some(peer)) => {
                *managed = Some(ManagedPeer {
                    sig,
                    peer: Arc::clone(&peer),
                });
                Some(peer)
            }
            _ => None,
        }
    }

    async fn prepare_media_load(self: &Arc<Self>, cmd: &Value) -> Result<PreparedMedia, CastError> {
        *self.active.lock().expect("cast media") = None;
        let entry = load_entry(cmd);
        let prefer_mode = match cmd.get("preferMode").and_then(Value::as_str) {
            Some("http") => "http",
            Some("p2p") => "p2p",
            _ => "auto",
        };
        let http_stream_url = cmd
            .get("httpStreamUrl")
            .and_then(Value::as_str)
            .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
            .map(str::to_owned);
        let magnet_url = cmd
            .get("magnetUrl")
            .and_then(Value::as_str)
            .filter(|url| url.starts_with("peartube://watch/"))
            .map(str::to_owned);
        let parsed = magnet_url.as_deref().and_then(|url| parse_uri(url).ok());
        let parsed_watch = match &parsed {
            Some(ParsedUri::Watch {
                source_key,
                source_kind,
                entry,
            }) => Some((source_key.clone(), *source_kind, entry)),
            _ => None,
        };
        let network = cmd.get("network");
        let private_routing = network
            .and_then(|n| n.get("privateRouting"))
            .or_else(|| cmd.get("senderPrivateRouting"))
            .is_some_and(truthy);
        let net = NetworkConfig {
            private_routing,
            bootstrap: network.and_then(|n| n.get("bootstrap")).cloned(),
            relay_through: network.and_then(|n| n.get("relayThrough")).cloned(),
            lan: match network.and_then(|n| n.get("lan")) {
                Some(value) => truthy(value),
                None => !private_routing,
            },
            source_key: network
                .and_then(|n| n.get("sourceKey"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| parsed_watch.as_ref().and_then(|w| w.0.clone()))
                .unwrap_or_default(),
            source_kind: network
                .and_then(|n| n.get("sourceKind"))
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| parsed_watch.as_ref().map(|w| w.1.to_owned()))
                .unwrap_or_else(|| "tracker".to_owned()),
        };
        let want_p2p = self.p2p_capable && magnet_url.is_some() && prefer_mode != "http";
        let p2p_mode = if net.private_routing {
            "private-p2p"
        } else {
            "p2p"
        };
        let allow_cold_http = http_stream_url.is_some()
            && (prefer_mode == "http"
                || (prefer_mode != "p2p" && (!net.private_routing || net.lan)));

        let mut peer: Option<Arc<dyn CastPeer>> = None;
        let mut local_p2p_url: Option<String> = None;
        let mut core: Option<Core> = None;
        let mut blob: Option<BlobId> = None;
        if want_p2p {
            let attempt: Result<(), CastError> = async {
                peer = self.ensure_peer_node(&net).await;
                let Some(active_peer) = peer.clone() else {
                    return Ok(());
                };
                let target_blobs = parsed_watch
                    .as_ref()
                    .map(|(_, _, entry)| entry.blobs.clone())
                    .unwrap_or_default();
                let free_now = disk::free_disk_bytes_or_unbounded(&self.peer_root);
                let last_blobs = self.last_loaded_blobs.lock().expect("cast blobs").clone();
                let different = !last_blobs.is_empty()
                    && !target_blobs.is_empty()
                    && last_blobs != target_blobs;
                let entry_size = entry.get("size").and_then(Value::as_u64).unwrap_or(0);
                if free_now < LOW_DISK_BOOT_BYTES
                    || (different && active_peer.memory_cache_bytes().is_some())
                    || (different
                        && free_now
                            < (3 * 1024 * 1024 * 1024).max(entry_size + LOW_DISK_STREAM_BYTES))
                {
                    let keep = (free_now >= LOW_DISK_STREAM_BYTES).then(|| target_blobs.clone());
                    active_peer
                        .clear_cache(ClearCacheOptions {
                            keep_blobs_hex: keep,
                        })
                        .await;
                }
                if !target_blobs.is_empty() {
                    *self.last_loaded_blobs.lock().expect("cast blobs") = target_blobs;
                }
                if !net.private_routing
                    && let Some(endpoint) = cmd.get("senderLan").filter(|v| truthy(v))
                {
                    active_peer.dial_lan_peer(endpoint);
                }
                active_peer.join_peer(
                    &cmd.get("senderPeerKey")
                        .map(state::text_value)
                        .unwrap_or_default()
                        .to_lowercase(),
                    true,
                );
                let link = match (&parsed_watch, magnet_url.as_deref()) {
                    (Some((source_key, _, entry)), Some(original)) => {
                        if source_key.is_none() && crate::protocol::is_hex64(&net.source_key) {
                            Some(format_watch_uri(
                                &watch_entry(entry),
                                &net.source_key,
                                "channel",
                            ))
                        } else {
                            Some(original.to_owned())
                        }
                    }
                    (_, Some(original)) => Some(original.to_owned()),
                    _ => None,
                };
                if let Some(link) = link {
                    let opened = active_peer.open_uri(&link, net.private_routing).await?;
                    local_p2p_url = opened.as_ref().and_then(|o| o.stream_url.clone());
                    blob = opened.and_then(|o| o.blob);
                    if let Some((_, _, entry)) = parsed_watch.as_ref() {
                        blob = blob.or(Some(blob_id(entry.blob)));
                        core = active_peer.blob_core(&entry.blobs).await?;
                    }
                }
                Ok(())
            }
            .await;
            if let Err(err) = attempt
                && !allow_cold_http
            {
                return Err(err);
            }
        }

        let effective_http = allow_cold_http.then(|| http_stream_url.clone()).flatten();
        if local_p2p_url.is_none() && effective_http.is_none() {
            return Err(CastError::bad_request(
                "Cast load requires an allowed HTTP Range URL or a reachable P2P peartube://watch URI",
            ));
        }
        let load_token = random_hex(16);
        let port = self.port.load(Ordering::Relaxed);
        if core.is_some() && blob.is_some() && local_p2p_url.is_some() {
            local_p2p_url = Some(format!("http://127.0.0.1:{port}/cast/v1/p2p/{load_token}"));
        }
        let direct_local_p2p = self.on_command.is_some() && local_p2p_url.is_some();
        let start_on_p2p = local_p2p_url.is_some()
            && (direct_local_p2p
                || !allow_cold_http
                || core
                    .as_ref()
                    .zip(blob)
                    .is_some_and(|(core, blob)| super::media::blob_has_block(core, blob, 0))
                || core
                    .as_ref()
                    .zip(blob)
                    .is_some_and(|(core, blob)| super::media::blob_has_peer(core, blob)));
        let initial_mode = if start_on_p2p { p2p_mode } else { "http" };
        let mime = super::http::guess_media_mime(&entry);
        let media = Arc::new(Media::new(
            load_token.clone(),
            entry.clone(),
            mime,
            prefer_mode.to_owned(),
            p2p_mode.to_owned(),
            net.private_routing,
            peer.clone(),
            effective_http.clone(),
            local_p2p_url.clone(),
            core.clone(),
            parsed_watch
                .as_ref()
                .map(|(_, _, entry)| entry.blobs.clone())
                .unwrap_or_default(),
            blob,
            allow_cold_http,
        ));
        if core.is_some() && blob.is_some() {
            self.remember_stream(Arc::clone(&media));
        }
        let host_for_player = if self.on_command.is_some() {
            "127.0.0.1".to_owned()
        } else {
            self.host().unwrap_or_else(|| "127.0.0.1".to_owned())
        };
        let stream_url = if direct_local_p2p {
            local_p2p_url.clone().unwrap_or_default()
        } else if local_p2p_url.is_some() && allow_cold_http {
            format!("http://{host_for_player}:{port}/cast/v1/hybrid/{load_token}")
        } else {
            local_p2p_url
                .clone()
                .or(effective_http.clone())
                .unwrap_or_default()
        };
        let peer_lan = if net.private_routing {
            None
        } else {
            peer.as_ref().and_then(|p| p.lan_endpoint())
        };
        Ok(PreparedMedia {
            entry,
            mode: initial_mode.to_owned(),
            stream_url,
            http_fallback_url: effective_http,
            peer_swarm_key: peer.as_ref().map(|p| p.peer_key()).unwrap_or_default(),
            peer_lan,
            media,
        })
    }

    async fn apply_command(self: &Arc<Self>, cmd: &Value) -> Result<Value, CastError> {
        let plan = state::plan_command(&self.state(), cmd)?;
        match plan {
            Plan::Status => Ok(state::to_json(&self.state())),
            Plan::Load => {
                let prepared = self.prepare_media_load(cmd).await?;
                let start_position = cmd
                    .get("startPositionMs")
                    .map(|v| num(v).max(0.0) as u64)
                    .unwrap_or(0);
                let cached = prepared.media.cached_bytes().await;
                let status = prepared
                    .media
                    .peer
                    .as_ref()
                    .map(|peer| peer.status())
                    .unwrap_or_default();
                let mut patch = Map::new();
                patch.insert("status".into(), json!("loading"));
                patch.insert("mode".into(), json!(prepared.mode));
                patch.insert("entry".into(), prepared.entry.clone());
                patch.insert("positionMs".into(), json!(start_position));
                patch.insert("durationMs".into(), Value::Null);
                patch.insert("playing".into(), json!(true));
                patch.insert("buffering".into(), json!(true));
                patch.insert("ended".into(), json!(false));
                patch.insert("p2pPeers".into(), json!(status.peers + status.lan_peers));
                patch.insert("cachedBytes".into(), json!(cached));
                patch.insert("seeding".into(), json!(cached > 0));
                patch.insert("tracks".into(), json!([]));
                patch.insert("audio".into(), Value::Null);
                patch.insert("subtitle".into(), Value::Null);
                patch.insert("error".into(), Value::Null);
                let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
                let mut full = Map::new();
                full.insert("seq".into(), json!(seq));
                full.insert("action".into(), json!("load"));
                full.insert("entry".into(), prepared.entry.clone());
                full.insert("mode".into(), json!(prepared.mode));
                full.insert("streamUrl".into(), json!(prepared.stream_url));
                full.insert(
                    "httpStreamUrl".into(),
                    match &prepared.http_fallback_url {
                        Some(url) => json!(url),
                        None => Value::Null,
                    },
                );
                full.insert("startPositionMs".into(), json!(start_position));
                let full = Value::Object(full);
                *self.active.lock().expect("cast media") = Some(Arc::clone(&prepared.media));
                self.send_web_command(&full);
                if let Some(sink) = self.on_command.as_ref() {
                    sink(full);
                }
                let mut next = self.update_state(&patch);
                next.insert("peerSwarmKey".into(), json!(prepared.peer_swarm_key));
                next.insert(
                    "peerLan".into(),
                    prepared.peer_lan.clone().unwrap_or(Value::Null),
                );
                Ok(Value::Object(next))
            }
            Plan::Stop => {
                *self.active.lock().expect("cast media") = None;
                let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
                let full = json!({ "seq": seq, "action": "stop" });
                self.send_web_command(&full);
                if let Some(sink) = self.on_command.as_ref() {
                    sink(full);
                }
                let now = self.now();
                Ok(state::to_json(&self.update_state(&state::idle(now))))
            }
            Plan::Patch(patch) => {
                let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
                let position = patch.get("positionMs").and_then(Value::as_u64).filter(|_| {
                    matches!(
                        cmd.get("action").and_then(Value::as_str),
                        Some("seek") | Some("skip")
                    )
                });
                let full = state::relayed_command(seq, cmd, position);
                self.send_web_command(&full);
                if let Some(sink) = self.on_command.as_ref() {
                    sink(full);
                }
                Ok(state::to_json(&self.update_state(&patch)))
            }
        }
    }

    fn report_state(&self, patch: &Value) -> CastState {
        let state = self.state();
        match report_patch(&state, patch) {
            None => {
                *self.active.lock().expect("cast media") = None;
                let now = self.now();
                self.update_state(&state::idle(now))
            }
            Some(patch) => {
                let ended = patch.get("ended").is_some_and(truthy);
                let next = self.update_state(&patch);
                if ended {
                    *self.active.lock().expect("cast media") = None;
                }
                next
            }
        }
    }

    async fn handle_http(
        self: Arc<Self>,
        req: Request<Incoming>,
        peer: SocketAddr,
    ) -> Response<Body> {
        let method = req.method().clone();
        let path = req.uri().path().to_owned();
        let query = req.uri().query().map(str::to_owned);
        if method == Method::OPTIONS {
            return preflight();
        }
        let range = req
            .headers()
            .get(header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if method == Method::GET && (path == "/" || path == "/tv" || path == "/cast/receiver") {
            return html_response(super::ui::receiver_page("standalone", &self.identity.name));
        }
        if (method == Method::GET || method == Method::HEAD)
            && let Some(token) = path.strip_prefix("/cast/v1/p2p/")
            && token.len() == 32
            && token.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return match self.stream_by_token(token) {
                Some(media) => windowed_response(
                    media,
                    range.as_deref(),
                    method == Method::HEAD,
                    self.free_bytes(),
                ),
                None => super::http::empty(StatusCode::NOT_FOUND),
            };
        }
        if (method == Method::GET || method == Method::HEAD)
            && let Some(token) = path.strip_prefix("/cast/v1/hybrid/")
            && token.len() == 32
        {
            let active = self.active.lock().expect("cast media").clone();
            let Some(media) = active else {
                return super::http::empty(StatusCode::NOT_FOUND);
            };
            if !safe_equal(Some(token), Some(&media.token)) {
                return super::http::empty(StatusCode::NOT_FOUND);
            }
            let inner = Arc::clone(&self);
            let upgrade: Arc<dyn Fn(&str) + Send + Sync> = Arc::new(move |mode: &str| {
                let current = inner.state();
                if current.get("mode").and_then(Value::as_str) != Some(mode) {
                    let mut patch = Map::new();
                    patch.insert("mode".into(), json!(mode));
                    inner.update_state(&patch);
                }
            });
            let head = method == Method::HEAD;
            return splice_response(
                media,
                Arc::clone(&self.client),
                method,
                range.as_deref(),
                head,
                upgrade,
            )
            .await;
        }
        if method == Method::GET && path == "/cast/v1/events" {
            if !self.authorized(req.headers(), query.as_deref()) {
                return json_response(401, &json!({ "error": "Unauthorized" }));
            }
            let (tx, rx) = mpsc::channel(16);
            let _ = tx.try_send(sse_frame(
                &json!({ "event": "state", "state": state::to_json(&self.state()) }),
            ));
            self.sse.lock().expect("cast sse").push(tx);
            return sse_response(rx);
        }
        if method == Method::GET && path == "/cast/v1/web-events" {
            let token = query_param(query.as_deref(), "token").unwrap_or_default();
            if !safe_equal(Some(&token), Some(&self.web_token)) {
                return json_response(401, &json!({ "error": "Unauthorized" }));
            }
            let (tx, rx) = mpsc::channel(16);
            let _ = tx.try_send(Bytes::from(": connected\n\n"));
            self.web_sse.lock().expect("cast web sse").push(tx);
            return sse_response(rx);
        }

        let outcome: Result<Value, CastError> = async {
            match (&method, path.as_str()) {
                (&Method::GET, "/cast/v1/info") => Ok(self.public_info()),
                (&Method::GET, "/cast/v1/web-info") => {
                    let remote = peer.ip().to_string();
                    let host = self.host().unwrap_or_default();
                    let is_local = remote == "127.0.0.1"
                        || remote == "::1"
                        || remote == "::ffff:127.0.0.1"
                        || (!host.is_empty() && remote.ends_with(&host));
                    let info = if is_local {
                        self.detailed_info()
                    } else {
                        let mut map = match self.public_info() {
                            Value::Object(map) => map,
                            _ => Map::new(),
                        };
                        map.insert(
                            "pairCode".into(),
                            json!(if self.open_auth {
                                "OPEN".to_owned()
                            } else {
                                self.identity.pair_code.clone()
                            }),
                        );
                        map.insert("pairingUri".into(), json!(self.pairing_uri()));
                        map.insert(
                            "qrSvg".into(),
                            json!(super::qr::qr_svg(&self.pairing_uri())),
                        );
                        Value::Object(map)
                    };
                    let mut map = match info {
                        Value::Object(map) => map,
                        _ => Map::new(),
                    };
                    map.insert("webToken".into(), json!(self.web_token));
                    Ok(Value::Object(map))
                }
                (&Method::POST, "/cast/v1/web-state") => {
                    let body = read_json_body(&collect_body(req).await?)?;
                    let token = body.get("token").map(state::text_value).unwrap_or_default();
                    if !safe_equal(Some(&token), Some(&self.web_token)) {
                        return Err(CastError::unauthorized("Unauthorized"));
                    }
                    if let Some(Value::Object(next)) = body.get("state")
                        && self.on_command.is_none()
                    {
                        let mut patch = Map::new();
                        for (key, value) in next {
                            if key != "mode" {
                                patch.insert(key.clone(), value.clone());
                            }
                        }
                        let fallback = self
                            .state()
                            .get("mode")
                            .cloned()
                            .filter(state::truthy)
                            .or_else(|| next.get("mode").cloned())
                            .unwrap_or_else(|| json!("http"));
                        patch.insert("mode".into(), fallback);
                        self.update_state(&patch);
                    }
                    Ok(json!({ "ok": true }))
                }
                (&Method::POST, "/cast/v1/pair") => {
                    let body = read_json_body(&collect_body(req).await?)?;
                    let code = body.get("code").map(state::text_value);
                    self.limiter.lock().expect("cast limiter").verify_code(
                        code.as_deref(),
                        &self.identity.pair_code,
                        self.open_auth,
                        self.now(),
                    )?;
                    Ok(pair_payload(
                        &self.identity.device_id,
                        &self.identity.name,
                        &self.public_key,
                        &self.peer.as_ref().map(|p| p.peer_key()).unwrap_or_default(),
                        self.port.load(Ordering::Relaxed),
                        &self.identity.pair_token,
                        &self.capabilities(),
                    ))
                }
                (&Method::POST, "/cast/v1/command") => {
                    if !self.authorized(req.headers(), query.as_deref()) {
                        return Err(CastError::unauthorized(
                            "Unauthorized; pair with this TV first",
                        ));
                    }
                    let body = read_json_body(&collect_body(req).await?)?;
                    let next = self.apply_command(&body).await?;
                    Ok(json!({ "ok": true, "state": next }))
                }
                _ => Err(CastError::not_found("Not found")),
            }
        }
        .await;
        match outcome {
            Ok(value) => json_response(200, &value),
            Err(err) => json_response(err.status, &json!({ "error": err.message })),
        }
    }

    fn noise_dispatch(self: &Arc<Self>) -> Dispatch {
        let inner = Arc::clone(self);
        Arc::new(move |message, writer| {
            let inner = Arc::clone(&inner);
            Box::pin(async move { Some(inner.handle_noise(message, writer).await) })
        })
    }

    async fn handle_noise(
        self: Arc<Self>,
        message: Map<String, Value>,
        writer: NoiseWriter,
    ) -> Value {
        let kind = message
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let result = self.noise_result(&kind, message, writer).await;
        match result {
            Ok(result) => rpc::ok_reply(result),
            Err(err) => rpc::error_reply(&err.message),
        }
    }

    async fn noise_result(
        self: &Arc<Self>,
        kind: &str,
        message: Map<String, Value>,
        writer: NoiseWriter,
    ) -> Result<Value, CastError> {
        match kind {
            "info" => Ok(self.public_info()),
            "pair" => {
                let code = message.get("code").map(state::text_value);
                self.limiter.lock().expect("cast limiter").verify_code(
                    code.as_deref(),
                    &self.identity.pair_code,
                    self.open_auth,
                    self.now(),
                )?;
                Ok(json!({
                    "deviceId": self.identity.device_id,
                    "name": self.identity.name,
                    "publicKey": self.public_key,
                    "port": self.port.load(Ordering::Relaxed),
                    "token": self.identity.pair_token,
                    "capabilities": Value::Object(self.capabilities()),
                }))
            }
            "subscribe" => {
                let token = message
                    .get("token")
                    .map(state::text_value)
                    .unwrap_or_default();
                if !self.open_auth && !safe_equal(Some(&token), Some(&self.identity.pair_token)) {
                    return Err(CastError::unauthorized("Unauthorized"));
                }
                self.noise_subscribers
                    .lock()
                    .expect("cast noise")
                    .push(writer);
                Ok(json!({ "state": state::to_json(&self.state()) }))
            }
            "cmd" => {
                let token = message
                    .get("token")
                    .map(state::text_value)
                    .unwrap_or_default();
                if !self.open_auth && !safe_equal(Some(&token), Some(&self.identity.pair_token)) {
                    return Err(CastError::unauthorized("Unauthorized"));
                }
                let payload = Value::Object(message);
                let next = self.apply_command(&payload).await?;
                Ok(json!({ "ok": true, "state": next }))
            }
            other => Err(CastError::bad_request(format!(
                "Unknown Noise cast message: {other}"
            ))),
        }
    }

    async fn refresh_mdns(self: &Arc<Self>) {
        let previous = self.mdns.lock().expect("cast mdns").take();
        if let Some(previous) = previous {
            previous.stop();
        }
        if self.closing.load(Ordering::SeqCst) || !self.mdns_enabled {
            return;
        }
        let Some(host) = self.host() else { return };
        let Ok(ip) = host.parse::<std::net::IpAddr>() else {
            return;
        };
        let info = AdvertiseInfo {
            name: mdns::instance_name(&self.identity.name, &self.identity.device_id),
            host: ip,
            port: self.port.load(Ordering::Relaxed),
            txt: mdns::advertisement_txt(
                &self.identity.device_id,
                &self.identity.name,
                Some(&host),
                &self.public_key,
                self.p2p_capable,
                self.open_auth,
            ),
        };
        if let Ok(handle) = mdns::advertise(info) {
            *self.mdns.lock().expect("cast mdns") = Some(handle);
        }
    }

    fn spawn_timers(self: &Arc<Self>) {
        let stats = {
            let inner = Arc::clone(self);
            tokio::spawn(async move {
                // The app keeps its one-second intervals; they also bound the status loop's cost.
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    if inner.closing.load(Ordering::SeqCst) {
                        break;
                    }
                    inner.stats_tick().await;
                    inner.steer_tick().await;
                }
            })
        };
        self.timers.lock().expect("cast timers").push(stats);
    }

    async fn stats_tick(&self) {
        let Some(media) = self.active.lock().expect("cast media").clone() else {
            return;
        };
        let Some(peer) = media.peer.clone() else {
            return;
        };
        let state = self.state();
        if state.get("entry").is_none_or(|v| v.is_null()) {
            return;
        }
        let status = peer.status();
        let peers = status.peers + status.lan_peers;
        let size = state
            .get("entry")
            .and_then(|e| e.get("size"))
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let cached = media.cached_bytes().await;
        let seeding = cached > 0;
        let mode = super::media::upgraded_mode(cached, media.has_active_peer(), &media.p2p_mode)
            .map(str::to_owned)
            .or_else(|| state.get("mode").and_then(Value::as_str).map(str::to_owned));
        let current_peers = state.get("p2pPeers").and_then(Value::as_u64).unwrap_or(0);
        let current_cached = state
            .get("cachedBytes")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        let current_seeding = state.get("seeding").is_some_and(truthy);
        let current_mode = state
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let _ = size;
        if peers != current_peers
            || cached != current_cached
            || seeding != current_seeding
            || mode.as_deref().unwrap_or_default() != current_mode
        {
            let mut patch = Map::new();
            patch.insert("p2pPeers".into(), json!(peers));
            patch.insert("cachedBytes".into(), json!(cached));
            patch.insert("seeding".into(), json!(seeding));
            if let Some(mode) = mode {
                patch.insert("mode".into(), json!(mode));
            }
            self.update_state(&patch);
        }
    }

    async fn steer_tick(&self) {
        let free = self.free_bytes();
        let streams: Vec<Arc<Media>> = self
            .streams
            .lock()
            .expect("cast streams")
            .iter()
            .cloned()
            .collect();
        let active = self.active.lock().expect("cast media").clone();
        for media in streams {
            media.refresh_rate();
            let Some(peer) = media.peer.clone() else {
                continue;
            };
            let Some(blob) = media.blob else { continue };
            if media.core_key_hex.is_empty() {
                continue;
            }
            let state = self.state();
            let duration = state
                .get("durationMs")
                .and_then(Value::as_u64)
                .filter(|ms| *ms > 0);
            let rate = media.rate(duration);
            let playhead = media.lead_byte(&state, duration);
            let memory_limit = peer.memory_cache_bytes();
            if memory_limit.is_some()
                && !active
                    .as_ref()
                    .is_some_and(|current| Arc::ptr_eq(current, &media))
            {
                continue;
            }
            let plan = memory_limit
                .map(|limit| super::traits::EvictOptions {
                    playhead_byte: playhead,
                    keep_behind_bytes: (limit / 8).min(64 * 1024 * 1024),
                    keep_ahead_bytes: Some(
                        media
                            .furthest_reader_byte()
                            .saturating_sub(playhead)
                            .saturating_add(limit * 7 / 8),
                    ),
                    max_bytes: limit,
                })
                .or_else(|| evict_plan(free, rate, playhead));
            if let Some(plan) = plan {
                let buffering = active.as_ref().is_some_and(|a| Arc::ptr_eq(a, &media))
                    && state.get("buffering").is_some_and(truthy);
                let settling = buffering
                    || media.last_reader_at().elapsed()
                        < Duration::from_millis(super::media::SETTLING_MS);
                let tight = memory_limit.is_some() || free < super::STREAM_RESERVE_BYTES;
                if (memory_limit.is_some() || !settling || free < super::STREAM_FLOOR_BYTES)
                    && self.last_evict_at.lock().expect("cast evict").elapsed()
                        >= Duration::from_millis(if tight { 3_000 } else { super::EVICT_EVERY_MS })
                {
                    *self.last_evict_at.lock().expect("cast evict") = Instant::now();
                    peer.evict_behind(&media.core_key_hex, blob, plan).await;
                }
            }
        }
    }
}

struct PreparedMedia {
    entry: Value,
    mode: String,
    stream_url: String,
    http_fallback_url: Option<String>,
    peer_swarm_key: String,
    peer_lan: Option<Value>,
    media: Arc<Media>,
}

fn sse_response(rx: mpsc::Receiver<Bytes>) -> Response<Body> {
    let mapped = mpsc::channel::<Result<hyper::body::Frame<Bytes>, std::io::Error>>(1);
    tokio::spawn(async move {
        let mut rx = rx;
        while let Some(chunk) = rx.recv().await {
            if mapped
                .0
                .send(Ok(hyper::body::Frame::data(chunk)))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let mut res = Response::new(channel_body(mapped.1));
    *res.status_mut() = StatusCode::OK;
    super::http::cors_headers(res.headers_mut());
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    res.headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
    res
}

fn same_bootstrap(a: Option<&Value>, b: Option<&Value>) -> bool {
    fn norm(value: Option<&Value>) -> String {
        let Some(Value::Array(items)) = value else {
            return String::new();
        };
        let mut parts: Vec<String> = items
            .iter()
            .map(|item| match item {
                Value::String(s) => s.clone(),
                other => other
                    .as_object()
                    .map(|o| {
                        format!(
                            "{}:{}",
                            o.get("host").map(state::text_value).unwrap_or_default(),
                            o.get("port").map(state::text_value).unwrap_or_default()
                        )
                    })
                    .unwrap_or_default(),
            })
            .collect();
        parts.sort();
        parts.join(",")
    }
    norm(a) == norm(b)
}

fn watch_entry(entry: &crate::protocol::ParsedEntry) -> WatchEntry {
    WatchEntry {
        id: entry.id.clone(),
        title: entry.title.clone(),
        sha256: Some(entry.sha256.clone()),
        blobs: entry.blobs.clone(),
        blob: Some(entry.blob),
    }
}

fn blob_id(pointer: BlobPointer) -> BlobId {
    BlobId {
        block_offset: pointer.block_offset,
        block_length: pointer.block_length,
        byte_offset: pointer.byte_offset,
        byte_length: pointer.byte_length,
    }
}

/// A running receiver; dropping it does not close it, [`Receiver::close`] does.
pub struct Receiver {
    inner: Arc<Inner>,
    server: HttpServer,
    dht: Option<Arc<dyn DhtServer>>,
    public_key_bytes: [u8; 32],
}

impl Receiver {
    /// `createReceiver(...)`.
    pub async fn start(options: ReceiverOptions) -> Result<Self, CastError> {
        let storage = options.storage.clone();
        std::fs::create_dir_all(&storage).map_err(|err| CastError::new(err.to_string(), 500))?;
        let peer_root = disk::resolve_peer_storage(options.peer_storage.as_deref(), &storage).await;
        if options.peer.is_none() && options.allow_peer_node {
            disk::reclaim_low_disk_receiver_storage(&peer_root);
        }
        let identity = ReceiverIdentity::load_or_create(&storage, &options.name)
            .map_err(|err| CastError::new(err.to_string(), 500))?;
        let p2p_capable = options.peer.is_some() || options.allow_peer_node;
        let key_pair = identity
            .seed_bytes()
            .filter(|_| p2p_capable)
            .map(|seed| KeyPair::from_seed(&seed));
        let public_key = key_pair
            .as_ref()
            .map(|k| hex(&k.public_key))
            .unwrap_or_default();
        let public_key_bytes = key_pair.as_ref().map(|k| k.public_key).unwrap_or([0u8; 32]);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let inner = Arc::new(Inner {
            identity,
            storage,
            peer_root,
            web_token: random_hex(16),
            public_key,
            open_auth: options.open_auth,
            p2p_capable,
            current_host: Mutex::new(options.lan_host.clone()),
            port: AtomicU16::new(0),
            state: Mutex::new(state::idle(now)),
            active: Mutex::new(None),
            streams: Mutex::new(VecDeque::new()),
            seq: AtomicU64::new(0),
            sse: Mutex::new(Vec::new()),
            web_sse: Mutex::new(Vec::new()),
            noise_subscribers: Mutex::new(Vec::new()),
            limiter: Mutex::new(PairLimiter::default()),
            peer: options.peer.clone(),
            peer_factory: options.peer_factory.clone(),
            managed: tokio::sync::Mutex::new(None),
            on_command: options.on_command.clone(),
            on_change: options.on_change.clone(),
            last_notify: Mutex::new(String::new()),
            qr_cache: Mutex::new((String::new(), String::new())),
            client: Arc::new(HttpClient::new()),
            mdns: Mutex::new(None),
            mdns_enabled: options.mdns,
            closing: AtomicBool::new(false),
            last_loaded_blobs: Mutex::new(String::new()),
            last_evict_at: Mutex::new(Instant::now()),
            free_cache: Mutex::new((Instant::now() - Duration::from_secs(5), 0)),
            timers: Mutex::new(Vec::new()),
        });
        let handler: Handler = {
            let inner = Arc::clone(&inner);
            Arc::new(move |req, peer| {
                let inner = Arc::clone(&inner);
                Box::pin(async move { inner.handle_http(req, peer).await })
                    as BoxFuture<'static, Response<Body>>
            })
        };
        let server = HttpServer::bind(options.port, &options.bind_host, options.any_port, handler)
            .await
            .map_err(|err| CastError::new(err.to_string(), 500))?;
        inner.port.store(server.port(), Ordering::Relaxed);
        if let Some(dht) = options.dht_server.clone()
            && p2p_capable
        {
            let dispatch = inner.noise_dispatch();
            let handler = Arc::new(move |conn: Box<dyn super::traits::Duplex>| {
                rpc::serve_connection(conn, Arc::clone(&dispatch));
            });
            if let Err(err) = dht.serve(public_key_bytes, handler).await {
                server.close().await;
                return Err(CastError::new(err.message, 502));
            }
        }
        inner.refresh_mdns().await;
        inner.spawn_timers();
        Ok(Self {
            inner,
            server,
            dht: options.dht_server,
            public_key_bytes,
        })
    }

    pub fn port(&self) -> u16 {
        self.server.port()
    }

    pub fn public_key(&self) -> &str {
        &self.inner.public_key
    }

    pub fn device_id(&self) -> &str {
        &self.inner.identity.device_id
    }

    pub fn peer_status(&self) -> Option<PeerStatus> {
        self.inner.current_peer().map(|peer| peer.status())
    }

    /// `info()`: the pairing details plus the state.
    pub fn info(&self) -> Value {
        self.inner.detailed_info()
    }

    pub fn state(&self) -> Value {
        state::to_json(&self.inner.state())
    }

    /// `reportState(patch)`: the TV player's own report.
    pub fn report_state(&self, patch: &Value) -> Value {
        state::to_json(&self.inner.report_state(patch))
    }

    /// `command(cmd)`.
    pub async fn command(&self, cmd: &Value) -> Result<Value, CastError> {
        self.inner.apply_command(cmd).await
    }

    /// `setHost(nextHost)`.
    pub async fn set_host(&self, host: Option<String>) {
        {
            let mut current = self.inner.current_host.lock().expect("cast host");
            if *current == host {
                return;
            }
            *current = host;
        }
        self.inner.refresh_mdns().await;
        self.inner.notify_local(true);
    }

    /// `close()`.
    pub async fn close(self) {
        if self.inner.closing.swap(true, Ordering::SeqCst) {
            return;
        }
        let timers: Vec<JoinHandle<()>> = {
            let mut timers = self.inner.timers.lock().expect("cast timers");
            std::mem::take(&mut *timers)
        };
        for timer in timers {
            timer.abort();
        }
        *self.inner.active.lock().expect("cast media") = None;
        self.inner.streams.lock().expect("cast streams").clear();
        {
            let mut mdns = self.inner.mdns.lock().expect("cast mdns");
            if let Some(handle) = mdns.take() {
                handle.stop();
            }
        }
        self.inner.sse.lock().expect("cast sse").clear();
        self.inner.web_sse.lock().expect("cast web sse").clear();
        self.inner
            .noise_subscribers
            .lock()
            .expect("cast noise")
            .clear();
        if let Some(dht) = self.dht.as_ref()
            && self.inner.p2p_capable
            && self.public_key_bytes != [0u8; 32]
        {
            dht.close().await;
        }
        if let Some(managed) = self.inner.managed.lock().await.take() {
            managed.peer.close().await;
        }
        self.server.close().await;
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The storage root a receiver keeps its identity in.
pub fn storage_dir(root: &Path) -> PathBuf {
    root.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Node;
    use corestore::{Corestore, GetOptions};

    #[tokio::test]
    async fn reloaded_memory_film_keeps_the_active_window() {
        let dir = tempfile::tempdir().unwrap();
        let node = crate::node::core::create_node(json!({
            "storage": dir.path().join("peer"), "bootstrap": [],
            "storageMemory": true, "memoryCacheBytes": 4 * 1024 * 1024,
        }))
        .await
        .unwrap();
        let writer = Corestore::open_with_options(
            &dir.path().join("writer"),
            hypercore::storage::StoreOptions {
                memory: true,
                ..Default::default()
            },
        )
        .unwrap();
        let source = writer.get(GetOptions::name("film")).await.unwrap();
        source.append(&vec![vec![7u8; 65536]; 256]).await.unwrap();
        let session = node
            .store()
            .get(GetOptions::key(*source.key()))
            .await
            .unwrap();
        let ((ar, aw), (br, bw)) = protomux::memory::duplex(256);
        let mux = |read, write, is_initiator| {
            protomux::Mux::new(
                read,
                write,
                protomux::StreamInfo {
                    is_initiator,
                    handshake_hash: Some([7; 64]),
                    remote_public_key: None,
                },
            )
        };
        let a = mux(ar, aw, true);
        let b = mux(br, bw, false);
        source.core().replicate(&a);
        session.core().replicate(&b);
        tokio::time::timeout(Duration::from_secs(10), async {
            session.update(None).await.unwrap();
            for index in 0..256 {
                session
                    .get(index, hypercore::GetOptions::default())
                    .await
                    .unwrap()
                    .unwrap();
            }
        })
        .await
        .unwrap();
        let key = hex(source.key());
        let session = node.blob_core(&key).await.unwrap();
        let peer = Arc::new(super::super::adapters::NodePeer::new(node.clone()));
        let blob = BlobId {
            block_offset: 0,
            block_length: 256,
            byte_offset: 0,
            byte_length: 16 * 1024 * 1024,
        };
        let media = |token: &str| {
            Arc::new(Media::new(
                token.into(),
                json!({"size": blob.byte_length}),
                "video/mp4".into(),
                "p2p".into(),
                "p2p".into(),
                false,
                Some(peer.clone()),
                None,
                None,
                Some(session.core().clone()),
                key.clone(),
                Some(blob),
                false,
            ))
        };
        let old = media("old");
        let active = media("active");
        let old_response = windowed_response(old.clone(), Some("bytes=0-"), false, u64::MAX);
        let active_response =
            windowed_response(active.clone(), Some("bytes=12582912-"), false, u64::MAX);
        let receiver = Receiver::start(ReceiverOptions {
            storage: dir.path().join("cast"),
            peer: Some(peer),
            port: 0,
            bind_host: "127.0.0.1".into(),
            mdns: false,
            ..Default::default()
        })
        .await
        .unwrap();
        *receiver.inner.active.lock().unwrap() = Some(active.clone());
        receiver
            .inner
            .streams
            .lock()
            .unwrap()
            .extend([old.clone(), active.clone()]);
        tokio::time::timeout(Duration::from_secs(5), async {
            while active.lead_byte(&Map::new(), None) < 12 * 1024 * 1024 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(old.lead_byte(&Map::new(), None) < 1024 * 1024);
        *receiver.inner.last_evict_at.lock().unwrap() = Instant::now() - Duration::from_secs(4);
        receiver.inner.steer_tick().await;
        assert!(
            session.has(192),
            "the old load must not evict the active playhead"
        );
        assert!(
            !session.has(128),
            "the active load still trims its spent prefix"
        );
        drop((old_response, active_response));
        receiver.close().await;
        source.close();
        writer.close().await;
        node.close().await.unwrap();
    }
}
