//! The seeder half of PearTube's `src/hiverelay.js`: the `hiverelay-seed` mirror plugin.
//!
//! It recruits existing HiveRelays as supplemental seeders for this relay's own cores, two ways:
//! `POST /seed-core` on operator-authed HTTP relays (`PEARTUBE_HIVERELAYS`, `=TOKEN` per relay),
//! and the zero-key `hiverelay-discovery-v1` DHT where a wire-exact 2-core Hyperdrive v11/v13
//! beacon and a signed `hiverelay.seed-request.v3` go out over the `hiverelay-seed` Protomux
//! channel. A HiveRelay connection is bulk: it goes through the relay's traffic gate, so it stays
//! off viewers' upload and connected relays hear that this one is busy.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use corestore::GetOptions;
use hyper::Request;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use protomux::{ChannelSender, Event, Mux};
use serde_json::{Map, Value, json};

use super::{
    BlobId, Core, HttpRequest, PAGE_INTERVAL_MS, PAGE_SIZE, PROTOCOL, SeedMessage, SeedRequest,
    bound_bytes, epoch_topics, hyperdrive_header, hyperdrive_root, publisher_key,
    seed_core_request, select_page,
};
use crate::app_settings::js_truthy;
use crate::mirror::{Ctx, MirrorError, MirrorPlugin};
use crate::node::Node;
use crate::node::api::SeedableCore;
use crate::node::core::CoreNode;
use crate::node::network::ConnectionHandle;
use crate::traffic::{Connection, TrafficGate};

/// How long a missing relay is looked for before the HTTP pin gives up on it.
const RELAY_CONNECT_WAIT_MS: u64 = 45_000;
/// How often the wait above redials.
const RELAY_REDIAL_MS: u64 = 10_000;
/// How often `hasPeer` is re-read while waiting.
const PEER_POLL_MS: u64 = 150;
/// How long a first broadcast waits for a discovery channel to open.
const FIRST_CHANNEL_WAIT_MS: u64 = 3_000;

/// The relay's `hiverelay*` settings, as `relay.js` hands them to the plugin.
#[derive(Clone, Debug, Default)]
pub struct HiverelayConfig {
    /// `hiverelays`: each entry `http(s)://host:port[=TOKEN]`, or `dht` to turn discovery on.
    pub relays: Vec<String>,
    pub api_key: Option<String>,
    pub discover: bool,
    pub replicas: u64,
    pub ttl_days: u64,
    pub include_cached: bool,
}

/// One HTTP relay and the token its `/seed-core` takes.
#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpTarget {
    pub url: String,
    pub api_key: Option<String>,
}

/// What `parseHiverelayConfig` returns.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct ParsedConfig {
    pub targets: Vec<HttpTarget>,
    pub discover: bool,
}

fn http_url(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

/// `parseHiverelayConfig`: comma-separated entries or a list, `=TOKEN` per entry, `dht`/`auto`/
/// `1`/`true` for discovery, duplicates by URL dropped.
fn parse_config(
    raw: &[String],
    default_api_key: Option<&str>,
    default_discover: bool,
) -> ParsedConfig {
    let mut targets: Vec<HttpTarget> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut discover = default_discover;
    for item in raw {
        for part in item.split(',') {
            let text = part.trim();
            if text.is_empty() {
                continue;
            }
            if matches!(text, "dht" | "auto" | "1" | "true") {
                discover = true;
                continue;
            }
            let (url, api_key) = match text.find('=') {
                Some(eq) if eq > 0 => {
                    let key = text[eq + 1..].trim();
                    (
                        &text[..eq],
                        if key.is_empty() {
                            default_api_key.map(str::to_owned)
                        } else {
                            Some(key.to_owned())
                        },
                    )
                }
                _ => (text, default_api_key.map(str::to_owned)),
            };
            let url = url.trim().trim_end_matches('/').to_owned();
            if !http_url(&url) || !seen.insert(url.clone()) {
                continue;
            }
            targets.push(HttpTarget { url, api_key });
        }
    }
    ParsedConfig { targets, discover }
}

/// `requestJson`'s answer; a transport error is `status: 0` with the message.
#[derive(Debug, Clone)]
struct JsonReply {
    pub status: u16,
    pub data: Value,
    pub error: Option<String>,
}

impl JsonReply {
    fn failed(error: impl Into<String>) -> Self {
        Self {
            status: 0,
            data: Value::Null,
            error: Some(error.into()),
        }
    }
}

type Https = HttpsConnector<HttpConnector>;
type HttpClient = Client<Https, http_body_util::Full<hyper::body::Bytes>>;

static CLIENT: LazyLock<HttpClient> = LazyLock::new(|| {
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_webpki_roots()
        .https_or_http()
        .enable_http1()
        .build();
    Client::builder(TokioExecutor::new()).build(https)
});

/// `requestJson`: one JSON reply, at most 1 MiB, without redirects.
async fn request_json(
    method: &str,
    url: &str,
    headers: &[(String, String)],
    body: Option<&str>,
    timeout: Duration,
) -> JsonReply {
    let Ok(uri) = url::Url::parse(url) else {
        return JsonReply::failed(format!("Invalid URL: {url}"));
    };
    if !matches!(uri.scheme(), "http" | "https") {
        return JsonReply::failed(format!("Invalid URL: {url}"));
    }
    let Ok(method) = hyper::Method::from_bytes(method.as_bytes()) else {
        return JsonReply::failed(format!("Invalid method: {method}"));
    };
    let mut builder = Request::builder().method(method.clone()).uri(uri.as_str());
    {
        let Some(map) = builder.headers_mut() else {
            return JsonReply::failed(format!("Invalid URL: {url}"));
        };
        let all = std::iter::once(("accept".to_owned(), "application/json".to_owned()))
            .chain(headers.iter().cloned());
        for (name, value) in all {
            let (Ok(name), Ok(value)) = (
                hyper::header::HeaderName::from_bytes(name.as_bytes()),
                hyper::header::HeaderValue::from_str(&value),
            ) else {
                continue;
            };
            map.insert(name, value);
        }
    }
    let payload = hyper::body::Bytes::from(body.map(str::to_owned).unwrap_or_default());
    let request = match builder.body(http_body_util::Full::new(payload)) {
        Ok(request) => request,
        Err(error) => return JsonReply::failed(error.to_string()),
    };
    let response = match tokio::time::timeout(timeout, CLIENT.request(request)).await {
        Err(_) => {
            return JsonReply::failed(format!(
                "HiveRelay HTTP timeout ({method} {url})"
            ));
        }
        Ok(Err(error)) => return JsonReply::failed(error.to_string()),
        Ok(Ok(response)) => response,
    };
    let status = response.status().as_u16();
    let collected = match http_body_util::BodyExt::collect(response.into_body()).await {
        Ok(body) => body.to_bytes(),
        Err(error) => return JsonReply::failed(error.to_string()),
    };
    if collected.len() > 1024 * 1024 {
        return JsonReply::failed("HiveRelay response exceeded 1 MiB");
    }
    let data = if collected.is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_slice(&collected).unwrap_or(Value::Null)
    };
    JsonReply {
        status,
        data,
        error: None,
    }
}

/// A relay's accept over `hiverelay-seed`.
#[derive(Clone, Debug)]
struct AcceptedRelay {
    pub region: String,
    pub available_storage_bytes: u64,
    pub accepted_at: u64,
    pub cores: HashSet<String>,
}

/// A relay's signed deny over `hiverelay-seed`, as the plugin reports it.
#[derive(Clone, Debug)]
struct DeniedRelay {
    pub pubkey: String,
    pub reason_code: String,
    pub detail: String,
    pub at: u64,
}

struct Pinned {
    bound_bytes: u64,
}

struct SeedChannel {
    id: u64,
    sender: ChannelSender,
    last_sent_at: AtomicU64,
}

struct BeaconDb {
    key: [u8; 32],
    disc_key: [u8; 32],
    byte_length: u64,
    last_len: Option<u64>,
    joined: bool,
}

#[derive(Default)]
struct Beacon {
    blobs: Option<hypercore::Session>,
    /// Insertion order, as the JS `Map` keeps it; the first is the reported `beaconDriveKey`.
    order: Vec<String>,
    dbs: HashMap<String, BeaconDb>,
    app_to_core: HashMap<String, String>,
}

#[derive(Default)]
struct Topics {
    discovery: bool,
    epochs: HashSet<[u8; 32]>,
}

struct Inner {
    node: Arc<CoreNode>,
    traffic: TrafficGate,
    targets: Mutex<Vec<HttpTarget>>,
    discover: AtomicBool,
    api_key: Option<String>,
    replicas: u64,
    ttl_days: u64,
    include_cached: bool,
    publisher: hypercore::crypto::KeyPair,
    accepted: Mutex<HashMap<String, AcceptedRelay>>,
    denied: Mutex<HashMap<String, DeniedRelay>>,
    http_pinned: Mutex<HashMap<String, Pinned>>,
    url_pubkeys: Mutex<HashMap<String, String>>,
    joined_peers: Mutex<HashSet<String>>,
    channels: Mutex<HashMap<String, Arc<SeedChannel>>>,
    /// Connections with a seed channel being opened, so the pairing callback never opens a second.
    opening: Mutex<HashSet<u64>>,
    current_requests: Mutex<Vec<SeedRequest>>,
    cursor: Mutex<usize>,
    last_page_at: AtomicU64,
    next_channel_id: AtomicU64,
    beacon: tokio::sync::Mutex<Beacon>,
    topics: Mutex<Topics>,
    epoch_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    detach: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    run_lock: tokio::sync::Mutex<()>,
    running: AtomicBool,
    closed: AtomicBool,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// The `hiverelay-seed` plugin. Cloning shares it.
#[derive(Clone)]
pub struct HiverelaySeeder(Arc<Inner>);

/// The beacon for one offered core: what a seed request names as its `appKey`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BeaconDrive {
    pub key: [u8; 32],
    pub byte_length: u64,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn unhex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in text.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(chunk).ok()?, 16).ok()?;
    }
    Some(out)
}

fn is_hex64(text: &str) -> bool {
    text.len() == 64 && text.bytes().all(|b| b.is_ascii_hexdigit())
}

/// `channel.local.keyPair?.secretKey || store.primaryKey`: the local writer core's secret when
/// this node writes, so a relay keeps the publisher key it had.
fn seed_material(node: &CoreNode) -> Vec<u8> {
    let local = node.channel().auto().local.key();
    let discovery = hypercore::crypto::discovery_key(local);
    if let Ok(Some(auth)) = node.store().storage().get_auth(&discovery)
        && let Some(pair) = auth.key_pair
        && let Some(secret) = pair.secret_key
    {
        return secret.to_vec();
    }
    node.store().primary_key().to_vec()
}

impl HiverelaySeeder {
    pub fn new(
        node: Arc<CoreNode>,
        traffic: TrafficGate,
        config: HiverelayConfig,
        onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        let parsed = parse_config(
            &config.relays,
            config.api_key.as_deref(),
            config.discover,
        );
        let publisher = publisher_key(&seed_material(&node));
        Self(Arc::new(Inner {
            node,
            traffic,
            targets: Mutex::new(parsed.targets),
            discover: AtomicBool::new(parsed.discover),
            api_key: config.api_key,
            replicas: config.replicas.max(1),
            ttl_days: config.ttl_days.max(1),
            include_cached: config.include_cached,
            publisher,
            accepted: Mutex::new(HashMap::new()),
            denied: Mutex::new(HashMap::new()),
            http_pinned: Mutex::new(HashMap::new()),
            url_pubkeys: Mutex::new(HashMap::new()),
            joined_peers: Mutex::new(HashSet::new()),
            channels: Mutex::new(HashMap::new()),
            opening: Mutex::new(HashSet::new()),
            current_requests: Mutex::new(Vec::new()),
            cursor: Mutex::new(0),
            last_page_at: AtomicU64::new(0),
            next_channel_id: AtomicU64::new(1),
            beacon: tokio::sync::Mutex::new(Beacon::default()),
            topics: Mutex::new(Topics::default()),
            epoch_task: Mutex::new(None),
            detach: Mutex::new(None),
            run_lock: tokio::sync::Mutex::new(()),
            running: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            onchange,
        }))
    }

    /// `enabled()`: nothing is recruited on a private-routed node, and nothing when no relay is
    /// configured and discovery is off.
    pub fn enabled(&self) -> bool {
        !self.0.node.private_routing()
            && (!self.0.targets.lock().expect("targets").is_empty()
                || self.0.discover.load(Ordering::SeqCst))
    }

    fn closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }

    fn changed(&self) {
        if let Some(onchange) = &self.0.onchange {
            onchange();
        }
    }

    async fn run_payload(&self, payload: Value) -> Result<Value, MirrorError> {
        // `run`'s `activeRun`: one sync at a time; a caller arriving meanwhile waits and then runs.
        let _serial = self.0.run_lock.lock().await;
        self.0.running.store(true, Ordering::SeqCst);
        let result = self.sync_once(&payload).await;
        self.0.running.store(false, Ordering::SeqCst);
        result
    }

    /// `syncOnce`: pin every seedable core over HTTP, then broadcast one paced page over the DHT.
    pub async fn sync_once(&self, payload: &Value) -> Result<Value, MirrorError> {
        if self.closed() {
            return Ok(json!({ "skipped": true, "reason": "closed" }));
        }
        if self.0.node.private_routing() {
            return Ok(json!({ "skipped": true, "reason": "private_routing_enabled" }));
        }
        let force = js_truthy(&payload["force"]);
        if let Some(extra) = payload.get("relays") {
            let raw = strings_of(extra);
            let added = parse_config(&raw, self.0.api_key.as_deref(), false);
            {
                let mut targets = self.0.targets.lock().expect("targets");
                for target in added.targets {
                    if !targets.iter().any(|seen| seen.url == target.url) {
                        targets.push(target);
                    }
                }
            }
            if added.discover {
                self.0.discover.store(true, Ordering::SeqCst);
            }
        }
        if payload.get("discover").is_some_and(js_truthy) {
            self.0.discover.store(true, Ordering::SeqCst);
        }
        if !self.enabled() {
            return Ok(json!({ "skipped": true, "reason": "no_hiverelays_configured" }));
        }

        self.ensure_swarm_wiring();
        let seedable: Vec<SeedableCore> = self
            .0
            .node
            .seedable_cores(self.0.include_cached)
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        if seedable.is_empty() {
            return Ok(json!({
                "cores": 0,
                "httpPinned": 0,
                "p2pBroadcasts": 0,
                "p2pAccepted": self.0.accepted.lock().expect("accepted").len(),
            }));
        }

        let lengths: HashMap<String, u64> = seedable
            .iter()
            .map(|core| (core.key.clone(), core.length))
            .collect();
        let cores: Vec<Core> = {
            let accepted = self.0.accepted.lock().expect("accepted");
            seedable
                .iter()
                .map(|core| Core {
                    key: core.key.clone(),
                    kind: core.kind.clone(),
                    byte_length: core.byte_length,
                    accepted: accepted
                        .values()
                        .filter(|relay| relay.cores.contains(&core.key))
                        .count(),
                })
                .collect()
        };

        let targets = self.0.targets.lock().expect("targets").clone();
        let mut http_pinned = 0u64;
        let mut need_p2p = self.0.discover.load(Ordering::SeqCst);
        let mut http_errors: Vec<Value> = Vec::new();
        for target in &targets {
            if self.closed() {
                break;
            }
            self.ensure_relay_connected(&target.url).await;
            let mut unauthorized = false;
            for core in &cores {
                let pin_key = format!("{}|{}", target.url, core.key);
                let bound = bound_bytes(core.byte_length);
                let previous = self
                    .0
                    .http_pinned
                    .lock()
                    .expect("pinned")
                    .get(&pin_key)
                    .map(|prev| prev.bound_bytes);
                if !force
                    && previous
                        .is_some_and(|bound| bound >= (core.byte_length as f64 * 1.1).ceil() as u64)
                {
                    http_pinned += 1;
                    continue;
                }
                let request =
                    seed_core_request(&target.url, target.api_key.as_deref(), &core.key, core.byte_length);
                let reply = self.post_seed_core(&request).await;
                if reply.status == 200 && js_truthy(&reply.data["ok"]) {
                    self.0
                        .http_pinned
                        .lock()
                        .expect("pinned")
                        .insert(pin_key, Pinned { bound_bytes: bound });
                    http_pinned += 1;
                } else if reply.status == 401 || reply.status == 403 {
                    unauthorized = true;
                    break;
                } else {
                    let detail = reply
                        .data
                        .get("error")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| reply.error.clone())
                        .unwrap_or_else(|| format!("HTTP {}", reply.status));
                    http_errors.push(json!({
                        "url": target.url,
                        "core": core.key,
                        "error": detail,
                    }));
                }
            }
            if unauthorized {
                need_p2p = true;
            }
        }

        let mut p2p_broadcasts = 0u64;
        if need_p2p && !self.closed() {
            let last = self.0.last_page_at.load(Ordering::SeqCst);
            let now = now_ms();
            if last > 0 && now.saturating_sub(last) < PAGE_INTERVAL_MS {
                tokio::time::sleep(Duration::from_millis(PAGE_INTERVAL_MS - (now - last))).await;
                if self.closed() {
                    return Ok(json!({ "skipped": true, "reason": "closed" }));
                }
            }
            let replicas = self.0.replicas.clamp(1, 32);
            let mut cursor = *self.0.cursor.lock().expect("cursor");
            let page: Vec<&Core> = select_page(&cores, replicas as usize, force, &mut cursor);
            *self.0.cursor.lock().expect("cursor") = cursor;
            let offered: HashSet<String> = page.iter().map(|core| core.key.clone()).collect();
            // Only the beacons of the page on offer stay on the DHT: a HiveRelay that accepts a core
            // is already connected, and Corestore serves it the beacon over that connection.
            let leave = {
                let mut beacon = self.0.beacon.lock().await;
                let mut leave = Vec::new();
                for (core_key, entry) in beacon.dbs.iter_mut() {
                    if offered.contains(core_key) || !entry.joined {
                        continue;
                    }
                    entry.joined = false;
                    leave.push(entry.disc_key);
                }
                leave
            };
            for disc in leave {
                let _ = self.0.node.net().swarm().leave(disc).await;
            }

            let mut requests: Vec<SeedRequest> = Vec::with_capacity(page.len());
            for core in &page {
                let length = lengths.get(&core.key).copied().unwrap_or(0);
                let drive = self.ensure_core_beacon(core, length).await?;
                let blobs_bytes = {
                    let beacon = self.0.beacon.lock().await;
                    beacon
                        .blobs
                        .as_ref()
                        .map_or(0, hypercore::Session::byte_length)
                };
                let mut request = SeedRequest {
                    app_key: drive.key,
                    discovery_keys: vec![unhex32(&core.key).unwrap_or_default()],
                    replication_factor: replicas,
                    geo_preference: "[]".into(),
                    max_storage_bytes: bound_bytes(
                        core.byte_length + drive.byte_length + blobs_bytes,
                    ),
                    bounty_rate: 0,
                    ttl_seconds: self.0.ttl_days * 86_400,
                    publisher_pubkey: self.0.publisher.public_key,
                    publisher_signature: [0; 64],
                    revocable: 1,
                    unseed_freeze_ms: 0,
                    durability: 0,
                };
                request.sign(&self.0.publisher);
                requests.push(request);
            }
            *self.0.current_requests.lock().expect("requests") = requests.clone();

            let discover = self.0.discover.load(Ordering::SeqCst);
            if self.0.channels.lock().expect("channels").is_empty() && discover {
                let end = now_ms() + FIRST_CHANNEL_WAIT_MS;
                while !self.closed()
                    && self.0.channels.lock().expect("channels").is_empty()
                    && now_ms() < end
                {
                    tokio::time::sleep(Duration::from_millis(PEER_POLL_MS)).await;
                }
            }
            self.0.last_page_at.store(now_ms(), Ordering::SeqCst);
            let peers: Vec<Arc<SeedChannel>> = self
                .0
                .channels
                .lock()
                .expect("channels")
                .values()
                .cloned()
                .collect();
            for peer in peers {
                let last = peer.last_sent_at.load(Ordering::SeqCst);
                if last > 0 {
                    let delta = now_ms().saturating_sub(last);
                    if delta < PAGE_INTERVAL_MS {
                        tokio::time::sleep(Duration::from_millis(PAGE_INTERVAL_MS - delta)).await;
                    }
                }
                peer.last_sent_at.store(now_ms(), Ordering::SeqCst);
                for request in &requests {
                    if peer.sender.send_raw(0, &request.encode()) {
                        p2p_broadcasts += 1;
                    }
                }
            }
        }

        Ok(json!({
            "cores": cores.len(),
            "coreKeys": cores.iter().map(|core| core.key.clone()).collect::<Vec<_>>(),
            "httpRelays": targets.len(),
            "httpPinned": http_pinned,
            "httpErrors": http_errors,
            "p2pPeers": self.0.channels.lock().expect("channels").len(),
            "p2pBroadcasts": p2p_broadcasts,
            "p2pAccepted": self.0.accepted.lock().expect("accepted").len(),
            "p2pDenied": self.0.denied.lock().expect("denied").len(),
        }))
    }

    async fn post_seed_core(&self, request: &HttpRequest) -> JsonReply {
        let headers: Vec<(String, String)> = request
            .headers
            .iter()
            .filter(|(name, _)| name.as_str() != "accept")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        request_json(
            request.method,
            &request.url,
            &headers,
            Some(&request.body),
            Duration::from_millis(15_000),
        )
        .await
    }

    /// `ensureSwarmWiring`: watch every connection, and join discovery when it is on.
    fn ensure_swarm_wiring(&self) {
        if self.closed() || self.0.node.private_routing() {
            return;
        }
        if self.0.detach.lock().expect("detach").is_none() {
            let inner = self.0.clone();
            let register = self.0.node.on_connection(Box::new(move |conn: ConnectionHandle| {
                HiverelaySeeder(inner.clone()).attach(conn);
            }));
            *self.0.detach.lock().expect("detach") = Some(register);
            for conn in self.0.node.peer_connections() {
                self.attach(conn);
            }
        }
        if self.0.discover.load(Ordering::SeqCst) && !self.0.topics.lock().expect("topics").discovery
        {
            self.0.topics.lock().expect("topics").discovery = true;
            join_topic(self.0.node.net().swarm(), super::discovery_topic());
            self.sync_epoch_topics();
            let inner = self.0.clone();
            let handle = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(super::EPOCH_REFRESH_MS)).await;
                    if inner.closed.load(Ordering::SeqCst) {
                        return;
                    }
                    HiverelaySeeder(inner.clone()).sync_epoch_topics();
                }
            });
            *self.0.epoch_task.lock().expect("epoch task") = Some(handle);
        }
    }

    /// `syncEpochTopics`: keep the current and next epoch's topics joined.
    fn sync_epoch_topics(&self) {
        if self.closed()
            || !self.0.discover.load(Ordering::SeqCst)
            || self.0.node.private_routing()
        {
            return;
        }
        let wanted: HashSet<[u8; 32]> = epoch_topics(now_ms()).into_iter().collect();
        let mut leave: Vec<[u8; 32]> = Vec::new();
        {
            let mut topics = self.0.topics.lock().expect("topics");
            topics.epochs.retain(|topic| {
                let keep = wanted.contains(topic);
                if !keep {
                    leave.push(*topic);
                }
                keep
            });
            for topic in &wanted {
                topics.epochs.insert(*topic);
            }
        }
        for disc in leave {
            let swarm = self.0.node.net().swarm().clone();
            tokio::spawn(async move {
                let _ = swarm.leave(disc).await;
            });
        }
        for topic in wanted {
            join_topic(self.0.node.net().swarm(), topic);
        }
    }

    fn is_bulk_peer(&self, remote: &str) -> bool {
        self.0.accepted.lock().expect("accepted").contains_key(remote)
            || self
                .0
                .url_pubkeys
                .lock()
                .expect("url pubkeys")
                .values()
                .any(|key| key == remote)
    }

    /// `attachConnection`: mark the relay bulk and open the seed channel on both sides.
    fn attach(&self, conn: ConnectionHandle) {
        if self.closed() || self.0.node.private_routing() || conn.destroyed() {
            return;
        }
        let Some(remote) = Connection::remote_public_key(&conn) else {
            return;
        };
        let remote_hex = hex(&remote);
        let shared: Arc<dyn Connection> = Arc::new(conn);
        if self.is_bulk_peer(&remote_hex) {
            self.0.traffic.mark_bulk(shared.clone());
        }
        let mux: Mux = shared.mux();
        if !mux.is_opened(PROTOCOL, None) {
            let pairs = mux.pair(PROTOCOL, None);
            let inner = self.0.clone();
            let (pair_mux, pair_conn) = (mux.clone(), shared.clone());
            tokio::spawn(async move {
                let mut pairs = pairs;
                while let Some(request) = pairs.recv().await {
                    if inner.closed.load(Ordering::SeqCst) {
                        break;
                    }
                    HiverelaySeeder(inner.clone()).open_seed(&pair_conn, &pair_mux, remote);
                    drop(request);
                }
            });
        }
        self.open_seed(&shared, &mux, remote);
    }

    fn open_seed(&self, conn: &Arc<dyn Connection>, mux: &Mux, remote: [u8; 32]) {
        if self.closed() || conn.destroyed() || mux.is_opened(PROTOCOL, None) {
            return;
        }
        let conn_id = conn.as_ref().id();
        if !self.0.opening.lock().expect("opening").insert(conn_id) {
            return;
        }
        let Some(mut channel) = super::open_channel(mux) else {
            self.0.opening.lock().expect("opening").remove(&conn_id);
            return;
        };
        let remote_hex = hex(&remote);
        let inner = self.0.clone();
        let id = inner.next_channel_id.fetch_add(1, Ordering::SeqCst);
        let conn = conn.clone();
        tokio::spawn(async move {
            let seeder = HiverelaySeeder(inner.clone());
            while let Some(event) = channel.recv().await {
                match event {
                    Event::Opened { .. } => {
                        inner.opening.lock().expect("opening").remove(&conn_id);
                        if inner.closed.load(Ordering::SeqCst) {
                            channel.close();
                            return;
                        }
                        seeder.register_channel(&remote_hex, id, channel.sender());
                        inner.traffic.mark_bulk(conn.clone());
                        let page: Vec<SeedRequest> = inner
                            .current_requests
                            .lock()
                            .expect("requests")
                            .iter()
                            .take(PAGE_SIZE)
                            .cloned()
                            .collect();
                        if !page.is_empty() {
                            if let Some(peer) = seeder.peer(&remote_hex, id) {
                                peer.last_sent_at.store(now_ms(), Ordering::SeqCst);
                            }
                            let sender = channel.sender();
                            for request in &page {
                                sender.send_raw(0, &request.encode());
                            }
                        }
                        seeder.changed();
                    }
                    Event::Message(message) => {
                        seeder
                            .on_seed_message(&remote_hex, &conn, message.kind, &message.data)
                            .await;
                    }
                    Event::Closed { .. } => break,
                }
            }
            seeder.unregister_channel(&remote_hex, id);
            inner.opening.lock().expect("opening").remove(&conn_id);
        });
    }

    fn peer(&self, remote: &str, id: u64) -> Option<Arc<SeedChannel>> {
        self.0
            .channels
            .lock()
            .expect("channels")
            .get(remote)
            .filter(|peer| peer.id == id)
            .cloned()
    }

    fn register_channel(&self, remote: &str, id: u64, sender: ChannelSender) {
        self.0.channels.lock().expect("channels").insert(
            remote.to_owned(),
            Arc::new(SeedChannel {
                id,
                sender,
                last_sent_at: AtomicU64::new(0),
            }),
        );
    }

    fn unregister_channel(&self, remote: &str, id: u64) {
        let mut channels = self.0.channels.lock().expect("channels");
        if channels.get(remote).is_some_and(|peer| peer.id == id) {
            channels.remove(remote);
        }
    }

    async fn on_seed_message(
        &self,
        remote_hex: &str,
        conn: &Arc<dyn Connection>,
        kind: u64,
        data: &[u8],
    ) {
        let Ok(message) = SeedMessage::decode(kind, data) else {
            return;
        };
        let now = now_ms();
        match message {
            SeedMessage::Request(_) => {
                // A publisher opens the same channel but sends seed requests; HiveRelays never do.
                self.0.traffic.unmark_bulk(conn);
            }
            SeedMessage::Accept(accept) => {
                let Some(remote) = unhex32(remote_hex) else {
                    return;
                };
                if !accept.valid_from(&remote) {
                    return;
                }
                let app_hex = hex(&accept.app_key);
                let core_key = {
                    let beacon = self.0.beacon.lock().await;
                    beacon.app_to_core.get(&app_hex).cloned()
                }
                .unwrap_or_else(|| app_hex.clone());
                self.0
                    .denied
                    .lock()
                    .expect("denied")
                    .remove(&format!("{remote_hex}|{core_key}"));
                {
                    let mut accepted = self.0.accepted.lock().expect("accepted");
                    let entry =
                        accepted
                            .entry(remote_hex.to_owned())
                            .or_insert_with(|| AcceptedRelay {
                                region: "unknown".into(),
                                available_storage_bytes: 0,
                                accepted_at: now,
                                cores: HashSet::new(),
                            });
                    entry.region = if accept.region.is_empty() {
                        "unknown".into()
                    } else {
                        accept.region.clone()
                    };
                    entry.available_storage_bytes = accept.available_storage_bytes;
                    entry.accepted_at = now;
                    entry.cores.insert(core_key);
                }
                self.0.traffic.mark_bulk(conn.clone());
                self.changed();
            }
            SeedMessage::Deny(deny) => {
                let Some(remote) = unhex32(remote_hex) else {
                    return;
                };
                if !deny.valid_from(&remote) {
                    return;
                }
                let app_hex = hex(&deny.app_key);
                let core_key = {
                    let beacon = self.0.beacon.lock().await;
                    beacon.app_to_core.get(&app_hex).cloned()
                }
                .unwrap_or(app_hex);
                self.0.denied.lock().expect("denied").insert(
                    format!("{remote_hex}|{core_key}"),
                    DeniedRelay {
                        pubkey: remote_hex.to_owned(),
                        reason_code: if deny.reason_code.is_empty() {
                            "denied".into()
                        } else {
                            deny.reason_code.clone()
                        },
                        detail: deny.detail.clone(),
                        at: now,
                    },
                );
                self.changed();
            }
            SeedMessage::Unseed(_) => {}
        }
    }

    /// `ensureCoreBeacon`: the tiny 2-core Hyperdrive v11/v13 beacon for one core.
    async fn ensure_core_beacon(
        &self,
        core: &Core,
        core_length: u64,
    ) -> Result<BeaconDrive, MirrorError> {
        let mut beacon = self.0.beacon.lock().await;
        if beacon.blobs.is_none() {
            let blobs = self
                .0
                .node
                .store()
                .get(GetOptions::name("hiverelay-beacon-blobs".as_bytes()))
                .await
                .map_err(|error| MirrorError::plain(error.to_string()))?;
            announce_topic(self.0.node.net().swarm(), *blobs.discovery_key());
            beacon.blobs = Some(blobs);
        }
        if let Some(entry) = beacon.dbs.get(&core.key)
            && entry.last_len == Some(core_length)
        {
            return Ok(BeaconDrive {
                key: entry.key,
                byte_length: entry.byte_length,
            });
        }
        let prefix: String = core.key.chars().take(16).collect();
        let db = self
            .0
            .node
            .store()
            .get(GetOptions::name(
                format!("hiverelay-beacon-{prefix}").as_bytes(),
            ))
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        let result = self.write_beacon(&mut beacon, core, core_length, &db).await;
        let join = match &result {
            Ok(_) => {
                let entry = beacon.dbs.get_mut(&core.key).expect("beacon entry");
                let fresh = !entry.joined;
                entry.joined = true;
                fresh.then_some(entry.disc_key)
            }
            Err(_) => None,
        };
        db.close();
        if let Some(disc) = join {
            announce_topic(self.0.node.net().swarm(), disc);
        }
        result
    }

    async fn write_beacon(
        &self,
        beacon: &mut Beacon,
        core: &Core,
        core_length: u64,
        db: &hypercore::Session,
    ) -> Result<BeaconDrive, MirrorError> {
        let blobs = beacon.blobs.as_ref().expect("beacon blobs open");
        if db.length() == 0 {
            let header = hyperdrive_header(blobs.key());
            db.append(&[header])
                .await
                .map_err(|error| MirrorError::plain(error.to_string()))?;
        }
        if !beacon.dbs.contains_key(&core.key) {
            let entry = BeaconDb {
                key: *db.key(),
                disc_key: *db.discovery_key(),
                byte_length: db.byte_length(),
                last_len: None,
                joined: false,
            };
            beacon.app_to_core.insert(hex(&entry.key), core.key.clone());
            beacon.order.push(core.key.clone());
            beacon.dbs.insert(core.key.clone(), entry);
        }
        let manifest = json!({
            "name": format!(
                "PearTube {} Core",
                if core.kind == "channel" { "Channel" } else { "Blob" }
            ),
            "version": "1.0.0",
            "description": "PearTube supplemental Hypercore pin",
            "entry": "/manifest.json",
            "categories": ["media"],
            "privacyTier": "public",
            "channel": self.0.node.status().await.channel,
            "core": core.key,
            "kind": core.kind,
        });
        let manifest_bytes =
            serde_json::to_vec(&manifest).map_err(|error| MirrorError::plain(error.to_string()))?;
        let block_offset = blobs.length();
        let byte_offset = blobs.byte_length();
        blobs
            .append(&[manifest_bytes.as_slice()])
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        let blob = BlobId {
            block_offset,
            block_length: 1,
            byte_offset,
            byte_length: manifest_bytes.len() as u64,
        };
        let root = hyperdrive_root(db.length(), "/manifest.json", &blob);
        db.append(&[root])
            .await
            .map_err(|error| MirrorError::plain(error.to_string()))?;
        let entry = beacon.dbs.get_mut(&core.key).expect("beacon entry");
        entry.byte_length = db.byte_length();
        entry.last_len = Some(core_length);
        Ok(BeaconDrive {
            key: entry.key,
            byte_length: entry.byte_length,
        })
    }

    /// `ensureRelayConnected`: find the relay's key over HTTP, dial it, and mark it bulk.
    async fn ensure_relay_connected(&self, url: &str) {
        let mut pubkey = self
            .0
            .url_pubkeys
            .lock()
            .expect("url pubkeys")
            .get(url)
            .cloned();
        if pubkey.is_none() {
            for suffix in ["/.well-known/hiverelay.json", "/api/capabilities", "/status"] {
                let reply = request_json(
                    "GET",
                    &format!("{url}{suffix}"),
                    &[],
                    None,
                    Duration::from_millis(5_000),
                )
                .await;
                let candidate = ["pubkey", "relayPubkey", "publicKey"]
                    .iter()
                    .find_map(|name| reply.data.get(*name).and_then(Value::as_str));
                if let Some(candidate) = candidate.filter(|text| is_hex64(text)) {
                    let key = candidate.to_ascii_lowercase();
                    self.0
                        .url_pubkeys
                        .lock()
                        .expect("url pubkeys")
                        .insert(url.to_owned(), key.clone());
                    pubkey = Some(key);
                    break;
                }
            }
        }
        let Some(pubkey) = pubkey else {
            return;
        };
        let Some(key) = unhex32(&pubkey) else {
            return;
        };
        if self
            .0
            .joined_peers
            .lock()
            .expect("joined peers")
            .insert(pubkey.clone())
        {
            self.0.node.net().join_peer(&pubkey, false);
        }
        let started = now_ms();
        let mut next_redial = started + RELAY_REDIAL_MS;
        while !self.closed()
            && !self.has_peer(key)
            && now_ms().saturating_sub(started) < RELAY_CONNECT_WAIT_MS
        {
            if now_ms() >= next_redial {
                self.0.node.net().join_peer(&pubkey, false);
                next_redial = now_ms() + RELAY_REDIAL_MS;
            }
            tokio::time::sleep(Duration::from_millis(PEER_POLL_MS)).await;
        }
        for conn in self.0.node.peer_connections() {
            if Connection::remote_public_key(&conn) == Some(key) {
                self.0.traffic.mark_bulk(Arc::new(conn));
            }
        }
    }

    fn has_peer(&self, key: [u8; 32]) -> bool {
        self.0
            .node
            .peer_connections()
            .into_iter()
            .any(|conn| !conn.destroyed() && Connection::remote_public_key(&conn) == Some(key))
    }

    pub fn info(&self) -> Value {
        // Read `enabled()` before taking the targets lock: it locks the same mutex.
        let enabled = self.enabled();
        let targets = self.0.targets.lock().expect("targets");
        let urls = self.0.url_pubkeys.lock().expect("url pubkeys");
        let pinned = self.0.http_pinned.lock().expect("pinned");
        let configured: Vec<Value> = targets
            .iter()
            .map(|target| {
                json!({
                    "url": public_url(&target.url),
                    "hasApiKey": target.api_key.as_deref().is_some_and(|key| !key.is_empty()),
                    "pubkey": urls.get(&target.url).cloned(),
                })
            })
            .collect();
        let beacon_drive = match self.0.beacon.try_lock() {
            Ok(beacon) => beacon
                .order
                .first()
                .and_then(|core| beacon.dbs.get(core))
                .map(|entry| hex(&entry.key)),
            Err(_) => None,
        };
        let mut accepted: Vec<Value> = self
            .0
            .accepted
            .lock()
            .expect("accepted")
            .iter()
            .map(|(pubkey, relay)| {
                json!({
                    "pubkey": pubkey,
                    "region": relay.region,
                    "availableStorageBytes": relay.available_storage_bytes,
                    "acceptedAt": relay.accepted_at,
                })
            })
            .collect();
        accepted.sort_by(|a, b| a["pubkey"].as_str().cmp(&b["pubkey"].as_str()));
        let mut denied: BTreeMap<String, Value> = BTreeMap::new();
        for relay in self.0.denied.lock().expect("denied").values() {
            denied.insert(
                relay.pubkey.clone(),
                json!({
                    "pubkey": relay.pubkey,
                    "reasonCode": relay.reason_code,
                    "detail": relay.detail,
                    "at": relay.at,
                }),
            );
        }
        let mut out = Map::new();
        out.insert("enabled".into(), json!(enabled));
        out.insert(
            "discover".into(),
            json!(self.0.discover.load(Ordering::SeqCst)),
        );
        out.insert(
            "publisherPubkey".into(),
            json!(hex(&self.0.publisher.public_key)),
        );
        out.insert("beaconDriveKey".into(), json!(beacon_drive));
        out.insert("httpPinnedCores".into(), json!(pinned.len()));
        out.insert("configuredRelays".into(), Value::Array(configured));
        if let Value::Object(fields) = serde_json::to_value(self.0.traffic.info()).unwrap_or_default()
        {
            for (key, value) in fields {
                out.insert(key, value);
            }
        }
        out.insert("p2pAcceptedRelays".into(), Value::Array(accepted));
        out.insert(
            "p2pDeniedRelays".into(),
            Value::Array(denied.into_values().collect()),
        );
        Value::Object(out)
    }

    /// `stop`: stop watching, leave the DHT topics, close every seed channel.
    pub async fn stop(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
        if let Some(task) = self.0.epoch_task.lock().expect("epoch task").take() {
            task.abort();
        }
        if let Some(detach) = self.0.detach.lock().expect("detach").take() {
            detach();
        }
        {
            let mut topics = self.0.topics.lock().expect("topics");
            let mut leave: Vec<[u8; 32]> = topics.epochs.drain().collect();
            if topics.discovery {
                topics.discovery = false;
                leave.push(super::discovery_topic());
            }
            for disc in leave {
                leave_topic(self.0.node.net().swarm(), disc);
            }
        }
        let channels: Vec<Arc<SeedChannel>> = {
            let mut channels = self.0.channels.lock().expect("channels");
            channels.drain().map(|(_, peer)| peer).collect()
        };
        for peer in channels {
            peer.sender.close();
        }
        let mut beacon = self.0.beacon.lock().await;
        let mut leave: Vec<[u8; 32]> = beacon
            .dbs
            .values()
            .filter(|entry| entry.joined)
            .map(|entry| entry.disc_key)
            .collect();
        beacon.dbs.clear();
        beacon.app_to_core.clear();
        beacon.order.clear();
        if let Some(blobs) = beacon.blobs.take() {
            leave.push(*blobs.discovery_key());
            blobs.close();
        }
        drop(beacon);
        for disc in leave {
            leave_topic(self.0.node.net().swarm(), disc);
        }
    }
}

/// `swarm.join(topic, { server: false, client: true })`: discovery only, never an announcement.
fn join_topic(swarm: &peeroxide::SwarmHandle, topic: [u8; 32]) {
    join_with(swarm, topic, false);
}

/// A beacon's own topic: announced and looked up, so a HiveRelay can fetch the beacon.
fn announce_topic(swarm: &peeroxide::SwarmHandle, topic: [u8; 32]) {
    join_with(swarm, topic, true);
}

fn join_with(swarm: &peeroxide::SwarmHandle, topic: [u8; 32], server: bool) {
    let swarm = swarm.clone();
    tokio::spawn(async move {
        let mut opts = peeroxide::JoinOpts::default();
        opts.server = server;
        opts.client = true;
        let _ = swarm.join(topic, opts).await;
    });
}

fn leave_topic(swarm: &peeroxide::SwarmHandle, topic: [u8; 32]) {
    let swarm = swarm.clone();
    // Shutdown path: start the leave and do not wait on the DHT unannounce.
    tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_millis(2_000), swarm.leave(topic)).await;
    });
}

/// The public part of a relay URL: scheme, host and path, never credentials or a query.
fn public_url(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or_default();
            let authority = match parsed.port() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_owned(),
            };
            format!(
                "{}://{}{}",
                parsed.scheme(),
                authority,
                parsed.path().trim_end_matches('/')
            )
        }
        Err(_) => url.to_owned(),
    }
}

fn strings_of(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        _ => Vec::new(),
    }
}

impl MirrorPlugin<CoreNode> for HiverelaySeeder {
    fn name(&self) -> &str {
        "hiverelay-seed"
    }

    fn description(&self) -> &str {
        "Recruits existing HiveRelays (HTTP /seed-core + P2P hiverelay-seed DHT) as supplemental seeders for public channels & 512 KiB blobs"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        _ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(self.run_payload(payload))
    }

    fn poll<'a>(
        &'a self,
        _ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            // `poll: () => (enabled() && !activeRun ? run({ once: true }) : null)`.
            if !self.enabled() || self.0.running.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let Ok(_serial) = self.0.run_lock.try_lock() else {
                return Ok(None);
            };
            self.0.running.store(true, Ordering::SeqCst);
            let result = self.sync_once(&json!({ "once": true })).await;
            self.0.running.store(false, Ordering::SeqCst);
            result.map(Some)
        })
    }

    fn stop<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(HiverelaySeeder::stop(self))
    }

    fn info(&self) -> Value {
        HiverelaySeeder::info(self)
    }
}

/// The seeder as the relay's `hiverelay-seed` plugin.
pub fn plugin(
    node: Arc<CoreNode>,
    traffic: TrafficGate,
    config: HiverelayConfig,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
) -> Arc<dyn MirrorPlugin<CoreNode>> {
    Arc::new(HiverelaySeeder::new(node, traffic, config, onchange))
}
