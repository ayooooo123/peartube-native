//! The cast stack against the app's own fixtures, and the control channel end to end over
//! in-memory Protomux connections: pairing rules, who may control a receiver, state pushes, a
//! session's play/pause/seek/stop, and the sender's refusal of a peer it never paired with.
//!
//! `fixtures/cast-reference.json` comes from `tests/js/cast-fixtures.mjs`, which runs the app's own
//! `src/cast.js` helpers. JS↔Rust wire interop lives in `tests/cast.rs`'s ignored tests.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use hyperblobs::BlobId;
use hypercore::Core;
use protomux::{Mux, StreamInfo};
use serde_json::{Map, Value, json};

use peartube_native::cast::adapters::NodePeer;
use peartube_native::cast::control::{Control, ControlHandlers, DEFAULT_TIMEOUT, open_control};
use peartube_native::cast::pairing::ReceiverIdentity;
use peartube_native::cast::traits::{
    CastPeer, ClearCacheOptions, EvictOptions, LanAddress, LanEndpoint, OpenedMedia, PeerConn,
    PeerConnection, PeerStatus,
};
use peartube_native::cast::{
    CastController, ControllerOptions, Receiver, ReceiverOptions, SavedReceiver, SavedReceivers,
    cast_lan_topic, list_receivers, qr, receiver_target, state,
};
use peartube_native::node::Node;
use peartube_native::node::api::{PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeSource, create_node};

const FIXTURES: &str = include_str!("../fixtures/cast-reference.json");
const DEVICE_ID: &str = "0123456789abcdef0123456789abcdef";
const SEED: &str = "abababababababababababababababababababababababababababababababab";
const PAIR_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const PAIR_CODE: &str = "ABC123";
const NAME: &str = "Kitchen TV";
const SENDER_KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const RECEIVER_KEY: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const STRANGER_KEY: &str = "3333333333333333333333333333333333333333333333333333333333333333";

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/cast-reference.json")
}

fn cases<'a>(fixtures: &'a Value, key: &str) -> &'a [Value] {
    fixtures[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} cases"))
}

fn mask(value: &Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.iter().map(mask).collect()),
        Value::Object(fields) => {
            let mut out = Map::new();
            for (key, item) in fields {
                if key == "updatedAt" || key == "storageFreeBytes" {
                    out.insert(key.clone(), json!(0));
                } else {
                    out.insert(key.clone(), mask(item));
                }
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

fn same_json(actual: &Value, expected: &Value, context: &str) {
    assert_eq!(
        serde_json::to_string(actual).expect("json"),
        serde_json::to_string(expected).expect("json"),
        "{context}"
    );
}

fn temp_dir(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "peartube-cast-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).expect("temp dir");
    path
}

fn seed_identity(dir: &Path) {
    let identity = json!({
        "deviceId": DEVICE_ID,
        "name": NAME,
        "seed": SEED,
        "pairToken": PAIR_TOKEN,
        "pairCode": PAIR_CODE,
    });
    std::fs::write(
        ReceiverIdentity::file(dir),
        serde_json::to_string_pretty(&identity).expect("json"),
    )
    .expect("identity");
}

// ---------------------------------------------------------------- in-memory peers

static NEXT_CONN: AtomicU64 = AtomicU64::new(1);

struct MemConn {
    id: u64,
    key: String,
    lan: bool,
    mux: Mux,
}

impl PeerConnection for MemConn {
    fn id(&self) -> u64 {
        self.id
    }
    fn remote_public_key(&self) -> String {
        self.key.clone()
    }
    fn is_lan(&self) -> bool {
        self.lan
    }
    fn destroyed(&self) -> bool {
        self.mux.is_destroyed()
    }
    fn mux(&self) -> Mux {
        self.mux.clone()
    }
    fn closed(&self) -> BoxFuture<'static, ()> {
        let mux = self.mux.clone();
        Box::pin(async move { mux.closed().await })
    }
    fn close(&self) {
        self.mux.destroy();
    }
}

type Listener = Arc<dyn Fn(PeerConn) + Send + Sync>;

/// A node as the cast stack uses it: a peer key, its connections, and a tiny catalog.
struct FakePeer {
    key: String,
    lan: bool,
    private: bool,
    /// Hands a new listener the connections already open, as a node that wires connections and
    /// then replays them to a late listener would.
    replay_on_listen: std::sync::atomic::AtomicBool,
    conns: Mutex<Vec<PeerConn>>,
    listeners: Mutex<Vec<Listener>>,
    topics: Mutex<Vec<(String, [u8; 32])>>,
    endpoints: Mutex<HashMap<String, PeerConn>>,
    entries: Mutex<HashMap<String, Value>>,
    warmed: Mutex<Vec<String>>,
    opened: Mutex<Vec<String>>,
}

impl FakePeer {
    fn new(key: &str) -> Arc<Self> {
        Arc::new(Self {
            key: key.to_owned(),
            lan: true,
            private: false,
            replay_on_listen: std::sync::atomic::AtomicBool::new(false),
            conns: Mutex::new(Vec::new()),
            listeners: Mutex::new(Vec::new()),
            topics: Mutex::new(Vec::new()),
            endpoints: Mutex::new(HashMap::new()),
            entries: Mutex::new(HashMap::new()),
            warmed: Mutex::new(Vec::new()),
            opened: Mutex::new(Vec::new()),
        })
    }

    fn add(&self, conn: PeerConn) {
        let key = conn.remote_public_key();
        self.endpoints
            .lock()
            .expect("endpoints")
            .insert(key, conn.clone());
        self.conns.lock().expect("conns").push(conn.clone());
        let listeners = self.listeners.lock().expect("listeners").clone();
        for listener in listeners {
            listener(conn.clone());
        }
    }

    fn publish(&self, entry: Value) {
        let key = entry
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.entries.lock().expect("entries").insert(key, entry);
    }

    fn warmed(&self) -> Vec<String> {
        self.warmed.lock().expect("warmed").clone()
    }

    fn topics(&self) -> Vec<String> {
        self.topics
            .lock()
            .expect("topics")
            .iter()
            .map(|(id, _)| id.clone())
            .collect()
    }
}

impl CastPeer for FakePeer {
    fn peer_key(&self) -> String {
        self.key.clone()
    }
    fn private_routing(&self) -> bool {
        self.private
    }
    fn has_lan(&self) -> bool {
        self.lan
    }
    fn lan_endpoint(&self) -> Option<LanEndpoint> {
        Some(LanEndpoint {
            host: "127.0.0.1".to_owned(),
            port: 40123,
            public_key: self.key.clone(),
        })
    }
    fn status(&self) -> PeerStatus {
        PeerStatus {
            peers: 0,
            lan_peers: 1,
            routes: None,
        }
    }
    fn join_lan_topic(&self, id: &str, topic: [u8; 32]) {
        self.topics
            .lock()
            .expect("topics")
            .push((id.to_owned(), topic));
    }
    fn peer_connections(&self) -> Vec<PeerConn> {
        self.conns.lock().expect("conns").clone()
    }
    fn on_connection(&self, callback: Listener) -> Box<dyn Fn() + Send + Sync> {
        self.listeners.lock().expect("listeners").push(callback.clone());
        if self.replay_on_listen.load(Ordering::Acquire) {
            for conn in self.conns.lock().expect("conns").clone() {
                callback(conn);
            }
        }
        Box::new(|| {})
    }
    fn connect_peer(
        &self,
        key: &str,
        _lan: Option<LanAddress>,
        _timeout: Duration,
    ) -> BoxFuture<'_, Result<PeerConn, peartube_native::cast::CastError>> {
        let key = key.to_owned();
        Box::pin(async move {
            match self.endpoints.lock().expect("endpoints").get(&key).cloned() {
                Some(conn) => Ok(conn),
                None => Err(peartube_native::cast::CastError::new(
                    "The peer did not connect",
                    0,
                )),
            }
        })
    }
    fn open_uri(
        &self,
        uri: &str,
        _join_source: bool,
    ) -> BoxFuture<'_, Result<Option<OpenedMedia>, peartube_native::cast::CastError>> {
        self.opened.lock().expect("opened").push(uri.to_owned());
        let parsed = peartube_native::protocol::parse_uri(uri);
        Box::pin(async move {
            match parsed {
                Ok(peartube_native::protocol::ParsedUri::Watch { entry, .. }) => {
                    Ok(Some(OpenedMedia {
                        action: "watch".to_owned(),
                        stream_url: None,
                        entry: serde_json::to_value(entry).expect("entry"),
                        blob: None,
                    }))
                }
                _ => Ok(None),
            }
        })
    }
    fn blob_core(
        &self,
        _blobs_hex: &str,
    ) -> BoxFuture<'_, Result<Option<Core>, peartube_native::cast::CastError>> {
        Box::pin(async { Ok(None) })
    }
    fn clear_cache(&self, _opts: ClearCacheOptions) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn evict_behind(
        &self,
        _core_key_hex: &str,
        _blob: BlobId,
        _opts: EvictOptions,
    ) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn find_entry(&self, key: &str) -> BoxFuture<'_, Option<Value>> {
        let entry = self.entries.lock().expect("entries").get(key).cloned();
        Box::pin(async move { entry })
    }
    fn search(&self, id: &str) -> BoxFuture<'_, Vec<Value>> {
        let entries = self.entries.lock().expect("entries").clone();
        let id = id.to_owned();
        Box::pin(async move {
            entries
                .values()
                .filter(|entry| entry.get("id").and_then(Value::as_str) == Some(id.as_str()))
                .cloned()
                .collect()
        })
    }
    fn warm(&self, raw: &Value) -> BoxFuture<'_, ()> {
        let key = raw
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.warmed.lock().expect("warmed").push(key);
        Box::pin(async {})
    }
    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// Two nodes on one in-memory connection, as `connectPeer` would leave them.
fn link(a: &Arc<FakePeer>, b: &Arc<FakePeer>, lan: bool) {
    let ((ar, aw), (br, bw)) = protomux::memory::duplex(64);
    let mux_a = Mux::new(
        ar,
        aw,
        StreamInfo {
            is_initiator: true,
            ..StreamInfo::default()
        },
    );
    let mux_b = Mux::new(br, bw, StreamInfo::default());
    let count = NEXT_CONN.fetch_add(1, Ordering::AcqRel);
    a.add(Arc::new(MemConn {
        id: count,
        key: b.key.clone(),
        lan,
        mux: mux_a,
    }));
    b.add(Arc::new(MemConn {
        id: count + 1_000_000,
        key: a.key.clone(),
        lan,
        mux: mux_b,
    }));
}

async fn start_receiver(dir: &Path, peer: Arc<dyn CastPeer>, open_auth: bool) -> Receiver {
    seed_identity(dir);
    Receiver::start(ReceiverOptions {
        storage: dir.to_path_buf(),
        peer_storage: None,
        name: NAME.to_owned(),
        port: 0,
        lan_host: None,
        peer: Some(peer),
        mdns: false,
        open_auth,
        any_port: true,
        on_command: None,
        on_change: None,
    })
    .await
    .expect("receiver starts")
}

async fn controller(peer: Arc<FakePeer>, dir: &Path) -> CastController {
    let node = peer.clone();
    CastController::start(ControllerOptions {
        storage: dir.to_path_buf(),
        get_node: Arc::new(move || Some(node.clone() as Arc<dyn CastPeer>)),
        get_lan_host: Arc::new(|| Some("127.0.0.1".to_owned())),
        mdns: false,
        on_change: None,
    })
    .await
    .expect("controller starts")
}

fn sender_conn(peer: &Arc<FakePeer>) -> PeerConn {
    peer.peer_connections()
        .into_iter()
        .find(|conn| conn.remote_public_key() == RECEIVER_KEY)
        .expect("the sender's connection to the receiver")
}

// ---------------------------------------------------------------- fixtures

#[test]
fn pair_codes_normalize_like_the_app() {
    for case in cases(&fixtures(), "pairCodes") {
        let input = case["input"].as_str().unwrap_or_default();
        assert_eq!(
            peartube_native::cast::normalize_pair_code(Some(input)),
            case["output"].as_str().unwrap(),
            "normalizePairCode({input:?})"
        );
    }
}

#[test]
fn the_idle_state_matches_the_app() {
    let fixtures = fixtures();
    let expected = &fixtures["idle"];
    let keys: Vec<String> = expected["keys"]
        .as_array()
        .expect("keys")
        .iter()
        .map(|key| key.as_str().unwrap_or_default().to_owned())
        .collect();
    let now = 1_700_000_000_000;
    let idle = state::idle(now);
    assert_eq!(
        idle.keys().cloned().collect::<Vec<String>>(),
        keys,
        "idleCastState key order"
    );
    let expected_state = mask(&expected["state"]);
    let actual = mask(&state::to_json(&idle));
    same_json(&actual, &expected_state, "idle state");
}

#[test]
fn ranges_match_the_app() {
    use peartube_native::cast::http::{RangeSpec, parse_http_range};
    for case in cases(&fixtures(), "ranges") {
        let total = case["total"].as_u64().unwrap_or(10);
        let header = case["header"].as_str();
        let expected = &case["result"];
        let actual = match parse_http_range(header, total) {
            RangeSpec::Invalid => json!({ "invalid": true }),
            RangeSpec::Partial { start, end } => json!({
                "partial": true, "start": start, "end": end
            }),
            RangeSpec::Full { start, end } => json!({
                "partial": false, "start": start, "end": end
            }),
        };
        let expected_partial = expected
            .get("partial")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let expected_invalid = expected
            .get("invalid")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let mut want = Map::new();
        want.insert("partial".into(), json!(expected_partial));
        if expected_invalid {
            want.insert("invalid".into(), json!(true));
        } else {
            want.insert(
                "start".into(),
                expected.get("start").cloned().unwrap_or(json!(0)),
            );
            want.insert(
                "end".into(),
                expected.get("end").cloned().unwrap_or(json!(0)),
            );
        }
        // The app's invalid answer is `{ invalid: true }` alone.
        if expected_invalid {
            assert_eq!(actual, json!({ "invalid": true }), "header {header:?}");
        } else {
            assert_eq!(actual, Value::Object(want), "header {header:?}");
        }
    }
}

#[test]
fn mime_guesses_match_the_app() {
    use peartube_native::cast::http::guess_media_mime;
    for case in cases(&fixtures(), "mime") {
        assert_eq!(
            guess_media_mime(&case["entry"]),
            case["mime"].as_str().unwrap(),
            "guessMediaMime({})",
            case["entry"]
        );
    }
}

#[test]
fn qr_codes_match_the_app() {
    for case in cases(&fixtures(), "qr") {
        let text = case["text"].as_str().unwrap();
        assert_eq!(
            qr::qr_svg(text),
            case["svg"].as_str().unwrap(),
            "qr({text:?})"
        );
    }
}

#[test]
fn saved_receivers_round_trip_through_the_file() {
    let dir = temp_dir("records");
    let mut saved = SavedReceivers::load(&dir);
    saved.set(SavedReceiver {
        id: "peer-1".into(),
        name: "Kitchen TV".into(),
        public_key: RECEIVER_KEY.to_owned(),
        token: PAIR_TOKEN.to_owned(),
    });
    saved.save(&dir).expect("save");
    let again = SavedReceivers::load(&dir);
    assert_eq!(again.get("peer-1").unwrap().token, PAIR_TOKEN);
    let rows = list_receivers(&[], &again);
    assert_eq!(rows[0].transport, "p2p");
    assert!(rows[0].paired);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_receiver_target_prefers_the_saved_key() {
    let mut saved = SavedReceivers::default();
    saved.set(SavedReceiver {
        id: "peer-1".into(),
        name: "Saved".into(),
        public_key: RECEIVER_KEY.to_owned(),
        token: PAIR_TOKEN.to_owned(),
    });
    let discovered = vec![peartube_native::cast::DiscoveredReceiver {
        id: "peer-1".into(),
        name: "MDNS".into(),
        host: "192.168.1.9".into(),
        public_key: STRANGER_KEY.to_owned(),
        lan: None,
        auth: "code".into(),
        current: true,
    }];
    let target = receiver_target("peer-1", &discovered, &saved);
    assert_eq!(target.public_key, RECEIVER_KEY);
    assert_eq!(target.token, PAIR_TOKEN);
    assert_eq!(target.name, "MDNS");
}

// ---------------------------------------------------------------- the control channel

async fn pair(control: &Control, code: &str) -> Result<Value, String> {
    let mut payload = Map::new();
    payload.insert("code".into(), json!(code));
    control
        .call("pair", payload, DEFAULT_TIMEOUT)
        .await
        .map_err(|error| error.message)
}

async fn cmd(control: &Control, token: &str, command: Value) -> Result<Value, String> {
    let mut payload = command.as_object().cloned().unwrap_or_default();
    payload.insert("token".into(), json!(token));
    control
        .call("cmd", payload, DEFAULT_TIMEOUT)
        .await
        .map_err(|error| error.message)
}

#[tokio::test(flavor = "multi_thread")]
async fn pairing_rules_and_who_may_control() {
    let dir = temp_dir("control");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let sender_peer = FakePeer::new(SENDER_KEY);
    let stranger_peer = FakePeer::new(STRANGER_KEY);
    let receiver = start_receiver(&dir, receiver_peer.clone(), false).await;
    assert_eq!(receiver_peer.topics(), vec!["cast".to_owned()]);
    link(&sender_peer, &receiver_peer, true);
    link(&stranger_peer, &receiver_peer, false);

    // A connected peer that never paired gets nothing.
    let stranger = open_control(
        &sender_conn(&stranger_peer).mux(),
        ControlHandlers::default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    let error = cmd(&stranger, "", json!({ "action": "stop" }))
        .await
        .expect_err("no command without the token");
    assert_eq!(error, "Unauthorized; pair with this TV first");
    for _ in 0..6 {
        let error = pair(&stranger, "000000").await.expect_err("wrong code");
        assert_eq!(error, "Invalid TV pairing code");
    }
    let error = pair(&stranger, PAIR_CODE).await.expect_err("locked out");
    assert_eq!(error, "Too many pairing attempts; wait 15 seconds");
    stranger.close();

    // The paired sender controls the receiver.
    let control = open_control(
        &sender_conn(&sender_peer).mux(),
        ControlHandlers::default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    let error = pair(&control, "000000").await.expect_err("wrong code");
    assert_eq!(error, "Invalid TV pairing code");
    let paired = pair(&control, "abc-123").await.expect("code accepted");
    assert_eq!(paired["deviceId"], DEVICE_ID);
    assert_eq!(paired["name"], NAME);
    assert_eq!(paired["publicKey"], RECEIVER_KEY);
    assert_eq!(
        paired["token"], PAIR_TOKEN,
        "code pairing returns the token"
    );

    let mut subscribe = Map::new();
    subscribe.insert("token".into(), json!(PAIR_TOKEN));
    let state = control
        .call("subscribe", subscribe, DEFAULT_TIMEOUT)
        .await
        .expect("subscribe");
    assert_eq!(state["state"]["status"], "idle");

    let reply = cmd(&control, PAIR_TOKEN, json!({ "action": "play" }))
        .await
        .expect("play");
    assert_eq!(reply["ok"], true);
    assert_eq!(reply["state"]["playing"], true);
    assert_eq!(reply["state"]["status"], "playing");
    let error = cmd(&control, PAIR_TOKEN, json!({ "action": "teleport" }))
        .await
        .expect_err("unknown action");
    assert_eq!(error, "Unsupported cast action: teleport");
    let error = cmd(&control, PAIR_TOKEN, json!({}))
        .await
        .expect_err("no action");
    assert_eq!(error, "action is required");
    control.close();
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_served_twice_still_takes_casts() {
    // The startup scan and a listener that replays existing connections both serve the same
    // connection; the second server must not unregister the first one's pairer.
    let dir = temp_dir("double-serve");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    receiver_peer
        .replay_on_listen
        .store(true, Ordering::Release);
    let sender_peer = FakePeer::new(SENDER_KEY);
    link(&sender_peer, &Arc::clone(&receiver_peer), true);
    let receiver = start_receiver(&dir, receiver_peer.clone(), false).await;
    let control = open_control(&sender_conn(&sender_peer).mux(), ControlHandlers::default(), DEFAULT_TIMEOUT)
        .await
        .expect("channel opens");
    let paired = pair(&control, PAIR_CODE).await.expect("paired");
    assert_eq!(paired["token"], PAIR_TOKEN);
    control.close();
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_open_receiver_answers_the_room_but_not_a_far_link() {
    let dir = temp_dir("open");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let room = FakePeer::new(SENDER_KEY);
    let far_peer = FakePeer::new(STRANGER_KEY);
    let receiver = start_receiver(&dir, receiver_peer.clone(), true).await;
    link(&room, &receiver_peer, true);
    link(&far_peer, &receiver_peer, false);

    let control = open_control(
        &sender_conn(&room).mux(),
        ControlHandlers::default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    let paired = pair(&control, "").await.expect("no code on the LAN");
    assert_eq!(
        paired["token"], "",
        "an open LAN pairing never hands out the token"
    );
    // The room is trusted: an open LAN link may command the TV without a token.
    let reply = cmd(&control, "", json!({ "action": "play" }))
        .await
        .expect("open LAN control");
    assert_eq!(reply["state"]["playing"], true);

    let far = open_control(
        &sender_conn(&far_peer).mux(),
        ControlHandlers::default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    let error = pair(&far, "").await.expect_err("a far link needs the code");
    assert_eq!(error, "Invalid TV pairing code");
    let paired = pair(&far, PAIR_CODE).await.expect("code accepted");
    assert_eq!(paired["token"], PAIR_TOKEN);

    control.close();
    far.close();
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_load_hands_the_player_a_loopback_url_and_state_pushes_follow() {
    let dir = temp_dir("load");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let sender_peer = FakePeer::new(SENDER_KEY);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    seed_identity(&dir);
    let receiver = Receiver::start(ReceiverOptions {
        storage: dir.clone(),
        peer_storage: None,
        name: NAME.to_owned(),
        port: 0,
        lan_host: None,
        peer: Some(receiver_peer.clone()),
        mdns: false,
        open_auth: false,
        any_port: true,
        on_command: Some(Arc::new(move |command| {
            let _ = tx.send(command);
        })),
        on_change: None,
    })
    .await
    .expect("receiver starts");
    link(&sender_peer, &receiver_peer, true);
    let (push_tx, mut pushed) = tokio::sync::mpsc::unbounded_channel();
    let control = open_control(
        &sender_conn(&sender_peer).mux(),
        ControlHandlers {
            on_event: Some(Arc::new(move |message| {
                let _ = push_tx.send(message);
            })),
            ..ControlHandlers::default()
        },
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    // One channel per connection: a second open is refused.
    assert!(
        open_control(
            &sender_conn(&sender_peer).mux(),
            ControlHandlers::default(),
            DEFAULT_TIMEOUT
        )
        .await
        .is_err(),
        "one control channel per connection"
    );
    let _ = pair(&control, PAIR_CODE).await.expect("paired");
    let mut subscribe = Map::new();
    subscribe.insert("token".into(), json!(PAIR_TOKEN));
    control
        .call("subscribe", subscribe, DEFAULT_TIMEOUT)
        .await
        .expect("subscribe");

    let watch = format!(
        "peartube://watch/{}:0:3:0:1200?id=imdb%3Att0133093&title=The%20Matrix",
        "ef".repeat(32)
    );
    let reply = cmd(
        &control,
        PAIR_TOKEN,
        json!({ "action": "load", "entry": { "key": "k1", "id": "imdb:tt0133093", "title": "The Matrix", "size": 1200 }, "magnetUrl": watch, "startPositionMs": 5000 }),
    )
    .await
    .expect("load");
    assert_eq!(reply["state"]["mode"], "p2p");
    let command = rx.recv().await.expect("load command reaches the player");
    assert_eq!(command["action"], "load");
    assert_eq!(command["startPositionMs"], 5000);
    assert!(command.get("token").is_none(), "the pair token never reaches the player");
    assert!(command.get("type").is_none());
    let stream_url = command["streamUrl"].as_str().expect("stream url");
    assert!(
        stream_url.starts_with(&format!(
            "http://127.0.0.1:{}/cast/v1/p2p/",
            receiver.port()
        )),
        "{stream_url}"
    );

    // The player reports its position; a seek moves it; a stop idles the TV.
    let patch = json!({ "playing": true, "positionMs": 6500, "durationMs": 60_000 });
    let state = receiver.report_state(&patch);
    assert_eq!(state["positionMs"], 6500);
    assert_eq!(state["status"], "playing");
    // A subscriber hears the player's report (after any earlier state pushes).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let pushed_state = loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let message = tokio::time::timeout(left, pushed.recv())
            .await
            .expect("push arrives")
            .expect("channel open");
        if message["state"]["positionMs"] == 6500 {
            break message;
        }
    };
    assert_eq!(pushed_state["event"], "state");

    let reply = cmd(
        &control,
        PAIR_TOKEN,
        json!({ "action": "seek", "positionMs": 24_000 }),
    )
    .await
    .expect("seek");
    assert_eq!(reply["state"]["positionMs"], 24_000);
    let command = rx.recv().await.expect("seek command");
    assert_eq!(command["action"], "seek");
    assert_eq!(command["positionMs"], 24_000);
    let reply = cmd(&control, PAIR_TOKEN, json!({ "action": "stop" }))
        .await
        .expect("stop");
    assert_eq!(reply["state"]["status"], "idle");
    assert!(rx.recv().await.is_some(), "stop command");
    control.close();
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wrong_link_is_not_a_cast() {
    let dir = temp_dir("link");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let receiver = start_receiver(&dir, receiver_peer.clone(), false).await;
    let sender = FakePeer::new(SENDER_KEY);
    link(&sender, &receiver_peer, true);
    let control = open_control(
        &sender_conn(&sender).mux(),
        ControlHandlers::default(),
        DEFAULT_TIMEOUT,
    )
    .await
    .expect("channel opens");
    let error = cmd(
        &control,
        PAIR_TOKEN,
        json!({ "action": "load", "magnetUrl": "https://example.test/film.mp4" }),
    )
    .await
    .expect_err("not a watch link");
    assert_eq!(error, "A cast load needs a peartube://watch link");
    control.close();
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------- the controller

fn entry(key: &str, id: &str, title: &str) -> Value {
    json!({
        "key": key,
        "id": id,
        "title": title,
        "size": 1200,
        "sha256": "ab".repeat(32),
        "blobs": "ef".repeat(32),
        "blob": { "blockOffset": 0, "blockLength": 3, "byteOffset": 0, "byteLength": 1200 },
        "source": "99".repeat(32),
        "local": true,
        "cachedBytes": 1200
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn the_controller_pairs_casts_and_controls() {
    let dir = temp_dir("controller");
    let receiver_dir = temp_dir("controller-tv");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let sender_peer = FakePeer::new(SENDER_KEY);
    let receiver = start_receiver(&receiver_dir, receiver_peer.clone(), false).await;
    link(&sender_peer, &receiver_peer, true);
    let cast = controller(sender_peer.clone(), &dir).await;
    assert!(cast.status()["session"].is_null());

    sender_peer.publish(entry("k1", "imdb:tt0133093", "The Matrix"));
    // Without a saved token, a code receiver refuses a cast before connecting.
    let error = cast
        .cast_play(&json!({ "receiverId": RECEIVER_KEY, "key": "k1" }))
        .await
        .expect_err("not paired");
    assert!(
        error.message.contains("6-character TV code"),
        "{}",
        error.message
    );

    let record = cast
        .pair_receiver(&json!({ "publicKey": RECEIVER_KEY, "code": PAIR_CODE }))
        .await
        .expect("paired");
    assert_eq!(record["id"], DEVICE_ID);
    assert_eq!(record["publicKey"], RECEIVER_KEY);
    assert_eq!(record["token"], PAIR_TOKEN);
    assert_eq!(cast.status()["receivers"][0]["paired"], true);

    let status = cast
        .cast_play(&json!({ "receiverId": DEVICE_ID, "key": "k1" }))
        .await
        .expect("cast");
    assert_eq!(status["session"]["receiverName"], NAME);
    assert_eq!(status["session"]["transport"], "lan");
    assert!(
        sender_peer.warmed().iter().all(|key| key == "k1"),
        "the sender warms what it holds"
    );

    let status = cast
        .cast_control(&json!({ "action": "pause" }))
        .await
        .expect("pause");
    assert_eq!(status["session"]["state"]["playing"], false);
    let status = cast
        .cast_control(&json!({ "action": "seek", "positionMs": 12_000 }))
        .await
        .expect("seek");
    assert_eq!(status["session"]["state"]["positionMs"], 12_000);

    // The attach path opens a fresh session for a TV that is already playing.
    cast.cast_stop(false).await.expect("stop without playback");
    assert!(cast.status()["session"].is_null());
    let status = cast
        .cast_attach(&json!({ "receiverId": DEVICE_ID }))
        .await
        .expect("attach");
    assert_eq!(status["session"]["transport"], "lan");
    let status = cast.cast_stop(true).await.expect("stop");
    assert!(status["session"].is_null());

    let removed = cast.remove_receiver(DEVICE_ID);
    assert_eq!(removed["removed"], true);

    cast.close().await;
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&receiver_dir);
}

// ---------------------------------------------------------------- the loopback player

fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    let status = response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_owned();
    (status, body)
}

#[tokio::test(flavor = "multi_thread")]
async fn the_loopback_port_answers_only_its_own_player() {
    let dir = temp_dir("loopback");
    let receiver_peer = FakePeer::new(RECEIVER_KEY);
    let receiver = start_receiver(&dir, receiver_peer, false).await;
    let (status, body) = http_get(receiver.port(), "/cast/v1/info");
    assert_eq!(status, 200);
    let info: Value = serde_json::from_str(&body).expect("info json");
    assert_eq!(info["deviceId"], DEVICE_ID);
    assert_eq!(info["name"], NAME);
    assert_eq!(info["state"]["status"], "idle");
    let (status, _) = http_get(
        receiver.port(),
        "/cast/v1/p2p/00000000000000000000000000000000",
    );
    assert_eq!(status, 404);
    let (status, _) = http_get(receiver.port(), "/tv");
    assert_eq!(status, 404);
    receiver.close().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_cast_lan_topic_is_the_hashed_service_name() {
    assert_eq!(
        cast_lan_topic(),
        peeroxide::discovery_key(b"peartube-cast/v1")
    );
}

// Keep the shared state helpers honest until the live interop run covers them.
#[test]
fn a_report_patch_fills_the_app_fields() {
    let mut idle = state::idle(1);
    idle.insert(
        "entry".into(),
        json!({ "id": "imdb:tt0133093", "title": "The Matrix" }),
    );
    let patch = state::report_patch(
        &idle,
        &json!({ "playing": true, "positionMs": 10, "durationMs": 1000, "videoDecoder": "h264", "droppedFrames": 2 }),
    )
    .expect("patch");
    let mut merged = idle.clone();
    state::update(&mut merged, &patch, 2);
    assert_eq!(merged["status"], "playing");
    assert_eq!(merged["videoDecoder"], "h264");
    assert_eq!(merged["droppedFrames"], 2);
    assert_eq!(merged["updatedAt"], 2);
}

#[test]
fn the_plan_covers_every_transport_action() {
    let idle = state::idle(1);
    for action in [
        "play",
        "pause",
        "togglePlay",
        "seek",
        "skip",
        "selectAudio",
        "selectSubtitle",
        "volume",
    ] {
        let cmd =
            json!({ "action": action, "positionMs": 5, "deltaMs": 5, "stream": 1, "volume": 0.5 });
        state::plan_command(&idle, &cmd).unwrap_or_else(|error| panic!("{action}: {error}"));
    }
    let error = state::plan_command(&idle, &json!({ "action": "nope" })).expect_err("unknown");
    assert_eq!(error.message, "Unsupported cast action: nope");
}

// ---------------------------------------------------------------- JS interop

fn app_dir() -> PathBuf {
    std::env::var("PEARTUBE_APP")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../peartube"))
}

fn has_app() -> bool {
    app_dir().join("src/cast.js").exists()
}

fn js_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/js")
}

/// One harness process: the app's own cast stack on its own testnet, over JSON lines.
struct Js {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: mpsc::Receiver<String>,
    next: u64,
    bootstrap: Value,
}

impl Drop for Js {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Js {
    fn spawn(size: u32) -> Self {
        let mut child = Command::new("node")
            .arg(js_dir().join("cast-interop.mjs"))
            .arg("serve")
            .arg(size.to_string())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PEARTUBE_APP", app_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("node must be installed");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout");
        let (sender, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let first = lines
            .recv_timeout(Duration::from_secs(120))
            .expect("harness printed nothing");
        let startup: Value = serde_json::from_str(&first).expect("harness startup json");
        Self {
            child,
            stdin,
            lines,
            next: 0,
            bootstrap: startup["bootstrap"].clone(),
        }
    }

    fn call(&mut self, op: &str, params: Value) -> Result<Value, String> {
        self.next += 1;
        let id = self.next;
        let line = json!({ "id": id, "op": op, "params": params }).to_string();
        writeln!(self.stdin.as_mut().expect("stdin"), "{line}").expect("harness stdin");
        loop {
            let line = self
                .lines
                .recv_timeout(Duration::from_secs(180))
                .unwrap_or_else(|_| panic!("no answer for {op}"));
            let value: Value = serde_json::from_str(&line).expect("harness json");
            if value.get("id").and_then(Value::as_u64) != Some(id) {
                continue;
            }
            if value["ok"] == json!(true) {
                return Ok(value["result"].clone());
            }
            return Err(value["error"]
                .as_str()
                .unwrap_or("harness error")
                .to_owned());
        }
    }

    fn ok(&mut self, op: &str, params: Value) -> Value {
        self.call(op, params)
            .unwrap_or_else(|error| panic!("{op}: {error}"))
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime")
}

fn receiver_storage(label: &str) -> PathBuf {
    let dir = temp_dir(label);
    seed_identity(&dir);
    dir
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn player_http_get(url: &str) -> (u16, Vec<u8>) {
    let rest = url.strip_prefix("http://").expect("an http url");
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let mut stream = TcpStream::connect(authority).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("timeout");
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )
    .expect("request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read");
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers")
        + 4;
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    (status, raw[split..].to_vec())
}

fn wait_until<T>(label: &str, until: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + until;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// A receiver node under its saved key, on the harness's testnet.
async fn receiver_on(bootstrap: &Value, dir: &Path) -> Arc<CoreNode> {
    let peer_dir = dir.join("peer");
    let key_pair = peartube_native::cast::receiver::receiver_key_pair_json(dir).expect("key pair");
    create_node(json!({
        "storage": peer_dir.to_string_lossy(),
        "bootstrap": bootstrap,
        "keyPair": key_pair,
        "storageWal": false,
        "storageMemory": true
    }))
    .await
    .expect("receiver node")
}

#[test]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
fn the_apps_controller_casts_to_this_receiver() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap.clone();
    let rt = runtime();
    let dir = receiver_storage("js-controller");
    let node = rt.block_on(receiver_on(&bootstrap, &dir));
    let commands: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = commands.clone();
    let receiver = rt
        .block_on(Receiver::start(ReceiverOptions {
            storage: dir.clone(),
            peer_storage: Some(dir.join("peer")),
            name: NAME.to_owned(),
            port: 0,
            lan_host: None,
            peer: Some(Arc::new(NodePeer::new(node.clone()))),
            mdns: false,
            open_auth: false,
            any_port: true,
            on_command: Some(Arc::new(move |command| {
                sink.lock().expect("commands").push(command);
            })),
            on_change: None,
        }))
        .expect("receiver starts");
    let receiver_key = receiver.public_key();

    let controller = js.ok("jsController", json!({}));
    let controller_name = controller["name"]
        .as_str()
        .expect("controller name")
        .to_owned();
    let published = js.ok("publish", json!({ "node": controller_name }));
    let digest = published["sha256"].as_str().expect("digest").to_owned();
    let size = published["size"].as_u64().expect("size");

    let record = js.ok(
        "castPair",
        json!({ "controller": controller_name, "params": { "publicKey": receiver_key, "code": PAIR_CODE } }),
    );
    assert_eq!(record["token"], PAIR_TOKEN, "{record}");
    let receiver_id = record["id"].as_str().expect("paired id").to_owned();

    let played = js.ok(
        "castPlay",
        json!({
            "controller": controller_name,
            "params": { "receiverId": receiver_id, "key": published["key"] }
        }),
    );
    assert_eq!(played["session"]["transport"], json!("p2p"), "{played}");
    assert_eq!(played["session"]["state"]["mode"], json!("p2p"), "{played}");

    // The TV pulls the film from the app's sender; its own player URL serves the exact bytes.
    wait_until("the TV to cache the film", Duration::from_secs(120), || {
        (receiver.state()["cachedBytes"].as_u64().unwrap_or(0) >= size).then_some(())
    });
    let stream_url = wait_until("a load command", Duration::from_secs(10), || {
        commands
            .lock()
            .expect("commands")
            .iter()
            .find(|command| command["action"] == json!("load"))
            .and_then(|command| command["streamUrl"].as_str().map(str::to_owned))
    });
    let (status, body) = player_http_get(&stream_url);
    assert_eq!(status, 200);
    assert_eq!(body.len() as u64, size);
    assert_eq!(
        sha256_hex(&body),
        digest,
        "the TV serves the app's exact bytes"
    );

    let paused = js.ok(
        "castControl",
        json!({ "controller": controller_name, "params": { "action": "pause" } }),
    );
    assert_eq!(paused["session"]["state"]["playing"], json!(false));
    assert_eq!(receiver.state()["playing"], json!(false));
    let sought = js.ok(
        "castControl",
        json!({ "controller": controller_name, "params": { "action": "seek", "positionMs": 24_000 } }),
    );
    assert_eq!(sought["session"]["state"]["positionMs"], json!(24_000));
    assert_eq!(receiver.state()["positionMs"], json!(24_000));
    let stopped = js.ok(
        "castStop",
        json!({ "controller": controller_name, "params": { "stopPlayback": true } }),
    );
    assert_eq!(stopped["session"], Value::Null);
    assert_eq!(receiver.state()["status"], json!("idle"));

    // A peer that connects without pairing is refused control and locked out after six codes.
    let stranger = js.ok("stranger", json!({ "publicKey": receiver_key }));
    assert_eq!(
        stranger["cmd"],
        json!("Unauthorized; pair with this TV first")
    );
    assert_eq!(stranger["pair"], json!("Invalid TV pairing code"));
    assert_eq!(
        stranger["locked"],
        json!("Too many pairing attempts; wait 15 seconds")
    );

    js.ok("close", json!({}));
    rt.block_on(receiver.close());
    rt.block_on(node.close()).expect("receiver node closes");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
fn this_controller_casts_to_the_apps_receiver_fixture() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap.clone();
    let rt = runtime();
    let dir = temp_dir("js-receiver");

    let fixture = js.ok(
        "jsReceiver",
        json!({
            "storage": dir.join("receiver").to_string_lossy(),
            "name": "Bedroom Roku TV"
        }),
    );
    let fixture_key = fixture["publicKey"]
        .as_str()
        .expect("fixture key")
        .to_owned();
    let fixture_code = fixture["pairCode"]
        .as_str()
        .expect("fixture code")
        .to_owned();

    let node = rt
        .block_on(create_node(json!({
            "storage": dir.join("sender").to_string_lossy(),
            "bootstrap": bootstrap,
        })))
        .expect("sender node");
    let bytes = (0..(2 * 1024 * 1024 + 333))
        .map(|index| ((index * 31 + 7) % 251) as u8)
        .collect::<Vec<u8>>();
    let digest = {
        use sha2::{Digest, Sha256};
        Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let _published = rt
        .block_on(node.publish(
            PutMeta {
                id: "imdb:tt0133093".to_owned(),
                title: "Interop Matrix".to_owned(),
                private_entry: None,
                epoch: None,
                store: Some("video:interop".to_owned()),
                expected_size: None,
                extra: serde_json::Map::new(),
            },
            NodeSource::bytes(bytes.clone()),
        ))
        .expect("publish");
    let entry = rt
        .block_on(node.search(Some("imdb:tt0133093"), SearchOptions::default()))
        .expect("search")
        .into_iter()
        .next()
        .expect("published entry");
    let key = entry.key;

    let cast = rt
        .block_on(CastController::start(ControllerOptions {
            storage: dir.join("cast"),
            get_node: {
                let node = node.clone();
                Arc::new(move || Some(Arc::new(NodePeer::new(node.clone())) as Arc<dyn CastPeer>))
            },
            get_lan_host: Arc::new(|| None),
            mdns: false,
            on_change: None,
        }))
        .expect("controller starts");
    let record = rt
        .block_on(cast.pair_receiver(&json!({ "publicKey": fixture_key, "code": fixture_code })))
        .expect("paired");
    let receiver_id = record["id"].as_str().expect("paired id").to_owned();
    let status = rt
        .block_on(cast.cast_play(&json!({ "receiverId": receiver_id, "key": key })))
        .expect("cast");
    assert_eq!(status["session"]["transport"], json!("p2p"), "{status}");
    assert_eq!(status["session"]["state"]["mode"], json!("p2p"), "{status}");

    // The app's fixture plays the film through its own player and records the hash it fetched.
    let storage = fixture["storage"].as_str().expect("fixture storage");
    let playback = wait_until(
        "the fixture's playback artifact",
        Duration::from_secs(120),
        || {
            let artifact = js.ok("lastPlayback", json!({ "storage": storage }));
            (artifact["fullSha256"] == json!(digest)).then_some(artifact)
        },
    );
    assert_eq!(playback["rangeStatus"], json!(206));
    assert_eq!(playback["rangeBytes"], json!(4096));
    assert_eq!(playback["fullBytes"], json!(bytes.len()));
    assert_eq!(playback["mode"], json!("p2p"));

    let paused = rt
        .block_on(cast.cast_control(&json!({ "action": "pause" })))
        .expect("pause");
    assert_eq!(paused["session"]["state"]["playing"], json!(false));
    let sought = rt
        .block_on(cast.cast_control(&json!({ "action": "seek", "positionMs": 24_000 })))
        .expect("seek");
    assert_eq!(sought["session"]["state"]["positionMs"], json!(24_000));
    let stopped = rt.block_on(cast.cast_stop(true)).expect("stop");
    assert_eq!(stopped["session"], Value::Null);

    js.ok("close", json!({}));
    rt.block_on(cast.close());
    rt.block_on(node.close()).expect("node closes");
    let _ = std::fs::remove_dir_all(&dir);
}
