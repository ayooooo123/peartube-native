//! The default cast adapters end to end: two `Backend::spawn` instances on one local HyperDHT
//! testnet act as the controller and the TV receiver. The receiver is opened by `receiverStart`
//! under its saved key, advertises itself on the LAN, and the controller finds it by mDNS, pairs
//! with its 6-character code, casts a locally published blob, and the receiver's own player URL
//! serves the source's exact bytes (full and ranged).
//!
//! The cross-implementation run lives in `tests/cast.rs` (Rust controller ↔ the app's receiver
//! fixture) and below (the app's controller ↔ this backend's receiver).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use peartube_native::backend::Backend;
use peartube_native::node::Node;
use peartube_native::node::api::{PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeSource, create_node};
use serde_json::{Value, json};

const BLOCK: u64 = 512 * 1024;

/// Deterministic video bytes.
fn data(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 31 + 7) % 251) as u8).collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn temp_dir(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "peartube-cast-default-{label}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

// ---------------------------------------------------------------------------------------------
// A local HyperDHT testnet: one DHT node both backends bootstrap from.

struct Testnet {
    _runtime: tokio::runtime::Runtime,
    _udx: libudx::UdxRuntime,
    _task: tokio::task::JoinHandle<Result<(), peeroxide_dht::hyperdht::HyperDhtError>>,
    bootstrap: Value,
}

fn testnet() -> Testnet {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("testnet runtime");
    let udx = libudx::UdxRuntime::new().expect("udx");
    let (task, port) = runtime.block_on(async {
        let mut config = peeroxide_dht::hyperdht::HyperDhtConfig::default();
        config.dht.bootstrap = Vec::new();
        config.dht.host = "127.0.0.1".to_owned();
        config.dht.port = 0;
        config.dht.firewalled = false;
        // A bare RPC node cannot store announcements or route peer handshakes.
        let (task, handle, _events) = peeroxide_dht::hyperdht::spawn(&udx, config)
            .await
            .expect("testnet node");
        let port = handle.local_port().await.expect("testnet port");
        (task, port)
    });
    Testnet {
        _runtime: runtime,
        _udx: udx,
        _task: task,
        bootstrap: json!([format!("127.0.0.1:{port}")]),
    }
}

// ---------------------------------------------------------------------------------------------
// Driving a default-spawned backend over its line protocol.

struct Harness {
    backend: Backend,
    lines: mpsc::Receiver<String>,
    events: Arc<Mutex<Vec<Value>>>,
    next: AtomicU64,
}

impl Harness {
    fn spawn() -> Self {
        let (tx, lines) = mpsc::channel();
        let backend = Backend::spawn(json!({}), move |line| {
            let _ = tx.send(line);
        })
        .expect("backend starts");
        Self {
            backend,
            lines,
            events: Arc::new(Mutex::new(Vec::new())),
            next: AtomicU64::new(1),
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        self.backend
            .send(&json!({ "id": id, "method": method, "params": params }).to_string())
            .expect("backend accepts");
        loop {
            let line = self
                .lines
                .recv_timeout(Duration::from_secs(180))
                .unwrap_or_else(|_| panic!("no response for {method}"));
            let value: Value = serde_json::from_str(line.trim()).expect("a JSON line");
            if value.get("id").and_then(Value::as_u64) == Some(id) {
                if let Some(error) = value.get("error") {
                    return Err(error.as_str().unwrap_or("backend error").to_owned());
                }
                return Ok(value.get("result").cloned().unwrap_or(Value::Null));
            }
            self.events.lock().expect("events").push(value);
        }
    }

    fn ok(&self, method: &str, params: Value) -> Value {
        self.call(method, params)
            .unwrap_or_else(|error| panic!("{method}: {error}"))
    }

    /// Waits for an event the worker emitted while a call was in flight.
    fn event(&self, kind: &str, until: Duration) -> Value {
        let deadline = Instant::now() + until;
        loop {
            if let Some(found) = self
                .events
                .lock()
                .expect("events")
                .iter()
                .find(|event| event.get("event").and_then(Value::as_str) == Some(kind))
                .cloned()
            {
                return found;
            }
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "no {kind} event");
            if let Ok(line) = self
                .lines
                .recv_timeout(left.min(Duration::from_millis(250)))
                && let Ok(value) = serde_json::from_str::<Value>(line.trim())
            {
                self.events.lock().expect("events").push(value);
            }
        }
    }

    fn start(&self, trackers: &Path, cast_storage: &Path, bootstrap: &Value) {
        self.ok(
            "init",
            json!({ "castStorage": cast_storage.to_string_lossy() }),
        );
        self.ok(
            "start",
            json!({
                "trackersDir": trackers.to_string_lossy(),
                "defaultFolder": "default",
                "settings": {
                    "tracker": "",
                    "mnemonic": "",
                    "sources": [],
                    "relayThrough": [],
                    "bootstrap": bootstrap,
                    "lanDiscovery": true,
                    "privateRouting": false,
                    "cacheMaxGb": 10
                },
            }),
        );
    }
}

fn trackers_storage(trackers: &Path) -> PathBuf {
    trackers.join("default")
}

/// Publishes one video into the storage a later `start` reopens (a `Backend::spawn` cannot
/// publish yet, so the fixture is written before the node hands its folder to the worker).
fn publish_fixture(trackers: &Path, bootstrap: &Value, bytes: &[u8]) -> (String, String) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("publish runtime");
    let storage = trackers_storage(trackers);
    let node: Arc<CoreNode> = runtime
        .block_on(create_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
        })))
        .expect("publish node");
    let id = "imdb:tt0000001".to_owned();
    let meta = PutMeta {
        id: id.clone(),
        title: "Cast Fixture".to_owned(),
        private_entry: None,
        epoch: None,
        store: Some("video:fixture-cast".to_owned()),
        expected_size: None,
        extra: serde_json::Map::new(),
    };
    let published = runtime
        .block_on(node.publish(meta, NodeSource::bytes(bytes.to_vec())))
        .expect("publish");
    let found = runtime
        .block_on(node.search(Some(&id), SearchOptions::default()))
        .expect("search");
    let entry = found
        .into_iter()
        .find(|entry| entry.id == id)
        .unwrap_or_else(|| panic!("the published entry is searchable"));
    assert!(entry.local, "the published entry is local");
    runtime.block_on(node.close()).expect("publish node closes");
    (entry.key, published.announce.blobs)
}

// ---------------------------------------------------------------------------------------------
// Minimal HTTP/1.1 GET with an optional Range.

struct HttpResponse {
    status: u16,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

fn try_http_get(url: &str, range: Option<&str>) -> Option<HttpResponse> {
    let rest = url.strip_prefix("http://").expect("an http url");
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let address = authority.to_socket_addrs().ok()?.next()?;
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(90)))
        .expect("read timeout");
    let range = range
        .map(|value| format!("Range: {value}\r\n"))
        .unwrap_or_default();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n{range}\r\n"
    )
    .expect("request");
    let mut raw = Vec::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => raw.extend_from_slice(&buffer[..read]),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break;
            }
            Err(error) => panic!("read the player url: {error}"),
        }
    }
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers end")
        + 4;
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    let mut headers = HashMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_lowercase(), value.trim().to_owned());
        }
    }
    Some(HttpResponse {
        status,
        headers,
        body: raw[split..].to_vec(),
    })
}

fn http_get(url: &str, range: Option<&str>) -> HttpResponse {
    try_http_get(url, range).unwrap_or_else(|| panic!("{url} answers"))
}

/// The receiver's player port answers only on loopback; the URL already says 127.0.0.1.
fn assert_loopback(url: &str) {
    assert!(url.starts_with("http://127.0.0.1:"), "{url}");
}

// ---------------------------------------------------------------------------------------------
// The controller and the receiver.

fn wait_for<T>(label: &str, until: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + until;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn default_backends_cast_over_the_lan() {
    let testnet = testnet();
    let bootstrap = testnet.bootstrap.clone();
    let bytes = data((BLOCK * 3 + 777) as usize);
    let digest = sha256_hex(&bytes);
    let tv_name = format!("Kitchen TV {}", std::process::id());

    let root = temp_dir("lan");
    let controller_trackers = root.join("controller");
    std::fs::create_dir_all(&controller_trackers).expect("controller dir");
    let (key, _blobs) = publish_fixture(&controller_trackers, &bootstrap, &bytes);

    let controller = Harness::spawn();
    controller.start(
        &controller_trackers,
        &root.join("controller-cast"),
        &bootstrap,
    );
    // The TV's node is opened by `receiverStart` under its saved receiver key.
    let receiver = Harness::spawn();
    receiver.ok(
        "init",
        json!({ "castStorage": root.join("receiver-cast").to_string_lossy() }),
    );
    let info = receiver.ok(
        "receiverStart",
        json!({ "name": tv_name, "openAuth": false }),
    );
    let code = info["pairCode"]
        .as_str()
        .expect("receiver pair code")
        .to_owned();
    assert_eq!(info["name"], json!(tv_name));
    assert_eq!(info["protocol"], json!("peartube-cast/v1"));
    assert_eq!(info["authRequired"], json!(true));
    let public_key = info["publicKey"].as_str().expect("receiver key").to_owned();
    assert_eq!(public_key.len(), 64);

    // mDNS finds the TV; the pair happens on the LAN link to it.
    let discovered = wait_for("the TV on the LAN", Duration::from_secs(60), || {
        let status = controller.ok("castStatus", json!({}));
        status["receivers"]
            .as_array()?
            .iter()
            .find(|row| row["name"] == json!(tv_name))
            .cloned()
    });
    assert_eq!(discovered["current"], json!(true), "{discovered}");
    let receiver_id = discovered["id"].as_str().expect("discovered id").to_owned();
    let paired = controller
        .call("castPair", json!({ "id": receiver_id, "code": code }))
        .unwrap_or_else(|error| {
            let status = controller.ok("castStatus", json!({}));
            let receiver_status = receiver.ok("castStatus", json!({}));
            panic!(
                "castPair: {error}\ninfo: {info}\ndiscovered: {discovered}\ncontroller: {status}\nreceiver: {receiver_status}"
            )
        });
    let paired_id = paired["pairedId"].as_str().expect("paired id").to_owned();
    assert!(!paired_id.is_empty(), "{paired}");

    let played = controller.ok(
        "castPlay",
        json!({ "receiverId": paired_id, "key": key, "startPositionMs": 0 }),
    );
    let session = &played["session"];
    assert_eq!(session["transport"], json!("lan"), "{played}");
    assert_eq!(session["receiverName"], json!(tv_name), "{played}");
    assert_eq!(session["state"]["mode"], json!("p2p"), "{played}");

    let command = receiver.event("receiverCommand", Duration::from_secs(60));
    assert_eq!(command["command"]["action"], json!("load"));
    let stream_url = command["command"]["streamUrl"]
        .as_str()
        .expect("the player's stream url")
        .to_owned();
    assert_loopback(&stream_url);
    assert!(stream_url.contains("/cast/v1/p2p/"), "{stream_url}");

    // The TV pulls the film over the connection it shares with the sender; its own player URL
    // serves the exact bytes once the blocks are local.
    let cached = wait_for("the TV to cache the film", Duration::from_secs(120), || {
        let status = receiver.ok("castStatus", json!({}));
        let cached = status["receiverMode"]["state"]["cachedBytes"].as_u64()?;
        (cached >= bytes.len() as u64).then_some(cached)
    });
    assert!(cached >= bytes.len() as u64);

    let full = http_get(&stream_url, None);
    assert_eq!(full.status, 200);
    assert_eq!(full.body.len(), bytes.len());
    assert_eq!(sha256_hex(&full.body), digest, "the served bytes match");

    let start = 1000usize;
    let end = 9999usize;
    let ranged = http_get(&stream_url, Some(&format!("bytes={start}-{end}")));
    assert_eq!(ranged.status, 206);
    assert_eq!(
        ranged.headers.get("content-range").map(String::as_str),
        Some(format!("bytes {start}-{end}/{}", bytes.len()).as_str())
    );
    assert_eq!(ranged.body, bytes[start..=end]);

    let played = controller.ok("castControl", json!({ "action": "play" }));
    assert_eq!(played["session"]["state"]["status"], json!("playing"));
    let sought = controller.ok(
        "castControl",
        json!({ "action": "seek", "positionMs": 2500 }),
    );
    assert_eq!(sought["session"]["state"]["positionMs"], json!(2500));
    let paused = controller.ok("castControl", json!({ "action": "pause" }));
    assert_eq!(paused["session"]["state"]["playing"], json!(false));

    let stopped = controller.ok("castStop", json!({ "stopPlayback": true }));
    assert_eq!(stopped["session"], Value::Null);
    let receiver_status = receiver.ok("castStatus", json!({}));
    assert_eq!(
        receiver_status["receiverMode"]["state"]["status"],
        json!("idle"),
        "{receiver_status}"
    );

    drop(controller);
    drop(receiver);
    let _ = std::fs::remove_dir_all(&root);
}
