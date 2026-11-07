//! The default cast adapters end to end: two `Backend::spawn` instances on one local HyperDHT
//! testnet act as the controller and the TV receiver, pair over LAN with a code, cast a locally
//! published blob, and the receiver's player-facing URL serves the source's exact bytes (full and
//! ranged). One interop run drives the app's own `src/cast.js` receiver through the same backend.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
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
    _task: tokio::task::JoinHandle<Result<(), peeroxide_dht::rpc::DhtError>>,
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
        let mut config = peeroxide_dht::rpc::DhtConfig::default();
        config.bootstrap = Vec::new();
        config.host = "127.0.0.1".to_owned();
        config.port = 0;
        config.firewalled = false;
        let (task, handle) = peeroxide_dht::rpc::spawn(&udx, config)
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

    fn start(&self, trackers: &Path, cast_storage: &Path, bootstrap: &Value) -> String {
        self.ok(
            "init",
            json!({ "castStorage": cast_storage.to_string_lossy() }),
        );
        let started = self.ok(
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
        started["apiUrl"]
            .as_str()
            .unwrap_or_else(|| panic!("start answers: {started}"))
            .to_owned()
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

fn http_get(url: &str, range: Option<&str>) -> HttpResponse {
    let rest = url.strip_prefix("http://").expect("an http url");
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let mut stream = TcpStream::connect(authority).expect("the player url listens");
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
    HttpResponse {
        status,
        headers,
        body: raw[split..].to_vec(),
    }
}

/// The node status the loopback API reports.
fn node_status(api: &str) -> Value {
    let response = http_get(&format!("{api}/v1/status"), None);
    serde_json::from_slice(&response.body).unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------------------------
// The controller and the receiver.

#[test]
fn default_backends_cast_a_local_blob_over_lan() {
    let testnet = testnet();
    let bootstrap = testnet.bootstrap.clone();
    let bytes = data((BLOCK * 3 + 777) as usize);
    let digest = sha256_hex(&bytes);

    let root = temp_dir("lan");
    let controller_trackers = root.join("controller");
    let receiver_trackers = root.join("receiver");
    std::fs::create_dir_all(&controller_trackers).expect("controller dir");
    std::fs::create_dir_all(&receiver_trackers).expect("receiver dir");
    let (key, _blobs) = publish_fixture(&controller_trackers, &bootstrap, &bytes);

    let controller = Harness::spawn();
    controller.start(
        &controller_trackers,
        &root.join("controller-cast"),
        &bootstrap,
    );
    let receiver = Harness::spawn();
    let receiver_api = receiver.start(&receiver_trackers, &root.join("receiver-cast"), &bootstrap);

    let info = receiver.ok(
        "receiverStart",
        json!({ "name": "Kitchen TV", "openAuth": false }),
    );
    let port = info["port"].as_u64().expect("receiver port");
    let code = info["pairCode"]
        .as_str()
        .expect("receiver pair code")
        .to_owned();
    assert_eq!(info["name"], json!("Kitchen TV"));

    let paired = controller.ok(
        "castPair",
        json!({ "host": "127.0.0.1", "port": port, "code": code }),
    );
    let receiver_id = paired["pairedId"].as_str().expect("paired id").to_owned();
    assert!(!receiver_id.is_empty(), "{paired}");

    let played = controller.ok("castPlay", json!({ "receiverId": receiver_id, "key": key }));
    let session = &played["session"];
    assert_eq!(session["transport"], json!("lan"), "{played}");
    assert_eq!(session["state"]["playing"], json!(true), "{played}");
    assert_eq!(session["state"]["mode"], json!("p2p"), "{played}");

    // The receiver's player got its own blob URL; it serves the source's exact bytes.
    let command = receiver.event("receiverCommand", Duration::from_secs(30));
    assert_eq!(command["command"]["action"], json!("load"));
    let stream_url = command["command"]["streamUrl"]
        .as_str()
        .expect("the player's stream url")
        .to_owned();
    assert!(stream_url.starts_with("http://"), "{stream_url}");

    let fallback = command["command"]["httpStreamUrl"]
        .as_str()
        .unwrap_or("")
        .to_owned();
    assert!(!fallback.is_empty(), "the sender offers a LAN blob URL");
    let direct = http_get(&fallback, None);
    assert_eq!(direct.status, 200);
    assert_eq!(
        sha256_hex(&direct.body),
        digest,
        "the token-gated LAN Range server serves the source"
    );

    // The receiver starts replicating the film as soon as the load hands it the watch link; wait
    // until the blocks are local so the Range reads below exercise its own player URL.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let status = node_status(&receiver_api);
        let cached = status["cachedBytes"].as_u64().unwrap_or(0);
        if cached >= bytes.len() as u64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the receiver pulled {cached} of {} bytes: {status}",
            bytes.len()
        );
        std::thread::sleep(Duration::from_millis(500));
    }

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

    let playing = controller.ok("castControl", json!({ "action": "play" }));
    assert_eq!(playing["session"]["state"]["status"], json!("playing"));
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

// ---------------------------------------------------------------------------------------------
// Interop: the app's own `src/cast.js` receiver, driven by a default-spawned backend.

fn app_dir() -> PathBuf {
    std::env::var("PEARTUBE_APP")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../peartube"))
}

fn has_app() -> bool {
    let app = app_dir();
    std::path::Path::new(&app).join("src/cast.js").exists()
}

fn js_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/js")
}

struct JsProcess {
    child: Child,
}

impl Drop for JsProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_js(args: &[&str]) -> JsProcess {
    let mut command = Command::new("node");
    command
        .arg(js_dir().join("cast-interop.mjs"))
        .args(args)
        .current_dir(js_dir())
        .env("PEARTUBE_APP", app_dir())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    JsProcess {
        child: command.spawn().expect("node must be installed"),
    }
}

#[test]
#[ignore = "needs node and the app checkout (PEARTUBE_APP)"]
fn js_cast_receiver_is_driven_by_the_default_backend() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let mut process = spawn_js(&["receiver-http"]);
    let ready = {
        let child = &mut process.child;
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let line = lines.next().expect("ready line").expect("read");
        serde_json::from_str::<Value>(line.trim_start_matches("READY ")).expect("ready json")
    };
    let port = ready["httpPort"].as_u64().expect("http port");
    let code = ready["pairCode"].as_str().expect("pair code").to_owned();

    let root = temp_dir("interop");
    let trackers = root.join("trackers");
    std::fs::create_dir_all(&trackers).expect("trackers");
    let controller = Harness::spawn();
    controller.start(&trackers, &root.join("cast"), &json!([]));

    let paired = controller.ok(
        "castPair",
        json!({ "host": "127.0.0.1", "port": port, "code": code }),
    );
    let receiver_id = paired["pairedId"].as_str().expect("paired id").to_owned();
    let attached = controller.ok("castAttach", json!({ "receiverId": receiver_id }));
    assert_eq!(attached["session"]["transport"], json!("lan"), "{attached}");
    let played = controller.ok("castControl", json!({ "action": "play" }));
    assert_eq!(played["session"]["state"]["playing"], json!(true));
    let sought = controller.ok(
        "castControl",
        json!({ "action": "seek", "positionMs": 2500 }),
    );
    assert_eq!(sought["session"]["state"]["positionMs"], json!(2500));
    let paused = controller.ok("castControl", json!({ "action": "pause" }));
    assert_eq!(paused["session"]["state"]["playing"], json!(false));
    let stopped = controller.ok("castStop", json!({ "stopPlayback": true }));
    assert_eq!(stopped["session"], Value::Null);

    drop(process);
    drop(controller);
    let _ = std::fs::remove_dir_all(&root);
}
