//! LAN discovery interop: the app's real node (`src/index.js` + `src/lan.js`) and this backend
//! over an isolated LAN HyperDHT, with no DHT reachable (both sides bootstrap to 127.0.0.1:9), the
//! shape `test/desktop.e2e.js` uses for its LAN test.
//!
//! (a) a JS-published entry listed and streamed by a default `Backend::spawn`
//! (b) a Rust-published entry listed and streamed by the JS node
//! (c) two Rust nodes
//! (d) the `lan.js` rules the port has to keep: token gating, smaller-key-dials-first, redial
//!
//! Needs `node` and the app checkout (`PEARTUBE_APP`, else the sibling `../peartube`) with its modules;
//! `#[ignore]` so a plain `cargo test` stays offline. Real mDNS on this machine's LAN interface is
//! used; when the machine has no private Wi-Fi/Ethernet address the tests say so and skip.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Ipv4Addr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use peartube_native::backend::Backend;
use peartube_native::lan::{DIAL_AFTER, IfaceAddress, KeyPair, LanDiscovery, lan_address};
use peartube_native::node::Node;
use peartube_native::node::api::{LanFactory, PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeSource, create_node};
use peartube_native::node::network::LanBinding;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const HARNESS: &str = "tests/js/lan-interop.mjs";
const DEAD_DHT: &str = "127.0.0.1:9";
const WAIT: Duration = Duration::from_secs(180);

// ── environment ─────────────────────────────────────────────────────────────

fn app_dir() -> String {
    std::env::var("PEARTUBE_APP").unwrap_or_else(|_| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../peartube")
            .to_string_lossy()
            .into_owned()
    })
}

fn has_app() -> bool {
    Path::new(&app_dir()).join("src/index.js").exists()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The address the app's own `lanAddress()` picks; `None` means this machine has no LAN to test on.
fn lan_host(rt: &tokio::runtime::Runtime) -> Option<Ipv4Addr> {
    // Two nodes on one machine cannot always reach each other on the machine's own LAN address
    // (the packet leaves and has to hairpin back), so the tests allow pinning the loopback, where
    // mDNS is just as real.
    if let Ok(host) = std::env::var("PEARTUBE_LAN_HOST") {
        return host.parse().ok();
    }
    let factory = peartube_native::lan::Factory;
    let interfaces = rt.block_on(LanFactory::interfaces(&factory)).ok()?;
    let view: Vec<IfaceAddress<'_>> = interfaces
        .iter()
        .map(|(name, address, internal)| IfaceAddress {
            name: name.as_str(),
            address: *address,
            internal: *internal,
        })
        .collect();
    lan_address(&view)
}

fn tempdir(prefix: &str) -> PathBuf {
    tempfile::Builder::new()
        .prefix(prefix)
        .tempdir()
        .expect("tempdir")
        .keep()
}

// ── the JS node ─────────────────────────────────────────────────────────────

/// One harness process; lines are parsed into JSON so a `t: "error"` fails the test readably.
struct JsNode {
    child: Child,
    lines: mpsc::Receiver<Value>,
}

impl JsNode {
    fn spawn(args: &[String]) -> Self {
        let mut child = Command::new("node")
            .arg(
                Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join(HARNESS)
                    .to_string_lossy()
                    .to_string(),
            )
            .args(args)
            .env("PEARTUBE_APP", app_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("node");
        let stdout = child.stdout.take().expect("node stdout");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str::<Value>(&line) {
                    let _ = tx.send(value);
                }
            }
        });
        Self { child, lines }
    }

    fn wait_for(&self, what: &str, mut matches: impl FnMut(&Value) -> bool) -> Value {
        let deadline = Instant::now() + WAIT;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(left.max(Duration::from_millis(1))) {
                Ok(value) if value["t"] == "error" => {
                    panic!("{what}: JS node failed: {value}")
                }
                Ok(value) if matches(&value) => return value,
                Ok(_) => {}
                Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("timed out waiting for {what}")
                }
            }
        }
    }

    fn expect_exit(&mut self) {
        let status = self.child.wait().expect("wait for node");
        assert!(status.success(), "the JS node exited with {status}");
    }
}

impl Drop for JsNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ── HTTP ────────────────────────────────────────────────────────────────────

fn request(url: &str, method: &str, body: Option<&str>) -> (u16, Vec<u8>) {
    let rest = url.strip_prefix("http://").expect("http url");
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let address: std::net::SocketAddr = authority.parse().expect("authority");
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(10))
        .unwrap_or_else(|error| panic!("connect {authority}: {error}"));
    stream
        .set_read_timeout(Some(Duration::from_secs(120)))
        .unwrap();
    let mut head =
        format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
    if let Some(body) = body {
        head.push_str("Content-Type: application/json\r\n");
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write head");
    if let Some(body) = body {
        stream.write_all(body.as_bytes()).expect("write body");
    }
    stream.flush().expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("headers end");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status code");
    (status, raw[split + 4..].to_vec())
}

fn json_get(url: &str) -> Value {
    let (status, body) = request(url, "GET", None);
    assert_eq!(status, 200, "GET {url}: {}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).expect("JSON response")
}

fn until<T>(what: &str, ms: u64, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_millis(ms);
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

// ── Rust nodes ──────────────────────────────────────────────────────────────

/// A node created by the default factory, wired to the default LAN factory on `host` — the
/// `createNode` + `moveLan` pair a default `Backend::spawn` runs.
fn rust_node(rt: &tokio::runtime::Runtime, host: Ipv4Addr, storage: &Path) -> Arc<CoreNode> {
    let node = rt
        .block_on(create_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": [DEAD_DHT],
            "streamHost": "127.0.0.1",
            "streamPort": 0,
        })))
        .expect("rust node");
    let factory = peartube_native::lan::Factory;
    let lan = rt
        .block_on(LanFactory::create(&factory, host, &node))
        .expect("the isolated LAN DHT opens");
    rt.block_on(node.net().set_lan(Some(lan)))
        .expect("the node takes the LAN binding");
    node
}

fn publish(node: &CoreNode, rt: &tokio::runtime::Runtime, id: &str, bytes: &[u8]) -> Value {
    let published = rt
        .block_on(node.publish(
            PutMeta {
                id: id.to_owned(),
                title: format!("Rust LAN {id}"),
                private_entry: None,
                epoch: None,
                store: None,
                expected_size: Some(Some(bytes.len() as u64)),
                extra: serde_json::Map::new(),
            },
            NodeSource::bytes(bytes.to_vec()),
        ))
        .expect("publish");
    json!({
        "channel": published.channel,
        "blobs": published.announce.blobs,
        "entry": published.announce,
        "streamUrl": published.stream_url,
        "sha256": published.announce.sha256,
        "size": published.announce.size,
    })
}

/// The follower's own copy of `id`, once its channel has synced.
fn find_entry(node: &CoreNode, rt: &tokio::runtime::Runtime, id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(150);
    loop {
        let list = rt
            .block_on(node.search(
                Some(id),
                SearchOptions {
                    source: Some(None),
                    query: Some(None),
                    limit: Some(50.into()),
                    offset: Some(0.into()),
                },
            ))
            .unwrap_or_default();
        if let Some(entry) = list.into_iter().find(|entry| entry.id == id) {
            return serde_json::to_value(entry).expect("entry json");
        }
        assert!(
            Instant::now() < deadline,
            "entry {id} never arrived; LAN: {:?}; node: {:?}",
            rt.block_on(node.lan_stats()),
            rt.block_on(node.status())
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn read_stream(node: &CoreNode, rt: &tokio::runtime::Runtime, entry: &Value) -> Vec<u8> {
    // `search` lists `Entry`s; the announce record behind it names the blob core.
    let key = entry["key"].as_str().expect("entry key");
    let blobs = rt
        .block_on(node.find_entry(key))
        .ok()
        .flatten()
        .and_then(|value| value["blobs"].as_str().map(str::to_owned))
        .expect("the announce names its blob");
    rt.block_on(node.blob_core(&blobs))
        .expect("the follower opens the blob core");
    let url = entry["streamUrl"].as_str().expect("stream url");
    let (status, bytes) = request(url, "GET", None);
    assert_eq!(status, 200, "streaming {url}");
    bytes
}

// ── the default backend ─────────────────────────────────────────────────────

fn wait_for(lines: &mpsc::Receiver<String>, id: u64) -> Value {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "no response for id {id}");
        let line = lines
            .recv_timeout(left)
            .unwrap_or_else(|_| panic!("no response for id {id}"));
        let value: Value = serde_json::from_str(line.trim()).expect("a JSON line");
        if value.get("id").and_then(Value::as_u64) == Some(id) {
            return value;
        }
    }
}

/// A bare `Backend::spawn` (the default adapters) started on the isolated LAN with `channel`
/// already followed, as the app starts with a saved source.
fn default_backend(channel: &str, root: &Path) -> (Backend, String, Value) {
    let trackers = root.join("trackers");
    let cast = root.join("cast");
    std::fs::create_dir_all(&trackers).unwrap();
    std::fs::create_dir_all(&cast).unwrap();
    let (tx, lines) = mpsc::channel();
    let backend = Backend::spawn(json!({}), move |line| {
        let _ = tx.send(line);
    })
    .expect("backend starts");
    backend
        .send(
            &json!({
                "id": 1,
                "method": "init",
                "params": { "castStorage": cast.to_string_lossy() },
            })
            .to_string(),
        )
        .unwrap();
    assert_eq!(wait_for(&lines, 1)["result"], json!({ "ok": true }));
    backend
        .send(
            &json!({
                "id": 2,
                "method": "start",
                "params": {
                    "trackersDir": trackers.to_string_lossy(),
                    "defaultFolder": "default",
                    "settings": {
                        "tracker": "",
                        "mnemonic": "",
                        "sources": [{ "key": channel, "kind": "channel" }],
                        "relayThrough": [],
                        "bootstrap": [DEAD_DHT],
                        "lanDiscovery": true,
                        "privateRouting": false,
                        "cacheMaxGb": 10
                    }
                },
            })
            .to_string(),
        )
        .unwrap();
    let started = wait_for(&lines, 2);
    let result = started
        .get("result")
        .unwrap_or_else(|| panic!("start failed: {started}"));
    let url = result["apiUrl"]
        .as_str()
        .unwrap_or_else(|| panic!("no apiUrl in {result}"))
        .to_owned();
    (backend, url, started)
}

fn entries(api: &str) -> Vec<Value> {
    json_get(&format!("{api}/v1/entries"))
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

// ── (a) JS publishes, a default backend lists and streams ───────────────────

#[test]
#[ignore = "needs real mDNS on this machine's LAN interface, node and the PearTube app checkout"]
fn js_publishes_and_default_backend_streams_over_lan() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let host = host.to_string();
    let id = "imdb:tt0068646";
    let publisher = JsNode::spawn(&[
        "publish".into(),
        host.clone(),
        "1048579".into(),
        id.into(),
        "LAN JS Publish".into(),
    ]);
    let ready = publisher.wait_for("the JS node publishes", |value| value["t"] == "ready");
    let channel = ready["channel"].as_str().expect("channel").to_owned();
    let expected = ready["sha256"].as_str().expect("sha256").to_owned();
    let size = ready["size"].as_u64().expect("size");

    let root = tempdir("peartube-lan-backend-");
    let (backend, api, started) = default_backend(&channel, &root);
    let note = format!("on {host}");
    assert_eq!(
        started["result"]["lan"].as_str(),
        Some(note.as_str()),
        "LAN discovery binds the app's own address: {started}"
    );

    let entry = until("the backend lists the JS entry", 150_000, || {
        entries(&api)
            .into_iter()
            .find(|entry| entry["id"] == id && entry["sha256"] == expected)
    });
    assert_eq!(entry["size"].as_u64(), Some(size));
    let url = entry["streamUrl"].as_str().expect("stream url").to_owned();
    let (status, bytes) = request(&url, "GET", None);
    assert_eq!(status, 200, "the backend streams the JS blob");
    assert_eq!(bytes.len() as u64, size);
    assert_eq!(sha256_hex(&bytes), expected, "SHA-256 of the JS blob");
    assert_eq!(entry["source"].as_str(), Some(channel.as_str()));

    backend.shutdown();
    drop(publisher);
    let _ = std::fs::remove_dir_all(&root);
}

// ── (b) Rust publishes, the JS node lists and streams ───────────────────────

#[test]
#[ignore = "needs real mDNS on this machine's LAN interface, node and the PearTube app checkout"]
fn rust_publishes_and_js_streams_over_lan() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let storage = tempdir("peartube-lan-rust-");
    let node = rust_node(&rt, host, &storage);
    let id = "imdb:tt0111161";
    let mut bytes = vec![0u8; 900_001];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 7) as u8;
    }
    let expected = sha256_hex(&bytes);
    let published = publish(&node, &rt, id, &bytes);
    assert_eq!(published["sha256"].as_str(), Some(expected.as_str()));
    let channel = published["channel"].as_str().expect("channel").to_owned();

    let mut follower = JsNode::spawn(&[
        "follow".into(),
        host.to_string(),
        channel,
        id.into(),
        expected.clone(),
        bytes.len().to_string(),
    ]);
    let entry = follower.wait_for("the JS node lists the Rust entry", |value| {
        value["t"] == "entry"
    });
    assert_eq!(entry["entry"]["sha256"].as_str(), Some(expected.as_str()));
    let result = follower.wait_for("the JS node streams the Rust blob", |value| {
        value["t"] == "result"
    });
    assert_eq!(result["sha256"].as_str(), Some(expected.as_str()));
    assert_eq!(result["size"].as_u64(), Some(bytes.len() as u64));
    assert_eq!(result["httpStatus"].as_u64(), Some(200));
    assert!(
        result["lanPeers"].as_u64().unwrap_or(0) >= 1,
        "the JS node met us over LAN: {result}"
    );
    assert!(
        result["lanRxBytes"].as_u64().unwrap_or(0) >= bytes.len() as u64,
        "the blob traveled over the LAN route: {result}"
    );
    follower.expect_exit();

    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

// ── (c) Rust ↔ Rust ─────────────────────────────────────────────────────────

#[test]
#[ignore = "needs real mDNS on this machine's LAN interface"]
fn rust_to_rust_over_lan() {
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let publisher_storage = tempdir("peartube-lan-pub-");
    let follower_storage = tempdir("peartube-lan-sub-");
    let publisher = rust_node(&rt, host, &publisher_storage);
    let id = "imdb:tt0133093";
    let bytes: Vec<u8> = (0..700_003u32).map(|index| (index >> 3) as u8).collect();
    let expected = sha256_hex(&bytes);
    let published = publish(&publisher, &rt, id, &bytes);
    let channel = published["channel"].as_str().expect("channel").to_owned();

    let follower = rust_node(&rt, host, &follower_storage);
    rt.block_on(follower.add_source(
        peartube_native::node::api::SourceSpec::Object(peartube_native::node::api::SourceObject {
            uri: None,
            key: Some(channel.clone()),
            kind: Some("channel".into()),
            alias: None,
        }),
        true,
    ))
    .expect("follow the source");
    let entry = find_entry(&follower, &rt, id);
    assert_eq!(entry["sha256"].as_str(), Some(expected.as_str()));
    assert_eq!(entry["source"].as_str(), Some(channel.as_str()));
    let streamed = read_stream(&follower, &rt, &entry);
    assert_eq!(streamed.len(), bytes.len());
    assert_eq!(
        sha256_hex(&streamed),
        expected,
        "SHA-256 across two Rust nodes"
    );

    // The bytes had to use the LAN route: the isolated DHT has no other way to reach the peer.
    let stats = rt
        .block_on(follower.lan_stats())
        .unwrap_or_else(|| panic!("the follower has a LAN binding"));
    assert!(
        stats["connections"].as_u64().unwrap_or(0) >= 1,
        "the follower is connected over LAN: {stats}"
    );

    let cast_peer = peartube_native::cast::adapters::NodePeer::new(follower.clone());
    let status = peartube_native::cast::traits::CastPeer::status(&cast_peer);
    assert_eq!(
        status.peers, 0,
        "cast peers excludes its separately counted LAN peers"
    );
    assert_eq!(status.lan_peers, follower.net().connection_count());
    assert_eq!(status.connected_peers.len() as u64, status.lan_peers);
    assert!(
        status
            .connected_peers
            .iter()
            .all(|peer| peer["route"] == "lan" && peer["remoteAddress"].is_string())
    );

    rt.block_on(follower.close()).expect("close follower");
    rt.block_on(publisher.close()).expect("close publisher");
    let _ = std::fs::remove_dir_all(&publisher_storage);
    let _ = std::fs::remove_dir_all(&follower_storage);
}

// ── (d) the lan.js rules: token gating, smaller key first, redial ───────────

/// Connections each side accepted, plus the mux so the test can close one like a lost peer.
struct Watcher {
    count: AtomicUsize,
    muxes: Mutex<Vec<protomux::Mux>>,
}

impl Watcher {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            muxes: Mutex::new(Vec::new()),
        })
    }

    fn connections(&self) -> usize {
        self.count.load(Ordering::Acquire)
    }
}

fn watch(lan: &LanDiscovery, watcher: &Arc<Watcher>) {
    let watcher = watcher.clone();
    let _ = lan.on_connection(Arc::new(move |connection| {
        watcher
            .muxes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(connection.mux.clone());
        watcher.count.fetch_add(1, Ordering::AcqRel);
    }));
}

/// `_sharesTopic` and the smaller-key-dials-first rule: a peer that shares no token is never
/// dialled or accepted, and once a token is shared the connection lands inside `DIAL_AFTER`
/// (the larger key would have waited that long, so only the smaller key could have initiated).
#[test]
#[ignore = "needs real mDNS on this machine's LAN interface"]
fn lan_token_gating_and_dial_order() {
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let a = rt
        .block_on(LanDiscovery::open(host, KeyPair::generate()))
        .expect("first LAN node");
    let b = rt
        .block_on(LanDiscovery::open(host, KeyPair::generate()))
        .expect("second LAN node");
    let a_watch = Watcher::new();
    let b_watch = Watcher::new();
    watch(&a, &a_watch);
    watch(&b, &b_watch);

    // Random topics, so other nodes on the same LAN (and any concurrent run) never share them.
    let topic_a: [u8; 32] = rand::random();
    let topic_b: [u8; 32] = rand::random();
    let common: [u8; 32] = rand::random();
    assert_ne!(topic_a, common);
    assert_ne!(topic_b, common);
    a.join(topic_a);
    b.join(topic_b);
    until("both peers discovered", 60_000, || {
        let a_peers = a
            .stats()
            .and_then(|stats| stats["peers"].as_u64())
            .unwrap_or(0);
        let b_peers = b
            .stats()
            .and_then(|stats| stats["peers"].as_u64())
            .unwrap_or(0);
        (a_peers >= 1 && b_peers >= 1).then_some(())
    });
    std::thread::sleep(Duration::from_secs(4));
    assert_eq!(a_watch.connections(), 0, "no shared token, no dial (a)");
    assert_eq!(b_watch.connections(), 0, "no shared token, no dial (b)");

    let joined = Instant::now();
    a.join(common);
    b.join(common);
    until("a connection once a token is shared", 60_000, || {
        (a_watch.connections() >= 1 && b_watch.connections() >= 1).then_some(())
    });
    let elapsed = joined.elapsed();
    assert!(
        elapsed < DIAL_AFTER,
        "the smaller key dials first: connected in {elapsed:?}, not after {DIAL_AFTER:?}"
    );

    rt.block_on(a.destroy());
    rt.block_on(b.destroy());
}

/// `_onSocketClosed`: the smaller key redials after 500 ms, the larger key's 3 s timer then finds
/// the connection open and adds no duplicate. This waits on peeroxide's swarm forgetting a closed
/// connection (a redial currently stalls in `handle_server_handshake`'s already-connected
/// early-return, so the assertion is kept intact but only runs when asked).
#[test]
#[ignore = "needs peeroxide swarm close tracking (feat/peartube-api); set LAN_RECONNECT=1 once it merges"]
fn lan_reconnect_after_close() {
    if std::env::var_os("LAN_RECONNECT").is_none() {
        eprintln!("skipped: peeroxide does not forget closed swarm connections yet");
        return;
    }
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let a = rt
        .block_on(LanDiscovery::open(host, KeyPair::generate()))
        .expect("first LAN node");
    let b = rt
        .block_on(LanDiscovery::open(host, KeyPair::generate()))
        .expect("second LAN node");
    let a_watch = Watcher::new();
    let b_watch = Watcher::new();
    watch(&a, &a_watch);
    watch(&b, &b_watch);
    let common: [u8; 32] = rand::random();
    a.join(common);
    b.join(common);
    until("the first connection", 60_000, || {
        (a_watch.connections() >= 1 && b_watch.connections() >= 1).then_some(())
    });

    a_watch
        .muxes
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .first()
        .expect("the first accepted connection")
        .destroy();
    until("a redial after the close", 30_000, || {
        (a_watch.connections() >= 2 && b_watch.connections() >= 2).then_some(())
    });
    std::thread::sleep(DIAL_AFTER + Duration::from_secs(1));
    assert_eq!(a_watch.connections(), 2, "one redial, no duplicate (a)");
    assert_eq!(b_watch.connections(), 2, "one redial, no duplicate (b)");

    rt.block_on(a.destroy());
    rt.block_on(b.destroy());
}

// ── the topic the two nodes agree on, without mDNS ──────────────────────────

/// Records the topics a node hands its LAN binding; the intersection of two is what a dial needs.
#[derive(Default)]
struct Recorder {
    topics: Mutex<Vec<[u8; 32]>>,
}

impl Recorder {
    fn topics(&self) -> Vec<[u8; 32]> {
        self.topics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

impl LanBinding for Recorder {
    fn join(&self, topic: [u8; 32]) {
        let mut topics = self
            .topics
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !topics.contains(&topic) {
            topics.push(topic);
        }
    }
    fn leave(&self, topic: [u8; 32]) {
        self.topics
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|kept| *kept != topic);
    }
    fn destroy(&self) -> futures_util::future::BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn host(&self) -> Option<String> {
        Some("127.0.0.1".to_owned())
    }
    fn port(&self) -> Option<u16> {
        Some(0)
    }
    fn stats(&self) -> Option<Value> {
        None
    }
    fn dial_direct(&self, _public_key: &str, _host: &str, _port: u16) {}
    fn on_connection(
        &self,
        _callback: Arc<dyn Fn(peartube_native::node::network::LanConnection) + Send + Sync>,
    ) -> Box<dyn Fn() + Send + Sync> {
        Box::new(|| {})
    }
}

/// The publisher joins `discoveryKey(channel.key)` for its own channel and the follower joins
/// `discoveryKey(source key)` for the same channel; unless those are the same 32 bytes, mDNS
/// introduces the peers and neither side ever dials. No mDNS in this test: just the topic sets.
#[test]
fn follower_and_publisher_join_the_same_channel_topic() {
    let rt = runtime();
    let publisher_storage = tempdir("peartube-lan-topic-pub-");
    let follower_storage = tempdir("peartube-lan-topic-sub-");
    let node = |storage: &Path| {
        rt.block_on(create_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": [DEAD_DHT],
            "streamHost": "127.0.0.1",
            "streamPort": 0,
        })))
        .expect("rust node")
    };
    let publisher = node(&publisher_storage);
    let follower = node(&follower_storage);
    let publisher_topics = Arc::new(Recorder::default());
    let follower_topics = Arc::new(Recorder::default());
    rt.block_on(
        publisher
            .net()
            .set_lan(Some(publisher_topics.clone() as Arc<dyn LanBinding>)),
    )
    .expect("publisher binding");
    rt.block_on(
        follower
            .net()
            .set_lan(Some(follower_topics.clone() as Arc<dyn LanBinding>)),
    )
    .expect("follower binding");

    let bytes = vec![7u8; 1024];
    let published = publish(&publisher, &rt, "imdb:tt0000001", &bytes);
    let channel = published["channel"].as_str().expect("channel").to_owned();
    rt.block_on(follower.add_source(
        peartube_native::node::api::SourceSpec::Object(peartube_native::node::api::SourceObject {
            uri: None,
            key: Some(channel.clone()),
            kind: Some("channel".into()),
            alias: None,
        }),
        true,
    ))
    .expect("follow the source");

    let shared: Vec<String> = publisher_topics
        .topics()
        .into_iter()
        .filter(|topic| follower_topics.topics().contains(topic))
        .map(|topic| {
            topic
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        })
        .collect();
    assert!(
        !shared.is_empty(),
        "the peers share no LAN topic: publisher {:?}, follower {:?} (channel {channel})",
        publisher_topics.topics().len(),
        follower_topics.topics().len()
    );

    rt.block_on(follower.close()).expect("close follower");
    rt.block_on(publisher.close()).expect("close publisher");
    let _ = std::fs::remove_dir_all(&publisher_storage);
    let _ = std::fs::remove_dir_all(&follower_storage);
}

/// `lan.dialDirect(publicKey, host, port)`: the same data path mDNS starts, mounted by address so a
/// blocked multicast domain still replicates. This is also the only LAN test that pins the
/// channel-data path without depending on a browse resolve.
#[test]
#[ignore = "needs real mDNS on this machine's LAN interface"]
fn a_direct_dial_carries_the_channel() {
    let rt = runtime();
    let Some(host) = lan_host(&rt) else {
        eprintln!("skipped: this machine has no private Wi-Fi/Ethernet IPv4 for mDNS");
        return;
    };
    let publisher_storage = tempdir("peartube-lan-direct-pub-");
    let follower_storage = tempdir("peartube-lan-direct-sub-");
    let publisher = rust_node(&rt, host, &publisher_storage);
    let bytes: Vec<u8> = (0..409_601u32).map(|index| (index >> 5) as u8).collect();
    let expected = sha256_hex(&bytes);
    let published = publish(&publisher, &rt, "imdb:tt0106063", &bytes);
    let channel = published["channel"].as_str().expect("channel").to_owned();

    let follower = rust_node(&rt, host, &follower_storage);
    rt.block_on(follower.add_source(
        peartube_native::node::api::SourceSpec::Object(peartube_native::node::api::SourceObject {
            uri: None,
            key: Some(channel.clone()),
            kind: Some("channel".into()),
            alias: None,
        }),
        true,
    ))
    .expect("follow the source");
    let endpoint = rt
        .block_on(publisher.lan_endpoint())
        .expect("the publisher has a LAN endpoint");
    rt.block_on(follower.dial_lan_peer(endpoint));

    let entry = find_entry(&follower, &rt, "imdb:tt0106063");
    assert_eq!(entry["sha256"].as_str(), Some(expected.as_str()));
    let streamed = read_stream(&follower, &rt, &entry);
    assert_eq!(
        sha256_hex(&streamed),
        expected,
        "SHA-256 over a direct dial"
    );

    rt.block_on(follower.close()).expect("close follower");
    rt.block_on(publisher.close()).expect("close publisher");
    let _ = std::fs::remove_dir_all(&publisher_storage);
    let _ = std::fs::remove_dir_all(&follower_storage);
}
