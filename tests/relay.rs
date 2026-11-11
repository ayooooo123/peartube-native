//! The relay process (`bin/relay.js`'s port): a relay started without a tracker founds its own
//! channel and keeps it across restarts, with its API up on the configured address.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use peartube_native::cast::adapters::NodePeer;
use peartube_native::cast::traits::CastPeer;
use peartube_native::cast::{Receiver, ReceiverOptions};
use peartube_native::node::core::{CoreNode, create_node};
use peartube_native::relay::Relay;
use serde_json::Value;

fn get_json(addr: SocketAddr, path: &str) -> Value {
    request_json(addr, "GET", path, "")
}

fn request_json(addr: SocketAddr, method: &str, path: &str, body: &str) -> Value {
    let (status, answer) = try_request_json(addr, method, path, body);
    assert_eq!(status, 200, "{method} {path}: {answer}");
    answer
}

/// One request and its parsed body, whatever the status.
fn try_request_json(addr: SocketAddr, method: &str, path: &str, body: &str) -> (u16, Value) {
    let mut stream = TcpStream::connect(addr).expect("api listens");
    stream
        .set_read_timeout(Some(Duration::from_secs(40)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("a response");
    let (head, body) = raw.split_once("\r\n\r\n").expect("headers end");
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status");
    (status, serde_json::from_str(body).expect("JSON body"))
}

fn env(storage: &std::path::Path) -> HashMap<String, String> {
    env_on(storage, "127.0.0.1:9")
}

fn env_on(storage: &std::path::Path, bootstrap: &str) -> HashMap<String, String> {
    [
        ("PEARTUBE_STORAGE", storage.display().to_string()),
        ("PEARTUBE_API_HOST", "127.0.0.1".into()),
        ("PEARTUBE_API_PORT", "0".into()),
        ("PEARTUBE_STREAM_HOST", "127.0.0.1".into()),
        ("PEARTUBE_STREAM_PORT", "0".into()),
        ("PEARTUBE_HTTPS_PORT", "0".into()),
        ("PEARTUBE_BOOTSTRAP", bootstrap.to_owned()),
        ("PEARTUBE_LAN", "0".into()),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_owned(), v))
    .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_relay_founds_its_channel_and_keeps_it_across_restarts() {
    let dir = tempfile::tempdir().unwrap();

    let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
    let ready = relay.start().await.expect("node opens");
    let tracker = ready["tracker"].as_str().unwrap().to_owned();
    assert_eq!(
        ready["writer"], ready["tracker"],
        "a founded channel is written by this relay"
    );
    let addr = relay.addresses()[0];
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/status"))
        .await
        .unwrap();
    assert_eq!(status["tracker"], tracker.as_str());
    assert_eq!(status["writable"], true);
    relay.shutdown().await;

    let settings: Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("settings.json")).unwrap())
            .unwrap();
    assert_eq!(
        settings["channel"],
        tracker.as_str(),
        "the founded channel is saved"
    );

    let again = Relay::listen(env(dir.path()))
        .await
        .expect("relay listens again");
    let reopened = again.start().await.expect("node reopens");
    assert_eq!(
        reopened["tracker"],
        tracker.as_str(),
        "a restart keeps the same channel"
    );
    again.shutdown().await;
}

/// A live setting applies in place and the request returns: applying reads the settings the
/// request just saved.
#[tokio::test(flavor = "multi_thread")]
async fn a_live_setting_applies_without_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let relay = Relay::listen(env(dir.path())).await.expect("relay listens");
    let ready = relay.start().await.expect("node opens");
    let addr = relay.addresses()[0];
    let updated = tokio::task::spawn_blocking(move || {
        request_json(addr, "PUT", "/v1/settings", r#"{"acquireConcurrency":2}"#)
    })
    .await
    .unwrap();
    assert_eq!(
        updated["changed"],
        serde_json::json!(["acquireConcurrency"])
    );
    assert_eq!(updated["warning"], Value::Null);
    assert_eq!(
        relay.status().await.expect("still open")["writer"],
        ready["writer"]
    );
    relay.shutdown().await;
}

// ---------------------------------------------------------------- cast routes

const DEVICE_ID: &str = "0123456789abcdef0123456789abcdef";
const SEED: &str = "abababababababababababababababababababababababababababababababab";
const PAIR_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
const PAIR_CODE: &str = "ABC123";

/// One DHT node both peers bootstrap from.
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
        bootstrap: serde_json::json!([format!("127.0.0.1:{port}")]),
    }
}

/// A receiver identity the test can pair with: fixed code, token and device id.
fn seed_identity(dir: &std::path::Path) {
    std::fs::create_dir_all(dir).expect("cast dir");
    let identity = serde_json::json!({
        "deviceId": DEVICE_ID,
        "name": "Kitchen TV",
        "seed": SEED,
        "pairToken": PAIR_TOKEN,
        "pairCode": PAIR_CODE,
    });
    std::fs::write(
        peartube_native::cast::pairing::ReceiverIdentity::file(dir),
        serde_json::to_string_pretty(&identity).expect("json"),
    )
    .expect("identity");
}

async fn receiver_on(bootstrap: &Value, dir: &std::path::Path) -> Arc<CoreNode> {
    let key_pair =
        peartube_native::cast::receiver::receiver_key_pair_json(dir).expect("key pair");
    create_node(serde_json::json!({
        "storage": dir.join("peer").to_string_lossy(),
        "bootstrap": bootstrap,
        "keyPair": key_pair,
        "storageWal": false,
        "storageMemory": true
    }))
    .await
    .expect("receiver node")
}

/// `/v1/cast/status` answers from the relay's live controller, and `/v1/cast/pair` reaches a
/// receiver on the network and saves the pairing it hands back.
#[test]
fn the_cast_routes_reach_a_receiver() {
    let testnet = testnet();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let dir = tempfile::tempdir().unwrap();
        cast_routes(&testnet.bootstrap, dir.path()).await;
    });
}

async fn cast_routes(bootstrap: &Value, dir: &std::path::Path) {
    let cast_dir = dir.join("tv");
    seed_identity(&cast_dir);
    let node = receiver_on(bootstrap, &cast_dir).await;
    let receiver = Receiver::start(ReceiverOptions {
        storage: cast_dir.clone(),
        peer_storage: Some(cast_dir.join("peer")),
        name: "Kitchen TV".to_owned(),
        port: 0,
        lan_host: None,
        peer: Some(Arc::new(NodePeer::new(node.clone())) as Arc<dyn CastPeer>),
        mdns: false,
        open_auth: false,
        any_port: true,
        on_command: None,
        on_change: None,
    })
    .await
    .expect("receiver starts");
    let receiver_key = receiver.public_key();

    let bootstrap_host = bootstrap[0].as_str().expect("bootstrap host").to_owned();
    let relay = Relay::listen(env_on(dir, &bootstrap_host))
        .await
        .expect("relay listens");
    relay.start().await.expect("node opens");
    let addr = relay.addresses()[0];

    // A live controller answers the status route: the relay's cast stack is open with its node.
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/cast/status"))
        .await
        .unwrap();
    assert!(status["receivers"].is_array(), "{status}");
    assert_eq!(status["session"], Value::Null, "{status}");
    assert_eq!(status["receiverMode"], Value::Null, "{status}");

    // The pair reaches the receiver itself: its code approves and its token comes back.
    let body = format!(r#"{{"publicKey":"{receiver_key}","code":"{PAIR_CODE}"}}"#);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(150);
    let paired = loop {
        let attempt = tokio::task::spawn_blocking({
            let body = body.clone();
            move || try_request_json(addr, "POST", "/v1/cast/pair", &body)
        })
        .await
        .unwrap();
        if attempt.0 == 200 {
            break attempt.1;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "pair: {} {}",
            attempt.0,
            attempt.1
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    };
    assert_eq!(paired["publicKey"], receiver_key.as_str());
    assert_eq!(paired["token"], PAIR_TOKEN);
    assert_eq!(paired["id"], DEVICE_ID);
    assert_eq!(paired["name"], "Kitchen TV");

    // The saved pairing is what the status route lists.
    let status = tokio::task::spawn_blocking(move || get_json(addr, "/v1/cast/status"))
        .await
        .unwrap();
    assert!(
        status["receivers"]
            .as_array()
            .expect("receivers")
            .iter()
            .any(|row| row["publicKey"] == receiver_key.as_str()),
        "{status}"
    );

    // A stop closes the cast stack with the node; the route is the app's 404 until it reopens.
    relay.shutdown().await;
    receiver.close().await;
    let _ = node.close().await;
    let _ = std::fs::remove_dir_all(dir);
}

// ------------------------------------------------------------------------------- guard mode

/// `PEARTUBE_PRIVATE_RELAY`: a standalone guard forwarder with no Corestore and no storage folder,
/// refusing every channel or content flag, as `bin/relay.js` does.
#[tokio::test(flavor = "multi_thread")]
async fn guard_mode_runs_a_guard_writes_no_storage_and_refuses_content_flags() {
    use peartube_native::private_overlay::dht::DhtOptions;
    use peartube_native::private_overlay::peeroxide::open_dht;

    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join("data");
    // A bootstrap node of its own, so the guard's announce completes without the public DHT.
    let bootstrap = open_dht(DhtOptions {
        host: Some("127.0.0.1".into()),
        port: 0,
        bootstrap: Vec::new(),
    })
    .await
    .expect("bootstrap dht");
    let bootstrap = bootstrap
        .address()
        .map(|addr| format!("127.0.0.1:{}", addr.port()))
        .unwrap_or_else(|| "127.0.0.1:9".into());

    let mut env: HashMap<String, String> = HashMap::new();
    env.insert("PEARTUBE_PRIVATE_RELAY".into(), "1".into());
    env.insert("PEARTUBE_PRIVATE_RELAY_PORT".into(), "0".into());
    env.insert("PEARTUBE_STORAGE".into(), storage.display().to_string());
    env.insert("PEARTUBE_BOOTSTRAP".into(), bootstrap);
    env.insert("PEARTUBE_GUARD_SEED".into(), "11".repeat(32));
    assert!(peartube_native::relay::guard_mode(&env));

    let guard = tokio::time::timeout(
        Duration::from_secs(120),
        peartube_native::relay::start_standalone_guard(&env),
    )
    .await
    .expect("the guard starts")
    .expect("the guard is up");
    let ready = guard.ready();
    let expected = peartube_native::private_overlay::to_hex(
        &hypercore::crypto::KeyPair::from_seed(&[0x11; 32]).public_key,
    );
    assert_eq!(ready["publicKey"], expected.as_str(), "the seeded guard key");
    assert_eq!(ready["host"], "127.0.0.1");
    assert!(ready["port"].as_u64().unwrap_or(0) > 0, "{ready}");
    assert!(
        !storage.exists(),
        "guard mode creates no storage folder: {ready}"
    );
    // The heartbeat line is the ready line's key plus `guard.stats()`, and nothing else: JS
    // spreads `guard.stats()` there, which carries no port.
    let heartbeat = guard.heartbeat();
    let mut keys: Vec<&str> = heartbeat
        .as_object()
        .expect("a heartbeat object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "activeCircuits",
            "publicKey",
            "putMutableCount",
            "registeredRoutes",
            "rendezvousTopics",
            "rvRegisterCount",
        ],
        "{heartbeat}"
    );
    guard.close().await;
    assert!(!storage.exists(), "still nothing written on close");

    env.insert("PEARTUBE_CHANNEL".into(), "ab".repeat(32));
    let error = peartube_native::relay::start_standalone_guard(&env)
        .await
        .err()
        .expect("a channel flag is refused");
    assert_eq!(
        error,
        "PEARTUBE_PRIVATE_RELAY runs a standalone Corestore-free guard forwarder and cannot be combined with channel or content flags; run peartube-private-relay separately"
    );
    assert!(!storage.exists());

    // An empty variable is unset, as JS truthiness has it.
    let mut empty: HashMap<String, String> = HashMap::new();
    empty.insert("PEARTUBE_PRIVATE_RELAY".into(), String::new());
    assert!(!peartube_native::relay::guard_mode(&empty));
    empty.insert("PEARTUBE_PRIVATE_RELAY_PORT".into(), "0".into());
    assert!(peartube_native::relay::guard_mode(&empty));
}

/// `PEARTUBE_GUARD_FOLLOW_PARENT`: a guard whose parent dies exits on its own, as
/// `private-relay.js`'s 2 s parent watch does (it calls `shutdown()`, which closes the guard and
/// exits). Without it the process outlived the relay that started it until it was signalled.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_following_guard_leaves_when_its_parent_dies() {
    use peartube_native::private_overlay::dht::DhtOptions;
    use peartube_native::private_overlay::peeroxide::open_dht;
    use std::process::{Command, Stdio};

    // A bootstrap node of its own, so the guard's announce completes without the public DHT.
    let bootstrap = open_dht(DhtOptions {
        host: Some("127.0.0.1".into()),
        port: 0,
        bootstrap: Vec::new(),
    })
    .await
    .expect("bootstrap dht");
    let bootstrap = bootstrap
        .address()
        .map(|addr| format!("127.0.0.1:{}", addr.port()))
        .expect("bootstrap address");
    let storage = tempfile::tempdir().unwrap();

    // `$0` is the relay binary, started in the background so its parent is this short-lived
    // shell rather than the test process. The shell outlives the guard's startup (a debug build
    // takes a moment to reach `main`), then leaves without stopping it.
    let mut parent = Command::new("sh")
        .arg("-c")
        .arg("\"$0\" & echo pid=$!; sleep 4")
        .arg(env!("CARGO_BIN_EXE_peartube-relay"))
        .env("PEARTUBE_PRIVATE_RELAY", "1")
        .env("PEARTUBE_PRIVATE_RELAY_PORT", "0")
        .env("PEARTUBE_GUARD_FOLLOW_PARENT", "1")
        .env("PEARTUBE_PRIVATE_HOST", "127.0.0.1")
        .env("PEARTUBE_BOOTSTRAP", &bootstrap)
        .env("PEARTUBE_GUARD_SEED", "22".repeat(32))
        .env("PEARTUBE_STORAGE", storage.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("parent shell");
    let guard_pid = parent
        .stdout
        .take()
        .map(|stdout| {
            // The guard inherits the pipe, so it stays open until the guard itself exits.
            let (sender, receiver) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let mut text = String::new();
                let mut stdout = std::io::BufReader::new(stdout);
                for line in std::io::BufRead::lines(&mut stdout).map_while(Result::ok) {
                    eprintln!("guard: {line}");
                    text.push_str(&line);
                    text.push('\n');
                }
                let _ = sender.send(text);
            });
            receiver
        })
        .expect("stdout");
    parent.wait().expect("the parent shell exits");
    let started = std::time::Instant::now();
    let output = guard_pid
        .recv_timeout(Duration::from_secs(30))
        .expect("the guard exits after its parent, not on a signal");
    let elapsed = started.elapsed();

    let pid: i32 = output
        .lines()
        .find_map(|line| line.strip_prefix("pid="))
        .and_then(|pid| pid.trim().parse().ok())
        .expect("the guard's pid");
    // Whatever happened, never leave the process behind.
    let _ = Command::new("kill")
        .arg("-9")
        .arg(pid.to_string())
        .stderr(Stdio::null())
        .status();
    assert!(
        output.contains("\"private-guard ready\""),
        "the guard started: {output}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "the guard left right after its parent: {elapsed:?}"
    );
}
