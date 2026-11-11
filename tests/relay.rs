//! The relay process (`bin/relay.js`'s port): a relay started without a tracker founds its own
//! channel and keeps it across restarts, with its API up on the configured address.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use peartube_native::relay::Relay;
use serde_json::Value;

fn get_json(addr: SocketAddr, path: &str) -> Value {
    request_json(addr, "GET", path, "")
}

fn request_json(addr: SocketAddr, method: &str, path: &str, body: &str) -> Value {
    let mut stream = TcpStream::connect(addr).expect("api listens");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
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
    assert!(head.starts_with("HTTP/1.1 200"), "{method} {path}: {head}");
    serde_json::from_str(body).expect("JSON body")
}

fn env(storage: &std::path::Path) -> HashMap<String, String> {
    [
        ("PEARTUBE_STORAGE", storage.display().to_string()),
        ("PEARTUBE_API_HOST", "127.0.0.1".into()),
        ("PEARTUBE_API_PORT", "0".into()),
        ("PEARTUBE_STREAM_HOST", "127.0.0.1".into()),
        ("PEARTUBE_STREAM_PORT", "0".into()),
        ("PEARTUBE_HTTPS_PORT", "0".into()),
        ("PEARTUBE_BOOTSTRAP", "127.0.0.1:9".into()),
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
    assert!(guard.heartbeat().get("registeredRoutes").is_some());
    guard.close().await;
    assert!(!storage.exists(), "still nothing written on close");

    env.insert("PEARTUBE_CHANNEL".into(), "ab".repeat(32));
    let error = peartube_native::relay::start_standalone_guard(&env)
        .await
        .err()
        .expect("a channel flag is refused");
    assert!(
        error.contains("cannot be combined with channel or content flags"),
        "{error}"
    );
    assert!(!storage.exists());

    // An empty variable is unset, as JS truthiness has it.
    let mut empty: HashMap<String, String> = HashMap::new();
    empty.insert("PEARTUBE_PRIVATE_RELAY".into(), String::new());
    assert!(!peartube_native::relay::guard_mode(&empty));
    empty.insert("PEARTUBE_PRIVATE_RELAY_PORT".into(), "0".into());
    assert!(peartube_native::relay::guard_mode(&empty));
}
