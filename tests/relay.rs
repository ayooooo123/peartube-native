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
        settings["tracker"],
        tracker.as_str(),
        "the founded tracker is saved"
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
