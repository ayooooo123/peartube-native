//! The default adapters: a bare `Backend::spawn` must behave like `worker.js` — `start` opens the
//! real node behind a loopback API, LAN discovery reports a note, and shutdown closes every port.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

use peartube_native::backend::Backend;
use peartube_native::lan::{KeyPair, LanDiscovery};
use peartube_native::node::network::LanBinding;
use serde_json::{Value, json};

/// Reads the next response line for `id`, skipping events and other replies.
fn wait_for(lines: &Receiver<String>, id: u64) -> Value {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        assert!(!remaining.is_zero(), "no response for id {id}");
        let line = lines
            .recv_timeout(remaining)
            .unwrap_or_else(|_| panic!("no response for id {id}"));
        let value: Value = serde_json::from_str(line.trim()).expect("a JSON line");
        if value.get("id").and_then(Value::as_u64) == Some(id) {
            return value;
        }
    }
}

fn get(url: &str) -> (u16, String) {
    let rest = url.strip_prefix("http://").expect("http url");
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let mut stream = TcpStream::connect(authority).expect("api listens");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("a response");
    let (head, body) = raw.split_once("\r\n\r\n").expect("headers end");
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("status code");
    (status, body.to_owned())
}

#[test]
fn default_spawn_opens_the_loopback_api_and_lan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let trackers = dir.path().join("trackers");
    let cast_dir = dir.path().join("cast");
    std::fs::create_dir_all(&trackers).unwrap();
    std::fs::create_dir_all(&cast_dir).unwrap();

    let (tx, lines) = mpsc::channel();
    // `apiPort: 0` keeps the loopback listener off the app's fixed port, so the shutdown
    // assertion cannot race another process rebinding it.
    let backend = Backend::spawn(json!({ "apiPort": 0 }), move |line| {
        let _ = tx.send(line);
    })
    .expect("backend starts");

    backend
        .send(
            &json!({
                "id": 1,
                "method": "init",
                "params": { "castStorage": cast_dir.to_string_lossy() },
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
                        "sources": [],
                        "relayThrough": [],
                        // An empty bootstrap is the isolated DHT: no public network in a test.
                        "bootstrap": [],
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
    let result = started.get("result").unwrap_or_else(|| panic!("{started}"));
    let api_url = result["apiUrl"]
        .as_str()
        .unwrap_or_else(|| panic!("no apiUrl in {result}"))
        .to_owned();
    assert!(api_url.starts_with("http://127.0.0.1:"), "{api_url}");
    assert!(
        result.get("lan").and_then(Value::as_str).is_some(),
        "LAN discovery reports a note: {result}"
    );
    assert_eq!(result["peers"], json!(0));

    // The loopback API serves the node's status, and says where it lives.
    let (status, body) = get(&format!("{api_url}/v1/status"));
    assert_eq!(status, 200, "{body}");
    let status: Value = serde_json::from_str(&body).expect("status JSON");
    assert_eq!(status["apiUrl"], json!(api_url));
    assert_eq!(status["tokenRequired"], json!(false));
    assert!(status["writer"].is_string(), "{status}");

    // The cast controller comes up from the same storage `init` provided.
    backend
        .send(&json!({ "id": 3, "method": "castStatus", "params": {} }).to_string())
        .unwrap();
    let cast = wait_for(&lines, 3);
    let cast = cast.get("result").unwrap_or_else(|| panic!("{cast}"));
    assert!(
        cast["receivers"].is_array(),
        "the cast controller reports its receivers: {cast}"
    );

    // Shutdown closes the node, the cast controller and every listener it opened.
    let address: std::net::SocketAddr = api_url
        .trim_start_matches("http://")
        .parse()
        .expect("socket address");
    backend.shutdown();
    assert!(backend.send("{}").is_err(), "the worker thread is gone");
    assert!(
        TcpStream::connect_timeout(&address, Duration::from_secs(2)).is_err(),
        "the loopback API port is closed"
    );
}

/// The isolated LAN half: an unannounced DHT on the host address, mDNS advertising its tokens.
#[tokio::test]
async fn lan_binding_opens_and_reports() {
    let lan = LanDiscovery::open(std::net::Ipv4Addr::LOCALHOST, KeyPair::generate())
        .await
        .expect("the isolated DHT opens");
    assert_eq!(lan.host().as_deref(), Some("127.0.0.1"));
    assert!(lan.port().unwrap_or(0) > 0, "the DHT bound a port");
    lan.join([7u8; 32]);
    assert_eq!(lan.stats().expect("stats")["topics"], json!(1));
    lan.destroy().await;
}
