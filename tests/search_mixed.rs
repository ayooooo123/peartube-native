//! Global search (`search`, the app's `src/search.js`) between the real JS node and this port on
//! one local HyperDHT testnet, both ways: a JS relay answers a Rust searcher, and a Rust relay
//! answers a JS searcher. Each searcher follows nothing, finds the film through the search topic,
//! and plays exact bytes from the relay that answered. The Rust searcher also finds a file the
//! relay got after a search that missed it, by asking the same search again.
//!
//! Needs `node`, the app checkout (`PEARTUBE_APP`, else the sibling `../peartube`) and its installed
//! modules; `#[ignore]` so `cargo test` stays offline.

use std::io::{BufRead, BufReader, Lines, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use peartube_native::node::Node;
use peartube_native::node::api::OpenUri;

const HARNESS: &str = "tests/js/search-mixed.mjs";

fn app_dir() -> String {
    std::env::var("PEARTUBE_APP").unwrap_or_else(|_| {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../peartube")
            .to_string_lossy()
            .into_owned()
    })
}

fn has_app() -> bool {
    std::path::Path::new(&app_dir())
        .join("src/search.js")
        .exists()
}

struct Harness {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Lines<BufReader<ChildStdout>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_js(mode: &str, args: &[String]) -> Harness {
    let mut child = Command::new("node")
        .arg(HARNESS)
        .arg(mode)
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env("PEARTUBE_APP", app_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("node");
    let stdin = child.stdin.take();
    let stdout = BufReader::new(child.stdout.take().expect("stdout")).lines();
    Harness {
        child,
        stdin,
        stdout,
    }
}

impl Harness {
    /// The harness's next JSON line.
    fn result(&mut self) -> Value {
        let line = self
            .stdout
            .next()
            .expect("harness printed nothing")
            .expect("read line");
        serde_json::from_str(&line).expect("harness json")
    }

    fn send(&mut self, command: &str) {
        let stdin = self.stdin.as_mut().expect("stdin");
        writeln!(stdin, "{command}").expect("write command");
        stdin.flush().expect("flush");
    }
}

fn tempdir() -> std::path::PathBuf {
    tempfile::Builder::new()
        .prefix("peartube-search-mixed-")
        .tempdir()
        .expect("tempdir")
        .keep()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn v2_search_discovery_vector() {
    let hex = peartube_native::private_overlay::to_hex(&*peartube_native::search::SEARCH_TOPIC);
    assert_eq!(
        hex,
        "4646a5cb08962943aef4a2e9d12a1ed5f150742b9aadf41389c8d4a894fefa6c"
    );
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

async fn rust_node(
    storage: &std::path::Path,
    bootstrap: &Value,
) -> Arc<peartube_native::node::core::CoreNode> {
    peartube_native::node::core::create_node(json!({
        "storage": storage.to_string_lossy(),
        "bootstrap": bootstrap,
    }))
    .await
    .expect("rust node")
}

/// A JS relay answers this port's search: the film, never its non-media file; the played link
/// reads exact bytes.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_relay_answers_a_rust_searcher() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to an app checkout with src/search.js");
        return;
    }
    let runtime = runtime();
    let mut testnet = spawn_js("testnet", &[]);
    let bootstrap = testnet.result()["bootstrap"].clone();
    let mut relay = spawn_js("relay", &[bootstrap.to_string()]);
    let film = relay.result();

    let storage = tempdir();
    let node = runtime.block_on(rust_node(&storage, &bootstrap));
    let end = Instant::now() + Duration::from_secs(120);
    let rows = loop {
        let rows = runtime
            .block_on(node.search_global("1979 stalker", None, 20))
            .expect("search");
        if !rows.is_empty() || Instant::now() > end {
            break rows;
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    let ids: Vec<&str> = rows.iter().filter_map(|row| row["id"].as_str()).collect();
    assert_eq!(
        ids,
        vec![film["id"].as_str().unwrap()],
        "only the checked film: {rows:?}"
    );
    let magnet = rows[0]["magnetUrl"].as_str().expect("magnet");
    assert!(!magnet.contains("source="), "a hit names no channel");
    let OpenUri::Watch { entry } = runtime
        .block_on(node.open_uri(magnet, false))
        .expect("open")
    else {
        panic!("a watch link");
    };
    let mut fetch = spawn_js("fetch", std::slice::from_ref(&entry.stream_url));
    assert_eq!(
        fetch.result()["sha256"],
        film["sha256"],
        "the played hit is the exact film"
    );

    // A file the relay gets after a search that missed it shows up when the same search is
    // asked again.
    let none = runtime
        .block_on(node.search_global("solaris", None, 20))
        .expect("search");
    assert!(none.is_empty(), "nothing matches yet: {none:?}");
    relay.send("publish");
    let late_film = relay.result();
    let end = Instant::now() + Duration::from_secs(60);
    let late = loop {
        let rows = runtime
            .block_on(node.search_global("solaris", None, 20))
            .expect("search");
        if !rows.is_empty() || Instant::now() > end {
            break rows;
        }
        std::thread::sleep(Duration::from_millis(500));
    };
    let ids: Vec<&str> = late.iter().filter_map(|row| row["id"].as_str()).collect();
    assert_eq!(
        ids,
        vec![late_film["id"].as_str().unwrap()],
        "the same search finds the file published after it: {late:?}"
    );
    runtime.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = &mut testnet;
}

/// This port's relay answers a JS searcher, and the JS searcher plays exact bytes from it.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_relay_answers_a_js_searcher() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to an app checkout with src/search.js");
        return;
    }
    let runtime = runtime();
    let mut testnet = spawn_js("testnet", &[]);
    let bootstrap = testnet.result()["bootstrap"].clone();

    let storage = tempdir();
    let node = runtime.block_on(rust_node(&storage, &bootstrap));
    let mut film = vec![0u8; 1024 * 1024 + 13];
    for (index, byte) in film.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 7) as u8;
    }
    film[..4].copy_from_slice(&[0x1a, 0x45, 0xdf, 0xa3]);
    let published = runtime
        .block_on(node.publish(
            peartube_native::node::api::PutMeta {
                id: "imdb:tt0079944".into(),
                title: "Stalker (1979)".into(),
                private_entry: None,
                epoch: None,
                store: None,
                expected_size: Some(Some(film.len() as u64)),
                extra: serde_json::Map::new(),
            },
            peartube_native::node::core::NodeSource::bytes(film.clone()),
        ))
        .expect("publish");
    let key = format!(
        "{}:{}",
        published.announce.blobs, published.announce.blob.block_offset
    );
    // The media check is the relay plugin's job (`media_check`); this test checks the wire.
    let first = runtime
        .block_on(node.first_block(
            &published.announce.blobs,
            published.announce.blob.block_offset,
        ))
        .expect("first block on disk");
    assert_eq!(peartube_native::protocol::media_format(&first), Some("mkv"));
    // Pairing and the topic join spawn on the node's runtime.
    let _enter = runtime.enter();
    node.serve_search(Some(Arc::new(move |held: &str| held == key)));

    let mut searcher = spawn_js("search", &[bootstrap.to_string(), "stalker 1979".into()]);
    let result = searcher.result();
    let rows = result["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1, "one row: {rows:?}");
    assert_eq!(rows[0]["id"], "imdb:tt0079944");
    assert_eq!(
        rows[0]["sha256"].as_str().unwrap(),
        published.announce.sha256
    );
    assert_eq!(
        result["sha256"].as_str().unwrap(),
        sha256_hex(&film),
        "the JS searcher played exact bytes"
    );
    runtime.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = &mut testnet;
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn global_search_request_limits_match_js() {
    let dir = tempfile::tempdir().unwrap();
    let mut testnet = spawn_js("testnet", &[]);
    let bootstrap = testnet.result()["bootstrap"].clone();
    let mut catalog = spawn_js(
        "limit-catalog",
        &[bootstrap.to_string(), dir.path().display().to_string()],
    );
    let reference = catalog.result();
    let expected = json!([20, 20, 20, 4, 1, 4, 2, 28]);
    assert_eq!(reference["node"], expected);
    assert_eq!(reference["http"], expected);

    let (send, replies) = std::sync::mpsc::channel();
    let backend = peartube_native::backend::Backend::spawn(json!({ "apiPort": 0 }), move |line| {
        let _ = send.send(line);
    })
    .unwrap();
    let call = |method: &str, params: &Value| {
        backend
            .send(&json!({ "id": 1, "method": method, "params": params }).to_string())
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let line = replies
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("worker reply");
            let reply: Value = serde_json::from_str(&line).unwrap();
            if reply["id"] == 1 {
                break reply;
            }
        }
    };
    let init = call("init", &json!({ "castStorage": dir.path().join("cast") }));
    assert_eq!(init["result"]["ok"], true, "{init}");
    let start = call(
        "start",
        &json!({
            "trackersDir": dir.path().join("trackers"), "defaultFolder": "default",
            "settings": { "bootstrap": bootstrap, "lanDiscovery": false, "privateRouting": false }
        }),
    );
    let url = start["result"]["apiUrl"].as_str().expect("worker API URL");
    // Prove that both relays answered before a small count can pass as a search limit.
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let reply = call(
            "searchGlobal",
            &json!({ "query": "catalogue", "limit": 80 }),
        );
        if reply["result"].as_array().expect("search results").len() == 28 {
            break;
        }
        assert!(Instant::now() < deadline, "worker did not find both relays");
        std::thread::sleep(Duration::from_millis(100));
    }
    let worker: Vec<Value> = reference["requests"]
        .as_array()
        .unwrap()
        .iter()
        .map(|params| {
            let reply = call("searchGlobal", params);
            match reply["result"].as_array() {
                Some(rows) => json!(rows.len()),
                None => reply["error"].clone(),
            }
        })
        .collect();
    catalog.send(url);
    let http = catalog.result();
    backend.shutdown();
    eprintln!(
        "JS: {expected}; Rust worker: {}; Rust HTTP: {http}",
        json!(worker)
    );
    assert_eq!(
        (json!(worker), http),
        (reference["node"].clone(), reference["http"].clone())
    );
}
