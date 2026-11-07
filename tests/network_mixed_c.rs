//! Slice C of the app's network scenarios, run against a real JS PearTube node on a local
//! HyperDHT testnet: private-routing fail-closed (8), clearCache disk shrink (9) and streaming
//! evict-behind (10), each in both role directions where the roles differ.
//!
//! Needs `node`, the app checkout (`PEARTUBE_APP`, else `~/projects/peartube`) and its installed
//! modules; `#[ignore]` so `cargo test` stays offline.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use peartube_native::node::Node;
use peartube_native::node::api::{Blob, Entry, PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, create_node};

const HARNESS: &str = "tests/js/network-mixed.mjs";

fn app_dir() -> String {
    std::env::var("PEARTUBE_APP").unwrap_or_else(|_| {
        let home = std::env::var("HOME").unwrap_or_default();
        format!("{home}/projects/peartube")
    })
}

fn has_app() -> bool {
    std::path::Path::new(&app_dir())
        .join("src/index.js")
        .exists()
}

// ---------------------------------------------------------------- JS process

/// One harness process: every JS node of a scenario, on its own testnet, over a JSON line protocol.
struct Js {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: std::io::Lines<BufReader<ChildStdout>>,
    next_id: u64,
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
            .arg(HARNESS)
            .arg("serve")
            .arg(size.to_string())
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .env("PEARTUBE_APP", app_dir())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("node");
        let stdin = child.stdin.take();
        let stdout = child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let first = lines
            .next()
            .expect("harness printed nothing")
            .expect("line");
        let startup: Value = serde_json::from_str(&first).expect("harness startup json");
        Self {
            child,
            stdin,
            lines,
            next_id: 0,
            bootstrap: startup["bootstrap"].clone(),
        }
    }

    fn bootstrap(&self) -> Value {
        self.bootstrap.clone()
    }

    fn call(&mut self, op: &str, args: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let mut command = Map::new();
        // The envelope is `msgId`: `publish`/`search` pass an entry `id` of their own.
        command.insert("msgId".into(), json!(id));
        command.insert("op".into(), json!(op));
        if let Value::Object(fields) = args {
            for (key, value) in fields {
                command.insert(key, value);
            }
        }
        let line = Value::Object(command).to_string();
        step(&format!("-> {op} {line}"));
        self.stdin
            .as_mut()
            .expect("harness stdin")
            .write_all(format!("{line}\n").as_bytes())
            .expect("harness stdin write");
        loop {
            let line = self
                .lines
                .next()
                .unwrap_or_else(|| panic!("harness died during {op}"))
                .expect("harness line");
            let reply: Value = serde_json::from_str(&line)
                .unwrap_or_else(|_| panic!("harness printed a non-JSON line during {op}: {line}"));
            if reply["msgId"].as_u64() != Some(id) {
                continue;
            }
            assert!(
                reply["ok"].as_bool().unwrap_or(false),
                "{op} failed: {}",
                reply["error"]
            );
            return reply["result"].clone();
        }
    }
}

// ---------------------------------------------------------------- small helpers

fn tempdir(label: &str) -> PathBuf {
    tempfile::Builder::new()
        .prefix(&format!("peartube-mixed-{label}-"))
        .tempdir()
        .expect("tempdir")
        .keep()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Progress for a mixed run too slow to read from a bare pass/fail: `MIXED_TRACE=1` prints each step.
fn step(label: &str) {
    if std::env::var_os("MIXED_TRACE").is_some() {
        eprintln!("[mixed-c] {label}");
    }
}

fn until<T>(mut check: impl FnMut() -> Option<T>, ms: u64) -> Option<T> {
    let end = Instant::now() + Duration::from_millis(ms);
    loop {
        if let Some(value) = check() {
            return Some(value);
        }
        if Instant::now() > end {
            return None;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .expect("runtime")
}

fn rust_node(rt: &tokio::runtime::Runtime, options: Value) -> Arc<CoreNode> {
    rt.block_on(create_node(options)).expect("rust node")
}

fn search(rt: &tokio::runtime::Runtime, node: &CoreNode, id: Option<&str>) -> Vec<Entry> {
    rt.block_on(async { node.search(id, SearchOptions::default()).await })
        .unwrap_or_default()
}

fn search_until(rt: &tokio::runtime::Runtime, node: &CoreNode, id: &str, ms: u64) -> Option<Entry> {
    until(
        || {
            search(rt, node, Some(id))
                .into_iter()
                .find(|entry| entry.id == id)
        },
        ms,
    )
}

fn no_entry(rt: &tokio::runtime::Runtime, node: &CoreNode, id: &str, ms: u64) -> bool {
    until(
        || {
            if search(rt, node, Some(id)).is_empty() {
                Some(())
            } else {
                std::thread::sleep(Duration::from_millis(500));
                None
            }
        },
        ms,
    )
    .is_some()
}

/// GET a stream URL, optionally with a Range header; returns the status and the body.
fn fetch(rt: &tokio::runtime::Runtime, url: &str, range: Option<(u64, u64)>) -> (u16, Vec<u8>) {
    rt.block_on(async {
        tokio::time::timeout(Duration::from_secs(180), async {
            let mut builder = hyper::Request::builder().uri(url);
            if let Some((start, end)) = range {
                builder = builder.header("range", format!("bytes={start}-{end}"));
            }
            let request = builder
                .body(http_body_util::Empty::<bytes::Bytes>::new())
                .expect("request");
            let client =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build_http();
            let response = client.request(request).await.expect("stream request");
            let status = response.status().as_u16();
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .expect("body")
                .to_bytes()
                .to_vec();
            (status, body)
        })
        .await
        .unwrap_or_else(|_| panic!("stream request for {url} did not finish"))
    })
}

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                dir_bytes(&path)
            } else {
                std::fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0)
            }
        })
        .sum()
}

fn put_meta(id: &str, title: &str) -> PutMeta {
    PutMeta {
        id: id.into(),
        title: title.into(),
        private_entry: None,
        epoch: None,
        store: None,
        expected_size: None,
        extra: Map::new(),
    }
}

// ---------------------------------------------------------------- 8. private routing

/// Without the private overlay this build does not ship, a private-routed node must fail closed:
/// no public or LAN join, no private route, and it says so in `status()`.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_private_routing_is_fail_closed_without_the_overlay() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    js.call(
        "publish",
        json!({"name": "a", "id": "imdb:tt0133093", "title": "Private Matrix", "size": 64 * 1024}),
    );
    let storage = tempdir("private");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"], "privateRouting": true}),
    );
    assert!(node.private_routing(), "the node is in private mode");
    step("private node up");
    let status = rt.block_on(node.status());
    // Fail closed: no peer is ever dialled or accepted, so no topic (public or LAN) reaches anyone;
    // the private route carries nothing because there is no overlay to carry it. `routes.public`
    // is the DHT's own wire counter (bootstrap pings included), so connections are the signal.
    assert_eq!(status.peers, 0, "no public connection");
    assert_eq!(status.lan_peers, 0, "no LAN connection");
    assert_eq!(status.routes.public.peers, 0, "no public join");
    assert_eq!(status.routes.lan.peers, 0, "no LAN join");
    assert_eq!(status.routes.lan.tx_bytes, 0, "no LAN traffic");
    assert_eq!(status.routes.lan.rx_bytes, 0, "no LAN traffic");
    assert_eq!(status.routes.private.rx_bytes, 0, "no private route exists");
    assert_eq!(status.routes.private.peers, 0, "no private peers");
    assert!(
        no_entry(&rt, &node, "imdb:tt0133093", 10_000),
        "fail closed: never syncs"
    );
    assert_eq!(
        js.call("status", json!({"name": "a"}))["peers"],
        json!(0),
        "the JS node never discovers the private-routed Rust node"
    );

    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

/// The full private-overlay assertion over two hops.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn ten_mixed_peers_stream_exact_bytes_over_two_hop_private_routing() {
    if !has_app() {
        return;
    }
    let mut js = Js::spawn(4);
    let bootstrap = js.bootstrap();
    js.call("guards", json!({}));
    let founder = js.call(
        "node",
        json!({"name": "founder", "bootstrap": bootstrap, "privateRouting": true}),
    );
    for index in 0..8 {
        js.call(
            "node",
            json!({"name": format!("n{index}"), "bootstrap": bootstrap, "tracker": founder["channel"], "privateRouting": true}),
        );
    }
    let last = js.call(
        "node",
        json!({"name": "n8", "bootstrap": bootstrap, "tracker": founder["channel"], "privateRouting": true}),
    );
    js.call(
        "grant",
        json!({"name": "founder", "writer": last["writer"]}),
    );
    let published = js.call(
        "publish",
        json!({"name": "n8", "id": "imdb:tt0133093", "title": "Private Matrix", "size": 2 * 1024 * 1024 + 13, "chunkSize": 8192}),
    );
    assert_eq!(
        published["blob"]["blockLength"],
        json!(5),
        "small source chunks coalesce into 512 KiB blocks"
    );
    let hit = until(
        || {
            let found = js.call("search", json!({"name": "founder", "id": "imdb:tt0133093"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        120_000,
    )
    .expect("private routing finds the entry");
    assert_eq!(hit["local"], json!(false));
    let full = js.call("fetch", json!({"url": hit["streamUrl"]}));
    assert_eq!(full["sha256"], published["sha256"]);
    for name in std::iter::once("founder".to_owned()).chain(
        (0..8)
            .map(|i| format!("n{i}"))
            .chain(std::iter::once("n8".to_owned())),
    ) {
        let exposure = js.call("exposure", json!({"name": name}));
        assert_eq!(exposure["report"]["directDestinationSends"], json!(0));
        let discovery = js.call(
            "swarmStatus",
            json!({"name": name, "key": founder["channel"]}),
        );
        assert_eq!(discovery["status"], json!(null));
    }
}

/// The mixed private route: a Rust publisher and the app's own JS node, both private-routed,
/// sync a channel entry and stream a blob byte-exact through the guards. The relay path is the
/// same Noise secret stream JS replicates over, so a Protomux framing mismatch shows up here.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_and_js_stream_exact_bytes_over_private_routing() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    js.call("guards", json!({}));

    let storage = tempdir("private-rust-pub");
    let publisher = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "privateRouting": true}),
    );
    assert!(publisher.private_routing(), "the publisher is private");
    let channel = rt.block_on(publisher.status()).channel;
    let total = 1024usize * 1024 + 313;
    let bytes: Vec<u8> = (0..total)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 11) as u8)
        .collect();
    let published = rt
        .block_on(
            publisher.publish(put_meta("imdb:tt0133093", "Private Matrix"), {
                use peartube_native::node::core::NodeSource;
                NodeSource::bytes(bytes.clone())
            }),
        )
        .expect("publish");
    assert_eq!(published.announce.sha256, sha256_hex(&bytes));
    step("rust published privately");

    let viewer_dir = tempdir("private-js-viewer");
    js.call(
        "node",
        json!({
            "name": "viewer",
            "storage": viewer_dir.to_string_lossy(),
            "bootstrap": bootstrap,
            "tracker": channel,
            "privateRouting": true
        }),
    );
    step("js viewer joined privately");

    let hit = until(
        || {
            let found = js.call("search", json!({"name": "viewer", "id": "imdb:tt0133093"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        180_000,
    )
    .expect("the private route synced the channel entry");
    assert_eq!(hit["local"], json!(false));
    assert_eq!(hit["sha256"], json!(published.announce.sha256));

    let fetched = js.call("fetch", json!({"url": hit["streamUrl"]}));
    assert_eq!(
        fetched["sha256"].as_str(),
        Some(published.announce.sha256.as_str()),
        "the JS viewer streamed the source byte for byte"
    );
    step("js streamed the blob over the private route");

    // Neither side used the public DHT or the LAN; the overlay carried every byte.
    let viewer = js.call("status", json!({"name": "viewer"}));
    assert_eq!(viewer["routes"]["public"]["peers"], json!(0), "{viewer}");
    assert_eq!(viewer["routes"]["lan"]["peers"], json!(0), "{viewer}");
    assert!(
        viewer["routes"]["private"]["rxBytes"].as_u64().unwrap_or(0) > 0,
        "{viewer}"
    );
    let status = rt.block_on(publisher.status());
    assert_eq!(status.routes.public.peers, 0, "no public connection");
    assert_eq!(status.lan_peers, 0, "no LAN connection");
    assert!(
        status.routes.private.tx_bytes > 0,
        "the publisher served over the overlay: {status:?}"
    );

    rt.block_on(publisher.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&viewer_dir);
}

// ---------------------------------------------------------------- 9. clearCache

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_viewer_clear_cache_shrinks_the_disk_immediately() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let pub_node = js.call("node", json!({"name": "pub", "bootstrap": bootstrap}));
    let storage = tempdir("cache-rust");
    let viewer = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": pub_node["channel"]}),
    );
    let payload = 16u64 * 1024 * 1024;
    let published = js.call(
        "publish",
        json!({"name": "pub", "id": "imdb:tt0816692", "title": "Interstellar", "size": payload}),
    );
    let entry = search_until(&rt, &viewer, "imdb:tt0816692", 180_000).expect("the entry");
    step("entry found");
    let (status, body) = fetch(&rt, &entry.stream_url, None);
    assert_eq!(status, 200);
    assert_eq!(body.len() as u64, payload);
    assert_eq!(sha256_hex(&body), published["sha256"].as_str().unwrap());
    step("downloaded");

    let db = storage.join("corestore");
    let before = dir_bytes(&db);
    assert!(
        before >= payload,
        "expected >= 16 MiB on disk before clearCache, got {before}"
    );
    let cleared = rt.block_on(viewer.clear_cache(None, None)).expect("clear");
    let after = dir_bytes(&db);
    step(&format!("clearCache reclaimed {}", cleared.reclaimed_bytes));
    assert!(
        cleared.reclaimed_bytes >= 14 * 1024 * 1024,
        "expected >= 14 MiB reclaimed, got {}",
        cleared.reclaimed_bytes
    );
    assert!(
        after < 1024 * 1024,
        "expected < 1 MiB on disk after clearCache, got {after}"
    );
    assert_eq!(rt.block_on(viewer.status()).cached_bytes, 0);

    rt.block_on(viewer.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_viewer_clear_cache_shrinks_the_disk_immediately() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let storage = tempdir("cache-rust-pub");
    let publisher = rust_node(&rt, json!({"storage": storage, "bootstrap": bootstrap}));
    let channel = rt.block_on(publisher.status()).channel;
    let payload: usize = 16 * 1024 * 1024;
    let mut bytes = vec![0u8; payload];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 11) as u8;
    }
    let expected = sha256_hex(&bytes);
    let published = rt
        .block_on(
            publisher.publish(put_meta("imdb:tt0816692", "Interstellar"), {
                use peartube_native::node::core::NodeSource;
                NodeSource::bytes(bytes.clone())
            }),
        )
        .expect("publish");
    assert_eq!(published.announce.sha256, expected);

    let view_dir = tempdir("cache-js");
    js.call(
        "node",
        json!({"name": "viewer", "storage": view_dir.to_string_lossy(), "bootstrap": bootstrap, "tracker": channel}),
    );
    let hit = until(
        || {
            let found = js.call("search", json!({"name": "viewer", "id": "imdb:tt0816692"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        180_000,
    )
    .expect("the JS viewer finds the entry");
    let fetched = js.call("fetch", json!({"url": hit["streamUrl"]}));
    assert_eq!(fetched["sha256"].as_str().unwrap(), expected);
    step("JS viewer downloaded");

    let db = view_dir.join("corestore").join("db");
    let before = js.call("diskBytes", json!({"dir": db.to_string_lossy()}));
    assert!(
        before["bytes"].as_u64().unwrap() >= payload as u64,
        "expected >= 16 MiB on disk before clearCache, got {}",
        before["bytes"]
    );
    let cleared = js.call("clearCache", json!({"name": "viewer"}));
    let after = js.call("diskBytes", json!({"dir": db.to_string_lossy()}));
    step(&format!(
        "JS clearCache reclaimed {}",
        cleared["reclaimedBytes"]
    ));
    assert!(
        cleared["reclaimedBytes"].as_u64().unwrap() >= 14 * 1024 * 1024,
        "expected >= 14 MiB reclaimed, got {}",
        cleared["reclaimedBytes"]
    );
    assert!(
        after["bytes"].as_u64().unwrap() < 1024 * 1024,
        "expected < 1 MiB on disk after clearCache, got {}",
        after["bytes"]
    );
    assert_eq!(
        js.call("status", json!({"name": "viewer"}))["cachedBytes"],
        json!(0)
    );

    rt.block_on(publisher.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&view_dir);
}

// ---------------------------------------------------------------- 10. evict behind

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_viewer_evicts_the_spent_prefix_without_stalling_the_tail() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let source = js.call("node", json!({"name": "source", "bootstrap": bootstrap}));
    let storage = tempdir("evict-rust");
    let viewer = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": source["channel"]}),
    );
    let total = 256usize * 1024 * 1024;
    let published = js.call(
        "publish",
        json!({"name": "source", "id": "imdb:tt0209144", "title": "Memento", "size": total}),
    );
    let entry = search_until(&rt, &viewer, "imdb:tt0209144", 180_000).expect("the entry");
    let (status, body) = fetch(&rt, &entry.stream_url, None);
    assert_eq!(status, 200);
    assert_eq!(body.len(), total);
    assert_eq!(sha256_hex(&body), published["sha256"].as_str().unwrap());
    assert_eq!(rt.block_on(viewer.status()).cached_bytes, total as u64);
    step("downloaded 256 MiB");
    rt.block_on(viewer.gc_storage(false, false)).expect("gc");
    let db = storage.join("corestore");
    let before = dir_bytes(&db);

    let keep_behind = 8 * 1024 * 1024u64;
    let stream_url = entry.stream_url.clone();
    let tail_expected = js.call(
        "hashSlice",
        json!({"name": "source", "start": (total - 524288) as u64, "end": (total - 1) as u64}),
    )["sha256"]
        .as_str()
        .unwrap()
        .to_owned();
    let probing = Arc::new(AtomicBool::new(true));
    let slowest = Arc::new(std::sync::Mutex::new(0u128));
    let probes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let tail = {
        let probing = probing.clone();
        let slowest = slowest.clone();
        let probes = probes.clone();
        let stream_url = stream_url.clone();
        std::thread::spawn(move || {
            let rt = runtime();
            let _enter = rt.enter();
            while probing.load(Ordering::Acquire) {
                let started = Instant::now();
                let (status, bytes) = fetch(
                    &rt,
                    &stream_url,
                    Some(((total - 524288) as u64, (total - 1) as u64)),
                );
                assert_eq!(status, 206);
                assert_eq!(sha256_hex(&bytes), tail_expected);
                let elapsed = started.elapsed().as_millis();
                let mut slowest = slowest.lock().unwrap();
                if elapsed > *slowest {
                    *slowest = elapsed;
                }
                probes.fetch_add(1, Ordering::AcqRel);
                std::thread::sleep(Duration::from_millis(75));
            }
        })
    };
    let opt = json!({
        "playheadByte": (total - 524288) as u64,
        "keepBehindBytes": keep_behind,
    });
    let blob: Blob = serde_json::from_value(published["blob"].clone()).expect("blob");
    let reclaimed = rt
        .block_on(viewer.evict_behind(published["blobs"].as_str().unwrap(), blob, opt))
        .expect("evict");
    let remaining = rt.block_on(viewer.status()).cached_bytes;
    probing.store(false, Ordering::Release);
    tail.join().expect("tail probe");
    step(&format!(
        "evicted {reclaimed}, remaining {remaining}, probes {}",
        probes.load(Ordering::Acquire)
    ));
    assert!(
        remaining <= 16 * 1024 * 1024,
        "one cleanup keeps pace, got {remaining}"
    );
    assert!(
        probes.load(Ordering::Acquire) > 0,
        "concurrent Range requests reached the receiver"
    );
    let slowest = *slowest.lock().unwrap();
    assert!(slowest < 2500, "live cached Range stalled for {slowest} ms");
    assert!(reclaimed > 0);
    let after = dir_bytes(&db);
    assert!(
        before.saturating_sub(after) >= 96 * 1024 * 1024,
        "cleanup must reclaim physical bytes ({before} -> {after})"
    );
    let (status, tail_bytes) = fetch(
        &rt,
        &stream_url,
        Some(((total - keep_behind as usize) as u64, (total - 1) as u64)),
    );
    assert_eq!(status, 206);
    assert_eq!(tail_bytes.len(), keep_behind as usize);
    assert_eq!(rt.block_on(viewer.status()).cached_bytes, remaining);

    rt.block_on(viewer.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_viewer_evicts_the_spent_prefix_over_rust_bytes() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let storage = tempdir("evict-rust-pub");
    let publisher = rust_node(&rt, json!({"storage": storage, "bootstrap": bootstrap}));
    let channel = rt.block_on(publisher.status()).channel;
    let total = 128 * 1024 * 1024;
    let mut bytes = vec![0u8; total];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 13) as u8;
    }
    let published = rt
        .block_on(publisher.publish(put_meta("imdb:tt0209144", "Memento"), {
            use peartube_native::node::core::NodeSource;
            NodeSource::bytes(bytes.clone())
        }))
        .expect("publish");
    assert_eq!(published.announce.sha256, sha256_hex(&bytes));
    let view_dir = tempdir("evict-js");
    js.call(
        "node",
        json!({"name": "viewer", "storage": view_dir.to_string_lossy(), "bootstrap": bootstrap, "tracker": channel}),
    );
    let hit = until(
        || {
            let found = js.call("search", json!({"name": "viewer", "id": "imdb:tt0209144"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        180_000,
    )
    .expect("the JS viewer finds the entry");
    let fetched = js.call("fetch", json!({"url": hit["streamUrl"]}));
    assert_eq!(
        fetched["sha256"].as_str().unwrap(),
        published.announce.sha256
    );
    step("JS viewer downloaded");
    js.call("gcStorage", json!({"name": "viewer"}));
    let reclaimed = js.call(
        "evictBehind",
        json!({
            "name": "viewer",
            "blobs": published.announce.blobs,
            "blob": {
                "blockOffset": published.announce.blob.block_offset,
                "blockLength": published.announce.blob.block_length,
                "byteOffset": published.announce.blob.byte_offset,
                "byteLength": published.announce.blob.byte_length
            },
            "playheadByte": total - 524288,
            "keepBehindBytes": 8 * 1024 * 1024
        }),
    );
    let status = js.call("status", json!({"name": "viewer"}));
    step(&format!(
        "JS evict reclaimed {} remaining {}",
        reclaimed["reclaimed"], status["cachedBytes"]
    ));
    assert!(
        status["cachedBytes"].as_u64().unwrap() <= 16 * 1024 * 1024,
        "one cleanup keeps pace, got {}",
        status["cachedBytes"]
    );
    assert!(reclaimed["reclaimed"].as_u64().unwrap_or(0) > 0);
    let tail = js.call(
        "fetch",
        json!({"url": hit["streamUrl"], "range": format!("bytes={}-{}", total - 8 * 1024 * 1024, total - 1)}),
    );
    assert_eq!(tail["status"], json!(206));

    rt.block_on(publisher.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&view_dir);
}
