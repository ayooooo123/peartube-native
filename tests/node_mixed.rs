//! The mixed-node milestones: a real JS PearTube node and this node on one local HyperDHT
//! testnet, in the app's own milestone order.
//!
//! Needs `node`, the app checkout (`PEARTUBE_APP`, else `~/projects/peartube`) and its installed
//! modules; `#[ignore]` so `cargo test` stays offline.

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use peartube_native::node::Node;

const HARNESS: &str = "tests/js/node-mixed.mjs";

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

/// One harness process: its JSON result line and its stdin (closing it stops the child).
struct Harness {
    child: Child,
    stdin: Option<ChildStdin>,
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
    Harness { child, stdin }
}

impl Harness {
    fn result(&mut self) -> Value {
        let stdout = self.child.stdout.take().expect("stdout");
        let mut lines = BufReader::new(stdout).lines();
        let line = lines
            .next()
            .expect("harness printed nothing")
            .expect("read line");
        serde_json::from_str(&line).expect("harness json")
    }
}

fn tempdir() -> std::path::PathBuf {
    tempfile::Builder::new()
        .prefix("peartube-node-mixed-")
        .tempdir()
        .expect("tempdir")
        .keep()
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

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn rust_node(options: Value) -> Arc<peartube_native::node::core::CoreNode> {
    peartube_native::node::core::create_node(options)
        .await
        .expect("rust node")
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
}

/// The node opens on a JS testnet, reports a channel, restarts on the same storage.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_node_opens_and_restarts() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let runtime = runtime();
    let mut testnet = spawn_js("testnet", &[]);
    let info = testnet.result();
    let bootstrap = info["bootstrap"].clone();

    let storage = tempdir();
    let node = runtime.block_on(async {
        rust_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
        }))
        .await
    });
    let status = runtime.block_on(node.status());
    assert_eq!(status.channel.len(), 64, "channel hex");
    assert_eq!(status.writer.len(), 64, "writer hex");
    assert!(status.writable, "a fresh node owns its channel");
    assert_eq!(status.peers, 0);
    runtime.block_on(node.close()).expect("close");

    let restarted = runtime.block_on(async {
        rust_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
        }))
        .await
    });
    let resumed = runtime.block_on(restarted.status());
    assert_eq!(
        resumed.channel, status.channel,
        "same channel after restart"
    );
    runtime.block_on(restarted.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = &mut testnet;
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_node_lists_js_published_videos() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let runtime = runtime();
    let mut testnet = spawn_js("testnet", &[]);
    let bootstrap = testnet.result()["bootstrap"].clone();
    let mut publisher = spawn_js(
        "publish",
        &[
            "1048579".into(),
            "imdb:tt0000001".into(),
            "Mixed Node Fixture".into(),
            bootstrap.to_string(),
        ],
    );
    let published = publisher.result();
    let channel = published["channel"].as_str().expect("channel").to_owned();
    let js_entry = published["entry"].clone();

    let storage = tempdir();
    let node = runtime.block_on(async {
        rust_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
            "tracker": channel,
        }))
        .await
    });
    let found: Arc<Mutex<Option<Vec<peartube_native::node::api::Entry>>>> =
        Arc::new(Mutex::new(None));
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let searcher = {
        let node = node.clone();
        let found = found.clone();
        let cancel = cancel.clone();
        let worker = runtime.handle().clone();
        std::thread::spawn(move || {
            while !cancel.load(std::sync::atomic::Ordering::Acquire) {
                let list = worker.block_on(async {
                    node.search(
                        Some("imdb:tt0000001"),
                        peartube_native::node::api::SearchOptions::default(),
                    )
                    .await
                    .unwrap_or_default()
                });
                if !list.is_empty() {
                    *found.lock().unwrap() = Some(list);
                    return;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        })
    };
    let hit = until(
        || found.lock().unwrap().clone().map(|list| list[0].clone()),
        180_000,
    )
    .expect("the JS-published entry");
    cancel.store(true, std::sync::atomic::Ordering::Release);
    let _ = searcher.join();
    assert_eq!(hit.id, "imdb:tt0000001");
    assert_eq!(hit.title, js_entry["title"].as_str().unwrap_or_default());
    assert_eq!(hit.sha256, published["sha256"].as_str().unwrap());
    assert_eq!(hit.size, published["size"].as_u64().unwrap());
    assert_eq!(hit.source_kind, "channel");
    assert_eq!(hit.source, channel);

    // The whole listing (no id filter) matches the publishing JS node's own listing.
    let js_listing = published["entries"].clone();
    let mut rust_listing = None;
    until(
        || {
            let list = runtime.block_on(async {
                node.search(None, peartube_native::node::api::SearchOptions::default())
                    .await
                    .unwrap_or_default()
            });
            if list.is_empty() {
                None
            } else {
                rust_listing = Some(list);
                Some(())
            }
        },
        60_000,
    )
    .expect("rust listing");
    let rust_listing = rust_listing.expect("listing");
    let js_listing = js_listing.as_array().cloned().unwrap_or_default();
    assert_eq!(
        rust_listing.len(),
        js_listing.len(),
        "same number of entries"
    );
    for (rust, js) in rust_listing.iter().zip(js_listing.iter()) {
        assert_eq!(rust.id, js["id"].as_str().unwrap_or_default());
        assert_eq!(rust.title, js["title"].as_str().unwrap_or_default());
        assert_eq!(rust.sha256, js["sha256"].as_str().unwrap_or_default());
        assert_eq!(rust.size, js["size"].as_u64().unwrap_or_default());
        assert_eq!(rust.season, js["season"].as_u64());
        assert_eq!(rust.episode, js["episode"].as_u64());
    }

    // Milestone 3: the JS file's own start, middle and end are byte-identical through the URL.
    let blob_hex = runtime.block_on(async {
        node.find_entry(&hit.key)
            .await
            .ok()
            .flatten()
            .and_then(|value| {
                value
                    .get("blobs")
                    .and_then(|blob| blob.as_str())
                    .map(str::to_owned)
            })
            .expect("the announce names its blob")
    });
    if let Err(error) = runtime.block_on(node.blob_core(&blob_hex)) {
        panic!("the follower cannot open the JS blob core: {error}");
    }
    let stream_url = hit.stream_url.clone();
    assert!(!stream_url.is_empty(), "a stream URL");
    let ranges = published["ranges"].as_array().cloned().unwrap_or_default();
    assert_eq!(ranges.len(), 3, "start, middle and end");
    for case in &ranges {
        let start = case["start"].as_u64().expect("range start");
        let end = case["end"].as_u64().expect("range end");
        let bytes = runtime.block_on(async {
            let request = hyper::Request::builder()
                .uri(&stream_url)
                .header("range", format!("bytes={start}-{end}"))
                .body(http_body_util::Empty::<bytes::Bytes>::new())
                .expect("request");
            let client =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
                    .build_http();
            let response = client.request(request).await.expect("range request");
            assert_eq!(response.status(), 206, "range status for {start}-{end}");
            let body = http_body_util::BodyExt::collect(response.into_body())
                .await
                .expect("body")
                .to_bytes();
            body.to_vec()
        });
        assert_eq!(
            bytes.len() as u64,
            case["length"].as_u64().expect("range length"),
            "range {start}-{end} length"
        );
        assert_eq!(
            sha256_hex(&bytes),
            case["sha256"].as_str().expect("range sha256"),
            "range {start}-{end} bytes"
        );
    }
    // The whole file through the same URL hashes to what the JS node published.
    let mut full = spawn_js("fetch", &[stream_url.clone(), String::new()]);
    let full_line = full.result();
    assert_eq!(
        full_line["size"].as_u64().unwrap(),
        published["size"].as_u64().unwrap()
    );
    assert_eq!(
        full_line["sha256"].as_str().unwrap(),
        published["sha256"].as_str().unwrap()
    );

    let _ = std::fs::remove_dir_all(&storage);
    runtime.block_on(node.close()).expect("close");
    let _ = &mut publisher;
    let _ = &mut testnet;
}

/// Milestone 4 and 5: the JS node follows a channel this node published and plays its blob; the
/// node then restarts on the same storage and still lists the video with the publisher gone.
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_follows_rust_published_video_and_node_resumes() {
    if !has_app() {
        eprintln!("skipped: set PEARTUBE_APP to the app checkout");
        return;
    }
    let runtime = runtime();
    let mut testnet = spawn_js("testnet", &[]);
    let bootstrap = testnet.result()["bootstrap"].clone();

    let storage = tempdir();
    let node = runtime.block_on(async {
        rust_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
        }))
        .await
    });
    let mut bytes = vec![0u8; 900_001];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 7) as u8;
    }
    let expected = sha256_hex(&bytes);
    let published = runtime
        .block_on(node.publish(
            peartube_native::node::api::PutMeta {
                id: "imdb:tt0000002".into(),
                title: "Rust Published Fixture".into(),
                private_entry: None,
                epoch: None,
                store: None,
                expected_size: Some(Some(bytes.len() as u64)),
                extra: serde_json::Map::new(),
            },
            peartube_native::node::core::NodeSource::bytes(bytes.clone()),
        ))
        .expect("publish");
    assert_eq!(published.announce.sha256, expected);
    let channel = published.channel.clone();

    let mut verifier = spawn_js(
        "verify",
        &[
            bootstrap.to_string(),
            channel.clone(),
            "imdb:tt0000002".into(),
        ],
    );
    let verified = verifier.result();
    assert_eq!(
        verified["sha256"].as_str().unwrap(),
        expected,
        "JS reads the Rust blob"
    );
    assert_eq!(verified["size"].as_u64().unwrap(), bytes.len() as u64);
    assert_eq!(verified["status"].as_u64().unwrap(), 200);
    let js_entry = verified["entry"].clone();
    assert_eq!(
        js_entry["title"].as_str().unwrap(),
        "Rust Published Fixture"
    );
    assert_eq!(js_entry["source"].as_str().unwrap(), channel);

    // The listing this node serves for its own channel matches what the JS node sees.
    let listing = runtime.block_on(async {
        node.search(None, peartube_native::node::api::SearchOptions::default())
            .await
            .expect("search")
    });
    let listed = listing
        .iter()
        .find(|entry| entry.id == "imdb:tt0000002")
        .expect("own entry");
    assert_eq!(listed.sha256, expected);
    assert_eq!(listed.size, bytes.len() as u64);
    assert!(listed.local, "the publisher owns the blob");

    // Milestone 5: restart on the same storage. The publisher is this node, now closed, so the
    // listing has to come back from its own persisted view and store.
    runtime.block_on(node.close()).expect("close");
    drop(verifier);
    let resumed = runtime.block_on(async {
        rust_node(json!({
            "storage": storage.to_string_lossy(),
            "bootstrap": bootstrap,
        }))
        .await
    });
    let status = runtime.block_on(resumed.status());
    assert_eq!(status.channel, channel, "same channel after restart");
    assert!(status.writable);
    let resumed_listing = runtime.block_on(async {
        resumed
            .search(None, peartube_native::node::api::SearchOptions::default())
            .await
            .expect("search")
    });
    let resumed_entry = resumed_listing
        .iter()
        .find(|entry| entry.id == "imdb:tt0000002")
        .expect("entry survives the restart");
    assert_eq!(resumed_entry.sha256, expected);
    assert!(resumed_entry.local);
    runtime.block_on(resumed.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = &mut testnet;
}
