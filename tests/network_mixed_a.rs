//! The app's own network scenarios, run with a real JS PearTube node on one side and this node on
//! the other, on a local HyperDHT testnet.
//!
//! Slice A of `peartube/test/network.test.js`: acquire-then-stream (1), a source that dies
//! mid-transfer (3), an acquire finished before first sync (7), and acquire resilience —
//! redirect auth, Range resume, 403 dead-letter (11) — each in both directions. The assertions are
//! the ones the JS suite makes: exact bytes, job state, request counts and the durable job file.
//!
//! Needs `node`, the app checkout (`PEARTUBE_APP`, else `~/projects/peartube`) and its installed
//! modules; `#[ignore]` so `cargo test` stays offline.

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use peartube_native::acquire::{
    AcquireOptions, Acquirer, AddJob, HyperHttp, LocalJobFiles, OsRandom, SourceSpec, UbitPages,
};
use peartube_native::node::Node;
use peartube_native::node::api::{Entry, PutMeta, SearchOptions};
use peartube_native::node::core::{CoreNode, NodeStreams, create_node};

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
    lines: std::sync::mpsc::Receiver<String>,
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
        // Read on a thread so a harness op that never answers fails the test instead of hanging it.
        let (sender, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        let first = lines
            .recv_timeout(Duration::from_secs(120))
            .expect("harness printed nothing");
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
        command.insert("msgId".into(), json!(id));
        command.insert("op".into(), json!(op));
        if let Value::Object(fields) = args {
            for (key, value) in fields {
                command.insert(key, value);
            }
        }
        let line = Value::Object(command).to_string();
        self.stdin
            .as_mut()
            .expect("harness stdin")
            .write_all(format!("{line}\n").as_bytes())
            .expect("harness stdin write");
        let deadline = Instant::now() + Duration::from_secs(240);
        loop {
            let line = self
                .lines
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap_or_else(|_| panic!("harness did not answer {op} within 240s"));
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
        eprintln!("[mixed] {label}");
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

/// The JS node's own listing for one id, as the app's `search` answers it.
fn js_search(js: &mut Js, name: &str, id: &str) -> Vec<Value> {
    let found = js.call("search", json!({"name": name, "id": id}));
    found["entries"].as_array().cloned().unwrap_or_default()
}

fn js_search_until(js: &mut Js, name: &str, id: &str, ms: u64) -> Option<Value> {
    until(|| js_search(js, name, id).into_iter().next(), ms)
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

/// The acquirer the app builds, against this node: real HTTP, real `node.put`, real jobs.json.
fn build_acquirer(
    rt: &tokio::runtime::Runtime,
    node: Arc<CoreNode>,
    jobs: &Path,
    opts: AcquireOptions,
) -> Acquirer<CoreNode> {
    let _guard = rt.enter();
    Acquirer::new(
        node,
        Arc::new(LocalJobFiles::new(jobs)),
        Arc::new(UbitPages {
            ubit: "ubit".into(),
        }),
        Arc::new(HyperHttp),
        Arc::new(NodeStreams),
        Arc::new(OsRandom),
        opts,
    )
}

fn http_job(id: &str, title: &str, url: &str, headers: &[(&str, &str)]) -> AddJob {
    AddJob {
        id: id.into(),
        title: title.into(),
        source: SourceSpec {
            url: url.into(),
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            page: false,
            private: false,
        },
        channel: Some(true),
        private: false,
        plugin: None,
    }
}

fn job_status(acquirer: &Acquirer<CoreNode>, job_id: &str) -> Option<(String, Value)> {
    let job = acquirer.get(job_id)?;
    let status = job["status"].as_str().unwrap_or_default().to_owned();
    Some((status, job))
}

fn await_status(acquirer: &Acquirer<CoreNode>, job_id: &str, wanted: &str, ms: u64) -> Value {
    until(
        || {
            let (status, job) = job_status(acquirer, job_id)?;
            if status == wanted {
                return Some(job);
            }
            assert!(
                !matches!(status.as_str(), "failed" | "cancelled" | "removed"),
                "job {job_id} became {status}: {job}"
            );
            None
        },
        ms,
    )
    .unwrap_or_else(|| {
        panic!(
            "job {job_id} never reached {wanted}: {:?}",
            acquirer.get(job_id)
        )
    })
}

// ---------------------------------------------------------------- 1. acquire, find, stream

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_relay_acquires_and_js_relay_streams_exact_bytes() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));

    let source = js.call(
        "httpSource",
        json!({"name": "src", "size": 2 * 1024 * 1024 + 7, "authorization": "Bearer source-secret"}),
    );
    let size = source["size"].as_u64().unwrap();
    let expected = source["sha256"].as_str().unwrap().to_owned();
    step("1A: js nodes and source ready");

    let storage = tempdir("acquire");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(node.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));
    let became_writable = until(|| rt.block_on(node.status()).writable.then_some(()), 30_000);
    step(&format!(
        "1A: rust node granted, writable after 30s = {}",
        became_writable.is_some()
    ));

    let jobs = storage.join("jobs.json");
    let acquirer = build_acquirer(&rt, node.clone(), &jobs, AcquireOptions::default());
    let added = acquirer
        .add(http_job(
            "imdb:tt0111161",
            "Fixture",
            source["url"].as_str().unwrap(),
            &[("authorization", "Bearer source-secret")],
        ))
        .expect("add");
    step("1A: job added");
    assert!(
        added.get("source").is_none(),
        "job views never expose the source"
    );
    let job_id = added["jobId"].as_str().unwrap().to_owned();
    let done = await_status(&acquirer, &job_id, "done", 180_000);
    step("1A: job done");
    assert_eq!(done["sha256"].as_str().unwrap(), expected);
    assert!(
        !Value::Array(acquirer.list())
            .to_string()
            .contains("source-secret"),
        "the listing never leaks the auth header"
    );

    // The JS relay finds the entry this node acquired, then streams exact bytes.
    let rust_status = rt.block_on(node.status());
    step(&format!(
        "1A: rust peers={} lan={} rx={} tx={} writable={}",
        rust_status.peers,
        rust_status.lan_peers,
        rust_status.rx_bytes,
        rust_status.tx_bytes,
        rust_status.writable
    ));
    let a_status = js.call("status", json!({"name": "a"}));
    step(&format!(
        "1A: js a peers={} rx={} tx={} writable={}",
        a_status["peers"], a_status["rxBytes"], a_status["txBytes"], a_status["writable"]
    ));
    step("1A: waiting for js a to list the entry");
    let hit = js_search_until(&mut js, "a", "imdb:tt0111161", 180_000)
        .expect("the JS relay lists the acquired entry");
    assert_eq!(hit["sha256"].as_str().unwrap(), expected);
    assert_eq!(hit["local"], json!(false));
    step("1A: js relay lists the entry");

    let stream_url = hit["streamUrl"].as_str().unwrap().to_owned();
    let ranged = js.call(
        "fetch",
        json!({"url": stream_url, "range": "bytes=1000000-1065535"}),
    );
    assert_eq!(ranged["status"], json!(206));
    let slice = js.call(
        "hashSlice",
        json!({"name": "src", "start": 1000000, "end": 1065535}),
    );
    assert_eq!(ranged["sha256"], slice["sha256"], "Range bytes");
    step("1A: js relay streamed the Range");
    let full = js.call("fetch", json!({"url": stream_url}));
    assert_eq!(full["sha256"].as_str().unwrap(), expected, "full file");

    // The whole tracker lists the entry the JS relay just streamed.
    let listed = js.call("apiEntries", json!({"name": "a"}));
    let results = listed["results"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        results.len(),
        1,
        "/v1/entries lists exactly the acquired entry"
    );
    assert_eq!(
        results[0]["key"], hit["key"],
        "/v1/entries lists the same entry"
    );

    // The public DHT carried the bytes and nothing else did.
    let received = js.call("status", json!({"name": "a"}));
    let public = received["routes"]["public"]["rxBytes"].as_u64().unwrap();
    let private = received["routes"]["private"]["rxBytes"].as_u64().unwrap();
    let lan = received["routes"]["lan"]["rxBytes"].as_u64().unwrap();
    assert!(public >= size, "the public DHT stream carried the bytes");
    assert_eq!(private + lan, 0, "no private or LAN stream carried them");
    assert_eq!(
        received["rxBytes"].as_u64().unwrap(),
        public,
        "the path counters account for all received bytes"
    );

    acquirer.close();
    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_relay_acquires_and_rust_relay_streams_exact_bytes() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));

    let storage = tempdir("acquire-js");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );

    let source = js.call(
        "httpSource",
        json!({"name": "src", "size": 2 * 1024 * 1024 + 7, "authorization": "Bearer source-secret"}),
    );
    let expected = source["sha256"].as_str().unwrap().to_owned();
    let jobs = tempdir("js-jobs").join("jobs.json");
    js.call(
        "acquirer",
        json!({"name": "acq", "node": "b", "file": jobs.to_string_lossy()}),
    );
    let added = js.call(
        "acquireAdd",
        json!({
            "name": "acq",
            "id": "imdb:tt0111161",
            "title": "Fixture",
            "url": source["url"],
            "headers": {"authorization": "Bearer source-secret"}
        }),
    );
    assert!(
        added.get("source").is_none(),
        "job views never expose the source"
    );
    let job_id = added["jobId"].as_str().unwrap().to_owned();
    let done = until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": job_id}));
            (job["status"] == json!("done")).then_some(job)
        },
        180_000,
    )
    .expect("the JS acquirer finished");
    assert_eq!(done["sha256"].as_str().unwrap(), expected);

    // This node finds the JS-acquired entry and streams exact bytes.
    let hit = search_until(&rt, &node, "imdb:tt0111161", 180_000).expect("the entry replicates");
    assert!(!hit.local, "the blob belongs to the other relay");
    assert_eq!(hit.sha256, expected);
    let (status, full) = fetch(&rt, &hit.stream_url, None);
    assert_eq!(status, 200);
    assert_eq!(sha256_hex(&full), expected, "the whole file");
    let (status, ranged) = fetch(&rt, &hit.stream_url, Some((1_000_000, 1_065_535)));
    assert_eq!(status, 206);
    assert_eq!(
        ranged.len(),
        65_536,
        "the app's own 1 MiB..1 MiB+64 KiB slice"
    );
    let slice = js.call(
        "hashSlice",
        json!({"name": "src", "start": 1000000, "end": 1065535}),
    );
    assert_eq!(sha256_hex(&ranged), slice["sha256"].as_str().unwrap());

    // This node's own counters say the public DHT carried it, and nothing else did.
    let status = rt.block_on(node.status());
    assert!(
        status.routes.public.rx_bytes >= full.len() as u64,
        "public rx {} counts the {} streamed bytes",
        status.routes.public.rx_bytes,
        full.len()
    );
    assert_eq!(
        status.routes.private.rx_bytes + status.routes.lan.rx_bytes,
        0,
        "no private or LAN path carried them"
    );
    assert!(
        status.peers > 0,
        "this node holds the peer it streamed from"
    );

    js.call("acquireClose", json!({"name": "acq"}));
    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

// ---------------------------------------------------------------- 3. a source that dies

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_acquirer_fails_a_dying_source_and_finishes_the_next() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let source = js.call(
        "brokenSource",
        json!({"name": "broken", "size": 300 * 1024}),
    );
    let storage = tempdir("broken");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(node.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));

    let acquirer = build_acquirer(
        &rt,
        node.clone(),
        &storage.join("jobs.json"),
        AcquireOptions::default(),
    );
    let broken = acquirer
        .add(http_job(
            "imdb:tt0000001",
            "Broken",
            source["brokenUrl"].as_str().unwrap(),
            &[],
        ))
        .expect("add broken");
    let next = acquirer
        .add(http_job(
            "imdb:tt0000002",
            "Next",
            source["goodUrl"].as_str().unwrap(),
            &[],
        ))
        .expect("add next");
    let broken_id = broken["jobId"].as_str().unwrap().to_owned();
    let next_id = next["jobId"].as_str().unwrap().to_owned();
    await_status(&acquirer, &broken_id, "failed", 120_000);
    let done = await_status(&acquirer, &next_id, "done", 120_000);
    assert_eq!(
        done["sha256"].as_str().unwrap(),
        source["sha256"].as_str().unwrap()
    );

    acquirer.close();
    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

/// A minimal HTTP source so a JS acquirer can hit the same two endpoints from Rust's side.
struct RustSource {
    port: u16,
    stop: Arc<AtomicBool>,
}

impl RustSource {
    fn start(good: Vec<u8>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).expect("nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let good_bytes = good.clone();
        std::thread::spawn(move || {
            while !flag.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let good_bytes = good_bytes.clone();
                        std::thread::spawn(move || serve(stream, &good_bytes));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });
        Self { port, stop }
    }
}

impl Drop for RustSource {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        // Unblock the accept loop's next poll.
        let _ = std::net::TcpStream::connect(("127.0.0.1", self.port));
    }
}

fn serve(mut stream: std::net::TcpStream, good: &[u8]) {
    // One `read` can carry half the request line, which would route `/broken` to the good file.
    let mut request = Vec::new();
    let mut buffer = [0u8; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") && request.len() < 8192 {
        match stream.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(read) => request.extend_from_slice(&buffer[..read]),
        }
    }
    let request = String::from_utf8_lossy(&request).to_string();
    if request.starts_with("GET /broken") {
        let header = format!(
            "HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n",
            10 * 1024 * 1024
        );
        let _ = stream.write_all(header.as_bytes());
        let _ = stream.write_all(&vec![0x5au8; 64 * 1024]);
        return;
    }
    let header = format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", good.len());
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(good);
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_acquirer_fails_a_dying_rust_source_and_finishes_the_next() {
    if !has_app() {
        return;
    }
    let mut good = vec![0u8; 300 * 1024];
    for (index, byte) in good.iter_mut().enumerate() {
        *byte = (index.wrapping_mul(2_654_435_761) >> 9) as u8;
    }
    let source = RustSource::start(good.clone());
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));
    let jobs = tempdir("js-broken").join("jobs.json");
    js.call(
        "acquirer",
        json!({"name": "acq", "node": "b", "file": jobs.to_string_lossy()}),
    );
    let broken = js.call(
        "acquireAdd",
        json!({"name": "acq", "id": "imdb:tt0000001", "title": "Broken", "url": format!("http://127.0.0.1:{}/broken", source.port)}),
    );
    let next = js.call(
        "acquireAdd",
        json!({"name": "acq", "id": "imdb:tt0000002", "title": "Next", "url": format!("http://127.0.0.1:{}/good", source.port)}),
    );
    let broken_id = broken["jobId"].as_str().unwrap().to_owned();
    let next_id = next["jobId"].as_str().unwrap().to_owned();
    until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": broken_id}));
            (job["status"] == json!("failed")).then_some(())
        },
        120_000,
    )
    .expect("the dying source fails its job");
    let done = until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": next_id}));
            (job["status"] == json!("done")).then_some(job)
        },
        120_000,
    )
    .expect("the next job still finishes");
    assert_eq!(done["sha256"].as_str().unwrap(), sha256_hex(&good));
}

// ---------------------------------------------------------------- 7. restart survival

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_acquire_before_first_sync_survives_a_restart() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let isolated = js.call("testnet", json!({"size": 3}))["bootstrap"].clone();
    let _a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let identity = js.call("identity", json!({"name": "a", "revealMnemonic": true}));
    let mnemonic = identity["mnemonic"].as_str().expect("mnemonic").to_owned();

    let storage = tempdir("restart-rust");
    let mut node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": isolated, "mnemonic": mnemonic, "join": true}),
    );
    let source = js.call("httpSource", json!({"name": "restart", "size": 200 * 1024}));
    let jobs = storage.join("jobs.json");
    let mut acquirer = build_acquirer(&rt, node.clone(), &jobs, AcquireOptions::default());
    let added = acquirer
        .add(http_job(
            "imdb:tt0000031",
            "Offline",
            source["url"].as_str().unwrap(),
            &[],
        ))
        .expect("add");
    let job_id = added["jobId"].as_str().unwrap().to_owned();
    await_status(&acquirer, &job_id, "announcing", 180_000);
    assert_ne!(
        job_status(&acquirer, &job_id).map(|(status, _)| status),
        Some("done".into()),
        "an isolated node cannot claim the announce yet"
    );
    acquirer.close();
    rt.block_on(node.close()).expect("close");
    drop(node);

    node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "mnemonic": mnemonic, "join": true}),
    );
    acquirer = build_acquirer(&rt, node.clone(), &jobs, AcquireOptions::default());
    await_status(&acquirer, &job_id, "done", 180_000);
    until(
        || {
            let found = js.call("search", json!({"name": "a", "id": "imdb:tt0000031"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .map(|entry| entry["sha256"] == source["sha256"])
        },
        180_000,
    )
    .expect("the restarted node announces the file it finished offline");

    acquirer.close();
    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

/// The JS side restarts its acquirer over a jobs.json that holds an `announcing` job, which the app
/// mishandles: `run()` in `src/acquire.js` dereferences `job.source.page` for a job whose source was
/// nulled once its announce became durable, and because `run` is async the TypeError rejects its
/// promise without registering the job, so `next()`'s `while` loop re-runs the same job forever and
/// `createAcquirer` never returns (the app's own scenario 7 hangs on this checkout the same way).
///
/// It passes with that one line fixed: copy `src/acquire.js` outside the app, change
/// `if (!job.source.page && !job.store)` to `if (!job.announce && !job.source.page && !job.store)`,
/// rewrite its three non-builtin imports (`b4a`, `streamx`, `./identify.js`) to absolute paths so the
/// copy resolves from wherever it sits, and point the harness at it:
///   PEARTUBE_ACQUIRE=/tmp/peartube-acquire-fixed/acquire.mjs cargo test --test network_mixed_a \
///     js_acquire_before_first_sync_survives_a_restart -- --ignored
#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_acquire_before_first_sync_survives_a_restart() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let isolated = js.call("testnet", json!({"size": 3}))["bootstrap"].clone();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let identity = js.call("identity", json!({"name": "a", "revealMnemonic": true}));
    let mnemonic = identity["mnemonic"].as_str().expect("mnemonic").to_owned();

    let dir = tempdir("restart-js");
    let jobs = dir.join("jobs.json");
    let source = js.call("httpSource", json!({"name": "restart", "size": 200 * 1024}));
    js.call(
        "node",
        json!({"name": "b", "storage": dir.to_string_lossy(), "bootstrap": isolated, "mnemonic": mnemonic, "join": true}),
    );
    js.call(
        "acquirer",
        json!({"name": "acq", "node": "b", "file": jobs.to_string_lossy()}),
    );
    let added = js.call(
        "acquireAdd",
        json!({"name": "acq", "id": "imdb:tt0000031", "title": "Offline", "url": source["url"]}),
    );
    let job_id = added["jobId"].as_str().unwrap().to_owned();
    until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": job_id}));
            (job["status"] == json!("announcing")).then_some(())
        },
        180_000,
    )
    .expect("the job waits for first sync");
    js.call("acquireClose", json!({"name": "acq"}));
    js.call("closeNode", json!({"name": "b"}));

    js.call(
        "node",
        json!({"name": "b", "storage": dir.to_string_lossy(), "bootstrap": bootstrap, "mnemonic": mnemonic, "join": true}),
    );
    js.call(
        "acquirer",
        json!({"name": "acq", "node": "b", "file": jobs.to_string_lossy()}),
    );
    until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": job_id}));
            (job["status"] == json!("done")).then_some(())
        },
        180_000,
    )
    .expect("the restarted acquirer finishes the announce");

    let storage = tempdir("restart-follower");
    let follower = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    until(
        || {
            search_until(&rt, &follower, "imdb:tt0000031", 5_000)
                .filter(|entry| entry.sha256 == source["sha256"].as_str().unwrap())
                .map(|_| ())
        },
        180_000,
    )
    .expect("the announced file replicates");

    js.call("acquireClose", json!({"name": "acq"}));
    rt.block_on(follower.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

// ---------------------------------------------------------------- 8. private routing

// ---------------------------------------------------------------- 11. acquire resilience

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn rust_acquirer_resumes_a_cdn_and_dead_letters_a_403() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b = js.call(
        "node",
        json!({"name": "b", "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));

    // A foreign clip cached from `a` must survive the acquire and the clear.
    let foreign = js.call(
        "publish",
        json!({"name": "a", "id": "imdb:tt0000040", "title": "Foreign Cache Clip", "size": 8 * 1024 * 1024}),
    );
    let foreign_hit = until(
        || {
            let found = js.call("search", json!({"name": "b", "id": "imdb:tt0000040"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        180_000,
    )
    .expect("the foreign entry replicates");
    let foreign_fetch = js.call("fetch", json!({"url": foreign_hit["streamUrl"]}));
    assert_eq!(foreign_fetch["sha256"], foreign["sha256"]);

    let source = js.call(
        "resilientSource",
        json!({"name": "res", "size": 1536 * 1024 + 19}),
    );
    let storage = tempdir("resilient");
    let node = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(node.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));
    let jobs = storage.join("jobs.json");
    let opts = AcquireOptions {
        concurrency: 1,
        retry_delay: Duration::from_millis(100),
        stall_timeout: Duration::from_millis(200),
        ..AcquireOptions::default()
    };
    let acquirer = build_acquirer(&rt, node.clone(), &jobs, opts);
    let dead = acquirer
        .add(http_job(
            "imdb:tt0000041",
            "Dead 403",
            source["forbiddenUrl"].as_str().unwrap(),
            &[],
        ))
        .expect("add dead");
    let resumed = acquirer
        .add(http_job(
            "imdb:tt0000042",
            "Resumed Film",
            source["sourceUrl"].as_str().unwrap(),
            &[("Authorization", "Bearer one-shot-secret")],
        ))
        .expect("add resumed");
    let dead_id = dead["jobId"].as_str().unwrap().to_owned();
    let resumed_id = resumed["jobId"].as_str().unwrap().to_owned();
    await_status(&acquirer, &dead_id, "failed", 120_000);
    let done = await_status(&acquirer, &resumed_id, "done", 240_000);
    std::thread::sleep(Duration::from_millis(350));

    let stats = js.call("sourceStats", json!({"name": "res"}));
    assert_eq!(
        stats["forbiddenHits"],
        json!(1),
        "a terminal 403 never retries"
    );
    assert_eq!(
        stats["cdnHits"],
        json!(5),
        "open + header timeout + 502 + slow seek/stall + finish: {:?}",
        stats["ranges"]
    );
    assert_eq!(
        done.get("attempts"),
        None,
        "Range hiccups never burn attempts"
    );
    assert_eq!(
        done["sha256"].as_str().unwrap(),
        source["sha256"].as_str().unwrap()
    );
    let cached = js.call("status", json!({"name": "b"}))["cachedBytes"]
        .as_u64()
        .unwrap();
    assert_eq!(
        cached,
        foreign["size"].as_u64().unwrap(),
        "acquire never wipes foreign cached peer blobs"
    );
    let saved = std::fs::read_to_string(&jobs).expect("jobs.json");
    let saved: Value = serde_json::from_str(&saved).expect("jobs json");
    let saved_dead = saved
        .as_array()
        .and_then(|list| list.iter().find(|job| job["jobId"] == json!(dead_id)))
        .expect("dead job saved");
    assert_eq!(
        saved_dead["source"],
        Value::Null,
        "a dead 403 clears its source"
    );

    let hit = search_until(&rt, &node, "imdb:tt0000042", 120_000).expect("the entry replicates");
    let (status, streamed) = fetch(&rt, &hit.stream_url, None);
    assert_eq!(status, 200);
    assert_eq!(sha256_hex(&streamed), done["sha256"].as_str().unwrap());

    acquirer.close();
    rt.block_on(node.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn js_acquirer_keeps_rust_published_blobs_and_clears_foreign_cache() {
    if !has_app() {
        return;
    }
    let rt = runtime();
    let _enter = rt.enter();
    let mut js = Js::spawn(3);
    let bootstrap = js.bootstrap();
    let a = js.call("node", json!({"name": "a", "bootstrap": bootstrap}));
    let b_dir = tempdir("js-acquirer-cache");
    let b = js.call(
        "node",
        json!({"name": "b", "storage": b_dir.to_string_lossy(), "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    js.call("grant", json!({"name": "a", "writer": b["writer"]}));

    let storage = tempdir("js-resilient-pub");
    let publisher = rust_node(
        &rt,
        json!({"storage": storage, "bootstrap": bootstrap, "tracker": a["channel"]}),
    );
    let writer = rt.block_on(publisher.status()).writer;
    js.call("grant", json!({"name": "a", "writer": writer}));
    let foreign = rt
        .block_on(
            publisher.publish(put_meta("imdb:tt0000040", "Foreign Cache Clip"), {
                use peartube_native::node::core::NodeSource;
                NodeSource::bytes(vec![42u8; 8 * 1024 * 1024])
            }),
        )
        .expect("publish");
    let foreign_hit = until(
        || {
            let found = js.call("search", json!({"name": "b", "id": "imdb:tt0000040"}));
            found["entries"]
                .as_array()
                .and_then(|list| list.first())
                .cloned()
        },
        180_000,
    )
    .expect("the foreign entry replicates");
    assert_eq!(
        foreign_hit["sha256"].as_str().unwrap(),
        foreign.announce.sha256
    );
    let fetched = js.call("fetch", json!({"url": foreign_hit["streamUrl"]}));
    assert_eq!(fetched["sha256"].as_str().unwrap(), foreign.announce.sha256);
    assert_eq!(
        js.call("status", json!({"name": "b"}))["cachedBytes"],
        json!(8 * 1024 * 1024)
    );

    let source = js.call(
        "resilientSource",
        json!({"name": "res", "size": 700 * 1024}),
    );
    let jobs = tempdir("js-resilient").join("jobs.json");
    js.call(
        "acquirer",
        json!({"name": "acq", "node": "b", "file": jobs.to_string_lossy()}),
    );
    let resumed = js.call(
        "acquireAdd",
        json!({
            "name": "acq",
            "id": "imdb:tt0000042",
            "title": "Resumed Film",
            "url": source["sourceUrl"],
            "headers": {"Authorization": "Bearer one-shot-secret"}
        }),
    );
    let resumed_id = resumed["jobId"].as_str().unwrap().to_owned();
    let done = until(
        || {
            let job = js.call("acquireGet", json!({"name": "acq", "jobId": resumed_id}));
            (job["status"] == json!("done")).then_some(job)
        },
        240_000,
    )
    .expect("the JS acquirer finished");
    assert_eq!(
        done["sha256"].as_str().unwrap(),
        source["sha256"].as_str().unwrap()
    );
    assert_eq!(
        js.call("status", json!({"name": "b"}))["cachedBytes"],
        json!(8 * 1024 * 1024),
        "acquire never wipes the Rust-published foreign cache"
    );

    js.call("gcStorage", json!({"name": "b"}));
    let db = b_dir.join("corestore").join("db");
    let blobs_before = js.call("diskBytes", json!({"dir": db.to_string_lossy()}))["blobs"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        !blobs_before.is_empty(),
        "owned blob files on disk after the flush"
    );
    let cleared = js.call("clearCache", json!({"name": "b"}));
    assert_eq!(cleared["clearedBytes"], json!(8 * 1024 * 1024));
    assert_eq!(
        js.call("status", json!({"name": "b"}))["cachedBytes"],
        json!(0)
    );
    let blobs_after = js.call("diskBytes", json!({"dir": db.to_string_lossy()}))["blobs"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        blobs_after.iter().any(|name| blobs_before.contains(name)),
        "the Rust-published live .blob file is not relocated by clearCache"
    );
    let noop = js.call("clearCache", json!({"name": "b"}));
    assert_eq!(noop["clearedBytes"], json!(0));
    assert_eq!(noop["reclaimedBytes"], json!(0));
    let after_noop = js.call("diskBytes", json!({"dir": db.to_string_lossy()}))["blobs"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(after_noop, blobs_after, "a zero-byte clear is a no-op");

    js.call("acquireClose", json!({"name": "acq"}));
    rt.block_on(publisher.close()).expect("close");
    let _ = std::fs::remove_dir_all(&storage);
    let _ = std::fs::remove_dir_all(&b_dir);
}
