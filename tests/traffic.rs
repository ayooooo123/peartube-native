//! The gate's decisions must match the app's `src/traffic.js` for the same
//! scripted byte counters, and its busy channel must talk to npm protomux.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peartube_native::traffic::{Connection, Options, TrafficGate};
use protomux::{Mux, StreamInfo, memory};
use serde_json::{Value, json};

const KEY_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const KEY_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn js_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js"))
}

#[derive(Default)]
struct FakeClock(AtomicU64);

impl FakeClock {
    fn advance(&self, ms: u64) {
        self.0.fetch_add(ms, Ordering::Relaxed);
    }
}

impl peartube_native::traffic::Clock for FakeClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

struct FakeConn {
    id: u64,
    written: AtomicU64,
    destroyed: AtomicBool,
    key: Option<[u8; 32]>,
    events: Arc<Mutex<Vec<String>>>,
    mux: Mux,
    // Keeps the far end of the in-memory transport alive so the mux stays up.
    _peer: (memory::MemoryReader, memory::MemoryWriter),
}

impl FakeConn {
    fn new(id: u64, key: Option<[u8; 32]>, events: Arc<Mutex<Vec<String>>>) -> Self {
        let ((reader, writer), peer) = memory::duplex(64);
        Self {
            id,
            written: AtomicU64::new(0),
            destroyed: AtomicBool::new(false),
            key,
            events,
            mux: Mux::new(reader, writer, StreamInfo::default()),
            _peer: peer,
        }
    }

    fn write(&self, bytes: u64) {
        self.written.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl Connection for FakeConn {
    fn id(&self) -> u64 {
        self.id
    }

    fn raw_bytes_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    fn pause(&self) {
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:pause", self.id));
    }

    fn resume(&self) {
        self.events
            .lock()
            .unwrap()
            .push(format!("{}:resume", self.id));
    }

    fn destroyed(&self) -> bool {
        self.destroyed.load(Ordering::Relaxed)
    }

    fn mux(&self) -> Mux {
        self.mux.clone()
    }

    fn remote_public_key(&self) -> Option<[u8; 32]> {
        self.key
    }
}

fn dyn_conn(conn: &Arc<FakeConn>) -> Arc<dyn Connection> {
    conn.clone()
}

fn unhex(s: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).unwrap();
    }
    out
}

fn scenario_a() -> Value {
    json!({
        "uploadMbps": 8.0,
        "steps": [
            {"op": "conn", "id": 1, "key": KEY_A},
            {"op": "conn", "id": 2},
            {"op": "conn", "id": 3, "key": KEY_A},
            {"op": "tick"},
            {"op": "write", "id": 2, "bytes": 31250},
            {"op": "tick", "ms": 250},
            {"op": "viewing", "value": true},
            {"op": "markBulk", "id": 1},
            {"op": "markBulk", "id": 2},
            {"op": "markBulk", "id": 3},
            {"op": "tick", "ms": 250},
            {"op": "write", "id": 1, "bytes": 2_000_000},
            {"op": "write", "id": 3, "bytes": 2_000_000},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 6000},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 250},
            {"op": "unmarkBulk", "id": 1},
            {"op": "tick", "ms": 6000},
            {"op": "tick", "ms": 250},
            {"op": "unmarkBulk", "id": 3},
            {"op": "destroy", "id": 2},
            {"op": "tick", "ms": 250},
            {"op": "conn", "id": 4, "key": KEY_B},
            {"op": "markBulk", "id": 4},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 250},
            {"op": "viewing", "value": false},
            {"op": "tick", "ms": 3001},
            {"op": "stop"},
            {"op": "tick"}
        ]
    })
}

fn scenario_b() -> Value {
    json!({
        "uploadMbps": 0.0,
        "steps": [
            {"op": "conn", "id": 1},
            {"op": "conn", "id": 2},
            {"op": "markBulk", "id": 1},
            {"op": "write", "id": 1, "bytes": 5_000_000},
            {"op": "tick"},
            {"op": "write", "id": 2, "bytes": 40000},
            {"op": "tick", "ms": 250},
            {"op": "write", "id": 2, "bytes": 1000},
            {"op": "tick", "ms": 250},
            {"op": "tick", "ms": 3001},
            {"op": "unmarkBulk", "id": 1},
            {"op": "tick", "ms": 250}
        ]
    })
}

async fn run_scenario(scenario: &Value) -> Value {
    let clock = Arc::new(FakeClock::default());
    let events = Arc::new(Mutex::new(Vec::new()));
    let marks = Arc::new(Mutex::new(Vec::new()));
    let http = Arc::new(AtomicBool::new(false));
    let hook_http = http.clone();
    let hook_marks = marks.clone();
    let gate = TrafficGate::new(Options {
        upload_mbps: scenario["uploadMbps"].as_f64().unwrap(),
        auto_tick: false,
        clock: clock.clone(),
        http_viewing: Some(Arc::new(move || hook_http.load(Ordering::Relaxed))),
        mark_bulk_peer: Some(Arc::new(move |key: &str, bulk| {
            hook_marks.lock().unwrap().push((key.to_owned(), bulk));
        })),
        // The oracle's fake node names its source peers explicitly; no claim is ever made over a
        // channel in the decision scenarios, so the set starts empty on both sides.
        is_source_peer: Some(Arc::new(|_: &str| false)),
        ..Options::default()
    });

    let mut conns: Vec<Arc<FakeConn>> = Vec::new();
    let mut steps = Vec::new();
    for step in scenario["steps"].as_array().unwrap() {
        if let Some(ms) = step["ms"].as_u64() {
            clock.advance(ms);
        }
        let id = step["id"].as_u64().unwrap_or(0);
        let conn = || {
            conns
                .iter()
                .find(|c| c.id == id)
                .unwrap_or_else(|| panic!("no connection {id}"))
                .clone()
        };
        match step["op"].as_str().unwrap() {
            "conn" => {
                let key = step["key"].as_str().map(unhex);
                let created = Arc::new(FakeConn::new(id, key, events.clone()));
                gate.attach(created.clone());
                conns.push(created);
            }
            "write" => conn().write(step["bytes"].as_u64().unwrap()),
            "tick" => gate.tick(),
            "markBulk" => gate.mark_bulk(conn()),
            "unmarkBulk" => gate.unmark_bulk(&dyn_conn(&conn())),
            "destroy" => conn().destroyed.store(true, Ordering::Relaxed),
            "viewing" => http.store(step["value"].as_bool().unwrap(), Ordering::Relaxed),
            "stop" => gate.stop(),
            other => panic!("unknown op {other}"),
        }
        let drained: Vec<String> = events.lock().unwrap().drain(..).collect();
        let marked: Vec<(String, bool)> = marks.lock().unwrap().drain(..).collect();
        steps.push(json!({
            "events": drained,
            "marks": marked,
            "info": gate.info(),
        }));
    }
    json!({ "steps": steps })
}

fn js_decisions(scenario: &Value) -> Value {
    assert!(
        js_dir().join("node_modules/protomux").exists(),
        "run `npm install` in {} first",
        js_dir().display()
    );
    let mut child = Command::new("node")
        .arg("traffic.mjs")
        .arg("decisions")
        .current_dir(js_dir())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("node on PATH");
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(scenario.to_string().as_bytes()).unwrap();
    drop(stdin);
    let out = child.wait_with_output().unwrap();
    assert!(out.status.success(), "node traffic.mjs decisions failed");
    serde_json::from_slice(&out.stdout).expect("decision log")
}

/// Numbers compare as f64; the app prints 8 where Rust prints 8.0.
fn normalize(value: Value) -> Value {
    match value {
        Value::Number(n) => json!(n.as_f64().unwrap_or_default()),
        Value::Array(items) => Value::Array(items.into_iter().map(normalize).collect()),
        Value::Object(map) => {
            Value::Object(map.into_iter().map(|(k, v)| (k, normalize(v))).collect())
        }
        other => other,
    }
}

fn collect_strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Array(items) => items.iter().for_each(|v| collect_strings(v, out)),
        Value::Object(map) => map.values().for_each(|v| collect_strings(v, out)),
        _ => {}
    }
}

fn has_bool(value: &Value, key: &str, expected: bool) -> bool {
    match value {
        Value::Object(map) => {
            map.get(key).and_then(Value::as_bool) == Some(expected)
                || map.values().any(|v| has_bool(v, key, expected))
        }
        Value::Array(items) => items.iter().any(|v| has_bool(v, key, expected)),
        _ => false,
    }
}

#[tokio::test]
#[ignore = "runs the PearTube app's src/traffic.js (checkout at ../peartube or PEARTUBE_APP)"]
async fn decisions_match_the_app() {
    for scenario in [scenario_a(), scenario_b()] {
        let from_js = js_decisions(&scenario);
        let from_rust = run_scenario(&scenario).await;
        assert_eq!(
            normalize(from_rust.clone()),
            normalize(from_js.clone()),
            "scenario at {} Mbps",
            scenario["uploadMbps"]
        );

        // The scripted byte counters must actually reach every decision.
        let mut strings = Vec::new();
        collect_strings(&from_js, &mut strings);
        let paused = strings.iter().any(|s| s.ends_with(":pause"));
        let expected_pause = scenario["uploadMbps"].as_f64().unwrap() > 0.0;
        assert_eq!(paused, expected_pause, "{strings:?}");
        assert_eq!(
            has_bool(&from_js, "bulkPaused", true),
            expected_pause,
            "bulkPaused never reached {expected_pause}"
        );
        if expected_pause {
            assert!(
                strings.iter().any(|s| s.ends_with(":resume")),
                "{strings:?}"
            );
        }
        assert!(has_bool(&from_js, "servingViewers", true));
    }
}

#[tokio::test]
async fn marked_peers_follow_bulk_state() {
    let clock = Arc::new(FakeClock::default());
    let marks = Arc::new(Mutex::new(Vec::new()));
    let hook_marks = marks.clone();
    let gate = TrafficGate::new(Options {
        upload_mbps: 0.0,
        auto_tick: false,
        clock: clock.clone(),
        mark_bulk_peer: Some(Arc::new(move |key: &str, bulk| {
            hook_marks.lock().unwrap().push((key.to_owned(), bulk));
        })),
        // The oracle's fake node names its source peers explicitly; no claim is ever made over a
        // channel in the decision scenarios, so the set starts empty on both sides.
        is_source_peer: Some(Arc::new(|_: &str| false)),
        ..Options::default()
    });
    let events = Arc::new(Mutex::new(Vec::new()));
    let a1 = Arc::new(FakeConn::new(1, Some(unhex(KEY_A)), events.clone()));
    let a2 = Arc::new(FakeConn::new(2, Some(unhex(KEY_A)), events.clone()));
    gate.attach(a1.clone());
    gate.attach(a2.clone());
    assert!(!gate.peers_busy());
    assert_eq!(gate.info().bulk_peers, 0);

    gate.mark_bulk(a1.clone());
    gate.mark_bulk(a2.clone());
    gate.mark_bulk(a1.clone());
    assert_eq!(gate.info().bulk_peers, 2);
    assert_eq!(
        marks.lock().unwrap().drain(..).collect::<Vec<_>>(),
        vec![(KEY_A.to_owned(), true), (KEY_A.to_owned(), true)]
    );

    // The peer stays bulk while another of its connections still is.
    gate.unmark_bulk(&dyn_conn(&a1));
    assert!(marks.lock().unwrap().is_empty());
    gate.unmark_bulk(&dyn_conn(&a2));
    assert_eq!(
        marks.lock().unwrap().drain(..).collect::<Vec<_>>(),
        vec![(KEY_A.to_owned(), false)]
    );
    assert_eq!(gate.info().bulk_peers, 0);

    gate.stop();
    assert!(!gate.info().peers_busy);
}

struct StreamConn {
    mux: Mux,
    raw: AtomicU64,
    destroyed: AtomicBool,
}

impl Connection for StreamConn {
    fn id(&self) -> u64 {
        1
    }

    fn raw_bytes_written(&self) -> u64 {
        self.raw.load(Ordering::Relaxed)
    }

    fn pause(&self) {}

    fn resume(&self) {}

    fn destroyed(&self) -> bool {
        self.destroyed.load(Ordering::Relaxed)
    }

    fn mux(&self) -> Mux {
        self.mux.clone()
    }
}

#[tokio::test]
#[ignore = "spawns a node peer over a TCP secret stream"]
async fn busy_channel_interop() {
    use peeroxide_dht::{noise, secret_stream::SecretStream};
    use tokio::io::{AsyncBufReadExt, BufReader, Lines};
    use tokio::net::TcpStream;
    use tokio::process::{ChildStdout, Command};

    async fn next_line(lines: &mut Lines<BufReader<ChildStdout>>) -> String {
        lines
            .next_line()
            .await
            .unwrap()
            .expect("peer closed stdout")
    }

    async fn wait_until(cond: impl Fn() -> bool) {
        for _ in 0..500 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition not reached");
    }

    assert!(
        js_dir().join("node_modules/protomux").exists(),
        "run `npm install` in {} first",
        js_dir().display()
    );
    let mut child = Command::new("node")
        .arg("traffic.mjs")
        .arg("peer")
        .current_dir(js_dir())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("node on PATH");
    let stdout = child.stdout.take().unwrap();
    let mut lines = BufReader::new(stdout).lines();
    let port_line = next_line(&mut lines).await;
    let port: u16 = port_line
        .strip_prefix("port ")
        .expect("port line")
        .parse()
        .unwrap();

    let clock = Arc::new(FakeClock::default());
    let http = Arc::new(AtomicBool::new(false));
    let hook_http = http.clone();
    let gate = TrafficGate::new(Options {
        auto_tick: false,
        clock: clock.clone(),
        http_viewing: Some(Arc::new(move || hook_http.load(Ordering::Relaxed))),
        ..Options::default()
    });

    let exchange = async {
        let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let stream = SecretStream::new(true, tcp, noise::generate_keypair())
            .await
            .unwrap();
        let conn = Arc::new(StreamConn {
            mux: Mux::from_secret_stream(stream),
            raw: AtomicU64::new(0),
            destroyed: AtomicBool::new(false),
        });
        gate.attach(conn);

        // Both sides open; the gate reports "not viewing" first, JS echoes it.
        assert_eq!(next_line(&mut lines).await, "open");
        assert_eq!(next_line(&mut lines).await, "msg 0");
        assert!(!gate.peers_busy());

        http.store(true, Ordering::Relaxed);
        gate.tick();
        assert_eq!(next_line(&mut lines).await, "msg 1");
        wait_until(|| gate.peers_busy()).await;
        assert!(gate.info().serving_viewers);

        http.store(false, Ordering::Relaxed);
        clock.advance(3001);
        gate.tick();
        assert_eq!(next_line(&mut lines).await, "msg 0");
        wait_until(|| !gate.peers_busy()).await;
        assert!(!gate.info().serving_viewers);
    };
    let result = tokio::time::timeout(Duration::from_secs(20), exchange).await;
    child.kill().await.ok();
    result.expect("busy channel exchange timed out");
}

/// A connection the test drives from both ends, for the busy channel's wire protocol.
struct WireConn {
    mux: Mux,
    key: [u8; 32],
}

impl Connection for WireConn {
    fn id(&self) -> u64 {
        1
    }
    fn raw_bytes_written(&self) -> u64 {
        0
    }
    fn pause(&self) {}
    fn resume(&self) {}
    fn destroyed(&self) -> bool {
        false
    }
    fn mux(&self) -> Mux {
        self.mux.clone()
    }
    fn remote_public_key(&self) -> Option<[u8; 32]> {
        Some(self.key)
    }
}

/// `peersBusy` honours a `peartube/bulk-busy` claim only from a peer that served this node blocks:
/// a stranger can open the channel, and counting its claim would hold every relay's backfill.
#[tokio::test]
async fn a_busy_claim_counts_only_from_a_source_peer() {
    use protomux::ChannelOptions;

    for (source, expected) in [(false, false), (true, true)] {
        let ((local_read, local_write), (peer_read, peer_write)) = protomux::memory::duplex(64);
        let conn = Arc::new(WireConn {
            mux: Mux::new(local_read, local_write, StreamInfo::default()),
            key: unhex(KEY_A),
        });
        let peer = Mux::new(peer_read, peer_write, StreamInfo::default());
        let gate = TrafficGate::new(Options {
            auto_tick: false,
            is_source_peer: Some(Arc::new(move |key: &str| source && key == KEY_A)),
            ..Options::default()
        });
        gate.attach(conn);

        // The gate opens its busy channel on attach; the peer answers with its own open and claims
        // viewers. Pairing first is what makes the remote open land instead of being rejected.
        let mut pairs = peer.pair(peartube_native::traffic::BUSY_PROTOCOL, None);
        let request = tokio::time::timeout(Duration::from_secs(5), pairs.recv())
            .await
            .expect("the remote open arrives")
            .expect("a pair request");
        drop(request);
        let channel = peer
            .open_channel(ChannelOptions::new(peartube_native::traffic::BUSY_PROTOCOL))
            .expect("peer channel");
        channel.sender().send(0, &1u64);

        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        while tokio::time::Instant::now() < deadline {
            if gate.peers_busy() == expected && gate.info().peers_busy == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            gate.peers_busy(),
            expected,
            "a busy claim counts only from a source peer"
        );
        assert_eq!(gate.info().peers_busy, expected);
        gate.stop();
    }
}

/// The writer side of one real replication connection: its frames are counted, so the gate's
/// measured upload is what the peer actually sent, and each write waits a little so the transfer
/// is still running when the connection is held.
struct CountingWrite<W>(W, Arc<AtomicU64>);

impl<W: protomux::FrameWrite> protomux::FrameWrite for CountingWrite<W> {
    async fn write(&mut self, frame: bytes::Bytes) -> std::io::Result<()> {
        self.1.fetch_add(frame.len() as u64, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(4)).await;
        self.0.write(frame).await
    }

    async fn close(&mut self) -> std::io::Result<()> {
        self.0.close().await
    }
}

/// One real connection as the gate sees it: its upload is the peer's frame bytes, and its pause
/// and resume move the read gate the mux reads through.
struct RealConn {
    id: u64,
    key: [u8; 32],
    written: Arc<AtomicU64>,
    gate: peartube_native::node::network::ReadGate,
    mux: Mux,
}

impl Connection for RealConn {
    fn id(&self) -> u64 {
        self.id
    }
    fn raw_bytes_written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
    fn pause(&self) {
        self.gate.pause();
    }
    fn resume(&self) {
        self.gate.resume();
    }
    fn destroyed(&self) -> bool {
        false
    }
    fn mux(&self) -> Mux {
        self.mux.clone()
    }
    fn remote_public_key(&self) -> Option<[u8; 32]> {
        Some(self.key)
    }
}

/// `conn.pause()` stops the mux polling this connection's inbound stream: the peer's blocks stop
/// arriving until it is resumed, and the gate's measured upload to a held bulk peer stays inside
/// its cap.
#[tokio::test]
async fn a_paused_connection_receives_no_new_blocks_until_resumed() {
    use peartube_native::node::network::{GatedRead, ReadGate};
    use protomux::StreamInfo;

    let dir_writer = tempfile::tempdir().unwrap();
    let dir_reader = tempfile::tempdir().unwrap();
    let writer_store = corestore::Corestore::open(dir_writer.path()).unwrap();
    let reader_store = corestore::Corestore::open(dir_reader.path()).unwrap();
    let writer = writer_store
        .get(corestore::GetOptions {
            key_pair: Some(hypercore::crypto::KeyPair::from_seed(&[9; 32])),
            ..Default::default()
        })
        .await
        .unwrap();
    let blocks: Vec<Vec<u8>> = (0..64u8).map(|i| vec![i; 64 * 1024]).collect();
    let blocks_len = blocks.len() as u64;
    writer.append(&blocks).await.unwrap();
    let reader = reader_store
        .get(corestore::GetOptions::key(*writer.key()))
        .await
        .unwrap();
    let _active = reader.core().session(true);

    let writer_key = *writer.core().key();
    let reader_key = reader_store.create_key_pair("reader").public_key;
    let ((writer_read, writer_write), (reader_read, reader_write)) = protomux::memory::duplex(64);
    let written = Arc::new(AtomicU64::new(0));
    let handshake = Some([9u8; 64]);
    let peer_info = |initiator: bool, remote: [u8; 32]| StreamInfo {
        is_initiator: initiator,
        handshake_hash: handshake,
        remote_public_key: Some(remote),
    };
    let writer_mux = Mux::new(
        writer_read,
        CountingWrite(writer_write, written.clone()),
        peer_info(true, reader_key),
    );
    let gate = ReadGate::new();
    let reader_mux = Mux::new(
        GatedRead::new(reader_read, gate.clone()),
        reader_write,
        peer_info(false, writer_key),
    );
    writer.core().replicate(&writer_mux);
    reader.core().replicate(&reader_mux);

    let conn = Arc::new(RealConn {
        id: 1,
        key: writer_key,
        written: written.clone(),
        gate: gate.clone(),
        mux: reader_mux.clone(),
    });
    // The gate sees the peer as bulk before any of its bytes arrive, and a viewer is streaming, so
    // the tick that counts those bytes has to hold it.
    let clock = Arc::new(FakeClock::default());
    let gate_ctl = TrafficGate::new(Options {
        upload_mbps: 0.5,
        auto_tick: false,
        clock: clock.clone(),
        http_viewing: Some(Arc::new(|| true)),
        ..Options::default()
    });
    let conn: Arc<dyn Connection> = conn.clone();
    gate_ctl.attach(conn.clone());
    gate_ctl.mark_bulk(conn.clone());

    let download = tokio::spawn({
        let reader = reader.core().clone();
        async move { reader.download(0, Some(blocks_len), true).await }
    });
    // Let the transfer get going: its bytes are what the gate measures.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while reader.core().count_local(0, blocks_len) < 8 {
        assert!(tokio::time::Instant::now() < deadline, "the transfer starts");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    clock.advance(250);
    gate_ctl.tick();
    assert!(
        gate_ctl.info().bulk_paused,
        "a viewer's presence pauses the bulk peer"
    );
    // Whatever the mux had already read when the pause landed still lands (the actor may hold a
    // couple of frames); after that nothing more arrives, because blocks need reads.
    let mut last = (
        reader.core().count_local(0, blocks_len),
        written.load(Ordering::Relaxed),
    );
    let settle_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        tokio::time::sleep(Duration::from_millis(150)).await;
        let now = (
            reader.core().count_local(0, blocks_len),
            written.load(Ordering::Relaxed),
        );
        if now == last {
            break;
        }
        assert!(
            tokio::time::Instant::now() < settle_deadline,
            "the pause settles"
        );
        last = now;
    }
    assert!(
        last.0 < blocks_len,
        "a paused connection stops the transfer ({} of {blocks_len} blocks)",
        last.0
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        (
            reader.core().count_local(0, blocks_len),
            written.load(Ordering::Relaxed)
        ),
        last,
        "a paused connection takes no new blocks and the peer sends nothing"
    );

    // Held bulk connections stay inside the cap: turns are on a 6 s schedule, not inside this
    // second, so nothing more is served.
    let before = written.load(Ordering::Relaxed);
    for _ in 0..4 {
        clock.advance(250);
        gate_ctl.tick();
    }
    let growth = written.load(Ordering::Relaxed).saturating_sub(before);
    assert_eq!(growth, 0, "a held bulk connection serves nothing");

    // Resuming lets the transfer finish.
    gate.resume();
    gate_ctl.stop();
    tokio::time::timeout(Duration::from_secs(20), download)
        .await
        .expect("the transfer resumes")
        .expect("download task")
        .expect("download result");
    assert_eq!(reader.core().count_local(0, blocks_len), blocks_len);
    writer_mux.destroy();
    reader_mux.destroy();
}
