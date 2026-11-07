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
