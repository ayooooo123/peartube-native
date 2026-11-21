use std::future::Future;
use std::net::{IpAddr, Ipv4Addr};
use std::pin::Pin;
use std::process::Command;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};

use peartube_native::backend::{Adapters, Backend};
use peartube_native::ipc::{Framer, Incoming, decode, response};
use serde_json::{Value, json};

#[test]
fn fragmented_lines_and_events() {
    let mut framer = Framer::new();
    let mut lines = Vec::new();
    framer.push(b"\n{\"event\":\"bonjour\",", |line| {
        lines.push(line.to_owned())
    });
    framer.push(
        b"\"value\":1}\n{\"id\":2,\"method\":\"status\"}\n",
        |line| lines.push(line.to_owned()),
    );
    assert_eq!(
        decode(&lines[0]),
        Incoming::Event(json!({"event":"bonjour","value":1}))
    );
    assert_eq!(
        decode(&lines[1]),
        Incoming::Request {
            id: Some(json!(2)),
            method: json!("status"),
            params: json!({})
        }
    );
}

#[test]
fn malformed_and_missing_id() {
    assert_eq!(decode("{"), Incoming::Malformed);
    assert_eq!(
        response(None, Err("Not started".into())),
        "{\"error\":\"Not started\"}\n"
    );
    assert_eq!(
        response(Some(json!(null)), Ok(json!(null))),
        "{\"id\":null,\"result\":null}\n"
    );
}

#[test]
fn lone_surrogates_are_replaced_like_to_well_formed() {
    let line = "{\"id\":1,\"method\":\"echo\",\"params\":{\"text\":\"a\\ud800b\"}}";
    assert_eq!(
        decode(line),
        Incoming::Request {
            id: Some(json!(1)),
            method: json!("echo"),
            params: json!({"text": "a\u{fffd}b"}),
        }
    );
}

mod fake {
    use super::*;
    use futures_util::future::BoxFuture;
    use peartube_native::node::api::*;
    use peartube_native::node::{Node, PutSource};
    use tokio::sync::watch;

    #[derive(Debug)]
    pub struct FakeError(String);

    impl std::fmt::Display for FakeError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl std::error::Error for FakeError {}

    pub struct FakeSwarm;

    impl Swarm for FakeSwarm {
        type Error = FakeError;
        async fn suspend(&self) -> Result<(), FakeError> {
            Ok(())
        }
        async fn resume(&self) -> Result<(), FakeError> {
            Ok(())
        }
        fn on_update(&self, _callback: Arc<dyn Fn() + Send + Sync>) -> Box<dyn Fn() + Send + Sync> {
            Box::new(|| {})
        }
    }

    /// The fake's store: [`Gate::access`] waits while it is parked, as a store access does.
    #[derive(Default)]
    pub struct Gate {
        /// (parked, closed)
        state: Mutex<(bool, bool)>,
        woke: Condvar,
    }

    impl Gate {
        fn state(&self) -> std::sync::MutexGuard<'_, (bool, bool)> {
            self.state.lock().unwrap_or_else(|e| e.into_inner())
        }

        pub fn parked(&self) -> bool {
            self.state().0
        }

        fn access(&self) {
            let mut state = self.state();
            while state.0 && !state.1 {
                state = self.woke.wait(state).unwrap_or_else(|e| e.into_inner());
            }
        }
    }

    impl StoreSuspension for Gate {
        fn suspend(&self) -> std::io::Result<()> {
            self.state().0 = true;
            Ok(())
        }
        fn resume(&self) -> std::io::Result<()> {
            self.state().0 = false;
            self.woke.notify_all();
            Ok(())
        }
        fn close(&self) {
            self.state().1 = true;
            self.woke.notify_all();
        }
    }

    pub struct FakePutSource;

    impl PutSource for FakePutSource {
        type Error = FakeError;
        async fn open(
            &mut self,
            _offset: u64,
            _meta: &mut PutMeta,
        ) -> Result<Pin<Box<dyn tokio::io::AsyncRead + Send>>, Self::Error> {
            Err(FakeError("no source".into()))
        }
        fn on_data(&mut self, _bytes: u64) {}
        fn on_hash_state(&mut self, _state: &[u8], _bytes: u64) {}
    }

    pub struct FakeNode {
        fixtures: Value,
        gate: Arc<Gate>,
        lan: Mutex<bool>,
        changes: watch::Sender<u64>,
    }

    impl FakeNode {
        pub fn new(fixtures: Value, gate: Arc<Gate>) -> Self {
            Self {
                fixtures,
                gate,
                lan: Mutex::new(false),
                changes: watch::channel(0).0,
            }
        }

        fn fixture(&self, key: &str) -> Value {
            self.fixtures[key].clone()
        }

        fn typed<T: serde::de::DeserializeOwned>(&self, key: &str) -> T {
            serde_json::from_value(self.fixture(key)).expect("fixture shape")
        }
    }

    impl Node for FakeNode {
        type Error = FakeError;
        type Store = ();
        type Core = ();
        type Channel = ();
        type Swarm = FakeSwarm;
        type Dht = ();
        type Connection = ();
        type ConnectionSubscription = ();
        type Lan = ();
        type BlobServer = ();
        type PutSource = FakePutSource;
        type RechunkOptions = ();

        fn store(&self) -> &() {
            &()
        }
        fn channel(&self) -> &() {
            &()
        }
        fn swarm(&self) -> &FakeSwarm {
            static SWARM: FakeSwarm = FakeSwarm;
            &SWARM
        }
        fn store_suspension(&self) -> Arc<dyn StoreSuspension> {
            self.gate.clone()
        }
        fn dht(&self) -> &() {
            &()
        }
        fn blob_server(&self) -> &() {
            &()
        }
        fn private_routing(&self) -> bool {
            false
        }
        fn bootstrap(&self) -> Option<Value> {
            None
        }
        fn relay_through(&self) -> Option<Value> {
            None
        }
        fn peer_key(&self) -> String {
            String::new()
        }
        fn closing(&self) -> bool {
            false
        }
        fn peer_connections(&self) -> Vec<()> {
            Vec::new()
        }
        fn on_connection(&self, _callback: Box<dyn Fn(()) + Send + Sync>) {}
        fn onchange(&self) -> watch::Receiver<u64> {
            self.changes.subscribe()
        }
        async fn find_entry(&self, _key: &str) -> Result<Option<Value>, FakeError> {
            Ok(self.fixture("entries").get(0).cloned())
        }
        async fn blob_core(&self, _key: &str) -> Result<(), FakeError> {
            Ok(())
        }
        async fn seedable_cores(
            &self,
            _include_cached: bool,
        ) -> Result<Vec<SeedableCore>, FakeError> {
            Ok(Vec::new())
        }
        async fn has_lan(&self) -> bool {
            *self.lan.lock().unwrap_or_else(|e| e.into_inner())
        }
        async fn lan_endpoint(&self) -> Option<LanEndpoint> {
            None
        }
        async fn dial_lan_peer(&self, _endpoint: LanEndpoint) {}
        async fn join_peer(&self, _key: &str, _lan_first: bool) {}
        async fn evict_behind(
            &self,
            _blobs: &str,
            _blob: Blob,
            _options: Value,
        ) -> Result<u64, FakeError> {
            Ok(0)
        }
        async fn refresh_block_count(&self, _core: &()) -> Result<(), FakeError> {
            Ok(())
        }
        async fn search(
            &self,
            _id: Option<&str>,
            _options: SearchOptions,
        ) -> Result<Vec<Entry>, FakeError> {
            self.gate.access();
            Ok(serde_json::from_value(self.fixture("entries")).expect("entries"))
        }
        async fn put(&self, _meta: PutMeta, _source: FakePutSource) -> Result<Announce, FakeError> {
            Err(FakeError("unused".into()))
        }
        async fn drop_store(&self, _name: &str) -> Result<(), FakeError> {
            Ok(())
        }
        async fn append(&self, _op: Value) -> Result<(), FakeError> {
            Ok(())
        }
        async fn publish(
            &self,
            _meta: PutMeta,
            _source: FakePutSource,
        ) -> Result<Published, FakeError> {
            Err(FakeError("unused".into()))
        }
        async fn publish_channel(
            &self,
            _meta_or_op: Value,
            _source: Option<FakePutSource>,
        ) -> Result<Published, FakeError> {
            Err(FakeError("unused".into()))
        }
        async fn remove(&self, _key: &str) -> Result<(), FakeError> {
            Ok(())
        }
        async fn publish_source(
            &self,
            _source_key: &str,
            _title: &str,
        ) -> Result<Value, FakeError> {
            Ok(Value::Null)
        }
        async fn sync_local_channel(&self) -> Result<SyncResult, FakeError> {
            Ok(SyncResult {
                backfilled: 0,
                total: 0,
            })
        }
        async fn rechunk_legacy_blobs(&self, _options: ()) -> Result<RechunkResult, FakeError> {
            Ok(RechunkResult {
                total: 0,
                migrated: 0,
                deduped: 0,
                remaining: 0,
                migrated_bytes: 0,
            })
        }
        async fn unpublish_source(&self, _key: &str) -> Result<Value, FakeError> {
            Ok(Value::Null)
        }
        async fn published_sources(&self) -> Result<Vec<PublishedSource>, FakeError> {
            Ok(Vec::new())
        }
        async fn add_source(&self, _spec: SourceSpec, _wake: bool) -> Result<Source, FakeError> {
            Ok(self.typed("source"))
        }
        async fn remove_source(&self, _key: &str) -> Result<Removed, FakeError> {
            Ok(Removed { removed: true })
        }
        async fn list_sources(&self) -> Vec<Source> {
            self.typed("sources")
        }
        async fn focus_source(&self, _key: &str) -> Result<bool, FakeError> {
            Ok(self.fixture("focus").as_bool().unwrap_or(false))
        }
        async fn set_player_active(&self, _active: bool) -> OkResult {
            OkResult { ok: true }
        }
        async fn active_source_count(&self) -> usize {
            0
        }
        async fn overlay_topic_count(&self) -> usize {
            0
        }
        async fn discovered_sources(&self) -> Result<Vec<DiscoveredSource>, FakeError> {
            Ok(self.typed("discovered"))
        }
        async fn open_uri(&self, _uri: &str, _join_source: bool) -> Result<OpenUri, FakeError> {
            Ok(self.typed("openUri"))
        }
        async fn warm(&self, _entry: &Value) -> Result<(), FakeError> {
            Ok(())
        }
        async fn identity(&self, _reveal_mnemonic: bool) -> Result<Identity, FakeError> {
            Ok(self.typed("identity"))
        }
        async fn grant_writer(
            &self,
            _writer: &str,
            _role: &str,
            _alias: &str,
        ) -> Result<Grant, FakeError> {
            Ok(Grant {
                granted: true,
                writer: String::new(),
                role: String::new(),
                alias: String::new(),
            })
        }
        async fn revoke_writer(
            &self,
            _writer: &str,
            _purge_entries: bool,
        ) -> Result<Revoke, FakeError> {
            Ok(self.typed("revoke"))
        }
        async fn claim_with_mnemonic(
            &self,
            _mnemonic: &str,
            _alias: &str,
        ) -> Result<Claim, FakeError> {
            Ok(Claim {
                claimed: true,
                reopen_needed: false,
                channel: String::new(),
                writer: None,
            })
        }
        async fn rotate_epoch(&self) -> Result<Epoch, FakeError> {
            Ok(self.typed("epoch"))
        }
        async fn set_allowed_peers(&self, _peers: Option<Vec<String>>) -> Result<(), FakeError> {
            Ok(())
        }
        async fn clear_cache(
            &self,
            _key: Option<&str>,
            _keep_blobs_hex: Option<&str>,
        ) -> Result<CacheCleared, FakeError> {
            Ok(self.typed("cacheCleared"))
        }
        async fn set_cache_max_bytes(&self, bytes: f64) -> u64 {
            bytes as u64
        }
        async fn enforce_cache_ceiling(
            &self,
            _keep_hex: Option<&str>,
        ) -> Result<CacheCeiling, FakeError> {
            Ok(CacheCeiling {
                evicted: 0,
                cached_bytes: 0,
            })
        }
        async fn gc_storage(&self, _full: bool, _streaming: bool) -> Result<StorageGc, FakeError> {
            Ok(StorageGc {
                before_bytes: 0,
                after_bytes: 0,
                reclaimed_bytes: 0,
            })
        }
        async fn status(&self) -> Status {
            self.typed("status")
        }
        async fn stream_url(&self, _entry: &Announce) -> String {
            String::new()
        }
        async fn http_viewing(&self) -> bool {
            false
        }
        async fn mark_bulk_peer(&self, _key: &str, _on: bool) {}
        async fn is_source_peer(&self, _key: &str) -> bool {
            false
        }
        async fn set_lan(&self, lan: Option<()>) -> Result<(), FakeError> {
            *self.lan.lock().unwrap_or_else(|e| e.into_inner()) = lan.is_some();
            Ok(())
        }
        async fn lan_stats(&self) -> Option<Value> {
            None
        }
        async fn close(&self) -> Result<(), FakeError> {
            Ok(())
        }
    }

    pub struct FakeFactory {
        pub fixtures: Value,
        pub gate: Arc<Gate>,
    }

    impl NodeFactory for FakeFactory {
        type Node = FakeNode;
        type Error = FakeError;
        async fn create(&self, _options: Value) -> Result<Arc<FakeNode>, FakeError> {
            Ok(Arc::new(FakeNode::new(
                self.fixtures.clone(),
                self.gate.clone(),
            )))
        }
    }

    pub struct FakeLan {
        pub interfaces: Arc<Mutex<Vec<(String, IpAddr, bool)>>>,
        pub events: Mutex<Vec<Value>>,
    }

    impl LanFactory<FakeNode> for FakeLan {
        type Error = FakeError;
        async fn interfaces(&self) -> Result<Vec<(String, IpAddr, bool)>, FakeError> {
            Ok(self
                .interfaces
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone())
        }
        async fn create(&self, _host: Ipv4Addr, _node: &FakeNode) -> Result<(), FakeError> {
            Ok(())
        }
        async fn host_event(&self, event: Value) {
            self.events
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event);
        }
    }

    pub struct FakeApi {
        pub url: String,
        pub notified: Mutex<Vec<String>>,
    }

    impl ApiServer<FakeNode> for FakeApi {
        type Error = FakeError;
        async fn start(
            &self,
            _node: Arc<FakeNode>,
            _on_sources: Arc<dyn Fn(Vec<Source>) + Send + Sync>,
        ) -> Result<Option<String>, FakeError> {
            Ok(Some(self.url.clone()))
        }
        fn notify(&self, event: &str) {
            self.notified
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(event.to_owned());
        }
        async fn close(&self) {}
    }

    pub struct FakeCast {
        pub fixtures: Value,
        pub on_change: Arc<dyn Fn() + Send + Sync>,
    }

    impl FakeCast {
        fn fixture(&self, key: &str) -> Value {
            self.fixtures[key].clone()
        }
    }

    impl Cast for FakeCast {
        fn set_lan_host(&self, _host: Option<String>) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn status(&self) -> Value {
            self.fixture("castStatus")
        }
        fn pair_receiver<'a>(&'a self, _params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
            Box::pin(async move {
                (self.on_change)();
                Ok(self.fixture("receiver"))
            })
        }
        fn remove_receiver(&self, _id: &str) -> Value {
            (self.on_change)();
            self.fixture("castStatus")
        }
        fn cast_play<'a>(&'a self, _params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
            Box::pin(async move { Ok(self.fixture("castPlay")) })
        }
        fn cast_attach<'a>(&'a self, _params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
            Box::pin(async move { Ok(self.fixture("castAttach")) })
        }
        fn cast_control<'a>(&'a self, _params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
            Box::pin(async move { Ok(self.fixture("castControl")) })
        }
        fn cast_stop<'a>(&'a self, _params: &'a Value) -> BoxFuture<'a, Result<Value, String>> {
            Box::pin(async move { Ok(self.fixture("castStop")) })
        }
        fn start_local_receiver(
            &self,
            _options: ReceiverStart,
            on_command: Arc<dyn Fn(Value) + Send + Sync>,
        ) -> BoxFuture<'_, Result<Value, String>> {
            Box::pin(async move {
                on_command(self.fixture("receiverCommand"));
                Ok(self.fixture("receiverStart"))
            })
        }
        fn report_local_receiver_state<'a>(
            &'a self,
            _patch: &'a Value,
        ) -> BoxFuture<'a, Option<Value>> {
            Box::pin(async move { Some(self.fixture("receiverState")) })
        }
        fn close(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    pub struct FakeCastFactory {
        pub fixtures: Value,
    }

    impl CastFactory<FakeNode> for FakeCastFactory {
        async fn create(&self, options: CastOptions<FakeNode>) -> Result<Arc<dyn Cast>, String> {
            Ok(Arc::new(FakeCast {
                fixtures: self.fixtures.clone(),
                on_change: options.on_change,
            }))
        }
        async fn peer_storage(&self, candidate: Option<&str>, fallback: &str) -> String {
            candidate
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{fallback}/peer"))
        }
        fn reclaim(&self, _storage: &str) {}
        fn receiver_key_pair(&self, _storage: &str) -> Option<Value> {
            None
        }
    }

    /// A clock the test advances. An advance returns only after the worker went idle at the new
    /// time, so it has run every timer due by then.
    pub struct TestClock {
        state: Mutex<ClockState>,
        idle: Condvar,
    }

    struct ClockState {
        now: u64,
        /// Counts the advances. The worker's wait records the one it last went idle at.
        advances: u64,
        idle_at: u64,
        waker: Option<Waker>,
    }

    impl TestClock {
        pub fn new(now: u64) -> Self {
            Self {
                state: Mutex::new(ClockState {
                    now,
                    advances: 0,
                    idle_at: 0,
                    waker: None,
                }),
                idle: Condvar::new(),
            }
        }

        fn state(&self) -> std::sync::MutexGuard<'_, ClockState> {
            self.state.lock().unwrap_or_else(|e| e.into_inner())
        }

        /// The time moves and the waker is taken under one lock with the wait's own check, so the
        /// worker cannot go idle on the old time and count for this advance.
        pub fn advance(&self, ms: u64) {
            let (advance, waker) = {
                let mut state = self.state();
                state.now += ms;
                state.advances += 1;
                (state.advances, state.waker.take())
            };
            if let Some(waker) = waker {
                waker.wake();
            }
            let mut state = self.state();
            while state.idle_at < advance {
                state = self.idle.wait(state).unwrap_or_else(|e| e.into_inner());
            }
        }
    }

    struct Wait<'a> {
        clock: &'a TestClock,
        deadline: u64,
    }

    impl Future for Wait<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let mut state = self.clock.state();
            if state.now >= self.deadline {
                return Poll::Ready(());
            }
            state.waker = Some(cx.waker().clone());
            state.idle_at = state.advances;
            self.clock.idle.notify_all();
            Poll::Pending
        }
    }

    impl Clock for TestClock {
        fn now_ms(&self) -> u64 {
            self.state().now
        }
        fn wait_until(&self, deadline_ms: u64) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            Box::pin(Wait {
                clock: self,
                deadline: deadline_ms,
            })
        }
    }
}

use fake::*;

fn flatten_interfaces(value: &Value) -> Vec<(String, IpAddr, bool)> {
    let mut flat = Vec::new();
    if let Value::Object(fields) = value {
        for (name, entries) in fields {
            for entry in entries.as_array().into_iter().flatten() {
                let address: IpAddr = entry["address"].as_str().unwrap().parse().unwrap();
                let internal = entry["internal"].as_bool().unwrap_or(false);
                flat.push((name.clone(), address, internal));
            }
        }
    }
    flat
}

/// Run the scenario on the Rust backend and collect every line it wrote.
fn run_scenario(scenario: &Value) -> Vec<String> {
    let fixtures = scenario["fixtures"].clone();
    let lines = Arc::new(Mutex::new(Vec::<String>::new()));
    let clock = Arc::new(TestClock::new(scenario["start"].as_u64().unwrap()));
    let interfaces = Arc::new(Mutex::new(flatten_interfaces(&scenario["interfaces"])));
    let sink = lines.clone();
    let backend = Backend::spawn_with(
        json!({}),
        Adapters {
            factory: Arc::new(FakeFactory {
                fixtures: fixtures.clone(),
                gate: Arc::default(),
            }),
            clock: clock.clone(),
            lan: Arc::new(FakeLan {
                interfaces: interfaces.clone(),
                events: Mutex::new(Vec::new()),
            }),
            api: Some(Arc::new(FakeApi {
                url: format!("http://127.0.0.1:{}", fixtures["apiPort"].as_u64().unwrap()),
                notified: Mutex::new(Vec::new()),
            })),
            cast: Some(Arc::new(FakeCastFactory {
                fixtures: fixtures.clone(),
            })),
        },
        move |line: String| sink.lock().unwrap_or_else(|e| e.into_inner()).push(line),
    )
    .expect("backend starts");
    for step in scenario["steps"].as_array().unwrap() {
        if let Some(line) = step.get("line").and_then(Value::as_str) {
            backend.send(line).unwrap();
        } else if let Some(ms) = step.get("advance").and_then(Value::as_u64) {
            clock.advance(ms);
            continue;
        } else if let Some(steps) = step.get("interfaces") {
            *interfaces.lock().unwrap_or_else(|e| e.into_inner()) = flatten_interfaces(steps);
        } else if let Some(linger) = step.get("suspend").and_then(Value::as_u64) {
            backend.suspend(linger);
        } else if step.get("resume").and_then(Value::as_bool) == Some(true) {
            backend.resume();
        }
        // The step is applied before the clock moves: a line that the worker still handles, or a
        // suspend that the suspension thread still holds, would read the next step's time.
        backend.caught_up();
        clock.advance(0);
    }
    backend.shutdown();
    lines.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

fn scenario() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js/ipc-scenario.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn js_lines() -> Vec<String> {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js");
    let output = Command::new("node")
        .arg("ipc-parity.mjs")
        .current_dir(dir)
        .env(
            "PC_SCENARIO",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js/ipc-scenario.json"),
        )
        .output()
        .expect("node runs the parity harness");
    assert!(
        output.status.success(),
        "node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| format!("{line}\n"))
        .collect()
}

#[test]
#[ignore = "needs node and the PearTube app checkout"]
fn worker_matches_worker_js_line_for_line() {
    let scenario = scenario();
    let rust = run_scenario(&scenario);
    let js = js_lines();
    let mismatch = rust
        .iter()
        .zip(&js)
        .position(|(a, b)| a != b)
        .or_else(|| (rust.len() != js.len()).then_some(rust.len().min(js.len())));
    assert!(
        mismatch.is_none(),
        "first difference at line {:?}:\nrust: {:?}\n js : {:?}\nrust has {} lines, js has {}",
        mismatch,
        mismatch.map(|index| rust.get(index)),
        mismatch.map(|index| js.get(index)),
        rust.len(),
        js.len()
    );
}

#[test]
fn backend_processes_lines_on_owned_thread() {
    let (tx, rx) = std::sync::mpsc::channel();
    let backend = Backend::spawn(json!({}), move |line| {
        tx.send(line).unwrap();
    })
    .unwrap();
    backend.send("{\"id\":1,\"method\":\"search\"}").unwrap();
    assert_eq!(
        rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
        "{\"id\":1,\"error\":\"Not started\"}\n"
    );
    backend.shutdown();
    assert!(backend.send("{}").is_err());
}

const LONG: std::time::Duration = std::time::Duration::from_secs(20);
/// How long a request must stay unanswered to count as waiting in the store.
const BLOCKED: std::time::Duration = std::time::Duration::from_millis(300);

/// A started backend over the fakes, whose node store is `gate`; the lines it writes arrive
/// on the receiver.
fn gated_backend(gate: &Arc<Gate>) -> (Backend, std::sync::mpsc::Receiver<Value>) {
    let fixtures = scenario()["fixtures"].clone();
    let (sink, lines) = std::sync::mpsc::channel();
    let backend = Backend::spawn_with(
        json!({}),
        Adapters {
            factory: Arc::new(FakeFactory {
                fixtures: fixtures.clone(),
                gate: gate.clone(),
            }),
            clock: Arc::new(TestClock::new(0)),
            lan: Arc::new(FakeLan {
                interfaces: Arc::default(),
                events: Mutex::new(Vec::new()),
            }),
            api: Some(Arc::new(FakeApi {
                url: format!("http://127.0.0.1:{}", fixtures["apiPort"].as_u64().unwrap()),
                notified: Mutex::new(Vec::new()),
            })),
            cast: Some(Arc::new(FakeCastFactory { fixtures })),
        },
        move |line: String| {
            let _ = sink.send(serde_json::from_str(line.trim()).expect("a JSON line"));
        },
    )
    .expect("backend starts");
    backend
        .send(r#"{"id":1,"method":"start","params":{"trackersDir":"/tmp/trackers","defaultFolder":"default","settings":{}}}"#)
        .unwrap();
    let started = reply(&lines, 1, LONG).expect("start answers");
    assert!(started.get("error").is_none(), "start failed: {started}");
    (backend, lines)
}

/// The response to `id`, skipping other lines, if it comes within `wait`.
fn reply(
    lines: &std::sync::mpsc::Receiver<Value>,
    id: u64,
    wait: std::time::Duration,
) -> Option<Value> {
    let deadline = std::time::Instant::now() + wait;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = lines.recv_timeout(remaining).ok()?;
        if line.get("id").and_then(Value::as_u64) == Some(id) {
            return Some(line);
        }
    }
}

/// Waits until the backend has parked the gate.
fn until_parked(gate: &Gate) {
    let deadline = std::time::Instant::now() + LONG;
    while !gate.parked() {
        assert!(
            std::time::Instant::now() < deadline,
            "the store was never parked"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

/// `Backend::resume` gets through while the loop waits in the suspended store.
#[test]
fn backend_resume_reaches_a_loop_blocked_in_the_store() {
    let gate = Arc::new(Gate::default());
    let (backend, lines) = gated_backend(&gate);
    backend.suspend(0);
    until_parked(&gate);
    backend
        .send(r#"{"id":2,"method":"search","params":{}}"#)
        .unwrap();
    assert!(
        reply(&lines, 2, BLOCKED).is_none(),
        "the loop waits in the store"
    );
    backend.resume();
    assert!(
        reply(&lines, 2, LONG).is_some(),
        "the search answers after the resume"
    );
    backend.shutdown();
}

/// `Backend::shutdown` returns while the loop waits in the suspended store.
#[test]
fn backend_shutdown_while_the_loop_waits_in_the_store_returns() {
    let gate = Arc::new(Gate::default());
    let (backend, lines) = gated_backend(&gate);
    backend.suspend(0);
    until_parked(&gate);
    backend
        .send(r#"{"id":2,"method":"search","params":{}}"#)
        .unwrap();
    assert!(
        reply(&lines, 2, BLOCKED).is_none(),
        "the loop waits in the store"
    );
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        backend.shutdown();
        let _ = done.send(());
    });
    finished.recv_timeout(LONG).expect("shutdown returns");
}
