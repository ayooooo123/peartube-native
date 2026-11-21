//! The Rust Soulseek provider must reproduce the app's own plugin, call for call and file for file.
//!
//! The oracle is `tests/js/soulseek-provider.mjs`, which runs the app's `src/providers/soulseek.js` (from
//! `PEARTUBE_APP`, else the sibling `../peartube`) through the sessions of
//! `tests/js/soulseek-provider-scenario.json` with a scripted Soulseek client, a scripted clock and real files.
//! Here the same sessions drive the Rust provider under tokio's paused clock; the two traces are compared as
//! JSON text, so the order of the keys counts too.

#[path = "support/soulseek_network.rs"]
mod network;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use network::{FakeBackend, Labels, Network, World, sha256_hex, sorted_names, tree};
use peartube_native::providers::soulseek::client::Backend;
use peartube_native::providers::soulseek::{Options, Provider, SoulseekError, StagingScan};
use serde::Deserialize;
use serde_json::{Value, json};

const NOW: u64 = 1_700_000_000_000;

#[derive(Deserialize)]
struct Scenario {
    sessions: Vec<Session>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Session {
    name: String,
    backend: Option<String>,
    login: Option<String>,
    bound_port: Option<u16>,
    #[serde(default)]
    import_folder: bool,
    scan: Option<bool>,
    options: SessionOptions,
    steps: Vec<Value>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionOptions {
    slsk_user: Option<String>,
    slsk_pass: Option<String>,
    slsk_server: Option<String>,
    slsk_listen_port: Option<u16>,
    slsk_share: Option<bool>,
    slsk_share_max_files: Option<u32>,
    search_timeout_ms: Option<u64>,
    browse_timeout_ms: Option<u64>,
    no_answer_ms: Option<u64>,
    no_connect_ms: Option<u64>,
}

/// What the mirror host's scan answers: one item for each media file it has not reported before.
struct FakeScan {
    world: Arc<World>,
    root: PathBuf,
    seen: Mutex<HashSet<String>>,
    busy: AtomicU64,
}

impl FakeScan {
    async fn run(self: Arc<Self>) -> Result<Value, String> {
        let folder = self
            .root
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        self.world.log(json!({ "scan": folder }));
        self.world.tick().await;
        if self.busy.load(Ordering::SeqCst) > 0 {
            self.busy.fetch_sub(1, Ordering::SeqCst);
            return Ok(json!({ "scanned": 0, "ingested": 0, "busy": true, "items": [] }));
        }
        let mut items = Vec::new();
        let mut scanned = 0;
        for folder in sorted_names(&self.root) {
            let dir = self.root.join(&folder);
            if folder.starts_with('.') || !dir.is_dir() {
                continue;
            }
            for name in sorted_names(&dir) {
                if name.starts_with('.') || name.ends_with(".json") {
                    continue;
                }
                scanned += 1;
                if !self.seen.lock().unwrap().insert(format!("{folder}/{name}")) {
                    continue;
                }
                let bytes = std::fs::read(dir.join(&name)).unwrap();
                items.push(
                    json!({ "file": name, "size": bytes.len(), "sha256": sha256_hex(&bytes) }),
                );
            }
        }
        Ok(json!({ "scanned": scanned, "ingested": items.len(), "items": items }))
    }
}

/// Lets every task run as far as it can before the clock moves again.
async fn flush() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

type Outcome = Arc<Mutex<Option<Value>>>;

fn outcome(result: Result<Value, SoulseekError>) -> Value {
    match result {
        Ok(value) => json!({ "result": value }),
        Err(error) => json!({ "error": { "message": error.message, "status": error.status } }),
    }
}

async fn run_session(network: &Arc<Network>, session: &Session) -> Value {
    let root = std::env::temp_dir().join(format!(
        "pt-slsk-rust-{}-{}",
        std::process::id(),
        session.name.replace(' ', "-")
    ));
    let _ = std::fs::remove_dir_all(&root);
    let storage = root.join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    let staging = if session.import_folder {
        root.join("import")
    } else {
        storage.join("staging")
    };
    let world = World::new(network.clone(), session.login.clone(), session.bound_port);
    let scan = Arc::new(FakeScan {
        world: world.clone(),
        root: staging.clone(),
        seen: Mutex::new(HashSet::new()),
        busy: AtomicU64::new(0),
    });
    let changes = Arc::new(AtomicU64::new(0));
    let start = tokio::time::Instant::now();
    let now_ms: Arc<dyn Fn() -> u64 + Send + Sync> =
        Arc::new(move || NOW + start.elapsed().as_millis() as u64);
    let backend: Option<Arc<dyn Backend>> = match session.backend.as_deref() {
        Some("none") => None,
        _ => Some(Arc::new(FakeBackend(world.clone()))),
    };
    let mut options = Options::new(
        storage.to_string_lossy(),
        backend,
        Arc::new(Labels::default()),
        now_ms.clone(),
    );
    if session.import_folder {
        options.staging_root = Some(staging.to_string_lossy().into_owned());
        options.staging_label = "import folder".to_owned();
    }
    let given = &session.options;
    options.user = given.slsk_user.clone().unwrap_or_default();
    options.pass = given.slsk_pass.clone().unwrap_or_default();
    if let Some(server) = &given.slsk_server {
        options.server = server.clone();
    }
    options.listen_port = given.slsk_listen_port.unwrap_or(options.listen_port);
    options.share = given.slsk_share.unwrap_or(options.share);
    options.share_max_files = given
        .slsk_share_max_files
        .unwrap_or(options.share_max_files);
    options.search_timeout_ms = given.search_timeout_ms.unwrap_or(options.search_timeout_ms);
    options.browse_timeout_ms = given.browse_timeout_ms.unwrap_or(options.browse_timeout_ms);
    options.no_answer_ms = given.no_answer_ms.unwrap_or(options.no_answer_ms);
    options.no_connect_ms = given.no_connect_ms.unwrap_or(options.no_connect_ms);
    options.onchange = Some({
        let changes = changes.clone();
        Arc::new(move || {
            changes.fetch_add(1, Ordering::SeqCst);
        })
    });
    let provider = Provider::new(options);
    let staging_scan: Option<StagingScan> = (session.scan != Some(false)).then(|| {
        let scan = scan.clone();
        let asked: StagingScan = Arc::new(move || Box::pin(scan.clone().run()));
        asked
    });

    let mut out: Vec<Value> = Vec::new();
    let mut put = |step: usize, value: Value| {
        out.push(json!({
            "step": step,
            "at": now_ms() - NOW,
            "changes": changes.swap(0, Ordering::SeqCst),
            "calls": world.take_log(),
            "value": value,
        }));
    };
    // A call's outcome, once it has one: the clock moves until then.
    let call = |step: &Value| -> Outcome {
        let slot: Outcome = Arc::new(Mutex::new(None));
        let (provider, scan) = (provider.clone(), staging_scan.clone());
        let (kind, payload) = (
            step["do"].as_str().unwrap().to_owned(),
            step["payload"].clone(),
        );
        let done = slot.clone();
        tokio::spawn(async move {
            let result = match kind.as_str() {
                "search" => provider.search(&payload).await,
                "browse" => provider.browse(&payload).await,
                "mirror" => provider.mirror(&payload, scan).await,
                "run" => provider.run(&payload, scan).await,
                other => panic!("unknown call {other}"),
            };
            *done.lock().unwrap() = Some(outcome(result));
        });
        slot
    };
    let tick = Duration::from_millis(network.tick);
    let settle = |slot: Outcome| async move {
        flush().await;
        for _ in 0..100_000 {
            if let Some(done) = slot.lock().unwrap().take() {
                return done;
            }
            tokio::time::sleep(tick).await;
            flush().await;
        }
        panic!("a call did not settle");
    };
    let mut pending: HashMap<String, Outcome> = HashMap::new();

    flush().await;
    for (i, step) in session.steps.iter().enumerate() {
        if step.get("do").is_some() {
            if let Some(name) = step.get("as").and_then(Value::as_str) {
                pending.insert(name.to_owned(), call(step));
                flush().await;
                put(i, json!({ "started": name }));
            } else {
                for _ in 0..step.get("repeat").and_then(Value::as_u64).unwrap_or(1) {
                    let done = settle(call(step)).await;
                    put(i, done);
                }
            }
        } else if let Some(name) = step.get("await").and_then(Value::as_str) {
            let done = settle(pending.remove(name).unwrap()).await;
            put(i, done);
        } else if let Some(ms) = step.get("advance").and_then(Value::as_u64) {
            for _ in 0..ms.div_ceil(network.tick) {
                tokio::time::sleep(tick).await;
                flush().await;
            }
            put(i, Value::Null);
        } else if step.get("jobs").is_some() {
            put(i, Value::Array(provider.list_jobs()));
        } else if let Some(n) = step.get("get").and_then(Value::as_u64) {
            put(i, json!(provider.get_job(&format!("job-{n}"))));
        } else if let Some(n) = step.get("cancel").and_then(Value::as_u64) {
            put(i, json!(provider.cancel_job(&format!("job-{n}"))));
        } else if step.get("info").is_some() {
            put(i, provider.info());
        } else if let Some(next) = step.get("configure") {
            let text = |key: &str| next.get(key).and_then(Value::as_str);
            provider.configure(text("slskUser"), text("slskPass"), text("slskServer"));
            flush().await;
            put(i, Value::Null);
        } else if let Some(times) = step.get("busy").and_then(Value::as_u64) {
            scan.busy.store(times, Ordering::SeqCst);
            put(i, Value::Null);
        } else if step.get("tree").is_some() {
            put(i, tree(&root));
        } else if step.get("stop").is_some() {
            provider.stop();
            flush().await;
            put(i, Value::Null);
        } else {
            panic!("unknown step {step}");
        }
    }
    provider.stop();
    let _ = std::fs::remove_dir_all(&root);
    let text = json!({ "name": session.name, "trace": out }).to_string();
    let root = serde_json::to_string(&root.to_string_lossy()).unwrap();
    serde_json::from_str(&text.replace(&root[1..root.len() - 1], "<root>")).unwrap()
}

/// Run the scenario through the app's plugin and return its trace.
fn js_trace() -> Value {
    let script =
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js")).join("soulseek-provider.mjs");
    let output = std::process::Command::new("node")
        .arg(&script)
        .output()
        .unwrap_or_else(|error| panic!("run {}: {error}", script.display()));
    assert!(
        output.status.success(),
        "{} failed: {}",
        script.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("the oracle prints JSON")
}

#[tokio::test(start_paused = true)]
async fn the_provider_does_what_the_app_does() {
    let oracle = js_trace();
    let network = Network::load();
    let scenario: Scenario =
        serde_json::from_str(include_str!("js/soulseek-provider-scenario.json")).unwrap();
    let expected = oracle["sessions"].as_array().unwrap();
    assert_eq!(expected.len(), scenario.sessions.len());
    let mut records = 0;
    for (session, expected) in scenario.sessions.iter().zip(expected) {
        let actual = run_session(&network, session).await;
        assert_eq!(actual["name"], expected["name"]);
        let (actual, expected) = (
            actual["trace"].as_array().unwrap(),
            expected["trace"].as_array().unwrap(),
        );
        for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
            assert_eq!(
                actual.to_string(),
                expected.to_string(),
                "session “{}”, record {index} (step {})",
                session.name,
                expected["step"]
            );
            records += 1;
        }
        assert_eq!(actual.len(), expected.len(), "session “{}”", session.name);
    }
    assert!(records > 250, "{records} records compared");
}
