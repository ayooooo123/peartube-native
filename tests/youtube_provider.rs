//! The Rust YouTube provider must reproduce the app's own plugin, call for call and file for file.
//!
//! The oracle is `tests/js/youtube-provider.mjs`, which runs the app's `src/providers/youtube.js` (from
//! `PEARTUBE_APP`, else the sibling `../peartube`) through the sessions of
//! `tests/js/youtube-provider-scenario.json` with scripted `ubit` calls, a scripted fetch, a scripted host, a
//! scripted clock and real files. Here the same sessions drive the Rust provider under tokio's paused clock; the
//! two traces are compared as JSON text, so the order of the keys counts too.

#[path = "support/files.rs"]
mod files;
#[path = "support/provider_host.rs"]
mod provider_host;
#[path = "support/trace.rs"]
mod trace;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use files::{Labels, sha256_hex, tree};
use peartube_native::identify::Controller;
use peartube_native::providers::staging::PlaceAndScan;
use peartube_native::providers::youtube::{
    Backend, BackendError, BackendFuture, Fetch, Fetched, Options, ProgressFn, Provider,
    YoutubeError,
};
use provider_host::FakeHost;
use serde_json::{Value, json};

const NOW: u64 = 1_700_000_000_000;

type Log = Arc<Mutex<Vec<Value>>>;

/// The relay's `ubit` calls and its guarded fetch, scripted by the scenario: `searches` is what `ubit info`
/// answers for a query, `videos` what `ubit get` does for a video id, and `thumbs` what the fetch answers for a
/// picture. Each call goes to the log.
struct Scripted {
    scenario: Arc<Value>,
    log: Log,
    tick: u64,
}

/// A number as JS prints it: a whole one without a fraction.
fn number(value: f64) -> Value {
    if value.fract() == 0.0 {
        json!(value as u64)
    } else {
        json!(value)
    }
}

fn failure(script: &Value) -> BackendError {
    BackendError {
        message: script["error"].as_str().unwrap().to_owned(),
        status: script
            .get("status")
            .and_then(Value::as_u64)
            .map(|status| status as u16),
    }
}

/// `Buffer.alloc(len, id)`: the id again and again.
fn filled(id: &str, len: usize) -> Vec<u8> {
    id.bytes().cycle().take(len).collect()
}

async fn wait(ms: Option<u64>) {
    if let Some(ms) = ms.filter(|ms| *ms > 0) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}

impl Backend for Scripted {
    /// `searches[<query>]` answers `reply` (else `{ entries }`) after `after` ms, or fails with `error` (and
    /// `status`). A query the scenario does not name has no results.
    fn info(&self, query: &str, timeout_ms: f64) -> BackendFuture<Value> {
        self.log
            .lock()
            .unwrap()
            .push(json!({ "info": query, "timeoutMs": number(timeout_ms) }));
        let script = self.scenario["searches"]
            .get(query)
            .cloned()
            .unwrap_or_else(|| json!({ "error": "No YouTube results" }));
        Box::pin(async move {
            wait(script.get("after").and_then(Value::as_u64)).await;
            if script.get("error").is_some() {
                return Err(failure(&script));
            }
            Ok(match script.get("reply") {
                Some(reply) => reply.clone(),
                None => json!({ "entries": script["entries"] }),
            })
        })
    }

    /// `videos[<id>]` reports each of `steps` (`[ms after the start, bytes so far]`), then `after` ms later (one
    /// tick when it names none) fails with `error` or leaves `file`, `bytes` long, in the folder it was given. A
    /// cancel ends it at once, as killing `ubit` does. The provider only asks for audio: the app's log entry says
    /// so with `audio: true`.
    fn download(
        &self,
        url: &str,
        out_dir: &Path,
        controller: Controller,
        mut on_progress: ProgressFn,
    ) -> BackendFuture<PathBuf> {
        let id = url.rsplit_once("v=").map_or("", |(_, id)| id).to_owned();
        let into = out_dir.file_name().unwrap().to_string_lossy().into_owned();
        self.log
            .lock()
            .unwrap()
            .push(json!({ "download": id, "audio": true, "into": into }));
        let script = self.scenario["videos"]
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({ "error": "Video unavailable" }));
        let (out_dir, tick) = (out_dir.to_owned(), self.tick);
        let cancelled = || BackendError {
            message: "Cancelled".to_owned(),
            status: None,
        };
        Box::pin(async move {
            if controller.is_aborted() {
                return Err(cancelled());
            }
            let run = async {
                let mut at = 0;
                for step in script["steps"].as_array().into_iter().flatten() {
                    let (ms, bytes) = (step[0].as_u64().unwrap(), step[1].as_u64().unwrap());
                    wait(Some(ms - at)).await;
                    at = ms;
                    on_progress(bytes);
                }
                wait(Some(
                    script.get("after").and_then(Value::as_u64).unwrap_or(tick),
                ))
                .await;
                if script.get("error").is_some() {
                    return Err(failure(&script));
                }
                let path = out_dir.join(script["file"].as_str().unwrap());
                let bytes = filled(&id, script["bytes"].as_u64().unwrap() as usize);
                std::fs::write(&path, bytes).unwrap();
                Ok(path)
            };
            tokio::select! {
                biased;
                () = controller.aborted() => Err(cancelled()),
                done = run => done,
            }
        })
    }
}

impl Fetch for Scripted {
    /// `thumbs[<id>]` is a reply (`status`, `type`, `bytes`) or an `error`. A picture the scenario does not name
    /// is not found.
    fn fetch(
        &self,
        url: &str,
    ) -> std::pin::Pin<Box<dyn Future<Output = Result<Fetched, String>> + Send>> {
        self.log.lock().unwrap().push(json!({ "fetch": url }));
        let id = url.split('/').nth(4).unwrap_or_default().to_owned();
        let script = self.scenario["thumbs"]
            .get(&id)
            .cloned()
            .unwrap_or_else(|| json!({ "status": 404, "type": "text/html", "bytes": 9 }));
        Box::pin(async move {
            wait(script.get("after").and_then(Value::as_u64)).await;
            if let Some(error) = script.get("error").and_then(Value::as_str) {
                return Err(error.to_owned());
            }
            Ok(Fetched {
                status: script["status"].as_u64().unwrap() as u16,
                content_type: script
                    .get("type")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                body: filled(&id, script["bytes"].as_u64().unwrap() as usize),
            })
        })
    }
}

/// Lets every task run as far as it can before the clock moves again.
async fn flush() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
}

type Outcome = Arc<Mutex<Option<Value>>>;

fn outcome(result: Result<Value, YoutubeError>) -> Value {
    match result {
        Ok(value) => json!({ "result": value }),
        Err(error) => json!({ "error": { "message": error.message, "status": error.status } }),
    }
}

/// A file a step puts under the session's folder.
fn write_file(root: &Path, step: &Value) {
    let path = root.join(step["write"].as_str().unwrap());
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, step["text"].as_str().unwrap()).unwrap();
}

/// A save's work folder has a random name: `youtube-work/job-<name>` reads `youtube-work/job-*`.
fn steady(text: &str) -> String {
    let mark = "youtube-work/job-";
    let mut out = String::new();
    let mut rest = text;
    while let Some(at) = rest.find(mark) {
        let (head, tail) = rest.split_at(at + mark.len());
        out.push_str(head);
        out.push('*');
        rest = tail.trim_start_matches(|c: char| c.is_ascii_alphanumeric());
    }
    out + rest
}

async fn run_session(scenario: &Arc<Value>, session: &Value) -> Value {
    let name = session["name"].as_str().unwrap();
    let tick_ms = scenario["tick"].as_u64().unwrap();
    let root = std::env::temp_dir().join(format!(
        "pt-yt-rust-{}-{}",
        std::process::id(),
        name.replace(' ', "-")
    ));
    let _ = std::fs::remove_dir_all(&root);
    let storage = root.join("storage");
    std::fs::create_dir_all(&storage).unwrap();
    for step in session["before"].as_array().into_iter().flatten() {
        write_file(&root, step);
    }
    let log: Log = Arc::default();
    let changes = Arc::new(AtomicU64::new(0));
    let start = tokio::time::Instant::now();
    let now_ms: Arc<dyn Fn() -> u64 + Send + Sync> =
        Arc::new(move || NOW + start.elapsed().as_millis() as u64);
    let tick = Duration::from_millis(tick_ms);
    let host = FakeHost::new(
        Arc::new({
            let log = log.clone();
            move |call| log.lock().unwrap().push(call)
        }),
        storage.join("staging"),
        tick,
        now_ms.clone(),
    );
    let scripted = Arc::new(Scripted {
        scenario: scenario.clone(),
        log: log.clone(),
        tick: tick_ms,
    });
    let backend = (session["backend"] != false).then(|| scripted.clone() as Arc<dyn Backend>);
    let mut options = Options::new(
        storage.to_string_lossy(),
        backend,
        Arc::new(Labels::default()),
        now_ms.clone(),
    );
    if session["fetch"] != false {
        options.fetch = Some(scripted.clone());
    }
    if let Some(ms) = session["options"]["searchTimeoutMs"].as_f64() {
        options.search_timeout_ms = ms;
    }
    options.onchange = Some({
        let changes = changes.clone();
        Arc::new(move || {
            changes.fetch_add(1, Ordering::SeqCst);
        })
    });
    let provider = Provider::new(options);
    let place_and_scan: Option<PlaceAndScan> =
        (session["scan"] != false).then(|| host.place_and_scan());

    let mut out: Vec<Value> = Vec::new();
    let mut put = |step: usize, value: Value| {
        out.push(json!({
            "step": step,
            "at": now_ms() - NOW,
            "changes": changes.swap(0, Ordering::SeqCst),
            "calls": std::mem::take(&mut *log.lock().unwrap()),
            "value": value,
        }));
    };
    // A call's outcome, once it has one: the clock moves until then.
    let call = |step: &Value| -> Outcome {
        let slot: Outcome = Arc::new(Mutex::new(None));
        let (provider, host) = (provider.clone(), place_and_scan.clone());
        let kind = step["do"].as_str().unwrap().to_owned();
        // A call with no payload takes the app's default, an empty one.
        let payload = step.get("payload").cloned().unwrap_or_else(|| json!({}));
        let done = slot.clone();
        tokio::spawn(async move {
            let result = match kind.as_str() {
                "search" => provider.search(&payload).await,
                "mirror" => provider.mirror(&payload, host).await,
                "run" => provider.run(&payload, host).await,
                other => panic!("unknown call {other}"),
            };
            *done.lock().unwrap() = Some(outcome(result));
        });
        slot
    };
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
    // A picture as its type, its size and its hash.
    let thumb = |id: String| -> Outcome {
        let slot: Outcome = Arc::new(Mutex::new(None));
        let (provider, done) = (provider.clone(), slot.clone());
        tokio::spawn(async move {
            let picture = provider.thumbnail(&id).await.map(|picture| {
                json!({
                    "type": picture.content_type,
                    "bytes": picture.bytes.len(),
                    "sha256": sha256_hex(&picture.bytes),
                })
            });
            *done.lock().unwrap() = Some(json!({ "result": picture }));
        });
        slot
    };
    let stop = || -> Outcome {
        let slot: Outcome = Arc::new(Mutex::new(None));
        let (provider, done) = (provider.clone(), slot.clone());
        tokio::spawn(async move {
            provider.stop().await;
            *done.lock().unwrap() = Some(Value::Null);
        });
        slot
    };
    let mut pending: HashMap<String, Outcome> = HashMap::new();

    flush().await;
    for (i, step) in session["steps"].as_array().unwrap().iter().enumerate() {
        if step.get("do").is_some() {
            if let Some(label) = step.get("as").and_then(Value::as_str) {
                pending.insert(label.to_owned(), call(step));
                flush().await;
                put(i, json!({ "started": label }));
            } else {
                // `repeat` makes the same call again and again; each has its own record.
                for _ in 0..step.get("repeat").and_then(Value::as_u64).unwrap_or(1) {
                    let done = settle(call(step)).await;
                    put(i, done);
                }
            }
        } else if let Some(label) = step.get("await").and_then(Value::as_str) {
            let done = settle(pending.remove(label).unwrap()).await;
            put(i, done);
        } else if let Some(ms) = step.get("advance").and_then(Value::as_u64) {
            for _ in 0..ms.div_ceil(tick_ms) {
                tokio::time::sleep(tick).await;
                flush().await;
            }
            put(i, Value::Null);
        } else if step.get("jobs").is_some() {
            put(i, Value::Array(provider.list_jobs()));
        } else if step.get("count").is_some() {
            put(i, json!(provider.list_jobs().len()));
        } else if let Some(n) = step.get("get").and_then(Value::as_u64) {
            put(i, json!(provider.get_job(&format!("job-{n}"))));
        } else if let Some(n) = step.get("cancel").and_then(Value::as_u64) {
            // What a cancel sets off (the save ends, the next one starts) happens now, before the clock
            // moves again.
            let answer = json!(provider.cancel_job(&format!("job-{n}")));
            flush().await;
            put(i, answer);
        } else if step.get("info").is_some() {
            put(i, provider.info());
        } else if let Some(id) = step.get("thumb") {
            // The app is handed the value as it is, text or a number, and reads it as text.
            let id = id.as_str().map_or_else(|| id.to_string(), str::to_owned);
            let done = settle(thumb(id)).await;
            put(i, done);
        } else if let Some(range) = step.get("thumbs").and_then(Value::as_array) {
            // Pictures asked for one after another, by a number each: only how many there were is recorded.
            let (from, to) = (range[0].as_u64().unwrap(), range[1].as_u64().unwrap());
            for n in from..to {
                settle(thumb(format!("n{n:010}"))).await;
            }
            put(i, json!(to - from));
        } else if let Some(ms) = step.get("hold").and_then(Value::as_u64) {
            host.hold(ms);
            put(i, Value::Null);
        } else if let Some(ms) = step.get("limit").and_then(Value::as_u64) {
            host.limit(ms);
            put(i, Value::Null);
        } else if let Some(refused) = step.get("refuse") {
            host.refuse(
                refused["name"].as_str().unwrap(),
                refused["error"].as_str().unwrap(),
            );
            put(i, Value::Null);
        } else if step.get("write").is_some() {
            write_file(&root, step);
            put(i, Value::Null);
        } else if step.get("tree").is_some() {
            put(i, tree(&root));
        } else if step.get("stop").is_some() {
            // A save that waits for the import folder ends at the host's next look, so the clock moves here
            // too.
            settle(stop()).await;
            put(i, Value::Null);
        } else {
            panic!("unknown step {step}");
        }
    }
    settle(stop()).await;
    let _ = std::fs::remove_dir_all(&root);
    let text = json!({ "name": name, "trace": out }).to_string();
    let root = serde_json::to_string(&root.to_string_lossy()).unwrap();
    serde_json::from_str(&steady(&text.replace(&root[1..root.len() - 1], "<root>"))).unwrap()
}

/// Run the scenario through the app's plugin and return its trace.
fn js_trace() -> Value {
    let script =
        Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/js")).join("youtube-provider.mjs");
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

/// Every session runs to its end before the test fails, so one run names all that differs.
#[tokio::test(start_paused = true)]
async fn the_provider_does_what_the_app_does() {
    let oracle = js_trace();
    let scenario: Arc<Value> =
        Arc::new(serde_json::from_str(include_str!("js/youtube-provider-scenario.json")).unwrap());
    let sessions = scenario["sessions"].as_array().unwrap();
    let expected = oracle["sessions"].as_array().unwrap();
    assert_eq!(expected.len(), sessions.len());
    let mut records = 0;
    let mut differs: Vec<String> = Vec::new();
    for (session, expected) in sessions.iter().zip(expected) {
        let actual = run_session(&scenario, session).await;
        assert_eq!(actual["name"], expected["name"]);
        let name = session["name"].as_str().unwrap();
        let (actual, expected) = (
            actual["trace"].as_array().unwrap(),
            expected["trace"].as_array().unwrap(),
        );
        for (index, (actual, expected)) in actual.iter().zip(expected).enumerate() {
            if let Some(difference) = trace::first_difference(actual, expected, "") {
                differs.push(format!(
                    "session “{name}”, record {index} (step {}): {difference}",
                    expected["step"]
                ));
            }
            records += 1;
        }
        if actual.len() != expected.len() {
            differs.push(format!(
                "session “{name}”: {} records here, {} in the app",
                actual.len(),
                expected.len()
            ));
        }
    }
    assert!(
        differs.is_empty(),
        "{} records differ from the app:\n  {}",
        differs.len(),
        differs.join("\n  ")
    );
    assert!(records > 340, "{records} records compared");
}
