//! Every case in `fixtures/identity-reference.json` must match the app's own output, including the
//! bytes and mode of the `identity.json` it writes.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use peartube_native::identity::{Channel, Identity, IdentityContext};
use serde_json::{Map, Value, json};

const FIXTURES: &str = include_str!("../fixtures/identity-reference.json");

fn fixtures() -> Value {
    serde_json::from_str(FIXTURES).expect("fixtures/identity-reference.json")
}

fn cases(fixtures: &Value) -> &[Value] {
    fixtures["identityFiles"].as_array().expect("identityFiles")
}

/// Polls a future that never really parks: the mock channel answers immediately.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[derive(Default)]
struct MockState {
    epoch: u64,
    appends: Vec<Value>,
}

/// The slice of `ctx.channel` identity.js reads, with the appended ops recorded.
#[derive(Clone)]
struct MockChannel {
    primary_key: String,
    writable: bool,
    local_key: String,
    role: Option<String>,
    writers: Vec<Value>,
    state: Arc<Mutex<MockState>>,
}

impl MockChannel {
    fn from_config(config: &Value) -> Self {
        let primary_key = config["primaryKey"].as_str().unwrap_or_default().to_owned();
        let local_key = config["localKey"]
            .as_str()
            .unwrap_or(&primary_key)
            .to_owned();
        let epoch = config["epoch"].as_u64().unwrap_or(1);
        Self {
            writable: config["writable"].as_bool().unwrap_or(true),
            role: config["role"].as_str().map(str::to_owned),
            writers: config["writers"].as_array().cloned().unwrap_or_default(),
            primary_key,
            local_key,
            state: Arc::new(Mutex::new(MockState {
                epoch,
                appends: Vec::new(),
            })),
        }
    }
}

fn same_json(actual: &Value, expected: &Value, context: &str) {
    assert_eq!(
        serde_json::to_string(actual).expect("json"),
        serde_json::to_string(expected).expect("json"),
        "{context}"
    );
}

impl Channel for MockChannel {
    fn primary_key(&self) -> String {
        self.primary_key.clone()
    }

    fn writable(&self) -> bool {
        self.writable
    }

    fn local_key(&self) -> String {
        self.local_key.clone()
    }

    async fn append(&mut self, op: Value) {
        if op["type"] == "epoch"
            && let Some(epoch) = op["epoch"].as_u64()
        {
            self.state.lock().expect("mock state").epoch = epoch;
        }
        self.state.lock().expect("mock state").appends.push(op);
    }

    async fn current_channel_epoch(&mut self) -> u64 {
        self.state.lock().expect("mock state").epoch
    }

    async fn local_role(&mut self) -> Option<String> {
        self.role.clone()
    }

    async fn list_writers(&mut self) -> Vec<Value> {
        self.writers.clone()
    }

    fn update(&mut self) {}
}

fn temp_root() -> PathBuf {
    std::env::temp_dir().join(format!("peartube-native-identity-{}", std::process::id()))
}

/// Writes the case's starting file with the mode the app uses.
fn prepare(root: &Path, case: &Value, index: usize) -> PathBuf {
    let dir = root.join(format!("case-{index}"));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("temp dir");
    if let Some(initial) = case["initial"].as_str() {
        let path = dir.join("identity.json");
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        std::io::Write::write_all(
            &mut options.open(&path).expect("initial file"),
            initial.as_bytes(),
        )
        .expect("initial file");
    }
    dir
}

fn mnemonic_of(case: &Value) -> Option<String> {
    case["mnemonic"].as_str().map(str::to_owned)
}

fn open(
    dir: &Path,
    case: &Value,
) -> (Result<Identity<MockChannel>, String>, Arc<Mutex<MockState>>) {
    let channel = MockChannel::from_config(&case["channel"]);
    let state = channel.state.clone();
    let opened = Identity::new(IdentityContext {
        storage: dir.to_path_buf(),
        mnemonic: mnemonic_of(case),
        channel,
    })
    .map_err(|error| error.message().to_owned());
    (opened, state)
}

fn run_step(
    identity: &mut Identity<MockChannel>,
    state: &Arc<Mutex<MockState>>,
    step: &Value,
) -> Result<Value, String> {
    let op = step["op"].as_str().expect("op");
    match op {
        "identity" => {
            block_on(identity.identity(step["revealMnemonic"].as_bool().unwrap_or(false)))
                .map_err(|error| error.message().to_owned())
        }
        "claim" => block_on(identity.claim_with_mnemonic(
            step["phrase"].as_str().expect("phrase"),
            step["alias"].as_str().unwrap_or_default(),
        ))
        .map_err(|error| error.message().to_owned()),
        "rotate" => block_on(identity.rotate_epoch()).map_err(|error| error.message().to_owned()),
        "addAccessKey" => block_on(identity.add_access_key(&step["request"]))
            .map_err(|error| error.message().to_owned()),
        "ensureVault" => {
            let vault = identity.ensure_vault();
            Ok(json!({
                "channelKey": vault.channel_key,
                "ownerPublicKey": vault.owner_public_key,
                "mnemonic": vault.mnemonic,
            }))
        }
        "save" => {
            identity.save_identity();
            Ok(Value::Null)
        }
        "foundedKey" => Ok(Value::String(identity.founded_key().to_owned())),
        "markFounded" => {
            identity.mark_founded(step["key"].as_str().unwrap_or_default());
            Ok(Value::Null)
        }
        "idVault" => Ok(match identity.id_vault() {
            Some(vault) => json!({
                "channelKey": vault.channel_key,
                "ownerPublicKey": vault.owner_public_key,
                "mnemonic": vault.mnemonic,
            }),
            None => Value::Null,
        }),
        "accessKeys" => Ok(Value::Object(identity.access_keys().clone())),
        "appends" => Ok(Value::Array(
            state.lock().expect("mock state").appends.clone(),
        )),
        "epochState" => Ok(Value::from(state.lock().expect("mock state").epoch)),
        other => panic!("unknown op {other}"),
    }
}

/// A restart reads the file Rust wrote: the same view the app gets from its own file.
fn reloaded(dir: &Path, case: &Value) -> Value {
    let (opened, _) = open(dir, case);
    let mut identity = opened.expect("restart opens the file it just wrote");
    let view = block_on(identity.identity(true)).expect("identity");
    json!({
        "view": view,
        "accessKeys": Value::Object(identity.access_keys().clone()),
    })
}

#[test]
fn identity_files_match_byte_for_byte() {
    let fixtures = fixtures();
    let cases = cases(&fixtures);
    assert!(cases.len() >= 30, "expected 30+ identity cases");
    let root = temp_root();
    let _ = fs::remove_dir_all(&root);
    for (index, case) in cases.iter().enumerate() {
        let name = case["name"].as_str().unwrap_or("case");
        let dir = prepare(&root, case, index);
        let (opened, state) = open(&dir, case);
        let mut identity = match opened {
            Ok(identity) => {
                assert!(
                    case.get("createErr").is_none(),
                    "case {index} {name}: unexpected open"
                );
                identity
            }
            Err(message) => {
                assert_eq!(
                    message,
                    case["createErr"].as_str().unwrap_or("<opened>"),
                    "case {index} {name}: open"
                );
                continue;
            }
        };
        for step in case["steps"].as_array().expect("steps") {
            let op = step["op"].as_str().expect("op");
            match run_step(&mut identity, &state, step) {
                Ok(value) => same_json(&value, &step["ok"], &format!("case {index} {name}: {op}")),
                Err(message) => assert_eq!(
                    message,
                    step["err"].as_str().expect("err"),
                    "case {index} {name}: {op}"
                ),
            }
        }
        let file = dir.join("identity.json");
        match case["finalFile"].as_str() {
            Some(expected) => {
                assert_eq!(
                    fs::read_to_string(&file).expect("written identity.json"),
                    expected,
                    "case {index} {name}: file"
                );
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = fs::metadata(&file).expect("metadata").permissions().mode() & 0o777;
                    let expected =
                        u32::from_str_radix(case["finalMode"].as_str().unwrap_or("600"), 8)
                            .expect("octal mode");
                    assert_eq!(mode, expected, "case {index} {name}: mode");
                }
            }
            None => assert!(
                !file.exists(),
                "case {index} {name}: unexpected identity.json"
            ),
        }
        if case["reloadedView"].is_object() {
            same_json(
                &reloaded(&dir, case),
                &case["reloadedView"],
                &format!("case {index} {name}: reload"),
            );
        }
    }
    let _ = fs::remove_dir_all(&root);
}

/// The generator cannot load files Rust writes, so this cross-check runs the JS reference on them.
#[test]
fn rust_written_identity_files_load_in_js() {
    let generator = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures/gen/identity.mjs");
    if !generator
        .parent()
        .expect("gen")
        .join("node_modules")
        .is_dir()
    {
        eprintln!("skipping: fixtures/gen/node_modules is missing");
        return;
    }
    let Some(app) = std::env::var_os("PEARTUBE_APP")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            let sibling = Path::new(env!("CARGO_MANIFEST_DIR")).join("../peartube");
            sibling.is_dir().then_some(sibling)
        })
    else {
        eprintln!("skipping: set PEARTUBE_APP to the app checkout");
        return;
    };
    let fixtures = fixtures();
    let root = temp_root().with_extension("js");
    let _ = fs::remove_dir_all(&root);
    let mut entries = Vec::new();
    for (index, case) in cases(&fixtures).iter().enumerate() {
        if case.get("createErr").is_some() || !case["reloadedView"].is_object() {
            continue;
        }
        let dir = prepare(&root, case, index);
        let (opened, state) = open(&dir, case);
        let mut identity = opened.expect("open");
        for step in case["steps"].as_array().expect("steps") {
            let _ = run_step(&mut identity, &state, step);
        }
        let mut entry = Map::new();
        entry.insert("dir".to_owned(), json!(dir.to_str().expect("utf-8 path")));
        entry.insert("mnemonic".to_owned(), case["mnemonic"].clone());
        entry.insert("channel".to_owned(), case["channel"].clone());
        entry.insert("value".to_owned(), reloaded(&dir, case));
        entries.push(Value::Object(entry));
    }
    let request = root.join("cases.json");
    fs::write(&request, serde_json::to_string(&entries).expect("json")).expect("cases.json");
    let output = Command::new("node")
        .arg(&generator)
        .arg("verify")
        .arg(&request)
        .env("PEARTUBE_APP", app)
        .output()
        .expect("node");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let _ = fs::remove_dir_all(&root);
}
