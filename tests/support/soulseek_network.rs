//! The scripted Soulseek network of `tests/js/soulseek-provider-scenario.json` (`scripts`, `search`, `peers`), for
//! the tests that run the Soulseek provider: a stand-in for the client, the files its peers send, and a listing of
//! what ends up on disk. `tests/js/soulseek-network.mjs` is the same network for the app.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use peartube_native::acquire::RandomSource;
use peartube_native::providers::soulseek::client::{
    Backend, Client, ClientFuture, ClientSetup, ClientState, Phase, Progress, ProgressFn,
};
use peartube_native::providers::soulseek::rank::PeerFile;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
pub struct Network {
    /// Every scripted event falls on a multiple of this many milliseconds.
    pub tick: u64,
    scripts: HashMap<String, Vec<Step>>,
    search: HashMap<String, Listing>,
    peers: HashMap<String, Peer>,
}

impl Network {
    pub fn load() -> Arc<Self> {
        Arc::new(
            serde_json::from_str(include_str!("../js/soulseek-provider-scenario.json")).unwrap(),
        )
    }
}

/// One moment of a scripted download: a progress report, its end, or its failure.
#[derive(Deserialize)]
struct Step {
    at: u64,
    phase: Option<String>,
    #[serde(default)]
    permille: u64,
    queue: Option<u32>,
    error: Option<String>,
    #[serde(default)]
    done: bool,
}

/// What a search or a browse answers.
#[derive(Deserialize, Default)]
struct Listing {
    #[serde(default, alias = "records")]
    hits: Vec<Record>,
    error: Option<String>,
}

#[derive(Deserialize, Default)]
struct Peer {
    browse: Option<Listing>,
    #[serde(default)]
    files: HashMap<String, FileScript>,
    file: Option<FileScript>,
}

#[derive(Deserialize, Clone)]
struct FileScript {
    script: String,
    tags: Option<Vec<(String, String)>>,
    #[serde(default)]
    pad: usize,
}

#[derive(Deserialize)]
struct Record {
    username: Option<String>,
    slots: Option<bool>,
    #[serde(default)]
    queue: u32,
    filename: String,
    #[serde(default)]
    size: u64,
    bitrate: Option<u32>,
    sample_rate: Option<u32>,
    bit_depth: Option<u32>,
    length_secs: Option<u32>,
}

impl Record {
    fn peer_file(&self, user: &str) -> PeerFile {
        PeerFile {
            username: self.username.clone().unwrap_or_else(|| user.to_owned()),
            slots: self.slots.unwrap_or(true),
            queue: self.queue,
            filename: self.filename.clone(),
            size: self.size,
            bitrate: self.bitrate,
            sample_rate: self.sample_rate,
            bit_depth: self.bit_depth,
            length: self.length_secs,
        }
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A FLAC file whose only metadata block is a Vorbis comment, then `pad` bytes.
pub fn flac(tags: &[(String, String)], pad: usize) -> Vec<u8> {
    let vendor = b"peartube-test";
    let mut comment = Vec::new();
    comment.extend_from_slice(&(vendor.len() as u32).to_le_bytes());
    comment.extend_from_slice(vendor);
    comment.extend_from_slice(&(tags.len() as u32).to_le_bytes());
    for (key, value) in tags {
        let entry = format!("{key}={value}");
        comment.extend_from_slice(&(entry.len() as u32).to_le_bytes());
        comment.extend_from_slice(entry.as_bytes());
    }
    let mut out = b"fLaC".to_vec();
    out.push(0x84);
    out.extend_from_slice(&(comment.len() as u32).to_be_bytes()[1..]);
    out.extend_from_slice(&comment);
    out.resize(out.len() + pad, 0x11);
    out
}

/// What the scripted client and a test share: the network and the list of calls made.
pub struct World {
    pub network: Arc<Network>,
    /// `ok`, `error:<message>`, or anything else for a rejected sign-in.
    login: String,
    /// The port the client reports instead of the one it was given.
    bound_port: Option<u16>,
    log: Mutex<Vec<Value>>,
}

impl World {
    pub fn new(network: Arc<Network>, login: Option<String>, bound_port: Option<u16>) -> Arc<Self> {
        Arc::new(Self {
            network,
            login: login.unwrap_or_else(|| "ok".to_owned()),
            bound_port,
            log: Mutex::new(Vec::new()),
        })
    }

    pub fn log(&self, call: Value) {
        self.log.lock().unwrap().push(call);
    }

    /// The calls made since the last time this was asked.
    pub fn take_log(&self) -> Vec<Value> {
        std::mem::take(&mut *self.log.lock().unwrap())
    }

    pub async fn tick(&self) {
        tokio::time::sleep(Duration::from_millis(self.network.tick)).await;
    }

    fn file(&self, user: &str, filename: &str) -> FileScript {
        let peer = self.network.peers.get(user);
        peer.and_then(|peer| peer.files.get(filename).or(peer.file.as_ref()))
            .cloned()
            .unwrap_or(FileScript {
                script: "silent".to_owned(),
                tags: None,
                pad: 0,
            })
    }
}

/// The stand-in for the Soulseek client: what the provider asks of it goes to the log.
pub struct FakeBackend(pub Arc<World>);

impl Backend for FakeBackend {
    fn client(&self, setup: ClientSetup) -> Arc<dyn Client> {
        self.0.log(json!({ "created": {
            "listenPort": setup.listen_port,
            "share": setup.share,
            "shareMaxFiles": setup.share_max_files,
        } }));
        Arc::new(FakeClient {
            world: self.0.clone(),
            listen_port: setup.listen_port,
            logged_in: AtomicBool::new(false),
        })
    }
}

struct FakeClient {
    world: Arc<World>,
    listen_port: u16,
    logged_in: AtomicBool,
}

impl Client for FakeClient {
    fn state(&self) -> ClientState {
        let logged_in = self.logged_in.load(Ordering::SeqCst);
        ClientState {
            connected: logged_in,
            logged_in,
            listen_port: self.world.bound_port.unwrap_or(self.listen_port),
        }
    }

    fn login<'a>(
        &'a self,
        host: &'a str,
        port: u16,
        user: &'a str,
        pass: &'a str,
    ) -> ClientFuture<'a, bool> {
        Box::pin(async move {
            self.world.log(json!({ "login": [host, port, user, pass] }));
            self.world.tick().await;
            if let Some(message) = self.world.login.strip_prefix("error:") {
                return Err(message.to_owned());
            }
            let ok = self.world.login == "ok";
            self.logged_in.store(ok, Ordering::SeqCst);
            Ok(ok)
        })
    }

    fn search<'a>(&'a self, query: &'a str, wait_ms: u64) -> ClientFuture<'a, Vec<PeerFile>> {
        Box::pin(async move {
            self.world.log(json!({ "search": [query, wait_ms] }));
            self.world.tick().await;
            let Some(script) = self.world.network.search.get(query) else {
                return Ok(Vec::new());
            };
            match &script.error {
                Some(message) => Err(message.clone()),
                None => Ok(script.hits.iter().map(|hit| hit.peer_file("")).collect()),
            }
        })
    }

    fn browse<'a>(&'a self, user: &'a str, timeout_ms: u64) -> ClientFuture<'a, Vec<PeerFile>> {
        Box::pin(async move {
            self.world.log(json!({ "browse": [user, timeout_ms] }));
            self.world.tick().await;
            let script = self
                .world
                .network
                .peers
                .get(user)
                .and_then(|peer| peer.browse.as_ref());
            let Some(script) = script else {
                return Ok(Vec::new());
            };
            match &script.error {
                Some(message) => Err(message.clone()),
                None => Ok(script
                    .hits
                    .iter()
                    .map(|record| record.peer_file(user))
                    .collect()),
            }
        })
    }

    fn download<'a>(
        &'a self,
        user: &'a str,
        filename: &'a str,
        dest: &'a Path,
        cancel: Arc<AtomicBool>,
        progress: ProgressFn,
    ) -> ClientFuture<'a, u64> {
        Box::pin(async move {
            let saved = dest.file_name().unwrap().to_string_lossy().into_owned();
            self.world
                .log(json!({ "download": [user, filename, saved] }));
            let file = self.world.file(user, filename);
            let bytes = file
                .tags
                .as_ref()
                .map(|tags| flac(tags, file.pad))
                .unwrap_or_default();
            let size = bytes.len() as u64;
            let start = tokio::time::Instant::now();
            for step in &self.world.network.scripts[&file.script] {
                tokio::time::sleep_until(start + Duration::from_millis(step.at)).await;
                if cancel.load(Ordering::SeqCst) {
                    break;
                }
                if let Some(message) = &step.error {
                    return Err(message.clone());
                }
                if step.done {
                    // A download the provider gave up on has no folder left to write into.
                    if std::fs::write(dest, &bytes).is_err() {
                        break;
                    }
                    return Ok(size);
                }
                let phase = match step.phase.as_deref() {
                    Some("queued") => Phase::Queued,
                    Some("connecting") => Phase::Connecting,
                    Some("transferring") => Phase::Transferring,
                    Some("completed") => Phase::Completed,
                    other => panic!("unknown phase {other:?}"),
                };
                progress(Progress {
                    phase,
                    bytes: size * step.permille / 1000,
                    total: if phase == Phase::Transferring {
                        size
                    } else {
                        0
                    },
                    queue: step.queue,
                });
            }
            // The peer never finishes.
            std::future::pending().await
        })
    }

    fn close(&self) {
        self.world.log(json!({ "close": true }));
    }
}

/// The names in a folder, in the order JS sorts them (by UTF-16 code unit).
pub fn sorted_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    names.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
    names
}

/// Every folder and file under `root`: a `.json` file with its text, any other file with its size and hash.
pub fn tree(root: &Path) -> Value {
    fn walk(dir: &Path, rel: &str, out: &mut Vec<Value>) {
        for name in sorted_names(dir) {
            let full = dir.join(&name);
            let path = if rel.is_empty() {
                name.clone()
            } else {
                format!("{rel}/{name}")
            };
            if full.is_dir() {
                out.push(json!({ "path": format!("{path}/") }));
                walk(&full, &path, out);
            } else if name.ends_with(".json") {
                out.push(json!({ "path": path, "text": std::fs::read_to_string(&full).unwrap() }));
            } else {
                let bytes = std::fs::read(&full).unwrap();
                out.push(
                    json!({ "path": path, "size": bytes.len(), "sha256": sha256_hex(&bytes) }),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    Value::Array(out)
}

/// Job ids by the order they were made in, as the oracles name the app's random ones.
#[derive(Default)]
pub struct Labels(AtomicU64);

impl RandomSource for Labels {
    fn uuid_v4(&self) -> String {
        format!("job-{}", self.0.fetch_add(1, Ordering::SeqCst) + 1)
    }

    fn bytes(&self, len: usize) -> Vec<u8> {
        vec![0; len]
    }
}
