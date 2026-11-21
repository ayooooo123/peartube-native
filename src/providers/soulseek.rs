//! Soulseek provider for PearTube relays, ported from `src/providers/soulseek.js`:
//!
//!   - Reads the user, password, server and listen port from the relay's settings; inert
//!     (`not configured`) until user and password are set.
//!   - Searches Soulseek peers, ranking candidates by open slots and empty queue, queue depth, and
//!     lossless depth and rate ([`rank`]).
//!   - Browses one peer's shared media (kept in memory for a few minutes) so a folder can be
//!     inspected before mirroring.
//!   - Downloads albums one folder deep into `<staging>/<Artist> - <Album> (<Year>)/` (the relay's
//!     swept import folder), never rewriting peer bytes; a caller-supplied release id pins the
//!     folder to that album. The files stay in a work folder until the host says no other import
//!     holds the import folder, then are placed and imported in one step. A single track lands in
//!     its album's folder as `NN - Title.ext`, or fails when nothing names its album ([`place`]).
//!   - Jobs report `queued` (waiting on the peer), `transferring` or `failed` with a plain reason;
//!     a peer that never answers or never connects back fails the job instead of leaving it at 0
//!     bytes.
//!
//! The client is the Rust crate the app's addon wraps ([`client`]). [`Provider`] knows nothing of
//! the node: the mirror host registers it as its `soulseek` plugin and hands it its `placeAndScan`.

pub mod client;
pub mod place;
pub mod rank;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::FutureExt;
use futures_util::future::Shared;
use serde_json::{Map, Value, json};

use self::client::{Backend, Client, ClientSetup, Phase, Progress, ProgressFn};
use self::place::{LoneTrack, peer_folder_hint, place_single_track};
use self::rank::{Candidate, ShareList, rank, share_list};
use super::staging::{
    ALBUM_PIN, Cancelled, Place, PlaceAndScan, PlaceError, format_album_dir, free_path,
    is_release_id, note_mirrored, safe_segment, split_ext,
};
use crate::acquire::RandomSource;
use crate::app_settings::{js_string_or_empty, js_truthy};
use crate::identify::js_num;
use crate::protocol::{MAX_TITLE, is_id, is_media_file, js_trim, truncate_utf16, utf16_len};
use crate::tags::{Tags, parse_tags};

/// Maximum normalised file candidates returned by `POST /v1/providers/soulseek/search` after ranking.
pub const SOULSEEK_SEARCH_CAP: usize = 200;
const DEFAULT_SERVER: &str = "vps.slsknet.org:2271";
/// Peers open file connections back to this TCP port; it must be published or forwarded for
/// downloads to arrive.
pub const DEFAULT_LISTEN_PORT: u16 = 50300;
pub const DEFAULT_SHARE_MAX_FILES: u32 = 2000;
const TAG_READ_LIMIT: u64 = 1024 * 1024;
const BROWSE_TIMEOUT_MS: u64 = 30_000;
const BROWSE_TTL_MS: u64 = 5 * 60_000;
const BROWSE_CACHE_MAX: usize = 16;
// A download whose peer never answers (no queue place, no transfer) or says "ready" but never opens
// the file connection is failed with the reason; a peer that reported a queue place is waited on.
const NO_ANSWER_MS: u64 = 3 * 60_000;
const NO_CONNECT_MS: u64 = 90_000;

/// A request the provider refuses or could not finish. `status` is the HTTP status the app's error
/// carries, when it carries one; `reason` tags a peer failure so `info` can flag the listen port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoulseekError {
    pub message: String,
    pub status: Option<u16>,
    pub reason: Option<&'static str>,
}

impl SoulseekError {
    fn status(status: u16, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: Some(status),
            reason: None,
        }
    }

    fn plain(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            status: None,
            reason: None,
        }
    }
}

impl std::fmt::Display for SoulseekError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SoulseekError {}

/// `createSoulseekPlugin`'s options.
pub struct Options {
    pub storage: String,
    /// The folder mirrors land in: the relay passes the root `watch-dir` sweeps; `None` is
    /// `<storage>/staging`.
    pub staging_root: Option<String>,
    /// That folder by kind only, for `/v1/plugins`, which is public and never names a path.
    pub staging_label: String,
    pub user: String,
    pub pass: String,
    pub server: String,
    pub search_timeout_ms: u64,
    /// 0 lets the system pick a port each start, which cannot be published.
    pub listen_port: u16,
    /// Advertise and serve the album folders to peers; the client caps the index at
    /// `share_max_files`.
    pub share: bool,
    pub share_max_files: u32,
    pub browse_timeout_ms: u64,
    pub no_answer_ms: u64,
    pub no_connect_ms: u64,
    pub onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    /// What makes clients; `None` is a relay with no Soulseek client, which reports `unavailable`.
    pub backend: Option<Arc<dyn Backend>>,
    pub random: Arc<dyn RandomSource>,
    pub now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
}

impl Options {
    /// The app's defaults for a relay that keeps its files in `storage`.
    pub fn new(
        storage: impl Into<String>,
        backend: Option<Arc<dyn Backend>>,
        random: Arc<dyn RandomSource>,
        now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            storage: storage.into(),
            staging_root: None,
            staging_label: "storage".to_owned(),
            user: String::new(),
            pass: String::new(),
            server: DEFAULT_SERVER.to_owned(),
            search_timeout_ms: 5000,
            listen_port: DEFAULT_LISTEN_PORT,
            share: false,
            share_max_files: DEFAULT_SHARE_MAX_FILES,
            browse_timeout_ms: BROWSE_TIMEOUT_MS,
            no_answer_ms: NO_ANSWER_MS,
            no_connect_ms: NO_CONNECT_MS,
            onchange: None,
            backend,
            random,
            now_ms,
        }
    }
}

struct Creds {
    user: String,
    pass: String,
    server: String,
}

impl Creds {
    fn configured(&self) -> bool {
        !self.user.is_empty() && !self.pass.is_empty()
    }

    /// What must never show in an error or a job: the password, then the user.
    fn secrets(&self) -> Vec<String> {
        [&self.pass, &self.user]
            .into_iter()
            .filter(|secret| !secret.is_empty())
            .cloned()
            .collect()
    }
}

/// Whether a client can be made: not asked yet, yes, or no.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Availability {
    Unknown,
    Available,
    Unavailable,
}

type LoginFuture =
    Shared<Pin<Box<dyn Future<Output = Result<Arc<dyn Client>, SoulseekError>> + Send>>>;
type BrowseFuture =
    Shared<Pin<Box<dyn Future<Output = Result<Arc<Browsed>, SoulseekError>> + Send>>>;

/// One peer's share list and when it was asked for.
struct Browsed {
    list: ShareList,
    at: u64,
}

struct KnownFile {
    filename: String,
    size: u64,
}

struct State {
    creds: Creds,
    availability: Availability,
    client: Option<Arc<dyn Client>>,
    client_key: String,
    login: Option<LoginFuture>,
    closed: bool,
    /// Jobs in the order they were made, for `/v1/jobs`.
    jobs: Vec<Arc<Job>>,
    /// Share lists by username, oldest first.
    browse_cache: Vec<(String, u64, BrowseFuture)>,
    // Files this relay has seen a peer share (search hits, browse replies), so a mirror request
    // that names a file by its bare name, or without a size, still asks for the peer's real path
    // and can show a percentage from the start.
    seen_files: HashMap<String, Arc<KnownFile>>,
    // Evidence about the listen port this session: failures that mean "a peer could not reach us",
    // and transfers that moved bytes. The relay cannot probe its own public port, so the UI warns
    // from these.
    inbound_failures: u64,
    last_inbound_failure: Option<u64>,
    transfers_ok: u64,
}

struct Job {
    id: String,
    record: Mutex<JobRecord>,
}

/// One mirror, as `/v1/jobs` shows it.
struct JobRecord {
    release: Option<String>,
    title: String,
    /// `queued` (waiting on the peer), `transferring`, `announcing`, `done`, `failed` or `cancelled`.
    status: &'static str,
    created: u64,
    peer: String,
    file: String,
    file_index: usize,
    file_count: usize,
    /// The client's transfer phase.
    phase: &'static str,
    queue_place: Option<u32>,
    speed: u64,
    listen_port: u16,
    bytes: u64,
    total: Option<u64>,
    error: Option<String>,
    note: Option<String>,
    /// The size and the sha256 of each file the mirror added, once it is done.
    done: Option<(u64, Vec<Value>)>,
    cancelled: bool,
    progress_at: u64,
}

impl Job {
    /// The job without what only the provider needs. `idleMs`: how long a waiting or transferring
    /// job has gone without a phase change or a new byte, so a client can tell a slow peer from a
    /// stuck one without comparing clocks.
    fn view(&self, now: u64) -> Map<String, Value> {
        let record = self.record.lock().expect("soulseek job");
        let mut out = Map::new();
        out.insert("jobId".into(), json!(self.id));
        out.insert("id".into(), json!(record.release));
        out.insert("title".into(), json!(record.title));
        out.insert("plugin".into(), json!("soulseek"));
        out.insert("channel".into(), json!(true));
        out.insert("private".into(), json!(false));
        out.insert("status".into(), json!(record.status));
        out.insert("created".into(), json!(record.created));
        out.insert("peer".into(), json!(record.peer));
        out.insert("file".into(), json!(record.file));
        out.insert("fileIndex".into(), json!(record.file_index));
        out.insert("fileCount".into(), json!(record.file_count));
        out.insert("phase".into(), json!(record.phase));
        out.insert("queuePlace".into(), json!(record.queue_place));
        out.insert("speed".into(), json!(record.speed));
        out.insert("listenPort".into(), json!(record.listen_port));
        out.insert("bytes".into(), json!(record.bytes));
        out.insert("total".into(), json!(record.total));
        out.insert("error".into(), json!(record.error));
        if let Some(note) = &record.note {
            out.insert("note".into(), json!(note));
        }
        if let Some((size, sha256s)) = &record.done {
            out.insert("size".into(), json!(size));
            out.insert("sha256s".into(), json!(sha256s));
        }
        let waiting = record.status == "queued" || record.status == "transferring";
        let idle = waiting.then(|| now.saturating_sub(record.progress_at));
        out.insert("idleMs".into(), json!(idle));
        out
    }

    fn status(&self) -> &'static str {
        self.record.lock().expect("soulseek job").status
    }

    fn cancelled(&self) -> bool {
        self.record.lock().expect("soulseek job").cancelled
    }
}

/// One file of a mirror request.
struct Item {
    /// The peer's path.
    filename: String,
    /// The name it is saved under.
    base: String,
    /// Its size, 0 until someone states it.
    size: u64,
}

/// What a mirror was asked for, read from its payload.
struct Request {
    username: String,
    /// The caller's id, lowercased, or empty.
    caller_id: String,
    /// The album release the caller named.
    release: Option<String>,
    title: String,
    /// The track number in the caller's id (`mb:<release>:s01e05`).
    track: Option<u32>,
}

/// What a download run keeps between progress reports.
struct Run {
    /// The bytes of the files already saved.
    completed: u64,
    last_notify: u64,
    rate_at: u64,
    rate_bytes: u64,
    /// The file the reports are about.
    index: usize,
}

struct Inner {
    storage: String,
    staging_root: String,
    staging_label: String,
    search_timeout_ms: u64,
    listen_port: u16,
    share: bool,
    share_max_files: u32,
    browse_timeout_ms: u64,
    no_answer_ms: u64,
    no_connect_ms: u64,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    backend: Option<Arc<dyn Backend>>,
    random: Arc<dyn RandomSource>,
    now_ms: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<State>,
}

/// The provider (`createSoulseekPlugin`'s result).
#[derive(Clone)]
pub struct Provider(Arc<Inner>);

pub const NAME: &str = "soulseek";
pub const DESCRIPTION: &str =
    "Finds and mirrors lossless albums from Soulseek peers into the one-level album staging folder";

impl Provider {
    pub fn new(options: Options) -> Self {
        let server = js_trim(&options.server);
        let creds = Creds {
            user: js_trim(&options.user).to_owned(),
            pass: options.pass,
            server: if server.is_empty() {
                DEFAULT_SERVER
            } else {
                server
            }
            .to_owned(),
        };
        let configured = creds.configured();
        let provider = Self(Arc::new(Inner {
            staging_root: options
                .staging_root
                .unwrap_or_else(|| join(&options.storage, "staging")),
            storage: options.storage,
            staging_label: options.staging_label,
            search_timeout_ms: options.search_timeout_ms,
            listen_port: options.listen_port,
            share: options.share,
            share_max_files: options.share_max_files,
            browse_timeout_ms: options.browse_timeout_ms,
            no_answer_ms: options.no_answer_ms,
            no_connect_ms: options.no_connect_ms,
            onchange: options.onchange,
            backend: options.backend,
            random: options.random,
            now_ms: options.now_ms,
            state: Mutex::new(State {
                creds,
                availability: Availability::Unknown,
                client: None,
                client_key: String::new(),
                login: None,
                closed: false,
                jobs: Vec::new(),
                browse_cache: Vec::new(),
                seen_files: HashMap::new(),
                inbound_failures: 0,
                last_inbound_failure: None,
                transfers_ok: 0,
            }),
        }));
        // Whether a client can be made is asked only once credentials are set, so `/v1/plugins`
        // stays inert without them and reports `unavailable` with them on a relay that has none.
        if configured {
            provider.probe();
            provider.changed();
        }
        provider
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.0.state.lock().expect("soulseek state")
    }

    fn now(&self) -> u64 {
        (self.0.now_ms)()
    }

    fn changed(&self) {
        if let Some(onchange) = &self.0.onchange {
            onchange();
        }
    }

    /// Finds out whether a client can be made, once (`ensureAddon`).
    fn probe(&self) -> bool {
        let mut state = self.state();
        if state.availability == Availability::Unknown {
            state.availability = if self.0.backend.is_some() {
                Availability::Available
            } else {
                Availability::Unavailable
            };
        }
        state.availability == Availability::Available
    }

    fn close_client(state: &mut State) {
        state.login = None;
        state.client_key.clear();
        if let Some(client) = state.client.take() {
            client.close();
        }
    }

    /// The signed-in client, signing in first when it is not (`ensureLoggedIn`). Callers that
    /// arrive while a sign-in runs wait for that one.
    async fn logged_in(&self) -> Result<Arc<dyn Client>, SoulseekError> {
        let login = {
            let mut state = self.state();
            if state.closed {
                return Err(SoulseekError::status(503, "Soulseek plugin is closed"));
            }
            if !state.creds.configured() {
                return Err(SoulseekError::status(
                    400,
                    "Soulseek credentials are not configured",
                ));
            }
            drop(state);
            let backend = self.0.backend.as_ref().filter(|_| self.probe());
            let Some(backend) = backend else {
                return Err(SoulseekError::status(
                    503,
                    "Soulseek addon is unavailable on this platform",
                ));
            };
            state = self.state();
            let (host, port) = parse_server(&state.creds.server);
            let want = format!("{host}:{port}\n{}\n{}", state.creds.user, state.creds.pass);
            if state.client.is_some() && state.client_key != want {
                Self::close_client(&mut state);
            }
            let client = match &state.client {
                Some(client) => client.clone(),
                None => {
                    let client = backend.client(ClientSetup {
                        listen_port: self.0.listen_port,
                        share: self.0.share,
                        share_max_files: self.0.share_max_files,
                    });
                    state.client = Some(client.clone());
                    state.client_key = want;
                    client
                }
            };
            let now = client.state();
            if now.connected && now.logged_in {
                return Ok(client);
            }
            match &state.login {
                Some(login) => login.clone(),
                None => {
                    let this = self.clone();
                    let (user, pass) = (state.creds.user.clone(), state.creds.pass.clone());
                    let login: LoginFuture = async move {
                        let failure = match client.login(&host, port, &user, &pass).await {
                            Ok(true) => None,
                            Ok(false) => Some("Soulseek login rejected".to_owned()),
                            Err(message) if message.is_empty() => {
                                Some("Soulseek login failed".to_owned())
                            }
                            Err(message) => Some(message),
                        };
                        let mut state = this.state();
                        match failure {
                            None => {
                                state.login = None;
                                Ok(client)
                            }
                            Some(message) => {
                                Self::close_client(&mut state);
                                Err(SoulseekError::status(
                                    502,
                                    scrub(&message, &state.creds.secrets()),
                                ))
                            }
                        }
                    }
                    .boxed()
                    .shared();
                    state.login = Some(login.clone());
                    login
                }
            }
        };
        login.await
    }

    /// Refuses a call on a provider with no credentials, after asking whether a client can be
    /// made, so the next `info` says so.
    fn require_configured(&self) -> Result<(), SoulseekError> {
        if self.state().creds.configured() {
            return Ok(());
        }
        self.probe();
        Err(SoulseekError::status(400, "Soulseek is not configured"))
    }

    /// `search({ query, limit, timeoutMs })`: `{ candidates, cap, truncated }`.
    pub async fn search(&self, payload: &Value) -> Result<Value, SoulseekError> {
        self.require_configured()?;
        let query = js_string_or_empty(payload.get("query"));
        let query = js_trim(&query);
        if query.is_empty() {
            return Err(SoulseekError::status(400, "query is required"));
        }
        let client = self.logged_in().await?;
        let limit = js_num(payload.get("limit"));
        let cap = if limit.is_finite() && limit > 0.0 {
            (limit.trunc() as usize).min(SOULSEEK_SEARCH_CAP)
        } else {
            SOULSEEK_SEARCH_CAP
        };
        let timeout = js_num(payload.get("timeoutMs"));
        let wait_ms = if timeout.is_finite() && timeout >= 50.0 {
            (timeout.trunc() as u64).min(30_000)
        } else {
            self.0.search_timeout_ms
        };
        let hits = client.search(query, wait_ms).await.map_err(|message| {
            let message = if message.is_empty() {
                "Soulseek search failed"
            } else {
                &message
            };
            SoulseekError::status(502, scrub(message, &self.state().creds.secrets()))
        })?;
        let found = hits.len();
        let candidates = rank(hits, cap);
        self.remember(&candidates);
        Ok(json!({
            "candidates": candidates,
            "cap": SOULSEEK_SEARCH_CAP,
            "truncated": found > candidates.len(),
        }))
    }

    /// The port peers are told to connect back to: the bound port once a client exists, else the
    /// configured one.
    fn advertised_port(&self) -> u16 {
        match &self.state().client {
            Some(client) => client.state().listen_port,
            None => self.0.listen_port,
        }
    }

    fn remember(&self, rows: &[Candidate]) {
        let mut state = self.state();
        if state.seen_files.len() > 50_000 {
            state.seen_files.clear();
        }
        for row in rows {
            let file = Arc::new(KnownFile {
                filename: row.filename.clone(),
                size: row.size,
            });
            state
                .seen_files
                .insert(format!("{}\n{}", row.username, row.filename), file.clone());
            let leaf = basename(&row.filename.replace('\\', "/")).to_lowercase();
            state
                .seen_files
                .insert(format!("{}\n{leaf}", row.username), file);
        }
    }

    /// The file `user` shares under `filename`: its full path, or its bare name in any case.
    fn known_file(&self, user: &str, filename: &str) -> Option<Arc<KnownFile>> {
        let state = self.state();
        if let Some(file) = state.seen_files.get(&format!("{user}\n{filename}")) {
            return Some(file.clone());
        }
        if filename.contains(['\\', '/']) {
            return None;
        }
        state
            .seen_files
            .get(&format!("{user}\n{}", filename.to_lowercase()))
            .cloned()
    }

    fn note_peer_failure(&self, error: &SoulseekError) {
        if !matches!(error.reason, Some("no-inbound" | "no-answer")) {
            return;
        }
        let now = self.now();
        let mut state = self.state();
        state.inbound_failures += 1;
        state.last_inbound_failure = Some(now);
    }

    /// `browse({ username, refresh })`: a peer's shared media as `{ username, files, tracks, size,
    /// at, cached }`, kept per username for five minutes. A second call while the first is on its
    /// way shares its answer.
    pub async fn browse(&self, payload: &Value) -> Result<Value, SoulseekError> {
        self.require_configured()?;
        let user = js_string_or_empty(payload.get("username"));
        let user = js_trim(&user);
        if user.is_empty() {
            return Err(SoulseekError::status(400, "username is required"));
        }
        let refresh = payload.get("refresh").is_some_and(js_truthy);
        let (browsed, cached) = self.browsed(user, refresh).await?;
        let mut out = match serde_json::to_value(&browsed.list) {
            Ok(Value::Object(out)) => out,
            _ => Map::new(),
        };
        out.insert("at".into(), json!(browsed.at));
        out.insert("cached".into(), json!(cached));
        Ok(Value::Object(out))
    }

    async fn browsed(
        &self,
        user: &str,
        refresh: bool,
    ) -> Result<(Arc<Browsed>, bool), SoulseekError> {
        let hit = {
            let state = self.state();
            let now = self.now();
            state
                .browse_cache
                .iter()
                .find(|(name, at, _)| {
                    name == user && !refresh && now.saturating_sub(*at) < BROWSE_TTL_MS
                })
                .map(|(_, _, answer)| answer.clone())
        };
        if let Some(answer) = hit {
            return Ok((answer.await?, true));
        }
        let client = self.logged_in().await?;
        let at = self.now();
        let this = self.clone();
        let name = user.to_owned();
        let answer: BrowseFuture = async move {
            match client.browse(&name, this.0.browse_timeout_ms).await {
                Ok(records) => {
                    let list = share_list(records, &name);
                    this.remember(&list.files);
                    Ok(Arc::new(Browsed { list, at }))
                }
                Err(message) => {
                    this.state()
                        .browse_cache
                        .retain(|(user, _, _)| *user != name);
                    let port = this.advertised_port();
                    let secrets = this.state().creds.secrets();
                    let error = peer_error(&message, &name, port, &secrets);
                    this.note_peer_failure(&error);
                    this.changed();
                    Err(error)
                }
            }
        }
        .boxed()
        .shared();
        {
            let mut state = self.state();
            state.browse_cache.retain(|(name, _, _)| name != user);
            state
                .browse_cache
                .push((user.to_owned(), at, answer.clone()));
            let over = state.browse_cache.len().saturating_sub(BROWSE_CACHE_MAX);
            state.browse_cache.drain(..over);
        }
        Ok((answer.await?, false))
    }

    fn trim_jobs(state: &mut State) {
        if state.jobs.len() <= 48 {
            return;
        }
        let mut left = state.jobs.len();
        state.jobs.retain(|job| {
            let over = left > 32 && matches!(job.status(), "done" | "failed" | "cancelled");
            if over {
                left -= 1;
            }
            !over
        });
    }

    /// `mirror({ username, files, id, releaseId, title, wait })`: downloads the files into a work
    /// folder, then has the host place them in one album folder of the staging root and import it
    /// (`place_and_scan`; without one, a bare provider only places them). With `wait` the answer is
    /// the finished mirror; without, `{ queued, job }` at once while the download goes on.
    pub async fn mirror(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, SoulseekError> {
        self.require_configured()?;
        let username = js_string_or_empty(payload.get("username"));
        let username = js_trim(&username).to_owned();
        if username.is_empty() {
            return Err(SoulseekError::status(400, "username is required"));
        }
        let mut items = Vec::new();
        for entry in payload
            .get("files")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let named = match entry {
                Value::String(name) => name.clone(),
                other => js_string_or_empty(other.get("filename")),
            };
            let named = js_trim(&named);
            if named.is_empty() {
                continue;
            }
            let known = self.known_file(&username, named);
            let filename = known
                .as_ref()
                .map_or(named, |known| known.filename.as_str());
            let base = basename(&filename.replace('\\', "/")).to_owned();
            if base.is_empty() || !is_media_file(&base) {
                continue;
            }
            let stated = js_num(entry.get("size"));
            let size = if stated.is_finite() && stated > 0.0 {
                stated.trunc() as u64
            } else {
                known.as_ref().map_or(0, |known| known.size)
            };
            items.push(Item {
                filename: filename.to_owned(),
                base: safe_segment(&base, "track.flac"),
                size,
            });
        }
        if items.is_empty() {
            return Err(SoulseekError::status(
                400,
                "files must list at least one audio file",
            ));
        }

        let client = self.logged_in().await?;
        let caller_id = payload
            .get("id")
            .and_then(Value::as_str)
            .map(|id| js_trim(id).to_lowercase())
            .unwrap_or_default();
        let request = Request {
            release: parse_release(&caller_id).or_else(|| {
                payload
                    .get("releaseId")
                    .and_then(Value::as_str)
                    .and_then(parse_release)
            }),
            title: payload
                .get("title")
                .and_then(Value::as_str)
                .map(|title| truncate_utf16(js_trim(title), MAX_TITLE))
                .unwrap_or_default(),
            // A caller that names the track (`mb:<release>:s01e05`) gives its number to a lone file
            // with none of its own.
            track: track_of(&caller_id),
            caller_id,
            username,
        };

        let now = self.now();
        let listen_port = self.advertised_port();
        let job = Arc::new(Job {
            id: self.0.random.uuid_v4(),
            record: Mutex::new(JobRecord {
                release: if is_id(&request.caller_id) {
                    Some(request.caller_id.clone())
                } else {
                    request.release.clone()
                },
                title: if !request.title.is_empty() {
                    request.title.clone()
                } else {
                    peer_folder_hint(&items[0].filename)
                        .map(|hint| hint.album)
                        .filter(|album| !album.is_empty())
                        .unwrap_or_else(|| "Album download".to_owned())
                },
                status: "queued",
                created: now,
                peer: request.username.clone(),
                file: basename(&items[0].filename.replace('\\', "/")).to_owned(),
                file_index: 1,
                file_count: items.len(),
                phase: Phase::Queued.as_str(),
                queue_place: None,
                speed: 0,
                listen_port,
                bytes: 0,
                total: album_total(&items),
                error: None,
                note: None,
                done: None,
                cancelled: false,
                progress_at: now,
            }),
        });
        {
            let mut state = self.state();
            Self::trim_jobs(&mut state);
            state.jobs.push(job.clone());
        }
        self.changed();

        let work_dir = Path::new(&self.0.storage).join(format!(".slsk-tmp-{}", job.id));
        make_private_dir(&work_dir).map_err(|error| SoulseekError::plain(error.to_string()))?;

        let wait = payload.get("wait") == Some(&Value::Bool(true));
        let run = self.clone().run_download(
            client,
            job.clone(),
            request,
            items,
            work_dir,
            place_and_scan,
        );
        if wait {
            return run.await;
        }
        tokio::spawn(run);
        Ok(json!({ "queued": true, "job": job.view(self.now()) }))
    }

    /// Downloads every file of a mirror, places them and has them imported; the job follows it.
    async fn run_download(
        self,
        client: Arc<dyn Client>,
        job: Arc<Job>,
        request: Request,
        items: Vec<Item>,
        work_dir: PathBuf,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, SoulseekError> {
        let outcome = self
            .download_and_place(&client, &job, &request, items, &work_dir, place_and_scan)
            .await;
        let failure = match outcome {
            Ok(done) => return Ok(done),
            Err(failure) => failure,
        };
        let _ = std::fs::remove_dir_all(&work_dir);
        let message = {
            let secrets = self.state().creds.secrets();
            let mut record = job.record.lock().expect("soulseek job");
            if !record.cancelled {
                record.status = "failed";
                record.phase = Phase::Failed.as_str();
                record.error = Some(scrub(&failure.message, &secrets));
            }
            record.speed = 0;
            record.error.clone()
        };
        if !job.cancelled() {
            self.note_peer_failure(&failure);
        }
        self.changed();
        Err(SoulseekError::status(
            failure.status.unwrap_or(502),
            message.unwrap_or_else(|| "Cancelled".to_owned()),
        ))
    }

    fn stopped(&self, job: &Job) -> bool {
        self.state().closed || job.cancelled()
    }

    async fn download_and_place(
        &self,
        client: &Arc<dyn Client>,
        job: &Arc<Job>,
        request: &Request,
        items: Vec<Item>,
        work_dir: &Path,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, SoulseekError> {
        let cancelled = || SoulseekError::plain("Cancelled");
        let items = Arc::new(Mutex::new(items));
        let run = Arc::new(Mutex::new(Run {
            completed: 0,
            last_notify: 0,
            rate_at: 0,
            rate_bytes: 0,
            index: 0,
        }));
        // The name each file was saved under in the work folder, and its size.
        let mut downloaded: Vec<(String, PathBuf, u64)> = Vec::new();
        let count = items.lock().expect("soulseek items").len();
        for index in 0..count {
            if self.stopped(job) {
                return Err(cancelled());
            }
            let (filename, base) = {
                let items = items.lock().expect("soulseek items");
                (items[index].filename.clone(), items[index].base.clone())
            };
            let taken = downloaded
                .iter()
                .any(|(name, _, _)| name.to_lowercase() == base.to_lowercase());
            let dest_name = if taken {
                let (stem, ext) = split_ext(&base);
                format!("{:02} - {stem}{ext}", index + 1)
            } else {
                base
            };
            let tmp_dest = work_dir.join(&dest_name);
            {
                let mut record = job.record.lock().expect("soulseek job");
                record.status = "queued";
                record.phase = Phase::Queued.as_str();
                record.queue_place = None;
                record.speed = 0;
                record.progress_at = self.now();
                record.file = basename(&filename.replace('\\', "/")).to_owned();
                record.file_index = index + 1;
            }
            {
                // Speed is measured here from bytes received over at least a second, smoothed.
                let mut run = run.lock().expect("soulseek run");
                run.rate_at = 0;
                run.rate_bytes = 0;
                run.index = index;
            }
            self.changed();
            let report = self.reporter(job.clone(), items.clone(), run.clone());
            let size = self
                .download_one(client, job, &request.username, &filename, &tmp_dest, report)
                .await?;
            let size = if size > 0 {
                size
            } else {
                std::fs::metadata(&tmp_dest)
                    .map_err(|error| SoulseekError::plain(error.to_string()))?
                    .len()
            };
            let completed = {
                let mut run = run.lock().expect("soulseek run");
                run.completed += size;
                run.completed
            };
            if size > 0 {
                self.state().transfers_ok += 1;
            }
            {
                let mut record = job.record.lock().expect("soulseek job");
                record.bytes = completed;
                record.speed = 0;
                if record.total.is_some_and(|total| completed > total) {
                    record.total = Some(completed);
                }
            }
            downloaded.push((dest_name, tmp_dest, size));
            self.changed();
        }

        if self.stopped(job) {
            return Err(cancelled());
        }
        // `waiting` until this album's own import starts: nothing is in the import folder yet and
        // Cancel works.
        {
            let mut record = job.record.lock().expect("soulseek job");
            record.status = "announcing";
            record.phase = "waiting";
        }
        self.changed();
        let completed = run.lock().expect("soulseek run").completed;
        let (first_filename, first_base) = {
            let items = items.lock().expect("soulseek items");
            (items[0].filename.clone(), items[0].base.clone())
        };

        // Name the folder from the peer's tags (or its folder name). Peer bytes are stored as
        // served: when the caller named the album, the folder is pinned to that release and the
        // importer announces every file under it. One track on its own (not a clip or film the
        // caller named) is placed as a track of its album instead.
        let tags: Vec<Option<Tags>> = downloaded
            .iter()
            .map(|(_, path, _)| read_head_tags(path))
            .collect();
        let named_other = is_id(&request.caller_id) && request.release.is_none();
        let single = if downloaded.len() == 1 && !named_other {
            let numbered = tags[0].as_ref().is_some_and(|tags| tags.track.is_some())
                || request.track.is_some()
                || leading_number(split_ext(&first_base).0);
            let position = if numbered {
                None
            } else {
                self.folder_position(&request.username, &first_filename)
                    .await
            };
            let placed = place_single_track(LoneTrack {
                tags: tags[0].as_ref(),
                filename: &first_filename,
                release: request.release.as_deref(),
                track: request.track,
                position,
                fallback_title: &request.title,
            })
            .map_err(|message| SoulseekError {
                message,
                status: Some(422),
                reason: Some("unplaced"),
            })?;
            Some(placed)
        } else {
            None
        };
        let said = |text: Option<&str>| text.filter(|text| !text.is_empty()).map(str::to_owned);
        let named = tags.iter().flatten().find(|tags| {
            [&tags.album, &tags.album_artist, &tags.artist]
                .into_iter()
                .any(|name| name.as_deref().is_some_and(|name| !name.is_empty()))
        });
        let hint = peer_folder_hint(&first_filename);
        let artist = said(named.and_then(|tags| tags.album_artist.as_deref()))
            .or_else(|| said(named.and_then(|tags| tags.artist.as_deref())))
            .or_else(|| said(hint.as_ref().map(|hint| hint.artist.as_str())));
        let album = said(named.and_then(|tags| tags.album.as_deref()))
            .or_else(|| said(hint.as_ref().map(|hint| hint.album.as_str())));
        let year = named
            .and_then(|tags| tags.year)
            .filter(|year| *year > 0)
            .or(hint.as_ref().and_then(|hint| hint.year));
        let folder_name = match &single {
            Some(single) => single.folder.clone(),
            None => format_album_dir(
                artist.as_deref().unwrap_or_default(),
                album.as_deref().unwrap_or_default(),
                year,
                &request.title,
            ),
        };
        {
            let mut record = job.record.lock().expect("soulseek job");
            if let Some(single) = &single {
                if request.title.is_empty() {
                    record.title = joined_title(&[&single.track_title, &single.album_title]);
                }
                // Said on the job, so the owner knows the album came from a folder name and not
                // from the file itself.
                if single.from == "peer folder" {
                    record.note = Some(format!(
                        "No album tag: placed in “{folder_name}” from the peer's folder name."
                    ));
                }
            } else if request.title.is_empty()
                && let Some(album) = &album
            {
                let title = joined_title(&[artist.as_deref().unwrap_or_default(), album]);
                record.title = if title.is_empty() {
                    folder_name.clone()
                } else {
                    title
                };
            }
        }

        if self.stopped(job) {
            return Err(cancelled());
        }
        let album_dir = Path::new(&self.0.staging_root).join(&folder_name);
        let placed: Arc<Mutex<Vec<String>>> = Arc::default();
        // Runs only once no other import holds the folder (`place_and_scan`), in one step with
        // this album's own import: until then the files stay in the work folder, so a mirror that
        // is cancelled or gives up while it waits leaves nothing for a later scan to add.
        let place: Place = Box::new({
            let (provider, job, placed) = (self.clone(), job.clone(), placed.clone());
            let (release, single) = (request.release.clone(), single.clone());
            let (caller_id, downloaded) = (request.caller_id.clone(), downloaded.clone());
            let work_dir = work_dir.to_owned();
            move || {
                let io = |error: std::io::Error| PlaceError {
                    message: error.to_string(),
                    status: None,
                };
                std::fs::create_dir_all(&album_dir).map_err(io)?;
                // The pin goes in before the files. A lone track pins a new folder to its album; a
                // folder that already has a pin keeps it.
                let pin_path = album_dir.join(ALBUM_PIN);
                let pin = match (&release, &single) {
                    (Some(release), Some(single)) => {
                        Some(json!({ "release": release, "title": single.album_title }))
                    }
                    (Some(release), None) => Some(json!({ "release": release })),
                    (None, Some(single)) if !pin_path.exists() => {
                        Some(json!({ "release": single.release, "title": single.album_title }))
                    }
                    _ => None,
                };
                if let Some(pin) = pin {
                    std::fs::write(&pin_path, format!("{pin}\n")).map_err(io)?;
                }
                let title = job.record.lock().expect("soulseek job").title.clone();
                let mut names = placed.lock().expect("placed names");
                for (name, tmp_path, _) in &downloaded {
                    let wanted = single.as_ref().map_or(name, |single| &single.file);
                    let dest = free_path(&album_dir, wanted);
                    let dest_name = dest
                        .file_name()
                        .map(|name| name.to_string_lossy().into_owned())
                        .unwrap_or_default();
                    if !names.contains(&dest_name) {
                        names.push(dest_name);
                    }
                    // Across file systems a rename fails: copy, then drop the original.
                    if std::fs::rename(tmp_path, &dest).is_err() {
                        std::fs::copy(tmp_path, &dest).map_err(io)?;
                        let _ = std::fs::remove_file(tmp_path);
                    }
                    // A single file mirrored for a non-album id (a clip or film) carries that id
                    // in a sidecar.
                    if downloaded.len() == 1 && named_other {
                        let sidecar = json!({ "id": caller_id, "title": title });
                        let mut path = dest.into_os_string();
                        path.push(".json");
                        std::fs::write(path, sidecar.to_string()).map_err(io)?;
                    }
                }
                note_mirrored(&album_dir, &names).map_err(io)?;
                drop(names);
                let _ = std::fs::remove_dir_all(&work_dir);
                job.record.lock().expect("soulseek job").phase = "importing";
                provider.changed();
                Ok(())
            }
        });
        let files = downloaded.len();

        let scanned = match &place_and_scan {
            Some(host) => {
                let stopped: Cancelled = Arc::new({
                    let (provider, job) = (self.clone(), job.clone());
                    move || provider.stopped(&job)
                });
                host(place, stopped).await
            }
            // A bare provider has no host to import for it: the files are only placed.
            None => place().map(|()| json!({ "scanned": 0, "ingested": 0, "items": [] })),
        }
        .map_err(|error| SoulseekError {
            message: error.message,
            status: error.status,
            reason: None,
        })?;
        let placed = placed.lock().expect("placed names").clone();
        let items = scanned
            .get("items")
            .filter(|items| js_truthy(items))
            .cloned()
            .unwrap_or_else(|| json!([]));
        // The sha256 of each file this album added, so a client can open the album the mirror
        // created. By path inside the import folder: another album's file of the same name is not
        // this mirror's.
        let sha256s: Vec<Value> = items
            .as_array()
            .into_iter()
            .flatten()
            .filter(|item| {
                item.get("file")
                    .and_then(Value::as_str)
                    .is_some_and(|file| {
                        placed.iter().any(|name| name == file)
                            && item.get("rel").and_then(Value::as_str)
                                == Some(format!("{folder_name}/{file}").as_str())
                    })
            })
            .map(|item| item.get("sha256").cloned().unwrap_or(Value::Null))
            .collect();
        let note = {
            let mut record = job.record.lock().expect("soulseek job");
            record.status = "done";
            record.bytes = completed;
            record.total = Some(completed);
            record.done = Some((completed, sha256s));
            record.note.clone()
        };
        self.changed();
        let mut out = Map::new();
        out.insert("jobId".into(), json!(job.id));
        out.insert("folder".into(), json!(folder_name));
        out.insert("files".into(), json!(files));
        out.insert("bytes".into(), json!(completed));
        let ingested = scanned.get("ingested").filter(|count| js_truthy(count));
        out.insert("ingested".into(), ingested.cloned().unwrap_or(json!(0)));
        out.insert("items".into(), items);
        // Where a lone track went and what named its album: `tags`, `peer folder` or the caller's
        // `release`.
        if let Some(single) = &single {
            out.insert(
                "placed".into(),
                json!({
                    "file": placed.first(),
                    "from": single.from,
                    "release": single.release,
                    "note": note,
                }),
            );
        }
        Ok(Value::Object(out))
    }

    /// What takes the progress of the file a run is on: the job's phase, bytes, queue place and
    /// speed follow it.
    fn reporter(
        &self,
        job: Arc<Job>,
        items: Arc<Mutex<Vec<Item>>>,
        run: Arc<Mutex<Run>>,
    ) -> Arc<dyn Fn(Progress) + Send + Sync> {
        let this = self.clone();
        Arc::new(move |progress: Progress| {
            let now = this.now();
            let notify = {
                let mut run = run.lock().expect("soulseek run");
                let mut record = job.record.lock().expect("soulseek job");
                // The peer states the file's size when it starts sending; a larger or first-known
                // size updates the total.
                {
                    let mut items = items.lock().expect("soulseek items");
                    if progress.total > items[run.index].size {
                        items[run.index].size = progress.total;
                        if let Some(total) = album_total(&items) {
                            record.total = Some(record.total.unwrap_or(0).max(total));
                        }
                    }
                }
                let phase = progress.phase.as_str();
                let changed = record.phase != phase;
                let bytes = run.completed + progress.bytes;
                if changed || bytes > record.bytes {
                    record.progress_at = now;
                }
                record.phase = phase;
                record.status = match progress.phase {
                    Phase::Transferring | Phase::Completed => "transferring",
                    _ => "queued",
                };
                if progress.queue.is_some() {
                    record.queue_place = progress.queue;
                }
                record.bytes = bytes;
                if progress.phase != Phase::Transferring {
                    run.rate_at = 0;
                    record.speed = 0;
                } else if run.rate_at == 0 {
                    run.rate_at = now;
                    run.rate_bytes = bytes;
                } else if now - run.rate_at >= 1000 {
                    // The client's own figure jumps to impossible values between reports.
                    let rate = (bytes as f64 - run.rate_bytes as f64) * 1000.0
                        / (now - run.rate_at) as f64;
                    let smoothed = if record.speed > 0 {
                        record.speed as f64 * 0.5 + rate * 0.5
                    } else {
                        rate
                    };
                    record.speed = js_round(smoothed);
                    run.rate_at = now;
                    run.rate_bytes = bytes;
                }
                let notify = changed || now - run.last_notify >= 350;
                if notify {
                    run.last_notify = now;
                }
                notify
            };
            if notify {
                this.changed();
            }
        })
    }

    /// One file through the client, failed by a watchdog when the peer never answers or never
    /// connects back (the client itself waits for as long as it takes), and when the job is
    /// cancelled. A peer that reported a queue place is waited on.
    async fn download_one(
        &self,
        client: &Arc<dyn Client>,
        job: &Arc<Job>,
        username: &str,
        filename: &str,
        dest: &Path,
        report: Arc<dyn Fn(Progress) + Send + Sync>,
    ) -> Result<u64, SoulseekError> {
        struct Watch {
            phase: Phase,
            /// When the phase last changed or, while transferring, a byte last arrived.
            since: u64,
            answered: bool,
            bytes: u64,
            settled: bool,
        }
        let watch = Arc::new(Mutex::new(Watch {
            phase: Phase::Queued,
            since: self.now(),
            answered: false,
            bytes: 0,
            settled: false,
        }));
        let port = self.advertised_port();
        let progress: ProgressFn = {
            let watch = watch.clone();
            let now_ms = self.0.now_ms.clone();
            Arc::new(move |progress: Progress| {
                {
                    let mut watch = watch.lock().expect("soulseek watch");
                    if watch.settled {
                        return;
                    }
                    if progress.phase != watch.phase || progress.bytes > watch.bytes {
                        watch.since = now_ms();
                    }
                    watch.phase = progress.phase;
                    watch.bytes = watch.bytes.max(progress.bytes);
                    if progress.queue.is_some() {
                        watch.answered = true;
                    }
                }
                report(progress);
            })
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let download = client.download(username, filename, dest, cancel.clone(), progress);
        tokio::pin!(download);
        let second = Duration::from_secs(1);
        let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + second, second);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let shown = if port == 0 {
            "(none)".to_owned()
        } else {
            port.to_string()
        };
        let outcome = loop {
            tokio::select! {
                done = &mut download => {
                    break done.map_err(|message| {
                        peer_error(&message, username, port, &self.state().creds.secrets())
                    });
                }
                _ = ticks.tick() => {
                    if self.stopped(job) {
                        break Err(SoulseekError::plain("Cancelled"));
                    }
                    let (phase, idle, answered) = {
                        let watch = watch.lock().expect("soulseek watch");
                        (watch.phase, self.now().saturating_sub(watch.since), watch.answered)
                    };
                    let minutes = js_round(self.0.no_answer_ms as f64 / 60_000.0).max(1);
                    let failure = if phase == Phase::Connecting && idle > self.0.no_connect_ms {
                        Some((
                            format!("{username} was ready to send but could not connect to this relay on port {shown}."),
                            "no-inbound",
                        ))
                    } else if phase == Phase::Queued && !answered && idle > self.0.no_answer_ms {
                        Some((
                            format!("{username} never answered in {minutes} min: it may be offline, or unable to reach this relay on port {shown}."),
                            "no-answer",
                        ))
                    } else if phase == Phase::Transferring && idle > self.0.no_answer_ms {
                        Some((
                            format!("{username} stopped sending: no data for {minutes} min."),
                            "stalled",
                        ))
                    } else {
                        None
                    };
                    if let Some((message, reason)) = failure {
                        break Err(SoulseekError { message, status: None, reason: Some(reason) });
                    }
                }
            }
        };
        watch.lock().expect("soulseek watch").settled = true;
        if outcome.is_err() {
            cancel.store(true, Ordering::SeqCst);
        }
        outcome
    }

    /// The place `filename` sorts at among the media files of its folder on the peer (1-based),
    /// from the peer's share list (kept after a browse, else asked for now), or `None`. Names sort
    /// the way a person reads them, so "A2" comes before "A10" and side A before side B.
    async fn folder_position(&self, username: &str, filename: &str) -> Option<u32> {
        let want = filename.replace('\\', "/");
        let dir = &want[..want.rfind('/').map_or(0, |slash| slash + 1)];
        let (browsed, _) = self.browsed(username, false).await.ok()?;
        let mut names: Vec<String> = browsed
            .list
            .files
            .iter()
            .map(|file| file.filename.replace('\\', "/"))
            .filter(|name| {
                name.strip_prefix(dir)
                    .is_some_and(|rest| !rest.contains('/'))
            })
            .collect();
        names.sort_by(|a, b| natural_order(a, b));
        let at = names.iter().position(|name| *name == want)?;
        u32::try_from(at + 1).ok()
    }

    /// `/v1/jobs`: every mirror this provider knows.
    pub fn list_jobs(&self) -> Vec<Value> {
        let jobs = self.state().jobs.clone();
        let now = self.now();
        jobs.iter()
            .map(|job| Value::Object(job.view(now)))
            .collect()
    }

    fn job(&self, job_id: &str) -> Option<Arc<Job>> {
        self.state()
            .jobs
            .iter()
            .find(|job| job.id == job_id)
            .cloned()
    }

    pub fn get_job(&self, job_id: &str) -> Option<Value> {
        let job = self.job(job_id)?;
        Some(Value::Object(job.view(self.now())))
    }

    /// Cancels a mirror that waits or transfers, or that waits for another import with nothing
    /// placed yet; one that is over is removed from the list instead (`status: "removed"`), and
    /// one whose own import has started is left alone.
    pub fn cancel_job(&self, job_id: &str) -> Option<Value> {
        let job = self.job(job_id)?;
        let status = {
            let mut record = job.record.lock().expect("soulseek job");
            if matches!(record.status, "queued" | "transferring")
                || (record.status == "announcing" && record.phase == "waiting")
            {
                record.cancelled = true;
                record.status = "cancelled";
                None
            } else {
                Some(record.status)
            }
        };
        match status {
            None => {
                self.changed();
                Some(Value::Object(job.view(self.now())))
            }
            Some("announcing") => Some(Value::Object(job.view(self.now()))),
            Some(_) => {
                self.state().jobs.retain(|other| other.id != job_id);
                self.changed();
                let mut view = job.view(self.now());
                view.insert("status".into(), json!("removed"));
                Some(Value::Object(view))
            }
        }
    }

    /// Live settings: a new user, password or server signs the client out; the next call signs in
    /// with them. `None` leaves a value as it is.
    pub fn configure(&self, user: Option<&str>, pass: Option<&str>, server: Option<&str>) {
        let probe = {
            let mut state = self.state();
            let mut changed = false;
            if let Some(user) = user.map(js_trim)
                && user != state.creds.user
            {
                state.creds.user = user.to_owned();
                changed = true;
            }
            if let Some(pass) = pass
                && pass != state.creds.pass
            {
                state.creds.pass = pass.to_owned();
                changed = true;
            }
            if let Some(server) = server.map(js_trim) {
                let server = if server.is_empty() {
                    DEFAULT_SERVER
                } else {
                    server
                };
                if server != state.creds.server {
                    state.creds.server = server.to_owned();
                    changed = true;
                }
            }
            if !changed {
                return;
            }
            Self::close_client(&mut state);
            state.creds.configured() && state.availability == Availability::Unknown
        };
        self.changed();
        if probe {
            self.probe();
            self.changed();
        }
    }

    /// What `/v1/plugins` shows of the provider.
    pub fn info(&self) -> Value {
        let state = self.state();
        let configured = state.creds.configured();
        let active = state
            .jobs
            .iter()
            .filter(|job| matches!(job.status(), "queued" | "transferring" | "announcing"))
            .count();
        if state.availability == Availability::Unavailable {
            return json!({
                "status": "unavailable",
                "available": false,
                "configured": configured,
                "connected": false,
                "activeJobs": active,
                "searchCap": SOULSEEK_SEARCH_CAP,
            });
        }
        let client = state.client.as_ref().map(|client| client.state());
        let logged_in = client.is_some_and(|client| client.logged_in);
        let status = if !configured {
            "not configured"
        } else if logged_in {
            "connected"
        } else {
            "configured"
        };
        json!({
            "status": status,
            "available": (state.availability == Availability::Available).then_some(true),
            "configured": configured,
            "connected": logged_in,
            "activeJobs": active,
            "searchCap": SOULSEEK_SEARCH_CAP,
            // Where mirrors land, by kind only ('import folder' | 'storage' …).
            "staging": self.0.staging_label,
            // Port peers connect back to: the bound port once signed in (0 = the configured port
            // could not be opened), null before the first sign-in. `listenPortSetting` 0 means
            // "the system picks one each start".
            "listenPort": client.map(|client| client.listen_port),
            "listenPortSetting": self.0.listen_port,
            // This session's evidence: peers that could not reach us against transfers that moved
            // bytes.
            "inboundFailures": state.inbound_failures,
            "lastInboundFailure": state.last_inbound_failure,
            "transfersOk": state.transfers_ok,
        })
    }

    /// Ends the provider: running mirrors are told to stop and the client signs out.
    pub fn stop(&self) {
        let mut state = self.state();
        state.closed = true;
        for job in &state.jobs {
            let mut record = job.record.lock().expect("soulseek job");
            if matches!(record.status, "queued" | "transferring") {
                record.cancelled = true;
            }
            // A mirror waiting for another import has placed nothing: closing ends it as a
            // cancel, not as a failure.
            if record.status == "announcing" && record.phase == "waiting" {
                record.cancelled = true;
                record.status = "cancelled";
            }
        }
        state.browse_cache.clear();
        Self::close_client(&mut state);
    }

    /// The plugin's `run`: a browse, a search or a mirror by what the payload holds, else a
    /// sign-in that answers with `info`.
    pub async fn run(
        &self,
        payload: &Value,
        place_and_scan: Option<PlaceAndScan>,
    ) -> Result<Value, SoulseekError> {
        if !self.state().creds.configured() {
            let status = if self.probe() {
                "not configured"
            } else {
                "unavailable"
            };
            return Ok(json!({ "skipped": true, "status": status }));
        }
        let action = payload.get("action").and_then(Value::as_str);
        let files = payload.get("files").is_some_and(Value::is_array);
        if action == Some("browse") {
            return self.browse(payload).await;
        }
        if action == Some("search")
            || (payload.get("query").is_some_and(Value::is_string) && !files)
        {
            return self.search(payload).await;
        }
        if files {
            return self.mirror(payload, place_and_scan).await;
        }
        self.logged_in().await?;
        let mut out = Map::new();
        out.insert("ok".into(), json!(true));
        if let Value::Object(info) = self.info() {
            out.extend(info);
        }
        Ok(Value::Object(out))
    }
}

/// `Math.round` of a number that is not negative.
fn js_round(number: f64) -> u64 {
    (number + 0.5).floor() as u64
}

fn join(dir: &str, name: &str) -> String {
    Path::new(dir).join(name).to_string_lossy().into_owned()
}

/// The last segment of a path with `/` separators, as `basename` gives it.
fn basename(path: &str) -> &str {
    leaf_of(path.trim_end_matches('/'))
}

fn leaf_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or_default()
}

/// One to three digits at the start of `stem`, and no fourth.
fn leading_number(stem: &str) -> bool {
    (1..=3).contains(&stem.bytes().take_while(u8::is_ascii_digit).count())
}

/// The album's size once every file's size is known (from the caller, an earlier search or browse,
/// or the peer as each transfer starts); `None` until then, so a percentage is never computed
/// against a partial sum.
fn album_total(items: &[Item]) -> Option<u64> {
    items
        .iter()
        .all(|item| item.size > 0)
        .then(|| items.iter().map(|item| item.size).sum())
}

/// The names that say something, joined with ` · `, within a title's length.
fn joined_title(names: &[&str]) -> String {
    let said: Vec<&str> = names
        .iter()
        .copied()
        .filter(|name| !name.is_empty())
        .collect();
    truncate_utf16(&said.join(" · "), MAX_TITLE)
}

/// `text` without the secrets a client's error may repeat. A secret of one character is left: it
/// would blank half the message.
fn scrub(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_owned();
    for secret in secrets {
        if utf16_len(secret) >= 2 {
            out = out.replace(secret.as_str(), "[redacted]");
        }
    }
    out
}

fn contains_ignore_case(text: &str, phrase: &str) -> bool {
    find_ignore_case(text, phrase, 0).is_some()
}

/// Where `phrase` (ASCII) starts in `text` at or after `from`, letters in any case.
fn find_ignore_case(text: &str, phrase: &str, from: usize) -> Option<usize> {
    let (text, phrase) = (text.as_bytes(), phrase.as_bytes());
    (from..=text.len().checked_sub(phrase.len())?)
        .find(|at| text[*at..at + phrase.len()].eq_ignore_ascii_case(phrase))
}

/// Plain-English reason for a failed browse or download (`peerError`).
fn peer_error(message: &str, username: &str, port: u16, secrets: &[String]) -> SoulseekError {
    let at = if port == 0 {
        "its Soulseek port".to_owned()
    } else {
        format!("port {port}")
    };
    let failed = |status, reason, message| SoulseekError {
        message,
        status: Some(status),
        reason: Some(reason),
    };
    if contains_ignore_case(message, "indirect connection") {
        return failed(
            502,
            "no-inbound",
            format!(
                "Could not connect to {username}: it is offline, or behind a firewall while peers cannot reach this relay on {at}."
            ),
        );
    }
    if contains_ignore_case(message, "timed out") {
        return failed(
            504,
            "no-answer",
            format!(
                "{username} did not answer: it may be offline, or unable to reach this relay on {at}."
            ),
        );
    }
    // The peer's reason is the rest of the message, when the rest is one line.
    let phrase = "upload denied by peer: ";
    let mut from = 0;
    while let Some(found) = find_ignore_case(message, phrase, from) {
        let reason = &message[found + phrase.len()..];
        if !reason.contains(['\n', '\r', '\u{2028}', '\u{2029}']) {
            let reason = if reason.is_empty() {
                "no reason given"
            } else {
                reason
            };
            let reason = scrub(reason, secrets);
            return failed(
                502,
                "refused",
                format!(
                    "{username} refused the file: {}.",
                    reason.trim_end_matches('.')
                ),
            );
        }
        from = found + 1;
    }
    if contains_ignore_case(message, "upload failed") {
        return failed(
            502,
            "refused",
            format!("{username} could not send the file (upload failed on their side)."),
        );
    }
    let message = if message.is_empty() {
        "Soulseek request failed"
    } else {
        message
    };
    failed(502, "error", scrub(message, secrets))
}

/// The server's host and port from `host:port`; the default server when it is not that
/// (`parseServer`). The brackets of an IPv6 address go.
fn parse_server(raw: &str) -> (String, u16) {
    let default = || ("vps.slsknet.org".to_owned(), 2271);
    let clean = js_trim(raw);
    let clean = if clean.is_empty() {
        DEFAULT_SERVER
    } else {
        clean
    };
    let Some((host, port)) = clean.rsplit_once(':') else {
        return default();
    };
    let host_ok = (1..=253).contains(&host.len())
        && host.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b':' | b'[' | b']' | b'-')
        });
    let port_ok = (1..=5).contains(&port.len()) && port.bytes().all(|byte| byte.is_ascii_digit());
    match port.parse::<u16>() {
        Ok(port) if host_ok && port_ok && port >= 1 => {
            let host = host.strip_prefix('[').unwrap_or(host);
            let host = host.strip_suffix(']').unwrap_or(host);
            (host.to_owned(), port)
        }
        _ => default(),
    }
}

/// `sNNeNN` or `sNNeNNN`.
fn episode_number(suffix: &str) -> Option<&str> {
    let rest = suffix.strip_prefix('s')?;
    let (season, episode) = rest.split_once('e')?;
    let digits = |text: &str| text.bytes().all(|byte| byte.is_ascii_digit());
    (season.len() == 2 && digits(season) && (2..=3).contains(&episode.len()) && digits(episode))
        .then_some(episode)
}

/// The track number at the end of a caller's id (`…:s01e05`), when it is not 0.
fn track_of(caller_id: &str) -> Option<u32> {
    let (_, suffix) = caller_id.rsplit_once(':')?;
    episode_number(suffix)?
        .parse()
        .ok()
        .filter(|track| *track > 0)
}

/// The album release a caller names (`parseRelease`): `mb:<32hex>`, `discogs:r<n>` or
/// `album:<20hex>`, bare or as a track id (`mb:<32hex>:s01e02`), or a bare or hyphenated
/// MusicBrainz release id.
fn parse_release(raw: &str) -> Option<String> {
    let clean = js_trim(raw).to_lowercase();
    if is_release_id(&clean) {
        return Some(clean);
    }
    if let Some((release, suffix)) = clean.rsplit_once(':')
        && is_release_id(release)
        && episode_number(suffix).is_some()
    {
        return Some(release.to_owned());
    }
    let id = clean.strip_prefix("mb:").unwrap_or(&clean).replace('-', "");
    let release = format!("mb:{id}");
    is_release_id(&release).then_some(release)
}

/// A folder only this user can read.
fn make_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)
}

/// The tags in the first megabyte of a file, when it has any.
fn read_head_tags(path: &Path) -> Option<Tags> {
    use std::io::Read;
    let mut head = Vec::new();
    std::fs::File::open(path)
        .ok()?
        .take(TAG_READ_LIMIT)
        .read_to_end(&mut head)
        .ok()?;
    parse_tags(&head)
}

/// A name as text and numbers by turns ("a10b" is "a", 10, "b"), lowercased.
fn natural_parts(name: &str) -> Vec<Result<f64, Vec<u16>>> {
    let lower = name.to_lowercase();
    let mut parts = Vec::new();
    let mut rest = lower.as_str();
    loop {
        let text = rest
            .find(|c: char| c.is_ascii_digit())
            .unwrap_or(rest.len());
        parts.push(Err(rest[..text].encode_utf16().collect()));
        rest = &rest[text..];
        if rest.is_empty() {
            return parts;
        }
        let digits = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        parts.push(Ok(rest[..digits].parse().unwrap_or(f64::INFINITY)));
        rest = &rest[digits..];
    }
}

/// The order a person reads names in: numbers by value, text as the app compares strings.
fn natural_order(a: &str, b: &str) -> std::cmp::Ordering {
    let (a, b) = (natural_parts(a), natural_parts(b));
    for (x, y) in a.iter().zip(&b) {
        let order = match (x, y) {
            (Ok(x), Ok(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Err(x), Err(y)) => x.cmp(y),
            // Both names start with text, so a number never meets text at the same place.
            _ => std::cmp::Ordering::Equal,
        };
        if order.is_ne() {
            return order;
        }
    }
    a.len().cmp(&b.len())
}
