//! `src/search.js`: global search (`peartube/search/v1`). Ask relays this node does not follow
//! for media they hold, on the connections the swarm already makes (`control_channel`), and
//! answer such asks when this node is a relay that opted in (the `globalSearch` setting). A hit
//! is a watch link without `source=`, for a file the answering relay holds whole and has checked
//! is media, so the searcher reads it from that relay on the same connection.
//!
//! ```text
//! searcher -> relay   {id, type: 'search', q, mid, limit}
//! relay -> searcher   {id, ok: true, result: {hits: [{uri}]}} or {id, ok: false, error: 'busy'}
//! ```
//!
//! Relays announce one topic; a searcher looks it up only while it searches, and closes what it
//! dialed for search a minute after its last search.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;

use crate::control_channel::{
    Control, ControlHandlers, RequestHandler, ServeHandle, Spec, open_channel, serve_channel,
};
use crate::node::api::Announce;
use crate::node::network::ConnectionHandle;
use crate::private_overlay::diversity_bucket_for_address;
use crate::protocol::{
    BlobPointer, ParsedUri, WatchEntry, describe_entry, format_watch_uri, is_id, parse_uri,
};
use crate::traffic::Connection as _;

/// `SEARCH_TOPIC`: `hash('peartube/search/v1/0')`.
pub static SEARCH_TOPIC: LazyLock<[u8; 32]> =
    LazyLock::new(|| hypercore::crypto::hash(&[b"peartube/search/v1/0"]));

static SEARCH: Spec = Spec {
    protocol: "peartube/search/v1",
    label: "Relay",
    refused: "This peer does not answer searches",
    duplicate: "A search channel is already open with this peer",
    timed_out: "Timed out opening the search channel",
};

const MAX_HITS: usize = 20;
const MAX_URI: usize = 2048;
const MAX_QUERY: usize = 120;
const MAX_WORDS: usize = 8;
/// Peers already connected that one search asks.
const ASK_CONNECTED: usize = 16;
/// Relays from the topic that one search asks, and of those at most this many per /24.
const ASK_TOPIC: usize = 4;
const PER_BUCKET: usize = 2;
/// A search returns as soon as every ask answered, and no later than this; later answers
/// come with the next call.
const COLLECT: Duration = Duration::from_millis(2000);
const ASK_TIMEOUT: Duration = Duration::from_secs(3);
/// After the last search the lookup ends and search-only connections close.
const IDLE: Duration = Duration::from_secs(60);
const KEEP_RUNS: usize = 8;
/// The same search asked again after this asks every relay again, so newer files show up.
const FRESH: Duration = Duration::from_secs(10);
/// A relay rebuilds its list of offered files at most this often.
const LIST_EVERY: Duration = Duration::from_secs(15);
const SERVE_CHANNELS: usize = 64;
const WINDOW: Duration = Duration::from_secs(10);
const PER_CHANNEL: usize = 4;
const PER_NODE: usize = 20;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

/// Waits until every ask answered or failed, and never past `COLLECT`.
async fn wait_for_asks(run: &Arc<Mutex<Run>>) {
    let _ = tokio::time::timeout(COLLECT, async {
        loop {
            let wake = {
                let r = lock(run);
                if r.pending == 0 {
                    return;
                }
                r.wake.clone().notified_owned()
            };
            wake.await;
        }
    })
    .await;
}

/// What the node gives global search.
pub trait SearchHost: Send + Sync + 'static {
    fn closing(&self) -> bool;
    fn private_routing(&self) -> bool;
    fn connections(&self) -> Vec<ConnectionHandle>;
    fn is_source_peer(&self, key_hex: &str) -> bool;
    fn public_entries(&self) -> BoxFuture<'static, Vec<Announce>>;
    /// `ctx.update()`: later answers arrived.
    fn update(&self);
    fn swarm(&self) -> peeroxide::SwarmHandle;
}

/// `isMedia(key)` for the file at `<blobs>:<blockOffset>`.
pub type IsMedia = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// A query as words: lower case, split at spaces and ASCII punctuation, at most 8 words of 2 or
/// more characters (`words` in the app).
pub fn words(q: &str) -> Vec<String> {
    let cut: String = q.chars().take(MAX_QUERY).collect::<String>().to_lowercase();
    cut.split(|c: char| c.is_whitespace() || c.is_ascii_punctuation())
        .filter(|w| w.chars().count() >= 2)
        .take(MAX_WORDS)
        .map(str::to_owned)
        .collect()
}

/// The node takes a row count, with zero reserved for the default. Keep a negative or
/// fractional count below one distinct from a missing, zero or nonnumeric JSON limit.
pub(crate) fn request_limit(raw: Option<&Value>) -> usize {
    let number = match raw {
        Some(Value::Number(n)) => n.as_f64().unwrap_or(f64::NAN),
        Some(Value::String(s)) => crate::protocol::js_number(s),
        Some(Value::Array(items)) => match items.as_slice() {
            // Number([x]) parses the string form of x. "true" and "false" are not numbers.
            [item] if !item.is_boolean() => return request_limit(Some(item)),
            _ => return 0,
        },
        Some(Value::Bool(flag)) => return usize::from(*flag),
        _ => return 0,
    };
    if number == 0.0 || number.is_nan() {
        0
    } else {
        (number as usize).max(1)
    }
}

fn valid_id(mid: Option<&Value>) -> Option<String> {
    let mid = mid?.as_str()?;
    let lower = mid.to_lowercase();
    (mid.chars().count() <= 80 && is_id(&lower)).then_some(lower)
}

fn watch_entry(entry: &Announce) -> WatchEntry {
    WatchEntry {
        id: entry.id.clone(),
        title: entry.title.clone(),
        sha256: Some(entry.sha256.clone()),
        blobs: entry.blobs.clone(),
        blob: Some(BlobPointer {
            block_offset: entry.blob.block_offset,
            block_length: entry.blob.block_length,
            byte_offset: entry.blob.byte_offset,
            byte_length: entry.blob.byte_length,
        }),
    }
}

/// A hit as the watch entry it names, or `None` (`checkHit`). Hits are a stranger's claims: only
/// the shape is checked here, and Hypercore checks every byte the link later reads.
fn check_hit(hit: &Value) -> Option<WatchEntry> {
    let uri = hit.get("uri")?.as_str()?;
    if uri.len() > MAX_URI {
        return None;
    }
    let ParsedUri::Watch { entry, .. } = parse_uri(uri).ok()? else {
        return None;
    };
    if entry.blob.byte_length == 0
        || entry.blob.block_length > 1 << 20
        || entry.sha256.bytes().all(|b| b == b'0')
    {
        return None;
    }
    Some(WatchEntry {
        id: entry.id,
        title: entry.title,
        sha256: Some(entry.sha256),
        blobs: entry.blobs,
        blob: Some(entry.blob),
    })
}

struct Offer {
    entry: Announce,
    text: String,
}

struct Row {
    key: String,
    entry: WatchEntry,
    size: u64,
    relays: HashSet<String>,
    exact: bool,
}

struct Run {
    q: String,
    mid: Option<String>,
    rows: Vec<Row>,
    asked: HashSet<String>,
    buckets: HashMap<String, usize>,
    topic_asked: usize,
    returned: bool,
    at: std::time::Instant,
    pending: usize,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct State {
    closed: bool,
    serving: Option<IsMedia>,
    session: bool,
    announcing: bool,
    idle: Option<tokio::task::JoinHandle<()>>,
    current: Option<Arc<Mutex<Run>>>,
    runs: Vec<(String, Arc<Mutex<Run>>)>,
    controls: HashMap<u64, Arc<OnceCell<Option<Control>>>>,
    served: Vec<(u64, Control)>,
    peer_hits: Vec<([u8; 32], VecDeque<Instant>)>,
    anon_hits: VecDeque<Instant>,
    unpairs: HashMap<u64, ServeHandle>,
    node_hits: VecDeque<Instant>,
    list: Option<(Instant, Arc<Vec<Offer>>)>,
}

struct Inner {
    host: Arc<dyn SearchHost>,
    state: Mutex<State>,
    weak: Weak<Inner>,
}

/// `createGlobalSearch(...)`.
#[derive(Clone)]
pub struct GlobalSearch {
    inner: Arc<Inner>,
}

fn over_limit(stamps: &mut VecDeque<Instant>, max: usize, now: Instant) -> bool {
    while stamps
        .front()
        .is_some_and(|at| now.duration_since(*at) > WINDOW)
    {
        stamps.pop_front();
    }
    if stamps.len() >= max {
        return true;
    }
    stamps.push_back(now);
    false
}

fn hex_key(conn: &ConnectionHandle) -> String {
    conn.remote_public_key()
}

impl GlobalSearch {
    pub fn new(host: Arc<dyn SearchHost>) -> Self {
        let inner = Arc::new_cyclic(|weak| Inner {
            host,
            state: Mutex::new(State::default()),
            weak: weak.clone(),
        });
        Self { inner }
    }

    /// The node's connection listener: pair the protocol while answering, and ask a connection
    /// the search lookup dialed for the newest search.
    pub fn connected(&self, conn: ConnectionHandle) {
        self.inner.pair(&conn);
        let current = {
            let state = lock(&self.inner.state);
            if !state.session || !conn.topics().contains(&*SEARCH_TOPIC) {
                return;
            }
            state.current.clone()
        };
        if let Some(run) = current {
            self.inner.spawn_ask(run, conn, true);
        }
    }

    /// Answering on or off (`serve`). `is_media(key)` says whether the file at
    /// `<blobs>:<blockOffset>` is media.
    pub fn serve(&self, is_media: Option<IsMedia>) {
        let inner = &self.inner;
        let swarm = inner.host.swarm();
        let mut state = lock(&inner.state);
        if state.closed {
            return;
        }
        if let Some(is_media) = is_media.filter(|_| !inner.host.private_routing()) {
            state.serving = Some(is_media);
            state.list = None;
            let join = !state.announcing;
            state.announcing = true;
            drop(state);
            for conn in inner.host.connections() {
                inner.pair(&conn);
            }
            if join {
                tokio::spawn(async move {
                    let mut opts = peeroxide::JoinOpts::default();
                    opts.server = true;
                    opts.client = false;
                    let _ = swarm.join(*SEARCH_TOPIC, opts).await;
                });
            }
            return;
        }
        state.serving = None;
        state.list = None;
        let unpairs = std::mem::take(&mut state.unpairs);
        let served = std::mem::take(&mut state.served);
        state.peer_hits.clear();
        state.anon_hits.clear();
        let was = std::mem::replace(&mut state.announcing, false);
        let session = state.session;
        drop(state);
        drop(unpairs);
        for (_, control) in served {
            control.close();
        }
        if was {
            tokio::spawn(async move {
                let _ = swarm.leave(*SEARCH_TOPIC).await;
                if session {
                    let mut opts = peeroxide::JoinOpts::default();
                    opts.server = false;
                    opts.client = true;
                    let _ = swarm.join(*SEARCH_TOPIC, opts).await;
                }
            });
        }
    }

    /// `searchGlobal({ query, id, limit })`: rows after `COLLECT`. Asked again within `FRESH` it
    /// returns the answers that arrived since; after that it is a new search, so a file published
    /// or removed since is seen.
    pub async fn search_global(
        &self,
        query: &str,
        id: Option<&Value>,
        limit: usize,
    ) -> Result<Vec<Value>, String> {
        let inner = &self.inner;
        if lock(&inner.state).closed || inner.host.closing() {
            return Err("Node is closing".to_owned());
        }
        if inner.host.private_routing() {
            return Err("Global search is off while traffic is routed privately".to_owned());
        }
        let ws = words(query);
        let mid = valid_id(id);
        if ws.is_empty() && mid.is_none() {
            return Err("Type at least 2 letters".to_owned());
        }
        let q = ws.join(" ");
        let run_key = format!("{q}\n{}", mid.clone().unwrap_or_default());
        let (run, join, topic_conns) = {
            let mut state = lock(&inner.state);
            let reuse = state
                .runs
                .iter()
                .position(|(key, _)| key == &run_key)
                .map(|index| state.runs.remove(index).1)
                .filter(|run| lock(run).at.elapsed() <= FRESH);
            let run = reuse.unwrap_or_else(|| {
                Arc::new(Mutex::new(Run {
                    q,
                    mid,
                    rows: Vec::new(),
                    asked: HashSet::new(),
                    buckets: HashMap::new(),
                    topic_asked: 0,
                    returned: false,
                    at: std::time::Instant::now(),
                    pending: 0,
                    wake: Arc::new(Notify::new()),
                }))
            });
            state.runs.push((run_key, run.clone()));
            while state.runs.len() > KEEP_RUNS {
                state.runs.remove(0);
            }
            state.current = Some(run.clone());
            let join = !state.session;
            state.session = true;
            if let Some(idle) = state.idle.take() {
                idle.abort();
            }
            let weak = inner.weak.clone();
            state.idle = Some(tokio::spawn(async move {
                tokio::time::sleep(IDLE).await;
                if let Some(inner) = weak.upgrade() {
                    inner.end_search();
                }
            }));
            let topic_conns: Vec<_> = inner
                .host
                .connections()
                .into_iter()
                .filter(|conn| conn.topics().contains(&*SEARCH_TOPIC))
                .collect();
            (run, join, topic_conns)
        };
        if join {
            let swarm = inner.host.swarm();
            tokio::spawn(async move {
                let mut opts = peeroxide::JoinOpts::default();
                opts.server = false;
                opts.client = true;
                let _ = swarm.join(*SEARCH_TOPIC, opts).await;
            });
        }
        for conn in &topic_conns {
            inner.spawn_ask(run.clone(), conn.clone(), true);
        }
        let topic_ids: HashSet<u64> = topic_conns.iter().map(|conn| conn.id()).collect();
        for conn in inner.host.connections() {
            if !topic_ids.contains(&conn.id()) {
                inner.spawn_ask(run.clone(), conn, false);
            }
        }
        if !lock(&run).returned {
            wait_for_asks(&run).await;
        }
        let mut run = lock(&run);
        run.returned = true;
        let limit = if limit == 0 {
            MAX_HITS
        } else {
            limit.min(MAX_HITS * ASK_TOPIC)
        };
        Ok(rows(&run, limit))
    }

    /// Ends the lookup and its connections when the node suspends.
    pub fn suspend(&self) {
        self.inner.end_search();
    }

    pub fn close(&self) {
        self.inner.end_search();
        self.serve(None);
        lock(&self.inner.state).closed = true;
    }
}

fn rows(run: &Run, limit: usize) -> Vec<Value> {
    let mut out: Vec<&Row> = run.rows.iter().collect();
    out.sort_by(|a, b| {
        b.exact
            .cmp(&a.exact)
            .then(b.relays.len().cmp(&a.relays.len()))
            .then(a.entry.title.cmp(&b.entry.title))
    });
    out.into_iter()
        .take(limit)
        .map(|row| {
            let info = describe_entry(&row.entry.id, &row.entry.title);
            json!({
                "key": row.key,
                "id": row.entry.id,
                "title": row.entry.title,
                "size": row.size,
                "sha256": row.entry.sha256,
                "relays": row.relays.len(),
                "season": info.season,
                "episode": info.episode,
                "seriesKey": info.series_key,
                "baseTitle": info.base_title,
                "year": info.year,
                "magnetUrl": format_watch_uri(&row.entry, "", ""),
            })
        })
        .collect()
}

impl Inner {
    fn answer_handler(&self) -> RequestHandler {
        let weak = self.weak.clone();
        Arc::new(move |message, control| {
            let weak = weak.clone();
            Box::pin(async move {
                let Some(inner) = weak.upgrade() else {
                    return Err("off".to_owned());
                };
                inner.answer(message, control).await
            })
        })
    }

    async fn offered(&self) -> Arc<Vec<Offer>> {
        let (fresh, is_media) = {
            let state = lock(&self.state);
            let fresh = state
                .list
                .as_ref()
                .filter(|(at, _)| at.elapsed() <= LIST_EVERY)
                .map(|(_, list)| list.clone());
            (fresh, state.serving.clone())
        };
        if let Some(list) = fresh {
            return list;
        }
        let Some(is_media) = is_media else {
            return Arc::new(Vec::new());
        };
        let list: Vec<Offer> = self
            .host
            .public_entries()
            .await
            .into_iter()
            .filter(|e| is_media(&format!("{}:{}", e.blobs, e.blob.block_offset)))
            .map(|e| Offer {
                text: format!("{} {}", e.title, e.id).to_lowercase(),
                entry: e,
            })
            .collect();
        let list = Arc::new(list);
        lock(&self.state).list = Some((Instant::now(), list.clone()));
        list
    }

    async fn answer(&self, message: Value, control: Control) -> Result<Value, String> {
        {
            let mut state = lock(&self.state);
            if state.serving.is_none() || state.closed || self.host.private_routing() {
                return Err("off".to_owned());
            }
            if message.get("type").and_then(Value::as_str) != Some("search") {
                return Err("unknown request".to_owned());
            }
            let now = Instant::now();
            let id = control.id();
            if let Some(pos) = state.served.iter().position(|(held, _)| *held == id) {
                let rec = state.served.remove(pos);
                state.served.push(rec);
            }
            let busy = match control.mux().stream_info().remote_public_key {
                Some(peer) => {
                    if let Some((_, stamps)) = state.peer_hits.iter_mut().find(|(k, _)| *k == peer)
                    {
                        over_limit(stamps, PER_CHANNEL, now)
                    } else {
                        if state.peer_hits.len() >= 256 {
                            state.peer_hits.remove(0);
                        }
                        let mut stamps = VecDeque::new();
                        let over = over_limit(&mut stamps, PER_CHANNEL, now);
                        state.peer_hits.push((peer, stamps));
                        over
                    }
                }
                None => over_limit(&mut state.anon_hits, PER_CHANNEL, now),
            };
            if busy || over_limit(&mut state.node_hits, PER_NODE, now) {
                return Err("busy".to_owned());
            }
        }
        let ws = words(message.get("q").and_then(Value::as_str).unwrap_or(""));
        let mid = valid_id(message.get("mid"));
        if ws.is_empty() && mid.is_none() {
            return Err("bad query".to_owned());
        }
        let limit = message
            .get("limit")
            .and_then(Value::as_f64)
            .filter(|n| *n >= 1.0)
            .map_or(MAX_HITS, |n| (n as usize).min(MAX_HITS));
        let mut hits = Vec::new();
        for offer in self.offered().await.iter() {
            let id = &offer.entry.id;
            if let Some(mid) = &mid
                && id != mid
                && !id.starts_with(&format!("{mid}:"))
            {
                continue;
            }
            if !ws.iter().all(|w| offer.text.contains(w.as_str())) {
                continue;
            }
            let uri = format_watch_uri(&watch_entry(&offer.entry), "", "");
            if uri.len() <= MAX_URI {
                hits.push(json!({ "uri": uri }));
            }
            if hits.len() >= limit {
                break;
            }
        }
        Ok(json!({ "hits": hits }))
    }

    fn track(&self, control: Control) {
        let mut state = lock(&self.state);
        let id = control.id();
        state.served.retain(|(held, _)| *held != id);
        state.served.push((id, control));
        // The least recently used channel goes, never the connection under it.
        while state.served.len() > SERVE_CHANNELS {
            let (_, old) = state.served.remove(0);
            old.close();
        }
    }

    fn pair(&self, conn: &ConnectionHandle) {
        let mut state = lock(&self.state);
        if state.serving.is_none()
            || state.closed
            || conn.destroyed()
            || state.unpairs.contains_key(&conn.id())
        {
            return;
        }
        let conn_id = conn.id();
        let on_open = {
            let weak = self.weak.clone();
            Arc::new(move |control: Control| {
                if let Some(inner) = weak.upgrade() {
                    inner.track(control.clone());
                    let cell = Arc::new(OnceCell::new_with(Some(Some(control))));
                    lock(&inner.state).controls.insert(conn_id, cell);
                }
            })
        };
        let on_close = {
            let weak = self.weak.clone();
            Arc::new(move |control: Control| {
                if let Some(inner) = weak.upgrade() {
                    let mut state = lock(&inner.state);
                    let id = control.id();
                    state.served.retain(|(held, _)| *held != id);
                    state.controls.remove(&conn_id);
                }
            })
        };
        let handle = serve_channel(
            conn.mux(),
            &SEARCH,
            ControlHandlers {
                on_request: Some(self.answer_handler()),
                on_event: None,
                on_open: Some(on_open),
                on_close: Some(on_close),
            },
        );
        state.unpairs.insert(conn_id, handle);
        drop(state);
        let weak = self.weak.clone();
        let mux = conn.mux().clone();
        tokio::spawn(async move {
            mux.closed().await;
            if let Some(inner) = weak.upgrade() {
                let mut state = lock(&inner.state);
                state.unpairs.remove(&conn_id);
                state.controls.remove(&conn_id);
            }
        });
    }

    async fn control(&self, conn: &ConnectionHandle) -> Option<Control> {
        let id = conn.id();
        let cell = lock(&self.state)
            .controls
            .entry(id)
            .or_insert_with(|| Arc::new(OnceCell::new()))
            .clone();
        let handlers = ControlHandlers {
            on_request: Some(self.answer_handler()),
            on_event: None,
            on_open: None,
            on_close: {
                let weak = self.weak.clone();
                Some(Arc::new(move |control: Control| {
                    if let Some(inner) = weak.upgrade() {
                        let mut state = lock(&inner.state);
                        let cid = control.id();
                        state.served.retain(|(held, _)| *held != cid);
                        state.controls.remove(&id);
                    }
                }))
            },
        };
        let mux = conn.mux().clone();
        let control = cell
            .get_or_init(|| async move {
                let opened = open_channel(&mux, &SEARCH, handlers, ASK_TIMEOUT).await;
                if let Err(error) = &opened {
                    eprintln!("search: opening a channel failed: {error}");
                }
                opened.ok()
            })
            .await
            .clone();
        if control.is_none() {
            let mut state = lock(&self.state);
            if state
                .controls
                .get(&id)
                .is_some_and(|held| Arc::ptr_eq(held, &cell))
            {
                state.controls.remove(&id);
            }
        }
        control
    }

    fn spawn_ask(&self, run: Arc<Mutex<Run>>, conn: ConnectionHandle, from_topic: bool) {
        let Some(inner) = self.weak.upgrade() else {
            return;
        };
        lock(&run).pending += 1;
        tokio::spawn(async move {
            inner.ask(run.clone(), conn, from_topic).await;
            let mut r = lock(&run);
            r.pending = r.pending.saturating_sub(1);
            r.wake.notify_one();
        });
    }

    async fn ask(&self, run: Arc<Mutex<Run>>, conn: ConnectionHandle, from_topic: bool) {
        let hex = hex_key(&conn);
        let (q, mid) = {
            let mut r = lock(&run);
            if hex.is_empty() || conn.destroyed() || r.asked.contains(&hex) {
                return;
            }
            if from_topic {
                if r.topic_asked >= ASK_TOPIC {
                    return;
                }
                let bucket = conn
                    .remote_addr()
                    .and_then(|addr| {
                        diversity_bucket_for_address(
                            &addr.ip().to_string(),
                            Some(u32::from(addr.port())),
                            true,
                        )
                    })
                    .unwrap_or_else(|| hex.clone());
                let count = r.buckets.get(&bucket).copied().unwrap_or(0);
                if count >= PER_BUCKET {
                    return;
                }
                r.buckets.insert(bucket, count + 1);
                r.topic_asked += 1;
            } else if r.asked.len() - r.topic_asked >= ASK_CONNECTED {
                return;
            }
            r.asked.insert(hex.clone());
            (r.q.clone(), r.mid.clone())
        };
        let Some(control) = self.control(&conn).await else {
            return;
        };
        let mut payload = Map::new();
        payload.insert("q".to_owned(), json!(q));
        payload.insert("mid".to_owned(), mid.map_or(Value::Null, Value::String));
        payload.insert("limit".to_owned(), json!(MAX_HITS));
        let result = match control.call("search", payload, ASK_TIMEOUT).await {
            Ok(result) => result,
            Err(error) => {
                eprintln!("search: asking a relay failed: {error}");
                return;
            }
        };
        let hits = result
            .get("hits")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut added = false;
        let returned = {
            let mut r = lock(&run);
            for hit in hits.iter().take(MAX_HITS) {
                let Some(entry) = check_hit(hit) else {
                    continue;
                };
                let b = entry.blob.unwrap_or_default();
                let key = format!(
                    "search/{}:{}:{}:{}:{}",
                    entry.blobs, b.block_offset, b.block_length, b.byte_offset, b.byte_length
                );
                let exact = r.mid.as_deref() == Some(entry.id.as_str());
                let index = match r.rows.iter().position(|row| row.key == key) {
                    Some(index) => index,
                    None => {
                        r.rows.push(Row {
                            key,
                            size: b.byte_length,
                            entry,
                            relays: HashSet::new(),
                            exact,
                        });
                        r.rows.len() - 1
                    }
                };
                if r.rows[index].relays.insert(hex.clone()) {
                    added = true;
                }
            }
            r.returned
        };
        if added && returned {
            self.host.update();
        }
    }

    /// Ends the lookup and closes the connections only it made: not LAN links, not peers in a
    /// shared topic, and not a peer that has served this node a block (a hit being played).
    fn end_search(&self) {
        let (session, announcing) = {
            let mut state = lock(&self.state);
            if let Some(idle) = state.idle.take() {
                idle.abort();
            }
            state.current = None;
            let session = std::mem::replace(&mut state.session, false);
            (session, state.announcing)
        };
        if !session {
            return;
        }
        let swarm = self.host.swarm();
        tokio::spawn(async move {
            let _ = swarm.leave(*SEARCH_TOPIC).await;
            if announcing {
                let mut opts = peeroxide::JoinOpts::default();
                opts.server = true;
                opts.client = false;
                let _ = swarm.join(*SEARCH_TOPIC, opts).await;
            }
        });
        for conn in self.host.connections() {
            let topics = conn.topics();
            let only_search = !topics.is_empty() && topics.iter().all(|t| t == &*SEARCH_TOPIC);
            if only_search && !conn.is_lan() && !self.host.is_source_peer(&hex_key(&conn)) {
                conn.close();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(pending: usize) -> Arc<Mutex<Run>> {
        Arc::new(Mutex::new(Run {
            q: String::new(),
            mid: None,
            rows: Vec::new(),
            asked: HashSet::new(),
            buckets: HashMap::new(),
            topic_asked: 0,
            returned: false,
            at: std::time::Instant::now(),
            pending,
            wake: Arc::new(Notify::new()),
        }))
    }

    /// No ask in flight means no wait at all; a flat sleep would always spend the window.
    #[tokio::test]
    async fn a_fast_answer_ends_the_wait_before_the_window() {
        let run = run_with(0);
        let started = std::time::Instant::now();
        wait_for_asks(&run).await;
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "the wait was {:?}",
            started.elapsed()
        );
    }

    /// A slow ask is waited for, but never past the window.
    #[tokio::test]
    async fn a_slow_ask_is_capped_by_the_window() {
        let run = run_with(1);
        let worker = run.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let mut r = lock(&worker);
            r.pending = 0;
            r.wake.notify_one();
        });
        let started = std::time::Instant::now();
        wait_for_asks(&run).await;
        let elapsed = started.elapsed();
        assert!(elapsed >= Duration::from_millis(90), "{elapsed:?}");
        assert!(elapsed < Duration::from_millis(1500), "{elapsed:?}");
    }

    /// The window outlasts a relay that answers after the app's old 1.2 s first call.
    #[test]
    fn the_window_outlasts_a_slow_relay() {
        assert!(
            COLLECT >= Duration::from_millis(1500),
            "the collect window is {COLLECT:?}"
        );
    }
}
