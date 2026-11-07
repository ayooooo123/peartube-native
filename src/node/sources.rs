//! `src/node/sources.js`: the followed-channel pool.
//!
//! At most `activeSourceCap` Autobees stay open; the rest serve their persisted snapshot until the
//! rotator wakes them. Every scan merges into the record so a partially applied view can never wipe
//! titles the node already proved.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use autobee::Autobee;
use autobee::types::Key;
use corestore::Corestore;
use protomux_wakeup::WakeupSwarm;
use serde_json::{Value, json};

use super::api::{DiscoveredSource, Removed, Source, SourceSpec};
use super::apply::Apply;
use crate::protocol::{clean_text, decode_op, is_system_view_key};

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex32(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

/// One announce record from a followed channel's view.
#[derive(Clone, Debug)]
pub struct Item {
    pub sub: String,
    pub entry: Value,
}

/// The persisted/merged catalog snapshot of a channel.
#[derive(Default)]
struct Cache {
    entries: Vec<Item>,
    by_sub: HashMap<String, Value>,
    discovered: Vec<DiscoveredSource>,
    hydrated: bool,
    synced_at: u64,
    cached_rev: i64,
    view_version: i64,
}

/// One followed channel.
pub struct SourceRecord {
    pub key: String,
    alias: Mutex<String>,
    cache: Mutex<Cache>,
    bee: Mutex<Option<Arc<Autobee>>>,
    opening: tokio::sync::Mutex<()>,
    pub touched_at: AtomicU64,
    rev: AtomicU64,
}

impl SourceRecord {
    pub fn entries(&self) -> Vec<Item> {
        lock(&self.cache).entries.clone()
    }

    pub fn discovered(&self) -> Vec<DiscoveredSource> {
        lock(&self.cache).discovered.clone()
    }

    pub fn hydrated(&self) -> bool {
        lock(&self.cache).hydrated
    }

    pub fn synced_at(&self) -> u64 {
        lock(&self.cache).synced_at
    }

    pub fn alias(&self) -> String {
        lock(&self.alias).clone()
    }

    pub fn set_alias(&self, alias: &str) {
        if !alias.is_empty() {
            *lock(&self.alias) = alias.to_owned();
        }
    }

    pub fn active(&self) -> bool {
        lock(&self.bee).is_some()
    }

    pub(super) fn bee(&self) -> Option<Arc<Autobee>> {
        lock(&self.bee).clone()
    }

    fn touch(&self, clock: &AtomicU64) {
        let at = clock.fetch_add(1, Ordering::AcqRel) + 1;
        self.touched_at.store(at, Ordering::Release);
    }

    /// `indexChannelRecords(target, entriesList, discoveredList, { merge })`.
    fn index(&self, entries: &[Item], discovered: &[DiscoveredSource], merge: bool) {
        let mut cache = lock(&self.cache);
        let mut by_sub = if merge {
            cache.by_sub.clone()
        } else {
            HashMap::new()
        };
        let mut items: HashMap<String, Item> = if merge {
            cache
                .entries
                .iter()
                .cloned()
                .map(|item| (item.sub.clone(), item))
                .collect()
        } else {
            HashMap::new()
        };
        for item in entries {
            by_sub.insert(item.sub.clone(), item.entry.clone());
            items.insert(item.sub.clone(), item.clone());
        }
        cache.entries = items.into_values().collect();
        cache.by_sub = by_sub;
        cache.discovered = discovered.to_vec();
    }
}

/// What the pool needs from the node.
pub struct SourcesDeps {
    pub store: Corestore,
    /// The node's wakeup swarm, shared with the primary channel: one pairing
    /// owner per connection (`channel.js` passes `ctx.channel.wakeup`).
    pub wakeup: Arc<WakeupSwarm>,
    pub closing: Arc<AtomicBool>,
    pub update: Arc<dyn Fn() + Send + Sync>,
    pub join_topic: Arc<dyn Fn(&str) + Send + Sync>,
    pub leave_topic: Arc<dyn Fn(String) + Send + Sync>,
    pub attach_wakeup: Arc<dyn Fn(Arc<Autobee>) + Send + Sync>,
    pub make_apply: Arc<dyn Fn(&str) -> Apply + Send + Sync>,
    pub cache_file: PathBuf,
    pub worklet: bool,
    pub cap: usize,
    pub max_active: Option<u64>,
}

/// The followed-channel pool (`createSources(ctx)`).
pub struct Sources {
    deps: Arc<SourcesDeps>,
    records: Mutex<HashMap<String, Arc<SourceRecord>>>,
    active: Mutex<HashSet<String>>,
    clock: AtomicU64,
    dirty: AtomicBool,
    saved: Mutex<HashMap<String, Value>>,
}

impl Sources {
    /// Reads the saved catalog cache, then registers every saved source.
    pub fn new(deps: Arc<SourcesDeps>, initial: &[SourceSpec]) -> Arc<Self> {
        let saved = std::fs::read_to_string(&deps.cache_file)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|value| value.get("channels").cloned())
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        let sources = Arc::new(Self {
            deps,
            records: Mutex::new(HashMap::new()),
            active: Mutex::new(HashSet::new()),
            clock: AtomicU64::new(0),
            dirty: AtomicBool::new(false),
            saved: Mutex::new(saved.into_iter().collect()),
        });
        for spec in initial {
            sources.ensure(spec, false);
        }
        sources
    }

    pub fn cap(&self) -> usize {
        self.deps.cap
    }

    fn ensure(&self, spec: &SourceSpec, wake: bool) -> Option<Source> {
        let (key, alias) = match spec {
            SourceSpec::Uri(text) => (text.trim().to_lowercase(), String::new()),
            SourceSpec::Object(object) => {
                let key = object
                    .key
                    .clone()
                    .or_else(|| object.uri.clone())
                    .unwrap_or_default()
                    .trim()
                    .to_lowercase();
                (key, clean_text(object.alias.as_deref(), ""))
            }
        };
        if !crate::protocol::is_hex64(&key) {
            return None;
        }
        let mut records = lock(&self.records);
        let record = records.entry(key.clone()).or_insert_with(|| {
            let record = Arc::new(SourceRecord {
                key: key.clone(),
                alias: Mutex::new(alias.clone()),
                cache: Mutex::new(Cache::default()),
                bee: Mutex::new(None),
                opening: tokio::sync::Mutex::new(()),
                touched_at: AtomicU64::new(0),
                rev: AtomicU64::new(0),
            });
            self.load_cached(&record);
            record
        });
        if !alias.is_empty() {
            record.set_alias(&alias);
        }
        let _ = wake;
        // The guard from the `entries` read must end before `hydrated()` takes the same lock: a
        // temporary inside the struct literal lives to the end of the tail expression.
        let entries = lock(&record.cache).entries.len() as u64;
        Some(Source {
            key: key.clone(),
            kind: "channel".to_owned(),
            alias: record.alias(),
            primary: false,
            writable: false,
            entries: Some(entries),
            active: Some(record.active()),
            hydrated: Some(record.hydrated()),
        })
    }

    fn load_cached(&self, record: &SourceRecord) {
        let saved = lock(&self.saved).get(&record.key).cloned();
        let Some(saved) = saved else {
            return;
        };
        let Some(pairs) = saved.get("entries").and_then(Value::as_array) else {
            return;
        };
        let mut items = Vec::new();
        for pair in pairs {
            let Some(pair) = pair.as_array() else {
                continue;
            };
            let (Some(sub), Some(entry)) = (pair.first().and_then(Value::as_str), pair.get(1))
            else {
                continue;
            };
            if entry.get("type").and_then(Value::as_str) == Some("announce") {
                items.push(Item {
                    sub: sub.to_owned(),
                    entry: entry.clone(),
                });
            }
        }
        let mut discovered = Vec::new();
        if let Some(list) = saved.get("discovered").and_then(Value::as_array) {
            for item in list {
                discovered.push(DiscoveredSource {
                    key: item
                        .get("key")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    kind: item
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or("channel")
                        .to_owned(),
                    title: item
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    via: item
                        .get("via")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                });
            }
        }
        record.index(&items, &discovered, false);
        let mut cache = lock(&record.cache);
        cache.hydrated = true;
        cache.cached_rev = 0;
        cache.synced_at = saved.get("syncedAt").and_then(Value::as_u64).unwrap_or(0);
    }

    pub fn get(&self, key: &str) -> Option<Arc<SourceRecord>> {
        lock(&self.records).get(&key.to_lowercase()).cloned()
    }

    pub fn keys(&self) -> Vec<String> {
        lock(&self.records).keys().cloned().collect()
    }

    pub fn active_count(&self) -> usize {
        lock(&self.active).len()
    }

    /// Wakes a record's Autobee (pool-bounded) and returns it.
    pub async fn wake(&self, key: &str, touch: bool) -> Option<Arc<Autobee>> {
        let record = self.get(key)?;
        if self.deps.closing.load(Ordering::Acquire) {
            return None;
        }
        if let Some(bee) = record.bee() {
            if touch {
                record.touch(&self.clock);
            }
            return Some(bee);
        }
        let _guard = record.opening.lock().await;
        if let Some(bee) = record.bee() {
            if touch {
                record.touch(&self.clock);
            }
            return Some(bee);
        }
        if self.deps.closing.load(Ordering::Acquire) {
            return None;
        }
        self.hibernate_to_fit(&record.key).await;
        if self.deps.closing.load(Ordering::Acquire) {
            return None;
        }
        let primary = parse_hex32(&record.key)?;
        let namespace = self.deps.store.namespace(format!("channel:{}", record.key));
        let apply = (self.deps.make_apply)(&record.key);
        let bee = Autobee::open_with_wakeup(
            &namespace,
            Some(primary),
            Arc::new(HandlersWrap(apply)),
            self.deps.wakeup.clone(),
        )
        .await
        .ok()?;
        if self.deps.closing.load(Ordering::Acquire) {
            let _ = bee.close().await;
            return None;
        }
        *lock(&record.bee) = Some(bee.clone());
        lock(&self.active).insert(record.key.clone());
        if touch {
            record.touch(&self.clock);
        }
        lock(&record.cache).cached_rev = -1;
        (self.deps.join_topic)(&format!("channel:{}", record.key));
        (self.deps.attach_wakeup)(bee.clone());
        Some(bee)
    }

    async fn hibernate_to_fit(&self, keep: &str) {
        loop {
            let active_count = self.active_count();
            if active_count < self.deps.cap {
                return;
            }
            let victim = {
                let active = lock(&self.active);
                let records = lock(&self.records);
                active
                    .iter()
                    .filter(|key| key.as_str() != keep)
                    .filter_map(|key| records.get(key).cloned())
                    .filter(|record| record.active())
                    .min_by_key(|record| record.touched_at.load(Ordering::Acquire))
            };
            let Some(victim) = victim else {
                return;
            };
            self.hibernate(&victim).await;
        }
    }

    /// `hibernateSource(src)`.
    pub async fn hibernate(&self, record: &Arc<SourceRecord>) {
        if record.bee().is_none() {
            return;
        }
        let old = lock(&record.bee).take();
        lock(&self.active).remove(&record.key);
        if let Some(bee) = old {
            (self.deps.leave_topic)(format!("channel:{}", record.key));
            let _ = bee.close().await;
        }
    }

    /// `scanChannelView(view, viaKey)`.
    pub async fn scan(
        &self,
        record: &Arc<SourceRecord>,
        bee: &Arc<Autobee>,
    ) -> (Vec<Item>, Vec<DiscoveredSource>) {
        let mut entries = Vec::new();
        let mut discovered = Vec::new();
        let view = bee.view.view().clone();
        let mut stream = view.range(hyperbee::RangeOptions::default());
        loop {
            let next = stream.next().await;
            let Ok(Some(node)) = next else {
                break;
            };
            let sub = String::from_utf8_lossy(&node.key).into_owned();
            if sub.starts_with("source/") {
                if let Some(op) = decode_op(node.value)
                    && op.get("type").and_then(Value::as_str) == Some("source")
                {
                    let source_key = op
                        .get("sourceKey")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned();
                    discovered.push(DiscoveredSource {
                        key: source_key.clone(),
                        kind: "channel".to_owned(),
                        title: clean_text(
                            op.get("title").and_then(Value::as_str),
                            &source_key[..source_key.len().min(12)],
                        ),
                        via: record.key.clone(),
                    });
                }
            } else if !is_system_view_key(&sub)
                && let Some(op) = decode_op(node.value)
                && op.get("type").and_then(Value::as_str) == Some("announce")
            {
                entries.push(Item { sub, entry: op });
            }
        }
        (entries, discovered)
    }

    /// `refreshSourceCache(src)`.
    pub async fn refresh(&self, key: &str, force: bool) -> bool {
        let Some(record) = self.get(key) else {
            return false;
        };
        if self.deps.closing.load(Ordering::Acquire) {
            return false;
        }
        let Some(bee) = record.bee() else {
            return false;
        };
        if force {
            if record.hydrated() {
                record.touch(&self.clock);
            }
            bee.wakeup.request_lookup();
            let _ = tokio::time::timeout(Duration::from_secs(2), bee.update()).await;
        } else {
            let _ = tokio::time::timeout(Duration::from_secs(1), bee.update()).await;
        }
        let (entries, discovered) = self.scan(&record, &bee).await;
        let previous = lock(&record.cache).entries.len();
        record.index(&entries, &discovered, true);
        {
            let mut cache = lock(&record.cache);
            cache.view_version = cache.view_version.saturating_add(1);
            cache.cached_rev = record.rev.load(Ordering::Acquire) as i64;
            if !entries.is_empty() || !discovered.is_empty() {
                cache.hydrated = true;
            }
            cache.synced_at = now_ms();
        }
        if lock(&record.cache).hydrated {
            self.dirty.store(true, Ordering::Release);
        }
        if entries.len() != previous {
            (self.deps.update)();
        }
        true
    }

    /// `addSource(spec, wake)`.
    pub async fn add(&self, spec: SourceSpec, wake: bool) -> Result<Source, String> {
        let (key, alias) = match &spec {
            SourceSpec::Uri(text) => {
                let lower = text.trim().to_lowercase();
                if !crate::protocol::is_hex64(&lower) {
                    return Err("Expected a channel source, not a watch URI".into());
                }
                (lower, String::new())
            }
            SourceSpec::Object(object) => {
                let key = object
                    .key
                    .clone()
                    .or_else(|| object.uri.clone())
                    .unwrap_or_default()
                    .trim()
                    .to_lowercase();
                (key, clean_text(object.alias.as_deref(), ""))
            }
        };
        if !crate::protocol::is_hex64(&key) {
            return Err("Expected a channel source, not a watch URI".into());
        }
        let existed = lock(&self.records).contains_key(&key);
        if !existed {
            self.ensure(&spec, false);
            if wake {
                self.wake(&key, true).await;
            }
            (self.deps.update)();
        } else {
            if !alias.is_empty()
                && let Some(record) = self.get(&key)
            {
                record.set_alias(&alias);
            }
            if wake {
                self.wake(&key, true).await;
            }
            if !alias.is_empty() {
                (self.deps.update)();
            }
        }
        let record = self.get(&key);
        Ok(Source {
            key: key.clone(),
            kind: "channel".to_owned(),
            alias: record
                .as_ref()
                .map_or(alias.clone(), |record| record.alias()),
            primary: false,
            writable: false,
            entries: record
                .as_ref()
                .map(|record| lock(&record.cache).entries.len() as u64),
            active: record.as_ref().map(|record| record.active()),
            hydrated: record.as_ref().map(|record| record.hydrated()),
        })
    }

    /// `removeSource(key)`.
    pub async fn remove(&self, key: &str) -> Removed {
        let key = key.trim().to_lowercase();
        let record = lock(&self.records).remove(&key);
        let Some(record) = record else {
            return Removed { removed: false };
        };
        lock(&self.saved).remove(&key);
        self.dirty.store(true, Ordering::Release);
        self.hibernate(&record).await;
        (self.deps.leave_topic)(format!("channel:{key}"));
        (self.deps.update)();
        Removed { removed: true }
    }

    /// `listSources()`.
    pub fn list(&self, primary: Option<Source>) -> Vec<Source> {
        let mut list = Vec::new();
        if let Some(primary) = primary {
            list.push(primary);
        }
        let records = lock(&self.records);
        for record in records.values() {
            let cache = lock(&record.cache);
            list.push(Source {
                key: record.key.clone(),
                kind: "channel".to_owned(),
                alias: record.alias(),
                primary: false,
                writable: false,
                entries: Some(cache.entries.len() as u64),
                active: Some(record.active()),
                hydrated: Some(cache.hydrated),
            });
        }
        list
    }

    /// `discoveredSources()` over the pool.
    pub async fn discovered(
        &self,
        primary: Vec<DiscoveredSource>,
        active: Vec<String>,
    ) -> Vec<DiscoveredSource> {
        for key in self.keys() {
            let _ = self.refresh(&key, false).await;
        }
        let active: HashSet<String> = active.into_iter().collect();
        let mut found: Vec<DiscoveredSource> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for item in primary {
            if !active.contains(&item.key) && seen.insert(item.key.clone()) {
                found.push(item);
            }
        }
        let records = lock(&self.records);
        for record in records.values() {
            for item in record.discovered() {
                if !active.contains(&item.key) && seen.insert(item.key.clone()) {
                    found.push(item);
                }
            }
        }
        found
    }

    /// `flushCatalogCache()`: tmp then rename, `0600`.
    pub fn flush(&self) {
        if !self.dirty.swap(false, Ordering::AcqRel) {
            return;
        }
        let mut channels = serde_json::Map::new();
        let records = lock(&self.records);
        for record in records.values() {
            let cache = lock(&record.cache);
            if !cache.hydrated {
                continue;
            }
            let entries: Vec<Value> = cache
                .entries
                .iter()
                .map(|item| json!([item.sub, item.entry]))
                .collect();
            channels.insert(
                record.key.clone(),
                json!({
                    "syncedAt": cache.synced_at,
                    "entries": entries,
                    "discovered": cache.discovered,
                }),
            );
        }
        let payload = json!({ "version": 1, "channels": channels });
        let Ok(text) = serde_json::to_string(&payload) else {
            return;
        };
        let tmp = self.deps.cache_file.with_extension("json.tmp");
        let write = || -> std::io::Result<()> {
            use std::io::Write;
            #[cfg(unix)]
            let mut options = {
                use std::os::unix::fs::OpenOptionsExt;
                let mut options = std::fs::OpenOptions::new();
                options.mode(0o600);
                options
            };
            #[cfg(not(unix))]
            let mut options = std::fs::OpenOptions::new();
            let mut file = options.write(true).create(true).truncate(true).open(&tmp)?;
            file.write_all(text.as_bytes())?;
            drop(file);
            std::fs::rename(&tmp, &self.deps.cache_file)
        };
        let _ = write();
        *lock(&self.saved) = channels.into_iter().collect();
    }

    pub fn flush_if_dirty(&self) {
        if self.dirty.load(Ordering::Acquire) {
            self.flush();
        }
    }

    /// `wakeSource(key, { touch })` for the catalog's filter path.
    pub async fn focus(&self, key: &str) -> bool {
        let key = key.trim().to_lowercase();
        if !lock(&self.records).contains_key(&key) {
            return false;
        }
        self.wake(&key, true).await.is_some()
    }
}

/// `Apply` as a `Handlers`, for a followed channel.
struct HandlersWrap(Apply);

impl autobee::apply_view::Handlers for HandlersWrap {
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [autobee::types::Node],
        view: &'a hyperbee::Bee,
        host: &'a autobee::apply_calls::ApplyCalls<'a, 'b>,
    ) -> autobee::apply_calls::BoxFuture<'a, autobee::apply_calls::Result<()>> {
        self.0.apply(nodes, view, host)
    }

    fn is_trusted<'a>(
        &'a self,
        key: &'a Key,
        reference: Option<&'a hyperbee::Bee>,
    ) -> std::pin::Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.0.is_trusted(key, reference)
    }

    fn update<'a>(
        &'a self,
        view: &'a hyperbee::Bee,
        changes: &'a autobee::updates::Changes,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.0.update(view, changes)
    }

    fn has_update(&self) -> bool {
        self.0.has_update()
    }
}

/// The source's Autobee namespace key.
pub fn topic_key(hex_key: &str) -> Option<[u8; 32]> {
    parse_hex32(hex_key)
}

/// The hex of a core key.
pub fn key_hex(key: &[u8; 32]) -> String {
    hex(key)
}
