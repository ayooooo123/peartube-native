//! Port of `mirror.js`'s `p2p-replicate` plugin: a replica relay downloads every catalog
//! title it does not hold, linearly through Hyperblobs ranges, until a free-disk reserve is
//! reached, without adding catalog entries. Backfill stands down while any connected relay
//! reports viewers over `peartube/bulk-busy`, and one pacer shares `replicateMbps` across it.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hypercore::Session;
use serde_json::{Value, json};

use crate::mirror::{Ctx, MirrorError, MirrorPlugin};
use crate::node::Node;
use crate::node::api::SearchOptions;
use crate::node::core::CoreNode;
use crate::protocol::{ParsedUri, parse_uri};
use crate::traffic::TrafficGate;

/// Settings the relay hands over; `sources` limits replication to titles those keys own.
#[derive(Clone, Debug, Default)]
pub struct ReplicateConfig {
    pub enabled: bool,
    pub sources: Option<HashSet<String>>,
    pub min_free_bytes: u64,
    pub mbps: f64,
}

impl ReplicateConfig {
    /// `setReplicate(value)` with the relay's settings: `replicate` and `replicateSources`.
    pub fn new(replicate: bool, sources: &[String], free_gb: f64, mbps: f64) -> Self {
        let sources: HashSet<String> = sources
            .iter()
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        Self {
            enabled: replicate,
            sources: (!sources.is_empty()).then_some(sources),
            min_free_bytes: (free_gb.max(0.0) * 1024.0 * 1024.0 * 1024.0) as u64,
            mbps: mbps.max(0.0),
        }
    }
}

const STALL: Duration = Duration::from_secs(20);
const POLL: Duration = Duration::from_millis(250);

/// One catalog title's block range in a blob core.
#[derive(Clone)]
struct Slice {
    id: String,
    title: String,
    size: u64,
    blobs: String,
    offset: u64,
    length: u64,
    in_channel: bool,
    owners: HashSet<String>,
}

enum Outcome {
    Done,
    Stalled,
    DiskFull,
    Closed,
}

struct Inner {
    node: Arc<CoreNode>,
    storage: PathBuf,
    traffic: TrafficGate,
    config: Mutex<ReplicateConfig>,
    state: Mutex<Value>,
    replicating: AtomicBool,
    rerun: AtomicBool,
    closed: AtomicBool,
    /// Blob cores no peer served lately, retried after their deadline.
    dead: Mutex<HashMap<String, Instant>>,
    pace_at: Mutex<Instant>,
    /// The pass a poll started; it outlives the poll so other plugins keep polling.
    polled: Mutex<Option<tokio::task::JoinHandle<()>>>,
    onchange: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// The `p2p-replicate` plugin. Cloning shares it.
#[derive(Clone)]
pub struct Replicator(Arc<Inner>);

impl Replicator {
    pub fn new(
        node: Arc<CoreNode>,
        storage: PathBuf,
        traffic: TrafficGate,
        config: ReplicateConfig,
        onchange: Option<Arc<dyn Fn() + Send + Sync>>,
    ) -> Self {
        Self(Arc::new(Inner {
            node,
            storage,
            traffic,
            config: Mutex::new(config),
            state: Mutex::new(json!({
                "replicatedEntries": 0, "totalEntries": 0, "replicatedBytes": 0, "targetBytes": 0,
                "activeSlices": 0, "activeTitles": [], "skippedDiskFull": 0, "skippedUnavailable": 0,
            })),
            replicating: AtomicBool::new(false),
            rerun: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            dead: Mutex::new(HashMap::new()),
            pace_at: Mutex::new(Instant::now()),
            polled: Mutex::new(None),
            onchange,
        }))
    }

    pub fn enabled(&self) -> bool {
        self.0.config.lock().unwrap().enabled
    }

    /// Live settings; turning replication off stops the workers at their next window.
    pub fn configure(&self, config: ReplicateConfig) {
        *self.0.config.lock().unwrap() = config;
    }

    pub fn close(&self) {
        self.0.closed.store(true, Ordering::SeqCst);
        if let Some(task) = self.0.polled.lock().unwrap().take() {
            task.abort();
        }
    }

    fn running(&self) -> bool {
        !self.0.closed.load(Ordering::SeqCst) && self.enabled()
    }

    fn set(&self, key: &str, value: Value) {
        self.0.state.lock().unwrap()[key] = value;
    }

    /// `statfsSync(storage)`: blocks available to us; unknown reads as unlimited, as in JS.
    fn free_disk(&self) -> u64 {
        crate::cast::disk::free_disk_bytes_or_unbounded(&self.0.storage)
    }

    /// One pacer for all of replication: greedy backfill leaves seeders and this disk room for viewers.
    async fn pace(&self, bytes: u64) {
        let mbps = self.0.config.lock().unwrap().mbps;
        if mbps <= 0.0 || bytes == 0 {
            return;
        }
        let wait = {
            let mut at = self.0.pace_at.lock().unwrap();
            let now = Instant::now();
            let start = (*at).max(now);
            *at = start + Duration::from_secs_f64(bytes as f64 / (mbps * 125_000.0));
            start - now
        };
        tokio::time::sleep(wait).await;
    }

    async fn wait_while_peers_busy(&self) {
        while !self.0.closed.load(Ordering::SeqCst) && self.0.traffic.peers_busy() {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Every title of the catalog and of followed channels, deduplicated by block range.
    async fn slices(&self) -> Vec<Slice> {
        let node = &self.0.node;
        let mut items = node
            .search(None, SearchOptions::default())
            .await
            .unwrap_or_default();
        for source in node.list_sources().await {
            if source.kind == "channel" && !source.writable {
                let options = SearchOptions {
                    source: Some(Some(source.key.clone())),
                    ..SearchOptions::default()
                };
                items.extend(node.search(None, options).await.unwrap_or_default());
            }
        }
        let mut by_tag: HashMap<String, Slice> = HashMap::new();
        let mut order = Vec::new();
        for item in items {
            if item.local || item.magnet_url.is_empty() {
                continue;
            }
            let Ok(ParsedUri::Watch { entry, .. }) = parse_uri(&item.magnet_url) else {
                continue;
            };
            if entry.blobs.is_empty() || entry.blob.block_length == 0 {
                continue;
            }
            let tag = format!(
                "{}:{}:{}",
                entry.blobs, entry.blob.block_offset, entry.blob.block_length
            );
            let slice = by_tag.entry(tag.clone()).or_insert_with(|| {
                order.push(tag.clone());
                Slice {
                    id: item.id.clone(),
                    title: if item.title.is_empty() {
                        item.id.clone()
                    } else {
                        item.title.clone()
                    },
                    size: if item.size > 0 {
                        item.size
                    } else {
                        entry.blob.byte_length
                    },
                    blobs: entry.blobs.clone(),
                    offset: entry.blob.block_offset,
                    length: entry.blob.block_length,
                    in_channel: false,
                    owners: HashSet::from([entry.blobs.clone()]),
                }
            });
            if !item.source.is_empty() {
                slice.owners.insert(item.source.to_lowercase());
            }
            if item.source_kind == "channel" {
                slice.in_channel = true;
            }
            if !item.key.starts_with("channel:") && !item.key.starts_with("watch/") {
                let raw = strip_tracker_prefix(&item.key);
                if let Some(writer) = raw.split('/').nth(1).filter(|w| !w.is_empty()) {
                    slice.owners.insert(writer.to_lowercase());
                }
            }
        }
        let filter = self.0.config.lock().unwrap().sources.clone();
        order
            .into_iter()
            .filter_map(|tag| by_tag.remove(&tag))
            .filter(|s| {
                filter
                    .as_ref()
                    .is_none_or(|keys| s.owners.iter().any(|o| keys.contains(o)))
            })
            .collect()
    }

    async fn pass(&self, payload: &Value) -> Value {
        let min_free = payload["minFreeBytes"]
            .as_f64()
            .map_or(self.0.config.lock().unwrap().min_free_bytes, |b| b as u64);
        let stall = payload["stallMs"]
            .as_u64()
            .map_or(STALL, Duration::from_millis);
        let mut slices = self.slices().await;
        let mut cores: HashMap<String, Arc<Session>> = HashMap::new();
        for hex in slices
            .iter()
            .map(|s| s.blobs.clone())
            .collect::<HashSet<_>>()
        {
            if let Ok(core) = self.0.node.blob_core(&hex).await {
                cores.insert(hex, core);
            }
        }
        // Channel titles first, then ones a peer can already serve, then by core and offset.
        slices.sort_by(|a, b| {
            let served = |s: &Slice| {
                cores
                    .get(&s.blobs)
                    .is_some_and(|c| c.length() >= s.offset + s.length)
            };
            b.in_channel
                .cmp(&a.in_channel)
                .then(served(b).cmp(&served(a)))
                .then(a.blobs.cmp(&b.blobs))
                .then(a.offset.cmp(&b.offset))
        });
        let target: u64 = slices.iter().map(|s| s.size).sum();
        self.set("totalEntries", json!(slices.len()));
        self.set("targetBytes", json!(target));
        let (mut done_entries, mut done_bytes, mut skipped_full, mut skipped_dead) =
            (0u64, 0u64, 0u64, 0u64);
        let mut pending = Vec::new();
        for slice in slices.iter() {
            let Some(core) = cores.get(&slice.blobs) else {
                skipped_dead += 1;
                continue;
            };
            let have = slice.held(core);
            if have >= slice.length {
                done_entries += 1;
                done_bytes += slice.size;
            } else {
                done_bytes += partial(slice, have);
                pending.push(slice.clone());
            }
        }
        self.set("replicatedEntries", json!(done_entries));
        self.set("replicatedBytes", json!(done_bytes));
        self.set("skippedDiskFull", json!(0));
        self.set("skippedUnavailable", json!(skipped_dead));

        let mut pruned = false;
        for slice in pending {
            if !self.running() {
                break;
            }
            let Some(core) = cores.get(&slice.blobs).cloned() else {
                continue;
            };
            if self
                .0
                .dead
                .lock()
                .unwrap()
                .get(&slice.blobs)
                .is_some_and(|until| *until > Instant::now())
            {
                skipped_dead += 1;
                self.set("skippedUnavailable", json!(skipped_dead));
                continue;
            }
            let have = slice.held(&core);
            if have >= slice.length {
                done_entries += 1;
                self.set("replicatedEntries", json!(done_entries));
                continue;
            }
            let prior = partial(&slice, have);
            let missing = slice.size.saturating_sub(prior);
            if self.free_disk().saturating_sub(missing) < min_free {
                if !pruned {
                    pruned = true;
                    self.prune_unreferenced().await;
                }
                if self.free_disk().saturating_sub(missing) < min_free {
                    skipped_full += 1;
                    self.set("skippedDiskFull", json!(skipped_full));
                    continue;
                }
            }
            self.set("activeSlices", json!(1));
            self.set(
                "activeTitles",
                json!([format!("{} · {}", slice.id, slice.title)]),
            );
            let outcome = self.download_slice(&core, &slice, min_free, stall).await;
            let after = partial(&slice, slice.held(&core));
            done_bytes += after.saturating_sub(prior);
            self.set("replicatedBytes", json!(done_bytes));
            self.set("activeSlices", json!(0));
            self.set("activeTitles", json!([]));
            match outcome {
                Outcome::Done => {
                    done_entries += 1;
                    self.set("replicatedEntries", json!(done_entries));
                    self.0.dead.lock().unwrap().remove(&slice.blobs);
                    if let Some(onchange) = &self.0.onchange {
                        onchange();
                    }
                }
                Outcome::Stalled => {
                    let status = self.0.node.status().await;
                    let retry = if status.peers + status.lan_peers > 0 {
                        300
                    } else {
                        15
                    };
                    self.0.dead.lock().unwrap().insert(
                        slice.blobs.clone(),
                        Instant::now() + Duration::from_secs(retry),
                    );
                    skipped_dead += 1;
                    self.set("skippedUnavailable", json!(skipped_dead));
                }
                Outcome::DiskFull => {
                    skipped_full += 1;
                    self.set("skippedDiskFull", json!(skipped_full));
                }
                Outcome::Closed => break,
            }
        }
        json!({
            "totalEntries": slices.len(),
            "replicatedEntries": done_entries,
            "replicatedBytes": done_bytes,
            "targetBytes": target,
            "freeDiskBytes": self.free_disk(),
            "skippedDiskFull": skipped_full,
            "skippedUnavailable": skipped_dead,
        })
    }

    /// Double-buffered linear windows of up to 16 MiB stay sequential on spinning disks
    /// while the pacer spaces them out.
    async fn download_slice(
        &self,
        core: &Arc<Session>,
        slice: &Slice,
        min_free: u64,
        stall: Duration,
    ) -> Outcome {
        let end = slice.offset + slice.length;
        let mut last = slice.held(core);
        if last >= slice.length {
            return Outcome::Done;
        }
        let avg = slice
            .size
            .checked_div(slice.length)
            .map_or(65_536, |avg| avg.max(4096));
        let window = (16 * 1024 * 1024 / avg).clamp(16, 256);
        self.wait_while_peers_busy().await;
        let mut cur = self.open_window(core, slice.offset, end, window, avg).await;
        let mut next = match &cur {
            Some(w) if w.end < end => self.open_window(core, w.end, end, window, avg).await,
            _ => None,
        };
        let mut progress_at = Instant::now();
        while let Some(mut window_now) = cur.take() {
            if !self.running() {
                return Outcome::Closed;
            }
            loop {
                if window_now.task.is_finished() {
                    break;
                }
                tokio::time::sleep(POLL).await;
                if !self.running() {
                    return Outcome::Closed;
                }
                if self.0.traffic.peers_busy() {
                    // Drop the windows so no requests sit queued at the seeder, then resume here.
                    let resume = window_now.start;
                    drop(next.take());
                    drop(window_now);
                    self.wait_while_peers_busy().await;
                    if !self.running() {
                        return Outcome::Closed;
                    }
                    progress_at = Instant::now();
                    let Some(again) = self.open_window(core, resume, end, window, avg).await else {
                        return Outcome::Closed;
                    };
                    window_now = again;
                    next = match window_now.end < end {
                        true => {
                            self.open_window(core, window_now.end, end, window, avg)
                                .await
                        }
                        false => None,
                    };
                    continue;
                }
                let have = slice.held(core);
                if have > last {
                    last = have;
                    progress_at = Instant::now();
                    if self.free_disk() < (min_free / 2).max(4 * 1024 * 1024 * 1024) {
                        return Outcome::DiskFull;
                    }
                } else if progress_at.elapsed() >= stall {
                    return Outcome::Stalled;
                }
            }
            let have = slice.held(core);
            if have > last {
                last = have;
                progress_at = Instant::now();
            }
            cur = next.take();
            next = match &cur {
                Some(w) if w.end < end => self.open_window(core, w.end, end, window, avg).await,
                _ => None,
            };
        }
        if slice.held(core) >= slice.length {
            Outcome::Done
        } else {
            Outcome::Stalled
        }
    }

    async fn open_window(
        &self,
        core: &Arc<Session>,
        start: u64,
        end: u64,
        window: u64,
        avg: u64,
    ) -> Option<Window> {
        if start >= end {
            return None;
        }
        let stop = end.min(start + window);
        let missing = (stop - start).saturating_sub(core.count_local(start, stop));
        self.pace(missing * avg).await;
        self.wait_while_peers_busy().await;
        if !self.running() {
            return None;
        }
        let download = core.download(start, Some(stop), true);
        Some(Window {
            start,
            end: stop,
            task: tokio::spawn(async move { download.await.unwrap_or(false) }),
        })
    }

    /// Clears cached spans of replica cores that no live catalog entry points at, so fresh
    /// titles can take their place.
    async fn prune_unreferenced(&self) {
        let mut keep: HashMap<String, Vec<(u64, u64)>> = HashMap::new();
        for slice in self.slices_all().await {
            keep.entry(slice.blobs.clone())
                .or_default()
                .push((slice.offset, slice.offset + slice.length));
        }
        let mut cleared_any = false;
        for (hex, mut spans) in keep {
            let Ok(core) = self.0.node.blob_core(&hex).await else {
                continue;
            };
            if core.writable() || core.length() == 0 {
                continue;
            }
            spans.sort();
            let mut merged: Vec<(u64, u64)> = Vec::new();
            for (s, e) in spans {
                match merged.last_mut() {
                    Some(last) if s <= last.1 => last.1 = last.1.max(e),
                    _ => merged.push((s, e)),
                }
            }
            let length = core.length();
            let mut cleared = false;
            let mut cursor = 0;
            for (s, e) in merged {
                let gap_end = s.min(length);
                if gap_end > cursor && core.count_local(cursor, gap_end) > 0 {
                    cleared |= core.clear(cursor, gap_end).await.is_ok();
                }
                cursor = cursor.max(e);
            }
            if cursor < length && core.count_local(cursor, length) > 0 {
                cleared |= core.clear(cursor, length).await.is_ok();
            }
            if cleared {
                cleared_any = true;
                let _ = self.0.node.refresh_block_count(&core).await;
            }
        }
        if cleared_any {
            let _ = self.0.node.gc_storage(false, false).await;
        }
    }

    /// Every catalog title, local ones included (`pruneUnreferencedReplicaBlocks(allItems)`).
    async fn slices_all(&self) -> Vec<Slice> {
        let node = &self.0.node;
        let mut items = node
            .search(None, SearchOptions::default())
            .await
            .unwrap_or_default();
        for source in node.list_sources().await {
            if source.kind == "channel" && !source.writable {
                let options = SearchOptions {
                    source: Some(Some(source.key.clone())),
                    ..SearchOptions::default()
                };
                items.extend(node.search(None, options).await.unwrap_or_default());
            }
        }
        items
            .into_iter()
            .filter_map(|item| match parse_uri(&item.magnet_url) {
                Ok(ParsedUri::Watch { entry, .. })
                    if !entry.blobs.is_empty() && entry.blob.block_length > 0 =>
                {
                    Some(Slice {
                        id: item.id,
                        title: item.title,
                        size: item.size,
                        blobs: entry.blobs,
                        offset: entry.blob.block_offset,
                        length: entry.blob.block_length,
                        in_channel: false,
                        owners: HashSet::new(),
                    })
                }
                _ => None,
            })
            .collect()
    }
}

struct Window {
    start: u64,
    end: u64,
    task: tokio::task::JoinHandle<bool>,
}

impl Drop for Window {
    /// `range.destroy()`: an abandoned window stops requesting its blocks.
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MirrorPlugin<CoreNode> for Replicator {
    fn name(&self) -> &str {
        "p2p-replicate"
    }

    fn description(&self) -> &str {
        "Replicates remote Hyperblobs slices across P2P/LAN up to a free-disk reserve with zero duplicate catalog entries"
    }

    fn run<'a>(
        &'a self,
        payload: Value,
        _ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Value, MirrorError>> + Send + 'a>> {
        Box::pin(async move { Ok(self.replicate(payload).await) })
    }

    fn poll<'a>(
        &'a self,
        _ctx: &'a Ctx<CoreNode>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, MirrorError>> + Send + 'a>> {
        Box::pin(async move {
            if self.enabled() && !self.0.replicating.load(Ordering::SeqCst) && self.running() {
                let replicator = self.clone();
                let task = tokio::spawn(async move {
                    replicator.replicate(json!({})).await;
                });
                *self.0.polled.lock().unwrap() = Some(task);
            }
            Ok(None)
        })
    }

    fn stop<'a>(&'a self) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(async move { self.close() })
    }

    fn info(&self) -> Value {
        let mut info = self.0.state.lock().unwrap().clone();
        let config = self.0.config.lock().unwrap().clone();
        if let Some(map) = info.as_object_mut() {
            map.insert("enabled".into(), json!(config.enabled));
            map.insert(
                "replicating".into(),
                json!(self.0.replicating.load(Ordering::SeqCst)),
            );
            map.insert("minFreeBytes".into(), json!(config.min_free_bytes));
            map.insert("freeDiskBytes".into(), json!(self.free_disk()));
            map.insert(
                "waitingForViewers".into(),
                json!(self.0.traffic.peers_busy()),
            );
        }
        info
    }
}

impl Replicator {
    /// `run('p2p-replicate', payload)`: one pass, or a note that one is running (which then
    /// passes again). `once` skips that rerun; `minFreeBytes` and `stallMs` override settings.
    pub async fn replicate(&self, payload: Value) -> Value {
        if !self.enabled() {
            return self.info_with("running", false);
        }
        if self.0.replicating.swap(true, Ordering::SeqCst) {
            self.0.rerun.store(true, Ordering::SeqCst);
            return self.info_with("running", true);
        }
        // A dropped run (a cancelled HTTP request, an aborted task) must not stay "running".
        struct Running<'a>(&'a AtomicBool);
        impl Drop for Running<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _running = Running(&self.0.replicating);
        let once = payload["once"].as_bool().unwrap_or(false);
        loop {
            self.0.rerun.store(false, Ordering::SeqCst);
            let result = self.pass(&payload).await;
            if !(self.0.rerun.load(Ordering::SeqCst) && self.running() && !once) {
                break result;
            }
        }
    }

    fn info_with(&self, key: &str, value: bool) -> Value {
        let mut info = MirrorPlugin::<CoreNode>::info(self);
        info[key] = json!(value);
        info
    }
}

impl Slice {
    /// Blocks of this slice the node stores (`core.count()`).
    fn held(&self, core: &Session) -> u64 {
        core.count_local(self.offset, self.offset + self.length)
    }
}

/// Bytes of a slice held, pro rata to its blocks.
fn partial(slice: &Slice, have: u64) -> u64 {
    if have >= slice.length {
        slice.size
    } else if have == 0 {
        0
    } else {
        ((have as f64 / slice.length as f64) * slice.size as f64)
            .round()
            .min(slice.size as f64) as u64
    }
}

fn strip_tracker_prefix(key: &str) -> &str {
    match key.strip_prefix("tracker:") {
        Some(rest) if rest.len() > 65 && rest.as_bytes()[64] == b'/' => &rest[65..],
        _ => key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_filters_sources_and_converts_reserve() {
        let config = ReplicateConfig::new(true, &[" ABC ".into(), String::new()], 15.0, 0.0);
        assert_eq!(config.sources, Some(HashSet::from(["abc".to_owned()])));
        assert_eq!(config.min_free_bytes, 15 * 1024 * 1024 * 1024);
        assert_eq!(ReplicateConfig::new(true, &[], 1.0, 0.0).sources, None);
    }

    #[test]
    fn tracker_prefix_is_stripped_only_with_a_full_key() {
        let key = format!("tracker:{}/imdb:1/writer/blob", "a".repeat(64));
        assert_eq!(strip_tracker_prefix(&key), "imdb:1/writer/blob");
        assert_eq!(
            strip_tracker_prefix("imdb:1/writer/blob"),
            "imdb:1/writer/blob"
        );
    }
}
