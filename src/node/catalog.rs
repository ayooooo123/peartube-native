//! `src/node/catalog.js`: the listing the app searches, and the URI opener.
//!
//! A listing unions the node's own channel with every awake channel's authoritative applied view,
//! using the persisted snapshot only while that channel sleeps. It deduplicates by id/sha256 in
//! catalog order with the JS ranking, then adds the play and share links.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};

use futures_util::future::join_all;
use serde_json::Value;

use super::api::{Announce, Blob, Entry, OpenUri, SearchOptions};
use super::channel::Channel;
use super::sources::{Item, Sources};
use crate::protocol::{
    BlobPointer, ParsedUri, WatchEntry, clean_text, decode_op, describe_entry, format_watch_uri,
    parse_uri,
};

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
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

/// What the catalog asks of the blob store.
pub trait CatalogBlobs: Send + Sync {
    fn owned(&self, blob: &[u8; 32]) -> bool;
    fn slot_contains(&self, blob: &[u8; 32]) -> bool;
    fn slot_keys(&self) -> Vec<[u8; 32]>;
    fn open_core(&self, blob: &[u8; 32]) -> bool;
    fn cached_meta(&self, blob: &[u8; 32]) -> Option<(u64, u64, u64)>;
    fn absent(&self, discovery: &[u8; 32]) -> bool;
    fn blob_disc(&self, blob: &[u8; 32]) -> [u8; 32];
    /// `coreStorage.getInfos(discs, { auth: false, head: true, hints: false })`: one RocksDB
    /// pass. `None` means the probe failed, which says nothing about the batch.
    fn infos(&self, discovery: &[[u8; 32]]) -> Option<Vec<Option<(u64, u64)>>>;
    /// `absentDisc.add`: this discovery key is not on disk.
    fn mark_absent(&self, discovery: &[u8; 32]);
    /// `recordCached(hex, 0, 0, 0)`: a stored core with no blocks.
    fn record_probe(&self, blob: [u8; 32], length: u64, byte_length: u64, blocks: u64);
    /// `blobCore(hex)`: the open session for a slot or open core, kept for the ledger.
    fn blob_core<'a>(&'a self, blob: &'a [u8; 32])
    -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    /// A stored core with data: open it inactive, count its blocks and record the ledger entry.
    fn probe_blocks<'a>(
        &'a self,
        blob: &'a [u8; 32],
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn entry_cached_bytes<'a>(
        &'a self,
        blob: &'a [u8; 32],
        entry: &'a Announce,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>>;
    fn resolve_video_key(&self, entry: &Announce) -> bool;
    /// `offerCopy`: a complete cached copy a relay may announce when a slot is free.
    fn offer_copy(&self, blob: &[u8; 32]);
    /// `ctx.net.dht`: private routing reaches only followed channels.
    fn private_routing(&self) -> bool;
    fn join_source_swarm(&self, source: &[u8; 32]);
    fn leave_source_swarm(&self, source: &[u8; 32]);
    fn stream_url(&self, entry: &Announce) -> String;
    fn warm<'a>(&'a self, entry: &'a Announce) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// The node's own channel snapshot.
#[derive(Default)]
struct PrimaryCache {
    entries: Vec<Item>,
    discovered: Vec<super::api::DiscoveredSource>,
    head: Option<hyperbee::Head>,
}

/// A record the catalog has not yet turned into an `Entry`.
#[derive(Clone)]
struct Raw {
    key: String,
    entry: Value,
    source: String,
    source_kind: String,
}

/// `createCatalog(ctx)`.
pub struct Catalog {
    channel: Arc<Channel>,
    sources: Arc<Sources>,
    blobs: Arc<dyn CatalogBlobs>,
    primary: Mutex<PrimaryCache>,
    ephemeral: Mutex<HashMap<String, (Value, String, String)>>,
    /// Channels whose swarm a watch link joined without following them, oldest first.
    link_swarms: Mutex<Vec<String>>,
}

impl Catalog {
    pub fn new(
        channel: Arc<Channel>,
        sources: Arc<Sources>,
        blobs: Arc<dyn CatalogBlobs>,
    ) -> Arc<Self> {
        Arc::new(Self {
            channel,
            sources,
            blobs,
            primary: Mutex::new(PrimaryCache::default()),
            ephemeral: Mutex::new(HashMap::new()),
            link_swarms: Mutex::new(Vec::new()),
        })
    }

    /// `refreshPrimaryCache()`: replace, never merge.
    pub async fn refresh_primary(&self, force: bool) {
        if self.channel.closing() {
            return;
        }
        if force {
            self.channel.auto().wakeup.request_lookup();
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.channel.auto().update(),
            )
            .await;
        } else {
            self.channel.applied().await;
        }
        let head = self.channel.view().head();
        if head.is_some() && lock(&self.primary).head == head {
            return;
        }
        // A landed view whose blocks nobody serves ends at the read timeout: the last complete
        // scan stands, and the head is left unrecorded so the next tick rescans.
        let Ok((entries, discovered)) =
            super::sources::scan_view(self.channel.view(), &self.channel.primary_key()).await
        else {
            return;
        };
        {
            let mut primary = lock(&self.primary);
            primary.entries = entries;
            primary.discovered = discovered;
            // A sparse/failed walk must be retried, and an apply during the walk invalidates it.
            primary.head = head.filter(|head| Some(*head) == self.channel.view().head());
        }
    }

    /// `search(id, { source, query, limit, offset })`.
    pub async fn search(&self, id: Option<&str>, options: SearchOptions) -> Vec<Entry> {
        let filter = options.source.clone().flatten();
        if let Some(filter) = &filter {
            let _ = self.sources.focus(filter).await;
        }
        let primary_used = self.channel.use_primary() || self.channel.has_entries();
        let primary_wanted = filter
            .as_deref()
            .is_none_or(|key| key == self.channel.primary_key());
        let source_keys: Vec<_> = self
            .sources
            .keys()
            .into_iter()
            .filter(|key| filter.as_deref().is_none_or(|filter| filter == key))
            .collect();
        let primary_refresh = async {
            if primary_used && primary_wanted {
                self.refresh_primary(false).await;
            }
        };
        let source_refresh = join_all(
            source_keys
                .iter()
                .map(|key| self.sources.refresh_entries(key, false)),
        );
        let (_, source_entries) = tokio::join!(primary_refresh, source_refresh);

        let mut raw: Vec<Raw> = Vec::new();
        let mut seen_keys: HashSet<String> = HashSet::new();
        if primary_used && primary_wanted {
            let items = lock(&self.primary).entries.clone();
            for item in items {
                if seen_keys.insert(item.sub.clone()) {
                    raw.push(Raw {
                        key: item.sub,
                        entry: item.entry,
                        source: self.channel.primary_key(),
                        source_kind: "channel".to_owned(),
                    });
                }
            }
        }
        for (key, entries) in source_keys.into_iter().zip(source_entries) {
            let Some(record) = self.sources.get(&key) else {
                continue;
            };
            // Awake scans are authoritative; only sleeping channels use the merged snapshot.
            let items = entries.unwrap_or_else(|| record.entries());
            for item in items {
                let full = format!("channel:{key}/{}", item.sub);
                if seen_keys.insert(full.clone()) {
                    raw.push(Raw {
                        key: full,
                        entry: item.entry,
                        source: key.clone(),
                        source_kind: "channel".to_owned(),
                    });
                }
            }
        }
        for (key, (entry, source, source_kind)) in lock(&self.ephemeral).iter() {
            if filter.as_deref().is_some_and(|filter| filter != "direct") {
                continue;
            }
            raw.push(Raw {
                key: key.clone(),
                entry: entry.clone(),
                source: source.clone(),
                source_kind: source_kind.clone(),
            });
        }

        if let Some(id) = id {
            raw.retain(|item| item.entry.get("id").and_then(Value::as_str) == Some(id));
        }

        // Batch-probe unindexed candidate discovery keys straight out of RocksDB in one `getInfos`
        // pass, negative-caching absent keys in `absent` so repeat catalog searches touch zero disk.
        let mut open_to_probe: Vec<[u8; 32]> = Vec::new();
        let mut disk_to_probe: Vec<[u8; 32]> = Vec::new();
        let mut seen_blobs: HashSet<[u8; 32]> = HashSet::new();
        for item in &raw {
            let Some(blob_hex) = item.entry.get("blobs").and_then(Value::as_str) else {
                continue;
            };
            let Some(blob) = parse_hex32(blob_hex) else {
                continue;
            };
            if !seen_blobs.insert(blob) || self.blobs.owned(&blob) {
                continue;
            }
            if self.blobs.slot_contains(&blob) || self.blobs.open_core(&blob) {
                open_to_probe.push(blob);
                continue;
            }
            if self.blobs.cached_meta(&blob).is_some() {
                continue;
            }
            if self.blobs.absent(&self.blobs.blob_disc(&blob)) {
                continue;
            }
            disk_to_probe.push(blob);
        }
        for chunk in open_to_probe.chunks(16) {
            join_all(chunk.iter().map(|blob| self.blobs.blob_core(blob))).await;
        }
        for batch in disk_to_probe.chunks(256) {
            let discovery: Vec<[u8; 32]> = batch
                .iter()
                .map(|blob| self.blobs.blob_disc(blob))
                .collect();
            // A failed probe says nothing about its batch: skipping it leaves those keys uncached
            // for the next search, where an empty answer would have recorded all of them absent.
            let Some(infos) = self.blobs.infos(&discovery) else {
                continue;
            };
            let mut with_data: Vec<[u8; 32]> = Vec::new();
            for (index, blob) in batch.iter().enumerate() {
                match infos.get(index).copied().flatten() {
                    None => {
                        let discovery = self.blobs.blob_disc(blob);
                        self.blobs.mark_absent(&discovery);
                    }
                    Some((0, _)) => {
                        self.blobs.record_probe(*blob, 0, 0, 0);
                    }
                    Some(_) => with_data.push(*blob),
                }
            }
            for chunk in with_data.chunks(16) {
                join_all(chunk.iter().map(|blob| self.blobs.probe_blocks(blob))).await;
            }
        }

        // Insertion order, as the JS `Map` keeps it: listings must come back in catalog order.
        let mut files: Vec<Entry> = Vec::new();
        let mut positions: HashMap<String, usize> = HashMap::new();
        for item in raw {
            let Some(announce) = announce_from(&item.entry) else {
                continue;
            };
            let private_entry = item.entry.get("wrappedKey").is_some();
            let video_key = if private_entry {
                self.blobs.resolve_video_key(&announce)
            } else {
                false
            };
            let locked = private_entry && !video_key;
            let blob = parse_hex32(&announce.blobs).unwrap_or_default();
            let owned = self.blobs.owned(&blob);
            let cached = if owned {
                announce.size
            } else {
                self.blobs.entry_cached_bytes(&blob, &announce).await
            };
            if !owned && cached > 0 && cached >= announce.size {
                self.blobs.offer_copy(&blob);
            }
            let local = owned || (self.blobs.slot_contains(&blob) && cached >= announce.size);
            let info = describe_entry(
                &item.entry_id(),
                &clean_text(Some(&announce.title), &announce.id),
            );
            let entry = Entry {
                key: item.key.clone(),
                id: announce.id.clone(),
                title: clean_text(Some(&announce.title), &announce.id),
                size: announce.size,
                sha256: announce.sha256.clone(),
                local,
                cached_bytes: cached,
                private_entry,
                epoch: announce.epoch.unwrap_or(0),
                locked,
                source: item.source.clone(),
                source_kind: item.source_kind.clone(),
                season: info.season.map(u64::from),
                episode: info.episode.map(u64::from),
                series_key: info.series_key.clone(),
                base_title: info.base_title.clone(),
                year: info.year.clone(),
                stream_url: self.blobs.stream_url(&announce),
                magnet_url: format_watch_uri(&watch_entry(&announce), &item.source, "channel"),
            };
            let dedupe = format!("{}/{}", entry.id, entry.sha256);
            match positions.get(&dedupe).copied() {
                Some(index) => {
                    let existing = &mut files[index];
                    if better(&entry, existing) {
                        *existing = entry;
                    }
                }
                None => {
                    positions.insert(dedupe, files.len());
                    files.push(entry);
                }
            }
        }

        let mut list = files;
        let query = options
            .query
            .clone()
            .flatten()
            .unwrap_or_default()
            .trim()
            .to_lowercase();
        if !query.is_empty() {
            list.retain(|entry| {
                entry.title.to_lowercase().contains(&query)
                    || entry.id.to_lowercase().contains(&query)
            });
        }
        let offset = options
            .offset
            .as_ref()
            .and_then(|number| number.as_u64())
            .unwrap_or(0) as usize;
        let limit = options
            .limit
            .as_ref()
            .and_then(|number| number.as_u64())
            .unwrap_or(0) as usize;
        if offset > 0 || limit > 0 {
            list = if limit > 0 {
                list.into_iter().skip(offset).take(limit).collect()
            } else {
                list.into_iter().skip(offset).collect()
            };
        }
        list
    }

    /// `findEntry(key)`: one record, with the shared `channel:<hex>/<sub>` form.
    pub async fn find_entry(&self, key: &str) -> Option<Value> {
        if let Some((entry, _, _)) = lock(&self.ephemeral).get(key) {
            return Some(entry.clone());
        }
        if let Some((channel, sub)) = split_key(key) {
            if channel == self.channel.primary_key() {
                self.channel.applied().await;
                let op = self.channel.view_get_timeout(sub.as_bytes()).await;
                if let Some(announce) = op.as_ref().and_then(announce_from) {
                    let _ = self.blobs.resolve_video_key(&announce);
                }
                return op;
            }
            let record = self.sources.get(&channel)?;
            if !record.active()
                && record.hydrated()
                && let Some(entry) = record.entries().into_iter().find(|item| item.sub == sub)
            {
                if let Some(announce) = announce_from(&entry.entry) {
                    let _ = self.blobs.resolve_video_key(&announce);
                }
                return Some(entry.entry);
            }
            let bee = self.sources.wake(&channel, true).await?;
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), bee.update()).await;
            let node = bee.view.view().get(sub.as_bytes()).await.ok().flatten();
            let op = node.and_then(|entry| decode_op(entry.value));
            if let Some(announce) = op.as_ref().and_then(announce_from) {
                let _ = self.blobs.resolve_video_key(&announce);
            }
            return op;
        }
        self.channel.applied().await;
        let op = self.channel.view_get_timeout(key.as_bytes()).await;
        if let Some(announce) = op.as_ref().and_then(announce_from) {
            let _ = self.blobs.resolve_video_key(&announce);
        }
        op
    }

    /// `openUri(uri)`. The Rust API keeps the caller's `joinSource` flag for its own callers
    /// (cast, the app and the relay HTTP routes), but the reference no longer reads it: a watch
    /// link joins the channel's swarm and never follows it unless the node private-routes.
    pub async fn open_uri(&self, uri: &str, _join_source: bool) -> Result<OpenUri, String> {
        let parsed = parse_uri(uri).map_err(|error| error.to_string())?;
        match parsed {
            ParsedUri::Receiver { .. } => {
                Err("A TV receiver link pairs through casting, not the catalog".into())
            }
            ParsedUri::Access { channel, epoch, .. } => {
                let source = self
                    .sources
                    .add(super::api::SourceSpec::Uri(uri.to_owned()), true)
                    .await?;
                Ok(OpenUri::Access {
                    channel,
                    epoch,
                    source,
                })
            }
            ParsedUri::Source { key, .. } => {
                let source = self
                    .sources
                    .add(
                        super::api::SourceSpec::Object(super::api::SourceObject {
                            uri: Some(uri.to_owned()),
                            key: Some(key),
                            kind: Some("channel".into()),
                            alias: None,
                        }),
                        true,
                    )
                    .await?;
                Ok(OpenUri::Source { source })
            }
            ParsedUri::Watch {
                source_key,
                entry: parsed_entry,
                ..
            } => {
                // A watch link's channel is where the video's peers are. Private routing only
                // reaches the channels it follows; otherwise joining the channel's swarm is
                // enough, and the link does not add the channel to the library.
                if let Some(key) = &source_key {
                    if self.blobs.private_routing() {
                        let _ = self
                            .sources
                            .add(
                                super::api::SourceSpec::Object(super::api::SourceObject {
                                    uri: None,
                                    key: Some(key.clone()),
                                    kind: Some("channel".into()),
                                    alias: None,
                                }),
                                true,
                            )
                            .await;
                    } else if let Some(source) = parse_hex32(key) {
                        let held = {
                            let mut links =
                                self.link_swarms.lock().unwrap_or_else(|e| e.into_inner());
                            if links.iter().any(|held| held == key) {
                                None
                            } else {
                                links.push(key.clone());
                                if links.len() > 4 {
                                    Some(links.remove(0))
                                } else {
                                    None
                                }
                            }
                        };
                        self.blobs.join_source_swarm(&source);
                        if let Some(dropped) = held.and_then(|dropped| parse_hex32(&dropped)) {
                            self.blobs.leave_source_swarm(&dropped);
                        }
                    }
                }
                let announce = Announce {
                    kind: "announce".to_owned(),
                    id: parsed_entry.id.clone(),
                    title: parsed_entry.title.clone(),
                    size: parsed_entry.size,
                    sha256: parsed_entry.sha256.clone(),
                    blobs: parsed_entry.blobs.clone(),
                    blob: Blob {
                        block_offset: parsed_entry.blob.block_offset,
                        block_length: parsed_entry.blob.block_length,
                        byte_offset: parsed_entry.blob.byte_offset,
                        byte_length: parsed_entry.blob.byte_length,
                        extra: serde_json::Map::new(),
                    },
                    epoch: None,
                    wrapped_key: None,
                    extra: serde_json::Map::new(),
                };
                let key = format!("watch/{}:{}", announce.blobs, announce.blob.block_offset);
                let source = source_key.clone().unwrap_or_else(|| "direct".to_owned());
                lock(&self.ephemeral).insert(
                    key.clone(),
                    (
                        serde_json::to_value(&parsed_entry).unwrap_or(Value::Null),
                        source.clone(),
                        "watch".to_owned(),
                    ),
                );
                self.blobs.warm(&announce).await;
                self.channel.changed();
                let blob = parse_hex32(&announce.blobs).unwrap_or_default();
                let cached = self.blobs.entry_cached_bytes(&blob, &announce).await;
                let info = describe_entry(
                    &announce.id,
                    &clean_text(Some(&announce.title), &announce.id),
                );
                let magnet_url = format_watch_uri(&watch_entry(&announce), &source, "channel");
                Ok(OpenUri::Watch {
                    entry: Entry {
                        key,
                        id: announce.id.clone(),
                        title: clean_text(Some(&announce.title), &announce.id),
                        size: announce.size,
                        sha256: announce.sha256.clone(),
                        local: self.blobs.owned(&blob),
                        cached_bytes: cached,
                        private_entry: false,
                        epoch: 0,
                        locked: false,
                        source,
                        source_kind: "watch".to_owned(),
                        season: info.season.map(u64::from),
                        episode: info.episode.map(u64::from),
                        series_key: info.series_key.clone(),
                        base_title: info.base_title.clone(),
                        year: info.year.clone(),
                        stream_url: self.blobs.stream_url(&announce),
                        magnet_url,
                    },
                })
            }
        }
    }

    /// `discoveredSources()`.
    pub async fn discovered_sources(&self) -> Vec<super::api::DiscoveredSource> {
        if self.channel.use_primary() || self.channel.has_entries() {
            self.refresh_primary(false).await;
        }
        let primary = lock(&self.primary).discovered.clone();
        let active: Vec<String> = self
            .sources
            .list(None)
            .into_iter()
            .map(|source| source.key)
            .collect();
        self.sources.discovered(primary, active).await
    }

    /// The direct watch links the catalog keeps in memory.
    pub fn ephemeral_announces(&self) -> Vec<Announce> {
        lock(&self.ephemeral)
            .values()
            .filter_map(|(entry, _, _)| announce_from(entry))
            .collect()
    }
}

/// `announce` records decode into the API type.
fn announce_from(value: &Value) -> Option<Announce> {
    if value.get("type").and_then(Value::as_str) != Some("announce") {
        return None;
    }
    serde_json::from_value(value.clone()).ok()
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

/// `key.match(/^(?:channel|tracker):([0-9a-f]{64})\/(.+)$/)`.
fn split_key(key: &str) -> Option<(String, String)> {
    let rest = key
        .strip_prefix("channel:")
        .or_else(|| key.strip_prefix("tracker:"))?;
    let (channel, sub) = rest.split_once('/')?;
    if !crate::protocol::is_hex64(channel) || sub.is_empty() {
        return None;
    }
    Some((channel.to_owned(), sub.to_owned()))
}

/// JS ranking: unlocked, local, cached bytes, a `:0` suffix, same-source parent.
fn rank(entry: &Entry) -> (u8, u8, u64, u8, u8) {
    let parts: Vec<&str> = entry.key.split('/').collect();
    let same_source = parts
        .len()
        .checked_sub(2)
        .map(|index| parts[index] == entry.source)
        .unwrap_or(false);
    (
        u8::from(!entry.locked),
        u8::from(entry.local),
        entry.cached_bytes,
        u8::from(entry.key.ends_with(":0")),
        u8::from(same_source),
    )
}

fn better(a: &Entry, b: &Entry) -> bool {
    rank(a) > rank(b)
}

impl Raw {
    fn entry_id(&self) -> String {
        self.entry
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    }
}
