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
use hyperbee::{Bee, RangeOptions};
use serde_json::Value;

use super::api::{Announce, Blob, Entry, OpenUri, SearchOptions};
use super::channel::Channel;
use super::sources::{Item, Sources};
use crate::protocol::{
    BlobPointer, ParsedUri, WatchEntry, clean_text, decode_op, describe_entry, format_watch_uri,
    is_system_view_key, parse_uri,
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
    fn stored(&self, discovery: &[u8; 32]) -> bool;
    fn open_core_session(&self, blob: &[u8; 32]) -> Option<Arc<hypercore::Session>>;
    fn probe<'a>(&'a self, blob: &'a [u8; 32]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
    fn entry_cached_bytes<'a>(
        &'a self,
        blob: &'a [u8; 32],
        entry: &'a Announce,
    ) -> Pin<Box<dyn Future<Output = u64> + Send + 'a>>;
    fn resolve_video_key(&self, entry: &Announce) -> bool;
    fn join_content(&self, blob: &[u8; 32], evict: bool);
    fn stream_url(&self, entry: &Announce) -> String;
    fn warm<'a>(&'a self, entry: &'a Announce) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

/// The node's own channel snapshot.
#[derive(Default)]
struct PrimaryCache {
    entries: Vec<Item>,
    discovered: Vec<super::api::DiscoveredSource>,
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
        })
    }

    async fn scan(&self, view: &Bee, via: &str) -> (Vec<Item>, Vec<super::api::DiscoveredSource>) {
        let mut entries = Vec::new();
        let mut discovered = Vec::new();
        let mut stream = view.range(RangeOptions::default());
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
                    discovered.push(super::api::DiscoveredSource {
                        key: source_key.clone(),
                        kind: "channel".to_owned(),
                        title: clean_text(
                            op.get("title").and_then(Value::as_str),
                            &source_key[..source_key.len().min(12)],
                        ),
                        via: via.to_owned(),
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
        let (entries, discovered) = self
            .scan(self.channel.view(), &self.channel.primary_key())
            .await;
        let mut primary = lock(&self.primary);
        primary.entries = entries;
        primary.discovered = discovered;
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
                if id.is_some() {
                    self.channel.applied().await;
                } else {
                    self.refresh_primary(false).await;
                }
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
            // An id lookup reads the applied view directly: the cache only holds what a full
            // listing walked, and a fresh follower has not walked anything yet.
            let items = if id.is_some() {
                let (entries, _) = self
                    .scan(self.channel.view(), &self.channel.primary_key())
                    .await;
                entries
            } else {
                lock(&self.primary).entries.clone()
            };
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

        // Only probe cores whose discovery key is stored and not known absent.
        let mut to_probe: Vec<[u8; 32]> = Vec::new();
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
            if self.blobs.slot_contains(&blob)
                || self.blobs.open_core(&blob)
                || self.blobs.cached_meta(&blob).is_some()
            {
                continue;
            }
            let discovery = self.blobs.blob_disc(&blob);
            if !self.blobs.stored(&discovery) || self.blobs.absent(&discovery) {
                continue;
            }
            to_probe.push(blob);
        }
        for chunk in to_probe.chunks(16) {
            join_all(chunk.iter().map(|blob| self.blobs.probe(blob))).await;
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
            if cached > 0 && !owned && !self.blobs.owned(&blob) {
                self.blobs.join_content(&blob, false);
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
                magnet_url: format_watch_uri(&watch_entry(&announce), "", ""),
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

    /// `openUri(uri, { joinSource })`.
    pub async fn open_uri(&self, uri: &str, join_source: bool) -> Result<OpenUri, String> {
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
                if join_source && let Some(key) = &source_key {
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
                        magnet_url: format_watch_uri(&watch_entry(&announce), "", ""),
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
