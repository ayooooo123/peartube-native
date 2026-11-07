//! `src/node/channel.js`: the unified Channel trackers (Autobee) and their view reads.
//!
//! One Autobee per channel, with `apply.rs`'s ACL as its handlers. Reads go through the applied
//! view; writes append an op and wait until the local core is durable. Followed channels open
//! their own Autobee under a `channel:<key>` namespace and share the node's wakeup discipline.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use autobee::apply_view::{Handlers, WakeupCapability};
use autobee::types::Key;
use autobee::updates::Changes;
use autobee::Autobee;
use bytes::Bytes;
use corestore::{Corestore, GetOptions as StoreGetOptions};
use hyperbee::{Bee, RangeOptions};
use hypercore::crypto::KeyPair;
use hypercore::core::Session;
use serde_json::Value;

use super::apply::Apply;
use super::blobs::{ChannelView, SendFuture};
use crate::protocol::{clean_text, decode_op, hex_encode, is_hex64, is_system_view_key};

/// A read waits at most this long for the view to apply (channel.js `applied`).
const APPLIED_TIMEOUT: Duration = Duration::from_secs(1);
/// A write polls the local core at this cadence until it is durable.
const WRITE_POLL: Duration = Duration::from_millis(500);
/// The roles a grant may name.
const ROLES: [&str; 3] = ["owner", "mod", "writer"];

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

/// Channel errors keep the JS message where the message is the contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelError(pub String);

impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ChannelError {}

impl From<Box<dyn std::error::Error + Send + Sync>> for ChannelError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self(error.to_string())
    }
}

impl From<autobee::boot::Error> for ChannelError {
    fn from(error: autobee::boot::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<corestore::Error> for ChannelError {
    fn from(error: corestore::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<hyperbee::Error> for ChannelError {
    fn from(error: hyperbee::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<std::io::Error> for ChannelError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

/// What a channel write needs from the identity (the vault), without owning it.
pub trait AppendAuthority: Send + Sync {
    /// `ctx.identity.idVault.channelKey`, if there is a vault.
    fn vault_channel_key(&self) -> Option<String>;
    /// `ctx.identity.idVault.ownerPublicKey`.
    fn owner_public_key(&self) -> Option<String>;
    /// `ctx.identity.idVault.signClaim(primaryKey, writerHex)`.
    fn sign_claim(&self, channel: &str, writer: &str) -> Option<String>;
}

/// Everything the channel reaches outside itself.
#[derive(Default)]
pub struct Hooks {
    /// `ctx.update()` after a change.
    pub update: Option<Arc<dyn Fn() + Send + Sync>>,
    /// `ctx.net.joinOverlayTopic(...)` on a write.
    pub join_topic: Option<Arc<dyn Fn(&str) + Send + Sync>>,
}

/// The apply handlers plus the founder key pair, which `Apply` itself does not carry.
struct ChannelHandlers {
    apply: Apply,
    key_pair: Option<KeyPair>,
}

impl Handlers for ChannelHandlers {
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [autobee::types::Node],
        view: &'a Bee,
        host: &'a autobee::apply_calls::ApplyCalls<'a, 'b>,
    ) -> autobee::apply_calls::BoxFuture<'a, autobee::apply_calls::Result<()>> {
        self.apply.apply(nodes, view, host)
    }

    fn is_trusted<'a>(
        &'a self,
        key: &'a Key,
        reference: Option<&'a Bee>,
    ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
        self.apply.is_trusted(key, reference)
    }

    fn update<'a>(
        &'a self,
        view: &'a Bee,
        changes: &'a Changes,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        self.apply.update(view, changes)
    }

    fn has_update(&self) -> bool {
        self.apply.has_update()
    }

    fn key_pair(&self) -> Option<KeyPair> {
        self.key_pair.clone()
    }

    fn wakeup_capability(&self) -> Option<WakeupCapability> {
        None
    }
}

/// A unified Channel tracker.
pub struct Channel {
    auto: Arc<Autobee>,
    primary_key: String,
    primary_buf: [u8; 32],
    writer_cores: Mutex<HashMap<String, Session>>,
    secondary_writers: Mutex<HashSet<String>>,
    use_primary: AtomicBool,
    has_entries: AtomicBool,
    authority: Mutex<Option<Arc<dyn AppendAuthority>>>,
    hooks: Hooks,
    tracker: Corestore,
    closing: Arc<AtomicBool>,
    worklet: bool,
}

impl Channel {
    /// Boots the tracker (`new Autobee(chNs, keyOrNull, opts)`).
    ///
    /// `primary` is the channel key resolved before the store opens (the target key, the founder
    /// key pair's public key, or the stored local core's key).
    pub async fn open(
        tracker: Corestore,
        target: Option<[u8; 32]>,
        primary: [u8; 32],
        founder: Option<KeyPair>,
        apply: Apply,
        hooks: Hooks,
        closing: Arc<AtomicBool>,
        worklet: bool,
    ) -> Result<Arc<Self>, ChannelError> {
        let store = tracker.clone();
        let handlers = Arc::new(ChannelHandlers {
            apply,
            key_pair: founder,
        });
        let auto = Autobee::open(&tracker, target, handlers).await?;
        let primary_key = hex_encode(&primary);
        Ok(Arc::new(Self {
            auto,
            primary_key,
            primary_buf: primary,
            writer_cores: Mutex::new(HashMap::new()),
            secondary_writers: Mutex::new(HashSet::new()),
            use_primary: AtomicBool::new(target.is_some()),
            has_entries: AtomicBool::new(false),
            authority: Mutex::new(None),
            hooks,
            tracker: store,
            closing,
            worklet,
        }))
    }

    /// Installs the vault-backed authority once identity exists.
    pub fn set_authority(&self, authority: Arc<dyn AppendAuthority>) {
        *lock(&self.authority) = Some(authority);
    }

    pub fn auto(&self) -> &Arc<Autobee> {
        &self.auto
    }

    pub fn primary_key(&self) -> String {
        self.primary_key.clone()
    }

    pub fn primary_buf(&self) -> [u8; 32] {
        self.primary_buf
    }

    pub fn key(&self) -> [u8; 32] {
        self.auto.key
    }

    pub fn writable(&self) -> bool {
        self.auto.bootstrap.writable()
    }

    pub fn local_key(&self) -> String {
        hex_encode(self.auto.local.key())
    }

    pub fn use_primary(&self) -> bool {
        self.use_primary.load(Ordering::Acquire)
    }

    pub fn mark_use_primary(&self) {
        self.use_primary.store(true, Ordering::Release);
    }

    /// `usePrimaryChannel = ...` at boot.
    pub fn set_use_primary(&self, value: bool) {
        self.use_primary.store(value, Ordering::Release);
    }

    /// `channel.local.length`: the local writer's oplog length.
    pub fn local_length(&self) -> u64 {
        self.auto.local.length()
    }

    /// `ctx.life.closing`.
    pub fn closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    pub fn has_entries(&self) -> bool {
        self.has_entries.load(Ordering::Acquire)
    }

    pub fn mark_has_entries(&self) {
        self.has_entries.store(true, Ordering::Release);
    }

    pub fn secondary_writers(&self) -> Vec<String> {
        lock(&self.secondary_writers).iter().cloned().collect()
    }

    /// `onWriterChange(wHex, added)`.
    pub fn writer_changed(&self, writer: &str, added: bool) {
        if writer == self.primary_key {
            return;
        }
        let mut writers = lock(&self.secondary_writers);
        if added {
            writers.insert(writer.to_owned());
        } else {
            writers.remove(writer);
        }
    }

    /// `applied(bee)`: race the update against a one-second cap.
    pub async fn applied(&self) {
        let update = self.auto.update();
        let _ = tokio::time::timeout(APPLIED_TIMEOUT, update).await;
    }

    /// `syncWriterCores`: hold an inactive core per secondary writer.
    pub async fn sync_writer_cores(&self) {
        let wanted: HashSet<String> = lock(&self.secondary_writers).clone();
        let stale: Vec<Session> = {
            let mut cores = lock(&self.writer_cores);
            let stale: Vec<String> = cores
                .keys()
                .filter(|key| !wanted.contains(*key))
                .cloned()
                .collect();
            stale
                .into_iter()
                .filter_map(|hex| cores.remove(&hex))
                .collect()
        };
        for core in stale {
            core.close();
        }
        for hex in wanted {
            if lock(&self.writer_cores).contains_key(&hex) {
                continue;
            }
            let Some(key) = parse_hex32(&hex) else {
                continue;
            };
            let mut opts = StoreGetOptions::key(key);
            opts.inactive = true;
            if let Ok(session) = self.tracker.get(opts).await {
                lock(&self.writer_cores).insert(hex, session);
            }
        }
    }

    /// The applied view as a hyperbee.
    pub fn view(&self) -> &Bee {
        self.auto.view.view()
    }

    /// `view.get`: one decoded op.
    pub async fn view_get(&self, key: impl Into<Bytes>) -> Option<Value> {
        let entry = self.view().get(key.into().as_ref()).await.ok()??;
        decode_op(entry.value)
    }

    /// `view.get` under a write timeout so a read never hangs on a sparse record.
    pub async fn view_get_timeout(&self, key: &[u8]) -> Option<Value> {
        match tokio::time::timeout(Duration::from_secs(5), self.view().get(key)).await {
            Ok(Ok(Some(entry))) => decode_op(entry.value),
            _ => None,
        }
    }

    /// Every non-system view key and value.
    pub async fn view_entries(&self) -> Result<Vec<(String, Bytes)>, ChannelError> {
        let mut stream = self.view().range(RangeOptions::default());
        let mut out = Vec::new();
        while let Some(entry) = stream.next().await? {
            let key = String::from_utf8_lossy(&entry.key).into_owned();
            out.push((key, entry.value));
        }
        Ok(out)
    }

    /// A prefix range (`gte: prefix`, `lt: prefix\xff`).
    pub async fn view_prefix(&self, prefix: &str) -> Result<Vec<(String, Bytes)>, ChannelError> {
        let mut stream = self.view().range(RangeOptions {
            gte: Some(Bytes::from(prefix.as_bytes().to_vec())),
            lt: Some(Bytes::from([prefix.as_bytes(), &[0xff]].concat())),
            ..RangeOptions::default()
        });
        let mut out = Vec::new();
        while let Some(entry) = stream.next().await? {
            out.push((String::from_utf8_lossy(&entry.key).into_owned(), entry.value));
        }
        Ok(out)
    }

    /// `currentChannelEpoch()`.
    pub async fn current_epoch(&self) -> u64 {
        self.applied().await;
        self.view_get_timeout(b"meta/epoch")
            .await
            .and_then(|value| match value {
                Value::Number(number) => number.as_u64(),
                other => Some(crate::app_settings::js_number(&crate::app_settings::js_string(&other)) as u64),
            })
            .unwrap_or(1)
    }

    /// `localRole()`: owner, the granted role, or `None`.
    pub async fn local_role(&self) -> Option<String> {
        let writer = self.local_key();
        if writer == self.primary_key {
            return Some("owner".to_owned());
        }
        self.applied().await;
        if let Some(value) = self
            .view_get_timeout(format!("writer/{writer}").as_bytes())
            .await
            && let Some(role) = value.get("role").and_then(Value::as_str)
            && !role.is_empty()
        {
            return Some(role.to_owned());
        }
        let authority = lock(&self.authority).clone();
        let Some(authority) = authority else {
            return None;
        };
        if authority.vault_channel_key().as_deref() == Some(self.primary_key.as_str()) {
            return Some("owner".to_owned());
        }
        let owner = self.view_get_timeout(b"meta/owner").await?;
        let owner_key = crate::app_settings::js_string(&owner);
        (authority.owner_public_key().as_deref() == Some(owner_key.as_str())).then(|| "owner".to_owned())
    }

    /// `listWriters()`.
    pub async fn list_writers(&self) -> Vec<Value> {
        self.applied().await;
        let self_hex = self.local_key();
        let mut out: Vec<Value> = Vec::new();
        out.push(serde_json::json!({
            "key": self.primary_key,
            "role": "owner",
            "alias": "primary",
            "self": self.primary_key == self_hex,
        }));
        let mut seen: HashSet<String> = HashSet::new();
        if let Ok(entries) = self.view_prefix("writer/").await {
            for (full, value) in entries {
                let key = full.get("writer/".len()..).unwrap_or_default().to_owned();
                if !seen.insert(key.clone()) {
                    continue;
                }
                let meta: Value = decode_op(value).unwrap_or(Value::Null);
                let role = meta
                    .get("role")
                    .and_then(Value::as_str)
                    .unwrap_or("writer")
                    .to_owned();
                let alias = clean_text(meta.get("alias").and_then(Value::as_str), "");
                out.push(serde_json::json!({
                    "key": key,
                    "role": role,
                    "alias": alias,
                    "self": key == self_hex,
                }));
            }
        }
        out
    }

    /// `append(op)`: durable, with the claim a linked device needs.
    pub async fn append(&self, op: Value) -> Result<(), ChannelError> {
        self.use_primary.store(true, Ordering::Release);
        match op.get("type").and_then(Value::as_str) {
            Some("announce") | Some("source") => self.mark_has_entries(),
            _ => {}
        }
        if let Some(join) = &self.hooks.join_topic {
            join(&format!("channel:{}", self.primary_key));
        }
        let writer_hex = self.local_key();
        let authority = lock(&self.authority).clone();
        let writable = self.writable();
        let can_claim = match &authority {
            Some(authority) => {
                let mut claim = authority.vault_channel_key().as_deref()
                    == Some(self.primary_key.as_str());
                if !claim
                    && let Some(owner) = self.view_get_timeout(b"meta/owner").await
                {
                    claim = authority.owner_public_key().as_deref()
                        == Some(crate::app_settings::js_string(&owner).as_str());
                }
                claim
            }
            None => false,
        };
        let payload = if !writable
            && can_claim
            && op.get("type").and_then(Value::as_str) != Some("claim")
        {
            match authority.as_ref().and_then(|authority| {
                authority.sign_claim(&self.primary_key, &writer_hex)
            }) {
                Some(claim) => {
                    let mut map = op;
                    if let Value::Object(object) = &mut map {
                        object.insert("claim".to_owned(), Value::String(claim));
                    }
                    map
                }
                None => op,
            }
        } else {
            op
        };
        let text = serde_json::to_string(&payload).map_err(|error| ChannelError(error.to_string()))?;
        let before = self.auto.local.length();
        self.auto
            .append(vec![Bytes::from(text.into_bytes())], !writable)
            .await
            .map_err(|error| ChannelError(error.to_string()))?;
        self.auto
            .updated()
            .await
            .map_err(|error| ChannelError(error.to_string()))?;
        loop {
            if self.auto.local.length() > before {
                break;
            }
            if self.closing.load(Ordering::Acquire) {
                return Err(ChannelError("Node closed before the write was durable".into()));
            }
            tokio::time::sleep(WRITE_POLL).await;
            let _ = self.auto.update().await;
        }
        self.sync_writer_cores().await;
        Ok(())
    }

    /// `publishSource({ sourceKey, title })`.
    pub async fn publish_source(&self, source_key: &str, title: &str) -> Result<Value, ChannelError> {
        let source_key = source_key.to_lowercase();
        let op = serde_json::json!({
            "type": "source",
            "sourceKey": source_key,
            "kind": "channel",
            "title": clean_text(Some(title), &source_key[..source_key.len().min(12)]),
        });
        self.append(op.clone()).await?;
        self.changed();
        Ok(op)
    }

    /// `unpublishSource(sourceKey)`.
    pub async fn unpublish_source(&self, source_key: &str) -> Result<Value, ChannelError> {
        let key = source_key.trim().to_lowercase();
        if !is_hex64(&key) {
            return Err(ChannelError("Invalid source key".into()));
        }
        let _ = self.auto.update().await;
        let writer = self.local_key();
        let owner = self.local_role().await.as_deref() == Some("owner");
        if let Ok(entries) = self.view_prefix("source/").await {
            for (full, _) in entries {
                if full.ends_with(&format!("/{key}"))
                    && (full == format!("source/{writer}/{key}") || owner)
                {
                    self.append(serde_json::json!({ "type": "remove", "key": full }))
                        .await?;
                }
            }
        }
        self.changed();
        Ok(serde_json::json!({ "unpublished": true, "key": key }))
    }

    /// `publishedSources()`.
    pub async fn published_sources(&self) -> Result<Vec<Value>, ChannelError> {
        if !self.use_primary() && !self.has_entries() {
            return Ok(Vec::new());
        }
        self.applied().await;
        let mut out: Vec<Value> = Vec::new();
        let mut seen = HashSet::new();
        if let Ok(entries) = self.view_prefix("source/").await {
            for (_, value) in entries {
                let Some(op) = decode_op(value) else {
                    continue;
                };
                if op.get("type").and_then(Value::as_str) != Some("source") {
                    continue;
                }
                let Some(source_key) = op.get("sourceKey").and_then(Value::as_str) else {
                    continue;
                };
                if !seen.insert(source_key.to_owned()) {
                    continue;
                }
                out.push(serde_json::json!({
                    "key": source_key,
                    "kind": "channel",
                    "title": clean_text(
                        op.get("title").and_then(Value::as_str),
                        &source_key[..source_key.len().min(12)],
                    ),
                }));
            }
        }
        Ok(out)
    }

    /// `grantWriter`.
    pub async fn grant_writer(
        &self,
        writer_key: &str,
        role: &str,
        alias: &str,
    ) -> Result<Value, ChannelError> {
        let writer_key = writer_key.trim().to_lowercase();
        if !is_hex64(&writer_key) {
            return Err(ChannelError("Writer key must be 64 hex characters".into()));
        }
        if !ROLES.contains(&role) {
            return Err(ChannelError("Role must be owner, mod, or writer".into()));
        }
        let alias = clean_text(Some(alias), "");
        self.append(serde_json::json!({
            "type": "grant",
            "writer": writer_key,
            "role": role,
            "alias": alias,
        }))
        .await?;
        Ok(serde_json::json!({
            "granted": true,
            "writer": writer_key,
            "role": role,
            "alias": alias,
        }))
    }

    /// `revokeWriter`.
    pub async fn revoke_writer(
        &self,
        writer_key: &str,
        purge_entries: bool,
    ) -> Result<Value, ChannelError> {
        let writer_key = writer_key.trim().to_lowercase();
        if !is_hex64(&writer_key) {
            return Err(ChannelError("Writer key must be 64 hex characters".into()));
        }
        if writer_key == self.primary_key {
            return Err(ChannelError(
                "Cannot revoke the genesis channel founder".into(),
            ));
        }
        self.append(serde_json::json!({
            "type": "revoke",
            "writer": writer_key,
            "purgeEntries": purge_entries,
        }))
        .await?;
        self.changed();
        Ok(serde_json::json!({
            "revoked": true,
            "writer": writer_key,
            "purgeEntries": purge_entries,
        }))
    }

    /// `ctx.update()`: the view changed.
    pub fn changed(&self) {
        if let Some(update) = &self.hooks.update {
            update();
        }
    }

    /// The view's decoded announce/source records.
    pub async fn records(&self) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        if let Ok(entries) = self.view_entries().await {
            for (key, value) in entries {
                if is_system_view_key(&key) {
                    continue;
                }
                if let Some(op) = decode_op(value) {
                    out.push((key, op));
                }
            }
        }
        out
    }

    /// `await (isWorklet ? applied(channel) : channel.update())` when the primary is in use.
    pub async fn prime(&self) {
        if self.use_primary() {
            if self.worklet {
                self.applied().await;
            } else {
                let _ = self.auto.update().await;
            }
        }
    }

    /// Closes the tracker (`channel.close()`).
    pub async fn close(&self) {
        let cores = std::mem::take(&mut *lock(&self.writer_cores));
        for (_, core) in cores {
            core.close();
        }
        let _ = self.auto.close().await;
    }

    /// The channel's seedable core record, for HiveRelays.
    pub fn bootstrap_len(&self) -> (u64, u64) {
        (self.auto.bootstrap.length(), self.auto.bootstrap.byte_length())
    }

    pub fn writer_core_len(&self, hex: &str) -> Option<(u64, u64)> {
        lock(&self.writer_cores)
            .get(hex)
            .map(|core| (core.length(), core.byte_length()))
    }

    pub fn writer_core_keys(&self) -> Vec<String> {
        lock(&self.writer_cores).keys().cloned().collect()
    }
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

/// The channel handle as the blob store's `ChannelView`.
impl ChannelView for Arc<Channel> {
    type Error = ChannelError;

    fn local_writer(&self) -> [u8; 32] {
        Channel::local_writer(self)
    }

    fn primary_key(&self) -> [u8; 32] {
        Channel::primary_buf(self)
    }

    fn current_epoch(&self) -> SendFuture<'_, u64> {
        Box::pin(Channel::current_epoch(self))
    }

    fn local_role(&self) -> SendFuture<'_, String> {
        Box::pin(async move { Channel::local_role(self).await.unwrap_or_default() })
    }

    fn update(&self) -> SendFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            Channel::applied(self).await;
            Ok(())
        })
    }

    fn entries(&self) -> SendFuture<'_, Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>> {
        Box::pin(async move {
            let mut stream = self.view().range(RangeOptions::default());
            let mut out = Vec::new();
            while let Some(entry) = stream.next().await? {
                out.push((entry.key.to_vec(), entry.value.to_vec()));
            }
            Ok(out)
        })
    }

    fn append(&self, op: &Value) -> SendFuture<'_, Result<(), Self::Error>> {
        let op = op.clone();
        Box::pin(async move { Channel::append(self, op).await })
    }

    fn add_secondary_writer(&self, writer_hex: &str) {
        self.writer_changed(writer_hex, true);
    }

    fn mark_has_entries(&self) {
        Channel::mark_has_entries(self);
    }

    fn sync_writer_cores(&self) -> SendFuture<'_, ()> {
        Box::pin(Channel::sync_writer_cores(self))
    }
}

/// The channel as the blob store's `ChannelView`.
impl ChannelView for Channel {
    type Error = ChannelError;

    fn local_writer(&self) -> [u8; 32] {
        *self.auto.local.key()
    }

    fn primary_key(&self) -> [u8; 32] {
        self.primary_buf
    }

    fn current_epoch(&self) -> SendFuture<'_, u64> {
        Box::pin(Channel::current_epoch(self))
    }

    fn local_role(&self) -> SendFuture<'_, String> {
        Box::pin(async move { Channel::local_role(self).await.unwrap_or_default() })
    }

    fn update(&self) -> SendFuture<'_, Result<(), Self::Error>> {
        Box::pin(async move {
            Channel::applied(self).await;
            Ok(())
        })
    }

    fn entries(&self) -> SendFuture<'_, Result<Vec<(Vec<u8>, Vec<u8>)>, Self::Error>> {
        Box::pin(async move {
            let mut stream = self.view().range(RangeOptions::default());
            let mut out = Vec::new();
            while let Some(entry) = stream.next().await? {
                out.push((entry.key.to_vec(), entry.value.to_vec()));
            }
            Ok(out)
        })
    }

    fn append(&self, op: &Value) -> SendFuture<'_, Result<(), Self::Error>> {
        let op = op.clone();
        Box::pin(async move { Channel::append(self, op).await })
    }

    fn add_secondary_writer(&self, writer_hex: &str) {
        self.writer_changed(writer_hex, true);
    }

    fn mark_has_entries(&self) {
        Channel::mark_has_entries(self);
    }

    fn sync_writer_cores(&self) -> SendFuture<'_, ()> {
        Box::pin(Channel::sync_writer_cores(self))
    }
}
