//! `src/apply.js`: the deterministic ACL gate at the mouth of every unified Channel, plus the
//! `isTrusted` / `update` handlers `src/node/channel.js` wires beside it. Ungranted strangers are
//! swallowed, linked devices prove ownership with detached 12-word vault claims or owner grants,
//! and owners/mods can purge any key.
//!
//! Every view key and value written here becomes a hyperbee value, so the record JSON is built
//! byte-for-byte as `JSON.stringify` would: `{"role":…,"alias":…}` in that order.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use autobee::apply_calls::{AddWriterOptions, ApplyCalls, BoxFuture, Error, Result};
use autobee::apply_view::Handlers;
use autobee::types::{Key, Node};
use autobee::updates::Changes;
use bytes::Bytes;
use hyperbee::{Bee, RangeOptions, ReadOptions};
use serde_json::{Map, Value};

use crate::mnemonic::verify_claim;
use crate::protocol::{clean_text, decode_op, hex_encode, is_system_view_key, js_number};

/// `handlers.isTrusted` reads the referenced view with this timeout: a record nobody serves counts
/// as untrusted rather than wedging the fast-forward search.
const REFERENCE_TIMEOUT: Duration = Duration::from_millis(5_000);

/// A buffered view op; `tryPut` / `tryDelete` become `put` / `del` on the flush.
enum Op {
    Put(Bytes, Bytes),
    Del(Bytes),
}

fn writer_record(role: &str, alias: &str) -> Bytes {
    let mut record = Map::new();
    record.insert("role".to_string(), Value::String(role.to_string()));
    record.insert("alias".to_string(), Value::String(alias.to_string()));
    Bytes::from(serde_json::to_vec(&Value::Object(record)).expect("writer record serializes"))
}

fn host_error(error: hyperbee::Error) -> Error {
    Error::Host(Box::new(error))
}

/// JavaScript truthiness of a value apply.js reads as `if (x)`, where `x` is a JSON field.
fn truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64() != Some(0.0),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(_) | Value::Object(_)) => true,
    }
}

/// `if (role)` for a role read from the view: an empty string marks a known stranger.
fn role_known(role: &Option<String>) -> bool {
    role.as_deref().is_some_and(|role| !role.is_empty())
}

/// `String(x || '')` for an untrusted JSON value: only strings and non-zero numbers survive
/// beyond the falsy check, and an object stringifies as JS does.
fn js_text(value: Option<&Value>) -> String {
    if !truthy(value) {
        return String::new();
    }
    match value.unwrap() {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_text(Some(other)),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_string(),
        Value::Null => String::new(),
    }
}

/// `key.split('/')[1]`.
fn second_segment(key: &str) -> Option<&str> {
    key.split('/').nth(1)
}

/// The `update` mirror from channel.js: a fast-forward landing downloads the landed system and view
/// cores whole, once each, and every update bumps the node. The store side is the caller's.
pub struct Mirror {
    landed: Mutex<HashSet<Key>>,
    land: Arc<dyn Fn(Key) + Send + Sync>,
    changed: Arc<dyn Fn() + Send + Sync>,
}

impl Mirror {
    /// `mirrorLanding(changes)` calls `land` for each newly landed core, then `changed` once.
    pub fn new(land: Arc<dyn Fn(Key) + Send + Sync>, changed: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self {
            landed: Mutex::new(HashSet::new()),
            land,
            changed,
        }
    }

    fn update(&self, changes: &Changes) {
        for name in ["system", "view"] {
            let Some(change) = changes.get(name) else {
                continue;
            };
            if change.incremental {
                continue;
            }
            let Some(key) = change.to.and_then(|to| to.key) else {
                continue;
            };
            if !self.landed.lock().unwrap().insert(key) {
                continue;
            }
            (self.land)(key);
        }
        (self.changed)();
    }
}

/// The channel's apply handler and its `isTrusted` / `update` companions.
pub struct Apply {
    channel: String,
    writer_change: Option<Arc<dyn Fn(&str, bool) + Send + Sync>>,
    mirror: Option<Mirror>,
}

impl Apply {
    /// `makeApply(getChannelHex)`; `getChannelHex` is resolved by the caller.
    pub fn new(channel: impl Into<String>) -> Self {
        Self {
            channel: channel.into(),
            writer_change: None,
            mirror: None,
        }
    }

    /// `onWriterChange`: tells the node which secondary writer cores to hold.
    pub fn on_writer_change(mut self, callback: Arc<dyn Fn(&str, bool) + Send + Sync>) -> Self {
        self.writer_change = Some(callback);
        self
    }

    /// channel.js's `update` handler. Without a mirror the handler supplies none.
    pub fn with_mirror(mut self, mirror: Mirror) -> Self {
        self.mirror = Some(mirror);
        self
    }
}

/// `isTrusted(key, reference)`: only the channel key and devices linked to it as owner (and not
/// revoked) may vouch for a view. The reference is this node's own view, sparse after a landing, so
/// a record nobody serves in time counts as untrusted.
async fn is_trusted(channel: &str, key: &Key, reference: Option<&Bee>) -> bool {
    let hex = hex_encode(key);
    if hex == channel {
        return true;
    }
    let Some(reference) = reference else {
        return false;
    };
    let read = |path: String| {
        let reference = reference.clone();
        async move {
            let options = ReadOptions {
                timeout: Some(REFERENCE_TIMEOUT),
                ..ReadOptions::default()
            };
            reference.get_with_options(path.as_bytes(), options).await
        }
    };
    if !matches!(read(format!("revoked/{hex}")).await, Ok(None)) {
        return false;
    }
    let Ok(Some(record)) = read(format!("writer/{hex}")).await else {
        return false;
    };
    let Ok(parsed) = serde_json::from_slice::<Value>(&record.value) else {
        return false;
    };
    parsed.get("role").and_then(Value::as_str) == Some("owner")
}

struct Context<'a, 'b> {
    channel: &'a str,
    view: &'a Bee,
    host: &'a ApplyCalls<'a, 'b>,
    writer_change: Option<&'a Arc<dyn Fn(&str, bool) + Send + Sync>>,
    ops: Vec<Op>,
    roles: HashMap<String, String>,
    revoked: HashMap<String, bool>,
    owner: Option<String>,
    epoch: Option<f64>,
}

impl<'a, 'b> Context<'a, 'b> {
    async fn flush(&mut self) -> Result<()> {
        if self.ops.is_empty() {
            return Ok(());
        }
        let mut batch = self.view.batch();
        for op in self.ops.drain(..) {
            match op {
                Op::Put(key, value) => batch.put(key, value),
                Op::Del(key) => batch.del(key),
            }
        }
        batch.flush().await.map_err(host_error)
    }

    async fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.view
            .get(key)
            .await
            .ok()
            .flatten()
            .map(|entry| entry.value)
    }

    async fn get_text(&self, key: &[u8]) -> String {
        self.get(key)
            .await
            .map(|value| String::from_utf8_lossy(&value).into_owned())
            .unwrap_or_default()
    }

    /// `getOwner`: the linked owner public key, cached for the batch.
    async fn get_owner(&mut self) -> String {
        if self.owner.is_none() {
            self.owner = Some(self.get_text(b"meta/owner").await);
        }
        self.owner.clone().unwrap_or_default()
    }

    async fn is_revoked(&mut self, writer: &str) -> bool {
        if let Some(revoked) = self.revoked.get(writer) {
            return *revoked;
        }
        let revoked = self
            .get(format!("revoked/{writer}").as_bytes())
            .await
            .is_some();
        self.revoked.insert(writer.to_string(), revoked);
        revoked
    }

    fn put(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) {
        self.ops.push(Op::Put(key.into(), value.into()));
    }

    fn del(&mut self, key: impl Into<Bytes>) {
        self.ops.push(Op::Del(key.into()));
    }

    async fn run(&mut self, nodes: &[Node]) -> Result<()> {
        for node in nodes {
            let value = node.value.clone().unwrap_or_default();
            let Some(op) = decode_op(&value) else {
                continue;
            };
            let writer = hex_encode(&node.key);
            let op_type = op.get("type").and_then(Value::as_str).unwrap_or_default();
            let mut role = if writer == self.channel {
                Some("owner".to_string())
            } else {
                self.roles.get(&writer).cloned()
            };
            let mut claimed_here = false;
            let mut legacy_granted_here = false;
            if !role_known(&role) {
                if !self.roles.contains_key(&writer) {
                    if let Some(record) = self.get(format!("writer/{writer}").as_bytes()).await {
                        if let Ok(parsed) = serde_json::from_slice::<Value>(&record) {
                            let role_here = parsed.get("role");
                            if truthy(role_here) {
                                role = Some(js_text(role_here));
                            }
                        }
                    }
                    if let Some(role_here) = role.clone() {
                        self.roles.insert(writer.clone(), role_here);
                    }
                }
            }
            if !role_known(&role) {
                let cur_owner = self.get_owner().await;
                let claim_sig = if op_type == "claim" {
                    op.get("sig")
                } else {
                    op.get("claim")
                };
                let claim_sig = claim_sig
                    .filter(|_| !cur_owner.is_empty())
                    .and_then(Value::as_str);
                if let Some(sig) = claim_sig {
                    if !self.is_revoked(&writer).await
                        && verify_claim(self.channel, &writer, &cur_owner, sig)
                    {
                        self.host
                            .add_writer(node.key, AddWriterOptions::writer())
                            .await?;
                        role = Some("owner".to_string());
                        self.roles.insert(writer.clone(), "owner".to_string());
                        claimed_here = true;
                        if let Some(callback) = self.writer_change {
                            callback(&writer, true);
                        }
                    }
                } else if cur_owner.is_empty() {
                    self.host
                        .add_writer(node.key, AddWriterOptions::writer())
                        .await?;
                    role = Some("writer".to_string());
                    self.roles.insert(writer.clone(), "writer".to_string());
                    legacy_granted_here = true;
                    if let Some(callback) = self.writer_change {
                        callback(&writer, true);
                    }
                } else {
                    self.roles.insert(writer.clone(), String::new());
                }
            }
            let Some(role) = role.filter(|role| !role.is_empty()) else {
                continue;
            };
            if claimed_here {
                let alias = clean_text(Some(&js_text(op.get("alias"))), "");
                self.put(format!("writer/{writer}"), writer_record("owner", &alias));
                if op_type == "claim" {
                    continue;
                }
            } else if legacy_granted_here {
                self.put(format!("writer/{writer}"), writer_record("writer", ""));
            }
            match op_type {
                "init" => {
                    if writer == self.channel {
                        self.roles.insert(writer.clone(), "owner".to_string());
                        self.put(
                            format!("writer/{writer}"),
                            writer_record("owner", "primary"),
                        );
                        if let Some(owner) = op.get("owner").and_then(Value::as_str) {
                            if !owner.is_empty() {
                                self.owner = Some(owner.to_string());
                                self.put("meta/owner", owner.to_string());
                            }
                        }
                        if let Some(epoch) = op.get("epoch").and_then(Value::as_f64) {
                            if epoch != 0.0 {
                                self.epoch = Some(epoch);
                                self.put("meta/epoch", js_number_text(epoch));
                            }
                        }
                    }
                }
                "owner" => {
                    if role == "owner" && self.get_owner().await.is_empty() {
                        let owner = op.get("owner").and_then(Value::as_str).unwrap_or_default();
                        self.owner = Some(owner.to_string());
                        self.put("meta/owner", owner.to_string());
                    }
                }
                "epoch" => {
                    if role == "owner" {
                        if self.epoch.is_none() {
                            let current = js_number(&self.get_text(b"meta/epoch").await);
                            self.epoch = Some(if current == 0.0 || !current.is_finite() {
                                1.0
                            } else {
                                current
                            });
                        }
                        let epoch = op.get("epoch").and_then(Value::as_f64).unwrap_or(0.0);
                        if epoch > self.epoch.unwrap_or(1.0) {
                            self.epoch = Some(epoch);
                            self.put("meta/epoch", js_number_text(epoch));
                        }
                    }
                }
                "grant" => {
                    if role == "owner" {
                        let target = op.get("writer").and_then(Value::as_str).unwrap_or_default();
                        let next_role = op
                            .get("role")
                            .filter(|role| truthy(Some(role)))
                            .and_then(Value::as_str)
                            .unwrap_or("writer");
                        self.revoked.insert(target.to_string(), false);
                        self.roles.insert(target.to_string(), next_role.to_string());
                        self.del(format!("revoked/{target}"));
                        self.host
                            .add_writer_id(target, AddWriterOptions::writer())
                            .await?;
                        let alias = clean_text(Some(&js_text(op.get("alias"))), "");
                        self.put(format!("writer/{target}"), writer_record(next_role, &alias));
                        if let Some(callback) = self.writer_change {
                            callback(target, true);
                        }
                    }
                }
                "revoke" => {
                    if role == "owner" {
                        let target = op.get("writer").and_then(Value::as_str).unwrap_or_default();
                        if target != self.channel {
                            self.host.remove_writer_id(target).await?;
                            self.roles.insert(target.to_string(), String::new());
                            self.revoked.insert(target.to_string(), true);
                            self.del(format!("writer/{target}"));
                            self.put(format!("revoked/{target}"), "1");
                            if let Some(callback) = self.writer_change {
                                callback(target, false);
                            }
                            if truthy(op.get("purgeEntries")) {
                                self.flush().await?;
                                let mut stream = self.view.range(RangeOptions::default());
                                let peer = format!("peer/{target}");
                                let source = format!("source/{target}/");
                                let mut purged = Vec::new();
                                while let Some(entry) = stream.next().await.map_err(host_error)? {
                                    let key = String::from_utf8_lossy(&entry.key).into_owned();
                                    if key == peer
                                        || key.starts_with(&source)
                                        || (!is_system_view_key(&key)
                                            && second_segment(&key) == Some(target))
                                    {
                                        purged.push(entry.key);
                                    }
                                }
                                for key in purged {
                                    self.del(key);
                                }
                            }
                        }
                    }
                }
                "remove" => {
                    let key = op.get("key").and_then(Value::as_str).unwrap_or_default();
                    if !key.starts_with("meta/")
                        && !key.starts_with("writer/")
                        && !key.starts_with("revoked/")
                        && (role == "owner"
                            || role == "mod"
                            || second_segment(key) == Some(writer.as_str()))
                    {
                        self.del(key.to_string());
                    }
                }
                "source" => {
                    let source_key = op
                        .get("sourceKey")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    self.put(format!("source/{writer}/{source_key}"), value);
                }
                "announce" => {
                    let id = op.get("id").and_then(Value::as_str).unwrap_or_default();
                    let blobs = op.get("blobs").and_then(Value::as_str).unwrap_or_default();
                    let offset = op
                        .get("blob")
                        .and_then(|blob| blob.get("blockOffset"))
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0);
                    self.put(
                        format!("{id}/{writer}/{blobs}:{}", js_number_text(offset)),
                        value,
                    );
                }
                _ => {}
            }
        }
        self.flush().await
    }
}

/// `String(number)`: safe integers have no fraction, and every op number `decode_op` lets through is
/// one.
fn js_number_text(number: f64) -> String {
    if number.fract() == 0.0 && number.abs() < 1e21 {
        format!("{number:.0}")
    } else {
        format!("{number}")
    }
}

impl Handlers for Apply {
    fn apply<'a, 'b>(
        &'a self,
        nodes: &'a [Node],
        view: &'a Bee,
        host: &'a ApplyCalls<'a, 'b>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut context = Context {
                channel: &self.channel,
                view,
                host,
                writer_change: self.writer_change.as_ref(),
                ops: Vec::new(),
                roles: HashMap::new(),
                revoked: HashMap::new(),
                owner: None,
                epoch: None,
            };
            context.run(nodes).await
        })
    }

    fn is_trusted<'a>(&'a self, key: &'a Key, reference: Option<&'a Bee>) -> BoxFuture<'a, bool> {
        Box::pin(is_trusted(&self.channel, key, reference))
    }

    fn update<'a>(&'a self, view: &'a Bee, changes: &'a Changes) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = view;
            if let Some(mirror) = &self.mirror {
                mirror.update(changes);
            }
        })
    }

    fn has_update(&self) -> bool {
        self.mirror.is_some()
    }
}
