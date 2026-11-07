//! HiveRelay wire formats and seeding state, ported from PearTube's `src/hiverelay.js`.
use compact_encoding::{Reader, Writer};
use hypercore::crypto::{self, KeyPair};
use protomux::{Channel, ChannelOptions, Mux};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const PROTOCOL: &str = "hiverelay-seed";
pub const HANDSHAKE: &[u8] = br#"{"major":1,"minor":0}"#;
pub const SIGNATURE_DOMAIN: &[u8] = b"hiverelay.seed-request.v3\0";
pub const MAX_SEED_CORES: usize = 64;
pub const PAGE_SIZE: usize = 16;
// Keep the JS interval so repeated broadcasts do not overwhelm relays.
pub const PAGE_INTERVAL_MS: u64 = 750;
pub const DISCOVERY_EPOCH_MS: u64 = 3_600_000;
// Refresh twice per epoch to retain the current and next discovery topics.
pub const EPOCH_REFRESH_MS: u64 = 1_800_000;
pub const MIN_BOUND_BYTES: u64 = 16 * 1024 * 1024;
pub const BOUND_STEP_BYTES: u64 = 8 * 1024 * 1024;

pub fn discovery_topic() -> [u8; 32] {
    crypto::hash(&[b"hiverelay-discovery-v1"])
}
pub fn epoch_topics(now_ms: u64) -> [[u8; 32]; 2] {
    let bucket = now_ms / DISCOVERY_EPOCH_MS;
    [bucket, bucket + 1]
        .map(|b| crypto::hash(&[format!("hiverelay-discovery-v1-epoch-{b}").as_bytes()]))
}
pub fn publisher_keypair(channel_secret: &[u8; 64]) -> KeyPair {
    let mut h = Sha256::new();
    h.update(b"peartube/hiverelay-publisher/v1");
    h.update(channel_secret);
    KeyPair::from_seed(&h.finalize().into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedRequest {
    pub app_key: [u8; 32],
    pub discovery_keys: Vec<[u8; 32]>,
    pub replication_factor: u64,
    pub geo_preference: String,
    pub max_storage_bytes: u64,
    pub bounty_rate: u64,
    pub ttl_seconds: u64,
    pub publisher_pubkey: [u8; 32],
    pub publisher_signature: [u8; 64],
    pub revocable: u64,
    pub unseed_freeze_ms: u64,
    pub durability: u64,
}
impl SeedRequest {
    pub fn signable(&self) -> Vec<u8> {
        let mut meta = [0u8; 40];
        meta[0] = self.replication_factor as u8;
        meta[1] = u8::from(self.revocable != 0);
        meta[2] = self.durability as u8;
        meta[8..16].copy_from_slice(&self.max_storage_bytes.to_be_bytes());
        meta[16..24].copy_from_slice(&self.ttl_seconds.to_be_bytes());
        meta[24..28].copy_from_slice(&(self.bounty_rate as u32).to_be_bytes());
        meta[28..36].copy_from_slice(&self.unseed_freeze_ms.to_be_bytes());
        let hash = if self.discovery_keys.is_empty() {
            [0; 32]
        } else {
            crypto::hash(
                &self
                    .discovery_keys
                    .iter()
                    .map(|k| k.as_slice())
                    .collect::<Vec<_>>(),
            )
        };
        [SIGNATURE_DOMAIN, &self.app_key, &hash, &meta].concat()
    }
    pub fn sign(&mut self, key: &KeyPair) {
        self.publisher_pubkey = key.public_key;
        self.publisher_signature = key.sign(&self.signable());
    }
    pub fn verify(&self) -> bool {
        crypto::verify(
            &self.signable(),
            &self.publisher_signature,
            &self.publisher_pubkey,
        )
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(&self.app_key);
        w.uint(self.discovery_keys.len() as u64);
        for k in &self.discovery_keys {
            w.raw(k);
        }
        w.uint(self.replication_factor);
        w.string(&self.geo_preference);
        w.uint(self.max_storage_bytes);
        w.uint(self.bounty_rate);
        w.uint(self.ttl_seconds);
        w.raw(&self.publisher_pubkey);
        w.raw(&self.publisher_signature);
        w.uint(self.revocable);
        w.uint(self.unseed_freeze_ms);
        w.uint(self.durability);
        w.into_vec()
    }
    pub fn decode(data: &[u8]) -> compact_encoding::Result<Self> {
        let mut r = Reader::new(data.to_vec().into());
        let app_key = r.fixed()?;
        let n = r.uint()?.min(MAX_SEED_CORES as u64);
        let mut discovery_keys = Vec::with_capacity(n as usize);
        for _ in 0..n {
            discovery_keys.push(r.fixed()?);
        }
        Ok(Self {
            app_key,
            discovery_keys,
            replication_factor: r.uint()?,
            geo_preference: r.string()?,
            max_storage_bytes: r.uint()?,
            bounty_rate: r.uint()?,
            ttl_seconds: r.uint()?,
            publisher_pubkey: r.fixed()?,
            publisher_signature: r.fixed()?,
            revocable: if r.is_empty() { 1 } else { r.uint()? },
            unseed_freeze_ms: if r.is_empty() { 0 } else { r.uint()? },
            durability: if r.is_empty() { 0 } else { r.uint()? },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedAccept {
    pub app_key: [u8; 32],
    pub relay_pubkey: [u8; 32],
    pub region: String,
    pub available_storage_bytes: u64,
    pub relay_signature: [u8; 64],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnseedRequest {
    pub app_key: [u8; 32],
    pub timestamp: u64,
    pub publisher_pubkey: [u8; 32],
    pub publisher_signature: [u8; 64],
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeedDeny {
    pub app_key: [u8; 32],
    pub relay_pubkey: [u8; 32],
    pub reason_code: String,
    pub detail: String,
    pub relay_signature: [u8; 64],
}
impl SeedAccept {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(&self.app_key);
        w.raw(&self.relay_pubkey);
        w.string(&self.region);
        w.uint(self.available_storage_bytes);
        w.raw(&self.relay_signature);
        w.into_vec()
    }
    pub fn decode(data: &[u8]) -> compact_encoding::Result<Self> {
        let mut r = Reader::new(data.to_vec().into());
        Ok(Self {
            app_key: r.fixed()?,
            relay_pubkey: r.fixed()?,
            region: r.string()?,
            available_storage_bytes: r.uint()?,
            relay_signature: r.fixed()?,
        })
    }
    pub fn valid_from(&self, remote: &[u8; 32]) -> bool {
        &self.relay_pubkey == remote
            && (self.relay_signature == [0; 64]
                || crypto::verify(
                    &[
                        self.app_key.as_slice(),
                        self.relay_pubkey.as_slice(),
                        self.region.as_bytes(),
                    ]
                    .concat(),
                    &self.relay_signature,
                    remote,
                ))
    }
}
impl SeedDeny {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(&self.app_key);
        w.raw(&self.relay_pubkey);
        w.string(&self.reason_code);
        w.string(&self.detail);
        w.raw(&self.relay_signature);
        w.into_vec()
    }
    pub fn decode(data: &[u8]) -> compact_encoding::Result<Self> {
        let mut r = Reader::new(data.to_vec().into());
        Ok(Self {
            app_key: r.fixed()?,
            relay_pubkey: r.fixed()?,
            reason_code: r.string()?,
            detail: r.string()?,
            relay_signature: r.fixed()?,
        })
    }
    pub fn valid_from(&self, remote: &[u8; 32]) -> bool {
        &self.relay_pubkey == remote
            && (self.relay_signature == [0; 64]
                || crypto::verify(
                    &[
                        self.app_key.as_slice(),
                        self.relay_pubkey.as_slice(),
                        self.reason_code.as_bytes(),
                    ]
                    .concat(),
                    &self.relay_signature,
                    remote,
                ))
    }
}
impl UnseedRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.raw(&self.app_key);
        w.uint(self.timestamp);
        w.raw(&self.publisher_pubkey);
        w.raw(&self.publisher_signature);
        w.into_vec()
    }
    pub fn decode(data: &[u8]) -> compact_encoding::Result<Self> {
        let mut r = Reader::new(data.to_vec().into());
        Ok(Self {
            app_key: r.fixed()?,
            timestamp: r.uint()?,
            publisher_pubkey: r.fixed()?,
            publisher_signature: r.fixed()?,
        })
    }
}

pub fn open_channel(mux: &Mux) -> Option<Channel> {
    mux.open_channel(ChannelOptions {
        handshake: HANDSHAKE,
        ..ChannelOptions::new(PROTOCOL)
    })
}
pub fn send_seed(channel: &Channel, request: &SeedRequest) -> bool {
    channel.send_raw(0, &request.encode())
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedMessage {
    Request(SeedRequest),
    Accept(SeedAccept),
    Unseed(UnseedRequest),
    Deny(SeedDeny),
}
impl SeedMessage {
    pub fn decode(kind: u64, bytes: &[u8]) -> compact_encoding::Result<Self> {
        match kind {
            0 => SeedRequest::decode(bytes).map(Self::Request),
            1 => SeedAccept::decode(bytes).map(Self::Accept),
            2 => UnseedRequest::decode(bytes).map(Self::Unseed),
            3 => SeedDeny::decode(bytes).map(Self::Deny),
            _ => Err(compact_encoding::Error::Invalid("seed message kind", kind)),
        }
    }
    pub fn send(&self, channel: &Channel) -> bool {
        let (kind, bytes) = match self {
            Self::Request(m) => (0, m.encode()),
            Self::Accept(m) => (1, m.encode()),
            Self::Unseed(m) => (2, m.encode()),
            Self::Deny(m) => (3, m.encode()),
        };
        channel.send_raw(kind, &bytes)
    }
}
pub fn page_delay_ms(last_sent_ms: u64, now_ms: u64) -> u64 {
    if last_sent_ms == 0 {
        0
    } else {
        PAGE_INTERVAL_MS.saturating_sub(now_ms.saturating_sub(last_sent_ms))
    }
}

fn varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while n > 127 {
        out.push((n as u8 & 127) | 128);
        n >>= 7;
    }
    out.push(n as u8);
    out
}
fn pb_bytes(field: u64, data: &[u8]) -> Vec<u8> {
    [
        varint((field << 3) | 2),
        varint(data.len() as u64),
        data.to_vec(),
    ]
    .concat()
}
pub fn hyperdrive_header(content_feed: &[u8; 32]) -> Vec<u8> {
    [
        pb_bytes(1, b"hyperbee"),
        pb_bytes(2, &pb_bytes(1, content_feed)),
    ]
    .concat()
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlobId {
    pub block_offset: u64,
    pub block_length: u64,
    pub byte_offset: u64,
    pub byte_length: u64,
}
pub fn hyperdrive_root(seq: u64, path: &str, blob: &BlobId) -> Vec<u8> {
    let index = pb_bytes(1, &pb_bytes(1, &varint(seq)));
    let path = format!(
        "files\0{}{}",
        if path.starts_with('/') { "" } else { "/" },
        path
    );
    let value = serde_json::json!({"executable":false,"linkname":null,"blob":blob,"metadata":null});
    [
        pb_bytes(1, &index),
        pb_bytes(2, path.as_bytes()),
        pb_bytes(
            3,
            serde_json::to_string(&value)
                .expect("serializable")
                .as_bytes(),
        ),
    ]
    .concat()
}
pub fn bound_bytes(actual: u64) -> u64 {
    let headroom = actual.saturating_add(9) / 10;
    let padded = actual
        .saturating_add(headroom.clamp(8 * 1024 * 1024, 128 * 1024 * 1024))
        .max(MIN_BOUND_BYTES);
    padded.saturating_add(BOUND_STEP_BYTES - 1) / BOUND_STEP_BYTES * BOUND_STEP_BYTES
}
#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    pub method: &'static str,
    pub url: String,
    pub headers: BTreeMap<String, String>,
    pub body: String,
}
pub fn seed_core_request(
    url: &str,
    api_key: Option<&str>,
    core_key: &str,
    byte_length: u64,
) -> HttpRequest {
    let body = format!(
        "{{\"coreKey\":{},\"maxStorageBytes\":{}}}",
        serde_json::to_string(core_key).expect("string"),
        bound_bytes(byte_length)
    );
    let mut headers = BTreeMap::new();
    headers.insert("accept".into(), "application/json".into());
    headers.insert("content-type".into(), "application/json".into());
    headers.insert("content-length".into(), body.len().to_string());
    if let Some(key) = api_key.filter(|s| !s.is_empty()) {
        headers.insert("authorization".into(), format!("Bearer {key}"));
    }
    HttpRequest {
        method: "POST",
        url: format!("{}/seed-core", url.trim_end_matches('/')),
        headers,
        body,
    }
}
#[derive(Debug, Clone)]
pub struct Core {
    pub key: String,
    pub kind: String,
    pub byte_length: u64,
    pub accepted: usize,
}
/// Reproduces JS ordering, preference for under-replicated blobs, and cursor rotation.
pub fn select_page<'a>(
    cores: &'a [Core],
    replicas: usize,
    force: bool,
    cursor: &mut usize,
) -> Vec<&'a Core> {
    let replicas = if replicas == 0 { 3 } else { replicas.min(32) };
    let mut ordered: Vec<_> = cores.iter().collect();
    ordered.sort_by_key(|c| (c.kind != "channel", c.byte_length));
    let mut page: Vec<_> = ordered
        .iter()
        .copied()
        .filter(|c| c.kind == "channel" && (force || c.accepted < replicas))
        .collect();
    let blobs: Vec<_> = ordered
        .iter()
        .copied()
        .filter(|c| c.kind != "channel")
        .collect();
    let unseeded: Vec<_> = blobs
        .iter()
        .copied()
        .filter(|c| c.accepted < replicas)
        .collect();
    let pool = if unseeded.is_empty() {
        &blobs
    } else {
        &unseeded
    };
    if !pool.is_empty() {
        let start = *cursor % pool.len();
        let take = PAGE_SIZE.saturating_sub(page.len()).max(1).min(pool.len());
        for i in 0..take {
            page.push(pool[(start + i) % pool.len()]);
        }
        *cursor = (start + take) % pool.len();
    }
    page
}
