//! Port of the app's `src/private-overlay.js`: a stateless 2-relay overlay on HyperDHT
//! (`Peer_A -> Guard_A <-> Guard_B <- Peer_B`) that splices raw UDX streams.
//!
//! Guards hold no Corestore state and touch no disk; they only relay. Endpoints never expose
//! their end-to-end Noise identity on the public DHT: every dial, rendezvous lookup and
//! owner-signed mutable descriptor write leaves through an auto-discovered `/24`-diverse guard.
//!
//! The DHT itself sits behind [`dht::Dht`], which mirrors the exact HyperDHT calls the JS file
//! makes. [`guard`] and [`swarm`] are written against that trait only; the peeroxide-backed
//! implementation lives in [`peeroxide`] behind the `peeroxide-dht` feature.

use std::net::IpAddr;
use std::sync::LazyLock;
use std::time::{SystemTime, UNIX_EPOCH};

use compact_encoding::{Reader, Writer};
use sha2::{Digest, Sha256};
use xsalsa20poly1305::aead::{Aead, KeyInit};
use xsalsa20poly1305::{Nonce, XSalsa20Poly1305};

pub mod dht;
pub mod guard;
#[cfg(feature = "peeroxide-dht")]
pub mod peeroxide;
pub mod rpc;
pub mod swarm;

pub use guard::{ControlPeer, GuardOptions, GuardStats, PrivateGuard, create_private_guard};
pub use swarm::{
    ExposureReport, PrivateRouting, PrivateSwarm, SourceHandle, SwarmOptions,
    attempt_direct_hop_bypass, create_private_swarm, publish_standalone_route_card,
};

/// Protomux protocol name of the guard control channel.
pub const PROTOCOL: &str = "peartube/private-guard/v1";
/// Domain separator for the owner signature on a route card.
pub const CARD_DOMAIN: &[u8] = b"peartube/private-route-card/v1\0";
/// How long a registered route or rendezvous slot lives.
pub const ROUTE_TTL_MS: u64 = 10 * 60_000;
/// Rendezvous epoch length; the topic salt rotates every epoch.
pub const RV_EPOCH_MS: u64 = 3_600_000;
pub const MAX_ROUTES_TOTAL: usize = 4096;
pub const MAX_ROUTES_PER_CONN: usize = 8;
pub const MAX_RV_TOPICS: usize = 1024;
pub const MAX_RV_SLOTS_PER_TOPIC: usize = 64;
pub const MAX_CIRCUITS_TOTAL: usize = 1024;
pub const MAX_CIRCUITS_PER_CONN: usize = 32;
pub const MAX_CARD_BYTES: usize = 512;
/// `crypto_secretbox` nonce plus MAC.
const BOX_OVERHEAD: usize = 24 + 16;
/// A route card is 112 body bytes plus a 64-byte signature.
pub const CARD_BYTES: usize = 176;
/// A card is only accepted when it expires within this window.
pub const CARD_MAX_LIFETIME_MS: u64 = 30 * 60_000;
/// Rendezvous guard re-announce interval.
pub const RV_REANNOUNCE_MS: u64 = 45_000;

/// Control-frame op codes.
pub mod op {
    pub const HELLO: u64 = 1;
    pub const REGISTER: u64 = 2;
    pub const REGISTER_ACK: u64 = 3;
    pub const PUT_MUTABLE: u64 = 4;
    pub const PUT_MUTABLE_ACK: u64 = 5;
    pub const GET_MUTABLE: u64 = 6;
    pub const GET_MUTABLE_REPLY: u64 = 7;
    pub const RV_REGISTER: u64 = 8;
    pub const RV_REGISTER_ACK: u64 = 9;
    pub const RV_UNREGISTER: u64 = 10;
    pub const RV_LOOKUP: u64 = 11;
    pub const RV_LOOKUP_REPLY: u64 = 12;
    pub const RV_FETCH: u64 = 13;
    pub const RV_FETCH_REPLY: u64 = 14;
    pub const DIAL_OPEN: u64 = 15;
    pub const DIAL_READY: u64 = 16;
    pub const HOP_OPEN: u64 = 17;
    pub const HOP_READY: u64 = 18;
    pub const INC_OPEN: u64 = 19;
    pub const INC_READY: u64 = 20;
    pub const CIRCUIT_DEAD: u64 = 21;
}

/// The Ed25519 identity the overlay signs cards and mutable records with.
pub use peeroxide_dht::hyperdht::KeyPair;

/// Errors surfaced by the overlay. `Unsupported` names a capability the DHT backend lacks.
#[derive(Debug, Clone)]
pub enum Error {
    /// The DHT backend failed.
    Dht(String),
    /// The control channel closed under us.
    Closed(&'static str),
    /// An RPC did not answer in time; carries the op code.
    Timeout(u64),
    /// The peer refused the request.
    Rejected(&'static str),
    /// The DHT backend cannot do this; the string names the missing capability.
    Unsupported(&'static str),
}

impl Error {
    pub fn dht(message: impl Into<String>) -> Self {
        Self::Dht(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Dht(message) => f.write_str(message),
            Self::Closed(message) => f.write_str(message),
            Self::Timeout(op) => write!(f, "RPC {op} timeout"),
            Self::Rejected(message) => f.write_str(message),
            Self::Unsupported(capability) => {
                write!(f, "peeroxide-dht is missing the `{capability}` capability")
            }
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// `sha256(...parts)` over the concatenation of the parts.
pub fn sha256(parts: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for part in parts {
        h.update(part);
    }
    h.finalize().into()
}

/// `HyperDHT.hash(buf)` (BLAKE2b-256).
pub fn hash(buf: &[u8]) -> [u8; 32] {
    peeroxide_dht::crypto::hash(buf)
}

pub fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

/// The four guard discovery topics. Note `HyperDHT.hash` is BLAKE2b-256, not SHA-256.
pub static GUARD_TOPICS: LazyLock<[[u8; 32]; 4]> = LazyLock::new(|| {
    [0usize, 1, 2, 3].map(|i| peeroxide_dht::crypto::hash(format!("{PROTOCOL}/{i}").as_bytes()))
});

/// `hypercoreCrypto.namespace('hyperswarm/dht', [4, 5, 6, 0, 1, 10])[2]`.
pub static NS_MUTABLE_PUT: LazyLock<[u8; 32]> =
    LazyLock::new(|| peeroxide_dht::crypto::namespace("hyperswarm/dht", &[4, 5, 6, 0, 1, 10])[2]);

/// Rendezvous topic for one source in one epoch.
pub fn rv_topic(source_key: &[u8; 32], epoch: &str) -> [u8; 32] {
    sha256(&[b"peartube/private-rv/v1/", source_key, epoch.as_bytes()])
}

/// Sealed card key derived from the advertised (Noise) public key.
pub fn peer_seal_key(public_key: &[u8; 32]) -> [u8; 32] {
    sha256(&[b"peartube/private-peer-seal/v1/", public_key])
}

/// Sealed card key derived from the source key.
pub fn rv_seal_key(source_key: &[u8; 32]) -> [u8; 32] {
    sha256(&[b"peartube/private-rv-seal/v1/", source_key])
}

/// `String(Math.floor(now / RV_EPOCH_MS))`.
pub fn rv_epoch(now: u64) -> String {
    (now / RV_EPOCH_MS).to_string()
}

/// 32 random bytes, `sodium.randombytes_buf`.
pub fn rand_bytes<const N: usize>() -> [u8; N] {
    use rand::RngCore;
    let mut out = [0u8; N];
    rand::rngs::OsRng.fill_bytes(&mut out);
    out
}

/// `diversityBucketForAddress(host, port, allowLoopback)`.
///
/// IPv4 peers bucket by their `/24`, IPv6 peers by the first three groups (`/48`), and loopback
/// peers by `host:port` when loopback buckets are allowed at all.
pub fn diversity_bucket_for_address(
    host: &str,
    port: Option<u32>,
    allow_loopback: bool,
) -> Option<String> {
    if host.is_empty() {
        return None;
    }
    let clean = host.strip_prefix("::ffff:").unwrap_or(host);
    let parts: Vec<&str> = clean.split('.').collect();
    let is_v4 = parts.len() == 4
        && parts.iter().all(|p| {
            !p.is_empty()
                && p.bytes().all(|b| b.is_ascii_digit())
                && p.parse::<u32>().is_ok_and(|n| n <= 255)
        });
    if is_v4 {
        let a = parts[0].parse::<u32>().ok()?;
        let b = parts[1].parse::<u32>().ok()?;
        let d = parts[2].parse::<u32>().ok()?;
        if a == 127 {
            let ok = allow_loopback && port.is_some_and(|p| p > 0);
            return ok.then(|| format!("loopback:{clean}:{}", port.unwrap_or(0)));
        }
        if a == 0 {
            return None;
        }
        return Some(format!("ipv4:{a}.{b}.{d}.0/24"));
    }
    if clean == "::1" {
        let ok = allow_loopback && port.is_some_and(|p| p > 0);
        return ok.then(|| format!("loopback:::1:{}", port.unwrap_or(0)));
    }
    if clean.contains(':') {
        let groups: Vec<String> = clean
            .to_lowercase()
            .split(':')
            .take(3)
            .map(|s| format!("{s:0>4}"))
            .collect();
        return Some(format!("ipv6:{}/48", groups.join(":")));
    }
    None
}

/// Bucket of a socket address, using its `IpAddr` text form.
pub fn bucket_of_address(addr: Option<(IpAddr, u16)>, allow_loopback: bool) -> Option<String> {
    let (host, port) = addr?;
    diversity_bucket_for_address(&host.to_string(), Some(port as u32), allow_loopback)
}

/// `isLocalBootstrap(host, bootstrap)`.
pub fn is_local_bootstrap(host: Option<&str>, bootstrap: &[String]) -> bool {
    matches!(host, Some("127.0.0.1") | Some("::1"))
        || bootstrap.iter().any(|b| b.starts_with("127."))
}

/// `signDhtMutable(seq, value, secretKey)`.
pub fn sign_dht_mutable(seq: u64, value: &[u8], key_pair: &KeyPair) -> [u8; 64] {
    let signable = peeroxide_dht::crypto::mutable_signable(&NS_MUTABLE_PUT, seq, value);
    peeroxide_dht::crypto::sign_detached(&signable, &key_pair.secret_key)
}

/// `verifyDhtMutable(sig, seq, value, publicKey)`.
pub fn verify_dht_mutable(sig: &[u8], seq: u64, value: &[u8], public_key: &[u8]) -> bool {
    if sig.len() != 64 || public_key.len() != 32 {
        return false;
    }
    let mut public_key_bytes = [0u8; 32];
    public_key_bytes.copy_from_slice(public_key);
    let mut signature = [0u8; 64];
    signature.copy_from_slice(sig);
    let signable = peeroxide_dht::crypto::mutable_signable(&NS_MUTABLE_PUT, seq, value);
    peeroxide_dht::crypto::verify_detached(&signature, &signable, &public_key_bytes)
}

/// `sealBox(plain, sealKey)`: `nonce || crypto_secretbox_easy(plain)`.
pub fn seal_box(plain: &[u8], seal_key: &[u8; 32]) -> Vec<u8> {
    let nonce: [u8; 24] = rand_bytes();
    let cipher = XSalsa20Poly1305::new_from_slice(seal_key).expect("32-byte key");
    let sealed = cipher
        .encrypt(Nonce::from_slice(&nonce), plain)
        .expect("secretbox encrypt");
    let mut out = Vec::with_capacity(24 + sealed.len());
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&sealed);
    out
}

/// `openBox(sealed, sealKey)`; `None` for anything that is not a valid sealed card.
pub fn open_box(sealed: &[u8], seal_key: &[u8; 32]) -> Option<Vec<u8>> {
    if sealed.len() <= BOX_OVERHEAD || sealed.len() > MAX_CARD_BYTES {
        return None;
    }
    let cipher = XSalsa20Poly1305::new_from_slice(seal_key).ok()?;
    cipher
        .decrypt(Nonce::from_slice(&sealed[..24]), &sealed[24..])
        .ok()
}

/// A decoded route card: the owner-signed half of the endpoint descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedCard {
    pub inner_public_key: [u8; 32],
    pub guard_public_key: [u8; 32],
    pub route_token: [u8; 32],
    pub seq: u64,
    pub expires_at: u64,
}

/// `encodeSignedCard(...)`.
pub fn encode_signed_card(
    inner_key_pair: &KeyPair,
    guard_public_key: &[u8; 32],
    route_token: &[u8; 32],
    seq: u64,
    expires_at: u64,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(CARD_BYTES);
    out.extend_from_slice(&inner_key_pair.public_key);
    out.extend_from_slice(guard_public_key);
    out.extend_from_slice(route_token);
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&expires_at.to_be_bytes());
    let mut signed = Vec::with_capacity(CARD_DOMAIN.len() + out.len());
    signed.extend_from_slice(CARD_DOMAIN);
    signed.extend_from_slice(&out);
    out.extend_from_slice(&peeroxide_dht::crypto::sign_detached(
        &signed,
        &inner_key_pair.secret_key,
    ));
    out
}

/// `decodeAndVerifyCard(buf, now, expectedKey)`.
pub fn decode_and_verify_card(
    buf: &[u8],
    now: u64,
    expected_key: Option<&[u8; 32]>,
) -> Option<SignedCard> {
    if buf.len() != CARD_BYTES {
        return None;
    }
    let body = &buf[..112];
    let signature = &buf[112..];
    let mut inner_public_key = [0u8; 32];
    inner_public_key.copy_from_slice(&body[..32]);
    if expected_key.is_some_and(|key| key != &inner_public_key) {
        return None;
    }
    let mut expires_at = [0u8; 8];
    expires_at.copy_from_slice(&body[104..112]);
    let expires_at = u64::from_be_bytes(expires_at);
    if expires_at <= now || expires_at > now + CARD_MAX_LIFETIME_MS {
        return None;
    }
    let mut signed = Vec::with_capacity(CARD_DOMAIN.len() + 112);
    signed.extend_from_slice(CARD_DOMAIN);
    signed.extend_from_slice(body);
    let mut signature_bytes = [0u8; 64];
    signature_bytes.copy_from_slice(signature);
    if !peeroxide_dht::crypto::verify_detached(&signature_bytes, &signed, &inner_public_key) {
        return None;
    }
    let mut guard_public_key = [0u8; 32];
    guard_public_key.copy_from_slice(&body[32..64]);
    let mut route_token = [0u8; 32];
    route_token.copy_from_slice(&body[64..96]);
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&body[96..104]);
    Some(SignedCard {
        inner_public_key,
        guard_public_key,
        route_token,
        seq: u64::from_be_bytes(seq),
        expires_at,
    })
}

/// One control-channel frame (`frameEnc`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub op: u64,
    pub id: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(op: u64, id: u64, payload: Vec<u8>) -> Self {
        Self { op, id, payload }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(
            compact_encoding::uint_len(self.op)
                + compact_encoding::uint_len(self.id)
                + compact_encoding::uint_len(self.payload.len() as u64)
                + self.payload.len(),
        );
        w.uint(self.op);
        w.uint(self.id);
        w.buffer(&self.payload);
        w.into_vec()
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let mut r = Reader::new(buf.to_vec().into());
        let op = r.uint().ok()?;
        let id = r.uint().ok()?;
        let payload = r.buffer().ok()?.to_vec();
        Some(Self { op, id, payload })
    }
}

/// `bufArrEnc` encode: a uint count then each buffer.
pub fn encode_buf_arr(items: &[Vec<u8>]) -> Vec<u8> {
    let mut w = Writer::with_capacity(
        compact_encoding::uint_len(items.len() as u64)
            + items
                .iter()
                .map(|i| compact_encoding::uint_len(i.len() as u64) + i.len())
                .sum::<usize>(),
    );
    w.uint(items.len() as u64);
    for item in items {
        w.buffer(item);
    }
    w.into_vec()
}

/// `bufArrEnc.decode`: the count is capped at 128 like the JS decoder.
pub fn decode_buf_arr(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut r = Reader::new(buf.to_vec().into());
    let n = r.uint().ok()?.min(128);
    let mut out = Vec::with_capacity(n as usize);
    for _ in 0..n {
        out.push(r.buffer().ok()?.to_vec());
    }
    Some(out)
}

/// Route/slot lifetime from a peer-supplied TTL, floored at 15s and capped at [`ROUTE_TTL_MS`].
pub fn route_lifetime(ttl: u32) -> u64 {
    (ttl as u64).clamp(15_000, ROUTE_TTL_MS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_topics_are_hyperdht_hash_of_the_protocol() {
        for (i, topic) in GUARD_TOPICS.iter().enumerate() {
            assert_eq!(
                *topic,
                peeroxide_dht::crypto::hash(format!("{PROTOCOL}/{i}").as_bytes())
            );
        }
        assert_ne!(GUARD_TOPICS[0], GUARD_TOPICS[1]);
    }

    #[test]
    fn bucket_assignment_matches_the_js_rules() {
        assert_eq!(
            diversity_bucket_for_address("10.1.2.3", Some(9000), false),
            Some("ipv4:10.1.2.0/24".into())
        );
        assert_eq!(
            diversity_bucket_for_address("127.0.0.1", Some(9000), false),
            None
        );
        assert_eq!(
            diversity_bucket_for_address("127.0.0.1", Some(9000), true),
            Some("loopback:127.0.0.1:9000".into())
        );
        assert_eq!(
            diversity_bucket_for_address("0.1.2.3", Some(1), false),
            None
        );
        assert_eq!(
            diversity_bucket_for_address("::ffff:10.1.2.3", None, false),
            Some("ipv4:10.1.2.0/24".into())
        );
        assert_eq!(
            diversity_bucket_for_address("2001:db8:1::5", None, false),
            Some("ipv6:2001:0db8:0001/48".into())
        );
        assert_eq!(
            diversity_bucket_for_address("example.com", None, false),
            None
        );
    }

    #[test]
    fn frames_round_trip() {
        let frame = Frame::new(19, 7, b"payload".to_vec());
        assert_eq!(Frame::decode(&frame.encode()), Some(frame));
        assert_eq!(Frame::decode(&[]), None);
    }

    #[test]
    fn buf_arr_round_trip() {
        let items = vec![b"a".to_vec(), vec![], b"ccc".to_vec()];
        assert_eq!(decode_buf_arr(&encode_buf_arr(&items)), Some(items));
    }

    #[test]
    fn cards_verify_and_respect_the_window() {
        let key_pair = KeyPair::from_seed([9u8; 32]);
        let guard = [7u8; 32];
        let token = [5u8; 32];
        let now = 1_000_000u64;
        let card = encode_signed_card(&key_pair, &guard, &token, 3, now + ROUTE_TTL_MS);
        let decoded = decode_and_verify_card(&card, now, Some(&key_pair.public_key)).unwrap();
        assert_eq!(decoded.route_token, token);
        assert_eq!(decoded.guard_public_key, guard);
        assert_eq!(decoded.seq, 3);
        assert!(decode_and_verify_card(&card, now, Some(&[1u8; 32])).is_none());
        assert!(decode_and_verify_card(&card, now + ROUTE_TTL_MS, None).is_none());
        assert!(decode_and_verify_card(&card, now - 1, None).is_some());
        let mut tampered = card.clone();
        tampered[0] ^= 1;
        assert!(decode_and_verify_card(&tampered, now, None).is_none());
    }

    #[test]
    fn sealed_boxes_round_trip_and_enforce_bounds() {
        let key = [3u8; 32];
        let sealed = seal_box(b"hello", &key);
        assert_eq!(sealed.len(), 24 + 16 + 5);
        assert_eq!(open_box(&sealed, &key).as_deref(), Some(&b"hello"[..]));
        assert!(open_box(&sealed, &[4u8; 32]).is_none());
        assert!(open_box(&[0u8; 40], &key).is_none());
        assert!(open_box(&[0u8; MAX_CARD_BYTES + 1], &key).is_none());
    }

    #[test]
    fn route_lifetime_is_clamped() {
        assert_eq!(route_lifetime(0), 15_000);
        assert_eq!(route_lifetime(u32::MAX), ROUTE_TTL_MS);
        assert_eq!(route_lifetime(60_000), 60_000);
    }
}
