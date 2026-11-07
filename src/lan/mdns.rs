//! mDNS half of the app's `src/lan.js`: PearTube's `hyperdht-mdns` record, its TXT sharding, the
//! parser, and the refcounted bonjour adapter, on top of the `mdns-sd` crate.
//!
//! `mdns-sd` was chosen over `astro-dnssd`/`libmdns` because it is pure Rust, needs no `unsafe` in
//! this crate, runs its own daemon thread (no async runtime required), keeps TXT keys in their
//! original case on the wire, and hands us resolved A records directly, which is what
//! `bonjour-service` does for the app.

use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mdns_sd::{
    DaemonEvent, RecvTimeoutError, ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo,
};
use sha2::{Digest, Sha256};

use super::host::{HostAdapter, HostAdvertisement, HostBrowse};

/// Service type the app advertises (`SERVICE_TYPE` in `src/lan.js`).
pub const SERVICE_TYPE: &str = "hyperdht-mdns";
/// Service protocol of that advertisement (`SERVICE_PROTOCOL`).
pub const SERVICE_PROTOCOL: &str = "udp";
/// The same service in the `_type._proto.local.` form `mdns-sd` uses.
pub const SERVICE_DOMAIN: &str = "_hyperdht-mdns._udp.local.";
/// Prefix hashed into every topic token (`TOPIC_PREFIX`).
pub const TOPIC_PREFIX: &[u8] = b"hyperdht-mdns:";
/// Topic tokens per `t<i>` TXT key.
pub const TOPICS_PER_TXT: usize = 5;
/// Topic cap per advertisement shard.
pub const MAX_SHARD_TOPICS: usize = 32;
/// TXT byte budget per advertisement shard.
pub const MAX_TXT_BYTES: usize = 900;
/// Browse restart cadence the app uses to recover from missed packets.
pub const DEFAULT_REBROWSE: Duration = Duration::from_secs(10);

const B64URL: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Callback for a resolved service (`browser.on('up')`).
pub type ServiceSink = Arc<dyn Fn(&DiscoveredService) + Send + Sync>;
/// Callback for daemon errors; every sink registered by an open advertisement or browse gets them.
pub type ErrorSink = Arc<dyn Fn(&str) + Send + Sync>;

/// Anything that stops this module from advertising or browsing.
#[derive(Debug)]
pub enum MdnsError {
    /// The daemon could not be started, is shut down, or rejected a command.
    Daemon(String),
    /// `mdns-sd` rejected the service description (bad TXT key or length, bad type).
    Service(String),
}

impl fmt::Display for MdnsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MdnsError::Daemon(msg) | MdnsError::Service(msg) => f.write_str(msg),
        }
    }
}

impl std::error::Error for MdnsError {}

fn base64url(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let n = u32::from(chunk[0]) << 16
            | u32::from(chunk.get(1).copied().unwrap_or(0)) << 8
            | u32::from(chunk.get(2).copied().unwrap_or(0));
        let chars = match chunk.len() {
            1 => 2,
            2 => 3,
            _ => 4,
        };
        for i in 0..chars {
            out.push(char::from(B64URL[(n >> (18 - 6 * i)) as usize & 0x3f]));
        }
    }
    out
}

/// Strict 64-digit lowercase hex, like the app's `/^[0-9a-f]{64}$/` after `toLowerCase()`.
fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn first12(s: &str) -> &str {
    s.get(..12).unwrap_or(s)
}

fn token_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'_'
}

/// `^[A-Za-z0-9_-]{43}$`: one base64url SHA-256 topic token.
fn is_token(t: &str) -> bool {
    t.len() == 43 && t.bytes().all(token_char)
}

/// Case-insensitive TXT lookup; mDNS keys are case-insensitive and `mdns-sd` resolves them so.
fn txt_get<'a>(txt: &'a [(String, String)], key: &str) -> &'a str {
    txt.iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(key))
        .map_or("", |(_, v)| v.as_str())
}

/// Base64url(SHA-256("hyperdht-mdns:" || topic)): `topicToken()`.
pub fn topic_token(topic: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(TOPIC_PREFIX);
    hash.update(topic);
    base64url(&hash.finalize())
}

/// `buildTxt()`: the fixed keys plus one `t<i>` key per `TOPICS_PER_TXT` tokens.
pub fn build_txt(peer_key_hex: &str, port: u16, tokens: &[String]) -> Vec<(String, String)> {
    let mut txt = vec![
        ("v".to_string(), "1".to_string()),
        ("peerKey".to_string(), peer_key_hex.to_string()),
        ("port".to_string(), port.to_string()),
        ("tc".to_string(), tokens.len().to_string()),
    ];
    for (i, chunk) in tokens.chunks(TOPICS_PER_TXT).enumerate() {
        txt.push((format!("t{i}"), chunk.join(",")));
    }
    txt
}

/// `encodedTxtBytes()`: every key and value plus its two length/separator bytes.
fn encoded_txt_bytes(txt: &[(String, String)]) -> usize {
    txt.iter().map(|(k, v)| k.len() + v.len() + 2).sum()
}

/// Three random base-36 characters, like `adNonce()`: no advertisement ever reuses a name.
pub fn ad_nonce() -> String {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    (0..3)
        .map(|_| char::from_digit(rng.gen_range(0..36), 36).unwrap_or('0'))
        .collect()
}

/// One advertised instance, equivalent to an entry of `createRecords()`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// mDNS instance name, `<type>-<peer prefix>-<generation>-<shard>-<total>`.
    pub name: String,
    /// Isolated DHT port carried in SRV and TXT.
    pub port: u16,
    /// TXT pairs in emission order (`v`, `peerKey`, `port`, `tc`, `t0..`).
    pub txt: Vec<(String, String)>,
}

impl Record {
    /// Decode this record, as a browsing peer does.
    pub fn parse(&self) -> Option<ParsedRecord> {
        parse_record(&self.name, self.port, &self.txt)
    }
}

/// `createRecords()` with a fresh advertisement nonce.
pub fn create_records(peer_key_hex: &str, port: u16, tokens: &[String]) -> Vec<Record> {
    create_records_with(peer_key_hex, port, tokens, &ad_nonce())
}

/// `createRecords()`: sorted tokens split into shards that fit [`MAX_SHARD_TOPICS`] and
/// [`MAX_TXT_BYTES`], one record per shard under a shared generation.
pub fn create_records_with(
    peer_key_hex: &str,
    port: u16,
    tokens: &[String],
    nonce: &str,
) -> Vec<Record> {
    let mut sorted = tokens.to_vec();
    // JS `.sort()` compares UTF-16 code units; every token byte is ASCII, so byte order matches.
    sorted.sort_unstable();

    let mut shards: Vec<Vec<String>> = Vec::new();
    if sorted.is_empty() {
        shards.push(Vec::new());
    } else {
        let mut cur: Vec<String> = Vec::new();
        for tok in &sorted {
            let mut next = cur.clone();
            next.push(tok.clone());
            if next.len() > MAX_SHARD_TOPICS
                || encoded_txt_bytes(&build_txt(peer_key_hex, port, &next)) > MAX_TXT_BYTES
            {
                shards.push(std::mem::take(&mut cur));
                cur = vec![tok.clone()];
            } else {
                cur = next;
            }
        }
        shards.push(cur);
    }

    let mut gen_hash = Sha256::new();
    for tok in &sorted {
        gen_hash.update(tok.as_bytes());
        gen_hash.update([0u8]);
    }
    let generation: String = format!(
        "{}{nonce}",
        base64url(&gen_hash.finalize()).get(..8).unwrap_or("")
    )
    .chars()
    .take(11)
    .collect();

    let prefix = first12(peer_key_hex);
    let total = shards.len();
    shards
        .into_iter()
        .enumerate()
        .map(|(idx, shard)| Record {
            name: format!("{SERVICE_TYPE}-{prefix}-{generation}-{idx}-{total}"),
            port,
            txt: build_txt(peer_key_hex, port, &shard),
        })
        .collect()
}

/// A peer decoded from one advertised record (`parseRecord()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRecord {
    /// 32-byte peer public key as lowercase hex.
    pub peer_key_hex: String,
    /// The same key as bytes.
    pub peer_key: [u8; 32],
    /// DHT port to dial.
    pub port: u16,
    /// Topic tokens this shard carries.
    pub tokens: Vec<String>,
    /// Generation shared by all shards of one advertisement; `None` if the name is not ours.
    pub generation: Option<String>,
    /// Shard index from the name.
    pub shard: usize,
    /// Shard count from the name.
    pub total_shards: usize,
}

/// `parseRecord()`: `None` for anything that is not a complete, well-formed PearTube record.
pub fn parse_record(name: &str, port: u16, txt: &[(String, String)]) -> Option<ParsedRecord> {
    if txt_get(txt, "v") != "1" {
        return None;
    }
    let peer_key_hex = txt_get(txt, "peerKey").to_ascii_lowercase();
    let peer_key = unhex32(&peer_key_hex)?;

    // The app trusts the SRV port unless it is 0, then falls back to the TXT copy.
    let port = if port != 0 {
        u32::from(port)
    } else {
        txt_get(txt, "port").parse().ok()?
    };
    if port == 0 || port > u32::from(u16::MAX) {
        return None;
    }

    let tc = txt_get(txt, "tc");
    let tc: usize = if tc.is_empty() { 0 } else { tc.parse().ok()? };
    if tc > MAX_SHARD_TOPICS {
        return None;
    }

    let mut tokens: Vec<String> = Vec::new();
    let mut i = 0;
    while tokens.len() < tc {
        let chunk = txt_get(txt, &format!("t{i}"));
        if chunk.is_empty() {
            return None;
        }
        tokens.extend(chunk.split(',').map(str::to_owned));
        i += 1;
    }
    if tokens.len() != tc || !tokens.iter().all(|t| is_token(t)) {
        return None;
    }

    let mut generation = None;
    let mut shard = 0;
    let mut total_shards = 1;
    let prefix = format!("{SERVICE_TYPE}-{}-", first12(&peer_key_hex));
    if let Some(tail) = name.strip_prefix(&prefix)
        && let Some((stamp, idx, total)) = parse_instance_tail(tail)
    {
        generation = Some(stamp.to_string());
        shard = idx as usize;
        total_shards = (total as usize).max(1);
    }

    Some(ParsedRecord {
        peer_key_hex,
        peer_key,
        port: port as u16,
        tokens,
        generation,
        shard,
        total_shards,
    })
}

/// `^([A-Za-z0-9_-]{11})-(\d+)-(\d+)$` over the part after `<type>-<peer prefix>-`.
fn parse_instance_tail(tail: &str) -> Option<(&str, u32, u32)> {
    if tail.len() < 15 {
        return None;
    }
    let stamp = tail.get(..11)?;
    if !stamp.bytes().all(token_char) || tail.as_bytes().get(11) != Some(&b'-') {
        return None;
    }
    let (shard, total) = tail.get(12..)?.split_once('-')?;
    Some((stamp, digits(shard)?, digits(total)?))
}

/// `\d+` as a `u32`.
fn digits(s: &str) -> Option<u32> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// One discovered service as the app's handler sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredService {
    /// Instance name, without the service-type suffix.
    pub name: String,
    /// SRV port.
    pub port: u16,
    /// Resolved IPv4 addresses, sorted so [`Self::ipv4`] is stable.
    pub addresses: Vec<Ipv4Addr>,
    /// TXT pairs.
    pub txt: Vec<(String, String)>,
}

impl DiscoveredService {
    /// The address the app would pick (`serviceIPv4()`: first IPv4, else the `h` hint).
    pub fn ipv4(&self) -> Option<Ipv4Addr> {
        self.addresses.first().copied()
    }

    /// Decode this service, as the app does in `_onService`.
    pub fn parse(&self) -> Option<ParsedRecord> {
        parse_record(&self.name, self.port, &self.txt)
    }
}

fn service_from_resolved(resolved: &ResolvedService) -> DiscoveredService {
    let suffix = format!(".{}", resolved.ty_domain);
    let name = resolved
        .fullname
        .strip_suffix(&suffix)
        .unwrap_or(&resolved.fullname)
        .to_string();
    let mut addresses: Vec<Ipv4Addr> = resolved.get_addresses_v4().into_iter().collect();
    // `mdns-sd` resolves into a set; sort so the picked address does not depend on hash order.
    addresses.sort_unstable();
    DiscoveredService {
        name,
        port: resolved.get_port(),
        addresses,
        txt: resolved
            .get_properties()
            .iter()
            .map(|p| (p.key().to_string(), p.val_str().to_string()))
            .collect(),
    }
}

/// Withdraw an advertisement (the app's `svc.stop(cb)`).
pub struct Advertisement {
    daemon: ServiceDaemon,
    fullname: String,
    inner: Arc<Inner>,
    on_error: Option<ErrorSink>,
    stopped: AtomicBool,
}

impl Advertisement {
    /// Full mDNS instance name, as registered.
    pub fn fullname(&self) -> &str {
        &self.fullname
    }

    /// Send the goodbye records, wait briefly for the daemon, then drop our daemon reference.
    pub fn stop(&self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Ok(status) = self.daemon.unregister(&self.fullname) {
            let _ = status.recv_timeout(Duration::from_secs(2));
        }
        self.inner.release(self.on_error.as_ref());
    }
}

impl Drop for Advertisement {
    fn drop(&mut self) {
        self.stop();
    }
}

/// One user of a shared browse (`browser` in the app); stopping it keeps the shared daemon open.
pub struct BrowseHandle {
    id: u64,
    inner: Arc<Inner>,
    state: Arc<BrowseState>,
    on_error: Option<ErrorSink>,
    stopped: AtomicBool,
}

impl BrowseHandle {
    /// Detach this subscriber; the last one stops the type's browse, like `browser.stop()`.
    pub fn stop(&self) {
        if self.stopped.swap(true, Ordering::SeqCst) {
            return;
        }
        let last = {
            let mut subs = self.state.subs.lock().expect("lan mdns subscribers");
            subs.retain(|sub| sub.id != self.id);
            subs.is_empty()
        };
        if last {
            self.state.stop.store(true, Ordering::SeqCst);
            let browses = self.inner.browses.lock().expect("lan mdns browses");
            let ours = browses
                .get(&self.state.ty)
                .is_some_and(|state| Arc::ptr_eq(state, &self.state));
            drop(browses);
            if ours {
                // The queued command ends the pump's receiver; the pump then exits on its flag.
                if let Some(daemon) = self.inner.daemon() {
                    let _ = daemon.stop_browse(&self.state.ty);
                }
                self.inner
                    .browses
                    .lock()
                    .expect("lan mdns browses")
                    .remove(&self.state.ty);
            }
        }
        self.inner.release(self.on_error.as_ref());
    }
}

impl Drop for BrowseHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The Bonjour backend `SelfAddressAdapter` wraps: this host's `mdns-sd` daemon, or the
/// platform's daemon on a device that denies raw multicast (iOS, see [`super::host`]).
#[derive(Clone)]
pub enum Adapter {
    Daemon(BonjourAdapter),
    Host(Arc<HostAdapter>),
}

/// A live advertisement from either backend; dropping it stops it.
pub enum Advert {
    Daemon(Advertisement),
    Host(HostAdvertisement),
}

impl Advert {
    /// `svc.stop()`.
    pub fn stop(&self) {
        match self {
            Self::Daemon(advertisement) => advertisement.stop(),
            Self::Host(advertisement) => advertisement.stop(),
        }
    }
}

/// A live browse from either backend; dropping it stops it.
pub enum Browse {
    Daemon(BrowseHandle),
    Host(HostBrowse),
}

impl Browse {
    /// `browser.stop()`.
    pub fn stop(&self) {
        match self {
            Self::Daemon(browse) => browse.stop(),
            Self::Host(browse) => browse.stop(),
        }
    }
}

#[derive(Clone)]
struct Sub {
    id: u64,
    up: ServiceSink,
    down: Option<ServiceSink>,
}

struct BrowseState {
    ty: String,
    stop: AtomicBool,
    subs: Mutex<Vec<Sub>>,
}

/// Refcounted `mdns-sd` wrapper: overlapping advertisements and browses share one daemon (the app
/// shares one multicast socket the same way), and the daemon closes with the last user.
#[derive(Clone)]
pub struct BonjourAdapter {
    inner: Arc<Inner>,
}

struct Inner {
    daemon: Mutex<Option<ServiceDaemon>>,
    /// Open users, `BonjourAdapter._refs`.
    refs: AtomicUsize,
    /// Whether this adapter started the daemon and must destroy it (`_owns`).
    owns: bool,
    /// Error handlers of all open users, refcounted like the app's `_errors` map.
    errors: Mutex<Vec<(ErrorSink, usize)>>,
    browses: Mutex<HashMap<String, Arc<BrowseState>>>,
    next_id: AtomicU64,
}

impl Default for BonjourAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl BonjourAdapter {
    /// A daemon of our own, started on first use.
    pub fn new() -> Self {
        Self::from_daemon(None, true)
    }

    /// Wrap an existing daemon; it is never shut down by this adapter (`new BonjourAdapter({ bonjour })`).
    pub fn with_daemon(daemon: ServiceDaemon) -> Self {
        Self::from_daemon(Some(daemon), false)
    }

    fn from_daemon(daemon: Option<ServiceDaemon>, owns: bool) -> Self {
        Self {
            inner: Arc::new(Inner {
                daemon: Mutex::new(daemon),
                refs: AtomicUsize::new(0),
                owns,
                errors: Mutex::new(Vec::new()),
                browses: Mutex::new(HashMap::new()),
                next_id: AtomicU64::new(1),
            }),
        }
    }

    /// Publish one record, addressed at `address` (the app lets bonjour-service add every
    /// interface's A record; `mdns-sd` needs the address explicitly).
    pub fn advertise(
        &self,
        record: &Record,
        address: Ipv4Addr,
        on_error: Option<ErrorSink>,
    ) -> Result<Advertisement, MdnsError> {
        let daemon = self.acquire(on_error.as_ref())?;
        let properties: Vec<(&str, &str)> = record
            .txt
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        let mut info = match ServiceInfo::new(
            SERVICE_DOMAIN,
            &record.name,
            &host_name(address),
            IpAddr::V4(address),
            record.port,
            &properties[..],
        ) {
            Ok(info) => info,
            Err(err) => {
                self.inner.release(on_error.as_ref());
                return Err(MdnsError::Service(err.to_string()));
            }
        };
        // Names carry a random nonce, so probing would only rename the instance and lose the
        // generation/shard suffix peers parse; the app publishes without probing either.
        info.set_requires_probe(false);
        let fullname = info.get_fullname().to_string();
        if let Err(err) = daemon.register(info) {
            self.inner.release(on_error.as_ref());
            return Err(MdnsError::Daemon(err.to_string()));
        }
        Ok(Advertisement {
            daemon,
            fullname,
            inner: Arc::clone(&self.inner),
            on_error,
            stopped: AtomicBool::new(false),
        })
    }

    /// Browse the PearTube service; `rebrowse` restarts the query on that cadence while the daemon
    /// (and its socket) stays open, like `selfAddressAdapter()` does every 10 s.
    pub fn browse(
        &self,
        rebrowse: Option<Duration>,
        on_up: ServiceSink,
        on_down: Option<ServiceSink>,
        on_error: Option<ErrorSink>,
    ) -> Result<BrowseHandle, MdnsError> {
        self.acquire(on_error.as_ref())?;
        let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
        let sub = Sub {
            id,
            up: on_up,
            down: on_down,
        };
        let state = {
            let mut browses = self.inner.browses.lock().expect("lan mdns browses");
            match browses.get(SERVICE_DOMAIN) {
                Some(state) => {
                    state.subs.lock().expect("lan mdns subscribers").push(sub);
                    Arc::clone(state)
                }
                None => {
                    let state = Arc::new(BrowseState {
                        ty: SERVICE_DOMAIN.to_string(),
                        stop: AtomicBool::new(false),
                        subs: Mutex::new(vec![sub]),
                    });
                    browses.insert(SERVICE_DOMAIN.to_string(), Arc::clone(&state));
                    spawn_pump(&self.inner, &state, rebrowse);
                    state
                }
            }
        };
        Ok(BrowseHandle {
            id,
            inner: Arc::clone(&self.inner),
            state,
            on_error,
            stopped: AtomicBool::new(false),
        })
    }

    fn acquire(&self, on_error: Option<&ErrorSink>) -> Result<ServiceDaemon, MdnsError> {
        if let Some(sink) = on_error {
            self.inner.add_error_sink(sink);
        }
        self.inner.refs.fetch_add(1, Ordering::SeqCst);
        let mut slot = self.inner.daemon.lock().expect("lan mdns daemon");
        if let Some(daemon) = slot.as_ref() {
            return Ok(daemon.clone());
        }
        let daemon = match ServiceDaemon::new() {
            Ok(daemon) => daemon,
            Err(err) => {
                self.inner.release(on_error);
                return Err(MdnsError::Daemon(err.to_string()));
            }
        };
        if let Ok(events) = daemon.monitor() {
            let inner = Arc::clone(&self.inner);
            std::thread::spawn(move || {
                while let Ok(event) = events.recv() {
                    if let DaemonEvent::Error(err) = event {
                        inner.report(&err.to_string());
                    }
                }
            });
        }
        *slot = Some(daemon.clone());
        Ok(daemon)
    }
}

impl Inner {
    fn daemon(&self) -> Option<ServiceDaemon> {
        self.daemon.lock().expect("lan mdns daemon").clone()
    }

    fn release(&self, on_error: Option<&ErrorSink>) {
        if let Some(sink) = on_error {
            self.remove_error_sink(sink);
        }
        if self.refs.fetch_sub(1, Ordering::SeqCst) != 1 {
            return;
        }
        let mut slot = self.daemon.lock().expect("lan mdns daemon");
        if self.owns
            && let Some(daemon) = slot.take()
        {
            let _ = daemon.shutdown();
        }
    }

    fn add_error_sink(&self, sink: &ErrorSink) {
        let mut sinks = self.errors.lock().expect("lan mdns errors");
        match sinks.iter_mut().find(|(open, _)| Arc::ptr_eq(open, sink)) {
            Some((_, users)) => *users += 1,
            None => sinks.push((Arc::clone(sink), 1)),
        }
    }

    fn remove_error_sink(&self, sink: &ErrorSink) {
        let mut sinks = self.errors.lock().expect("lan mdns errors");
        if let Some(pos) = sinks.iter().position(|(open, _)| Arc::ptr_eq(open, sink)) {
            if sinks[pos].1 > 1 {
                sinks[pos].1 -= 1;
            } else {
                sinks.remove(pos);
            }
        }
    }

    /// Hand a daemon-level error to every open user's handler.
    fn report(&self, message: &str) {
        let sinks: Vec<ErrorSink> = self
            .errors
            .lock()
            .expect("lan mdns errors")
            .iter()
            .map(|(sink, _)| Arc::clone(sink))
            .collect();
        for sink in sinks {
            sink(message);
        }
    }
}

fn host_name(address: Ipv4Addr) -> String {
    format!("{address}.local.")
}

fn spawn_pump(inner: &Arc<Inner>, state: &Arc<BrowseState>, rebrowse: Option<Duration>) {
    let inner = Arc::clone(inner);
    let state = Arc::clone(state);
    std::thread::spawn(move || pump(inner, state, rebrowse));
}

/// One browse per service type, fanned out to every subscriber: `mdns-sd` keeps a single querier
/// per type, so a second `browse()` would silently replace the first one's events.
fn pump(inner: Arc<Inner>, state: Arc<BrowseState>, rebrowse: Option<Duration>) {
    // A century of waiting still ends when `stop_browse` disconnects the receiver.
    const NO_REBROWSE: Duration = Duration::from_secs(3600);
    let ty = state.ty.clone();
    let mut known: HashMap<String, DiscoveredService> = HashMap::new();
    while !state.stop.load(Ordering::SeqCst) {
        let Some(daemon) = inner.daemon() else {
            break;
        };
        let events = match daemon.browse(&ty) {
            Ok(events) => events,
            Err(err) => {
                inner.report(&err.to_string());
                break;
            }
        };
        let deadline = Instant::now() + rebrowse.unwrap_or(NO_REBROWSE);
        loop {
            let wait = deadline.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            match events.recv_timeout(wait) {
                Ok(event) => dispatch(&state, event, &mut known),
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => {
                    if state.stop.load(Ordering::SeqCst) {
                        return;
                    }
                    break;
                }
            }
        }
        if !state.stop.load(Ordering::SeqCst) {
            let _ = daemon.stop_browse(&ty);
        }
    }
}

fn dispatch(
    state: &BrowseState,
    event: ServiceEvent,
    known: &mut HashMap<String, DiscoveredService>,
) {
    match event {
        ServiceEvent::ServiceResolved(resolved) => {
            let service = service_from_resolved(&resolved);
            known.insert(resolved.fullname.to_lowercase(), service.clone());
            for sub in subscribers(state) {
                (sub.up)(&service);
            }
        }
        // `ServiceRemoved` carries only the name, so reply from our own last resolution, which is
        // what the app's `down` event holds too.
        ServiceEvent::ServiceRemoved(_, fullname) => {
            if let Some(service) = known.remove(&fullname.to_lowercase()) {
                for sub in subscribers(state) {
                    if let Some(down) = &sub.down {
                        down(&service);
                    }
                }
            }
        }
        _ => {}
    }
}

fn subscribers(state: &BrowseState) -> Vec<Sub> {
    state.subs.lock().expect("lan mdns subscribers").clone()
}

/// `selfAddressAdapter()`: stamps the host's LAN address into TXT `h` so peers dial it directly
/// instead of through an mDNS reflector, and re-browses while keeping the shared daemon open.
#[derive(Clone)]
pub struct SelfAddressAdapter {
    inner: Adapter,
    host: Ipv4Addr,
    rebrowse: Option<Duration>,
}

impl SelfAddressAdapter {
    /// Wrap `inner`, stamping `host` and re-browsing every [`DEFAULT_REBROWSE`].
    pub fn new(inner: BonjourAdapter, host: Ipv4Addr) -> Self {
        Self::with_adapter(Adapter::Daemon(inner), host)
    }

    /// Wrap either backend, stamping `host` and re-browsing every [`DEFAULT_REBROWSE`].
    pub fn with_adapter(inner: Adapter, host: Ipv4Addr) -> Self {
        Self {
            inner,
            host,
            rebrowse: Some(DEFAULT_REBROWSE),
        }
    }

    /// Override the rebrowse cadence (`None` browses once and lets `mdns-sd` retransmit).
    pub fn with_rebrowse(
        inner: BonjourAdapter,
        host: Ipv4Addr,
        rebrowse: Option<Duration>,
    ) -> Self {
        Self {
            inner: Adapter::Daemon(inner),
            host,
            rebrowse,
        }
    }

    /// The address this adapter stamps into every record.
    pub fn host(&self) -> Ipv4Addr {
        self.host
    }

    /// Advertise `record` with `h` added, on this host.
    pub fn advertise(
        &self,
        record: &Record,
        on_error: Option<ErrorSink>,
    ) -> Result<Advert, MdnsError> {
        let mut record = record.clone();
        record.txt.push(("h".to_string(), self.host.to_string()));
        match &self.inner {
            Adapter::Daemon(bonjour) => bonjour
                .advertise(&record, self.host, on_error)
                .map(Advert::Daemon),
            Adapter::Host(host) => host.advertise(&record, on_error).map(Advert::Host),
        }
    }

    /// Browse, preferring a peer's stamped `h` over whatever address mDNS resolved.
    pub fn browse(
        &self,
        on_up: ServiceSink,
        on_down: Option<ServiceSink>,
        on_error: Option<ErrorSink>,
    ) -> Result<Browse, MdnsError> {
        let up: ServiceSink = Arc::new(move |service: &DiscoveredService| {
            let mut service = service.clone();
            if let Ok(h) = txt_get(&service.txt, "h").parse::<Ipv4Addr>() {
                service.addresses = vec![h];
            }
            on_up(&service);
        });
        match &self.inner {
            Adapter::Daemon(bonjour) => bonjour
                .browse(self.rebrowse, up, on_down, on_error)
                .map(Browse::Daemon),
            Adapter::Host(host) => host.browse(up, on_down, on_error).map(Browse::Host),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push_str(&format!("{byte:02x}"));
        }
        out
    }

    /// Value produced by the app's `topicToken(Buffer.from('topic-0'))`.
    const TOPIC_0_TOKEN: &str = "P7QAGHZlaitUh7x7nrW4h26d8rmK4_UsVARsSY5kT4k";

    #[test]
    fn topic_tokens_match_the_app() {
        assert_eq!(topic_token(b"topic-0"), TOPIC_0_TOKEN);
        assert_eq!(TOPIC_0_TOKEN.len(), 43);
    }

    #[test]
    fn txt_keys_follow_the_app_order() {
        let tokens = vec!["a".repeat(43); 6];
        let txt = build_txt(&"cd".repeat(32), 4242, &tokens);
        assert_eq!(
            txt,
            vec![
                ("v".to_string(), "1".to_string()),
                ("peerKey".to_string(), "cd".repeat(32)),
                ("port".to_string(), "4242".to_string()),
                ("tc".to_string(), "6".to_string()),
                ("t0".to_string(), vec!["a".repeat(43); 5].join(",")),
                ("t1".to_string(), "a".repeat(43)),
            ]
        );
    }

    #[test]
    fn empty_topic_set_makes_one_empty_record() {
        let records = create_records_with(&"ab".repeat(32), 4000, &[], "001");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].txt[3], ("tc".to_string(), "0".to_string()));
        assert_eq!(records[0].txt.len(), 4);
        assert_eq!(
            parse_record(&records[0].name, 4000, &records[0].txt)
                .unwrap()
                .tokens,
            Vec::<String>::new()
        );
    }

    #[test]
    fn records_round_trip() {
        let tokens: Vec<String> = (0..37)
            .map(|i| topic_token(format!("topic-{i}").as_bytes()))
            .collect();
        let peer_key_hex = hex(&[7u8; 32]);
        let records = create_records_with(&peer_key_hex, 4123, &tokens, "a0z");
        assert_eq!(records.len(), 3);
        let mut seen = Vec::new();
        for record in &records {
            let parsed = parse_record(&record.name, record.port, &record.txt).expect("parses");
            assert_eq!(parsed.peer_key_hex, peer_key_hex);
            assert_eq!(parsed.port, 4123);
            assert_eq!(parsed.generation.as_ref().unwrap().len(), 11);
            assert_eq!(parsed.total_shards, 3);
            seen.extend(parsed.tokens);
        }
        let mut expected = tokens.clone();
        expected.sort_unstable();
        assert_eq!(seen, expected);
        assert_eq!(records[0].name, records[1].name.replace("-1-3", "-0-3"));
    }

    #[test]
    fn shards_respect_the_byte_budget() {
        // 18 tokens encode to 898 TXT bytes; the 19th crosses the 900-byte budget.
        let token = |i: usize| topic_token(format!("t{i}").as_bytes());
        let eighteen: Vec<String> = (0..18).map(token).collect();
        let records = create_records_with(&"ef".repeat(32), 1, &eighteen, "zzz");
        assert_eq!(records.len(), 1);
        assert!(encoded_txt_bytes(&records[0].txt) <= MAX_TXT_BYTES);
        let nineteen: Vec<String> = (0..19).map(token).collect();
        assert_eq!(
            create_records_with(&"ef".repeat(32), 1, &nineteen, "zzz").len(),
            2
        );
    }

    #[test]
    fn parse_rejects_malformed_records() {
        let peer_key = hex(&[1u8; 32]);
        let good = vec![
            ("v".to_string(), "1".to_string()),
            ("peerKey".to_string(), peer_key.clone()),
            ("port".to_string(), "4100".to_string()),
            ("tc".to_string(), "1".to_string()),
            ("t0".to_string(), topic_token(b"x")),
        ];
        assert!(parse_record("anything", 4100, &good).is_some());
        let with = |key: &str, value: &str| {
            good.iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        if k == key {
                            value.to_string()
                        } else {
                            v.clone()
                        },
                    )
                })
                .collect::<Vec<_>>()
        };
        assert!(parse_record("n", 4100, &with("v", "2")).is_none());
        assert!(parse_record("n", 4100, &with("peerKey", "abcd")).is_none());
        assert!(parse_record("n", 4100, &with("tc", "33")).is_none());
        assert!(parse_record("n", 4100, &with("tc", "2")).is_none());
        assert!(parse_record("n", 4100, &with("t0", "short")).is_none());
        assert!(
            parse_record(
                "n",
                4100,
                &with("t0", &format!("{},{}", "a".repeat(43), "b".repeat(43)))
            )
            .is_none()
        );
        assert!(parse_record("n", 0, &with("port", "0")).is_none());
    }

    #[test]
    fn srv_port_wins_unless_zero() {
        let peer_key = hex(&[2u8; 32]);
        let txt = build_txt(&peer_key, 4500, &[]);
        assert_eq!(parse_record("n", 4200, &txt).unwrap().port, 4200);
        assert_eq!(parse_record("n", 0, &txt).unwrap().port, 4500);
    }

    #[test]
    fn names_without_our_prefix_still_parse() {
        let peer_key = hex(&[3u8; 32]);
        let txt = build_txt(&peer_key, 4200, &[topic_token(b"a")]);
        let parsed = parse_record("_hyperdht-mdns._udp", 4200, &txt).unwrap();
        assert_eq!(parsed.generation, None);
        assert_eq!((parsed.shard, parsed.total_shards), (0, 1));
    }
}
