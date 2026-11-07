//! Port of the relay's `src/settings.js`: settings.json beside jobs.json, an env fallback per field, and the
//! view/update API the apps call.
//!
//! settings.json (set from the UI) wins, then an env var, then the default. The file is private (0600) and never
//! in the Corestore. Secrets (HiveRelay API keys, tokens inside URLs) are write-only: views carry a redacted label
//! and a `#n` ref that keeps the stored value on update. `null` clears a saved value, so env or default applies
//! again. `live` settings apply in place; the rest reopen the node.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde_json::{Map, Number, Value};

use crate::app_settings::{js_number, js_string, js_truthy};
use crate::protocol::{is_hex64, is_js_space, js_trim};

/// `CINEMETA_URL` from `src/art.js`: the default metadata host.
pub const CINEMETA_URL: &str = "https://v3-cinemeta.strem.io";

/// A settings failure: the message the worklet throws, with the status it answers with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingsError {
    status: u16,
    message: String,
}

impl SettingsError {
    fn new(status: u16, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

fn bad(key: &str, why: impl fmt::Display) -> SettingsError {
    SettingsError::new(400, format!("{key}: {why}"))
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SettingsError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bool,
    Port,
    Number,
    Host,
    Key,
    Path,
    Secret,
    Url,
    Keys,
    Hosts,
    Urls,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::Port => "port",
            Self::Number => "number",
            Self::Host => "host",
            Self::Key => "key",
            Self::Path => "path",
            Self::Secret => "secret",
            Self::Url => "url",
            Self::Keys => "keys",
            Self::Hosts => "hosts",
            Self::Urls => "urls",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Default {
    Bool(bool),
    Num(f64),
    Str(&'static str),
    List,
}

impl Default {
    fn value(self) -> Value {
        match self {
            Self::Bool(flag) => Value::Bool(flag),
            Self::Num(number) => number_value(number),
            Self::Str(text) => Value::String(text.to_owned()),
            Self::List => Value::Array(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Spec {
    key: &'static str,
    env: &'static str,
    kind: Kind,
    def: Default,
    live: bool,
    min: f64,
    max: f64,
    from_env: Option<fn(&str) -> Value>,
    empty: bool,
}

impl Spec {
    const fn live(mut self) -> Self {
        self.live = true;
        self
    }

    const fn range(mut self, min: f64, max: f64) -> Self {
        self.min = min;
        self.max = max;
        self
    }

    const fn env_parse(mut self, parse: fn(&str) -> Value) -> Self {
        self.from_env = Some(parse);
        self
    }

    const fn empty(mut self) -> Self {
        self.empty = true;
        self
    }
}

const fn spec(key: &'static str, env: &'static str, kind: Kind, def: Default) -> Spec {
    Spec {
        key,
        env,
        kind,
        def,
        live: false,
        min: 0.0,
        max: 0.0,
        from_env: None,
        empty: false,
    }
}

/// The relay fields, in view order.
const SPEC: &[Spec] = &[
    spec(
        "streamHost",
        "PEARTUBE_STREAM_HOST",
        Kind::Host,
        Default::Str("127.0.0.1"),
    ),
    spec(
        "streamPort",
        "PEARTUBE_STREAM_PORT",
        Kind::Port,
        Default::Num(8175.0),
    ),
    spec("lan", "PEARTUBE_LAN_HOST", Kind::Bool, Default::Bool(false))
        .live()
        .env_parse(env_truthy),
    spec("lanHost", "PEARTUBE_LAN_HOST", Kind::Host, Default::Str("")).live(),
    spec(
        "dhtPort",
        "PEARTUBE_DHT_PORT",
        Kind::Port,
        Default::Num(0.0),
    ),
    spec(
        "bootstrap",
        "PEARTUBE_BOOTSTRAP",
        Kind::Hosts,
        Default::List,
    ),
    spec(
        "relayThrough",
        "PEARTUBE_RELAY_THROUGH",
        Kind::Keys,
        Default::List,
    ),
    spec(
        "privateRouting",
        "PEARTUBE_PRIVATE_ROUTING",
        Kind::Bool,
        Default::Bool(false),
    )
    .env_parse(env_truthy),
    spec(
        "privateGuard",
        "PEARTUBE_PRIVATE_GUARD",
        Kind::Bool,
        Default::Bool(false),
    )
    .live()
    .env_parse(env_flag),
    spec(
        "privateGuardPort",
        "PEARTUBE_PRIVATE_GUARD_PORT",
        Kind::Port,
        Default::Num(0.0),
    )
    .live(),
    spec("tracker", "PEARTUBE_TRACKER", Kind::Key, Default::Str("")),
    spec(
        "allowedPeers",
        "PEARTUBE_ALLOWED_PEERS",
        Kind::Keys,
        Default::List,
    )
    .live(),
    spec(
        "publishChannel",
        "PEARTUBE_PUBLISH_CHANNEL",
        Kind::Bool,
        Default::Bool(true),
    )
    .env_parse(env_not_zero),
    spec(
        "directory",
        "PEARTUBE_DIRECTORY",
        Kind::Bool,
        Default::Bool(false),
    )
    .live()
    .env_parse(env_truthy),
    spec(
        "acquireConcurrency",
        "PEARTUBE_ACQUIRE_CONCURRENCY",
        Kind::Number,
        Default::Num(4.0),
    )
    .range(1.0, 32.0)
    .live(),
    spec(
        "watchDir",
        "PEARTUBE_WATCH_DIR",
        Kind::Path,
        Default::Str(""),
    )
    .live(),
    spec("feeds", "PEARTUBE_MIRROR_FEEDS", Kind::Urls, Default::List).live(),
    spec(
        "replicate",
        "PEARTUBE_REPLICATE",
        Kind::Bool,
        Default::Bool(false),
    )
    .live()
    .env_parse(env_replicate),
    spec(
        "replicateSources",
        "PEARTUBE_REPLICATE",
        Kind::Keys,
        Default::List,
    )
    .live()
    .env_parse(env_replicate_sources),
    spec(
        "replicateFreeGb",
        "PEARTUBE_REPLICATE_FREE_GB",
        Kind::Number,
        Default::Num(20.0),
    )
    .range(0.0, 1e6)
    .live(),
    spec(
        "replicateMbps",
        "PEARTUBE_REPLICATE_MBPS",
        Kind::Number,
        Default::Num(80.0),
    )
    .range(0.0, 1e5)
    .live(),
    // Playback cache ceiling: 0 leaves it unlimited (relays seed on purpose); clients set it so one watched 4K
    // remux cannot fill the disk.
    spec(
        "cacheMaxGb",
        "PEARTUBE_CACHE_MAX_GB",
        Kind::Number,
        Default::Num(0.0),
    )
    .range(0.0, 1e6)
    .live(),
    spec(
        "hiverelays",
        "PEARTUBE_HIVERELAYS",
        Kind::Urls,
        Default::List,
    ),
    spec(
        "hiverelayApiKey",
        "PEARTUBE_HIVERELAY_API_KEY",
        Kind::Secret,
        Default::Str(""),
    ),
    spec(
        "hiverelayDiscover",
        "PEARTUBE_HIVERELAY_DISCOVER",
        Kind::Bool,
        Default::Bool(false),
    )
    .env_parse(env_truthy),
    spec(
        "hiverelayReplicas",
        "PEARTUBE_HIVERELAY_REPLICAS",
        Kind::Number,
        Default::Num(3.0),
    )
    .range(1.0, 32.0),
    spec(
        "hiverelayUploadMbps",
        "PEARTUBE_HIVERELAY_UPLOAD_MBPS",
        Kind::Number,
        Default::Num(50.0),
    )
    .range(0.0, 1e5),
    spec(
        "rechunk",
        "PEARTUBE_RECHUNK",
        Kind::Bool,
        Default::Bool(false),
    )
    .live()
    .env_parse(env_flag),
    spec(
        "cinemetaUrl",
        "PEARTUBE_CINEMETA_URL",
        Kind::Url,
        Default::Str(CINEMETA_URL),
    )
    .live()
    .empty(),
];

fn env_truthy(text: &str) -> Value {
    Value::Bool(!text.is_empty())
}

fn env_flag(text: &str) -> Value {
    Value::Bool(text == "1" || text == "true")
}

fn env_not_zero(text: &str) -> Value {
    Value::Bool(text != "0")
}

fn env_replicate(text: &str) -> Value {
    Value::Bool(!text.is_empty() && text != "0" && text != "false")
}

fn env_replicate_sources(text: &str) -> Value {
    if text == "1" || text == "true" {
        Value::Array(Vec::new())
    } else {
        csv(&text.to_lowercase())
    }
}

/// Comma separated env lists.
fn csv(text: &str) -> Value {
    Value::Array(
        text.split(',')
            .map(js_trim)
            .filter(|part| !part.is_empty())
            .map(|part| Value::String(part.to_owned()))
            .collect(),
    )
}

/// `JSON.stringify` prints NaN and the infinities as `null`; so does a settings view.
fn number_value(number: f64) -> Value {
    if !number.is_finite() {
        return Value::Null;
    }
    if number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_992.0 {
        return Value::Number(Number::from(number as i64));
    }
    Number::from_f64(number).map_or(Value::Null, Value::Number)
}

fn json_string(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned())
}

fn parse_env(spec: &Spec, raw: &str) -> Value {
    if let Some(parse) = spec.from_env {
        return parse(raw);
    }
    match spec.kind {
        Kind::Port | Kind::Number => number_value(js_number(raw)),
        Kind::Keys | Kind::Hosts | Kind::Urls => csv(raw),
        _ => Value::String(raw.to_owned()),
    }
}

/// The relay settings a running node was opened with, and the update API the apps drive.
pub struct Settings {
    file: PathBuf,
    saved: Map<String, Value>,
    env: HashMap<String, String>,
    applied: Map<String, Value>,
}

impl Settings {
    /// `createSettings`: reads `settings.json` from `storage`; a missing or broken file reads as defaults.
    pub fn new(storage: impl AsRef<Path>, env: HashMap<String, String>) -> Self {
        let file = storage.as_ref().join("settings.json");
        // shortcut: a file whose JSON is not an object reads as defaults, where JS would spread its keys.
        let saved = fs::read_to_string(&file)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        let mut settings = Self {
            file,
            saved,
            env,
            applied: Map::new(),
        };
        settings.applied = settings.snapshot_map();
        settings
    }

    /// The `settings.json` this instance reads and writes.
    pub fn file(&self) -> &Path {
        &self.file
    }

    /// The value the running relay would use for `key` (unknown keys read as `null`).
    pub fn get(&self, key: &str) -> Value {
        self.read(key, &self.saved)
    }

    /// What the apps show: every field with its type, where its value came from, and what a restart would apply.
    pub fn view(&self) -> Value {
        let mut settings = Map::new();
        for spec in SPEC {
            let mut value = self.get(spec.key);
            let mut out = Map::new();
            out.insert(
                "type".to_owned(),
                Value::String(spec.kind.name().to_owned()),
            );
            out.insert("live".to_owned(), Value::Bool(spec.live));
            let source = if self.saved.contains_key(spec.key) {
                "file"
            } else if self.env_value(spec).is_some() {
                "env"
            } else {
                "default"
            };
            out.insert("source".to_owned(), Value::String(source.to_owned()));
            out.insert("env".to_owned(), Value::String(spec.env.to_owned()));
            out.insert(
                "envSet".to_owned(),
                Value::Bool(self.env_value(spec).is_some()),
            );
            match spec.kind {
                Kind::Secret => {
                    out.insert("set".to_owned(), Value::Bool(js_truthy(&value)));
                    value = Value::String(String::new());
                }
                Kind::Urls => value = Value::Array(url_views(&value)),
                _ => {}
            }
            out.insert("value".to_owned(), value);
            settings.insert(spec.key.to_owned(), Value::Object(out));
        }
        let restart = SPEC
            .iter()
            .filter(|spec| !spec.live && self.differs(spec.key))
            .map(|spec| Value::String(spec.key.to_owned()))
            .collect();
        let mut out = Map::new();
        out.insert("settings".to_owned(), Value::Object(settings));
        out.insert("restartNeeded".to_owned(), Value::Array(restart));
        Value::Object(out)
    }

    /// Applies a patch to settings.json and returns the changed keys with the new view.
    pub fn update(&mut self, patch: &Value) -> Result<Value, SettingsError> {
        let Value::Object(patch) = patch else {
            return Err(SettingsError::new(400, "settings: body must be an object"));
        };
        let mut next = self.saved.clone();
        let mut changed: Vec<String> = Vec::new();
        for (key, raw) in patch {
            let Some(spec) = SPEC.iter().find(|spec| spec.key == key) else {
                return Err(bad(key, "unknown setting"));
            };
            let before = json_string(&self.read(key, &next));
            if raw.is_null() {
                next.remove(key);
            } else {
                // `#n` keeps a stored URL whose view was redacted.
                let value = if spec.kind == Kind::Urls {
                    resolve_refs(raw, &self.get(key))
                } else {
                    raw.clone()
                };
                next.insert(key.clone(), validate(key, spec, &value)?);
            }
            if json_string(&self.read(key, &next)) != before {
                changed.push(key.clone());
            }
        }
        // The guard runs its own HyperDHT node beside the relay's: a fixed port both ask for would leave one deaf.
        let guard_port = self.read("privateGuardPort", &next);
        let guard = self.read("privateGuard", &next);
        if js_truthy(&guard)
            && js_truthy(&guard_port)
            && js_same(&guard_port, &self.read("dhtPort", &next))
            && changed
                .iter()
                .any(|key| key == "dhtPort" || key == "privateGuardPort" || key == "privateGuard")
        {
            let key = if changed.iter().any(|key| key == "dhtPort") {
                "dhtPort"
            } else {
                "privateGuardPort"
            };
            return Err(bad(key, "the peer port and the guard port must differ"));
        }
        if json_string(&Value::Object(next.clone()))
            != json_string(&Value::Object(self.saved.clone()))
        {
            self.write(&next)?;
            self.saved = next;
        }
        let mut out = Map::new();
        out.insert(
            "changed".to_owned(),
            Value::Array(changed.into_iter().map(Value::String).collect()),
        );
        if let Value::Object(view) = self.view() {
            out.extend(view);
        }
        Ok(Value::Object(out))
    }

    /// Saves a value the running relay already uses (the tracker it founded) without flagging a restart.
    pub fn remember(&mut self, key: &str, value: &Value) -> Result<(), SettingsError> {
        if self.saved.contains_key(key) {
            return Ok(());
        }
        let mut patch = Map::new();
        patch.insert(key.to_owned(), value.clone());
        self.update(&Value::Object(patch))?;
        let applied = json_string(&self.get(key));
        self.applied.insert(key.to_owned(), Value::String(applied));
        Ok(())
    }

    /// What the running relay uses: a snapshot of the values taken before its node opened.
    pub fn snapshot(&self) -> Value {
        Value::Object(self.snapshot_map())
    }

    /// Marks a snapshot as what the running relay uses, clearing matching `restartNeeded` entries.
    pub fn mark_applied(&mut self, snapshot: &Value) {
        self.applied = match snapshot {
            Value::Object(snapshot) => snapshot.clone(),
            _ => Map::new(),
        };
    }

    pub fn is_live(&self, key: &str) -> bool {
        SPEC.iter()
            .find(|spec| spec.key == key)
            .is_some_and(|spec| spec.live)
    }

    fn read(&self, key: &str, from: &Map<String, Value>) -> Value {
        let Some(spec) = SPEC.iter().find(|spec| spec.key == key) else {
            return Value::Null;
        };
        if let Some(value) = from.get(key) {
            return value.clone();
        }
        match self.env_value(spec) {
            Some(raw) => parse_env(spec, raw),
            None => spec.def.value(),
        }
    }

    fn env_value(&self, spec: &Spec) -> Option<&str> {
        self.env
            .get(spec.env)
            .map(String::as_str)
            .filter(|raw| !raw.is_empty())
    }

    fn snapshot_map(&self) -> Map<String, Value> {
        SPEC.iter()
            .map(|spec| {
                (
                    spec.key.to_owned(),
                    Value::String(json_string(&self.get(spec.key))),
                )
            })
            .collect()
    }

    fn differs(&self, key: &str) -> bool {
        match self.applied.get(key).and_then(Value::as_str) {
            Some(applied) => applied != json_string(&self.get(key)),
            None => true,
        }
    }

    /// Writes tmp then renames, so a crash leaves the old file whole.
    fn write(&self, value: &Map<String, Value>) -> Result<(), SettingsError> {
        let text = serde_json::to_string_pretty(&Value::Object(value.clone()))
            .map_err(|err| SettingsError::new(500, err.to_string()))?;
        let mut tmp = self.file.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        let mut options = fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let write = || -> std::io::Result<()> {
            let mut file = options.open(&tmp)?;
            file.write_all(text.as_bytes())?;
            drop(file);
            fs::rename(&tmp, &self.file)
        };
        write().map_err(|err| SettingsError::new(500, err.to_string()))
    }
}

// shortcut: a hand-edited non-list URLs field views as empty, where the JS would throw.
fn url_views(value: &Value) -> Vec<Value> {
    value
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .enumerate()
        .map(|(index, url)| {
            let mut out = Map::new();
            out.insert("ref".to_owned(), Value::String(format!("#{index}")));
            out.insert(
                "label".to_owned(),
                Value::String(url_label(&js_string(url))),
            );
            Value::Object(out)
        })
        .collect()
}

/// `#n` keeps the stored URL the view redacted.
fn resolve_refs(raw: &Value, current: &Value) -> Value {
    let Some(items) = raw.as_array() else {
        return raw.clone();
    };
    let current = current.as_array();
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        if let Value::String(text) = item
            && let Some(index) = ref_index(text)
        {
            if let Some(value) = current.and_then(|list| list.get(index)) {
                out.push(value.clone());
            }
            continue;
        }
        out.push(item.clone());
    }
    Value::Array(out)
}

fn ref_index(text: &str) -> Option<usize> {
    let digits = text.strip_prefix('#')?;
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

fn js_same(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left.as_f64() == right.as_f64(),
        _ => left == right,
    }
}

/// What a view may show of a URL: host and path. Query strings, credentials and HiveRelay `=TOKEN` suffixes stay
/// private.
fn url_label(raw: &str) -> String {
    let plain = match raw.rfind('=') {
        Some(index) if !raw[index + 1..].contains('/') => &raw[..index],
        _ => raw,
    };
    label_of(plain).unwrap_or_else(|| "link".to_owned())
}

// shortcut: `URL` normalization here covers host and path only, which is all a label shows.
fn label_of(plain: &str) -> Option<String> {
    let colon = plain.find(':')?;
    let raw_scheme = &plain[..colon];
    if raw_scheme.is_empty()
        || !raw_scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        || !raw_scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'))
    {
        return None;
    }
    let scheme = raw_scheme.to_ascii_lowercase();
    let rest = &plain[colon + 1..];
    let (host, path) = match rest.strip_prefix("//") {
        Some(authority) => {
            let end = authority.find(['/', '?', '#']).unwrap_or(authority.len());
            let (authority, path) = authority.split_at(end);
            (host_of(authority, &scheme)?, path)
        }
        None => (String::new(), rest),
    };
    let path = match path.find(['?', '#']) {
        Some(end) => &path[..end],
        None => path,
    };
    Some(if path.is_empty() || path == "/" {
        host
    } else {
        format!("{host}{path}")
    })
}

fn host_of(authority: &str, scheme: &str) -> Option<String> {
    let host_port = match authority.rfind('@') {
        Some(at) => &authority[at + 1..],
        None => authority,
    };
    if host_port.is_empty() {
        return None;
    }
    let (host, port) = if host_port.starts_with('[') {
        let close = host_port.find(']')?;
        (
            &host_port[..=close],
            host_port[close + 1..].strip_prefix(':').unwrap_or(""),
        )
    } else {
        match host_port.rfind(':') {
            Some(colon) => (&host_port[..colon], &host_port[colon + 1..]),
            None => (host_port, ""),
        }
    };
    if host.is_empty() {
        return None;
    }
    let port = if port.is_empty() {
        None
    } else {
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        Some(port.parse::<u32>().ok().filter(|port| *port <= 65535)?)
    };
    let default = matches!((scheme, port), ("http", Some(80)) | ("https", Some(443)));
    let host = host.to_ascii_lowercase();
    Some(match port {
        Some(port) if !default => format!("{host}:{port}"),
        _ => host,
    })
}

fn validate(key: &str, spec: &Spec, value: &Value) -> Result<Value, SettingsError> {
    match spec.kind {
        Kind::Bool => match value {
            Value::Bool(_) => Ok(value.clone()),
            _ => Err(bad(key, "must be true or false")),
        },
        Kind::Port => match number_of(value) {
            Some(number) if number.fract() == 0.0 && (0.0..=65535.0).contains(&number) => {
                Ok(value.clone())
            }
            _ => Err(bad(key, "must be a port from 0 to 65535")),
        },
        Kind::Number => match number_of(value) {
            Some(number) if number.is_finite() && number >= spec.min && number <= spec.max => {
                Ok(value.clone())
            }
            _ => Err(bad(
                key,
                format!(
                    "must be a number from {} to {}",
                    number_text(spec.min),
                    number_text(spec.max)
                ),
            )),
        },
        Kind::Host => match value {
            Value::String(text) if text.is_empty() || is_host(text) => {
                Ok(Value::String(js_trim(text).to_owned()))
            }
            _ => Err(bad(key, "must be an IP address or host name")),
        },
        Kind::Key => match value {
            Value::String(text) if text.is_empty() || is_hex64(&text.to_lowercase()) => {
                Ok(Value::String(text.to_lowercase()))
            }
            _ => Err(bad(key, "must be a 64-character hex key")),
        },
        Kind::Path => match value {
            Value::String(text) if text.is_empty() || text.starts_with('/') => {
                Ok(Value::String(js_trim(text).to_owned()))
            }
            _ => Err(bad(key, "must be an absolute folder path")),
        },
        Kind::Secret => match value {
            Value::String(text) if text.encode_utf16().count() <= 512 => Ok(value.clone()),
            _ => Err(bad(key, "must be text")),
        },
        Kind::Url => {
            let Value::String(text) = value else {
                return Err(bad(key, "must be an http(s) URL"));
            };
            if !text.is_empty() && !is_http_url(text) {
                return Err(bad(key, "must be an http(s) URL"));
            }
            if text.is_empty() && !spec.empty {
                return Err(bad(key, "is required"));
            }
            Ok(Value::String(text.trim_end_matches('/').to_owned()))
        }
        Kind::Keys | Kind::Hosts | Kind::Urls => {
            let Some(items) = value.as_array() else {
                return Err(bad(key, "must be a list"));
            };
            if items.len() > 64 {
                return Err(bad(key, "must be a list"));
            }
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let Value::String(text) = item else {
                    return Err(bad(key, "items must be text"));
                };
                let text = js_trim(text);
                match spec.kind {
                    Kind::Keys => {
                        if !is_hex64(&text.to_lowercase()) {
                            return Err(bad(
                                key,
                                format!("{}… is not a 64-character hex key", slice_utf16(text, 16)),
                            ));
                        }
                        out.push(Value::String(text.to_lowercase()));
                    }
                    Kind::Hosts => {
                        if !is_hosts_entry(text) {
                            return Err(bad(key, format!("{text} must be host:port")));
                        }
                        out.push(Value::String(text.to_owned()));
                    }
                    _ => {
                        if !is_http_url(text) {
                            return Err(bad(
                                key,
                                format!("{} is not an http(s) URL", slice_utf16(text, 40)),
                            ));
                        }
                        out.push(Value::String(text.to_owned()));
                    }
                }
            }
            Ok(Value::Array(out))
        }
    }
}

fn number_of(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        _ => None,
    }
}

fn number_text(number: f64) -> String {
    if number.fract() == 0.0 && number.abs() < 1e21 {
        format!("{number:.0}")
    } else {
        number.to_string()
    }
}

/// `String#slice(0, limit)` counts UTF-16 code units.
fn slice_utf16(text: &str, limit: usize) -> String {
    let mut out = String::new();
    let mut used = 0;
    for c in text.chars() {
        used += c.len_utf16();
        if used > limit {
            break;
        }
        out.push(c);
    }
    out
}

/// `HOST`: an IP address or host name.
fn is_host(text: &str) -> bool {
    (1..=253).contains(&text.len())
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | ':' | '[' | ']' | '-'))
}

/// `^[a-z0-9.:[\]-]+:\d{1,5}$`, case-insensitively.
fn is_hosts_entry(text: &str) -> bool {
    let Some((host, port)) = text.rsplit_once(':') else {
        return false;
    };
    !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | ':' | '[' | ']' | '-'))
        && (1..=5).contains(&port.len())
        && port.bytes().all(|byte| byte.is_ascii_digit())
}

/// `^https?:\/\/[^\s]+$`, case-insensitively.
fn is_http_url(text: &str) -> bool {
    let rest = strip_scheme(text, "http://").or_else(|| strip_scheme(text, "https://"));
    rest.is_some_and(|rest| !rest.is_empty() && !rest.chars().any(is_js_space))
}

fn strip_scheme<'a>(text: &'a str, scheme: &str) -> Option<&'a str> {
    let head = text.get(..scheme.len())?;
    if head.eq_ignore_ascii_case(scheme) {
        text.get(scheme.len()..)
    } else {
        None
    }
}
