//! Port of the apps' `src/app-settings.js`: validate the apps' settings.json fields and turn them into node options.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::mnemonic::{normalize_mnemonic, validate_mnemonic};
use crate::protocol::{ParsedUri, is_hex64, is_js_space, js_trim, parse_uri};

/// A followed channel as a settings file writes it: a key, its kind and the name the user gave it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelSource {
    pub key: String,
    /// Always `"channel"`: the only kind a settings form can follow.
    pub kind: String,
    pub alias: String,
}

/// The apps' settings.json, the same camelCase shape on the Mac, Android and iOS.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppSettings {
    pub tracker: String,
    pub mnemonic: String,
    pub sources: Vec<ChannelSource>,
    #[serde(rename = "relayThrough")]
    pub relay_through: Vec<String>,
    pub bootstrap: Vec<String>,
    #[serde(rename = "lanDiscovery")]
    pub lan_discovery: bool,
    #[serde(rename = "privateRouting")]
    pub private_routing: bool,
    #[serde(rename = "cacheMaxGb", with = "js_float")]
    pub cache_max_gb: f64,
}

/// A field failure; the message is the error the app shows the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppSettingsError(String);

impl AppSettingsError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    pub fn message(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AppSettingsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for AppSettingsError {}

/// `APP_SETTINGS_DEFAULTS`: the settings.json a first run starts from.
pub fn app_settings_defaults() -> AppSettings {
    AppSettings {
        tracker: String::new(),
        mnemonic: String::new(),
        sources: Vec::new(),
        relay_through: Vec::new(),
        bootstrap: Vec::new(),
        lan_discovery: true,
        private_routing: false,
        cache_max_gb: 10.0,
    }
}

/// `normalizeAppSettings`: form fields or a saved settings.json as complete settings.
///
/// A form must name at least one channel (`require_channel`); a first-run settings.json names none.
/// `previous` is the saved settings being edited, of which only the `sources` aliases are kept: a channel
/// listed again without a name keeps the name it had.
pub fn normalize_app_settings(
    raw: &Value,
    require_channel: bool,
    previous: Option<&Value>,
) -> Result<AppSettings, AppSettingsError> {
    let tracker = match raw.get("tracker") {
        Some(value) if js_truthy(value) => channel_spec(value)?.key,
        _ => String::new(),
    };
    let mnemonic = normalize_mnemonic(&js_string_or_empty(raw.get("mnemonic")));
    if !mnemonic.is_empty() && !validate_mnemonic(&mnemonic) {
        return Err(AppSettingsError::new(
            "Recovery phrase must be 12 or 24 English recovery words.",
        ));
    }
    let mut known: HashMap<String, String> = HashMap::new();
    if let Some(sources) = previous
        .and_then(|previous| previous.get("sources"))
        .and_then(Value::as_array)
    {
        for source in sources {
            let key = source
                .get("key")
                .map_or_else(|| "undefined".to_owned(), js_string)
                .to_lowercase();
            known.insert(key, js_string_or_empty(source.get("alias")));
        }
    }
    let mut sources: Vec<ChannelSource> = Vec::new();
    for token in list(raw.get("sources"), is_source_separator) {
        let mut source = channel_spec(&token)?;
        if source.alias.is_empty() {
            source.alias = known.get(&source.key).cloned().unwrap_or_default();
        }
        // A source equal to the tracker stays: adding it gives the node's own channel that name.
        if !sources.iter().any(|seen| seen.key == source.key) {
            sources.push(source);
        }
    }
    let relay_through: Vec<String> = list(raw.get("relayThrough"), is_space_or_comma)
        .iter()
        .map(|token| js_string(token).to_lowercase())
        .collect();
    if let Some(bad) = relay_through.iter().find(|key| !is_hex64(key)) {
        return Err(AppSettingsError::new(format!(
            "Relay key {bad} is not 64 hex characters."
        )));
    }
    let bootstrap: Vec<String> = list(raw.get("bootstrap"), is_space_or_comma)
        .iter()
        .map(js_string)
        .collect();
    if let Some(bad) = bootstrap.iter().find(|node| !is_host_port(node)) {
        return Err(AppSettingsError::new(format!(
            "Bootstrap node {bad} is not host:port."
        )));
    }
    if require_channel && tracker.is_empty() && mnemonic.is_empty() && sources.is_empty() {
        return Err(AppSettingsError::new(
            "Add a channel key, a 12-word recovery phrase, or a peartube://channel/ link.",
        ));
    }
    Ok(AppSettings {
        tracker,
        mnemonic,
        sources,
        relay_through,
        bootstrap,
        lan_discovery: match raw.get("lanDiscovery") {
            None => true,
            Some(value) => js_truthy(value),
        },
        private_routing: raw.get("privateRouting").is_some_and(js_truthy),
        cache_max_gb: cache_max_gb(raw.get("cacheMaxGb")),
    })
}

/// What the node needs to open: the folder under `trackers/` decides which identity.json it reads.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeOptions {
    pub storage: String,
    pub tracker: Option<String>,
    pub mnemonic: Option<String>,
    #[serde(rename = "join")]
    pub join: bool,
    pub sources: Vec<ChannelSource>,
    #[serde(rename = "relayThrough")]
    pub relay_through: Option<Vec<String>>,
    pub bootstrap: Option<Vec<String>>,
    pub lan: bool,
    #[serde(rename = "privateRouting")]
    pub private_routing: bool,
    #[serde(rename = "cacheMaxGb", with = "js_float")]
    pub cache_max_gb: f64,
}

/// `nodeOptions`: a fixed channel, a device linked by recovery phrase, or the platform's default folder.
pub fn node_options(
    settings: &AppSettings,
    trackers_dir: &str,
    default_folder: &str,
) -> NodeOptions {
    let folder = if !settings.tracker.is_empty() {
        settings.tracker.clone()
    } else if !settings.mnemonic.is_empty() {
        "linked".to_owned()
    } else {
        default_folder.to_owned()
    };
    NodeOptions {
        storage: join(trackers_dir, &folder),
        tracker: (!settings.tracker.is_empty()).then(|| settings.tracker.clone()),
        mnemonic: (!settings.mnemonic.is_empty()).then(|| settings.mnemonic.clone()),
        join: !settings.mnemonic.is_empty() && settings.tracker.is_empty(),
        sources: settings.sources.clone(),
        relay_through: (!settings.relay_through.is_empty()).then(|| settings.relay_through.clone()),
        bootstrap: (!settings.bootstrap.is_empty()).then(|| settings.bootstrap.clone()),
        lan: settings.lan_discovery,
        private_routing: settings.private_routing,
        cache_max_gb: settings.cache_max_gb,
    }
}

/// `JSON.stringify` prints integral numbers without a decimal point; serde would keep `10.0`.
mod js_float {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        if !value.is_finite() {
            return serializer.serialize_none();
        }
        if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_992.0 {
            return serializer.serialize_i64(*value as i64);
        }
        serializer.serialize_f64(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        f64::deserialize(deserializer)
    }
}

/// A saved channel as a settings field writes it: a 64-hex key, `channel:<key>` / `tracker:<key>`, a channel
/// link, or a saved `{ key, alias }`.
fn channel_spec(token: &Value) -> Result<ChannelSource, AppSettingsError> {
    if matches!(token, Value::Object(_) | Value::Array(_)) {
        let mut source = channel_spec(token.get("key").unwrap_or(&Value::Null))?;
        source.alias = js_string_or_empty(token.get("alias"));
        return Ok(source);
    }
    let raw = js_trim(&js_string_or_empty(Some(token))).to_owned();
    let lower = raw.to_lowercase();
    if let Some(key) = short_key(&lower) {
        return Ok(ChannelSource {
            key: key.to_owned(),
            kind: "channel".to_owned(),
            alias: String::new(),
        });
    }
    let parsed = parse_uri(&raw)
        .map_err(|_| AppSettingsError::new(format!("Invalid key or peartube:// URI: {raw}")))?;
    match parsed {
        ParsedUri::Source { key, alias, .. } => Ok(ChannelSource {
            key,
            kind: "channel".to_owned(),
            alias,
        }),
        _ => Err(AppSettingsError::new(format!(
            "Expected peartube://channel/..., got {raw}"
        ))),
    }
}

/// `^(?:peartube:)?(?:channel|tracker):([0-9a-f]{64})$` on an already lowercased token.
fn short_key(lower: &str) -> Option<&str> {
    let rest = lower.strip_prefix("peartube:").unwrap_or(lower);
    let rest = rest
        .strip_prefix("channel:")
        .or_else(|| rest.strip_prefix("tracker:"))?;
    is_hex64(rest).then_some(rest)
}

/// Form fields are text or lists of text; `split` separates a text field, list items stay as given.
fn list(raw: Option<&Value>, split: fn(char) -> bool) -> Vec<Value> {
    match raw {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| {
                if matches!(item, Value::Object(_) | Value::Array(_)) {
                    return Some(item.clone());
                }
                let text = js_trim(&js_string(item)).to_owned();
                (!text.is_empty()).then_some(Value::String(text))
            })
            .collect(),
        other => {
            let text = match other {
                Some(value) if !value.is_null() => js_string(value),
                _ => String::new(),
            };
            text.split(split)
                .map(|part| js_trim(part).to_owned())
                .filter(|part| !part.is_empty())
                .map(Value::String)
                .collect()
        }
    }
}

fn cache_max_gb(raw: Option<&Value>) -> f64 {
    match raw {
        None | Some(Value::Null) => 10.0,
        Some(Value::String(text)) if text.is_empty() => 10.0,
        Some(value) => {
            let number = match value {
                Value::Number(number) => number.as_f64().unwrap_or(0.0),
                Value::Bool(flag) => f64::from(u8::from(*flag)),
                other => js_number(&js_string(other)),
            };
            // `Number(raw) || 0`, then floor and clamp.
            let number = if number == 0.0 || number.is_nan() {
                0.0
            } else {
                number
            };
            number.floor().max(0.0)
        }
    }
}

/// `isHostPort`: a final `:port` in 1..=65535.
fn is_host_port(text: &str) -> bool {
    let Some(colon) = text.rfind(':') else {
        return false;
    };
    if colon == 0 {
        return false;
    }
    let port = js_number(&text[colon + 1..]);
    port.fract() == 0.0 && port > 0.0 && port < 65536.0
}

/// `/[\\r\\n,]+/` between sources.
fn is_source_separator(c: char) -> bool {
    matches!(c, '\r' | '\n' | ',')
}

/// `/[\s,]+/` between keys and hosts.
fn is_space_or_comma(c: char) -> bool {
    c == ',' || is_js_space(c)
}

/// `String(value || '')`: the empty string for missing, null and other falsy fields.
pub(crate) fn js_string_or_empty(value: Option<&Value>) -> String {
    match value {
        Some(value) if js_truthy(value) => js_string(value),
        _ => String::new(),
    }
}

/// `String(value)` for the JSON forms settings fields can hold.
pub(crate) fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => match number.as_f64() {
            Some(number) if number.is_finite() && number.fract() == 0.0 && number.abs() < 1e21 => {
                format!("{number:.0}")
            }
            Some(number) => number.to_string(),
            None => number.to_string(),
        },
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(|item| match item {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

/// JavaScript truthiness of a JSON value.
pub(crate) fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number
            .as_f64()
            .is_some_and(|number| number != 0.0 && !number.is_nan()),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// `Number(text)`: env values and port fields come in as text and are compared as JS numbers.
pub(crate) fn js_number(text: &str) -> f64 {
    let text = js_trim(text);
    if text.is_empty() {
        return 0.0;
    }
    let lower = text.to_ascii_lowercase();
    let (negative, body) = match lower.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, lower.strip_prefix('+').unwrap_or(&lower)),
    };
    if body == "infinity" {
        return if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }
    for (prefix, radix) in [("0x", 16), ("0b", 2), ("0o", 8)] {
        if let Some(digits) = body.strip_prefix(prefix) {
            if digits.is_empty() || !digits.chars().all(|c| c.is_digit(radix)) {
                return f64::NAN;
            }
            let number = digits.chars().fold(0.0, |number, c| {
                number * f64::from(radix) + f64::from(c.to_digit(radix).unwrap_or(0))
            });
            return if negative { -number } else { number };
        }
    }
    // shortcut: Rust also parses "inf"/"1_0"; env fields here are plain numbers.
    text.parse::<f64>().unwrap_or(f64::NAN)
}

/// `path.join` for a trackers folder: dot segments and duplicate slashes collapse.
fn join(base: &str, name: &str) -> String {
    let raw = match (base.is_empty(), name.is_empty()) {
        (true, true) => return ".".to_owned(),
        (true, false) => name.to_owned(),
        (false, true) => base.to_owned(),
        (false, false) => format!("{base}/{name}"),
    };
    let absolute = raw.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in raw.split('/') {
        match part {
            "" | "." => {}
            ".." => match parts.last() {
                Some(last) if *last != ".." => {
                    parts.pop();
                }
                None if !absolute => parts.push(".."),
                _ => {}
            },
            part => parts.push(part),
        }
    }
    let body = parts.join("/");
    if absolute {
        format!("/{body}")
    } else if body.is_empty() {
        ".".to_owned()
    } else {
        body
    }
}
