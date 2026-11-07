//! Port of the app's `src/node/identity.js`: a private `identity.json` beside `settings.json` that holds
//! the recovery phrase and subscriber epoch keys, the owner claim and epoch rotation the apps call.
//!
//! The file is `0600`, written to `.tmp` and renamed so a crash leaves the old one whole, and never goes
//! into the Corestore. A phrase option wins over the saved one and is fatal when invalid; a saved phrase
//! that no longer validates is ignored.

use std::fmt;
use std::fs;
use std::future::Future;
use std::path::PathBuf;

use serde_json::{Map, Value};

use crate::app_settings::{js_number, js_string, js_string_or_empty, js_truthy};
use crate::mnemonic::{
    Identity as Vault, MnemonicError, derive_identity, format_access_uri, generate_mnemonic,
    normalize_mnemonic, validate_mnemonic,
};
use crate::protocol::{clean_text, hex_encode, is_hex64, js_trim};

/// The slice of `ctx.channel` and `ctx.update` this module reads.
pub trait Channel {
    /// `ctx.channel.primaryKey`.
    fn primary_key(&self) -> String;

    /// `ctx.channel.channel.writable`.
    fn writable(&self) -> bool;

    /// `b4a.toString(ctx.channel.channel.local.key, 'hex')`.
    fn local_key(&self) -> String;

    /// `ctx.channel.append(op)`.
    fn append(&mut self, op: Value) -> impl Future<Output = ()>;

    /// `ctx.channel.currentChannelEpoch()`.
    fn current_channel_epoch(&mut self) -> impl Future<Output = u64>;

    /// `ctx.channel.localRole()`.
    fn local_role(&mut self) -> impl Future<Output = Option<String>>;

    /// `ctx.channel.listWriters()`.
    fn list_writers(&mut self) -> impl Future<Output = Vec<Value>>;

    /// `ctx.update()`.
    fn update(&mut self);
}

/// What `createIdentity(ctx)` reads.
pub struct IdentityContext<C> {
    /// `ctx.opts.storage`: the folder holding `identity.json`.
    pub storage: PathBuf,
    /// `ctx.opts.mnemonic`: an explicit phrase wins over the saved one.
    pub mnemonic: Option<String>,
    pub channel: C,
}

/// The message `identity.js` throws, as an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityError {
    message: String,
}

impl IdentityError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for IdentityError {}

impl From<MnemonicError> for IdentityError {
    fn from(error: MnemonicError) -> Self {
        Self::new(error.to_string())
    }
}

/// `createIdentity(ctx)`: the vault, the saved subscriber keys and the channel calls that use them.
pub struct Identity<C: Channel> {
    channel: C,
    file: PathBuf,
    access_keys: Map<String, Value>,
    current_mnemonic: String,
    vault: Option<Vault>,
    founded_key: String,
}

impl<C: Channel> Identity<C> {
    /// Reads `identity.json`; an unreadable or nonsensical file is treated as empty.
    pub fn new(ctx: IdentityContext<C>) -> Result<Self, IdentityError> {
        let file = ctx.storage.join("identity.json");
        let saved = fs::read_to_string(&file)
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
            .unwrap_or(Value::Object(Map::new()));
        let mut access_keys = Map::new();
        if let Some(entries) = saved.get("accessKeys") {
            for (key, value) in entries_of(entries) {
                let Value::String(hex) = value else { continue };
                let hex = hex.to_lowercase();
                if is_hex64(&hex) {
                    access_keys.insert(key.to_lowercase(), Value::String(hex));
                }
            }
        }
        let option_phrase = ctx.mnemonic.filter(|phrase| !phrase.is_empty());
        let saved_phrase = saved
            .get("mnemonic")
            .filter(|value| js_truthy(value))
            .map(|value| normalize_mnemonic(&js_string(value)))
            .unwrap_or_default();
        let mut current_mnemonic = match &option_phrase {
            Some(phrase) => normalize_mnemonic(phrase),
            None => saved_phrase,
        };
        if !current_mnemonic.is_empty() && !validate_mnemonic(&current_mnemonic) {
            if option_phrase.is_some() {
                return Err(MnemonicError.into());
            }
            current_mnemonic.clear();
        }
        let vault = if current_mnemonic.is_empty() {
            None
        } else {
            Some(derive_identity(&current_mnemonic)?)
        };
        let founded_key = match saved.get("founded") {
            Some(Value::String(hex)) if is_hex64(hex) => hex.clone(),
            _ => String::new(),
        };
        Ok(Self {
            channel: ctx.channel,
            file,
            access_keys,
            current_mnemonic,
            vault,
            founded_key,
        })
    }

    /// `idVault`.
    pub fn id_vault(&self) -> Option<&Vault> {
        self.vault.as_ref()
    }

    /// `ensureVault`: a fresh 12-word phrase when none is configured; nothing is saved yet.
    pub fn ensure_vault(&mut self) -> &Vault {
        if self.vault.is_none() {
            self.current_mnemonic = generate_mnemonic(12);
            self.vault = derive_identity(&self.current_mnemonic).ok();
        }
        self.vault.as_ref().expect("freshly generated phrase")
    }

    /// `foundedKey`.
    pub fn founded_key(&self) -> &str {
        &self.founded_key
    }

    /// `markFounded`.
    pub fn mark_founded(&mut self, key: &str) {
        self.founded_key = key.to_owned();
    }

    /// The saved subscriber epoch keys, keyed `channel:epoch` in insertion order.
    pub fn access_keys(&self) -> &Map<String, Value> {
        &self.access_keys
    }

    /// `saveIdentity`: tmp then rename with mode `0600`, errors ignored like the JS.
    pub fn save_identity(&self) {
        let mut payload = Map::new();
        payload.insert(
            "mnemonic".to_owned(),
            Value::String(self.current_mnemonic.clone()),
        );
        payload.insert(
            "founded".to_owned(),
            Value::String(self.founded_key.clone()),
        );
        payload.insert(
            "accessKeys".to_owned(),
            Value::Object(self.access_keys.clone()),
        );
        let Ok(text) = serde_json::to_string_pretty(&Value::Object(payload)) else {
            return;
        };
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
            use std::io::Write;
            let mut handle = options.open(&tmp)?;
            handle.write_all(text.as_bytes())?;
            drop(handle);
            fs::rename(&tmp, &self.file)
        };
        let _ = write();
    }

    /// `claimWithMnemonic`: adopt a phrase, then claim the open channel for it when it matches.
    pub async fn claim_with_mnemonic(
        &mut self,
        phrase: &str,
        alias: &str,
    ) -> Result<Value, IdentityError> {
        let next = derive_identity(phrase)?;
        self.current_mnemonic = next.mnemonic.clone();
        let channel_key = next.channel_key.clone();
        let primary = self.channel.primary_key();
        // Save the phrase before deciding: a phrase for another channel still replaces the saved one.
        self.vault = Some(next);
        self.save_identity();
        let mut out = Map::new();
        if channel_key != primary {
            out.insert("claimed".to_owned(), Value::Bool(false));
            out.insert("reopenNeeded".to_owned(), Value::Bool(true));
            out.insert("channel".to_owned(), Value::String(channel_key));
            return Ok(Value::Object(out));
        }
        if !self.channel.writable() {
            let writer = self.channel.local_key();
            let vault = self.vault.as_ref().expect("just derived");
            let mut op = Map::new();
            op.insert("type".to_owned(), Value::String("claim".to_owned()));
            op.insert(
                "sig".to_owned(),
                Value::String(vault.sign_claim(&primary, &writer)),
            );
            op.insert(
                "alias".to_owned(),
                Value::String(clean_text(Some(alias), "")),
            );
            self.channel.append(Value::Object(op)).await;
        }
        self.channel.update();
        out.insert("claimed".to_owned(), Value::Bool(true));
        out.insert("reopenNeeded".to_owned(), Value::Bool(false));
        out.insert("channel".to_owned(), Value::String(primary));
        out.insert("writer".to_owned(), Value::String(self.channel.local_key()));
        Ok(Value::Object(out))
    }

    /// `rotateEpoch`: append the next epoch and hand back its access link.
    pub async fn rotate_epoch(&mut self) -> Result<Value, IdentityError> {
        if self.vault.is_none() {
            return Err(IdentityError::new(
                "12-word recovery vault required to rotate subscriber epochs",
            ));
        }
        let next_epoch = self.channel.current_channel_epoch().await + 1;
        let mut op = Map::new();
        op.insert("type".to_owned(), Value::String("epoch".to_owned()));
        op.insert("epoch".to_owned(), Value::from(next_epoch));
        self.channel.append(Value::Object(op)).await;
        let primary = self.channel.primary_key();
        let epoch_key = hex_encode(
            &self
                .vault
                .as_ref()
                .expect("checked above")
                .epoch_key(&primary, next_epoch),
        );
        self.channel.update();
        let mut out = Map::new();
        out.insert("epoch".to_owned(), Value::from(next_epoch));
        out.insert("epochKey".to_owned(), Value::String(epoch_key.clone()));
        out.insert(
            "accessUri".to_owned(),
            Value::String(format_access_uri(&primary, next_epoch, &epoch_key)),
        );
        Ok(Value::Object(out))
    }

    /// `addAccessKey`: remember a paid epoch key from a `peartube://access/` link.
    pub async fn add_access_key(&mut self, request: &Value) -> Result<Value, IdentityError> {
        let channel = js_trim(&js_string_or_empty(request.get("channel"))).to_lowercase();
        let key = js_trim(&js_string_or_empty(request.get("key"))).to_lowercase();
        let epoch = js_number_of(request.get("epoch"));
        if !is_hex64(&channel) || !is_hex64(&key) || !is_safe_integer(epoch) || epoch < 1.0 {
            return Err(IdentityError::new("Invalid subscriber access key"));
        }
        let epoch = epoch as i64;
        self.access_keys
            .insert(format!("{channel}:{epoch}"), Value::String(key));
        self.save_identity();
        self.channel.update();
        let mut out = Map::new();
        out.insert("channel".to_owned(), Value::String(channel));
        out.insert("epoch".to_owned(), Value::from(epoch));
        Ok(Value::Object(out))
    }

    /// `identity`: the channel, this device's writer, role and epoch key for the apps.
    pub async fn identity(&mut self, reveal_mnemonic: bool) -> Result<Value, IdentityError> {
        let epoch = self.channel.current_channel_epoch().await;
        let role = self.channel.local_role().await;
        let primary = self.channel.primary_key();
        let manages_epoch = self
            .vault
            .as_ref()
            .is_some_and(|vault| primary == self.founded_key || vault.channel_key == primary);
        let epoch_key = match &self.vault {
            Some(vault) if manages_epoch => hex_encode(&vault.epoch_key(&primary, epoch)),
            _ => String::new(),
        };
        let writable = self.channel.writable() || role.as_deref() == Some("owner");
        let writers = self.channel.list_writers().await;
        let mut out = Map::new();
        out.insert("channel".to_owned(), Value::String(primary.clone()));
        out.insert("writer".to_owned(), Value::String(self.channel.local_key()));
        out.insert("role".to_owned(), role.map_or(Value::Null, Value::String));
        out.insert("writable".to_owned(), Value::Bool(writable));
        out.insert(
            "mnemonicSet".to_owned(),
            Value::Bool(!self.current_mnemonic.is_empty()),
        );
        if reveal_mnemonic {
            out.insert(
                "mnemonic".to_owned(),
                Value::String(self.current_mnemonic.clone()),
            );
        }
        out.insert("writers".to_owned(), Value::Array(writers));
        out.insert("epoch".to_owned(), Value::from(epoch));
        if !epoch_key.is_empty() {
            out.insert("epochKey".to_owned(), Value::String(epoch_key.clone()));
            out.insert(
                "accessUri".to_owned(),
                Value::String(format_access_uri(&primary, epoch, &epoch_key)),
            );
        }
        Ok(Value::Object(out))
    }
}

/// `Object.entries(value)` for the forms a hand-written `accessKeys` can hold.
fn entries_of(value: &Value) -> Vec<(String, Value)> {
    match value {
        Value::Object(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        Value::Array(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| (index.to_string(), value.clone()))
            .collect(),
        Value::String(text) => text
            .chars()
            .enumerate()
            .map(|(index, c)| (index.to_string(), Value::String(c.to_string())))
            .collect(),
        _ => Vec::new(),
    }
}

/// `Number(value)` for a field that arrives as JSON.
fn js_number_of(value: Option<&Value>) -> f64 {
    let Some(value) = value else { return f64::NAN };
    match value {
        Value::Null => 0.0,
        Value::Bool(flag) => f64::from(u8::from(*flag)),
        Value::Number(number) => number.as_f64().unwrap_or(f64::NAN),
        Value::String(text) => js_number(text),
        Value::Array(_) => js_number(&js_string(value)),
        Value::Object(_) => f64::NAN,
    }
}

/// `Number.isSafeInteger`.
fn is_safe_integer(number: f64) -> bool {
    number.is_finite() && number.fract() == 0.0 && number.abs() <= 9_007_199_254_740_991.0
}
