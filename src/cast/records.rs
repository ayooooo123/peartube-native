//! Saved receivers (`cast-receivers.json`), the address parser and the merged receiver listing,
//! ported from `createCastController`'s `saved` map, `receiverAddress` and `listReceivers`.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

use crate::protocol::{RECEIVER_PORT, clean_name, is_hex64};

use super::pairing::{PairingError, write_json_atomic};

/// One entry of `cast-receivers.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedReceiver {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u64,
    #[serde(default, rename = "publicKey")]
    pub public_key: String,
    #[serde(default)]
    pub token: String,
    #[serde(default)]
    pub p2p: bool,
    /// A Smart TV browser receiver the hub hosts itself.
    #[serde(default, skip_serializing_if = "is_false")]
    pub web_hosted: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The saved map, in insertion order like the JavaScript `Map`.
#[derive(Debug, Clone, Default)]
pub struct SavedReceivers {
    entries: Vec<SavedReceiver>,
}

impl SavedReceivers {
    pub fn file(storage: &Path) -> std::path::PathBuf {
        storage.join("cast-receivers.json")
    }

    /// Reads the saved array; unreadable or malformed files start empty, as in the app.
    pub fn load(storage: &Path) -> Self {
        let mut entries = Vec::new();
        if let Ok(bytes) = fs::read(Self::file(storage))
            && let Ok(list) = serde_json::from_slice::<Vec<SavedReceiver>>(&bytes)
        {
            for item in list {
                if !item.id.is_empty() {
                    entries.push(item);
                }
            }
        }
        Self { entries }
    }

    pub fn save(&self, storage: &Path) -> Result<(), PairingError> {
        fs::create_dir_all(storage)?;
        let value = serde_json::to_value(&self.entries)?;
        write_json_atomic(&Self::file(storage), &value)
    }

    /// `saved.set(id, record)`: an existing id keeps its position.
    pub fn set(&mut self, record: SavedReceiver) {
        match self.entries.iter_mut().find(|e| e.id == record.id) {
            Some(slot) => *slot = record,
            None => self.entries.push(record),
        }
    }

    pub fn get(&self, id: &str) -> Option<&SavedReceiver> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn find_by_public_key(&self, public_key: &str) -> Option<&SavedReceiver> {
        (!public_key.is_empty())
            .then(|| {
                self.entries
                    .iter()
                    .find(|e| !e.public_key.is_empty() && e.public_key == public_key)
            })
            .flatten()
    }

    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.id != id);
        before != self.entries.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &SavedReceiver> {
        self.entries.iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// One receiver seen on the LAN over mDNS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReceiver {
    pub id: String,
    pub name: String,
    pub host: String,
    pub port: u64,
    #[serde(default)]
    pub public_key: String,
    #[serde(default)]
    pub p2p: bool,
    /// `open` or `code`.
    #[serde(default)]
    pub auth: String,
    #[serde(default)]
    pub seen_at: u64,
}

/// A Smart TV browser receiver the hub hosts itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebListing {
    pub id: String,
    pub name: String,
    pub clients: usize,
}

/// One row of `listReceivers()`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedReceiver {
    pub id: String,
    pub name: String,
    pub host: Option<String>,
    pub port: u64,
    pub public_key: String,
    pub online: bool,
    pub paired: bool,
    pub p2p: bool,
    pub transport: &'static str,
    pub auth: &'static str,
}

/// The address a user typed for a screen: a 64-hex key, or `host[:port]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiverAddress {
    pub host: String,
    pub port: u16,
    pub public_key: String,
}

/// `receiverAddress`.
pub fn receiver_address(address: &str) -> Result<ReceiverAddress, String> {
    let mut a = address.trim().to_lowercase();
    if let Some(rest) = a.strip_prefix("http://") {
        a = rest.to_owned();
    } else if let Some(rest) = a.strip_prefix("https://") {
        a = rest.to_owned();
    }
    if let Some(slash) = a.find('/') {
        a.truncate(slash);
    }
    if is_hex64(&a) {
        return Ok(ReceiverAddress {
            host: String::new(),
            port: 0,
            public_key: a,
        });
    }
    let Some(colon) = a.rfind(':') else {
        if a.is_empty() {
            return Err("Enter a receiver link, host:port or 64-hex key".to_owned());
        }
        return Ok(ReceiverAddress {
            host: a,
            port: RECEIVER_PORT,
            public_key: String::new(),
        });
    };
    if colon == 0 {
        return Err("Enter a receiver link, host:port or 64-hex key".to_owned());
    }
    let port = a[colon + 1..].parse::<u16>().unwrap_or(RECEIVER_PORT);
    Ok(ReceiverAddress {
        host: a[..colon].to_owned(),
        port: if port == 0 { RECEIVER_PORT } else { port },
        public_key: String::new(),
    })
}

fn upsert(rows: &mut Vec<(String, ListedReceiver)>, key: String, row: ListedReceiver) {
    match rows.iter_mut().find(|(id, _)| *id == key) {
        Some(slot) => slot.1 = row,
        None => rows.push((key, row)),
    }
}

/// `listReceivers()`: hosted web receivers, then discovered LAN receivers, then saved pairings.
pub fn list_receivers(
    web: &[WebListing],
    discovered: &[DiscoveredReceiver],
    saved: &SavedReceivers,
    active_id: Option<&str>,
    lan_host: Option<&str>,
    hub_port: u16,
) -> Vec<ListedReceiver> {
    let mut rows: Vec<(String, ListedReceiver)> = Vec::new();
    let fallback_host = lan_host.unwrap_or("127.0.0.1").to_owned();
    for rec in web {
        if rec.clients == 0 && active_id != Some(rec.id.as_str()) {
            continue;
        }
        upsert(
            &mut rows,
            rec.id.clone(),
            ListedReceiver {
                id: rec.id.clone(),
                name: rec.name.clone(),
                host: Some(fallback_host.clone()),
                port: u64::from(hub_port),
                public_key: String::new(),
                online: rec.clients > 0,
                paired: true,
                p2p: false,
                transport: "web",
                auth: "open",
            },
        );
    }
    for d in discovered {
        let pair = saved
            .get(&d.id)
            .or_else(|| saved.find_by_public_key(&d.public_key));
        let id = pair.map(|p| p.id.clone()).unwrap_or_else(|| d.id.clone());
        upsert(
            &mut rows,
            id.clone(),
            ListedReceiver {
                id,
                name: if d.name.is_empty() {
                    pair.map(|p| p.name.clone())
                        .unwrap_or_else(|| "PearTube TV".to_owned())
                } else {
                    d.name.clone()
                },
                host: Some(d.host.clone()),
                port: d.port,
                public_key: if d.public_key.is_empty() {
                    pair.map(|p| p.public_key.clone()).unwrap_or_default()
                } else {
                    d.public_key.clone()
                },
                online: true,
                paired: pair.is_some_and(|p| !p.token.is_empty()) || d.auth == "open",
                p2p: d.p2p || pair.is_some_and(|p| p.p2p),
                transport: if !d.public_key.is_empty()
                    || pair.is_some_and(|p| !p.public_key.is_empty())
                {
                    "lan+p2p"
                } else {
                    "lan"
                },
                auth: if d.auth == "open" { "open" } else { "code" },
            },
        );
    }
    for s in saved.iter() {
        if s.web_hosted || rows.iter().any(|(id, _)| *id == s.id) {
            continue;
        }
        let same_address = !s.host.is_empty()
            && s.port > 0
            && discovered
                .iter()
                .any(|d| d.host == s.host && d.port == s.port);
        if same_address {
            continue;
        }
        upsert(
            &mut rows,
            s.id.clone(),
            ListedReceiver {
                id: s.id.clone(),
                name: if s.name.is_empty() {
                    "Paired TV".to_owned()
                } else {
                    s.name.clone()
                },
                host: (!s.host.is_empty()).then(|| s.host.clone()),
                port: s.port,
                public_key: s.public_key.clone(),
                online: !s.public_key.is_empty() || active_id == Some(s.id.as_str()),
                paired: !s.token.is_empty(),
                p2p: s.p2p || !s.public_key.is_empty(),
                transport: if !s.public_key.is_empty() {
                    if s.host.is_empty() { "p2p" } else { "lan+p2p" }
                } else {
                    "lan"
                },
                auth: "code",
            },
        );
    }
    rows.into_iter().map(|(_, row)| row).collect()
}

/// The `{deviceId, name, publicKey, port, token, capabilities}` pair answer, in the app's order.
pub fn pair_payload(
    device_id: &str,
    name: &str,
    public_key: &str,
    swarm_key: &str,
    port: u16,
    token: &str,
    capabilities: &Map<String, Value>,
) -> Value {
    let mut out = Map::new();
    out.insert("deviceId".into(), json!(device_id));
    out.insert("name".into(), json!(clean_name(Some(name), "PearTube TV")));
    out.insert("publicKey".into(), json!(public_key));
    out.insert("swarmKey".into(), json!(swarm_key));
    out.insert("port".into(), json!(port));
    out.insert("token".into(), json!(token));
    out.insert("capabilities".into(), Value::Object(capabilities.clone()));
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_parse_like_the_app() {
        let key = "a".repeat(64);
        assert_eq!(receiver_address(&key).unwrap().public_key, key);
        let typed = receiver_address("192.168.1.5").unwrap();
        assert_eq!(typed.host, "192.168.1.5");
        assert_eq!(typed.port, RECEIVER_PORT);
        let with_port = receiver_address("http://tv.local:9000/tv").unwrap();
        assert_eq!(
            (with_port.host.as_str(), with_port.port),
            ("tv.local", 9000)
        );
        assert!(receiver_address("").is_err());
        assert!(receiver_address(":1234").is_err());
    }

    #[test]
    fn saved_receivers_keep_insertion_order_and_positions() {
        let mut saved = SavedReceivers::default();
        saved.set(SavedReceiver {
            id: "a".into(),
            name: "A".into(),
            ..Default::default()
        });
        saved.set(SavedReceiver {
            id: "b".into(),
            name: "B".into(),
            ..Default::default()
        });
        saved.set(SavedReceiver {
            id: "a".into(),
            name: "A2".into(),
            ..Default::default()
        });
        let ids: Vec<&str> = saved.iter().map(|e| e.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(saved.get("a").unwrap().name, "A2");
    }

    #[test]
    fn listing_merges_web_discovered_and_saved() {
        let mut saved = SavedReceivers::default();
        saved.set(SavedReceiver {
            id: "tv1".into(),
            name: "TV".into(),
            host: "192.168.1.9".into(),
            port: 8176,
            public_key: "b".repeat(64),
            token: "t".into(),
            p2p: true,
            web_hosted: false,
        });
        let discovered = vec![DiscoveredReceiver {
            id: "tv1".into(),
            name: "TV".into(),
            host: "192.168.1.9".into(),
            port: 8176,
            public_key: "b".repeat(64),
            p2p: true,
            auth: "code".into(),
            seen_at: 1,
        }];
        let web = vec![WebListing {
            id: "web-1".into(),
            name: "Browser".into(),
            clients: 1,
        }];
        let rows = list_receivers(&web, &discovered, &saved, None, Some("192.168.1.2"), 8177);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].transport, "web");
        assert_eq!(rows[1].id, "tv1");
        assert!(rows[1].paired);
        assert_eq!(rows[1].transport, "lan+p2p");
    }
}
