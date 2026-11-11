//! Saved receivers (`cast-receivers.json`), the receiver a cast targets, and the merged receiver
//! listing, ported from `createCastController`'s `saved` map, `pairedWith`, `receiverTarget` and
//! `listReceivers`.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::pairing::{PairingError, write_json_atomic};
use super::traits::LanAddress;

/// One entry of `cast-receivers.json`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SavedReceiver {
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub public_key: String,
    #[serde(default)]
    pub token: String,
}

/// The saved map, in insertion order like the JavaScript `Map`.
#[derive(Debug, Clone, Default)]
pub struct SavedReceivers {
    entries: Vec<SavedReceiver>,
}

impl SavedReceivers {
    /// `join(storage, 'cast-receivers.json')`.
    pub fn file(storage: &Path) -> PathBuf {
        storage.join("cast-receivers.json")
    }

    /// Reads the file; a broken or missing one is an empty map.
    pub fn load(storage: &Path) -> Self {
        let mut out = Self::default();
        let Ok(bytes) = fs::read(Self::file(storage)) else {
            return out;
        };
        let Ok(items) = serde_json::from_slice::<Vec<Value>>(&bytes) else {
            return out;
        };
        for item in items {
            let Ok(record) = serde_json::from_value::<SavedReceiver>(item) else {
                continue;
            };
            if !record.id.is_empty() {
                out.set(record);
            }
        }
        out
    }

    /// `writeJsonAtomic(savedFile, [...saved.values()])`.
    pub fn save(&self, storage: &Path) -> Result<(), PairingError> {
        let items: Vec<Value> = self
            .entries
            .iter()
            .map(|record| serde_json::to_value(record).unwrap_or(Value::Null))
            .collect();
        write_json_atomic(&Self::file(storage), &Value::Array(items))
    }

    /// `saved.set(id, record)`, keeping the key's original position.
    pub fn set(&mut self, record: SavedReceiver) {
        match self.entries.iter_mut().find(|item| item.id == record.id) {
            Some(slot) => *slot = record,
            None => self.entries.push(record),
        }
    }

    pub fn get(&self, id: &str) -> Option<&SavedReceiver> {
        self.entries.iter().find(|item| item.id == id)
    }

    /// `[...saved.values()].find(s => s.publicKey && s.publicKey === publicKey)`.
    pub fn find_by_public_key(&self, public_key: &str) -> Option<&SavedReceiver> {
        if public_key.is_empty() {
            return None;
        }
        self.entries
            .iter()
            .find(|item| !item.public_key.is_empty() && item.public_key == public_key)
    }

    /// `saved.delete(id)`.
    pub fn remove(&mut self, id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|item| item.id != id);
        self.entries.len() != before
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
    pub public_key: String,
    /// Where the receiver's node answers on this LAN (`h` + `lp`).
    pub lan: Option<LanAddress>,
    /// `open` or `code`.
    pub auth: String,
    /// Receivers from before the cast channel advertise v1 and cannot be reached.
    pub current: bool,
}

impl DiscoveredReceiver {
    /// `auth === 'open'`.
    pub fn open(&self) -> bool {
        self.auth == "open"
    }
}

/// One row of `listReceivers()`.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListedReceiver {
    pub id: String,
    pub name: String,
    pub host: Option<String>,
    pub public_key: String,
    pub online: bool,
    pub paired: bool,
    pub p2p: bool,
    pub transport: &'static str,
    pub auth: &'static str,
    /// False for a v1 receiver: it has no cast channel to reach.
    pub current: bool,
}

/// What is known about a receiver: what pairing saved and what mDNS shows now.
#[derive(Debug, Clone, Default)]
pub struct ReceiverTarget {
    pub public_key: String,
    pub lan: Option<LanAddress>,
    pub current: bool,
    pub token: String,
    pub name: String,
    pub auth: String,
}

/// `pairedWith(disc)`: the saved record for that id, else one with its key.
pub fn paired_with<'a>(
    saved: &'a SavedReceivers,
    disc: &DiscoveredReceiver,
) -> Option<&'a SavedReceiver> {
    saved
        .get(&disc.id)
        .or_else(|| saved.find_by_public_key(&disc.public_key))
}

/// `receiverTarget(receiverId)`. The saved key wins: mDNS is unsigned.
pub fn receiver_target(
    receiver_id: &str,
    discovered: &[DiscoveredReceiver],
    saved: &SavedReceivers,
) -> ReceiverTarget {
    let disc = discovered.iter().find(|item| item.id == receiver_id);
    let pair = saved
        .get(receiver_id)
        .or_else(|| disc.and_then(|disc| paired_with(saved, disc)));
    let public_key = pair
        .map(|pair| pair.public_key.clone())
        .filter(|key| !key.is_empty())
        .or_else(|| disc.map(|disc| disc.public_key.clone()))
        .unwrap_or_default();
    ReceiverTarget {
        public_key,
        lan: disc.and_then(|disc| disc.lan.clone()),
        current: disc.is_none_or(|disc| disc.current),
        token: pair.map(|pair| pair.token.clone()).unwrap_or_default(),
        name: disc
            .map(|disc| disc.name.clone())
            .filter(|name| !name.is_empty())
            .or_else(|| pair.map(|pair| pair.name.clone()))
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| "PearTube TV".to_owned()),
        auth: disc
            .map(|disc| disc.auth.clone())
            .unwrap_or_else(|| "code".to_owned()),
    }
}

fn upsert(rows: &mut Vec<(String, ListedReceiver)>, key: String, row: ListedReceiver) {
    match rows.iter_mut().find(|(id, _)| *id == key) {
        Some(slot) => slot.1 = row,
        None => rows.push((key, row)),
    }
}

/// `listReceivers()`: the discovered LAN receivers first, then the saved pairings not seen there.
pub fn list_receivers(
    discovered: &[DiscoveredReceiver],
    saved: &SavedReceivers,
) -> Vec<ListedReceiver> {
    let mut rows: Vec<(String, ListedReceiver)> = Vec::new();
    for disc in discovered {
        let pair = paired_with(saved, disc);
        let id = pair
            .map(|pair| pair.id.clone())
            .unwrap_or_else(|| disc.id.clone());
        let public_key = pair
            .map(|pair| pair.public_key.clone())
            .filter(|key| !key.is_empty())
            .unwrap_or_else(|| disc.public_key.clone());
        upsert(
            &mut rows,
            id.clone(),
            ListedReceiver {
                id,
                name: if disc.name.is_empty() {
                    pair.map(|pair| pair.name.clone())
                        .filter(|name| !name.is_empty())
                        .unwrap_or_else(|| "PearTube TV".to_owned())
                } else {
                    disc.name.clone()
                },
                host: Some(disc.host.clone()),
                public_key,
                online: true,
                paired: pair.is_some_and(|pair| !pair.token.is_empty()) || disc.open(),
                p2p: true,
                transport: "lan+p2p",
                auth: if disc.open() { "open" } else { "code" },
                current: disc.current,
            },
        );
    }
    for record in saved.iter() {
        if rows.iter().any(|(id, _)| *id == record.id) || record.public_key.is_empty() {
            continue;
        }
        upsert(
            &mut rows,
            record.id.clone(),
            ListedReceiver {
                id: record.id.clone(),
                name: if record.name.is_empty() {
                    "Paired TV".to_owned()
                } else {
                    record.name.clone()
                },
                host: None,
                public_key: record.public_key.clone(),
                online: true,
                paired: !record.token.is_empty(),
                p2p: true,
                transport: "p2p",
                auth: "code",
                current: true,
            },
        );
    }
    rows.into_iter().map(|(_, row)| row).collect()
}

/// The record `pairReceiver` saves, in the app's field order.
pub fn record_json(record: &SavedReceiver) -> Value {
    json!({
        "id": record.id,
        "name": record.name,
        "publicKey": record.public_key,
        "token": record.token
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disc(id: &str, key: &str, auth: &str, current: bool) -> DiscoveredReceiver {
        DiscoveredReceiver {
            id: id.to_owned(),
            name: format!("TV {id}"),
            host: "192.168.1.20".to_owned(),
            public_key: key.to_owned(),
            lan: Some(LanAddress {
                host: "192.168.1.20".to_owned(),
                port: 40123,
            }),
            auth: auth.to_owned(),
            current,
        }
    }

    fn record(id: &str, key: &str, token: &str) -> SavedReceiver {
        SavedReceiver {
            id: id.to_owned(),
            name: "Kitchen TV".to_owned(),
            public_key: key.to_owned(),
            token: token.to_owned(),
        }
    }

    #[test]
    fn a_discovered_receiver_merges_with_its_saved_pairing() {
        let key = "aa".repeat(32);
        let mut saved = SavedReceivers::default();
        saved.set(record("peer-1", &key, "tok"));
        let rows = list_receivers(&[disc("peer-1", &key, "code", true)], &saved);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "peer-1");
        assert!(rows[0].paired);
        assert_eq!(rows[0].transport, "lan+p2p");
        assert!(rows[0].current);
    }

    #[test]
    fn a_v1_receiver_is_listed_but_not_current() {
        let rows = list_receivers(
            &[disc("old", "", "code", false)],
            &SavedReceivers::default(),
        );
        assert_eq!(rows.len(), 1);
        assert!(!rows[0].current);
        assert_eq!(rows[0].transport, "lan+p2p");
        assert!(!rows[0].paired);
    }

    #[test]
    fn an_open_receiver_counts_as_paired() {
        let rows = list_receivers(
            &[disc("tv", &"bb".repeat(32), "open", true)],
            &SavedReceivers::default(),
        );
        assert!(rows[0].paired);
        assert_eq!(rows[0].auth, "open");
    }

    #[test]
    fn a_saved_receiver_not_discovered_is_a_p2p_target() {
        let mut saved = SavedReceivers::default();
        saved.set(record("peer-9", &"cc".repeat(32), "tok"));
        let rows = list_receivers(&[], &saved);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].transport, "p2p");
        assert_eq!(rows[0].host, None);
        assert!(rows[0].paired);
        assert!(rows[0].current);
    }

    #[test]
    fn the_saved_key_wins_over_mdns() {
        let mut saved = SavedReceivers::default();
        saved.set(record("peer-1", &"dd".repeat(32), "tok"));
        let target = receiver_target(
            "peer-1",
            &[disc("peer-1", &"ee".repeat(32), "code", false)],
            &saved,
        );
        assert_eq!(target.public_key, "dd".repeat(32));
        assert_eq!(target.token, "tok");
        assert!(!target.current, "the discovered receiver's build decides");
    }

    #[test]
    fn a_receiver_known_only_by_a_saved_key_is_current() {
        let mut saved = SavedReceivers::default();
        saved.set(record("peer-2", &"ff".repeat(32), "tok"));
        let target = receiver_target("peer-2", &[], &saved);
        assert_eq!(target.public_key, "ff".repeat(32));
        assert!(target.current);
        assert!(target.lan.is_none());
    }
}
