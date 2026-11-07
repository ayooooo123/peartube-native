//! `ios/bonjour.cjs`: Bonjour through the platform's daemon.
//!
//! iOS denies a listener raw multicast, so the app asks the native `NetService` layer
//! (`ios/Runtime/Bonjour.swift`) instead of opening a socket. This is that adapter over the same
//! messages: `publish`/`browse`/`stop` go out as `{event: "bonjour", operation, handle, record}`
//! lines, and the host answers `{event: "bonjour", handle, action: "up"|"down"|"error", …}`.
//!
//! `shortcut: ` only the LAN discovery's service type goes through the bridge; the cast
//! receiver's advertisement and the controller's browse still open `mdns-sd` daemons
//! (`src/cast/mdns.rs`), so they need raw multicast. Upgrade them the same way when the cast UI
//! has to work on a device that already needs this one.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use serde_json::{Map, Value, json};

use super::mdns::{
    DiscoveredService, ErrorSink, MdnsError, Record, SERVICE_PROTOCOL, SERVICE_TYPE, ServiceSink,
};

fn lock<T>(cell: &Mutex<T>) -> MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|error| error.into_inner())
}

/// Writes one Bonjour request to the host (`Bare.IPC.write` in `bonjour.cjs`).
type Writer = Arc<dyn Fn(Value) + Send + Sync>;

#[derive(Clone)]
enum Handle {
    Browse {
        up: ServiceSink,
        down: Option<ServiceSink>,
        on_error: Option<ErrorSink>,
    },
    Publish {
        on_error: Option<ErrorSink>,
    },
}

impl Handle {
    fn on_error(&self) -> Option<&ErrorSink> {
        match self {
            Self::Browse { on_error, .. } | Self::Publish { on_error } => on_error.as_ref(),
        }
    }
}

/// The platform's Bonjour daemon, driven over host messages.
pub struct HostAdapter {
    send: Writer,
    handles: Mutex<HashMap<u64, Handle>>,
    next: AtomicU64,
}

impl HostAdapter {
    /// `new Bonjour(options, onError)` with `send` writing to the host.
    pub fn new(send: impl Fn(Value) + Send + Sync + 'static) -> Self {
        Self {
            send: Arc::new(send),
            handles: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
        }
    }

    /// `bonjour.publish(record)`: the host registers it with the system daemon.
    pub fn advertise(
        self: &Arc<Self>,
        record: &Record,
        on_error: Option<ErrorSink>,
    ) -> Result<HostAdvertisement, MdnsError> {
        let id = self.next.fetch_add(1, Ordering::AcqRel) + 1;
        lock(&self.handles).insert(id, Handle::Publish { on_error });
        let txt: Map<String, Value> = record
            .txt
            .iter()
            .map(|(key, value)| (key.clone(), json!(value)))
            .collect();
        (self.send)(json!({
            "event": "bonjour",
            "operation": "publish",
            "handle": id,
            "record": {
                "name": record.name,
                "type": SERVICE_TYPE,
                "protocol": SERVICE_PROTOCOL,
                "port": record.port,
                "txt": txt,
            },
        }));
        Ok(HostAdvertisement {
            id,
            adapter: Arc::downgrade(self),
            stopped: AtomicBool::new(false),
        })
    }

    /// `bonjour.find(query)`: the host browses and re-resolves on its own (`startMonitoring`), so
    /// `rebrowse` is unused — `Worker.refreshDiscovery` restarts the host's browsers instead.
    pub fn browse(
        self: &Arc<Self>,
        up: ServiceSink,
        down: Option<ServiceSink>,
        on_error: Option<ErrorSink>,
    ) -> Result<HostBrowse, MdnsError> {
        let id = self.next.fetch_add(1, Ordering::AcqRel) + 1;
        lock(&self.handles).insert(id, Handle::Browse { up, down, on_error });
        (self.send)(json!({
            "event": "bonjour",
            "operation": "browse",
            "handle": id,
            "record": { "type": SERVICE_TYPE, "protocol": SERVICE_PROTOCOL },
        }));
        Ok(HostBrowse {
            id,
            adapter: Arc::downgrade(self),
            stopped: AtomicBool::new(false),
        })
    }

    /// One host answer (`Bare.on('appEvent')`): routed to the handle that asked for it.
    pub fn dispatch(&self, event: &Value) {
        let Some(id) = event.get("handle").and_then(Value::as_u64) else {
            return;
        };
        let handle = lock(&self.handles).get(&id).cloned();
        let Some(handle) = handle else {
            return;
        };
        match event.get("action").and_then(Value::as_str) {
            Some("error") => {
                if let (Some(on_error), Some(text)) = (
                    handle.on_error(),
                    event.get("error").and_then(Value::as_str),
                ) {
                    on_error(text);
                }
            }
            Some(action @ ("up" | "down")) => {
                let Handle::Browse { up, down, .. } = &handle else {
                    return;
                };
                let Some(service) = event.get("service").and_then(service) else {
                    return;
                };
                if action == "up" {
                    up(&service);
                } else if let Some(down) = down {
                    down(&service);
                }
            }
            _ => {}
        }
    }

    fn stop(&self, id: u64) {
        lock(&self.handles).remove(&id);
        (self.send)(json!({ "event": "bonjour", "operation": "stop", "handle": id }));
    }
}

/// A service the host resolved (`report()` in `Bonjour.swift`).
fn service(value: &Value) -> Option<DiscoveredService> {
    let name = value.get("name")?.as_str()?.to_owned();
    let port = value
        .get("port")
        .and_then(Value::as_u64)
        .and_then(|port| u16::try_from(port).ok())
        .unwrap_or(0);
    let addresses: Vec<Ipv4Addr> = value
        .get("addresses")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|address| address.as_str()?.parse::<Ipv4Addr>().ok())
        .collect();
    let txt = match value.get("txt") {
        Some(Value::Object(map)) => map
            .iter()
            .map(|(key, value)| {
                let value = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                (key.clone(), value)
            })
            .collect(),
        _ => Vec::new(),
    };
    Some(DiscoveredService {
        name,
        port,
        addresses,
        txt,
    })
}

/// A live publication on the host's daemon.
pub struct HostAdvertisement {
    id: u64,
    adapter: Weak<HostAdapter>,
    stopped: AtomicBool,
}

impl HostAdvertisement {
    /// `svc.stop()`.
    pub fn stop(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(adapter) = self.adapter.upgrade() {
            adapter.stop(self.id);
        }
    }
}

impl Drop for HostAdvertisement {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A live browse on the host's daemon.
pub struct HostBrowse {
    id: u64,
    adapter: Weak<HostAdapter>,
    stopped: AtomicBool,
}

impl HostBrowse {
    /// `browser.stop()`.
    pub fn stop(&self) {
        if self.stopped.swap(true, Ordering::AcqRel) {
            return;
        }
        if let Some(adapter) = self.adapter.upgrade() {
            adapter.stop(self.id);
        }
    }
}

impl Drop for HostBrowse {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lan::mdns::create_records_with;

    fn adapter() -> (Arc<HostAdapter>, Arc<Mutex<Vec<Value>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&sent);
        let adapter = Arc::new(HostAdapter::new(move |value| {
            lock(&sink).push(value);
        }));
        (adapter, sent)
    }

    fn record() -> Record {
        let tokens = vec![crate::lan::mdns::topic_token(b"x")];
        create_records_with(&"ab".repeat(32), 4242, &tokens, "n01")
            .into_iter()
            .next()
            .expect("one shard")
    }

    /// A browse asks the host once, and every answer lands in the subscriber it belongs to.
    #[test]
    fn host_answers_reach_the_browse_subscriber() {
        let (adapter, sent) = adapter();
        let seen: Arc<Mutex<Vec<DiscoveredService>>> = Arc::default();
        let errors: Arc<Mutex<Vec<String>>> = Arc::default();
        let up: ServiceSink = {
            let seen = Arc::clone(&seen);
            Arc::new(move |service| lock(&seen).push(service.clone()))
        };
        let on_error: ErrorSink = {
            let errors = Arc::clone(&errors);
            Arc::new(move |message| lock(&errors).push(message.to_owned()))
        };
        let browse = adapter
            .browse(up, None, Some(on_error))
            .expect("a browse handle");

        let request = lock(&sent)[0].clone();
        assert_eq!(request["event"], json!("bonjour"));
        assert_eq!(request["operation"], json!("browse"));
        assert_eq!(request["record"]["type"], json!(SERVICE_TYPE));
        assert_eq!(request["record"]["protocol"], json!(SERVICE_PROTOCOL));
        let handle = request["handle"].as_u64().expect("a handle");

        let record = record();
        let txt: Map<String, Value> = record
            .txt
            .iter()
            .map(|(key, value)| (key.clone(), json!(value)))
            .collect();
        adapter.dispatch(&json!({
            "event": "bonjour",
            "handle": handle,
            "action": "up",
            "service": {
                "name": record.name,
                "type": SERVICE_TYPE,
                "protocol": SERVICE_PROTOCOL,
                "port": record.port,
                "txt": txt,
                "addresses": ["10.0.0.9"],
                "host": "peartube.local.",
            },
        }));
        {
            let seen = lock(&seen);
            assert_eq!(seen.len(), 1, "the up sink saw the service");
            assert_eq!(seen[0].ipv4(), Some("10.0.0.9".parse().unwrap()));
            let parsed = seen[0].parse().expect("a PearTube record");
            assert_eq!(parsed.peer_key_hex, "ab".repeat(32));
            assert_eq!(parsed.port, 4242);
            assert_eq!(parsed.tokens, vec![crate::lan::mdns::topic_token(b"x")]);
        }

        adapter.dispatch(
            &json!({"event": "bonjour", "handle": handle, "action": "error", "error": "boom"}),
        );
        assert_eq!(lock(&errors).as_slice(), ["boom".to_owned()]);

        browse.stop();
        let outbound = lock(&sent);
        assert_eq!(outbound.last().expect("a stop")["operation"], json!("stop"));
        assert_eq!(outbound.last().expect("a stop")["handle"], json!(handle));
    }

    /// A publication carries the record the host publishes, and dropping it withdraws it once.
    #[test]
    fn host_publishes_and_stops_once() {
        let (adapter, sent) = adapter();
        let advertisement = adapter
            .advertise(&record(), None)
            .expect("an advertisement");
        {
            let sent = lock(&sent);
            assert_eq!(sent.len(), 1);
            let request = &sent[0];
            assert_eq!(request["operation"], json!("publish"));
            assert_eq!(request["record"]["type"], json!(SERVICE_TYPE));
            assert_eq!(request["record"]["protocol"], json!(SERVICE_PROTOCOL));
            assert_eq!(request["record"]["port"], json!(4242));
            assert_eq!(request["record"]["txt"]["peerKey"], json!("ab".repeat(32)));
            assert_eq!(request["record"]["name"], json!(record().name));
        }
        let handle = lock(&sent)[0]["handle"].as_u64().expect("a handle");
        advertisement.stop();
        drop(advertisement);
        let sent = lock(&sent);
        assert_eq!(sent.len(), 2, "stop is sent once: {sent:?}");
        assert_eq!(sent[1]["operation"], json!("stop"));
        assert_eq!(sent[1]["handle"], json!(handle));
    }
}
