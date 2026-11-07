//! mDNS advertising and discovery for `peartube-cast`, ported from `refreshMdns` and
//! `refreshBrowse` in `src/cast.js`.
//!
//! The app reaches mDNS through the platform's bonjour adapter; the port binds one `mdns-sd`
//! daemon per role, which is enough for a LAN TV (see `docs/port-notes-lan.md` for the LAN
//! HyperDHT half, whose records this module does not touch).

use mdns_sd::RecvTimeoutError;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};

use super::CAST_SERVICE_TYPE;
use super::records::DiscoveredReceiver;

/// `_peartube-cast._tcp.local.`
pub fn service_type() -> String {
    format!("_{CAST_SERVICE_TYPE}._tcp.local.")
}

/// What `advertise` publishes.
#[derive(Debug, Clone)]
pub struct AdvertiseInfo {
    /// Instance name, `<name> (<deviceId[0..6]>)` as the app builds it.
    pub name: String,
    pub host: IpAddr,
    pub port: u16,
    pub txt: Vec<(String, String)>,
}

/// A live advertisement; dropping it unregisters the service.
pub struct AdvertiseHandle {
    daemon: ServiceDaemon,
    fullname: String,
}

impl AdvertiseHandle {
    pub fn fullname(&self) -> &str {
        &self.fullname
    }

    /// `adHandle.stop()`.
    pub fn stop(&self) {
        let _ = self.daemon.unregister(&self.fullname);
    }
}

impl Drop for AdvertiseHandle {
    fn drop(&mut self) {
        self.stop();
        let _ = self.daemon.shutdown();
    }
}

/// The TXT record `refreshMdns` publishes.
pub fn advertisement_txt(
    device_id: &str,
    name: &str,
    host: Option<&str>,
    public_key: &str,
    p2p: bool,
    open_auth: bool,
) -> Vec<(String, String)> {
    vec![
        ("proto".to_owned(), CAST_SERVICE_TYPE.to_owned()),
        ("v".to_owned(), "1".to_owned()),
        ("id".to_owned(), device_id.to_owned()),
        ("name".to_owned(), name.to_owned()),
        (
            "h".to_owned(),
            host.filter(|h| !h.is_empty())
                .unwrap_or("127.0.0.1")
                .to_owned(),
        ),
        ("pk".to_owned(), public_key.to_owned()),
        ("p2p".to_owned(), if p2p { "1" } else { "0" }.to_owned()),
        (
            "auth".to_owned(),
            if open_auth { "open" } else { "code" }.to_owned(),
        ),
    ]
}

/// The instance name the app registers.
pub fn instance_name(name: &str, device_id: &str) -> String {
    let short: String = device_id.chars().take(6).collect();
    format!("{name} ({short})")
}

/// `refreshMdns`: publish the receiver, replacing any previous advertisement.
pub fn advertise(info: AdvertiseInfo) -> Result<AdvertiseHandle, String> {
    let daemon = ServiceDaemon::new().map_err(|err| err.to_string())?;
    let txt: Vec<(&str, &str)> = info
        .txt
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let host = format!(
        "{}.local.",
        match info.host {
            IpAddr::V4(v4) => v4.to_string().replace('.', "-"),
            IpAddr::V6(v6) => v6.to_string(),
        }
    );
    let mut service = ServiceInfo::new(
        &service_type(),
        &info.name,
        &host,
        info.host,
        info.port,
        &txt[..],
    )
    .map_err(|err| err.to_string())?;
    // The app publishes without probing, and probing would rename the instance.
    service.set_requires_probe(false);
    let fullname = service.get_fullname().to_string();
    daemon.register(service).map_err(|err| err.to_string())?;
    Ok(AdvertiseHandle { daemon, fullname })
}

/// One resolved service, as the browse sink sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrowsedService {
    /// Instance name as advertised.
    pub name: String,
    /// Full mDNS name, the only key `ServiceRemoved` carries.
    pub fullname: String,
    pub port: u16,
    pub addresses: Vec<IpAddr>,
    pub txt: Vec<(String, String)>,
}

impl BrowsedService {
    fn txt_value(&self, key: &str) -> Option<&str> {
        self.txt
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .map(|(_, v)| v.as_str())
    }

    /// `onService`'s record, or `None` for a service that is not a usable receiver.
    pub fn to_discovered(&self) -> Option<DiscoveredReceiver> {
        if self
            .txt_value("proto")
            .is_some_and(|p| p != CAST_SERVICE_TYPE)
        {
            return None;
        }
        let txt = |key: &str| self.txt_value(key).map(str::to_owned).unwrap_or_default();
        let id = {
            let txt_id = txt("id");
            if txt_id.trim().is_empty() {
                txt("pk")
            } else {
                txt_id
            }
        };
        let id = id.trim().to_owned();
        if id.is_empty() || self.port == 0 {
            return None;
        }
        let txt_host = txt("h");
        let addr = txt_host.parse::<std::net::Ipv4Addr>().ok().or_else(|| {
            self.addresses.iter().find_map(|ip| match ip {
                IpAddr::V4(v4) => Some(*v4),
                IpAddr::V6(_) => None,
            })
        })?;
        let public_key = txt("pk").to_lowercase();
        Some(DiscoveredReceiver {
            id,
            name: crate::protocol::clean_name(Some(&txt("name")), "PearTube TV"),
            host: addr.to_string(),
            port: u64::from(self.port),
            public_key: if crate::protocol::is_hex64(&public_key) {
                public_key
            } else {
                String::new()
            },
            p2p: txt("p2p") == "1",
            auth: if txt("auth") == "open" {
                "open"
            } else {
                "code"
            }
            .to_owned(),
            seen_at: 0,
        })
    }
}

type ServiceSink = Arc<dyn Fn(BrowsedService) + Send + Sync>;
type NameSink = Arc<dyn Fn(String) + Send + Sync>;

/// A running browse; dropping it stops the query.
pub struct BrowseHandle {
    daemon: ServiceDaemon,
    ty: String,
    stopped: std::sync::atomic::AtomicBool,
}

impl BrowseHandle {
    /// `browseHandle.stop()`.
    pub fn stop(&self) {
        if self.stopped.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let _ = self.daemon.stop_browse(&self.ty);
        let _ = self.daemon.shutdown();
    }
}

impl Drop for BrowseHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// `refreshBrowse`: browse the cast service and report every resolution.
pub fn browse(on_up: ServiceSink, on_down: Option<NameSink>) -> Result<BrowseHandle, String> {
    let daemon = ServiceDaemon::new().map_err(|err| err.to_string())?;
    let ty = service_type();
    let receiver = daemon.browse(&ty).map_err(|err| err.to_string())?;
    std::thread::spawn(move || {
        loop {
            match receiver.recv_timeout(Duration::from_secs(1)) {
                Ok(event) => match event {
                    ServiceEvent::ServiceResolved(info) => {
                        let service = BrowsedService {
                            name: info.get_fullname().to_owned(),
                            fullname: info.get_fullname().to_owned(),
                            port: info.get_port(),
                            addresses: info
                                .get_addresses()
                                .iter()
                                .map(|a| a.to_ip_addr())
                                .collect(),
                            txt: info
                                .get_properties()
                                .iter()
                                .map(|p| (p.key().to_owned(), p.val_str().to_owned()))
                                .collect(),
                        };
                        on_up(service);
                    }
                    ServiceEvent::ServiceRemoved(_ty, fullname) => {
                        if let Some(sink) = on_down.as_ref() {
                            sink(fullname);
                        }
                    }
                    _ => {}
                },
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    });
    Ok(BrowseHandle {
        daemon,
        ty,
        stopped: std::sync::atomic::AtomicBool::new(false),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn service(txt: &[(&str, &str)], port: u16, addresses: Vec<IpAddr>) -> BrowsedService {
        BrowsedService {
            name: "tv".into(),
            fullname: "tv._peartube-cast._tcp.local.".into(),
            port,
            addresses,
            txt: txt
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        }
    }

    #[test]
    fn records_follow_the_advertised_txt() {
        let txt = advertisement_txt(
            "dev1",
            "TV",
            Some("192.168.1.5"),
            "a".repeat(64).as_str(),
            true,
            false,
        );
        let discovered = service(
            &txt.iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect::<Vec<_>>(),
            8176,
            vec![],
        )
        .to_discovered()
        .expect("record");
        assert_eq!(discovered.id, "dev1");
        assert_eq!(discovered.host, "192.168.1.5");
        assert!(discovered.p2p);
        assert_eq!(discovered.auth, "code");
    }

    #[test]
    fn a_foreign_service_is_ignored() {
        let s = service(&[("proto", "hyperdht-mdns"), ("id", "x")], 1, vec![]);
        assert!(s.to_discovered().is_none());
    }

    #[test]
    fn the_host_falls_back_to_a_resolved_ipv4() {
        let s = service(
            &[("proto", "peartube-cast"), ("id", "x")],
            8176,
            vec!["fe80::1".parse().unwrap(), "10.0.0.4".parse().unwrap()],
        );
        assert_eq!(s.to_discovered().unwrap().host, "10.0.0.4");
    }
}
