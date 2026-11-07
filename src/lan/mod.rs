//! LAN discovery, ported from the app's `src/lan.js`.
//!
//! [`mdns`] carries the mDNS half (the `hyperdht-mdns` record and the refcounted bonjour adapter);
//! [`discovery`] is the isolated LAN HyperDHT half (`docs/port-notes-lan.md`); [`host`] is
//! `ios/bonjour.cjs`, the adapter that asks the platform's Bonjour daemon over the line protocol
//! on a device that denies raw multicast.

pub mod discovery;
pub mod host;
pub mod mdns;

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

pub use discovery::{DIAL_AFTER, LanDiscovery, LanError};
pub use peeroxide::KeyPair;

pub use host::{HostAdapter, HostAdvertisement, HostBrowse};
pub use mdns::{
    Adapter, Advert, BonjourAdapter, Browse, BrowseHandle, DEFAULT_REBROWSE, DiscoveredService,
    ErrorSink, MAX_SHARD_TOPICS, MAX_TXT_BYTES, MdnsError, ParsedRecord, Record, SERVICE_DOMAIN,
    SERVICE_PROTOCOL, SERVICE_TYPE, SelfAddressAdapter, ServiceSink, TOPIC_PREFIX, TOPICS_PER_TXT,
    ad_nonce, build_txt, create_records, create_records_with, parse_record, topic_token,
};

/// One entry of the host's interface table, as `os.networkInterfaces()` reports it.
#[derive(Debug, Clone, Copy)]
pub struct IfaceAddress<'a> {
    /// Interface name (`en0`, `wl0an0`, `utun3`, …).
    pub name: &'a str,
    /// Address assigned to that interface.
    pub address: IpAddr,
    /// Loopback or otherwise host-internal.
    pub internal: bool,
}

/// Picks the first private IPv4 on Wi-Fi (`wl*`), else `en*`/`eth*`, ignoring VPN and cellular
/// tunnels: `lanAddress()` in `src/lan.js`.
pub fn lan_address(ifaces: &[IfaceAddress<'_>]) -> Option<Ipv4Addr> {
    let mut candidates: Vec<&IfaceAddress<'_>> = ifaces
        .iter()
        .filter(|i| {
            i.name.starts_with("wl") || i.name.starts_with("en") || i.name.starts_with("eth")
        })
        .collect();
    // Stable sort keeps the enumeration order inside each group, like the app's stable sort.
    candidates.sort_by_key(|i| !i.name.starts_with("wl"));
    candidates.iter().find_map(|i| match i.address {
        IpAddr::V4(v4) if !i.internal && is_private(v4) => Some(v4),
        _ => None,
    })
}

/// RFC 1918 ranges, minus 172.32+ and IPv6 link-local, matching `PRIVATE` in `src/lan.js`.
fn is_private(ip: Ipv4Addr) -> bool {
    let [a, b, ..] = ip.octets();
    a == 10 || (a == 192 && b == 168) || (a == 172 && (16..=31).contains(&b))
}

/// The host's interfaces, as `os.networkInterfaces()` reports them.
async fn interfaces() -> Result<Vec<(String, IpAddr, bool)>, LanError> {
    let interfaces = if_addrs::get_if_addrs()
        .map_err(|error| LanError::new(error.to_string()))?
        .into_iter()
        .map(|interface| {
            (
                interface.name.clone(),
                interface.ip(),
                interface.is_loopback(),
            )
        })
        .collect();
    Ok(interfaces)
}

async fn open_lan(
    node: &crate::node::core::CoreNode,
    adapter: SelfAddressAdapter,
) -> Result<Arc<dyn crate::node::network::LanBinding>, LanError> {
    let key_pair = node.net().key_pair().clone();
    let lan = LanDiscovery::open_with_adapter(adapter, key_pair).await?;
    Ok(lan)
}

/// The default LAN adapter: the host's interfaces and one isolated DHT per address.
pub struct Factory;

impl crate::node::api::LanFactory<crate::node::core::CoreNode> for Factory {
    type Error = LanError;

    async fn interfaces(&self) -> Result<Vec<(String, IpAddr, bool)>, LanError> {
        interfaces().await
    }

    async fn create(
        &self,
        host: Ipv4Addr,
        node: &crate::node::core::CoreNode,
    ) -> Result<Arc<dyn crate::node::network::LanBinding>, LanError> {
        let adapter = SelfAddressAdapter::new(BonjourAdapter::new(), host);
        open_lan(node, adapter).await
    }

    async fn host_event(&self, _event: serde_json::Value) {}
}

/// The same adapter for a platform that denies raw multicast (iOS): Bonjour requests are lines to
/// the host (`ios/bonjour.cjs`), and its answers arrive through [`crate::node::api::LanFactory::host_event`].
pub struct HostFactory {
    adapter: Arc<HostAdapter>,
}

impl HostFactory {
    /// `send` writes one request line to the host.
    pub fn new(send: impl Fn(serde_json::Value) + Send + Sync + 'static) -> Self {
        Self {
            adapter: Arc::new(HostAdapter::new(send)),
        }
    }
}

impl crate::node::api::LanFactory<crate::node::core::CoreNode> for HostFactory {
    type Error = LanError;

    async fn interfaces(&self) -> Result<Vec<(String, IpAddr, bool)>, LanError> {
        interfaces().await
    }

    async fn create(
        &self,
        host: Ipv4Addr,
        node: &crate::node::core::CoreNode,
    ) -> Result<Arc<dyn crate::node::network::LanBinding>, LanError> {
        let adapter =
            SelfAddressAdapter::with_adapter(Adapter::Host(Arc::clone(&self.adapter)), host);
        open_lan(node, adapter).await
    }

    async fn host_event(&self, event: serde_json::Value) {
        self.adapter.dispatch(&event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iface<'a>(name: &'a str, address: &str, internal: bool) -> IfaceAddress<'a> {
        IfaceAddress {
            name,
            address: address.parse().unwrap(),
            internal,
        }
    }

    #[test]
    fn wifi_beats_wired() {
        let ifaces = [
            iface("en0", "192.168.1.20", false),
            iface("wl0an0", "10.1.2.3", false),
        ];
        assert_eq!(lan_address(&ifaces), Some("10.1.2.3".parse().unwrap()));
    }

    #[test]
    fn skips_vpn_public_and_internal() {
        let ifaces = [
            iface("utun3", "10.8.0.2", false),
            iface("en1", "1.2.3.4", false),
            iface("en0", "127.0.0.1", true),
            iface("eth1", "172.20.5.6", false),
        ];
        assert_eq!(lan_address(&ifaces), Some("172.20.5.6".parse().unwrap()));
    }

    #[test]
    fn none_when_only_tunnels() {
        assert_eq!(lan_address(&[iface("utun0", "192.168.9.9", false)]), None);
    }
}
