//! LAN discovery, ported from the app's `src/lan.js`.
//!
//! [`mdns`] carries the mDNS half (the `hyperdht-mdns` record and the refcounted bonjour adapter).
//! The isolated LAN HyperDHT half is specified in `docs/port-notes-lan.md`.

pub mod mdns;

use std::net::{IpAddr, Ipv4Addr};

pub use mdns::{
    Advertisement, BonjourAdapter, BrowseHandle, DEFAULT_REBROWSE, DiscoveredService, ErrorSink,
    MAX_SHARD_TOPICS, MAX_TXT_BYTES, MdnsError, ParsedRecord, Record, SERVICE_DOMAIN,
    SERVICE_PROTOCOL, SERVICE_TYPE, SelfAddressAdapter, ServiceSink, TOPIC_PREFIX, TOPICS_PER_TXT,
    ad_nonce, build_txt, create_records, create_records_with, parse_record, process_adapter,
    topic_token,
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
