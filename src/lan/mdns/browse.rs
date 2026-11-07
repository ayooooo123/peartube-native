//! bonjour-service's packet-local browser: one PTR query per rebrowse, no resolver/cache crawl.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::{Arc, atomic::Ordering};
use std::time::{Duration, Instant};

use mio::{Events, Interest, Poll, Token, Waker};
use simple_dns::{CLASS, Name, Packet, PacketFlag, Question, TYPE, rdata::RData};
use socket2::{Domain, Protocol, Socket, Type};

use super::{BrowseState, DiscoveredService, Inner, SERVICE_DOMAIN, subscribers};

const GROUP: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const PORT: u16 = 5353;
const MEMBERSHIP_POLL: Duration = Duration::from_secs(5);

pub(super) struct Listener {
    poll: Poll,
    socket: mio::net::UdpSocket,
    pub(super) wake: Arc<Waker>,
    memberships: HashSet<Ipv4Addr>,
}

impl Listener {
    pub(super) fn open(host: Ipv4Addr) -> io::Result<Self> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_address(true)?;
        #[cfg(unix)]
        socket.set_reuse_port(true)?;
        socket.bind(&SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, PORT).into())?;
        socket.set_multicast_ttl_v4(255)?;
        socket.set_multicast_loop_v4(true)?;
        socket.join_multicast_v4(&GROUP, &host)?;
        socket.set_multicast_if_v4(&host)?;
        socket.set_nonblocking(true)?;
        let mut socket = mio::net::UdpSocket::from_std(socket.into());
        let poll = Poll::new()?;
        poll.registry()
            .register(&mut socket, Token(0), Interest::READABLE)?;
        let wake = Arc::new(Waker::new(poll.registry(), Token(1))?);
        Ok(Self {
            poll,
            socket,
            wake,
            memberships: HashSet::from([host]),
        })
    }

    fn update_memberships(&mut self, inner: &Inner) {
        match if_addrs::get_if_addrs() {
            Ok(interfaces) => {
                for interface in interfaces {
                    let std::net::IpAddr::V4(ip) = interface.ip() else {
                        continue;
                    };
                    if !self.memberships.contains(&ip) {
                        match self.socket.join_multicast_v4(&GROUP, &ip) {
                            Ok(()) => {
                                self.memberships.insert(ip);
                            }
                            Err(error) => inner.report(&error.to_string()),
                        }
                    }
                }
            }
            Err(error) => inner.report(&error.to_string()),
        }
    }

    pub(super) fn run(
        mut self,
        inner: Arc<Inner>,
        state: Arc<BrowseState>,
        rebrowse: Option<Duration>,
    ) {
        let mut events = Events::with_capacity(2);
        let mut bytes = [0u8; 65536];
        let query = query();
        let mut known = HashMap::new();
        let mut next_query = Some(Instant::now());
        let mut next_membership = Instant::now();
        while !state.stop.load(Ordering::Acquire) {
            let now = Instant::now();
            if now >= next_membership {
                self.update_memberships(&inner);
                next_membership = now + MEMBERSHIP_POLL;
            }
            if next_query.is_some_and(|deadline| now >= deadline) {
                // JS creates a fresh browser on rebrowse, rather than resolving stale cached PTRs.
                known.clear();
                if let Err(error) = self
                    .socket
                    .send_to(&query, SocketAddrV4::new(GROUP, PORT).into())
                {
                    inner.report(&error.to_string());
                }
                next_query = rebrowse.map(|delay| now + delay.max(Duration::from_millis(1)));
            }
            let deadline = next_query.map_or(next_membership, |query| query.min(next_membership));
            match self.poll.poll(
                &mut events,
                Some(deadline.saturating_duration_since(Instant::now())),
            ) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    inner.report(&error.to_string());
                    return;
                }
            }
            if state.stop.load(Ordering::Acquire) {
                return;
            }
            for event in &events {
                if event.token() != Token(0) {
                    continue;
                }
                loop {
                    match self.socket.recv_from(&mut bytes) {
                        Ok((len, std::net::SocketAddr::V4(source))) => {
                            let _ =
                                visit(&bytes[..len], *source.ip(), |name, service| match service {
                                    Some(service) => {
                                        let entry = known
                                            .entry(name.to_ascii_lowercase())
                                            .insert_entry(service);
                                        for sub in subscribers(&state) {
                                            (sub.up)(entry.get());
                                        }
                                    }
                                    None => {
                                        if let Some(service) =
                                            known.remove(&name.to_ascii_lowercase())
                                        {
                                            for sub in subscribers(&state) {
                                                if let Some(down) = sub.down {
                                                    down(&service);
                                                }
                                            }
                                        }
                                    }
                                });
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                        Err(error) => {
                            inner.report(&error.to_string());
                            return;
                        }
                    }
                }
            }
        }
    }
}

fn query() -> Vec<u8> {
    let mut packet = Packet::new_query(0);
    packet.questions.push(Question::new(
        Name::new_unchecked(SERVICE_DOMAIN),
        TYPE::PTR.into(),
        CLASS::IN.into(),
        false,
    ));
    packet.build_bytes_vec().expect("constant mDNS question")
}

fn visit(
    bytes: &[u8],
    source: Ipv4Addr,
    mut found: impl FnMut(&str, Option<DiscoveredService>),
) -> simple_dns::Result<()> {
    let packet = Packet::parse(bytes)?;
    if !packet.has_flags(PacketFlag::RESPONSE) {
        return Ok(());
    }
    let records = || packet.answers.iter().chain(&packet.additional_records);
    let domain = Name::new_unchecked(SERVICE_DOMAIN);
    for ptr in records().filter(|record| record.ttl == 0 && record.name == domain) {
        if let RData::PTR(target) = &ptr.rdata {
            found(&target.to_string(), None);
        }
    }
    for ptr in records() {
        let RData::PTR(target) = &ptr.rdata else {
            continue;
        };
        if ptr.ttl == 0 || ptr.name != domain {
            continue;
        }
        let fullname = target.to_string();
        let Some(srv) = records().find_map(|record| {
            if record.ttl > 0
                && record.name == target.0
                && let RData::SRV(srv) = &record.rdata
            {
                return Some(srv);
            }
            None
        }) else {
            continue;
        };
        let Some(name) = target.without(&domain) else {
            continue;
        };
        let txt = records()
            .filter(|record| record.ttl > 0 && record.name == target.0)
            .filter_map(|record| {
                if let RData::TXT(txt) = &record.rdata {
                    Some(txt)
                } else {
                    None
                }
            })
            .flat_map(|txt| txt.iter_raw())
            .map(|(key, value)| {
                (
                    String::from_utf8_lossy(key).into_owned(),
                    String::from_utf8_lossy(value.unwrap_or_default()).into_owned(),
                )
            })
            .collect();
        let mut addresses: Vec<_> = records()
            .filter(|record| record.ttl > 0 && record.name == srv.target)
            .filter_map(|record| {
                if let RData::A(address) = &record.rdata {
                    Some(Ipv4Addr::from(address.address))
                } else {
                    None
                }
            })
            .collect();
        if addresses.is_empty() {
            addresses.push(source);
        }
        found(
            &fullname,
            Some(DiscoveredService {
                name: name.to_string(),
                port: srv.port,
                addresses,
                txt,
            }),
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use simple_dns::{
        CLASS, ResourceRecord,
        rdata::{A, PTR, SRV, TXT},
    };

    fn response() -> Packet<'static> {
        let mut packet = Packet::new_reply(0);
        let name = Name::new_unchecked("peer._hyperdht-mdns._udp.local");
        packet.answers.push(ResourceRecord::new(
            Name::new_unchecked(SERVICE_DOMAIN),
            CLASS::IN,
            28800,
            RData::PTR(PTR(name.clone())),
        ));
        packet.additional_records.push(ResourceRecord::new(
            name.clone(),
            CLASS::IN,
            120,
            RData::SRV(SRV {
                priority: 0,
                weight: 0,
                port: 4343,
                target: Name::new_unchecked("peer.local"),
            }),
        ));
        let mut txt = TXT::new();
        txt.add_attribute("h", Some(b"10.0.0.7")).unwrap();
        packet
            .additional_records
            .push(ResourceRecord::new(name, CLASS::IN, 4500, RData::TXT(txt)));
        packet.additional_records.push(ResourceRecord::new(
            Name::new_unchecked("peer.local"),
            CLASS::IN,
            120,
            RData::A(A {
                address: u32::from(Ipv4Addr::new(10, 0, 0, 8)),
            }),
        ));
        packet
    }

    fn services(packet: &Packet<'_>) -> Vec<Option<DiscoveredService>> {
        let bytes = packet.build_bytes_vec_compressed().unwrap();
        let mut out = Vec::new();
        visit(&bytes, Ipv4Addr::new(10, 0, 0, 9), |_, service| {
            out.push(service)
        })
        .unwrap();
        out
    }

    #[test]
    fn same_packet_records_and_referer_match_bonjour_browser() {
        let mut packet = response();
        let service = services(&packet).pop().unwrap().unwrap();
        assert_eq!(service.name, "peer");
        assert_eq!(service.port, 4343);
        assert_eq!(service.txt, [("h".into(), "10.0.0.7".into())]);
        assert_eq!(service.addresses, [Ipv4Addr::new(10, 0, 0, 8)]);
        packet.additional_records.pop();
        assert_eq!(
            services(&packet)[0].as_ref().unwrap().addresses,
            [Ipv4Addr::new(10, 0, 0, 9)]
        );
    }

    #[test]
    fn stale_ptrs_never_trigger_followup_resolution() {
        let mut packet = response();
        packet.additional_records.clear();
        assert!(services(&packet).is_empty());
        packet.answers[0].ttl = 0;
        assert_eq!(services(&packet), [None]);
        assert!(
            visit(&[0; 5], Ipv4Addr::LOCALHOST, |_, _| panic!(
                "malformed packet"
            ))
            .is_err()
        );
    }

    #[test]
    fn goodbyes_precede_positive_records_like_bonjour_browser() {
        let mut packet = response();
        let mut goodbye = packet.answers[0].clone();
        goodbye.ttl = 0;
        packet.answers.push(goodbye);
        let observed = services(&packet);
        assert!(observed[0].is_none());
        assert!(observed[1].is_some());
    }

    #[test]
    fn one_ptr_query_has_no_cached_answers_or_resolve_questions() {
        let bytes = query();
        let packet = Packet::parse(&bytes).unwrap();
        assert_eq!(packet.questions.len(), 1);
        assert_eq!(packet.questions[0].qtype, TYPE::PTR.into());
        assert_eq!(
            packet.questions[0].qname,
            Name::new_unchecked(SERVICE_DOMAIN)
        );
        assert!(packet.answers.is_empty());
        assert!(packet.additional_records.is_empty());
    }
}
