//! Op indexes and body types shared by the search, cast and private-guard channels.

use bytes::Bytes;
use compact_encoding::{Codec, Reader};

use crate::wire::peartube::*;

pub const MAX_UINT: u64 = (1 << 53) - 1;
const MIN_INT: i64 = -(1 << 52);
const MAX_INT: i64 = (1 << 52) - 1;

fn int(n: i64) -> bool {
    (MIN_INT..=MAX_INT).contains(&n)
}

// Only whole values with a flags word need a prefix check. The generated decoder masks
// it away, so there is no field left to validate after decode. Other integers (including
// those in nested records) and non-finite floats are checked identically on send/receive.
macro_rules! bodies {
    ($($ty:ident => $name:literal, $flags:literal, |$v:ident| $valid:expr;)*) => {
        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        pub enum Struct { $($ty,)* }
        #[derive(Debug, Clone, PartialEq)]
        pub enum Body { $($ty($ty),)* }
        impl Struct {
            pub fn name(self) -> &'static str {
                match self { $(Self::$ty => concat!("@peartube/", $name),)* }
            }
            pub fn has_flags(self) -> bool {
                match self { $(Self::$ty => $flags,)* }
            }
            pub fn decode(self, bytes: Bytes) -> Option<Body> {
                let mut prefix = Reader::new(bytes.clone());
                if self.has_flags() && prefix.uint().ok()? > MAX_UINT {
                    return None;
                }
                if let Some(max) = self.max_items() && prefix.uint().ok()? > max as u64 {
                    return None;
                }
                let value = match self { $(Self::$ty => Body::$ty(compact_encoding::decode(bytes).ok()?),)* };
                value.valid().then_some(value)
            }
        }
        impl Body {
            pub fn schema(&self) -> Struct {
                match self { $(Self::$ty(_) => Struct::$ty,)* }
            }
            fn valid(&self) -> bool {
                match self { $(Self::$ty($v) => $valid,)* }
            }
            pub fn encode(&self) -> Option<Bytes> {
                if !self.valid() || self.schema().max_items().is_some_and(|max| self.items() > max) { return None; }
                match self { $(Self::$ty(v) => {
                    let len = v.encoded_len();
                    if len > crate::control_channel::MAX_FRAME { return None; }
                    let mut writer = compact_encoding::Writer::with_capacity(len);
                    v.encode(&mut writer);
                    Some(writer.finish())
                },)* }
            }
        }
        $(impl From<$ty> for Body { fn from(value: $ty) -> Self { Self::$ty(value) } })*
    };
}

bodies! {
    SearchQuery => "search-query", false, |q| q.limit <= MAX_UINT;
    SearchHits => "search-hits", false, |_v| true;
    CastPair => "cast-pair", false, |_v| true;
    CastPaired => "cast-paired", false, |_v| true;
    CastSubscribe => "cast-subscribe", false, |_v| true;
    CastCommand => "cast-command", true, |v| v.volume.is_finite() && int(v.muted) && int(v.stream) && int(v.delta_ms) && v.start_position_ms <= MAX_UINT && v.position_ms <= MAX_UINT && v.entry.as_ref().is_none_or(|e| e.size <= MAX_UINT);
    CastState => "cast-state", true, |v| v.tracks.iter().all(|t| int(t.stream)) && v.entry.as_ref().is_none_or(|e| e.size <= MAX_UINT) && v.position_ms <= MAX_UINT && v.duration_ms <= MAX_UINT && v.p2p_peers <= MAX_UINT && v.cached_bytes <= MAX_UINT && v.updated_at <= MAX_UINT && v.volume.is_finite() && int(v.audio) && int(v.subtitle) && int(v.dropped_frames);
    GuardHello => "guard-hello", false, |_v| true;
    GuardAck => "guard-ack", false, |v| v.status <= MAX_UINT;
    GuardRegister => "guard-register", false, |v| v.ttl_ms <= MAX_UINT;
    GuardKey => "guard-key", false, |_v| true;
    GuardPutMutable => "guard-put-mutable", false, |v| v.seq <= MAX_UINT;
    GuardGetResult => "guard-get-result", true, |v| v.record.as_ref().is_none_or(|r| r.seq <= MAX_UINT);
    GuardRvRegister => "guard-rv-register", false, |v| v.ttl_ms <= MAX_UINT;
    GuardRvSlot => "guard-rv-slot", false, |_v| true;
    GuardCards => "guard-cards", false, |_v| true;
    GuardDial => "guard-dial", false, |v| v.udx_id <= MAX_UINT;
    GuardOpen => "guard-open", false, |v| v.udx_id <= MAX_UINT;
    GuardReady => "guard-ready", false, |v| v.status <= MAX_UINT && v.udx_id <= MAX_UINT;
    GuardCircuitDead => "guard-circuit-dead", false, |v| v.udx_id <= MAX_UINT;
}

impl Struct {
    pub const fn max_items(self) -> Option<usize> {
        match self {
            Self::SearchHits => Some(20),
            Self::GuardCards => Some(64),
            Self::CastState => Some(128),
            _ => None,
        }
    }
}

impl Body {
    fn items(&self) -> usize {
        match self {
            Self::SearchHits(v) => v.hits.len(),
            Self::GuardCards(v) => v.cards.len(),
            Self::CastState(v) => v.tracks.len(),
            _ => 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Op {
    pub name: &'static str,
    pub value: Struct,
    /// None for an event.
    pub response: Option<Struct>,
}

const fn request(name: &'static str, value: Struct, response: Struct) -> Op {
    Op {
        name,
        value,
        response: Some(response),
    }
}

const fn event(name: &'static str, value: Struct) -> Op {
    Op {
        name,
        value,
        response: None,
    }
}

#[derive(Debug)]
pub struct Protocol {
    pub name: &'static str,
    pub ops: &'static [Op],
}

pub const SEARCH: Protocol = Protocol {
    name: "peartube/search/v2",
    ops: &[request("query", Struct::SearchQuery, Struct::SearchHits)],
};
pub const CAST: Protocol = Protocol {
    name: "peartube-cast/v2",
    ops: &[
        request("pair", Struct::CastPair, Struct::CastPaired),
        request("subscribe", Struct::CastSubscribe, Struct::CastState),
        request("command", Struct::CastCommand, Struct::CastState),
        event("state", Struct::CastState),
    ],
};
pub const GUARD: Protocol = Protocol {
    name: "peartube/private-guard/v2",
    ops: &[
        request("hello", Struct::GuardHello, Struct::GuardAck),
        request("register", Struct::GuardRegister, Struct::GuardAck),
        request("putMutable", Struct::GuardPutMutable, Struct::GuardAck),
        request("getMutable", Struct::GuardKey, Struct::GuardGetResult),
        request("rvRegister", Struct::GuardRvRegister, Struct::GuardAck),
        event("rvUnregister", Struct::GuardRvSlot),
        request("rvLookup", Struct::GuardKey, Struct::GuardCards),
        request("rvFetch", Struct::GuardKey, Struct::GuardCards),
        request("dialOpen", Struct::GuardDial, Struct::GuardReady),
        request("hopOpen", Struct::GuardOpen, Struct::GuardReady),
        request("incOpen", Struct::GuardOpen, Struct::GuardReady),
        event("circuitDead", Struct::GuardCircuitDead),
    ],
};
