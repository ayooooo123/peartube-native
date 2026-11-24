//! Cast control on the existing peer connection. Host and HTTP documents remain JSON;
//! only action-relevant command fields cross the channel.

use std::time::Duration;

use protomux::Mux;

use super::CastError;
pub use crate::control_channel::{
    Control, ControlHandlers, DEFAULT_TIMEOUT, EventHandler, OpenHandler, RequestHandler,
    ServeHandle,
};
use crate::control_channel::{Spec, open_channel, serve_channel};

/// The Protomux protocol name.
pub const PROTOCOL: &str = crate::control_protocol::CAST.name;
pub const VERSION_ERROR: &str = "This device runs a different PearTube version, or takes no casts: update PearTube on both devices";

static CAST: Spec = Spec {
    protocol: &crate::control_protocol::CAST,
    label: "Receiver",
    refused: VERSION_ERROR,
    duplicate: "A cast session is already open with this receiver",
    timed_out: "Timed out opening the cast channel",
};

impl From<crate::control_channel::ControlError> for CastError {
    fn from(error: crate::control_channel::ControlError) -> Self {
        CastError::new(error.message, 0)
    }
}

/// `serveControl(conn, { onRequest, onOpen, onClose })`: the receiver's half, answering every
/// sender that opens the channel on this connection. Returns a handle that stops answering.
pub fn serve_control(mux: &Mux, mut handlers: ControlHandlers) -> ServeHandle {
    handlers.handled = 0b111;
    serve_channel(mux, &CAST, handlers)
}

/// `openControl(conn, { onEvent, onClose }, timeoutMs)`: the sender's half. Resolves once the
/// receiver answers the open; a peer that is not a receiver refuses the channel, which is an error.
pub async fn open_control(
    mux: &Mux,
    handlers: ControlHandlers,
    timeout: Duration,
) -> Result<Control, CastError> {
    let handlers = ControlHandlers {
        handled: 1 << 3,
        on_request: None,
        ..handlers
    };
    Ok(open_channel(mux, &CAST, handlers, timeout).await?)
}

use crate::wire::peartube::{CastCommand, CastEntry, CastState, CastTrack};
use serde_json::{Map, Value, json};

// JS slice(0, max).toWellFormed(): a cut surrogate pair leaves one replacement character.
fn cut(s: &str, max: usize) -> (usize, bool) {
    if s.len() <= max {
        return (s.len(), false);
    }
    let mut units = 0;
    for (i, c) in s.char_indices() {
        units += c.len_utf16();
        if units > max {
            return (i, units - c.len_utf16() < max);
        }
    }
    (s.len(), false)
}
pub(super) fn bounded(s: &str, max: usize) -> String {
    let (end, split) = cut(s, max);
    let mut result = String::with_capacity(end + if split { 3 } else { 0 });
    result.push_str(s.get(..end).unwrap_or_default());
    if split {
        result.push('\u{fffd}');
    }
    result
}
fn truncate(s: &mut String, max: usize) {
    let (end, split) = cut(s, max);
    s.truncate(end);
    if split {
        s.push('\u{fffd}');
    }
}
fn short(v: &Value, k: &str, max: usize) -> String {
    bounded(v[k].as_str().unwrap_or_default(), max)
}
fn uint(v: Option<&Value>) -> u64 {
    (v.and_then(Value::as_f64).unwrap_or_default().round() as u64).min((1 << 53) - 1)
}
fn signed(v: Option<&Value>, default: i64) -> i64 {
    let Some(n) = v.and_then(Value::as_f64) else {
        return default;
    };
    // Math.round takes a half step toward positive infinity (including negative deltas).
    let floor = n.floor();
    ((floor + f64::from(n - floor >= 0.5)) as i64).clamp(-(1 << 52), (1 << 52) - 1)
}
fn entry(v: &Value) -> Option<CastEntry> {
    v.is_object().then(|| CastEntry {
        key: short(v, "key", 256),
        id: short(v, "id", 128),
        title: short(v, "title", 80),
        size: uint(v.get("size")),
        sha256: short(v, "sha256", 64),
    })
}
fn entry_json(e: CastEntry) -> Value {
    json!({"key":e.key,"id":e.id,"title":e.title,"size":e.size,"sha256":e.sha256})
}

pub fn command_to_wire(v: &Value, mut token: String) -> CastCommand {
    truncate(&mut token, 256);
    let mut c = CastCommand {
        token,
        action: short(v, "action", 32),
        entry: None,
        magnet_url: String::new(),
        start_position_ms: 0,
        position_ms: 0,
        delta_ms: 0,
        stream: -1,
        volume: -1.,
        muted: -1,
    };
    match c.action.as_str() {
        "load" => {
            c.entry = entry(&v["entry"]);
            c.magnet_url = short(v, "magnetUrl", 2048);
            c.start_position_ms = uint(v.get("startPositionMs"));
        }
        "seek" => c.position_ms = uint(v.get("positionMs")),
        "skip" => c.delta_ms = signed(v.get("deltaMs"), 0),
        "selectAudio" | "selectSubtitle" => c.stream = signed(v.get("stream"), -1),
        "volume" => {
            c.volume = v["volume"].as_f64().map_or(-1., |v| v.clamp(0., 1.));
            c.muted = v["muted"].as_bool().map_or(-1, i64::from);
        }
        _ => {}
    }
    c
}

pub fn command_from_wire(c: CastCommand) -> Value {
    let mut out = json!({"action":c.action});
    match c.action.as_str() {
        "load" => {
            if let Some(mut e) = c.entry {
                truncate(&mut e.key, 256);
                truncate(&mut e.id, 128);
                truncate(&mut e.title, 80);
                truncate(&mut e.sha256, 64);
                out["entry"] = entry_json(e);
            }
            // The stand-in is exact: an empty link on the wire is "none".
            out["magnetUrl"] = (!c.magnet_url.is_empty()).then_some(c.magnet_url).into();
            out["startPositionMs"] = c.start_position_ms.into();
        }
        "seek" => out["positionMs"] = c.position_ms.into(),
        "skip" => out["deltaMs"] = c.delta_ms.into(),
        "selectAudio" | "selectSubtitle" => out["stream"] = c.stream.into(),
        "volume" => {
            if c.volume >= 0. {
                out["volume"] = super::state::number_json(c.volume);
            }
            if matches!(c.muted, 0 | 1) {
                out["muted"] = (c.muted == 1).into();
            }
        }
        _ => {}
    }
    out
}

pub fn state_to_wire(s: &Map<String, Value>) -> CastState {
    let string = |k: &str, max| bounded(s.get(k).and_then(Value::as_str).unwrap_or_default(), max);
    let flag = |k: &str| s.get(k).and_then(Value::as_bool).unwrap_or(false);
    let integer = |k: &str, d| signed(s.get(k), d);
    let n = |k: &str| uint(s.get(k));
    let tracks = s
        .get("tracks")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(128)
        .map(|t| CastTrack {
            stream: signed(t.get("stream"), -1),
            kind: short(t, "kind", 16),
            codec: short(t, "codec", 32),
            language: short(t, "language", 32),
            title: short(t, "title", 64),
        })
        .collect();
    CastState {
        tracks,
        status: string("status", 32),
        mode: string("mode", 32),
        entry: s.get("entry").and_then(entry),
        position_ms: n("positionMs"),
        duration_ms: n("durationMs"),
        playing: flag("playing"),
        buffering: flag("buffering"),
        ended: flag("ended"),
        p2p_peers: n("p2pPeers"),
        cached_bytes: n("cachedBytes"),
        seeding: flag("seeding"),
        volume: s.get("volume").and_then(Value::as_f64).unwrap_or(1.),
        muted: flag("muted"),
        audio: integer("audio", -2),
        subtitle: integer("subtitle", -2),
        error: string("error", 256),
        updated_at: n("updatedAt"),
        video_decoder: string("videoDecoder", 64),
        dropped_frames: s
            .get("droppedFrames")
            .filter(|v| !v.is_null())
            .map_or(-1, |_| n("droppedFrames").min((1 << 52) - 1) as i64),
    }
}

pub fn state_from_wire(s: CastState) -> Value {
    fn nullable(s: String) -> Option<String> {
        (!s.is_empty()).then_some(s)
    }
    let tracks: Vec<_> = s.tracks.into_iter().map(|t| json!({"stream":t.stream,"kind":t.kind,"codec":t.codec,"language":nullable(t.language),"title":nullable(t.title)})).collect();
    json!({"status":s.status,"mode":nullable(s.mode),"entry":s.entry.map(entry_json),"positionMs":s.position_ms,"durationMs":(s.duration_ms != 0).then_some(s.duration_ms),"playing":s.playing,"buffering":s.buffering,"ended":s.ended,"p2pPeers":s.p2p_peers,"cachedBytes":s.cached_bytes,"seeding":s.seeding,"volume":super::state::number_json(s.volume),"muted":s.muted,"tracks":tracks,"audio":(s.audio != -2).then_some(s.audio),"subtitle":(s.subtitle != -2).then_some(s.subtitle),"error":nullable(s.error),"updatedAt":s.updated_at,"videoDecoder":nullable(s.video_decoder),"droppedFrames":(s.dropped_frames != -1).then_some(s.dropped_frames)})
}
