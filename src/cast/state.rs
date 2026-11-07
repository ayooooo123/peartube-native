//! The cast state document and its transitions, ported from `idleCastState`, `updateState`,
//! `applyCommand`'s pure half and `reportState` in `src/cast.js`.
//!
//! The document stays a JSON map rather than a struct: JavaScript spreads arbitrary keys into it
//! (`/cast/v1/web-state` merges whatever the TV page reports) and preserves its key order, which
//! the worker forwards verbatim. A map keeps both properties, and [`merge`] reproduces the object
//! spread order (existing keys keep their place, new ones are appended).

use serde_json::{Map, Value, json};

use crate::protocol::clean_name;

use super::CastError;

/// The cast state document, key-for-key what `idleCastState()` returns.
pub type CastState = Map<String, Value>;

/// What `applyCommand` decided for a command.
#[derive(Debug, Clone)]
pub enum Plan {
    /// `status`: answer the current state.
    Status,
    /// `load`: prepare media, then broadcast the load command.
    Load,
    /// `stop`: idle the receiver and broadcast the stop command.
    Stop,
    /// A transport-level command: merge this patch into the state.
    Patch(Map<String, Value>),
}

/// `idleCastState()`; `now_ms` stands in for `Date.now()`.
pub fn idle(now_ms: u64) -> CastState {
    let mut state = CastState::new();
    state.insert("status".into(), json!("idle"));
    state.insert("mode".into(), Value::Null);
    state.insert("entry".into(), Value::Null);
    state.insert("positionMs".into(), json!(0));
    state.insert("durationMs".into(), Value::Null);
    state.insert("playing".into(), json!(false));
    state.insert("buffering".into(), json!(false));
    state.insert("ended".into(), json!(false));
    state.insert("p2pPeers".into(), json!(0));
    state.insert("cachedBytes".into(), json!(0));
    state.insert("seeding".into(), json!(false));
    state.insert("volume".into(), json!(1));
    state.insert("muted".into(), json!(false));
    state.insert("tracks".into(), json!([]));
    state.insert("audio".into(), Value::Null);
    state.insert("subtitle".into(), Value::Null);
    state.insert("error".into(), Value::Null);
    state.insert("updatedAt".into(), json!(now_ms));
    state
}

/// Object spread: `{ ...base, ...patch }`, in JavaScript's key order.
pub fn merge(base: &CastState, patch: &Map<String, Value>) -> CastState {
    let mut out = base.clone();
    for (key, value) in patch {
        out.insert(key.clone(), value.clone());
    }
    out
}

/// `state = { ...state, ...patch, updatedAt: Date.now() }`.
pub fn update(state: &mut CastState, patch: &Map<String, Value>, now_ms: u64) {
    for (key, value) in patch {
        state.insert(key.clone(), value.clone());
    }
    state.insert("updatedAt".into(), json!(now_ms));
}

/// JavaScript truthiness of a JSON value.
pub fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// `String(value)` for JSON scalars, `""` for missing ones.
pub fn text_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(items) => items.iter().map(text_value).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

/// `value || fallback` on a JSON value.
fn or_default(value: Option<&Value>, fallback: &str) -> String {
    match value {
        Some(v) if truthy(v) => text_value(v),
        _ => fallback.to_owned(),
    }
}

/// The state as the app serializes it.
///
/// `reportState` always defines `videoDecoder`, falling back to `state.videoDecoder`, which is
/// `undefined` until a player first reports one; JavaScript omits such keys from JSON. The port
/// keeps the key's position and leaves `null` there, so serialization drops a null `videoDecoder`.
pub fn to_json(state: &CastState) -> Value {
    let mut out = CastState::with_capacity(state.len());
    for (key, value) in state {
        if key == "videoDecoder" && value.is_null() {
            continue;
        }
        out.insert(key.clone(), value.clone());
    }
    Value::Object(out)
}

/// `Number(value)`, for the handful of places the port coerces a JSON value.
pub fn num(value: &Value) -> f64 {
    match value {
        Value::Null => 0.0,
        Value::Bool(flag) => f64::from(u8::from(*flag)),
        Value::Number(n) => n.as_f64().unwrap_or(f64::NAN),
        Value::String(s) => js_number(s),
        Value::Array(items) => match items.len() {
            0 => 0.0,
            1 => num(&items[0]),
            _ => f64::NAN,
        },
        Value::Object(_) => f64::NAN,
    }
}

/// `Number(string)`: trimmed decimals and exponential notation; hex/binary literals are not.
pub fn js_number(text: &str) -> f64 {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return 0.0;
    }
    match trimmed {
        "Infinity" | "+Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        _ => trimmed.parse::<f64>().unwrap_or(f64::NAN),
    }
}

/// `!Number.isNaN` plus integral bounds: JavaScript's `Number.isSafeInteger`.
pub fn is_safe_integer(value: f64) -> bool {
    value.is_finite() && value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0
}

/// A JavaScript number as JSON: integral values stay integers, so `1` never serializes as `1.0`.
pub fn number_json(value: f64) -> Value {
    if !value.is_finite() {
        return Value::Null;
    }
    if value.fract() == 0.0 && value.abs() <= 9_007_199_254_740_991.0 {
        json!(value as i64)
    } else {
        json!(value)
    }
}

/// `Math.max(0, Number(v) || 0)` for a millisecond-ish field.
pub fn non_negative(value: Option<&Value>) -> u64 {
    match value {
        Some(v) => {
            let n = num(v);
            if n.is_finite() && n > 0.0 {
                n as u64
            } else {
                0
            }
        }
        None => 0,
    }
}

fn flag(state: &CastState, key: &str) -> bool {
    state.get(key).is_some_and(truthy)
}

/// `reportState`'s status line: idle until something is loaded, then error/ended/buffering/playing/paused.
pub fn playback_status(
    has_entry: bool,
    error: Option<&str>,
    ended: bool,
    buffering: bool,
    playing: bool,
) -> &'static str {
    if !has_entry {
        "idle"
    } else if error.is_some_and(|e| !e.is_empty()) {
        "error"
    } else if ended {
        "ended"
    } else if buffering {
        "buffering"
    } else if playing {
        "playing"
    } else {
        "paused"
    }
}

/// The patch `reportState(patch)` hands to `updateState`; `None` means `patch.idle` was set and
/// the caller resets to [`idle`].
pub fn report_patch(state: &CastState, patch: &Value) -> Option<Map<String, Value>> {
    let Value::Object(patch) = patch else {
        return None;
    };
    if patch.get("idle").is_some_and(truthy) {
        return None;
    }
    let playing = patch.get("playing").is_some_and(truthy);
    let ended = patch.get("ended").is_some_and(truthy);
    let buffering = patch.get("buffering").is_some_and(truthy);
    let error = match patch.get("error") {
        Some(v) if truthy(v) => Some(text_value(v)),
        _ => None,
    };
    let entry_present = state.get("entry").is_some_and(|v| !v.is_null());
    let mut next = Map::new();
    next.insert(
        "status".into(),
        json!(playback_status(
            entry_present,
            error.as_deref(),
            ended,
            buffering,
            playing
        )),
    );
    next.insert(
        "positionMs".into(),
        json!(non_negative(patch.get("positionMs"))),
    );
    let duration = match patch.get("durationMs") {
        Some(Value::Null) | None => state.get("durationMs").cloned().unwrap_or(Value::Null),
        Some(v) if num(v) > 0.0 => number_json(num(v)),
        Some(_) => state.get("durationMs").cloned().unwrap_or(Value::Null),
    };
    next.insert("durationMs".into(), duration);
    next.insert("playing".into(), json!(playing));
    next.insert("buffering".into(), json!(buffering));
    next.insert("ended".into(), json!(ended));
    next.insert(
        "tracks".into(),
        match patch.get("tracks") {
            Some(Value::Array(items)) => Value::Array(items.clone()),
            _ => state.get("tracks").cloned().unwrap_or(json!([])),
        },
    );
    for key in ["audio", "subtitle", "videoDecoder"] {
        let value = match patch.get(key) {
            Some(v) => v.clone(),
            None => state.get(key).cloned().unwrap_or(Value::Null),
        };
        next.insert(key.into(), value);
    }
    let dropped = match patch.get("droppedFrames") {
        Some(v) => {
            let n = num(v);
            json!(if n.is_finite() { n as i64 } else { 0 })
        }
        None => state.get("droppedFrames").cloned().unwrap_or(json!(0)),
    };
    next.insert("droppedFrames".into(), dropped);
    next.insert(
        "error".into(),
        match error {
            Some(message) => Value::String(message),
            None => Value::Null,
        },
    );
    Some(next)
}

/// `applyCommand`'s pure half: which command it is and, for transport commands, the state patch.
pub fn plan_command(state: &CastState, cmd: &Value) -> Result<Plan, CastError> {
    let Value::Object(cmd) = cmd else {
        return Err(CastError::bad_request("action is required"));
    };
    let action = cmd.get("action").map(text_value).unwrap_or_default();
    if action.is_empty() {
        return Err(CastError::bad_request("action is required"));
    }
    let mut patch = Map::new();
    match action.as_str() {
        "status" => return Ok(Plan::Status),
        "load" => return Ok(Plan::Load),
        "stop" => return Ok(Plan::Stop),
        "play" => {
            patch.insert("playing".into(), json!(true));
            patch.insert("status".into(), json!("playing"));
            patch.insert("ended".into(), json!(false));
        }
        "pause" => {
            patch.insert("playing".into(), json!(false));
            patch.insert("status".into(), json!("paused"));
        }
        "togglePlay" => {
            let playing = !flag(state, "playing");
            patch.insert("playing".into(), json!(playing));
            patch.insert(
                "status".into(),
                json!(if playing { "playing" } else { "paused" }),
            );
        }
        "seek" => {
            patch.insert(
                "positionMs".into(),
                json!(non_negative(cmd.get("positionMs"))),
            );
            patch.insert("ended".into(), json!(false));
        }
        "skip" => {
            let current = num(state.get("positionMs").unwrap_or(&json!(0)));
            let delta = num(cmd.get("deltaMs").unwrap_or(&json!(0)));
            let next = (current + delta).max(0.0);
            patch.insert("positionMs".into(), number_json(next));
            patch.insert("ended".into(), json!(false));
        }
        "selectAudio" => {
            patch.insert(
                "audio".into(),
                cmd.get("stream").cloned().unwrap_or(Value::Null),
            );
        }
        "selectSubtitle" => {
            patch.insert(
                "subtitle".into(),
                cmd.get("stream").cloned().unwrap_or(Value::Null),
            );
        }
        "volume" => {
            if let Some(volume) = cmd.get("volume").filter(|v| v.is_number()) {
                let clamped = num(volume).clamp(0.0, 1.0);
                patch.insert("volume".into(), number_json(clamped));
            }
            if let Some(muted) = cmd.get("muted").filter(|v| v.is_boolean()) {
                patch.insert("muted".into(), muted.clone());
            }
        }
        other => {
            return Err(CastError::bad_request(format!(
                "Unsupported cast action: {other}"
            )));
        }
    }
    Ok(Plan::Patch(patch))
}

/// The command broadcast to a web receiver: `{ seq, ...cmd, action, positionMs? }`.
pub fn relayed_command(seq: u64, cmd: &Value, position_ms: Option<u64>) -> Value {
    let mut out = Map::new();
    out.insert("seq".into(), json!(seq));
    if let Value::Object(fields) = cmd {
        for (key, value) in fields {
            out.insert(key.clone(), value.clone());
        }
    }
    out.insert(
        "action".into(),
        cmd.get("action").cloned().unwrap_or(Value::Null),
    );
    if let Some(position) = position_ms {
        out.insert("positionMs".into(), json!(position));
    }
    Value::Object(out)
}

/// `notifyLocalHost`'s dedupe key.
pub fn notify_key(state: &CastState, host: &str, port: u16, pair_code: &str) -> String {
    let mode = or_default(state.get("mode"), "");
    let error = or_default(state.get("error"), "");
    let status = or_default(state.get("status"), "");
    let peers = state.get("p2pPeers").and_then(Value::as_u64).unwrap_or(0);
    let playing = flag(state, "playing");
    let buffering = flag(state, "buffering");
    let ended = flag(state, "ended");
    let entry_id = state
        .get("entry")
        .and_then(|e| e.get("id"))
        .map(text_value)
        .unwrap_or_default();
    format!(
        "{host}:{port}:{pair_code}:{status}:{mode}:{peers}:{playing}:{buffering}:{ended}:{error}:{entry_id}"
    )
}

/// The `entry` a load command carries, with the app's defaults.
pub fn load_entry(cmd: &Value) -> Value {
    let entry = cmd.get("entry");
    let id = entry
        .and_then(|e| e.get("id"))
        .map(text_value)
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "stream".to_owned());
    let title = entry
        .and_then(|e| e.get("title"))
        .map(text_value)
        .filter(|s| !s.is_empty());
    let mut out = Map::new();
    out.insert(
        "key".into(),
        json!(
            entry
                .and_then(|e| e.get("key"))
                .map(text_value)
                .unwrap_or_default()
        ),
    );
    out.insert("id".into(), json!(id));
    out.insert("title".into(), json!(clean_name(title.as_deref(), &id)));
    out.insert(
        "size".into(),
        json!(
            entry
                .and_then(|e| e.get("size"))
                .map(num)
                .filter(|n| n.is_finite() && *n > 0.0)
                .map(|n| n as u64)
                .unwrap_or(0)
        ),
    );
    out.insert(
        "sha256".into(),
        json!(
            entry
                .and_then(|e| e.get("sha256"))
                .map(text_value)
                .unwrap_or_default()
        ),
    );
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_state_has_the_app_field_order() {
        let state = idle(7);
        let keys: Vec<&str> = state.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [
                "status",
                "mode",
                "entry",
                "positionMs",
                "durationMs",
                "playing",
                "buffering",
                "ended",
                "p2pPeers",
                "cachedBytes",
                "seeding",
                "volume",
                "muted",
                "tracks",
                "audio",
                "subtitle",
                "error",
                "updatedAt"
            ]
        );
        assert_eq!(state["updatedAt"], json!(7));
    }

    #[test]
    fn merge_keeps_existing_positions_and_appends_new_keys() {
        let state = idle(0);
        let mut patch = Map::new();
        patch.insert("status".into(), json!("loading"));
        patch.insert("fresh".into(), json!(1));
        let merged = merge(&state, &patch);
        let keys: Vec<&str> = merged.keys().map(String::as_str).collect();
        assert_eq!(keys.last(), Some(&"fresh"));
        assert_eq!(keys[0], "status");
    }

    #[test]
    fn toggle_uses_the_current_playing_flag() {
        let mut state = idle(0);
        state.insert("playing".into(), json!(true));
        let plan = plan_command(&state, &json!({"action": "togglePlay"})).unwrap();
        let Plan::Patch(patch) = plan else {
            panic!("patch")
        };
        assert_eq!(patch["playing"], json!(false));
        assert_eq!(patch["status"], json!("paused"));
    }

    #[test]
    fn skip_clamps_at_zero_and_reports_the_position() {
        let mut state = idle(0);
        state.insert("positionMs".into(), json!(1500));
        let Plan::Patch(patch) =
            plan_command(&state, &json!({"action": "skip", "deltaMs": -5000})).unwrap()
        else {
            panic!("patch")
        };
        assert_eq!(patch["positionMs"], json!(0));
    }

    #[test]
    fn unsupported_actions_are_bad_requests() {
        let err = plan_command(&idle(0), &json!({"action": "teleport"})).unwrap_err();
        assert_eq!(err.status, 400);
        assert_eq!(err.message, "Unsupported cast action: teleport");
    }
}
