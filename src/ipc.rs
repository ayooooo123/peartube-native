//! Newline-delimited framing for the worklet protocol.

use std::borrow::Cow;

use serde_json::{Value, json};

/// A host event is routed independently of method requests.
#[derive(Debug, PartialEq)]
pub enum Incoming {
    Event(Value),
    Request {
        id: Option<Value>,
        method: Value,
        params: Value,
    },
    Malformed,
}

/// Decode the same message envelope used by `worker.js`.
pub fn decode(line: &str) -> Incoming {
    let Ok(message) = serde_json::from_str::<Value>(&well_formed(line)) else {
        return Incoming::Malformed;
    };
    if message.get("event").and_then(Value::as_str).is_some() {
        return Incoming::Event(message);
    }
    let params = message
        .get("params")
        .filter(|v| crate::app_settings::js_truthy(v))
        .cloned()
        .unwrap_or_else(|| json!({}));
    Incoming::Request {
        id: message.get("id").cloned(),
        method: message.get("method").cloned().unwrap_or(Value::Null),
        params,
    }
}

fn hex4(bytes: &[u8]) -> Option<u32> {
    let mut value = 0;
    for byte in bytes {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        value = value * 16 + u32::from(digit);
    }
    Some(value)
}

/// A JavaScript string can hold a lone surrogate and `toWellFormed` replaces it on the way out; a
/// Rust string cannot, so replace the escapes before parsing. Valid pairs are left alone.
fn well_formed(input: &str) -> Cow<'_, str> {
    if !input.contains("\\u") {
        return Cow::Borrowed(input);
    }
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut copied = 0;
    let mut index = 0;
    let mut replaced = false;
    let cut = |out: &mut String, copied: &mut usize, at: usize| {
        out.push_str(&input[*copied..at]);
        out.push_str("\\ufffd");
        *copied = at + 6;
    };
    while index + 1 < bytes.len() {
        if bytes[index] != b'\\' {
            index += 1;
            continue;
        }
        match bytes[index + 1] {
            b'\\' => index += 2,
            b'u' if index + 6 <= bytes.len() && hex4(&bytes[index + 2..index + 6]).is_some() => {
                let code = hex4(&bytes[index + 2..index + 6]).unwrap_or(0);
                let paired = (0xD800..=0xDBFF).contains(&code)
                    && index + 12 <= bytes.len()
                    && bytes[index + 6] == b'\\'
                    && bytes[index + 7] == b'u'
                    && hex4(&bytes[index + 8..index + 12])
                        .is_some_and(|low| (0xDC00..=0xDFFF).contains(&low));
                if paired {
                    index += 12;
                } else if (0xD800..=0xDFFF).contains(&code) {
                    cut(&mut out, &mut copied, index);
                    replaced = true;
                    index += 6;
                } else {
                    index += 6;
                }
            }
            _ => index += 2,
        }
    }
    if !replaced {
        return Cow::Borrowed(input);
    }
    out.push_str(&input[copied..]);
    Cow::Owned(out)
}

/// Encode a response, omitting the ID when the request did not contain one.
pub fn response(id: Option<Value>, result: Result<Value, String>) -> String {
    let mut message = serde_json::Map::new();
    if let Some(id) = id {
        message.insert("id".into(), id);
    }
    match result {
        Ok(value) => {
            message.insert("result".into(), value);
        }
        Err(error) => {
            message.insert("error".into(), Value::String(error));
        }
    }
    let mut line = Value::Object(message).to_string();
    line.push('\n');
    line
}

/// Dispatch operations that act directly on the current node.
pub async fn node_method<N: crate::node::Node>(
    node: Option<&N>,
    method: &str,
    params: &Value,
    update: impl Fn(),
) -> Option<Result<Value, String>> {
    use crate::node::api::{SearchOptions, SourceSpec};
    let supported = matches!(
        method,
        "search"
            | "focusSource"
            | "setPlayerActive"
            | "warm"
            | "addSource"
            | "removeSource"
            | "remove"
            | "identity"
            | "revokeWriter"
            | "rotateEpoch"
            | "clearCache"
            | "setCacheMax"
    );
    if !supported {
        return None;
    }
    let Some(n) = node else {
        return Some(Err("Not started".into()));
    };
    let field = |name: &str| params.get(name).and_then(Value::as_str).unwrap_or("");
    let flag = |name: &str| params.get(name).is_some_and(crate::app_settings::js_truthy);
    let result = async {
        let value = match method {
            "search" => {
                let options = SearchOptions {
                    source: Some(params.get("source").and_then(Value::as_str).map(str::to_owned)),
                    query: Some(params.get("query").and_then(Value::as_str).map(str::to_owned)),
                    limit: Some(params.get("limit").and_then(Value::as_number).cloned().unwrap_or_else(|| 0.into())),
                    offset: Some(params.get("offset").and_then(Value::as_number).cloned().unwrap_or_else(|| 0.into())),
                };
                json!(n.search(params.get("id").and_then(Value::as_str), options).await.map_err(|e| e.to_string())?)
            }
            "focusSource" => json!({"ok": n.focus_source(field("key")).await.map_err(|e| e.to_string())?}),
            "setPlayerActive" => json!(n.set_player_active(flag("active")).await),
            "warm" => {
                let entry = n.find_entry(field("key")).await.map_err(|e| e.to_string())?;
                if let Some(entry) = &entry { n.warm(entry).await.map_err(|e| e.to_string())?; }
                json!({"ok": entry.is_some()})
            }
            "addSource" => {
                let spec: SourceSpec = serde_json::from_value(params.clone()).map_err(|e| e.to_string())?;
                let source = n.add_source(spec, true).await.map_err(|e| e.to_string())?;
                json!({"source": source, "sources": n.list_sources().await, "discovered": n.discovered_sources().await.map_err(|e| e.to_string())?})
            }
            "removeSource" => {
                let res = n.remove_source(field("key")).await.map_err(|e| e.to_string())?;
                json!({"removed": res.removed, "sources": n.list_sources().await, "discovered": n.discovered_sources().await.map_err(|e| e.to_string())?})
            }
            "remove" => {
                n.remove(field("key")).await.map_err(|e| e.to_string())?;
                update();
                json!({"removed": true, "key": params.get("key")})
            }
            "identity" => json!(n.identity(flag("revealMnemonic")).await.map_err(|e| e.to_string())?),
            "revokeWriter" => {
                let res = n.revoke_writer(field("writer"), flag("purgeEntries")).await.map_err(|e| e.to_string())?;
                update(); json!(res)
            }
            "rotateEpoch" => {
                let res = n.rotate_epoch().await.map_err(|e| e.to_string())?;
                update(); json!(res)
            }
            "clearCache" => {
                let res = n.clear_cache(params.get("key").and_then(Value::as_str), None).await.map_err(|e| e.to_string())?;
                update(); json!(res)
            }
            "setCacheMax" => {
                let bytes = n.set_cache_max_bytes(params.get("maxBytes").and_then(Value::as_f64).unwrap_or(0.0)).await;
                update(); json!({"cacheMaxBytes": bytes})
            }
            _ => unreachable!(),
        };
        Ok(value)
    }.await;
    Some(result)
}
/// Accumulates host bytes and yields each nonempty, newline-terminated message.
#[derive(Default)]
pub struct Framer {
    pending: Vec<u8>,
}

impl Framer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deliver complete lines in arrival order, retaining an incomplete trailing line.
    pub fn push(&mut self, bytes: &[u8], mut line: impl FnMut(&str)) {
        self.pending.extend_from_slice(bytes);
        let mut start = 0;
        for end in 0..self.pending.len() {
            if self.pending[end] == b'\n' {
                if end != start {
                    line(&String::from_utf8_lossy(&self.pending[start..end]));
                }
                start = end + 1;
            }
        }
        if start != 0 {
            self.pending.drain(..start);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Framer;

    #[test]
    fn partial_lines_and_utf8_cross_chunks() {
        let mut framer = Framer::new();
        let mut lines = Vec::new();
        framer.push(b"\n{\"v\":\"\xe2", |s| lines.push(s.to_owned()));
        framer.push(b"\x82\xac\"}\nfirst\npar", |s| lines.push(s.to_owned()));
        framer.push(b"t\n", |s| lines.push(s.to_owned()));
        assert_eq!(lines, ["{\"v\":\"€\"}", "first", "part"]);
    }
}
