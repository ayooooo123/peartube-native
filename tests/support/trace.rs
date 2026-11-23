//! What the oracle tests share when they compare a trace of the port with the app's.

use serde_json::Value;

/// Where a record of the port first differs from the app's, as a path and both values; `None` when
/// the two are the same text. The order of the keys counts: the traces are JSON text.
pub fn first_difference(here: &Value, app: &Value, path: &str) -> Option<String> {
    let brief = |value: &Value| {
        let text = value.to_string();
        match text.char_indices().nth(160) {
            Some((cut, _)) => format!("{}…", &text[..cut]),
            None => text,
        }
    };
    match (here, app) {
        (Value::Object(a), Value::Object(b)) if a.keys().eq(b.keys()) => a
            .iter()
            .find_map(|(key, value)| first_difference(value, &b[key], &format!("{path}.{key}"))),
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => a
            .iter()
            .zip(b)
            .enumerate()
            .find_map(|(i, (a, b))| first_difference(a, b, &format!("{path}[{i}]"))),
        _ if here == app => None,
        _ => Some(format!(
            "{path}: {} here, {} in the app",
            brief(here),
            brief(app)
        )),
    }
}
