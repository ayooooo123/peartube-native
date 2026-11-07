//! The line protocol over [`crate::app::App`]: `worklet/worker.js`'s wire format.
//!
//! Mobile and relay hosts hand the backend newline-delimited JSON and read lines back. Everything
//! a line can ask for is a typed [`App`] method; this module only decodes the params into the
//! method's request and serializes its reply.

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::app::{App, AppEvent};
use crate::ipc::{self, Incoming};
use crate::node::Node;
use crate::node::api::{ApiServer, CastFactory, Clock, LanFactory, NodeFactory, SourceSpec};

/// Handle one input line; answers with the response line when the input was a request.
///
/// The host writes the events the app emitted, in order, ahead of this response.
pub(crate) async fn input<N, F, C, L, A, K>(
    app: &mut App<N, F, C, L, A, K>,
    line: &str,
) -> Option<String>
where
    N: Node + 'static,
    F: NodeFactory<Node = N> + 'static,
    C: Clock + 'static,
    L: LanFactory<N> + 'static,
    A: ApiServer<N> + 'static,
    K: CastFactory<N> + 'static,
{
    match ipc::decode(line) {
        Incoming::Malformed => {
            app.fatal("Malformed message from app");
            None
        }
        Incoming::Event(event) => {
            app.host_event(event).await;
            None
        }
        Incoming::Request { id, method, params } => {
            let response = ipc::response(id, call(app, &method_name(&method), params).await);
            Some(response)
        }
    }
}

/// One event as the line a JSON host reads.
pub(crate) fn event_line(event: &AppEvent) -> Option<String> {
    serde_json::to_string(event).ok().map(|line| line + "\n")
}

async fn call<N, F, C, L, A, K>(
    app: &mut App<N, F, C, L, A, K>,
    method: &str,
    params: Value,
) -> Result<Value, String>
where
    N: Node + 'static,
    F: NodeFactory<Node = N> + 'static,
    C: Clock + 'static,
    L: LanFactory<N> + 'static,
    A: ApiServer<N> + 'static,
    K: CastFactory<N> + 'static,
{
    match method {
        "init" => value(app.init(decode(params)?).await),
        "start" => value(app.start(decode(params)?).await),
        "normalizeSettings" => value(app.normalize_settings(decode(params)?).await),
        "status" => value(app.status().await),
        "search" => value(app.search(decode(params)?).await),
        "focusSource" => value(app.focus_source(decode(params)?).await),
        "setPlayerActive" => value(app.set_player_active(decode(params)?).await),
        "warm" => value(app.warm(decode(params)?).await),
        "addSource" => value(app.add_source(decode::<SourceSpec>(params)?).await),
        "removeSource" => value(app.remove_source(decode(params)?).await),
        "remove" => value(app.remove_entry(decode(params)?).await),
        "identity" => value(app.identity(decode(params)?).await),
        "revokeWriter" => value(app.revoke_writer(decode(params)?).await),
        "rotateEpoch" => value(app.rotate_epoch().await),
        "clearCache" => value(app.clear_cache(decode(params)?).await),
        "setCacheMax" => value(app.set_cache_max(decode(params)?).await),
        "castStatus" => value(app.cast_status().await),
        "castPair" => value(app.cast_pair(params).await),
        "castRemove" => value(app.cast_remove(decode(params)?).await),
        "castPlay" => app.cast_play(params).await,
        "castAttach" => app.cast_attach(params).await,
        "castControl" => app.cast_control(params).await,
        "castStop" => app.cast_stop(params).await,
        "receiverStart" => value(app.receiver_start(decode(params)?).await),
        "receiverReportState" => app.receiver_report_state(params).await,
        "openUri" => value(app.open_uri(decode(params)?).await),
        other => Err(format!("Unknown method {other}")),
    }
}

/// The params of a call, decoded into the method's request; a missing object is an empty one.
fn decode<T: DeserializeOwned>(params: Value) -> Result<T, String> {
    let params = match params {
        Value::Null => Value::Object(serde_json::Map::new()),
        params => params,
    };
    serde_json::from_value(params).map_err(|error| error.to_string())
}

/// A typed reply as the value the line protocol carries.
fn value<T: Serialize>(reply: Result<T, String>) -> Result<Value, String> {
    reply.and_then(|reply| serde_json::to_value(reply).map_err(|error| error.to_string()))
}

/// The method name as the app sent it, as `String(msg.method)` would print it.
fn method_name(method: &Value) -> String {
    match method {
        Value::String(name) => name.clone(),
        Value::Null => "undefined".to_owned(),
        other => other.to_string(),
    }
}
