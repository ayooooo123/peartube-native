//! The newline-delimited JSON protocol the cast controller and receiver speak on the Noise pipe,
//! ported from `connectNoiseOnce`, `connectNoiseRpc` and `handleNoiseLine` in `src/cast.js`.
//!
//! Requests are `{ id, type, ...payload }`; replies are `{ id, ok: true, result }` or
//! `{ id, ok: false, error }`; subscriptions receive `{ event: "state", state }` lines.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::{Map, Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

use super::CastError;
use super::state::text_value;
use super::traits::{DhtConnect, Duplex};

/// The app's `connectNoiseRpc` retries exactly these codes while the DHT finishes its NAT check.
pub const RETRYABLE_CODES: [&str; 4] = [
    "HOLEPUNCH_ABORTED",
    "PEER_CONNECTION_FAILED",
    "TRY_LATER",
    "ERR_PRIVACY_UNAVAILABLE",
];

/// `connectNoiseOnce`'s 15 s dial attempt.
pub const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(15);
/// `connectNoiseRpc`'s overall deadline.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(90);
/// The `info` handshake timeout.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(2_500);
/// `connectNoiseRpc`'s pause between attempts.
pub const RETRY_DELAY: Duration = Duration::from_secs(1);

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Value, CastError>>>>>;

/// A live controller→receiver RPC connection.
pub struct RpcClient {
    writer: mpsc::Sender<Value>,
    pending: Pending,
    closed: Arc<AtomicBool>,
    next_id: AtomicU64,
}

/// Callbacks `connectNoiseOnce` takes.
#[derive(Clone, Default)]
pub struct ConnectOptions {
    pub relay_through: Option<Vec<String>>,
    pub on_state: Option<Arc<dyn Fn(Value) + Send + Sync>>,
    pub on_close: Option<Arc<dyn Fn() + Send + Sync>>,
    pub attempt_timeout: Option<Duration>,
    pub timeout: Option<Duration>,
}

impl RpcClient {
    /// Dials once and completes the `info` handshake.
    pub async fn connect(
        conn: Box<dyn Duplex>,
        options: &ConnectOptions,
    ) -> Result<Self, CastError> {
        let (reader, writer) = tokio::io::split(conn);
        let (tx, mut rx) = mpsc::channel::<Value>(16);
        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));

        let writer_task = tokio::spawn(async move {
            let mut writer = writer;
            while let Some(message) = rx.recv().await {
                let mut line = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_owned());
                line.push('\n');
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });

        let client = Self {
            writer: tx,
            pending: Arc::clone(&pending),
            closed: Arc::clone(&closed),
            next_id: AtomicU64::new(1),
        };

        let reader_pending = Arc::clone(&pending);
        let reader_closed = Arc::clone(&closed);
        let on_state = options.on_state.clone();
        let on_close = options.on_close.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(reader).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.is_empty() {
                    continue;
                }
                let Ok(message) = serde_json::from_str::<Value>(&line) else {
                    continue;
                };
                if let Some(id) = message.get("id").and_then(Value::as_u64)
                    && let Some(slot) = reader_pending.lock().expect("cast rpc pending").remove(&id)
                {
                    let reply = if message.get("ok").is_some_and(super::state::truthy) {
                        Ok(message.get("result").cloned().unwrap_or(json!({})))
                    } else {
                        let error = message
                            .get("error")
                            .map(text_value)
                            .unwrap_or_else(|| "Receiver P2P error".to_owned());
                        Err(CastError::new(error, 502))
                    };
                    let _ = slot.send(reply);
                    continue;
                }
                if message.get("event").and_then(Value::as_str) == Some("state")
                    && let (Some(state), Some(sink)) = (message.get("state"), on_state.as_ref())
                {
                    sink(state.clone());
                }
            }
            reader_closed.store(true, Ordering::SeqCst);
            for (_, slot) in reader_pending.lock().expect("cast rpc pending").drain() {
                let _ = slot.send(Err(CastError::new(
                    "Receiver P2P Noise connection closed",
                    502,
                )));
            }
            writer_task.abort();
            if let Some(sink) = on_close.as_ref() {
                sink();
            }
        });

        let attempt = options.attempt_timeout.unwrap_or(ATTEMPT_TIMEOUT);
        match client
            .call("info", Map::new(), HANDSHAKE_TIMEOUT.min(attempt))
            .await
        {
            Ok(_) => Ok(client),
            Err(err) => {
                client.close();
                Err(err)
            }
        }
    }

    /// Rejects a retryable dial failure only when the deadline still allows another attempt.
    pub fn retryable(err: &CastError) -> bool {
        RETRYABLE_CODES
            .iter()
            .any(|code| err.message.contains(code))
            || err.message.contains("timed out")
            || err.message.contains("closed")
    }

    pub fn closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// One request/response, `{ id, type, ...payload }`.
    pub async fn call(
        &self,
        kind: &str,
        payload: Map<String, Value>,
        timeout: Duration,
    ) -> Result<Value, CastError> {
        if self.closed() {
            return Err(CastError::new("P2P connection closed", 502));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut message = Map::new();
        message.insert("id".into(), json!(id));
        message.insert("type".into(), json!(kind));
        for (key, value) in payload {
            message.insert(key, value);
        }
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("cast rpc pending")
            .insert(id, tx);
        if self.writer.send(Value::Object(message)).await.is_err() {
            self.pending.lock().expect("cast rpc pending").remove(&id);
            return Err(CastError::new("P2P connection closed", 502));
        }
        match tokio::time::timeout(timeout, rx).await {
            Err(_) => {
                self.pending.lock().expect("cast rpc pending").remove(&id);
                Err(CastError::new(
                    format!("Receiver P2P {kind} timed out"),
                    504,
                ))
            }
            Ok(Err(_)) => Err(CastError::new("Receiver P2P connection closed", 502)),
            Ok(Ok(result)) => result,
        }
    }

    /// `rpc.close()`: fails every pending call and drops the pipe.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for (_, slot) in self.pending.lock().expect("cast rpc pending").drain() {
            let _ = slot.send(Err(CastError::new("Closed", 502)));
        }
        let _ = self.writer.try_send(Value::Null);
    }
}

/// `connectNoiseRpc`: retries transient holepunch aborts until the deadline.
pub async fn connect_with_retry(
    dht: &dyn DhtConnect,
    public_key: [u8; 32],
    options: &ConnectOptions,
) -> Result<RpcClient, CastError> {
    let deadline = tokio::time::Instant::now() + options.timeout.unwrap_or(CONNECT_TIMEOUT);
    let mut last: Option<CastError> = None;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        let attempt = options
            .attempt_timeout
            .unwrap_or(ATTEMPT_TIMEOUT)
            .min(remaining.min(Duration::from_secs(30)));
        let dial = dht.connect(public_key, options.relay_through.clone());
        let dialled = match tokio::time::timeout(attempt, dial).await {
            Err(_) => {
                last = Some(CastError::new(
                    "Timed out connecting to receiver over P2P Noise",
                    504,
                ));
                None
            }
            Ok(Err(err)) => {
                last = Some(CastError::new(err.message.clone(), 502));
                None
            }
            Ok(Ok(conn)) => Some(conn),
        };
        if let Some(conn) = dialled {
            let mut attempt_options = options.clone();
            attempt_options.attempt_timeout = Some(attempt);
            match RpcClient::connect(conn, &attempt_options).await {
                Ok(client) => return Ok(client),
                Err(err) => last = Some(err),
            }
        }
        let failed = last
            .clone()
            .unwrap_or_else(|| CastError::new("P2P dial failed", 502));
        let code_retryable = failed.message.contains("HOLEPUNCH_ABORTED")
            || failed.message.contains("PEER_CONNECTION_FAILED")
            || failed.message.contains("TRY_LATER")
            || failed.message.contains("ERR_PRIVACY_UNAVAILABLE")
            || failed.message.contains("timed out");
        if !code_retryable || tokio::time::Instant::now() + RETRY_DELAY >= deadline {
            return Err(failed);
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
    Err(last
        .unwrap_or_else(|| CastError::new("Timed out connecting to receiver over P2P Noise", 504)))
}

/// Writes to one accepted Noise connection (a reply, or a state broadcast later).
#[derive(Clone)]
pub struct NoiseWriter {
    tx: mpsc::Sender<Value>,
}

impl NoiseWriter {
    pub async fn send(&self, message: Value) -> bool {
        self.tx.send(message).await.is_ok()
    }

    /// Non-blocking variant for state broadcasts.
    pub fn try_send(&self, message: Value) -> bool {
        self.tx.try_send(message).is_ok()
    }
}

/// What a receiver's message handler answers: the object merged with `id`, or `None` for silence.
pub type Dispatch =
    Arc<dyn Fn(Map<String, Value>, NoiseWriter) -> BoxFuture<'static, Option<Value>> + Send + Sync>;

/// `createServer`'s per-connection loop: one line at a time through `dispatch`.
pub fn serve_connection(conn: Box<dyn Duplex>, dispatch: Dispatch) {
    tokio::spawn(async move {
        let (reader, writer) = tokio::io::split(conn);
        let (tx, mut rx) = mpsc::channel::<Value>(16);
        let writer_task = tokio::spawn(async move {
            let mut writer = writer;
            while let Some(message) = rx.recv().await {
                let mut line = serde_json::to_string(&message).unwrap_or_else(|_| "{}".to_owned());
                line.push('\n');
                if writer.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() {
                continue;
            }
            let Ok(Value::Object(message)) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            let reply = dispatch(message.clone(), NoiseWriter { tx: tx.clone() }).await;
            let Some(reply) = reply else { continue };
            let mut out = Map::new();
            if let Some(id) = message.get("id") {
                out.insert("id".into(), id.clone());
            }
            if let Value::Object(fields) = reply {
                for (key, value) in fields {
                    out.insert(key, value);
                }
            }
            if tx.send(Value::Object(out)).await.is_err() {
                break;
            }
        }
        writer_task.abort();
    });
}

/// The `{ok:true,result}` / `{ok:false,error}` bodies `handleNoiseLine` writes.
pub fn ok_reply(result: Value) -> Value {
    json!({ "ok": true, "result": result })
}

pub fn error_reply(message: &str) -> Value {
    json!({ "ok": false, "error": message })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::duplex;

    #[tokio::test]
    async fn a_client_call_round_trips_over_a_duplex() {
        let (ours, theirs) = duplex(4096);
        let dispatch: Dispatch = Arc::new(|message, _writer| {
            Box::pin(async move {
                if message.get("type").and_then(Value::as_str) == Some("info") {
                    return Some(ok_reply(json!({ "protocol": "peartube-cast/v1" })));
                }
                assert_eq!(message.get("type").and_then(Value::as_str), Some("cmd"));
                Some(ok_reply(json!({ "state": { "status": "idle" } })))
            })
        });
        serve_connection(Box::new(theirs), dispatch);
        let client = RpcClient::connect(Box::new(ours), &ConnectOptions::default())
            .await
            .expect("handshake");
        // The `info` handshake went through the same dispatcher.
        let mut payload = Map::new();
        payload.insert("action".into(), json!("status"));
        let result = client
            .call("cmd", payload, Duration::from_secs(2))
            .await
            .expect("call");
        assert_eq!(result["state"]["status"], json!("idle"));
    }

    #[tokio::test]
    async fn an_error_reply_surfaces_the_message() {
        let (ours, theirs) = duplex(4096);
        let dispatch: Dispatch = Arc::new(|_message, _writer| {
            Box::pin(async move { Some(error_reply("Invalid TV pairing code")) })
        });
        serve_connection(Box::new(theirs), dispatch);
        match RpcClient::connect(Box::new(ours), &ConnectOptions::default()).await {
            Ok(_) => panic!("handshake must fail on an error reply"),
            Err(err) => assert_eq!(err.message, "Invalid TV pairing code"),
        }
    }
}
