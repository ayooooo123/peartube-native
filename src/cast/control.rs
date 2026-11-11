//! `src/cast-control.js`: the control channel between a cast sender and a receiver
//! (`peartube-cast/v1`), one Protomux channel on the peer connection the two nodes already share.
//! A cast needs no connection, port or server of its own, and its messages travel under that
//! connection's Noise encryption.
//!
//! ```text
//! sender -> receiver   {id, type: 'pair' | 'subscribe' | 'cmd', ...}
//! receiver -> sender   {id, ok, result | error}, and {event: 'state', state} once subscribed
//! ```
//!
//! Any peer that connects to a receiver can open this channel, so the receiver decides per
//! request what it allows.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use protomux::{ChannelOptions, ChannelSender, Event, Mux};
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, oneshot};

use super::CastError;

/// The Protomux protocol name.
pub const PROTOCOL: &str = "peartube-cast/v1";
/// Largest frame the channel accepts; anything longer is dropped.
pub const MAX_FRAME: usize = 64 * 1024;
/// The app's default for every request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// The channel's only message type (the app's single `addMessage`).
const MESSAGE_KIND: u64 = 0;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

/// One JSON object per message, in the app's frame codec: `c.json` is `utf8`, so the payload is
/// `[compact uint length][UTF-8 JSON]`. Anything else a peer sends (too long, broken JSON,
/// `null`, a number) decodes to `None` and is dropped: Protomux closes the whole connection on a
/// decoder that throws.
pub fn decode_frame(data: &[u8]) -> Option<Value> {
    if data.len() > MAX_FRAME {
        return None;
    }
    let mut reader = compact_encoding::Reader::new(Bytes::copy_from_slice(data));
    let text = reader.buffer().ok()?;
    let value: Value = serde_json::from_slice(&text).ok()?;
    match value {
        Value::Object(_) => Some(value),
        _ => None,
    }
}

/// The same codec on the way out.
pub fn encode_frame(message: &Value) -> Option<Bytes> {
    let json = serde_json::to_vec(message).ok()?;
    let mut writer =
        compact_encoding::Writer::with_capacity(compact_encoding::buffer_len(json.len()));
    writer.buffer(&json);
    Some(writer.finish())
}

/// The answer a receiver's request handler gives, or the error text it refuses with.
pub type RequestFuture = BoxFuture<'static, Result<Value, String>>;
pub type RequestHandler = Arc<dyn Fn(Value, Control) -> RequestFuture + Send + Sync>;
pub type EventHandler = Arc<dyn Fn(Value) + Send + Sync>;
pub type OpenHandler = Arc<dyn Fn(Control) + Send + Sync>;

/// What a control channel does with what it receives; the app passes `onRequest` from
/// `serveControl` and `onEvent`/`onClose` from `openControl`.
#[derive(Clone, Default)]
pub struct ControlHandlers {
    pub on_request: Option<RequestHandler>,
    pub on_event: Option<EventHandler>,
    pub on_open: Option<OpenHandler>,
    pub on_close: Option<OpenHandler>,
}

struct Inner {
    mux: Mux,
    sender: Mutex<Option<ChannelSender>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, CastError>>>>,
    seq: AtomicU64,
    opened: AtomicBool,
    closed: AtomicBool,
    changed: Notify,
    handlers: ControlHandlers,
}

/// One half of the channel on a connection, as `createControl` returns it.
#[derive(Clone)]
pub struct Control {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("opened", &self.is_opened())
            .field("closed", &self.is_closed())
            .field("mux_drained", &self.inner.mux.is_drained())
            .finish()
    }
}

impl Control {
    fn new(mux: Mux, sender: ChannelSender, handlers: ControlHandlers) -> Self {
        Self {
            inner: Arc::new(Inner {
                mux,
                sender: Mutex::new(Some(sender)),
                pending: Mutex::new(HashMap::new()),
                seq: AtomicU64::new(1),
                opened: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                changed: Notify::new(),
                handlers,
            }),
        }
    }

    pub fn mux(&self) -> &Mux {
        &self.inner.mux
    }

    /// Unique per channel; the receiver keys its subscribers and backpressure state on it.
    pub fn id(&self) -> u64 {
        Arc::as_ptr(&self.inner) as usize as u64
    }

    /// The channel closed (or its connection went).
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    /// The remote opened its side.
    pub fn is_opened(&self) -> bool {
        self.inner.opened.load(Ordering::Acquire)
    }

    /// Sends an event or a reply; `false` when the connection wants the sender to wait for
    /// `drained()` (or the channel is gone).
    pub fn push(&self, message: Value) -> bool {
        let Some(bytes) = encode_frame(&message) else {
            return false;
        };
        self.raw_push(bytes)
    }

    fn raw_push(&self, bytes: Bytes) -> bool {
        let sender = lock(&self.inner.sender).clone();
        match sender {
            Some(sender) => sender.send_raw(MESSAGE_KIND, &bytes),
            None => false,
        }
    }

    /// Sends a message, waiting for the connection to drain while it is backpressured. `false`
    /// once the channel is closed.
    async fn write(&self, bytes: Bytes, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.is_closed() {
                return false;
            }
            if self.raw_push(bytes.clone()) {
                return true;
            }
            if self.is_closed() {
                return false;
            }
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() {
                return false;
            }
            let _ = tokio::time::timeout(left, self.inner.mux.drained()).await;
        }
    }

    /// `control.call(type, payload, timeoutMs)`: a request answered by the peer.
    pub async fn call(
        &self,
        kind: &str,
        mut payload: Map<String, Value>,
        timeout: Duration,
    ) -> Result<Value, CastError> {
        if self.is_closed() {
            return Err(CastError::new("Receiver connection closed", 0));
        }
        let id = self.inner.seq.fetch_add(1, Ordering::AcqRel);
        let (tx, rx) = oneshot::channel();
        lock(&self.inner.pending).insert(id, tx);
        payload.insert("id".to_owned(), json!(id));
        payload.insert("type".to_owned(), json!(kind));
        let bytes = match encode_frame(&Value::Object(payload)) {
            Some(bytes) => bytes,
            None => {
                lock(&self.inner.pending).remove(&id);
                return Err(CastError::new("The request is not JSON", 0));
            }
        };
        if !self.write(bytes, timeout).await {
            lock(&self.inner.pending).remove(&id);
            return Err(CastError::new("Receiver connection closed", 0));
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CastError::new("Receiver connection closed", 0)),
            Err(_) => {
                lock(&self.inner.pending).remove(&id);
                Err(CastError::new(format!("Receiver {kind} timed out"), 0))
            }
        }
    }

    /// Waits until the connection is drained (`conn.once('drain')`), for the receiver's state
    /// pushes. Returns at once when it already is, or when the channel closed.
    pub async fn drained(&self) {
        if self.is_closed() || self.inner.mux.is_drained() {
            return;
        }
        let _ = tokio::time::timeout(Duration::from_secs(30), self.inner.mux.drained()).await;
    }

    pub fn close(&self) {
        let sender = lock(&self.inner.sender).take();
        if let Some(sender) = sender {
            sender.close();
        }
        self.finish();
    }

    /// Resolves once the channel closed.
    pub async fn closed(&self) {
        loop {
            if self.is_closed() {
                return;
            }
            let notified = self.inner.changed.notified();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    async fn opened(&self) {
        loop {
            if self.is_opened() || self.is_closed() {
                return;
            }
            let notified = self.inner.changed.notified();
            if self.is_opened() || self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    fn finish(&self) {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let pending = std::mem::take(&mut *lock(&self.inner.pending));
        for (_, waiter) in pending {
            let _ = waiter.send(Err(CastError::new("Receiver connection closed", 0)));
        }
        self.inner.changed.notify_waiters();
        if let Some(handler) = self.inner.handlers.on_close.clone() {
            handler(self.clone());
        }
    }

    fn answer(&self, id: Value, result: Result<Value, String>) {
        let message = match result {
            Ok(result) => json!({ "id": id, "ok": true, "result": result }),
            Err(error) => json!({ "id": id, "ok": false, "error": error }),
        };
        let Some(bytes) = encode_frame(&message) else {
            return;
        };
        let control = self.clone();
        tokio::spawn(async move {
            control.write(bytes, DEFAULT_TIMEOUT).await;
        });
    }

    /// Handles one message the reader task received.
    fn dispatch(&self, data: Bytes) {
        let Some(message) = decode_frame(&data) else {
            return;
        };
        if let Some(id) = message.get("id").and_then(Value::as_u64) {
            let waiter = lock(&self.inner.pending).remove(&id);
            if let Some(waiter) = waiter {
                let result = if message.get("ok").is_some_and(js_truthy) {
                    Ok(message.get("result").cloned().unwrap_or(Value::Null))
                } else {
                    Err(match message.get("error").and_then(Value::as_str) {
                        Some(error) if !error.is_empty() => error.to_owned(),
                        _ => "Receiver error".to_owned(),
                    })
                };
                let _ = waiter.send(result.map_err(|error| CastError::new(error, 0)));
                return;
            }
        }
        if message.get("event").is_some() {
            if let Some(handler) = self.inner.handlers.on_event.clone() {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(message)));
            }
            return;
        }
        if self.inner.handlers.on_request.is_some()
            && message.get("type").is_some_and(Value::is_string)
        {
            let Some(handler) = self.inner.handlers.on_request.clone() else {
                return;
            };
            let control = self.clone();
            let control_for_answer = control.clone();
            let id = message.get("id").cloned().unwrap_or(Value::Null);
            tokio::spawn(async move {
                let result = handler(message, control).await;
                control_for_answer.answer(id, result);
            });
        }
    }
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number
            .as_f64()
            .is_some_and(|value| value != 0.0 && !value.is_nan()),
        Value::String(text) => !text.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Builds one half of the channel on `mux`, not yet opened; `None` when this connection already has
/// one, or the mux is gone.
fn create_control(mux: &Mux, handlers: ControlHandlers) -> Option<Control> {
    if mux.is_opened(PROTOCOL, None) {
        return None;
    }
    let channel = mux.open_channel(ChannelOptions::new(PROTOCOL))?;
    let sender = channel.sender();
    let control = Control::new(mux.clone(), sender, handlers);
    let reading = control.clone();
    let mut channel = channel;
    tokio::spawn(async move {
        while let Some(event) = channel.recv().await {
            match event {
                Event::Opened { .. } => {
                    reading.inner.opened.store(true, Ordering::Release);
                    reading.inner.changed.notify_waiters();
                    if let Some(handler) = reading.inner.handlers.on_open.clone() {
                        handler(reading.clone());
                    }
                }
                Event::Message(message) => reading.dispatch(message.data),
                Event::Closed { .. } => break,
            }
        }
        reading.finish();
    });
    Some(control)
}

/// `serveControl(conn, { onRequest, onOpen, onClose })`: the receiver's half, answering every
/// sender that opens the channel on this connection. Returns a handle that stops answering.
pub fn serve_control(mux: &Mux, handlers: ControlHandlers) -> ServeHandle {
    let mut pairs = mux.pair(PROTOCOL, None);
    let open_on = mux.clone();
    let task = tokio::spawn(async move {
        while let Some(request) = pairs.recv().await {
            let _ = create_control(&open_on, handlers.clone());
            drop(request);
        }
    });
    ServeHandle {
        mux: mux.clone(),
        task: Some(task),
    }
}

/// Stops one connection's receiver half (`mux.unpair({ protocol })`).
pub struct ServeHandle {
    mux: Mux,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ServeHandle {
    pub fn stop(&mut self) {
        self.mux.unpair(PROTOCOL, None);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl Drop for ServeHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// `openControl(conn, { onEvent, onClose }, timeoutMs)`: the sender's half. Resolves once the
/// receiver answers the open; a peer that is not a receiver refuses the channel, which is an error.
pub async fn open_control(
    mux: &Mux,
    handlers: ControlHandlers,
    timeout: Duration,
) -> Result<Control, CastError> {
    let control = create_control(
        mux,
        ControlHandlers {
            on_request: None,
            ..handlers
        },
    )
    .ok_or_else(|| CastError::new("A cast session is already open with this receiver", 0))?;
    match tokio::time::timeout(timeout, control.opened()).await {
        Ok(()) if control.is_opened() => Ok(control),
        Ok(()) => Err(CastError::new("This peer does not take casts", 0)),
        Err(_) => {
            control.close();
            Err(CastError::new("Timed out opening the cast channel", 0))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protomux::StreamInfo;
    use protomux::memory::duplex;
    use std::sync::atomic::AtomicUsize;

    fn pair_of_muxes() -> (Mux, Mux) {
        let ((ar, aw), (br, bw)) = duplex(64);
        let info = StreamInfo {
            is_initiator: true,
            ..StreamInfo::default()
        };
        let remote = StreamInfo::default();
        (Mux::new(ar, aw, info), Mux::new(br, bw, remote))
    }

    #[test]
    fn frames_are_length_prefixed_json_like_the_app() {
        // `c.json` is `utf8`: one compact uint length, then the JSON text.
        let value = json!({ "id": 1, "type": "pair", "code": "ABC123" });
        let text = br#"{"id":1,"type":"pair","code":"ABC123"}"#;
        let mut expected = vec![text.len() as u8];
        expected.extend_from_slice(text);
        assert_eq!(&encode_frame(&value).expect("encoded")[..], &expected[..]);
        assert_eq!(decode_frame(&expected), Some(value));
    }

    #[test]
    fn frames_drop_what_the_app_drops() {
        let frame = |text: &[u8]| {
            let mut bytes = vec![text.len() as u8];
            bytes.extend_from_slice(text);
            bytes
        };
        assert!(decode_frame(&frame(b"{\"type\":\"pair\"}")).is_some());
        assert!(decode_frame(&frame(b"null")).is_none());
        assert!(decode_frame(&frame(b"7")).is_none());
        assert!(decode_frame(&frame(b"[1]")).is_none());
        assert!(decode_frame(&frame(b"{broken")).is_none());
        assert!(
            decode_frame(b"\x02{").is_none(),
            "a short buffer is dropped"
        );
        assert!(decode_frame(&vec![b' '; MAX_FRAME + 1]).is_none());
    }

    #[tokio::test]
    async fn a_request_round_trips_with_json_fields() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        let serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_request: Some(Arc::new(move |message, _control| {
                    count.fetch_add(1, Ordering::AcqRel);
                    Box::pin(async move {
                        assert_eq!(message["type"], "pair");
                        assert_eq!(message["code"], "ABC123");
                        Ok(json!({ "token": "t" }))
                    })
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let mut payload = Map::new();
        payload.insert("code".to_owned(), json!("ABC123"));
        let reply = control
            .call("pair", payload, Duration::from_secs(5))
            .await
            .expect("pair answer");
        assert_eq!(reply, json!({ "token": "t" }));
        assert_eq!(seen.load(Ordering::Acquire), 1);
        control.close();
        drop(serve);
    }

    #[tokio::test]
    async fn a_failing_handler_answers_ok_false() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let _serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_request: Some(Arc::new(|_message, _control| {
                    Box::pin(async { Err("Invalid TV pairing code".to_owned()) })
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let error = control
            .call("pair", Map::new(), Duration::from_secs(5))
            .await
            .expect_err("refused");
        assert_eq!(error.message, "Invalid TV pairing code");
    }

    #[tokio::test]
    async fn a_peer_that_does_not_pair_refuses_the_open() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        // The node marks a connection ready once its protocols are attached; unknown opens are
        // then refused at once instead of held.
        receiver_mux.ready();
        let error = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect_err("no receiver half");
        assert_eq!(error.message, "This peer does not take casts");
    }

    #[tokio::test]
    async fn closing_the_channel_fails_a_pending_call() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let _serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                // Never answers: the call must fail when the channel closes.
                on_request: Some(Arc::new(|_message, _control| {
                    Box::pin(std::future::pending::<Result<Value, String>>())
                })),
                ..ControlHandlers::default()
            },
        );
        let control = open_control(
            &sender_mux,
            ControlHandlers::default(),
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let waiting = {
            let control = control.clone();
            tokio::spawn(async move {
                control
                    .call("cmd", Map::new(), Duration::from_secs(30))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        control.close();
        let error = waiting.await.expect("joined").expect_err("closed");
        assert_eq!(error.message, "Receiver connection closed");
    }

    #[tokio::test]
    async fn state_events_reach_the_sender() {
        let (sender_mux, receiver_mux) = pair_of_muxes();
        let serve = serve_control(
            &receiver_mux,
            ControlHandlers {
                on_open: Some(Arc::new(|control| {
                    control.push(json!({ "event": "state", "state": { "status": "playing" } }));
                })),
                ..ControlHandlers::default()
            },
        );
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let control = open_control(
            &sender_mux,
            ControlHandlers {
                on_event: Some(Arc::new(move |message| {
                    let _ = tx.send(message);
                })),
                ..ControlHandlers::default()
            },
            Duration::from_secs(5),
        )
        .await
        .expect("channel opens");
        let message = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("event arrives")
            .expect("channel open");
        assert_eq!(message["event"], "state");
        assert_eq!(message["state"]["status"], "playing");
        control.close();
        drop(serve);
    }
}
