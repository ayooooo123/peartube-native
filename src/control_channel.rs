//! `src/control-channel.js`: a request/reply channel between two peers, one Protomux channel on
//! the connection the two nodes already share, beside Hypercore replication. Cast control
//! (`cast::control`, `peartube-cast/v1`) and global search (`search`, `peartube/search/v1`) each
//! run one under their own [`Spec`].
//!
//! ```text
//! asking side    -> answering side   {id, type, ...}
//! answering side -> asking side      {id, ok, result | error}, and {event, ...} pushed at any time
//! ```
//!
//! Any peer on the connection can open the channel, so the answering side decides per request
//! what it allows.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use protomux::{ChannelOptions, ChannelSender, Event, Mux};
use serde_json::{Map, Value, json};
use tokio::sync::{Notify, oneshot};

/// Largest frame the channel accepts; anything longer is dropped.
pub const MAX_FRAME: usize = 64 * 1024;
/// The app's default for every request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// The channel's only message type (the app's single `addMessage`).
const MESSAGE_KIND: u64 = 0;

/// One protocol on the shared channel code: its Protomux name, the word that names the other
/// side in errors ("Receiver connection closed"), and the asking side's open errors.
#[derive(Debug)]
pub struct Spec {
    pub protocol: &'static str,
    pub label: &'static str,
    pub refused: &'static str,
    pub duplicate: &'static str,
    pub timed_out: &'static str,
}

/// A failed request or open, with the app's message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlError {
    pub message: String,
}

impl ControlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ControlError {}

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

/// The answer a request handler gives, or the error text it refuses with.
pub type RequestFuture = BoxFuture<'static, Result<Value, String>>;
pub type RequestHandler = Arc<dyn Fn(Value, Control) -> RequestFuture + Send + Sync>;
pub type EventHandler = Arc<dyn Fn(Value) + Send + Sync>;
pub type OpenHandler = Arc<dyn Fn(Control) + Send + Sync>;

/// What a channel does with what it receives (`onRequest`, `onEvent`, `onOpen`, `onClose`).
#[derive(Clone, Default)]
pub struct ControlHandlers {
    pub on_request: Option<RequestHandler>,
    pub on_event: Option<EventHandler>,
    pub on_open: Option<OpenHandler>,
    pub on_close: Option<OpenHandler>,
}

struct Inner {
    spec: &'static Spec,
    mux: Mux,
    sender: Mutex<Option<ChannelSender>>,
    pending: Mutex<HashMap<u64, oneshot::Sender<Result<Value, ControlError>>>>,
    seq: AtomicU64,
    opened: AtomicBool,
    closed: AtomicBool,
    changed: Notify,
    handlers: ControlHandlers,
}

/// One half of the channel on a connection, as `createChannel` returns it.
#[derive(Clone)]
pub struct Control {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Control {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Control")
            .field("protocol", &self.inner.spec.protocol)
            .field("opened", &self.is_opened())
            .field("closed", &self.is_closed())
            .field("mux_drained", &self.inner.mux.is_drained())
            .finish()
    }
}

impl Control {
    fn new(
        spec: &'static Spec,
        mux: Mux,
        sender: ChannelSender,
        handlers: ControlHandlers,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                spec,
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

    fn closed_error(&self) -> ControlError {
        ControlError::new(format!("{} connection closed", self.inner.spec.label))
    }

    pub fn mux(&self) -> &Mux {
        &self.inner.mux
    }

    /// Unique per channel; callers key per-channel state on it.
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
    ) -> Result<Value, ControlError> {
        if self.is_closed() {
            return Err(self.closed_error());
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
                return Err(ControlError::new("The request is not JSON"));
            }
        };
        if !self.write(bytes, timeout).await {
            lock(&self.inner.pending).remove(&id);
            return Err(self.closed_error());
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(self.closed_error()),
            Err(_) => {
                lock(&self.inner.pending).remove(&id);
                Err(ControlError::new(format!(
                    "{} {kind} timed out",
                    self.inner.spec.label
                )))
            }
        }
    }

    /// Waits until the connection is drained (`conn.once('drain')`). Returns at once when it
    /// already is, or when the channel closed.
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
            let _ = waiter.send(Err(self.closed_error()));
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
                        _ => format!("{} error", self.inner.spec.label),
                    })
                };
                let _ = waiter.send(result.map_err(ControlError::new));
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

/// Builds one half of the channel on `mux`, not yet opened; `None` when this connection already
/// has one, or the mux is gone.
fn create_channel(mux: &Mux, spec: &'static Spec, handlers: ControlHandlers) -> Option<Control> {
    if mux.is_opened(spec.protocol, None) {
        return None;
    }
    let channel = mux.open_channel(ChannelOptions::new(spec.protocol))?;
    let sender = channel.sender();
    let control = Control::new(spec, mux.clone(), sender, handlers);
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

/// `serveChannel(conn, { protocol, onRequest, onOpen, onClose })`: the answering half, answering
/// every peer that opens the protocol on this connection. Returns a handle that stops answering.
pub fn serve_channel(mux: &Mux, spec: &'static Spec, handlers: ControlHandlers) -> ServeHandle {
    let mut pairs = mux.pair(spec.protocol, None);
    let open_on = mux.clone();
    let task = tokio::spawn(async move {
        while let Some(request) = pairs.recv().await {
            let _ = create_channel(&open_on, spec, handlers.clone());
            drop(request);
        }
    });
    ServeHandle {
        mux: mux.clone(),
        protocol: spec.protocol,
        task: Some(task),
    }
}

/// Stops one connection's answering half (`mux.unpair({ protocol })`).
pub struct ServeHandle {
    mux: Mux,
    protocol: &'static str,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl ServeHandle {
    pub fn stop(&mut self) {
        self.mux.unpair(self.protocol, None);
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

/// `openChannel(conn, { protocol, onRequest, onEvent, onClose }, timeoutMs)`: the asking half.
/// Resolves once the other side answers the open; a peer that does not serve the protocol refuses
/// the channel, which is the spec's `refused` error. With `on_request`, this side also answers the
/// other side's requests on the same channel.
pub async fn open_channel(
    mux: &Mux,
    spec: &'static Spec,
    handlers: ControlHandlers,
    timeout: Duration,
) -> Result<Control, ControlError> {
    let control =
        create_channel(mux, spec, handlers).ok_or_else(|| ControlError::new(spec.duplicate))?;
    match tokio::time::timeout(timeout, control.opened()).await {
        Ok(()) if control.is_opened() => Ok(control),
        Ok(()) => Err(ControlError::new(spec.refused)),
        Err(_) => {
            control.close();
            Err(ControlError::new(spec.timed_out))
        }
    }
}
