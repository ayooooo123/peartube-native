//! Symmetric request/reply and events on one Protomux message slot. Only this channel is
//! closed on a decode-budget violation; other protocols on the same mux remain usable.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::control_protocol::{Body, MAX_UINT, Protocol};
use crate::wire::peartube::{Frame, RemoteError};
use bytes::Bytes;
use compact_encoding::Codec;
use futures_util::{FutureExt, future::BoxFuture};
use protomux::{ChannelOptions, ChannelSender, Event, Mux};
use tokio::sync::{Notify, oneshot};

/// Largest frame the channel accepts; anything longer is dropped.
pub const MAX_FRAME: usize = 64 * 1024;
/// The app's default for every request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
/// The channel's only message type (the app's single `addMessage`).
const MESSAGE_KIND: u64 = 0;
/// The envelope's error codes; any other received code is delivered as `HANDLER_ERROR`.
const REMOTE_CODES: [&str; 4] = [
    "UNKNOWN_OP",
    "DECODE_ERROR",
    "ENCODE_ERROR",
    "HANDLER_ERROR",
];

/// One protocol on the shared channel code: its Protomux name, the word that names the other
/// side in errors ("Receiver connection closed"), and the asking side's open errors.
#[derive(Debug)]
pub struct Spec {
    pub protocol: &'static Protocol,
    pub label: &'static str,
    pub refused: &'static str,
    pub duplicate: &'static str,
    pub timed_out: &'static str,
}

/// A failed request or open, with the app's message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlError {
    pub code: Option<String>,
    pub message: String,
}

impl ControlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            code: None,
            message: message.into(),
        }
    }

    /// A failure raised on this side with the app's code: `ENCODE_ERROR`, `DECODE_ERROR`,
    /// `ETIMEDOUT`.
    fn coded(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: Some(code.into()),
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

pub fn decode_frame(data: Bytes) -> Option<Frame> {
    if data.len() > MAX_FRAME {
        return None;
    }
    let f: Frame = compact_encoding::decode(data).ok()?;
    (f.kind <= MAX_UINT && f.op <= MAX_UINT && f.id <= MAX_UINT).then_some(f)
}

fn encode_frame(kind: u64, op: u64, id: u64, value: Bytes) -> Option<Frame> {
    let frame = Frame {
        kind,
        op,
        id,
        value,
    };
    (frame.encoded_len() <= MAX_FRAME).then_some(frame)
}

/// The answer a request handler gives, or the error text it refuses with.
pub type RequestFuture = BoxFuture<'static, Result<Body, String>>;
pub type RequestHandler = Arc<dyn Fn(u64, Body, Control) -> RequestFuture + Send + Sync>;
pub type EventHandler = Arc<dyn Fn(u64, Body, Control) + Send + Sync>;
pub type OpenHandler = Arc<dyn Fn(Control) + Send + Sync>;

/// What a channel does with what it receives (`onRequest`, `onEvent`, `onOpen`, `onClose`).
#[derive(Clone, Default)]
pub struct ControlHandlers {
    /// Subset of the protocol's ops actually implemented on this half. Check before decoding.
    pub handled: u64,
    pub on_request: Option<RequestHandler>,
    pub on_event: Option<EventHandler>,
    pub on_open: Option<OpenHandler>,
    pub on_close: Option<OpenHandler>,
}

type PendingCalls = HashMap<(u64, u64), oneshot::Sender<Result<Body, ControlError>>>;

struct Inner {
    spec: &'static Spec,
    mux: Mux,
    sender: ChannelSender,
    pending: Mutex<PendingCalls>,
    bad: Mutex<VecDeque<Instant>>,
    seq: AtomicU64,
    opened: AtomicBool,
    closed: AtomicBool,
    changed: Notify,
    handlers: ControlHandlers,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PushResult {
    NotSent,
    Sent,
    WaitForDrain,
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
                sender,
                bad: Mutex::new(VecDeque::new()),
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

    /// A queued frame must never be retried. Only `WaitForDrain` needs a drain. "Not sent" is
    /// decided before the send; a frame the mux took stays sent whatever happens next.
    pub fn push(&self, op: u64, value: Body) -> PushResult {
        let Some(desc) = self.inner.spec.protocol.ops.get(op as usize) else {
            return PushResult::NotSent;
        };
        if desc.response.is_some() || value.schema() != desc.value {
            return PushResult::NotSent;
        }
        let Some(frame) = value.encode().and_then(|v| encode_frame(3, op, 0, v)) else {
            return PushResult::NotSent;
        };
        if self.is_closed() || self.inner.sender.is_closed() {
            PushResult::NotSent
        } else if self.inner.sender.send(MESSAGE_KIND, &frame) {
            PushResult::Sent
        } else {
            PushResult::WaitForDrain
        }
    }

    /// One send per frame, as it comes: a full queue only delays the frame (it is queued, never
    /// resent). Large or repeated producers wait for drain themselves (see [`Control::push`]).
    fn write(&self, frame: &Frame) -> bool {
        if self.is_closed() || self.inner.sender.is_closed() {
            return false;
        }
        self.inner.sender.send(MESSAGE_KIND, frame);
        true
    }

    pub async fn call(
        &self,
        op: u64,
        value: Body,
        timeout: Duration,
    ) -> Result<Body, ControlError> {
        // As in the app: the close first, then the value, and only then an id, so a request
        // that fails here uses none.
        if self.is_closed() {
            return Err(self.closed_error());
        }
        let label = self.inner.spec.label;
        let desc = self.inner.spec.protocol.ops.get(op as usize);
        let name = desc.map_or("request", |d| d.name);
        let value = desc
            .filter(|d| d.response.is_some() && d.value == value.schema())
            .and_then(|_| value.encode())
            .ok_or_else(|| {
                ControlError::coded(
                    "ENCODE_ERROR",
                    format!("{label} {name} could not be encoded"),
                )
            })?;
        let mut frame = None;
        let taken = self
            .inner
            .seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                frame = (n <= MAX_UINT)
                    .then(|| encode_frame(0, op, n, value.clone()))
                    .flatten();
                frame.as_ref().map(|_| n + 1)
            });
        let (id, frame) = match (taken, frame) {
            (Ok(id), Some(frame)) => (id, frame),
            (Err(next), _) if next > MAX_UINT => {
                return Err(ControlError::coded(
                    "ENCODE_ERROR",
                    format!("{label} channel ran out of request ids"),
                ));
            }
            _ => {
                return Err(ControlError::coded(
                    "ENCODE_ERROR",
                    format!("{label} {name} is too large"),
                ));
            }
        };
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = lock(&self.inner.pending);
            if self.is_closed() {
                return Err(self.closed_error());
            }
            pending.insert((op, id), tx);
        }
        // Removing on Drop also covers callers cancelling the future.
        struct Waiting<'a>(&'a Inner, (u64, u64));
        impl Drop for Waiting<'_> {
            fn drop(&mut self) {
                lock(&self.0.pending).remove(&self.1);
            }
        }
        let _wait = Waiting(&self.inner, (op, id));
        tokio::time::timeout(timeout, async {
            if !self.write(&frame) {
                return Err(self.closed_error());
            }
            rx.await.unwrap_or_else(|_| Err(self.closed_error()))
        })
        .await
        .unwrap_or_else(|_| {
            Err(ControlError::coded(
                "ETIMEDOUT",
                format!("{label} {name} timed out"),
            ))
        })
    }

    /// Waits until the connection is drained (`conn.once('drain')`), or the channel closed;
    /// returns at once when either already happened. Elapsed time is not a drain.
    pub async fn drained(&self) {
        tokio::select! {
            _ = self.inner.mux.drained() => {}
            _ = self.closed() => {}
        }
    }

    pub fn close(&self) {
        self.inner.sender.close();
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

    fn error(&self, frame: &Frame, code: &str, message: &str) -> Frame {
        Frame {
            kind: 2,
            op: frame.op,
            id: frame.id,
            value: compact_encoding::encode(&RemoteError {
                code: code.into(),
                message: message.chars().take(256).collect(),
            }),
        }
    }

    fn bad_value(&self) {
        let now = Instant::now();
        let close = {
            let mut bad = lock(&self.inner.bad);
            while bad
                .front()
                .is_some_and(|t| strike_expired(now.duration_since(*t)))
            {
                bad.pop_front();
            }
            bad.push_back(now);
            bad.len() >= 8
        };
        if close {
            self.close();
        }
    }

    async fn dispatch(&self, data: Bytes) {
        let Some(frame) = decode_frame(data) else {
            self.bad_value();
            return;
        };
        let desc = usize::try_from(frame.op)
            .ok()
            .and_then(|i| self.inner.spec.protocol.ops.get(i));
        let handled = frame.op < 64 && self.inner.handlers.handled & (1 << frame.op) != 0;
        match frame.kind {
            0 => {
                if frame.id == 0 {
                    self.bad_value();
                    return;
                }
                let handler = self.inner.handlers.on_request.as_ref().filter(|_| handled);
                let Some((schema, handler)) = desc.filter(|d| d.response.is_some()).zip(handler)
                else {
                    // The envelope's own errors carry the app's empty message.
                    self.write(&self.error(&frame, "UNKNOWN_OP", ""));
                    return;
                };
                let Some(value) = schema.value.decode(frame.value.clone()) else {
                    // The reply goes out before the strike, so the eighth is answered too.
                    self.write(&self.error(&frame, "DECODE_ERROR", ""));
                    self.bad_value();
                    return;
                };
                let handler = handler.clone();
                let control = self.clone();
                // Frames are handled in wire order: the handler starts here and runs to its
                // first suspension before the next frame is read; only the rest is a task.
                let mut answer = Box::pin(async move {
                    let run = async { handler(frame.op, value, control.clone()).await };
                    let answer = match std::panic::AssertUnwindSafe(run).catch_unwind().await {
                        Ok(Ok(body)) => (Some(body.schema()) == schema.response)
                            .then(|| body.encode())
                            .flatten()
                            .and_then(|v| encode_frame(1, frame.op, frame.id, v))
                            .unwrap_or_else(|| control.error(&frame, "ENCODE_ERROR", "")),
                        Ok(Err(msg)) => control.error(&frame, "HANDLER_ERROR", &msg),
                        Err(panic) => {
                            let msg = panic
                                .downcast_ref::<String>()
                                .map(String::as_str)
                                .or_else(|| panic.downcast_ref::<&str>().copied())
                                .unwrap_or("Handler panicked");
                            control.error(&frame, "HANDLER_ERROR", msg)
                        }
                    };
                    control.write(&answer);
                });
                if futures_util::poll!(answer.as_mut()).is_pending() {
                    tokio::spawn(answer);
                }
            }
            1 | 2 => {
                let waiter = lock(&self.inner.pending).remove(&(frame.op, frame.id));
                let Some(waiter) = waiter else { return };
                let answer = if frame.kind == 1 {
                    desc.and_then(|d| d.response)
                        .and_then(|s| s.decode(frame.value))
                        .map(Ok)
                } else {
                    compact_encoding::decode::<RemoteError>(frame.value)
                        .ok()
                        .map(|err| {
                            // A peer cannot make a call fail with a local-looking code.
                            let code = if REMOTE_CODES.contains(&err.code.as_str()) {
                                err.code
                            } else {
                                "HANDLER_ERROR".into()
                            };
                            Err(ControlError {
                                code: Some(code),
                                message: err.message,
                            })
                        })
                };
                if let Some(answer) = answer {
                    let _ = waiter.send(answer);
                } else {
                    let _ = waiter.send(Err(ControlError::coded(
                        "DECODE_ERROR",
                        format!(
                            "{} sent an answer that does not decode",
                            self.inner.spec.label
                        ),
                    )));
                    self.bad_value();
                }
            }
            3 => {
                if frame.id != 0 || !handled {
                    return;
                }
                let Some((desc, handler)) = desc
                    .filter(|d| d.response.is_none())
                    .zip(self.inner.handlers.on_event.as_ref())
                else {
                    return;
                };
                let Some(body) = desc.value.decode(frame.value) else {
                    self.bad_value();
                    return;
                };
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handler(frame.op, body, self.clone())
                }));
            }
            _ => self.bad_value(),
        }
    }
}

/// A bad frame stops counting once it is older than 10,000 ms, in whole milliseconds as the
/// app's `now - t > 10_000` on `Date.now()`.
fn strike_expired(age: Duration) -> bool {
    age.as_millis() > 10_000
}

/// Builds one half of the channel on `mux`, not yet opened; `None` when this connection already
/// has one, or the mux is gone.
fn create_channel(mux: &Mux, spec: &'static Spec, handlers: ControlHandlers) -> Option<Control> {
    if mux.is_opened(spec.protocol.name, None) {
        return None;
    }
    let channel = mux.open_channel(ChannelOptions::new(spec.protocol.name))?;
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
                Event::Message(message) if message.kind == MESSAGE_KIND && !reading.is_closed() => {
                    reading.dispatch(message.data).await
                }
                Event::Message(_) => (),
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
    let mut pairs = mux.pair(spec.protocol.name, None);
    let open_on = mux.clone();
    let task = tokio::spawn(async move {
        while let Some(request) = pairs.recv().await {
            let _ = create_channel(&open_on, spec, handlers.clone());
            drop(request);
        }
    });
    ServeHandle {
        mux: mux.clone(),
        protocol: spec.protocol.name,
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
        // The channel closed before it opened. On a live connection the other side refused
        // the protocol (another version); a connection that went is only a closed connection.
        Ok(()) if mux.is_destroyed() => Err(control.closed_error()),
        Ok(()) => Err(ControlError::new(spec.refused)),
        Err(_) => {
            control.close();
            Err(ControlError::new(spec.timed_out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The app's window: `now - t > 10_000` in whole milliseconds. A strike exactly 10,000 ms old
    // still counts.
    #[test]
    fn a_strike_expires_only_when_older_than_ten_seconds() {
        assert!(!strike_expired(Duration::from_millis(9_999)));
        assert!(!strike_expired(Duration::from_millis(10_000)));
        assert!(strike_expired(Duration::from_millis(10_001)));
    }
}
