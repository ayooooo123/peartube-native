//! `attachRpcChannel(conn, onRequest)`: request/response frames over one Protomux channel.
//!
//! The channel opens protocol [`PROTOCOL`] and carries a single message type (index 0) whose body
//! is the compact-encoded [`Frame`]. Requests wait for the matching reply op on the same id, with
//! a timeout; everything else goes to the connection's request handler.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::future::BoxFuture;
use protomux::{ChannelOptions, Event, Mux};

use super::dht::Connection;
use super::{Error, Frame, PROTOCOL, Result};

/// The single message type of the guard channel (`ch.addMessage(...)` index 0).
const MESSAGE_KIND: u64 = 0;

/// A request handler: the frame plus the channel it arrived on.
pub type Handler = Arc<dyn Fn(Frame, RpcSender) -> BoxFuture<'static, ()> + Send + Sync>;

struct Pending {
    reply_op: u64,
    tx: tokio::sync::oneshot::Sender<Vec<u8>>,
}

/// Sending half, cloneable and usable from any task.
#[derive(Clone)]
pub struct RpcSender {
    sender: protomux::ChannelSender,
    closed: Arc<AtomicBool>,
    conn: Arc<dyn Connection>,
}

impl RpcSender {
    /// `send(op, id, payload)`: one-way frame, no reply expected.
    pub fn send(&self, op: u64, id: u64, payload: &[u8]) -> bool {
        if self.closed.load(Ordering::Relaxed) || self.conn.is_closed() {
            return false;
        }
        self.sender
            .send_raw(MESSAGE_KIND, &Frame::new(op, id, payload.to_vec()).encode())
    }
}

pub struct Rpc {
    sender: RpcSender,
    pending: Mutex<HashMap<u64, Pending>>,
    next_id: AtomicU64,
    closed: Arc<AtomicBool>,
}

impl Rpc {
    /// `isClosed()`.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed) || self.sender.conn.is_closed()
    }

    pub fn sender(&self) -> RpcSender {
        self.sender.clone()
    }

    fn reject_all(&self) {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.clear();
    }

    /// `request(op, replyOp, payload, ms)`.
    pub async fn request(
        &self,
        op: u64,
        reply_op: u64,
        payload: &[u8],
        ms: u64,
    ) -> Result<Vec<u8>> {
        if self.is_closed() {
            return Err(Error::Closed("Control channel closed"));
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::oneshot::channel();
        {
            let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
            pending.insert(id, Pending { reply_op, tx });
        }
        if !self.sender.send(op, id, payload) {
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&id);
            return Err(Error::Closed("Control channel closed"));
        }
        match tokio::time::timeout(Duration::from_millis(ms), rx).await {
            Ok(Ok(payload)) => Ok(payload),
            Ok(Err(_)) => Err(Error::Closed("Guard channel closed")),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&id);
                Err(Error::Timeout(op))
            }
        }
    }

    fn resolve(&self, frame: &Frame) -> bool {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = pending.get(&frame.id) else {
            return false;
        };
        if entry.reply_op != frame.op {
            return false;
        }
        let entry = pending.remove(&frame.id).expect("checked above");
        let _ = entry.tx.send(frame.payload.clone());
        true
    }
}

/// Builds the channel on `mux` and starts its reader.
pub fn attach_rpc_channel(
    mux: &Mux,
    conn: Arc<dyn Connection>,
    on_request: Option<Handler>,
) -> Result<Arc<Rpc>> {
    // Pair before opening so an eager peer frame arriving mid-handshake never hits an unpaired
    // Protomux slot; a late remote open is answered by the channel already open here.
    let mut pairs = mux.pair(PROTOCOL, None);
    let watched = mux.clone();
    tokio::spawn(async move {
        while let Some(request) = pairs.recv().await {
            drop(request);
            if watched.is_destroyed() {
                break;
            }
        }
    });
    let mut channel = mux
        .open_channel(ChannelOptions::new(PROTOCOL))
        .ok_or(Error::Closed("Control channel closed"))?;
    let closed = Arc::new(AtomicBool::new(false));
    let rpc = Arc::new(Rpc {
        sender: RpcSender {
            sender: channel.sender(),
            closed: Arc::clone(&closed),
            conn: Arc::clone(&conn),
        },
        pending: Mutex::new(HashMap::new()),
        next_id: AtomicU64::new(1),
        closed: Arc::clone(&closed),
    });
    let reader = Arc::clone(&rpc);
    tokio::spawn(async move {
        while let Some(event) = channel.recv().await {
            match event {
                Event::Message(message) if message.kind == MESSAGE_KIND => {
                    let Some(frame) = Frame::decode(&message.data) else {
                        continue;
                    };
                    if reader.resolve(&frame) {
                        continue;
                    }
                    if let Some(handler) = &on_request {
                        let sender = reader.sender.clone();
                        let future = handler(frame, sender);
                        tokio::spawn(future);
                    }
                }
                Event::Closed { .. } => break,
                _ => {}
            }
        }
        closed.store(true, Ordering::Relaxed);
        reader.reject_all();
    });
    Ok(rpc)
}
