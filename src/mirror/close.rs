//! Close the host before the relay closes the node. A failed stop must not leave another
//! plugin running on a closed node or leave the traffic gate attached to its connections.

use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::future::poll_fn;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared, join_all};
use tokio::sync::watch;

use super::{MirrorError, MirrorHost, Node, Plugin};

pub type Stop<'a> = BoxFuture<'a, Result<(), MirrorError>>;

/// A close handle, with its result fixed when `close` returns it. The shared close reports the
/// stops that failed; the handle a stop gets during its first poll only succeeds.
pub type Closing = Shared<BoxFuture<'static, Result<(), CloseError>>>;

/// One failed stop is reported as it is, with its status. Several are listed in registration
/// order, not in completion order.
#[derive(Debug, Clone, PartialEq)]
pub enum CloseError {
    Plugin(MirrorError),
    Several(Vec<MirrorError>),
}

impl fmt::Display for CloseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plugin(error) => fmt::Display::fmt(error, f),
            Self::Several(errors) => write!(f, "{} plugins failed to stop", errors.len()),
        }
    }
}

impl std::error::Error for CloseError {}

/// A stop that panicked has failed. The host still waits for the other plugins.
fn panicked(reason: Box<dyn Any + Send>) -> MirrorError {
    MirrorError::plain(match reason.downcast::<String>() {
        Ok(message) => *message,
        Err(reason) => reason
            .downcast_ref::<&str>()
            .copied()
            .unwrap_or("plugin panicked while stopping")
            .to_owned(),
    })
}

/// A stop whose first poll runs on this thread. `inner` is what a `close` call in that poll gets:
/// it succeeds once every stop that made no such call has settled.
struct StopCaller {
    host: Arc<AtomicBool>,
    inner: Closing,
    reentered: bool,
}

// The stops whose first poll runs on this thread, innermost last. A record lives for that one poll
// only, so the cell is never borrowed while plugin code runs.
thread_local! {
    static STOP_CALLERS: RefCell<Vec<StopCaller>> = const { RefCell::new(Vec::new()) };
}

/// The inner handle when a stop of `host` calls `close` during its first poll. The call marks the
/// record, so that stop's receiver drops when the poll returns and the stop cannot wait for itself.
fn stop_close(host: &Arc<AtomicBool>) -> Option<Closing> {
    STOP_CALLERS.with_borrow_mut(|stack| {
        let caller = stack
            .iter_mut()
            .rev()
            .find(|caller| Arc::ptr_eq(&caller.host, host))?;
        caller.reentered = true;
        Some(caller.inner.clone())
    })
}

/// Runs one plugin's stop. Its receiver clone lives until the stop really completes, so the inner
/// close waits for it. A stop that calls `close` during its first poll gets that inner close and
/// so must not hold its receiver while it waits: the receiver drops when that poll returns.
async fn stop_one<N: Node + 'static>(
    plugin: &Plugin<N>,
    receiver: watch::Receiver<()>,
    record: StopCaller,
) -> Result<Result<(), MirrorError>, Box<dyn Any + Send>> {
    let mut receiver = Some(receiver);
    let mut record = Some(record);
    // `stop` runs inside the unwind boundary: it can panic before it returns its future.
    let mut body = pin!(AssertUnwindSafe(async { plugin.stop().await }).catch_unwind());
    let outcome = poll_fn(|cx| {
        let Some(caller) = record.take() else {
            return body.as_mut().poll(cx);
        };
        STOP_CALLERS.with_borrow_mut(|stack| stack.push(caller));
        let polled = body.as_mut().poll(cx);
        if STOP_CALLERS
            .with_borrow_mut(|stack| stack.pop().expect("first poll of a stop").reentered)
        {
            drop(receiver.take());
        }
        polled
    })
    .await;
    drop(receiver);
    outcome
}

impl<N: Node + 'static> MirrorHost<N> {
    /// Stop every plugin, release the traffic gate, then report what failed. The first call starts
    /// every stop before it returns. A close that a stop makes during its first poll, its own
    /// included, gets a handle that succeeds once every stop that made no such call has settled;
    /// it never reports an error. A later call from that stop gets this shared close, so a stop
    /// that calls after a real yield can still wait for itself.
    pub fn close(&self) -> Closing {
        if let Some(inner) = stop_close(&self.closed) {
            return inner;
        }
        let mut first = false;
        let closing = self.closing.get_or_init(|| {
            first = true;
            self.closed.store(true, Ordering::SeqCst);
            if let Some(handle) = self.poll_task.lock().expect("poll task").take() {
                handle.abort();
            }
            let plugins: Vec<_> = self
                .plugins
                .lock()
                .expect("mirror plugins")
                .iter()
                .map(|(_, p)| p.clone())
                .collect();
            let tasks = self.tasks.clone();
            let traffic = self.stop_traffic.lock().expect("traffic gate").take();
            let host = self.closed.clone();
            let (sender, receiver) = watch::channel(());
            let inner: Closing = async move {
                // Only a close signal: no value is ever sent. The inner close is done when its
                // last receiver drops.
                sender.closed().await;
                Ok(())
            }
            .boxed()
            .shared();
            async move {
                // join_all consumes its argument now, so every stop gets its receiver here; then
                // the receiver this block holds drops, and the inner close waits for the stops.
                let stopped = join_all(plugins.iter().map(|plugin| {
                    stop_one(
                        plugin,
                        receiver.clone(),
                        StopCaller {
                            host: host.clone(),
                            inner: inner.clone(),
                            reentered: false,
                        },
                    )
                }));
                drop(receiver);
                let stopped = stopped.await;
                if let Some(stop) = traffic {
                    stop();
                }
                for handle in tasks.lock().expect("mirror tasks").drain(..) {
                    handle.abort();
                }
                let mut failed: Vec<MirrorError> = stopped
                    .into_iter()
                    .filter_map(|outcome| match outcome {
                        Ok(result) => result.err(),
                        Err(reason) => Some(panicked(reason)),
                    })
                    .collect();
                if failed.len() > 1 {
                    return Err(CloseError::Several(failed));
                }
                failed
                    .pop()
                    .map_or(Ok(()), |one| Err(CloseError::Plugin(one)))
            }
            .boxed()
            .shared()
        });
        if first {
            // The shared close is now visible even to a plugin that calls back into this host.
            // Start every stop in this call, then let the runtime finish the close: it does not
            // wait for a caller to poll it, and a caller that drops its handle does not cancel it.
            let mut driver = closing.clone();
            if (&mut driver).now_or_never().is_none() {
                tokio::spawn(driver);
            }
        }
        closing.clone()
    }
}
