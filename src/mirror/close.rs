//! Close the host before the relay closes the node. A failed stop must not leave another
//! plugin running on a closed node or leave the traffic gate attached to its connections.

use std::any::Any;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;

use futures_util::FutureExt;
use futures_util::future::{BoxFuture, Shared, join_all};

use super::{MirrorError, MirrorHost, Node};

pub type Stop<'a> = BoxFuture<'a, Result<(), MirrorError>>;

/// The one close of a host. Every caller holds it and gets its result.
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

impl<N: Node + 'static> MirrorHost<N> {
    /// Stop every plugin, release the traffic gate, then report what failed. The first call
    /// starts every stop before it returns. Every later call, a plugin's own included, gets the
    /// same close.
    pub fn close(&self) -> Closing {
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
            async move {
                // Call `stop` inside the unwind boundary: it can panic before it returns its future.
                let stopped = join_all(
                    plugins
                        .iter()
                        .map(|p| AssertUnwindSafe(async { p.stop().await }).catch_unwind()),
                )
                .await;
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
