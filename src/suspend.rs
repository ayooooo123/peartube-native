//! The host's `suspend`/`resume`, ordered on a thread of their own.
//!
//! `worker.js` suspends the node when the linger timer fires (`node.suspend()`: the swarm, then
//! `store.suspend()`) and resumes it on Bare's `resume` (`store.resume()`, then the swarm). A
//! suspended store blocks every access until it resumes, and the app loop awaits node work
//! inline, so the loop and every runtime worker may be blocked when the host comes back. The
//! store therefore resumes here, on a thread that never accesses the store otherwise, before the
//! loop hears `resume`; the loop then resumes the swarm. The host's calls only queue a request,
//! so a UI thread never waits on disk.
//!
//! Parking runs on the same thread, so a park and a resume keep the order the host asked for.
//! Each host suspend gets a generation, and a park names the suspend it belongs to: a park whose
//! suspend was followed by a resume is dropped, even when a newer suspend came after that
//! resume, and a resume never wakes a store that was not parked.

use std::io;
use std::sync::mpsc::{self, Sender, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use tokio::sync::oneshot;

use crate::node::api::StoreSuspension;

/// What the thread hands the app loop, in the host's order.
pub(crate) enum Lifecycle {
    /// The linger and the generation the loop passes back to [`Parker::park`].
    Suspend(u64, u64),
    /// The host is back; the error is the store's, if it could not reopen.
    Resume(Option<String>),
}

enum Request {
    Suspend(u64),
    Resume,
    Park(
        u64,
        Arc<dyn StoreSuspension>,
        oneshot::Sender<io::Result<()>>,
    ),
    Release(SyncSender<()>),
}

/// The host side: queues `suspend`/`resume` and releases the store before a shutdown.
pub(crate) struct Suspension {
    requests: Sender<Request>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

/// The app loop side: parks a node's store.
#[derive(Clone)]
pub(crate) struct Parker(Sender<Request>);

impl Suspension {
    /// Starts the thread; `forward` hands each lifecycle step to the app loop.
    pub(crate) fn spawn(forward: impl Fn(Lifecycle) + Send + 'static) -> io::Result<Self> {
        let (requests, incoming) = mpsc::channel();
        let thread = thread::Builder::new()
            .name("peartube-suspend".into())
            .spawn(move || run(&incoming, &forward))?;
        Ok(Self {
            requests,
            thread: Mutex::new(Some(thread)),
        })
    }

    pub(crate) fn parker(&self) -> Parker {
        Parker(self.requests.clone())
    }

    pub(crate) fn suspend(&self, linger_ms: u64) {
        let _ = self.requests.send(Request::Suspend(linger_ms));
    }

    pub(crate) fn resume(&self) {
        let _ = self.requests.send(Request::Resume);
    }

    /// `close()` resumes a suspended store first: wakes every access that waits on a parked
    /// store, refuses later parks and stops the thread. Blocks until done.
    pub(crate) fn release(&self) {
        let (done, released) = mpsc::sync_channel(1);
        if self.requests.send(Request::Release(done)).is_ok() {
            let _ = released.recv();
        }
        let thread = self
            .thread
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
    }
}

impl Parker {
    /// `store.suspend()` for the host suspend `generation`, unless the host came back (or shut
    /// down) since.
    pub(crate) async fn park(
        &self,
        generation: u64,
        store: Arc<dyn StoreSuspension>,
    ) -> io::Result<()> {
        let (reply, parked) = oneshot::channel();
        if self
            .0
            .send(Request::Park(generation, store, reply))
            .is_err()
        {
            return Ok(());
        }
        parked.await.unwrap_or(Ok(()))
    }
}

fn run(incoming: &mpsc::Receiver<Request>, forward: &dyn Fn(Lifecycle)) {
    let mut generation = 0;
    // The generation of the suspend the host is in, if it has not come back since.
    let mut background = None;
    let mut parked: Vec<Arc<dyn StoreSuspension>> = Vec::new();
    while let Ok(request) = incoming.recv() {
        match request {
            Request::Suspend(linger_ms) => {
                generation += 1;
                background = Some(generation);
                forward(Lifecycle::Suspend(linger_ms, generation));
            }
            Request::Resume => {
                background = None;
                let error = resume(&mut parked).err().map(|error| error.to_string());
                forward(Lifecycle::Resume(error));
            }
            Request::Park(suspend, store, reply) => {
                let result = if background == Some(suspend) {
                    store.suspend().map(|()| parked.push(store))
                } else {
                    Ok(())
                };
                let _ = reply.send(result);
            }
            Request::Release(done) => {
                // `close()` swallows a failed `store.resume()` and closes the store; closing
                // wakes its waiters with `Closed`, so no thread stays blocked on it.
                let _ = resume(&mut parked);
                for store in parked.drain(..) {
                    store.close();
                }
                let _ = done.send(());
                return;
            }
        }
    }
}

/// Resumes every parked store, keeping the first error. A store that could not reopen stays
/// parked, so the next resume tries it again.
fn resume(parked: &mut Vec<Arc<dyn StoreSuspension>>) -> io::Result<()> {
    let mut result = Ok(());
    parked.retain(|store| match store.resume() {
        Ok(()) => false,
        Err(error) => {
            if result.is_ok() {
                result = Err(error);
            }
            true
        }
    });
    result
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc::Receiver;
    use std::time::Duration;

    use super::*;

    /// Counts the store calls; `failures` resumes fail before one succeeds.
    #[derive(Default)]
    struct Counted {
        suspends: AtomicUsize,
        resumes: AtomicUsize,
        failures: AtomicUsize,
        closes: AtomicUsize,
    }

    impl StoreSuspension for Counted {
        fn suspend(&self) -> io::Result<()> {
            self.suspends.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn resume(&self) -> io::Result<()> {
            self.resumes.fetch_add(1, Ordering::SeqCst);
            let failing = self
                .failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    left.checked_sub(1)
                })
                .is_ok();
            if failing {
                return Err(io::Error::other("cannot reopen"));
            }
            Ok(())
        }
        fn close(&self) {
            self.closes.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn spawn() -> (Suspension, Receiver<String>) {
        let (steps, forwarded) = mpsc::channel();
        let suspension = Suspension::spawn(move |step| {
            let _ = steps.send(match step {
                Lifecycle::Suspend(linger, generation) => format!("suspend {linger} #{generation}"),
                Lifecycle::Resume(error) => format!("resume {error:?}"),
            });
        })
        .unwrap();
        (suspension, forwarded)
    }

    fn park(parker: &Parker, generation: u64, store: &Arc<Counted>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime
            .block_on(parker.park(generation, store.clone()))
            .unwrap();
    }

    fn next<T>(received: &Receiver<T>) -> T {
        received.recv_timeout(Duration::from_secs(10)).unwrap()
    }

    /// A park that reaches the thread after the host came back is dropped, so the resume
    /// never woke a store that was not parked and the store stays open; a later suspend parks
    /// again and the next resume wakes it.
    #[test]
    fn a_resume_before_the_park_keeps_the_store_open() {
        let (suspension, forwarded) = spawn();
        let store = Arc::new(Counted::default());
        suspension.suspend(5);
        suspension.resume();
        park(&suspension.parker(), 1, &store);
        assert_eq!(next(&forwarded), "suspend 5 #1");
        assert_eq!(next(&forwarded), "resume None");
        assert_eq!(store.suspends.load(Ordering::SeqCst), 0);
        assert_eq!(store.resumes.load(Ordering::SeqCst), 0);

        suspension.suspend(0);
        assert_eq!(next(&forwarded), "suspend 0 #2");
        park(&suspension.parker(), 2, &store);
        assert_eq!(store.suspends.load(Ordering::SeqCst), 1);
        suspension.resume();
        assert_eq!(next(&forwarded), "resume None");
        assert_eq!(store.resumes.load(Ordering::SeqCst), 1);
        suspension.release();
    }

    /// The park of a suspend the host already came back from is dropped, even when a newer
    /// suspend arrived after that resume; the newer suspend's own park still parks.
    #[test]
    fn a_park_from_before_a_resume_is_dropped_after_a_new_suspend() {
        let (suspension, forwarded) = spawn();
        let store = Arc::new(Counted::default());
        suspension.suspend(0);
        suspension.resume();
        suspension.suspend(5000);
        park(&suspension.parker(), 1, &store);
        assert_eq!(next(&forwarded), "suspend 0 #1");
        assert_eq!(next(&forwarded), "resume None");
        assert_eq!(next(&forwarded), "suspend 5000 #2");
        assert_eq!(store.suspends.load(Ordering::SeqCst), 0);
        park(&suspension.parker(), 2, &store);
        assert_eq!(store.suspends.load(Ordering::SeqCst), 1);
        suspension.release();
        assert_eq!(store.resumes.load(Ordering::SeqCst), 1);
    }

    /// A shutdown wakes the parked store and refuses later parks.
    #[test]
    fn release_resumes_the_store_and_refuses_parks() {
        let (suspension, forwarded) = spawn();
        let store = Arc::new(Counted::default());
        let parker = suspension.parker();
        suspension.suspend(0);
        assert_eq!(next(&forwarded), "suspend 0 #1");
        park(&parker, 1, &store);
        suspension.release();
        assert_eq!(store.resumes.load(Ordering::SeqCst), 1);
        park(&parker, 1, &store);
        assert_eq!(store.suspends.load(Ordering::SeqCst), 1);
    }

    /// A resume that fails is reported to the loop once and leaves the store parked; the next
    /// resume tries it again.
    #[test]
    fn a_failed_resume_is_reported_and_retried() {
        let (suspension, forwarded) = spawn();
        let store = Arc::new(Counted::default());
        store.failures.store(1, Ordering::SeqCst);
        suspension.suspend(0);
        assert_eq!(next(&forwarded), "suspend 0 #1");
        park(&suspension.parker(), 1, &store);
        suspension.resume();
        assert_eq!(next(&forwarded), "resume Some(\"cannot reopen\")");
        assert_eq!(store.resumes.load(Ordering::SeqCst), 1);
        suspension.resume();
        assert_eq!(next(&forwarded), "resume None");
        assert_eq!(store.resumes.load(Ordering::SeqCst), 2);
        suspension.resume();
        assert_eq!(next(&forwarded), "resume None");
        assert_eq!(store.resumes.load(Ordering::SeqCst), 2);
        suspension.release();
        assert_eq!(store.closes.load(Ordering::SeqCst), 0);
    }

    /// A shutdown closes a store that still cannot reopen, so its waiters wake, and returns.
    #[test]
    fn release_closes_a_store_that_cannot_resume() {
        let (suspension, forwarded) = spawn();
        let store = Arc::new(Counted::default());
        store.failures.store(usize::MAX, Ordering::SeqCst);
        suspension.suspend(0);
        assert_eq!(next(&forwarded), "suspend 0 #1");
        park(&suspension.parker(), 1, &store);
        let (released, done) = mpsc::channel();
        thread::spawn(move || {
            suspension.release();
            let _ = released.send(());
        });
        next(&done);
        assert_eq!(store.resumes.load(Ordering::SeqCst), 1);
        assert_eq!(store.closes.load(Ordering::SeqCst), 1);
    }
}
