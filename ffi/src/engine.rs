//! The only module that names the backend implementation.
//!
//! Everything else in the crate talks to [`Engine`], so a signature change in
//! [`peartube_native::backend::Backend`] is fixed here and nowhere else.

use serde_json::Value;

/// A backend line sink; `pt_init` puts the host callback behind it.
pub(crate) type Sink = Box<dyn Fn(String) + Send + 'static>;

/// The backend surface the C ABI drives. Production is [`Backend`], the test
/// echo in [`crate::test_support`] stands in for it.
///
/// [`Backend`]: peartube_native::backend::Backend
pub(crate) trait Engine: Send + Sync {
    fn send(&self, line: &str) -> Result<(), String>;
    fn suspend(&self, linger_ms: u64);
    fn resume(&self);
    fn shutdown(&self);
}

impl Engine for peartube_native::backend::Backend {
    fn send(&self, line: &str) -> Result<(), String> {
        peartube_native::backend::Backend::send(self, line).map_err(|err| err.to_string())
    }

    fn suspend(&self, linger_ms: u64) {
        peartube_native::backend::Backend::suspend(self, linger_ms);
    }

    fn resume(&self) {
        peartube_native::backend::Backend::resume(self);
    }

    fn shutdown(&self) {
        peartube_native::backend::Backend::shutdown(self);
    }
}

pub(crate) fn spawn(config: Value, sink: Sink) -> Result<Box<dyn Engine>, String> {
    if crate::test_support::Panicking::selected(&config) {
        return Ok(Box::new(crate::test_support::Panicking));
    }
    peartube_native::backend::Backend::spawn(config, sink)
        .map(|backend| Box::new(backend) as Box<dyn Engine>)
        .map_err(|err| err.to_string())
}
