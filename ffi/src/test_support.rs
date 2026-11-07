//! A backend double whose `send` panics, so the ABI's panic containment can be
//! exercised without a real fault. Nothing here is part of the C ABI: no
//! symbol, config key or status code in it appears in `include/peartube_native.h`.

use serde_json::Value;

use crate::engine::Engine;

pub(crate) struct Panicking;

impl Panicking {
    /// Selects this double when the config carries `"__test_panic": true`.
    /// [`peartube_native::backend::Backend`] ignores its config today, so the
    /// key cannot change production behaviour.
    pub(crate) fn selected(config: &Value) -> bool {
        config
            .get("__test_panic")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    }
}

impl Engine for Panicking {
    fn send(&self, _line: &str) -> Result<(), String> {
        panic!("test backend panic");
    }

    fn suspend(&self, _linger_ms: u64) {}

    fn resume(&self) {}

    fn shutdown(&self) {}
}
