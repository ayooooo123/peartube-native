//! The default `NodeFactory`: a real node from the options the app passes to `createNode`.

use std::sync::Arc;

use serde_json::Value;

use crate::backend::BackendError;
use crate::node::api::NodeFactory;
use crate::node::core::{create_node, CoreNode};

/// Opens the native node the backend's `start` drives.
pub struct Factory;

impl NodeFactory for Factory {
    type Node = CoreNode;
    type Error = BackendError;

    async fn create(&self, options: Value) -> Result<Arc<Self::Node>, Self::Error> {
        create_node(options)
            .await
            .map_err(|error| BackendError::new(error.to_string()))
    }
}
