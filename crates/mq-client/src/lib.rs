//! Rust client scaffold. R1 will add Remoting transport, producer and pull
//! consumer; the API shape is intentionally small so it can evolve.

use mq_common::{Message, Result};

#[derive(Debug, Clone)]
pub struct Producer {
    pub group: String,
    pub namesrv: Vec<String>,
}

impl Producer {
    pub fn new(group: impl Into<String>, namesrv: Vec<String>) -> Self {
        Self {
            group: group.into(),
            namesrv,
        }
    }

    /// Placeholder: R1 sends through `mq-protocol::RemotingCommand`.
    pub async fn send(&self, _message: Message) -> Result<()> {
        Ok(())
    }
}
