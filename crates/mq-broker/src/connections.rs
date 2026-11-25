//! Per-connection producer registry used for transaction check-back.
//!
//! A producer registers itself via HEART_BEAT; the broker keeps an unbounded
//! sender for its connection so it can push CHECK_TRANSACTION_STATE (code 39)
//! to the producer. Stale senders are harmless: `send` fails and is ignored.

use mq_protocol::RemotingCommand;
use std::collections::HashMap;
use std::sync::RwLock;
use tokio::sync::mpsc::UnboundedSender;

#[derive(Default)]
pub struct ProducerConnections {
    inner: RwLock<HashMap<String, Vec<UnboundedSender<RemotingCommand>>>>,
}

impl ProducerConnections {
    pub fn register(&self, group: &str, sender: UnboundedSender<RemotingCommand>) {
        let mut map = self.inner.write().unwrap();
        let list = map.entry(group.to_string()).or_default();
        if !list.iter().any(|s| s.same_channel(&sender)) {
            list.push(sender);
        }
    }

    /// Send to any live connection of `group`; returns false if none accepted.
    pub fn send(&self, group: &str, cmd: RemotingCommand) -> bool {
        let map = self.inner.read().unwrap();
        let Some(list) = map.get(group) else {
            return false;
        };
        for sender in list {
            if sender.send(cmd.clone()).is_ok() {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[test]
    fn registers_and_sends_to_group() {
        let reg = ProducerConnections::default();
        let (tx, mut rx) = mpsc::unbounded_channel();
        reg.register("g", tx);
        assert!(reg.send("g", RemotingCommand::request(39, 1)));
        assert!(rx.try_recv().is_ok());
        assert!(!reg.send("missing", RemotingCommand::request(39, 2)));
    }
}
