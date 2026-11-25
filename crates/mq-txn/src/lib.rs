//! Transaction state machine shared by the broker and the transaction
//! coordinator.
//!
//! The WAL / Raft integration lives above this module: it must persist the
//! transition before the in-memory state is changed. Keeping the transition
//! rules here makes them property-testable without IO.

use mq_common::{MqError, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxState {
    Prepared,
    Committing,
    Committed,
    RolledBack,
    TimedOut,
}

impl TxState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::RolledBack)
    }

    pub fn can_transition_to(self, next: Self) -> bool {
        use TxState::*;
        match (self, next) {
            // Normal two-phase path.
            (Prepared, Committing) => true,
            (Prepared, RolledBack) => true,
            (Committing, Committed) => true,
            // Recovery may replay a Committing transition.
            (Committing, Committing) => true,
            // Timeout policy.
            (Prepared, TimedOut) => true,
            (TimedOut, RolledBack) => true,
            (TimedOut, Committed) => true,
            // Idempotent repeats are accepted by the API layer, not here.
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub tx_id: String,
    pub producer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub half_offset: u64,
    pub state: TxState,
    pub check_count: u16,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub version: u64,
}

impl TxRecord {
    pub fn prepared(
        tx_id: impl Into<String>,
        producer_group: impl Into<String>,
        topic: impl Into<String>,
        queue_id: i32,
        half_offset: u64,
        now_ms: i64,
    ) -> Self {
        Self {
            tx_id: tx_id.into(),
            producer_group: producer_group.into(),
            topic: topic.into(),
            queue_id,
            half_offset,
            state: TxState::Prepared,
            check_count: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            version: 0,
        }
    }

    /// CAS-style transition. The caller must compare `expected_version` with
    /// the current `version`; a mismatch means another checker won the race.
    pub fn transition(&mut self, next: TxState, expected_version: u64, now_ms: i64) -> Result<()> {
        if self.version != expected_version {
            return Err(MqError::Invalid(format!(
                "tx {} version mismatch: expected {}, actual {}",
                self.tx_id, expected_version, self.version
            )));
        }
        if !self.state.can_transition_to(next) {
            return Err(MqError::Invalid(format!(
                "tx {} illegal transition {:?} -> {:?}",
                self.tx_id, self.state, next
            )));
        }
        self.state = next;
        self.updated_at_ms = now_ms;
        self.version += 1;
        Ok(())
    }

    pub fn record_check(&mut self, now_ms: i64) {
        self.check_count = self.check_count.saturating_add(1);
        self.updated_at_ms = now_ms;
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_commit_path() {
        let mut tx = TxRecord::prepared("tx-1", "g", "orders", 0, 10, 0);
        tx.transition(TxState::Committing, 0, 1).unwrap();
        tx.transition(TxState::Committed, 1, 2).unwrap();
        assert_eq!(tx.state, TxState::Committed);
        assert_eq!(tx.version, 2);
    }

    #[test]
    fn rollback_is_terminal() {
        let mut tx = TxRecord::prepared("tx-2", "g", "orders", 0, 10, 0);
        tx.transition(TxState::RolledBack, 0, 1).unwrap();
        assert!(tx.transition(TxState::Committed, 1, 2).is_err());
        assert_eq!(tx.state, TxState::RolledBack);
    }

    #[test]
    fn stale_version_is_rejected() {
        let mut tx = TxRecord::prepared("tx-3", "g", "orders", 0, 10, 0);
        tx.transition(TxState::Committing, 0, 1).unwrap();
        assert!(tx.transition(TxState::Committed, 0, 2).is_err());
    }

    #[test]
    fn timeout_can_be_resolved_manually() {
        let mut tx = TxRecord::prepared("tx-4", "g", "orders", 0, 10, 0);
        tx.transition(TxState::TimedOut, 0, 1).unwrap();
        tx.transition(TxState::Committed, 1, 2).unwrap();
        assert_eq!(tx.state, TxState::Committed);
    }
}
