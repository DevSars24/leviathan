//! Raft state machine abstraction.
//!
//! The [`StateMachine`] trait defines the interface between the Raft consensus
//! engine and the application-specific logic. The Raft engine guarantees that
//! entries are applied in log order, exactly once, and only after they have
//! been committed to a quorum.
//!
//! # Thread Safety
//!
//! The state machine is accessed through `Arc<std::sync::Mutex<dyn StateMachine>>`.
//! We use `std::sync::Mutex` (not `tokio::sync::Mutex`) because `apply` is
//! a synchronous, CPU-bound operation — holding an async mutex across a
//! blocking computation would starve the executor. The trade-off is that we
//! must handle mutex poisoning explicitly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::error::{RaftError, Result};
use crate::message::LogEntry;

// ---------------------------------------------------------------------------
// StateMachine trait
// ---------------------------------------------------------------------------

/// A deterministic state machine driven by the Raft replicated log.
///
/// # Contract
///
/// - `apply` must be **deterministic**: given the same sequence of entries,
///   every node must produce identical state.
/// - `apply` must be **idempotent** with respect to log index: re-applying
///   an already-applied entry (e.g., after a restart) must not corrupt state.
/// - `apply` must not perform I/O or blocking operations.
pub trait StateMachine: Send {
    /// Apply a committed log entry to the state machine.
    ///
    /// Returns an opaque response that can be forwarded to the client.
    ///
    /// # Errors
    ///
    /// Implementations should return `Err` only for truly unrecoverable
    /// conditions (e.g., corrupt command payload). Transient errors should
    /// be retried internally.
    fn apply(&mut self, entry: &LogEntry) -> Result<Vec<u8>>;

    /// Return a snapshot of the current state for debugging / observability.
    fn snapshot(&self) -> Vec<u8>;
}

// ---------------------------------------------------------------------------
// Guard wrapper for poisoned mutex
// ---------------------------------------------------------------------------

/// Safely lock an `Arc<Mutex<dyn StateMachine>>`, handling poison.
///
/// If the mutex is poisoned (a previous holder panicked), we log a warning
/// and recover the inner value. This is safe because our `StateMachine::apply`
/// is side-effect-free — partial application from a panicked thread does not
/// leave the state machine in an unrecoverable state.
///
/// # Errors
///
/// Returns `RaftError::MutexPoisoned` only if recovery is impossible (this
/// currently never happens — we always recover).
pub fn lock_state_machine<'a>(
    sm: &'a Arc<Mutex<dyn StateMachine + 'static>>,
) -> Result<std::sync::MutexGuard<'a, dyn StateMachine + 'static>> {
    Ok(sm.lock().unwrap_or_else(|poisoned: PoisonError<_>| {
        warn!("StateMachine mutex was poisoned — recovering inner value");
        poisoned.into_inner()
    }))
}

// ---------------------------------------------------------------------------
// ClusterStateMachine — a key-value store for workload state
// ---------------------------------------------------------------------------

/// A state machine command for the cluster workload store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClusterCommand {
    /// Schedule a workload onto the cluster.
    SubmitWorkload {
        /// Unique workload identifier.
        workload_id: String,
        /// Serialized workload specification.
        spec: Vec<u8>,
    },
    /// Update the status of a workload.
    UpdateStatus {
        /// Workload identifier.
        workload_id: String,
        /// New status string.
        status: String,
    },
    /// Remove a workload from the cluster.
    RemoveWorkload {
        /// Workload identifier.
        workload_id: String,
    },
}

/// A simple key-value state machine for cluster workload management.
///
/// Maps workload IDs to their current state (serialized spec + status).
/// This is the concrete `StateMachine` used by Leviathan's control plane.
#[derive(Debug, Default)]
pub struct ClusterStateMachine {
    /// Workload ID → (serialized spec, status).
    workloads: HashMap<String, (Vec<u8>, String)>,
    /// The index of the last applied entry, for idempotency.
    last_applied_index: u64,
}

impl ClusterStateMachine {
    /// Create a new, empty cluster state machine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of tracked workloads.
    #[must_use]
    pub fn workload_count(&self) -> usize {
        self.workloads.len()
    }

    /// Check if a workload exists.
    #[must_use]
    pub fn has_workload(&self, id: &str) -> bool {
        self.workloads.contains_key(id)
    }

    /// Get the status of a workload.
    #[must_use]
    pub fn workload_status(&self, id: &str) -> Option<&str> {
        self.workloads.get(id).map(|(_, status)| status.as_str())
    }
}

impl StateMachine for ClusterStateMachine {
    fn apply(&mut self, entry: &LogEntry) -> Result<Vec<u8>> {
        // Idempotency guard: skip entries we've already applied.
        if entry.index <= self.last_applied_index {
            return Ok(Vec::new());
        }

        let cmd: ClusterCommand = bincode::deserialize(&entry.command).map_err(|e| {
            RaftError::Internal(format!("failed to deserialize cluster command: {e}"))
        })?;

        match cmd {
            ClusterCommand::SubmitWorkload { workload_id, spec } => {
                self.workloads
                    .insert(workload_id.clone(), (spec, "pending".to_string()));
                self.last_applied_index = entry.index;
                Ok(format!("workload {workload_id} submitted").into_bytes())
            }
            ClusterCommand::UpdateStatus {
                workload_id,
                status,
            } => {
                if let Some(entry_data) = self.workloads.get_mut(&workload_id) {
                    entry_data.1 = status;
                }
                self.last_applied_index = entry.index;
                Ok(format!("workload {workload_id} updated").into_bytes())
            }
            ClusterCommand::RemoveWorkload { workload_id } => {
                self.workloads.remove(&workload_id);
                self.last_applied_index = entry.index;
                Ok(format!("workload {workload_id} removed").into_bytes())
            }
        }
    }

    fn snapshot(&self) -> Vec<u8> {
        serde_json::to_vec(&self.workloads).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_and_query_workload() {
        let mut sm = ClusterStateMachine::new();
        let cmd = ClusterCommand::SubmitWorkload {
            workload_id: "wl-1".into(),
            spec: b"nginx:latest".to_vec(),
        };
        let entry = LogEntry::new(1, 1, bincode::serialize(&cmd).unwrap());
        sm.apply(&entry).unwrap();

        assert!(sm.has_workload("wl-1"));
        assert_eq!(sm.workload_status("wl-1"), Some("pending"));
    }

    #[test]
    fn update_status() {
        let mut sm = ClusterStateMachine::new();

        let cmd1 = ClusterCommand::SubmitWorkload {
            workload_id: "wl-1".into(),
            spec: vec![],
        };
        sm.apply(&LogEntry::new(1, 1, bincode::serialize(&cmd1).unwrap()))
            .unwrap();

        let cmd2 = ClusterCommand::UpdateStatus {
            workload_id: "wl-1".into(),
            status: "running".into(),
        };
        sm.apply(&LogEntry::new(2, 1, bincode::serialize(&cmd2).unwrap()))
            .unwrap();

        assert_eq!(sm.workload_status("wl-1"), Some("running"));
    }

    #[test]
    fn idempotent_apply() {
        let mut sm = ClusterStateMachine::new();
        let cmd = ClusterCommand::SubmitWorkload {
            workload_id: "wl-1".into(),
            spec: vec![],
        };
        let entry = LogEntry::new(1, 1, bincode::serialize(&cmd).unwrap());

        sm.apply(&entry).unwrap();
        sm.apply(&entry).unwrap(); // re-apply same index — should be no-op

        assert_eq!(sm.workload_count(), 1);
    }

    #[test]
    fn remove_workload() {
        let mut sm = ClusterStateMachine::new();
        let cmd1 = ClusterCommand::SubmitWorkload {
            workload_id: "wl-1".into(),
            spec: vec![],
        };
        sm.apply(&LogEntry::new(1, 1, bincode::serialize(&cmd1).unwrap()))
            .unwrap();

        let cmd2 = ClusterCommand::RemoveWorkload {
            workload_id: "wl-1".into(),
        };
        sm.apply(&LogEntry::new(2, 1, bincode::serialize(&cmd2).unwrap()))
            .unwrap();

        assert!(!sm.has_workload("wl-1"));
    }

    #[test]
    fn lock_state_machine_handles_clean_lock() {
        let sm: Arc<Mutex<dyn StateMachine>> =
            Arc::new(Mutex::new(ClusterStateMachine::new()));
        let guard = lock_state_machine(&sm);
        assert!(guard.is_ok());
    }
}
