//! Raft consensus error types.
//!
//! Every failure mode in the Raft engine is represented by a variant of
//! [`RaftError`]. No `unwrap()` / `expect()` in non-test code — all errors
//! propagate through this enum.

use thiserror::Error;

/// Exhaustive error enum for the Raft consensus subsystem.
///
/// Each variant carries enough context for structured logging and upstream
/// error mapping without requiring a backtrace in production.
#[derive(Debug, Error)]
pub enum RaftError {
    /// The current node is not the leader and cannot service a write request.
    /// The optional `NodeId` is the last known leader (if any).
    #[error("not leader; known leader: {leader:?}")]
    NotLeader {
        /// Last known leader, if any. Clients should redirect to this node.
        leader: Option<u64>,
    },

    /// A message arrived with a stale term. The sender's term is behind the
    /// receiver's current term, indicating a partition-healed or restarted node.
    #[error("term mismatch: local={local_term}, remote={remote_term}")]
    TermMismatch {
        /// The term of the local node.
        local_term: u64,
        /// The term carried by the inbound message.
        remote_term: u64,
    },

    /// The follower's log does not match the leader's at the specified
    /// `prev_log_index` / `prev_log_term`, indicating a divergence that
    /// requires the leader to back-track.
    #[error("log divergence at index {index}: expected term {expected_term}, found {found_term:?}")]
    LogDivergence {
        /// The index at which divergence was detected.
        index: u64,
        /// The term the leader expected at that index.
        expected_term: u64,
        /// The term the follower actually has (None if the index is beyond the log).
        found_term: Option<u64>,
    },

    /// An error from the underlying durable storage (WAL).
    #[error("storage failure: {0}")]
    StorageFailure(String),

    /// A bounded `mpsc` channel was closed or full, indicating the recipient
    /// task has been cancelled or is applying excessive back-pressure.
    #[error("channel closed or full: {0}")]
    ChannelClosed(String),

    /// A `std::sync::Mutex` was poisoned by a panicking thread. We handle
    /// this explicitly rather than propagating the panic.
    #[error("mutex poisoned: {0}")]
    MutexPoisoned(String),

    /// An election round timed out without reaching quorum.
    #[error("election timeout: no quorum after {attempts} attempt(s)")]
    ElectionTimeout {
        /// How many election rounds were attempted.
        attempts: u64,
    },

    /// A Raft proposal was submitted but the log has reached its maximum
    /// uncommitted entry limit (back-pressure).
    #[error("proposal rejected: too many uncommitted entries ({count})")]
    BackPressure {
        /// Current number of uncommitted entries.
        count: u64,
    },

    /// An internal invariant was violated — this should never happen.
    #[error("internal raft error: {0}")]
    Internal(String),
}

/// Convenience alias for Raft operations.
pub type Result<T> = std::result::Result<T, RaftError>;

// ---------------------------------------------------------------------------
// Bridge into the platform-wide error type
// ---------------------------------------------------------------------------

impl From<RaftError> for leviathan_core::LeviathanError {
    fn from(e: RaftError) -> Self {
        leviathan_core::LeviathanError::Consensus(e.to_string())
    }
}

impl From<leviathan_storage::StorageError> for RaftError {
    fn from(e: leviathan_storage::StorageError) -> Self {
        RaftError::StorageFailure(e.to_string())
    }
}

impl From<leviathan_core::LeviathanError> for RaftError {
    fn from(e: leviathan_core::LeviathanError) -> Self {
        RaftError::StorageFailure(e.to_string())
    }
}
