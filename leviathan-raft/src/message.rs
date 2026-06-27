//! Raft protocol messages.
//!
//! All messages exchanged between Raft nodes are represented by [`RaftMessage`].
//! Each variant maps directly to an RPC in the Raft paper (§5):
//!
//! - `RequestVote` / `RequestVoteResponse` — leader election (§5.2)
//! - `AppendEntries` / `AppendEntriesResponse` — log replication + heartbeats (§5.3)
//! - `PreVote` / `PreVoteResponse` — pre-vote extension (§9.6, dissertation)
//!
//! [`LogEntry`] is the unit of replication. It bridges to [`WalEntry`] for
//! durable persistence but carries richer type information at the consensus layer.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// LogEntry
// ---------------------------------------------------------------------------

/// A single entry in the Raft replicated log.
///
/// `LogEntry` is the consensus-layer view of a log record. It is converted
/// to/from [`leviathan_storage::WalEntry`] at the persistence boundary.
///
/// The `command` field carries a serialized state machine command. The Raft
/// engine treats it as opaque bytes — interpretation is deferred to the
/// [`StateMachine`](crate::state_machine::StateMachine) implementation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    /// Monotonically increasing log index (1-based, per Raft convention).
    pub index: u64,
    /// The Raft term in which this entry was proposed.
    pub term: u64,
    /// Serialized state machine command. Opaque to the consensus layer.
    pub command: Vec<u8>,
}

impl LogEntry {
    /// Create a new log entry.
    #[must_use]
    pub fn new(index: u64, term: u64, command: Vec<u8>) -> Self {
        Self {
            index,
            term,
            command,
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion: LogEntry <-> WalEntry
// ---------------------------------------------------------------------------

impl From<LogEntry> for leviathan_storage::WalEntry {
    fn from(entry: LogEntry) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
            data: entry.command,
        }
    }
}

impl From<leviathan_storage::WalEntry> for LogEntry {
    fn from(entry: leviathan_storage::WalEntry) -> Self {
        Self {
            index: entry.index,
            term: entry.term,
            command: entry.data,
        }
    }
}

// ---------------------------------------------------------------------------
// RaftMessage
// ---------------------------------------------------------------------------

/// All messages exchanged between Raft peers.
///
/// Serialized with `bincode` for intra-cluster communication. The `term`
/// field on every message enables the fundamental term-comparison protocol
/// that underpins Raft's safety guarantees.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    // --- Leader Election (§5.2) ---
    /// Sent by candidates to request votes from peers.
    RequestVote {
        /// Candidate's current term.
        term: u64,
        /// ID of the candidate requesting the vote.
        candidate_id: u64,
        /// Index of the candidate's last log entry.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },

    /// Response to a `RequestVote` RPC.
    RequestVoteResponse {
        /// The responder's current term (for the candidate to update itself).
        term: u64,
        /// `true` if the vote was granted.
        vote_granted: bool,
        /// ID of the responding node.
        from: u64,
    },

    // --- Pre-Vote Extension (§9.6, dissertation) ---
    /// Pre-vote request. Identical fields to `RequestVote` but does NOT
    /// increment the candidate's term. A node must win a pre-vote round
    /// before starting a real election, preventing disruptive term inflation
    /// from partitioned nodes.
    PreVote {
        /// The term the candidate *would* use if it starts a real election.
        term: u64,
        /// ID of the pre-candidate.
        candidate_id: u64,
        /// Index of the pre-candidate's last log entry.
        last_log_index: u64,
        /// Term of the pre-candidate's last log entry.
        last_log_term: u64,
    },

    /// Response to a `PreVote` RPC.
    PreVoteResponse {
        /// The responder's current term.
        term: u64,
        /// `true` if the pre-vote was granted.
        vote_granted: bool,
        /// ID of the responding node.
        from: u64,
    },

    // --- Log Replication (§5.3) ---
    /// Sent by the leader to replicate log entries and serve as heartbeats.
    ///
    /// When `entries` is empty, this is a heartbeat. The `prev_log_index`
    /// and `prev_log_term` fields implement the log-matching invariant:
    /// a follower rejects the RPC if its log does not contain an entry at
    /// `prev_log_index` with term `prev_log_term`.
    AppendEntries {
        /// Leader's current term.
        term: u64,
        /// ID of the leader sending this RPC.
        leader_id: u64,
        /// Index of the log entry immediately preceding the new entries.
        prev_log_index: u64,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: u64,
        /// Log entries to replicate (empty for heartbeat).
        entries: Vec<LogEntry>,
        /// Leader's commit index — followers advance their commit index
        /// to `min(leader_commit, index of last new entry)`.
        leader_commit: u64,
    },

    /// Response to an `AppendEntries` RPC.
    AppendEntriesResponse {
        /// The responder's current term.
        term: u64,
        /// `true` if the follower successfully appended the entries.
        success: bool,
        /// The follower's match index after processing (optimization for
        /// the leader to update `match_index` without a round-trip).
        match_index: u64,
        /// ID of the responding follower.
        from: u64,
    },
}

impl RaftMessage {
    /// Extract the term from any message variant.
    ///
    /// Every Raft message carries a term, enabling the universal
    /// "if term > currentTerm, step down" rule.
    #[must_use]
    pub fn term(&self) -> u64 {
        match self {
            Self::RequestVote { term, .. }
            | Self::RequestVoteResponse { term, .. }
            | Self::PreVote { term, .. }
            | Self::PreVoteResponse { term, .. }
            | Self::AppendEntries { term, .. }
            | Self::AppendEntriesResponse { term, .. } => *term,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_entry_roundtrip_via_wal_entry() {
        let entry = LogEntry::new(42, 3, b"hello".to_vec());
        let wal: leviathan_storage::WalEntry = entry.clone().into();
        let back: LogEntry = wal.into();
        assert_eq!(entry, back);
    }

    #[test]
    fn message_term_extraction() {
        let msg = RaftMessage::AppendEntries {
            term: 7,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        assert_eq!(msg.term(), 7);
    }

    #[test]
    fn request_vote_serialization() {
        let msg = RaftMessage::RequestVote {
            term: 5,
            candidate_id: 2,
            last_log_index: 10,
            last_log_term: 4,
        };
        let bytes = bincode::serialize(&msg).expect("serialize");
        let back: RaftMessage = bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back.term(), 5);
    }
}
