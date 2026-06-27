//! Raft cluster configuration.
//!
//! [`RaftConfig`] encapsulates all tunable parameters for the Raft consensus
//! engine. Default values are chosen to balance liveness (fast election) with
//! stability (avoiding spurious elections under moderate network jitter).
//!
//! # Cache-Line Alignment
//!
//! Hot-path fields (election timeout bounds, heartbeat interval) are grouped
//! at the top of the struct. The struct is `repr(C)` to ensure deterministic
//! layout — preventing the compiler from re-ordering fields in ways that would
//! split a hot read across two cache lines (64 bytes on x86-64 / ARM).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Configuration for a single Raft node.
///
/// All duration fields are in milliseconds for JSON-friendly serialization.
/// The randomized election timeout is drawn uniformly from
/// `[election_timeout_min, election_timeout_max)` on each election round to
/// prevent synchronized elections across the cluster (§5.2 of the Raft paper).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct RaftConfig {
    // --- Hot path (fits in first cache line) ---
    /// Minimum election timeout in milliseconds.
    /// Raft paper recommends 150–300ms for LAN clusters.
    pub election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds (exclusive upper bound).
    pub election_timeout_max_ms: u64,

    /// Heartbeat interval in milliseconds. Must be substantially less than
    /// `election_timeout_min_ms` to prevent spurious elections.
    /// Raft paper: heartbeat << election timeout.
    pub heartbeat_interval_ms: u64,

    /// Maximum number of log entries to batch in a single `AppendEntries` RPC.
    /// Controls the trade-off between throughput (large batches) and latency
    /// (small batches). Also bounds memory usage per RPC.
    pub max_entries_per_append: usize,

    // --- Back-pressure ---
    /// Maximum number of uncommitted entries before proposals are rejected
    /// with [`RaftError::BackPressure`]. Prevents unbounded memory growth
    /// when the cluster is partitioned or a follower is slow.
    pub max_uncommitted_entries: u64,

    /// Bounded channel capacity for inter-node message passing.
    /// Too small → dropped messages under load.
    /// Too large → unbounded memory if a consumer stalls.
    pub channel_capacity: usize,

    /// Unique numeric identifier for this node in the cluster.
    /// Must be unique across all nodes and stable across restarts.
    pub node_id: u64,

    /// Set of all node IDs in the cluster (including self).
    /// Used to compute quorum size: `peers.len() / 2 + 1`.
    pub peers: Vec<u64>,

    /// Enable the pre-vote extension (§9.6 of the Raft dissertation).
    ///
    /// When enabled, a node must win a pre-vote round before incrementing
    /// its term and starting a real election. This prevents disruptive
    /// elections from partitioned nodes that rejoin with a high term.
    pub pre_vote_enabled: bool,
}

impl RaftConfig {
    /// Compute the quorum size for the current cluster configuration.
    ///
    /// Quorum = floor(N/2) + 1, where N = total nodes (including self).
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.peers.len() / 2 + 1
    }

    /// Return the election timeout range as `Duration` values.
    #[must_use]
    pub fn election_timeout_range(&self) -> (Duration, Duration) {
        (
            Duration::from_millis(self.election_timeout_min_ms),
            Duration::from_millis(self.election_timeout_max_ms),
        )
    }

    /// Return the heartbeat interval as a `Duration`.
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms)
    }

    /// Validate the configuration and return an error string if invalid.
    #[must_use]
    pub fn validate(&self) -> Option<String> {
        if self.election_timeout_min_ms >= self.election_timeout_max_ms {
            return Some("election_timeout_min_ms must be < election_timeout_max_ms".into());
        }
        if self.heartbeat_interval_ms >= self.election_timeout_min_ms {
            return Some(
                "heartbeat_interval_ms must be << election_timeout_min_ms".into(),
            );
        }
        if self.peers.is_empty() {
            return Some("peers must contain at least one node (self)".into());
        }
        if !self.peers.contains(&self.node_id) {
            return Some("peers must contain self (node_id)".into());
        }
        if self.max_entries_per_append == 0 {
            return Some("max_entries_per_append must be > 0".into());
        }
        None
    }
}

impl Default for RaftConfig {
    /// Sensible defaults for a 3-node LAN cluster.
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            max_entries_per_append: 64,
            max_uncommitted_entries: 4096,
            channel_capacity: 256,
            node_id: 1,
            peers: vec![1, 2, 3],
            pre_vote_enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        let cfg = RaftConfig::default();
        assert!(cfg.validate().is_none(), "default config should be valid");
    }

    #[test]
    fn quorum_size_3_nodes() {
        let cfg = RaftConfig {
            peers: vec![1, 2, 3],
            ..RaftConfig::default()
        };
        assert_eq!(cfg.quorum_size(), 2);
    }

    #[test]
    fn quorum_size_5_nodes() {
        let cfg = RaftConfig {
            peers: vec![1, 2, 3, 4, 5],
            ..RaftConfig::default()
        };
        assert_eq!(cfg.quorum_size(), 3);
    }

    #[test]
    fn invalid_timeout_order() {
        let cfg = RaftConfig {
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 150,
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_some());
    }

    #[test]
    fn heartbeat_must_be_less_than_election_timeout() {
        let cfg = RaftConfig {
            heartbeat_interval_ms: 200,
            election_timeout_min_ms: 150,
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_some());
    }

    #[test]
    fn self_must_be_in_peers() {
        let cfg = RaftConfig {
            node_id: 99,
            peers: vec![1, 2, 3],
            ..RaftConfig::default()
        };
        assert!(cfg.validate().is_some());
    }
}
