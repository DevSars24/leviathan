//! # leviathan-raft
//!
//! Production-grade Raft consensus engine for the Leviathan distributed
//! container orchestration platform.
//!
//! ## Architecture
//!
//! The crate is organized into focused modules:
//!
//! - [`config`] — Cluster configuration (election timeouts, quorum size, etc.)
//! - [`error`] — Typed error enum covering all Raft failure modes
//! - [`message`] — Protocol messages (RequestVote, AppendEntries, PreVote)
//! - [`log`] — WAL-backed replicated log with Raft-specific operations
//! - [`state_machine`] — Generic state machine trait + cluster workload impl
//! - [`node`] — The core consensus engine (leader election, log replication)
//!
//! ## Safety Invariants
//!
//! This implementation enforces the three Raft safety properties:
//!
//! 1. **Election Safety**: At most one leader per term. Enforced by the
//!    `voted_for` check in `handle_request_vote`.
//! 2. **Log Matching**: If two logs contain an entry with the same index and
//!    term, all preceding entries are identical. Enforced by `prev_log_index`
//!    / `prev_log_term` checks in `append_entries`.
//! 3. **Leader Completeness**: If an entry is committed in a given term, it
//!    will be present in the logs of all leaders for all higher terms.
//!    Enforced by the log up-to-date check in `handle_request_vote`.

#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod log;
pub mod message;
pub mod node;
pub mod state_machine;

pub use config::RaftConfig;
pub use error::RaftError;
pub use log::RaftLog;
pub use message::{LogEntry, RaftMessage};
pub use node::{ChannelTransport, ProposalResponse, RaftNode, RaftTransport, Role};
pub use state_machine::{ClusterCommand, ClusterStateMachine, StateMachine};
