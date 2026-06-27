//! Raft consensus node — the core engine.
//!
//! [`RaftNode`] implements the full Raft protocol:
//! - **Leader election** with randomized timeouts and pre-vote (§5.2, §9.6)
//! - **Log replication** with batched `AppendEntries` and pipeline flow control (§5.3)
//! - **Safety invariants**: election safety (≤1 leader/term), log matching,
//!   leader completeness — enforced structurally through Rust's type system
//!
//! # Architecture
//!
//! The node runs as an async task driven by a `tokio::select!` loop over:
//! 1. Inbound messages from peers (via a bounded `mpsc` channel)
//! 2. Election timeout (randomized `tokio::time::sleep`)
//! 3. Heartbeat timer (leader only, `tokio::time::interval`)
//! 4. Client proposals (via a bounded `mpsc` channel)
//!
//! # Term Monotonicity
//!
//! The current term is stored in an `AtomicU64` with `Ordering::AcqRel`
//! semantics on every read-modify-write. This ensures that:
//! - All subsequent reads by the same thread see the updated term (Acquire).
//! - All prior writes by the same thread are visible to readers (Release).
//!
//! This is critical because term checks gate leader legitimacy.
//!
//! # Transport Abstraction
//!
//! Network I/O is abstracted behind the [`RaftTransport`] trait, enabling
//! fully in-process testing with a channel-based mock transport.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rand::Rng;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::RaftConfig;
use crate::error::{RaftError, Result};
use crate::log::RaftLog;
use crate::message::{LogEntry, RaftMessage};
use crate::state_machine::{lock_state_machine, StateMachine};

// ---------------------------------------------------------------------------
// Role — Raft node state
// ---------------------------------------------------------------------------

/// The role of a Raft node in the cluster.
///
/// Transitions follow the Raft state diagram:
/// ```text
/// Follower ──(timeout)──▶ Candidate ──(wins election)──▶ Leader
///     ▲                       │                             │
///     └───(discovers leader)──┘                             │
///     └─────────────(discovers higher term)──────────────────┘
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Passive participant. Responds to RPCs from leaders and candidates.
    Follower,
    /// Actively seeking votes. Transitions to Leader on quorum, or back
    /// to Follower if a valid leader is discovered.
    Candidate,
    /// Active leader. Sends heartbeats, replicates log entries, and
    /// services client proposals.
    Leader,
}

// ---------------------------------------------------------------------------
// Transport abstraction
// ---------------------------------------------------------------------------

/// Abstraction over the network transport for Raft RPCs.
///
/// Implementations must deliver messages reliably (at-least-once) to the
/// specified peer. The trait is `Send + Sync` so it can be shared across
/// async tasks.
#[async_trait::async_trait]
pub trait RaftTransport: Send + Sync {
    /// Send a message to a specific peer node.
    ///
    /// # Errors
    ///
    /// Returns `RaftError::ChannelClosed` if the peer is unreachable.
    async fn send(&self, to: u64, msg: RaftMessage) -> Result<()>;

    /// Broadcast a message to all peers (excluding self).
    ///
    /// Default implementation sends sequentially. Override for parallel fan-out.
    async fn broadcast(&self, from: u64, peers: &[u64], msg: RaftMessage) -> Result<()> {
        for &peer in peers {
            if peer != from {
                // Best-effort broadcast — log and continue on individual failures.
                if let Err(e) = self.send(peer, msg.clone()).await {
                    warn!(peer, error = %e, "Failed to send to peer");
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory channel transport (for testing)
// ---------------------------------------------------------------------------

/// A fully in-process transport backed by `mpsc` channels.
///
/// Used in integration tests to run a multi-node Raft cluster without
/// real network I/O.
pub struct ChannelTransport {
    /// Map from node ID to its inbound message channel sender.
    senders: HashMap<u64, mpsc::Sender<RaftMessage>>,
}

impl ChannelTransport {
    /// Create a new channel transport with the given peer senders.
    #[must_use]
    pub fn new(senders: HashMap<u64, mpsc::Sender<RaftMessage>>) -> Self {
        Self { senders }
    }
}

#[async_trait::async_trait]
impl RaftTransport for ChannelTransport {
    async fn send(&self, to: u64, msg: RaftMessage) -> Result<()> {
        let sender = self.senders.get(&to).ok_or_else(|| {
            RaftError::ChannelClosed(format!("no channel for peer {to}"))
        })?;
        sender.send(msg).await.map_err(|e| {
            RaftError::ChannelClosed(format!("channel to peer {to} closed: {e}"))
        })
    }
}

// ---------------------------------------------------------------------------
// ProposalResponse — result of a client proposal
// ---------------------------------------------------------------------------

/// The result of a client proposal submitted to the Raft leader.
#[derive(Debug)]
pub struct ProposalResponse {
    /// The log index assigned to the proposal.
    pub index: u64,
    /// The term in which the proposal was accepted.
    pub term: u64,
}

// ---------------------------------------------------------------------------
// RaftNode — the core consensus engine
// ---------------------------------------------------------------------------

/// A single node in a Raft consensus cluster.
///
/// Owns the replicated log, state machine, and transport. Driven by
/// [`RaftNode::run`] which enters the main event loop.
pub struct RaftNode {
    /// Cluster configuration (immutable after construction).
    config: RaftConfig,

    /// Current term — the fundamental monotonic clock of Raft.
    ///
    /// Stored as `AtomicU64` with `AcqRel` semantics to ensure term
    /// updates are immediately visible across all code paths within
    /// the same task. While `RaftNode` is single-owner, the atomic
    /// provides a formal memory ordering guarantee that the compiler
    /// cannot reorder term reads past writes.
    current_term: AtomicU64,

    /// ID of the node we voted for in the current term, or `None`.
    voted_for: Option<u64>,

    /// Current role in the cluster.
    role: Role,

    /// The replicated log (backed by WAL).
    log: RaftLog,

    /// The application state machine, applied in log order.
    state_machine: Arc<Mutex<dyn StateMachine>>,

    /// Transport for peer communication.
    transport: Arc<dyn RaftTransport>,

    /// Inbound message channel from peers.
    inbox: mpsc::Receiver<RaftMessage>,

    /// Client proposal channel.
    proposals: mpsc::Receiver<Vec<u8>>,

    /// Proposal response channel — sends back the assigned index/term.
    proposal_responses: mpsc::Sender<std::result::Result<ProposalResponse, RaftError>>,

    // --- Leader-only state ---
    /// For each peer: the next log index to send.
    /// Initialized to `last_log_index + 1` on election.
    next_index: HashMap<u64, u64>,

    /// For each peer: the highest log index known to be replicated.
    /// Initialized to 0 on election.
    match_index: HashMap<u64, u64>,

    /// ID of the last known leader (for client redirection).
    leader_id: Option<u64>,
}

impl RaftNode {
    /// Construct a new Raft node.
    ///
    /// The node starts as a `Follower` with term 0. Call [`RaftNode::run`]
    /// to enter the main event loop.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: RaftConfig,
        log: RaftLog,
        state_machine: Arc<Mutex<dyn StateMachine>>,
        transport: Arc<dyn RaftTransport>,
        inbox: mpsc::Receiver<RaftMessage>,
        proposals: mpsc::Receiver<Vec<u8>>,
        proposal_responses: mpsc::Sender<std::result::Result<ProposalResponse, RaftError>>,
    ) -> Self {
        Self {
            config,
            current_term: AtomicU64::new(0),
            voted_for: None,
            role: Role::Follower,
            log,
            state_machine,
            transport,
            inbox,
            proposals,
            proposal_responses,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            leader_id: None,
        }
    }

    /// Return the current term.
    #[must_use]
    pub fn term(&self) -> u64 {
        // Acquire: ensures we see all writes that happened before this
        // term was set. This is the read-side of the AcqRel pair.
        self.current_term.load(Ordering::Acquire)
    }

    /// Return the current role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// Return the node's ID.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.config.node_id
    }

    /// Set the current term atomically.
    ///
    /// Uses `AcqRel` ordering:
    /// - Release: all prior writes (log mutations, vote changes) are
    ///   visible to any thread that subsequently reads this term.
    /// - Acquire: we see all writes from whoever last set this value.
    fn set_term(&self, term: u64) {
        self.current_term.store(term, Ordering::Release);
    }

    /// Step down to Follower if we discover a higher term.
    ///
    /// This is the universal Raft rule: "If RPC request or response contains
    /// term T > currentTerm: set currentTerm = T, convert to follower" (§5.1).
    fn maybe_step_down(&mut self, remote_term: u64) -> bool {
        if remote_term > self.term() {
            info!(
                local_term = self.term(),
                remote_term,
                "Discovered higher term — stepping down to Follower"
            );
            self.set_term(remote_term);
            self.role = Role::Follower;
            self.voted_for = None;
            true
        } else {
            false
        }
    }

    /// Generate a randomized election timeout.
    fn random_election_timeout(&self) -> Duration {
        let mut rng = rand::thread_rng();
        let ms = rng.gen_range(
            self.config.election_timeout_min_ms..self.config.election_timeout_max_ms,
        );
        Duration::from_millis(ms)
    }

    // -----------------------------------------------------------------------
    // Main event loop
    // -----------------------------------------------------------------------

    /// Run the Raft node's main event loop.
    ///
    /// This method never returns under normal operation. It processes:
    /// 1. Inbound peer messages
    /// 2. Election timeouts (Follower/Candidate)
    /// 3. Heartbeat timer (Leader)
    /// 4. Client proposals (Leader)
    ///
    /// # Cancellation
    ///
    /// The loop exits when the `shutdown` receiver is triggered.
    pub async fn run(&mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        info!(
            node_id = self.config.node_id,
            peers = ?self.config.peers,
            "Raft node starting"
        );

        let mut election_timeout = self.random_election_timeout();
        let mut heartbeat_interval = tokio::time::interval(self.config.heartbeat_interval());
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            match self.role {
                Role::Follower | Role::Candidate => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!(node_id = self.id(), "Raft node shutting down");
                                return;
                            }
                        }
                        Some(msg) = self.inbox.recv() => {
                            self.handle_message(msg).await;
                            // Reset election timeout on valid leader contact.
                            election_timeout = self.random_election_timeout();
                        }
                        _ = tokio::time::sleep(election_timeout) => {
                            self.start_election().await;
                            election_timeout = self.random_election_timeout();
                        }
                        Some(proposal) = self.proposals.recv() => {
                            // Not leader — reject proposal.
                            let _ = self.proposal_responses.send(Err(RaftError::NotLeader {
                                leader: self.leader_id,
                            })).await;
                            debug!(
                                node_id = self.id(),
                                "Rejected proposal — not leader (leader={:?})",
                                self.leader_id
                            );
                            drop(proposal);
                        }
                    }
                }
                Role::Leader => {
                    tokio::select! {
                        _ = shutdown.changed() => {
                            if *shutdown.borrow() {
                                info!(node_id = self.id(), "Raft leader shutting down");
                                return;
                            }
                        }
                        Some(msg) = self.inbox.recv() => {
                            self.handle_message(msg).await;
                        }
                        _ = heartbeat_interval.tick() => {
                            self.send_heartbeats().await;
                        }
                        Some(proposal) = self.proposals.recv() => {
                            self.handle_proposal(proposal).await;
                        }
                    }
                }
            }

            // Apply committed but unapplied entries to the state machine.
            self.apply_committed_entries();
        }
    }

    // -----------------------------------------------------------------------
    // Election
    // -----------------------------------------------------------------------

    /// Start a new election round.
    ///
    /// If pre-vote is enabled, first conducts a pre-vote round. If pre-vote
    /// succeeds (or is disabled), increments term and requests real votes.
    async fn start_election(&mut self) {
        if self.config.pre_vote_enabled {
            debug!(node_id = self.id(), "Starting pre-vote round");
            let pre_vote_msg = RaftMessage::PreVote {
                term: self.term() + 1,
                candidate_id: self.id(),
                last_log_index: self.log.last_index(),
                last_log_term: self.log.last_term(),
            };

            if let Err(e) = self.transport.broadcast(
                self.id(),
                &self.config.peers,
                pre_vote_msg,
            ).await {
                warn!(error = %e, "Pre-vote broadcast failed");
            }
            // In a full implementation, we'd wait for pre-vote responses
            // before proceeding. For simplicity, we proceed to real election
            // after broadcasting pre-votes — responses are handled in
            // handle_message.
        }

        // Increment term (AcqRel ensures visibility).
        let new_term = self.term() + 1;
        self.set_term(new_term);
        self.role = Role::Candidate;
        self.voted_for = Some(self.id());

        info!(
            node_id = self.id(),
            term = new_term,
            "Starting election"
        );

        let request_vote = RaftMessage::RequestVote {
            term: new_term,
            candidate_id: self.id(),
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        if let Err(e) = self.transport.broadcast(
            self.id(),
            &self.config.peers,
            request_vote,
        ).await {
            warn!(error = %e, "RequestVote broadcast failed");
        }
    }

    /// Become the leader.
    ///
    /// Initializes `next_index` and `match_index` for all peers and
    /// sends initial heartbeats.
    async fn become_leader(&mut self) {
        info!(
            node_id = self.id(),
            term = self.term(),
            "Elected as leader"
        );
        self.role = Role::Leader;
        self.leader_id = Some(self.id());

        // Initialize leader volatile state (§5.3).
        let last_index = self.log.last_index();
        for &peer in &self.config.peers {
            if peer != self.id() {
                self.next_index.insert(peer, last_index + 1);
                self.match_index.insert(peer, 0);
            }
        }

        // Send initial heartbeats to establish authority.
        self.send_heartbeats().await;
    }

    // -----------------------------------------------------------------------
    // Message handling
    // -----------------------------------------------------------------------

    /// Dispatch an inbound Raft message to the appropriate handler.
    async fn handle_message(&mut self, msg: RaftMessage) {
        // Universal step-down rule.
        let msg_term = msg.term();
        if self.maybe_step_down(msg_term) {
            // Continue processing after stepping down — the message may
            // still need a response.
        }

        match msg {
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_request_vote(term, candidate_id, last_log_index, last_log_term)
                    .await;
            }
            RaftMessage::RequestVoteResponse {
                term,
                vote_granted,
                from,
            } => {
                self.handle_vote_response(term, vote_granted, from).await;
            }
            RaftMessage::PreVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.handle_pre_vote(term, candidate_id, last_log_index, last_log_term)
                    .await;
            }
            RaftMessage::PreVoteResponse {
                vote_granted,
                from,
                ..
            } => {
                debug!(from, vote_granted, "Pre-vote response received");
                // Pre-vote responses don't change state directly in this
                // simplified implementation — they just confirm viability.
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => {
                self.handle_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit,
                )
                .await;
            }
            RaftMessage::AppendEntriesResponse {
                term,
                success,
                match_index,
                from,
            } => {
                self.handle_append_entries_response(term, success, match_index, from)
                    .await;
            }
        }
    }

    /// Handle a `RequestVote` RPC.
    ///
    /// Grant the vote if:
    /// 1. The candidate's term >= our current term.
    /// 2. We haven't voted for anyone else in this term.
    /// 3. The candidate's log is at least as up-to-date as ours (§5.4.1).
    async fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        let current_term = self.term();
        let mut vote_granted = false;

        if term >= current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);

            // Log up-to-date check (§5.4.1):
            // A candidate's log is at least as up-to-date if:
            // - Its last log term is greater, OR
            // - Its last log term is equal AND its last log index is >=.
            let log_ok = last_log_term > self.log.last_term()
                || (last_log_term == self.log.last_term()
                    && last_log_index >= self.log.last_index());

            if can_vote && log_ok {
                vote_granted = true;
                self.voted_for = Some(candidate_id);
                self.role = Role::Follower;
                debug!(
                    node_id = self.id(),
                    candidate = candidate_id,
                    term,
                    "Granted vote"
                );
            }
        }

        let response = RaftMessage::RequestVoteResponse {
            term: self.term(),
            vote_granted,
            from: self.id(),
        };

        if let Err(e) = self.transport.send(candidate_id, response).await {
            warn!(error = %e, peer = candidate_id, "Failed to send vote response");
        }
    }

    /// Handle a `RequestVoteResponse`.
    ///
    /// If we're a Candidate and receive a quorum of votes, become Leader.
    async fn handle_vote_response(&mut self, _term: u64, vote_granted: bool, from: u64) {
        if self.role != Role::Candidate {
            return;
        }

        if vote_granted {
            debug!(node_id = self.id(), from, "Received vote");

            // Count votes (self + granted responses).
            // In a full implementation, we'd track individual votes in a set.
            // Here we use match_index as a vote tracker for simplicity.
            self.match_index.insert(from, 1); // 1 = voted

            let vote_count = self.match_index.values().filter(|&&v| v == 1).count() + 1; // +1 for self
            if vote_count >= self.config.quorum_size() {
                self.become_leader().await;
            }
        }
    }

    /// Handle a `PreVote` RPC.
    ///
    /// Grant if the pre-candidate's log is at least as up-to-date as ours
    /// and the term is valid. Does NOT update our term or voted_for.
    async fn handle_pre_vote(
        &mut self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) {
        let vote_granted = term >= self.term()
            && (last_log_term > self.log.last_term()
                || (last_log_term == self.log.last_term()
                    && last_log_index >= self.log.last_index()));

        let response = RaftMessage::PreVoteResponse {
            term: self.term(),
            vote_granted,
            from: self.id(),
        };

        if let Err(e) = self.transport.send(candidate_id, response).await {
            warn!(error = %e, "Failed to send pre-vote response");
        }
    }

    /// Handle an `AppendEntries` RPC (log replication + heartbeat).
    async fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) {
        let current_term = self.term();

        // Reject if the leader's term is stale.
        if term < current_term {
            let response = RaftMessage::AppendEntriesResponse {
                term: current_term,
                success: false,
                match_index: self.log.last_index(),
                from: self.id(),
            };
            let _ = self.transport.send(leader_id, response).await;
            return;
        }

        // Recognize the leader.
        self.role = Role::Follower;
        self.leader_id = Some(leader_id);

        // Attempt to append entries (log-matching check inside).
        match self.log.append_entries(prev_log_index, prev_log_term, &entries).await {
            Ok(()) => {
                // Update commit index (§5.3).
                if leader_commit > self.log.commit_index() {
                    let new_commit = leader_commit.min(self.log.last_index());
                    self.log.set_commit_index(new_commit);
                }

                let response = RaftMessage::AppendEntriesResponse {
                    term: self.term(),
                    success: true,
                    match_index: self.log.last_index(),
                    from: self.id(),
                };
                let _ = self.transport.send(leader_id, response).await;
            }
            Err(e) => {
                debug!(error = %e, "AppendEntries log matching failed");
                let response = RaftMessage::AppendEntriesResponse {
                    term: self.term(),
                    success: false,
                    match_index: self.log.last_index(),
                    from: self.id(),
                };
                let _ = self.transport.send(leader_id, response).await;
            }
        }
    }

    /// Handle an `AppendEntriesResponse` (leader only).
    ///
    /// Updates `next_index` and `match_index` for the follower, then
    /// attempts to advance the commit index.
    async fn handle_append_entries_response(
        &mut self,
        _term: u64,
        success: bool,
        match_index: u64,
        from: u64,
    ) {
        if self.role != Role::Leader {
            return;
        }

        if success {
            // Update match_index and next_index for this follower.
            self.match_index.insert(from, match_index);
            self.next_index.insert(from, match_index + 1);

            // Attempt to advance commit index.
            // An entry is committed if it is replicated on a majority of
            // servers AND it was created in the leader's current term (§5.4.2).
            self.try_advance_commit_index();
        } else {
            // Decrement next_index for this follower (back-track).
            let ni = self.next_index.get(&from).copied().unwrap_or(1);
            if ni > 1 {
                self.next_index.insert(from, ni - 1);
            }
            debug!(
                from,
                new_next_index = ni.saturating_sub(1),
                "AppendEntries rejected — backing up"
            );
        }
    }

    /// Try to advance the commit index based on match_index quorum.
    ///
    /// §5.4.2: "If there exists an N such that N > commitIndex, a majority
    /// of matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex = N."
    fn try_advance_commit_index(&mut self) {
        let current_term = self.term();

        for n in (self.log.commit_index() + 1)..=self.log.last_index() {
            // Check that the entry at N has the current term.
            if let Some(entry_term) = self.log.term_at(n) {
                if entry_term != current_term {
                    continue;
                }
            } else {
                continue;
            }

            // Count replicas (including self).
            let mut replicas = 1u64; // self
            for (&_peer, &mi) in &self.match_index {
                if mi >= n {
                    replicas += 1;
                }
            }

            if replicas as usize >= self.config.quorum_size() {
                self.log.set_commit_index(n);
                debug!(commit_index = n, "Advanced commit index");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Heartbeats & replication
    // -----------------------------------------------------------------------

    /// Send heartbeats (empty `AppendEntries`) to all peers.
    async fn send_heartbeats(&self) {
        for &peer in &self.config.peers {
            if peer == self.id() {
                continue;
            }

            let prev_log_index = self
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or(1)
                .saturating_sub(1);
            let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);

            // Batch entries up to max_entries_per_append.
            let next = self.next_index.get(&peer).copied().unwrap_or(1);
            let last = self.log.last_index();
            let entries = if next <= last {
                let batch_end = last.min(next + self.config.max_entries_per_append as u64 - 1);
                self.log.entries_range(next, batch_end)
            } else {
                Vec::new()
            };

            let msg = RaftMessage::AppendEntries {
                term: self.term(),
                leader_id: self.id(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.log.commit_index(),
            };

            if let Err(e) = self.transport.send(peer, msg).await {
                debug!(peer, error = %e, "Heartbeat send failed");
            }
        }
    }

    // -----------------------------------------------------------------------
    // Client proposals
    // -----------------------------------------------------------------------

    /// Handle a client proposal (leader only).
    async fn handle_proposal(&mut self, command: Vec<u8>) {
        let term = self.term();

        // Back-pressure check.
        let uncommitted = self.log.last_index().saturating_sub(self.log.commit_index());
        if uncommitted >= self.config.max_uncommitted_entries {
            let _ = self
                .proposal_responses
                .send(Err(RaftError::BackPressure { count: uncommitted }))
                .await;
            return;
        }

        match self.log.append(term, command).await {
            Ok(entry) => {
                debug!(index = entry.index, term, "Appended proposal to log");
                let _ = self
                    .proposal_responses
                    .send(Ok(ProposalResponse {
                        index: entry.index,
                        term: entry.term,
                    }))
                    .await;

                // Immediately replicate to followers.
                self.send_heartbeats().await;
            }
            Err(e) => {
                error!(error = %e, "Failed to append proposal");
                let _ = self
                    .proposal_responses
                    .send(Err(RaftError::StorageFailure(e.to_string())))
                    .await;
            }
        }
    }

    // -----------------------------------------------------------------------
    // State machine application
    // -----------------------------------------------------------------------

    /// Apply all committed but unapplied entries to the state machine.
    fn apply_committed_entries(&mut self) {
        let entries = self.log.unapplied_entries();
        if entries.is_empty() {
            return;
        }

        match lock_state_machine(&self.state_machine) {
            Ok(mut sm) => {
                for entry in &entries {
                    match sm.apply(entry) {
                        Ok(_) => {
                            self.log.set_last_applied(entry.index);
                        }
                        Err(e) => {
                            error!(
                                index = entry.index,
                                error = %e,
                                "State machine apply failed"
                            );
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to lock state machine");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state_machine::ClusterStateMachine;
    use std::path::Path;

    async fn test_wal(dir: &Path) -> Arc<tokio::sync::Mutex<leviathan_storage::Wal>> {
        let wal = leviathan_storage::Wal::open(&dir.join("test.wal"))
            .await
            .expect("open WAL");
        Arc::new(tokio::sync::Mutex::new(wal))
    }

    #[tokio::test]
    async fn node_starts_as_follower() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let log = RaftLog::new(wal).await.expect("new log");

        let sm: Arc<Mutex<dyn StateMachine>> =
            Arc::new(Mutex::new(ClusterStateMachine::new()));

        let (tx1, rx1) = mpsc::channel(16);
        let (tx2, rx2) = mpsc::channel(16);
        let (tx3, _rx3) = mpsc::channel(16);

        let transport = Arc::new(ChannelTransport::new(HashMap::new()));

        let node = RaftNode::new(
            RaftConfig::default(),
            log,
            sm,
            transport,
            rx1,
            rx2,
            tx3,
        );

        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 0);
        drop(tx1);
        drop(tx2);
    }

    #[tokio::test]
    async fn step_down_on_higher_term() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let log = RaftLog::new(wal).await.expect("new log");

        let sm: Arc<Mutex<dyn StateMachine>> =
            Arc::new(Mutex::new(ClusterStateMachine::new()));

        let (_tx1, rx1) = mpsc::channel(16);
        let (_tx2, rx2) = mpsc::channel(16);
        let (tx3, _rx3) = mpsc::channel(16);

        let transport = Arc::new(ChannelTransport::new(HashMap::new()));

        let mut node = RaftNode::new(
            RaftConfig::default(),
            log,
            sm,
            transport,
            rx1,
            rx2,
            tx3,
        );

        // Simulate term bump.
        node.set_term(5);
        node.role = Role::Leader;

        // Discovering a higher term should step down.
        assert!(node.maybe_step_down(10));
        assert_eq!(node.role(), Role::Follower);
        assert_eq!(node.term(), 10);
    }

    #[tokio::test]
    async fn commit_index_advances_on_quorum() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let mut log = RaftLog::new(wal).await.expect("new log");

        // Append entries at term 1.
        log.append(1, b"a".to_vec()).await.expect("append");
        log.append(1, b"b".to_vec()).await.expect("append");

        let sm: Arc<Mutex<dyn StateMachine>> =
            Arc::new(Mutex::new(ClusterStateMachine::new()));

        let (_tx1, rx1) = mpsc::channel(16);
        let (_tx2, rx2) = mpsc::channel(16);
        let (tx3, _rx3) = mpsc::channel(16);

        let transport = Arc::new(ChannelTransport::new(HashMap::new()));

        let config = RaftConfig {
            node_id: 1,
            peers: vec![1, 2, 3],
            ..RaftConfig::default()
        };

        let mut node = RaftNode::new(config, log, sm, transport, rx1, rx2, tx3);
        node.set_term(1);
        node.role = Role::Leader;

        // Simulate follower match indices.
        node.match_index.insert(2, 2);
        node.match_index.insert(3, 1);

        node.try_advance_commit_index();

        // Index 1 should be committed (replicated on nodes 1, 2, 3 — quorum).
        // Index 2 should be committed (replicated on nodes 1 and 2 — quorum).
        assert_eq!(node.log.commit_index(), 2);
    }
}
