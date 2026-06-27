//! Raft log — the core replicated data structure.
//!
//! [`RaftLog`] wraps the durable WAL with Raft-specific operations:
//! - Indexed append with term tracking
//! - Log matching (prev_log_index / prev_log_term consistency check)
//! - Batch append with conflict detection and truncation
//! - Commit index management
//!
//! # Persistence Model
//!
//! Every append is immediately forwarded to the WAL. The caller (typically
//! [`RaftNode`](crate::node::RaftNode)) decides when to call `flush()` to
//! issue the `fsync` — typically after a batch of entries but before
//! responding to the leader.

use std::sync::Arc;

use tokio::sync::Mutex;
use tracing::warn;

use leviathan_storage::Wal;

use crate::error::{RaftError, Result};
use crate::message::LogEntry;

/// The Raft replicated log, backed by a durable WAL.
///
/// Maintains an in-memory index of entries for fast lookup and a commit
/// index for state machine application. The WAL provides crash recovery.
pub struct RaftLog {
    /// In-memory copy of the log entries for fast random access.
    /// The WAL on disk is the source of truth; this vec is rebuilt from the
    /// WAL on startup.
    entries: Vec<LogEntry>,

    /// Shared WAL handle. Protected by an async mutex because WAL I/O is
    /// async. The same WAL instance may be shared with other subsystems
    /// (e.g., snapshotting) via `Arc<Mutex<_>>`.
    wal: Arc<Mutex<Wal>>,

    /// The highest log index known to be committed (replicated to a quorum).
    /// State machine entries up to this index are safe to apply.
    commit_index: u64,

    /// The highest log index applied to the state machine.
    last_applied: u64,
}

impl RaftLog {
    /// Create a new `RaftLog`, recovering any existing entries from the WAL.
    ///
    /// # Errors
    ///
    /// Returns `RaftError::StorageFailure` if the WAL cannot be read.
    pub async fn new(wal: Arc<Mutex<Wal>>) -> Result<Self> {
        let entries = {
            let wal_guard = wal.lock().await;
            let wal_entries = wal_guard.read_all().await?;
            wal_entries.into_iter().map(LogEntry::from).collect()
        };

        Ok(Self {
            entries,
            wal,
            commit_index: 0,
            last_applied: 0,
        })
    }

    /// Return the index of the last entry in the log, or 0 if empty.
    #[must_use]
    pub fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }

    /// Return the term of the last entry in the log, or 0 if empty.
    #[must_use]
    pub fn last_term(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.term)
    }

    /// Return the term of the entry at `index`, or `None` if out of range.
    #[must_use]
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entry_at(index).map(|e| e.term)
    }

    /// Return a reference to the entry at `index`, or `None` if out of range.
    #[must_use]
    pub fn entry_at(&self, index: u64) -> Option<&LogEntry> {
        if index == 0 || self.entries.is_empty() {
            return None;
        }
        let first_index = self.entries[0].index;
        if index < first_index {
            return None;
        }
        let offset = (index - first_index) as usize;
        self.entries.get(offset)
    }

    /// Return entries in the range `[from_index, to_index]` (inclusive).
    /// Clamps to available entries.
    #[must_use]
    pub fn entries_range(&self, from_index: u64, to_index: u64) -> Vec<LogEntry> {
        if self.entries.is_empty() || from_index > to_index {
            return Vec::new();
        }
        let first = self.entries[0].index;
        let start = if from_index >= first {
            (from_index - first) as usize
        } else {
            0
        };
        let end = if to_index >= first {
            ((to_index - first) as usize).min(self.entries.len().saturating_sub(1))
        } else {
            return Vec::new();
        };
        self.entries[start..=end].to_vec()
    }

    /// Append a single entry to the log and persist it to the WAL.
    ///
    /// The entry's index is set to `last_index + 1`.
    ///
    /// # Errors
    ///
    /// Returns `RaftError::StorageFailure` on WAL write failure.
    pub async fn append(&mut self, term: u64, command: Vec<u8>) -> Result<LogEntry> {
        let index = self.last_index() + 1;
        let entry = LogEntry::new(index, term, command);

        // Persist to WAL first — if this fails, the entry is not committed.
        {
            let mut wal = self.wal.lock().await;
            wal.append(entry.clone().into()).await?;
        }

        self.entries.push(entry.clone());
        Ok(entry)
    }

    /// Append entries received from a leader's `AppendEntries` RPC.
    ///
    /// Implements the Raft log-matching invariant (§5.3):
    /// 1. Check that our log contains an entry at `prev_log_index` with
    ///    term `prev_log_term`.
    /// 2. If a conflict is found (different term at the same index),
    ///    truncate from the conflict point.
    /// 3. Append any new entries not already present.
    ///
    /// # Errors
    ///
    /// Returns `RaftError::LogDivergence` if `prev_log_index`/`prev_log_term`
    /// do not match, or `RaftError::StorageFailure` on WAL errors.
    pub async fn append_entries(
        &mut self,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: &[LogEntry],
    ) -> Result<()> {
        // Step 1: Verify the log-matching invariant.
        if prev_log_index > 0 {
            match self.term_at(prev_log_index) {
                Some(term) if term == prev_log_term => { /* match — proceed */ }
                Some(found_term) => {
                    return Err(RaftError::LogDivergence {
                        index: prev_log_index,
                        expected_term: prev_log_term,
                        found_term: Some(found_term),
                    });
                }
                None => {
                    return Err(RaftError::LogDivergence {
                        index: prev_log_index,
                        expected_term: prev_log_term,
                        found_term: None,
                    });
                }
            }
        }

        // Step 2: Detect conflicts and truncate if needed.
        for entry in entries {
            if let Some(existing_term) = self.term_at(entry.index) {
                if existing_term != entry.term {
                    // Conflict — truncate from this point onward.
                    warn!(
                        conflict_index = entry.index,
                        existing_term,
                        new_term = entry.term,
                        "Log conflict detected — truncating"
                    );
                    self.truncate_from(entry.index).await?;
                    break;
                }
                // Same term at same index — already present, skip.
                continue;
            }
            // Entry is beyond our log — will be appended below.
            break;
        }

        // Step 3: Append entries that are not already in our log.
        for entry in entries {
            if entry.index > self.last_index() {
                let mut wal = self.wal.lock().await;
                wal.append(entry.clone().into()).await?;
                drop(wal);
                self.entries.push(entry.clone());
            }
        }

        Ok(())
    }

    /// Truncate all entries with index >= `from_index`.
    ///
    /// # Errors
    ///
    /// Returns `RaftError::StorageFailure` on WAL truncation failure.
    pub async fn truncate_from(&mut self, from_index: u64) -> Result<()> {
        // Truncate in-memory.
        self.entries.retain(|e| e.index < from_index);

        // Truncate on disk.
        let mut wal = self.wal.lock().await;
        // WAL truncate_from may return InvalidIndex if from_index is past
        // the end — this is benign for our use case.
        match wal.truncate_from(from_index).await {
            Ok(()) => Ok(()),
            Err(e) => {
                // If the WAL has no entries at that index, it's fine.
                let err_str = e.to_string();
                if err_str.contains("invalid WAL truncation index") {
                    Ok(())
                } else {
                    Err(RaftError::StorageFailure(err_str))
                }
            }
        }
    }

    /// Flush the WAL to disk (`fsync`).
    ///
    /// # Errors
    ///
    /// Returns `RaftError::StorageFailure` on flush failure.
    pub async fn flush(&self) -> Result<()> {
        let mut wal = self.wal.lock().await;
        wal.flush().await?;
        Ok(())
    }

    /// Return the current commit index.
    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    /// Set the commit index. Caller must ensure this only advances.
    pub fn set_commit_index(&mut self, index: u64) {
        if index > self.commit_index {
            self.commit_index = index;
        }
    }

    /// Return the last applied index.
    #[must_use]
    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    /// Set the last applied index.
    pub fn set_last_applied(&mut self, index: u64) {
        self.last_applied = index;
    }

    /// Return entries that are committed but not yet applied.
    #[must_use]
    pub fn unapplied_entries(&self) -> Vec<LogEntry> {
        if self.last_applied >= self.commit_index {
            return Vec::new();
        }
        self.entries_range(self.last_applied + 1, self.commit_index)
    }

    /// Return the number of entries in the log.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Return `true` if the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    async fn test_wal(dir: &Path) -> Arc<Mutex<Wal>> {
        let wal = Wal::open(&dir.join("test.wal"))
            .await
            .expect("open WAL");
        Arc::new(Mutex::new(wal))
    }

    #[tokio::test]
    async fn empty_log_has_zero_indices() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let log = RaftLog::new(wal).await.expect("new log");
        assert_eq!(log.last_index(), 0);
        assert_eq!(log.last_term(), 0);
        assert!(log.is_empty());
    }

    #[tokio::test]
    async fn append_and_read_back() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let mut log = RaftLog::new(wal).await.expect("new log");

        let e1 = log.append(1, b"cmd-1".to_vec()).await.expect("append");
        let e2 = log.append(1, b"cmd-2".to_vec()).await.expect("append");

        assert_eq!(e1.index, 1);
        assert_eq!(e2.index, 2);
        assert_eq!(log.last_index(), 2);
        assert_eq!(log.len(), 2);
    }

    #[tokio::test]
    async fn log_matching_check() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let mut log = RaftLog::new(wal).await.expect("new log");

        log.append(1, b"a".to_vec()).await.expect("append");

        // Correct prev_log matches
        let entries = vec![LogEntry::new(2, 1, b"b".to_vec())];
        log.append_entries(1, 1, &entries).await.expect("append_entries");

        // Incorrect prev_log_term should fail
        let entries = vec![LogEntry::new(3, 2, b"c".to_vec())];
        let err = log.append_entries(2, 99, &entries).await;
        assert!(matches!(err, Err(RaftError::LogDivergence { .. })));
    }

    #[tokio::test]
    async fn commit_and_apply_tracking() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let wal = test_wal(tmp.path()).await;
        let mut log = RaftLog::new(wal).await.expect("new log");

        log.append(1, b"x".to_vec()).await.expect("append");
        log.append(1, b"y".to_vec()).await.expect("append");

        log.set_commit_index(2);
        assert_eq!(log.commit_index(), 2);

        let unapplied = log.unapplied_entries();
        assert_eq!(unapplied.len(), 2);

        log.set_last_applied(2);
        assert!(log.unapplied_entries().is_empty());
    }
}
