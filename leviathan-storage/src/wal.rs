//! Write-Ahead Log (WAL) implementation for the Leviathan storage engine.
//!
//! # Design
//!
//! The WAL is a sequential, append-only file of length-prefixed binary frames.
//! Each frame on disk looks like this:
//!
//! ```text
//! ┌─────────────────────────────┬────────────────────────────────┐
//! │  frame_length : u64 (8 B LE)│  bincode(WalEntry) : N bytes   │
//! └─────────────────────────────┴────────────────────────────────┘
//! ```
//!
//! The 8-byte little-endian length prefix lets the reader know exactly how
//! many bytes to pull for each entry, enabling reliable partial-write
//! detection on crash recovery.
//!
//! # Crash Recovery
//!
//! On [`Wal::open`], the WAL scans the entire file from the beginning. Any
//! frame whose stated length cannot be fully satisfied by the remaining bytes
//! in the file is treated as a partial (torn) write and discarded, along with
//! everything after it. This matches Raft's requirement: only complete,
//! fsync-confirmed entries are considered durable.
//!
//! # Truncation
//!
//! [`Wal::truncate_from`] supports Raft log repair: given a WAL `index`, all
//! entries with `entry.index >= index` are removed from the file. This is
//! implemented by replaying the file to find the exact byte offset of the
//! first frame that must be cut, then calling `set_len` on the underlying
//! file.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::error::{Result, StorageError};

// ---------------------------------------------------------------------------
// WalEntry
// ---------------------------------------------------------------------------

/// A single entry in the Write-Ahead Log.
///
/// Entries are written sequentially and identified by a monotonically
/// increasing `index`. The `term` field is the Raft consensus term in which
/// the entry was produced; it is used during leader election and log repair.
///
/// `data` is an opaque byte payload — the WAL does not interpret its contents.
/// Upper layers (e.g., the state machine) are responsible for encoding and
/// decoding the payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalEntry {
    /// Monotonically increasing log index. Must be unique within the WAL.
    pub index: u64,
    /// The Raft consensus term in which this entry was produced.
    pub term: u64,
    /// Opaque byte payload. Interpretation is left to the caller.
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Wal
// ---------------------------------------------------------------------------

/// A durable Write-Ahead Log backed by an OS file.
///
/// All writes are sequentially appended in length-prefixed binary frames.
/// The file is kept open for the lifetime of the `Wal`; callers must call
/// [`Wal::flush`] to guarantee durability via `fsync`.
///
/// # Thread Safety
///
/// `Wal` is `Send` but not `Sync`. It is designed to be owned by a single
/// actor or task. If shared access is required, wrap it in a
/// `tokio::sync::Mutex`.
pub struct Wal {
    /// The open file handle used for all write operations.
    file: tokio::fs::File,
    /// The path to the WAL file, retained for truncation (which requires
    /// reopening the file to set its length).
    path: PathBuf,
    /// The next index that will be assigned to an appended entry.
    next_index: u64,
}

impl Wal {
    /// Open an existing WAL or create a new one at `path`.
    ///
    /// On open, the file is replayed from the beginning. Any partial (torn)
    /// write at the end of the file is detected and silently truncated so
    /// that only complete, consistent entries survive a crash.
    ///
    /// The returned `Wal` is positioned at the end of the file and ready for
    /// further appends.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the file cannot be opened or created,
    /// and [`StorageError::Serialization`] if a complete frame cannot be
    /// deserialized.
    pub async fn open(path: &Path) -> Result<Self> {
        // Create parent directories if they don't exist yet.
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Open in read+write+create mode so we can both replay and append.
        let mut file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .await?;

        // --- Crash-recovery replay pass ---
        //
        // We read from the start of the file, collecting complete entries.
        // The byte offset of the first partial frame is recorded; if we find
        // one we truncate the file there after the loop.
        file.seek(SeekFrom::Start(0)).await?;

        let mut valid_byte_end: u64 = 0;
        let mut next_index: u64 = 0;

        loop {
            // Attempt to read the 8-byte length prefix.
            let mut len_buf = [0u8; 8];
            match file.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // We hit the end of the file cleanly between frames.
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }

            let frame_len = u64::from_le_bytes(len_buf) as usize;

            // Sanity check: refuse absurdly large frames (> 256 MiB) to
            // avoid allocating gigabytes on a corrupted length prefix.
            if frame_len > 256 * 1024 * 1024 {
                // Treat as a torn write — discard everything from here on.
                break;
            }

            // Attempt to read exactly `frame_len` bytes of payload.
            let mut payload = vec![0u8; frame_len];
            match file.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Partial payload — this frame is torn. Discard.
                    break;
                }
                Err(e) => return Err(StorageError::Io(e)),
            }

            // Deserialize to validate structural integrity.
            let entry: WalEntry = bincode::deserialize(&payload)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            // Advance our high-water marks.
            next_index = entry.index + 1;
            // 8 bytes for the prefix + `frame_len` bytes for the payload.
            valid_byte_end += 8 + frame_len as u64;
        }

        // If a partial write was detected, truncate the file to discard it.
        let actual_len = file.seek(SeekFrom::End(0)).await?;
        if actual_len != valid_byte_end {
            tracing::warn!(
                path = %path.display(),
                discarded_bytes = actual_len - valid_byte_end,
                "WAL: detected partial write at end of file — truncating to last complete entry"
            );
            file.set_len(valid_byte_end).await?;
        }

        // Seek to end so all subsequent writes are appended.
        file.seek(SeekFrom::End(0)).await?;

        tracing::info!(
            path = %path.display(),
            recovered_entries = next_index,
            "WAL opened — recovery complete"
        );

        Ok(Self {
            file,
            path: path.to_path_buf(),
            next_index,
        })
    }

    /// Append a single [`WalEntry`] to the log.
    ///
    /// The entry is serialized with `bincode`, then written as a
    /// length-prefixed frame in a single `write_all` call. The write is
    /// buffered by the OS; call [`Wal::flush`] to guarantee durability.
    ///
    /// The `entry.index` field is overwritten with the WAL's internal
    /// monotonic counter to guarantee strict ordering regardless of the
    /// value supplied by the caller.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Serialization`] if `bincode` cannot encode
    /// the entry, or [`StorageError::Io`] if the write fails.
    pub async fn append(&mut self, mut entry: WalEntry) -> Result<()> {
        // Enforce monotonic indexing — the WAL is the source of truth for
        // log indices, not the caller.
        entry.index = self.next_index;

        let payload = bincode::serialize(&entry)
            .map_err(|e| StorageError::Serialization(e.to_string()))?;

        let frame_len = payload.len() as u64;

        // Build the frame: 8-byte LE length prefix followed by the payload.
        // We allocate a single contiguous buffer to keep the write atomic at
        // the application level (one write_all syscall).
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&frame_len.to_le_bytes());
        frame.extend_from_slice(&payload);

        self.file.write_all(&frame).await?;

        tracing::debug!(
            index = entry.index,
            term = entry.term,
            payload_bytes = payload.len(),
            "WAL: appended entry"
        );

        self.next_index += 1;
        Ok(())
    }

    /// Read and deserialize every complete [`WalEntry`] from disk.
    ///
    /// This method opens a fresh read handle to the WAL file (independent of
    /// the write handle held by `self`) and replays from the beginning.
    /// Partial frames at the tail are silently ignored, matching the behaviour
    /// of [`Wal::open`].
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] on file read failures and
    /// [`StorageError::Serialization`] if a structurally complete frame
    /// contains an invalid bincode payload.
    pub async fn read_all(&self) -> Result<Vec<WalEntry>> {
        // Open a separate read-only handle so we don't interfere with the
        // write position of `self.file`.
        let mut reader = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;

        let mut entries = Vec::new();

        loop {
            let mut len_buf = [0u8; 8];
            match reader.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }

            let frame_len = u64::from_le_bytes(len_buf) as usize;

            // Guard against corrupt length prefixes.
            if frame_len > 256 * 1024 * 1024 {
                break;
            }

            let mut payload = vec![0u8; frame_len];
            match reader.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }

            let entry: WalEntry = bincode::deserialize(&payload)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            entries.push(entry);
        }

        Ok(entries)
    }

    /// Remove all WAL entries with `entry.index >= index`.
    ///
    /// This implements Raft log repair: when a follower receives a conflicting
    /// entry from a new leader, it truncates its local log from the conflict
    /// point and then accepts the leader's entries.
    ///
    /// The implementation replays the file to find the byte offset of the
    /// first entry that must be cut, then issues `set_len` on the underlying
    /// file. The write handle is seeked back to the new end of file so
    /// subsequent appends are correctly positioned.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::InvalidIndex`] if `index` is greater than the
    /// highest index in the log (nothing to truncate). Returns
    /// [`StorageError::Io`] on file errors.
    pub async fn truncate_from(&mut self, index: u64) -> Result<()> {
        // We need a read pass to locate the byte boundary. Use the dedicated
        // reader so we don't disrupt the write position.
        let mut reader = tokio::fs::OpenOptions::new()
            .read(true)
            .open(&self.path)
            .await?;

        let mut current_offset: u64 = 0;
        let mut found = false;
        let mut truncate_at: u64 = 0;

        loop {
            let mut len_buf = [0u8; 8];
            match reader.read_exact(&mut len_buf).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }

            let frame_len = u64::from_le_bytes(len_buf) as usize;

            if frame_len > 256 * 1024 * 1024 {
                break;
            }

            let mut payload = vec![0u8; frame_len];
            match reader.read_exact(&mut payload).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(StorageError::Io(e)),
            }

            let entry: WalEntry = bincode::deserialize(&payload)
                .map_err(|e| StorageError::Serialization(e.to_string()))?;

            if entry.index >= index {
                // This is the first entry to be removed — record its offset.
                truncate_at = current_offset;
                found = true;
                break;
            }

            current_offset += 8 + frame_len as u64;
        }

        if !found {
            return Err(StorageError::InvalidIndex(index));
        }

        tracing::info!(
            truncate_from_index = index,
            truncate_at_byte = truncate_at,
            "WAL: truncating log"
        );

        // Truncate the file at the boundary byte.
        self.file.set_len(truncate_at).await?;

        // Reposition the write handle at the new end of file.
        self.file.seek(SeekFrom::End(0)).await?;

        // Update the monotonic counter so future appends have correct indices.
        self.next_index = index;

        Ok(())
    }

    /// Flush all buffered writes to disk via `fsync`.
    ///
    /// `write_all` guarantees the data is handed to the OS kernel but does
    /// **not** guarantee persistence across a power failure. This method calls
    /// `sync_all` (Linux: `fsync`, macOS: `fcntl(F_FULLFSYNC)`) to ensure
    /// durability.
    ///
    /// Call this after each batch of [`Wal::append`] calls that must be
    /// durable before proceeding.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the underlying `sync_all` call fails.
    pub async fn flush(&mut self) -> Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;
        tracing::debug!("WAL: flushed and synced to disk");
        Ok(())
    }

    /// Return the next index that will be assigned on the next [`Wal::append`].
    ///
    /// This is useful for callers that need to predict the index of an entry
    /// before it is written (e.g., for Raft log matching).
    pub fn next_index(&self) -> u64 {
        self.next_index
    }
}

// ---------------------------------------------------------------------------
// Storage trait implementation
// ---------------------------------------------------------------------------

/// Bridge [`StorageError`] into [`leviathan_core::LeviathanError`].
///
/// This conversion is used exclusively by the `Storage` trait impl below so
/// the trait's error type (`LeviathanError`) stays consistent with the rest of
/// the platform while the concrete WAL implementation uses its own richer
/// `StorageError`.
impl From<crate::error::StorageError> for leviathan_core::LeviathanError {
    fn from(e: crate::error::StorageError) -> Self {
        match e {
            crate::error::StorageError::Io(io) => leviathan_core::LeviathanError::Io(io),
            crate::error::StorageError::Serialization(msg) => {
                leviathan_core::LeviathanError::Serialization(msg)
            }
            crate::error::StorageError::CorruptEntry { index, reason } => {
                leviathan_core::LeviathanError::Consensus(format!(
                    "corrupt WAL entry at index {index}: {reason}"
                ))
            }
            crate::error::StorageError::OutOfBounds {
                offset,
                len,
                capacity,
            } => leviathan_core::LeviathanError::Internal(format!(
                "mmap out of bounds: offset={offset} len={len} capacity={capacity}"
            )),
            crate::error::StorageError::InvalidIndex(idx) => {
                leviathan_core::LeviathanError::Consensus(format!(
                    "invalid WAL truncation index: {idx}"
                ))
            }
        }
    }
}

#[async_trait::async_trait]
impl leviathan_core::Storage<WalEntry> for Wal {
    /// Append a [`WalEntry`] to the WAL, bridging errors into
    /// [`leviathan_core::LeviathanError`].
    async fn append(
        &mut self,
        entry: WalEntry,
    ) -> std::result::Result<(), leviathan_core::LeviathanError> {
        Wal::append(self, entry).await.map_err(Into::into)
    }

    /// Read all complete entries from the WAL.
    async fn read_all(
        &self,
    ) -> std::result::Result<Vec<WalEntry>, leviathan_core::LeviathanError> {
        Wal::read_all(self).await.map_err(Into::into)
    }

    /// Truncate the WAL from the given index onward.
    async fn truncate_from(
        &mut self,
        index: u64,
    ) -> std::result::Result<(), leviathan_core::LeviathanError> {
        Wal::truncate_from(self, index).await.map_err(Into::into)
    }

    /// Flush and fsync the WAL.
    async fn flush(
        &mut self,
    ) -> std::result::Result<(), leviathan_core::LeviathanError> {
        Wal::flush(self).await.map_err(Into::into)
    }
}
