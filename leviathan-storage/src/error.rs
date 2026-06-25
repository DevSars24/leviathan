//! Unified error type for all storage operations in the Leviathan platform.
//!
//! All public APIs in this crate return `Result<T, StorageError>`. Callers
//! that need to bridge into [`leviathan_core::LeviathanError`] can do so via
//! the `From` implementation on the core error type.

use thiserror::Error;

/// All errors that can be produced by [`crate::wal::Wal`] or
/// [`crate::mmap::MmapStore`].
///
/// Every variant carries enough information to diagnose the failure without
/// requiring a full backtrace in production.
#[derive(Debug, Error)]
pub enum StorageError {
    /// An I/O error raised by the operating system or async runtime.
    ///
    /// Covers file open/create failures, read/write errors, sync failures,
    /// and file truncation errors.
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A bincode serialization or deserialization failure.
    ///
    /// Returned when an entry's bytes cannot be encoded before writing, or
    /// when a frame's bytes cannot be decoded after reading.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// A WAL entry at the given `index` is structurally corrupt.
    ///
    /// This typically means a partial write occurred (e.g., process crashed
    /// mid-write). The WAL open routine detects these and discards them;
    /// this variant is surfaced when an explicit read encounters corruption.
    #[error("corrupt WAL entry at index {index}: {reason}")]
    CorruptEntry {
        /// The WAL index at which corruption was detected.
        index: u64,
        /// Human-readable description of the corruption.
        reason: String,
    },

    /// A memory-mapped access was out of bounds.
    ///
    /// Returned when `offset + len` exceeds the mapped region's `capacity`.
    #[error(
        "mmap access out of bounds: offset={offset}, len={len}, capacity={capacity}"
    )]
    OutOfBounds {
        /// The starting byte offset of the attempted access.
        offset: usize,
        /// The number of bytes requested.
        len: usize,
        /// The total size of the mapped region.
        capacity: usize,
    },

    /// A [`crate::wal::Wal::truncate_from`] call referenced a WAL index that
    /// does not exist in the current log.
    ///
    /// Truncating past the end of the log, or truncating an empty log, both
    /// produce this error.
    #[error("invalid WAL truncation index: {0}")]
    InvalidIndex(u64),
}

impl From<Box<bincode::ErrorKind>> for StorageError {
    fn from(e: Box<bincode::ErrorKind>) -> Self {
        StorageError::Serialization(e.to_string())
    }
}

/// Convenience alias so callers inside this crate can write `Result<T>`.
pub type Result<T> = std::result::Result<T, StorageError>;
