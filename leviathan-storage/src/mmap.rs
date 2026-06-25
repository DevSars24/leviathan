//! Memory-mapped file storage for the Leviathan storage engine.
//!
//! # Design
//!
//! [`MmapStore`] maps a fixed-size region of a file into the process's virtual
//! address space using [`memmap2::MmapMut`]. Once mapped, byte-level reads and
//! writes go through virtual memory — no explicit `read`/`write` syscalls are
//! needed per access, making it suitable for high-throughput, low-latency
//! use-cases such as a page cache or a ring buffer.
//!
//! # Safety Contract
//!
//! `memmap2` operations are inherently unsafe: the mapped memory could be
//! invalidated if the underlying file is modified by another process or if the
//! file is truncated. Within Leviathan we uphold the following invariants:
//!
//! 1. The file is exclusively owned by this [`MmapStore`] instance for its
//!    entire lifetime. No other process or thread modifies the file directly.
//! 2. The file length is set to `size` before mapping, so the mapped region
//!    is always within the file bounds.
//! 3. The mapping is dropped before any subsequent truncation or deletion of
//!    the underlying file.
//!
//! # Persistence
//!
//! [`MmapStore::flush`] calls `msync` to push dirty pages back to the
//! backing file. [`Drop`] attempts a best-effort flush so that data written
//! since the last explicit [`flush`](MmapStore::flush) is not silently lost.

use std::fs::OpenOptions;
use std::path::Path;

use memmap2::MmapMut;

use crate::error::{Result, StorageError};

/// A memory-mapped, fixed-size byte store backed by a file on disk.
///
/// The file is created if it does not exist, then extended to `size` bytes
/// if smaller, and mapped into memory for the lifetime of this struct.
///
/// Reads and writes are bounds-checked; any out-of-range access returns
/// [`StorageError::OutOfBounds`] rather than panicking.
pub struct MmapStore {
    /// The live mutable memory mapping.
    mmap: MmapMut,
    /// The total number of bytes in the mapped region.
    size: usize,
}

impl MmapStore {
    /// Open or create a memory-mapped file at `path` with the given `size`.
    ///
    /// If the file already exists but is smaller than `size`, it is extended
    /// with zero bytes. If it is larger, the mapping covers only the first
    /// `size` bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the file cannot be opened, extended,
    /// or mapped.
    pub fn open(path: &Path, size: usize) -> Result<Self> {
        // Create parent directories if they don't exist.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        // Ensure the file is at least `size` bytes so the mapping is valid.
        let metadata = file.metadata()?;
        if metadata.len() < size as u64 {
            file.set_len(size as u64)?;
        }

        // SAFETY: We have exclusive ownership of this file. No other process
        // or thread holds a reference to the same file region. The file has
        // been extended to at least `size` bytes above, so the mapping cannot
        // extend past the end of the file. We uphold these invariants for the
        // entire lifetime of the `MmapStore`.
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(Self { mmap, size })
    }

    /// Write `data` into the mapped region starting at `offset`.
    ///
    /// The entire write must fit within `[offset, offset + data.len())`.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OutOfBounds`] if `offset + data.len() > self.size`.
    pub fn write(&mut self, offset: usize, data: &[u8]) -> Result<()> {
        let end = offset.checked_add(data.len()).ok_or(StorageError::OutOfBounds {
            offset,
            len: data.len(),
            capacity: self.size,
        })?;

        if end > self.size {
            return Err(StorageError::OutOfBounds {
                offset,
                len: data.len(),
                capacity: self.size,
            });
        }

        self.mmap[offset..end].copy_from_slice(data);
        Ok(())
    }

    /// Read `len` bytes from the mapped region starting at `offset`.
    ///
    /// Returns a slice into the mapped memory — no copying occurs.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::OutOfBounds`] if `offset + len > self.size`.
    pub fn read(&self, offset: usize, len: usize) -> Result<&[u8]> {
        let end = offset.checked_add(len).ok_or(StorageError::OutOfBounds {
            offset,
            len,
            capacity: self.size,
        })?;

        if end > self.size {
            return Err(StorageError::OutOfBounds {
                offset,
                len,
                capacity: self.size,
            });
        }

        Ok(&self.mmap[offset..end])
    }

    /// Flush dirty pages back to the underlying file via `msync`.
    ///
    /// This is the memory-mapped equivalent of `fsync`. Call it after a
    /// batch of [`MmapStore::write`] operations that must be durable.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::Io`] if the `msync` call fails.
    pub fn flush(&self) -> Result<()> {
        self.mmap.flush()?;
        tracing::debug!("MmapStore: flushed dirty pages to disk");
        Ok(())
    }

    /// Return the total number of bytes in the mapped region.
    pub fn size(&self) -> usize {
        self.size
    }
}

impl Drop for MmapStore {
    /// Attempt a best-effort `msync` before the mapping is released.
    ///
    /// Any error from the flush is logged and silently swallowed because
    /// `drop` cannot return a `Result`. Callers that need guaranteed
    /// durability should call [`MmapStore::flush`] explicitly before
    /// dropping.
    fn drop(&mut self) {
        // SAFETY: `self.mmap` is still valid here — we are in `drop`, and
        // Rust guarantees that `drop` runs exactly once before the value's
        // memory is freed. The underlying file descriptor remains open until
        // the end of this function because `MmapMut` holds its own `Arc`
        // reference to the file.
        if let Err(e) = self.mmap.flush() {
            tracing::warn!(
                error = %e,
                "MmapStore: best-effort flush on drop failed — data may not be persisted"
            );
        }
    }
}
