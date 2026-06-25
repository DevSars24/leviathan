//! Integration tests for `leviathan-storage`.
//!
//! All tests use a [`tempfile::TempDir`] so they run in isolation and leave no
//! artefacts on the filesystem. Each test is `#[tokio::test]` (async) except
//! the pure-mmap tests which are synchronous.

use std::io::Write;

use tempfile::TempDir;

use crate::mmap::MmapStore;
use crate::wal::{Wal, WalEntry};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a [`WalEntry`] with a text payload for convenience in tests.
fn make_entry(index: u64, term: u64, payload: &str) -> WalEntry {
    WalEntry {
        index,
        term,
        data: payload.as_bytes().to_vec(),
    }
}

// ---------------------------------------------------------------------------
// WAL tests
// ---------------------------------------------------------------------------

/// Write three entries and read them all back, asserting exact equality.
///
/// This is the baseline smoke test: if append + read_all doesn't round-trip,
/// nothing else will work.
#[tokio::test]
async fn wal_append_and_read_all() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let wal_path = dir.path().join("test.wal");

    let mut wal = Wal::open(&wal_path)
        .await
        .expect("WAL open should succeed on a fresh file");

    let entries = vec![
        make_entry(0, 1, "first entry"),
        make_entry(0, 1, "second entry"),
        make_entry(0, 1, "third entry"),
    ];

    for e in &entries {
        wal.append(e.clone())
            .await
            .expect("append should not fail");
    }

    wal.flush().await.expect("flush should not fail");

    let recovered = wal.read_all().await.expect("read_all should not fail");

    // The WAL reassigns indices monotonically (0, 1, 2) regardless of the
    // index supplied by the caller, so we fix up our expected values.
    assert_eq!(recovered.len(), 3, "expected exactly 3 entries");
    assert_eq!(recovered[0].index, 0);
    assert_eq!(recovered[1].index, 1);
    assert_eq!(recovered[2].index, 2);
    assert_eq!(recovered[0].data, entries[0].data);
    assert_eq!(recovered[1].data, entries[1].data);
    assert_eq!(recovered[2].data, entries[2].data);
}

/// Simulate a crash by writing two complete entries followed by raw garbage
/// bytes at the end of the WAL file. On reopening, the WAL must discard the
/// garbage and return only the two complete entries.
///
/// This verifies the partial-write detection / crash-recovery code path.
#[tokio::test]
async fn wal_crash_recovery() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let wal_path = dir.path().join("crash.wal");

    // Phase 1: write two complete entries and flush.
    {
        let mut wal = Wal::open(&wal_path)
            .await
            .expect("WAL open should succeed");

        wal.append(make_entry(0, 1, "entry one"))
            .await
            .expect("append entry one");
        wal.append(make_entry(0, 1, "entry two"))
            .await
            .expect("append entry two");
        wal.flush().await.expect("flush");
    }

    // Phase 2: append garbage bytes directly to the file to simulate a torn
    // write (process killed after writing the length prefix but before the
    // full payload).
    {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .expect("open WAL for corruption");

        // Write a plausible 8-byte length prefix (claims 64 bytes follow)…
        let fake_len: u64 = 64;
        file.write_all(&fake_len.to_le_bytes())
            .expect("write fake length prefix");
        // …but only write 10 bytes of payload — simulating an incomplete write.
        file.write_all(b"truncated!").expect("write partial payload");
        file.flush().expect("flush corruption");
    }

    // Phase 3: reopen the WAL — it must recover gracefully.
    {
        let wal = Wal::open(&wal_path)
            .await
            .expect("WAL reopen after crash should succeed");

        let entries = wal.read_all().await.expect("read_all after recovery");

        assert_eq!(
            entries.len(),
            2,
            "only the two complete entries should survive crash recovery"
        );
        assert_eq!(entries[0].data, b"entry one");
        assert_eq!(entries[1].data, b"entry two");
    }
}

/// Append five entries, truncate from index 3 (removing entries 3 and 4),
/// then read back and verify only the first three entries (indices 0–2) remain.
///
/// This tests the Raft log-repair code path used during leader election.
#[tokio::test]
async fn wal_truncate_from() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let wal_path = dir.path().join("truncate.wal");

    let mut wal = Wal::open(&wal_path)
        .await
        .expect("WAL open should succeed");

    for i in 0u64..5 {
        wal.append(make_entry(0, 1, &format!("entry {i}")))
            .await
            .unwrap_or_else(|e| panic!("append entry {i} failed: {e}"));
    }
    wal.flush().await.expect("flush before truncation");

    // Truncate: remove all entries with index >= 3, keeping entries 0, 1, 2.
    wal.truncate_from(3).await.expect("truncate_from should succeed");

    let remaining = wal.read_all().await.expect("read_all after truncation");

    assert_eq!(
        remaining.len(),
        3,
        "only entries 0, 1, 2 should remain after truncate_from(3)"
    );
    assert_eq!(remaining[0].index, 0);
    assert_eq!(remaining[1].index, 1);
    assert_eq!(remaining[2].index, 2);

    // The next append should get index 3 (the WAL counter was reset).
    assert_eq!(wal.next_index(), 3);
}

// ---------------------------------------------------------------------------
// MmapStore tests
// ---------------------------------------------------------------------------

/// Write a byte slice at an offset and read it back, asserting the round-trip
/// is lossless. Also verifies that out-of-bounds writes return an error rather
/// than panicking.
#[test]
fn mmap_write_read_roundtrip() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let mmap_path = dir.path().join("store.mmap");

    let mut store = MmapStore::open(&mmap_path, 4096)
        .expect("MmapStore open should succeed");

    let data = b"leviathan storage engine";
    let offset = 16;

    store.write(offset, data).expect("write should succeed");

    let read_back = store
        .read(offset, data.len())
        .expect("read should succeed");

    assert_eq!(
        read_back, data,
        "data read back must match what was written"
    );

    // Out-of-bounds write must return an error, not panic.
    let result = store.write(4090, b"this does not fit");
    assert!(
        result.is_err(),
        "out-of-bounds write should return StorageError::OutOfBounds"
    );
}

/// Write data to an MmapStore, drop it (triggering the best-effort flush),
/// then reopen the backing file and verify the data was persisted.
///
/// This test confirms that the `Drop` impl actually flushes dirty pages before
/// the mapping is released.
#[test]
fn mmap_drop_flushes() {
    let dir = TempDir::new().expect("failed to create temp dir");
    let mmap_path = dir.path().join("drop_flush.mmap");

    let payload = b"persisted via drop flush";
    let offset = 0usize;

    // Write and drop — the Drop impl should msync.
    {
        let mut store = MmapStore::open(&mmap_path, 512)
            .expect("MmapStore open should succeed");
        store.write(offset, payload).expect("write should succeed");
        // `store` is dropped here, triggering the best-effort flush.
    }

    // Reopen and read the raw file bytes to verify persistence.
    let file_contents =
        std::fs::read(&mmap_path).expect("should be able to read the backing file");

    assert!(
        file_contents.len() >= offset + payload.len(),
        "backing file must be at least {} bytes",
        offset + payload.len()
    );

    assert_eq!(
        &file_contents[offset..offset + payload.len()],
        payload,
        "data written before drop must survive in the backing file"
    );
}
