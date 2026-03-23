/// Unit-style tests for concurrent sparse writes and a standalone semaphore sanity check.
///
/// End-to-end `read_at` + mock CDN + global semaphore + `max_parallel_streams` lives in
/// `concurrent_chunking_integration.rs` (`integration_concurrent_chunking_limits`).
use rd_rs::cache::item::CacheItem;
use std::sync::Arc;
use tempfile::tempdir;

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Builds a fresh CacheItem backed by a temp sparse file of `file_size` bytes.
fn make_cache_item(dir: &std::path::Path, name: &str, file_size: u64) -> Arc<CacheItem> {
    let path = dir.join(name);
    CacheItem::open_or_create(path, file_size).unwrap()
}

// ─── Tests ────────────────────────────────────────────────────────────────────

/// Verify that writing to a sparse file from multiple threads (simulating concurrent
/// chunk workers) does not corrupt the ByteRanges bitmap and all bytes are readable.
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_sparse_writes_are_consistent() {
    let dir = tempdir().unwrap();
    let file_size: u64 = 10 * 1024 * 1024; // 10 MB
    let chunk_size: u64 = 1024 * 1024; // 1 MB per chunk
    let item = make_cache_item(dir.path(), "concurrent_test.bin", file_size);

    // Divide the file into non-overlapping 1MB slices.
    let mut handles = vec![];
    let mut offset = 0u64;
    while offset < file_size {
        let end = (offset + chunk_size).min(file_size);
        let data: Vec<u8> = (0..(end - offset))
            .map(|i| ((offset + i) % 256) as u8)
            .collect();
        let item_clone = Arc::clone(&item);

        handles.push(tokio::spawn(async move {
            item_clone.write_range(offset, &data).unwrap();
        }));
        offset = end;
    }

    for h in handles {
        h.await.unwrap();
    }

    // All 10MB should be marked as cached.
    assert!(
        item.has_range(0, file_size),
        "All regions should be marked present after concurrent writes"
    );

    // Spot-check that data reads back correctly.
    let sample = item.read_from_file(0, 256).unwrap();
    for (i, &byte) in sample.iter().enumerate() {
        assert_eq!(byte, i as u8, "byte mismatch at offset {i}");
    }

    let sample2 = item.read_from_file(2 * 1024 * 1024, 256).unwrap();
    for (i, &byte) in sample2.iter().enumerate() {
        let expected_offset = 2 * 1024 * 1024u64 + i as u64;
        assert_eq!(
            byte,
            (expected_offset % 256) as u8,
            "byte mismatch at offset {expected_offset}"
        );
    }
}

/// Verify that ByteRanges correctly tracks disjoint holes when only partial chunks are written.
#[test]
fn test_partial_chunked_write_has_gaps() {
    let dir = tempdir().unwrap();
    let file_size: u64 = 8 * 1024 * 1024; // 8 MB
    let chunk_size: u64 = 1024 * 1024; // 1 MB

    let item = make_cache_item(dir.path(), "gaps_test.bin", file_size);

    // Write only every other chunk: 0-1MB, 2-3MB, 4-5MB, 6-7MB
    for i in [0u64, 2, 4, 6] {
        let start = i * chunk_size;
        let end = start + chunk_size;
        let data = vec![0xAAu8; chunk_size as usize];
        item.write_range(start, &data).unwrap();

        // This chunk should be present.
        assert!(item.has_range(start, end), "chunk {i} should be cached");
        // The gap after this chunk should NOT be present.
        if i < 6 {
            assert!(
                !item.has_range(end, end + chunk_size),
                "gap after chunk {i} should be missing"
            );
        }
    }

    // Total bytes in bitmap should be exactly 4MB.
    let total = item.total_cached_bytes();
    assert_eq!(
        total,
        4 * 1024 * 1024,
        "bitmap should track exactly 4MB of data"
    );
}

/// Verify the global connection semaphore correctly throttles concurrent acquisitions.
#[tokio::test]
async fn test_semaphore_limits_concurrent_connections() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let limit = 4u32;
    let semaphore = Arc::new(tokio::sync::Semaphore::new(limit as usize));
    let max_concurrent = Arc::new(AtomicU32::new(0));
    let current = Arc::new(AtomicU32::new(0));

    let mut handles = vec![];
    for _ in 0..16 {
        let sem = Arc::clone(&semaphore);
        let max = Arc::clone(&max_concurrent);
        let cur = Arc::clone(&current);

        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let live = cur.fetch_add(1, Ordering::SeqCst) + 1;
            max.fetch_max(live, Ordering::SeqCst);
            // Simulate chunk download time.
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            cur.fetch_sub(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let peak = max_concurrent.load(Ordering::SeqCst);
    assert!(
        peak <= limit,
        "Peak concurrent connections {peak} exceeded semaphore limit {limit}"
    );
}
