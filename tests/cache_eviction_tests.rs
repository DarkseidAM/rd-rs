use rd_rs::cache::cache::Cache;
use rd_rs::config::VfsConfig;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use tempfile::tempdir;

#[tokio::test]
async fn test_evict_disk_lru_sparse_file() {
    let dir = tempdir().unwrap();
    let config = Arc::new(VfsConfig {
        cache_max_size: "50MB".to_string(),
        cache_min_free_space: "0".to_string(),
        ..VfsConfig::default()
    });

    let cache = Cache::new(dir.path(), config);
    let file_path = dir.path().join("fake_key").join("large_sparse");
    std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();

    // Create a 1GB sparse file, allocate only 1 page (4KB)
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&file_path)
        .unwrap();
    file.set_len(1024 * 1024 * 1024).unwrap(); // 1 GB apparent size

    // 2. Write 60MB of actual data to a second file.
    let big_path = dir.path().join("fake_key").join("big_file");
    let mut big = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&big_path)
        .unwrap();
    big.write_all(&vec![0u8; 60 * 1024 * 1024]).unwrap();
    big.sync_all().unwrap();
    drop(big);

    // Wait a few milliseconds to ensure reliable access time ordering
    std::thread::sleep(std::time::Duration::from_millis(100));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 3. Grace period protection test:
    // Set now_secs to 'now', which is < 300 seconds from file_path access time.
    // It should NOT evict `big_file.bin` despite being over the 50MB threshold.
    cache.evict_disk_lru(50 * 1024 * 1024, now);
    assert!(
        big_path.exists(),
        "Grace period should protect recently created heavy files"
    );

    // 4. Force eviction:
    // Advance time 10 minutes (600s). The 60MB file is now evictable.
    cache.evict_disk_lru(50 * 1024 * 1024, now + 600);

    assert!(
        !big_path.exists(),
        "60MB physical file should be evicted to honor 50MB limit"
    );

    // Ensure the small file was not evicted (since dropping the 60MB file satisfied the threshold)
    assert!(
        file_path.exists(),
        "Tiny sparse file survives because freed capacity is enough"
    );
}
