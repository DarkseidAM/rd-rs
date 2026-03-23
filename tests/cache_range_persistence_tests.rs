use std::sync::Arc;
use std::time::Duration;

use rd_rs::cache::Cache;
use rd_rs::config::VfsConfig;
use tempfile::tempdir;

fn cfg(max_age: &str) -> Arc<VfsConfig> {
    Arc::new(VfsConfig {
        cache_max_size: "5GB".into(),
        cache_max_age: max_age.into(),
        cache_min_free_space: "0".into(),
        ..VfsConfig::default()
    })
}

#[tokio::test]
async fn ranges_survive_cache_restart() {
    let dir = tempdir().unwrap();
    let access_key = "ak";
    let filename = "video.mkv";

    let cache = Cache::new(dir.path(), cfg("24h"));
    let item = cache.get_or_create(access_key, filename, 1024).unwrap();
    item.write_range(10, b"hello world").unwrap();
    item.flush_ranges(true);
    assert!(item.has_range(10, 21));

    drop(cache);

    let cache2 = Cache::new(dir.path(), cfg("24h"));
    let item2 = cache2.get_or_create(access_key, filename, 1024).unwrap();
    assert!(
        item2.has_range(10, 21),
        "ranges should restore from cache_ranges.db"
    );
}

#[tokio::test]
async fn cache_max_age_ttl_prunes_persisted_ranges() {
    let dir = tempdir().unwrap();
    let access_key = "ak2";
    let filename = "movie.mp4";
    let data_path = dir.path().join(access_key).join(filename);

    let cache = Cache::new(dir.path(), cfg("1s"));
    let item = cache.get_or_create(access_key, filename, 4096).unwrap();
    item.write_range(0, b"abcdef").unwrap();
    item.flush_ranges(true);
    assert!(data_path.exists());
    drop(cache);

    tokio::time::sleep(Duration::from_secs(2)).await;

    let cache2 = Cache::new(dir.path(), cfg("1s"));
    // startup evict loop runs once immediately; give it a short window.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let item2 = cache2.get_or_create(access_key, filename, 4096).unwrap();
    assert!(
        !item2.has_range(0, 6),
        "ttl should remove stale persisted ranges so reopen is cold"
    );
}
