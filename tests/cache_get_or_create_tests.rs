//! Cache `get_or_create` returns a single shared item per key.

use std::sync::Arc;

use rd_rs::cache::Cache;
use rd_rs::config::VfsConfig;

#[tokio::test]
async fn get_or_create_same_arc_per_key() {
    let dir = tempfile::tempdir().unwrap();
    let vfs = Arc::new(VfsConfig::default());
    let cache = Cache::new(dir.path(), vfs);
    let a = cache
        .get_or_create("ak", "movie.mkv", 4096)
        .expect("first create");
    let b = cache
        .get_or_create("ak", "movie.mkv", 4096)
        .expect("second get");
    assert!(
        Arc::ptr_eq(&a, &b),
        "get_or_create must return the same Arc as the live map entry"
    );
}
