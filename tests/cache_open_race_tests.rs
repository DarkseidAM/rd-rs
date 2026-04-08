use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rd_rs::cache::Cache;
use rd_rs::config::VfsConfig;

#[tokio::test]
async fn get_or_create_marks_open_before_return_to_avoid_eviction_race() {
    let tmp = tempfile::tempdir().unwrap();
    let cfg = Arc::new(VfsConfig::default());
    let cache = Cache::new(tmp.path(), cfg);

    // Create once, then age it out to be "idle-evictable".
    let item = cache.get_or_create("ak", "f.bin", 10).unwrap();
    item.release(); // balance open from get_or_create
    item.set_atime_secs(0);

    // Start a task that will acquire it and hold it open briefly.
    let acquired = Arc::new(AtomicBool::new(false));
    let acquired2 = Arc::clone(&acquired);
    let cache2 = Arc::clone(&cache);
    let (tx, rx) = tokio::sync::oneshot::channel::<usize>();
    let t = tokio::spawn(async move {
        let it = cache2.get_or_create("ak", "f.bin", 10).unwrap();
        let _ = tx.send(Arc::as_ptr(&it) as usize);
        acquired2.store(true, Ordering::Release);
        tokio::time::sleep(Duration::from_millis(50)).await;
        it.release();
    });

    // Wait until the task has acquired the item, then run eviction.
    tokio::time::timeout(Duration::from_secs(2), async {
        while !acquired.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let held_ptr = rx.await.unwrap();
    cache.evict_once();
    let it2 = cache.get_or_create("ak", "f.bin", 10).unwrap();
    let ptr2 = Arc::as_ptr(&it2) as usize;
    assert_eq!(ptr2, held_ptr, "eviction removed an in-use cache item");
    it2.release();

    t.await.unwrap();
}
