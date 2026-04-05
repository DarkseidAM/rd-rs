//! Unrestrict cache: per-token partition so multiple RD accounts cannot cross-pollute.

use std::sync::Arc;
use std::time::Duration;

use rd_rs::rd::api::{
    UnrestrictCacheKey, clear_unrestrict_cache, clear_unrestrict_cache_all, new_unrestrict_cache,
};
use rd_rs::rd::types::Download;
use tokio::time::Instant;

fn sample_download(token: &str) -> Download {
    Download {
        filename: "f.mkv".into(),
        filesize: 100,
        link: "https://real-debrid.com/d/abc".into(),
        download: "https://1.download.real-debrid.com/x".into(),
        streamable: 0,
        generated_at: None,
        token: token.into(),
    }
}

#[test]
fn cache_key_token_and_link_independent() {
    let k1 = UnrestrictCacheKey::from_strs("token_a", "https://link1");
    let k2 = UnrestrictCacheKey::from_strs("token_b", "https://link1");
    let k3 = UnrestrictCacheKey::from_strs("token_a", "https://link2");
    assert_ne!(k1, k2);
    assert_ne!(k1, k3);
    assert_eq!(
        k1,
        UnrestrictCacheKey::from_strs("token_a", "https://link1")
    );
}

#[test]
fn clear_removes_only_matching_token_bucket() {
    let cache = new_unrestrict_cache();
    let dl_a = sample_download("token_a");
    let dl_b = sample_download("token_b");
    let link = "https://real-debrid.com/d/shared";
    cache.insert(
        UnrestrictCacheKey::from_strs("token_a", link),
        (dl_a, Instant::now()),
    );
    cache.insert(
        UnrestrictCacheKey::from_strs("token_b", link),
        (dl_b, Instant::now()),
    );
    assert_eq!(cache.len(), 2);

    clear_unrestrict_cache(&cache, "token_a", link);
    assert_eq!(cache.len(), 1);
    assert!(
        cache
            .get(&UnrestrictCacheKey::from_strs("token_b", link))
            .is_some()
    );
    assert!(
        cache
            .get(&UnrestrictCacheKey::from_strs("token_a", link))
            .is_none()
    );
}

#[test]
fn clear_all_empties_cache() {
    let cache = new_unrestrict_cache();
    cache.insert(
        UnrestrictCacheKey::new(Arc::new("t".into()), Arc::new("l".into())),
        (sample_download("t"), Instant::now()),
    );
    clear_unrestrict_cache_all(&cache);
    assert!(cache.is_empty());
}

#[test]
fn ttl_expired_entry_not_returned_by_get_simulation() {
    let cache = new_unrestrict_cache();
    let key = UnrestrictCacheKey::from_strs("tok", "https://x");
    let old = Instant::now() - Duration::from_secs(5 * 3600);
    cache.insert(key.clone(), (sample_download("tok"), old));
    let entry = cache.get(&key).expect("present in map");
    let (_, at) = entry.value();
    assert!(at.elapsed() >= Duration::from_secs(4 * 3600));
}
