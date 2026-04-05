//! RD API method implementations.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::time::Instant;

use crate::rd::types::Download;

mod helpers;
mod methods;
mod methods2;
mod methods3;

pub use helpers::extract_base_download_url;

/// Logical max age for an in-memory unrestrict row (matches RD-style refresh window).
pub const UNRESTRICT_CACHE_TTL: Duration = Duration::from_secs(4 * 3600);

/// Cache key: API token used for `POST /unrestrict/link` plus the source link.
///
/// Multiple RD accounts must not share unrestricted rows; CDN Bearer may differ per account.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct UnrestrictCacheKey {
    pub token: Arc<String>,
    pub link: Arc<String>,
}

impl UnrestrictCacheKey {
    pub fn new(token: impl Into<Arc<String>>, link: impl Into<Arc<String>>) -> Self {
        Self {
            token: token.into(),
            link: link.into(),
        }
    }

    pub fn from_strs(token: &str, link: &str) -> Self {
        Self::new(Arc::new(token.to_string()), Arc::new(link.to_string()))
    }
}

pub type UnrestrictCache = Arc<DashMap<UnrestrictCacheKey, (Download, Instant)>>;

pub fn new_unrestrict_cache() -> UnrestrictCache {
    Arc::new(DashMap::new())
}

/// Remove one unrestricted row for the given API token and source link.
pub fn clear_unrestrict_cache(cache: &UnrestrictCache, token: &str, link: &str) {
    cache.remove(&UnrestrictCacheKey::from_strs(token, link));
}

/// Remove every in-memory row for `link` (all token buckets). Use when the source link is bad
/// and you do not know which Bearer was used to populate the cache.
pub fn clear_unrestrict_cache_for_source_link(cache: &UnrestrictCache, link: &str) {
    let keys: Vec<UnrestrictCacheKey> = cache
        .iter()
        .filter_map(|e| (e.key().link.as_str() == link).then(|| e.key().clone()))
        .collect();
    for k in keys {
        cache.remove(&k);
    }
}

/// Drop all unrestricted rows (e.g. after credential hot-reload).
pub fn clear_unrestrict_cache_all(cache: &UnrestrictCache) {
    cache.clear();
}

/// Remove in-memory unrestrict entries at least `max_age` old (wall time since insert).
///
/// Typically pass [`UNRESTRICT_CACHE_TTL`] so the map does not grow unbounded with stale rows.
/// Returns how many entries were removed.
pub fn sweep_unrestrict_cache_expired(cache: &UnrestrictCache, max_age: Duration) -> usize {
    let stale_keys: Vec<UnrestrictCacheKey> = cache
        .iter()
        .filter_map(|e| {
            let (_, at) = e.value();
            (at.elapsed() >= max_age).then(|| e.key().clone())
        })
        .collect();
    let n = stale_keys.len();
    for k in stale_keys {
        cache.remove(&k);
    }
    n
}
