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

pub(crate) const UNRESTRICT_CACHE_TTL: Duration = Duration::from_secs(4 * 3600);

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

/// Drop all unrestricted rows (e.g. after credential hot-reload).
pub fn clear_unrestrict_cache_all(cache: &UnrestrictCache) {
    cache.clear();
}
