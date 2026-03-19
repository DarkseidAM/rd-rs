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

pub type UnrestrictCache = Arc<DashMap<String, (Download, Instant)>>;

pub fn new_unrestrict_cache() -> UnrestrictCache {
    Arc::new(DashMap::new())
}
