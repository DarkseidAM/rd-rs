//! Refresh expired CDN URLs via `POST /unrestrict/link` when Range GET fails with RD CDN errors.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use tokio::sync::{Mutex, RwLock};

use crate::rd::RealDebrid;
use crate::rd::api::{UnrestrictCache, clear_unrestrict_cache_for_source_link};
use crate::rd::client::RdError;

/// Max automatic unrestrict refreshes per downloader session (shared across chunk workers).
pub const MAX_SESSION_LINK_HEALS: u32 = 3;

/// If `err` warrants it and budget remains, clear unrestrict cache, call unrestrict, update `live_url`.
/// Returns `true` when the caller should retry the HTTP Range GET without counting a normal retry.
pub async fn attempt_cdn_link_refresh(
    err: &RdError,
    rd: &Arc<RealDebrid>,
    cache: &UnrestrictCache,
    source_link: &str,
    live_url: &RwLock<String>,
    refresh_lock: &Mutex<()>,
    heal_remaining: &AtomicU32,
) -> bool {
    if !err.should_refresh_via_unrestrict() {
        return false;
    }

    let _guard = refresh_lock.lock().await;
    if heal_remaining.load(Ordering::Relaxed) == 0 {
        return false;
    }

    tracing::warn!(
        link = %source_link,
        error = %format!("{err:#}"),
        "CDN download link expired or invalid — refreshing via POST /unrestrict/link"
    );

    if let Err(refresh_err) = refresh_cdn_url(rd, cache, source_link, live_url).await {
        tracing::warn!(
            error = %format!("{refresh_err:#}"),
            "unrestrict/link failed during CDN link refresh"
        );
        return false;
    }

    heal_remaining.fetch_sub(1, Ordering::Relaxed);
    tracing::info!("Refreshed Real-Debrid CDN URL after CDN failure; retrying range GET");
    true
}

async fn refresh_cdn_url(
    rd: &Arc<RealDebrid>,
    cache: &UnrestrictCache,
    source_link: &str,
    live_url: &RwLock<String>,
) -> Result<(), RdError> {
    // When healing an expired CDN URL, drop *all* token buckets for this source link.
    // The next unrestrict may use a different eligible token, and we must not reuse stale URLs
    // from other accounts.
    clear_unrestrict_cache_for_source_link(cache, source_link);
    let download = rd.unrestrict_link(cache, source_link).await?;
    *live_url.write().await = download.download;
    Ok(())
}
