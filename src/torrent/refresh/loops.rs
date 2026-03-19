//! Premium and downloads check loops.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::sleep;

use crate::torrent::TorrentManager;

pub async fn run_premium_check_loop(mgr: Arc<TorrentManager>) {
    tracing::info!("Premium check loop: starting (interval=15m)");
    let interval = Duration::from_secs(15 * 60);

    loop {
        match mgr.rd.get_user().await {
            Ok(user) => {
                if !user.is_premium() {
                    tracing::warn!(
                        "RD account is NOT premium! Background tasks will likely fail. ({}s remaining)",
                        user.premium
                    );
                } else if user.premium < 86400 * 3 {
                    tracing::warn!(
                        "RD account premium expires in less than 3 days ({}s)!",
                        user.premium
                    );
                }
            }
            Err(e) => tracing::warn!("Premium check: failed to fetch user state: {e:#}"),
        }

        tokio::select! {
            _ = sleep(interval) => {}
            _ = mgr.shutdown.cancelled() => {
                tracing::info!("Premium check loop: shutting down");
                return;
            }
        }
    }
}

pub async fn run_downloads_check_loop(mgr: Arc<TorrentManager>) {
    tracing::info!("Downloads check loop: starting (interval=30s)");
    let interval = Duration::from_secs(30);

    loop {
        if mgr.config.load().api.retain_non_rd_downloads {
            match mgr.rd.get_downloads(1, 1).await {
                Ok(downloads) => {
                    if let Some(item) = downloads.first() {
                        tracing::debug!(
                            "Downloads check: latest non-RD item is '{}' ({} bytes)",
                            item.filename,
                            item.filesize
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!("Downloads check: failed to fetch recent downloads: {e:#}")
                }
            }
        }

        tokio::select! {
            _ = sleep(interval) => {}
            _ = mgr.shutdown.cancelled() => {
                tracing::info!("Downloads check loop: shutting down");
                return;
            }
        }
    }
}
