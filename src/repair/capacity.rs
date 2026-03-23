//! Wait until RD active-download slots are available (zurg `canCapacityHandle`).

use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::rd::RealDebrid;

const MAX_RETRIES: u32 = 10;
const BASE_DELAY_SECS: u64 = 60;
const MAX_DELAY_SECS: u64 = 600;

/// Returns `false` if shutdown or max wait exceeded (zurg: abort repair).
pub async fn wait_for_repair_capacity(rd: &RealDebrid, shutdown: &CancellationToken) -> bool {
    let mut retry = 0u32;
    let mut total = Duration::ZERO;

    loop {
        if shutdown.is_cancelled() {
            info!("Repair capacity wait cancelled after {:?}", total);
            return false;
        }

        match rd.get_active_count().await {
            Ok(count) => {
                let cap = count.max_number_of_torrents.max(1);
                if count.downloading_count < cap.saturating_sub(1) {
                    if total > Duration::ZERO {
                        info!("RD repair capacity available after {:?}", total);
                    }
                    return true;
                }
            }
            Err(e) => {
                warn!("get_active_count failed: {e:#}");
                if retry >= MAX_RETRIES {
                    return false;
                }
            }
        }

        let pow = retry.min(31);
        let mut delay_secs = (1u64 << pow)
            .saturating_mul(BASE_DELAY_SECS / 2)
            .max(BASE_DELAY_SECS);
        delay_secs = delay_secs.min(MAX_DELAY_SECS);
        let delay = Duration::from_secs(delay_secs);
        total += delay;
        info!(
            "RD active torrent slots full; waiting {:?} (retry {}, total {:?})",
            delay, retry, total
        );

        if retry >= MAX_RETRIES {
            warn!("Repair capacity: max retries after {:?}", total);
            return false;
        }

        tokio::select! {
            _ = shutdown.cancelled() => {
                info!("Repair capacity wait cancelled during sleep ({:?} waited)", total);
                return false;
            }
            _ = tokio::time::sleep(delay) => {}
        }

        retry += 1;
    }
}
