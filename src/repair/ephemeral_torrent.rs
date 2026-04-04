//! Ephemeral Real-Debrid torrents created during repair (per-strategy magnets).
//!
//! [`EphemeralRdTorrent`] deletes its id on drop via a spawned task (best-effort if the
//! runtime is still alive). Call [`EphemeralRdTorrent::dismiss`] only when the RD torrent
//! becomes the new canonical row (Strategy 1 / 3 success) so [`Drop`] does **not** remove it.
//!
//! [`info_ready`] does not call `delete_torrent`. For outcomes where the ephemeral magnet must
//! be removed, **do not** [`EphemeralRdTorrent::dismiss`] unless the RD row is kept on purpose.
//! Use [`EphemeralRdTorrent::delete_ephemeral`] before returning terminal outcomes so
//! `delete_torrent` errors propagate (`?` / `map_rd`); on `Err` the id stays set so [`Drop`]
//! can retry. Calling `dismiss()` on a failure path would skip cleanup and orphan the torrent on
//! Real-Debrid.
//!
//! # `id` / `dismiss` contract
//! After [`EphemeralRdTorrent::new`], [`EphemeralRdTorrent::id`] is [`Some`] until
//! [`EphemeralRdTorrent::dismiss`] runs. Do not call [`EphemeralRdTorrent::id`] after
//! `dismiss` on the same guard; use the [`String`] returned from `dismiss` if you need the id.

use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::rd::RealDebrid;
use crate::rd::client::RdError;

/// Message for [`Option::expect`] on [`EphemeralRdTorrent::id`] when the cascade guarantees the
/// id exists (i.e. [`EphemeralRdTorrent::dismiss`] has not been called on this guard).
pub(crate) const EXPECT_EPHEMERAL_ID: &str =
    "ephemeral RD torrent id missing: do not call id() after dismiss()";

/// Message for [`Option::expect`] on [`EphemeralRdTorrent::dismiss`] when the success path must
/// consume the id exactly once.
pub(crate) const EXPECT_DISMISS_HAS_ID: &str = "id already taken";

/// Result of [`info_ready`]. Does not delete the RD torrent; [`EphemeralRdTorrent`] cleans up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InfoReadyOutcome {
    pub(crate) is_ready: bool,
    /// `restrict_to_cached` is on and polling stopped without 100% progress and links (unrepairable
    /// as not cached). Call [`EphemeralRdTorrent::delete_ephemeral`] before returning, or rely on
    /// [`Drop`] if deletion is not awaited there.
    pub(crate) restrict_cached_not_ready: bool,
}

impl InfoReadyOutcome {
    fn ready() -> Self {
        Self {
            is_ready: true,
            restrict_cached_not_ready: false,
        }
    }

    fn not_ready_keep() -> Self {
        Self {
            is_ready: false,
            restrict_cached_not_ready: false,
        }
    }

    fn restrict_cached_exhausted() -> Self {
        Self {
            is_ready: false,
            restrict_cached_not_ready: true,
        }
    }
}

/// Poll RD until the torrent is 100% with links, or give up after 3 attempts.
/// Backoff: immediate, then 1s, then 2s before subsequent `get_torrent_info` calls.
///
/// With `restrict_cached`, early `progress < 100` (common right after select) does **not** end the
/// poll; only exhaustion without reaching ready sets [`InfoReadyOutcome::restrict_cached_not_ready`].
pub(crate) async fn info_ready(
    rd: &RealDebrid,
    id: &str,
    restrict_cached: bool,
) -> Result<InfoReadyOutcome, RdError> {
    for attempt in 0..3 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(attempt as u64)).await;
        }

        let info = rd.get_torrent_info(id).await?;
        if info.progress == 100 && !info.links.is_empty() {
            return Ok(InfoReadyOutcome::ready());
        }
    }
    Ok(if restrict_cached {
        InfoReadyOutcome::restrict_cached_exhausted()
    } else {
        InfoReadyOutcome::not_ready_keep()
    })
}

/// Deletes the RD torrent id on drop unless [`Self::dismiss`] was called.
/// If there is no current Tokio runtime, cleanup is skipped (avoids `tokio::spawn` panicking).
///
/// **Invariant:** [`Self::id`] is only valid until the first [`Self::dismiss`] (see module docs).
pub(crate) struct EphemeralRdTorrent {
    rd: Arc<RealDebrid>,
    id: Option<String>,
}

impl EphemeralRdTorrent {
    pub(crate) fn new(rd: Arc<RealDebrid>, id: String) -> Self {
        Self { rd, id: Some(id) }
    }

    /// Clears the ephemeral id so [`Drop`] will not delete it. Returns the id if it was present.
    pub(crate) fn dismiss(&mut self) -> Option<String> {
        self.id.take()
    }

    /// Real-Debrid torrent id while this guard still owns it. [`None`] after [`Self::dismiss`].
    pub(crate) fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Deletes this ephemeral torrent on Real-Debrid and clears the id so [`Drop`] will not delete
    /// again. On `Err`, the id is left in place for a later attempt (e.g. [`Drop`]).
    pub(crate) async fn delete_ephemeral(&mut self) -> Result<(), RdError> {
        let Some(id) = self.id.as_deref() else {
            return Ok(());
        };
        self.rd.delete_torrent(id).await?;
        self.id = None;
        Ok(())
    }
}

impl Drop for EphemeralRdTorrent {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let rd = self.rd.clone();
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        if let Err(e) = rd.delete_torrent(&id).await {
                            warn!(
                                rd_id = %id,
                                error = %e,
                                "EphemeralRdTorrent drop: delete_torrent failed, retrying once"
                            );
                            tokio::time::sleep(Duration::from_secs(1)).await;
                            if let Err(e2) = rd.delete_torrent(&id).await {
                                warn!(
                                    rd_id = %id,
                                    error = %e2,
                                    "EphemeralRdTorrent drop: delete_torrent failed after retry; orphan may remain on Real-Debrid"
                                );
                            }
                        }
                    });
                }
                Err(_) => {
                    warn!(
                        rd_id = %id,
                        "EphemeralRdTorrent drop: no Tokio runtime; skipped delete_torrent cleanup"
                    );
                }
            }
        }
    }
}
