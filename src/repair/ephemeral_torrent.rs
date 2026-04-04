//! Ephemeral Real-Debrid torrents created during repair (per-strategy magnets).
//!
//! [`EphemeralRdTorrent`] deletes its id on drop via a spawned task (best-effort if the
//! runtime is still alive). Call [`EphemeralRdTorrent::dismiss`] when the torrent becomes
//! the new canonical row (Strategy 1 / 3 success) or was already removed (e.g. inside
//! [`info_ready`] for `restrict_to_cached`).
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

/// Result of [`info_ready`]: whether RD reports the torrent link-ready, and whether this
/// helper already called `delete_torrent` (caller must [`EphemeralRdTorrent::dismiss`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InfoReadyOutcome {
    pub(crate) is_ready: bool,
    pub(crate) deleted_by_info_ready: bool,
}

impl InfoReadyOutcome {
    fn ready() -> Self {
        Self {
            is_ready: true,
            deleted_by_info_ready: false,
        }
    }

    fn not_ready_deleted() -> Self {
        Self {
            is_ready: false,
            deleted_by_info_ready: true,
        }
    }

    fn not_ready_keep() -> Self {
        Self {
            is_ready: false,
            deleted_by_info_ready: false,
        }
    }
}

/// Poll RD until the torrent is 100% with links, or give up after 3 attempts.
/// Backoff: immediate, then 1s, then 2s before subsequent `get_torrent_info` calls.
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
        if restrict_cached && info.progress < 100 {
            let _ = rd.delete_torrent(id).await;
            return Ok(InfoReadyOutcome::not_ready_deleted());
        }
        if info.progress == 100 && !info.links.is_empty() {
            return Ok(InfoReadyOutcome::ready());
        }
    }
    Ok(InfoReadyOutcome::not_ready_keep())
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
}

impl Drop for EphemeralRdTorrent {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let rd = self.rd.clone();
            match tokio::runtime::Handle::try_current() {
                Ok(handle) => {
                    handle.spawn(async move {
                        let _ = rd.delete_torrent(&id).await;
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
