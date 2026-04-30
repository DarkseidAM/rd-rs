//! One spawned `run_downloader` is shared by overlapping `read_at` calls on the same prefetch
//! window. `WaiterGuard` ref-counts them so we abort prefetch only when the last waiter finishes.
//!
//! Session bounds are fixed at spawn (`anchor`..`fetch_until`). A single `AtomicU64` “write head”
//! cannot represent parallel chunk workers (each overwrites the others), so we do not track `pos`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

/// Shared downloader session for a prefetch window.
///
/// This is a low-level cache primitive (used by `CacheItem::read_at`). It is public only so
/// integration tests can validate cancellation / abort behavior.
pub struct DownloadSession {
    /// First byte passed to `run_downloader` as `start`.
    anchor: u64,
    /// Exclusive end of this prefetch pass: `(read_end + read_ahead).min(file_size)` from the
    /// reader that spawned the session.
    fetch_until: u64,
    waiters: AtomicUsize,
    abort: Mutex<Option<tokio::task::AbortHandle>>,
    cancel: CancellationToken,
}

impl DownloadSession {
    pub fn new(anchor: u64, fetch_until: u64) -> Self {
        Self {
            anchor,
            fetch_until,
            waiters: AtomicUsize::new(0),
            abort: Mutex::new(None),
            cancel: CancellationToken::new(),
        }
    }

    /// Whether this session’s downloader is responsible for bytes at `offset`.
    pub(crate) fn covers_fuse_offset(&self, offset: u64) -> bool {
        offset >= self.anchor && offset < self.fetch_until
    }

    pub fn set_abort_handle(&self, h: tokio::task::AbortHandle) {
        *self
            .abort
            .lock()
            .expect("download session abort mutex poisoned") = Some(h);
    }

    pub(crate) fn cancel_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Immediately cancel this session's downloader task without waiting for all
    /// waiter guards to drop. Used by the kicker to stop a stalled session before
    /// spawning a priority replacement.
    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
        let mut g = self
            .abort
            .lock()
            .expect("download session abort mutex poisoned");
        if let Some(h) = g.take() {
            h.abort();
        }
    }
}

/// RAII waiter counter; last drop cancels + aborts the session.
pub struct WaiterGuard {
    session: Arc<DownloadSession>,
}

impl WaiterGuard {
    pub fn new(session: Arc<DownloadSession>) -> Self {
        session.waiters.fetch_add(1, Ordering::SeqCst);
        Self { session }
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let prev = self.session.waiters.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.session.cancel.cancel();
            let mut g = self
                .session
                .abort
                .lock()
                .expect("download session abort mutex poisoned");
            if let Some(h) = g.take() {
                h.abort();
            }
        }
    }
}
