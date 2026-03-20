//! One spawned `run_downloader` is shared by overlapping `read_at` calls on the same prefetch
//! window. `WaiterGuard` ref-counts them so we abort prefetch only when the last waiter finishes.
//!
//! Session bounds are fixed at spawn (`anchor`..`fetch_until`). A single `AtomicU64` “write head”
//! cannot represent parallel chunk workers (each overwrites the others), so we do not track `pos`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct DownloadSession {
    /// First byte passed to `run_downloader` as `start`.
    anchor: u64,
    /// Exclusive end of this prefetch pass: `(read_end + read_ahead).min(file_size)` from the
    /// reader that spawned the session.
    fetch_until: u64,
    waiters: AtomicUsize,
    abort: Mutex<Option<tokio::task::AbortHandle>>,
}

impl DownloadSession {
    pub(crate) fn new(anchor: u64, fetch_until: u64) -> Self {
        Self {
            anchor,
            fetch_until,
            waiters: AtomicUsize::new(0),
            abort: Mutex::new(None),
        }
    }

    /// Whether this session’s downloader is responsible for bytes at `offset`.
    pub(crate) fn covers_fuse_offset(&self, offset: u64) -> bool {
        offset >= self.anchor && offset < self.fetch_until
    }

    pub(crate) fn set_abort_handle(&self, h: tokio::task::AbortHandle) {
        *self
            .abort
            .lock()
            .expect("download session abort mutex poisoned") = Some(h);
    }
}

pub(crate) struct WaiterGuard {
    session: Arc<DownloadSession>,
}

impl WaiterGuard {
    pub(crate) fn new(session: Arc<DownloadSession>) -> Self {
        session.waiters.fetch_add(1, Ordering::SeqCst);
        Self { session }
    }
}

impl Drop for WaiterGuard {
    fn drop(&mut self) {
        let prev = self.session.waiters.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
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
