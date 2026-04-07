//! Bridge per-FUSE-request cancellation: **file handle** (`release`) vs **single syscall** (FUSE
//! [`interrupt`](https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html)).
//!
//! Ctrl+C on a blocked `read()` typically sends `interrupt` before `release`; without handling it,
//! background downloads keep running until the fd is closed.

use std::sync::Arc;

use dashmap::DashMap;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Registers `unique` → cancel token, links cancellation to `fh_token`, and cleans up on drop.
pub struct FuseReadCancelRegistration {
    unique: u64,
    pending: Arc<DashMap<u64, CancellationToken>>,
    fh_link: JoinHandle<()>,
    token: CancellationToken,
}

impl FuseReadCancelRegistration {
    pub fn new(
        unique: u64,
        fh_token: CancellationToken,
        pending: Arc<DashMap<u64, CancellationToken>>,
    ) -> Self {
        let token = CancellationToken::new();
        pending.insert(unique, token.clone());
        let token_fh = token.clone();
        let fh_link = tokio::spawn(async move {
            fh_token.cancelled().await;
            token_fh.cancel();
        });
        Self {
            unique,
            pending,
            fh_link,
            token,
        }
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Drop for FuseReadCancelRegistration {
    fn drop(&mut self) {
        self.fh_link.abort();
        self.pending.remove(&self.unique);
    }
}
