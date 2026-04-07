//! `FuseReadCancelRegistration` — per-request cancel + fh propagation.

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use rd_rs::fuse::FuseReadCancelRegistration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn interrupt_removes_pending_and_cancels_token() {
    let pending = Arc::new(DashMap::new());
    let fh = CancellationToken::new();
    let reg = FuseReadCancelRegistration::new(7, fh.clone(), Arc::clone(&pending));
    let t = reg.token();
    assert!(pending.contains_key(&7));

    let removed = pending.remove(&7).expect("entry");
    removed.1.cancel();
    tokio::time::timeout(Duration::from_secs(2), t.cancelled())
        .await
        .expect("timeout");
}

#[tokio::test]
async fn fh_cancel_propagates_to_read_token() {
    let pending = Arc::new(DashMap::new());
    let fh = CancellationToken::new();
    let reg = FuseReadCancelRegistration::new(1, fh.clone(), Arc::clone(&pending));
    let t = reg.token();
    fh.cancel();
    tokio::time::timeout(Duration::from_secs(2), t.cancelled())
        .await
        .expect("timeout");
}
