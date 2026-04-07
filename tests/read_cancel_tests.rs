//! Cancellation propagation: last waiter aborts download session promptly.

use std::time::Duration;

use rd_rs::cache::download_session::{DownloadSession, WaiterGuard};

#[tokio::test]
async fn last_waiter_aborts_session_task() {
    let session = std::sync::Arc::new(DownloadSession::new(0, 1024));
    let handle = tokio::spawn(async {
        tokio::time::sleep(Duration::from_secs(60)).await;
    });
    session.set_abort_handle(handle.abort_handle());

    {
        let _w = WaiterGuard::new(session);
        // Drop _w at end of scope: last waiter → cancel + abort.
    }

    let res = tokio::time::timeout(Duration::from_secs(2), handle).await;
    assert!(
        res.is_ok(),
        "session task should abort quickly after last waiter drops"
    );
}
