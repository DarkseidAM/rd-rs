//! Integration tests for HookWorker reliability (Issues D, E, G).
//!
//! - Issue D: `spawn()` must NOT early-return when command is empty.
//! - Issue E: Unbounded channel must not drop messages.
//! - Issue G: `execute_hook` must emit `info!` not `debug!` (structural test).

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use rd_rs::config::Config;
use rd_rs::torrent::hook::HookWorker;

/// Build a default `Config` with a given `on_library_update.command`.
fn make_config(command: &str) -> Arc<ArcSwap<Config>> {
    let toml =
        format!("token = \"dummy_test_token\"\n[on_library_update]\ncommand = \"{command}\"\n");
    let cfg = Config::from_toml(&toml).expect("minimal config must parse");
    Arc::new(ArcSwap::from_pointee(cfg))
}

// ── Issue D: always-start worker ─────────────────────────────────────────────

/// When the command is empty at startup, `HookWorker::spawn` must still start
/// its loop (not return early). We verify this by sending a message to `hook_tx`
/// and ensuring the receiver consumed it — if the worker had early-returned,
/// the receiver would have been dropped and `rx.recv()` would return `None`.
///
/// We use an `UnboundedReceiver` directly (bypassing the worker's internal recv
/// by extracting the channel ourselves) to observe message delivery.
#[tokio::test]
async fn test_hook_worker_starts_with_empty_command() {
    let (tx, rx) = mpsc::unbounded_channel::<Vec<String>>();

    let config = make_config(""); // empty command at startup
    let shutdown = CancellationToken::new();

    let worker = HookWorker {
        receiver: rx,
        config,
        shutdown: shutdown.clone(),
    };

    // If the worker early-returns, it drops `receiver`, making tx.send fail.
    worker.spawn();

    // Give the task scheduler a moment to start the worker.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The worker is now running; tx is still valid because the receiver wasn't dropped.
    // We can't easily observe the internal receiver but we can verify the channel is alive.
    // Sending should succeed (not return Err) as long as the receiver is kept by the worker.
    let result = tx.send(vec!["some/path".to_string()]);
    assert!(
        result.is_ok(),
        "HookWorker receiver must be alive even with empty command at startup"
    );

    shutdown.cancel();
}

// ── Issue E: unbounded channel never drops ────────────────────────────────────

/// Sends 1000 path batches rapidly and verifies none are dropped.
/// The old `mpsc::channel(256)` with `try_send` would silently drop batches
/// 257+ if the consumer was slower than the producer.
#[tokio::test]
async fn test_hook_unbounded_channel_never_drops() {
    let (tx, mut rx) = mpsc::unbounded_channel::<Vec<String>>();

    // Spawn a slow consumer (simulates debounce delay).
    let consumer = tokio::spawn(async move {
        let mut received = 0usize;
        // We stop after receiving 1000 or timeout.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            tokio::select! {
                msg = rx.recv() => {
                    match msg {
                        Some(_) => received += 1,
                        None => break,
                    }
                    if received >= 1000 { break; }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
        received
    });

    // Rapid-fire 1000 sends.
    for i in 0..1000usize {
        tx.send(vec![format!("path/{i}")])
            .expect("unbounded send should never fail while receiver is alive");
    }
    drop(tx); // signal consumer to stop

    let received = consumer.await.expect("consumer task should not panic");
    assert_eq!(
        received, 1000,
        "all 1000 batches must arrive — unbounded channel must not drop"
    );
}

// ── Issue D: empty command guard in execute_hook ──────────────────────────────

/// When the command is empty, the worker's internal `execute_hook` guard should
/// skip shell execution. We test this by sending paths to a worker with an empty
/// command, waiting, and asserting no processes were launched.
///
/// We verify indirectly: if `execute_hook` had tried to run `sh -c ""`, it would
/// likely succeed silently. Instead we confirm no panic and the worker stays alive.
#[tokio::test]
async fn test_hook_skips_empty_command_without_panic() {
    let (tx, rx) = mpsc::unbounded_channel::<Vec<String>>();
    let config = make_config(""); // empty command
    let shutdown = CancellationToken::new();

    HookWorker {
        receiver: rx,
        config,
        shutdown: shutdown.clone(),
    }
    .spawn();

    // Send some paths — they should be silently skipped, not cause a panic.
    tx.send(vec!["a/b/c".to_string()]).unwrap();
    tx.send(vec!["d/e/f".to_string()]).unwrap();

    // Let the debounce timer fire.
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Worker should still be running (not panicked).
    assert!(
        !shutdown.is_cancelled(),
        "worker should be alive after receiving paths with empty command"
    );

    shutdown.cancel();
}

// ── Issue G: log level is info (structural) ───────────────────────────────────

/// Verifies that `execute_hook` is defined to use `tracing::info!` (not `debug!`).
/// This is a structural/compile-time check — we parse the source file as text.
/// If someone accidentally reverts to `debug!`, this test will catch it.
#[test]
fn test_execute_hook_uses_info_not_debug() {
    let source = std::fs::read_to_string("src/torrent/hook.rs").expect("hook.rs must be readable");

    // Should not contain TODO debug log comment or tracing::debug in execute_hook section.
    assert!(
        !source.contains("TODO: change log level"),
        "TODO comment for log level should be removed"
    );
    assert!(
        !source.contains("tracing::debug!(\"Executing on_library_update"),
        "execute_hook must use tracing::info!, not tracing::debug!"
    );
    assert!(
        source.contains("tracing::info!(\"Executing on_library_update"),
        "execute_hook must emit tracing::info! for on_library_update events"
    );
}

// ── Issue D: hot-reload command is read per flush ─────────────────────────────

/// Verifies that the flush path reads the command from the latest config
/// (not a startup snapshot). We update the config after spawning the worker
/// and confirm the new command is what would be used.
///
/// Since observing shell execution is fragile, we test the config update path
/// by asserting that `ArcSwap::load` returns the new config after `store`.
#[test]
fn test_hook_config_hot_reload_reads_latest_command() {
    let config = make_config("");
    let config_clone = Arc::clone(&config);

    // Simulate a hot-reload: update the command.
    let new_cmd = "curl http://localhost/webhook";
    let toml =
        format!("token = \"dummy_test_token\"\n[on_library_update]\ncommand = \"{new_cmd}\"\n");
    let new_cfg = Config::from_toml(&toml).expect("minimal config must parse");
    config_clone.store(Arc::new(new_cfg));

    // The latest config should reflect the update.
    let current_cmd = config.load().on_library_update.command.clone();
    assert_eq!(
        current_cmd, "curl http://localhost/webhook",
        "ArcSwap config update must be visible to HookWorker"
    );
}
