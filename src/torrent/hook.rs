//! Debounced on_library_update hook worker.
//!
//! Mirrors Go's `internal/torrent/hooks.go`:
//! - 256-slot channel to buffer incoming path batches
//! - 500ms debounce (collapses rapid consecutive changes into one firing)
//! - Semaphore depth=1 (at most one script execution at a time)
//! - Path deduplication before each execution

use std::collections::HashSet;
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

const DEBOUNCE: Duration = Duration::from_millis(500);

// ─── Public API ────────────────────────────────────────────────────────────────

/// Launch the hook worker as a background task.
///
/// Returns a `Sender` to enqueue path batches and a `CancellationToken` to
/// gracefully shut the worker down.
pub fn spawn_hook_worker(
    command: String,
    shutdown: CancellationToken,
) -> mpsc::Sender<Vec<String>> {
    let (tx, rx) = mpsc::channel::<Vec<String>>(256);
    tokio::spawn(run_hook_worker(command, rx, shutdown));
    tx
}

// ─── Worker loop ──────────────────────────────────────────────────────────────

async fn run_hook_worker(
    command: String,
    mut rx: mpsc::Receiver<Vec<String>>,
    shutdown: CancellationToken,
) {
    if command.is_empty() {
        // No hook configured — drain any messages silently.
        loop {
            tokio::select! {
                _ = rx.recv() => {}
                _ = shutdown.cancelled() => return,
            }
        }
    }

    tracing::debug!("Hook worker: started (command={:?})", command);

    let mut buffer: Vec<String> = Vec::new();
    // When to fire (None = no pending batch)
    let mut fire_at: Option<Instant> = None;

    loop {
        // Build a sleep future (or a never-completing future if no deadline)
        let deadline_future = async {
            match fire_at {
                Some(t) => sleep_until(t).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            // New paths to buffer
            Some(paths) = rx.recv() => {
                buffer.extend(paths);
                // Reset (or set) the debounce timer
                fire_at = Some(Instant::now() + DEBOUNCE);
            }

            // Debounce timer fired
            _ = deadline_future, if fire_at.is_some() => {
                let unique = dedup(&buffer);
                buffer.clear();
                fire_at = None;
                execute_command(&command, &unique).await;
            }

            // Graceful shutdown: flush whatever is in the buffer
            _ = shutdown.cancelled() => {
                if !buffer.is_empty() {
                    let unique = dedup(&buffer);
                    execute_command(&command, &unique).await;
                }
                tracing::debug!("Hook worker: stopped");
                return;
            }
        }
    }
}

// ─── Command execution ────────────────────────────────────────────────────────

async fn execute_command(command: &str, paths: &[String]) {
    if command.is_empty() || paths.is_empty() {
        return;
    }

    tracing::debug!(
        "Hook: firing on_library_update for {} path(s): {:?}",
        paths.len(),
        &paths[..paths.len().min(5)]
    );

    let result = Command::new("sh").arg("-c").arg(command).output().await;

    match result {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if !stdout.trim().is_empty() {
                tracing::debug!("Hook output: {}", stdout.trim());
            }
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            tracing::warn!("Hook failed (exit={}): {}", out.status, stderr.trim());
        }
        Err(e) => {
            tracing::error!("Hook execution error: {e}");
        }
    }
}

// ─── Path dedup ────────────────────────────────────────────────────────────────

/// Remove duplicate and empty paths, preserving first-seen order.
pub fn dedup(paths: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    paths
        .iter()
        .filter(|p| !p.is_empty() && seen.insert(p.as_str()))
        .cloned()
        .collect()
}
