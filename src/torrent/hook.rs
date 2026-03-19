//! Debounced on_library_update hook execution.
//!
//! Mirrors `hooks.go` logic: batches path changes and executes a shell command
//! (e.g. `curl http://localhost:5000/webhook`) where `%s` is replaced with the changed path.
//! A 500ms debounce ensures rapid refreshes don't spam the external system.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::process::Command;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use arc_swap::ArcSwap;

const DEBOUNCE_WAIT: Duration = Duration::from_millis(500);

pub struct HookWorker {
    pub receiver: mpsc::Receiver<Vec<String>>,
    pub config: Arc<ArcSwap<Config>>,
    pub shutdown: CancellationToken,
}

impl HookWorker {
    /// Spawns the debounced webhook processing loop.
    pub fn spawn(mut self) {
        let command_template = self.config.load().on_library_update.command.clone();
        if command_template.is_empty() {
            // Disabled
            return;
        }

        tokio::spawn(async move {
            tracing::info!(
                "HookWorker started: command='{}' debounce={:?}",
                command_template,
                DEBOUNCE_WAIT
            );

            // One simultaneous hook execution at a time to prevent thundering herd.
            let limiter = Arc::new(Semaphore::new(1));
            let mut pending_paths: HashSet<String> = HashSet::new();
            let mut flush_deadline: Option<Instant> = None;

            loop {
                tokio::select! {
                    _ = self.shutdown.cancelled() => {
                        tracing::info!("HookWorker: shutting down");
                        break;
                    }

                    // Flush timer expires
                    _ = async {
                        if let Some(deadline) = flush_deadline {
                            sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if flush_deadline.is_some() => {
                        let paths_to_trigger: Vec<String> = pending_paths.drain().collect();
                        flush_deadline = None;

                        if !paths_to_trigger.is_empty() {
                            // Read current command on each flush (hot-reload)
                            let cmd = self.config.load().on_library_update.command.clone();
                            let limiter = limiter.clone();

                            tokio::spawn(async move {
                                let _permit = limiter.acquire().await;
                                execute_hook(&cmd, paths_to_trigger).await;
                            });
                        }
                    }

                    // Receive paths from the manager
                    msg = self.receiver.recv() => {
                        match msg {
                            Some(paths) => {
                                for p in paths {
                                    pending_paths.insert(p);
                                }
                                // Reset or start the debounce timer
                                flush_deadline = Some(Instant::now() + DEBOUNCE_WAIT);
                            }
                            None => {
                                // Channel closed
                                break;
                            }
                        }
                    }
                }
            }
        });
    }
}

async fn execute_hook(command_template: &str, paths: Vec<String>) {
    // Mirror zurg: if no %s is provided, we just run the command once.
    // If %s is provided, we run the command once PER path.
    if !command_template.contains("%s") {
        tracing::debug!("Executing on_library_update: {}", command_template);
        if let Err(e) = launch_shell(command_template).await {
            tracing::warn!("on_library_update failed: {}", e);
        }
        return;
    }

    for path in paths {
        let cmd = command_template.replace("%s", &path);
        tracing::debug!("Executing on_library_update: {}", cmd);
        if let Err(e) = launch_shell(&cmd).await {
            tracing::warn!("on_library_update failed for {}: {}", path, e);
        }
    }
}

async fn launch_shell(cmd: &str) -> std::io::Result<()> {
    Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .status()
        .await
        .map(|_| ())
}
