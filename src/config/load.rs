//! Config loading and hot-reload.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::watch;

use super::structs::Config;

impl Config {
    /// Read and parse a TOML config file.
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file: {}", path.display()))?;
        Self::from_toml(&content)
    }

    /// Parse from a TOML string (useful in tests).
    pub fn from_toml(s: &str) -> Result<Self> {
        let cfg: Config = toml::from_str(s).context("parsing TOML config")?;
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        if self.token.is_empty() {
            bail!("config: `token` must not be empty");
        }
        if self.api.rate_limit_per_minute == 0 {
            bail!("config: api.rate_limit_per_minute must be > 0");
        }
        if self.api.timeout_secs == 0 {
            bail!("config: api.timeout_secs must be > 0");
        }
        Ok(())
    }

    /// Returns all download tokens (primary first, then extras).
    pub fn all_download_tokens(&self) -> Vec<String> {
        let mut tokens = vec![self.token.clone()];
        for t in &self.download_tokens {
            if !tokens.contains(t) {
                tokens.push(t.clone());
            }
        }
        tokens
    }

    /// Start a background hot-reload watcher.
    pub fn watch(
        path: impl AsRef<std::path::Path> + Send + 'static,
    ) -> Result<(watch::Receiver<Config>, RecommendedWatcher)> {
        let initial = Config::load(path.as_ref())?;
        let (tx, rx) = watch::channel(initial);
        let path_clone = path.as_ref().to_path_buf();

        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| match res {
                Ok(event) if event.kind.is_modify() => {
                    std::thread::sleep(Duration::from_millis(500));
                    match Config::load(&path_clone) {
                        Ok(new_cfg) => {
                            tracing::info!("Config reloaded from {}", path_clone.display());
                            let _ = tx.send(new_cfg);
                        }
                        Err(e) => {
                            tracing::warn!("Config reload failed (keeping current): {e}");
                        }
                    }
                }
                Err(e) => tracing::warn!("Config watcher error: {e}"),
                _ => {}
            })?;

        watcher.watch(path.as_ref(), RecursiveMode::NonRecursive)?;
        Ok((rx, watcher))
    }
}
