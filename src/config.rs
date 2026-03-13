//! Configuration loading and hot-reload.
//!
//! `Config::load(path)` reads a TOML file and validates it.
//! `Config::watch()` spawns a background watcher; changes are broadcast
//! via a `tokio::sync::watch` channel so downstream components can react
//! without a restart.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use serde::Deserialize;
use tokio::sync::watch;

// ─── Top-level ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Primary RD token.
    pub token: String,

    /// Where to mount the FUSE filesystem.
    #[serde(default = "default_mount_path")]
    pub mount_path: PathBuf,

    /// Directory for VFS disk cache and SQLite state.
    #[serde(default = "default_cache_dir")]
    pub cache_dir: PathBuf,

    /// Additional RD download tokens (rotated when primary hits bandwidth limit).
    #[serde(default)]
    pub download_tokens: Vec<String>,

    #[serde(default)]
    pub on_library_update: OnLibraryUpdateConfig,

    #[serde(default)]
    pub repair: RepairConfig,

    #[serde(default)]
    pub api: ApiConfig,

    #[serde(default)]
    pub vfs: VfsConfig,
}

fn default_mount_path() -> PathBuf {
    PathBuf::from("/mnt/zurg")
}

fn default_cache_dir() -> PathBuf {
    PathBuf::from("/cache/zurg-vfs")
}

// ─── [on_library_update] ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OnLibraryUpdateConfig {
    /// Shell command to run. `%s` is replaced with the changed path.
    #[serde(default)]
    pub command: String,
}

// ─── [repair] ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct RepairConfig {
    #[serde(default = "bool_true")]
    pub enable: bool,

    /// How often to run the repair loop (minutes).
    #[serde(default = "default_repair_every_mins")]
    pub every_mins: u64,

    /// Per-repair-job timeout (minutes).
    #[serde(default = "default_repair_timeout_mins")]
    pub timeout_mins: u64,

    /// Minutes since last byte written before declaring a download stalled.
    #[serde(default = "default_stalled_download_mins")]
    pub stalled_download_mins: u64,

    /// Only repair using torrents that are already cached on RD.
    #[serde(default)]
    pub restrict_to_cached: bool,

    /// Automatically delete RD torrents in error state.
    #[serde(default)]
    pub delete_error_torrents: bool,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            enable: true,
            every_mins: 60,
            timeout_mins: 30,
            stalled_download_mins: 10,
            restrict_to_cached: false,
            delete_error_torrents: false,
        }
    }
}

// ─── [api] ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_rate_limit")]
    pub rate_limit_per_minute: u32,

    #[serde(default = "default_torrents_rate_limit")]
    pub torrents_rate_limit_per_minute: u32,

    #[serde(default = "default_page_size")]
    pub fetch_torrents_page_size: u32,

    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "default_retries")]
    pub retries_until_failed: u32,

    /// false = HEAD (fast), true = GET bytes=0-0 (thorough).
    #[serde(default)]
    pub use_range_verification: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 250,
            torrents_rate_limit_per_minute: 75,
            fetch_torrents_page_size: 5000,
            timeout_secs: 60,
            retries_until_failed: 2,
            use_range_verification: false,
        }
    }
}

// ─── [vfs] ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct VfsConfig {
    #[serde(default = "default_cache_max_size")]
    pub cache_max_size: String,

    #[serde(default = "default_cache_max_age")]
    pub cache_max_age: String,

    #[serde(default = "default_cache_min_free")]
    pub cache_min_free_space: String,

    #[serde(default = "default_buffer_size")]
    pub buffer_size: String,

    #[serde(default = "default_read_ahead")]
    pub read_ahead: String,

    /// CRITICAL: benchmark to find best TTFB for your connection.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: String,

    /// CRITICAL: higher values cause severe slowdowns.
    #[serde(default = "default_read_wait")]
    pub read_wait: String,

    #[serde(default = "default_parallel_streams")]
    pub max_parallel_streams: u32,
}

impl Default for VfsConfig {
    fn default() -> Self {
        Self {
            cache_max_size: "100G".into(),
            cache_max_age: "24h".into(),
            cache_min_free_space: "20G".into(),
            buffer_size: "256M".into(),
            read_ahead: "128M".into(),
            chunk_size: "4M".into(),
            read_wait: "5ms".into(),
            max_parallel_streams: 8,
        }
    }
}

// ─── Default helpers ─────────────────────────────────────────────────────────

fn bool_true() -> bool {
    true
}
fn default_repair_every_mins() -> u64 {
    60
}
fn default_repair_timeout_mins() -> u64 {
    30
}
fn default_stalled_download_mins() -> u64 {
    10
}
fn default_rate_limit() -> u32 {
    250
}
fn default_torrents_rate_limit() -> u32 {
    75
}
fn default_page_size() -> u32 {
    5000
}
fn default_timeout_secs() -> u64 {
    60
}
fn default_retries() -> u32 {
    2
}
fn default_cache_max_size() -> String {
    "100G".into()
}
fn default_cache_max_age() -> String {
    "24h".into()
}
fn default_cache_min_free() -> String {
    "20G".into()
}
fn default_buffer_size() -> String {
    "256M".into()
}
fn default_read_ahead() -> String {
    "128M".into()
}
fn default_chunk_size() -> String {
    "4M".into()
}
fn default_read_wait() -> String {
    "5ms".into()
}
fn default_parallel_streams() -> u32 {
    8
}

// ─── Loading ─────────────────────────────────────────────────────────────────

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
    ///
    /// Returns a `watch::Receiver<Config>` that delivers new configs on change.
    /// The returned `RecommendedWatcher` must be kept alive (drop it to stop watching).
    pub fn watch(
        path: impl AsRef<std::path::Path> + Send + 'static,
    ) -> Result<(watch::Receiver<Config>, RecommendedWatcher)> {
        let initial = Config::load(path.as_ref())?;
        let (tx, rx) = watch::channel(initial);
        let path_clone = path.as_ref().to_path_buf();

        let mut watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                match res {
                    Ok(event) if event.kind.is_modify() => {
                        // Small debounce: wait briefly for the write to flush.
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
                }
            })?;

        watcher.watch(path.as_ref(), RecursiveMode::NonRecursive)?;
        Ok((rx, watcher))
    }
}
