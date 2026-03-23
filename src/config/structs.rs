//! Config and sub-config structs.

use std::path::PathBuf;

use serde::Deserialize;

use super::defaults;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// Primary RD token.
    pub token: String,

    /// Where to mount the FUSE filesystem.
    #[serde(default = "defaults::default_mount_path")]
    pub mount_path: PathBuf,

    /// Directory for VFS disk cache and SQLite state.
    #[serde(default = "defaults::default_cache_dir")]
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct OnLibraryUpdateConfig {
    /// Shell command to run. `%s` is replaced with the changed path.
    #[serde(default)]
    pub command: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RepairConfig {
    #[serde(default = "defaults::bool_true")]
    pub enable: bool,

    #[serde(default = "defaults::default_repair_every_mins")]
    pub every_mins: u64,

    #[serde(default = "defaults::default_repair_timeout_mins")]
    pub timeout_mins: u64,

    #[serde(default = "defaults::default_stalled_download_mins")]
    pub stalled_download_mins: u64,

    #[serde(default)]
    pub restrict_to_cached: bool,

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

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "defaults::default_rate_limit")]
    pub rate_limit_per_minute: u32,

    #[serde(default = "defaults::default_torrents_rate_limit")]
    pub torrents_rate_limit_per_minute: u32,

    #[serde(default = "defaults::default_page_size")]
    pub fetch_torrents_page_size: u32,

    #[serde(default = "defaults::default_timeout_secs")]
    pub timeout_secs: u64,

    #[serde(default = "defaults::default_retries")]
    pub retries_until_failed: u32,

    #[serde(default = "defaults::default_refresh_interval_secs")]
    pub refresh_interval_secs: u64,

    #[serde(default)]
    pub use_range_verification: bool,

    #[serde(default)]
    pub retain_non_rd_downloads: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 250,
            torrents_rate_limit_per_minute: 75,
            fetch_torrents_page_size: 5000,
            timeout_secs: 60,
            retries_until_failed: 2,
            refresh_interval_secs: 15,
            use_range_verification: false,
            retain_non_rd_downloads: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct VfsConfig {
    #[serde(default = "defaults::default_cache_max_size")]
    pub cache_max_size: String,

    #[serde(default = "defaults::default_cache_max_age")]
    pub cache_max_age: String,

    #[serde(default = "defaults::default_cache_min_free")]
    pub cache_min_free_space: String,

    /// RAM window per open file: bytes fetched from cache/HTTP for this FUSE fd but not yet returned
    /// in a later `read` (rclone-style `--buffer-size`). Sequential reads consume the front; seek
    /// drops the window. Effective size is clamped (see `fuse::vfs_read_buffer`).
    #[serde(default = "defaults::default_buffer_size")]
    pub buffer_size: String,

    #[serde(default = "defaults::default_read_ahead")]
    pub read_ahead: String,

    #[serde(default = "defaults::default_chunk_size")]
    pub chunk_size: String,

    #[serde(default = "defaults::default_parallel_streams")]
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
            max_parallel_streams: 8,
        }
    }
}
