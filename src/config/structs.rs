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

    /// Periodically probe unrestricted CDN URLs (HEAD or range per `[api]`) for playable files.
    #[serde(default = "defaults::bool_true")]
    pub head_check_enabled: bool,

    /// Enqueue repair when passive HEAD finds at least this many bad slots (≥ 1).
    #[serde(default = "defaults::default_head_unreachable_threshold")]
    pub head_unreachable_threshold: usize,

    /// Minimum wall time between passive HEAD runs for the same torrent.
    #[serde(default = "defaults::default_head_check_min_interval_mins")]
    pub head_check_min_interval_mins: u64,

    /// Max number of file IDs per Strategy 4 `select_torrent_files` batch (clamped 1–32).
    #[serde(default = "defaults::default_repair_batch_file_group_size")]
    pub batch_file_group_size: u32,
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
            head_check_enabled: true,
            head_unreachable_threshold: 1,
            head_check_min_interval_mins: 30,
            batch_file_group_size: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "defaults::default_base_url")]
    pub base_url: String,

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

    /// If > 0, re-run CDN latency test every N minutes and update pinned host (`RankedHosts`) without restart. 0 = disabled.
    #[serde(default = "defaults::default_cdn_reprobe_interval_mins")]
    pub cdn_reprobe_interval_mins: u64,

    /// Controls how we shape CDN download URLs (zurg-style).
    #[serde(default)]
    pub cdn_mode: CdnMode,

    /// Location/region code for `cdn_mode = "force_location"` (e.g. `"mum"`, `"lax"`, `"syd"`).
    /// Ignored for other modes.
    #[serde(default)]
    pub cdn_location: Option<String>,

    /// Max time for a single CDN Range GET (including reading the response body for that chunk). Separate from `timeout_secs` used for API calls.
    #[serde(default = "defaults::default_download_read_timeout_secs")]
    pub download_read_timeout_secs: u64,

    /// IANA timezone for scheduling daily clear of per-token bandwidth exhaustion flags (aligns with RD daily window, e.g. CET).
    #[serde(default = "defaults::default_bandwidth_reset_timezone")]
    pub bandwidth_reset_timezone: String,

    /// How often to drop in-memory unrestrict cache entries past the 4h TTL. 0 = disabled.
    #[serde(default = "defaults::default_unrestrict_cache_sweep_interval_mins")]
    pub unrestrict_cache_sweep_interval_mins: u64,

    /// Poll `GET /traffic/details` for every download token this often (seconds). Default 300 (5m, zurg-style). 0 = disabled.
    #[serde(default = "defaults::default_traffic_details_refresh_secs")]
    pub traffic_details_refresh_secs: u64,

    /// Include IPv6 CDN hosts when ranking the fastest download server.
    /// When `true` (default), ipv4 + ipv6 latency results are merged before selecting the
    /// fastest host. When `false`, only IPv4 hosts are considered.
    #[serde(default = "defaults::bool_true")]
    pub cdn_ipv6_enabled: bool,

    /// Force all CDN downloads over IPv6, discarding IPv4 hosts entirely.
    /// Only meaningful when `cdn_ipv6_enabled = true`. Default: `false`.
    #[serde(default)]
    pub cdn_force_ipv6: bool,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            base_url: defaults::default_base_url(),
            rate_limit_per_minute: 250,
            torrents_rate_limit_per_minute: 75,
            fetch_torrents_page_size: 5000,
            timeout_secs: 60,
            retries_until_failed: 2,
            refresh_interval_secs: 15,
            use_range_verification: false,
            retain_non_rd_downloads: false,
            cdn_reprobe_interval_mins: 0,
            cdn_mode: CdnMode::Auto,
            cdn_location: None,
            download_read_timeout_secs: 300,
            bandwidth_reset_timezone: defaults::default_bandwidth_reset_timezone(),
            unrestrict_cache_sweep_interval_mins: 60,
            traffic_details_refresh_secs: 300,
            cdn_ipv6_enabled: true,
            cdn_force_ipv6: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CdnMode {
    /// Default behavior: keep geo-prefixed hosts if RD assigned them; otherwise pin to fastest.
    #[default]
    Auto,
    /// Rewrite CDN host to `.download.real-debrid.cloud` (Cloudflare) variant.
    ForceCloudflare,
    /// Ensure we use a numbered host (e.g. `53.download.real-debrid.com`) chosen from reachable set.
    ForceNumbered,
    /// Force a location prefix by rewriting to a reachable host matching `cdn_location`.
    ForceLocation,
}

#[derive(Debug, Clone, Deserialize)]
pub struct VfsConfig {
    /// Maximum total size of the chunk cache on disk.
    ///
    /// **NOTE: requires a process restart** — the eviction loop reads this once at
    /// `Cache::new` time and does not pick up hot-reloads.
    #[serde(default = "defaults::default_cache_max_size")]
    pub cache_max_size: String,

    /// Maximum age of a cached chunk before it is evicted.
    ///
    /// **NOTE: requires a process restart** (same as `cache_max_size`).
    #[serde(default = "defaults::default_cache_max_age")]
    pub cache_max_age: String,

    /// Minimum free space to keep on the cache partition; eviction runs when this threshold is breached.
    ///
    /// **NOTE: requires a process restart** (same as `cache_max_size`).
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

    /// Kernel attribute cache timeout in seconds (equiv. rclone `--attr-timeout`).
    /// Higher values reduce `getattr` round-trips. Default: 60.
    #[serde(default = "defaults::default_attr_timeout_secs")]
    pub attr_timeout_secs: u64,

    /// Kernel directory-entry cache timeout in seconds (equiv. rclone `--dir-cache-time`).
    /// Default: 600.
    #[serde(default = "defaults::default_entry_timeout_secs")]
    pub entry_timeout_secs: u64,
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
            attr_timeout_secs: 60,
            entry_timeout_secs: 600,
        }
    }
}
