//! Default value helpers for config structs (used by serde).

use std::path::PathBuf;

pub(super) fn bool_true() -> bool {
    true
}
pub(super) fn default_repair_every_mins() -> u64 {
    60
}
pub(super) fn default_repair_timeout_mins() -> u64 {
    30
}
pub(super) fn default_stalled_download_mins() -> u64 {
    10
}
pub(super) fn default_head_unreachable_threshold() -> usize {
    1
}
pub(super) fn default_head_check_min_interval_mins() -> u64 {
    30
}
pub(super) fn default_repair_batch_file_group_size() -> u32 {
    5
}
pub(super) fn default_rate_limit() -> u32 {
    250
}
pub(super) fn default_torrents_rate_limit() -> u32 {
    75
}
pub(super) fn default_page_size() -> u32 {
    5000
}
pub(super) fn default_timeout_secs() -> u64 {
    60
}
pub(super) fn default_download_read_timeout_secs() -> u64 {
    300
}
pub(super) fn default_bandwidth_reset_timezone() -> String {
    "Europe/Paris".to_string()
}
pub(super) fn default_unrestrict_cache_sweep_interval_mins() -> u64 {
    60
}
pub(super) fn default_retries() -> u32 {
    2
}
pub(super) fn default_refresh_interval_secs() -> u64 {
    15
}
pub(super) fn default_cdn_reprobe_interval_mins() -> u64 {
    0
}
pub(super) fn default_cache_max_size() -> String {
    "100G".into()
}
pub(super) fn default_cache_max_age() -> String {
    "24h".into()
}
pub(super) fn default_cache_min_free() -> String {
    "20G".into()
}
pub(super) fn default_buffer_size() -> String {
    "256M".into()
}
pub(super) fn default_read_ahead() -> String {
    "128M".into()
}
pub(super) fn default_chunk_size() -> String {
    "4M".into()
}
pub(super) fn default_parallel_streams() -> u32 {
    8
}
pub(super) fn default_attr_timeout_secs() -> u64 {
    60
}
pub(super) fn default_entry_timeout_secs() -> u64 {
    600
}

pub fn default_base_url() -> String {
    "https://api.real-debrid.com".to_string()
}

pub(super) fn default_mount_path() -> PathBuf {
    PathBuf::from("/mnt/zurg")
}
pub(super) fn default_cache_dir() -> PathBuf {
    PathBuf::from("/cache/zurg-vfs")
}
