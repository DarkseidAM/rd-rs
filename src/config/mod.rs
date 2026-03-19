//! Configuration loading and hot-reload.
//!
//! `Config::load(path)` reads a TOML file and validates it.
//! `Config::watch()` spawns a background watcher; changes are broadcast
//! via a `tokio::sync::watch` channel so downstream components can react
//! without a restart.

mod defaults;
mod load;
mod structs;

pub use structs::{ApiConfig, Config, OnLibraryUpdateConfig, RepairConfig, VfsConfig};

/// Parse a human-readable byte-size string (e.g. `"100G"`, `"128M"`, `"4K"`) into bytes.
/// Supported suffixes (case-insensitive): `K`, `M`, `G`, `T`. No suffix = raw bytes.
pub fn parse_byte_size(s: &str) -> u64 {
    let s = s.trim();
    if s.is_empty() {
        return 0;
    }
    let (num_str, suffix) = s
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, ""));

    let base: u64 = num_str.trim().parse().unwrap_or(0);
    match suffix.trim().to_ascii_uppercase().as_str() {
        "K" | "KB" | "KIB" => base * 1_024,
        "M" | "MB" | "MIB" => base * 1_048_576,
        "G" | "GB" | "GIB" => base * 1_073_741_824,
        "T" | "TB" | "TIB" => base * 1_099_511_627_776,
        _ => base,
    }
}
