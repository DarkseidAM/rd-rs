//! CDN network test entry point and cache.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::SystemTime;

use anyhow::Result;

use super::probe;
use super::types::{CACHE_TTL_SECS, NetworkTestResults};

/// Initialized once at startup from `cfg.cache_dir`. All CDN functions use
/// this to locate `cdn_cache/network_test_results.json` and the timestamp.
static CDN_CACHE_DIR: OnceLock<PathBuf> = OnceLock::new();

fn cdn_cache_dir() -> &'static Path {
    CDN_CACHE_DIR
        .get()
        .map(PathBuf::as_path)
        .unwrap_or_else(|| {
            tracing::warn!("CDN: cdn_cache_dir not initialised, falling back to ./data");
            Path::new("data")
        })
}

pub(super) fn results_file() -> PathBuf {
    cdn_cache_dir().join("network_test_results.json")
}

pub(super) fn timestamp_file() -> PathBuf {
    cdn_cache_dir().join("network_test_timestamp")
}

/// Run the CDN network test (or load from cache if recent enough).
pub async fn run_network_test(_rd: &crate::rd::RealDebrid, cfg: &crate::config::Config) {
    let cdn_dir = cfg.cache_dir.join("cdn_cache");
    CDN_CACHE_DIR.get_or_init(|| cdn_dir.clone());
    let _ = std::fs::create_dir_all(&cdn_dir);

    if let Some(cached) = load_cached_results() {
        if cached.ipv4_latency.is_empty() {
            tracing::info!("CDN: cached results are empty, re-running network test");
        } else {
            tracing::info!(
                "CDN: loaded cached results — {} IPv4, {} IPv6 hosts",
                cached.ipv4_latency.len(),
                cached.ipv6_latency.len(),
            );
            return;
        }
    }

    run_fresh_network_test().await;
}

/// Re-run latency discovery and persist results (ignores TTL). For periodic hot re-probe.
pub async fn rerun_cdn_network_test() {
    // cdn_cache_dir() is always set by `run_network_test` at startup before this is called.
    let _ = std::fs::create_dir_all(cdn_cache_dir());
    tracing::info!("CDN: re-probe (forced) starting…");
    run_fresh_network_test().await;
}

async fn run_fresh_network_test() {
    tracing::info!("CDN: fetching server list from Supabase…");
    match probe::fetch_server_list().await {
        Ok(entries) if !entries.is_empty() => {
            tracing::info!("CDN: {} servers in list", entries.len());
            let results = probe::run_latency_test_on_entries(entries).await;
            tracing::info!(
                "CDN: {} IPv4, {} IPv6 reachable from server list",
                results.ipv4_latency.len(),
                results.ipv6_latency.len(),
            );
            if let Err(e) = save_results(&results) {
                tracing::warn!("CDN: failed to persist results: {e}");
            }
        }
        Ok(_) | Err(_) => {
            tracing::info!("CDN: server list empty or unavailable, falling back to DNS probe…");
            let results = probe::dns_probe_fallback().await;
            tracing::info!(
                "CDN: {} IPv4 reachable from DNS probe",
                results.ipv4_latency.len()
            );
            if results.ipv4_latency.is_empty() {
                tracing::warn!("CDN: no hosts found, will use default DNS resolution");
            }
            if let Err(e) = save_results(&results) {
                tracing::warn!("CDN: failed to persist results: {e}");
            }
        }
    }
}

pub(super) fn load_cached_results() -> Option<NetworkTestResults> {
    let ts_bytes = std::fs::read(timestamp_file()).ok()?;
    let ts: u64 = std::str::from_utf8(&ts_bytes).ok()?.trim().parse().ok()?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs();

    if now.saturating_sub(ts) >= CACHE_TTL_SECS {
        return None;
    }

    let json = std::fs::read(results_file()).ok()?;
    serde_json::from_slice(&json).ok()
}

fn save_results(results: &NetworkTestResults) -> Result<()> {
    let json = serde_json::to_vec_pretty(results)?;
    std::fs::write(results_file(), json)?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();
    std::fs::write(timestamp_file(), now.to_string())?;

    Ok(())
}
