//! CDN network test entry point and cache.

use std::time::SystemTime;

use anyhow::Result;

use super::probe;
use super::types::{CACHE_TTL_SECS, NetworkTestResults, RESULTS_FILE, TIMESTAMP_FILE};

/// Run the CDN network test (or load from cache if recent enough).
pub async fn run_network_test(_rd: &crate::rd::RealDebrid, _cfg: &crate::config::Config) {
    let _ = std::fs::create_dir_all("data");

    if let Some(cached) = load_cached_results() {
        if cached.ipv4_latency.is_empty() {
            tracing::info!("CDN: cached results are empty, re-running network test");
        } else {
            tracing::info!(
                "CDN: loaded {} cached IPv4 hosts",
                cached.ipv4_latency.len()
            );
            return;
        }
    }

    run_fresh_network_test().await;
}

/// Re-run latency discovery and persist results (ignores TTL). For periodic hot re-probe.
pub async fn rerun_cdn_network_test() {
    let _ = std::fs::create_dir_all("data");
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
                "CDN: {} IPv4 reachable from server list",
                results.ipv4_latency.len()
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
    let ts_bytes = std::fs::read(TIMESTAMP_FILE).ok()?;
    let ts: u64 = std::str::from_utf8(&ts_bytes).ok()?.trim().parse().ok()?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?
        .as_secs();

    if now.saturating_sub(ts) >= CACHE_TTL_SECS {
        return None;
    }

    let json = std::fs::read(RESULTS_FILE).ok()?;
    serde_json::from_slice(&json).ok()
}

fn save_results(results: &NetworkTestResults) -> Result<()> {
    let json = serde_json::to_vec_pretty(results)?;
    std::fs::write(RESULTS_FILE, json)?;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)?
        .as_secs();
    std::fs::write(TIMESTAMP_FILE, now.to_string())?;

    Ok(())
}
