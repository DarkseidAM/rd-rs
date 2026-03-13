//! CDN host selection — mirrors `internal/rdclient/ip.go`.
//!
//! Startup flow:
//!   1. Try to load cached results from `data/network_test_results.json` (< 24h old → reuse)
//!   2. Fetch Supabase server list (hostname|ip lines)
//!   3. If Supabase list is empty or fails → fall back to DNS probe of
//!      `{N}-4.download.real-debrid.com` (N = 1..ceiling) with dynamic
//!      ceiling extending by 30 whenever a reachable server is found, start 100
//!   4. Run parallel HEAD latency tests (pool of MAX_CONCURRENT tasks)
//!   5. Sort by latency; persist results to disk
//!
//! The latency map is used in Phase 3 to pick the fastest CDN host for downloads.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::time::Instant;

const SERVER_LIST_URL: &str =
    "https://nzimhzbfnannoxumremm.supabase.co/storage/v1/object/public/public-files/servers.txt";
const RESULTS_FILE: &str = "data/network_test_results.json";
const TIMESTAMP_FILE: &str = "data/network_test_timestamp";
const CACHE_TTL_SECS: u64 = 24 * 3600;
const MAX_CONCURRENT: usize = 8;

/// Initial ceiling for the dynamic DNS probe (matches Zurg).
const DNS_PROBE_INITIAL_CEILING: u32 = 100;
/// Extend ceiling by this much when we find a new reachable server.
const DNS_PROBE_CEILING_EXTENSION: u32 = 30;

// ─── Persistence types ───────────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NetworkTestResults {
    pub ipv4_latency: HashMap<String, f64>,
    pub ipv6_latency: HashMap<String, f64>,
    /// hostname → IP (for DNS bypass in download_client)
    pub ipv4_addresses: HashMap<String, String>,
    pub ipv6_addresses: HashMap<String, String>,
}

// ─── Entry from server list ───────────────────────────────────────────────────

struct ServerEntry {
    hostname: String,
    ipv4: Option<String>,
    ipv6: Option<String>,
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Run the CDN network test (or load from cache if recent enough).
pub async fn run_network_test(_rd: &super::RealDebrid, _cfg: &crate::config::Config) {
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

    // 1. Try Supabase pre-computed list first.
    tracing::info!("CDN: fetching server list from Supabase…");
    match fetch_server_list().await {
        Ok(entries) if !entries.is_empty() => {
            tracing::info!("CDN: {} servers in list", entries.len());
            let results = run_latency_test_on_entries(entries).await;
            tracing::info!(
                "CDN: {} IPv4 reachable from server list",
                results.ipv4_latency.len()
            );
            if let Err(e) = save_results(&results) {
                tracing::warn!("CDN: failed to persist results: {e}");
            }
        }
        Ok(_) | Err(_) => {
            // Supabase list is empty or request failed → DNS probe fallback.
            tracing::info!("CDN: server list empty or unavailable, falling back to DNS probe…");
            let results = dns_probe_fallback().await;
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

// ─── DNS probe fallback ───────────────────────────────────────────────────────

/// Probe `{N}-4.download.real-debrid.com` for N in 1..ceiling.
/// The ceiling starts at DNS_PROBE_INITIAL_CEILING and extends by
/// DNS_PROBE_CEILING_EXTENSION whenever a reachable server is found
/// (mirrors Zurg's dynamic ceiling logic).
async fn dns_probe_fallback() -> NetworkTestResults {
    let client = Arc::new(
        Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build client"),
    );

    let ceiling = Arc::new(AtomicU32::new(DNS_PROBE_INITIAL_CEILING));
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));

    // Shared results collected from tasks
    let ipv4_latency: Arc<tokio::sync::Mutex<HashMap<String, f64>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let mut n = 0u32;
    let mut handles = Vec::new();

    // We submit jobs in batches up to the current ceiling; the ceiling may
    // grow as results come in.  To avoid a complex multi-pass loop we
    // pre-allocate up to the initial ceiling, then check if we need more.
    loop {
        let current_ceiling = ceiling.load(Ordering::Acquire);
        while n < current_ceiling {
            n += 1;
            let hostname = format!("{n}-4.download.real-debrid.com");
            let client = client.clone();
            let sem = sem.clone();
            let ceiling = ceiling.clone();
            let ipv4_latency = ipv4_latency.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.ok()?;

                // DNS resolve first — skip if host doesn't exist.
                use std::net::ToSocketAddrs;
                let addr_str = format!("{hostname}:443");
                let resolved_ip: Option<String> = tokio::task::spawn_blocking({
                    let a = addr_str.clone();
                    move || {
                        a.to_socket_addrs()
                            .ok()?
                            .find(|a| a.is_ipv4())
                            .map(|a| a.ip().to_string())
                    }
                })
                .await
                .ok()
                .flatten();

                let ip = resolved_ip?;

                // Measure latency via 3 HEAD requests (avg).
                let test_url = format!(
                    "https://{}/speedtest/test.rar/{:.6}",
                    hostname,
                    rand_fraction()
                );
                let latency = measure_avg_latency(&client, &test_url, 3).await?;

                // Extend the ceiling.
                let subdomain_n = n;
                let new_ceil = subdomain_n + DNS_PROBE_CEILING_EXTENSION;
                let mut old = ceiling.load(Ordering::Acquire);
                loop {
                    if new_ceil <= old {
                        break;
                    }
                    match ceiling.compare_exchange(
                        old,
                        new_ceil,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            tracing::debug!("CDN: extended ceiling to {new_ceil}");
                            break;
                        }
                        Err(actual) => old = actual,
                    }
                }

                ipv4_latency.lock().await.insert(hostname.clone(), latency);
                Some((hostname, ip))
            }));
        }

        // Wait for the current batch.
        // We re-check after so the loop terminates once ceiling stops growing.
        for h in handles.drain(..) {
            let _ = h.await;
        }

        let new_ceiling = ceiling.load(Ordering::Acquire);
        if new_ceiling <= n {
            break; // no extension happened in this batch
        }
    }

    let latency_map = ipv4_latency.lock().await.clone();

    // Sort for logging
    let mut sorted: Vec<_> = latency_map.iter().collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal));
    if let Some((host, latency)) = sorted.first() {
        tracing::info!("CDN: fastest host = {} ({:.3}s)", host, latency);
    }

    NetworkTestResults {
        ipv4_latency: latency_map,
        ..Default::default()
    }
}

// ─── Supabase-list path ───────────────────────────────────────────────────────

async fn run_latency_test_on_entries(entries: Vec<ServerEntry>) -> NetworkTestResults {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::with_capacity(entries.len());

    for entry in entries {
        let sem = sem.clone();
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            let test_url = format!("https://{}/__test", entry.hostname);
            let start = Instant::now();
            // Any response (even 404) means the host is reachable.
            let _ = c.head(&test_url).send().await;
            let latency = start.elapsed().as_secs_f64();
            Some((entry, latency))
        }));
    }

    let mut ipv4_latency = HashMap::new();
    let mut ipv4_addresses = HashMap::new();
    let mut ipv6_latency = HashMap::new();
    let mut ipv6_addresses = HashMap::new();

    for h in handles {
        if let Ok(Some((entry, latency))) = h.await {
            // We use the same latency for both v4 and v6 for now.
            if let Some(ipv4) = entry.ipv4 {
                ipv4_latency.insert(entry.hostname.clone(), latency);
                ipv4_addresses.insert(entry.hostname.clone(), ipv4);
            }
            if let Some(ipv6) = entry.ipv6 {
                ipv6_latency.insert(entry.hostname.clone(), latency);
                ipv6_addresses.insert(entry.hostname, ipv6);
            }
        }
    }

    let mut sorted: Vec<_> = ipv4_latency.iter().collect();
    sorted.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal));
    if let Some((host, latency)) = sorted.first() {
        tracing::info!("CDN: fastest IPv4 host = {} ({:.3}s)", host, latency);
    }

    NetworkTestResults {
        ipv4_latency,
        ipv4_addresses,
        ipv6_latency,
        ipv6_addresses,
    }
}

// ─── Supabase fetch ───────────────────────────────────────────────────────────

async fn fetch_server_list() -> Result<Vec<ServerEntry>> {
    let client = Client::builder().timeout(Duration::from_secs(10)).build()?;

    let text = client.get(SERVER_LIST_URL).send().await?.text().await?;

    let mut map: HashMap<String, ServerEntry> = HashMap::new();

    for line in text.lines() {
        let mut parts = line.splitn(2, '|');
        if let (Some(hostname), Some(ip)) = (parts.next(), parts.next()) {
            let hostname = hostname.trim().to_string();
            let ip = ip.trim().to_string();

            if hostname.is_empty() || ip.is_empty() || hostname.starts_with("generated") {
                continue;
            }
            if !hostname.contains(".download.real-debrid.com")
                && !hostname.contains(".download.real-debrid.net")
            {
                continue;
            }

            let entry = map.entry(hostname.clone()).or_insert(ServerEntry {
                hostname,
                ipv4: None,
                ipv6: None,
            });

            if ip.contains(':') {
                entry.ipv6 = Some(ip);
            } else {
                entry.ipv4 = Some(ip);
            }
        }
    }

    Ok(map.into_values().collect())
}

// ─── Latency helpers ──────────────────────────────────────────────────────────

/// Average of `iters` HEAD requests to `url`. Returns None if all fail.
async fn measure_avg_latency(client: &Client, url: &str, iters: usize) -> Option<f64> {
    let mut total = 0.0f64;
    let mut ok = 0usize;
    for _ in 0..iters {
        let start = Instant::now();
        if client.head(url).send().await.is_ok() {
            total += start.elapsed().as_secs_f64();
            ok += 1;
        }
    }
    if ok == 0 {
        None
    } else {
        Some(total / ok as f64)
    }
}

/// Cheap pseudo-random fraction in [0,1) using nanosecond timestamp.
fn rand_fraction() -> f64 {
    let ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (ns % 1_000_000) as f64 / 1_000_000.0
}

// ─── Cache helpers ────────────────────────────────────────────────────────────

fn load_cached_results() -> Option<NetworkTestResults> {
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
