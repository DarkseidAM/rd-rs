//! CDN DNS probe and latency testing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use anyhow::Result;
use reqwest::Client;
use tokio::sync::Semaphore;
use tokio::time::Instant;

use super::types::{
    DNS_PROBE_CEILING_EXTENSION, DNS_PROBE_INITIAL_CEILING, MAX_CONCURRENT, NetworkTestResults,
    SERVER_LIST_URL, ServerEntry,
};

pub(super) async fn dns_probe_fallback() -> NetworkTestResults {
    let client = Arc::new(
        Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build client"),
    );

    let ceiling = Arc::new(AtomicU32::new(DNS_PROBE_INITIAL_CEILING));
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));

    let ipv4_latency: Arc<tokio::sync::Mutex<HashMap<String, f64>>> =
        Arc::new(tokio::sync::Mutex::new(HashMap::new()));

    let mut n = 0u32;
    let mut handles = Vec::new();

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

                let test_url = format!(
                    "https://{}/speedtest/test.rar/{:.6}",
                    hostname,
                    rand_fraction()
                );
                let latency = measure_avg_latency(&client, &test_url, 3).await?;

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

        for h in handles.drain(..) {
            let _ = h.await;
        }

        let new_ceiling = ceiling.load(Ordering::Acquire);
        if new_ceiling <= n {
            break;
        }
    }

    let latency_map = ipv4_latency.lock().await.clone();

    if let Some((host, latency)) = latency_map.iter().min_by(|a, b| a.1.total_cmp(b.1)) {
        tracing::info!(
            "CDN: fastest host (dns-fallback) = {} ({:.3}s)",
            host,
            latency
        );
    }

    NetworkTestResults {
        ipv4_latency: latency_map,
        ..Default::default()
    }
}

pub(super) async fn run_latency_test_on_entries(entries: Vec<ServerEntry>) -> NetworkTestResults {
    // Split entries by which families they have addresses for.
    let mut ipv4_entries: Vec<(String, String)> = Vec::new(); // (hostname, ipv4)
    let mut ipv6_entries: Vec<(String, String)> = Vec::new(); // (hostname, ipv6)
    for e in &entries {
        if let Some(ip) = &e.ipv4 {
            ipv4_entries.push((e.hostname.clone(), ip.clone()));
        }
        if let Some(ip) = &e.ipv6 {
            ipv6_entries.push((e.hostname.clone(), ip.clone()));
        }
    }

    // Run both family probes in parallel.
    let (ipv4_result, ipv6_result) =
        tokio::join!(probe_family(ipv4_entries), probe_family(ipv6_entries),);

    let (ipv4_latency, ipv4_addresses) = ipv4_result;
    let (ipv6_latency, ipv6_addresses) = ipv6_result;

    if let Some((host, latency)) = ipv4_latency.iter().min_by(|a, b| a.1.total_cmp(b.1)) {
        tracing::info!(
            "CDN: fastest IPv4 host = {} ({:.3}s) [{} reachable]",
            host,
            latency,
            ipv4_latency.len()
        );
    }
    if let Some((host, latency)) = ipv6_latency.iter().min_by(|a, b| a.1.total_cmp(b.1)) {
        tracing::info!(
            "CDN: fastest IPv6 host = {} ({:.3}s) [{} reachable]",
            host,
            latency,
            ipv6_latency.len()
        );
    }

    NetworkTestResults {
        ipv4_latency,
        ipv4_addresses,
        ipv6_latency,
        ipv6_addresses,
    }
}

/// Probe a list of `(hostname, ip)` pairs over a single address family.
///
/// The reqwest client is pre-seeded with only the provided IPs so every TCP
/// connection is forced through the correct address family — no Happy Eyeballs
/// ambiguity.  Returns `(latency_map, address_map)`.
async fn probe_family(
    entries: Vec<(String, String)>,
) -> (HashMap<String, f64>, HashMap<String, String>) {
    if entries.is_empty() {
        return (HashMap::new(), HashMap::new());
    }

    let mut builder = Client::builder().timeout(Duration::from_secs(5));
    for (hostname, ip_str) in &entries {
        let clean = ip_str.trim_matches(|c| c == '[' || c == ']');
        if let Ok(ip) = clean.parse::<std::net::IpAddr>() {
            builder = builder.resolve(hostname, std::net::SocketAddr::new(ip, 443));
        }
    }
    let client = Arc::new(builder.build().expect("build family client"));
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::with_capacity(entries.len());

    for (hostname, ip) in entries {
        let c = client.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            let test_url = format!("https://{}/__test", hostname);
            let start = Instant::now();
            let _ = c.head(&test_url).send().await;
            let latency = start.elapsed().as_secs_f64();
            Some((hostname, ip, latency))
        }));
    }

    let mut latency_map = HashMap::new();
    let mut addr_map = HashMap::new();
    for h in handles {
        if let Ok(Some((hostname, ip, latency))) = h.await {
            latency_map.insert(hostname.clone(), latency);
            addr_map.insert(hostname, ip);
        }
    }
    (latency_map, addr_map)
}

pub(super) async fn fetch_server_list() -> Result<Vec<ServerEntry>> {
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

fn rand_fraction() -> f64 {
    let ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (ns % 1_000_000) as f64 / 1_000_000.0
}
