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

    if let Some((host, latency)) = latency_map
        .iter()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
    {
        tracing::info!("CDN: fastest host = {} ({:.3}s)", host, latency);
    }

    NetworkTestResults {
        ipv4_latency: latency_map,
        ..Default::default()
    }
}

pub(super) async fn run_latency_test_on_entries(entries: Vec<ServerEntry>) -> NetworkTestResults {
    let mut builder = Client::builder().timeout(Duration::from_secs(5));

    // Pre-seed reqwest's DNS resolver map to absolutely bypass DNS lookups during the latency sweep.
    // This removes DNS fetching variability and strictly tests TCP/TLS rtt.
    for entry in &entries {
        if let Some(ipv4) = &entry.ipv4
            && let Ok(ip) = ipv4.parse::<std::net::IpAddr>()
        {
            builder = builder.resolve(&entry.hostname, std::net::SocketAddr::new(ip, 443));
        }
        // ipv6 addresses might contain brackets or port specs in some cases, handle standard parsing
        if let Some(ipv6) = &entry.ipv6
            && let Ok(ip) = ipv6
                .trim_matches(|c| c == '[' || c == ']')
                .parse::<std::net::IpAddr>()
        {
            builder = builder.resolve(&entry.hostname, std::net::SocketAddr::new(ip, 443));
        }
    }

    let client = builder.build().expect("build client");

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT));
    let mut handles = Vec::with_capacity(entries.len());

    for entry in entries {
        let sem = sem.clone();
        let c = client.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.ok()?;
            let test_url = format!("https://{}/__test", entry.hostname);
            let start = Instant::now();
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

    if let Some((host, latency)) = ipv4_latency
        .iter()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
    {
        tracing::info!("CDN: fastest IPv4 host = {} ({:.3}s)", host, latency);
    }

    NetworkTestResults {
        ipv4_latency,
        ipv4_addresses,
        ipv6_latency,
        ipv6_addresses,
    }
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
