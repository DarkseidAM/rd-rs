use reqwest::Url;
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::CdnMode;

/// In-memory holder for the fastest CDN host.
#[derive(Debug, Clone)]
pub struct RankedHosts {
    pub fastest_host: String,
    pub reachable_ipv4_hosts: Arc<Vec<String>>,
    pub ipv4_addresses: Arc<HashMap<String, String>>,
}

impl RankedHosts {
    /// Loads the latest NetworkTestResults from disk and parses the fastest host.
    /// Returns None if results don't exist, are expired, or empty.
    pub fn try_load() -> Option<Arc<Self>> {
        let results = super::run::load_cached_results()?;

        // Find the host with the minimum latency in a single pass O(N).
        // TODO: check for ipv6_latency also
        let (fastest, latency) = results
            .ipv4_latency
            .into_iter()
            .min_by(|a, b| a.1.total_cmp(&b.1))?;

        tracing::info!("RankedHosts: pinning to {} ({:.3}s)", fastest, latency);

        Some(Arc::new(Self {
            fastest_host: fastest.clone(),
            reachable_ipv4_hosts: Arc::new(results.ipv4_addresses.keys().cloned().collect()),
            ipv4_addresses: Arc::new(results.ipv4_addresses),
        }))
    }

    /// Rewrites a `.download.real-debrid.com` URL to use the fastest host instead.
    /// Retains the original path and scheme.
    /// Returns None if it's not a real-debrid CDN URL, parsing fails,
    /// or the URL is **already** on the pinned host (no-op rewrite avoided).
    pub fn rewrite_url(
        &self,
        download_url: &str,
        mode: CdnMode,
        location: Option<&str>,
    ) -> Option<String> {
        let mut parsed = Url::parse(download_url).ok()?;

        let host = parsed.host_str()?;
        if !is_rd_cdn_host(host) {
            return None;
        }

        let target_host: String = match mode {
            CdnMode::Auto => {
                // Preserve letter-prefixed geo hosts (zurg-style).
                if is_geo_prefixed(host) {
                    return None;
                }
                self.fastest_host.clone()
            }
            CdnMode::ForceCloudflare => return rewrite_to_cloudflare(&parsed),
            CdnMode::ForceNumbered => pick_numbered_host(host, &self.reachable_ipv4_hosts)?,
            CdnMode::ForceLocation => {
                let loc = location?;
                pick_location_host(host, loc, &self.reachable_ipv4_hosts)?
            }
        };

        // Skip rewrite if already on the chosen host — avoids wasting a fallback retry.
        if host == target_host {
            return None;
        }

        parsed.set_host(Some(&target_host)).ok()?;
        Some(parsed.to_string())
    }

    /// Returns a verified IPv4 address that matches `loc` (used for geo-aware unrestrict).
    pub fn geo_unrestrict_ip(&self, loc: &str, seed: &str) -> Option<String> {
        let host = pick_location_host(seed, loc, &self.reachable_ipv4_hosts)?;
        self.ipv4_addresses.get(&host).cloned()
    }
}

fn is_rd_cdn_host(host: &str) -> bool {
    host.ends_with(".download.real-debrid.com")
        || host.ends_with(".download.real-debrid.net")
        || host.ends_with(".download.real-debrid.cloud")
}

fn is_geo_prefixed(host: &str) -> bool {
    // e.g. "lax2-4.download.real-debrid.com" / "mum1-4.download.real-debrid.com"
    let prefix = host.split('.').next().unwrap_or(host);
    prefix
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        && prefix.contains('-')
}

fn is_numbered_prefix(prefix: &str) -> bool {
    !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_digit())
}

fn pick_numbered_host(current_host: &str, reachable: &[String]) -> Option<String> {
    let current_prefix = current_host.split('.').next().unwrap_or(current_host);
    if is_numbered_prefix(current_prefix) && reachable.iter().any(|h| h == current_host) {
        return Some(current_host.to_string());
    }

    // Deterministic “random”: hash the current host, then mod into candidates.
    let candidates: Vec<String> = reachable
        .iter()
        .map(|h| h.to_string())
        .filter(|h| is_numbered_prefix(h.split('.').next().unwrap_or(h)))
        .collect();
    pick_deterministic(current_host, &candidates)
}

fn pick_location_host(current_host: &str, loc: &str, reachable: &[String]) -> Option<String> {
    let loc = loc.trim().to_ascii_lowercase();
    let candidates: Vec<String> = reachable
        .iter()
        .map(|h| h.to_string())
        .filter(|h| {
            let prefix = h.split('.').next().unwrap_or(h);
            prefix.to_ascii_lowercase().starts_with(&loc)
        })
        .collect();
    pick_deterministic(current_host, &candidates)
}

fn pick_deterministic(seed: &str, candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    seed.hash(&mut hasher);
    let idx = (hasher.finish() as usize) % candidates.len();
    Some(candidates[idx].clone())
}

fn rewrite_to_cloudflare(parsed: &Url) -> Option<String> {
    let mut u = parsed.clone();
    let host = u.host_str()?;
    if host.ends_with(".download.real-debrid.cloud") {
        return None;
    }
    let prefix = host.split('.').next().unwrap_or(host);
    let new_host = format!("{prefix}.download.real-debrid.cloud");
    u.set_host(Some(&new_host)).ok()?;
    Some(u.to_string())
}
