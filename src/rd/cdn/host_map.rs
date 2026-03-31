use reqwest::Url;
use std::sync::Arc;

/// In-memory holder for the fastest CDN host.
#[derive(Debug, Clone)]
pub struct RankedHosts {
    pub fastest_host: String,
}

impl RankedHosts {
    /// Loads the latest NetworkTestResults from disk and parses the fastest host.
    /// Returns None if results don't exist, are expired, or empty.
    pub fn try_load() -> Option<Arc<Self>> {
        let results = super::run::load_cached_results()?;

        // Convert to Vec and sort by latency (lowest first)
        let mut sorted: Vec<_> = results.ipv4_latency.into_iter().collect();
        sorted.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let (fastest, latency) = sorted.first()?;
        tracing::info!("RankedHosts: pinning to {} ({:.3}s)", fastest, latency);

        Some(Arc::new(Self {
            fastest_host: fastest.clone(),
        }))
    }

    /// Rewrites a `.download.real-debrid.com` URL to use the fastest host instead.
    /// Retains the original path and scheme.
    /// Returns None if it's not a real-debrid CDN URL or parsing fails.
    pub fn rewrite_url(&self, download_url: &str) -> Option<String> {
        let mut parsed = Url::parse(download_url).ok()?;

        let host = parsed.host_str()?;
        if host.ends_with(".download.real-debrid.com")
            || host.ends_with(".download.real-debrid.net")
        {
            // Un-pinned URL -> Pinned URL
            parsed.set_host(Some(&self.fastest_host)).ok()?;
            return Some(parsed.to_string());
        }

        None
    }
}
