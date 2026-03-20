//! Real-Debrid client module.
//!
//! Exports the `RealDebrid` struct (Tasks 5 & 6), CDN selection (Task 7),
//! and re-exports all types.

pub mod api;
pub mod cdn;
pub mod client;
pub mod types;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use reqwest::ClientBuilder;

use crate::config::Config;
use client::{RateLimiter, RdClient, RdClientConfig};

// ─── RealDebrid ───────────────────────────────────────────────────────────────

/// Holds the three HTTP clients and provides all RD API operations.
///
/// Three clients mirror private zurg:
/// - `api_client`        — authenticated, rate-limited, HTTP/2, for all API calls
/// - `unrestrict_client` — authenticated, no rate limit, HTTP/2, for link unrestriction  
/// - `download_client`   — unauthenticated, HTTP/1.1, TLS 1.2+, for CDN byte-range reads
pub struct RealDebrid {
    pub api_client: Arc<RdClient>,
    pub unrestrict_client: Arc<RdClient>,
    pub download_client: Arc<RdClient>,
    pub torrents_rate_limiter: Arc<RateLimiter>,
    /// Global cap on concurrent CDN calls on [`Self::download_client`] (chunk `Range` GETs and link verify).
    pub connection_semaphore: Arc<tokio::sync::Semaphore>,
    pub config: Arc<Config>,
}

impl RealDebrid {
    pub fn new(cfg: &Config) -> Result<Self> {
        Self::build(cfg, 30)
    }

    /// Like [`new`](Self::new), but sets the global CDN download connection semaphore
    /// (production uses 30 to stay under RD’s ~32 connection cap).
    pub fn new_with_connection_limit(
        cfg: &Config,
        max_concurrent_download_connections: usize,
    ) -> Result<Self> {
        Self::build(cfg, max_concurrent_download_connections)
    }

    fn build(cfg: &Config, connection_semaphore_permits: usize) -> Result<Self> {
        let api_rl = RateLimiter::new(cfg.api.rate_limit_per_minute);
        let torrents_rl = RateLimiter::new(cfg.api.torrents_rate_limit_per_minute);
        let timeout = Duration::from_secs(cfg.api.timeout_secs);
        let max_retries = cfg.api.retries_until_failed;

        // ── api_client: HTTP/2, rate-limited, authenticated ────────────────
        let api_http = ClientBuilder::new()
            .timeout(timeout)
            .pool_max_idle_per_host(100)
            .build()?;

        let api_client = Arc::new(RdClient::new(
            api_http,
            RdClientConfig {
                token: cfg.token.clone(),
                rate_limiter: Some(api_rl),
                max_retries,
                timeout,
                is_download_client: false,
            },
        ));

        // ── unrestrict_client: HTTP/2, no rate limit, authenticated ────────
        let unrestrict_http = ClientBuilder::new()
            .timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(100)
            .build()?;

        let unrestrict_client = Arc::new(RdClient::new(
            unrestrict_http,
            RdClientConfig {
                token: cfg.token.clone(),
                rate_limiter: None,
                max_retries,
                timeout: Duration::from_secs(30),
                is_download_client: false,
            },
        ));

        // ── download_client: HTTP/1.1, unauthenticated ─────────────────────
        // Disable HTTP/2: RD .com CDN servers only support HTTP/1.1.
        // `pool_max_idle_per_host` is only the idle connection cache for this Client;
        // concurrent CDN usage is capped by `connection_semaphore` (see worker + verify).
        let download_http = ClientBuilder::new()
            .http1_only()
            .pool_max_idle_per_host(32)
            .build()?;

        let download_client = Arc::new(RdClient::new(
            download_http,
            RdClientConfig {
                token: String::new(), // no auth on download client
                rate_limiter: None,
                max_retries,
                timeout: Duration::from_secs(cfg.api.timeout_secs),
                is_download_client: true,
            },
        ));

        let permits = connection_semaphore_permits.max(1);
        let connection_semaphore = Arc::new(tokio::sync::Semaphore::new(permits));

        Ok(Self {
            api_client,
            unrestrict_client,
            download_client,
            torrents_rate_limiter: torrents_rl,
            connection_semaphore,
            config: Arc::new(cfg.clone()),
        })
    }
}
