//! Real-Debrid client module.
//!
//! Exports the `RealDebrid` struct (Tasks 5 & 6), CDN selection (Task 7),
//! and re-exports all types.

pub mod api;
pub mod bandwidth_reset;
pub mod cdn;
pub mod client;
pub mod token_pool;
pub mod traffic_snapshot;
pub mod types;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arc_swap::ArcSwap;
use reqwest::ClientBuilder;

use crate::config::Config;
use crate::rd::types::TrafficDetailsSnapshot;
use client::{Credentials, RateLimiter, RdClient, RdClientConfig};
use token_pool::TokenPool;

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
    /// Round-robin token pool for the download client; rotated on bandwidth limit responses.
    pub token_pool: Arc<TokenPool>,
    pub config: Arc<ArcSwap<Config>>,
    pub ranked_hosts: Arc<arc_swap::ArcSwapOption<cdn::RankedHosts>>,
    /// Latest per-token `GET /traffic/details` snapshot when refresh is enabled in config.
    pub traffic_details: Arc<arc_swap::ArcSwapOption<TrafficDetailsSnapshot>>,
    /// Shared, hot-swappable credentials. Updated atomically by [`reload_credentials`](Self::reload_credentials).
    pub credentials: Arc<ArcSwap<Credentials>>,
}

impl RealDebrid {
    pub fn new(cfg: &Config) -> Result<Self> {
        Self::build(cfg, 30)
    }

    /// Like [`new`](Self::new), but sets the global CDN download connection semaphore
    /// (production uses 30 to stay under RD's ~32 connection cap).
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

        // Shared credentials — all three clients point at the same ArcSwap.
        let credentials = Arc::new(ArcSwap::from_pointee(Credentials {
            token: Arc::from(cfg.token.as_str()),
            download_tokens: cfg
                .all_download_tokens()
                .into_iter()
                .map(Arc::from)
                .collect(),
        }));

        // ── api_client: HTTP/2, rate-limited, authenticated ────────────────
        let api_http = ClientBuilder::new()
            .timeout(timeout)
            .pool_max_idle_per_host(100)
            .build()?;

        let api_client = Arc::new(RdClient::new(
            api_http,
            RdClientConfig {
                credentials: credentials.clone(),
                rate_limiter: Some(api_rl),
                max_retries,
                timeout,
                is_download_client: false,
                download_token_pool: None,
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
                credentials: credentials.clone(),
                rate_limiter: None,
                max_retries,
                timeout: Duration::from_secs(30),
                is_download_client: false,
                download_token_pool: None,
            },
        ));

        // ── download_client: HTTP/1.1, unauthenticated ─────────────────────
        // Disable HTTP/2: RD .com CDN servers only support HTTP/1.1.
        // `pool_max_idle_per_host` is only the idle connection cache for this Client;
        // concurrent CDN usage is capped by `connection_semaphore` (see worker + verify).
        let download_read = Duration::from_secs(cfg.api.download_read_timeout_secs.max(1));
        let download_http = ClientBuilder::new()
            .http1_only()
            .pool_max_idle_per_host(32)
            .timeout(download_read)
            .build()?;

        // Build the token pool first so we can share it with the download client config.
        let token_pool = Arc::new(TokenPool::new(cfg.all_download_tokens())); // TokenPool::new wraps in Arc internally

        let download_client = Arc::new(RdClient::new(
            download_http,
            RdClientConfig {
                credentials: credentials.clone(),
                rate_limiter: None,
                max_retries,
                timeout: download_read,
                is_download_client: true,
                download_token_pool: Some(token_pool.clone()),
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
            token_pool,
            config: Arc::new(ArcSwap::from_pointee(cfg.clone())),
            ranked_hosts: Arc::new(arc_swap::ArcSwapOption::new(None)),
            traffic_details: Arc::new(arc_swap::ArcSwapOption::new(None)),
            credentials,
        })
    }

    /// Hot-swaps the RD credentials (token + download tokens) without disrupting
    /// any HTTP connections, semaphores, or CDN state.
    ///
    /// Safe to call from the config-watcher task; all three clients see the new
    /// credentials on their very next `execute()` loop iteration.
    /// Hot-swaps the RD credentials (token + download tokens) without disrupting
    /// any HTTP connections, semaphores, or CDN state.
    ///
    /// Safe to call from the config-watcher task; all three clients see the new
    /// credentials on their very next `execute()` loop iteration.
    pub fn reload_credentials(&self, new_cfg: &Config) {
        let arc_tokens: Vec<Arc<str>> = new_cfg
            .all_download_tokens()
            .into_iter()
            .map(Arc::from)
            .collect();
        let new_creds = Credentials {
            token: Arc::from(new_cfg.token.as_str()),
            download_tokens: arc_tokens.clone(),
        };
        self.credentials.store(Arc::new(new_creds));
        self.token_pool.update_tokens(arc_tokens);
        tracing::info!("RD credentials hot-reloaded (token rotated, pool refreshed)");
    }

    /// Atomically update the shared config snapshot so that `api.*`, `cdn_mode`,
    /// `bandwidth_reset_timezone`, and other fields are picked up by the next caller
    /// that does `self.config.load()`.
    pub fn reload_config(&self, new_cfg: Config) {
        self.config.store(Arc::new(new_cfg));
        tracing::debug!("RD config snapshot hot-reloaded");
    }
}
