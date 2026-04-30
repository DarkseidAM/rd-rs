//! RdClient and execute logic.

use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use reqwest::{Client, Response};
use tokio::time::sleep;

use super::credentials::Credentials;
use super::errors::{ApiError, DownloadError, RdError};
use super::rate_limit::{RateLimiter, backoff};
use crate::rd::token_pool::TokenPool;

const RETRY_BACKOFF_INTERNAL_SECS: u64 = 30;
const RETRY_BACKOFF_DEFAULT_SECS: u64 = 1;

pub struct RdClientConfig {
    /// Shared, hot-swappable credentials (token + download token list).
    pub credentials: Arc<ArcSwap<Credentials>>,
    pub rate_limiter: Option<Arc<RateLimiter>>,
    pub max_retries: u32,
    pub timeout: Duration,
    pub is_download_client: bool,
    /// Token pool for the download client (rotated on bandwidth limit). `None` for other clients.
    pub download_token_pool: Option<Arc<TokenPool>>,
}

pub struct RdClient {
    pub(crate) client: Client,
    config: RdClientConfig,
}

impl RdClient {
    pub fn new(client: Client, config: RdClientConfig) -> Self {
        Self { client, config }
    }

    pub async fn execute(
        &self,
        build: impl Fn(bool) -> reqwest::RequestBuilder,
    ) -> Result<Response, RdError> {
        self.execute_impl(None, build).await
    }

    /// Like [`Self::execute`], but always sends the given Bearer (for `POST /unrestrict/link`
    /// when trying alternate pool tokens). Ignores the download token pool rotation index.
    pub async fn execute_with_fixed_bearer(
        &self,
        bearer: Arc<str>,
        build: impl Fn(bool) -> reqwest::RequestBuilder,
    ) -> Result<Response, RdError> {
        self.execute_impl(Some(bearer), build).await
    }

    async fn execute_impl(
        &self,
        fixed_bearer: Option<Arc<str>>,
        build: impl Fn(bool) -> reqwest::RequestBuilder,
    ) -> Result<Response, RdError> {
        let mut attempt: u32 = 0;
        let max_token_attempts = self
            .config
            .download_token_pool
            .as_ref()
            .map(|p| p.len())
            .unwrap_or(1);
        let mut token_attempts: usize = 0;
        let mut use_fallback = false;

        loop {
            if let Some(rl) = &self.config.rate_limiter {
                rl.wait().await;
            }

            let creds = self.config.credentials.load();
            let active_token: Arc<str> = if let Some(b) = &fixed_bearer {
                b.clone()
            } else if let Some(pool) = &self.config.download_token_pool {
                pool.download_bearer()
            } else {
                creds.token.clone()
            };

            let req = build(use_fallback).bearer_auth(&*active_token);

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) if e.is_timeout() || e.is_connect() || is_network_error(&e) => {
                    if !use_fallback && attempt == 0 && self.config.is_download_client {
                        use_fallback = true;
                        tracing::warn!(
                            "Network error using pinned host, falling back to original URL: {e}"
                        );
                        continue;
                    }
                    if attempt >= self.config.max_retries {
                        tracing::error!(attempt, "Network error exceeded max retries: {e}");
                        return Err(RdError::MaxRetriesExceeded);
                    }
                    let delay = backoff(attempt, 1);
                    tracing::warn!(attempt, "Network error, retry in {delay:?}: {e}");
                    sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                Err(e) if e.is_request() && e.to_string().contains("context canceled") => {
                    return Err(RdError::Cancelled);
                }
                Err(e) => return Err(RdError::Network(e)),
            };

            let status = resp.status();

            if self.config.is_download_client && fixed_bearer.is_none() && !status.is_success() {
                let x_err = resp
                    .headers()
                    .get("X-Error")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let dl_err = DownloadError::from_header(&x_err, status.as_u16());
                let rd_err = RdError::Download(dl_err);

                if rd_err.is_bandwidth_limited()
                    && let Some(pool) = &self.config.download_token_pool
                {
                    pool.mark_exhausted(&active_token);
                    token_attempts += 1;
                    if token_attempts < max_token_attempts && pool.any_non_exhausted() {
                        tracing::warn!(
                            attempt = token_attempts,
                            max_token_attempts,
                            "Download bandwidth limit — trying next eligible token"
                        );
                        attempt = 0;
                        continue;
                    }
                }
                return Err(rd_err);
            }

            if status.is_success() {
                return Ok(resp);
            }

            if resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(|ct| ct.contains("application/json"))
                .unwrap_or(false)
            {
                #[derive(serde::Deserialize)]
                struct ApiErrBody {
                    #[serde(default)]
                    error: String,
                    #[serde(rename = "error_code", default)]
                    code: i32,
                }
                let body = resp.bytes().await.map_err(RdError::Network)?;
                if let Ok(err_body) = serde_json::from_slice::<ApiErrBody>(&body) {
                    let api_err = ApiError::from_code(err_body.code, err_body.error);
                    if api_err.is_bandwidth_limited() {
                        // TrafficExhausted (23) / FairUsageLimit (36): no point retrying this
                        // token — daily quota is exhausted. Return immediately so the caller
                        // can skip to the next token or surface the error.
                        return Err(RdError::Api(api_err));
                    }
                    if api_err.should_retry() {
                        // Distinguish back-off duration by error class:
                        //   RateLimit (5, 34, 429) → short exponential (base=1s)
                        //   Internal  (-1)          → longer base (30s)
                        let base_secs: u64 = match &api_err {
                            ApiError::Internal { .. } => RETRY_BACKOFF_INTERNAL_SECS,
                            _ => RETRY_BACKOFF_DEFAULT_SECS,
                        };
                        let delay = backoff(attempt, base_secs);
                        tracing::warn!(
                            attempt,
                            code = err_body.code,
                            "API rate-limit/transient, retry in {delay:?}"
                        );
                        sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(RdError::Api(api_err));
                }
            }

            if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                if attempt >= self.config.max_retries {
                    return Err(RdError::MaxRetriesExceeded);
                }
                let delay = backoff(attempt, 1);
                tracing::warn!(attempt, status = %status, "HTTP server error, retry in {delay:?}");
                sleep(delay).await;
                attempt += 1;
                continue;
            }

            return Err(RdError::Api(ApiError::Other {
                code: status.as_u16() as i32,
                message: format!("HTTP {}", status),
            }));
        }
    }
}

fn is_network_error(e: &reqwest::Error) -> bool {
    let s = e.to_string();
    s.contains("EOF")
        || s.contains("connection reset")
        || s.contains("broken pipe")
        || s.contains("timeout")
}
