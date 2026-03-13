//! Low-level HTTP client with retry, backoff, and rate limiting.
//!
//! This mirrors the retry logic from `internal/rdclient/retry.go` and
//! `internal/rdclient/client.go` in the private zurg repository.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, Response};
use thiserror::Error;
use tokio::time::{interval, sleep};

// ─── Error types ─────────────────────────────────────────────────────────────

/// Errors returned by the RD API JSON body (api.real-debrid.com).
#[derive(Debug, Error, Clone)]
pub enum ApiError {
    /// Rate limit (code 5, 34, 36) — retried forever with backoff.
    #[error("RD rate limit (code={code}): {message}")]
    RateLimit { code: i32, message: String },

    /// Traffic exhausted (code 23) — retry with warning.
    #[error("RD traffic exhausted: {message}")]
    TrafficExhausted { message: String },

    /// Internal RD error (code -1) — retry with backoff.
    #[error("RD internal error: {message}")]
    Internal { message: String },

    /// Fair usage limit (code 36) — retry.
    #[error("RD fair usage limit: {message}")]
    FairUsageLimit { message: String },

    /// Any other API error — not retried.
    #[error("RD API error (code={code}): {message}")]
    Other { code: i32, message: String },
}

impl ApiError {
    pub fn from_code(code: i32, message: String) -> Self {
        match code {
            5 | 34 | 429 => Self::RateLimit { code, message },
            23 => Self::TrafficExhausted { message },
            36 => Self::FairUsageLimit { message },
            -1 => Self::Internal { message },
            _ => Self::Other { code, message },
        }
    }

    /// Whether this error should be retried (indefinitely).
    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Self::RateLimit { .. }
                | Self::TrafficExhausted { .. }
                | Self::Internal { .. }
                | Self::FairUsageLimit { .. }
        )
    }
}

/// Errors from the RD download CDN (`*.download.real-debrid.*`).
/// These come as the `X-Error` header or non-200 response.
/// Do **not** retry these — clear unrestrict cache and return immediately so
/// the VFS read() logic can decide whether to repair.
#[derive(Debug, Error, Clone)]
pub enum DownloadError {
    #[error("invalid_download_code")]
    InvalidDownloadCode,
    #[error("failed_generation")]
    FailedGeneration,
    #[error("too_many_attempts")]
    TooManyAttempts,
    #[error("file_unavailable")]
    FileUnavailable,
    #[error("bytes_limit_reached")]
    BytesLimitReached,
    /// HTTP 5xx from RD download server (e.g. Cloudflare 52x/53x).
    #[error("server error (status={0})")]
    ServerError(u16),
    /// Other download error.
    #[error("download error: {0}")]
    Other(String),
}

impl DownloadError {
    pub fn from_header(msg: &str, status: u16) -> Self {
        match msg {
            "invalid_download_code" => Self::InvalidDownloadCode,
            "failed_generation" => Self::FailedGeneration,
            "too_many_attempts" => Self::TooManyAttempts,
            "file_unavailable" => Self::FileUnavailable,
            "bytes_limit_reached" => Self::BytesLimitReached,
            _ if (500..=599).contains(&status) => Self::ServerError(status),
            _ => Self::Other(msg.to_string()),
        }
    }
}

/// Unified error for `RdClient::execute`.
#[derive(Debug, Error)]
pub enum RdError {
    #[error("api error: {0}")]
    Api(#[from] ApiError),
    #[error("download error: {0}")]
    Download(#[from] DownloadError),
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("request cancelled")]
    Cancelled,
    #[error("max retries exceeded")]
    MaxRetriesExceeded,
}

// ─── Rate limiter ─────────────────────────────────────────────────────────────

/// Token-based rate limiter using a Tokio interval.
/// Each `wait()` consumes one token (one tick of the interval).
pub struct RateLimiter {
    interval: tokio::sync::Mutex<tokio::time::Interval>,
}

impl RateLimiter {
    /// Create a limiter that allows `rate_per_minute` requests per minute.
    pub fn new(rate_per_minute: u32) -> Arc<Self> {
        assert!(rate_per_minute > 0, "rate_per_minute must be > 0");
        let period = Duration::from_secs(60) / rate_per_minute;
        let mut iv = interval(period);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Arc::new(Self {
            interval: tokio::sync::Mutex::new(iv),
        })
    }

    pub async fn wait(&self) {
        self.interval.lock().await.tick().await;
    }
}

// ─── Backoff ──────────────────────────────────────────────────────────────────

/// Exponential backoff with jitter, capped at 60s.
/// `base_secs * 2^attempt`, max 60, plus up to 20% jitter.
pub fn backoff(attempt: u32, base_secs: u64) -> Duration {
    let secs = (base_secs * (1u64 << attempt.min(6))).min(60);
    let jitter = (secs as f64 * 0.20 * rand_fraction()) as u64;
    Duration::from_secs(secs + jitter)
}

/// Poor-man's deterministic fraction in [0, 1) using system time.
/// Avoids pulling in the `rand` crate for Phase 1.
fn rand_fraction() -> f64 {
    use std::time::SystemTime;
    let ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (ns % 1000) as f64 / 1000.0
}

// ─── RdClient ─────────────────────────────────────────────────────────────────

/// Configuration for one RD HTTP client instance.
pub struct RdClientConfig {
    /// Bearer token (empty for unauthenticated clients like download_client).
    pub token: String,
    /// Optional rate limiter (shared with other callers).
    pub rate_limiter: Option<Arc<RateLimiter>>,
    /// Maximum retries for transient errors (-1 = unlimited for API rate limits).
    pub max_retries: u32,
    /// Per-request timeout.
    pub timeout: Duration,
    /// True when this client makes requests to RD CDN download hosts.
    pub is_download_client: bool,
}

/// A configured `reqwest::Client` with RD-specific retry/backoff logic.
pub struct RdClient {
    pub(crate) client: Client,
    config: RdClientConfig,
}

impl RdClient {
    pub fn new(client: Client, config: RdClientConfig) -> Self {
        Self { client, config }
    }

    /// Execute a request with retry/backoff logic mirroring Go's `shouldRetry`.
    ///
    /// Returns `Ok(Response)` on the first successful response.  
    /// The caller is responsible for reading the body.
    pub async fn execute(
        &self,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Response, RdError> {
        let mut attempt: u32 = 0;
        loop {
            // Apply rate limiting before each attempt
            if let Some(rl) = &self.config.rate_limiter {
                rl.wait().await;
            }

            let req = build().bearer_auth(&self.config.token);

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) if e.is_timeout() || e.is_connect() || is_network_error(&e) => {
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

            // ── Download client errors ──────────────────────────────────────
            if self.config.is_download_client && !status.is_success() {
                let x_err = resp
                    .headers()
                    .get("X-Error")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                return Err(RdError::Download(DownloadError::from_header(
                    &x_err,
                    status.as_u16(),
                )));
            }

            // ── Successful response ─────────────────────────────────────────
            if status.is_success() {
                // For download GETs, require a Content-Range header (partial content)
                if self.config.is_download_client
                    && resp.headers().get(reqwest::header::CONTENT_RANGE).is_none()
                {
                    if attempt >= self.config.max_retries {
                        return Err(RdError::MaxRetriesExceeded);
                    }
                    let delay = backoff(attempt, 1);
                    tracing::info!(attempt, "No Content-Range header, retry in {delay:?}");
                    sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Ok(resp);
            }

            // ── API error body ──────────────────────────────────────────────
            let status = resp.status();

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
                    if api_err.should_retry() {
                        let delay = backoff(attempt, 1);
                        tracing::warn!(
                            attempt,
                            code = err_body.code,
                            "API rate-limit/transient, retry in {delay:?}"
                        );
                        sleep(delay).await;
                        attempt += 1;
                        // Don't enforce max_retries for rate-limit errors (retry forever)
                        continue;
                    }
                    return Err(RdError::Api(api_err));
                }
            }

            // Fallback if we couldn't parse the JSON or it wasn't JSON.
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
