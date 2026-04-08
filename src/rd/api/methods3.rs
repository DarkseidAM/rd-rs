//! `http_range_get` — byte-range HTTP download helper for `RealDebrid`.

use std::time::Duration;

use reqwest::StatusCode;
use tokio::time::sleep;

use crate::rd::RealDebrid;
use crate::rd::client::{RdError, backoff};

impl RealDebrid {
    /// Perform an HTTP Range GET against a CDN download URL.
    ///
    /// Returns the raw bytes for the requested range. The `range` argument
    /// is the raw `Range:` header value, e.g. `"bytes=0-4194303"`.
    ///
    /// Uses `download_client` (the already-configured sub-client with its
    /// own rate-limiter and retry logic) so this shares the same
    /// connection pool and backoff machinery as `verify_link`.
    ///
    /// Retries with backoff when the response is successful but lacks a
    /// `Content-Range` header (same policy as before, scoped to range GETs only;
    /// `verify_head` uses HEAD and must not trigger this path).
    ///
    /// If the status is `200 OK` without `Content-Range`, returns
    /// [`RdError::RangeNotSupported`] immediately (unrestrict cannot fix that).
    ///
    /// Caller must hold `connection_semaphore` (see cache worker); this method does not acquire it.
    /// Applies CDN host pinning logic if a pinned host is available and fallback is not requested.
    /// Returns `Some(String)` if the URL was rewritten, otherwise `None` (use the original URL).
    pub(crate) fn rewrite_download_url(&self, url: &str, use_fallback: bool) -> Option<String> {
        if !use_fallback && let Some(pin) = &*self.ranked_hosts.load() {
            pin.rewrite_url(
                url,
                self.config.api.cdn_mode,
                self.config.api.cdn_location.as_deref(),
            )
        } else {
            None
        }
    }

    pub async fn http_range_get(
        &self,
        url: &str,
        range: &str,
    ) -> Result<reqwest::Response, RdError> {
        let max_retries = self.config.api.retries_until_failed;
        let read_to = Duration::from_secs(self.config.api.download_read_timeout_secs.max(1));
        let mut attempt: u32 = 0;
        loop {
            let resp = self
                .download_client
                .execute(|use_fallback| {
                    let active_url = self.rewrite_download_url(url, use_fallback);
                    self.download_client
                        .client
                        .get(active_url.as_deref().unwrap_or(url))
                        .header("Range", range)
                        .timeout(read_to)
                })
                .await?;

            if resp.headers().get(reqwest::header::CONTENT_RANGE).is_some() {
                return Ok(resp);
            }
            if resp.status() == StatusCode::OK {
                tracing::warn!(
                    url = %url,
                    range,
                    "CDN returned 200 (full body) for Range GET; \
                     server does not support Range — failing fast"
                );
                return Err(RdError::RangeNotSupported);
            }
            if attempt >= max_retries {
                return Err(RdError::MaxRetriesExceeded);
            }
            let delay = backoff(attempt, 1);
            tracing::info!(attempt, "No Content-Range header, retry in {delay:?}");
            sleep(delay).await;
            attempt += 1;
        }
    }
}
