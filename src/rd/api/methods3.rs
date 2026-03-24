//! `http_range_get` — byte-range HTTP download helper for `RealDebrid`.

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
    pub async fn http_range_get(
        &self,
        url: &str,
        range: &str,
    ) -> Result<reqwest::Response, RdError> {
        let max_retries = self.config.api.retries_until_failed;
        let mut attempt: u32 = 0;
        loop {
            let resp = self
                .download_client
                .execute(|| self.download_client.client.get(url).header("Range", range))
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
