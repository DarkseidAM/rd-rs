//! `http_range_get` — byte-range HTTP download helper for `RealDebrid`.

use anyhow::{Context, Result};

use crate::rd::RealDebrid;

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
    /// Caller must hold `connection_semaphore` (see cache worker); this method does not acquire it.
    pub async fn http_range_get(&self, url: &str, range: &str) -> Result<reqwest::Response> {
        let resp = self
            .download_client
            .execute(|| self.download_client.client.get(url).header("Range", range))
            .await
            .with_context(|| format!("http_range_get: {range}"))?;

        let status = resp.status();
        // 206 Partial Content is expected; 200 OK is also acceptable if the
        // server ignores the Range header (rare but possible).
        if !status.is_success() {
            anyhow::bail!("http_range_get: unexpected status {status} for {range}");
        }

        Ok(resp)
    }
}
