//! Periodic `GET /traffic/details` per download token (zurg-style traffic refresher).

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;

use super::RealDebrid;
use crate::rd::types::{TrafficDetailsResponse, TrafficDetailsSnapshot};

impl RealDebrid {
    /// Fetches `/traffic/details` once per token in the pool and stores the result in
    /// [`Self::traffic_details`]. Failed tokens are skipped with a warning.
    pub async fn refresh_traffic_details_snapshot(&self) {
        let base = self
            .config
            .load()
            .api
            .base_url
            .trim_end_matches('/')
            .to_string();
        let url = format!("{base}/rest/1.0/traffic/details");
        let tokens = self.token_pool.tokens_in_order();
        let mut by_token = Vec::with_capacity(tokens.len());
        for token in tokens {
            match self.fetch_traffic_details_token(token.clone(), &url).await {
                Ok(details) => {
                    tracing::debug!(
                        days = details.len(),
                        "traffic/details refreshed for one token slot"
                    );
                    by_token.push((token, details));
                }
                Err(e) => tracing::warn!(
                    error = %e,
                    "traffic/details failed for one token slot; skipping"
                ),
            }
        }
        self.traffic_details
            .store(Some(Arc::new(TrafficDetailsSnapshot {
                fetched_at: Utc::now(),
                by_token,
            })));
    }

    async fn fetch_traffic_details_token(
        &self,
        token: Arc<str>,
        url: &str,
    ) -> Result<TrafficDetailsResponse> {
        let resp = self
            .api_client
            .execute_with_fixed_bearer(token, |_| self.api_client.client.get(url))
            .await
            .context("traffic/details HTTP")?;
        let t: TrafficDetailsResponse = resp.json().await.context("traffic/details decode")?;
        Ok(t)
    }
}
