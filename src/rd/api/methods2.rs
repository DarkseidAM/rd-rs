//! RealDebrid API methods: unrestrict, select files, delete, add magnet, downloads, verify.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};

use crate::rd::RealDebrid;
use crate::rd::api::helpers::{extract_base_download_url, urlencoding_encode};
use crate::rd::api::{UNRESTRICT_CACHE_TTL, UnrestrictCache, UnrestrictCacheKey};
use crate::rd::client::{ApiError, RdError};
use crate::rd::types::*;
use tokio::time::Instant;

impl RealDebrid {
    pub async fn unrestrict_link(
        &self,
        cache: &UnrestrictCache,
        link: &str,
    ) -> Result<Download, RdError> {
        let link_arc = Arc::new(link.to_string());
        let mut form_body = format!("link={}", urlencoding_encode(link));
        if self.config.api.cdn_mode == crate::config::CdnMode::ForceLocation
            && let Some(loc) = self.config.api.cdn_location.as_deref()
            && let Some(pin) = &*self.ranked_hosts.load()
            && let Some(ip) = pin.geo_unrestrict_ip(loc, link)
        {
            form_body.push_str("&ip=");
            form_body.push_str(&urlencoding_encode(&ip));
        }
        let url = format!(
            "{}/rest/1.0/unrestrict/link",
            self.config.api.base_url.trim_end_matches('/')
        );

        let mut last_bandwidth: Option<RdError> = None;

        loop {
            let eligible = self.token_pool.eligible_tokens_in_order();
            let Some(token) = eligible.first().cloned() else {
                return Err(last_bandwidth.unwrap_or_else(|| {
                    RdError::Api(ApiError::TrafficExhausted {
                        message: "all RD tokens are bandwidth-exhausted for unrestrict".into(),
                    })
                }));
            };

            let key = UnrestrictCacheKey::new(Arc::clone(&token), Arc::clone(&link_arc));
            if let Some(entry) = cache.get(&key) {
                let (dl, cached_at) = entry.value();
                if cached_at.elapsed() < UNRESTRICT_CACHE_TTL {
                    return Ok(dl.clone());
                }
            }

            let resp = match self
                .unrestrict_client
                .execute_with_fixed_bearer(token.as_str(), |_| {
                    self.unrestrict_client
                        .client
                        .post(&url)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .body(form_body.clone())
                })
                .await
            {
                Ok(r) => r,
                Err(e) if e.is_bandwidth_limited() => {
                    self.token_pool.mark_exhausted(token.as_str());
                    last_bandwidth = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            };

            let mut download: Download = resp.json().await.map_err(RdError::Network)?;
            download.generated_at = Some(chrono::Utc::now());
            download.token = (*token).clone();
            download.download = extract_base_download_url(&download.download);
            cache.insert(key, (download.clone(), Instant::now()));
            return Ok(download);
        }
    }

    pub async fn select_torrent_files(&self, id: &str, files: &str) -> Result<(), RdError> {
        let url = format!(
            "{}/rest/1.0/torrents/selectFiles/{id}",
            self.config.api.base_url.trim_end_matches('/')
        );
        let body = format!("files={files}");
        self.api_client
            .execute(|_| {
                self.api_client
                    .client
                    .post(&url)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(body.clone())
            })
            .await?;
        Ok(())
    }

    pub async fn delete_torrent(&self, id: &str) -> Result<(), RdError> {
        let url = format!(
            "{}/rest/1.0/torrents/delete/{id}",
            self.config.api.base_url.trim_end_matches('/')
        );
        self.api_client
            .execute(|_| self.api_client.client.delete(&url))
            .await?;
        Ok(())
    }

    pub async fn add_magnet(&self, hash: &str) -> Result<MagnetResponse, RdError> {
        let body = format!("magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A{hash}");
        let url = format!(
            "{}/rest/1.0/torrents/addMagnet",
            self.config.api.base_url.trim_end_matches('/')
        );
        let resp = self
            .api_client
            .execute(|_| {
                self.api_client
                    .client
                    .post(&url)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(body.clone())
            })
            .await?;
        let mr: MagnetResponse = resp.json().await.map_err(RdError::Network)?;
        Ok(mr)
    }

    pub async fn get_active_count(&self) -> Result<ActiveTorrentCountResponse> {
        let url = format!(
            "{}/rest/1.0/torrents/activeCount",
            self.config.api.base_url.trim_end_matches('/')
        );
        let resp = self
            .api_client
            .execute(|_| self.api_client.client.get(&url))
            .await
            .context("get_active_count")?;
        let r: ActiveTorrentCountResponse =
            resp.json().await.context("get_active_count: decode")?;
        Ok(r)
    }

    pub async fn get_traffic_details(&self) -> Result<TrafficDetailsResponse> {
        let url = format!(
            "{}/rest/1.0/traffic/details",
            self.config.api.base_url.trim_end_matches('/')
        );
        let resp = self
            .api_client
            .execute(|_| self.api_client.client.get(&url))
            .await
            .context("get_traffic_details")?;
        let t: TrafficDetailsResponse = resp.json().await.context("get_traffic_details: decode")?;
        Ok(t)
    }

    pub async fn get_downloads(&self, page: u32, limit: u32) -> Result<Vec<DownloadItem>> {
        let url = format!(
            "{}/rest/1.0/downloads?page={}&limit={}",
            self.config.api.base_url.trim_end_matches('/'),
            page,
            limit
        );
        let resp = self
            .api_client
            .execute(|_| self.api_client.client.get(&url))
            .await
            .context("get_downloads")?;
        let items: Vec<DownloadItem> = resp.json().await.context("get_downloads: decode")?;
        Ok(items)
    }

    pub async fn verify_link(&self, url: &str) -> Result<()> {
        if self.config.api.use_range_verification {
            self.verify_range(url).await
        } else {
            self.verify_head(url).await
        }
    }

    async fn verify_head(&self, url: &str) -> Result<()> {
        let _permit = self
            .connection_semaphore
            .acquire()
            .await
            .context("verify_head: connection semaphore closed")?;
        let api_to = Duration::from_secs(self.config.api.timeout_secs.max(1));
        let resp = self
            .download_client
            .execute(|use_fallback| {
                let active_url = self.rewrite_download_url(url, use_fallback);
                self.download_client
                    .client
                    .head(active_url.as_deref().unwrap_or(url))
                    .timeout(api_to)
            })
            .await
            .context("verify_head")?;
        anyhow::ensure!(
            resp.status().is_success(),
            "verify_head: unexpected status {}",
            resp.status()
        );
        Ok(())
    }

    async fn verify_range(&self, url: &str) -> Result<()> {
        let _permit = self
            .connection_semaphore
            .acquire()
            .await
            .context("verify_range: connection semaphore closed")?;
        let api_to = Duration::from_secs(self.config.api.timeout_secs.max(1));
        let resp = self
            .download_client
            .execute(|use_fallback| {
                let active_url = self.rewrite_download_url(url, use_fallback);
                self.download_client
                    .client
                    .get(active_url.as_deref().unwrap_or(url))
                    .header("Range", "bytes=0-0")
                    .timeout(api_to)
            })
            .await
            .context("verify_range")?;
        anyhow::ensure!(
            resp.status().as_u16() == 206 || resp.status().is_success(),
            "verify_range: unexpected status {}",
            resp.status()
        );
        Ok(())
    }
}
