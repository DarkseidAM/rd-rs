//! RealDebrid API methods: unrestrict, select files, delete, add magnet, downloads, verify.

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::rd::RealDebrid;
use crate::rd::api::helpers::{extract_base_download_url, urlencoding_encode};
use crate::rd::api::{UNRESTRICT_CACHE_TTL, UnrestrictCache, UnrestrictCacheKey};
use crate::rd::client::RdError;
use crate::rd::types::*;
use tokio::time::Instant;

impl RealDebrid {
    pub async fn unrestrict_link(
        &self,
        cache: &UnrestrictCache,
        link: &str,
    ) -> Result<Download, RdError> {
        let token = self.credentials.load().token.clone();
        let key = UnrestrictCacheKey::new(Arc::clone(&token), Arc::new(link.to_string()));

        if let Some(entry) = cache.get(&key) {
            let (dl, cached_at) = entry.value();
            if cached_at.elapsed() < UNRESTRICT_CACHE_TTL {
                return Ok(dl.clone());
            }
        }

        let form_body = format!("link={}", urlencoding_encode(link));
        let url = format!(
            "{}/rest/1.0/unrestrict/link",
            self.config.api.base_url.trim_end_matches('/')
        );
        let resp = self
            .unrestrict_client
            .execute(|_| {
                self.unrestrict_client
                    .client
                    .post(&url)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(form_body.clone())
            })
            .await?;

        let mut download: Download = resp.json().await.map_err(RdError::Network)?;
        download.generated_at = Some(chrono::Utc::now());
        download.token = (*token).clone();

        download.download = extract_base_download_url(&download.download);
        cache.insert(key, (download.clone(), Instant::now()));

        Ok(download)
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
        let resp = self
            .download_client
            .execute(|use_fallback| {
                let active_url = self.rewrite_download_url(url, use_fallback);
                self.download_client
                    .client
                    .head(active_url.as_deref().unwrap_or(url))
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
        let resp = self
            .download_client
            .execute(|use_fallback| {
                let active_url = self.rewrite_download_url(url, use_fallback);
                self.download_client
                    .client
                    .get(active_url.as_deref().unwrap_or(url))
                    .header("Range", "bytes=0-0")
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
