//! RealDebrid API methods: user, torrents list, torrent info.

use std::sync::Arc;

use anyhow::{Context, Result};

use crate::rd::RealDebrid;
use crate::rd::client::RdError;
use crate::rd::types::{Torrent, TorrentInfo, User};

impl RealDebrid {
    pub async fn get_user(&self) -> Result<User> {
        let resp = self
            .api_client
            .execute(|_| {
                self.api_client
                    .client
                    .get(format!("{}/rest/1.0/user", self.config.api.base_url))
            })
            .await
            .context("get_user")?;
        let user: User = resp.json().await.context("get_user: decode")?;
        Ok(user)
    }

    pub async fn list_torrents(&self, page: u32, limit: u32) -> Result<(Vec<Torrent>, u32)> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let url = format!(
            "{}/rest/1.0/torrents?_t={ts}&page={page}&limit={limit}",
            self.config.api.base_url
        );

        self.torrents_rate_limiter.wait().await;

        let resp = self
            .api_client
            .execute(|_| self.api_client.client.get(&url))
            .await
            .context("list_torrents")?;

        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok((Vec::new(), 0));
        }

        let total: u32 = resp
            .headers()
            .get("x-total-count")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let torrents: Vec<Torrent> = resp.json().await.context("list_torrents: decode")?;
        Ok((torrents, total))
    }

    pub async fn list_all_torrents(self: &Arc<Self>) -> Result<Vec<Torrent>> {
        let page_size = self.config.api.fetch_torrents_page_size;

        let (first_page, total) = self.list_torrents(1, page_size).await?;
        let total_pages = total.div_ceil(page_size.max(1));

        tracing::debug!(
            "list_all_torrents: page 1/{} → {} torrents (total={})",
            total_pages,
            first_page.len(),
            total,
        );

        if total == 0 || total_pages <= 1 {
            return Ok(first_page);
        }

        let mut all = first_page;
        let mut set = tokio::task::JoinSet::new();

        for page in 2..=total_pages {
            let client = Arc::clone(self);
            set.spawn(async move {
                let (page_torrents, _) = client.list_torrents(page, page_size).await?;
                Ok::<(u32, Vec<Torrent>), anyhow::Error>((page, page_torrents))
            });
        }

        let mut pages_results: Vec<(u32, Vec<Torrent>)> =
            Vec::with_capacity((total_pages - 1) as usize);
        while let Some(res) = set.join_next().await {
            match res {
                Ok(Ok((page, torrents))) => {
                    tracing::debug!(
                        "list_all_torrents: page {page}/{total_pages} → {} torrents",
                        torrents.len()
                    );
                    pages_results.push((page, torrents));
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(anyhow::anyhow!("Join error fetching torrents: {}", e)),
            }
        }

        pages_results.sort_by_key(|(p, _)| *p);

        for (_, torrents) in pages_results {
            all.extend(torrents);
        }

        tracing::info!("list_all_torrents: fetched {} total torrents", all.len());
        Ok(all)
    }

    pub async fn get_torrent_info(&self, id: &str) -> Result<TorrentInfo, RdError> {
        let url = format!("{}/rest/1.0/torrents/info/{id}", self.config.api.base_url);
        let resp = self
            .api_client
            .execute(|_| self.api_client.client.get(&url))
            .await?;
        let info: TorrentInfo = resp.json().await.map_err(RdError::Network)?;
        Ok(info)
    }
}
