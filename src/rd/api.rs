//! RD API method implementations.
//!
//! All methods operate on `&RealDebrid` and return strongly-typed results.
//! Unrestrict cache: `DashMap<link, (Download, Instant)>` with 4-hour TTL.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use dashmap::DashMap;
use tokio::time::Instant;

use super::RealDebrid;
use super::client::RdError;
use super::types::*;

/// How long an unrestricted link is cached before re-unrestricting.
const UNRESTRICT_CACHE_TTL: Duration = Duration::from_secs(4 * 3600);

/// Key: RD link (e.g. `https://real-debrid.com/d/XXX`).
/// Value: (Download, cached_at Instant).
pub type UnrestrictCache = Arc<DashMap<String, (Download, Instant)>>;

/// Create a new unrestrict cache (call once in `RealDebrid::new`).
pub fn new_unrestrict_cache() -> UnrestrictCache {
    Arc::new(DashMap::new())
}

// ─── API methods ──────────────────────────────────────────────────────────────

impl RealDebrid {
    // ── User ──────────────────────────────────────────────────────────────────

    pub async fn get_user(&self) -> Result<User> {
        let resp = self
            .api_client
            .execute(|| {
                self.api_client
                    .client
                    .get("https://api.real-debrid.com/rest/1.0/user")
            })
            .await
            .context("get_user")?;
        let user: User = resp.json().await.context("get_user: decode")?;
        Ok(user)
    }

    // ── Torrents ──────────────────────────────────────────────────────────────

    /// List one page of torrents. `page` is 1-indexed (RD convention).
    /// Returns `(Vec<Torrent>, total_count)`.
    pub async fn list_torrents(&self, page: u32, limit: u32) -> Result<(Vec<Torrent>, u32)> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let url = format!(
            "https://api.real-debrid.com/rest/1.0/torrents?_t={ts}&page={page}&limit={limit}"
        );

        // Use the torrents rate limiter (separate budget from general API)
        self.torrents_rate_limiter.wait().await;

        let resp = self
            .api_client
            .execute(|| self.api_client.client.get(&url))
            .await
            .context("list_torrents")?;

        // RD returns 204 No Content when the page is out of range or list is empty.
        if resp.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok((Vec::new(), 0));
        }

        // x-total-count header holds the total across all pages.
        let total: u32 = resp
            .headers()
            .get("x-total-count")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let torrents: Vec<Torrent> = resp.json().await.context("list_torrents: decode")?;
        Ok((torrents, total))
    }

    /// Fetch all torrents by paginating until done in parallel.
    /// Uses x-total-count to calculate the number of pages.
    pub async fn list_all_torrents(self: &Arc<Self>) -> Result<Vec<Torrent>> {
        let page_size = self.config.api.fetch_torrents_page_size;

        // Fetch page 1 first to get the total count.
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

        // Spawn a task for each remaining page
        for page in 2..=total_pages {
            let client = Arc::clone(self);
            set.spawn(async move {
                let (page_torrents, _) = client.list_torrents(page, page_size).await?;
                Ok::<(u32, Vec<Torrent>), anyhow::Error>((page, page_torrents))
            });
        }

        // Collect results as they complete
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

        // Sort by page number to keep results ordered (RD api sorts by added desc)
        pages_results.sort_by_key(|(p, _)| *p);

        for (_, torrents) in pages_results {
            all.extend(torrents);
        }

        tracing::info!("list_all_torrents: fetched {} total torrents", all.len());
        Ok(all)
    }

    // ── Torrent info ──────────────────────────────────────────────────────────

    pub async fn get_torrent_info(&self, id: &str) -> Result<TorrentInfo> {
        let url = format!("https://api.real-debrid.com/rest/1.0/torrents/info/{id}");
        let resp = self
            .api_client
            .execute(|| self.api_client.client.get(&url))
            .await
            .context("get_torrent_info")?;
        let info: TorrentInfo = resp.json().await.context("get_torrent_info: decode")?;
        Ok(info)
    }

    // ── Unrestrict ────────────────────────────────────────────────────────────

    /// Unrestrict a link, using the in-memory cache (4h TTL).
    pub async fn unrestrict_link(
        &self,
        cache: &UnrestrictCache,
        link: &str,
    ) -> Result<Download, RdError> {
        // Check cache first
        if let Some(entry) = cache.get(link) {
            let (dl, cached_at) = entry.value();
            if cached_at.elapsed() < UNRESTRICT_CACHE_TTL {
                return Ok(dl.clone());
            }
        }

        // Cache miss or expired: call the RD API
        let form_body = format!("link={}", urlencoding_encode(link));
        let resp = self
            .unrestrict_client
            .execute(|| {
                self.unrestrict_client
                    .client
                    .post("https://api.real-debrid.com/rest/1.0/unrestrict/link")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(form_body.clone())
            })
            .await?;

        let mut download: Download = resp.json().await.map_err(|e| RdError::Network(e))?;
        download.generated_at = Some(chrono::Utc::now());
        download.token = self.config.token.clone();

        // Store in cache (remove filename from download URL — keep base code URL)
        download.download = extract_base_download_url(&download.download);
        cache.insert(link.to_string(), (download.clone(), Instant::now()));

        Ok(download)
    }

    /// Remove a link from the unrestrict cache so the next call gets a fresh URL.
    pub fn clear_unrestrict_cache(cache: &UnrestrictCache, link: &str) {
        cache.remove(link);
    }

    // ── File selection ────────────────────────────────────────────────────────

    /// Select files on a torrent (start download). `files` = comma-separated file IDs or "all".
    pub async fn select_torrent_files(&self, id: &str, files: &str) -> Result<()> {
        let url = format!("https://api.real-debrid.com/rest/1.0/torrents/selectFiles/{id}");
        let body = format!("files={files}");
        self.api_client
            .execute(|| {
                self.api_client
                    .client
                    .post(&url)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(body.clone())
            })
            .await
            .context("select_torrent_files")?;
        Ok(())
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    pub async fn delete_torrent(&self, id: &str) -> Result<()> {
        let url = format!("https://api.real-debrid.com/rest/1.0/torrents/delete/{id}");
        self.api_client
            .execute(|| self.api_client.client.delete(&url))
            .await
            .context("delete_torrent")?;
        Ok(())
    }

    // ── Add magnet ────────────────────────────────────────────────────────────

    pub async fn add_magnet(&self, hash: &str) -> Result<MagnetResponse> {
        let body = format!("magnet=magnet%3A%3Fxt%3Durn%3Abtih%3A{hash}");
        let resp = self
            .api_client
            .execute(|| {
                self.api_client
                    .client
                    .post("https://api.real-debrid.com/rest/1.0/torrents/addMagnet")
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(body.clone())
            })
            .await
            .context("add_magnet")?;
        let mr: MagnetResponse = resp.json().await.context("add_magnet: decode")?;
        Ok(mr)
    }

    // ── Active count ──────────────────────────────────────────────────────────

    pub async fn get_active_count(&self) -> Result<ActiveTorrentCountResponse> {
        let resp = self
            .api_client
            .execute(|| {
                self.api_client
                    .client
                    .get("https://api.real-debrid.com/rest/1.0/torrents/activeCount")
            })
            .await
            .context("get_active_count")?;
        let r: ActiveTorrentCountResponse =
            resp.json().await.context("get_active_count: decode")?;
        Ok(r)
    }

    // ── Link verification ────────────────────────────────────────────────────

    /// Verify a download URL is still alive.
    /// Uses HEAD by default; Range GET when `use_range_verification = true`.
    pub async fn verify_link(&self, url: &str) -> Result<()> {
        if self.config.api.use_range_verification {
            self.verify_range(url).await
        } else {
            self.verify_head(url).await
        }
    }

    async fn verify_head(&self, url: &str) -> Result<()> {
        let resp = self
            .download_client
            .execute(|| self.download_client.client.head(url))
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
        let resp = self
            .download_client
            .execute(|| {
                self.download_client
                    .client
                    .get(url)
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

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Percent-encode a URL for use in form data (just the essential chars).
fn urlencoding_encode(s: &str) -> String {
    // Simple: only encode `&` and `=` and `+` (sufficient for RD links)
    s.replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
}

/// Strip the filename from a RD CDN download URL, keeping the base code URL.
/// Example: `https://host/d/CODE/movie.mkv` → `https://host/d/CODE`
pub fn extract_base_download_url(url: &str) -> String {
    if let Some(idx) = url.find("/d/") {
        let hash_start = idx + 3;
        if let Some(slash) = url[hash_start..].find('/') {
            return url[..hash_start + slash].to_string();
        }
    }
    url.to_string()
}
