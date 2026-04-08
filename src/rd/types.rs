//! Real-Debrid API request/response types.
//!
//! RD returns timestamps that *look* like UTC (they carry a trailing `Z`)
//! but are actually **Europe/Paris** local time. We parse them accordingly
//! via `parse_paris_time` and store all times as `DateTime<Utc>`.

use std::sync::Arc;

use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use chrono_tz::Europe::Paris;
use serde::{Deserialize, Deserializer, Serialize};

// ─── Timestamp helper ────────────────────────────────────────────────────────

/// Parse a Paris-disguised-as-UTC timestamp from the RD API.
/// Input examples: `"2024-01-15T14:30:00Z"`, `"2024-01-15T14:30:00.000Z"`
/// Output: the actual UTC instant.
pub fn parse_paris_time(s: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    // Strip the misleading Z suffix
    let s = s.trim_end_matches('Z');

    // Try with milliseconds, then without
    let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.3f")
        .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))?;

    // Treat as Europe/Paris → convert to UTC
    let paris_dt = Paris
        .from_local_datetime(&naive)
        .single()
        .or_else(|| Paris.from_local_datetime(&naive).earliest())
        .ok_or_else(|| {
            // Manufacture a parse error (no public constructor; use a known-bad parse)
            NaiveDateTime::parse_from_str("", "%Q").unwrap_err()
        })?;

    Ok(paris_dt.with_timezone(&Utc))
}

/// Serde visitor that calls `parse_paris_time` to deserialize RD timestamps.
fn deserialize_paris_time<'de, D>(de: D) -> Result<DateTime<Utc>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    parse_paris_time(&s).map_err(serde::de::Error::custom)
}

/// Deserialize an optional Paris-time field.
fn deserialize_opt_paris_time<'de, D>(de: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(de)?;
    match opt {
        None => Ok(None),
        Some(s) if s.is_empty() => Ok(None),
        Some(s) => parse_paris_time(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
    }
}

// ─── Torrent (list endpoint) ──────────────────────────────────────────────────

/// Entry from `GET /rest/1.0/torrents`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Torrent {
    pub id: String,
    #[serde(rename = "filename")]
    pub name: String,
    pub hash: String,
    /// Integer 0–100 (RD returns a float, we floor it).
    #[serde(deserialize_with = "deserialize_progress")]
    pub progress: u8,
    pub status: String,
    #[serde(default)]
    pub links: Vec<String>,
    #[serde(deserialize_with = "deserialize_paris_time")]
    pub added: DateTime<Utc>,
}

fn deserialize_progress<'de, D>(de: D) -> Result<u8, D::Error>
where
    D: Deserializer<'de>,
{
    let f = f64::deserialize(de)?;
    Ok(f.floor() as u8)
}

// ─── TorrentInfo (detail endpoint) ───────────────────────────────────────────

/// Full torrent detail from `GET /rest/1.0/torrents/info/{id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TorrentInfo {
    pub id: String,
    #[serde(rename = "filename")]
    pub name: String,
    pub hash: String,
    #[serde(deserialize_with = "deserialize_progress")]
    pub progress: u8,
    pub status: String,
    #[serde(deserialize_with = "deserialize_paris_time")]
    pub added: DateTime<Utc>,
    #[serde(default, deserialize_with = "deserialize_opt_paris_time")]
    pub ended: Option<DateTime<Utc>>,
    /// Total size in bytes.
    pub bytes: i64,
    #[serde(default)]
    pub links: Vec<String>,
    #[serde(rename = "original_filename", default)]
    pub original_name: String,
    #[serde(default)]
    pub original_bytes: i64,
    #[serde(default)]
    pub files: Vec<File>,
}

// ─── File ─────────────────────────────────────────────────────────────────────

/// A file within a torrent (from `TorrentInfo.files`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct File {
    pub id: i32,
    /// Path relative to torrent root, e.g. `/Show.S01E01.mkv`
    pub path: String,
    pub bytes: i64,
    /// 1 = selected for download, 0 = not selected.
    pub selected: i32,
}

impl File {
    pub fn is_selected(&self) -> bool {
        self.selected == 1
    }
}

// ─── Download (unrestrict response) ──────────────────────────────────────────

/// Response from `POST /rest/1.0/unrestrict/link`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Download {
    pub filename: String,
    /// File size in bytes (0 if unknown).
    pub filesize: i64,
    /// Original RD link (e.g. `https://real-debrid.com/d/XXXXX`).
    pub link: String,
    /// CDN download URL (e.g. `https://105-4.download.real-debrid.com/d/XXXXX`).
    pub download: String,
    pub streamable: i32,
    /// When this unrestrict result was generated (RFC3339, set by us not RD).
    /// We store this to implement the 4-hour TTL unrestrict cache.
    #[serde(skip)]
    pub generated_at: Option<DateTime<Utc>>,
    /// The token used to generate this download (for cache keying).
    #[serde(skip)]
    pub token: String,
}

impl Download {
    /// Returns the file extension of the download URL.
    pub fn extension(&self) -> Option<&str> {
        std::path::Path::new(&self.download)
            .extension()
            .and_then(|s| s.to_str())
    }
}

// ─── User ─────────────────────────────────────────────────────────────────────

/// Response from `GET /rest/1.0/user`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: i64,
    pub username: String,
    pub email: String,
    #[serde(rename = "type")]
    pub account_type: String,
    /// Seconds remaining as a Premium user.
    pub premium: i64,
}

impl User {
    pub fn is_premium(&self) -> bool {
        self.account_type == "premium" && self.premium > 0
    }
}

// ─── Magnet ───────────────────────────────────────────────────────────────────

/// Response from `POST /rest/1.0/torrents/addMagnet`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MagnetResponse {
    pub id: String,
}

// ─── Active count ─────────────────────────────────────────────────────────────

/// Response from `GET /rest/1.0/torrents/activeCount`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveTorrentCountResponse {
    #[serde(rename = "nb")]
    pub downloading_count: usize,
    #[serde(rename = "limit")]
    pub max_number_of_torrents: usize,
}

// ─── Traffic ──────────────────────────────────────────────────────────────────

/// One day bucket in `GET /rest/1.0/traffic/details` (API keys are ISO dates, e.g. `2015-12-09`).
///
/// Not to be confused with `GET /rest/1.0/traffic` (host limits); `/details` is usage **by day**.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrafficDetailDay {
    /// Per-host byte totals for that day.
    #[serde(default)]
    pub host: std::collections::HashMap<String, i64>,
    #[serde(default)]
    pub bytes: i64,
}

/// Response from `GET /rest/1.0/traffic/details`: date string → that day’s host breakdown + total bytes.
pub type TrafficDetailsResponse = std::collections::HashMap<String, TrafficDetailDay>;

/// In-memory snapshot of [`TrafficDetailsResponse`] for each configured download token (primary first).
#[derive(Debug, Clone)]
pub struct TrafficDetailsSnapshot {
    pub fetched_at: DateTime<Utc>,
    pub by_token: Vec<(Arc<str>, TrafficDetailsResponse)>,
}

// ─── Downloads ────────────────────────────────────────────────────────────────

/// Entry from `GET /rest/1.0/downloads`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadItem {
    pub id: String,
    pub filename: String,
    #[serde(rename = "mimeType")]
    pub mime_type: String,
    pub filesize: i64,
    pub link: String,
    pub host: String,
    pub host_icon: String,
    pub chunks: i64,
    pub download: String,
    pub streamable: i32,
    #[serde(deserialize_with = "deserialize_paris_time")]
    pub generated: DateTime<Utc>,
}
