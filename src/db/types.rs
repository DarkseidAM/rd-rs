//! DB state types (torrents and repair_jobs rows).

use std::str::FromStr;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentState {
    Ok,
    Broken,
    UnderRepair,
}

impl std::fmt::Display for TorrentState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok => write!(f, "ok"),
            Self::Broken => write!(f, "broken"),
            Self::UnderRepair => write!(f, "under_repair"),
        }
    }
}

impl FromStr for TorrentState {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        match s {
            "ok" => Ok(Self::Ok),
            "broken" => Ok(Self::Broken),
            "under_repair" => Ok(Self::UnderRepair),
            _ => anyhow::bail!("unknown torrent state: {s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TorrentRow {
    pub access_key: String,
    pub rd_ids: Vec<String>,
    pub hash: String,
    pub name: String,
    pub state: TorrentState,
    pub unrepairable_reason: Option<String>,
    pub file_states: Option<String>,
    pub last_seen_at: Option<i64>,
    pub last_repaired_at: Option<i64>,
}

impl TorrentRow {
    pub fn rd_ids_first_or_empty(&self, default: &str) -> String {
        self.rd_ids
            .first()
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairJobStatus {
    Pending,
    Running,
    Done,
    Failed,
}

impl std::fmt::Display for RepairJobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Done => "done",
            Self::Failed => "failed",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone)]
pub struct RepairJobRow {
    pub id: String,
    pub torrent_key: String,
    pub strategy: String,
    pub status: RepairJobStatus,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}
