pub mod capacity;
pub mod detect;
pub mod engine;
pub mod preflight;
pub mod reasons;
pub mod strategies;

pub use detect::{
    check_head_unreachable, duplicate_selected_file_ids, has_non_playable_selected,
    is_stalled_download, passive_head_probe_slot_count, path_looks_playable,
    unassigned_selected_link_count,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Strategy {
    ReinsertTorrent,
    IndividualFiles,
    ArchiveAll,
    BatchDownload,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::ReinsertTorrent => "reinsert_torrent",
            Self::IndividualFiles => "individual_files",
            Self::ArchiveAll => "archive_all",
            Self::BatchDownload => "batch_download",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnrepairableReason {
    LoneBroken,
    NoRepairableFiles,
    DuplicateFileIDs,
    InvalidFileIDs,
    /// `restrict_to_cached` is on and RD did not finish at 100%.
    NotCached,
}

impl std::fmt::Display for UnrepairableReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::LoneBroken => "lone_broken",
            Self::NoRepairableFiles => "no_repairable_files",
            Self::DuplicateFileIDs => "duplicate_file_ids",
            Self::InvalidFileIDs => "invalid_file_ids",
            Self::NotCached => reasons::NOT_CACHED,
        };
        write!(f, "{s}")
    }
}

/// Outcome of the full strategy cascade for one torrent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CascadeOutcome {
    /// Repaired; caller should set `ok` and `last_repaired_at`.
    /// When `new_rd_ids` is set (reinsert / archive-all), caller must persist before marking ok.
    Success { new_rd_ids: Option<Vec<String>> },
    /// Permanently (for this cycle) unrepairable.
    Unrepairable(UnrepairableReason),
    /// RD API / zurg-style free-form reason (persisted to `unrepairable`).
    UnrepairableMsg(String),
    /// Bandwidth / account limit — stay broken, retry next cycle.
    DeferBandwidth,
}
