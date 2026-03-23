//! Refresh vs repair invariants (zurg `inProgress` / `IDsToDelete` equivalents — no global sets).

use crate::db::TorrentState;

use super::super::TorrentManager;

/// Do not drop a local torrent row while repair holds it in `under_repair`.
#[must_use]
pub fn skip_local_remove_for_state(state: TorrentState) -> bool {
    state == TorrentState::UnderRepair
}

/// Avoid RD `delete_torrent` for an id that still belongs to a torrent being repaired.
#[must_use]
pub fn rd_id_belongs_to_under_repair(mgr: &TorrentManager, rd_id: &str) -> bool {
    mgr.torrents.iter().any(|e| {
        e.value().state == TorrentState::UnderRepair
            && e.value().rd_ids.iter().any(|id| id == rd_id)
    })
}
