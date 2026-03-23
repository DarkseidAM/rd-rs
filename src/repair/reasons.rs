//! String reasons aligned with zurg `unrepairable_reasons.go` (DB / Jellyfin-facing).

pub const INFRINGING: &str = "infringing torrent";
pub const UNSUPPORTED: &str = "unsupported torrent";
pub const UNAVAILABLE: &str = "unavailable torrent";
pub const INVALID: &str = "invalid torrent";
pub const TOO_BIG: &str = "torrent too big";
pub const NOT_ALLOWED: &str = "torrent not allowed";
pub const NO_REPAIRABLE_FILES: &str = "no repairable files";
pub const NO_SEEDERS: &str = "repair failed, no seeders";
pub const INVALID_FILE_IDS: &str = "invalid file ids";
pub const RAR_BY_RD: &str = "rar'ed by RD";
pub const LONE_BROKEN: &str = "the lone cached file is broken";
pub const REPAIR_FAILED: &str = "repair failed";
pub const NOT_CACHED: &str = "not cached (restricted to cached)";
pub const STALLED_DOWNLOAD: &str = "stalled download";
pub const DUPLICATE_FILE_IDS: &str = "duplicate file IDs (pack torrent)";
/// Detail API failed or returned nothing (often stale `rd_ids` after RD removed the torrent).
pub const MISSING_TORRENT_DETAIL: &str = "missing torrent detail (stale or removed rd id)";

/// Map RD API error substrings (zurg `UnrepairableErrorsMap`) to canonical reasons.
pub fn from_rd_error_message(msg: &str) -> Option<&'static str> {
    let m = msg.to_lowercase();
    if m.contains("infringing") {
        return Some(INFRINGING);
    }
    if m.contains("unsupported") {
        return Some(UNSUPPORTED);
    }
    if m.contains("unavailable") {
        return Some(UNAVAILABLE);
    }
    if m.contains("invalid") && m.contains("torrent") {
        return Some(INVALID);
    }
    if m.contains("big") || m.contains("too large") {
        return Some(TOO_BIG);
    }
    if m.contains("not allowed") {
        return Some(NOT_ALLOWED);
    }
    if m.contains("unknown") && (m.contains("resource") || m.contains("ressource")) {
        return Some(MISSING_TORRENT_DETAIL);
    }
    None
}
