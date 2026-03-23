use rd_rs::repair::reasons::{
    self, DUPLICATE_FILE_IDS, INFRINGING, INVALID, MISSING_TORRENT_DETAIL, NOT_ALLOWED, NOT_CACHED,
    TOO_BIG, UNAVAILABLE, UNSUPPORTED, from_rd_error_message,
};

#[test]
fn from_rd_error_maps_infringing() {
    assert_eq!(
        from_rd_error_message("This is INFRINGING content"),
        Some(INFRINGING)
    );
}

#[test]
fn from_rd_error_maps_unsupported() {
    assert_eq!(
        from_rd_error_message("Unsupported torrent type"),
        Some(UNSUPPORTED)
    );
}

#[test]
fn from_rd_error_maps_unavailable() {
    assert_eq!(from_rd_error_message("Link unavailable"), Some(UNAVAILABLE));
}

#[test]
fn from_rd_error_maps_invalid_torrent() {
    assert_eq!(from_rd_error_message("Invalid torrent hash"), Some(INVALID));
}

#[test]
fn from_rd_error_invalid_without_torrent_is_none() {
    assert_eq!(from_rd_error_message("invalid file ids"), None);
}

#[test]
fn from_rd_error_maps_too_big() {
    assert_eq!(from_rd_error_message("torrent too big"), Some(TOO_BIG));
    assert_eq!(
        from_rd_error_message("file is too large for your plan"),
        Some(TOO_BIG)
    );
}

#[test]
fn from_rd_error_maps_not_allowed() {
    assert_eq!(
        from_rd_error_message("torrent not allowed"),
        Some(NOT_ALLOWED)
    );
}

#[test]
fn from_rd_error_unknown_returns_none() {
    assert_eq!(from_rd_error_message("random network glitch"), None);
}

#[test]
fn from_rd_error_maps_unknown_resource() {
    assert_eq!(
        from_rd_error_message("RD API error (code=7): unknown_ressource"),
        Some(MISSING_TORRENT_DETAIL)
    );
}

#[test]
fn display_constants_are_stable() {
    assert_eq!(NOT_CACHED, "not cached (restricted to cached)");
    assert_eq!(DUPLICATE_FILE_IDS, "duplicate file IDs (pack torrent)");
    assert_eq!(reasons::LONE_BROKEN, "the lone cached file is broken");
}
