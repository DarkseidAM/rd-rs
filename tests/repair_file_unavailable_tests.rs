//! Tests for Issue H: `FileUnavailable` must NOT trigger CDN heal via unrestrict.
//!
//! Confirmed against zurg: `file_unavailable` is a `DownloadErrorResponse` which
//! zurg treats as non-retryable and fatal (retry.go:101-104). The FUSE read layer
//! must call `mark_file_broken` + `enqueue_repair` instead of a CDN heal loop.

use rd_rs::rd::client::DownloadError;

// ── Core contract ─────────────────────────────────────────────────────────────

/// `FileUnavailable` must NOT trigger `should_refresh_via_unrestrict`.
/// This is the primary regression guard for Issue H.
#[test]
fn test_file_unavailable_not_in_refresh_via_unrestrict() {
    assert!(
        !DownloadError::FileUnavailable.should_refresh_via_unrestrict(),
        "FileUnavailable must NOT trigger CDN heal — it is fatal like zurg's DownloadErrorResponse"
    );
}

/// All the other transient errors that SHOULD trigger a CDN heal still do.
#[test]
fn test_transient_errors_still_trigger_refresh() {
    assert!(
        DownloadError::InvalidDownloadCode.should_refresh_via_unrestrict(),
        "InvalidDownloadCode should trigger CDN refresh"
    );
    assert!(
        DownloadError::FailedGeneration.should_refresh_via_unrestrict(),
        "FailedGeneration should trigger CDN refresh"
    );
    assert!(
        DownloadError::TooManyAttempts.should_refresh_via_unrestrict(),
        "TooManyAttempts should trigger CDN refresh"
    );
    assert!(
        DownloadError::LinkUnavailable { status: 403 }.should_refresh_via_unrestrict(),
        "LinkUnavailable should trigger CDN refresh"
    );
}

/// `BytesLimitReached` should also NOT trigger CDN heal (it's a quota error).
#[test]
fn test_bytes_limit_reached_not_in_refresh() {
    assert!(
        !DownloadError::BytesLimitReached.should_refresh_via_unrestrict(),
        "BytesLimitReached must not trigger CDN heal"
    );
}

/// `ServerError` should NOT trigger CDN heal (it is a CDN-side error).
#[test]
fn test_server_error_not_in_refresh() {
    assert!(
        !DownloadError::ServerError(503).should_refresh_via_unrestrict(),
        "ServerError must not trigger CDN heal"
    );
}

/// `Other` errors should NOT trigger CDN heal.
#[test]
fn test_other_error_not_in_refresh() {
    assert!(
        !DownloadError::Other("weird error".to_string()).should_refresh_via_unrestrict(),
        "Other errors must not trigger CDN heal"
    );
}

// ── from_header parsing ───────────────────────────────────────────────────────

/// Verifies `DownloadError::from_header` correctly maps `"file_unavailable"`.
#[test]
fn test_from_header_maps_file_unavailable() {
    let err = DownloadError::from_header("file_unavailable", 200);
    assert!(
        matches!(err, DownloadError::FileUnavailable),
        "from_header must produce FileUnavailable for 'file_unavailable'"
    );
}

/// Verifies that the other `DownloadErrorResponse` codes still map correctly.
#[test]
fn test_from_header_maps_all_download_error_codes() {
    assert!(matches!(
        DownloadError::from_header("invalid_download_code", 200),
        DownloadError::InvalidDownloadCode
    ));
    assert!(matches!(
        DownloadError::from_header("failed_generation", 200),
        DownloadError::FailedGeneration
    ));
    assert!(matches!(
        DownloadError::from_header("too_many_attempts", 200),
        DownloadError::TooManyAttempts
    ));
    assert!(matches!(
        DownloadError::from_header("bytes_limit_reached", 200),
        DownloadError::BytesLimitReached
    ));
}

/// Verifies `LinkUnavailable` is produced for 401/403/404 status codes.
#[test]
fn test_from_header_link_unavailable_status_codes() {
    for status in [401u16, 403, 404] {
        let err = DownloadError::from_header("whatever", status);
        assert!(
            matches!(err, DownloadError::LinkUnavailable { status: s } if s == status),
            "status {status} should map to LinkUnavailable"
        );
    }
}
