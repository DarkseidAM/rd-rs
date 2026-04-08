//! Integration tests for bandwidth reset scheduling (TODO-2).
//!
//! Tests both the existing `duration_until_timezone_midnight` helper and the new
//! startup-catchup helpers: `startup_reset_needed` and `stamp_reset`.

use rd_rs::rd::bandwidth_reset::{
    duration_until_timezone_midnight, stamp_reset, startup_reset_needed,
};

// ── Existing scheduling helpers ───────────────────────────────────────────────

#[test]
fn midnight_duration_positive_and_bounded() {
    let d = duration_until_timezone_midnight("Europe/Paris");
    assert!(d.as_secs() > 0);
    assert!(d.as_secs() <= 25 * 3600);
}

#[test]
fn invalid_timezone_falls_back_without_panic() {
    let d = duration_until_timezone_midnight("not-a-valid-iana-timezone");
    assert!(d.as_secs() > 0);
    assert!(d.as_secs() <= 25 * 3600);
}

// ── startup_reset_needed ──────────────────────────────────────────────────────

/// When no stamp file exists, a reset is needed.
#[test]
fn test_startup_reset_needed_when_no_stamp_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    assert!(
        startup_reset_needed(tmp.path(), "Europe/Paris"),
        "no stamp file → reset is needed on startup"
    );
}

/// When the stamp file contains yesterday's date, a reset is needed.
#[test]
fn test_startup_reset_needed_when_stamp_is_yesterday() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let stamp_path = tmp.path().join("bw_last_reset");

    // Write yesterday's date.
    use chrono::Utc;
    let tz: chrono_tz::Tz = "Europe/Paris".parse().unwrap();
    let yesterday = (Utc::now().with_timezone(&tz) - chrono::Duration::days(1)).date_naive();
    std::fs::write(&stamp_path, yesterday.format("%Y-%m-%d").to_string())
        .expect("write stale stamp");

    assert!(
        startup_reset_needed(tmp.path(), "Europe/Paris"),
        "stale stamp (yesterday) → reset is needed on startup"
    );
}

/// When the stamp file already contains today's date, no reset is needed.
#[test]
fn test_startup_no_reset_if_already_stamped_today() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Write today's stamp.
    stamp_reset(tmp.path(), "Europe/Paris");

    assert!(
        !startup_reset_needed(tmp.path(), "Europe/Paris"),
        "stamp is today → no reset needed on startup"
    );
}

/// When the stamp contains a future date (!), no reset is needed (guard).
#[test]
fn test_startup_no_reset_if_stamp_is_future() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let stamp_path = tmp.path().join("bw_last_reset");

    use chrono::Utc;
    let tz: chrono_tz::Tz = "Europe/Paris".parse().unwrap();
    let tomorrow = (Utc::now().with_timezone(&tz) + chrono::Duration::days(1)).date_naive();
    std::fs::write(&stamp_path, tomorrow.format("%Y-%m-%d").to_string())
        .expect("write future stamp");

    assert!(
        !startup_reset_needed(tmp.path(), "Europe/Paris"),
        "future stamp → no reset needed (guards against clock skew)"
    );
}

// ── stamp_reset ───────────────────────────────────────────────────────────────

/// `stamp_reset` creates the `bw_last_reset` file with today's date.
#[test]
fn test_stamp_reset_creates_file_with_today() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let stamp_path = tmp.path().join("bw_last_reset");

    assert!(!stamp_path.exists(), "stamp should not exist yet");

    stamp_reset(tmp.path(), "Europe/Paris");

    assert!(stamp_path.exists(), "stamp should exist after stamp_reset");

    // Read and parse.
    let content = std::fs::read_to_string(&stamp_path).expect("read stamp");
    let parsed: chrono::NaiveDate = chrono::NaiveDate::parse_from_str(content.trim(), "%Y-%m-%d")
        .expect("stamp should be a valid YYYY-MM-DD date");

    use chrono::Utc;
    let tz: chrono_tz::Tz = "Europe/Paris".parse().unwrap();
    let today = Utc::now().with_timezone(&tz).date_naive();
    assert_eq!(
        parsed, today,
        "stamp date must be today in the configured tz"
    );
}

/// Calling `stamp_reset` twice overwrites the old value without error.
#[test]
fn test_stamp_reset_is_idempotent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    stamp_reset(tmp.path(), "Europe/Paris");
    stamp_reset(tmp.path(), "Europe/Paris"); // second call must not panic

    // Still consistent.
    assert!(
        !startup_reset_needed(tmp.path(), "Europe/Paris"),
        "double-stamp should still leave reset_needed as false"
    );
}

/// Invalid timezone in stamp functions falls back safely without panicking.
#[test]
fn test_stamp_reset_invalid_timezone_no_panic() {
    let tmp = tempfile::tempdir().expect("tempdir");
    stamp_reset(tmp.path(), "Not/A/Timezone"); // must not panic
    // startup_reset_needed uses the same fallback; just call it.
    let _ = startup_reset_needed(tmp.path(), "Not/A/Timezone");
}
