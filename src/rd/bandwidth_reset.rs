//! Scheduling helpers for daily bandwidth / quota reset windows.

use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use chrono_tz::Tz;

/// Wall-clock duration until the next local midnight in `tz_name` (IANA, e.g. `Europe/Paris`).
/// Malformed names fall back to `Europe/Paris` with a warning.
pub fn duration_until_timezone_midnight(tz_name: &str) -> Duration {
    let tz: Tz = Tz::from_str(tz_name).unwrap_or_else(|_| {
        tracing::warn!(
            tz = %tz_name,
            "invalid bandwidth_reset_timezone; using Europe/Paris"
        );
        chrono_tz::Europe::Paris
    });

    let zoned = Utc::now().with_timezone(&tz);
    let mut mid_naive = zoned
        .date_naive()
        .and_hms_opt(0, 0, 0)
        .expect("midnight valid");
    let mut mid_z = match tz.from_local_datetime(&mid_naive).latest() {
        Some(t) => t,
        None => {
            return Duration::from_secs(3600);
        }
    };
    if mid_z <= zoned {
        mid_naive = (zoned.date_naive() + ChronoDuration::days(1))
            .and_hms_opt(0, 0, 0)
            .expect("midnight valid");
        mid_z = match tz.from_local_datetime(&mid_naive).latest() {
            Some(t) => t,
            None => {
                return Duration::from_secs(3600);
            }
        };
    }

    (mid_z - zoned)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(3600))
}

/// Check on startup whether the daily bandwidth reset was missed (e.g. the process was offline
/// at midnight). If today's reset hasn't been stamped yet, returns `true` so the caller can
/// call `clear_all_exhausted()` immediately before sleeping until the next midnight.
///
/// The last-reset date is persisted as a `YYYY-MM-DD` string in `cache_dir/bw_last_reset`.
pub fn startup_reset_needed(cache_dir: &Path, tz_name: &str) -> bool {
    let stamp_path = cache_dir.join("bw_last_reset");

    let tz: Tz = Tz::from_str(tz_name).unwrap_or(chrono_tz::Europe::Paris);
    let today = Utc::now().with_timezone(&tz).date_naive();

    let last = std::fs::read_to_string(&stamp_path)
        .ok()
        .and_then(|s| chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d").ok());

    match last {
        Some(d) if d >= today => false, // already reset today or in the future
        _ => true,                      // missed or never run
    }
}

/// Persist the current local date as the last-reset timestamp.
/// Call this immediately after `clear_all_exhausted()`.
pub fn stamp_reset(cache_dir: &Path, tz_name: &str) {
    let stamp_path = cache_dir.join("bw_last_reset");
    let tz: Tz = Tz::from_str(tz_name).unwrap_or(chrono_tz::Europe::Paris);
    let today = Utc::now().with_timezone(&tz).date_naive();
    if let Err(e) = std::fs::write(&stamp_path, today.format("%Y-%m-%d").to_string()) {
        tracing::warn!(path = %stamp_path.display(), "failed to write bw_last_reset: {e}");
    }
}
