//! Scheduling helpers for daily bandwidth / quota reset windows.

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
