//! Tests for daily bandwidth-reset scheduling.

use rd_rs::rd::bandwidth_reset::duration_until_timezone_midnight;

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
