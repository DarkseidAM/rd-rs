use rd_rs::rd::client::{ApiError, DownloadError, RdError, backoff};
use std::time::Duration;

#[test]
fn backoff_caps_at_60() {
    // At attempt=10, base=1: 2^10=1024, capped at 60 + up to 20% jitter
    let d = backoff(10, 1);
    assert!(d >= Duration::from_secs(60));
    assert!(d <= Duration::from_secs(73));
}

#[test]
fn backoff_grows_with_attempt() {
    let d0 = backoff(0, 1);
    let d1 = backoff(1, 1);
    let d2 = backoff(2, 1);
    assert!(d0 < d1);
    assert!(d1 < d2);
}

#[test]
fn api_error_should_retry() {
    assert!(ApiError::from_code(5, "slow down".into()).should_retry());
    assert!(ApiError::from_code(34, "too many".into()).should_retry());
    assert!(ApiError::from_code(23, "exhausted".into()).should_retry());
    assert!(ApiError::from_code(-1, "internal".into()).should_retry());
}

#[test]
fn api_error_no_retry_for_other() {
    assert!(!ApiError::from_code(9, "resource not found".into()).should_retry());
}

#[test]
fn download_error_from_header() {
    let e = DownloadError::from_header("invalid_download_code", 403);
    assert!(matches!(e, DownloadError::InvalidDownloadCode));

    let e2 = DownloadError::from_header("", 503);
    assert!(matches!(e2, DownloadError::ServerError(503)));

    let e404 = DownloadError::from_header("", 404);
    assert!(matches!(
        e404,
        DownloadError::LinkUnavailable { status: 404 }
    ));
    assert!(e404.should_refresh_via_unrestrict());
}

#[test]
fn rd_error_bandwidth_limited_detection() {
    assert!(
        RdError::Api(ApiError::TrafficExhausted {
            message: "x".into(),
        })
        .is_bandwidth_limited()
    );
    assert!(
        RdError::Api(ApiError::FairUsageLimit {
            message: "x".into(),
        })
        .is_bandwidth_limited()
    );
    assert!(RdError::Download(DownloadError::BytesLimitReached).is_bandwidth_limited());
    assert!(
        !RdError::Api(ApiError::Other {
            code: 9,
            message: "nope".into(),
        })
        .is_bandwidth_limited()
    );
}
