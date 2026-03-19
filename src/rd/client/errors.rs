//! RD API and download error types.

use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub enum ApiError {
    #[error("RD rate limit (code={code}): {message}")]
    RateLimit { code: i32, message: String },

    #[error("RD traffic exhausted: {message}")]
    TrafficExhausted { message: String },

    #[error("RD internal error: {message}")]
    Internal { message: String },

    #[error("RD fair usage limit: {message}")]
    FairUsageLimit { message: String },

    #[error("RD API error (code={code}): {message}")]
    Other { code: i32, message: String },
}

impl ApiError {
    pub fn from_code(code: i32, message: String) -> Self {
        match code {
            5 | 34 | 429 => Self::RateLimit { code, message },
            23 => Self::TrafficExhausted { message },
            36 => Self::FairUsageLimit { message },
            -1 => Self::Internal { message },
            _ => Self::Other { code, message },
        }
    }

    pub fn should_retry(&self) -> bool {
        matches!(
            self,
            Self::RateLimit { .. }
                | Self::TrafficExhausted { .. }
                | Self::Internal { .. }
                | Self::FairUsageLimit { .. }
        )
    }
}

#[derive(Debug, Error, Clone)]
pub enum DownloadError {
    #[error("invalid_download_code")]
    InvalidDownloadCode,
    #[error("failed_generation")]
    FailedGeneration,
    #[error("too_many_attempts")]
    TooManyAttempts,
    #[error("file_unavailable")]
    FileUnavailable,
    #[error("bytes_limit_reached")]
    BytesLimitReached,
    #[error("server error (status={0})")]
    ServerError(u16),
    #[error("download error: {0}")]
    Other(String),
}

impl DownloadError {
    pub fn from_header(msg: &str, status: u16) -> Self {
        match msg {
            "invalid_download_code" => Self::InvalidDownloadCode,
            "failed_generation" => Self::FailedGeneration,
            "too_many_attempts" => Self::TooManyAttempts,
            "file_unavailable" => Self::FileUnavailable,
            "bytes_limit_reached" => Self::BytesLimitReached,
            _ if (500..=599).contains(&status) => Self::ServerError(status),
            _ => Self::Other(msg.to_string()),
        }
    }
}

#[derive(Debug, Error)]
pub enum RdError {
    #[error("api error: {0}")]
    Api(#[from] ApiError),
    #[error("download error: {0}")]
    Download(#[from] DownloadError),
    #[error("network error: {0}")]
    Network(#[from] reqwest::Error),
    #[error("request cancelled")]
    Cancelled,
    #[error("max retries exceeded")]
    MaxRetriesExceeded,
}
