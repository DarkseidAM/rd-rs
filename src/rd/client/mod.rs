//! Low-level HTTP client with retry, backoff, and rate limiting.

mod errors;
mod exec;
mod rate_limit;

pub use errors::{ApiError, DownloadError, RdError};
pub use exec::{RdClient, RdClientConfig};
pub use rate_limit::{RateLimiter, backoff};
