//! Swappable RD client credentials for hot-reload support.

use std::sync::Arc;

/// The hot-swappable subset of `Config` that the RD HTTP clients consume.
///
/// Stored inside an `Arc<arc_swap::ArcSwap<Credentials>>` shared across all
/// three `RdClient` instances. On config reload, only these fields change;
/// all HTTP connections, semaphores, and CDN state are preserved.
///
/// Tokens are `Arc<str>` so cloning them in the hot path (`execute()` loop)
/// is a single atomic reference-count increment, not a heap allocation.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Primary RD API token. Used by `api_client` and `unrestrict_client`.
    pub token: Arc<str>,
    /// All download tokens (primary first, extras follow).
    /// Used to populate a fresh `TokenPool` on reload.
    pub download_tokens: Vec<Arc<str>>,
}
