//! Round-robin token pool for download client rotation.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arc_swap::ArcSwap;

/// Thread-safe round-robin pool of RD API tokens.
///
/// Used by the download client to rotate to the next token when the active one
/// hits a bandwidth limit (`BytesLimitReached` / `TrafficExhausted`).
///
/// Tokens are stored as `Arc<String>` so `current()` only does a reference-count
/// increment — no heap allocation per request. The token list itself is stored in
/// an [`ArcSwap`] so it can be replaced atomically via
/// [`update_tokens`](Self::update_tokens) during a config hot-reload.
pub struct TokenPool {
    tokens: ArcSwap<Vec<Arc<String>>>,
    current: AtomicUsize,
}

impl TokenPool {
    /// Create a new pool. Panics if `tokens` is empty.
    pub fn new(tokens: Vec<String>) -> Self {
        assert!(!tokens.is_empty(), "TokenPool requires at least one token");
        Self {
            tokens: ArcSwap::from_pointee(tokens.into_iter().map(Arc::new).collect()),
            current: AtomicUsize::new(0),
        }
    }

    /// Returns the currently active token (cheap Arc clone — no string allocation).
    pub fn current(&self) -> Arc<String> {
        let t = self.tokens.load();
        let idx = self.current.load(Ordering::Relaxed) % t.len();
        t[idx].clone()
    }

    /// Advances to the next token (wraps around).
    ///
    /// Returns `true` if the pool has more than one token (rotation happened),
    /// `false` if there is only one token (no rotation possible).
    pub fn rotate(&self) -> bool {
        if self.tokens.load().len() <= 1 {
            return false;
        }
        self.current.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Atomically replaces the token list.
    ///
    /// The current rotation index is reset to 0 so the new primary token
    /// becomes active immediately. In-flight `current()` calls using the old
    /// `Arc<Vec<Arc<String>>>` complete safely.
    pub fn update_tokens(&self, new_tokens: Vec<Arc<String>>) {
        assert!(
            !new_tokens.is_empty(),
            "TokenPool requires at least one token"
        );
        self.tokens.store(Arc::new(new_tokens));
        self.current.store(0, Ordering::Relaxed);
    }

    /// How many tokens are in the pool (primary + extras).
    pub fn len(&self) -> usize {
        self.tokens.load().len()
    }

    /// True if the pool has no tokens (should only happen if constructed incorrectly).
    pub fn is_empty(&self) -> bool {
        self.tokens.load().is_empty()
    }

    /// True if only the primary token is present (no extras configured).
    pub fn is_single(&self) -> bool {
        self.tokens.load().len() == 1
    }
}
