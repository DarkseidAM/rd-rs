//! Round-robin token pool for download client rotation.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Thread-safe round-robin pool of RD API tokens.
///
/// Used by the download client to rotate to the next token when the active one
/// hits a bandwidth limit (`BytesLimitReached` / `TrafficExhausted`).
/// Rotation is atomic and lock-free; zero allocations after construction.
pub struct TokenPool {
    tokens: Vec<String>,
    current: AtomicUsize,
}

impl TokenPool {
    /// Create a new pool. Panics if `tokens` is empty.
    pub fn new(tokens: Vec<String>) -> Self {
        assert!(!tokens.is_empty(), "TokenPool requires at least one token");
        Self {
            tokens,
            current: AtomicUsize::new(0),
        }
    }

    /// Returns the currently active token.
    pub fn current(&self) -> &str {
        let idx = self.current.load(Ordering::Relaxed) % self.tokens.len();
        &self.tokens[idx]
    }

    /// Advances to the next token (wraps around).
    ///
    /// Returns `true` if the pool has more than one token (rotation happened),
    /// `false` if there is only one token (no rotation possible).
    pub fn rotate(&self) -> bool {
        if self.tokens.len() <= 1 {
            return false;
        }
        self.current.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// How many tokens are in the pool (primary + extras).
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// True if the pool has no tokens (should only happen if constructed incorrectly).
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// True if only the primary token is present (no extras configured).
    pub fn is_single(&self) -> bool {
        self.tokens.len() == 1
    }
}
