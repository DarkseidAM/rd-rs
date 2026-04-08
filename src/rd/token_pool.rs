//! Round-robin token pool for download client rotation and bandwidth exhaustion.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use arc_swap::ArcSwap;

struct TokenPoolState {
    tokens: Vec<Arc<String>>,
    exhausted: Vec<AtomicBool>,
}

/// Thread-safe round-robin pool of RD API tokens.
///
/// Used by the download client to rotate when an account hits a bandwidth limit
/// (`BytesLimitReached` / `TrafficExhausted`). Each slot can be marked **exhausted**
/// so [`Self::download_bearer`] and unrestrict token selection skip it until
/// [`Self::clear_all_exhausted`] (e.g. daily CET reset).
///
/// Tokens are stored as `Arc<String>` so `current()` only does a reference-count
/// increment. The state is stored in an [`ArcSwap`] so it can be replaced
/// atomically via [`Self::update_tokens`] during config hot-reload.
pub struct TokenPool {
    state: ArcSwap<TokenPoolState>,
    current: AtomicUsize,
}

impl TokenPool {
    /// Create a new pool. Panics if `tokens` is empty.
    pub fn new(tokens: Vec<String>) -> Self {
        assert!(!tokens.is_empty(), "TokenPool requires at least one token");
        let v: Vec<Arc<String>> = tokens.into_iter().map(Arc::new).collect();
        let ex: Vec<AtomicBool> = (0..v.len()).map(|_| AtomicBool::new(false)).collect();
        Self {
            state: ArcSwap::from_pointee(TokenPoolState {
                tokens: v,
                exhausted: ex,
            }),
            current: AtomicUsize::new(0),
        }
    }

    /// Returns the token at the current rotation index (tests / explicit [`Self::rotate`]).
    pub fn current(&self) -> Arc<String> {
        let state = self.state.load();
        let t = &state.tokens;
        let idx = self.current.load(Ordering::Relaxed) % t.len();
        t[idx].clone()
    }

    /// Picks a Bearer for CDN downloads: first non-exhausted token starting from the
    /// current index; updates the stored index when skipping exhausted slots.
    pub fn download_bearer(&self) -> Arc<String> {
        let state = self.state.load();
        let t = &state.tokens;
        let e = &state.exhausted;
        let n = t.len();
        let start = self.current.load(Ordering::Relaxed) % n;
        for step in 0..n {
            let idx = (start + step) % n;
            if !e[idx].load(Ordering::Acquire) {
                if idx != start {
                    self.current.store(idx, Ordering::Relaxed);
                }
                return t[idx].clone();
            }
        }
        tracing::warn!("all download tokens marked bandwidth-exhausted; using primary slot anyway");
        t[start].clone()
    }

    /// Tokens in config order (primary first, then extras).
    pub fn tokens_in_order(&self) -> Vec<Arc<String>> {
        self.state.load().tokens.clone()
    }

    /// Ordered list of tokens not marked exhausted (may be empty).
    pub fn eligible_tokens_in_order(&self) -> Vec<Arc<String>> {
        let state = self.state.load();
        state
            .tokens
            .iter()
            .enumerate()
            .filter(|(i, _)| !state.exhausted[*i].load(Ordering::Acquire))
            .map(|(_, tok)| tok.clone())
            .collect()
    }

    pub fn is_exhausted(&self, token: &str) -> bool {
        let state = self.state.load();
        if let Some((i, _)) = state
            .tokens
            .iter()
            .enumerate()
            .find(|(_, tok)| tok.as_str() == token)
        {
            state.exhausted[i].load(Ordering::Acquire)
        } else {
            false
        }
    }

    pub fn mark_exhausted(&self, token: &str) {
        let state = self.state.load();
        for (i, tok) in state.tokens.iter().enumerate() {
            if tok.as_str() == token {
                state.exhausted[i].store(true, Ordering::Release);
                tracing::info!(token = %token, "RD token marked bandwidth-exhausted");
                return;
            }
        }
    }

    /// Clears exhaustion on every slot (e.g. after RD daily quota reset).
    pub fn clear_all_exhausted(&self) {
        let state = self.state.load();
        for x in state.exhausted.iter() {
            x.store(false, Ordering::Release);
        }
        tracing::info!("cleared bandwidth-exhausted flags on all download tokens");
    }

    pub fn any_non_exhausted(&self) -> bool {
        let state = self.state.load();
        state
            .tokens
            .iter()
            .enumerate()
            .any(|(i, _)| !state.exhausted[i].load(Ordering::Acquire))
    }

    /// Advances to the next token (wraps around).
    ///
    /// Returns `true` if the pool has more than one token, `false` if there is only one.
    pub fn rotate(&self) -> bool {
        if self.state.load().tokens.len() <= 1 {
            return false;
        }
        self.current.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Atomically replaces the token list and resets exhaustion flags.
    pub fn update_tokens(&self, new_tokens: Vec<Arc<String>>) {
        assert!(
            !new_tokens.is_empty(),
            "TokenPool requires at least one token"
        );
        let ex: Vec<AtomicBool> = (0..new_tokens.len())
            .map(|_| AtomicBool::new(false))
            .collect();
        self.state.store(Arc::new(TokenPoolState {
            tokens: new_tokens,
            exhausted: ex,
        }));
        self.current.store(0, Ordering::Relaxed);
    }

    /// How many tokens are in the pool (primary + extras).
    pub fn len(&self) -> usize {
        self.state.load().tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.state.load().tokens.is_empty()
    }

    pub fn is_single(&self) -> bool {
        self.state.load().tokens.len() == 1
    }
}
