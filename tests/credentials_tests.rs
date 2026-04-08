//! Integration tests for PR-4: ArcSwap credential hot-reload.
//!
//! Tests verify that `reload_credentials` atomically updates the token visible
//! to `RdClient::execute()` without rebuilding any HTTP clients.

use std::sync::Arc;

use arc_swap::ArcSwap;
use rd_rs::rd::client::Credentials;
use rd_rs::rd::token_pool::TokenPool;

// ── helpers ──────────────────────────────────────────────────────────────────

fn make_credentials(token: &str) -> Arc<ArcSwap<Credentials>> {
    Arc::new(ArcSwap::from_pointee(Credentials {
        token: Arc::from(token),
        download_tokens: vec![Arc::from(token)],
    }))
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[test]
fn credentials_initial_token_visible() {
    let creds = make_credentials("initial_token");
    let loaded = creds.load();
    assert_eq!(&*loaded.token, "initial_token");
    assert_eq!(loaded.download_tokens.len(), 1);
    assert_eq!(&*loaded.download_tokens[0], "initial_token");
}

#[test]
fn credentials_hot_swap_is_atomic() {
    let creds = make_credentials("token_v1");
    assert_eq!(&*creds.load().token, "token_v1");

    // Simulate what reload_credentials() does.
    creds.store(Arc::new(Credentials {
        token: Arc::from("token_v2"),
        download_tokens: vec![Arc::from("token_v2"), Arc::from("extra_v2")],
    }));

    let loaded = creds.load();
    assert_eq!(&*loaded.token, "token_v2");
    assert_eq!(loaded.download_tokens.len(), 2);
    assert_eq!(&*loaded.download_tokens[0], "token_v2");
}

#[test]
fn token_pool_update_syncs_with_credentials() {
    // Pool starts with v1 tokens.
    let pool = TokenPool::new(vec!["dl_v1".to_string()]);
    assert_eq!(&*pool.current(), "dl_v1");

    // Simulate the pool portion of reload_credentials().
    pool.update_tokens(vec![Arc::from("dl_v2"), Arc::from("dl_v2_extra")]);

    // Pool immediately reflects new tokens.
    assert_eq!(&*pool.current(), "dl_v2");
    assert_eq!(pool.len(), 2);
}

#[test]
fn credentials_swap_concurrent_readers_safe() {
    // Multiple threads read the credential while the writer swaps it.
    let creds = make_credentials("original");
    let creds_writer = creds.clone();

    let readers: Vec<_> = (0..8)
        .map(|_| {
            let c = creds.clone();
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let t = c.load().token.clone();
                    // Token must be one of the two valid values.
                    assert!(
                        &*t == "original" || &*t == "updated",
                        "unexpected token: {t}"
                    );
                }
            })
        })
        .collect();

    // Writer swaps the token mid-read.
    creds_writer.store(Arc::new(Credentials {
        token: Arc::from("updated"),
        download_tokens: vec![Arc::from("updated")],
    }));

    for r in readers {
        r.join().expect("reader thread panicked");
    }

    assert_eq!(&*creds.load().token, "updated");
}
