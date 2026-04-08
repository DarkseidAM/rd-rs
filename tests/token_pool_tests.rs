//! Integration tests for `rd::token_pool::TokenPool`.

use rd_rs::rd::token_pool::TokenPool;

#[test]
fn single_token_no_rotation() {
    let pool = TokenPool::new(vec!["token_a".to_string()]);
    assert_eq!(&*pool.current(), "token_a");
    assert!(
        !pool.rotate(),
        "rotate() should return false for single-token pool"
    );
    assert_eq!(
        &*pool.current(),
        "token_a",
        "token unchanged after failed rotate"
    );
    assert!(pool.is_single());
    assert_eq!(pool.len(), 1);
}

#[test]
fn rotation_advances_to_next_token() {
    let pool = TokenPool::new(vec!["token_a".to_string(), "token_b".to_string()]);
    assert_eq!(&*pool.current(), "token_a");
    assert!(pool.rotate());
    assert_eq!(&*pool.current(), "token_b");
}

#[test]
fn rotation_wraps_around() {
    let pool = TokenPool::new(vec![
        "token_a".to_string(),
        "token_b".to_string(),
        "token_c".to_string(),
    ]);
    assert_eq!(&*pool.current(), "token_a");
    pool.rotate();
    assert_eq!(&*pool.current(), "token_b");
    pool.rotate();
    assert_eq!(&*pool.current(), "token_c");
    pool.rotate();
    // Wraps back to first
    assert_eq!(&*pool.current(), "token_a");
}

#[test]
fn len_and_is_single() {
    let single = TokenPool::new(vec!["only".to_string()]);
    assert_eq!(single.len(), 1);
    assert!(single.is_single());

    let multi = TokenPool::new(vec!["a".to_string(), "b".to_string()]);
    assert_eq!(multi.len(), 2);
    assert!(!multi.is_single());
}

#[test]
fn concurrent_rotation_is_safe() {
    use std::sync::Arc;
    use std::thread;

    let pool = Arc::new(TokenPool::new(vec![
        "t0".to_string(),
        "t1".to_string(),
        "t2".to_string(),
    ]));

    let handles: Vec<_> = (0..12)
        .map(|_| {
            let p = pool.clone();
            thread::spawn(move || {
                p.rotate();
                // Just assert it doesn't panic and returns a valid token.
                let tok = p.current();
                assert!(["t0", "t1", "t2"].contains(&&*tok));
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }
}

#[test]
fn download_bearer_skips_exhausted_slots() {
    let pool = TokenPool::new(vec!["token_a".to_string(), "token_b".to_string()]);
    pool.mark_exhausted("token_a");
    assert_eq!(&*pool.download_bearer(), "token_b");
}

#[test]
fn clear_all_exhausted_restores_eligible_primary() {
    let pool = TokenPool::new(vec!["token_a".to_string(), "token_b".to_string()]);
    pool.mark_exhausted("token_a");
    pool.clear_all_exhausted();
    assert_eq!(&*pool.download_bearer(), "token_a");
}

#[test]
fn update_tokens_replaces_pool() {
    let pool = TokenPool::new(vec!["old_token".to_string()]);
    assert_eq!(&*pool.current(), "old_token");

    pool.update_tokens(vec![
        std::sync::Arc::from("new_primary"),
        std::sync::Arc::from("new_extra"),
    ]);
    // After update, index resets to 0 → primary is active.
    assert_eq!(&*pool.current(), "new_primary");
    assert_eq!(pool.len(), 2);

    // Rotation works on the new set.
    pool.rotate();
    assert_eq!(&*pool.current(), "new_extra");
}
