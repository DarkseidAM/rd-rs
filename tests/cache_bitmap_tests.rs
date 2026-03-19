//! Tests for `cache::bitmap::ByteRanges`.

use rd_rs::cache::bitmap::ByteRanges;

#[test]
fn empty() {
    let r = ByteRanges::new();
    assert!(!r.has_range(0, 10));
    assert_eq!(r.find_missing(0, 10), Some((0, 10)));
}

#[test]
fn single_insert_and_query() {
    let mut r = ByteRanges::new();
    r.insert(10, 30);

    assert!(!r.has_range(0, 10));
    assert!(!r.has_range(5, 20));
    assert!(r.has_range(10, 30));
    assert!(r.has_range(10, 20));
    assert!(r.has_range(15, 25));
    assert!(!r.has_range(10, 31));
}

#[test]
fn merge_adjacent() {
    let mut r = ByteRanges::new();
    r.insert(0, 10);
    r.insert(10, 20);
    assert_eq!(r.len(), 1);
    assert!(r.has_range(0, 20));
}

#[test]
fn merge_overlapping() {
    let mut r = ByteRanges::new();
    r.insert(0, 15);
    r.insert(10, 25);
    assert_eq!(r.len(), 1);
    assert!(r.has_range(0, 25));
}

#[test]
fn merge_three_into_one() {
    let mut r = ByteRanges::new();
    r.insert(0, 5);
    r.insert(10, 15);
    r.insert(20, 25);
    assert_eq!(r.len(), 3);
    r.insert(0, 25);
    assert_eq!(r.len(), 1);
    assert!(r.has_range(0, 25));
}

#[test]
fn find_missing_basic() {
    let mut r = ByteRanges::new();
    r.insert(10, 20);
    // Gap at start
    assert_eq!(r.find_missing(0, 30), Some((0, 10)));
    // Gap at end
    assert_eq!(r.find_missing(10, 30), Some((20, 30)));
    // Fully covered
    assert_eq!(r.find_missing(10, 20), None);
}

#[test]
fn find_missing_multi_gap() {
    let mut r = ByteRanges::new();
    r.insert(5, 10);
    r.insert(20, 30);
    // First gap is [0, 5)
    assert_eq!(r.find_missing(0, 40), Some((0, 5)));
}

#[test]
fn total_bytes() {
    let mut r = ByteRanges::new();
    r.insert(0, 10);
    r.insert(20, 30);
    assert_eq!(r.total_bytes(), 20);
}

#[test]
fn zero_size_insert_noop() {
    let mut r = ByteRanges::new();
    r.insert(5, 5);
    assert!(r.is_empty());
}

#[test]
fn cache_key_format() {
    use rd_rs::cache::Cache;
    assert_eq!(Cache::build_key("abc123", "video.mkv"), "abc123/video.mkv");
}
