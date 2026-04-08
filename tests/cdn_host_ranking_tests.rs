//! Integration tests for CDN host ranking (TODO-1).
//!
//! Tests the pure `rank_candidates` function directly — no disk I/O, no network.
//! Covers the three candidate-pool modes: IPv4-only, merged, and force-IPv6.

use std::collections::HashMap;

use rd_rs::config::ApiConfig;
use rd_rs::rd::cdn::{NetworkTestResults, rank_candidates};

/// Build a `NetworkTestResults` with the given latency maps.
fn make_results(ipv4: &[(&str, f64)], ipv6: &[(&str, f64)]) -> NetworkTestResults {
    NetworkTestResults {
        ipv4_latency: ipv4.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        ipv6_latency: ipv6.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
        ipv4_addresses: HashMap::new(),
        ipv6_addresses: HashMap::new(),
    }
}

/// Build an `ApiConfig` with the given IPv6 flags; other fields use defaults.
fn cfg(ipv6_enabled: bool, force_ipv6: bool) -> ApiConfig {
    ApiConfig {
        cdn_ipv6_enabled: ipv6_enabled,
        cdn_force_ipv6: force_ipv6,
        ..ApiConfig::default()
    }
}

// ── Mode 1: IPv4-only ────────────────────────────────────────────────────────

/// When `cdn_ipv6_enabled = false`, only IPv4 hosts are considered even if an
/// IPv6 host has lower latency.
#[test]
fn test_cdn_ipv6_disabled_uses_ipv4_only() {
    let results = make_results(
        &[("slow.download.real-debrid.com", 0.10)],
        &[("fast-ipv6.download.real-debrid.com", 0.01)], // faster but should be ignored
    );

    let (host, _) = rank_candidates(&results, &cfg(false, false)).expect("should have a result");

    assert_eq!(
        host, "slow.download.real-debrid.com",
        "IPv4-only mode must ignore the faster IPv6 host"
    );
}

/// With `cdn_ipv6_enabled = false` and an empty IPv4 map, `rank_candidates` returns `None`.
#[test]
fn test_cdn_ipv6_disabled_empty_ipv4_returns_none() {
    let results = make_results(&[], &[("ipv6-only.download.real-debrid.com", 0.01)]);
    assert!(
        rank_candidates(&results, &cfg(false, false)).is_none(),
        "empty IPv4 pool with IPv6 disabled should yield None"
    );
}

// ── Mode 2: Merged (default) ─────────────────────────────────────────────────

/// When `cdn_ipv6_enabled = true` and the IPv6 map has a faster host, that host wins.
#[test]
fn test_cdn_ipv6_enabled_merges_pools_ipv6_wins() {
    let results = make_results(
        &[("ipv4-host.download.real-debrid.com", 0.10)],
        &[("ipv6-host.download.real-debrid.com", 0.02)], // faster
    );

    let (host, latency) =
        rank_candidates(&results, &cfg(true, false)).expect("merged pool should yield a result");

    assert_eq!(host, "ipv6-host.download.real-debrid.com");
    assert!((latency - 0.02).abs() < f64::EPSILON);
}

/// When `cdn_ipv6_enabled = true` and the IPv4 host is faster, that host wins.
#[test]
fn test_cdn_ipv6_enabled_merges_pools_ipv4_wins() {
    let results = make_results(
        &[("ipv4-fast.download.real-debrid.com", 0.01)],
        &[("ipv6-slow.download.real-debrid.com", 0.10)],
    );

    let (host, _) =
        rank_candidates(&results, &cfg(true, false)).expect("merged pool should yield a result");

    assert_eq!(host, "ipv4-fast.download.real-debrid.com");
}

/// When the same hostname appears in both ipv4 and ipv6 maps, the lower latency wins.
#[test]
fn test_cdn_merge_collision_keeps_lower_latency_ipv6_is_lower() {
    let shared_host = "53.download.real-debrid.com";
    let results = make_results(
        &[(shared_host, 0.10)],
        &[(shared_host, 0.03)], // same host, lower via IPv6
    );

    let (host, latency) =
        rank_candidates(&results, &cfg(true, false)).expect("should have a result");

    assert_eq!(host, shared_host);
    assert!(
        (latency - 0.03).abs() < f64::EPSILON,
        "lower latency should win"
    );
}

#[test]
fn test_cdn_merge_collision_keeps_lower_latency_ipv4_is_lower() {
    let shared_host = "53.download.real-debrid.com";
    let results = make_results(
        &[(shared_host, 0.02)], // ipv4 is faster
        &[(shared_host, 0.09)],
    );

    let (host, latency) =
        rank_candidates(&results, &cfg(true, false)).expect("should have a result");

    assert_eq!(host, shared_host);
    assert!(
        (latency - 0.02).abs() < f64::EPSILON,
        "ipv4 lower latency should win"
    );
}

/// With `cdn_ipv6_enabled = true` but an empty IPv6 map, we fall back to IPv4 gracefully.
#[test]
fn test_cdn_ipv6_empty_falls_back_to_ipv4_gracefully() {
    let results = make_results(
        &[("ipv4-only.download.real-debrid.com", 0.05)],
        &[], // no IPv6 probed
    );

    let (host, _) = rank_candidates(&results, &cfg(true, false)).expect("should fall back to IPv4");

    assert_eq!(host, "ipv4-only.download.real-debrid.com");
}

/// Both maps empty → None.
#[test]
fn test_cdn_both_empty_returns_none() {
    let results = make_results(&[], &[]);
    assert!(rank_candidates(&results, &cfg(true, false)).is_none());
}

// ── Mode 3: Force IPv6 ───────────────────────────────────────────────────────

/// When `cdn_force_ipv6 = true`, only IPv6 hosts are candidates, even if IPv4 is faster.
#[test]
fn test_cdn_force_ipv6_discards_ipv4() {
    let results = make_results(
        &[("ipv4-super-fast.download.real-debrid.com", 0.001)], // much faster
        &[("ipv6-host.download.real-debrid.com", 0.50)],
    );

    let (host, _) =
        rank_candidates(&results, &cfg(true, true)).expect("IPv6-only pool should yield a result");

    assert_eq!(
        host, "ipv6-host.download.real-debrid.com",
        "force_ipv6 must discard the faster IPv4 host"
    );
}

/// `force_ipv6 = true` but empty IPv6 map → None.
#[test]
fn test_cdn_force_ipv6_empty_returns_none() {
    let results = make_results(
        &[("ipv4.download.real-debrid.com", 0.05)],
        &[], // no IPv6 probed
    );
    assert!(
        rank_candidates(&results, &cfg(true, true)).is_none(),
        "force_ipv6 with no probed IPv6 hosts should yield None"
    );
}

/// Among multiple IPv6 hosts, the one with minimum latency wins.
#[test]
fn test_cdn_force_ipv6_picks_fastest_ipv6() {
    let results = make_results(
        &[],
        &[
            ("slow-ipv6.download.real-debrid.com", 0.20),
            ("fast-ipv6.download.real-debrid.com", 0.04),
            ("mid-ipv6.download.real-debrid.com", 0.10),
        ],
    );

    let (host, _) = rank_candidates(&results, &cfg(true, true)).expect("should pick the fastest");

    assert_eq!(host, "fast-ipv6.download.real-debrid.com");
}
