//! CDN types and constants.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub const SERVER_LIST_URL: &str =
    "https://nzimhzbfnannoxumremm.supabase.co/storage/v1/object/public/public-files/servers.txt";
pub const RESULTS_FILE: &str = "data/network_test_results.json";
pub const TIMESTAMP_FILE: &str = "data/network_test_timestamp";
pub const CACHE_TTL_SECS: u64 = 24 * 3600;
pub const MAX_CONCURRENT: usize = 8;
pub const DNS_PROBE_INITIAL_CEILING: u32 = 100;
pub const DNS_PROBE_CEILING_EXTENSION: u32 = 30;

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct NetworkTestResults {
    pub ipv4_latency: HashMap<String, f64>,
    pub ipv6_latency: HashMap<String, f64>,
    pub ipv4_addresses: HashMap<String, String>,
    pub ipv6_addresses: HashMap<String, String>,
}

pub(super) struct ServerEntry {
    pub(super) hostname: String,
    pub(super) ipv4: Option<String>,
    pub(super) ipv6: Option<String>,
}
