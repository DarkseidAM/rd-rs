//! CDN host selection — startup network test and latency sorting.

mod probe;
mod run;
mod types;

mod host_map;

pub use host_map::{RankedHosts, rank_candidates};
pub use run::{rerun_cdn_network_test, run_network_test};
pub use types::NetworkTestResults;
