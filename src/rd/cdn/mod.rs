//! CDN host selection — startup network test and latency sorting.

mod probe;
mod run;
mod types;

mod host_map;

pub use host_map::RankedHosts;
pub use run::run_network_test;
pub use types::NetworkTestResults;
