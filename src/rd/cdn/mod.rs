//! CDN host selection — startup network test and latency sorting.

mod probe;
mod run;
mod types;

pub use run::run_network_test;
pub use types::NetworkTestResults;
