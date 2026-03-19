//! Rate limiter and backoff.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::interval;

pub struct RateLimiter {
    interval: tokio::sync::Mutex<tokio::time::Interval>,
}

impl RateLimiter {
    pub fn new(rate_per_minute: u32) -> Arc<Self> {
        assert!(rate_per_minute > 0, "rate_per_minute must be > 0");
        let period = Duration::from_secs(60) / rate_per_minute;
        let mut iv = interval(period);
        iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Arc::new(Self {
            interval: tokio::sync::Mutex::new(iv),
        })
    }

    pub async fn wait(&self) {
        self.interval.lock().await.tick().await;
    }
}

/// Exponential backoff with jitter, capped at 60s.
pub fn backoff(attempt: u32, base_secs: u64) -> Duration {
    let secs = (base_secs * (1u64 << attempt.min(6))).min(60);
    let jitter = (secs as f64 * 0.20 * rand_fraction()) as u64;
    Duration::from_secs(secs + jitter)
}

fn rand_fraction() -> f64 {
    use std::time::SystemTime;
    let ns = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (ns % 1000) as f64 / 1000.0
}
