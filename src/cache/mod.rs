//! Cache layer — sparse file VFS cache for rd-rs.
//!
//! Public surface:
//! - [`Cache`]     — global store; call `Cache::new()` once at startup
//! - [`CacheItem`] — per-file sparse file handle; obtain via `Cache::get_or_create`
//! - [`bitmap::ByteRanges`] — interval list tracking downloaded byte ranges

pub mod bitmap;
#[allow(clippy::module_inception)]
pub mod cache;
pub mod item;

pub use cache::Cache;
pub use item::{CacheItem, CacheReadError};
