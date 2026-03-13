//! FUSE filesystem for rd-rs.
//!
//! Exposes the torrent library via a read-only FUSE mount at the structure:
//!
//! ```text
//! <mount_path>/
//! └── __all__/
//!     ├── <hash>/<name>/   ← access_key as a directory
//!     │   ├── movie.mkv
//!     │   └── ...
//!     └── ...
//! ```
//!
//! VFS `read()` is stubbed in Phase 2 — Phase 3 adds the HTTP range + disk
//! cache layer.

pub mod fs;

pub use fs::RdFs;
