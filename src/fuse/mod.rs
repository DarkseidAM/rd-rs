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

pub(crate) mod consts;
pub mod fs;
mod fs_helpers;
mod fs_lookup;
mod fs_readdir;
mod fs_readdirplus;
pub(crate) mod read;

pub use consts::{INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE};
pub use fs::RdFs;
