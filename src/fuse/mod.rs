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
//! VFS `read()` uses the disk cache, HTTP range fill, and per-fd read buffering
//! (`vfs.buffer_size`).

pub(crate) mod consts;
pub mod fs;
mod fs_helpers;
mod fs_lookup;
mod fs_readdir;
mod fs_readdirplus;
pub(crate) mod read;
pub mod vfs_read_buffer;

pub use consts::{INODE_ALL, INODE_FILE_BASE, INODE_ROOT, INODE_TORRENT_BASE};
pub use fs::RdFs;
