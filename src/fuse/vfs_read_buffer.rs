//! Per-file-handle read-ahead window (rclone-style `vfs.buffer_size`).
//!
//! Holds bytes already fetched via `read_at` but not yet returned to the kernel
//! for this `fh`. Sequential reads consume from the front; any other offset
//! invalidates the window.

use bytes::Bytes;

/// Minimum effective `buffer_size` (avoids tiny buffers and zero).
pub const MIN_BUFFER_SIZE: u64 = 64 * 1024;

/// Upper bound to avoid accidental multi-gigabyte single allocations.
pub const MAX_BUFFER_SIZE: u64 = 512 * 1024 * 1024;

pub fn clamp_buffer_size(configured: u64) -> u64 {
    configured.clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE)
}

pub struct VfsReadBuffer {
    /// File offset of the first byte in `data`.
    buf_start: u64,
    data: Bytes,
}

impl Default for VfsReadBuffer {
    fn default() -> Self {
        Self {
            buf_start: 0,
            data: Bytes::new(),
        }
    }
}

impl VfsReadBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.buf_start = 0;
        self.data = Bytes::new();
    }

    /// Bytes the current `read(offset, size)` must return (clamped to EOF).
    fn need_len(offset: u64, size: u32, file_size: u64) -> u32 {
        let end = offset.saturating_add(size as u64).min(file_size);
        end.saturating_sub(offset).min(u32::MAX as u64) as u32
    }

    /// Read-ahead: `max(buffer_size, need)` bytes, capped at EOF and `u32::MAX`.
    fn compute_fill_len(offset: u64, need: u32, file_size: u64, buffer_size: u64) -> u32 {
        let remaining = file_size.saturating_sub(offset);
        if remaining == 0 {
            return 0;
        }
        remaining
            .min(buffer_size.max(need as u64))
            .min(u32::MAX as u64) as u32
    }

    /// Decide whether this read is satisfied from the buffer or needs a `read_at` fetch.
    pub fn prepare_read(
        &mut self,
        offset: u64,
        size: u32,
        file_size: u64,
        buffer_size: u64,
    ) -> PrepareRead {
        let need = Self::need_len(offset, size, file_size);
        if need == 0 {
            return PrepareRead::Hit(Bytes::new());
        }

        if !self.data.is_empty() && offset != self.buf_start {
            self.clear();
        }

        if !self.data.is_empty() {
            let avail = self.data.len();
            if avail >= need as usize {
                let out = self.data.slice(0..need as usize);
                self.buf_start += need as u64;
                self.data = self.data.slice(need as usize..avail);
                return PrepareRead::Hit(out);
            }
            self.clear();
        }

        let fill_len = Self::compute_fill_len(offset, need, file_size, buffer_size);
        PrepareRead::Miss {
            fill_offset: offset,
            fill_len,
            take: need,
        }
    }

    /// After `read_at(fill_offset, fill_len)` succeeds, keep `take` leading bytes for the
    /// kernel reply and store the rest for subsequent reads.
    pub fn after_fetch(&mut self, fill_offset: u64, filled: Bytes, take: u32) {
        let take = take.min(filled.len() as u32) as usize;
        self.buf_start = fill_offset + take as u64;
        self.data = filled.slice(take..filled.len());
    }
}

pub enum PrepareRead {
    Hit(Bytes),
    Miss {
        fill_offset: u64,
        fill_len: u32,
        take: u32,
    },
}
