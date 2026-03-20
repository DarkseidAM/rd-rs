use crate::cache::bitmap::ByteRanges;
use std::os::unix::io::AsRawFd;

// ─── Native Sparse File Extent Scanner ────────────────────────────────────────

/// Scans mapped extents of an existing sparse file to reconstruct its ByteRanges.
pub(crate) fn scan_sparse_file(file: &std::fs::File, file_size: u64) -> ByteRanges {
    let mut ranges = ByteRanges::new();
    let fd = file.as_raw_fd();
    let mut offset: i64 = 0;
    let end = file_size as i64;

    while offset < end {
        // Find next data segment
        let data_start = unsafe { libc::lseek(fd, offset, libc::SEEK_DATA) };
        if data_start < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::ENXIO) {
                // No more data segments.
                break;
            }
            tracing::warn!("lseek(SEEK_DATA) failed: {err}");
            break;
        }

        // Find next hole after this data segment
        let hole_start = unsafe { libc::lseek(fd, data_start, libc::SEEK_HOLE) };
        if hole_start < 0 {
            tracing::warn!(
                "lseek(SEEK_HOLE) failed: {}",
                std::io::Error::last_os_error()
            );
            break;
        }

        let slice_end = hole_start.min(end);
        ranges.insert(data_start as u64, slice_end as u64);

        offset = slice_end;
    }

    ranges
}
