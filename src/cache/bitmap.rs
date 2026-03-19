//! `ByteRanges` — a sorted, merged list of `[start, end)` byte intervals.
//!
//! Used to track which regions of a sparse cache file have been downloaded.
//! We use a `Vec` of intervals rather than `bit-vec` because real-world
//! streaming produces large contiguous regions, so there are typically fewer
//! than 10 distinct intervals even for a file with many seeks.

/// A sorted, non-overlapping collection of `[start, end)` byte intervals.
#[derive(Debug, Clone, Default)]
pub struct ByteRanges {
    /// Invariant: sorted by `start`, non-overlapping, non-adjacent.
    intervals: Vec<(u64, u64)>,
}

impl ByteRanges {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of distinct intervals.
    pub fn len(&self) -> usize {
        self.intervals.len()
    }

    pub fn is_empty(&self) -> bool {
        self.intervals.is_empty()
    }

    /// Total bytes covered by all intervals.
    pub fn total_bytes(&self) -> u64 {
        self.intervals.iter().map(|(s, e)| e - s).sum()
    }

    /// Insert the interval `[start, end)`, merging with any overlapping or
    /// adjacent existing intervals.
    pub fn insert(&mut self, start: u64, end: u64) {
        if start >= end {
            return;
        }

        // Find the first interval that may overlap or be adjacent.
        let mut new_start = start;
        let mut new_end = end;

        let left = self.intervals.partition_point(|(_, e)| *e < new_start);

        // The right bound: first interval whose start is strictly after new_end.
        let right = {
            let mut r = left;
            while r < self.intervals.len() && self.intervals[r].0 <= new_end {
                r += 1;
            }
            r
        };

        // Merge all overlapping / adjacent intervals.
        if left < right {
            new_start = new_start.min(self.intervals[left].0);
            new_end = new_end.max(self.intervals[right - 1].1);
        }

        self.intervals.splice(left..right, [(new_start, new_end)]);
    }

    /// Returns `true` if `[start, end)` is fully covered.
    pub fn has_range(&self, start: u64, end: u64) -> bool {
        if start >= end {
            return true;
        }
        // Binary search for an interval that starts at or before `start`.
        let idx = self.intervals.partition_point(|(s, _)| *s <= start);
        if idx == 0 {
            return false;
        }
        let (_, ie) = self.intervals[idx - 1];
        ie >= end
    }

    /// Returns the first sub-range of `[start, end)` that is **not** present,
    /// i.e. the leading missing bytes. Returns `None` if fully covered.
    ///
    /// This is used by the downloader to know where to start the next HTTP request.
    pub fn find_missing(&self, start: u64, end: u64) -> Option<(u64, u64)> {
        if start >= end || self.has_range(start, end) {
            return None;
        }

        // Walk intervals to find the first gap inside [start, end).
        let mut cursor = start;
        for &(is, ie) in &self.intervals {
            if is > cursor {
                // Gap between cursor and the start of this interval.
                return Some((cursor, is.min(end)));
            }
            if ie > cursor {
                cursor = ie;
            }
            if cursor >= end {
                return None;
            }
        }
        // Cursor is still inside [start, end).
        Some((cursor, end))
    }
}
