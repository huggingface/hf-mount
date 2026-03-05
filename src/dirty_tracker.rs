/// Tracks dirty (modified) byte ranges in a sparsely-opened file.
///
/// Uses a sorted, non-overlapping range list. For typical append + small-edit
/// workloads the list stays very small (1-3 entries).
#[derive(Debug, Clone)]
pub struct DirtyTracker {
    /// Sorted, non-overlapping dirty ranges as `(start, end)` (end exclusive).
    ranges: Vec<(u64, u64)>,
    /// Size of the original (pre-modification) file.
    pub original_size: u64,
}

impl DirtyTracker {
    pub fn new(original_size: u64) -> Self {
        Self {
            ranges: Vec::new(),
            original_size,
        }
    }

    /// Mark the byte range `[offset, offset+len)` as dirty. Merges overlapping/adjacent ranges.
    pub fn mark_dirty(&mut self, offset: u64, len: u64) {
        if len == 0 {
            return;
        }
        let start = offset;
        let end = offset + len;

        // Find the insertion point and merge with any overlapping/adjacent ranges.
        // A range (a, b) overlaps or is adjacent to [start, end) when a <= end && b >= start.
        let mut new_start = start;
        let mut new_end = end;
        let mut first = self.ranges.len();
        let mut last = 0;

        for (i, &(a, b)) in self.ranges.iter().enumerate() {
            if a <= end && b >= start {
                if first > i {
                    first = i;
                }
                last = i + 1;
                new_start = new_start.min(a);
                new_end = new_end.max(b);
            }
        }

        if first < last {
            // Replace the overlapping range(s) with the merged one
            self.ranges.splice(first..last, std::iter::once((new_start, new_end)));
        } else {
            // No overlap — insert at sorted position
            let pos = self.ranges.partition_point(|&(a, _)| a < start);
            self.ranges.insert(pos, (new_start, new_end));
        }
    }

    /// Returns `true` if any byte in `[offset, offset+len)` is dirty.
    pub fn is_dirty(&self, offset: u64, len: u64) -> bool {
        if len == 0 {
            return false;
        }
        let end = offset + len;
        self.ranges.iter().any(|&(a, b)| a < end && b > offset)
    }

    /// Returns `true` if the entire range `[offset, offset+len)` is covered by dirty ranges.
    pub fn is_fully_dirty(&self, offset: u64, len: u64) -> bool {
        if len == 0 {
            return true;
        }
        let end = offset + len;
        // Find ranges that overlap [offset, end). They must fully cover it.
        let mut cursor = offset;
        for &(a, b) in &self.ranges {
            if a > cursor {
                // Gap before this range
                return false;
            }
            if a <= cursor && b > cursor {
                cursor = b;
                if cursor >= end {
                    return true;
                }
            }
        }
        false
    }

    /// Returns the sorted, non-overlapping dirty ranges as `(offset, length)` pairs.
    pub fn dirty_ranges(&self) -> Vec<(u64, u64)> {
        self.ranges.iter().map(|&(start, end)| (start, end - start)).collect()
    }

    /// Returns `true` if no bytes have been marked dirty.
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tracker() {
        let tracker = DirtyTracker::new(1024);
        assert!(tracker.is_empty());
        assert!(!tracker.is_dirty(0, 100));
        assert!(tracker.dirty_ranges().is_empty());
    }

    #[test]
    fn test_single_range() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 50);
        assert!(tracker.is_dirty(100, 50));
        assert!(tracker.is_dirty(120, 10));
        assert!(!tracker.is_dirty(0, 100));
        assert!(!tracker.is_dirty(150, 10));
        assert_eq!(tracker.dirty_ranges(), vec![(100, 50)]);
    }

    #[test]
    fn test_append_range() {
        let mut tracker = DirtyTracker::new(500);
        tracker.mark_dirty(500, 20);
        assert!(tracker.is_dirty(500, 20));
        assert!(!tracker.is_dirty(0, 500));
        assert_eq!(tracker.dirty_ranges(), vec![(500, 20)]);
    }

    #[test]
    fn test_merge_overlapping() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 50); // [100, 150)
        tracker.mark_dirty(130, 50); // [130, 180) — overlaps
        assert_eq!(tracker.dirty_ranges(), vec![(100, 80)]); // merged: [100, 180)
    }

    #[test]
    fn test_merge_adjacent() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 50); // [100, 150)
        tracker.mark_dirty(150, 50); // [150, 200) — adjacent
        assert_eq!(tracker.dirty_ranges(), vec![(100, 100)]); // merged: [100, 200)
    }

    #[test]
    fn test_merge_multiple() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 10); // [100, 110)
        tracker.mark_dirty(200, 10); // [200, 210)
        tracker.mark_dirty(300, 10); // [300, 310)
        assert_eq!(tracker.dirty_ranges(), vec![(100, 10), (200, 10), (300, 10)]);

        // Now merge all three with one big range
        tracker.mark_dirty(100, 210); // [100, 310)
        assert_eq!(tracker.dirty_ranges(), vec![(100, 210)]);
    }

    #[test]
    fn test_zero_length() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 0);
        assert!(tracker.is_empty());
        assert!(!tracker.is_dirty(100, 0));
    }

    #[test]
    fn test_is_fully_dirty() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 50); // [100, 150)
        assert!(tracker.is_fully_dirty(100, 50));
        assert!(tracker.is_fully_dirty(110, 20));
        assert!(!tracker.is_fully_dirty(90, 50)); // starts before dirty
        assert!(!tracker.is_fully_dirty(140, 20)); // extends past dirty
    }

    #[test]
    fn test_non_overlapping_ranges() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 10); // [100, 110)
        tracker.mark_dirty(200, 10); // [200, 210)
        assert!(tracker.is_dirty(100, 10));
        assert!(tracker.is_dirty(200, 10));
        assert!(!tracker.is_dirty(110, 90)); // gap between
        assert_eq!(tracker.dirty_ranges(), vec![(100, 10), (200, 10)]);
    }

    #[test]
    fn test_insert_before_existing() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(200, 10);
        tracker.mark_dirty(100, 10);
        assert_eq!(tracker.dirty_ranges(), vec![(100, 10), (200, 10)]);
    }

    #[test]
    fn test_superset_merge() {
        let mut tracker = DirtyTracker::new(1024);
        tracker.mark_dirty(100, 10);
        tracker.mark_dirty(200, 10);
        // Mark a superset that covers both
        tracker.mark_dirty(50, 250);
        assert_eq!(tracker.dirty_ranges(), vec![(50, 250)]);
    }
}
