use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use tracing::debug;

use crate::xet::DownloadStreamOps;

// ── Constants ──────────────────────────────────────────────────────────
// TODO: expose these as CLI args / config to allow runtime tuning without recompilation.

/// Initial prefetch window for sequential reads on a newly opened remote file.
pub(crate) const INITIAL_WINDOW: u64 = 8 * 1_048_576; // 8 MiB
/// Maximum prefetch window after repeated sequential reads double the window.
pub(crate) const MAX_WINDOW: u64 = 128 * 1_048_576; // 128 MiB
/// How far back a read can reach into the existing buffer before we consider
/// it a backward seek and reset the stream. Covers small backward jumps
/// (e.g. tar/zip re-reading headers).
pub(crate) const SEEK_WINDOW: usize = 1_048_576; // 1 MiB
/// How far forward a read can skip past the buffer end before we reset the
/// stream. Reads within this range are served by draining/discarding the
/// gap from the current stream (cheaper than a new CAS request).
pub(crate) const FORWARD_SKIP: u64 = 16 * 1_048_576; // 16 MiB

// ── FetchPlan ────────────────────────────────────────────────────────

/// How data should be fetched for this read, determined by access pattern:
///
/// - **StartStream / ContinueStream** (sequential): offset is contiguous with the
///   previous buffer end (or first read at offset 0). Uses an unbounded stream from
///   `download_stream_from_offset` with the xorb disk cache enabled, and prefetches
///   ahead (`window_size`) to amortise latency.
///
/// - **RangeDownload** (random/seek): offset is non-contiguous (backward seek, or
///   forward jump beyond `FORWARD_SKIP`). Uses a bounded `FileRange` via
///   `FileReconstructor` so the CAS API only returns terms covering the needed bytes.
///   Skips the xorb disk cache (it downloads full 64MB xorbs even for small ranges)
///   and fetches only `needed` bytes (no prefetch window).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FetchStrategy {
    /// First sequential read from offset 0, no stream yet: start one.
    StartStream,
    /// Sequential read, existing stream at the right position: keep reading.
    ContinueStream,
    /// Non-sequential or gap: use a bounded range download.
    RangeDownload,
}

impl FetchStrategy {
    pub(crate) fn is_stream(self) -> bool {
        matches!(self, Self::StartStream | Self::ContinueStream)
    }
}

/// Returned by `PrefetchState::prepare_fetch` to tell the caller what to download.
pub(crate) struct FetchPlan {
    pub strategy: FetchStrategy,
    pub fetch_size: u64,
}

// ── PrefetchState ─────────────────────────────────────────────────────

/// Per-file-handle prefetch state with adaptive window sizing.
/// The forward buffer stores download chunks as `VecDeque<Bytes>` to avoid
/// large memcpy/memmove operations. Reads within a single chunk are zero-copy
/// via `Bytes::slice()`.
pub(crate) struct PrefetchState {
    pub(crate) xet_hash: String,
    pub(crate) file_size: u64,
    // Forward buffer: chain of Bytes chunks [buf_start .. buf_start + chunks_len)
    pub(crate) chunks: VecDeque<Bytes>,
    pub(crate) chunks_len: usize,
    /// Bytes already consumed from chunks[0] (partial consumption).
    pub(crate) chunks_front_offset: usize,
    pub(crate) buf_start: u64,
    // Backward seek window: bytes [seek_start .. seek_start + seek_data.len())
    // Kept flat — capped at SEEK_WINDOW (1 MiB), not worth the complexity of Bytes.
    seek_data: VecDeque<u8>,
    seek_start: u64,
    // Adaptive window size
    pub(crate) window_size: u64,
    // Full-file stream for sequential reads (reuses one FileReconstructor)
    pub(crate) stream: Option<Box<dyn DownloadStreamOps>>,
    /// When true, drain consumed bytes after serving (no re-read from buffer).
    forward_only: bool,
}

impl PrefetchState {
    pub(crate) fn new(xet_hash: String, file_size: u64, forward_only: bool) -> Self {
        Self {
            xet_hash,
            file_size,
            chunks: VecDeque::new(),
            chunks_len: 0,
            chunks_front_offset: 0,
            buf_start: 0,
            seek_data: VecDeque::new(),
            seek_start: 0,
            window_size: INITIAL_WINDOW,
            stream: None,
            forward_only,
        }
    }

    /// Cache miss: drain consumed data, classify access pattern, adjust window,
    /// and compute how many bytes the caller should fetch.
    pub(crate) fn prepare_fetch(&mut self, offset: u64, size: u32) -> FetchPlan {
        let old_buf_end = self.buf_start + self.chunks_len as u64;
        let is_first_fetch = self.chunks_len == 0 && self.buf_start == 0;

        // Drain consumed forward bytes to seek window
        if self.chunks_len > 0 {
            let consumed = if offset >= self.buf_start { self.chunks_len } else { 0 };
            self.drain_to_seek(consumed);
        }

        // Classify access pattern and adjust window
        let is_sequential = if is_first_fetch {
            true
        } else if offset >= old_buf_end && offset <= old_buf_end + FORWARD_SKIP {
            // Sequential or small forward skip: double window (TCP slow-start)
            self.window_size = (self.window_size * 2).min(MAX_WINDOW);
            debug!("prefetch window doubled to {}", self.window_size);
            true
        } else {
            // Far seek (backward, or forward jump > FORWARD_SKIP): treat as random.
            // Trade-off: if the app actually reads sequentially from here, we pay one
            // extra CAS round-trip (~70ms) for the first RangeDownload; the next
            // contiguous read will hit is_sequential and restart streaming with
            // window=INITIAL. This is a good default for mmap/safetensors workloads
            // which are heavily random — avoids wasting ~8MB of prefetch on every seek.
            self.window_size = INITIAL_WINDOW;
            debug!("prefetch window reset to {}", self.window_size);
            false
        };

        // Decide fetch strategy
        let strategy = if is_sequential {
            if offset == self.buf_start {
                // Contiguous with buffer — keep streaming.
                if self.stream.is_none() && is_first_fetch && offset == 0 {
                    FetchStrategy::StartStream
                } else {
                    FetchStrategy::ContinueStream
                }
            } else {
                // Small forward gap (within FORWARD_SKIP): restart stream at new offset.
                // Keeps windowed prefetch for strided sequential patterns (e.g. tar skip).
                if let Some(s) = self.stream.take() {
                    debug!("prefetch: restarting stream (forward skip)");
                    drop(s);
                }
                FetchStrategy::StartStream
            }
        } else {
            // Far seek: cancel stale stream, use bounded range download.
            if let Some(s) = self.stream.take() {
                debug!("prefetch: cancelling stream");
                drop(s);
            }
            FetchStrategy::RangeDownload
        };

        let needed = (size as u64).min(self.file_size - offset);
        // For sequential reads, prefetch ahead (window_size) to amortise latency.
        // For random/seek reads (RangeDownload), fetch only what's needed — any
        // extra bytes are likely wasted on the next random seek.
        let fetch_size = match strategy {
            FetchStrategy::RangeDownload => needed,
            _ => needed.max(self.window_size),
        }
        .min(self.file_size - offset);

        FetchPlan { strategy, fetch_size }
    }

    /// Store freshly downloaded chunks in the forward buffer.
    pub(crate) fn store_fetched(&mut self, offset: u64, chunks: VecDeque<Bytes>, total: usize) {
        self.chunks = chunks;
        self.chunks_len = total;
        self.chunks_front_offset = 0;
        self.buf_start = offset;
    }

    /// Effective length of the front chunk, accounting for partially consumed bytes.
    fn front_chunk_remaining(&self) -> Option<usize> {
        self.chunks.front().map(|c| c.len() - self.chunks_front_offset)
    }

    /// Try to serve a read from the forward buffer.
    /// Returns a zero-copy `Bytes` slice when the read fits in one chunk.
    /// In forward-only mode, drains consumed bytes so re-reads must refetch.
    pub(crate) fn try_serve_forward(&mut self, offset: u64, size: u32) -> Option<Bytes> {
        if self.chunks_len == 0 {
            return None;
        }
        let buf_end = self.buf_start + self.chunks_len as u64;
        if offset < self.buf_start || offset >= buf_end {
            return None;
        }
        let logical_off = (offset - self.buf_start) as usize;
        let avail = self.chunks_len - logical_off;
        let to_read = (size as usize).min(avail);
        let data = read_chunk_range(&self.chunks, self.chunks_front_offset, logical_off, to_read);
        // In forward-only mode (--direct-io), evict consumed bytes so the
        // prefetch buffer cannot serve re-reads — forces a CAS refetch,
        // giving honest benchmark numbers without buffer-as-cache effects.
        if self.forward_only && to_read > 0 {
            let consumed = logical_off + to_read;
            self.drain_to_seek(consumed);
        }
        Some(data)
    }

    /// Try to serve a read from the backward seek window.
    /// Disabled in forward-only mode (no backward cache).
    pub(crate) fn try_serve_seek(&mut self, offset: u64, size: u32) -> Option<Bytes> {
        if self.forward_only || self.seek_data.is_empty() {
            return None;
        }
        let seek_end = self.seek_start + self.seek_data.len() as u64;
        if offset >= self.seek_start && offset < seek_end {
            let local_off = (offset - self.seek_start) as usize;
            let avail = self.seek_data.len() - local_off;
            let to_read = (size as usize).min(avail);
            let slice = self.seek_data.make_contiguous();
            Some(Bytes::copy_from_slice(&slice[local_off..local_off + to_read]))
        } else {
            None
        }
    }

    /// Move consumed bytes from the front of the forward buffer into the seek window,
    /// then pop the consumed chunks. Uses `pop_front()` — O(1) per chunk, no memmove.
    pub(crate) fn drain_to_seek(&mut self, consumed: usize) {
        if consumed == 0 {
            return;
        }
        let to_move = consumed.min(self.chunks_len);

        // In forward-only mode, skip seek window population (just discard).
        if !self.forward_only {
            let seek_end = self.seek_start + self.seek_data.len() as u64;
            let contiguous = self.seek_data.is_empty() || seek_end == self.buf_start;

            if contiguous && to_move <= SEEK_WINDOW {
                self.copy_chunks_to_seek(0, to_move);
                if self.seek_data.len() > SEEK_WINDOW {
                    let excess = self.seek_data.len() - SEEK_WINDOW;
                    drop(self.seek_data.drain(..excess));
                    self.seek_start += excess as u64;
                }
            } else {
                let keep = to_move.min(SEEK_WINDOW);
                self.seek_data.clear();
                self.seek_start = self.buf_start + (to_move - keep) as u64;
                self.copy_chunks_to_seek(to_move - keep, keep);
            }
        }

        // Pop consumed chunks from front
        let mut remaining = to_move;
        while remaining > 0 {
            let Some(eff) = self.front_chunk_remaining() else { break };
            if remaining >= eff {
                self.chunks.pop_front();
                self.chunks_len -= eff;
                self.chunks_front_offset = 0;
                remaining -= eff;
            } else {
                self.chunks_front_offset += remaining;
                self.chunks_len -= remaining;
                remaining = 0;
            }
        }
        self.buf_start += to_move as u64;
    }

    fn copy_chunks_to_seek(&mut self, skip: usize, count: usize) {
        let data = read_chunk_range(&self.chunks, self.chunks_front_offset, skip, count);
        self.seek_data.extend(&data[..]);
    }
}

/// Read `count` bytes starting at logical offset `skip` from the chunk buffer.
/// Returns zero-copy `Bytes::slice()` when the read fits in a single chunk.
fn read_chunk_range(chunks: &VecDeque<Bytes>, front_offset: usize, skip: usize, count: usize) -> Bytes {
    if count == 0 {
        return Bytes::new();
    }
    let mut logical_off = skip;
    for (i, chunk) in chunks.iter().enumerate() {
        let start = if i == 0 { front_offset } else { 0 };
        let eff = chunk.len() - start;
        if logical_off < eff {
            let chunk_start = start + logical_off;
            let chunk_avail = chunk.len() - chunk_start;
            if count <= chunk_avail {
                return chunk.slice(chunk_start..chunk_start + count);
            }
            let mut out = BytesMut::with_capacity(count);
            out.extend_from_slice(&chunk[chunk_start..]);
            let mut remaining = count - chunk_avail;
            for next_chunk in chunks.iter().skip(i + 1) {
                let take = remaining.min(next_chunk.len());
                out.extend_from_slice(&next_chunk[..take]);
                remaining -= take;
                if remaining == 0 {
                    break;
                }
            }
            return out.freeze();
        }
        logical_off -= eff;
    }
    Bytes::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a PrefetchState pre-loaded with chunks at a given offset.
    fn ps_with_chunks(buf_start: u64, chunks: &[&[u8]]) -> PrefetchState {
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false);
        ps.buf_start = buf_start;
        for chunk in chunks {
            let b = Bytes::copy_from_slice(chunk);
            ps.chunks_len += b.len();
            ps.chunks.push_back(b);
        }
        ps
    }

    // ── Forward buffer tests ────────────────────────────────────────

    #[test]
    fn forward_single_chunk_zero_copy() {
        // Read [1..4) from a single chunk — should be a Bytes::slice(), no memcpy.
        let mut ps = ps_with_chunks(0, &[&[10, 20, 30, 40, 50]]);
        let result = ps.try_serve_forward(1, 3).unwrap();
        assert_eq!(&result[..], &[20, 30, 40]);
    }

    #[test]
    fn forward_spans_two_chunks() {
        // Read crosses chunk boundary: last byte of chunk 0 + first 2 of chunk 1.
        // This path allocates a BytesMut and copies from both chunks.
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3], &[4, 5, 6]]);
        let result = ps.try_serve_forward(2, 3).unwrap();
        assert_eq!(&result[..], &[3, 4, 5]);
    }

    #[test]
    fn forward_with_front_offset() {
        // After a partial drain, chunks_front_offset skips consumed bytes in chunks[0].
        // Buffer: chunk=[10,20,30,40,50], front_offset=2 → effective data is [30,40,50]
        let mut ps = ps_with_chunks(102, &[&[10, 20, 30, 40, 50]]);
        ps.chunks_front_offset = 2;
        ps.chunks_len -= 2;
        let result = ps.try_serve_forward(103, 2).unwrap();
        assert_eq!(&result[..], &[40, 50]);
    }

    #[test]
    fn forward_out_of_range() {
        // Reads before or after the buffer should return None.
        let mut ps = ps_with_chunks(100, &[&[1, 2, 3]]);
        assert!(ps.try_serve_forward(99, 1).is_none()); // before
        assert!(ps.try_serve_forward(103, 1).is_none()); // after
    }

    #[test]
    fn forward_clamps_to_available() {
        // Requesting more bytes than available should clamp to what's left.
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3]]);
        let result = ps.try_serve_forward(1, 100).unwrap();
        assert_eq!(&result[..], &[2, 3]);
    }

    // ── Seek window tests ─────────────────────────────────────────

    #[test]
    fn seek_window_basic() {
        // Backward seek window serves previously consumed bytes.
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false);
        ps.seek_data.extend(&[10, 20, 30, 40, 50]);
        ps.seek_start = 100;
        let result = ps.try_serve_seek(102, 2).unwrap();
        assert_eq!(&result[..], &[30, 40]);
    }

    #[test]
    fn seek_window_out_of_range() {
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false);
        ps.seek_data.extend(&[1, 2, 3]);
        ps.seek_start = 100;
        assert!(ps.try_serve_seek(99, 1).is_none());
        assert!(ps.try_serve_seek(103, 1).is_none());
    }

    // ── Drain tests ───────────────────────────────────────────────

    #[test]
    fn drain_pops_full_chunks() {
        // Consuming 6 bytes should pop the first two 3-byte chunks entirely,
        // leaving only the third chunk. Drained bytes go to the seek window.
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3], &[4, 5, 6], &[7, 8, 9]]);
        ps.drain_to_seek(6);
        assert_eq!(ps.buf_start, 6);
        assert_eq!(ps.chunks_len, 3);
        assert_eq!(ps.chunks.len(), 1);
        assert_eq!(&ps.chunks[0][..], &[7, 8, 9]);
        let seek: Vec<u8> = ps.seek_data.iter().copied().collect();
        assert_eq!(seek, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn drain_partial_chunk() {
        // Consuming 3 of 5 bytes should advance chunks_front_offset,
        // not pop the chunk. The remaining 2 bytes stay readable.
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3, 4, 5]]);
        ps.drain_to_seek(3);
        assert_eq!(ps.buf_start, 3);
        assert_eq!(ps.chunks_len, 2);
        assert_eq!(ps.chunks_front_offset, 3);
        let result = ps.try_serve_forward(3, 2).unwrap();
        assert_eq!(&result[..], &[4, 5]);
    }

    #[test]
    fn drain_seek_window_capped() {
        // The seek window is capped at SEEK_WINDOW (1 MiB). Draining more
        // should only keep the last SEEK_WINDOW bytes.
        let big = vec![42u8; SEEK_WINDOW + 100];
        let mut ps = ps_with_chunks(0, &[&big]);
        ps.drain_to_seek(big.len());
        assert_eq!(ps.seek_data.len(), SEEK_WINDOW);
        assert!(ps.seek_data.iter().all(|&b| b == 42));
    }

    #[test]
    fn drain_then_forward_serves_remainder() {
        // After draining across a chunk boundary (3 bytes = full chunk [1,2]
        // + 1 byte from [3,4]), the remaining [4,5,6] should still be readable.
        let mut ps = ps_with_chunks(0, &[&[1, 2], &[3, 4], &[5, 6]]);
        ps.drain_to_seek(3);
        assert_eq!(ps.chunks_len, 3);
        let result = ps.try_serve_forward(3, 3).unwrap();
        assert_eq!(&result[..], &[4, 5, 6]);
    }

    #[test]
    fn empty_buffer() {
        let mut ps = PrefetchState::new("hash".into(), 100, false);
        assert!(ps.try_serve_forward(0, 10).is_none());
    }

    // ── Edge case tests ─────────────────────────────────────────────

    #[test]
    fn forward_zero_size() {
        // A zero-size read at a valid offset should return Some(empty).
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3]]);
        let result = ps.try_serve_forward(0, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn seek_zero_size() {
        // A zero-size seek read at a valid offset should return Some(empty).
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false);
        ps.seek_data.extend(&[1, 2, 3]);
        ps.seek_start = 0;
        let result = ps.try_serve_seek(0, 0).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn drain_non_contiguous_seek_resets() {
        // Gap between seek window end and forward buffer start: seek should reset.
        let mut ps = ps_with_chunks(1000, &[&[10, 20, 30, 40, 50]]);
        ps.seek_data.extend(&[1, 2, 3]);
        ps.seek_start = 0; // seek covers [0..3), buffer starts at 1000 → gap
        ps.drain_to_seek(5);
        assert_eq!(ps.seek_start, 1000);
        let seek: Vec<u8> = ps.seek_data.iter().copied().collect();
        assert_eq!(seek, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn drain_contiguous_trims_excess() {
        // Seek window almost full + contiguous append should trim oldest bytes.
        let initial_size = SEEK_WINDOW - 3;
        let mut ps = ps_with_chunks(initial_size as u64, &[&[1, 2, 3, 4, 5]]);
        ps.seek_data.extend(vec![0u8; initial_size]);
        ps.seek_start = 0; // seek covers [0..initial_size), contiguous with buffer
        ps.drain_to_seek(5);
        // Appended 5 bytes → total initial_size + 5, excess = 2
        assert_eq!(ps.seek_data.len(), SEEK_WINDOW);
        assert_eq!(ps.seek_start, 2);
    }

    #[test]
    fn drain_with_front_offset_across_boundary() {
        // Partial first chunk (front_offset=2) + drain across chunk boundary.
        let mut ps = ps_with_chunks(10, &[&[1, 2, 3, 4, 5], &[6, 7, 8]]);
        ps.chunks_front_offset = 2;
        ps.chunks_len -= 2; // effective: [3,4,5] + [6,7,8] = 6 bytes
        ps.drain_to_seek(4); // consume [3,4,5,6]
        assert_eq!(ps.buf_start, 14);
        assert_eq!(ps.chunks_len, 2); // [7,8] remain
        assert_eq!(ps.chunks_front_offset, 1); // 1 byte into second chunk
        let seek: Vec<u8> = ps.seek_data.iter().copied().collect();
        assert_eq!(seek, vec![3, 4, 5, 6]);
    }

    #[test]
    fn drain_clamps_to_available() {
        // Draining more bytes than available should clamp to chunks_len.
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3]]);
        ps.drain_to_seek(100);
        assert_eq!(ps.buf_start, 3);
        assert_eq!(ps.chunks_len, 0);
        assert!(ps.chunks.is_empty());
        let seek: Vec<u8> = ps.seek_data.iter().copied().collect();
        assert_eq!(seek, vec![1, 2, 3]);
    }

    // ── Forward-only mode tests ─────────────────────────────────────

    fn ps_forward_only(buf_start: u64, chunks: &[&[u8]]) -> PrefetchState {
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, true);
        ps.buf_start = buf_start;
        for chunk in chunks {
            let b = Bytes::copy_from_slice(chunk);
            ps.chunks_len += b.len();
            ps.chunks.push_back(b);
        }
        ps
    }

    #[test]
    fn forward_only_reread_misses() {
        let mut ps = ps_forward_only(0, &[&[1, 2, 3, 4, 5]]);
        let result = ps.try_serve_forward(0, 3).unwrap();
        assert_eq!(&result[..], &[1, 2, 3]);
        // Re-read at same offset misses (data was drained)
        assert!(ps.try_serve_forward(0, 3).is_none());
        // Remaining data still available
        let result = ps.try_serve_forward(3, 2).unwrap();
        assert_eq!(&result[..], &[4, 5]);
    }

    #[test]
    fn non_forward_only_reread_hits() {
        let mut ps = ps_with_chunks(0, &[&[1, 2, 3, 4, 5]]);
        let result = ps.try_serve_forward(0, 3).unwrap();
        assert_eq!(&result[..], &[1, 2, 3]);
        // Re-read at same offset hits (data retained)
        let result = ps.try_serve_forward(0, 3).unwrap();
        assert_eq!(&result[..], &[1, 2, 3]);
    }
}
