use std::collections::VecDeque;

use bytes::{Bytes, BytesMut};
use tracing::{debug, warn};

use crate::xet::DownloadStreamOps;

/// Parse a safetensors JSON header into a sorted list of `(start, end)`
/// absolute file offsets, one per tensor. `header_len` is the value of the
/// first u64 LE in the file; `header_json` is the next `header_len` bytes.
///
/// Returns `None` on any parse failure (the file may not actually be
/// safetensors, even if the extension says so) — callers fall back to the
/// generic `MIN_RANGE_FETCH_MMAP` path.
pub(crate) fn parse_safetensors_layout(
    header_len: u64,
    header_json: &[u8],
    file_size: u64,
) -> Option<Vec<(u64, u64)>> {
    let value: serde_json::Value = serde_json::from_slice(header_json).ok()?;
    let obj = value.as_object()?;
    // `data_offsets` in the header are relative to the start of the data
    // section, which lives at byte `8 + header_len` in the file.
    let data_origin = 8u64.checked_add(header_len)?;
    let mut layout: Vec<(u64, u64)> = Vec::with_capacity(obj.len());
    for (key, v) in obj {
        if key == "__metadata__" {
            continue;
        }
        let offsets = v.get("data_offsets").and_then(|x| x.as_array())?;
        if offsets.len() != 2 {
            warn!("safetensors header: tensor {} has malformed data_offsets", key);
            return None;
        }
        let start_rel = offsets[0].as_u64()?;
        let end_rel = offsets[1].as_u64()?;
        if end_rel < start_rel {
            warn!("safetensors header: tensor {} has reversed data_offsets", key);
            return None;
        }
        let start = data_origin.checked_add(start_rel)?;
        let end = data_origin.checked_add(end_rel)?;
        if end > file_size {
            warn!(
                "safetensors header: tensor {} extends past file_size ({} > {})",
                key, end, file_size
            );
            return None;
        }
        layout.push((start, end));
    }
    layout.sort();
    // Detect overlaps: each tensor must end before (or at) the next one starts.
    for w in layout.windows(2) {
        if w[0].1 > w[1].0 {
            warn!("safetensors header: tensors overlap at offset {}", w[1].0);
            return None;
        }
    }
    if layout.is_empty() {
        warn!("safetensors header parsed but contained no tensors");
        return None;
    }
    Some(layout)
}

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
/// Minimum bytes to fetch on a RangeDownload (random/seek). Prevents the
/// pathological "32×128 KiB CAS round-trips per 4 MiB pread" pattern when the
/// kernel splits a userspace read into FUSE chunks: with this floor, the first
/// chunk pulls a fat block, subsequent chunks of the same pread hit the
/// forward buffer.
pub(crate) const MIN_RANGE_FETCH: u64 = 4 * 1_048_576; // 4 MiB
/// Larger floor used when the file looks mmap-friendly (e.g. `.safetensors`,
/// `.bin`, `.gguf`). transformers/torch loaders mmap these files and access
/// them via many small page-sized reads, which appear random to FUSE but are
/// actually a scan within a tensor. Pulling 32 MiB on the first miss covers
/// most of a tensor in one CAS round-trip. Used as fallback when the
/// tensor-aligned path (`tensor_end_for`) doesn't apply.
pub(crate) const MIN_RANGE_FETCH_MMAP: u64 = 32 * 1_048_576; // 32 MiB
/// Cap on a tensor-aligned RangeDownload. Some safetensors tensors are
/// hundreds of MiB (a single big embedding); fetching the whole thing at the
/// first FUSE chunk would block other readers and waste CAS bandwidth if
/// only the first few MiB are actually consumed. 64 MiB is roughly one CAS
/// xorb, which is the natural batching granularity downstream.
pub(crate) const MAX_TENSOR_FETCH: u64 = 64 * 1_048_576; // 64 MiB

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
    /// File looks like a tensor checkpoint that's typically mmap'd (safetensors,
    /// pytorch bin, gguf). Bumps the floor on RangeDownload fetches so a
    /// pages-faulted scan within a tensor doesn't hammer CAS once per FUSE chunk.
    pub(crate) mmap_hint: bool,
    /// Sorted list of `(start, end)` absolute file offsets, one per tensor.
    /// Populated only for `.safetensors` files whose header parsed at open
    /// time. Used in `prepare_fetch` to align `fetch_size` on tensor
    /// boundaries — fetch exactly the remainder of the tensor that contains
    /// `offset`, no more, no less. Avoids both over-fetch into the next
    /// tensor and the per-FUSE-chunk CAS round-trip pattern.
    pub(crate) tensor_layout: Option<Vec<(u64, u64)>>,
    /// Speculative buffer for tensor i+1, fetched in the background while
    /// tensor i is being scanned. Matched by absolute offset; promoted to
    /// the forward buffer on a hit.
    pub(crate) speculative: Option<SpeculativeBuf>,
    /// Tensor start currently being prefetched in the background (to avoid
    /// double-spawn). Cleared when the task stores its result.
    pub(crate) speculative_inflight: Option<u64>,
}

/// Background-prefetched bytes for the tensor following the one currently
/// being scanned.
pub(crate) struct SpeculativeBuf {
    pub(crate) start: u64,
    pub(crate) data: Bytes,
}

impl PrefetchState {
    pub(crate) fn new(xet_hash: String, file_size: u64, forward_only: bool, mmap_hint: bool) -> Self {
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
            mmap_hint,
            tensor_layout: None,
            speculative: None,
            speculative_inflight: None,
        }
    }

    pub(crate) fn set_tensor_layout(&mut self, layout: Vec<(u64, u64)>) {
        self.tensor_layout = Some(layout);
    }

    /// Pre-populate the forward buffer with bytes we already pulled (e.g. the
    /// safetensors header probe). The first reads at this offset hit the
    /// buffer with zero added latency, hiding the open-time CAS round-trip.
    pub(crate) fn seed_forward_buffer(&mut self, offset: u64, data: Bytes) {
        if data.is_empty() {
            return;
        }
        self.buf_start = offset;
        self.chunks_len = data.len();
        self.chunks_front_offset = 0;
        self.chunks.clear();
        self.chunks.push_back(data);
    }

    /// If `offset` falls inside a known tensor, return that tensor's end
    /// (absolute file offset). Returns `None` for the safetensors header
    /// region (offset < 8 + header_len, before the first tensor) or when no
    /// layout is available.
    fn tensor_end_for(&self, offset: u64) -> Option<u64> {
        let (_, end) = self.tensor_for(offset)?;
        Some(end)
    }

    /// `(start, end)` of the tensor containing `offset`, or `None`.
    fn tensor_for(&self, offset: u64) -> Option<(u64, u64)> {
        let layout = self.tensor_layout.as_ref()?;
        let idx = layout.partition_point(|(start, _end)| *start <= offset);
        if idx == 0 {
            return None;
        }
        let (start, end) = layout[idx - 1];
        if offset >= start && offset < end { Some((start, end)) } else { None }
    }

    /// Atomic claim of the next-tensor speculative fetch. Returns
    /// `(next_start, next_end_capped)` if speculation should run AND records
    /// the claim by setting `speculative_inflight`. Concurrent callers will
    /// get `None` from the second invocation, so at most one task is spawned
    /// per tensor (closes the TOCTOU between "decide" and "mark inflight").
    pub(crate) fn claim_speculative_prefetch(&mut self, offset: u64, size: u32) -> Option<(u64, u64)> {
        const SPECULATION_TRIGGER_RATIO: u64 = 4; // 3/4 of the way through
        const MIN_CUR_TENSOR_FOR_SPEC: u64 = 4 * 1_048_576;
        const MIN_NEXT_TENSOR_FOR_SPEC: u64 = 1_048_576;

        let layout = self.tensor_layout.as_ref()?;
        let (cur_start, cur_end) = self.tensor_for(offset)?;
        let cur_len = cur_end - cur_start;
        if cur_len < MIN_CUR_TENSOR_FOR_SPEC {
            return None;
        }
        let trigger = cur_start + cur_len * (SPECULATION_TRIGGER_RATIO - 1) / SPECULATION_TRIGGER_RATIO;
        if offset + size as u64 <= trigger {
            return None;
        }
        let idx = layout.partition_point(|(s, _)| *s <= cur_start);
        let (next_start, next_end) = *layout.get(idx)?;
        if next_end - next_start < MIN_NEXT_TENSOR_FOR_SPEC {
            return None;
        }
        if let Some(spec) = &self.speculative
            && spec.start == next_start
        {
            return None;
        }
        if self.speculative_inflight == Some(next_start) {
            return None;
        }
        let capped_end = (next_start + MAX_TENSOR_FETCH).min(next_end);
        // Atomic claim: mark inflight here so a concurrent caller in another
        // task observes it and skips the duplicate spawn.
        self.speculative_inflight = Some(next_start);
        Some((next_start, capped_end))
    }

    /// Pure check: would `try_promote_speculative(offset)` succeed? Used to
    /// decide whether to invoke the (mutating) promote without first
    /// touching the forward-buffer probe.
    pub(crate) fn would_serve_speculative(&self, offset: u64) -> bool {
        let Some(spec) = &self.speculative else { return false };
        let spec_end = spec.start + spec.data.len() as u64;
        offset >= spec.start && offset < spec_end
    }

    /// If `offset` lies in the speculative buffer, take ownership of it and
    /// promote to the forward buffer.
    pub(crate) fn try_promote_speculative(&mut self, offset: u64) -> bool {
        if !self.would_serve_speculative(offset) {
            return false;
        }
        let spec = self.speculative.take().unwrap();
        self.seed_forward_buffer(spec.start, spec.data);
        true
    }

    /// Store the result of a background prefetch task.
    pub(crate) fn store_speculative(&mut self, start: u64, data: Bytes) {
        self.speculative = Some(SpeculativeBuf { start, data });
        if self.speculative_inflight == Some(start) {
            self.speculative_inflight = None;
        }
    }

    /// Clear the inflight marker if a prefetch failed without storing.
    pub(crate) fn clear_speculative_inflight(&mut self, tensor_start: u64) {
        if self.speculative_inflight == Some(tensor_start) {
            self.speculative_inflight = None;
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
        let range_floor = if self.mmap_hint {
            MIN_RANGE_FETCH_MMAP
        } else {
            MIN_RANGE_FETCH
        };
        // For sequential reads, prefetch ahead (window_size) to amortise latency.
        // For random/seek reads (RangeDownload), prefer a tensor-aligned fetch
        // when we have a parsed safetensors layout: pull up to the end of the
        // current tensor (capped at MAX_TENSOR_FETCH), but never below
        // `range_floor` so a read near the end of a small tensor still pays
        // off the round-trip with a meaningful fetch.
        let to_tensor_end = self.tensor_end_for(offset).map(|e| e - offset).unwrap_or(0);
        let range_fetch = to_tensor_end.max(range_floor).min(MAX_TENSOR_FETCH);
        let fetch_size = match strategy {
            FetchStrategy::RangeDownload => needed.max(range_fetch),
            _ => needed.max(self.window_size),
        }
        .min(self.file_size - offset);

        FetchPlan { strategy, fetch_size }
    }

    /// Store freshly downloaded chunks in the forward buffer.
    /// Atomically install a freshly fetched buffer AND the stream that
    /// produced it. The stream's read cursor must be at `offset + total`.
    /// Concurrent writers may interleave under the lock-released-during-fetch
    /// pattern, but each `store_fetched` is itself atomic: the stream slot
    /// always reflects the LAST writer's buffer, so a future `ContinueStream`
    /// read uses a stream whose cursor matches `buf_start + chunks_len`.
    /// Pass `stream_to_return = None` when the fetch was range-based (no
    /// persistent stream produced); any older stream is dropped because its
    /// cursor is now unrelated to the new buffer offset.
    pub(crate) fn store_fetched(
        &mut self,
        offset: u64,
        chunks: VecDeque<Bytes>,
        total: usize,
        stream_to_return: Option<Box<dyn DownloadStreamOps>>,
    ) {
        self.chunks = chunks;
        self.chunks_len = total;
        self.chunks_front_offset = 0;
        self.buf_start = offset;
        self.stream = stream_to_return;
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
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    /// Helper: create a PrefetchState pre-loaded with chunks at a given offset.
    fn ps_with_chunks(buf_start: u64, chunks: &[&[u8]]) -> PrefetchState {
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false, false);
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
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false, false);
        ps.seek_data.extend(&[10, 20, 30, 40, 50]);
        ps.seek_start = 100;
        let result = ps.try_serve_seek(102, 2).unwrap();
        assert_eq!(&result[..], &[30, 40]);
    }

    #[test]
    fn seek_window_out_of_range() {
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false, false);
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
        let mut ps = PrefetchState::new("hash".into(), 100, false, false);
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
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, false, false);
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
        let mut ps = PrefetchState::new("hash".into(), u64::MAX, true, false);
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

    // ── Safetensors header parser ───────────────────────────────────

    fn make_header(json: &str) -> (u64, Vec<u8>) {
        let bytes = json.as_bytes().to_vec();
        (bytes.len() as u64, bytes)
    }

    #[test]
    fn safetensors_layout_rejects_offset_past_eof() {
        // file_size = 100, but a tensor's data_offsets end at 200 (relative).
        // With data_origin = 8 + header_len, the absolute end exceeds file_size → reject.
        let json = r#"{"a":{"data_offsets":[0,200]}}"#;
        let (header_len, bytes) = make_header(json);
        let layout = parse_safetensors_layout(header_len, &bytes, 100);
        assert!(layout.is_none(), "should reject tensor extending past file_size");
    }

    #[test]
    fn safetensors_layout_rejects_overlapping_tensors() {
        // a: 0..50, b: 30..80 → overlap at 30..50
        let json = r#"{"a":{"data_offsets":[0,50]}, "b":{"data_offsets":[30,80]}}"#;
        let (header_len, bytes) = make_header(json);
        let big_file = 1_000_000;
        let layout = parse_safetensors_layout(header_len, &bytes, big_file);
        assert!(layout.is_none(), "should reject overlapping tensors");
    }

    #[test]
    fn safetensors_layout_rejects_overflow_arithmetic() {
        // header_len = u64::MAX would overflow data_origin = 8 + header_len.
        let json = r#"{"a":{"data_offsets":[0,1]}}"#;
        let (_, bytes) = make_header(json);
        let layout = parse_safetensors_layout(u64::MAX, &bytes, u64::MAX);
        assert!(layout.is_none(), "should reject when 8 + header_len overflows");
    }

    #[test]
    fn safetensors_layout_accepts_valid() {
        let json = r#"{"a":{"data_offsets":[0,10]}, "b":{"data_offsets":[10,20]}}"#;
        let (header_len, bytes) = make_header(json);
        let file_size = 8 + header_len + 20;
        let layout = parse_safetensors_layout(header_len, &bytes, file_size).unwrap();
        assert_eq!(layout.len(), 2);
        let origin = 8 + header_len;
        assert_eq!(layout[0], (origin, origin + 10));
        assert_eq!(layout[1], (origin + 10, origin + 20));
    }

    // ── store_fetched stream invalidation ──────────────────────────

    #[test]
    fn store_fetched_atomically_pairs_buffer_and_stream() {
        // Buffer + stream are paired: store_fetched always replaces the
        // stream slot with the one passed in. A None drops any stale stream
        // (range-style fetches that don't produce a persistent stream).
        struct EmptyStream;
        #[async_trait::async_trait]
        impl crate::xet::DownloadStreamOps for EmptyStream {
            async fn next(&mut self) -> crate::error::Result<Option<Bytes>> {
                Ok(None)
            }
        }
        let mut ps = PrefetchState::new("h".into(), 1024, false, false);
        ps.buf_start = 0;
        ps.chunks_len = 10;
        ps.chunks.push_back(Bytes::from(vec![0u8; 10]));
        ps.stream = Some(Box::new(EmptyStream));
        // Range-style store: stream_to_return = None → stream slot cleared.
        ps.store_fetched(100, VecDeque::from(vec![Bytes::from(vec![1u8; 5])]), 5, None);
        assert!(ps.stream.is_none(), "range-style store must clear stale stream");
        assert_eq!(ps.buf_start, 100);
        assert_eq!(ps.chunks_len, 5);
    }

    #[test]
    fn store_fetched_replaces_stream_on_contiguous_overwrite() {
        // Even when the new buffer is contiguous with the old, the freshly
        // provided stream replaces whatever was there. Closes the
        // concurrent-contiguous race where a slower writer would otherwise
        // leave its earlier-positioned stream installed under a buffer
        // produced by a faster writer.
        struct ProbeStream {
            calls: Arc<AtomicUsize>,
        }
        #[async_trait::async_trait]
        impl crate::xet::DownloadStreamOps for ProbeStream {
            async fn next(&mut self) -> crate::error::Result<Option<Bytes>> {
                self.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(None)
            }
        }
        let old_calls = Arc::new(AtomicUsize::new(0));
        let new_calls = Arc::new(AtomicUsize::new(0));

        let mut ps = PrefetchState::new("h".into(), 1024, false, false);
        ps.buf_start = 0;
        ps.chunks_len = 10;
        ps.chunks.push_back(Bytes::from(vec![0u8; 10]));
        ps.stream = Some(Box::new(ProbeStream { calls: old_calls.clone() }));
        ps.store_fetched(
            10,
            VecDeque::from(vec![Bytes::from(vec![1u8; 5])]),
            5,
            Some(Box::new(ProbeStream { calls: new_calls.clone() })),
        );
        // Drive the installed stream once and verify it was the *new* one.
        let mut s = ps.stream.take().expect("contiguous store must keep a stream slot");
        let _ = futures::executor::block_on(s.next());
        assert_eq!(new_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(old_calls.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    // ── claim_speculative_prefetch atomicity ───────────────────────

    #[test]
    fn claim_speculative_prefetch_marks_inflight() {
        let mut ps = PrefetchState::new("h".into(), u64::MAX, false, true);
        // Layout: tensor 0 at [100, 100+8MiB), tensor 1 at [100+8MiB, 100+10MiB)
        let t0_start = 100u64;
        let t0_end = t0_start + 8 * 1_048_576;
        let t1_start = t0_end;
        let t1_end = t1_start + 2 * 1_048_576;
        ps.tensor_layout = Some(vec![(t0_start, t0_end), (t1_start, t1_end)]);
        // Read past 75% of tensor 0 → should claim.
        let trigger = t0_start + (t0_end - t0_start) * 3 / 4 + 1;
        let claim = ps.claim_speculative_prefetch(trigger, 1024);
        assert_eq!(claim, Some((t1_start, t1_end)));
        assert_eq!(ps.speculative_inflight, Some(t1_start));
        // Second call must not double-claim.
        let claim2 = ps.claim_speculative_prefetch(trigger, 1024);
        assert!(claim2.is_none(), "second claim must yield None when inflight");
    }

    #[test]
    fn claim_speculative_skips_small_tensors() {
        let mut ps = PrefetchState::new("h".into(), u64::MAX, false, true);
        // Small current tensor (1 MiB) → speculation off.
        ps.tensor_layout = Some(vec![(0, 1_048_576), (1_048_576, 100_000_000)]);
        let claim = ps.claim_speculative_prefetch(1_000_000, 1024);
        assert!(claim.is_none(), "small current tensor must not trigger spec");
    }
}
