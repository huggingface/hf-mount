//! Concurrent reads of remote (xet-backed) files.
//!
//! A file is split into `BLOCK_SIZE` blocks. A read collects the blocks it
//! covers: fetched blocks are served at once, blocks in flight are awaited,
//! and missing blocks are fetched by background tasks that each stream one
//! bounded byte range from CAS. No lock is held across network I/O, so reads
//! of different regions of a file proceed in parallel, and a block in flight
//! is never fetched a second time.
//!
//! Read-ahead follows each sequential stream on its own: interleaved readers
//! of one handle (parallel tensor loaders over a single mmap, or torch copying
//! one tensor with several threads) each get a growing window, like the
//! kernel's readahead but for many streams per file.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tracing::{debug, error, warn};
use xet_data::processing::XetFileInfo;

use crate::xet::XetOps;

/// Unit of caching and fetching.
pub(crate) const BLOCK_SIZE: u64 = 256 * 1024;
/// Read-ahead window of a stream when it first steps forward, in blocks: how
/// far past its last read the stream keeps blocks requested. The window
/// doubles each time the stream reads half of it.
const INITIAL_WINDOW: u64 = 8;
/// Largest read-ahead window, in blocks. Requests top the window up as the
/// stream reads and as its fetches complete, in runs of at least half the
/// window (or `MAX_FETCH_BLOCKS`), so that fetching goes on while a read
/// waits for a slow fetch.
const MAX_WINDOW: u64 = 2048;
/// Largest run of blocks one fetch downloads. Windows are split in runs of
/// this size that download in parallel: one range streams at about the
/// speed of a single connection.
const MAX_FETCH_BLOCKS: u64 = 128;
/// Bytes in flight plus bytes fetched ahead that no read has touched yet,
/// per reader. Read-ahead is trimmed to stay below it; a read always
/// fetches the blocks it needs.
const MAX_AHEAD_BYTES: u64 = 512 * 1_048_576;
/// The same budget, split between the readers of a mount that hold
/// read-ahead: many files read at once must not each hold a full window.
pub(crate) const MAX_TOTAL_AHEAD_BYTES: u64 = 1024 * 1_048_576;
/// Share of `MAX_TOTAL_AHEAD_BYTES` a reader keeps however many others read.
const MIN_AHEAD_SHARE: u64 = 16 * 1_048_576;
/// Bytes of blocks already read that a reader keeps, for re-reads and for
/// streams that meet at a block boundary. The page cache keeps the rest.
const MAX_BEHIND_BYTES: u64 = 8 * 1_048_576;
/// Sequential streams tracked per reader.
const MAX_STREAMS: usize = 32;
/// A new stream that starts at most this many blocks past the furthest
/// block read continues a forward scan of the file.
const FRONTIER_GAP: u64 = 256;
/// A stream that has not read for this many reads of its reader is taken
/// as finished: its read-ahead may be evicted, and it no longer limits the
/// read-ahead of the streams behind it.
const STALE_READS: u64 = 256;
/// Attempts per fetch before its remaining blocks fail with EIO.
const MAX_ATTEMPTS: u32 = 3;

/// A block fetch outcome: `None` while in flight, then the data or an errno.
type Fill = Option<Result<Bytes, i32>>;

enum Block {
    /// Fetched. `read` stays false until a read touches the block.
    Ready { data: Bytes, read: bool, last_used: u64 },
    /// In flight.
    Pending(watch::Receiver<Fill>),
}

/// A sequential stream: consecutive reads that move forward block by block.
struct Stream {
    id: u64,
    /// Block of the read that started the stream.
    first_block: u64,
    /// Last block a read of this stream touched.
    last_block: u64,
    /// Blocks before this one have been requested by read-ahead.
    ahead_end: u64,
    /// Read-ahead window, in blocks (0 until the stream steps forward).
    window: u64,
    /// `last_block` when `window` last grew.
    grown_at: u64,
    last_used: u64,
}

#[derive(Default)]
struct State {
    blocks: HashMap<u64, Block>,
    streams: Vec<Stream>,
    fetches: Vec<AbortHandle>,
    /// Bytes of pending blocks and of fetched blocks no read has touched.
    ahead_bytes: u64,
    /// Bytes of pending blocks.
    pending_bytes: u64,
    /// Bytes of fetched blocks a read has touched.
    behind_bytes: u64,
    /// Blocks in the order reads first touched them: eviction order of
    /// `behind_bytes`. May list blocks already gone.
    behind: VecDeque<u64>,
    /// `ahead_bytes` as last reported to the mount budget.
    reported_ahead: u64,
    /// Reads planned so far: the clock of `last_used`.
    tick: u64,
    /// Last stream id handed out.
    last_stream: u64,
    /// Furthest block read so far.
    frontier: Option<u64>,
    /// Read-ahead window, in blocks, given at once to a stream that starts
    /// past `frontier`. It doubles with each such stream, and drops to 0
    /// when a read lands far from `frontier`.
    frontier_window: u64,
}

/// A contiguous run of blocks fetched by one background task.
struct FetchRun {
    first_block: u64,
    senders: Vec<watch::Sender<Fill>>,
    /// Read-ahead runs go through the on-disk chunk cache; reads of isolated
    /// blocks do not.
    read_ahead: bool,
    /// Stream whose read-ahead this run serves, topped up again when it ends.
    stream: Option<u64>,
}

/// Read-ahead memory of a mount, split evenly between the readers that hold
/// read-ahead: the first files read cannot keep a budget that files opened
/// later then never get.
pub(crate) struct ReadAheadBudget {
    /// Readers holding read-ahead (pending or unread blocks).
    readers: AtomicU64,
    limit: u64,
}

impl ReadAheadBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            readers: AtomicU64::new(0),
            limit,
        })
    }

    /// Read-ahead bytes one reader may hold. `holding` tells whether the
    /// asking reader is already counted.
    fn share(&self, holding: bool) -> u64 {
        let readers = self.readers.load(Ordering::Relaxed) + u64::from(!holding);
        (self.limit / readers.max(1)).max(MIN_AHEAD_SHARE)
    }
}

/// Reads one remote file for one open handle.
pub(crate) struct RemoteReader {
    file_info: XetFileInfo,
    file_size: u64,
    xet: Arc<dyn XetOps>,
    /// Longest wait for the next chunk of a fetch before the attempt fails
    /// (`Duration::ZERO` waits forever).
    fetch_timeout: Duration,
    /// Drop blocks once read (`--direct-io`): re-reads must fetch again.
    forward_only: bool,
    max_ahead_bytes: u64,
    max_behind_bytes: u64,
    budget: Arc<ReadAheadBudget>,
    state: Mutex<State>,
}

impl RemoteReader {
    pub(crate) fn new(
        xet_hash: String,
        file_size: u64,
        xet: Arc<dyn XetOps>,
        fetch_timeout: Duration,
        forward_only: bool,
        budget: Arc<ReadAheadBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            file_info: XetFileInfo::new(xet_hash, file_size),
            file_size,
            xet,
            fetch_timeout,
            forward_only,
            max_ahead_bytes: MAX_AHEAD_BYTES,
            max_behind_bytes: MAX_BEHIND_BYTES,
            budget,
            state: Mutex::new(State::default()),
        })
    }

    /// Read `[offset, offset + size)`, clamped to the file size. Returns
    /// `(data, eof)`. The data is never short except at EOF: the kernel
    /// shrinks the file size on short FUSE reads.
    pub(crate) async fn read(self: &Arc<Self>, offset: u64, size: u32) -> Result<(Bytes, bool), i32> {
        if offset >= self.file_size || size == 0 {
            return Ok((Bytes::new(), offset >= self.file_size));
        }
        let end = (offset + size as u64).min(self.file_size);
        let first = offset / BLOCK_SIZE;
        let last = (end - 1) / BLOCK_SIZE;

        let (mut parts, waits) = self.plan(first, last);
        let waited = !waits.is_empty();
        let started = std::time::Instant::now();
        for (index, mut receiver) in waits {
            let fill = match receiver.wait_for(Option::is_some).await {
                Ok(fill) => fill.clone().expect("wait_for returns a filled value"),
                // The fetch task ended without an outcome (runtime shutdown).
                Err(_) => Err(libc::EIO),
            };
            parts[index] = Some(fill?);
        }
        if waited {
            debug!(
                "reader: read at {} of {} waited {:?}",
                offset,
                self.file_info.hash(),
                started.elapsed()
            );
        }
        if waited || self.forward_only {
            self.after_read(first, last, end);
        }

        let data = assemble(&parts, offset - first * BLOCK_SIZE, (end - offset) as usize);
        Ok((data, end == self.file_size))
    }

    fn block_count(&self) -> u64 {
        self.file_size.div_ceil(BLOCK_SIZE)
    }

    fn block_len(&self, block: u64) -> u64 {
        BLOCK_SIZE.min(self.file_size - block * BLOCK_SIZE)
    }

    /// Collect the blocks of a read, start fetches for missing blocks and for
    /// read-ahead, and return the data at hand plus the blocks to wait for.
    #[allow(clippy::type_complexity)]
    fn plan(self: &Arc<Self>, first: u64, last: u64) -> (Vec<Option<Bytes>>, Vec<(usize, watch::Receiver<Fill>)>) {
        let mut guard = self.state.lock().expect("reader state poisoned");
        let state = &mut *guard;
        state.tick += 1;
        let tick = state.tick;

        let mut parts = Vec::with_capacity((last - first + 1) as usize);
        let mut waits = Vec::new();
        let mut runs: Vec<(u64, u64, bool)> = Vec::new();
        for block in first..=last {
            let index = (block - first) as usize;
            match state.blocks.get_mut(&block) {
                Some(Block::Ready { data, read, last_used }) => {
                    *last_used = tick;
                    if !std::mem::replace(read, true) {
                        state.ahead_bytes -= data.len() as u64;
                        state.behind_bytes += data.len() as u64;
                        state.behind.push_back(block);
                    }
                    parts.push(Some(data.clone()));
                }
                Some(Block::Pending(receiver)) => {
                    waits.push((index, receiver.clone()));
                    parts.push(None);
                }
                None => {
                    push_block(&mut runs, block, false);
                    parts.push(None);
                }
            }
        }

        let mut stream = None;
        if let Some((index, start, end)) = state.track(first, last, self.block_count(), tick) {
            self.extend_read_ahead(state, index, start, end, &mut runs);
            stream = Some(state.streams[index].id);
        }
        for (block, receiver) in self.start_fetches(state, runs, stream) {
            if block <= last {
                waits.push(((block - first) as usize, receiver));
            }
        }
        self.evict_behind(state);
        self.report_ahead(state);
        (parts, waits)
    }

    /// Add the read-ahead range `[start, end)` of stream `index` to `runs`,
    /// cut where another stream starts and to what the budgets allow.
    fn extend_read_ahead(
        &self,
        state: &mut State,
        index: usize,
        start: u64,
        end: u64,
        runs: &mut Vec<(u64, u64, bool)>,
    ) {
        let end = end.min(state.next_stream_start(index));
        let run = (state.streams[index].window / 2).clamp(1, MAX_FETCH_BLOCKS);
        let end = self.trim_read_ahead(state, start, end, run);
        // With no budget left, the stream tries again at its next step or
        // when one of its fetches completes.
        state.streams[index].ahead_end = end;
        for block in start..end {
            if !state.blocks.contains_key(&block) {
                push_block(runs, block, true);
            }
        }
    }

    /// Mark the blocks of `runs` pending and start their fetches. Returns
    /// the receivers of the new blocks, for the read that asked for them.
    fn start_fetches(
        self: &Arc<Self>,
        state: &mut State,
        runs: Vec<(u64, u64, bool)>,
        stream: Option<u64>,
    ) -> Vec<(u64, watch::Receiver<Fill>)> {
        let mut receivers = Vec::new();
        for (start, end, read_ahead) in runs {
            let mut senders = Vec::with_capacity((end - start) as usize);
            for block in start..end {
                let (sender, receiver) = watch::channel(None);
                receivers.push((block, receiver.clone()));
                state.blocks.insert(block, Block::Pending(receiver));
                state.ahead_bytes += self.block_len(block);
                state.pending_bytes += self.block_len(block);
                senders.push(sender);
            }
            debug!(
                "reader: fetch [{}, {}) of {} (read_ahead={})",
                start * BLOCK_SIZE,
                (end * BLOCK_SIZE).min(self.file_size),
                self.file_info.hash(),
                read_ahead
            );
            let run = FetchRun {
                first_block: start,
                senders,
                read_ahead,
                stream: stream.filter(|_| read_ahead),
            };
            let task = tokio::spawn(fetch_run(Arc::downgrade(self), run));
            state.fetches.push(task.abort_handle());
        }
        state.fetches.retain(|task| !task.is_finished());
        receivers
    }

    /// A read-ahead fetch of stream `id` finished: top its window up again,
    /// so that fetching goes on while its reads wait for a slower fetch.
    /// Only for streams that have read sequentially for a while: a stream
    /// made of one read (a record header) would keep fetching for nothing.
    fn refill(self: &Arc<Self>, id: u64) {
        let mut guard = self.state.lock().expect("reader state poisoned");
        let state = &mut *guard;
        let Some(index) = state.streams.iter().position(|stream| stream.id == id) else {
            return;
        };
        let stream = &state.streams[index];
        if state.tick - stream.last_used > STALE_READS || stream.last_block - stream.first_block < INITIAL_WINDOW {
            return;
        }
        if let Some((start, end)) = state.read_ahead_range(index, self.block_count()) {
            let mut runs = Vec::new();
            self.extend_read_ahead(state, index, start, end, &mut runs);
            self.start_fetches(state, runs, Some(id));
            self.report_ahead(state);
        }
    }

    /// Shrink a read-ahead range `[start, end)` so that the bytes ahead stay
    /// within the budgets, first dropping fetched blocks no read touched and
    /// no stream is heading to (read-ahead of a stream that moved on).
    /// Returns the new end: `start` when the budget leaves room for less than
    /// `run` blocks (capped at a quarter of the budget), so that a stream held
    /// back by it fetches in runs worth a round trip, not block by block.
    fn trim_read_ahead(&self, state: &mut State, start: u64, end: u64, run: u64) -> u64 {
        let wanted: u64 = (start..end)
            .filter(|block| !state.blocks.contains_key(block))
            .map(|block| self.block_len(block))
            .sum();
        let limit = self.max_ahead_bytes.min(self.budget.share(state.reported_ahead > 0));
        if state.ahead_bytes + wanted > limit {
            let heading_to = |block: u64| {
                (start..end).contains(&block)
                    || state.streams.iter().any(|other| {
                        state.tick - other.last_used <= STALE_READS
                            && block > other.last_block
                            && block < other.ahead_end
                    })
            };
            let mut stale: Vec<(u64, u64)> = state
                .blocks
                .iter()
                .filter_map(|(block, slot)| match slot {
                    Block::Ready {
                        read: false, last_used, ..
                    } if !heading_to(*block) => Some((*last_used, *block)),
                    _ => None,
                })
                .collect();
            stale.sort_unstable();
            for (_, block) in stale {
                if state.ahead_bytes + wanted <= limit {
                    break;
                }
                if let Some(Block::Ready { data, .. }) = state.blocks.remove(&block) {
                    state.ahead_bytes -= data.len() as u64;
                }
            }
        }
        // At most half of it in flight. A fetch delivers its data only when
        // its whole range has arrived, so fetches finish out of order: the
        // other half holds data fetched past a slow fetch, and new fetches
        // start while a read waits for it instead of the network idling.
        let mut budget = limit
            .saturating_sub(state.ahead_bytes)
            .min((limit / 2).saturating_sub(state.pending_bytes));
        let run = run.min(limit / BLOCK_SIZE / 4).max(1);
        for block in start..end {
            if state.blocks.contains_key(&block) {
                continue;
            }
            let len = self.block_len(block);
            if len > budget {
                return if block - start >= run { block } else { start };
            }
            budget -= len;
        }
        end
    }

    /// Drop the blocks read first beyond the budget of blocks already read,
    /// and forget queued blocks that are gone.
    fn evict_behind(&self, state: &mut State) {
        while let Some(&block) = state.behind.front() {
            let live = matches!(state.blocks.get(&block), Some(Block::Ready { read: true, .. }));
            if live && state.behind_bytes <= self.max_behind_bytes {
                break;
            }
            state.behind.pop_front();
            if live && let Some(Block::Ready { data, .. }) = state.blocks.remove(&block) {
                state.behind_bytes -= data.len() as u64;
            }
        }
    }

    /// Mark the blocks of a finished read as read (the ones it waited for
    /// were still pending when it planned), and in forward-only mode drop
    /// the blocks it read to their end.
    fn after_read(&self, first: u64, last: u64, end: u64) {
        let mut guard = self.state.lock().expect("reader state poisoned");
        let state = &mut *guard;
        let tick = state.tick;
        for block in first..=last {
            let Some(Block::Ready { data, read, last_used }) = state.blocks.get_mut(&block) else {
                continue;
            };
            let len = data.len() as u64;
            *last_used = tick;
            if !std::mem::replace(read, true) {
                state.ahead_bytes -= len;
                state.behind_bytes += len;
                state.behind.push_back(block);
            }
            if self.forward_only && block * BLOCK_SIZE + len <= end {
                state.blocks.remove(&block);
                state.behind_bytes -= len;
            }
        }
        self.evict_behind(state);
        self.report_ahead(state);
    }

    /// A fetch delivered `block`.
    fn complete(&self, block: u64, data: Bytes) {
        let mut guard = self.state.lock().expect("reader state poisoned");
        let state = &mut *guard;
        let last_used = state.tick;
        if let Some(slot) = state.blocks.get_mut(&block)
            && matches!(slot, Block::Pending(_))
        {
            state.pending_bytes -= data.len() as u64;
            *slot = Block::Ready {
                data,
                read: false,
                last_used,
            };
        }
    }

    /// A fetch gave up on `blocks`: forget them so a later read fetches again.
    fn fail(&self, blocks: std::ops::Range<u64>) {
        let mut guard = self.state.lock().expect("reader state poisoned");
        let state = &mut *guard;
        for block in blocks {
            if let Some(Block::Pending(_)) = state.blocks.get(&block) {
                state.blocks.remove(&block);
                state.ahead_bytes -= self.block_len(block);
                state.pending_bytes -= self.block_len(block);
            }
        }
        self.report_ahead(state);
    }

    /// Count this reader among the readers holding read-ahead while it
    /// holds some.
    fn report_ahead(&self, state: &mut State) {
        let (was, now) = (state.reported_ahead > 0, state.ahead_bytes > 0);
        if now && !was {
            self.budget.readers.fetch_add(1, Ordering::Relaxed);
        } else if was && !now {
            self.budget.readers.fetch_sub(1, Ordering::Relaxed);
        }
        state.reported_ahead = state.ahead_bytes;
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        // Cancel read-ahead nobody will read.
        if let Ok(state) = self.state.get_mut() {
            for task in state.fetches.drain(..) {
                task.abort();
            }
            if state.reported_ahead > 0 {
                self.budget.readers.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
}

impl State {
    /// First block of the nearest stream that started ahead of `stream` and
    /// has moved since: read-ahead stops there, as that stream reads (or
    /// has read) what follows. Parallel copies of one tensor split it in
    /// adjacent ranges; without this, each range's read-ahead would fetch
    /// the start of the next range again.
    fn next_stream_start(&self, stream: usize) -> u64 {
        let position = self.streams[stream].last_block;
        self.streams
            .iter()
            .filter(|other| {
                other.first_block > position
                    && other.last_block > other.first_block
                    && self.tick - other.last_used <= STALE_READS
            })
            .map(|other| other.first_block)
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Record a read of blocks `[first, last]`. When its stream should read
    /// ahead, return the stream index and the block range to fetch; the
    /// caller records on the stream the part it fetches.
    fn track(&mut self, first: u64, last: u64, block_count: u64, tick: u64) -> Option<(usize, u64, u64)> {
        // A read continues a stream when it starts in the stream's last block,
        // the next one, or the one before (FUSE may deliver concurrent
        // requests slightly out of order).
        let continued = self
            .streams
            .iter()
            .enumerate()
            .filter(|(_, stream)| first + 1 >= stream.last_block && first <= stream.last_block + 1)
            .max_by_key(|(_, stream)| stream.last_used)
            .map(|(index, _)| index);

        let frontier = self.frontier;
        self.frontier = Some(frontier.map_or(last, |frontier| frontier.max(last)));
        let (index, stepped) = match continued {
            Some(index) => {
                let stream = &mut self.streams[index];
                let stepped = last > stream.last_block;
                stream.last_block = stream.last_block.max(last);
                (index, stepped)
            }
            None => {
                if self.streams.len() >= MAX_STREAMS
                    && let Some(index) = self
                        .streams
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, stream)| stream.last_used)
                        .map(|(index, _)| index)
                {
                    self.streams.swap_remove(index);
                }
                // A read at the start of the file, or just past everything read
                // so far, begins or continues a forward scan: loaders copy tensor
                // after tensor in file order, each with several threads, and
                // torch reads the record headers of a zip checkpoint in order
                // before their data. Such a stream reads ahead at once, with a
                // window that grows from one stream to the next, so that what
                // comes next downloads while the current reads are served.
                // Elsewhere, wait for a second read.
                let forward = first == 0
                    || frontier.is_some_and(|frontier| first >= frontier && first - frontier <= FRONTIER_GAP);
                if !forward && frontier.is_some_and(|frontier| first.abs_diff(frontier) > FRONTIER_GAP) {
                    self.frontier_window = 0;
                }
                let window = match (forward, self.frontier_window) {
                    (false, _) => 0,
                    (true, 0) => INITIAL_WINDOW,
                    (true, previous) => (previous * 2).min(MAX_WINDOW),
                };
                if forward {
                    self.frontier_window = window;
                }
                self.last_stream += 1;
                self.streams.push(Stream {
                    id: self.last_stream,
                    first_block: first,
                    last_block: last,
                    ahead_end: last + 1,
                    window,
                    grown_at: last,
                    last_used: tick,
                });
                (self.streams.len() - 1, window > 0)
            }
        };
        let stream = &mut self.streams[index];
        stream.last_used = tick;
        if !stepped {
            return None;
        }

        if stream.window == 0 {
            stream.window = INITIAL_WINDOW;
            stream.grown_at = last;
        } else if last.saturating_sub(stream.grown_at) >= stream.window / 2 {
            stream.window = (stream.window * 2).min(MAX_WINDOW);
            stream.grown_at = last;
        }
        let (start, end) = self.read_ahead_range(index, block_count)?;
        Some((index, start, end))
    }

    /// The blocks stream `index` should request now to fill its window.
    fn read_ahead_range(&mut self, index: usize, block_count: u64) -> Option<(u64, u64)> {
        let next = self.streams[index].last_block + 1;
        let stream = &mut self.streams[index];
        // Read the window again from `next` when its first block went missing
        // (evicted or failed) instead of fetching the rest block by block.
        if next >= stream.ahead_end || !self.blocks.contains_key(&next) {
            stream.ahead_end = next;
        }
        let end = (next + stream.window).min(block_count);
        let wanted = end.saturating_sub(stream.ahead_end);
        // Top up in runs worth a round trip, except at the end of the file.
        if wanted == 0 || (wanted < (stream.window / 2).min(MAX_FETCH_BLOCKS) && end < block_count) {
            return None;
        }
        Some((stream.ahead_end, end))
    }
}

/// Add `block` to the fetch runs, extending the last run when contiguous
/// and not full.
fn push_block(runs: &mut Vec<(u64, u64, bool)>, block: u64, read_ahead: bool) {
    if let Some(run) = runs.last_mut()
        && run.1 == block
        && run.1 - run.0 < MAX_FETCH_BLOCKS
    {
        run.1 += 1;
        run.2 |= read_ahead;
        return;
    }
    runs.push((block, block + 1, read_ahead));
}

/// Concatenate block data and cut `[skip, skip + len)` out of it. A read
/// inside one block is a zero-copy slice.
fn assemble(parts: &[Option<Bytes>], skip: u64, len: usize) -> Bytes {
    let skip = skip as usize;
    if let [Some(block)] = parts {
        return block.slice(skip..skip + len);
    }
    let mut out = BytesMut::with_capacity(len);
    let mut skip = skip;
    for block in parts.iter().map(|part| part.as_ref().expect("all blocks collected")) {
        if skip >= block.len() {
            skip -= block.len();
            continue;
        }
        let take = (block.len() - skip).min(len - out.len());
        out.extend_from_slice(&block[skip..skip + take]);
        skip = 0;
        if out.len() == len {
            break;
        }
    }
    out.freeze()
}

/// Stream the byte range of `run` and deliver it block by block. A failed
/// attempt resumes at the first block not yet delivered.
async fn fetch_run(reader: Weak<RemoteReader>, run: FetchRun) {
    let Some((xet, file_info, file_size, timeout)) = reader.upgrade().map(|reader| {
        (
            reader.xet.clone(),
            reader.file_info.clone(),
            reader.file_size,
            reader.fetch_timeout,
        )
    }) else {
        return;
    };
    let block_count = run.senders.len() as u64;
    let end = ((run.first_block + block_count) * BLOCK_SIZE).min(file_size);
    let block_len = |block: u64| BLOCK_SIZE.min(file_size - block * BLOCK_SIZE) as usize;
    let mut next = run.first_block;
    let started = std::time::Instant::now();

    for attempt in 1..=MAX_ATTEMPTS {
        let start = next * BLOCK_SIZE;
        match xet.download_stream_boxed(&file_info, start, end, run.read_ahead) {
            Err(err) => warn!(
                "reader: stream open failed at {} of {}, attempt {}/{}: {}",
                start,
                file_info.hash(),
                attempt,
                MAX_ATTEMPTS,
                err
            ),
            Ok(mut stream) => {
                let mut block = BytesMut::with_capacity(block_len(next));
                loop {
                    let chunk = match timeout {
                        Duration::ZERO => stream.next().await,
                        timeout => match tokio::time::timeout(timeout, stream.next()).await {
                            Ok(result) => result,
                            Err(_elapsed) => {
                                error!(
                                    "reader: no data for {:?} at {} of {}, attempt {}/{}",
                                    timeout,
                                    next * BLOCK_SIZE + block.len() as u64,
                                    file_info.hash(),
                                    attempt,
                                    MAX_ATTEMPTS
                                );
                                break;
                            }
                        },
                    };
                    let mut chunk = match chunk {
                        Ok(Some(chunk)) => chunk,
                        Ok(None) => {
                            error!(
                                "reader: stream of {} ended at {}, expected {}, attempt {}/{}",
                                file_info.hash(),
                                next * BLOCK_SIZE + block.len() as u64,
                                end,
                                attempt,
                                MAX_ATTEMPTS
                            );
                            break;
                        }
                        Err(err) => {
                            warn!(
                                "reader: stream error at {} of {}, attempt {}/{}: {}",
                                next * BLOCK_SIZE + block.len() as u64,
                                file_info.hash(),
                                attempt,
                                MAX_ATTEMPTS,
                                err
                            );
                            break;
                        }
                    };
                    // Copy into block-sized buffers: a slice of a chunk would
                    // keep the whole chunk (up to a 64 MiB term) alive.
                    while !chunk.is_empty() {
                        let take = (block_len(next) - block.len()).min(chunk.len());
                        block.extend_from_slice(&chunk.split_to(take));
                        if block.len() < block_len(next) {
                            continue;
                        }
                        let data = std::mem::take(&mut block).freeze();
                        let Some(owner) = reader.upgrade() else { return };
                        owner.complete(next, data.clone());
                        drop(owner);
                        let _ = run.senders[(next - run.first_block) as usize].send(Some(Ok(data)));
                        next += 1;
                        if next == run.first_block + block_count {
                            debug!(
                                "reader: fetched [{}, {}) of {} in {:?}",
                                run.first_block * BLOCK_SIZE,
                                end,
                                file_info.hash(),
                                started.elapsed()
                            );
                            if let Some(stream) = run.stream
                                && let Some(owner) = reader.upgrade()
                            {
                                owner.refill(stream);
                            }
                            return;
                        }
                        block = BytesMut::with_capacity(block_len(next));
                    }
                }
            }
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
        }
    }

    error!(
        "reader: giving up on [{}, {}) of {} after {} attempts",
        next * BLOCK_SIZE,
        end,
        file_info.hash(),
        MAX_ATTEMPTS
    );
    if let Some(reader) = reader.upgrade() {
        reader.fail(next..run.first_block + block_count);
    }
    for sender in &run.senders[(next - run.first_block) as usize..] {
        let _ = sender.send(Some(Err(libc::EIO)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_mocks::MockXet;

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn reader_for(xet: &Arc<MockXet>, content: &[u8], forward_only: bool) -> Arc<RemoteReader> {
        xet.add_file("hash", content);
        RemoteReader::new(
            "hash".into(),
            content.len() as u64,
            xet.clone(),
            Duration::from_secs(5),
            forward_only,
            ReadAheadBudget::new(MAX_TOTAL_AHEAD_BYTES),
        )
    }

    fn stream_calls(xet: &MockXet) -> Vec<(u64, u64, bool)> {
        xet.stream_calls.lock().unwrap().clone()
    }

    #[tokio::test]
    async fn read_within_and_across_blocks() {
        let xet = MockXet::new();
        let content = pattern(3 * BLOCK_SIZE as usize + 1000);
        let reader = reader_for(&xet, &content, false);

        for (offset, size) in [(0u64, 4096u32), (BLOCK_SIZE - 10, 20), (5, 3 * BLOCK_SIZE as u32)] {
            let (data, _) = reader.read(offset, size).await.unwrap();
            let end = (offset as usize + size as usize).min(content.len());
            assert!(data[..] == content[offset as usize..end], "read at {offset}+{size}");
        }
    }

    #[tokio::test]
    async fn read_reports_eof_and_clamps() {
        let xet = MockXet::new();
        let content = pattern(BLOCK_SIZE as usize + 10);
        let reader = reader_for(&xet, &content, false);

        let (data, eof) = reader.read(BLOCK_SIZE, 100).await.unwrap();
        assert_eq!(&data[..], &content[BLOCK_SIZE as usize..]);
        assert!(eof);
        let (data, eof) = reader.read(content.len() as u64 + 5, 100).await.unwrap();
        assert!(data.is_empty() && eof);
        let (_, eof) = reader.read(0, 100).await.unwrap();
        assert!(!eof);
    }

    /// Reads of different regions wait for their own fetch, not for each other.
    #[tokio::test]
    async fn reads_of_different_regions_run_in_parallel() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.set_stream_delay(Duration::from_millis(300));

        let started = std::time::Instant::now();
        let reads: Vec<_> = (0..8u64)
            .map(|i| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.read((i * 8 + 3) * BLOCK_SIZE, 4096).await })
            })
            .collect();
        for read in reads {
            read.await.unwrap().unwrap();
        }
        let elapsed = started.elapsed();
        assert!(elapsed < Duration::from_millis(900), "8 reads took {elapsed:?}");
    }

    /// Concurrent reads of one block share a single fetch.
    #[tokio::test]
    async fn concurrent_reads_share_a_block_in_flight() {
        let xet = MockXet::new();
        let content = pattern(16 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.set_stream_delay(Duration::from_millis(100));

        let offset = 5 * BLOCK_SIZE;
        let reads: Vec<_> = (0..4u64)
            .map(|i| {
                let reader = reader.clone();
                tokio::spawn(async move { reader.read(offset + i * 4096, 4096).await })
            })
            .collect();
        for (i, read) in reads.into_iter().enumerate() {
            let (data, _) = read.await.unwrap().unwrap();
            let start = offset as usize + i * 4096;
            assert!(data[..] == content[start..start + 4096]);
        }
        assert_eq!(stream_calls(&xet), vec![(offset, offset + BLOCK_SIZE, false)]);
    }

    /// Interleaved sequential streams each get read-ahead and none of them
    /// makes the others fetch again.
    #[tokio::test]
    async fn interleaved_sequential_streams_fetch_each_byte_once() {
        let xet = MockXet::new();
        let streams = 8usize;
        let stream_len = 24 * BLOCK_SIZE as usize;
        let content = pattern(streams * stream_len);
        let reader = reader_for(&xet, &content, false);

        let read_size = 128 * 1024usize;
        for step in 0..stream_len / read_size {
            for stream in 0..streams {
                let offset = stream * stream_len + step * read_size;
                let (data, _) = reader.read(offset as u64, read_size as u32).await.unwrap();
                assert!(data[..] == content[offset..offset + read_size]);
            }
        }

        let calls = stream_calls(&xet);
        let fetched: u64 = calls.iter().map(|(start, end, _)| end - start).sum();
        assert_eq!(fetched, content.len() as u64, "calls: {calls:?}");
        assert!(
            calls.len() < streams * 8,
            "{} fetches for {} streams",
            calls.len(),
            streams
        );
    }

    /// A loader copying tensors in file order, each with several threads,
    /// finds the next tensors already fetched: a stream that starts just
    /// past the furthest block read reads ahead at once.
    #[tokio::test]
    async fn forward_scan_fetches_the_next_tensors_ahead() {
        let xet = MockXet::new();
        let tensor = 32 * BLOCK_SIZE as usize;
        let tensors = 16;
        let threads = 8;
        let content = pattern(tensor * tensors);
        let reader = reader_for(&xet, &content, false);

        let chunk = tensor / threads;
        let read_size = 128 * 1024;
        for index in 0..tensors {
            for step in 0..chunk / read_size {
                for thread in 0..threads {
                    let offset = index * tensor + thread * chunk + step * read_size;
                    let (data, _) = reader.read(offset as u64, read_size as u32).await.unwrap();
                    assert!(data[..] == content[offset..offset + read_size]);
                }
            }
        }

        // Without read-ahead across streams, the first read of every chunk
        // would fetch its block alone.
        let lone = stream_calls(&xet).iter().filter(|call| !call.2).count();
        assert!(lone < tensors, "{lone} lone fetches for {} chunks", tensors * threads);
    }

    /// Small reads at increasing offsets (torch reading the record headers of
    /// a zip checkpoint before their data) soon find the next records
    /// fetched: each continues the forward scan and reads further ahead.
    #[tokio::test]
    async fn header_scan_reads_ahead() {
        let xet = MockXet::new();
        let records = 16u64;
        let spacing = 20 * BLOCK_SIZE;
        let content = pattern((records * spacing) as usize);
        let reader = reader_for(&xet, &content, false);

        for record in 0..records {
            let offset = record * spacing + 100;
            let (data, _) = reader.read(offset, 4096).await.unwrap();
            assert!(data[..] == content[offset as usize..offset as usize + 4096]);
        }
        let lone = stream_calls(&xet).iter().filter(|call| !call.2).count() as u64;
        assert!(lone <= 2, "{lone} of {records} headers fetched alone");
    }

    /// A sequential scan requests growing read-ahead fetches through the chunk
    /// cache, and a far read fetches only its block, without it.
    #[tokio::test]
    async fn sequential_scan_grows_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(1024 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        let read_size = 128 * 1024u64;
        let mut offset = 0;
        while offset < 200 * BLOCK_SIZE {
            let (data, _) = reader.read(offset, read_size as u32).await.unwrap();
            assert!(data[..] == content[offset as usize..(offset + read_size) as usize]);
            offset += read_size;
        }
        let calls = stream_calls(&xet);
        let sizes: Vec<u64> = calls.iter().map(|(start, end, _)| (end - start) / BLOCK_SIZE).collect();
        assert_eq!(sizes[0], 1 + INITIAL_WINDOW, "sizes: {sizes:?}");
        assert!(sizes[..4].windows(2).all(|pair| pair[1] > pair[0]), "sizes: {sizes:?}");
        assert!(sizes.contains(&MAX_FETCH_BLOCKS), "sizes: {sizes:?}");
        assert!(
            calls.iter().all(|call| call.2),
            "read-ahead goes through the cache: {calls:?}"
        );

        let far = 900 * BLOCK_SIZE + 17;
        let (data, _) = reader.read(far, 4096).await.unwrap();
        assert!(data[..] == content[far as usize..far as usize + 4096]);
        let last = *stream_calls(&xet).last().unwrap();
        assert_eq!(last, (900 * BLOCK_SIZE, 901 * BLOCK_SIZE, false));
    }

    /// A sequential scan reads ahead up to the end of the file: the last
    /// window, shorter than a run, is still fetched ahead.
    #[tokio::test]
    async fn sequential_scan_reads_ahead_to_the_end() {
        for blocks in [200, 300, 450, 600] {
            let xet = MockXet::new();
            let content = pattern(blocks * BLOCK_SIZE as usize + 1000);
            let reader = reader_for(&xet, &content, false);

            let read_size = 128 * 1024u64;
            let mut offset = 0;
            while offset < content.len() as u64 {
                let (data, eof) = reader.read(offset, read_size as u32).await.unwrap();
                let end = (offset + read_size).min(content.len() as u64);
                assert!(data[..] == content[offset as usize..end as usize]);
                assert_eq!(eof, end == content.len() as u64);
                offset = end;
            }
            let lone: Vec<_> = stream_calls(&xet).into_iter().filter(|call| !call.2).collect();
            assert!(lone.is_empty(), "{blocks} blocks: fetched alone {lone:?}");
        }
    }

    /// Read-ahead stays within the reader's budget, and blocks fetched ahead
    /// for a stream that stopped make room for the stream still reading.
    #[tokio::test]
    async fn read_ahead_stays_within_budget() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        xet.add_file("hash", &content);
        let max_ahead = 24 * BLOCK_SIZE;
        let max_behind = 4 * BLOCK_SIZE;
        let reader = Arc::new(RemoteReader {
            file_info: XetFileInfo::new("hash".into(), content.len() as u64),
            file_size: content.len() as u64,
            xet: xet.clone(),
            fetch_timeout: Duration::from_secs(5),
            forward_only: false,
            max_ahead_bytes: max_ahead,
            max_behind_bytes: max_behind,
            budget: ReadAheadBudget::new(MAX_TOTAL_AHEAD_BYTES),
            state: Mutex::new(State::default()),
        });

        // A first stream reads four blocks, gets read-ahead, then stops.
        for block in 0..4 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        // A second stream scans the second half of the file.
        let start = 256 * BLOCK_SIZE;
        let read_size = 128 * 1024u64;
        for offset in (start..content.len() as u64).step_by(read_size as usize) {
            let (data, _) = reader.read(offset, read_size as u32).await.unwrap();
            assert!(data[..] == content[offset as usize..(offset + read_size) as usize]);
            let state = reader.state.lock().unwrap();
            assert!(state.behind_bytes <= max_behind, "behind {}", state.behind_bytes);
            assert!(
                state.ahead_bytes <= max_ahead + BLOCK_SIZE,
                "ahead {}",
                state.ahead_bytes
            );
        }
        // Once the first stream is stale, the second one fetches in runs
        // again (a quarter of the budget, shorter at the end of the file),
        // not block by block.
        let late = start + (STALE_READS / 2 + 32) * BLOCK_SIZE;
        let late_blocks = (content.len() as u64 - late) / BLOCK_SIZE;
        let fetches = stream_calls(&xet).iter().filter(|call| call.0 >= late).count() as u64;
        assert!(
            fetches * (max_ahead / BLOCK_SIZE / 8) <= late_blocks,
            "{fetches} fetches for the last {late_blocks} blocks"
        );
    }

    #[test]
    fn budget_is_shared_evenly_between_readers() {
        let budget = ReadAheadBudget::new(8 * MIN_AHEAD_SHARE);
        assert_eq!(budget.share(false), 8 * MIN_AHEAD_SHARE);
        budget.readers.store(3, Ordering::Relaxed);
        assert_eq!(budget.share(true), 8 * MIN_AHEAD_SHARE / 3);
        assert_eq!(budget.share(false), 2 * MIN_AHEAD_SHARE);
        budget.readers.store(100, Ordering::Relaxed);
        assert_eq!(budget.share(true), MIN_AHEAD_SHARE);
    }

    /// A reader counts toward the split while it holds read-ahead, and
    /// stops counting when its handle closes.
    #[tokio::test]
    async fn readers_holding_read_ahead_are_counted() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        let budget = reader.budget.clone();

        reader.read(0, 4096).await.unwrap();
        assert_eq!(budget.readers.load(Ordering::Relaxed), 1);
        drop(reader);
        assert_eq!(budget.readers.load(Ordering::Relaxed), 0);
    }

    /// When a stream stops reading (its read waits for a slow fetch), the
    /// fetches that complete keep filling its window up to the budget: only
    /// half of it can be in flight at the last read.
    #[tokio::test]
    async fn completed_fetches_keep_filling_the_window() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        xet.add_file("hash", &content);
        let max_ahead = 128 * BLOCK_SIZE;
        let reader = Arc::new(RemoteReader {
            file_info: XetFileInfo::new("hash".into(), content.len() as u64),
            file_size: content.len() as u64,
            xet: xet.clone(),
            fetch_timeout: Duration::from_secs(5),
            forward_only: false,
            max_ahead_bytes: max_ahead,
            max_behind_bytes: MAX_BEHIND_BYTES,
            budget: ReadAheadBudget::new(MAX_TOTAL_AHEAD_BYTES),
            state: Mutex::new(State::default()),
        });

        for block in 0..200 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let state = reader.state.lock().unwrap();
        assert_eq!(state.pending_bytes, 0);
        assert!(
            state.ahead_bytes > max_ahead / 2 + max_ahead / 4,
            "{} blocks fetched ahead, budget {}",
            state.ahead_bytes / BLOCK_SIZE,
            max_ahead / BLOCK_SIZE
        );
    }

    #[tokio::test]
    async fn forward_only_fetches_again_on_re_read() {
        let xet = MockXet::new();
        let content = pattern(4 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, true);

        reader.read(2 * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        let (data, _) = reader.read(2 * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        assert!(data[..] == content[2 * BLOCK_SIZE as usize..3 * BLOCK_SIZE as usize]);
        assert_eq!(stream_calls(&xet).len(), 2);
    }

    #[tokio::test]
    async fn failed_attempt_resumes_at_the_first_missing_block() {
        let xet = MockXet::new();
        let content = pattern(8 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.empty_range_downloads(1);

        let (data, _) = reader.read(BLOCK_SIZE, 4096).await.unwrap();
        assert!(data[..] == content[BLOCK_SIZE as usize..BLOCK_SIZE as usize + 4096]);
        assert_eq!(stream_calls(&xet).len(), 2);
    }

    #[tokio::test]
    async fn exhausted_attempts_fail_with_eio_then_recover() {
        let xet = MockXet::new();
        let content = pattern(8 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.fail_range_downloads(MAX_ATTEMPTS);

        assert_eq!(reader.read(BLOCK_SIZE, 4096).await.unwrap_err(), libc::EIO);
        // The failed block is forgotten: the next read fetches it again.
        let (data, _) = reader.read(BLOCK_SIZE, 4096).await.unwrap();
        assert!(data[..] == content[BLOCK_SIZE as usize..BLOCK_SIZE as usize + 4096]);
    }

    #[tokio::test]
    async fn dropping_the_reader_cancels_its_fetches() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.stall_stream_reads();

        let pending = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(0, 4096).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        pending.abort();
        let fetches: Vec<AbortHandle> = reader.state.lock().unwrap().fetches.clone();
        assert!(!fetches.is_empty());
        drop(reader);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(fetches.iter().all(AbortHandle::is_finished));
    }
}
