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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use futures::stream::{FuturesUnordered, TryStreamExt};
use tokio::sync::watch;
use tokio::task::AbortHandle;
use tracing::{debug, error, warn};
use xet_data::processing::XetFileInfo;

use crate::xet::{DownloadStreamOps, XetOps};

/// Unit of caching and fetching.
pub(crate) const BLOCK_SIZE: u64 = 256 * 1024;
/// Read-ahead window of a stream when it first steps forward, in blocks: how
/// far past its last read the stream keeps blocks requested. Each time the
/// stream reads half of it, the window grows fourfold (twofold past
/// `MAX_WINDOW / 8`).
const INITIAL_WINDOW: u64 = 8;
/// Read-ahead window of a stream that starts at the beginning of the file,
/// in blocks: such reads most often copy the whole file, and a file of a
/// few dozen MiB would otherwise be read before the window has grown.
const START_WINDOW: u64 = 256;
/// Largest read-ahead window, in blocks. Requests top the window up as the
/// stream reads and as its fetches complete, in runs of at least half the
/// window (or `MAX_FETCH_BLOCKS`), so that fetching goes on while a read
/// waits for a slow fetch.
const MAX_WINDOW: u64 = 2048;
/// Largest run of blocks one fetch downloads. Windows are split in runs of
/// this size that download in parallel: one range streams at about the
/// speed of a single connection.
const MAX_FETCH_BLOCKS: u64 = 128;
/// Smallest run of blocks one fetch downloads. A fetch delivers nothing
/// until its whole range has arrived, so a run is at most as long as its
/// distance to the reader (and at least this long): the first blocks a
/// reader needs arrive after a short fetch, not after a 32 MiB one.
const MIN_FETCH_BLOCKS: u64 = 4;
/// Bytes in flight plus bytes fetched ahead that no read has touched yet,
/// per reader. Read-ahead is trimmed to stay below it; a read always
/// fetches the blocks it needs.
const MAX_AHEAD_BYTES: u64 = 512 * 1_048_576;
/// Bytes of blocks already read that a reader keeps, for re-reads and for
/// streams that meet at a block boundary. The page cache keeps the rest.
const MAX_BEHIND_BYTES: u64 = 8 * 1_048_576;
/// The same for all the readers open on a mount, which split it: many files
/// open at once (memory-mapped dataset shards) must not each keep
/// `MAX_BEHIND_BYTES`.
const MAX_TOTAL_BEHIND_BYTES: u64 = 256 * 1_048_576;
/// Sequential streams tracked per reader.
const MAX_STREAMS: usize = 32;
/// A new stream that starts at most this many blocks past the furthest
/// block read continues a forward scan of the file, and its read-ahead
/// fetches the gap too. Further reads skip data the reader may never read
/// (the slices of a tensor-parallel rank, sparse sampling): they wait for a
/// second read before reading ahead.
const FRONTIER_GAP: u64 = 64;
/// A stream that has not read for this many reads of its reader is taken
/// as finished: its read-ahead may be evicted, and it no longer limits the
/// read-ahead of the streams behind it.
const STALE_READS: u64 = 256;
/// A reader that has not read for this long gives back the blocks it
/// fetched ahead beyond its share of the mount budget, so that the readers
/// still at work get theirs.
const IDLE_DROP: Duration = Duration::from_secs(10);
/// A reader that has not read for this long gives back all its read-ahead
/// and the blocks it read, and its next read starts its streams with small
/// windows again: a file kept open after its reads must not keep a share of
/// the mount budget, and a read after a long pause must not fetch a whole
/// window again. Shorter pauses (a training step, a batch) keep what the
/// next reads need.
const IDLE_RELEASE: Duration = Duration::from_secs(120);
/// Attempts in a row that deliver nothing before the remaining blocks of a
/// fetch fail with EIO. An attempt that delivers blocks resets the count.
const MAX_ATTEMPTS: u32 = 3;
/// Age of a read-ahead fetch in flight past which no read-ahead starts,
/// when the fetch timeout is disabled (otherwise a quarter of it): a quarter
/// of the default fetch timeout.
const SLOW_FETCH: Duration = Duration::from_millis(7_500);
/// Read-ahead bytes in flight a reader starts with. Each read-ahead fetch
/// that arrives before it is slow adds its bytes to the cap, up to half the
/// reader limit, and each one that arrives later or fails halves it, down to
/// `MIN_IN_FLIGHT`: on a slow link, the bytes in flight stay about what the
/// link downloads before a fetch is slow, well within the fetch timeout.
const START_IN_FLIGHT: u64 = 64 * 1_048_576;
const MIN_IN_FLIGHT: u64 = 8 * 1_048_576;

/// A block fetch outcome: `None` while in flight, then the data or an errno.
type Fill = Option<Result<Bytes, i32>>;

/// A block of the file, in memory or in flight. `waiters` counts the reads
/// waiting for it: a block with waiters arrives as read, and joins the
/// eviction queue once the last of them ends. `fetch` tells apart the
/// fetches of one block: a read waited for one of them only.
enum Block {
    /// Fetched. `read` stays false until a read touches the block.
    /// Forward-only mode: reads have consumed the block up to `read_to`, and
    /// a read that starts before it fetches the block again.
    Ready {
        data: Bytes,
        read: bool,
        last_used: u64,
        waiters: u32,
        fetch: u64,
        read_to: u64,
    },
    /// In flight.
    Pending {
        fill: watch::Receiver<Fill>,
        waiters: u32,
        fetch: u64,
    },
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

impl Stream {
    /// Whether the stream read within the last `STALE_READS` reads.
    fn is_live(&self, tick: u64) -> bool {
        tick - self.last_used <= STALE_READS
    }

    /// Fewest blocks worth a fetch when topping the window up.
    fn min_run(&self) -> u64 {
        (self.window / 2).clamp(1, MAX_FETCH_BLOCKS)
    }

    /// Whether `block` is in the read-ahead window of the stream.
    fn heads_to(&self, block: u64) -> bool {
        block > self.last_block && block < self.ahead_end
    }
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
    /// Bytes of pending blocks that reads wait for: in `ahead_bytes` until
    /// they arrive, but not read-ahead, so they do not count the reader
    /// among those that split the mount budget.
    pending_read_bytes: u64,
    /// Bytes of fetched blocks a read has touched.
    behind_bytes: u64,
    /// Blocks in the order reads first touched them: eviction order of
    /// `behind_bytes`.
    behind: VecDeque<u64>,
    /// Bytes of this reader counted in the mount budget: `ahead_bytes` as
    /// last reported, plus read-ahead reserved for fetches about to start.
    reported_ahead: u64,
    /// The mount budget cut the read-ahead this reader asked for, and the
    /// reader has not gone idle since.
    starved: bool,
    /// Counted among the readers that split the mount budget, as last
    /// reported: it holds read-ahead, or it is starved.
    counted: bool,
    /// Reads planned so far: the clock of `last_used`.
    tick: u64,
    /// When a read last touched a block for the first time or stopped
    /// waiting for a fetch: re-reads of blocks at hand do not count.
    progress_at: Option<tokio::time::Instant>,
    /// Reads waiting for fetches.
    waiting: u32,
    /// A task waits to drop the read-ahead once the reader stops reading.
    idle_watch: bool,
    /// Last stream id handed out.
    last_stream: u64,
    /// Last fetch id handed out.
    last_fetch: u64,
    /// Read-ahead fetches in flight, by id: when each started, its blocks
    /// not delivered yet, and its bytes.
    in_flight: HashMap<u64, (tokio::time::Instant, u64, u64)>,
    /// Most bytes in flight for read-ahead (see `START_IN_FLIGHT`).
    in_flight_cap: u64,
    /// Furthest block read so far.
    frontier: Option<u64>,
    /// Read-ahead window, in blocks, given at once to a stream that starts
    /// past `frontier`. It doubles with each such stream, and drops to 0
    /// when a read lands far from `frontier`.
    frontier_window: u64,
}

/// A contiguous run of blocks fetched by one background task. Dropped with
/// blocks left to deliver (the fetch gave up, or its task ended early), it
/// fails them: no block stays in flight with no fetch behind it.
struct FetchRun {
    reader: Weak<RemoteReader>,
    /// Next block to deliver.
    next: u64,
    /// Senders of the blocks not delivered yet: a delivered block's channel
    /// keeps its data alive until no one holds the channel.
    senders: VecDeque<watch::Sender<Fill>>,
    /// Stream whose read-ahead this run serves, topped up again when it ends.
    /// Read-ahead runs go through the on-disk chunk cache; runs of blocks
    /// only reads need do not.
    stream: Option<u64>,
    /// Reads planned when the run started (`State::tick`).
    since: u64,
}

impl Drop for FetchRun {
    fn drop(&mut self) {
        if self.senders.is_empty() {
            return;
        }
        if let Some(reader) = self.reader.upgrade() {
            reader.fail(self.next..self.next + self.senders.len() as u64);
        }
        for sender in self.senders.drain(..) {
            let _ = sender.send(Some(Err(libc::EIO)));
        }
    }
}

/// Read-ahead memory of a mount. Read-ahead is reserved before its fetches
/// start, so the readers together stay within the limit, and each one may
/// hold an even share of it: the first files read cannot keep a budget that
/// files opened later then never get.
pub(crate) struct ReadAheadBudget {
    /// Readers that hold read-ahead or were refused some.
    readers: AtomicU64,
    /// Bytes the readers hold or have reserved.
    held: AtomicU64,
    limit: u64,
    /// Readers open on the mount: they split `MAX_TOTAL_BEHIND_BYTES`.
    open: AtomicU64,
}

impl ReadAheadBudget {
    pub(crate) fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            readers: AtomicU64::new(0),
            held: AtomicU64::new(0),
            limit,
            open: AtomicU64::new(0),
        })
    }

    /// Read-ahead bytes one reader may hold. `counted` tells whether the
    /// asking reader is among `readers` already.
    fn share(&self, counted: bool) -> u64 {
        let readers = self.readers.load(Ordering::Relaxed) + u64::from(!counted);
        self.limit / readers.max(1)
    }

    /// Reserve up to `bytes` of read-ahead within the limit. Returns the
    /// bytes reserved.
    fn reserve(&self, bytes: u64) -> u64 {
        let mut reserved = 0;
        let _ = self.held.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |held| {
            reserved = bytes.min(self.limit.saturating_sub(held));
            Some(held + reserved)
        });
        reserved
    }

    /// Give back reserved bytes that no fetch will use.
    fn release(&self, bytes: u64) {
        self.held.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Bring the budget up to date with the read-ahead of a reader: its
    /// bytes, and whether it counts among the readers that split the budget.
    fn report(&self, state: &mut State) {
        let (now, before) = (state.ahead_bytes, state.reported_ahead);
        if now > before {
            self.held.fetch_add(now - before, Ordering::Relaxed);
        } else if now < before {
            self.held.fetch_sub(before - now, Ordering::Relaxed);
        }
        state.reported_ahead = now;
        let counted = now > state.pending_read_bytes || state.starved;
        if counted != state.counted {
            if counted {
                self.readers.fetch_add(1, Ordering::Relaxed);
            } else {
                self.readers.fetch_sub(1, Ordering::Relaxed);
            }
            state.counted = counted;
        }
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

/// The state of a reader, locked. Unlocking reports the read-ahead to the
/// mount budget, so that no path can change it and forget to.
struct Locked<'a> {
    reader: &'a RemoteReader,
    state: MutexGuard<'a, State>,
}

impl Deref for Locked<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        &self.state
    }
}

impl DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.state
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        self.reader.budget.report(&mut self.state);
    }
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
        budget.open.fetch_add(1, Ordering::Relaxed);
        Arc::new(Self {
            file_info: XetFileInfo::new(xet_hash, file_size),
            file_size,
            xet,
            fetch_timeout,
            forward_only,
            max_ahead_bytes: MAX_AHEAD_BYTES,
            max_behind_bytes: MAX_BEHIND_BYTES,
            budget,
            state: Mutex::new(State {
                in_flight_cap: START_IN_FLIGHT,
                ..State::default()
            }),
        })
    }

    /// Lock the state. A panic while it was locked (a bug in this module)
    /// leaves the bookkeeping as it was rather than failing every later read.
    fn lock(&self) -> Locked<'_> {
        Locked {
            reader: self,
            state: self.state.lock().unwrap_or_else(PoisonError::into_inner),
        }
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

        let skip = offset - first * BLOCK_SIZE;
        let parts = match self.collect(first, last, skip).await {
            Ok(parts) => parts,
            // A fetch this read only joined gave up, maybe before the network
            // came back: try once more, with fetches of its own.
            Err((_, true)) => self.collect(first, last, skip).await.map_err(|(errno, _)| errno)?,
            Err((errno, false)) => return Err(errno),
        };
        if self.forward_only {
            self.drop_read(first, last, end);
        }

        let data = assemble(&parts, skip, (end - offset) as usize);
        Ok((data, end == self.file_size))
    }

    /// The blocks `[first, last]` of a read that starts `skip` bytes into
    /// block `first`, once all of them arrived. On failure, the errno, and
    /// whether the fetch that failed was one the read joined rather than
    /// started.
    async fn collect(self: &Arc<Self>, first: u64, last: u64, skip: u64) -> Result<Vec<Option<Bytes>>, (i32, bool)> {
        let (mut parts, waiting) = self.plan(first, last, skip);
        if let Some(mut waiting) = waiting {
            let started = std::time::Instant::now();
            // All at once, in the order they arrive: a block whose fetch gave
            // up ends the read without waiting for the slower ones.
            let mut fills: FuturesUnordered<_> = waiting
                .waits
                .iter_mut()
                .map(|wait| async move {
                    let joined = wait.joined;
                    match wait.fill.wait_for(Option::is_some).await {
                        Ok(fill) => fill
                            .clone()
                            .expect("wait_for returns a filled value")
                            .map(|data| (wait.index, data))
                            .map_err(|errno| (errno, joined)),
                        // The fetch task ended without an outcome (runtime shutdown).
                        Err(_) => Err((libc::EIO, joined)),
                    }
                })
                .collect();
            while let Some((index, data)) = fills.try_next().await? {
                parts[index] = Some(data);
            }
            debug!(
                "reader: read at {} of {} waited {:?}",
                first * BLOCK_SIZE + skip,
                self.file_info.hash(),
                started.elapsed()
            );
        }
        Ok(parts)
    }

    fn block_count(&self) -> u64 {
        self.file_size.div_ceil(BLOCK_SIZE)
    }

    fn block_len(&self, block: u64) -> u64 {
        block_len(self.file_size, block)
    }

    /// Collect the blocks of a read, which starts `skip` bytes into block
    /// `first`, start fetches for missing blocks and for read-ahead, and
    /// return the data at hand plus the wait for the rest.
    fn plan(self: &Arc<Self>, first: u64, last: u64, skip: u64) -> (Vec<Option<Bytes>>, Option<Waiting<'_>>) {
        // Declared before the lock, so that a panic releases the lock before
        // the fetches drop: dropping one locks the state to fail its blocks.
        let fetches;
        let mut guard = self.lock();
        let state = &mut *guard;
        if state.progress_at.is_some_and(|at| at.elapsed() >= IDLE_RELEASE) {
            for stream in &mut state.streams {
                stream.window = stream.window.min(INITIAL_WINDOW);
                stream.grown_at = stream.last_block;
            }
            state.frontier_window = 0;
        }
        state.tick += 1;
        let tick = state.tick;
        // Now and then, give back the read-ahead of streams that stopped (a
        // header read at the start of the file), and stop counting in the
        // split until the budget cuts a fetch again: a reader that keeps
        // reading elsewhere never goes idle.
        if tick.is_multiple_of(STALE_READS) {
            self.evict_stale(state);
            state.starved = false;
        }

        let mut parts = Vec::with_capacity((last - first + 1) as usize);
        let mut waits = Vec::new();
        let mut runs: Vec<(u64, u64, bool)> = Vec::new();
        let mut progress = false;
        for block in first..=last {
            let index = (block - first) as usize;
            let start = if block == first { skip } else { 0 };
            if self.forward_only
                && matches!(state.blocks.get(&block), Some(Block::Ready { read_to, .. }) if start < *read_to)
            {
                state.remove_read(block);
            }
            match state.blocks.get_mut(&block) {
                Some(Block::Ready {
                    data, read, last_used, ..
                }) => {
                    *last_used = tick;
                    let first_read = !std::mem::replace(read, true);
                    let data = data.clone();
                    if first_read {
                        state.mark_read(block, data.len() as u64);
                        progress = true;
                    }
                    parts.push(Some(data));
                }
                Some(Block::Pending { fill, waiters, fetch }) => {
                    let first_waiter = *waiters == 0;
                    *waiters += 1;
                    waits.push(Wait {
                        index,
                        fetch: *fetch,
                        joined: true,
                        fill: fill.clone(),
                    });
                    if first_waiter {
                        state.pending_read_bytes += self.block_len(block);
                    }
                    parts.push(None);
                    progress = true;
                }
                None => {
                    push_block(&mut runs, block, false, first);
                    parts.push(None);
                    progress = true;
                }
            }
        }
        if progress {
            state.progress_at = Some(tokio::time::Instant::now());
        }

        let mut stream = None;
        if let Some((index, start, end)) = state.track(first, last, self.block_count()) {
            self.extend_read_ahead(state, index, start, end, &mut runs);
            stream = Some(state.streams[index].id);
        }
        let receivers;
        (receivers, fetches) = self.prepare_fetches(state, runs, stream, Some(last));
        for (block, fetch, fill) in receivers {
            waits.push(Wait {
                index: (block - first) as usize,
                fetch,
                joined: false,
                fill,
            });
        }
        // The share shrinks as other readers start: give fetched read-ahead
        // back down to it.
        let limit = self.ahead_limit(state);
        self.evict_unread(state, limit, true);
        self.evict_behind(state);
        self.watch_idle(state);
        // Built last: dropped by a panic above, it would lock the state this
        // call still holds.
        let waiting = if waits.is_empty() {
            None
        } else {
            state.waiting += 1;
            Some(Waiting {
                reader: self,
                first,
                waits,
            })
        };
        drop(guard);
        self.spawn_fetches(fetches);
        (parts, waiting)
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
        let run = state.streams[index].min_run();
        // The whole range counts as the stream's while room is made for it.
        state.streams[index].ahead_end = end;
        let missing: Vec<u64> = (start..end).filter(|block| !state.blocks.contains_key(block)).collect();
        let end = self.trim_read_ahead(state, start, end, &missing, run);
        // With no budget left, the stream tries again at its next step or
        // when one of its fetches completes.
        state.streams[index].ahead_end = end;
        let next = state.streams[index].last_block + 1;
        for &block in missing.iter().take_while(|&&block| block < end) {
            push_block(runs, block, true, next);
        }
    }

    /// Mark the blocks of `runs` pending and build their fetches, for
    /// `spawn_fetches` once the state is unlocked. The blocks up to
    /// `demanded_until` are the ones the calling read needs: returns them,
    /// with their fetch id and receiver, along with the fetches.
    #[allow(clippy::type_complexity)]
    fn prepare_fetches(
        self: &Arc<Self>,
        state: &mut State,
        runs: Vec<(u64, u64, bool)>,
        stream: Option<u64>,
        demanded_until: Option<u64>,
    ) -> (Vec<(u64, u64, watch::Receiver<Fill>)>, Vec<FetchRun>) {
        let mut receivers = Vec::new();
        let mut fetches = Vec::with_capacity(runs.len());
        for (start, end, read_ahead) in runs {
            state.last_fetch += 1;
            let fetch = state.last_fetch;
            if read_ahead {
                let bytes = (end * BLOCK_SIZE).min(self.file_size) - start * BLOCK_SIZE;
                state
                    .in_flight
                    .insert(fetch, (tokio::time::Instant::now(), end - start, bytes));
            }
            let mut senders = VecDeque::with_capacity((end - start) as usize);
            for block in start..end {
                let (sender, fill) = watch::channel(None);
                let demanded = demanded_until.is_some_and(|last| block <= last);
                if demanded {
                    receivers.push((block, fetch, fill.clone()));
                }
                state.blocks.insert(
                    block,
                    Block::Pending {
                        fill,
                        waiters: u32::from(demanded),
                        fetch,
                    },
                );
                state.ahead_bytes += self.block_len(block);
                state.pending_bytes += self.block_len(block);
                if demanded {
                    state.pending_read_bytes += self.block_len(block);
                }
                senders.push_back(sender);
            }
            debug!(
                "reader: fetch [{}, {}) of {} (read_ahead={})",
                start * BLOCK_SIZE,
                (end * BLOCK_SIZE).min(self.file_size),
                self.file_info.hash(),
                read_ahead
            );
            fetches.push(FetchRun {
                reader: Arc::downgrade(self),
                next: start,
                senders,
                stream: stream.filter(|_| read_ahead),
                since: state.tick,
            });
        }
        (receivers, fetches)
    }

    /// Start the fetch tasks prepared under the lock. Not with the state
    /// locked: a runtime that shuts down drops a spawned future at once, and
    /// `FetchRun::drop` locks the state to fail its blocks.
    fn spawn_fetches(&self, fetches: Vec<FetchRun>) {
        if fetches.is_empty() {
            return;
        }
        let tasks: Vec<AbortHandle> = fetches
            .into_iter()
            .map(|run| tokio::spawn(fetch_run(run)).abort_handle())
            .collect();
        let mut guard = self.lock();
        guard.fetches.retain(|task| !task.is_finished());
        guard.fetches.extend(tasks);
    }

    /// A read-ahead fetch of stream `id`, started when `since` reads were
    /// planned, finished: top its window up again, so that fetching goes on
    /// while its reads wait for a slower fetch. Only for streams that have
    /// read sequentially for a while: a stream made of one read (a record
    /// header) would keep fetching for nothing. Only while the reader reads:
    /// a read waits, or the stream read since the fetch started. A stream
    /// that stopped would fill its whole window for nothing.
    fn refill(self: &Arc<Self>, id: u64, since: u64) {
        // Before the lock, as in `plan`.
        let fetches;
        let mut guard = self.lock();
        let state = &mut *guard;
        let Some(index) = state.streams.iter().position(|stream| stream.id == id) else {
            return;
        };
        let stream = &state.streams[index];
        if !stream.is_live(state.tick)
            || stream.last_block - stream.first_block < INITIAL_WINDOW
            || (stream.last_used <= since && state.waiting == 0)
        {
            return;
        }
        if let Some((start, end)) = state.read_ahead_range(index, self.block_count()) {
            let mut runs = Vec::new();
            self.extend_read_ahead(state, index, start, end, &mut runs);
            (_, fetches) = self.prepare_fetches(state, runs, Some(id), None);
            self.watch_idle(state);
            drop(guard);
            self.spawn_fetches(fetches);
        }
    }

    /// A block of fetch `fetch` arrived (`ok`) or failed. A read-ahead fetch
    /// leaves `in_flight` with its last block, and adjusts the cap of bytes
    /// in flight by how long it took.
    fn settle(&self, state: &mut State, fetch: u64, ok: bool) {
        let Some((started, left, bytes)) = state.in_flight.get_mut(&fetch) else {
            return;
        };
        *left -= 1;
        if *left > 0 {
            return;
        }
        let (in_time, bytes) = (ok && started.elapsed() <= self.max_fetch_age(), *bytes);
        state.in_flight.remove(&fetch);
        state.in_flight_cap = if in_time {
            (state.in_flight_cap + bytes).min(self.max_ahead_bytes / 2)
        } else {
            (state.in_flight_cap / 2).max(MIN_IN_FLIGHT)
        };
    }

    /// Age of a read-ahead fetch in flight past which no read-ahead starts.
    fn max_fetch_age(&self) -> Duration {
        match self.fetch_timeout {
            Duration::ZERO => SLOW_FETCH,
            timeout => timeout / 4,
        }
    }

    /// Read-ahead bytes this reader may hold.
    fn ahead_limit(&self, state: &State) -> u64 {
        self.max_ahead_bytes.min(self.budget.share(state.counted))
    }

    /// Shrink a read-ahead range `[start, end)`, of which `missing` lists the
    /// blocks not at hand, so that the bytes ahead stay within the budgets,
    /// first dropping fetched blocks no read touched and no stream is heading
    /// to (read-ahead of a stream that moved on), and reserve the bytes of
    /// what is left in the mount budget. Returns the new end: `start` when the
    /// budget leaves room for less than `run` blocks (capped at a quarter of
    /// the budget), so that a stream held back by it fetches in runs worth a
    /// round trip, not block by block.
    fn trim_read_ahead(&self, state: &mut State, start: u64, end: u64, missing: &[u64], run: u64) -> u64 {
        let limit = self.ahead_limit(state);
        let wanted: u64 = missing.iter().map(|&block| self.block_len(block)).sum();
        self.evict_unread(state, limit.saturating_sub(state.pending_bytes + wanted), false);
        // At most half of it in flight. A fetch delivers its data only when
        // its whole range has arrived, so fetches finish out of order: the
        // other half holds data fetched past a slow fetch, and new fetches
        // start while a read waits for it instead of the network idling.
        let allowed = limit
            .saturating_sub(state.ahead_bytes)
            .min((limit / 2).saturating_sub(state.pending_bytes));
        // A fetch slow to arrive means the link does not keep up: more in
        // flight would only queue behind it and make every fetch wait past
        // the fetch timeout. Wait for fetches to arrive instead, so that the
        // bytes in flight stay about what the link downloads meanwhile.
        let slow = self.max_fetch_age();
        let allowed = if state.in_flight.values().any(|(at, _, _)| at.elapsed() > slow) {
            0
        } else {
            allowed.min(state.in_flight_cap.saturating_sub(state.pending_bytes))
        };
        self.budget.report(state);
        let reserved = self.budget.reserve(allowed);
        // Short of the mount budget, any run worth a fetch is better than
        // reading block by block until others give theirs back.
        let run = if reserved < allowed {
            run.min(MIN_FETCH_BLOCKS)
        } else {
            run
        };
        let run = run
            .min(limit / BLOCK_SIZE / 4)
            .min(state.in_flight_cap / BLOCK_SIZE / 2)
            .max(1);
        let (mut new_end, mut taken, mut cut) = (end, 0, false);
        let (mut cell, mut taken_to_cell) = (start, 0);
        for &block in missing {
            let len = self.block_len(block);
            if block.is_multiple_of(MAX_FETCH_BLOCKS) {
                (cell, taken_to_cell) = (block, taken);
            }
            if taken + len > reserved {
                // Back to the last cell boundary when that leaves a run
                // worth a fetch: see `push_block`.
                (new_end, taken) = if cell - start >= run {
                    (cell, taken_to_cell)
                } else if block - start >= run {
                    (block, taken)
                } else {
                    (start, 0)
                };
                cut = true;
                break;
            }
            taken += len;
        }
        if new_end == start {
            taken = 0;
        }
        // Cut by the mount budget, the reader counts in its split, so that
        // readers above their share give read-ahead back.
        state.starved = cut && reserved < allowed;
        // The fetches about to start add `taken` to `ahead_bytes`.
        self.budget.release(reserved - taken);
        state.reported_ahead += taken;
        new_end
    }

    /// Drop fetched blocks no read touched until the reader holds at most
    /// `keep` bytes of them: first the blocks no live stream heads to, oldest
    /// first, then, with `heading_too`, the blocks furthest ahead of their
    /// stream. A stream fetches the blocks dropped from its window again when
    /// it gets there.
    fn evict_unread(&self, state: &mut State, keep: u64, heading_too: bool) {
        if state.fetched_ahead() <= keep {
            return;
        }
        let live: Vec<&Stream> = state.live_streams().collect();
        let mut unread: Vec<(bool, u64, u64)> = state
            .blocks
            .iter()
            .filter_map(|(&block, slot)| {
                let Block::Ready {
                    read: false, last_used, ..
                } = slot
                else {
                    return None;
                };
                let distance = live
                    .iter()
                    .filter(|stream| stream.heads_to(block))
                    .map(|stream| block - stream.last_block)
                    .min();
                match distance {
                    None => Some((false, *last_used, block)),
                    Some(distance) if heading_too => Some((true, u64::MAX - distance, block)),
                    Some(_) => None,
                }
            })
            .collect();
        unread.sort_unstable();
        for (_, _, block) in unread {
            if state.fetched_ahead() <= keep {
                break;
            }
            state.drop_unread(block);
        }
    }

    /// Drop the fetched blocks no read touched, no live stream heads to, and
    /// fetched more than `STALE_READS` reads ago.
    fn evict_stale(&self, state: &mut State) {
        let live: Vec<&Stream> = state.live_streams().collect();
        let stale: Vec<u64> = state
            .blocks
            .iter()
            .filter_map(|(&block, slot)| match slot {
                Block::Ready {
                    read: false, last_used, ..
                } if state.tick - last_used > STALE_READS && !live.iter().any(|stream| stream.heads_to(block)) => {
                    Some(block)
                }
                _ => None,
            })
            .collect();
        for block in stale {
            state.drop_unread(block);
        }
    }

    /// Bytes of blocks already read this reader may keep.
    fn behind_limit(&self) -> u64 {
        let open = self.budget.open.load(Ordering::Relaxed).max(1);
        self.max_behind_bytes
            .min((MAX_TOTAL_BEHIND_BYTES / open).max(BLOCK_SIZE))
    }

    /// Drop the blocks read first beyond the budget of blocks already read,
    /// except the last block of each live stream: its next read most often
    /// starts in it, and another stream reading meanwhile must not make it
    /// fetch that block again.
    fn evict_behind(&self, state: &mut State) {
        let limit = self.behind_limit();
        let mut kept = 0;
        while state.behind_bytes > limit
            && kept < state.behind.len()
            && let Some(block) = state.behind.pop_front()
        {
            if state.live_streams().any(|stream| stream.last_block == block) {
                state.behind.push_back(block);
                kept += 1;
            } else if let Some(Block::Ready { data, .. }) = state.blocks.remove(&block) {
                state.behind_bytes -= data.len() as u64;
            }
        }
    }

    /// Forward-only mode: drop the blocks a finished read covered to their
    /// end, and mark how far it read the last one, so that a re-read fetches
    /// them again.
    fn drop_read(&self, first: u64, last: u64, end: u64) {
        let mut guard = self.lock();
        for block in first..=last {
            let start = block * BLOCK_SIZE;
            if start + self.block_len(block) <= end {
                guard.remove_read(block);
            } else if let Some(Block::Ready { read_to, .. }) = guard.blocks.get_mut(&block) {
                *read_to = (*read_to).max(end - start);
            }
        }
    }

    /// A fetch delivered `block`. A block reads wait for arrives as read.
    fn complete(&self, block: u64, data: Bytes) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let last_used = state.tick;
        let Some(slot) = state.blocks.get_mut(&block) else {
            return;
        };
        let Block::Pending { waiters, fetch, .. } = *slot else {
            return;
        };
        let len = data.len() as u64;
        *slot = Block::Ready {
            data,
            read: waiters > 0,
            last_used,
            waiters,
            fetch,
            read_to: 0,
        };
        state.pending_bytes -= len;
        self.settle(state, fetch, true);
        // Reads wait for it: no longer read-ahead, and not in the eviction
        // queue before the last of them ends (`Waiting` queues it then).
        if waiters > 0 {
            state.ahead_bytes -= len;
            state.pending_read_bytes -= len;
        }
    }

    /// A fetch gave up on `blocks`: forget them so a later read fetches again.
    fn fail(&self, blocks: std::ops::Range<u64>) {
        let mut guard = self.lock();
        for block in blocks {
            if let Some(&Block::Pending { waiters, fetch, .. }) = guard.blocks.get(&block) {
                guard.blocks.remove(&block);
                self.settle(&mut guard, fetch, false);
                guard.ahead_bytes -= self.block_len(block);
                guard.pending_bytes -= self.block_len(block);
                if waiters > 0 {
                    guard.pending_read_bytes -= self.block_len(block);
                }
            }
        }
    }

    /// Start the task that drops the read-ahead once the reader stops
    /// reading, unless it runs already.
    fn watch_idle(self: &Arc<Self>, state: &mut State) {
        if (state.ahead_bytes > 0 || state.behind_bytes > 0 || state.starved) && !state.idle_watch {
            state.idle_watch = true;
            let task = tokio::spawn(drop_read_ahead_when_idle(Arc::downgrade(self)));
            state.fetches.push(task.abort_handle());
        }
    }
}

impl Drop for RemoteReader {
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        // Cancel read-ahead nobody will read, and give its budget back.
        for task in state.fetches.drain(..) {
            task.abort();
        }
        state.ahead_bytes = 0;
        state.starved = false;
        self.budget.report(state);
        self.budget.open.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A read waiting for fetches: its reader is not idle until the read ends,
/// cancelled or not, and each block it waits for joins the eviction queue
/// once no read waits for it, so that concurrent reads still find it.
struct Waiting<'a> {
    reader: &'a RemoteReader,
    /// First block of the read.
    first: u64,
    waits: Vec<Wait>,
}

/// A block a read waits for.
struct Wait {
    /// Index of the block from the first block of the read.
    index: usize,
    /// The fetch of the block.
    fetch: u64,
    /// The read found the block in flight rather than starting its fetch.
    joined: bool,
    fill: watch::Receiver<Fill>,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut guard = self.reader.lock();
        let state = &mut *guard;
        state.waiting -= 1;
        state.progress_at = Some(tokio::time::Instant::now());
        for wait in &self.waits {
            let block = self.first + wait.index as u64;
            state.leave(block, wait.fetch, self.reader.block_len(block));
        }
        self.reader.evict_behind(state);
    }
}

impl State {
    /// No read waits for a fetch, and none reached a new block for
    /// `IDLE_DROP`: the streams of the reader have most likely stopped.
    fn idle(&self) -> bool {
        self.waiting == 0 && self.progress_at.is_none_or(|at| at.elapsed() >= IDLE_DROP)
    }

    /// Bytes of fetched blocks no read has touched.
    fn fetched_ahead(&self) -> u64 {
        self.ahead_bytes - self.pending_bytes
    }

    /// Streams that read within the last `STALE_READS` reads.
    fn live_streams(&self) -> impl Iterator<Item = &Stream> {
        self.streams.iter().filter(|stream| stream.is_live(self.tick))
    }

    /// Drop the blocks already read.
    fn drop_behind(&mut self) {
        while let Some(block) = self.behind.pop_front() {
            if let Some(Block::Ready { data, .. }) = self.blocks.remove(&block) {
                self.behind_bytes -= data.len() as u64;
            }
        }
    }

    /// Put a fetched `block` of `len` bytes in the eviction queue.
    fn queue_behind(&mut self, block: u64, len: u64) {
        self.behind_bytes += len;
        self.behind.push_back(block);
    }

    /// A read touched `block` (`len` bytes, fetched) for the first time.
    fn mark_read(&mut self, block: u64, len: u64) {
        self.ahead_bytes -= len;
        self.queue_behind(block, len);
    }

    /// A read that waited for `block` (`len` bytes) from fetch `fetch` ended.
    /// The block joins the eviction queue once no read waits for it, or, in
    /// flight still, becomes read-ahead. A block fetched again since (its
    /// fetch gave up, or a forward-only read dropped it) counts the reads
    /// waiting for the new fetch only.
    fn leave(&mut self, block: u64, fetch: u64, len: u64) {
        match self.blocks.get_mut(&block) {
            Some(Block::Pending { waiters, fetch: id, .. }) if *id == fetch => {
                *waiters -= 1;
                if *waiters == 0 {
                    self.pending_read_bytes -= len;
                }
            }
            Some(Block::Ready {
                waiters,
                data,
                fetch: id,
                ..
            }) if *id == fetch => {
                *waiters -= 1;
                let last = *waiters == 0;
                let len = data.len() as u64;
                if last {
                    self.queue_behind(block, len);
                }
            }
            _ => {}
        }
    }

    /// Drop `block`, fetched and untouched, and cut it out of the windows of
    /// the streams heading to it.
    fn drop_unread(&mut self, block: u64) {
        let Some(Block::Ready { data, .. }) = self.blocks.remove(&block) else {
            return;
        };
        self.ahead_bytes -= data.len() as u64;
        for stream in &mut self.streams {
            if stream.heads_to(block) {
                stream.ahead_end = block;
            }
        }
    }

    /// Drop `block` if a read touched it, with its place in `behind`.
    fn remove_read(&mut self, block: u64) {
        if !matches!(self.blocks.get(&block), Some(Block::Ready { read: true, .. })) {
            return;
        }
        let Some(Block::Ready { data, .. }) = self.blocks.remove(&block) else {
            return;
        };
        // Not queued yet while a read waits for it.
        if let Some(position) = self.behind.iter().rposition(|&queued| queued == block) {
            self.behind.remove(position);
            self.behind_bytes -= data.len() as u64;
        }
    }

    /// First block of the nearest stream that started ahead of `stream`, or
    /// in its last block, and has moved since: read-ahead stops there, as
    /// that stream reads (or has read) what follows. Parallel copies of one
    /// tensor split it in adjacent ranges, which most often meet inside a
    /// block; without this, each range's read-ahead would fetch the next
    /// range again.
    fn next_stream_start(&self, stream: usize) -> u64 {
        let (id, position) = (self.streams[stream].id, self.streams[stream].last_block);
        self.live_streams()
            .filter(|other| other.id != id && other.first_block >= position && other.last_block > other.first_block)
            .map(|other| other.first_block.max(position + 1))
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Record a read of blocks `[first, last]`. When its stream should read
    /// ahead, return the stream index and the block range to fetch; the
    /// caller records on the stream the part it fetches.
    fn track(&mut self, first: u64, last: u64, block_count: u64) -> Option<(usize, u64, u64)> {
        let tick = self.tick;
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
                // The first read of the whole file gets a larger window, but
                // does not pass it on to the scan.
                let window = if first == 0 { window.max(START_WINDOW) } else { window };
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
            let factor = if stream.window < MAX_WINDOW / 8 { 4 } else { 2 };
            stream.window = (stream.window * factor).min(MAX_WINDOW);
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
        if wanted == 0 || (wanted < stream.min_run() && end < block_count) {
            return None;
        }
        Some((stream.ahead_end, end))
    }
}

/// Length of `block` in a file of `file_size` bytes: the last one is short.
fn block_len(file_size: u64, block: u64) -> u64 {
    BLOCK_SIZE.min(file_size - block * BLOCK_SIZE)
}

/// Add `block` to the fetch runs, extending the last run when contiguous,
/// shorter than its distance to `next`, the block the reader needs next
/// (clamped to `[MIN_FETCH_BLOCKS, MAX_FETCH_BLOCKS]`), and in the same cell
/// of `MAX_FETCH_BLOCKS` blocks. Runs that stay within cells come back the
/// same on every pass over a file, and so do their reconstruction queries:
/// the chunk cache serves a range only from one item that holds all of it,
/// and the plan cache only for the same range.
fn push_block(runs: &mut Vec<(u64, u64, bool)>, block: u64, read_ahead: bool, next: u64) {
    if let Some(run) = runs.last_mut()
        && run.1 == block
        && !block.is_multiple_of(MAX_FETCH_BLOCKS)
        && run.1 - run.0 < run.0.saturating_sub(next).clamp(MIN_FETCH_BLOCKS, MAX_FETCH_BLOCKS)
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

/// Once `reader` is idle, give back its read-ahead beyond its share of the
/// mount budget, then all of it and the blocks it read after
/// `IDLE_RELEASE`; end when it holds none.
async fn drop_read_ahead_when_idle(reader: Weak<RemoteReader>) {
    loop {
        let wake_at = {
            let Some(reader) = reader.upgrade() else { return };
            let mut guard = reader.lock();
            let state = &mut *guard;
            let now = tokio::time::Instant::now();
            if !state.idle() {
                match state.progress_at {
                    Some(at) if state.waiting == 0 => at + IDLE_DROP,
                    _ => now + IDLE_DROP,
                }
            } else {
                state.starved = false;
                let release_at = state.progress_at.map_or(now, |at| at + IDLE_RELEASE);
                let release = now >= release_at;
                if release {
                    state.drop_behind();
                }
                let keep = if release { 0 } else { reader.ahead_limit(state) };
                reader.evict_unread(state, keep, true);
                if state.ahead_bytes == 0 && state.behind_bytes == 0 {
                    state.idle_watch = false;
                    return;
                }
                if release || state.fetched_ahead() < state.ahead_bytes {
                    // Blocks still in flight: check them soon after they arrive.
                    now + IDLE_DROP / 10
                } else {
                    // Others may start reading and shrink the share.
                    (now + IDLE_DROP).min(release_at)
                }
            }
        };
        tokio::time::sleep_until(wake_at).await;
    }
}

/// The next chunk of `stream`, or `None` when none came within `timeout`
/// (`Duration::ZERO` waits forever).
async fn next_chunk(
    stream: &mut dyn DownloadStreamOps,
    timeout: Duration,
) -> Option<crate::error::Result<Option<Bytes>>> {
    match timeout {
        Duration::ZERO => Some(stream.next().await),
        timeout => tokio::time::timeout(timeout, stream.next()).await.ok(),
    }
}

/// Stream the byte range of `run` and deliver it block by block. A failed
/// attempt resumes at the first block not yet delivered; after
/// `MAX_ATTEMPTS` in a row that deliver nothing, dropping `run` fails the
/// blocks left and wakes their readers.
async fn fetch_run(mut run: FetchRun) {
    let Some((xet, file_info, file_size, timeout)) = run.reader.upgrade().map(|reader| {
        (
            reader.xet.clone(),
            reader.file_info.clone(),
            reader.file_size,
            reader.fetch_timeout,
        )
    }) else {
        return;
    };
    let first_block = run.next;
    let end = ((first_block + run.senders.len() as u64) * BLOCK_SIZE).min(file_size);
    let started = std::time::Instant::now();

    let mut attempt = 1;
    loop {
        let start = run.next * BLOCK_SIZE;
        let resumed_at = run.next;
        match xet.download_stream_boxed(&file_info, start, end, run.stream.is_some()) {
            Err(err) => warn!(
                "reader: stream open failed at {} of {}, attempt {}/{}: {}",
                start,
                file_info.hash(),
                attempt,
                MAX_ATTEMPTS,
                err
            ),
            Ok(mut stream) => {
                let mut block = BytesMut::with_capacity(block_len(file_size, run.next) as usize);
                loop {
                    let at = run.next * BLOCK_SIZE + block.len() as u64;
                    let mut chunk = match next_chunk(stream.as_mut(), timeout).await {
                        Some(Ok(Some(chunk))) => chunk,
                        None => {
                            error!(
                                "reader: no data for {:?} at {} of {}, attempt {}/{}",
                                timeout,
                                at,
                                file_info.hash(),
                                attempt,
                                MAX_ATTEMPTS
                            );
                            break;
                        }
                        Some(Ok(None)) => {
                            error!(
                                "reader: stream of {} ended at {}, expected {}, attempt {}/{}",
                                file_info.hash(),
                                at,
                                end,
                                attempt,
                                MAX_ATTEMPTS
                            );
                            break;
                        }
                        Some(Err(err)) => {
                            warn!(
                                "reader: stream error at {} of {}, attempt {}/{}: {}",
                                at,
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
                        let len = block_len(file_size, run.next) as usize;
                        let take = (len - block.len()).min(chunk.len());
                        block.extend_from_slice(&chunk.split_to(take));
                        if block.len() < len {
                            continue;
                        }
                        let data = std::mem::take(&mut block).freeze();
                        let Some(owner) = run.reader.upgrade() else { return };
                        owner.complete(run.next, data.clone());
                        drop(owner);
                        if let Some(sender) = run.senders.pop_front() {
                            let _ = sender.send(Some(Ok(data)));
                        }
                        run.next += 1;
                        if run.senders.is_empty() {
                            debug!(
                                "reader: fetched [{}, {}) of {} in {:?}",
                                first_block * BLOCK_SIZE,
                                end,
                                file_info.hash(),
                                started.elapsed()
                            );
                            if let Some(id) = run.stream
                                && let Some(owner) = run.reader.upgrade()
                            {
                                owner.refill(id, run.since);
                            }
                            return;
                        }
                        block = BytesMut::with_capacity(block_len(file_size, run.next) as usize);
                    }
                }
            }
        }
        if run.next > resumed_at {
            attempt = 1;
        } else if attempt == MAX_ATTEMPTS {
            break;
        } else {
            attempt += 1;
        }
        tokio::time::sleep(Duration::from_millis(100 * u64::from(attempt.max(2) - 1))).await;
    }

    error!(
        "reader: giving up on [{}, {}) of {} after {} attempts in a row",
        run.next * BLOCK_SIZE,
        end,
        file_info.hash(),
        MAX_ATTEMPTS
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_mocks::MockXet;
    use futures::FutureExt;

    /// The default read-ahead budget of a mount (`--read-ahead-mb`).
    const MAX_TOTAL_AHEAD_BYTES: u64 = 1024 * 1_048_576;

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

    /// A reader of `content` with some fields changed before its first use.
    fn reader_with(xet: &Arc<MockXet>, content: &[u8], configure: impl FnOnce(&mut RemoteReader)) -> Arc<RemoteReader> {
        let mut reader = reader_for(xet, content, false);
        configure(Arc::get_mut(&mut reader).expect("reader not shared yet"));
        reader
    }

    fn reader_with_limits(xet: &Arc<MockXet>, content: &[u8], max_ahead: u64, max_behind: u64) -> Arc<RemoteReader> {
        reader_with(xet, content, |reader| {
            reader.max_ahead_bytes = max_ahead;
            reader.max_behind_bytes = max_behind;
        })
    }

    fn stream_calls(xet: &MockXet) -> Vec<(u64, u64, bool)> {
        xet.stream_calls.lock().unwrap().clone()
    }

    fn ahead(reader: &RemoteReader) -> u64 {
        reader.lock().ahead_bytes
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

    /// The same streams split inside blocks, as the threads copying a tensor
    /// that follows a header of any length: the read-ahead of a stream stops
    /// where the next stream started, in its last block too, instead of
    /// fetching the next range again.
    #[tokio::test]
    async fn interleaved_streams_split_inside_blocks_fetch_each_byte_about_once() {
        let xet = MockXet::new();
        let (streams, header) = (8usize, 100 * 1024usize);
        let stream_len = 24 * BLOCK_SIZE as usize;
        let content = pattern(header + streams * stream_len);
        let reader = reader_for(&xet, &content, false);

        let read_size = 128 * 1024usize;
        for step in 0..stream_len / read_size {
            for stream in 0..streams {
                let offset = header + stream * stream_len + step * read_size;
                let (data, _) = reader.read(offset as u64, read_size as u32).await.unwrap();
                assert!(data[..] == content[offset..offset + read_size]);
            }
        }

        let fetched: u64 = stream_calls(&xet).iter().map(|(start, end, _)| end - start).sum();
        let ratio = fetched as f64 / content.len() as f64;
        assert!(ratio < 1.1, "fetched {ratio:.2}x the file");
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

    /// Small reads far apart at increasing offsets (the slices of a
    /// tensor-parallel rank) fetch only what they read, not the gaps.
    #[tokio::test]
    async fn sparse_forward_reads_fetch_only_what_they_read() {
        let xet = MockXet::new();
        // 17 MiB apart.
        let (reads, stride) = (8u64, 68 * BLOCK_SIZE);
        let content = pattern((reads * stride) as usize);
        let reader = reader_for(&xet, &content, false);

        for read in 0..reads {
            let offset = 7 * BLOCK_SIZE + read * stride;
            let (data, _) = reader.read(offset, BLOCK_SIZE as u32).await.unwrap();
            assert!(data[..] == content[offset as usize..(offset + BLOCK_SIZE) as usize]);
        }
        let fetched: u64 = stream_calls(&xet).iter().map(|(start, end, _)| end - start).sum();
        assert!(
            fetched <= 2 * reads * BLOCK_SIZE,
            "fetched {} blocks for {reads} read",
            fetched / BLOCK_SIZE
        );
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

    /// A sequential scan requests read-ahead through the chunk cache: short
    /// fetches first, so that the first blocks arrive early, then runs of
    /// the largest size, without gaps or overlaps.
    #[tokio::test]
    async fn sequential_scan_grows_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(1024 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        let read_size = 128 * 1024u64;
        let mut offset = 0;
        while offset < 600 * BLOCK_SIZE {
            let (data, _) = reader.read(offset, read_size as u32).await.unwrap();
            assert!(data[..] == content[offset as usize..(offset + read_size) as usize]);
            offset += read_size;
        }
        let mut calls = stream_calls(&xet);
        let sizes: Vec<u64> = calls.iter().map(|(start, end, _)| (end - start) / BLOCK_SIZE).collect();
        assert_eq!(sizes[0], MIN_FETCH_BLOCKS, "sizes: {sizes:?}");
        assert!(sizes.contains(&MAX_FETCH_BLOCKS), "sizes: {sizes:?}");
        assert!(
            calls.iter().all(|call| call.2),
            "read-ahead goes through the cache: {calls:?}"
        );
        calls.sort();
        assert!(
            calls.windows(2).all(|pair| pair[0].1 == pair[1].0),
            "fetches overlap or leave gaps: {calls:?}"
        );
    }

    /// Fetch runs stay within cells of `MAX_FETCH_BLOCKS` blocks, also for a
    /// scan that starts inside a cell, so that every pass over a file fetches
    /// the same ranges: past the first cell, whole cells.
    #[tokio::test]
    async fn fetch_runs_stay_within_cells() {
        let xet = MockXet::new();
        let content = pattern(1024 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        let read_size = 128 * 1024u64;
        let mut offset = 37 * BLOCK_SIZE + 1000;
        while offset < 700 * BLOCK_SIZE {
            reader.read(offset, read_size as u32).await.unwrap();
            offset += read_size;
        }
        let cell = MAX_FETCH_BLOCKS * BLOCK_SIZE;
        let calls = stream_calls(&xet);
        assert!(
            calls.iter().all(|(start, end, _)| start / cell == (end - 1) / cell),
            "a run crosses a cell boundary: {calls:?}"
        );
        let whole = calls.iter().filter(|(start, end, _)| end - start == cell).count();
        assert!(whole >= 3, "{whole} whole cells fetched: {calls:?}");
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
        let max_ahead = 24 * BLOCK_SIZE;
        let max_behind = 4 * BLOCK_SIZE;
        let reader = reader_with_limits(&xet, &content, max_ahead, max_behind);

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
            let state = reader.lock();
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
        let budget = ReadAheadBudget::new(120 * BLOCK_SIZE);
        assert_eq!(budget.share(false), 120 * BLOCK_SIZE);
        budget.readers.store(3, Ordering::Relaxed);
        assert_eq!(budget.share(true), 40 * BLOCK_SIZE);
        assert_eq!(budget.share(false), 30 * BLOCK_SIZE);
        assert_eq!(budget.reserve(100 * BLOCK_SIZE), 100 * BLOCK_SIZE);
        assert_eq!(budget.reserve(100 * BLOCK_SIZE), 20 * BLOCK_SIZE);
    }

    /// Readers that start at once stay within the mount budget together,
    /// although the first ones asked while their share was larger.
    #[tokio::test]
    async fn readers_stay_within_the_mount_budget() {
        let xet = MockXet::new();
        let content = pattern(128 * BLOCK_SIZE as usize);
        let budget = ReadAheadBudget::new(256 * 1_048_576);
        let readers: Vec<_> = (0..16)
            .map(|_| reader_with(&xet, &content, |reader| reader.budget = budget.clone()))
            .collect();

        for reader in &readers {
            reader.read(0, 1).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let ahead: u64 = readers.iter().map(|reader| ahead(reader)).sum();
        assert!(
            ahead <= budget.limit,
            "{} MiB ahead, limit {} MiB",
            ahead >> 20,
            budget.limit >> 20
        );
        assert_eq!(budget.held.load(Ordering::Relaxed), ahead);
        drop(readers);
        assert_eq!(budget.held.load(Ordering::Relaxed), 0);
    }

    /// A reader the mount budget turns away counts in its split: at its next
    /// read, a reader above its new share gives read-ahead back, and the
    /// newcomer then gets some.
    #[tokio::test]
    async fn readers_above_their_share_give_read_ahead_back() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        let limit = 256 * BLOCK_SIZE;
        let budget = ReadAheadBudget::new(limit);
        let open = || reader_with(&xet, &content, |reader| reader.budget = budget.clone());
        let unread = |reader: &RemoteReader| reader.lock().fetched_ahead();
        let settle = || tokio::time::sleep(Duration::from_millis(100));

        let first = open();
        for block in 0..40 {
            first.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        settle().await;
        assert!(
            unread(&first) > limit / 2,
            "{} blocks ahead",
            unread(&first) / BLOCK_SIZE
        );

        let second = open();
        second.read(0, 1).await.unwrap();
        assert!(second.lock().starved);
        assert_eq!(budget.readers.load(Ordering::Relaxed), 2);

        first.read(40 * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            unread(&first) <= limit / 2,
            "{} blocks ahead",
            unread(&first) / BLOCK_SIZE
        );

        second.read(BLOCK_SIZE, 4096).await.unwrap();
        settle().await;
        assert!(ahead(&second) > 0);
        assert!(budget.held.load(Ordering::Relaxed) <= limit);
    }

    /// A reader the mount budget refuses all read-ahead to still counts in
    /// its split, until it goes idle.
    #[tokio::test(start_paused = true)]
    async fn refused_reader_counts_until_idle() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        let budget = reader.budget.clone();
        // Other readers hold the whole budget.
        budget.reserve(budget.limit);

        reader.read(0, 1).await.unwrap();
        {
            let state = reader.lock();
            assert!(state.starved);
            assert_eq!(state.ahead_bytes, 0);
        }
        assert_eq!(budget.readers.load(Ordering::Relaxed), 1);

        tokio::time::sleep(IDLE_DROP * 3 / 2).await;
        assert_eq!(budget.readers.load(Ordering::Relaxed), 0);
    }

    /// A block whose only waiting read was cancelled before it arrived is
    /// plain read-ahead: it counts in the budget, and the idle task drops it.
    #[tokio::test(start_paused = true)]
    async fn block_of_a_cancelled_read_becomes_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        let offset = 4 * BLOCK_SIZE;
        xet.set_slow_offset(offset, Duration::from_secs(1));

        let cancelled = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(offset, 4096).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancelled.abort();
        tokio::time::sleep(Duration::from_secs(2)).await;
        {
            let state = reader.lock();
            assert_eq!(state.fetched_ahead(), BLOCK_SIZE);
            assert_eq!(state.waiting, 0);
        }
        tokio::time::sleep(IDLE_RELEASE).await;
        assert_eq!(ahead(&reader), 0);
        assert!(reader.lock().blocks.is_empty());
    }

    /// Blocks a read waits for stay at hand for concurrent reads until that
    /// read ends, even beyond the budget of blocks already read.
    #[tokio::test(start_paused = true)]
    async fn blocks_a_read_waits_for_stay_until_it_ends() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_with_limits(&xet, &content, MAX_AHEAD_BYTES, BLOCK_SIZE);
        xet.set_slow_offset(4 * BLOCK_SIZE, Duration::from_secs(3));

        let long = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(0, 6 * BLOCK_SIZE as u32).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        let calls = stream_calls(&xet).len();
        let (data, _) = reader.read(0, 4096).await.unwrap();
        assert!(data[..] == content[..4096]);
        assert_eq!(stream_calls(&xet).len(), calls, "block 0 fetched again");

        let (data, _) = long.await.unwrap().unwrap();
        assert!(data[..] == content[..6 * BLOCK_SIZE as usize]);
        // Beyond the budget, only the last blocks of the streams stay.
        let state = reader.lock();
        let last_blocks: Vec<u64> = state.streams.iter().map(|stream| stream.last_block).collect();
        let others = state.behind.iter().filter(|block| !last_blocks.contains(block)).count() as u64;
        assert!(
            others * BLOCK_SIZE <= BLOCK_SIZE,
            "behind {:?}, last blocks {last_blocks:?}",
            state.behind
        );
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

    /// A reader that stops reading drops the blocks it fetched ahead, and no
    /// longer counts toward the split of the mount budget.
    #[tokio::test(start_paused = true)]
    async fn idle_reader_releases_its_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        let budget = reader.budget.clone();

        reader.read(0, 4096).await.unwrap();
        tokio::time::sleep(IDLE_RELEASE / 2).await;
        assert!(ahead(&reader) > 0);
        // A read of a new block restarts the delay.
        reader.read(BLOCK_SIZE, 4096).await.unwrap();
        tokio::time::sleep(IDLE_RELEASE * 3 / 4).await;
        assert!(ahead(&reader) > 0);
        tokio::time::sleep(IDLE_RELEASE / 2).await;
        assert_eq!(ahead(&reader), 0);
        assert_eq!(budget.readers.load(Ordering::Relaxed), 0);

        // The blocks are fetched again when the reader resumes.
        let offset = 8 * BLOCK_SIZE;
        let (data, _) = reader.read(offset, 4096).await.unwrap();
        assert!(data[..] == content[offset as usize..offset as usize + 4096]);
    }

    /// Re-reads of a block at hand do not keep the read-ahead of a reader:
    /// only reads that reach new blocks count as activity.
    #[tokio::test(start_paused = true)]
    async fn rereads_do_not_keep_the_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        reader.read(0, 1).await.unwrap();
        let mut reread_for = Duration::ZERO;
        while reread_for <= IDLE_RELEASE {
            tokio::time::sleep(IDLE_DROP / 2).await;
            reader.read(0, 1).await.unwrap();
            reread_for += IDLE_DROP / 2;
        }
        assert_eq!(reader.lock().ahead_bytes, 0);
    }

    /// A read that waits long for a slow fetch does not make its reader look
    /// idle: the blocks fetched ahead before it stay until the read is done.
    #[tokio::test(start_paused = true)]
    async fn waiting_read_keeps_the_read_ahead() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        let reader = reader_with(&xet, &content, |reader| reader.fetch_timeout = Duration::ZERO);

        reader.read(0, 4096).await.unwrap();
        xet.set_stream_delay(IDLE_DROP * 3);
        let far = 400 * BLOCK_SIZE;
        let slow = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(far, 4096).await })
        };
        tokio::time::sleep(IDLE_DROP * 3 / 2).await;
        assert!(ahead(&reader) > BLOCK_SIZE, "read-ahead dropped while a read waits");

        let (data, _) = slow.await.unwrap().unwrap();
        assert!(data[..] == content[far as usize..far as usize + 4096]);
        tokio::time::sleep(IDLE_RELEASE + IDLE_DROP).await;
        assert_eq!(ahead(&reader), 0);
    }

    /// A reader that reads in bursts, with pauses shorter than `IDLE_RELEASE`
    /// (training steps), keeps its read-ahead through the pauses: no block is
    /// fetched twice.
    #[tokio::test(start_paused = true)]
    async fn bursts_with_pauses_fetch_each_block_once() {
        let xet = MockXet::new();
        let content = pattern(1024 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        let mut block = 0;
        for _ in 0..6 {
            for _ in 0..32 {
                reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
                block += 1;
            }
            tokio::time::sleep(IDLE_DROP * 3).await;
        }
        let fetched: u64 = stream_calls(&xet).iter().map(|(start, end, _)| end - start).sum();
        assert!(
            fetched <= content.len() as u64,
            "fetched {:.2}x the file",
            fetched as f64 / content.len() as f64
        );
    }

    /// After a pause longer than `IDLE_RELEASE`, a stream starts again with a
    /// small window: a short read does not fetch a whole window again.
    #[tokio::test(start_paused = true)]
    async fn read_after_a_long_pause_reads_ahead_little() {
        let xet = MockXet::new();
        let content = pattern(2048 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        for block in 0..300 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(IDLE_RELEASE + IDLE_DROP).await;
        let before = stream_calls(&xet).len();
        for block in 300..302 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let fetched: u64 = stream_calls(&xet)[before..]
            .iter()
            .map(|(start, end, _)| end - start)
            .sum();
        assert!(
            fetched <= 4 * INITIAL_WINDOW * BLOCK_SIZE,
            "fetched {} blocks",
            fetched / BLOCK_SIZE
        );
    }

    /// A finished read-ahead fetch tops the window up only while the stream
    /// reads: not once it stopped before the fetch started.
    #[tokio::test]
    async fn refill_stops_once_the_stream_stops_reading() {
        let xet = MockXet::new();
        let content = pattern(1024 * BLOCK_SIZE as usize);
        let reader = reader_with_limits(&xet, &content, 64 * BLOCK_SIZE, MAX_BEHIND_BYTES);

        for block in 0..40 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let (id, tick) = {
            let mut guard = reader.lock();
            let state = &mut *guard;
            // Room in the window again.
            reader.evict_unread(state, 0, true);
            (state.streams[0].id, state.tick)
        };
        let calls = stream_calls(&xet).len();
        reader.refill(id, tick);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(stream_calls(&xet).len(), calls, "refilled a stream that stopped");
        reader.refill(id, tick - 1);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(stream_calls(&xet).len() > calls, "no refill for a stream that read");
    }

    /// Readers whose only fetches are blocks their reads wait for do not
    /// count in the split of the mount budget: a reader that holds
    /// read-ahead keeps it while they wait.
    #[tokio::test(start_paused = true)]
    async fn demand_fetches_do_not_shrink_the_shares() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        xet.add_file("hash", &content);
        let budget = ReadAheadBudget::new(64 * BLOCK_SIZE);
        let new_reader = || {
            RemoteReader::new(
                "hash".into(),
                content.len() as u64,
                xet.clone(),
                Duration::from_secs(5),
                false,
                budget.clone(),
            )
        };
        let streaming = new_reader();
        for block in 0..16 {
            streaming.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let held = ahead(&streaming);
        assert!(held > 40 * BLOCK_SIZE, "{} blocks ahead", held / BLOCK_SIZE);

        let far = 300 * BLOCK_SIZE;
        xet.set_slow_offset(far, Duration::from_secs(1));
        let waiting: Vec<_> = (0..3)
            .map(|_| {
                let reader = new_reader();
                tokio::spawn(async move { reader.read(far, 4096).await })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(budget.readers.load(Ordering::Relaxed), 1);
        streaming.read(16 * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        assert!(
            ahead(&streaming) + BLOCK_SIZE >= held,
            "{} of {} blocks ahead left",
            ahead(&streaming) / BLOCK_SIZE,
            held / BLOCK_SIZE
        );
        for read in waiting {
            read.await.unwrap().unwrap();
        }
    }

    /// A reader that keeps reading elsewhere gives back the read-ahead of a
    /// stream that stopped (a header read at the start of the file), and no
    /// longer counts in the split of the mount budget.
    #[tokio::test]
    async fn read_ahead_of_a_stopped_stream_is_given_back() {
        let xet = MockXet::new();
        let content = pattern(2048 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        let budget = reader.budget.clone();

        reader.read(0, 8).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(ahead(&reader) > 64 * BLOCK_SIZE);
        // Reads far behind each other: none reads ahead.
        for read in 0..2 * STALE_READS + 1 {
            reader.read((2047 - 3 * read) * BLOCK_SIZE, 4096).await.unwrap();
        }
        assert_eq!(ahead(&reader), 0);
        assert_eq!(budget.readers.load(Ordering::Relaxed), 0);
    }

    /// Readers open on one mount split the memory for blocks already read:
    /// many open files (memory-mapped dataset shards) do not each keep
    /// `MAX_BEHIND_BYTES`.
    #[tokio::test]
    async fn open_readers_split_the_blocks_already_read() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        xet.add_file("hash", &content);
        let budget = ReadAheadBudget::new(MAX_TOTAL_AHEAD_BYTES);
        let readers: Vec<_> = (0..64)
            .map(|_| {
                RemoteReader::new(
                    "hash".into(),
                    content.len() as u64,
                    xet.clone(),
                    Duration::from_secs(5),
                    false,
                    budget.clone(),
                )
            })
            .collect();

        for block in 0..64 {
            readers[0].read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        let behind = readers[0].lock().behind_bytes;
        // One more block: the last block of the stream stays.
        assert!(
            behind <= MAX_TOTAL_BEHIND_BYTES / 64 + BLOCK_SIZE,
            "{} blocks already read kept",
            behind / BLOCK_SIZE
        );
    }

    /// An idle reader gives back the blocks it read along with its
    /// read-ahead, after `IDLE_RELEASE`: the page cache holds them.
    #[tokio::test(start_paused = true)]
    async fn idle_reader_releases_the_blocks_it_read() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        for block in 8..16 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        assert!(reader.lock().behind_bytes > 0);
        tokio::time::sleep(IDLE_RELEASE + IDLE_DROP).await;
        let state = reader.lock();
        assert_eq!(state.behind_bytes, 0);
        assert!(state.blocks.is_empty(), "{} blocks left", state.blocks.len());
    }

    /// The last block of a live stream stays at hand while other streams
    /// read: the next read of that stream most often starts in it.
    #[tokio::test]
    async fn last_block_of_a_live_stream_stays() {
        let xet = MockXet::new();
        let content = pattern(256 * BLOCK_SIZE as usize);
        let reader = reader_with_limits(&xet, &content, MAX_AHEAD_BYTES, 4 * BLOCK_SIZE);

        reader.read(100 * BLOCK_SIZE, (BLOCK_SIZE / 2) as u32).await.unwrap();
        for block in 150..182 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        assert!(reader.lock().blocks.contains_key(&100), "block 100 evicted");
        let offset = 100 * BLOCK_SIZE + BLOCK_SIZE / 2;
        let (data, _) = reader.read(offset, (BLOCK_SIZE / 2) as u32).await.unwrap();
        assert!(data[..] == content[offset as usize..(offset + BLOCK_SIZE / 2) as usize]);
    }

    /// An idle reader gives back its read-ahead beyond its share once
    /// another reader is at work, which then gets read-ahead in turn.
    #[tokio::test(start_paused = true)]
    async fn idle_reader_gives_back_read_ahead_beyond_its_share() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        xet.add_file("hash", &content);
        let budget = ReadAheadBudget::new(64 * BLOCK_SIZE);
        let new_reader = || {
            RemoteReader::new(
                "hash".into(),
                content.len() as u64,
                xet.clone(),
                Duration::from_secs(5),
                false,
                budget.clone(),
            )
        };
        let (idle, busy) = (new_reader(), new_reader());

        for block in 0..16 {
            idle.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            ahead(&idle) > 40 * BLOCK_SIZE,
            "{} blocks ahead",
            ahead(&idle) / BLOCK_SIZE
        );
        for block in 0..(2 * IDLE_DROP.as_secs()) {
            busy.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        assert!(
            ahead(&idle) <= 32 * BLOCK_SIZE,
            "{} blocks ahead",
            ahead(&idle) / BLOCK_SIZE
        );
        assert!(ahead(&busy) > 0);
    }

    /// When a stream stops reading (its read waits for a slow fetch), the
    /// fetches that complete keep filling its window up to the budget: only
    /// half of it can be in flight at the last read.
    #[tokio::test]
    async fn completed_fetches_keep_filling_the_window() {
        let xet = MockXet::new();
        let content = pattern(512 * BLOCK_SIZE as usize);
        let max_ahead = 128 * BLOCK_SIZE;
        let reader = reader_with_limits(&xet, &content, max_ahead, MAX_BEHIND_BYTES);

        for block in 0..200 {
            reader.read(block * BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        let state = reader.lock();
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

    /// Forward-only mode: a read that goes on inside a block is served from
    /// it, and a read of bytes already read fetches them again, even inside a
    /// block read in part.
    #[tokio::test]
    async fn forward_only_fetches_again_on_partial_re_read() {
        let xet = MockXet::new();
        let content = pattern(4 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, true);
        let at = BLOCK_SIZE as usize;

        reader.read(BLOCK_SIZE, 4096).await.unwrap();
        let (data, _) = reader.read(BLOCK_SIZE + 4096, 4096).await.unwrap();
        assert!(data[..] == content[at + 4096..at + 8192]);
        assert_eq!(stream_calls(&xet).len(), 1);

        let (data, _) = reader.read(BLOCK_SIZE + 2048, 4096).await.unwrap();
        assert!(data[..] == content[at + 2048..at + 6144]);
        assert_eq!(stream_calls(&xet).len(), 2);
    }

    /// Blocks dropped once read leave the eviction queue, even behind a
    /// block read only in part.
    #[tokio::test]
    async fn forward_only_reads_keep_the_eviction_queue_short() {
        let xet = MockXet::new();
        let content = pattern(4 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, true);

        reader.read(0, 1).await.unwrap();
        for _ in 0..100 {
            let (data, _) = reader.read(BLOCK_SIZE, BLOCK_SIZE as u32).await.unwrap();
            assert!(data[..] == content[BLOCK_SIZE as usize..2 * BLOCK_SIZE as usize]);
        }
        let queued: Vec<u64> = reader.lock().behind.iter().copied().collect();
        assert_eq!(queued, vec![0]);
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

    /// On a slow link, read-ahead waits for slow fetches instead of piling
    /// more in flight behind them: every fetch gets its data within the
    /// fetch timeout, and a long sequential read gets no error.
    #[tokio::test(start_paused = true)]
    async fn slow_link_reads_without_fetch_timeouts() {
        let xet = MockXet::new();
        let content = pattern(384 * 1_048_576);
        let reader = reader_with(&xet, &content, |reader| reader.fetch_timeout = Duration::from_secs(30));
        // 3 MB/s, in 8 MiB terms served in the order they were asked for.
        xet.set_link(3_000_000, 8 * 1_048_576);

        let chunk = 1_048_576u64;
        for offset in (0..256 * chunk).step_by(chunk as usize) {
            let (data, _) = reader.read(offset, chunk as u32).await.unwrap();
            assert!(data[..] == content[offset as usize..(offset + chunk) as usize]);
        }
    }

    /// Attempts that each deliver part of a fetch do not use up its
    /// attempts: only attempts in a row that deliver nothing do.
    #[tokio::test(start_paused = true)]
    async fn attempts_that_deliver_blocks_do_not_count() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        // Every attempt delivers one block, then fails.
        xet.fail_streams_after(BLOCK_SIZE as usize);

        let offset = 8 * BLOCK_SIZE;
        let size = (u64::from(MAX_ATTEMPTS) + 2) * BLOCK_SIZE;
        let (data, _) = reader.read(offset, size as u32).await.unwrap();
        assert!(data[..] == content[offset as usize..(offset + size) as usize]);
    }

    /// A read that joined a fetch that gave up tries once more with a fetch
    /// of its own, as the network may be back by then. The read that started
    /// the fetch fails.
    #[tokio::test(start_paused = true)]
    async fn read_that_joined_a_failed_fetch_tries_again() {
        let xet = MockXet::new();
        let content = pattern(64 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);
        xet.fail_range_downloads(MAX_ATTEMPTS);

        let offset = 8 * BLOCK_SIZE;
        let starting = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(offset, 4096).await })
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        let joining = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(offset, 4096).await })
        };
        assert_eq!(starting.await.unwrap().unwrap_err(), libc::EIO);
        let (data, _) = joining.await.unwrap().unwrap();
        assert!(data[..] == content[offset as usize..offset as usize + 4096]);
    }

    /// A read that waited for a block its fetch gave up on may end after a
    /// later read fetched the block again: it must not count itself out of
    /// that new fetch, in flight or arrived.
    #[test]
    fn leaving_a_refetched_block_keeps_its_waiters() {
        let mut state = State::default();
        let (_sender, fill) = watch::channel(None);
        state.blocks.insert(
            4,
            Block::Pending {
                fill,
                waiters: 1,
                fetch: 2,
            },
        );
        state.leave(4, 1, BLOCK_SIZE);
        assert!(matches!(state.blocks.get(&4), Some(Block::Pending { waiters: 1, .. })));

        state.blocks.insert(
            4,
            Block::Ready {
                data: Bytes::from_static(b"data"),
                read: true,
                last_used: 0,
                waiters: 1,
                fetch: 2,
                read_to: 0,
            },
        );
        state.leave(4, 1, 4);
        assert!(matches!(state.blocks.get(&4), Some(Block::Ready { waiters: 1, .. })));
        assert!(state.behind.is_empty());
    }

    /// A fetch task that ends before delivering its blocks (aborted, or it
    /// panicked) fails them: the reads waiting get EIO, and the blocks do not
    /// stay in flight with no fetch behind them.
    #[tokio::test]
    async fn aborted_fetch_fails_its_blocks() {
        let xet = MockXet::new();
        let content = pattern(8 * BLOCK_SIZE as usize);
        let reader = reader_with(&xet, &content, |reader| reader.fetch_timeout = Duration::ZERO);
        xet.stall_stream_reads();

        let stalled = {
            let reader = reader.clone();
            tokio::spawn(async move { reader.read(BLOCK_SIZE, 4096).await })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        for fetch in reader.lock().fetches.clone() {
            fetch.abort();
        }
        assert_eq!(stalled.await.unwrap().unwrap_err(), libc::EIO);
        let state = reader.lock();
        assert!(state.blocks.is_empty(), "{} blocks left in flight", state.blocks.len());
        assert_eq!(state.ahead_bytes, 0);
        assert_eq!(state.pending_bytes, 0);
    }

    /// A read waiting for several fetches ends as soon as one of them gives
    /// up, even while an earlier one is still in flight, however many blocks
    /// it covers.
    #[tokio::test(start_paused = true)]
    async fn failed_block_ends_the_read_without_waiting_for_the_others() {
        for blocks in [8, 40] {
            let xet = MockXet::new();
            let content = pattern(512 * BLOCK_SIZE as usize);
            let reader = reader_with(&xet, &content, |reader| reader.fetch_timeout = Duration::ZERO);
            // The first run of the read takes an hour; the second one fails.
            xet.set_slow_offset(0, Duration::from_secs(3600));
            xet.set_fail_offset(5 * BLOCK_SIZE);

            let started = tokio::time::Instant::now();
            let result = reader.read(0, blocks * BLOCK_SIZE as u32).await;
            assert_eq!(result.unwrap_err(), libc::EIO);
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "{blocks} blocks: waited {:?}",
                started.elapsed()
            );
        }
    }

    /// Fetches start once the state is unlocked: a runtime that shuts down
    /// drops a spawned future at once, and dropping a fetch locks the state
    /// to fail its blocks. Under the lock, that was a deadlock.
    #[test]
    fn fetches_spawned_on_a_closed_runtime_fail_without_deadlock() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        runtime.shutdown_background();
        let xet = MockXet::new();
        let content = pattern(8 * BLOCK_SIZE as usize);
        let reader = reader_for(&xet, &content, false);

        let (done, result) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _runtime = handle.enter();
            let _ = done.send(reader.read(0, 4096).now_or_never());
        });
        let read = result
            .recv_timeout(Duration::from_secs(10))
            .expect("the read deadlocked");
        assert_eq!(read, Some(Err(libc::EIO)));
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
        let fetches: Vec<AbortHandle> = reader.lock().fetches.clone();
        assert!(!fetches.is_empty());
        drop(reader);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(fetches.iter().all(AbortHandle::is_finished));
    }
}
