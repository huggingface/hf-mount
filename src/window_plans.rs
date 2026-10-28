//! Reconstruction plans derived from one plan per window of `WINDOW` bytes and
//! the chunk layout of that window (`GET /v2/chunk-layout`).
//!
//! xet asks CAS for the plan of each fetch in pieces, about 96 requests per GiB
//! read. With the plan and the size of each chunk of a whole window, the plan of
//! any range inside it is computed here: the first and last terms are trimmed on
//! chunk boundaries with the uncompressed sizes, and each term is fetched as a
//! sub-range of the window's signed ranges, located with the serialized sizes.
//! That is the plan CAS would return for the range, with the window's URLs.
//!
//! A window is asked for in the background, so no read waits for one. A reader that
//! starts at the beginning of the file, or that comes from the previous window, reads
//! on: its window is asked at once, with the next `AHEAD` ones, so that they arrive
//! before the read-ahead reaches them. Elsewhere a window is asked on the second
//! request that falls in it, so a lone random read never pulls one. Files smaller
//! than `MIN_FILE` keep exact plans: they are read before a window arrives.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::StatusCode;
use xet_client::ClientError;
use xet_client::cas_client::Client;
use xet_client::cas_types::{
    ChunkLayoutResponse, ChunkRange, FileRange, HexMerkleHash, HttpRange, QueryReconstructionResponseV2,
    XorbMultiRangeFetch, XorbRangeDescriptor, XorbReconstructionTerm,
};
use xet_core_structures::merklehash::MerkleHash;

/// A window takes about 0.3 s plus 1 s per GiB to arrive from CAS: a smaller one arrives
/// sooner, so fewer plans are asked from CAS before it does, for a few more windows per GiB.
pub const WINDOW: u64 = 256 << 20;
/// Windows asked ahead of the one a reader reads on into, 1 GiB in all. At 1 GB/s a reader
/// goes through a window in a quarter of a second, less than one takes to arrive.
const AHEAD: u64 = 4;
/// Files smaller than this keep exact plans: they are read before a window arrives.
const MIN_FILE: u64 = 256 << 20;
/// File sizes kept for the windows, the files read through the mount most recently.
const MAX_SIZES: usize = 4096;
/// Signed URLs live 60 minutes; a window is replaced before.
const WINDOW_TTL: Duration = Duration::from_secs(50 * 60);
/// A window that could not be fetched is asked again after this delay.
const RETRY_AFTER: Duration = Duration::from_secs(60);
/// Windows kept, by last use. A window of a 1 GiB fragmented range weighs a few hundred KB.
const MAX_WINDOWS: usize = 128;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum WindowPlansMode {
    /// Every plan is asked from CAS.
    #[default]
    Off,
    /// Plans inside a cached window are derived from it.
    On,
    /// Plans are derived, compared with the plan CAS returns, and the CAS plan is used.
    Check,
}

/// What a window is built from: its plan and its chunk layout, both from CAS.
#[async_trait::async_trait]
pub trait WindowSource: Send + Sync {
    async fn plan(
        &self,
        file_id: &MerkleHash,
        range: FileRange,
    ) -> Result<Option<QueryReconstructionResponseV2>, ClientError>;

    async fn chunk_layout(
        &self,
        file_id: &MerkleHash,
        range: FileRange,
    ) -> Result<Option<ChunkLayoutResponse>, LayoutError>;
}

#[derive(Debug)]
pub enum LayoutError {
    /// CAS does not serve chunk layouts.
    NotServed,
    Client(ClientError),
}

#[async_trait::async_trait]
impl WindowSource for Arc<dyn Client> {
    async fn plan(
        &self,
        file_id: &MerkleHash,
        range: FileRange,
    ) -> Result<Option<QueryReconstructionResponseV2>, ClientError> {
        self.get_reconstruction(file_id, Some(range)).await
    }

    async fn chunk_layout(
        &self,
        file_id: &MerkleHash,
        range: FileRange,
    ) -> Result<Option<ChunkLayoutResponse>, LayoutError> {
        self.get_chunk_layout(file_id, range, true)
            .await
            .map_err(|err| match err {
                // CAS answers 404 when it does not serve chunk layouts.
                ClientError::ReqwestError(ref inner, _) if inner.status() == Some(StatusCode::NOT_FOUND) => {
                    LayoutError::NotServed
                }
                err => LayoutError::Client(err),
            })
    }
}

/// Chunk ranges to fetch, keyed by the index of the signed range that holds them.
type BySignedRange = HashMap<usize, Vec<(u32, u32)>>;

/// A window's plan, with the serialized size of every chunk of its terms.
pub struct Window {
    start: u64,
    plan: QueryReconstructionResponseV2,
    layout: ChunkLayoutResponse,
    serialized: HashMap<(HexMerkleHash, u32), u32>,
    fetched_at: Instant,
}

impl Window {
    fn new(start: u64, plan: QueryReconstructionResponseV2, layout: ChunkLayoutResponse) -> Result<Self, &'static str> {
        if plan
            .xorbs
            .values()
            .flatten()
            .any(|fetch| fetch.url.contains("X-Amz-Signature"))
        {
            return Err("direct S3 URLs sign the exact range");
        }
        if layout.offset_into_first_range != plan.offset_into_first_range || layout.terms.len() != plan.terms.len() {
            return Err("chunk layout does not match the plan");
        }
        let mut serialized = HashMap::new();
        for (term, term_layout) in plan.terms.iter().zip(&layout.terms) {
            let chunks = (term.range.end - term.range.start) as usize;
            let Some(sizes) = term_layout.serialized.as_ref() else {
                return Err("chunk layout without serialized sizes");
            };
            if term_layout.hash != term.hash
                || term_layout.range != term.range
                || term_layout.uncompressed.len() != chunks
                || sizes.len() != chunks
            {
                return Err("chunk layout does not match the plan");
            }
            for (chunk, size) in (term.range.start..term.range.end).zip(sizes) {
                serialized.insert((term.hash, chunk), *size);
            }
        }
        Ok(Self {
            start,
            plan,
            layout,
            serialized,
            fetched_at: Instant::now(),
        })
    }

    /// Offset in the xorb of the start of `chunk`, through the signed range that holds it.
    fn byte_offset(&self, hash: &HexMerkleHash, chunk: u32) -> Option<u64> {
        let signed = self
            .plan
            .xorbs
            .get(hash)?
            .iter()
            .flat_map(|fetch| &fetch.ranges)
            .find(|signed| signed.chunks.start <= chunk && chunk <= signed.chunks.end)?;
        self.offset_in(hash, signed, chunk)
    }

    fn offset_in(&self, hash: &HexMerkleHash, signed: &XorbRangeDescriptor, chunk: u32) -> Option<u64> {
        let sizes: Option<u64> = (signed.chunks.start..chunk)
            .map(|c| self.serialized.get(&(*hash, c)).map(|size| *size as u64))
            .sum();
        Some(signed.bytes.start + sizes?)
    }

    /// The plan CAS would return for `range`, which lies in this window. `None` when it
    /// cannot be derived; `Some(None)` past the end of the file, as CAS answers.
    fn derive(&self, range: FileRange) -> Option<Option<QueryReconstructionResponseV2>> {
        let plan = &self.plan;
        let mut terms = Vec::new();
        let mut offset_into_first_range = 0;
        // Chunk ranges to fetch, per xorb and fetch entry of the window.
        let mut fetches: HashMap<(HexMerkleHash, usize), BySignedRange> = HashMap::new();
        let mut term_start = self.start.checked_sub(plan.offset_into_first_range)?;
        for (term, term_layout) in plan.terms.iter().zip(&self.layout.terms) {
            let term_end = term_start + term.unpacked_length as u64;
            if term_end <= range.start {
                term_start = term_end;
                continue;
            }
            if term_start >= range.end {
                break;
            }
            let mut first = None;
            let mut last = 0;
            let mut chunk_start = term_start;
            for (index, size) in term_layout.uncompressed.iter().enumerate() {
                if chunk_start >= range.end {
                    break;
                }
                let chunk_end = chunk_start + *size as u64;
                if first.is_none() && chunk_end > range.start {
                    first = Some((index, chunk_start));
                }
                last = index;
                chunk_start = chunk_end;
            }
            let (first, first_start) = first?;
            if terms.is_empty() {
                offset_into_first_range = range.start - first_start;
            }
            let chunks = ChunkRange::new(term.range.start + first as u32, term.range.start + last as u32 + 1);
            terms.push(XorbReconstructionTerm {
                hash: term.hash,
                range: chunks,
                unpacked_length: term_layout.uncompressed[first..=last].iter().sum(),
            });
            let (entry, index) = plan
                .xorbs
                .get(&term.hash)?
                .iter()
                .enumerate()
                .find_map(|(entry, fetch)| {
                    fetch
                        .ranges
                        .iter()
                        .position(|signed| {
                            signed.chunks.start <= term.range.start && term.range.end <= signed.chunks.end
                        })
                        .map(|index| (entry, index))
                })?;
            fetches
                .entry((term.hash, entry))
                .or_default()
                .entry(index)
                .or_default()
                .push((chunks.start, chunks.end));
            term_start = term_end;
        }
        if terms.is_empty() {
            // Like CAS: an empty plan only for an empty file read from its start.
            return (range.start > 0 || !plan.terms.is_empty()).then_some(None);
        }

        let mut xorbs: HashMap<HexMerkleHash, Vec<XorbMultiRangeFetch>> = HashMap::new();
        for ((hash, entry), by_signed_range) in fetches {
            let fetch = &plan.xorbs[&hash][entry];
            let mut descriptors = Vec::new();
            for (index, mut ranges) in by_signed_range {
                let signed = &fetch.ranges[index];
                // xet shares a downloaded block between terms by its first chunk: merge
                // overlapping and adjacent ranges, as CAS does.
                ranges.sort_unstable();
                let mut merged: Vec<(u32, u32)> = Vec::new();
                for (start, end) in ranges {
                    match merged.last_mut() {
                        Some(last) if start <= last.1 => last.1 = last.1.max(end),
                        _ => merged.push((start, end)),
                    }
                }
                for (start, end) in merged {
                    let bytes = HttpRange::new(
                        self.offset_in(&hash, signed, start)?,
                        self.offset_in(&hash, signed, end)? - 1,
                    );
                    // The CDN serves a sub-range of a signed range, nothing outside it.
                    if bytes.end > signed.bytes.end {
                        return None;
                    }
                    descriptors.push(XorbRangeDescriptor {
                        chunks: ChunkRange::new(start, end),
                        bytes,
                    });
                }
            }
            descriptors.sort_unstable_by_key(|descriptor| descriptor.chunks.start);
            xorbs.entry(hash).or_default().push(XorbMultiRangeFetch {
                url: fetch.url.clone(),
                ranges: descriptors,
            });
        }
        Some(Some(QueryReconstructionResponseV2 {
            offset_into_first_range,
            terms,
            xorbs,
        }))
    }

    /// Whether `derived` asks for what `exact`, the plan CAS returned, asks for.
    fn matches(
        &self,
        derived: &Option<QueryReconstructionResponseV2>,
        exact: &Option<QueryReconstructionResponseV2>,
    ) -> Result<(), String> {
        let (derived, exact) = match (derived, exact) {
            (None, None) => return Ok(()),
            (Some(derived), Some(exact)) => (derived, exact),
            _ => return Err("one plan is past the end of the file".to_string()),
        };
        if derived.offset_into_first_range != exact.offset_into_first_range {
            return Err(format!(
                "offset {} instead of {}",
                derived.offset_into_first_range, exact.offset_into_first_range
            ));
        }
        let term_key =
            |term: &XorbReconstructionTerm| (term.hash, term.range.start, term.range.end, term.unpacked_length);
        if !derived.terms.iter().map(term_key).eq(exact.terms.iter().map(term_key)) {
            return Err("terms differ".to_string());
        }
        let covered = |plan: &QueryReconstructionResponseV2| {
            let mut chunks: Vec<(MerkleHash, u32, u32)> = plan
                .xorbs
                .iter()
                .flat_map(|(hash, fetches)| {
                    fetches
                        .iter()
                        .flat_map(|fetch| &fetch.ranges)
                        .map(move |descriptor| (hash.0, descriptor.chunks.start, descriptor.chunks.end))
                })
                .collect();
            chunks.sort_unstable();
            let mut merged: Vec<(MerkleHash, u32, u32)> = Vec::new();
            for (hash, start, end) in chunks {
                match merged.last_mut() {
                    Some(last) if last.0 == hash && start <= last.2 => last.2 = last.2.max(end),
                    _ => merged.push((hash, start, end)),
                }
            }
            merged
        };
        if covered(derived) != covered(exact) {
            return Err("fetched chunks differ".to_string());
        }
        for (hash, fetches) in &exact.xorbs {
            for descriptor in fetches.iter().flat_map(|fetch| &fetch.ranges) {
                let start = self.byte_offset(hash, descriptor.chunks.start);
                let end = self.byte_offset(hash, descriptor.chunks.end);
                if start != Some(descriptor.bytes.start) || end != Some(descriptor.bytes.end + 1) {
                    return Err(format!(
                        "xorb {} chunks {:?}: bytes {:?} in CAS, {:?}..{:?} from the layout",
                        hash, descriptor.chunks, descriptor.bytes, start, end
                    ));
                }
            }
        }
        Ok(())
    }
}

enum SlotState {
    /// One request fell in the window.
    Seen,
    Fetching,
    Ready(Arc<Window>),
    Failed(Instant),
}

struct Slot {
    state: SlotState,
    used_at: Instant,
}

pub struct WindowPlans {
    source: Arc<dyn WindowSource>,
    mode: WindowPlansMode,
    /// Set when CAS does not serve chunk layouts: every plan is asked from CAS.
    disabled: AtomicBool,
    slots: Mutex<HashMap<(MerkleHash, u64), Slot>>,
    /// The size of the files read through the mount. Windows need it to stop at the end of a file.
    sizes: Mutex<HashMap<MerkleHash, u64>>,
}

/// A derived plan and the window it comes from.
pub struct Derived {
    pub plan: Option<QueryReconstructionResponseV2>,
    window: Arc<Window>,
}

impl WindowPlans {
    pub fn new(source: Arc<dyn WindowSource>, mode: WindowPlansMode) -> Arc<Self> {
        Arc::new(Self {
            source,
            mode,
            disabled: AtomicBool::new(false),
            slots: Mutex::new(HashMap::new()),
            sizes: Mutex::new(HashMap::new()),
        })
    }

    pub fn mode(&self) -> WindowPlansMode {
        self.mode
    }

    /// Record the size of a file read through the mount.
    pub fn note_size(&self, file_id: &MerkleHash, size: u64) {
        let mut sizes = self.sizes.lock().expect("sizes poisoned");
        if sizes.len() >= MAX_SIZES && !sizes.contains_key(file_id) {
            sizes.clear();
        }
        sizes.insert(*file_id, size);
    }

    /// The plan of `range` from its window, or `None` when the caller must ask CAS.
    pub fn derive(self: &Arc<Self>, file_id: &MerkleHash, range: FileRange) -> Option<Derived> {
        if self.disabled.load(Ordering::Relaxed) {
            return None;
        }
        let size = self.sizes.lock().expect("sizes poisoned").get(file_id).copied()?;
        let start = range.start / WINDOW * WINDOW;
        if size < MIN_FILE || range.end <= range.start || range.end > start + WINDOW {
            return None;
        }
        let reads_on = range.start == 0 || (start >= WINDOW && self.asked(file_id, start - WINDOW));
        let window = self.window(*file_id, start, reads_on);
        if self.asked(file_id, start) {
            for next in (1..=AHEAD).map(|i| start + i * WINDOW).take_while(|next| *next < size) {
                self.window(*file_id, next, true);
            }
        }
        let window = window?;
        let plan = window.derive(range)?;
        Some(Derived { plan, window })
    }

    /// Whether the window at `start` was asked: in flight, ready or failed.
    fn asked(&self, file_id: &MerkleHash, start: u64) -> bool {
        self.slots
            .lock()
            .expect("window slots poisoned")
            .get(&(*file_id, start))
            .is_some_and(|slot| !matches!(slot.state, SlotState::Seen))
    }

    /// Whether `derived` matches `exact`; logs the difference when it does not.
    pub fn check(
        &self,
        file_id: &MerkleHash,
        range: FileRange,
        derived: &Derived,
        exact: &Option<QueryReconstructionResponseV2>,
    ) {
        match derived.window.matches(&derived.plan, exact) {
            Ok(()) => tracing::debug!("window plans: {:.8} {:?} matches CAS", file_id, range),
            Err(diff) => tracing::warn!("window plans: {:.8} {:?} differs from CAS: {}", file_id, range, diff),
        }
    }

    /// The window at `start` if it is ready. Otherwise counts the request and asks for
    /// the window in the background on the second request, or at once with `ask`.
    fn window(self: &Arc<Self>, file_id: MerkleHash, start: u64, ask: bool) -> Option<Arc<Window>> {
        let now = Instant::now();
        let fetch = {
            let mut slots = self.slots.lock().expect("window slots poisoned");
            match slots.get_mut(&(file_id, start)) {
                Some(slot) => {
                    slot.used_at = now;
                    match &slot.state {
                        SlotState::Ready(window) if window.fetched_at.elapsed() < WINDOW_TTL => {
                            return Some(window.clone());
                        }
                        SlotState::Fetching => return None,
                        SlotState::Failed(at) if at.elapsed() < RETRY_AFTER => return None,
                        SlotState::Seen | SlotState::Ready(_) | SlotState::Failed(_) => {
                            slot.state = SlotState::Fetching;
                            true
                        }
                    }
                }
                None => {
                    if slots.len() >= MAX_WINDOWS
                        && let Some(oldest) = slots.iter().min_by_key(|(_, slot)| slot.used_at).map(|(key, _)| *key)
                    {
                        slots.remove(&oldest);
                    }
                    let state = if ask { SlotState::Fetching } else { SlotState::Seen };
                    slots.insert((file_id, start), Slot { state, used_at: now });
                    ask
                }
            }
        };
        if fetch {
            let this = self.clone();
            tokio::spawn(async move {
                let state = this.fetch_window(&file_id, start).await;
                if let Some(slot) = this
                    .slots
                    .lock()
                    .expect("window slots poisoned")
                    .get_mut(&(file_id, start))
                {
                    slot.state = state;
                }
            });
        }
        None
    }

    async fn fetch_window(&self, file_id: &MerkleHash, start: u64) -> SlotState {
        let range = FileRange::new(start, start + WINDOW);
        let (plan, layout) = tokio::join!(
            self.source.plan(file_id, range),
            self.source.chunk_layout(file_id, range)
        );
        match (plan, layout) {
            (Ok(Some(plan)), Ok(Some(layout))) => match Window::new(start, plan, layout) {
                Ok(window) => {
                    tracing::debug!("window plans: {:.8} window at {} ready", file_id, start);
                    return SlotState::Ready(Arc::new(window));
                }
                Err(reason) => tracing::warn!("window plans: {:.8} window at {}: {}", file_id, start, reason),
            },
            // Past the end of the file.
            (Ok(None), _) | (_, Ok(None)) => {}
            (Ok(Some(_)), Err(LayoutError::NotServed)) => {
                if !self.disabled.swap(true, Ordering::Relaxed) {
                    tracing::info!("window plans: CAS does not serve chunk layouts, plans stay exact");
                }
            }
            (Err(err), _) => tracing::warn!("window plans: {:.8} window at {}: plan: {}", file_id, start, err),
            (_, Err(err)) => tracing::warn!(
                "window plans: {:.8} window at {}: chunk layout: {:?}",
                file_id,
                start,
                err
            ),
        }
        SlotState::Failed(Instant::now())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use xet_core_structures::merklehash::compute_data_hash;

    use super::*;

    /// A xorb's chunks: uncompressed and serialized sizes.
    struct Xorb {
        hash: HexMerkleHash,
        uncompressed: Vec<u32>,
        serialized: Vec<u32>,
    }

    impl Xorb {
        fn new(seed: u8, chunks: u32) -> Self {
            Self {
                hash: compute_data_hash(&[seed]).into(),
                uncompressed: (0..chunks).map(|c| 1000 + 37 * c + seed as u32).collect(),
                serialized: (0..chunks).map(|c| 600 + 11 * c + seed as u32).collect(),
            }
        }

        fn byte_offset(&self, chunk: u32) -> u64 {
            self.serialized[..chunk as usize].iter().map(|size| *size as u64).sum()
        }
    }

    /// A file made of whole-xorb chunk ranges; plans built the way CAS builds them.
    struct File {
        xorbs: Vec<Xorb>,
        /// (xorb index, chunk start, chunk end) in file order.
        terms: Vec<(usize, u32, u32)>,
    }

    impl File {
        fn size(&self) -> u64 {
            self.terms
                .iter()
                .map(|(x, s, e)| {
                    self.xorbs[*x].uncompressed[*s as usize..*e as usize]
                        .iter()
                        .map(|u| *u as u64)
                        .sum::<u64>()
                })
                .sum()
        }

        /// The plan CAS returns for `range`, with one signed range per condensed chunk range.
        fn plan(&self, range: FileRange) -> Option<QueryReconstructionResponseV2> {
            let mut terms = Vec::new();
            let mut offset = 0;
            let mut chunk_start = 0u64;
            for (x, start, end) in &self.terms {
                let xorb = &self.xorbs[*x];
                let mut first = None;
                let mut last = 0;
                for c in *start..*end {
                    let chunk_end = chunk_start + xorb.uncompressed[c as usize] as u64;
                    if chunk_end > range.start && chunk_start < range.end {
                        if first.is_none() {
                            first = Some(c);
                            if terms.is_empty() {
                                offset = range.start - chunk_start;
                            }
                        }
                        last = c;
                    }
                    chunk_start = chunk_end;
                }
                if let Some(first) = first {
                    terms.push(XorbReconstructionTerm {
                        hash: xorb.hash,
                        range: ChunkRange::new(first, last + 1),
                        unpacked_length: xorb.uncompressed[first as usize..=last as usize].iter().sum(),
                    });
                }
            }
            if terms.is_empty() {
                return None;
            }
            let mut by_xorb: HashMap<HexMerkleHash, Vec<(u32, u32)>> = HashMap::new();
            for term in &terms {
                by_xorb
                    .entry(term.hash)
                    .or_default()
                    .push((term.range.start, term.range.end));
            }
            let xorbs = by_xorb
                .into_iter()
                .map(|(hash, mut ranges)| {
                    let xorb = self.xorbs.iter().find(|x| x.hash == hash).unwrap();
                    ranges.sort_unstable();
                    let mut merged: Vec<(u32, u32)> = Vec::new();
                    for (s, e) in ranges {
                        match merged.last_mut() {
                            Some(last) if s <= last.1 => last.1 = last.1.max(e),
                            _ => merged.push((s, e)),
                        }
                    }
                    let ranges = merged
                        .into_iter()
                        .map(|(s, e)| XorbRangeDescriptor {
                            chunks: ChunkRange::new(s, e),
                            bytes: HttpRange::new(xorb.byte_offset(s), xorb.byte_offset(e) - 1),
                        })
                        .collect();
                    let url = format!("https://cdn.example/{}?Expires=1", hash);
                    (hash, vec![XorbMultiRangeFetch { url, ranges }])
                })
                .collect();
            Some(QueryReconstructionResponseV2 {
                offset_into_first_range: offset,
                terms,
                xorbs,
            })
        }

        fn layout(&self, range: FileRange) -> ChunkLayoutResponse {
            let plan = self.plan(range).unwrap();
            let terms = plan
                .terms
                .iter()
                .map(|term| {
                    let xorb = self.xorbs.iter().find(|x| x.hash == term.hash).unwrap();
                    let chunks = term.range.start as usize..term.range.end as usize;
                    xet_client::cas_types::TermChunkLayout {
                        hash: term.hash,
                        range: term.range,
                        uncompressed: xorb.uncompressed[chunks.clone()].to_vec(),
                        serialized: Some(xorb.serialized[chunks].to_vec()),
                    }
                })
                .collect();
            ChunkLayoutResponse {
                offset_into_first_range: plan.offset_into_first_range,
                terms,
            }
        }

        fn window(&self, start: u64) -> Window {
            let range = FileRange::new(start, start + WINDOW);
            Window::new(start, self.plan(range).unwrap(), self.layout(range)).unwrap()
        }
    }

    /// Two xorbs, the second deduplicated against the first in the middle of the file.
    fn file() -> File {
        File {
            xorbs: vec![Xorb::new(1, 40), Xorb::new(2, 30)],
            terms: vec![(0, 0, 25), (1, 0, 30), (0, 10, 40)],
        }
    }

    fn assert_same_plan(
        derived: &Option<QueryReconstructionResponseV2>,
        exact: &Option<QueryReconstructionResponseV2>,
    ) {
        // The JSON objects compare the xorb maps without their order.
        assert_eq!(
            serde_json::to_value(derived).unwrap(),
            serde_json::to_value(exact).unwrap()
        );
    }

    #[test]
    fn derived_plans_are_the_plans_cas_returns() {
        let file = file();
        let window = file.window(0);
        let size = file.size();
        let mut ranges = vec![(0, size), (1, size - 1), (0, 1), (size - 1, size)];
        for step in [997, 4093, 25_000] {
            ranges.extend(
                (0..size)
                    .step_by(step)
                    .map(|start| (start, (start + step as u64 * 2).min(size))),
            );
        }
        for (start, end) in ranges {
            let range = FileRange::new(start, end);
            let derived = window.derive(range).expect("derivable");
            let exact = file.plan(range);
            assert_same_plan(&derived, &exact);
            window.matches(&derived, &exact).unwrap();
        }
        // Past the end of the file, CAS answers with no plan.
        assert!(window.derive(FileRange::new(size, size + 10)).unwrap().is_none());
    }

    #[test]
    fn check_reports_a_wrong_byte_range() {
        let file = file();
        let window = file.window(0);
        let range = FileRange::new(5000, 30_000);
        let derived = window.derive(range).unwrap();
        let mut exact = file.plan(range);
        let fetch = exact.as_mut().unwrap().xorbs.values_mut().next().unwrap();
        fetch[0].ranges[0].bytes.end += 1;
        assert!(window.matches(&derived, &exact).is_err());
    }

    #[test]
    fn direct_s3_urls_are_not_derived() {
        let file = file();
        let range = FileRange::new(0, WINDOW);
        let mut plan = file.plan(range).unwrap();
        for fetch in plan.xorbs.values_mut().flatten() {
            fetch.url.push_str("&X-Amz-Signature=abc");
        }
        assert!(Window::new(0, plan, file.layout(range)).is_err());
    }

    struct FileSource {
        file: File,
        plans: AtomicUsize,
        layouts: AtomicUsize,
        not_served: bool,
    }

    impl FileSource {
        fn new(not_served: bool) -> Arc<Self> {
            Arc::new(Self {
                file: file(),
                plans: AtomicUsize::new(0),
                layouts: AtomicUsize::new(0),
                not_served,
            })
        }
    }

    #[async_trait::async_trait]
    impl WindowSource for FileSource {
        async fn plan(
            &self,
            _: &MerkleHash,
            range: FileRange,
        ) -> Result<Option<QueryReconstructionResponseV2>, ClientError> {
            self.plans.fetch_add(1, Ordering::Relaxed);
            Ok(self.file.plan(range))
        }

        async fn chunk_layout(
            &self,
            _: &MerkleHash,
            range: FileRange,
        ) -> Result<Option<ChunkLayoutResponse>, LayoutError> {
            self.layouts.fetch_add(1, Ordering::Relaxed);
            if self.not_served {
                return Err(LayoutError::NotServed);
            }
            Ok(self.file.plan(range).map(|_| self.file.layout(range)))
        }
    }

    async fn settle(plans: &WindowPlans) {
        for _ in 0..200 {
            let fetching = plans
                .slots
                .lock()
                .unwrap()
                .values()
                .any(|slot| matches!(slot.state, SlotState::Fetching));
            if !fetching {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("window fetch did not finish");
    }

    #[tokio::test]
    async fn the_second_request_in_a_window_fetches_it() {
        let source = FileSource::new(false);
        let plans = WindowPlans::new(source.clone(), WindowPlansMode::On);
        let file_id = MerkleHash::default();
        plans.note_size(&file_id, MIN_FILE);
        let range = FileRange::new(100, 20_000);

        assert!(plans.derive(&file_id, range).is_none());
        settle(&plans).await;
        assert_eq!(
            source.layouts.load(Ordering::Relaxed),
            0,
            "one request does not pull a window"
        );

        assert!(plans.derive(&file_id, range).is_none());
        settle(&plans).await;
        assert_eq!(source.layouts.load(Ordering::Relaxed), 1);
        assert_eq!(source.plans.load(Ordering::Relaxed), 1);

        let derived = plans.derive(&file_id, range).expect("window ready");
        assert_same_plan(&derived.plan, &file().plan(range));
        assert_eq!(source.plans.load(Ordering::Relaxed), 1, "derived without asking CAS");
    }

    #[tokio::test]
    async fn a_read_from_the_start_fetches_the_window_at_once() {
        let source = FileSource::new(false);
        let plans = WindowPlans::new(source.clone(), WindowPlansMode::On);
        let file_id = MerkleHash::default();
        plans.note_size(&file_id, MIN_FILE);

        assert!(plans.derive(&file_id, FileRange::new(0, 8_000)).is_none());
        settle(&plans).await;
        assert_eq!(source.layouts.load(Ordering::Relaxed), 1);
        let range = FileRange::new(8_000, 20_000);
        let derived = plans.derive(&file_id, range).expect("window ready");
        assert_same_plan(&derived.plan, &file().plan(range));
    }

    #[tokio::test]
    async fn a_reader_that_reads_on_asks_the_windows_ahead() {
        let source = FileSource::new(false);
        let plans = WindowPlans::new(source.clone(), WindowPlansMode::On);
        let file_id = MerkleHash::default();
        plans.note_size(&file_id, (AHEAD + 2) * WINDOW);

        // From the start of the file: the first window and the `AHEAD` next ones.
        assert!(plans.derive(&file_id, FileRange::new(0, 8_000)).is_none());
        settle(&plans).await;
        assert_eq!(source.layouts.load(Ordering::Relaxed), 1 + AHEAD as usize);

        // Into the window after the last one asked: at once, on its first request.
        let next = (AHEAD + 1) * WINDOW;
        assert!(plans.derive(&file_id, FileRange::new(next + 100, next + 200)).is_none());
        settle(&plans).await;
        assert_eq!(source.layouts.load(Ordering::Relaxed), 2 + AHEAD as usize);
    }

    #[tokio::test]
    async fn small_files_and_files_of_unknown_size_keep_exact_plans() {
        let source = FileSource::new(false);
        let plans = WindowPlans::new(source.clone(), WindowPlansMode::On);
        let unknown = MerkleHash::default();
        let small = compute_data_hash(b"small");
        plans.note_size(&small, MIN_FILE - 1);
        for file_id in [unknown, small] {
            for _ in 0..3 {
                assert!(plans.derive(&file_id, FileRange::new(0, 8_000)).is_none());
                settle(&plans).await;
            }
        }
        assert_eq!(source.layouts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn a_cas_without_chunk_layouts_turns_derivation_off() {
        let source = FileSource::new(true);
        let plans = WindowPlans::new(source.clone(), WindowPlansMode::On);
        let file_id = MerkleHash::default();
        plans.note_size(&file_id, MIN_FILE);
        for _ in 0..2 {
            assert!(plans.derive(&file_id, FileRange::new(0, 100)).is_none());
            settle(&plans).await;
        }
        assert!(plans.disabled.load(Ordering::Relaxed));
        let other = compute_data_hash(b"other");
        plans.note_size(&other, MIN_FILE);
        for _ in 0..3 {
            assert!(plans.derive(&other, FileRange::new(0, 100)).is_none());
        }
        settle(&plans).await;
        assert_eq!(source.layouts.load(Ordering::Relaxed), 1);
    }
}
