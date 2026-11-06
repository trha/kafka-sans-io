//! The consumer job: start each selected part at a timestamp or a few
//! records before the end, optionally stop at a second timestamp, long-poll
//! Fetch, and report when each part is caught up. See `plans/consumer.md`.
//!
//! `Waiting -> ListEnd -> ListStart -> ListStop -> Fetch`, with
//! `ListEarliest` after an `OFFSET_OUT_OF_RANGE` and `Done` once every
//! selected part reached its stop. One request is out at a time. The
//! offsets move only on a reply, so a lost request goes again with the same
//! offsets, and nothing is delivered twice.
//!
//! Fetch replies are read in place. Each part's new records come out as a
//! `Delivery`, a span of the receive buffer that `delivered` reads.

use core::mem::size_of;

use heapless::Vec as ArrayVec;
use zerocopy::FromBytes;

use crate::{
    batch::{self, BatchHeader, RecordView, Records},
    conn::{Conn, FetchLimits, FetchPart, PartOffset, Reply},
    error::{Fatal, RETRIABLE},
    frame::{RxBuf, RxSpan},
    limits::MAX_PARTS,
    time::Instant,
    wire::{FetchHead, FetchPartitionHead},
};

pub struct Consumer {
    config: ConsumerConfig,
    phase: Phase,
    /// The request of this round is out and its reply not yet read.
    waiting: bool,
    parts: ArrayVec<Part, MAX_PARTS>,
}

impl Consumer {
    pub fn new(config: ConsumerConfig) -> Self {
        assert!(
            config.limits.partition_max_bytes > 0,
            "partition_max_bytes must be above zero"
        );
        Consumer {
            config,
            phase: Phase::Waiting,
            waiting: false,
            parts: ArrayVec::new(),
        }
    }

    pub fn step(&mut self, input: ConsumerIn<'_>, out: &mut ConsumerOut) -> Result<(), Fatal> {
        out.list = None;
        out.fetch = false;
        out.close = false;
        out.deliveries.clear();
        if self.phase == Phase::Waiting
            && let Some(partitions) = input.partitions
        {
            let selected = match self.config.select {
                Select::All => None,
                Select::One { topic, partition } => {
                    if partitions
                        .get(topic)
                        .is_none_or(|&count| partition >= count)
                    {
                        return Err(Fatal::Metadata);
                    }
                    Some(
                        partitions[..topic]
                            .iter()
                            .map(|&count| usize::from(count))
                            .sum::<usize>()
                            + usize::from(partition),
                    )
                }
            };
            self.parts.clear();
            for index in 0..partitions.iter().map(|&count| usize::from(count)).sum() {
                if self
                    .parts
                    .push(Part {
                        selected: selected.is_none_or(|selected| selected == index),
                        end: 0,
                        next: 0,
                        stop: i64::MAX,
                        high_watermark: -1,
                        out_of_range: false,
                        starved: false,
                    })
                    .is_err()
                {
                    unreachable!("the conn checks the counts against MAX_PARTS");
                }
            }
            self.phase = Phase::ListEnd;
        }

        if let Some(reply) = match input.replies {
            [] => None,
            [reply] => {
                // The caller passes only replies to this job's requests.
                assert!(self.waiting, "a reply with no consumer request out");
                self.waiting = false;
                Some(reply)
            }
            _ => unreachable!("one consumer request is out at a time"),
        } {
            match (self.phase, reply) {
                (Phase::ListEnd, Reply::ListOffsets { offsets }) => {
                    if !self.list_codes(offsets)? {
                        out.close = true;
                        return Ok(());
                    }
                    for offset in offsets {
                        let part = &mut self.parts[offset.part];
                        if !part.selected {
                            continue;
                        }
                        if offset.offset < 0 {
                            return Err(Fatal::Protocol);
                        }
                        part.end = offset.offset;
                        if let Start::Tail(count) = self.config.start {
                            part.next = (offset.offset - i64::from(count)).max(0);
                        }
                    }
                    self.phase = match (self.config.start, self.config.stop_timestamp_ms) {
                        (Start::Timestamp(_), _) => Phase::ListStart,
                        (Start::Tail(_), Some(_)) => Phase::ListStop,
                        (Start::Tail(_), None) => Phase::Fetch,
                    };
                }
                (Phase::ListStart, Reply::ListOffsets { offsets }) => {
                    if !self.list_codes(offsets)? {
                        out.close = true;
                        return Ok(());
                    }
                    for offset in offsets {
                        let part = &mut self.parts[offset.part];
                        if !part.selected {
                            continue;
                        }
                        part.next = if offset.offset < 0 {
                            part.end
                        } else {
                            offset.offset
                        };
                    }
                    self.phase = match self.config.stop_timestamp_ms {
                        Some(_) => Phase::ListStop,
                        None => Phase::Fetch,
                    };
                }
                (Phase::ListStop, Reply::ListOffsets { offsets }) => {
                    if !self.list_codes(offsets)? {
                        out.close = true;
                        return Ok(());
                    }
                    for offset in offsets {
                        let part = &mut self.parts[offset.part];
                        if !part.selected {
                            continue;
                        }
                        part.stop = if offset.offset < 0 {
                            part.end
                        } else {
                            offset.offset
                        };
                    }
                    self.phase = Phase::Fetch;
                }
                (Phase::Fetch, Reply::Fetch { code, parts }) => {
                    match *code {
                        0 => {}
                        code if RETRIABLE.contains(&code) => {
                            out.close = true;
                            return Ok(());
                        }
                        _ => return Err(Fatal::Fetch),
                    }
                    if parts.len() != self.fetches().count() {
                        return Err(Fatal::Protocol);
                    }
                    for fetched in parts {
                        let FetchPart {
                            part: index,
                            code,
                            high_watermark,
                            log_start_offset: _,
                            records,
                        } = *fetched;
                        let part = &mut self.parts[index];
                        if !part.selected || part.next >= part.stop {
                            return Err(Fatal::Protocol);
                        }
                        match code {
                            0 => {}
                            OFFSET_OUT_OF_RANGE => {
                                part.out_of_range = true;
                                continue;
                            }
                            code if RETRIABLE.contains(&code) => {
                                out.close = true;
                                continue;
                            }
                            _ => return Err(Fatal::Fetch),
                        }
                        part.high_watermark = high_watermark;
                        // A code-0 reply ends the starve, unless the branch
                        // below starts a new one. A starved part can come
                        // back with nothing to read: `ListEarliest` moved it
                        // to a log start at its end.
                        part.starved = false;
                        // Whole batches from the first that reaches `next`.
                        let blob = input.rx.slice(records);
                        let from = part.next;
                        let mut first = None;
                        let mut at = 0;
                        while let Some(split) =
                            batch::split_batch(&blob[at..]).map_err(|_| Fatal::Fetch)?
                        {
                            let (header, _) = BatchHeader::ref_from_prefix(split.batch)
                                .expect("a whole batch holds its header");
                            let last = header
                                .base_offset
                                .get()
                                .checked_add(i64::from(header.last_offset_delta.get()))
                                .ok_or(Fatal::Fetch)?;
                            let start = at;
                            at += split.batch.len();
                            if last < part.next {
                                continue;
                            }
                            first.get_or_insert(start);
                            part.next = last + 1;
                        }
                        if let Some(first) = first {
                            let until = part.next.min(part.stop);
                            if until > from
                                && out
                                    .deliveries
                                    .push(Delivery {
                                        part: index,
                                        records: records.subspan(first..at),
                                        from,
                                        until,
                                    })
                                    .is_err()
                            {
                                unreachable!("one delivery per part");
                            }
                        } else if part.next < high_watermark {
                            // Records wait below the high watermark, but no
                            // whole batch reaching `part.next` came: only
                            // older batches, a cut-short one, or none at
                            // all. With other parts, the reply budget ran
                            // out first: ask for this part alone. Alone, the
                            // broker sends the first batch whole even past
                            // both byte limits, so a lone reply without one
                            // breaks that rule, and asking again would only
                            // repeat it. (A corrupt batch already failed in
                            // `split_batch`.)
                            if parts.len() == 1 {
                                return Err(Fatal::Fetch);
                            }
                            part.starved = true;
                        }
                    }
                    if self.parts.iter().any(|part| part.out_of_range) {
                        self.phase = Phase::ListEarliest;
                    }
                }
                (Phase::ListEarliest, Reply::ListOffsets { offsets }) => {
                    if !self.list_codes(offsets)? {
                        out.close = true;
                        return Ok(());
                    }
                    for offset in offsets {
                        let part = &mut self.parts[offset.part];
                        if !part.out_of_range {
                            continue;
                        }
                        if offset.offset < 0 {
                            return Err(Fatal::Protocol);
                        }
                        // Retention dropped records, or `Tail` started too
                        // far back. Else `next` is above the end.
                        if part.next >= offset.offset {
                            return Err(Fatal::Fetch);
                        }
                        part.next = offset.offset;
                        part.out_of_range = false;
                    }
                    self.phase = Phase::Fetch;
                }
                (phase, reply) => unreachable!("reply {reply:?} in phase {phase:?}"),
            }
            if self.phase == Phase::Fetch && self.fetches().next().is_none() {
                self.phase = Phase::Done;
            }
            // A retriable code in a Fetch: the deliveries before it stay,
            // and the round goes again on the next connection.
            if out.close {
                return Ok(());
            }
        }

        if input.lost {
            self.waiting = false;
        }
        if self.waiting || input.room == 0 {
            return Ok(());
        }
        match self.phase {
            Phase::Waiting | Phase::Done => return Ok(()),
            Phase::ListEnd => out.list = Some(LATEST),
            Phase::ListStart => {
                let Start::Timestamp(timestamp) = self.config.start else {
                    unreachable!("only a timestamp start lists the start");
                };
                out.list = Some(timestamp);
            }
            Phase::ListStop => {
                out.list = Some(
                    self.config
                        .stop_timestamp_ms
                        .expect("only a stop timestamp lists the stop"),
                );
            }
            Phase::Fetch => out.fetch = true,
            Phase::ListEarliest => out.list = Some(EARLIEST),
        }
        self.waiting = true;
        Ok(())
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn limits(&self) -> FetchLimits {
        self.config.limits
    }

    /// (part, next) of the next Fetch, in part order: the first starved
    /// part alone, else every selected part below its stop.
    pub fn fetches(&self) -> impl Clone + Iterator<Item = (usize, i64)> {
        let starved = self
            .parts
            .iter()
            .position(|part| part.selected && part.next < part.stop && part.starved);
        self.parts
            .iter()
            .enumerate()
            .filter(move |&(index, part)| {
                part.selected
                    && part.next < part.stop
                    && starved.is_none_or(|starved| starved == index)
            })
            .map(|(index, part)| (index, part.next))
    }

    /// The part's next offset is at its last high watermark.
    pub fn caught_up(&self, part: usize) -> bool {
        let part = &self.parts[part];
        part.next == part.high_watermark
    }

    /// The part reached its stop.
    pub fn done(&self, part: usize) -> bool {
        let part = &self.parts[part];
        part.next >= part.stop
    }

    /// The next offset to deliver of `part`.
    pub fn next(&self, part: usize) -> i64 {
        self.parts[part].next
    }

    /// The selected parts.
    pub fn selected(&self) -> impl Clone + Iterator<Item = usize> {
        self.parts
            .iter()
            .enumerate()
            .filter(|(_, part)| part.selected)
            .map(|(index, _)| index)
    }

    /// Checks that `offsets` answers every part once. Returns false on a
    /// retriable code of a selected part. Other parts' codes are not read.
    fn list_codes(&self, offsets: &[PartOffset]) -> Result<bool, Fatal> {
        if offsets.len() != self.parts.len() {
            return Err(Fatal::Protocol);
        }
        let mut clean = true;
        for offset in offsets {
            if !self.parts[offset.part].selected {
                continue;
            }
            match offset.code {
                0 => {}
                code if RETRIABLE.contains(&code) => clean = false,
                _ => return Err(Fatal::Fetch),
            }
        }
        Ok(clean)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsumerConfig {
    pub select: Select,
    pub start: Start,
    /// Each part stops before the first offset with a timestamp at or above
    /// this. When no record has one: at the end found by `ListEnd`.
    pub stop_timestamp_ms: Option<i64>,
    pub limits: FetchLimits,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Start {
    /// The first offset with a timestamp at or above this, in ms. When no
    /// record has one: the end.
    Timestamp(i64),
    /// `count` records before the end, or the log start when it has fewer.
    Tail(u32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Select {
    All,
    One { topic: usize, partition: u16 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The conn has not been `Up` yet, so the part count is unknown.
    Waiting,
    ListEnd,
    ListStart,
    ListStop,
    Fetch,
    /// After `OFFSET_OUT_OF_RANGE`: ListOffsets -2, then back to `Fetch`.
    ListEarliest,
    /// Every selected part reached its stop.
    Done,
}

pub struct ConsumerIn<'a> {
    pub now: Instant,
    /// `Conn::room()`.
    pub room: usize,
    /// `Conn::partitions()`: the count of each topic, once known.
    pub partitions: Option<&'a [u16]>,
    pub replies: &'a [Reply],
    pub lost: bool,
    pub rx: &'a RxBuf,
}

#[derive(Default)]
pub struct ConsumerOut {
    /// Pass the timestamp to `Conn::send_list_offsets`.
    pub list: Option<i64>,
    /// Pass `limits()` and `fetches()` to `Conn::send_fetch`.
    pub fetch: bool,
    /// A retriable error: close the conn, and the round goes again.
    pub close: bool,
    pub deliveries: ArrayVec<Delivery, MAX_PARTS>,
}

/// New records of one part, in the receive buffer. Pass
/// `rx.slice(records)`, or a copy of it, to `delivered` before the next
/// `RxBuf::spare` ends the span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub part: usize,
    /// Whole batches, from the one that holds `from`.
    pub records: RxSpan,
    /// Offsets in `from..until`, without control records.
    pub from: i64,
    pub until: i64,
}

/// The records of a delivery in offset order. `records` holds the bytes of
/// `Delivery::records`: the receive buffer's own (`RxBuf::slice`), or a
/// copy that outlives the next read. `from` and `until` are the delivery's.
/// Batches are read with `batch::read_batch` (CRC, no compression), control
/// batches skipped, records outside `from..until` skipped. A batch that does
/// not read is `Fatal::Fetch`, and the iterator ends after it.
pub fn delivered(
    records: &[u8],
    from: i64,
    until: i64,
) -> impl Iterator<Item = Result<RecordView<'_>, Fatal>> {
    Delivered {
        rest: records,
        records: None,
        from,
        until,
    }
}

/// Bytes of the largest Fetch reply frame for `parts` parts of `conn`,
/// length field included. Size the `RxBuf` with it.
///
/// Layout: length i32, correlation id i32, head, topics array of (name
/// string, partitions array of (partition head, aborted transactions
/// array, preferred read replica i32, records bytes)). Every topic of the
/// conn may be in the reply. A reply at READ_UNCOMMITTED has no aborted
/// transaction.
///
/// Records: a part may hold the batch that crosses `partition_max_bytes`
/// whole, so less than twice the limit when every batch is at most the
/// limit. The broker keeps the whole reply within `max_bytes` (the limit
/// times the parts) except for the first part with records. So the records
/// of one reply are at most the limit times `parts + 1`.
pub fn fetch_reply_bytes(conn: &Conn, parts: usize, limits: FetchLimits) -> usize {
    const LENGTH: usize = 4;
    const CORRELATION: usize = 4;
    const ARRAY: usize = 4;
    const STRING: usize = 2;
    const PREFERRED: usize = 4;
    const BYTES: usize = 4;
    LENGTH
        + CORRELATION
        + size_of::<FetchHead>()
        + ARRAY
        + conn
            .config()
            .topics
            .iter()
            .map(|topic| STRING + topic.len() + ARRAY)
            .sum::<usize>()
        + parts * (size_of::<FetchPartitionHead>() + ARRAY + PREFERRED + BYTES)
        + (parts + 1) * usize::try_from(limits.partition_max_bytes).expect("a positive byte limit")
}

#[derive(Clone, Copy, Debug)]
struct Part {
    selected: bool,
    /// The end from `ListEnd`.
    end: i64,
    /// The next offset to deliver.
    next: i64,
    /// Offsets at or above this are never delivered. `i64::MAX`: none.
    stop: i64,
    /// From this part's last Fetch reply. -1 before the first.
    high_watermark: i64,
    /// Got `OFFSET_OUT_OF_RANGE` on the last Fetch.
    out_of_range: bool,
    /// The parts before this one used up the last Fetch's reply budget, so
    /// the next Fetch asks for this part alone. Alone, a part always gets its
    /// next batch, so only a part asked alone that still gets nothing is
    /// `Fatal::Fetch`.
    ///
    /// Set when the last Fetch held other parts too, and this part got no
    /// whole batch reaching `next` while records waited below its high
    /// watermark.
    ///
    /// Why this happens: Redpanda 25.3 fills a Fetch reply as follows
    /// (measured, see `plans/consumer.md` "Facts").
    ///
    /// - Per part, it sends whole batches from the one holding the fetch
    ///   offset while the part's bytes are below `partition_max_bytes`. The
    ///   batch that crosses the limit comes whole, and nothing follows it. So
    ///   a part can go over its limit by up to one batch (41 114 bytes
    ///   against 32 768). It never cut a batch short.
    /// - Across parts, in request order, a part whose bytes would push the
    ///   reply past `max_bytes` gets nothing at all. It is not trimmed to the
    ///   budget left: in one reply the third part got 0 bytes with 32 953
    ///   bytes still free. `max_bytes` is `partition_max_bytes` times the
    ///   part count, so the overshoot of the parts before it can use the
    ///   budget up.
    /// - The first part with records always gets its batches, even past
    ///   `max_bytes`. A part fetched alone got 41 114 bytes against a
    ///   `max_bytes` of 32 768.
    starved: bool,
}

struct Delivered<'a> {
    /// Batches not yet read.
    rest: &'a [u8],
    /// Records of the batch being read.
    records: Option<Records<'a>>,
    from: i64,
    until: i64,
}

impl<'a> Iterator for Delivered<'a> {
    type Item = Result<RecordView<'a>, Fatal>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(records) = &mut self.records {
                match records.next() {
                    Some(Ok(record)) if record.offset < self.from => continue,
                    Some(Ok(record)) if record.offset >= self.until => {
                        // Offsets rise in a batch and across batches.
                        self.records = None;
                        self.rest = &[];
                        return None;
                    }
                    Some(Ok(record)) => return Some(Ok(record)),
                    Some(Err(_)) => {
                        self.records = None;
                        self.rest = &[];
                        return Some(Err(Fatal::Fetch));
                    }
                    None => self.records = None,
                }
            }
            match batch::split_batch(self.rest).and_then(|split| {
                let Some(split) = split else {
                    return Ok(None);
                };
                self.rest = split.rest;
                batch::read_batch(split.batch).map(Some)
            }) {
                Ok(None) => return None,
                Ok(Some(view)) => {
                    if !view.control && view.last_offset >= self.from {
                        self.records = Some(view.records);
                    }
                }
                Err(_) => {
                    self.rest = &[];
                    return Some(Err(Fatal::Fetch));
                }
            }
        }
    }
}

/// ListOffsets timestamps with a meaning of their own.
const LATEST: i64 = -1;

const EARLIEST: i64 = -2;

const OFFSET_OUT_OF_RANGE: i16 = 1;
