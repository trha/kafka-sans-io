//! The recovery job: find, per part, the position header of the last
//! committed record.
//!
//! The caller's contract: every record in every topic carries the header.
//! So the last record of a part holds the result, and one Fetch per part
//! reads it. `ListEnd` reads the end offset of every part, `ListFloor` the
//! first offset at or after the floor timestamp. A part with a record at or
//! after the floor goes to `Fetch` at `end - 1`.
//!
//! Record batches in a Fetch reply are read in place with `batch`, in place
//! of `kafka_protocol::records::RecordBatchDecoder`. The decoder stops with
//! an error on the cut-short last batch a Fetch may hold, and it allocates
//! every record with a map of its headers. The recovery needs only the
//! offset and one header.

use heapless::Vec as ArrayVec;

use crate::{
    batch::{self, RecordView},
    conn::{FetchLimits, FetchPart, PartOffset, Reply},
    error::{Fatal, RETRIABLE},
    frame::RxBuf,
    limits::MAX_PARTS,
    time::Instant,
};

pub struct Recovery {
    config: RecoveryConfig,
    phase: Phase,
    /// The request of this round is out and its reply not yet read.
    waiting: bool,
    parts: ArrayVec<PartRead, MAX_PARTS>,
}

impl Recovery {
    /// The parts come from `RecoveryIn::partitions` on the first `Up`.
    pub fn new(config: RecoveryConfig) -> Self {
        assert!(
            config.fetch_max_bytes > 0,
            "fetch_max_bytes must be above zero"
        );
        Recovery {
            config,
            phase: Phase::Waiting,
            waiting: false,
            parts: ArrayVec::new(),
        }
    }

    pub fn step(&mut self, input: RecoveryIn<'_>, out: &mut RecoveryOut) -> Result<(), Fatal> {
        *out = RecoveryOut::default();
        if self.phase == Phase::Waiting
            && let Some(partitions) = input.partitions
        {
            self.parts.clear();
            for _ in 0..partitions
                .iter()
                .map(|&count| usize::from(count))
                .sum::<usize>()
            {
                if self.parts.push(PartRead::Unknown).is_err() {
                    unreachable!("the conn checks the counts against MAX_PARTS");
                }
            }
            self.phase = Phase::ListEnd;
        }
        for reply in input.replies {
            // The caller passes only replies to this job's requests.
            assert!(self.waiting, "a reply with no recovery request out");
            self.waiting = false;
            let read = match (self.phase, reply) {
                (Phase::ListEnd, Reply::ListOffsets { offsets }) => self.list_end(offsets)?,
                (Phase::ListFloor, Reply::ListOffsets { offsets }) => self.list_floor(offsets)?,
                (Phase::Fetch, Reply::Fetch { code, parts }) => {
                    self.fetch(*code, parts, input.rx)?
                }
                (phase, reply) => unreachable!("reply {reply:?} in phase {phase:?}"),
            };
            if !read {
                out.close = true;
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
            Phase::Waiting => return Ok(()),
            Phase::ListEnd => out.list = Some(LATEST),
            Phase::ListFloor => out.list = Some(self.config.floor_timestamp_ms),
            Phase::Fetch => out.fetch = true,
            Phase::Done => return Ok(()),
        }
        self.waiting = true;
        Ok(())
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Per part, the last record at or after the floor, or `None` when there
    /// is none. Only when `done()`.
    pub fn results(&self) -> impl Clone + Iterator<Item = Option<Found>> {
        assert!(self.done(), "results before Done");
        self.parts.iter().map(|read| match *read {
            PartRead::Found(found) => Some(found),
            PartRead::Empty => None,
            PartRead::Unknown | PartRead::Listed { .. } | PartRead::Fetching { .. } => {
                unreachable!("every part is read before Done")
            }
        })
    }

    /// The limits of the recovery Fetch: `fetch_max_bytes` per part, and no
    /// wait.
    pub fn limits(&self) -> FetchLimits {
        FetchLimits {
            partition_max_bytes: self.config.fetch_max_bytes,
            max_wait_ms: 0,
            min_bytes: 1,
        }
    }

    /// (part, fetch offset) of every part to fetch: the next Fetch.
    pub fn fetches(&self) -> impl Clone + Iterator<Item = (usize, i64)> {
        self.parts
            .iter()
            .enumerate()
            .filter_map(|(part, read)| match *read {
                PartRead::Fetching { last } => Some((part, last)),
                _ => None,
            })
    }

    fn list_end(&mut self, offsets: &[PartOffset]) -> Result<bool, Fatal> {
        if !self.list_codes(offsets)? {
            return Ok(false);
        }
        for offset in offsets {
            if offset.offset < 0 {
                return Err(Fatal::Protocol);
            }
            self.parts[offset.part] = PartRead::Listed { end: offset.offset };
        }
        self.phase = Phase::ListFloor;
        Ok(true)
    }

    fn list_floor(&mut self, offsets: &[PartOffset]) -> Result<bool, Fatal> {
        if !self.list_codes(offsets)? {
            return Ok(false);
        }
        for offset in offsets {
            let PartRead::Listed { end } = self.parts[offset.part] else {
                return Err(Fatal::Protocol);
            };
            // -1: no record at or after the timestamp. An offset at or above
            // `end` is a record appended after `ListEnd`.
            self.parts[offset.part] = if (0..end).contains(&offset.offset) {
                PartRead::Fetching { last: end - 1 }
            } else {
                PartRead::Empty
            };
        }
        self.phase = Phase::Fetch;
        self.finish();
        Ok(true)
    }

    fn fetch(&mut self, code: i16, parts: &[FetchPart], rx: &RxBuf) -> Result<bool, Fatal> {
        if parts.len() != self.fetches().count() {
            return Err(Fatal::Protocol);
        }
        let codes = core::iter::once(code).chain(parts.iter().map(|fetched| fetched.code));
        if !read_codes(codes)? {
            return Ok(false);
        }
        let key = self.config.header_key.as_bytes();
        for fetched in parts {
            // Also catches a part twice in one reply.
            let PartRead::Fetching { last } = self.parts[fetched.part] else {
                return Err(Fatal::Protocol);
            };
            let found =
                last_record(rx.slice(fetched.records), key, last).map_err(|_| Fatal::Fetch)?;
            self.parts[fetched.part] = PartRead::Found(found);
        }
        self.finish();
        Ok(true)
    }

    /// Checks that `offsets` answers every part once. Returns false on a
    /// retriable code.
    fn list_codes(&self, offsets: &[PartOffset]) -> Result<bool, Fatal> {
        if offsets.len() != self.parts.len() {
            return Err(Fatal::Protocol);
        }
        read_codes(offsets.iter().map(|offset| offset.code))
    }

    /// `Done` when no part waits for its Fetch.
    fn finish(&mut self) {
        if self.fetches().next().is_none() {
            self.phase = Phase::Done;
        }
    }
}

pub struct RecoveryConfig {
    pub header_key: &'static str,
    /// No record older than this is read (fleeb-kafka: start of the day).
    pub floor_timestamp_ms: i64,
    /// Fetch bytes per part. The batch that holds a part's last record must
    /// come whole, so this must be at least the largest batch in the topics.
    /// Above zero.
    pub fetch_max_bytes: i32,
}

pub struct RecoveryIn<'a> {
    pub now: Instant,
    /// `Conn::room()`: requests the conn takes now.
    pub room: usize,
    /// `Conn::partitions()`: the count of each topic, once known.
    pub partitions: Option<&'a [u16]>,
    pub replies: &'a [Reply],
    pub lost: bool,
    /// The receive buffer the conn read the replies from. Fetch records are
    /// read in place there.
    pub rx: &'a RxBuf,
}

#[derive(Default)]
pub struct RecoveryOut {
    /// Pass the timestamp to `Conn::send_list_offsets`.
    pub list: Option<i64>,
    /// Pass `limits()` and `fetches()` to `Conn::send_fetch`.
    pub fetch: bool,
    /// A retriable error. Close the conn; the round goes again on the next
    /// connection.
    pub close: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The conn has not been `Up` yet, so the part count is unknown.
    Waiting,
    /// ListOffsets with timestamp -1 (end) for every part.
    ListEnd,
    /// ListOffsets with `floor_timestamp_ms` for every part.
    ListFloor,
    /// One Fetch of the last record of every part with a record at or after
    /// the floor.
    Fetch,
    Done,
}

/// The last record of a part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Found {
    pub offset: i64,
    /// The header value has a null value.
    null: bool,
    len: u8,
    bytes: [u8; MAX_HEADER_VALUE],
}

impl Found {
    /// The header value, `None` for a null value.
    pub fn value(&self) -> Option<&[u8]> {
        (!self.null).then(|| &self.bytes[..usize::from(self.len)])
    }
}

/// Bytes of a header value the recovery keeps. A position fits: fleeb-kafka
/// writes a `u64`. A longer value with the header key is `Fatal::Fetch`.
pub const MAX_HEADER_VALUE: usize = 32;

#[derive(Clone, Copy, Debug)]
enum PartRead {
    Unknown,
    /// The end offset from `ListEnd`.
    Listed {
        end: i64,
    },
    /// Fetch the record at `last`, the part's last offset.
    Fetching {
        last: i64,
    },
    Found(Found),
    /// No record in [floor, end).
    Empty,
}

/// Ok(true) when every code is 0, Ok(false) when one is retriable and the
/// rest are 0 or retriable. Any other code is `Fatal::Fetch`.
fn read_codes(codes: impl Iterator<Item = i16>) -> Result<bool, Fatal> {
    let mut clean = true;
    for code in codes {
        match code {
            0 => {}
            code if RETRIABLE.contains(&code) => clean = false,
            _ => return Err(Fatal::Fetch),
        }
    }
    Ok(clean)
}

/// The header of the record at offset `last` in the records of a Fetch at
/// `last`. The caller reports every error as `Fatal::Fetch`. The errors:
///
/// - No whole batch holds `last`. `ListEnd` read the end, and a Fetch at an
///   offset below the high watermark returns the batch that holds it, so
///   the reply is empty or cut short only when that batch is larger than
///   `fetch_max_bytes`. A high watermark that moved back would look the
///   same. Both need a person, so neither is retried; a restart of the
///   process retries.
/// - The batch is a control batch. The producer is not transactional, so
///   its topics hold none.
/// - The record has no header with the key, which breaks the caller's
///   contract.
/// - The batch does not read: compressed, corrupt, or a header value above
///   `MAX_HEADER_VALUE`.
fn last_record(records: &[u8], key: &[u8], last: i64) -> Result<Found, Fatal> {
    let mut rest = records;
    while let Some(split) = batch::split_batch(rest)? {
        rest = split.rest;
        let view = batch::read_batch(split.batch)?;
        if view.last_offset < last {
            continue;
        }
        if view.control {
            return Err(Fatal::Fetch);
        }
        for record in view.records {
            let record = record?;
            if record.offset == last {
                return last_header(record, key)?.ok_or(Fatal::Fetch);
            }
        }
        return Err(Fatal::Fetch);
    }
    Err(Fatal::Fetch)
}

/// The value of the record's last header with `key`.
fn last_header(record: RecordView<'_>, key: &[u8]) -> Result<Option<Found>, Fatal> {
    let mut found = None;
    for header in record.headers {
        let header = header?;
        if header.key != key {
            continue;
        }
        let mut kept = Found {
            offset: record.offset,
            null: header.value.is_none(),
            len: 0,
            bytes: [0; MAX_HEADER_VALUE],
        };
        if let Some(value) = header.value {
            if value.len() > MAX_HEADER_VALUE {
                return Err(Fatal::Fetch);
            }
            kept.bytes[..value.len()].copy_from_slice(value);
            kept.len = value.len() as u8;
        }
        found = Some(kept);
    }
    Ok(found)
}

/// ListOffsets timestamp that asks for the end offset.
const LATEST: i64 = -1;
