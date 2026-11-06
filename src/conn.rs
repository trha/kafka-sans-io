//! The connection to the broker: connect, handshake, requests in flight,
//! retry.
//!
//! `Down -> Connecting -> Versions -> Metadata -> Up`. In `Up` the job sends
//! up to `max_in_flight` requests. The broker handles the requests of one
//! connection in order and replies in that order, so each reply matches the
//! oldest request in flight.

use core::mem;

use heapless::{Deque, Vec as ArrayVec};

use crate::{
    batch::ProducerId,
    error::Fatal,
    frame::{Frame, RxBuf, RxSpan, TxBuf},
    limits::{MAX_IN_FLIGHT, MAX_PARTS},
    time::{Duration, Instant},
    wire::{self, Api, FetchTarget, MetadataItem, ProduceTarget, Writer},
};

pub use crate::wire::FetchLimits;

pub struct Conn {
    config: Config,
    /// Partition count of each topic, from the first `Up` on.
    partitions: Box<[u16]>,
    /// First part of each topic, from the first `Up` on.
    first_part: Box<[usize]>,
    /// The first Metadata reply filled `partitions` and `first_part`.
    learned: bool,
    state: ConnState,
    correlation: i32,
    backoff: Duration,
    tx_capacity: usize,
}

impl Conn {
    pub fn new(config: Config) -> Self {
        assert!(
            (1..=MAX_IN_FLIGHT).contains(&usize::from(config.max_in_flight)),
            "max_in_flight out of range"
        );
        assert!(
            !config.backoff_min.is_zero(),
            "a zero backoff would close and connect in one step"
        );
        assert!(
            (1..=MAX_PARTS).contains(&config.topics.len()),
            "topic count out of range"
        );
        assert!(
            config.produce_parts <= MAX_PARTS,
            "produce_parts above MAX_PARTS"
        );
        Conn {
            // Fields run in order: this block reads `config` before it moves.
            tx_capacity: {
                let client_id = config.client_id;
                let topics = &config.topics;
                let handshake =
                    request_bytes(client_id, Api::ApiVersions, wire::api_versions_request)
                        + request_bytes(client_id, Api::Metadata, |writer| {
                            wire::metadata_request(writer, topic_names(topics));
                        });
                let init = request_bytes(
                    client_id,
                    Api::InitProducerId,
                    wire::init_producer_id_request,
                );
                // The counts are not known yet, so each part gets its own
                // topic entry with the longest name: two names of that
                // length take turns, so no two neighbours share an entry.
                let longest = topics.iter().map(String::len).max().unwrap_or(0).max(1);
                let names = ["a".repeat(longest), "b".repeat(longest)];
                let name = |part: usize| names[part % 2].as_str();
                let produce = {
                    let batch = vec![0; config.max_batch_bytes];
                    request_bytes(client_id, Api::Produce, |writer| {
                        wire::produce_request(
                            writer,
                            0,
                            (0..config.produce_parts).map(|part| ProduceTarget {
                                topic: name(part),
                                partition: 0,
                                batch: &batch[..],
                            }),
                        );
                    })
                };
                let list = request_bytes(client_id, Api::ListOffsets, |writer| {
                    wire::list_offsets_request(
                        writer,
                        0,
                        (0..MAX_PARTS).map(|part| (name(part), 0)),
                    );
                });
                let fetch = request_bytes(client_id, Api::Fetch, |writer| {
                    wire::fetch_request(
                        writer,
                        FetchLimits {
                            partition_max_bytes: 0,
                            max_wait_ms: 0,
                            min_bytes: 0,
                        },
                        (0..MAX_PARTS).map(|part| FetchTarget {
                            topic: name(part),
                            partition: 0,
                            fetch_offset: 0,
                        }),
                    );
                });
                let largest = init.max(produce).max(list).max(fetch);
                handshake + usize::from(config.max_in_flight) * largest
            },
            partitions: vec![0; config.topics.len()].into_boxed_slice(),
            first_part: vec![0; config.topics.len()].into_boxed_slice(),
            learned: false,
            state: ConnState::Down {
                retry_at: Instant(0),
            },
            correlation: 0,
            backoff: config.backoff_min,
            config,
        }
    }

    pub fn step(&mut self, input: ConnIn<'_>, out: &mut ConnOut) -> Result<(), Fatal> {
        out.cmd = None;
        out.replies.clear();
        out.lost = false;
        out.up = false;
        out.down = false;
        let now = input.now;
        let timeout = now.plus(self.config.request_timeout);
        // A close of a connection this step started with. A close seen while
        // Down is an old event.
        let closed = matches!(input.net, Some(NetEvent::ConnectFailed | NetEvent::Closed))
            && !matches!(self.state, ConnState::Down { .. });

        self.state = match mem::replace(&mut self.state, ConnState::Down { retry_at: now }) {
            ConnState::Down { retry_at } if now >= retry_at => {
                out.cmd = Some(NetCmd::Connect);
                ConnState::Connecting { deadline: timeout }
            }
            ConnState::Connecting { .. } if input.net == Some(NetEvent::Connected) => {
                input.rx.clear();
                let correlation = self.next_correlation();
                push(
                    &mut out.tx,
                    self.config.client_id,
                    Api::ApiVersions,
                    correlation,
                    wire::api_versions_request,
                );
                ConnState::Versions {
                    correlation,
                    deadline: timeout,
                }
            }
            ConnState::Versions {
                correlation,
                deadline,
            } => match input.rx.next_frame()? {
                Some(frame) => {
                    versions(reply_body(frame.body, Api::ApiVersions, correlation)?)?;
                    let correlation = self.next_correlation();
                    push(
                        &mut out.tx,
                        self.config.client_id,
                        Api::Metadata,
                        correlation,
                        |writer| wire::metadata_request(writer, topic_names(&self.config.topics)),
                    );
                    ConnState::Metadata {
                        correlation,
                        deadline: timeout,
                    }
                }
                None => ConnState::Versions {
                    correlation,
                    deadline,
                },
            },
            ConnState::Metadata {
                correlation,
                deadline,
            } => match input.rx.next_frame()? {
                Some(frame) => {
                    let body = reply_body(frame.body, Api::Metadata, correlation)?;
                    if metadata(
                        &self.config.topics,
                        self.learned,
                        &mut self.partitions,
                        body,
                    )? {
                        if !self.learned {
                            first_parts(&self.partitions, &mut self.first_part);
                            self.learned = true;
                        }
                        self.backoff = self.config.backoff_min;
                        out.up = true;
                        ConnState::Up {
                            in_flight: Deque::new(),
                        }
                    } else {
                        // A partition has no leader yet: retry on a new
                        // connection.
                        self.down(now, out)
                    }
                }
                None => ConnState::Metadata {
                    correlation,
                    deadline,
                },
            },
            ConnState::Up { mut in_flight } => {
                while let Some(frame) = input.rx.next_frame()? {
                    let sent = in_flight.pop_front().ok_or(Fatal::Protocol)?;
                    let body = reply_body(frame.body, sent.api, sent.correlation)?;
                    let reply = self.reply(sent.api, &frame, body)?;
                    if out.replies.push(reply).is_err() {
                        unreachable!("replies are bounded by the requests in flight");
                    }
                }
                ConnState::Up { in_flight }
            }
            // Waiting: Down before `retry_at`, Connecting with no answer yet.
            state @ (ConnState::Down { .. } | ConnState::Connecting { .. }) => state,
        };

        // Every connected state goes down on a close or a passed deadline.
        // This runs after the frames, so frames that came before a close
        // are read.
        let deadline = match &self.state {
            ConnState::Down { .. } => None,
            ConnState::Connecting { deadline }
            | ConnState::Versions { deadline, .. }
            | ConnState::Metadata { deadline, .. } => Some(*deadline),
            ConnState::Up { in_flight } => in_flight.front().map(|sent| sent.deadline),
        };
        if (closed && !matches!(self.state, ConnState::Down { .. }))
            || deadline.is_some_and(|deadline| now >= deadline)
        {
            self.state = self.down(now, out);
        }
        Ok(())
    }

    /// Encodes one InitProducerId. Only when `can_send()`.
    pub fn send_init_producer_id(&mut self, now: Instant, out: &mut ConnOut) {
        self.request(
            now,
            Api::InitProducerId,
            |_, writer| wire::init_producer_id_request(writer),
            out,
        );
    }

    /// Encodes one ListOffsets of every part at `timestamp`: -1 asks for the
    /// end, -2 for the log start, and a time in ms for the first offset with
    /// a timestamp at or above it. Only when `can_send()`.
    pub fn send_list_offsets(&mut self, now: Instant, timestamp: i64, out: &mut ConnOut) {
        self.request(
            now,
            Api::ListOffsets,
            |conn, writer| {
                wire::list_offsets_request(
                    writer,
                    timestamp,
                    every_partition(&conn.config.topics, &conn.partitions),
                );
            },
            out,
        );
    }

    /// Encodes one Fetch of `offsets`: (part, fetch offset), in increasing
    /// part order, with `limits`. `partition_max_bytes` bounds each part,
    /// and `max_bytes` is it times the part count. Only when `can_send()`.
    pub fn send_fetch(
        &mut self,
        now: Instant,
        limits: FetchLimits,
        offsets: impl Clone + Iterator<Item = (usize, i64)>,
        out: &mut ConnOut,
    ) {
        self.request(
            now,
            Api::Fetch,
            |conn, writer| {
                let targets = targets(
                    &conn.config.topics,
                    &conn.first_part,
                    offsets.clone(),
                    |topic, partition, fetch_offset| FetchTarget {
                        topic,
                        partition,
                        fetch_offset,
                    },
                );
                wire::fetch_request(writer, limits, targets);
            },
            out,
        );
    }

    /// Encodes one Produce with `batches`: sealed batches by part, in
    /// increasing part order, at most `produce_parts`. Only when
    /// `can_send()`.
    pub fn send_produce<'b>(
        &mut self,
        now: Instant,
        batches: impl Clone + Iterator<Item = (usize, &'b [u8])>,
        out: &mut ConnOut,
    ) {
        assert!(
            batches.clone().count() <= self.config.produce_parts,
            "a Produce above produce_parts"
        );
        let timeout_ms = i32::try_from(self.config.request_timeout.as_millis()).unwrap_or(i32::MAX);
        self.request(
            now,
            Api::Produce,
            |conn, writer| {
                let targets = targets(
                    &conn.config.topics,
                    &conn.first_part,
                    batches.clone(),
                    |topic, partition, batch| ProduceTarget {
                        topic,
                        partition,
                        batch,
                    },
                );
                wire::produce_request(writer, timeout_ms, targets);
            },
            out,
        );
    }

    /// Closes at once. Replies not yet read are dropped. Sets `lost` when
    /// requests were in flight.
    pub fn close(&mut self, now: Instant, out: &mut ConnOut) {
        if !matches!(self.state, ConnState::Down { .. }) {
            self.state = self.down(now, out);
        }
    }

    /// `Up` with room for one more request.
    pub fn can_send(&self) -> bool {
        self.room() > 0
    }

    /// Requests the job may send now: zero unless `Up`.
    pub fn room(&self) -> usize {
        match &self.state {
            ConnState::Up { in_flight } => {
                usize::from(self.config.max_in_flight).saturating_sub(in_flight.len())
            }
            _ => 0,
        }
    }

    /// When the conn next acts with no input: the retry while `Down`, else
    /// the deadline of the handshake step or of the oldest request. A caller
    /// that waits for events steps the conn again by then. `None` when only
    /// an event can move it.
    pub fn deadline(&self) -> Option<Instant> {
        match &self.state {
            ConnState::Down { retry_at } => Some(*retry_at),
            ConnState::Connecting { deadline }
            | ConnState::Versions { deadline, .. }
            | ConnState::Metadata { deadline, .. } => Some(*deadline),
            ConnState::Up { in_flight } => in_flight.front().map(|sent| sent.deadline),
        }
    }

    /// Parts over all topics, from the first `Up` on.
    pub fn parts(&self) -> Option<usize> {
        self.learned.then(|| {
            self.partitions
                .iter()
                .map(|&count| usize::from(count))
                .sum()
        })
    }

    /// The partition count of each topic, in config order, from the first
    /// `Up` on. `None` before it.
    pub fn partitions(&self) -> Option<&[u16]> {
        self.learned.then_some(&self.partitions[..])
    }

    /// The part of `partition` in `topics[topic]`. Panics before the counts
    /// are known or outside them.
    pub fn part(&self, topic: usize, partition: u16) -> usize {
        assert!(self.learned, "part before the first Up");
        assert!(
            partition < self.partitions[topic],
            "partition outside the topic"
        );
        self.first_part[topic] + usize::from(partition)
    }

    /// The topic index and partition of `part`. Panics before the counts
    /// are known or outside them.
    pub fn topic_partition(&self, part: usize) -> (usize, u16) {
        assert!(
            part < self.parts().expect("topic_partition before the first Up"),
            "part outside the topics"
        );
        let topic = self.first_part.partition_point(|&first| first <= part) - 1;
        (topic, (part - self.first_part[topic]) as u16)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Reads the reply `body` of `frame` to a job request of `api`.
    fn reply(&self, api: Api, frame: &Frame<'_>, body: &[u8]) -> Result<Reply, Fatal> {
        match api {
            Api::InitProducerId => {
                let reply = wire::init_producer_id_response(body)?;
                Ok(Reply::InitProducerId {
                    code: reply.error_code.get(),
                    id: ProducerId {
                        id: reply.producer_id.get(),
                        epoch: reply.producer_epoch.get(),
                    },
                })
            }
            Api::Produce => {
                let mut codes = PartCodes::new();
                wire::produce_response(body, |topic, partition| {
                    let part = self.part_of(topic, partition.index.get())?;
                    if codes.iter().any(|seen: &PartCode| seen.part == part) {
                        return Err(Fatal::Protocol);
                    }
                    let code = PartCode {
                        part,
                        code: partition.error_code.get(),
                    };
                    codes.push(code).map_err(|_| Fatal::Protocol)
                })?;
                Ok(Reply::Produce { codes })
            }
            Api::ListOffsets => {
                let mut offsets = ArrayVec::new();
                wire::list_offsets_response(body, |topic, partition| {
                    let part = self.part_of(topic, partition.partition_index.get())?;
                    if offsets.iter().any(|seen: &PartOffset| seen.part == part) {
                        return Err(Fatal::Protocol);
                    }
                    let offset = PartOffset {
                        part,
                        code: partition.error_code.get(),
                        offset: partition.offset.get(),
                    };
                    offsets.push(offset).map_err(|_| Fatal::Protocol)
                })?;
                Ok(Reply::ListOffsets { offsets })
            }
            Api::Fetch => {
                // Where the body starts in the frame.
                let at = frame.body.len() - body.len();
                let mut parts = ArrayVec::new();
                let head = wire::fetch_response(body, |partition| {
                    let part =
                        self.part_of(partition.topic, partition.head.partition_index.get())?;
                    if parts.iter().any(|seen: &FetchPart| seen.part == part) {
                        return Err(Fatal::Protocol);
                    }
                    let records = partition.records.unwrap_or_default();
                    let fetched = FetchPart {
                        part,
                        code: partition.head.error_code.get(),
                        high_watermark: partition.head.high_watermark.get(),
                        log_start_offset: partition.head.log_start_offset.get(),
                        records: frame.span(at + records.start..at + records.end),
                    };
                    parts.push(fetched).map_err(|_| Fatal::Protocol)
                })?;
                Ok(Reply::Fetch {
                    code: head.error_code.get(),
                    parts,
                })
            }
            Api::ApiVersions | Api::Metadata => unreachable!("only job requests are in flight"),
        }
    }

    /// The part of `partition` in topic `name`. A topic or partition outside
    /// the config or the counts is `Fatal::Protocol`.
    fn part_of(&self, name: &[u8], partition: i32) -> Result<usize, Fatal> {
        let index = self
            .config
            .topics
            .iter()
            .position(|config| name == config.as_bytes())
            .ok_or(Fatal::Protocol)?;
        let partition = u16::try_from(partition)
            .ok()
            .filter(|&partition| partition < self.partitions[index])
            .ok_or(Fatal::Protocol)?;
        Ok(self.first_part[index] + usize::from(partition))
    }

    /// The Down state after a close: drops the unsent bytes and doubles the
    /// backoff. Sets `lost` when requests were in flight.
    fn down(&mut self, now: Instant, out: &mut ConnOut) -> ConnState {
        out.lost |= matches!(&self.state, ConnState::Up { in_flight } if !in_flight.is_empty());
        out.cmd = Some(NetCmd::Close);
        out.tx.clear();
        out.down = true;
        let retry_at = now.plus(self.backoff);
        self.backoff = (self.backoff * 2).min(self.config.backoff_max);
        ConnState::Down { retry_at }
    }

    /// Every send: checks the room, takes a correlation id, pushes the frame
    /// of `api` with the body `write` puts, and adds the request in flight.
    /// `write` gets the conn, because it reads the topics and counts while
    /// this holds `&mut self`.
    fn request(
        &mut self,
        now: Instant,
        api: Api,
        write: impl Fn(&Conn, &mut Writer<'_>),
        out: &mut ConnOut,
    ) {
        assert!(self.can_send(), "send without room");
        let correlation = self.next_correlation();
        push(
            &mut out.tx,
            self.config.client_id,
            api,
            correlation,
            |writer| write(self, writer),
        );
        let ConnState::Up { in_flight } = &mut self.state else {
            unreachable!("can_send means Up");
        };
        if in_flight
            .push_back(Sent {
                api,
                correlation,
                deadline: now.plus(self.config.request_timeout),
            })
            .is_err()
        {
            unreachable!("max_in_flight is at most MAX_IN_FLIGHT");
        }
    }

    fn next_correlation(&mut self) -> i32 {
        let correlation = self.correlation;
        self.correlation = self.correlation.wrapping_add(1);
        correlation
    }
}

pub struct Config {
    pub client_id: &'static str,
    /// Topic names. Part numbers count through them in this order once the
    /// counts are known: topic 0 has parts `0..count`, topic 1 the next
    /// ones, and so on. See `Conn::part`.
    pub topics: Box<[String]>,
    pub request_timeout: Duration,
    /// Above zero.
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Requests in flight, 1..=MAX_IN_FLIGHT.
    pub max_in_flight: u8,
    /// Bound of one sealed batch, header included. The accumulator's
    /// `slot_bytes` must not be above it. Sizes the send buffer.
    pub max_batch_bytes: usize,
    /// The most parts one Produce carries: 0 for a consumer, the
    /// accumulator's part count for a producer. Sizes the send buffer.
    pub produce_parts: usize,
}

pub struct ConnIn<'a> {
    pub now: Instant,
    pub net: Option<NetEvent>,
    /// Received bytes. Whole frames are taken off the front.
    pub rx: &'a mut RxBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetEvent {
    Connected,
    ConnectFailed,
    Closed,
}

/// Kept by the caller across steps. `step` clears all but `tx`.
pub struct ConnOut {
    pub cmd: Option<NetCmd>,
    /// Request frames to write to the socket.
    pub tx: TxBuf,
    /// Replies in send order.
    pub replies: ArrayVec<Reply, MAX_IN_FLIGHT>,
    /// The connection dropped with requests in flight. All of them are lost.
    pub lost: bool,
    /// The handshake finished in this step: jobs may send, and
    /// `Conn::partitions()` is set. When `down` is set too, the conn went
    /// up first.
    pub up: bool,
    /// The connection went away in this step, or in a `close` after it.
    pub down: bool,
}

impl ConnOut {
    /// `tx` holds the handshake and `max_in_flight` of the largest requests,
    /// so a send never finds it full.
    pub fn new(conn: &Conn) -> Self {
        ConnOut {
            cmd: None,
            tx: TxBuf::new(conn.tx_capacity),
            replies: ArrayVec::new(),
            lost: false,
            up: false,
            down: false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetCmd {
    Connect,
    /// Drop the socket. Bytes not yet written are gone.
    Close,
}

// Replies live in the fixed `ConnOut::replies` buffer, so a box would only
// add an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    InitProducerId {
        code: i16,
        id: ProducerId,
    },
    Produce {
        codes: PartCodes,
    },
    ListOffsets {
        offsets: ArrayVec<PartOffset, MAX_PARTS>,
    },
    Fetch {
        /// Error code of the whole request.
        code: i16,
        parts: ArrayVec<FetchPart, MAX_PARTS>,
    },
}

/// The broker's error code for each part of one Produce.
pub type PartCodes = ArrayVec<PartCode, MAX_PARTS>;

/// The broker's error code for one part of a Produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartCode {
    pub part: usize,
    pub code: i16,
}

/// The broker's answer for one part of a ListOffsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartOffset {
    pub part: usize,
    pub code: i16,
    /// -1 when the broker has no offset for the timestamp.
    pub offset: i64,
}

/// The broker's answer for one part of a Fetch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchPart {
    pub part: usize,
    pub code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    /// RecordBatch v2 bytes as the broker sent them, in the receive buffer:
    /// read them with `RxBuf::slice` before the next `RxBuf::spare`. Empty
    /// for null records. The first batch may start before the fetch offset,
    /// and the last one may be cut short at the byte limit.
    pub records: RxSpan,
}

/// Metadata partition codes while a leader is chosen, as after a broker
/// restart. The handshake goes again on a new connection.
pub const LEADER_MOVING: [i16; 2] = [
    5, // LEADER_NOT_AVAILABLE
    6, // NOT_LEADER_OR_FOLLOWER
];

enum ConnState {
    Down {
        retry_at: Instant,
    },
    Connecting {
        deadline: Instant,
    },
    /// ApiVersions sent.
    Versions {
        correlation: i32,
        deadline: Instant,
    },
    /// Metadata sent for the configured topics.
    Metadata {
        correlation: i32,
        deadline: Instant,
    },
    /// Job requests in send order.
    Up {
        in_flight: Deque<Sent, MAX_IN_FLIGHT>,
    },
}

struct Sent {
    api: Api,
    correlation: i32,
    deadline: Instant,
}

/// Bytes of the request frame of `api` with `body`, length field included.
fn request_bytes(client_id: &str, api: Api, body: impl FnOnce(&mut Writer<'_>)) -> usize {
    LENGTH_BYTES
        + Writer::size(|writer| {
            wire::request_header(writer, api, 0, client_id);
            body(writer);
        })
}

fn topic_names(topics: &[String]) -> impl ExactSizeIterator<Item = &str> {
    topics.iter().map(String::as_str)
}

/// The body of a reply to `api`, after a header with the `expected`
/// correlation id.
fn reply_body(frame: &[u8], api: Api, expected: i32) -> Result<&[u8], Fatal> {
    let (correlation, body) = wire::response_header(frame, api)?;
    if correlation != expected {
        return Err(Fatal::Protocol);
    }
    Ok(body)
}

/// Each pinned version is in the broker's range. A broker without
/// ApiVersions v3 answers with an error code.
fn versions(body: &[u8]) -> Result<(), Fatal> {
    let mut supported = [false; Api::ALL.len()];
    let code = wire::api_versions_response(body, |range| {
        let versions = range.min_version.get()..=range.max_version.get();
        for (supported, api) in supported.iter_mut().zip(Api::ALL) {
            *supported |= range.api_key.get() == api.key() && versions.contains(&api.version());
        }
    })?;
    if code != 0 {
        return Err(Fatal::Version {
            api: Api::ApiVersions.key(),
        });
    }
    match Api::ALL
        .iter()
        .zip(supported)
        .find(|&(_, supported)| !supported)
    {
        Some((api, _)) => Err(Fatal::Version { api: api.key() }),
        None => Ok(()),
    }
}

/// Every topic exists, and the one broker in the reply leads every
/// partition. Each count is above 0, and all counts together are at most
/// `MAX_PARTS`. Once `learned`, each count equals the one in `counts`;
/// before, the reply's counts go to `counts`. Ok(false) when the topics
/// match and a partition has a `LEADER_MOVING` code: retry later. `counts`
/// changes only on Ok(true).
fn metadata(
    topics: &[String],
    learned: bool,
    counts: &mut [u16],
    body: &[u8],
) -> Result<bool, Fatal> {
    // The counts this reply gives, written to `counts` once it all checks.
    let mut read = [0u16; MAX_PARTS];
    let mut brokers = 0;
    let mut leader = 0;
    let mut found = [false; MAX_PARTS];
    // The configured topic of the partitions that follow, and the
    // partitions of it seen so far, one bit each.
    let mut topic = None;
    let mut seen = 0u64;
    // A missing topic or a wrong count stays fatal, so every topic is
    // checked before a moving leader gives a retry.
    let mut moving = false;
    wire::metadata_response(body, |item| {
        match item {
            MetadataItem::Broker { node_id } => {
                brokers += 1;
                leader = node_id;
            }
            MetadataItem::Topic {
                error_code,
                name,
                partitions,
            } => {
                if brokers != 1 {
                    return Err(Fatal::Metadata);
                }
                topic =
                    name.and_then(|name| topics.iter().position(|topic| topic.as_bytes() == name));
                seen = 0;
                if let Some(index) = topic {
                    if found[index]
                        || error_code != 0
                        || partitions == 0
                        || partitions > MAX_PARTS
                        || (learned && partitions != usize::from(counts[index]))
                    {
                        return Err(Fatal::Metadata);
                    }
                    found[index] = true;
                    read[index] = partitions as u16;
                }
            }
            MetadataItem::Partition(partition) => {
                let Some(index) = topic else {
                    return Ok(());
                };
                let bit = u32::try_from(partition.partition_index.get())
                    .ok()
                    .filter(|&bit| bit < u32::from(read[index]))
                    .ok_or(Fatal::Metadata)?;
                let code = partition.error_code.get();
                if seen & (1 << bit) != 0 {
                    return Err(Fatal::Metadata);
                }
                seen |= 1 << bit;
                if LEADER_MOVING.contains(&code) {
                    moving = true;
                } else if code != 0 || partition.leader_id.get() != leader {
                    return Err(Fatal::Metadata);
                }
            }
        }
        Ok(())
    })?;
    if brokers != 1 || !found[..topics.len()].iter().all(|&found| found) {
        return Err(Fatal::Metadata);
    }
    if read[..topics.len()]
        .iter()
        .map(|&count| usize::from(count))
        .sum::<usize>()
        > MAX_PARTS
    {
        return Err(Fatal::Metadata);
    }
    if moving {
        return Ok(false);
    }
    counts.copy_from_slice(&read[..topics.len()]);
    Ok(true)
}

/// The first part of each topic of `counts`, into `first_part`.
fn first_parts(counts: &[u16], first_part: &mut [usize]) {
    let mut next = 0;
    for (first, &count) in first_part.iter_mut().zip(counts) {
        *first = next;
        next += usize::from(count);
    }
}

/// (topic name, partition index) of every part with `counts`, in part
/// order.
fn every_partition<'a>(
    topics: &'a [String],
    counts: &'a [u16],
) -> impl Clone + Iterator<Item = (&'a str, i32)> {
    topics.iter().zip(counts).flat_map(|(topic, &count)| {
        (0..i32::from(count)).map(move |partition| (topic.as_str(), partition))
    })
}

/// `make(topic name, partition index, value)` of each (part, value) of
/// `parts`, which come in increasing part order.
fn targets<'a, T, R>(
    topics: &'a [String],
    first_part: &'a [usize],
    parts: impl Clone + Iterator<Item = (usize, T)>,
    make: impl Clone + Fn(&'a str, i32, T) -> R,
) -> impl Clone + Iterator<Item = R> {
    let mut last_part = None;
    parts.map(move |(part, value)| {
        assert!(
            last_part.is_none_or(|last| last < part),
            "parts out of order"
        );
        last_part = Some(part);
        let topic = first_part.partition_point(|&first| first <= part) - 1;
        let partition = (part - first_part[topic]) as i32;
        make(topics[topic].as_str(), partition, value)
    })
}

/// Pushes one request frame: the header of `api`, then `body`. The send
/// buffer is sized for it at `Conn::new`.
fn push(
    tx: &mut TxBuf,
    client_id: &str,
    api: Api,
    correlation: i32,
    body: impl Fn(&mut Writer<'_>),
) {
    tx.push(|writer| {
        wire::request_header(writer, api, correlation, client_id);
        body(writer);
    })
    .expect("the send buffer holds the handshake and max_in_flight requests");
}

/// Bytes of the 4-byte frame length.
const LENGTH_BYTES: usize = 4;
