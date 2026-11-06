//! A compio socket driver for the machines: one `TcpStream` and a 1 ms tick
//! loop. It runs the Composition of `plans/kafka-sans-io.md`, with the
//! producer, the recovery, or the consumer (`plans/consumer.md`) as the job.

#![allow(dead_code)]

use std::{
    future::{Future, poll_fn},
    io,
    net::SocketAddr,
    pin::{Pin, pin},
    task::Poll,
    time::{self, Duration},
};

use crate::{
    accumulator::Accumulator,
    conn::{Config, Conn, ConnIn, ConnOut, NetCmd, NetEvent, PartCodes, PartOffset, Reply},
    consumer::{self, Consumer, ConsumerConfig, ConsumerIn, ConsumerOut, Phase},
    error::Fatal,
    frame::{RxBuf, TxBuf},
    producer::{Producer, ProducerIn, ProducerOut},
    recovery::{Found, Recovery, RecoveryConfig, RecoveryIn, RecoveryOut},
    time::Instant,
};
use compio::{
    BufResult,
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::sleep_until,
};

use super::record::OwnedRecord;

/// One socket, or none.
pub struct Socket {
    addr: SocketAddr,
    connection: Connection,
    /// Given to the conn on the next step.
    event: Option<NetEvent>,
}

impl Socket {
    pub fn new(addr: SocketAddr) -> Self {
        Socket {
            addr,
            connection: Connection::None,
            event: None,
        }
    }

    /// Does IO until `deadline`: reads into `rx` and writes `tx`. Returns
    /// the event for the next step.
    pub async fn exchange(
        &mut self,
        rx: &mut RxBuf,
        tx: &mut TxBuf,
        deadline: time::Instant,
    ) -> Option<NetEvent> {
        let mut tick = pin!(sleep_until(deadline));
        loop {
            if let Connection::Open {
                stream,
                read,
                write,
            } = &mut self.connection
            {
                // Only this loop fills `rx`, so its spare room cannot shrink
                // while the read is in flight.
                if read.is_none() {
                    let size = rx.spare().len().min(READ_BYTES);
                    if size > 0 {
                        let mut stream = stream.clone();
                        *read = Some(Box::pin(async move {
                            stream.read(Vec::with_capacity(size)).await
                        }));
                    }
                }
                if write.is_none() && !tx.pending().is_empty() {
                    let mut stream = stream.clone();
                    let bytes = tx.pending().to_vec();
                    *write = Some(Box::pin(async move { stream.write(bytes).await }));
                }
            }
            let done = poll_fn(|context| {
                if tick.as_mut().poll(context).is_ready() {
                    return Poll::Ready(Done::Tick);
                }
                match &mut self.connection {
                    Connection::None => {}
                    Connection::Connecting(connect) => {
                        if let Poll::Ready(result) = connect.as_mut().poll(context) {
                            return Poll::Ready(Done::Connect(result));
                        }
                    }
                    Connection::Open { read, write, .. } => {
                        if let Some(future) = read
                            && let Poll::Ready(result) = future.as_mut().poll(context)
                        {
                            *read = None;
                            return Poll::Ready(Done::Read(result));
                        }
                        if let Some(future) = write
                            && let Poll::Ready(result) = future.as_mut().poll(context)
                        {
                            *write = None;
                            return Poll::Ready(Done::Write(result));
                        }
                    }
                }
                Poll::Pending
            })
            .await;
            match done {
                Done::Tick => return self.event.take(),
                Done::Connect(Ok(stream)) => {
                    stream.set_nodelay(true).expect("set nodelay");
                    self.connection = Connection::Open {
                        stream,
                        read: None,
                        write: None,
                    };
                    self.event = Some(NetEvent::Connected);
                }
                Done::Connect(Err(_)) => {
                    self.connection = Connection::None;
                    self.event = Some(NetEvent::ConnectFailed);
                }
                Done::Read(BufResult(Ok(count), bytes)) if count > 0 => {
                    rx.spare()[..count].copy_from_slice(&bytes[..count]);
                    rx.filled(count);
                }
                Done::Write(BufResult(Ok(count), _)) => tx.consume(count),
                Done::Read(_) | Done::Write(_) => self.closed(),
            }
        }
    }

    /// Carries out the conn's command. A close drops the reads and writes
    /// in flight: the conn drops every byte of a closed connection.
    pub fn cmd(&mut self, cmd: Option<NetCmd>) {
        match cmd {
            Some(NetCmd::Close) => self.connection = Connection::None,
            Some(NetCmd::Connect) => {
                self.connection = Connection::Connecting(Box::pin(TcpStream::connect(self.addr)));
            }
            None => {}
        }
    }

    fn closed(&mut self) {
        self.connection = Connection::None;
        self.event = Some(NetEvent::Closed);
    }
}

pub struct Options {
    pub max_in_flight: u8,
    /// Sets the source seals in one tick, at most.
    pub sets_per_tick: usize,
    pub limit: Duration,
    /// The process dies at the first lost request: the run ends there.
    pub stop_on_lost: bool,
    /// The partition count of each topic the plan's parts assume. Another
    /// layout on `Up` ends the run with `Fatal::Metadata`.
    pub partitions: Vec<u16>,
}

/// What a produce run saw.
#[derive(Debug)]
pub struct Outcome {
    /// `Err` on a fatal. Panics on the time limit.
    pub result: Result<(), Fatal>,
    /// Each Produce reply, with the number of the connection (from 1) it
    /// came on.
    pub replies: Vec<(usize, PartCodes)>,
    /// The code of each InitProducerId reply.
    pub init_codes: Vec<i16>,
    /// Steps where the conn reported requests lost.
    pub lost: usize,
    pub connections: usize,
    /// Sets of the plan acked, all of them unless the run ended early.
    pub acked_sets: usize,
}

/// What a recovery run saw.
#[derive(Debug)]
pub struct RecoverOutcome {
    /// Per part, the last record at or after the floor, or a fatal.
    pub result: Result<Vec<Option<Found>>, Fatal>,
    /// Each ListOffsets reply, in order: the end, then the floor.
    pub lists: Vec<Vec<PartOffset>>,
    /// Fetch replies read.
    pub fetches: usize,
    pub connections: usize,
}

/// What a consume run saw.
#[derive(Debug)]
pub struct Consumed {
    /// `Err` on a fatal. Panics on the time limit.
    pub result: Result<(), Fatal>,
    /// Per part, the delivered records with their offsets and when they
    /// came. Empty when the run keeps no records.
    pub records: Vec<Vec<(i64, OwnedRecord, Duration)>>,
    /// Records and record bytes (key and value) delivered.
    pub count: u64,
    pub bytes: u64,
    /// Each ListOffsets reply, with the timestamp it asked for.
    pub lists: Vec<(i64, Vec<PartOffset>)>,
    pub fetches: Vec<FetchReply>,
    pub connections: usize,
    /// `Conn::parts()` at the end.
    pub parts: Option<usize>,
    pub phase: Phase,
    /// The size of the receive buffer after the first `Up`.
    pub rx_bytes: usize,
    /// Since the start of the run.
    pub elapsed: Duration,
}

/// The state `consume` passes to its visitor after each step.
pub struct ConsumeStep<'a> {
    pub consumer: &'a Consumer,
    pub consumed: &'a Consumed,
    /// The step read a Fetch reply.
    pub fetched: bool,
}

/// One Fetch reply, and how long after its send it came.
#[derive(Clone, Debug)]
pub struct FetchReply {
    /// Since the start of the run.
    pub at: Duration,
    pub wait: Duration,
    pub code: i16,
    pub parts: Vec<FetchSeen>,
}

/// One part of a Fetch reply, without its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchSeen {
    pub part: usize,
    pub code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    /// Bytes of the records blob.
    pub bytes: usize,
}

/// Runs Conn + Producer + Accumulator until every set of `plan` is acked, or
/// a fatal. Each set is a list of `(part, record)`, sealed together.
/// `progress` gets the count of acked sets after each step.
pub async fn produce(
    addr: SocketAddr,
    config: Config,
    slot_bytes: usize,
    plan: &[Vec<(usize, OwnedRecord)>],
    options: &Options,
    mut progress: impl AsyncFnMut(usize),
) -> Outcome {
    let parts = config.produce_parts;
    let mut conn = Conn::new(config);
    let mut conn_out = ConnOut::new(&conn);
    let mut rx = RxBuf::new(RX_BYTES);
    let mut producer = Producer::new();
    let mut producer_out = ProducerOut::default();
    let mut accumulator = Accumulator::new(parts, options.max_in_flight, slot_bytes);
    let mut socket = Socket::new(addr);
    let mut outcome = Outcome {
        result: Ok(()),
        replies: Vec::new(),
        init_codes: Vec::new(),
        lost: 0,
        connections: 0,
        acked_sets: 0,
    };
    let (mut next_set, mut acked_sets) = (0, 0);
    let start = time::Instant::now();

    let fatal = loop {
        let net = socket
            .exchange(&mut rx, &mut conn_out.tx, time::Instant::now() + TICK)
            .await;
        let now = Instant(start.elapsed().as_nanos() as u64);
        assert!(
            start.elapsed() < options.limit,
            "produce run not done after {:?}: {acked_sets}/{} sets acked, {outcome:?}",
            options.limit,
            plan.len()
        );
        if let Err(fatal) = conn.step(
            ConnIn {
                now,
                net,
                rx: &mut rx,
            },
            &mut conn_out,
        ) {
            break fatal;
        }
        outcome.connections += usize::from(conn_out.up);
        if conn_out.up && conn.partitions() != Some(&options.partitions[..]) {
            break Fatal::Metadata;
        }
        outcome.lost += usize::from(conn_out.lost);
        if options.stop_on_lost && conn_out.lost {
            outcome.acked_sets = acked_sets;
            return outcome;
        }
        for reply in &conn_out.replies {
            match reply {
                Reply::Produce { codes } => {
                    outcome.replies.push((outcome.connections, codes.clone()))
                }
                Reply::InitProducerId { code, .. } => outcome.init_codes.push(*code),
                Reply::ListOffsets { .. } | Reply::Fetch { .. } => {}
            }
        }

        let producer_in = ProducerIn {
            now,
            room: conn.room(),
            replies: &conn_out.replies,
            lost: conn_out.lost,
            accumulator: &accumulator,
        };
        if let Err(fatal) = producer.step(producer_in, &mut producer_out) {
            break fatal;
        }
        for &(set, parts) in &producer_out.acked {
            if accumulator.ack(set, parts).is_some() {
                acked_sets += 1;
            }
        }
        if producer_out.close {
            conn.close(now, &mut conn_out);
        }
        if producer_out.init {
            conn.send_init_producer_id(now, &mut conn_out);
        }
        for &set in &producer_out.produce {
            conn.send_produce(now, accumulator.batches(set), &mut conn_out);
        }
        if let Some(id) = producer.id() {
            let mut sealed = 0;
            while sealed < options.sets_per_tick
                && next_set < plan.len()
                && accumulator.sets().count() < usize::from(options.max_in_flight)
            {
                for (part, record) in &plan[next_set] {
                    record
                        .with_borrowed(|record| accumulator.append(*part, record))
                        .expect("a planned set fits its slots");
                }
                accumulator
                    .seal(id, next_set)
                    .expect("a planned set has records");
                next_set += 1;
                sealed += 1;
            }
        }
        progress(acked_sets).await;
        if acked_sets == plan.len() {
            outcome.acked_sets = acked_sets;
            return outcome;
        }

        socket.cmd(conn_out.cmd);
    };
    outcome.result = Err(fatal);
    outcome.acked_sets = acked_sets;
    outcome
}

/// Runs Conn + Recovery to `Done`, or a fatal. Panics after `limit`.
pub async fn recover(
    addr: SocketAddr,
    config: Config,
    recovery: RecoveryConfig,
    limit: Duration,
) -> RecoverOutcome {
    let fetch_max_bytes = recovery.fetch_max_bytes as usize;
    let mut conn = Conn::new(config);
    let mut conn_out = ConnOut::new(&conn);
    // The handshake buffer. The first `Up` replaces it.
    let mut rx = RxBuf::new(RX_BYTES);
    let mut grown = false;
    let mut recovery = Recovery::new(recovery);
    let mut recovery_out = RecoveryOut::default();
    let mut socket = Socket::new(addr);
    let mut outcome = RecoverOutcome {
        result: Ok(Vec::new()),
        lists: Vec::new(),
        fetches: 0,
        connections: 0,
    };
    let start = time::Instant::now();

    let fatal = loop {
        let net = socket
            .exchange(&mut rx, &mut conn_out.tx, time::Instant::now() + TICK)
            .await;
        let now = Instant(start.elapsed().as_nanos() as u64);
        assert!(
            start.elapsed() < limit,
            "recovery not done after {limit:?}: {:?}, {outcome:?}",
            recovery.phase()
        );
        if let Err(fatal) = conn.step(
            ConnIn {
                now,
                net,
                rx: &mut rx,
            },
            &mut conn_out,
        ) {
            break fatal;
        }
        outcome.connections += usize::from(conn_out.up);
        // One Fetch reply holds up to `fetch_max_bytes` per part. No job
        // request is out at the first `Up`, so the buffer holds no byte.
        if !grown && let Some(parts) = conn.parts() {
            grown = true;
            assert!(rx.is_empty(), "bytes in the buffer at the first Up");
            rx = RxBuf::new(RX_BYTES.max(parts * fetch_max_bytes + (64 << 10)));
        }
        for reply in &conn_out.replies {
            match reply {
                Reply::ListOffsets { offsets } => outcome.lists.push(offsets.to_vec()),
                Reply::Fetch { .. } => outcome.fetches += 1,
                Reply::InitProducerId { .. } | Reply::Produce { .. } => {}
            }
        }
        let recovery_in = RecoveryIn {
            now,
            room: conn.room(),
            partitions: conn.partitions(),
            replies: &conn_out.replies,
            lost: conn_out.lost,
            rx: &rx,
        };
        if let Err(fatal) = recovery.step(recovery_in, &mut recovery_out) {
            break fatal;
        }
        if recovery.done() {
            outcome.result = Ok(recovery.results().collect());
            return outcome;
        }
        if recovery_out.close {
            conn.close(now, &mut conn_out);
        }
        if let Some(timestamp) = recovery_out.list {
            conn.send_list_offsets(now, timestamp, &mut conn_out);
        }
        if recovery_out.fetch {
            conn.send_fetch(now, recovery.limits(), recovery.fetches(), &mut conn_out);
        }
        socket.cmd(conn_out.cmd);
    };
    outcome.result = Err(fatal);
    outcome
}

/// Runs Conn + Consumer until `Done`, a fatal, or `visit` returns false.
/// `visit` runs after each step; while it awaits, no socket IO happens.
/// `keep` keeps each record in `Consumed::records`. Panics after `limit`.
pub async fn consume(
    addr: SocketAddr,
    config: Config,
    consumer: ConsumerConfig,
    keep: bool,
    limit: Duration,
    mut visit: impl AsyncFnMut(ConsumeStep<'_>) -> bool,
) -> Consumed {
    let mut conn = Conn::new(config);
    let mut conn_out = ConnOut::new(&conn);
    // The handshake buffer. The first `Up` replaces it.
    let mut rx = RxBuf::new(READ_BYTES);
    let mut consumer = Consumer::new(consumer);
    let mut consumer_out = ConsumerOut::default();
    let mut socket = Socket::new(addr);
    let mut consumed = Consumed {
        result: Ok(()),
        records: Vec::new(),
        count: 0,
        bytes: 0,
        lists: Vec::new(),
        fetches: Vec::new(),
        connections: 0,
        parts: None,
        phase: Phase::Waiting,
        rx_bytes: 0,
        elapsed: Duration::ZERO,
    };
    // The timestamp of the ListOffsets out, and when the Fetch out went.
    let mut listed = 0;
    let mut sent_at = Duration::ZERO;
    let start = time::Instant::now();

    let fatal = loop {
        let net = socket
            .exchange(&mut rx, &mut conn_out.tx, time::Instant::now() + TICK)
            .await;
        let elapsed = start.elapsed();
        let now = Instant(elapsed.as_nanos() as u64);
        assert!(
            elapsed < limit,
            "consume not done after {limit:?}: {:?}, {} records, {} connections",
            consumer.phase(),
            consumed.count,
            consumed.connections
        );
        if let Err(fatal) = conn.step(
            ConnIn {
                now,
                net,
                rx: &mut rx,
            },
            &mut conn_out,
        ) {
            break fatal;
        }
        consumed.connections += usize::from(conn_out.up);
        let mut fetched = false;
        for reply in &conn_out.replies {
            match reply {
                Reply::ListOffsets { offsets } => consumed.lists.push((listed, offsets.to_vec())),
                Reply::Fetch { code, parts } => {
                    fetched = true;
                    consumed.fetches.push(FetchReply {
                        at: elapsed,
                        wait: elapsed.saturating_sub(sent_at),
                        code: *code,
                        parts: parts
                            .iter()
                            .map(|part| FetchSeen {
                                part: part.part,
                                code: part.code,
                                high_watermark: part.high_watermark,
                                log_start_offset: part.log_start_offset,
                                bytes: part.records.len(),
                            })
                            .collect(),
                    });
                }
                Reply::InitProducerId { .. } | Reply::Produce { .. } => {}
            }
        }
        if let Err(fatal) = consumer.step(
            ConsumerIn {
                now,
                room: conn.room(),
                partitions: conn.partitions(),
                replies: &conn_out.replies,
                lost: conn_out.lost,
                rx: &rx,
            },
            &mut consumer_out,
        ) {
            break fatal;
        }
        consumed.parts = conn.parts();
        consumed.phase = consumer.phase();
        if consumed.records.len() < consumed.parts.unwrap_or(0) {
            consumed
                .records
                .resize(consumed.parts.unwrap_or(0), Vec::new());
        }
        let mut broken = None;
        for delivery in &consumer_out.deliveries {
            for record in
                consumer::delivered(rx.slice(delivery.records), delivery.from, delivery.until)
            {
                let record = match record {
                    Ok(record) => record,
                    Err(fatal) => {
                        broken = Some(fatal);
                        break;
                    }
                };
                consumed.count += 1;
                consumed.bytes += (record.key.map_or(0, <[u8]>::len)
                    + record.value.map_or(0, <[u8]>::len)) as u64;
                if keep {
                    consumed.records[delivery.part].push((
                        record.offset,
                        OwnedRecord {
                            timestamp_ms: record.timestamp_ms,
                            key: record.key.map(<[u8]>::to_vec),
                            value: record.value.map(<[u8]>::to_vec),
                            headers: record
                                .headers
                                .clone()
                                .map(|header| {
                                    let header = header.expect("delivered headers read");
                                    (
                                        String::from_utf8_lossy(header.key).into_owned(),
                                        header.value.map(<[u8]>::to_vec),
                                    )
                                })
                                .collect(),
                        },
                        elapsed,
                    ));
                }
            }
        }
        if let Some(fatal) = broken {
            break fatal;
        }
        // The first `Up`: the receive buffer grows to the largest Fetch
        // reply. No job request was out yet, so it holds no byte.
        if conn_out.up && consumed.rx_bytes == 0 {
            assert!(rx.is_empty(), "bytes in the buffer at the first Up");
            consumed.rx_bytes =
                consumer::fetch_reply_bytes(&conn, consumer.selected().count(), consumer.limits());
            rx = RxBuf::new(consumed.rx_bytes);
        }
        if consumer_out.close {
            conn.close(now, &mut conn_out);
        }
        if let Some(timestamp) = consumer_out.list {
            listed = timestamp;
            conn.send_list_offsets(now, timestamp, &mut conn_out);
        }
        if consumer_out.fetch {
            sent_at = elapsed;
            conn.send_fetch(now, consumer.limits(), consumer.fetches(), &mut conn_out);
        }
        consumed.elapsed = elapsed;
        if consumer.phase() == Phase::Done
            || !visit(ConsumeStep {
                consumer: &consumer,
                consumed: &consumed,
                fetched,
            })
            .await
        {
            return consumed;
        }
        socket.cmd(conn_out.cmd);
    };
    consumed.result = Err(fatal);
    consumed
}

enum Connection {
    None,
    Connecting(Pending<io::Result<TcpStream>>),
    /// A read or write stays in flight across ticks until it completes: a
    /// dropped read loses the bytes the kernel already wrote to its buffer.
    Open {
        stream: TcpStream,
        read: Option<Pending<BufResult<usize, Vec<u8>>>>,
        write: Option<Pending<BufResult<usize, Vec<u8>>>>,
    },
}

/// What ended one wait of `Socket::exchange`.
enum Done {
    Tick,
    Connect(io::Result<TcpStream>),
    Read(BufResult<usize, Vec<u8>>),
    Write(BufResult<usize, Vec<u8>>),
}

type Pending<T> = Pin<Box<dyn Future<Output = T>>>;

const TICK: Duration = Duration::from_millis(1);

const RX_BYTES: usize = 1 << 20;

/// Bytes one socket read takes, at most.
const READ_BYTES: usize = 64 << 10;
