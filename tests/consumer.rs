//! Plan test 3 of `plans/consumer.md`: the consumer job against the model
//! broker.
//!
//! The broker starts with random batches and a random log start in each
//! partition. Then Conn + Consumer run under faults while records are
//! appended and logs are trimmed, then healthy rounds follow. Per selected
//! part, the delivered offsets must be the stored offsets in
//! `[start, stop)`, in order, each once. A trim during the run may only
//! take offsets below the final log start.
//!
//! Appended records have timestamps above every start and stop the test
//! draws, and trims wait until the consumer reached `Fetch`. So the start
//! and the stop follow from the log at the start of the run, and the `Tail`
//! start from the end the accepted `ListEnd` reply gave.

use kafka_sans_io::{
    conn::{Config, Conn, ConnIn, ConnOut, FetchLimits, NetCmd, NetEvent, Reply},
    consumer::{self, Consumer, ConsumerConfig, ConsumerIn, ConsumerOut, Phase, Select, Start},
    error::Fatal,
    frame::RxBuf,
    testing::{
        model::{Broker, Connection, Fault},
        record::OwnedRecord,
    },
    time::{Duration, Instant},
};
use proptest::{option, prelude::*, sample::Index, test_runner::TestCaseError};

/// Topics and partition counts. Parts: a/0, a/1, a/2, b/0, b/1.
const TOPICS: [(&str, i32); 2] = [("a", 3), ("b", 2)];
const PARTS: usize = 5;
const SMALL_TICK: Duration = Duration::from_millis(1);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(50);
const RETRIABLE: [i16; 5] = [5, 6, 7, 19, 20];
const LEADER_MOVING: [i16; 2] = [5, 6];
const DRAIN_ROUNDS: usize = 2000;
/// Timestamps of the first records lie in `FIRST_MS..FIRST_MS + SPAN_MS`.
const FIRST_MS: i64 = 1000;
const SPAN_MS: i64 = 60;
/// Appended records have timestamps from here up: above every drawn start
/// and stop.
const APPENDED_MS: i64 = 5000;

/// (topic, partition) of each part.
fn partition(part: usize) -> (&'static str, i32) {
    TOPICS
        .iter()
        .flat_map(|&(name, count)| (0..count).map(move |partition| (name, partition)))
        .nth(part)
        .expect("part in range")
}

#[derive(Clone, Debug)]
struct RecordSpec {
    /// Milliseconds after `FIRST_MS`, or after `APPENDED_MS` when appended.
    at_ms: i64,
    /// Value bytes after the 8-byte id.
    filler: u8,
}

#[derive(Clone, Debug)]
enum Action {
    Tick,
    /// The deadline of every request in flight passes.
    TickPast,
    Deliver {
        chunk: usize,
    },
    Reply,
    PartError {
        part: Index,
        code: i16,
    },
    LeaderMoving {
        part: Index,
        code: i16,
    },
    /// The broker handles the oldest request; in a Fetch, one part's last
    /// batch is cut short, as Apache Kafka sends at the byte limit.
    CutShort {
        part: Index,
    },
    /// The broker handles the first `handled` requests, then the socket
    /// closes before any reply.
    Drop {
        handled: usize,
    },
    ConnectFails,
    /// One batch is appended to a part.
    Append {
        part: Index,
        records: Vec<RecordSpec>,
    },
    /// The log start of a part moves up by `by`, once the consumer is
    /// past its list rounds.
    Trim {
        part: Index,
        by: u8,
    },
}

fn record_spec(span: i64) -> impl Strategy<Value = RecordSpec> {
    (0..span, 0u8..=40).prop_map(|(at_ms, filler)| RecordSpec { at_ms, filler })
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        40 => Just(Action::Tick),
        6 => Just(Action::TickPast),
        70 => prop_oneof![1usize..64, Just(usize::MAX)].prop_map(|chunk| Action::Deliver { chunk }),
        70 => Just(Action::Reply),
        10 => (any::<Index>(), proptest::sample::select(RETRIABLE.to_vec()))
            .prop_map(|(part, code)| Action::PartError { part, code }),
        3 => (any::<Index>(), proptest::sample::select(LEADER_MOVING.to_vec()))
            .prop_map(|(part, code)| Action::LeaderMoving { part, code }),
        10 => any::<Index>().prop_map(|part| Action::CutShort { part }),
        8 => (0usize..=2).prop_map(|handled| Action::Drop { handled }),
        5 => Just(Action::ConnectFails),
        15 => (any::<Index>(), proptest::collection::vec(record_spec(10), 1..=4))
            .prop_map(|(part, records)| Action::Append { part, records }),
        6 => (any::<Index>(), 1u8..=5).prop_map(|(part, by)| Action::Trim { part, by }),
    ]
}

/// The network and the broker.
struct World {
    now: Instant,
    broker: Broker,
    connection: Option<Connection>,
    event: Option<NetEvent>,
    connect_fails: bool,
    /// The next appended record's id, from 1 << 32 up.
    next_id: u64,
    appended_ms: i64,
    /// A trim moved a log start during the run.
    trimmed: bool,
}

impl World {
    fn act(&mut self, action: &Action, rx: &mut RxBuf, phase: Phase, limit: i32) {
        match action {
            Action::Tick => self.now = self.now.plus(SMALL_TICK),
            Action::TickPast => self.now = self.now.plus(REQUEST_TIMEOUT + SMALL_TICK),
            Action::Deliver { chunk } => self.deliver(*chunk, rx),
            Action::Reply => self.handle(None),
            Action::PartError { part, code } => self.handle(Some(Fault::Part(*part, *code))),
            Action::LeaderMoving { part, code } => self.handle(Some(Fault::Leader(*part, *code))),
            Action::CutShort { part } => self.handle(Some(Fault::CutShort(*part))),
            Action::Drop { handled } => {
                if let Some(mut connection) = self.connection.take() {
                    for _ in 0..*handled {
                        connection.handle_one(&mut self.broker, None);
                    }
                    self.event = Some(NetEvent::Closed);
                }
            }
            Action::ConnectFails => self.connect_fails = true,
            Action::Append { part, records } => {
                let (topic, index) = partition(part.index(PARTS));
                put(
                    &mut self.broker,
                    topic,
                    index,
                    &records
                        .iter()
                        .map(|spec| {
                            self.appended_ms += spec.at_ms;
                            self.next_id += 1;
                            plain(self.next_id, self.appended_ms, spec.filler)
                        })
                        .collect::<Vec<_>>(),
                    limit,
                );
            }
            Action::Trim { part, by } => {
                if matches!(phase, Phase::Fetch | Phase::ListEarliest | Phase::Done) {
                    let (topic, index) = partition(part.index(PARTS));
                    let key = (topic.to_string(), index);
                    let start = self.broker.log_start[&key];
                    self.broker.trim(topic, index, start + i64::from(*by));
                    self.trimmed |= self.broker.log_start[&key] != start;
                }
            }
        }
    }

    fn handle(&mut self, fault: Option<Fault>) {
        if let Some(connection) = &mut self.connection {
            connection.handle_one(&mut self.broker, fault);
        }
    }

    fn deliver(&mut self, chunk: usize, rx: &mut RxBuf) {
        if let Some(connection) = &mut self.connection {
            let count = connection.deliver(chunk, rx.spare());
            rx.filled(count);
        }
    }

    fn answer_all(&mut self, rx: &mut RxBuf) {
        if let Some(connection) = &mut self.connection {
            while connection.handle_one(&mut self.broker, None) {}
        }
        self.deliver(usize::MAX, rx);
    }

    fn network(&mut self, conn_out: &mut ConnOut) {
        match conn_out.cmd {
            Some(NetCmd::Connect) => {
                assert!(self.connection.is_none(), "connect on an open connection");
                if std::mem::take(&mut self.connect_fails) {
                    self.event = Some(NetEvent::ConnectFailed);
                } else {
                    self.connection = Some(Connection::default());
                    self.event = Some(NetEvent::Connected);
                }
            }
            Some(NetCmd::Close) => self.connection = None,
            None => {}
        }
        let bytes = conn_out.tx.pending().to_vec();
        conn_out.tx.consume(bytes.len());
        match &mut self.connection {
            Some(connection) => {
                connection.receive(&bytes);
            }
            None => assert!(bytes.is_empty(), "bytes sent with no connection"),
        }
    }
}

/// A record whose value starts with its id.
fn plain(id: u64, timestamp_ms: i64, filler: u8) -> OwnedRecord {
    let mut value = id.to_be_bytes().to_vec();
    value.resize(8 + usize::from(filler), 0xcd);
    OwnedRecord {
        timestamp_ms,
        key: filler.is_multiple_of(3).then(|| vec![filler; 3]),
        value: Some(value),
        headers: Vec::new(),
    }
}

/// Appends one batch. Every batch fits the partition limit, so a Fetch at
/// a part's next offset always returns a whole batch.
fn put(broker: &mut Broker, topic: &str, index: i32, records: &[OwnedRecord], limit: i32) {
    broker.put(topic, index, records);
    let stored = broker.batches[&(topic.to_string(), index)]
        .last()
        .expect("a batch was put");
    assert!(
        stored.bytes.len() <= limit as usize,
        "a batch of {} bytes above the limit {limit}",
        stored.bytes.len()
    );
}

fn conn_config() -> Config {
    Config {
        client_id: "consumer",
        topics: TOPICS.iter().map(|&(name, _)| name.to_string()).collect(),
        request_timeout: REQUEST_TIMEOUT,
        backoff_min: Duration::from_millis(1),
        backoff_max: Duration::from_millis(8),
        max_in_flight: 1,
        max_batch_bytes: 0,
        produce_parts: 0,
    }
}

#[derive(Clone, Debug)]
struct Setup {
    /// Per part, batches of records.
    contents: Vec<Vec<Vec<RecordSpec>>>,
    /// Per part, where the log start lies, as an index over `0..=end`, or
    /// `None` for 0.
    trims: Vec<Option<Index>>,
    start: Start,
    select: Select,
    stop_ms: Option<i64>,
    limit: i32,
}

fn setup() -> impl Strategy<Value = Setup> {
    let timestamp = FIRST_MS - 5..FIRST_MS + SPAN_MS + 5;
    (
        proptest::collection::vec(
            proptest::collection::vec(
                proptest::collection::vec(record_spec(SPAN_MS), 1..=4),
                0..=6,
            ),
            PARTS,
        ),
        proptest::collection::vec(
            prop_oneof![2 => Just(None), 1 => any::<Index>().prop_map(Some)],
            PARTS,
        ),
        prop_oneof![
            timestamp.clone().prop_map(Start::Timestamp),
            (0u32..=12).prop_map(Start::Tail),
        ],
        prop_oneof![
            Just(Select::All),
            any::<Index>().prop_map(|part| {
                let (topic, partition) = partition(part.index(PARTS));
                Select::One {
                    topic: usize::from(topic == "b"),
                    partition: partition as u16,
                }
            }),
        ],
        option::of(timestamp),
        prop_oneof![400i32..=1200, Just(1 << 20)],
    )
        .prop_map(|(contents, trims, start, select, stop_ms, limit)| Setup {
            contents,
            trims,
            start,
            select,
            stop_ms,
            limit,
        })
}

/// Fills the broker and sets the log starts.
fn fill(broker: &mut Broker, setup: &Setup) {
    let mut id = 0;
    for (part, batches) in setup.contents.iter().enumerate() {
        let (topic, index) = partition(part);
        for batch in batches {
            put(
                broker,
                topic,
                index,
                &batch
                    .iter()
                    .map(|spec| {
                        id += 1;
                        plain(id, FIRST_MS + spec.at_ms, spec.filler)
                    })
                    .collect::<Vec<_>>(),
                setup.limit,
            );
        }
        if let Some(trim) = setup.trims[part] {
            broker.trim(
                topic,
                index,
                trim.index(broker.logs[&(topic.to_string(), index)].len() + 1) as i64,
            );
        }
    }
}

/// The first offset at or above the log start with a timestamp at or above
/// `timestamp`, else the end, in the log at the start of the run.
fn first_at(broker: &Broker, part: usize, timestamp: i64) -> i64 {
    let (topic, index) = partition(part);
    let key = (topic.to_string(), index);
    let log = &broker.logs[&key];
    log[broker.log_start[&key] as usize..]
        .iter()
        .find(|committed| committed.record.timestamp_ms >= timestamp)
        .map_or(log.len() as i64, |committed| committed.offset)
}

/// What a run read.
#[derive(Debug)]
struct Run {
    /// Per part, the delivered (offset, record).
    delivered: Vec<Vec<(i64, OwnedRecord)>>,
    /// Per part, the end of the accepted `ListEnd` reply.
    ends: Option<Vec<i64>>,
    phase: Phase,
}

/// Runs Conn + Consumer: `actions`, then healthy rounds until the consumer
/// is `Done`, or caught up at the end of every selected part.
fn consume(setup: &Setup, actions: &[Action]) -> Result<(Run, World), TestCaseError> {
    let mut world = World {
        now: Instant(1_000_000_000),
        broker: Broker::new(&TOPICS),
        connection: None,
        event: None,
        connect_fails: false,
        next_id: 1 << 32,
        appended_ms: APPENDED_MS,
        trimmed: false,
    };
    fill(&mut world.broker, setup);
    let mut conn = Conn::new(conn_config());
    let mut conn_out = ConnOut::new(&conn);
    let mut rx = RxBuf::new(1 << 20);
    let mut consumer = Consumer::new(ConsumerConfig {
        select: setup.select,
        start: setup.start,
        stop_timestamp_ms: setup.stop_ms,
        limits: FetchLimits {
            partition_max_bytes: setup.limit,
            max_wait_ms: 500,
            min_bytes: 1,
        },
    });
    let mut consumer_out = ConsumerOut::default();
    let mut run = Run {
        delivered: vec![Vec::new(); PARTS],
        ends: None,
        phase: Phase::Waiting,
    };
    let selected: Vec<usize> = match setup.select {
        Select::All => (0..PARTS).collect(),
        Select::One { topic, partition } => vec![3 * topic + usize::from(partition)],
    };

    let mut healthy = 0;
    let mut actions = actions.iter();
    // The first part a Fetch with other parts starved: no whole batch below
    // its high watermark. The next Fetch asks for it alone.
    let mut starved = None;
    loop {
        match actions.next() {
            Some(action) => world.act(action, &mut rx, consumer.phase(), setup.limit),
            None => {
                // Settled: `Done`, or caught up at every selected end.
                if healthy == DRAIN_ROUNDS
                    || match consumer.phase() {
                        Phase::Done => true,
                        Phase::Fetch if setup.stop_ms.is_none() => selected.iter().all(|&part| {
                            let (topic, index) = partition(part);
                            consumer.caught_up(part)
                                && consumer.next(part)
                                    == world.broker.logs[&(topic.to_string(), index)].len() as i64
                        }),
                        _ => false,
                    }
                {
                    break;
                }
                healthy += 1;
                world.connect_fails = false;
                world.answer_all(&mut rx);
                world.now = world.now.plus(SMALL_TICK);
            }
        }

        let now = world.now;
        let result = conn.step(
            ConnIn {
                now,
                net: world.event.take(),
                rx: &mut rx,
            },
            &mut conn_out,
        );
        prop_assert!(result.is_ok(), "conn fatal: {:?}", result);
        let before = consumer.phase();
        let result = consumer.step(
            ConsumerIn {
                now,
                room: conn.room(),
                partitions: conn.partitions(),
                replies: &conn_out.replies,
                lost: conn_out.lost,
                rx: &rx,
            },
            &mut consumer_out,
        );
        prop_assert!(result.is_ok(), "consumer fatal: {:?}", result);
        if before == Phase::ListEnd
            && consumer.phase() != Phase::ListEnd
            && let [Reply::ListOffsets { offsets }] = &conn_out.replies[..]
        {
            let mut ends = vec![0; PARTS];
            for offset in offsets {
                ends[offset.part] = offset.offset;
            }
            run.ends = Some(ends);
        }
        if let [Reply::Fetch { parts, .. }] = &conn_out.replies[..] {
            if parts.len() > 1 {
                starved = parts
                    .iter()
                    .find(|fetched| {
                        fetched.code == 0
                            && fetched.records.is_empty()
                            && consumer.next(fetched.part) < fetched.high_watermark
                    })
                    .map(|fetched| fetched.part);
            }
            // The bound `fetch_reply_bytes` sizes the buffer with.
            let records: usize = parts.iter().map(|fetched| fetched.records.len()).sum();
            prop_assert!(
                records <= (parts.len() + 1) * setup.limit as usize,
                "a reply of {} record bytes for {} parts at limit {}",
                records,
                parts.len(),
                setup.limit
            );
            for fetched in parts.iter().filter(|fetched| fetched.code == 0) {
                prop_assert_eq!(
                    consumer.caught_up(fetched.part),
                    consumer.next(fetched.part) == fetched.high_watermark,
                    "caught up exactly at the reply's high watermark, part {}",
                    fetched.part
                );
            }
        }
        if consumer.phase() != Phase::Waiting {
            prop_assert_eq!(
                consumer.selected().collect::<Vec<_>>(),
                selected.clone(),
                "selected parts"
            );
            prop_assert_eq!(
                consumer.phase() == Phase::Done,
                selected.iter().all(|&part| consumer.done(part)),
                "Done exactly when every selected part is at its stop"
            );
        }
        for delivery in &consumer_out.deliveries {
            prop_assert!(
                selected.contains(&delivery.part),
                "delivery for part {} outside the selection",
                delivery.part
            );
            for record in
                consumer::delivered(rx.slice(delivery.records), delivery.from, delivery.until)
            {
                let record = record.map_err(|fatal| TestCaseError::fail(format!("{fatal}")))?;
                run.delivered[delivery.part].push((
                    record.offset,
                    OwnedRecord {
                        timestamp_ms: record.timestamp_ms,
                        key: record.key.map(<[u8]>::to_vec),
                        value: record.value.map(<[u8]>::to_vec),
                        headers: Vec::new(),
                    },
                ));
            }
        }
        if consumer_out.close {
            conn.close(now, &mut conn_out);
        }
        if let Some(timestamp) = consumer_out.list {
            conn.send_list_offsets(now, timestamp, &mut conn_out);
        }
        if consumer_out.fetch {
            if let Some(part) = starved.take() {
                prop_assert_eq!(
                    consumer.fetches().collect::<Vec<_>>(),
                    vec![(part, consumer.next(part))],
                    "a starved part is fetched alone next"
                );
            }
            for (part, _) in consumer.fetches() {
                prop_assert!(
                    selected.contains(&part),
                    "Fetch of part {} outside the selection",
                    part
                );
            }
            conn.send_fetch(now, consumer.limits(), consumer.fetches(), &mut conn_out);
        }
        world.network(&mut conn_out);
    }
    run.phase = consumer.phase();
    if consumer.phase() == Phase::Fetch {
        for &part in &selected {
            let (topic, index) = partition(part);
            let end = world.broker.logs[&(topic.to_string(), index)].len() as i64;
            prop_assert!(
                consumer.caught_up(part) && consumer.next(part) == end,
                "progress: part {} at {} of {}, caught up {}",
                part,
                consumer.next(part),
                end,
                consumer.caught_up(part)
            );
        }
    }
    Ok((run, world))
}

fn check(setup: &Setup, actions: &[Action]) -> Result<(), TestCaseError> {
    let mut initial = Broker::new(&TOPICS);
    fill(&mut initial, setup);
    let (run, world) = consume(setup, actions)?;
    prop_assert!(
        match setup.stop_ms {
            Some(_) => run.phase == Phase::Done,
            None => run.phase == Phase::Fetch,
        },
        "progress: phase {:?} after the drain",
        run.phase
    );
    let selected: Vec<usize> = match setup.select {
        Select::All => (0..PARTS).collect(),
        Select::One { topic, partition } => vec![3 * topic + usize::from(partition)],
    };
    let ends = run.ends.as_ref().expect("a ListEnd reply was accepted");
    for (part, delivered) in run.delivered.iter().enumerate() {
        if !selected.contains(&part) {
            prop_assert!(delivered.is_empty(), "part {} is not selected", part);
            continue;
        }
        let (topic, index) = partition(part);
        let key = (topic.to_string(), index);
        // The ListOffsets rule, or the log start when that is later.
        let start = match setup.start {
            Start::Timestamp(timestamp) => first_at(&initial, part, timestamp),
            Start::Tail(count) => (ends[part] - i64::from(count)).max(0),
        }
        .max(initial.log_start[&key]);
        let stop = match setup.stop_ms {
            Some(timestamp) => first_at(&initial, part, timestamp),
            None => world.broker.logs[&key].len() as i64,
        };
        let final_start = world.broker.log_start[&key];
        let log = &world.broker.logs[&key];
        let offsets: Vec<i64> = delivered.iter().map(|(offset, _)| *offset).collect();
        prop_assert!(
            offsets.windows(2).all(|pair| pair[0] < pair[1]),
            "part {}: each offset once, in order: {:?}",
            part,
            offsets
        );
        prop_assert!(
            offsets.iter().all(|offset| (start..stop).contains(offset)),
            "part {}: offsets {:?} outside [{}, {})",
            part,
            offsets,
            start,
            stop
        );
        // Every offset no trim took is delivered.
        prop_assert_eq!(
            offsets
                .iter()
                .copied()
                .filter(|&offset| offset >= start.max(final_start))
                .collect::<Vec<_>>(),
            (start.max(final_start)..stop).collect::<Vec<_>>(),
            "part {}: offsets at or above the final log start",
            part
        );
        if !world.trimmed {
            prop_assert_eq!(
                &offsets,
                &(start..stop.max(start)).collect::<Vec<_>>(),
                "part {}: no trim, every offset",
                part
            );
        }
        for (offset, record) in delivered {
            let stored = &log[*offset as usize].record;
            prop_assert_eq!(
                (&record.timestamp_ms, &record.key, &record.value),
                (&stored.timestamp_ms, &stored.key, &stored.value),
                "part {} offset {}: the stored record",
                part,
                offset
            );
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]
    #[test]
    fn consumer_delivers_each_offset_once(
        setup in setup(),
        actions in proptest::collection::vec(action(), 0..=150),
    ) {
        check(&setup, &[])?;
        check(&setup, &actions)?;
    }
}

/// `Select::One` outside the learned counts is `Fatal::Metadata`.
#[test]
fn select_outside_the_counts_is_fatal() {
    let limits = FetchLimits {
        partition_max_bytes: 1 << 20,
        max_wait_ms: 500,
        min_bytes: 1,
    };
    for select in [
        Select::One {
            topic: 0,
            partition: 3,
        },
        Select::One {
            topic: 2,
            partition: 0,
        },
    ] {
        let mut consumer = Consumer::new(ConsumerConfig {
            select,
            start: Start::Tail(4),
            stop_timestamp_ms: None,
            limits,
        });
        assert_eq!(
            consumer.step(
                ConsumerIn {
                    now: Instant(0),
                    room: 1,
                    partitions: Some(&[3, 2]),
                    replies: &[],
                    lost: false,
                    rx: &RxBuf::new(64),
                },
                &mut ConsumerOut::default(),
            ),
            Err(Fatal::Metadata),
            "{select:?}"
        );
    }
}
