//! Plan tests 3 and 4: the recovery job, and the restart rule.
//!
//! Test 3 fills the model broker directly, then runs Conn + Recovery to
//! `Done` under conn faults. Test 4 runs whole processes (recovery, then
//! producer) that crash and replay their source with the skip rule.

use kafka_sans_io::{
    accumulator::Accumulator,
    batch::HEADER_BYTES,
    conn::{Config, Conn, ConnIn, ConnOut, NetCmd, NetEvent},
    error::Fatal,
    frame::RxBuf,
    producer::{Producer, ProducerIn, ProducerOut},
    recovery::{Recovery, RecoveryConfig, RecoveryIn, RecoveryOut},
    testing::{
        model::{Broker, Connection, Fault},
        record::OwnedRecord,
    },
    time::{Duration, Instant},
};
use proptest::{prelude::*, sample::Index, test_runner::TestCaseError};

/// Topics and partition counts. Parts: a/0, a/1, a/2, b/0.
const TOPICS: [(&str, i32); 2] = [("a", 3), ("b", 1)];
const PARTS: usize = 4;
const HEADER: &str = "pos";
/// One source position (at most 4 records of about 70 bytes) fits an empty
/// slot.
const SLOT_BYTES: usize = 512;
const SMALL_TICK: Duration = Duration::from_millis(1);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(50);
const RETRIABLE: [i16; 5] = [5, 6, 7, 19, 20];
/// COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR: a fresh broker's
/// InitProducerId.
const COORDINATOR: [i16; 3] = [14, 15, 16];
/// LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER: Metadata right after a
/// broker restart.
const LEADER_MOVING: [i16; 2] = [5, 6];
/// Healthy rounds after the actions. Each has one tick of 1 ms, so the
/// backoff (8 ms at most) and every round trip fit many times over.
const DRAIN_ROUNDS: usize = 2000;

/// (topic, partition) of each part.
fn partition(part: usize) -> (&'static str, i32) {
    TOPICS
        .iter()
        .flat_map(|&(name, count)| (0..count).map(move |partition| (name, partition)))
        .nth(part)
        .expect("part in range")
}

/// Conn faults, and for test 4 the source feed and crashes.
#[derive(Clone, Debug)]
enum Action {
    /// 1 ms passes.
    Tick,
    /// The deadline of every request in flight passes.
    TickPast,
    /// The next source position goes to the accumulator (test 4 only).
    Feed,
    Deliver {
        chunk: usize,
    },
    /// The broker handles the oldest request.
    Reply,
    /// The broker handles the oldest request; one of its partitions gets a
    /// retriable error.
    PartError {
        part: Index,
        code: i16,
    },
    /// The broker handles the oldest request; an InitProducerId answers
    /// this code.
    InitError {
        code: i16,
    },
    /// The broker handles the oldest request; one partition of a Metadata
    /// reply gets this code and no leader.
    LeaderMoving {
        part: Index,
        code: i16,
    },
    /// The broker handles the first `handled` requests, then the socket
    /// closes before any reply.
    Drop {
        handled: usize,
    },
    ConnectFails,
    /// The process dies after the broker handled the first `handled`
    /// requests. The broker stays (test 4 only).
    Crash {
        handled: usize,
    },
}

fn fault() -> impl Strategy<Value = Action> {
    prop_oneof![
        40 => Just(Action::Tick),
        8 => Just(Action::TickPast),
        70 => prop_oneof![1usize..64, Just(usize::MAX)].prop_map(|chunk| Action::Deliver { chunk }),
        70 => Just(Action::Reply),
        10 => (any::<Index>(), proptest::sample::select(RETRIABLE.to_vec()))
            .prop_map(|(part, code)| Action::PartError { part, code }),
        4 => (any::<Index>(), proptest::sample::select(LEADER_MOVING.to_vec()))
            .prop_map(|(part, code)| Action::LeaderMoving { part, code }),
        10 => (0usize..=5).prop_map(|handled| Action::Drop { handled }),
        8 => Just(Action::ConnectFails),
    ]
}

/// Fewer faults than `fault()`, and mostly whole deliveries, so a process
/// often commits sets before it crashes.
fn restart_action() -> impl Strategy<Value = Action> {
    prop_oneof![
        30 => Just(Action::Tick),
        3 => Just(Action::TickPast),
        50 => Just(Action::Feed),
        80 => prop_oneof![1 => 1usize..64, 3 => Just(usize::MAX)]
            .prop_map(|chunk| Action::Deliver { chunk }),
        80 => Just(Action::Reply),
        6 => (any::<Index>(), proptest::sample::select(RETRIABLE.to_vec()))
            .prop_map(|(part, code)| Action::PartError { part, code }),
        2 => proptest::sample::select(COORDINATOR.to_vec())
            .prop_map(|code| Action::InitError { code }),
        2 => (any::<Index>(), proptest::sample::select(LEADER_MOVING.to_vec()))
            .prop_map(|(part, code)| Action::LeaderMoving { part, code }),
        4 => (0usize..=5).prop_map(|handled| Action::Drop { handled }),
        3 => Just(Action::ConnectFails),
        3 => (0usize..=5).prop_map(|handled| Action::Crash { handled }),
    ]
}

/// The network and the broker: everything a crash keeps.
struct World {
    now: Instant,
    broker: Broker,
    connection: Option<Connection>,
    event: Option<NetEvent>,
    connect_fails: bool,
    /// Requests the recovery sent, over every connection.
    requests: usize,
    /// Recovery steps that saw a lost request or asked for a close: each
    /// makes its round go again.
    redos: usize,
}

impl World {
    fn new() -> Self {
        World {
            now: Instant(1_000_000_000),
            broker: Broker::new(&TOPICS),
            connection: None,
            event: None,
            connect_fails: false,
            requests: 0,
            redos: 0,
        }
    }

    /// The conn and network actions. Returns false for an action it does not
    /// handle.
    fn act(&mut self, action: &Action, rx: &mut RxBuf) -> bool {
        match action {
            Action::Tick => self.now = self.now.plus(SMALL_TICK),
            Action::TickPast => self.now = self.now.plus(REQUEST_TIMEOUT + SMALL_TICK),
            Action::Deliver { chunk } => self.deliver(*chunk, rx),
            Action::Reply => {
                if let Some(connection) = &mut self.connection {
                    connection.handle_one(&mut self.broker, None);
                }
            }
            Action::PartError { part, code } => self.handle(Fault::Part(*part, *code)),
            Action::InitError { code } => self.handle(Fault::Init(*code)),
            Action::LeaderMoving { part, code } => self.handle(Fault::Leader(*part, *code)),
            Action::Drop { handled } => {
                if let Some(connection) = self.connection.take() {
                    self.handle_then_close(connection, *handled);
                    self.event = Some(NetEvent::Closed);
                }
            }
            Action::ConnectFails => self.connect_fails = true,
            Action::Feed | Action::Crash { .. } => return false,
        }
        true
    }

    /// The broker handles the oldest request with `fault`. A fault of
    /// another request kind leaves the reply as normal.
    fn handle(&mut self, fault: Fault) {
        if let Some(connection) = &mut self.connection {
            connection.handle_one(&mut self.broker, Some(fault));
        }
    }

    fn handle_then_close(&mut self, mut connection: Connection, handled: usize) {
        for _ in 0..handled {
            connection.handle_one(&mut self.broker, None);
        }
    }

    fn deliver(&mut self, chunk: usize, rx: &mut RxBuf) {
        if let Some(connection) = &mut self.connection {
            let count = connection.deliver(chunk, rx.spare());
            rx.filled(count);
        }
    }

    /// A healthy broker answers every request, and every reply arrives.
    fn answer_all(&mut self, rx: &mut RxBuf) {
        if let Some(connection) = &mut self.connection {
            while connection.handle_one(&mut self.broker, None) {}
        }
        self.deliver(usize::MAX, rx);
    }

    /// Carries out the conn's command and sends its bytes.
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

fn conn_config(max_in_flight: u8) -> Config {
    Config {
        client_id: "recovery",
        topics: TOPICS.iter().map(|&(name, _)| name.to_string()).collect(),
        request_timeout: REQUEST_TIMEOUT,
        backoff_min: Duration::from_millis(1),
        backoff_max: Duration::from_millis(8),
        max_in_flight,
        max_batch_bytes: SLOT_BYTES,
        produce_parts: PARTS,
    }
}

/// One step of the recovery composition: conn, then recovery, then sends.
fn recover_step(
    world: &mut World,
    conn: &mut Conn,
    conn_out: &mut ConnOut,
    rx: &mut RxBuf,
    recovery: &mut Recovery,
    recovery_out: &mut RecoveryOut,
) -> Result<(), Fatal> {
    let now = world.now;
    let net = world.event.take();
    conn.step(ConnIn { now, net, rx }, conn_out)?;
    let recovery_in = RecoveryIn {
        now,
        room: conn.room(),
        partitions: conn.partitions(),
        replies: &conn_out.replies,
        lost: conn_out.lost,
        rx,
    };
    recovery.step(recovery_in, recovery_out)?;
    world.requests += usize::from(recovery_out.list.is_some() || recovery_out.fetch);
    world.redos += usize::from(conn_out.lost || recovery_out.close);
    if recovery_out.close {
        conn.close(now, conn_out);
    }
    if let Some(timestamp) = recovery_out.list {
        conn.send_list_offsets(now, timestamp, conn_out);
    }
    if recovery_out.fetch {
        conn.send_fetch(now, recovery.limits(), recovery.fetches(), conn_out);
    }
    world.network(conn_out);
    Ok(())
}

// ---------------------------------------------------------------- Test 3

/// One record of the generated log. Every record has the header, as the
/// caller's contract says, except a deliberate break in `fill`.
#[derive(Clone, Debug)]
struct RecordSpec {
    /// The header value is null.
    null: bool,
    /// Another header comes before the position header.
    other: bool,
    /// Milliseconds after the previous record.
    step_ms: i64,
}

/// What the recovery returns for one part: the last record's offset and
/// header value.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Last {
    offset: i64,
    value: Option<Vec<u8>>,
}

fn contents() -> impl Strategy<Value = Vec<Vec<Vec<RecordSpec>>>> {
    let record = (
        proptest::bool::weighted(0.1),
        proptest::bool::weighted(0.3),
        0i64..=3,
    )
        .prop_map(|(null, other, step_ms)| RecordSpec {
            null,
            other,
            step_ms,
        });
    let batch = proptest::collection::vec(record, 1..=4);
    proptest::collection::vec(proptest::collection::vec(batch, 0..=6), PARTS)
}

/// Fills the broker. Each record's value is its id, and so is its header
/// value unless `null`. The last record of part `headless` has no position
/// header: a break of the caller's contract. Returns the largest timestamp.
fn fill(broker: &mut Broker, contents: &[Vec<Vec<RecordSpec>>], headless: Option<usize>) -> i64 {
    let mut timestamp = 1000;
    let mut id = 0u64;
    for (part, batches) in contents.iter().enumerate() {
        let (topic, index) = partition(part);
        let count: usize = batches.iter().map(Vec::len).sum();
        let mut at = 0;
        for batch in batches {
            let records: Vec<OwnedRecord> = batch
                .iter()
                .map(|spec| {
                    timestamp += spec.step_ms;
                    id += 1;
                    at += 1;
                    let value = id.to_be_bytes().to_vec();
                    let mut headers = Vec::new();
                    if spec.other {
                        headers.push(("other".to_string(), Some(vec![1])));
                    }
                    if headless != Some(part) || at < count {
                        let header = (!spec.null).then(|| value.clone());
                        headers.push((HEADER.to_string(), header));
                    }
                    OwnedRecord {
                        timestamp_ms: timestamp,
                        key: None,
                        value: Some(value),
                        headers,
                    }
                })
                .collect();
            broker.put(topic, index, &records);
        }
    }
    timestamp
}

/// The rule straight from the logs: per part, the header of the last record
/// at or after the floor, or none. `Fatal::Fetch` when that record has no
/// header.
fn last_records(broker: &Broker, floor_ms: i64) -> Result<Vec<Option<Last>>, Fatal> {
    (0..PARTS)
        .map(|part| {
            let (topic, index) = partition(part);
            let log = &broker.logs[&(topic.to_string(), index)];
            let Some(last) = log
                .iter()
                .skip_while(|committed| committed.record.timestamp_ms < floor_ms)
                .last()
            else {
                return Ok(None);
            };
            let (_, value) = last
                .record
                .headers
                .iter()
                .rfind(|(key, _)| key == HEADER)
                .ok_or(Fatal::Fetch)?;
            Ok(Some(Last {
                offset: last.offset,
                value: value.clone(),
            }))
        })
        .collect()
}

/// Runs Conn + Recovery to `Done` or a fatal: `actions`, then healthy
/// rounds. Fails on more requests than the rounds allow, or on no end.
fn consume(
    contents: &[Vec<Vec<RecordSpec>>],
    headless: Option<usize>,
    floor_ms: i64,
    fetch_max_bytes: i32,
    actions: &[Action],
) -> Result<Result<Vec<Option<Last>>, Fatal>, TestCaseError> {
    let mut world = World::new();
    fill(&mut world.broker, contents, headless);
    let mut conn = Conn::new(conn_config(1));
    let mut conn_out = ConnOut::new(&conn);
    let mut rx = RxBuf::new(1 << 20);
    let config = RecoveryConfig {
        header_key: HEADER,
        floor_timestamp_ms: floor_ms,
        fetch_max_bytes,
    };
    let mut recovery = Recovery::new(config);
    let mut recovery_out = RecoveryOut::default();

    let healthy = std::iter::repeat_n(None, DRAIN_ROUNDS);
    for (step, action) in actions.iter().map(Some).chain(healthy).enumerate() {
        if recovery.done() {
            break;
        }
        match action {
            Some(action) => {
                world.act(action, &mut rx);
            }
            None => {
                world.connect_fails = false;
                world.answer_all(&mut rx);
                world.now = world.now.plus(SMALL_TICK);
            }
        }
        let result = recover_step(
            &mut world,
            &mut conn,
            &mut conn_out,
            &mut rx,
            &mut recovery,
            &mut recovery_out,
        );
        // ListEnd, ListFloor, one Fetch, and one more for each round a
        // close or a lost request made go again.
        prop_assert!(
            world.requests <= 3 + world.redos,
            "bounded: {} requests after {} redone rounds at step {}",
            world.requests,
            world.redos,
            step
        );
        if let Err(fatal) = result {
            return Ok(Err(fatal));
        }
    }
    prop_assert!(
        recovery.done(),
        "progress: recovery in {:?} after the drain",
        recovery.phase()
    );
    Ok(Ok(recovery
        .results()
        .map(|found| {
            found.map(|found| Last {
                offset: found.offset,
                value: found.value().map(<[u8]>::to_vec),
            })
        })
        .collect()))
}

proptest! {
    #[test]
    fn recovery(
        contents in contents(),
        headless in prop_oneof![9 => Just(None), 1 => any::<Index>().prop_map(Some)],
        floor in any::<Index>(),
        extra in prop_oneof![0i32..=200, Just(1 << 20)],
        actions in proptest::collection::vec(fault(), 0..=150),
    ) {
        let headless = headless.map(|part| part.index(PARTS));
        let mut broker = Broker::new(&TOPICS);
        let last_ms = fill(&mut broker, &contents, headless);
        // The recovery's contract: the limit is at least the largest batch.
        // Below it, Redpanda may send a part nothing (parent scenario 5).
        let fetch_max_bytes = (broker
            .batches
            .values()
            .flatten()
            .map(|batch| batch.bytes.len() as i32)
            .max()
            .unwrap_or(0)
            + extra)
            .max(1);
        // From before the first record to after the last.
        let floor_ms = 999 + floor.index((last_ms - 998) as usize + 1) as i64;
        let expected = last_records(&broker, floor_ms);

        let clean = consume(&contents, headless, floor_ms, fetch_max_bytes, &[])?;
        prop_assert_eq!(&clean, &expected, "last record: floor {} ms", floor_ms);
        let faulted = consume(&contents, headless, floor_ms, fetch_max_bytes, &actions)?;
        prop_assert_eq!(&faulted, &clean, "faults: results differ from the clean run");
    }
}

// ---------------------------------------------------------------- Test 4

/// One source record: its part, and filler bytes after its id.
#[derive(Clone, Debug)]
struct SourceSpec {
    part: Index,
    filler: u8,
}

/// The records of each source position, 1 to 3 over random parts.
fn source() -> impl Strategy<Value = Vec<Vec<SourceSpec>>> {
    let record = (any::<Index>(), 0u8..30).prop_map(|(part, filler)| SourceSpec { part, filler });
    proptest::collection::vec(proptest::collection::vec(record, 1..=3), 1..=30)
}

/// A source record as the caller appends it.
#[derive(Clone, Debug)]
struct SourceRecord {
    part: usize,
    /// Unique over the source; the first 4 bytes of the value.
    id: u32,
    plain: OwnedRecord,
}

/// Records of each position, in source order. Positions count from 1.
fn expand(source: &[Vec<SourceSpec>]) -> Vec<Vec<SourceRecord>> {
    let mut id = 0;
    source
        .iter()
        .enumerate()
        .map(|(at, records)| {
            let position = at as u64 + 1;
            records
                .iter()
                .map(|spec| {
                    id += 1;
                    let mut value = u32::to_be_bytes(id).to_vec();
                    value.resize(4 + usize::from(spec.filler), 0xab);
                    SourceRecord {
                        part: spec.part.index(PARTS),
                        id,
                        plain: OwnedRecord {
                            timestamp_ms: 0,
                            key: None,
                            value: Some(value),
                            headers: vec![(
                                HEADER.to_string(),
                                Some(position.to_be_bytes().to_vec()),
                            )],
                        },
                    }
                })
                .collect()
        })
        .collect()
}

enum Phase {
    Recover {
        // Boxed: its part list is a fixed array of `MAX_PARTS` entries.
        recovery: Box<Recovery>,
        recovery_out: RecoveryOut,
    },
    Produce {
        /// `p[i]`: the position of the last committed header record per part.
        skip: Vec<Option<u64>>,
        /// Positions fed so far in this process.
        fed: usize,
        producer: Producer,
        producer_out: ProducerOut,
    },
}

/// Every machine of one process. A crash drops it.
struct Process {
    conn: Conn,
    conn_out: ConnOut,
    rx: RxBuf,
    accumulator: Accumulator<usize>,
    phase: Phase,
}

impl Process {
    fn new(max_in_flight: u8, fetch_max_bytes: i32) -> Self {
        let conn = Conn::new(conn_config(max_in_flight));
        let conn_out = ConnOut::new(&conn);
        let config = RecoveryConfig {
            header_key: HEADER,
            floor_timestamp_ms: 0,
            fetch_max_bytes,
        };
        Process {
            conn,
            conn_out,
            rx: RxBuf::new(1 << 20),
            accumulator: Accumulator::new(PARTS, max_in_flight, SLOT_BYTES),
            phase: Phase::Recover {
                recovery: Box::new(Recovery::new(config)),
                recovery_out: RecoveryOut::default(),
            },
        }
    }

    /// One pass of the plan's composition loop.
    fn step(&mut self, world: &mut World) -> Result<(), Fatal> {
        match &mut self.phase {
            Phase::Recover {
                recovery,
                recovery_out,
            } => {
                recover_step(
                    world,
                    &mut self.conn,
                    &mut self.conn_out,
                    &mut self.rx,
                    recovery,
                    recovery_out,
                )?;
                if recovery.done() {
                    let skip = recovery
                        .results()
                        .map(|found| {
                            found.map(|found| {
                                let value = found.value().expect("a position value");
                                u64::from_be_bytes(value.try_into().expect("8 position bytes"))
                            })
                        })
                        .collect();
                    self.phase = Phase::Produce {
                        skip,
                        fed: 0,
                        producer: Producer::new(),
                        producer_out: ProducerOut::default(),
                    };
                }
            }
            Phase::Produce {
                producer,
                producer_out,
                ..
            } => {
                let now = world.now;
                let net = world.event.take();
                let conn_in = ConnIn {
                    now,
                    net,
                    rx: &mut self.rx,
                };
                self.conn.step(conn_in, &mut self.conn_out)?;
                let producer_in = ProducerIn {
                    now,
                    room: self.conn.room(),
                    replies: &self.conn_out.replies,
                    lost: self.conn_out.lost,
                    accumulator: &self.accumulator,
                };
                producer.step(producer_in, producer_out)?;
                for &(set, parts) in &producer_out.acked {
                    self.accumulator.ack(set, parts);
                }
                if producer_out.close {
                    self.conn.close(now, &mut self.conn_out);
                }
                if producer_out.init {
                    self.conn.send_init_producer_id(now, &mut self.conn_out);
                }
                for &set in &producer_out.produce {
                    self.conn
                        .send_produce(now, self.accumulator.batches(set), &mut self.conn_out);
                }
                if let Some(id) = producer.id() {
                    self.accumulator.seal(id, 0);
                }
                world.network(&mut self.conn_out);
            }
        }
        Ok(())
    }

    /// Appends the next source position when every part has room, with the
    /// skip rule. Checks `skip safe` on each skipped record.
    fn feed(&mut self, world: &World, source: &[Vec<SourceRecord>]) -> Result<(), TestCaseError> {
        let Phase::Produce { skip, fed, .. } = &mut self.phase else {
            return Ok(());
        };
        let Some(records) = source.get(*fed) else {
            return Ok(());
        };
        let position = *fed as u64 + 1;
        let timestamp_ms = (world.now.0 / 1_000_000) as i64;
        let kept = records
            .iter()
            .filter(|record| skip[record.part].is_none_or(|last| position > last));
        for record in records {
            if kept.clone().any(|kept| kept.id == record.id) {
                continue;
            }
            prop_assert!(
                committed_ids(&world.broker, record.part).contains(&record.id),
                "skip safe: record {} of position {} on part {} skipped, not committed",
                record.id,
                position,
                record.part
            );
        }
        // The record size bound: header room aside, the wire size with the
        // largest timestamp and offset deltas.
        let mut need = [0usize; PARTS];
        for record in kept.clone() {
            need[record.part] += record.plain.wire_size(0, 0) + 15;
        }
        let most = need.iter().max().copied().unwrap_or(0);
        assert!(
            most <= SLOT_BYTES - HEADER_BYTES,
            "position {position} needs {most} bytes, more than an empty slot"
        );
        if (0..PARTS).any(|part| need[part] > self.accumulator.free(part)) {
            return Ok(());
        }
        for record in kept {
            let plain = OwnedRecord {
                timestamp_ms,
                ..record.plain.clone()
            };
            let accumulator = &mut self.accumulator;
            let appended =
                plain.with_borrowed(|crate_record| accumulator.append(record.part, crate_record));
            prop_assert!(appended.is_ok(), "append after the room check");
        }
        *fed += 1;
        Ok(())
    }

    /// Fed every position and nothing waits to be sealed or acked.
    fn finished(&self, positions: usize) -> bool {
        matches!(self.phase, Phase::Produce { fed, .. } if fed == positions)
            && self.accumulator.sets().count() == 0
            && !self.accumulator.can_seal()
    }
}

/// Ids of the records committed to a part, in offset order.
fn committed_ids(broker: &Broker, part: usize) -> Vec<u32> {
    let (topic, index) = partition(part);
    broker.logs[&(topic.to_string(), index)]
        .iter()
        .map(|committed| {
            let value = committed.record.value.as_deref().expect("record value");
            u32::from_be_bytes(value[..4].try_into().expect("id bytes"))
        })
        .collect()
}

fn restart(
    max_in_flight: u8,
    fetch_max_bytes: i32,
    source: &[Vec<SourceSpec>],
    actions: &[Action],
) -> Result<(), TestCaseError> {
    let source = expand(source);
    let mut world = World::new();
    let mut process = Process::new(max_in_flight, fetch_max_bytes);

    for (step, action) in actions.iter().enumerate() {
        match action {
            Action::Feed => process.feed(&world, &source)?,
            Action::Crash { handled } => {
                if let Some(connection) = world.connection.take() {
                    world.handle_then_close(connection, *handled);
                }
                world.event = None;
                process = Process::new(max_in_flight, fetch_max_bytes);
            }
            action => {
                world.act(action, &mut process.rx);
            }
        }
        let result = process.step(&mut world);
        prop_assert!(result.is_ok(), "fatal: {:?} at step {}", result, step);
    }

    // Drain: a healthy broker, time moves, the source feeds to its end.
    world.connect_fails = false;
    let mut rounds = 0;
    while !process.finished(source.len()) && rounds < DRAIN_ROUNDS {
        world.answer_all(&mut process.rx);
        world.now = world.now.plus(SMALL_TICK);
        process.feed(&world, &source)?;
        let result = process.step(&mut world);
        prop_assert!(
            result.is_ok(),
            "fatal: {:?} at drain round {}",
            result,
            rounds
        );
        rounds += 1;
    }
    prop_assert!(
        process.finished(source.len()),
        "progress: not finished after {} drain rounds",
        DRAIN_ROUNDS
    );

    for part in 0..PARTS {
        let sent: Vec<u32> = source
            .iter()
            .flatten()
            .filter(|record| record.part == part)
            .map(|record| record.id)
            .collect();
        prop_assert_eq!(
            &committed_ids(&world.broker, part),
            &sent,
            "exactly once across restarts: part {} commits each source record once, in order",
            part
        );
    }
    Ok(())
}

proptest! {
    #[test]
    fn restart_rule(
        max_in_flight in 1u8..=5,
        // At least `SLOT_BYTES`, the largest batch the producer seals.
        fetch_max_bytes in prop_oneof![SLOT_BYTES as i32..=SLOT_BYTES as i32 + 300, Just(1 << 20)],
        source in source(),
        actions in proptest::collection::vec(restart_action(), 0..=300),
    ) {
        restart(max_in_flight, fetch_max_bytes, &source, &actions)?;
    }
}
