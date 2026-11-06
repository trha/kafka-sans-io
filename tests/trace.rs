//! Plan test 2: producer trace. Conn + Producer + Accumulator against the
//! model broker. A generated list of actions drives the broker, the network,
//! time, and the caller's appends. A healthy drain phase follows.

use std::collections::{BTreeMap, VecDeque};

use kafka_sans_io::{
    accumulator::Accumulator,
    batch::ProducerId,
    conn::{Config, Conn, ConnIn, ConnOut, NetCmd, NetEvent},
    error::Fatal,
    frame::RxBuf,
    producer::{Producer, ProducerIn, ProducerOut},
    testing::{
        model::{Broker, Connection, Fault, Kind},
        record::OwnedRecord,
    },
    time::{Duration, Instant},
};
use proptest::{prelude::*, sample::Index, test_runner::TestCaseError};

/// Topics and partition counts. Parts: a/0, a/1, a/2, b/0.
const TOPICS: [(&str, i32); 2] = [("a", 3), ("b", 1)];
/// The layout the producer's accumulator has. It checks it on each `Up`.
const PARTITIONS: [u16; 2] = [3, 1];
const PARTS: usize = 4;
const SLOT_BYTES: usize = 200;
const SMALL_TICK: Duration = Duration::from_millis(1);
const REQUEST_TIMEOUT: Duration = Duration::from_millis(50);
/// Drain rounds for every appended record to commit. A round is one tick of
/// 1 ms: at most 8 ms of backoff, the handshake, and two rounds per set.
const DRAIN_ROUNDS: usize = 100;
const RETRIABLE: [i16; 5] = [5, 6, 7, 19, 20];
/// COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR: a fresh broker's
/// InitProducerId.
const COORDINATOR: [i16; 3] = [14, 15, 16];
/// LEADER_NOT_AVAILABLE, NOT_LEADER_OR_FOLLOWER: Metadata right after a
/// broker restart.
const LEADER_MOVING: [i16; 2] = [5, 6];

#[derive(Clone, Debug)]
enum Action {
    /// 1 ms passes.
    Tick,
    /// The deadline of every request in flight passes.
    TickPast,
    /// Records with this many filler bytes each. The record value is its id
    /// then the filler.
    Append {
        part: Index,
        fillers: Vec<u8>,
    },
    /// The next `chunk` bytes of the broker's replies reach the receive buffer.
    Deliver {
        chunk: usize,
    },
    /// The broker handles the oldest request.
    Reply,
    /// The broker handles the oldest request; one partition of a Produce gets
    /// a retriable error.
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
    WrongPartitions,
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        40 => Just(Action::Tick),
        8 => Just(Action::TickPast),
        60 => (any::<Index>(), proptest::collection::vec(0u8..40, 1..=3))
            .prop_map(|(part, fillers)| Action::Append { part, fillers }),
        70 => prop_oneof![1usize..64, Just(usize::MAX)].prop_map(|chunk| Action::Deliver { chunk }),
        70 => Just(Action::Reply),
        10 => (any::<Index>(), proptest::sample::select(RETRIABLE.to_vec()))
            .prop_map(|(part, code)| Action::PartError { part, code }),
        4 => proptest::sample::select(COORDINATOR.to_vec())
            .prop_map(|code| Action::InitError { code }),
        4 => (any::<Index>(), proptest::sample::select(LEADER_MOVING.to_vec()))
            .prop_map(|(part, code)| Action::LeaderMoving { part, code }),
        10 => (0usize..=5).prop_map(|handled| Action::Drop { handled }),
        8 => Just(Action::ConnectFails),
        1 => Just(Action::WrongPartitions),
    ]
}

/// The trace ended on an allowed fatal.
struct Ended;

struct Run {
    max_in_flight: usize,
    now: Instant,
    conn: Conn,
    conn_out: ConnOut,
    rx: RxBuf,
    producer: Producer,
    producer_out: ProducerOut,
    accumulator: Accumulator<u32>,
    broker: Broker,
    connection: Option<Connection>,
    event: Option<NetEvent>,
    connect_fails: bool,
    /// Record ids appended, per part.
    appended: Vec<Vec<u32>>,
    next_record: u32,
    /// Tokens of the sealed sets not yet done, in seal order.
    tokens: VecDeque<u32>,
    next_token: u32,
    id: Option<ProducerId>,
    /// First bytes sent for each (producer id, topic, partition, base sequence).
    first_sends: BTreeMap<(i64, String, i32, i32), Vec<u8>>,
}

impl Run {
    fn new(max_in_flight: u8) -> Self {
        let config = Config {
            client_id: "trace",
            topics: TOPICS.iter().map(|&(name, _)| name.to_string()).collect(),
            request_timeout: REQUEST_TIMEOUT,
            backoff_min: Duration::from_millis(1),
            backoff_max: Duration::from_millis(8),
            max_in_flight,
            max_batch_bytes: SLOT_BYTES,
            produce_parts: PARTS,
        };
        let parts = PARTS;
        let conn = Conn::new(config);
        let conn_out = ConnOut::new(&conn);
        Run {
            max_in_flight: usize::from(max_in_flight),
            now: Instant(1_000_000_000),
            conn,
            conn_out,
            rx: RxBuf::new(1 << 20),
            producer: Producer::new(),
            producer_out: ProducerOut::default(),
            accumulator: Accumulator::new(parts, max_in_flight, SLOT_BYTES),
            broker: Broker::new(&TOPICS),
            connection: None,
            event: None,
            connect_fails: false,
            appended: vec![Vec::new(); parts],
            next_record: 0,
            tokens: VecDeque::new(),
            next_token: 0,
            id: None,
            first_sends: BTreeMap::new(),
        }
    }

    fn act(&mut self, action: &Action) {
        match action {
            Action::Tick => self.now = self.now.plus(SMALL_TICK),
            Action::TickPast => self.now = self.now.plus(REQUEST_TIMEOUT + SMALL_TICK),
            Action::Append { part, fillers } => {
                let part = part.index(self.appended.len());
                for &filler in fillers {
                    let id = self.next_record;
                    let mut value = id.to_be_bytes().to_vec();
                    value.resize(4 + usize::from(filler), 0xab);
                    let record = OwnedRecord {
                        timestamp_ms: (self.now.0 / 1_000_000) as i64,
                        key: None,
                        value: Some(value),
                        headers: Vec::new(),
                    };
                    let accumulator = &mut self.accumulator;
                    if record
                        .with_borrowed(|record| accumulator.append(part, record))
                        .is_ok()
                    {
                        self.appended[part].push(id);
                        self.next_record += 1;
                    }
                }
            }
            Action::Deliver { chunk } => self.deliver(*chunk),
            Action::Reply => {
                if let Some(connection) = &mut self.connection {
                    connection.handle_one(&mut self.broker, None);
                }
            }
            Action::PartError { part, code } => self.handle(Fault::Part(*part, *code)),
            Action::InitError { code } => self.handle(Fault::Init(*code)),
            Action::LeaderMoving { part, code } => self.handle(Fault::Leader(*part, *code)),
            Action::Drop { handled } => {
                if let Some(mut connection) = self.connection.take() {
                    for _ in 0..*handled {
                        connection.handle_one(&mut self.broker, None);
                    }
                    self.event = Some(NetEvent::Closed);
                }
            }
            Action::ConnectFails => self.connect_fails = true,
            Action::WrongPartitions => self.broker.wrong_partitions = true,
        }
    }

    /// The broker handles the oldest request with `fault`. A fault of
    /// another request kind leaves the reply as normal.
    fn handle(&mut self, fault: Fault) {
        if let Some(connection) = &mut self.connection {
            connection.handle_one(&mut self.broker, Some(fault));
        }
    }

    fn deliver(&mut self, chunk: usize) {
        if let Some(connection) = &mut self.connection {
            let count = connection.deliver(chunk, self.rx.spare());
            self.rx.filled(count);
        }
    }

    /// One pass of the composition loop, then the network moves the bytes
    /// and commands. Checks the per-step invariants.
    fn step(&mut self, step: usize) -> Result<Option<Ended>, TestCaseError> {
        let now = self.now;
        let net = self.event.take();
        let conn_in = ConnIn {
            now,
            net,
            rx: &mut self.rx,
        };
        if let Err(fatal) = self.conn.step(conn_in, &mut self.conn_out) {
            return self.fatal(step, fatal);
        }
        // The conn learns the counts on the first `Up`. A layout other than
        // the accumulator's stops the job, as fleeb-kafka does.
        if self.conn_out.up && self.conn.partitions() != Some(&PARTITIONS[..]) {
            return self.fatal(step, Fatal::Metadata);
        }
        let producer_in = ProducerIn {
            now,
            room: self.conn.room(),
            replies: &self.conn_out.replies,
            lost: self.conn_out.lost,
            accumulator: &self.accumulator,
        };
        if let Err(fatal) = self.producer.step(producer_in, &mut self.producer_out) {
            return self.fatal(step, fatal);
        }
        for &(set, parts) in &self.producer_out.acked {
            if let Some(token) = self.accumulator.ack(set, parts) {
                let expected = self.tokens.pop_front();
                prop_assert_eq!(Some(token), expected, "token order at step {}", step);
            }
        }
        if self.producer_out.close {
            self.conn.close(now, &mut self.conn_out);
        }
        if self.producer_out.init {
            self.conn.send_init_producer_id(now, &mut self.conn_out);
        }
        for &set in &self.producer_out.produce {
            self.conn
                .send_produce(now, self.accumulator.batches(set), &mut self.conn_out);
        }
        if let Some(id) = self.producer.id() {
            if self.accumulator.seal(id, self.next_token).is_some() {
                self.tokens.push_back(self.next_token);
            }
            self.next_token += 1;
        }

        let id = self.producer.id();
        prop_assert!(
            self.id.is_none() || id == self.id,
            "handshake: producer id changed from {:?} to {:?} at step {}",
            self.id,
            id,
            step
        );
        self.id = id;

        match self.conn_out.cmd {
            Some(NetCmd::Connect) => {
                prop_assert!(
                    self.connection.is_none(),
                    "connect on an open connection at step {}",
                    step
                );
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

        let bytes = self.conn_out.tx.pending().to_vec();
        self.conn_out.tx.consume(bytes.len());
        if let Some(connection) = &mut self.connection {
            for frame in connection.receive(&bytes) {
                let kind = Broker::inspect(&frame);
                let first = connection.requests == 1;
                match &kind {
                    Kind::ApiVersions => {
                        prop_assert!(
                            first,
                            "handshake: ApiVersions after another request at step {}",
                            step
                        )
                    }
                    Kind::Metadata => prop_assert!(
                        connection.versions_delivered,
                        "handshake: Metadata before the ApiVersions reply at step {}",
                        step
                    ),
                    Kind::InitProducerId | Kind::Produce(_) | Kind::ListOffsets | Kind::Fetch => {
                        prop_assert!(
                            connection.metadata_delivered,
                            "handshake: job request before the Metadata reply at step {}",
                            step
                        )
                    }
                }
                if let Kind::InitProducerId = kind {
                    prop_assert!(
                        self.id.is_none(),
                        "handshake: InitProducerId again after id {:?} at step {}",
                        self.id,
                        step
                    );
                }
                if let Kind::Produce(batches) = kind {
                    for batch in batches {
                        let key = (
                            batch.producer_id,
                            batch.topic.clone(),
                            batch.partition,
                            batch.base_sequence,
                        );
                        let earlier = self
                            .first_sends
                            .entry(key)
                            .or_insert_with(|| batch.bytes.clone());
                        prop_assert!(
                            *earlier == batch.bytes,
                            "same bytes: {}/{} base sequence {} resent with other bytes at step {}",
                            batch.topic,
                            batch.partition,
                            batch.base_sequence,
                            step
                        );
                    }
                }
            }
            let in_flight = connection.jobs_received - connection.jobs_delivered;
            prop_assert!(
                in_flight <= self.max_in_flight,
                "in flight: {} requests without their reply, max {} at step {}",
                in_flight,
                self.max_in_flight,
                step
            );
        } else {
            prop_assert!(
                bytes.is_empty(),
                "bytes sent with no connection at step {}",
                step
            );
        }
        Ok(None)
    }

    /// A fatal ends the trace. Only `Metadata`, after a wrong Metadata reply
    /// on this connection, is allowed.
    fn fatal(&self, step: usize, fatal: Fatal) -> Result<Option<Ended>, TestCaseError> {
        let wrong = self
            .connection
            .as_ref()
            .is_some_and(|connection| connection.wrong_metadata_delivered);
        prop_assert!(
            fatal == Fatal::Metadata && wrong,
            "fatal: {:?} at step {} (wrong Metadata delivered: {})",
            fatal,
            step,
            wrong
        );
        Ok(Some(Ended))
    }

    /// Nothing sealed waits for an ack and nothing waits for a seal.
    fn drained(&self) -> bool {
        self.accumulator.sets().count() == 0 && !self.accumulator.can_seal()
    }

    /// Record ids committed per part, in offset order.
    fn committed(&self) -> Vec<Vec<u32>> {
        TOPICS
            .iter()
            .flat_map(|&(name, count)| (0..count).map(move |partition| (name, partition)))
            .map(|(name, partition)| {
                self.broker.logs[&(name.to_string(), partition)]
                    .iter()
                    .map(|committed| {
                        let value = committed.record.value.as_deref().expect("record value");
                        u32::from_be_bytes(value[..4].try_into().expect("id bytes"))
                    })
                    .collect()
            })
            .collect()
    }
}

fn trace(max_in_flight: u8, actions: Vec<Action>) -> Result<(), TestCaseError> {
    let mut run = Run::new(max_in_flight);
    for (step, action) in actions.iter().enumerate() {
        run.act(action);
        if run.step(step)?.is_some() {
            return Ok(());
        }
    }

    // Drain: a healthy broker answers everything at once, and time moves.
    run.connect_fails = false;
    run.broker.wrong_partitions = false;
    let mut rounds = 0;
    while !run.drained() && rounds < DRAIN_ROUNDS {
        if let Some(connection) = &mut run.connection {
            while connection.handle_one(&mut run.broker, None) {}
        }
        run.deliver(usize::MAX);
        run.now = run.now.plus(SMALL_TICK);
        if run.step(actions.len() + rounds)?.is_some() {
            return Ok(());
        }
        rounds += 1;
    }
    prop_assert!(
        run.drained(),
        "progress: {} sets still sealed after {} drain rounds",
        run.accumulator.sets().count(),
        DRAIN_ROUNDS
    );

    let committed = run.committed();
    for (part, (committed, appended)) in committed.iter().zip(&run.appended).enumerate() {
        let mut committed_sorted = committed.clone();
        committed_sorted.sort_unstable();
        let mut appended_sorted = appended.clone();
        appended_sorted.sort_unstable();
        prop_assert_eq!(
            &committed_sorted,
            &appended_sorted,
            "exactly once: part {} commits each appended record once",
            part
        );
        prop_assert_eq!(
            committed,
            appended,
            "order: part {} commits in append order",
            part
        );
    }
    Ok(())
}

proptest! {
    #[test]
    fn producer_trace(
        max_in_flight in 1u8..=5,
        actions in proptest::collection::vec(action(), 0..=200),
    ) {
        trace(max_in_flight, actions)?;
    }
}
