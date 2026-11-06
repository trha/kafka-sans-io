//! The conn makes no heap allocation after `Conn::new`.
//!
//! A counting global allocator counts the allocations of this thread while
//! `counted` runs. Only crate calls run inside it: the model broker, the
//! network copy, and the checks run outside. Every buffer and machine is
//! made before the count. Then one counted window runs the whole life of a
//! conn: connect, ApiVersions, Metadata, both ListOffsets rounds, the Fetch
//! of the last records, a close, a reconnect with a new
//! handshake, InitProducerId, and many steps of appends, seals, Produce
//! sends, and acks. The window must allocate nothing.

use proptest::{prelude::*, sample::Index, strategy::ValueTree, test_runner::TestRunner};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    time::Duration,
};

use kafka_sans_io::{
    accumulator::Accumulator,
    batch::Record,
    conn::{Config, Conn, ConnIn, ConnOut, FetchLimits, NetCmd, NetEvent},
    consumer::{
        self, Consumer, ConsumerConfig, ConsumerIn, ConsumerOut, Phase as ConsumerPhase, Select,
        Start,
    },
    error::Fatal,
    frame::RxBuf,
    producer::{Producer, ProducerIn, ProducerOut},
    recovery::{Phase, Recovery, RecoveryConfig, RecoveryIn, RecoveryOut},
    testing::{
        model::{Broker, Connection, Fault},
        record::OwnedRecord,
    },
    time::Instant,
};

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static COUNT: Cell<u64> = const { Cell::new(0) };
}

/// Counts allocations of the current thread while `COUNTING` is set.
struct Counting;

fn note() {
    // `try_with`: the allocator may run while the thread's locals are gone.
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            COUNT.with(|count: &Cell<u64>| count.set(count.get() + 1));
        }
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Runs `body` and returns its result and the allocations it made.
fn counted<R>(body: impl FnOnce() -> R) -> (R, u64) {
    let before = COUNT.with(Cell::get);
    COUNTING.with(|on| on.set(true));
    let result = body();
    COUNTING.with(|on| on.set(false));
    (result, COUNT.with(Cell::get) - before)
}

const TOPICS: [(&str, i32); 2] = [("alloc-a", 3), ("alloc-b", 1)];
const PARTS: usize = 4;
const SLOT_BYTES: usize = 4096;
const HEADER_KEY: &str = "pos";

fn config(max_in_flight: u8) -> Config {
    Config {
        client_id: "alloc",
        topics: TOPICS.iter().map(|&(name, _)| name.to_string()).collect(),
        request_timeout: Duration::from_secs(30),
        backoff_min: Duration::from_millis(10),
        backoff_max: Duration::from_secs(1),
        max_in_flight,
        max_batch_bytes: SLOT_BYTES,
        produce_parts: PARTS,
    }
}

/// The model broker on one connection that answers every request at once.
struct World {
    broker: Broker,
    connection: Option<Connection>,
    net: Option<NetEvent>,
    /// The next request handled gets this fault.
    fault: Option<Fault>,
}

impl World {
    fn new(broker: Broker) -> Self {
        World {
            broker,
            connection: None,
            net: None,
            fault: None,
        }
    }

    /// Carries out the conn's command, hands the sent frames to the broker,
    /// and delivers every reply.
    fn network(&mut self, conn_out: &mut ConnOut, rx: &mut RxBuf) {
        match conn_out.cmd {
            Some(NetCmd::Connect) => {
                self.connection = Some(Connection::default());
                self.net = Some(NetEvent::Connected);
            }
            Some(NetCmd::Close) => self.connection = None,
            None => {}
        }
        let Some(connection) = &mut self.connection else {
            return;
        };
        let pending = conn_out.tx.pending().len();
        connection.receive(conn_out.tx.pending());
        conn_out.tx.consume(pending);
        let fault = self.fault.take();
        let mut first = true;
        while connection.handle_one(&mut self.broker, fault.filter(|_| first)) {
            first = false;
        }
        loop {
            let count = connection.deliver(usize::MAX, rx.spare());
            if count == 0 {
                break;
            }
            rx.filled(count);
        }
    }
}

fn now(step: u64) -> Instant {
    Instant(step * 1_000_000)
}

#[test]
fn counter_counts() {
    let (boxed, allocations) = counted(|| Box::new(1u64));
    assert_eq!((*boxed, allocations), (1, 1));
}

/// Batches of 3 records put in every part before the start. The recovery
/// reads the last one.
const BATCHES: usize = 60;
/// Producer steps after the reconnect.
const PRODUCE_STEPS: u64 = 2000;

/// A record whose header value is `position`.
fn plain(timestamp_ms: i64, position: u64) -> OwnedRecord {
    OwnedRecord {
        timestamp_ms,
        key: Some(b"key".to_vec()),
        value: Some(vec![3; 40]),
        headers: vec![(
            HEADER_KEY.to_string(),
            Some(position.to_be_bytes().to_vec()),
        )],
    }
}

/// What one counted step did.
#[derive(Default)]
struct Work {
    acks: u64,
    records: u64,
}

fn lifecycle(max_in_flight: u8) {
    let mut broker = Broker::new(&TOPICS);
    for &(topic, partitions) in &TOPICS {
        for partition in 0..partitions {
            for batch in 0..BATCHES {
                let records: Vec<OwnedRecord> = (0..3)
                    .map(|at| plain(batch as i64, (3 * batch + at) as u64))
                    .collect();
                broker.put(topic, partition, &records);
            }
        }
    }
    let config = config(max_in_flight);
    let parts = PARTS;
    let mut world = World::new(broker);
    let mut conn = Conn::new(config);
    let mut conn_out = ConnOut::new(&conn);
    let mut rx = RxBuf::new(1 << 20);
    let recovery_config = RecoveryConfig {
        header_key: HEADER_KEY,
        floor_timestamp_ms: 0,
        fetch_max_bytes: 400,
    };
    let mut recovery = Recovery::new(recovery_config);
    let mut recovery_out = RecoveryOut::default();
    let mut producer = Producer::new();
    let mut producer_out = ProducerOut::default();
    let mut accumulator: Accumulator<u64> = Accumulator::new(parts, max_in_flight, SLOT_BYTES);
    let value = [7u8; 64];
    let (mut allocations, mut connects, mut fetch_steps) = (0, 0, 0);
    let (mut acks, mut appended, mut produce_steps) = (0, 0, 0);

    for step in 0.. {
        assert!(step < 20_000, "lifecycle not done: {:?}", recovery.phase());
        if produce_steps == PRODUCE_STEPS {
            break;
        }
        let now = now(step);
        let net = world.net.take();
        let fetching = recovery.phase() == Phase::Fetch;
        let producing = recovery.done();
        let position = step.to_be_bytes();
        let headers = [(HEADER_KEY, Some(&position[..]))];
        let record = Record {
            timestamp_ms: step as i64,
            key: Some(b"key"),
            value: Some(&value),
            headers: &headers,
        };
        let (result, count) = counted(|| -> Result<Work, Fatal> {
            conn.step(
                ConnIn {
                    now,
                    net,
                    rx: &mut rx,
                },
                &mut conn_out,
            )?;
            if !recovery.done() {
                let recovery_in = RecoveryIn {
                    now,
                    room: conn.room(),
                    partitions: conn.partitions(),
                    replies: &conn_out.replies,
                    lost: conn_out.lost,
                    rx: &rx,
                };
                recovery.step(recovery_in, &mut recovery_out)?;
                if recovery_out.close {
                    conn.close(now, &mut conn_out);
                }
                if let Some(timestamp) = recovery_out.list {
                    conn.send_list_offsets(now, timestamp, &mut conn_out);
                }
                if recovery_out.fetch {
                    conn.send_fetch(now, recovery.limits(), recovery.fetches(), &mut conn_out);
                }
                // The producer starts on a new connection.
                if recovery.done() {
                    conn.close(now, &mut conn_out);
                }
                return Ok(Work::default());
            }

            let producer_in = ProducerIn {
                now,
                room: conn.room(),
                replies: &conn_out.replies,
                lost: conn_out.lost,
                accumulator: &accumulator,
            };
            producer.step(producer_in, &mut producer_out)?;
            let mut work = Work::default();
            for &(set, parts) in &producer_out.acked {
                work.acks += u64::from(accumulator.ack(set, parts).is_some());
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
            if let Some(id) = producer.id()
                && accumulator.sets().count() < usize::from(max_in_flight)
            {
                for part in 0..parts {
                    accumulator
                        .append(part, &record)
                        .expect("one record fits a slot");
                    work.records += 1;
                }
                accumulator.seal(id, step).expect("a set with records");
            }
            Ok(work)
        });
        let work = result.expect("no fatal with a healthy broker");
        allocations += count;
        connects += u64::from(conn_out.cmd == Some(NetCmd::Connect));
        fetch_steps += u64::from(fetching);
        produce_steps += u64::from(producing);
        acks += work.acks;
        appended += work.records;
        world.network(&mut conn_out, &mut rx);
    }

    assert_eq!(
        allocations, 0,
        "allocations over the conn's life, max_in_flight {max_in_flight}"
    );
    // The window did real work: the recovery read the last record of every
    // part, the conn reconnected, and the producer acked a set on most steps
    // with every record of the acked sets committed.
    assert!(fetch_steps >= 1, "no Fetch step");
    let last = (BATCHES * 3 - 1) as u64;
    for (part, found) in recovery.results().enumerate() {
        let found = found.expect("every part has records");
        assert_eq!(found.offset, last as i64, "part {part}: the last record");
        assert_eq!(found.value(), Some(&last.to_be_bytes()[..]), "part {part}");
    }
    assert_eq!(
        connects, 2,
        "one connect for recovery, one for the producer"
    );
    // Less the steps of the backoff, the handshake, and InitProducerId.
    assert!(
        acks >= PRODUCE_STEPS / 2 - 50,
        "only {acks} sets acked in {PRODUCE_STEPS} producer steps"
    );
    let committed: usize = world.broker.logs.values().map(Vec::len).sum();
    // Records put in the broker before the start.
    let recovered = parts * BATCHES * 3;
    assert!(
        (committed - recovered) as u64 + parts as u64 * u64::from(max_in_flight) >= appended,
        "{} records committed of {appended} appended",
        committed - recovered
    );
}

#[test]
fn conn_lifecycle_allocates_nothing() {
    for max_in_flight in [1, 5] {
        lifecycle(max_in_flight);
    }
}

/// Plan test 6 of `plans/consumer.md`: a consumer run with drops and
/// retriable codes allocates nothing after `Conn::new`. Records are
/// appended while it runs.
#[test]
fn consumer_run_allocates_nothing() {
    let mut broker = Broker::new(&TOPICS);
    for &(topic, partitions) in &TOPICS {
        for partition in 0..partitions {
            for batch in 0..BATCHES {
                broker.put(
                    topic,
                    partition,
                    &(0..3)
                        .map(|at| plain(batch as i64, (3 * batch + at) as u64))
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
    let mut world = World::new(broker);
    let mut conn = Conn::new(Config {
        max_in_flight: 1,
        max_batch_bytes: 0,
        produce_parts: 0,
        ..config(1)
    });
    let mut conn_out = ConnOut::new(&conn);
    let mut rx = RxBuf::new(1 << 20);
    let mut consumer = Consumer::new(ConsumerConfig {
        select: Select::All,
        start: Start::Timestamp(BATCHES as i64 / 2),
        stop_timestamp_ms: None,
        limits: FetchLimits {
            partition_max_bytes: 400,
            max_wait_ms: 500,
            min_bytes: 1,
        },
    });
    let mut consumer_out = ConsumerOut::default();
    let (mut allocations, mut connects, mut closes, mut records) = (0, 0, 0, 0u64);

    for step in 0..3000u64 {
        let now = now(step);
        let (result, count) = counted(|| -> Result<u64, Fatal> {
            conn.step(
                ConnIn {
                    now,
                    net: world.net.take(),
                    rx: &mut rx,
                },
                &mut conn_out,
            )?;
            consumer.step(
                ConsumerIn {
                    now,
                    room: conn.room(),
                    partitions: conn.partitions(),
                    replies: &conn_out.replies,
                    lost: conn_out.lost,
                    rx: &rx,
                },
                &mut consumer_out,
            )?;
            let mut read = 0;
            for delivery in &consumer_out.deliveries {
                for record in
                    consumer::delivered(rx.slice(delivery.records), delivery.from, delivery.until)
                {
                    record?;
                    read += 1;
                }
            }
            if consumer_out.close {
                conn.close(now, &mut conn_out);
            }
            if let Some(timestamp) = consumer_out.list {
                conn.send_list_offsets(now, timestamp, &mut conn_out);
            }
            if consumer_out.fetch {
                conn.send_fetch(now, consumer.limits(), consumer.fetches(), &mut conn_out);
            }
            Ok(read)
        });
        records += result.expect("no fatal with a healthy broker");
        allocations += count;
        connects += u64::from(conn_out.cmd == Some(NetCmd::Connect));
        closes += u64::from(consumer_out.close);
        // Faults and appends run outside the count.
        match step % 400 {
            100 => {
                // The socket drops with a request in flight.
                world.connection = None;
                world.net = Some(NetEvent::Closed);
            }
            200 => world.fault = Some(Fault::Part(any_index(), 6)),
            300 => {
                for &(topic, partitions) in &TOPICS {
                    for partition in 0..partitions {
                        world.broker.put(
                            topic,
                            partition,
                            &(0..3)
                                .map(|at| plain(1000 + step as i64, at))
                                .collect::<Vec<_>>(),
                        );
                    }
                }
            }
            _ => {}
        }
        world.network(&mut conn_out, &mut rx);
    }

    assert_eq!(allocations, 0, "allocations over the consumer's life");
    assert_eq!(consumer.phase(), ConsumerPhase::Fetch);
    assert!(connects >= 8, "only {connects} connects");
    assert!(closes >= 5, "only {closes} retriable closes");
    let parts: usize = TOPICS.iter().map(|&(_, count)| count as usize).sum();
    // From the middle batch on, plus the appended batches.
    assert_eq!(
        records,
        (parts * (BATCHES / 2) * 3 + parts * 7 * 3) as u64,
        "records read"
    );
    for part in consumer.selected() {
        assert!(consumer.caught_up(part), "part {part} caught up");
    }
}

/// One part index, the same on every run.
fn any_index() -> Index {
    any::<Index>()
        .new_tree(&mut TestRunner::deterministic())
        .expect("Index has no filter")
        .current()
}
