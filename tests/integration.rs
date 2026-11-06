//! Scenarios against a real Redpanda. See `plans/kafka-sans-io.md`,
//! "Integration tests", and test 7 of `plans/consumer.md`. Run inside
//! `nix develop`.
//!
//! Records are random from a seed. Set `KAFKA_SEED` to replay one; each test
//! prints its seed.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime},
};

use compio::time::sleep;
use futures_util::future;
use kafka_sans_io::{
    batch::HEADER_BYTES,
    conn::{Config, FetchLimits, PartCode},
    consumer::{Consumer, ConsumerConfig, Phase, Select, Start},
    error::Fatal,
    recovery::RecoveryConfig,
    testing::{
        driver::{self, Consumed, FetchReply, Options, Outcome, RecoverOutcome},
        record::{OwnedRecord, plain},
        redpanda::{ConsumedRecord, Proxy, Redpanda},
    },
};
use local_event::Event;
use proptest::{
    collection::vec,
    option,
    prelude::*,
    strategy::ValueTree,
    test_runner::{RngAlgorithm, TestRng, TestRunner},
};

const PRODUCE: i16 = 0;
const INIT_PRODUCER_ID: i16 = 22;
const SLOT_BYTES: usize = 128 << 10;
const LIMIT: Duration = Duration::from_secs(120);
/// The position header, as fleeb-kafka names it. Its 9 characters keep it
/// apart from the random header keys of `plain` (8 at most).
const POSITION: &str = "fleeb-pos";

/// Draws values from proptest strategies with a seeded generator. No
/// shrinking: a real broker is not deterministic, so only the data replays.
struct Draw(TestRunner);

impl Draw {
    fn new(test: &str) -> Draw {
        let seed: u64 = std::env::var("KAFKA_SEED")
            .ok()
            .map(|seed| seed.parse().expect("KAFKA_SEED is a u64"))
            .unwrap_or_else(|| {
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64
            });
        eprintln!("{test}: KAFKA_SEED={seed}");
        let mut key = [0u8; 32];
        key[..8].copy_from_slice(&seed.to_le_bytes());
        let rng = TestRng::from_seed(RngAlgorithm::ChaCha, &key);
        Draw(TestRunner::new_with_rng(Default::default(), rng))
    }

    fn draw<T: std::fmt::Debug>(&mut self, strategy: impl Strategy<Value = T>) -> T {
        strategy
            .new_tree(&mut self.0)
            .expect("the strategy has no filter")
            .current()
    }
}

/// Sets of records over `parts`. Timestamps go up, down, and repeat, within
/// an hour of now.
fn plan(
    draw: &mut Draw,
    parts: usize,
    sets: usize,
    per_set: usize,
) -> Vec<Vec<(usize, OwnedRecord)>> {
    let now = now_ms();
    let step = prop_oneof![Just(0i64), -5_000i64..=5_000, -7_200_000i64..=7_200_000];
    let record = (
        0..parts,
        plain(step, 32),
        option::weighted(0.875, vec(any::<u8>(), 0..=256)),
    )
        .prop_map(|(part, mut record, value)| {
            record.value = value;
            (part, record)
        });
    let mut plan = draw.draw(vec(vec(record, per_set), sets));
    let mut timestamp = now;
    for (_, record) in plan.iter_mut().flatten() {
        timestamp = (timestamp + record.timestamp_ms).clamp(now - 3_600_000, now + 3_600_000);
        record.timestamp_ms = timestamp;
    }
    plan
}

fn config(topics: &[(&str, u16)], max_in_flight: u8) -> Config {
    Config {
        client_id: "integration",
        topics: topics.iter().map(|&(name, _)| name.to_string()).collect(),
        request_timeout: Duration::from_secs(5),
        backoff_min: Duration::from_millis(10),
        backoff_max: Duration::from_millis(500),
        max_in_flight,
        max_batch_bytes: SLOT_BYTES,
        produce_parts: topics.iter().map(|&(_, count)| usize::from(count)).sum(),
    }
}

async fn produce(
    addr: SocketAddr,
    topics: &[(&str, u16)],
    plan: &[Vec<(usize, OwnedRecord)>],
    max_in_flight: u8,
    sets_per_tick: usize,
    progress: impl AsyncFnMut(usize),
) -> Outcome {
    let options = Options {
        max_in_flight,
        sets_per_tick,
        limit: LIMIT,
        stop_on_lost: false,
        partitions: topics.iter().map(|&(_, count)| count).collect(),
    };
    driver::produce(
        addr,
        config(topics, max_in_flight),
        SLOT_BYTES,
        plan,
        &options,
        progress,
    )
    .await
}

/// Runs the recovery job on `topics` with READ_UNCOMMITTED.
async fn recover(
    addr: SocketAddr,
    topics: &[(&str, u16)],
    floor_timestamp_ms: i64,
    fetch_max_bytes: i32,
) -> RecoverOutcome {
    let recovery = RecoveryConfig {
        header_key: POSITION,
        floor_timestamp_ms,
        fetch_max_bytes,
    };
    driver::recover(addr, config(topics, 1), recovery, LIMIT).await
}

/// The position of each part's result: its header value as a big-endian
/// u64.
fn positions(recovery: &RecoverOutcome) -> Vec<Option<u64>> {
    let results = recovery.result.as_ref().expect("recovery done");
    results
        .iter()
        .map(|found| {
            found.map(|found| {
                let value = found.value().expect("a position value");
                u64::from_be_bytes(value.try_into().expect("8 position bytes"))
            })
        })
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Produce replies with a code other than 0, by connection.
fn odd_codes(outcome: &Outcome) -> Vec<(usize, Vec<PartCode>)> {
    outcome
        .replies
        .iter()
        .filter(|(_, codes)| codes.iter().any(|code| code.code != 0))
        .map(|(connection, codes)| (*connection, codes.to_vec()))
        .collect()
}

/// High watermark of each partition, from `rpk topic describe -p`.
async fn high_watermarks(broker: &Redpanda, name: &str) -> Vec<i64> {
    let out = broker.rpk(&["topic", "describe", name, "-p"], b"").await;
    let out = String::from_utf8_lossy(&out);
    let mut lines = out.lines().filter(|line| !line.trim().is_empty());
    let header = lines.next().expect("rpk describe header");
    let columns = header.split_whitespace().collect::<Vec<_>>();
    let at = |column: &str| {
        columns
            .iter()
            .position(|&found| found == column)
            .unwrap_or_else(|| panic!("rpk describe column {column}: {out}"))
    };
    let (partition, high) = (at("PARTITION"), at("HIGH-WATERMARK"));
    let mut marks = lines
        .map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            let parse = |at: usize| fields[at].parse::<i64>().expect("rpk describe number");
            (parse(partition), parse(high))
        })
        .collect::<Vec<_>>();
    marks.sort();
    marks.into_iter().map(|(_, high)| high).collect()
}

/// Reads every topic back with `rpk`. Per partition: each planned record
/// once, in append order, with its key, value, headers, and CreateTime.
/// Null reads as empty.
async fn check(broker: &Redpanda, topics: &[(&str, u16)], plan: &[Vec<(usize, OwnedRecord)>]) {
    let empty = |bytes: &Option<Vec<u8>>| bytes.clone().unwrap_or_default();
    let mut first_part = 0;
    for &(name, partitions) in topics {
        let parts = first_part..first_part + usize::from(partitions);
        let mut expected = vec![Vec::new(); usize::from(partitions)];
        for (part, record) in plan.iter().flatten() {
            if parts.contains(part) {
                expected[part - first_part].push(record);
            }
        }
        first_part = parts.end;

        let counts = expected.iter().map(|records| records.len() as i64);
        assert_eq!(
            high_watermarks(broker, name).await,
            counts.collect::<Vec<_>>(),
            "{name}: each record once (high watermark per partition)"
        );
        let consumed = broker
            .consume(name, expected.iter().map(Vec::len).sum())
            .await;
        for (partition, expected) in expected.iter().enumerate() {
            let got = consumed
                .iter()
                .filter(|record| record.partition == partition as i32)
                .collect::<Vec<_>>();
            assert_eq!(got.len(), expected.len(), "{name}/{partition}: count");
            for (index, (got, want)) in got.iter().zip(expected).enumerate() {
                let at = format!("{name}/{partition} record {index}");
                assert_eq!(got.offset, index as i64, "{at}: order");
                assert_eq!(got.key, empty(&want.key), "{at}: key");
                assert_eq!(got.value, empty(&want.value), "{at}: value");
                let headers = want
                    .headers
                    .iter()
                    .map(|(key, value)| (key.clone(), empty(value)))
                    .collect::<Vec<_>>();
                assert_eq!(got.headers, headers, "{at}: headers");
                assert_eq!(got.timestamp, want.timestamp_ms, "{at}: CreateTime");
            }
        }
    }
}

/// Scenario 1.
#[compio::test]
async fn round_trip() {
    let mut draw = Draw::new("round_trip");
    let broker = Redpanda::start().await;
    let topics = [("round_trip_a", 4), ("round_trip_b", 1)];
    for (name, partitions) in topics {
        broker.create_topic(name, u32::from(partitions)).await;
    }
    let max_in_flight = draw.draw(1..=5u8);
    eprintln!("round_trip: max_in_flight {max_in_flight}");
    let plan = plan(&mut draw, 5, 50, 200);
    let outcome = produce(
        broker.addr(),
        &topics,
        &plan,
        max_in_flight,
        1,
        async |_| {},
    )
    .await;
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
    check(&broker, &topics, &plan).await;
}

/// Scenario 2.
#[compio::test]
async fn lost_reply() {
    let mut draw = Draw::new("lost_reply");
    let broker = Redpanda::start().await;
    let topics = [("lost_reply", 3)];
    broker.create_topic("lost_reply", 3).await;
    let proxy = Proxy::start(broker.addr()).await;
    proxy.drop_after(PRODUCE, 2);
    let plan = plan(&mut draw, 3, 10, 50);
    let outcome = proxy
        .serve(produce(proxy.addr(), &topics, &plan, 1, 1, async |_| {}))
        .await;
    eprintln!(
        "lost_reply: codes other than 0: {:?}, InitProducerId codes: {:?}",
        odd_codes(&outcome),
        outcome.init_codes
    );
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
    assert!(outcome.lost >= 1, "the drop lost a request: {outcome:?}");
    // A fresh broker may answer 15 or 16 first; the producer then asks
    // again. It keeps the one id it gets across the drop.
    let ids = outcome.init_codes.iter().filter(|&&code| code == 0).count();
    assert_eq!(ids, 1, "one producer id: {outcome:?}");
    assert_eq!(
        proxy.count(INIT_PRODUCER_ID),
        outcome.init_codes.len(),
        "each InitProducerId answered"
    );
    check(&broker, &topics, &plan).await;
}

/// Scenario 3. A fatal `UNKNOWN_PRODUCER_ID` (59) fails the test: the rule
/// for it is the user's decision.
#[compio::test]
async fn broker_restart() {
    let mut draw = Draw::new("broker_restart");
    let mut broker = Redpanda::start().await;
    let topics = [("broker_restart", 3)];
    broker.create_topic("broker_restart", 3).await;
    let addr = broker.addr();
    let plan = plan(&mut draw, 3, 300, 20);
    let thirty_acked = Event::new();
    let listener = thirty_acked.listen();
    let run = async {
        let outcome = produce(addr, &topics, &plan, 5, 1, async |sets| {
            if sets >= 30 {
                thirty_acked.notify_all();
            }
        })
        .await;
        // A fatal before 30 sets would leave the restart waiting.
        thirty_acked.notify_all();
        outcome
    };
    let restart = async {
        listener.await;
        broker.restart().await;
    };
    let (outcome, ()) = future::join(run, restart).await;
    eprintln!(
        "broker_restart: {} connections, {} lost, codes other than 0: {:?}",
        outcome.connections,
        outcome.lost,
        odd_codes(&outcome)
    );
    assert_ne!(
        outcome.result,
        Err(Fatal::Produce { code: 59 }),
        "UNKNOWN_PRODUCER_ID after a broker restart: the rule for 59 is a user decision"
    );
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
    assert!(
        outcome.connections >= 2,
        "the conn reconnected: {outcome:?}"
    );
    check(&broker, &topics, &plan).await;
}

/// Scenario 4. Each set of the plan is one source position, from 1, and
/// each of its records carries the position. The first process dies when
/// the proxy drops a Produce in the middle, before its reply. A second
/// process recovers `p[i]` and replays every position with the skip rule.
#[compio::test]
async fn process_restart() {
    let mut draw = Draw::new("process_restart");
    let broker = Redpanda::start().await;
    let topics = [("process_restart", 3)];
    broker.create_topic("process_restart", 3).await;
    let mut plan = plan(&mut draw, 3, 200, 3);
    for (at, set) in plan.iter_mut().enumerate() {
        let position = at as u64 + 1;
        set.truncate(draw.draw(1..=3usize));
        for (_, record) in set.iter_mut() {
            let value = position.to_be_bytes().to_vec();
            record.headers.push((POSITION.to_string(), Some(value)));
        }
    }
    let max_in_flight = draw.draw(1..=5u8);
    let drop_at = draw.draw(50..150usize);
    eprintln!("process_restart: max_in_flight {max_in_flight}, drop after Produce {drop_at}");

    let proxy = Proxy::start(broker.addr()).await;
    proxy.drop_after(PRODUCE, drop_at);
    let options = Options {
        max_in_flight,
        sets_per_tick: 1,
        limit: LIMIT,
        stop_on_lost: true,
        partitions: topics.iter().map(|&(_, count)| count).collect(),
    };
    let first = proxy
        .serve(driver::produce(
            proxy.addr(),
            config(&topics, max_in_flight),
            SLOT_BYTES,
            &plan,
            &options,
            async |_| {},
        ))
        .await;
    assert_eq!(first.result, Ok(()), "{first:?}");
    assert!(first.lost >= 1, "the first process died on the drop");
    assert!(
        first.acked_sets < plan.len(),
        "the first process died early"
    );

    // systemd waits `RestartSec` (1 s) before the new process.
    sleep(Duration::from_secs(1)).await;
    let recovery = recover(broker.addr(), &topics, 0, 1 << 20).await;
    let skip = positions(&recovery);
    eprintln!(
        "process_restart: first process acked {} sets; p[i] = {skip:?}, {} Fetch rounds",
        first.acked_sets, recovery.fetches
    );
    // The skip rule: a record goes out when its position is above `p[i]`.
    let replay = plan
        .iter()
        .enumerate()
        .map(|(at, set)| {
            let position = at as u64 + 1;
            set.iter()
                .filter(|(part, _)| skip[*part].is_none_or(|last| position > last))
                .cloned()
                .collect::<Vec<_>>()
        })
        .filter(|set| !set.is_empty())
        .collect::<Vec<_>>();
    let second = produce(
        broker.addr(),
        &topics,
        &replay,
        max_in_flight,
        1,
        async |_| {},
    )
    .await;
    assert_eq!(second.result, Ok(()), "{second:?}");
    // Each position once per partition, in order: the log equals the plan.
    check(&broker, &topics, &plan).await;
}

/// Scenario 5. Part 0 ends with many small batches. Part 1 ends with one
/// batch just below `fetch_max_bytes`, and it is second in the Fetch. Part 2
/// is empty. Part 3 has records only before the floor timestamp. A second
/// run with a floor after every record shows what ListOffsets answers for
/// it. A third run with a fetch limit below part 1's last batch shows what
/// the broker does then.
#[compio::test]
async fn recover_last() {
    let mut draw = Draw::new("recover_last");
    let broker = Redpanda::start().await;
    let topics = [("recover_last", 4)];
    broker.create_topic("recover_last", 4).await;
    let now = now_ms();
    let floor = now - 3_600_000;
    let fetch_max_bytes = 16 << 10;
    let record = |draw: &mut Draw, timestamp_ms: i64, position: u64, value: usize| OwnedRecord {
        timestamp_ms,
        key: draw.draw(option::weighted(0.875, vec(any::<u8>(), 0..=8))),
        value: Some(draw.draw(vec(any::<u8>(), value))),
        headers: vec![(POSITION.to_string(), Some(position.to_be_bytes().to_vec()))],
    };
    let mut plan = Vec::new();
    // Part 3: records two hours back, below the floor.
    for position in 1..=3 {
        plan.push(vec![(3, record(&mut draw, now - 7_200_000, position, 16))]);
    }
    // Part 0: 500 records, 10 per batch. Part 1: 20 records, 1 per batch.
    for set in 0..50u64 {
        let mut records: Vec<(usize, OwnedRecord)> = (1..=10)
            .map(|at| (0, record(&mut draw, now, set * 10 + at, 16)))
            .collect();
        if set < 20 {
            records.push((1, record(&mut draw, now, set + 1, 16)));
        }
        plan.push(records);
    }
    // Part 1: one last batch that fills `fetch_max_bytes` as far as whole
    // records go.
    let mut last = Vec::new();
    let mut bytes = HEADER_BYTES;
    loop {
        let next = record(&mut draw, now, 21 + last.len() as u64, 100);
        let size = next.wire_size(now, last.len() as i64);
        if bytes + size > fetch_max_bytes as usize {
            break;
        }
        bytes += size;
        last.push((1, next));
    }
    let big = last.len() as u64;
    assert!(
        bytes > fetch_max_bytes as usize - 200,
        "part 1's last batch is {bytes} bytes"
    );
    plan.push(last);
    let outcome = produce(broker.addr(), &topics, &plan, 5, 5, async |_| {}).await;
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");

    let recovery = recover(broker.addr(), &topics, floor, fetch_max_bytes).await;
    eprintln!(
        "recover_last: {} Fetch replies, ListOffsets {:?}",
        recovery.fetches, recovery.lists
    );
    let results = recovery.result.as_ref().expect("recovery done");
    let offsets: Vec<Option<i64>> = results
        .iter()
        .map(|found| found.map(|found| found.offset))
        .collect();
    assert_eq!(
        offsets,
        [Some(499), Some(20 + big as i64 - 1), None, None],
        "offsets of the last records; part 2 empty, part 3 below the floor"
    );
    assert_eq!(
        positions(&recovery),
        [Some(500), Some(20 + big), None, None],
        "positions of the last records"
    );

    // A floor after every record.
    let late = recover(broker.addr(), &topics, now + 3_600_000, fetch_max_bytes).await;
    eprintln!(
        "recover_last: ListOffsets with a timestamp after every record: {:?}",
        late.lists
    );
    assert_eq!(late.result, Ok(vec![None; 4]), "a floor after every record");
    let [ends, floors] = late.lists.as_slice() else {
        panic!("two ListOffsets replies: {late:?}");
    };
    for (end, floor) in ends.iter().zip(floors) {
        assert!(
            floor.offset == -1 || floor.offset == end.offset,
            "ListOffsets after every record answers -1 or the end: {floor:?}, end {end:?}"
        );
    }

    // A fetch limit below part 1's last batch: the broker returns it whole,
    // or the recovery stops with `Fatal::Fetch`.
    let small = recover(broker.addr(), &topics, floor, 1024).await;
    eprintln!(
        "recover_last: fetch limit 1024 below a batch of {bytes} bytes: {:?}",
        small.result
    );
    match &small.result {
        Ok(_) => assert_eq!(positions(&small), positions(&recovery), "small limit"),
        Err(fatal) => assert_eq!(*fatal, Fatal::Fetch, "small limit"),
    }
}

/// Scenario 6.
#[compio::test]
async fn wrong_partitions() {
    let mut draw = Draw::new("wrong_partitions");
    let broker = Redpanda::start().await;
    broker.create_topic("wrong_partitions", 2).await;
    let proxy = Proxy::start(broker.addr()).await;
    let plan = plan(&mut draw, 4, 2, 10);
    let outcome = proxy
        .serve(produce(
            proxy.addr(),
            &[("wrong_partitions", 4)],
            &plan,
            1,
            1,
            async |_| {},
        ))
        .await;
    assert_eq!(outcome.result, Err(Fatal::Metadata), "{outcome:?}");
    assert_eq!(proxy.count(PRODUCE), 0, "no Produce");
}

/// Scenario 7.
#[compio::test]
async fn missing_topic() {
    let mut draw = Draw::new("missing_topic");
    let broker = Redpanda::start().await;
    let proxy = Proxy::start(broker.addr()).await;
    let plan = plan(&mut draw, 2, 2, 10);
    let outcome = proxy
        .serve(produce(
            proxy.addr(),
            &[("missing_topic", 2)],
            &plan,
            1,
            1,
            async |_| {},
        ))
        .await;
    assert_eq!(outcome.result, Err(Fatal::Metadata), "{outcome:?}");
    assert_eq!(proxy.count(PRODUCE), 0, "no Produce");
}

/// Scenario 8. Five sets go out at once; the proxy forwards all five, holds
/// every reply, and closes once the broker had time to commit them. The job
/// resends all five, and the broker must answer each copy as a duplicate.
#[compio::test]
async fn pipeline_drop() {
    let mut draw = Draw::new("pipeline_drop");
    let broker = Redpanda::start().await;
    let topics = [("pipeline_drop", 3)];
    broker.create_topic("pipeline_drop", 3).await;
    let proxy = Proxy::start(broker.addr()).await;
    proxy.drop_after_range(PRODUCE, 1, 5, Duration::from_millis(200));
    let plan = plan(&mut draw, 3, 20, 30);
    // Records the broker holds after the drop, before the resend: tells a
    // committed first send (the resend is a duplicate) from a dropped one.
    let mut before_resend = None;
    let outcome = proxy
        .serve(produce(proxy.addr(), &topics, &plan, 5, 5, async |_| {
            if before_resend.is_none() && proxy.count(PRODUCE) >= 5 {
                sleep(Duration::from_millis(500)).await;
                before_resend = Some(high_watermarks(&broker, "pipeline_drop").await);
            }
        }))
        .await;
    let prefixes = (0..=5)
        .map(|sets| {
            let mut marks = [0i64; 3];
            plan[..sets]
                .iter()
                .flatten()
                .for_each(|&(part, _)| marks[part] += 1);
            marks
        })
        .collect::<Vec<_>>();
    eprintln!(
        "pipeline_drop: before the resend the broker holds {before_resend:?}; \
         marks after 0..=5 sets: {prefixes:?}"
    );
    eprintln!(
        "pipeline_drop: {} connections, {} lost, replies on connection 2: {:?}",
        outcome.connections,
        outcome.lost,
        outcome
            .replies
            .iter()
            .filter(|(connection, _)| *connection == 2)
            .take(5)
            .map(|(_, codes)| codes.to_vec())
            .collect::<Vec<_>>()
    );
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
    assert_eq!(
        before_resend,
        Some(prefixes[5].to_vec()),
        "the broker committed all five sets before the resend"
    );
    assert!(
        outcome
            .replies
            .iter()
            .all(|&(connection, _)| connection >= 2),
        "no reply on the dropped connection: {outcome:?}"
    );
    assert!(proxy.count(PRODUCE) >= 10, "five sends, then five resends");
    check(&broker, &topics, &plan).await;
}

// ------------------------------------------------- Consumer (consumer.md 7)

const FETCH: i16 = 1;
const MAX_WAIT_MS: i32 = 500;

/// A consumer conn: learned counts, nothing to produce.
fn learned(topics: &[&str]) -> Config {
    Config {
        client_id: "integration-consumer",
        topics: topics.iter().map(|&name| name.to_string()).collect(),
        request_timeout: Duration::from_secs(5),
        backoff_min: Duration::from_millis(10),
        backoff_max: Duration::from_millis(500),
        max_in_flight: 1,
        max_batch_bytes: 0,
        produce_parts: 0,
    }
}

fn consumer_config(
    start: Start,
    stop_timestamp_ms: Option<i64>,
    partition_max_bytes: i32,
) -> ConsumerConfig {
    ConsumerConfig {
        select: Select::All,
        start,
        stop_timestamp_ms,
        limits: FetchLimits {
            partition_max_bytes,
            max_wait_ms: MAX_WAIT_MS,
            min_bytes: 1,
        },
    }
}

/// Every selected part is caught up at `ends`. False before the parts are
/// known.
fn at_ends(consumer: &Consumer, ends: &[i64]) -> bool {
    consumer.phase() != Phase::Waiting
        && consumer
            .selected()
            .all(|part| consumer.caught_up(part) && consumer.next(part) == ends[part])
}

/// Consumes until every part is caught up at `ends`.
async fn consume_to(
    addr: SocketAddr,
    topics: &[&str],
    consumer: ConsumerConfig,
    ends: &[i64],
) -> Consumed {
    let consumed = driver::consume(addr, learned(topics), consumer, true, LIMIT, async |step| {
        !at_ends(step.consumer, ends)
    })
    .await;
    assert_eq!(consumed.result, Ok(()), "consume: {:?}", consumed.lists);
    consumed
}

/// Offsets of each part's delivered records.
fn offsets(consumed: &Consumed) -> Vec<Vec<i64>> {
    consumed
        .records
        .iter()
        .map(|records| records.iter().map(|(offset, _, _)| *offset).collect())
        .collect()
}

/// The delivered records equal `expected`, rpk's records, in order. Null
/// reads as empty, as rpk prints it.
fn same_records(consumed: &Consumed, expected: &[Vec<&ConsumedRecord>]) {
    let empty = |bytes: &Option<Vec<u8>>| bytes.clone().unwrap_or_default();
    for (partition, expected) in expected.iter().enumerate() {
        let got = &consumed.records[partition];
        assert_eq!(got.len(), expected.len(), "partition {partition}: count");
        for ((offset, record, _), want) in got.iter().zip(expected) {
            let at = format!("partition {partition} offset {offset}");
            assert_eq!(*offset, want.offset, "{at}: offset");
            assert_eq!(empty(&record.key), want.key, "{at}: key");
            assert_eq!(empty(&record.value), want.value, "{at}: value");
            assert_eq!(
                record
                    .headers
                    .iter()
                    .map(|(key, value)| (key.clone(), empty(value)))
                    .collect::<Vec<_>>(),
                want.headers,
                "{at}: headers"
            );
            assert_eq!(record.timestamp_ms, want.timestamp, "{at}: timestamp");
        }
    }
}

/// One set of one record per entry: `(part, timestamp, value bytes)`.
fn records_at(entries: &[(usize, i64, usize)]) -> Vec<Vec<(usize, OwnedRecord)>> {
    entries
        .iter()
        .enumerate()
        .map(|(at, &(part, timestamp_ms, size))| {
            let mut value = (at as u64).to_be_bytes().to_vec();
            value.resize(size.max(8), 0x5a);
            vec![(
                part,
                OwnedRecord {
                    timestamp_ms,
                    key: Some(format!("key-{at}").into_bytes()),
                    value: Some(value),
                    headers: Vec::new(),
                },
            )]
        })
        .collect()
}

async fn produce_all(addr: SocketAddr, topics: &[(&str, u16)], plan: &[Vec<(usize, OwnedRecord)>]) {
    let outcome = produce(addr, topics, plan, 5, 5, async |_| {}).await;
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
}

/// Test 7 `consume_from_time`.
#[compio::test]
async fn consume_from_time() {
    let mut draw = Draw::new("consume_from_time");
    let broker = Redpanda::start().await;
    broker.create_topic("consume_from_time", 4).await;
    produce_all(
        broker.addr(),
        &[("consume_from_time", 4)],
        &plan(&mut draw, 4, 100, 100),
    )
    .await;
    let stored = broker.consume("consume_from_time", 10_000).await;
    let mut timestamps: Vec<i64> = stored.iter().map(|record| record.timestamp).collect();
    timestamps.sort();
    let middle = timestamps[timestamps.len() / 2];
    let ends = high_watermarks(&broker, "consume_from_time").await;
    let consumed = consume_to(
        broker.addr(),
        &["consume_from_time"],
        consumer_config(Start::Timestamp(middle), None, 1 << 20),
        &ends,
    )
    .await;
    eprintln!(
        "consume_from_time: start {middle}, ends {ends:?}, starts {:?}, {} Fetch replies, rx {} bytes",
        consumed
            .lists
            .iter()
            .find(|(timestamp, _)| *timestamp == middle)
            .map(|(_, offsets)| offsets
                .iter()
                .map(|offset| offset.offset)
                .collect::<Vec<_>>()),
        consumed.fetches.len(),
        consumed.rx_bytes
    );
    // The rule: from the first record at or above the timestamp.
    same_records(
        &consumed,
        &(0..4)
            .map(|partition| {
                let records: Vec<&ConsumedRecord> = stored
                    .iter()
                    .filter(|record| record.partition == partition)
                    .collect();
                records[records
                    .iter()
                    .position(|record| record.timestamp >= middle)
                    .unwrap_or(records.len())..]
                    .to_vec()
            })
            .collect::<Vec<_>>(),
    );
}

/// Test 7 `long_poll`.
#[compio::test]
async fn long_poll() {
    let mut draw = Draw::new("long_poll");
    let broker = Redpanda::start().await;
    let topics = [("long_poll", 2)];
    broker.create_topic("long_poll", 2).await;
    produce_all(broker.addr(), &topics, &plan(&mut draw, 2, 5, 1)).await;
    let ends = high_watermarks(&broker, "long_poll").await;
    let proxy = Proxy::start(broker.addr()).await;
    let idle_done = Event::new();
    let listener = idle_done.listen();
    let acked = std::cell::Cell::new(None);
    // (proxy Fetch count, time) when caught up, and after 2 s idle.
    let mut idle_from = None;
    let mut idle_fetches = None;
    let mut arrived = None;
    let (consumed, ()) = future::join(
        proxy.serve(driver::consume(
            proxy.addr(),
            learned(&["long_poll"]),
            consumer_config(Start::Tail(0), None, 1 << 20),
            true,
            LIMIT,
            async |step| {
                let now = std::time::Instant::now();
                match idle_from {
                    None => {
                        if at_ends(step.consumer, &ends) {
                            idle_from = Some((proxy.count(FETCH), now));
                        }
                    }
                    Some((count, from)) => {
                        if idle_fetches.is_none() && now - from >= Duration::from_secs(2) {
                            idle_fetches = Some(proxy.count(FETCH) - count);
                            idle_done.notify_all();
                        }
                    }
                }
                if step.consumed.count > 0 {
                    arrived = Some(now);
                    return false;
                }
                true
            },
        )),
        async {
            listener.await;
            produce_all(broker.addr(), &topics, &records_at(&[(1, now_ms(), 16)])).await;
            acked.set(Some(std::time::Instant::now()));
        },
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    let acked = acked.get().expect("the record was produced");
    let latency = arrived
        .expect("the record arrived")
        .saturating_duration_since(acked);
    let idle_fetches = idle_fetches.expect("2 s idle");
    eprintln!(
        "long_poll: {idle_fetches} Fetches in 2 s idle at max_wait_ms {MAX_WAIT_MS}; \
         the record arrived {latency:?} after its ack"
    );
    assert!(
        idle_fetches <= (2000 / MAX_WAIT_MS + 2) as usize,
        "{idle_fetches} Fetches in 2 s idle"
    );
    assert!(
        latency <= Duration::from_millis(MAX_WAIT_MS as u64 + 100),
        "the record arrived {latency:?} after its ack"
    );
    assert_eq!(offsets(&consumed), vec![vec![], vec![ends[1]]]);
}

/// Test 7 `tail`.
#[compio::test]
async fn tail() {
    let broker = Redpanda::start().await;
    broker.create_topic("tail", 1).await;
    let now = now_ms();
    produce_all(
        broker.addr(),
        &[("tail", 1)],
        &records_at(&(0..10).map(|at| (0, now + at, 16)).collect::<Vec<_>>()),
    )
    .await;
    let ends = high_watermarks(&broker, "tail").await;
    assert_eq!(ends, vec![10]);
    assert_eq!(
        offsets(
            &consume_to(
                broker.addr(),
                &["tail"],
                consumer_config(Start::Tail(4), None, 1 << 20),
                &ends,
            )
            .await
        ),
        vec![(6..10).collect::<Vec<i64>>()],
        "Tail(4)"
    );
    assert_eq!(
        offsets(
            &consume_to(
                broker.addr(),
                &["tail"],
                consumer_config(Start::Tail(100), None, 1 << 20),
                &ends,
            )
            .await
        ),
        vec![(0..10).collect::<Vec<i64>>()],
        "Tail(100)"
    );
}

/// Test 7 `stop_window`. A second run starts and stops after every record:
/// it shows what ListOffsets answers for a timestamp past the end.
#[compio::test]
async fn stop_window() {
    let broker = Redpanda::start().await;
    broker.create_topic("stop_window", 2).await;
    let first = now_ms() - 60_000;
    let entries: Vec<(usize, i64, usize)> = (0..40)
        .map(|at| (at % 2, first + at as i64 * 1000, 32))
        .collect();
    produce_all(broker.addr(), &[("stop_window", 2)], &records_at(&entries)).await;
    let (start, stop) = (first + 5_000, first + 25_000);
    let consumed = driver::consume(
        broker.addr(),
        learned(&["stop_window"]),
        consumer_config(Start::Timestamp(start), Some(stop), 1 << 20),
        true,
        LIMIT,
        async |_| true,
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    assert_eq!(consumed.phase, Phase::Done, "the window ends by itself");
    let mut expected = vec![Vec::new(), Vec::new()];
    for (at, &(part, timestamp, _)) in entries.iter().enumerate() {
        if (start..stop).contains(&timestamp) {
            expected[part].push(at as i64 / 2);
        }
    }
    assert_eq!(offsets(&consumed), expected, "records in [start, stop)");
    for (part, records) in consumed.records.iter().enumerate() {
        for (offset, record, _) in records {
            assert!(
                (start..stop).contains(&record.timestamp_ms),
                "part {part} offset {offset}"
            );
        }
    }

    let late = now_ms() + 3_600_000;
    let past = driver::consume(
        broker.addr(),
        learned(&["stop_window"]),
        consumer_config(Start::Timestamp(late), Some(late), 1 << 20),
        true,
        LIMIT,
        async |_| true,
    )
    .await;
    assert_eq!(past.result, Ok(()));
    assert_eq!(past.phase, Phase::Done, "an empty window");
    eprintln!(
        "stop_window: ListOffsets after every record (end first): {:?}",
        past.lists
            .iter()
            .map(|(timestamp, offsets)| {
                (
                    *timestamp,
                    offsets
                        .iter()
                        .map(|offset| offset.offset)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>()
    );
    assert!(
        offsets(&past).iter().all(Vec::is_empty),
        "nothing delivered"
    );
}

/// Test 7 `learned_counts`.
#[compio::test]
async fn learned_counts() {
    let broker = Redpanda::start().await;
    broker.create_topic("learned_counts", 24).await;
    let mut selected = 0;
    let consumed = driver::consume(
        broker.addr(),
        learned(&["learned_counts"]),
        consumer_config(Start::Tail(4), None, 1 << 20),
        true,
        LIMIT,
        async |step| {
            selected = step.consumer.selected().count();
            !step.fetched
        },
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    assert_eq!(consumed.parts, Some(24));
    assert_eq!(selected, 24);
    let reply = consumed.fetches.last().expect("one Fetch reply");
    assert_eq!(reply.parts.len(), 24, "every part fetched");
    assert!(
        reply
            .parts
            .iter()
            .all(|part| part.code == 0 && part.high_watermark == 0)
    );
}

/// Test 7 `trimmed`. After the first delivery, `rpk topic trim-prefix`
/// moves the log start past the consumer's next offset.
#[compio::test]
async fn trimmed() {
    let broker = Redpanda::start().await;
    broker.create_topic("trimmed", 1).await;
    let now = now_ms();
    // One record per Produce, so one batch per record.
    let outcome = produce(
        broker.addr(),
        &[("trimmed", 1)],
        &records_at(&(0..200).map(|at| (0, now + at, 200)).collect::<Vec<_>>()),
        1,
        1,
        async |_| {},
    )
    .await;
    assert_eq!(outcome.result, Ok(()), "{outcome:?}");
    let ends = high_watermarks(&broker, "trimmed").await;
    assert_eq!(ends, vec![200]);
    let mut trimmed_at = None;
    let consumed = driver::consume(
        broker.addr(),
        learned(&["trimmed"]),
        consumer_config(Start::Timestamp(0), None, 1024),
        true,
        LIMIT,
        async |step| {
            if trimmed_at.is_none() && step.consumed.count > 0 {
                trimmed_at = Some(step.consumer.next(0));
                broker
                    .rpk(
                        &[
                            "topic",
                            "trim-prefix",
                            "trimmed",
                            "-p",
                            "0",
                            "-o",
                            "150",
                            "--no-confirm",
                        ],
                        b"",
                    )
                    .await;
            }
            !at_ends(step.consumer, &ends)
        },
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    let trimmed_at = trimmed_at.expect("records before the trim");
    let out_of_range: Vec<&FetchReply> = consumed
        .fetches
        .iter()
        .filter(|reply| reply.parts.iter().any(|part| part.code == 1))
        .collect();
    let earliest: Vec<i64> = consumed
        .lists
        .iter()
        .filter(|(timestamp, _)| *timestamp == -2)
        .map(|(_, offsets)| offsets[0].offset)
        .collect();
    eprintln!(
        "trimmed: next {trimmed_at} at the trim; code-1 replies {out_of_range:?}; ListOffsets -2 {earliest:?}"
    );
    assert!(
        !out_of_range.is_empty(),
        "a Fetch below the log start gets code 1"
    );
    assert_eq!(
        offsets(&consumed),
        vec![
            (0..trimmed_at)
                .chain(*earliest.last().expect("ListOffsets -2 after code 1")..200)
                .collect::<Vec<_>>()
        ],
        "on from the new log start"
    );
}

/// Test 7 `consumer_broker_restart`.
#[compio::test]
async fn consumer_broker_restart() {
    let mut draw = Draw::new("consumer_broker_restart");
    let mut broker = Redpanda::start().await;
    broker.create_topic("consumer_broker_restart", 3).await;
    let addr = broker.addr();
    produce_all(
        addr,
        &[("consumer_broker_restart", 3)],
        &plan(&mut draw, 3, 100, 30),
    )
    .await;
    let ends = high_watermarks(&broker, "consumer_broker_restart").await;
    let stored = broker.consume("consumer_broker_restart", 3000).await;
    let mut restarted = false;
    let consumed = driver::consume(
        addr,
        learned(&["consumer_broker_restart"]),
        // Above the largest batch (a set of 30 records), so Redpanda returns
        // each batch whole. Small enough for many Fetch rounds.
        consumer_config(Start::Timestamp(0), None, 32 << 10),
        true,
        LIMIT,
        async |step| {
            if !restarted && step.consumed.count >= 500 {
                restarted = true;
                broker.restart().await;
            }
            !at_ends(step.consumer, &ends)
        },
    )
    .await;
    if consumed.result.is_err() {
        eprintln!(
            "consumer_broker_restart: last Fetch replies {:?}, last lists {:?}",
            &consumed.fetches[consumed.fetches.len().saturating_sub(4)..],
            &consumed.lists[consumed.lists.len().saturating_sub(4)..]
        );
    }
    assert_eq!(consumed.result, Ok(()));
    eprintln!(
        "consumer_broker_restart: {} connections, {} Fetch replies, ends {ends:?}",
        consumed.connections,
        consumed.fetches.len()
    );
    assert!(
        restarted && consumed.connections >= 2,
        "the conn reconnected"
    );
    same_records(
        &consumed,
        &(0..3)
            .map(|partition| {
                stored
                    .iter()
                    .filter(|record| record.partition == partition)
                    .collect()
            })
            .collect::<Vec<_>>(),
    );
}

/// Test 7 `idle_reply`.
#[compio::test]
async fn idle_reply() {
    let broker = Redpanda::start().await;
    broker.create_topic("idle_reply", 2).await;
    let now = now_ms();
    produce_all(
        broker.addr(),
        &[("idle_reply", 2)],
        &records_at(&[(0, now, 16), (1, now, 16), (1, now, 16)]),
    )
    .await;
    let ends = high_watermarks(&broker, "idle_reply").await;
    let mut caught_up_at = None;
    let consumed = driver::consume(
        broker.addr(),
        learned(&["idle_reply"]),
        consumer_config(Start::Tail(10), None, 1 << 20),
        true,
        LIMIT,
        async |step| {
            if caught_up_at.is_none() && at_ends(step.consumer, &ends) {
                caught_up_at = Some(step.consumed.fetches.len());
            }
            caught_up_at.is_none_or(|at| step.consumed.fetches.len() < at + 5)
        },
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    let idle = &consumed.fetches[caught_up_at.expect("caught up")..];
    eprintln!(
        "idle_reply: waits {:?}, replies {idle:?}",
        idle.iter().map(|reply| reply.wait).collect::<Vec<_>>()
    );
    for reply in idle {
        assert_eq!(reply.code, 0);
        assert_eq!(reply.parts.len(), 2, "a reply part for each part");
        for part in &reply.parts {
            assert_eq!((part.code, part.bytes), (0, 0), "empty: {reply:?}");
            assert_eq!(
                part.high_watermark, ends[part.part],
                "high watermark: {reply:?}"
            );
        }
        let wait = Duration::from_millis(MAX_WAIT_MS as u64);
        assert!(
            reply.wait >= wait - Duration::from_millis(20)
                && reply.wait <= wait + Duration::from_millis(200),
            "idle reply after {:?}",
            reply.wait
        );
    }
}

/// Test 7 `replay_rate`: one partition of `REPLAY_MB` MB (default 1024),
/// read with one Fetch in flight and 1 MiB per part. Run it with
/// `--release` for a rate that means something.
#[compio::test]
async fn replay_rate() {
    let broker = Redpanda::start().await;
    broker.create_topic("replay_rate", 1).await;
    // Values of 120 KiB fill most of a 128 KiB slot: one record per batch.
    let size = 120 << 10;
    let count = (std::env::var("REPLAY_MB")
        .ok()
        .map(|value| value.parse::<usize>().expect("REPLAY_MB is a count"))
        .unwrap_or(1024)
        << 20)
        / size;
    let now = now_ms();
    let produced = std::time::Instant::now();
    produce_all(
        broker.addr(),
        &[("replay_rate", 1)],
        &records_at(
            &(0..count)
                .map(|at| (0, now + at as i64 / 100, size))
                .collect::<Vec<_>>(),
        ),
    )
    .await;
    let produce_time = produced.elapsed();
    let ends = high_watermarks(&broker, "replay_rate").await;
    let consumed = driver::consume(
        broker.addr(),
        learned(&["replay_rate"]),
        consumer_config(Start::Timestamp(0), None, 1 << 20),
        false,
        LIMIT * 5,
        async |step| !at_ends(step.consumer, &ends),
    )
    .await;
    assert_eq!(consumed.result, Ok(()));
    assert_eq!(consumed.count, count as u64);
    eprintln!(
        "replay_rate: {} records, {} bytes in {:?} ({:.0} MB/s), {} Fetch replies; produce took {produce_time:?}",
        consumed.count,
        consumed.bytes,
        consumed.elapsed,
        consumed.bytes as f64 / consumed.elapsed.as_secs_f64() / 1e6,
        consumed.fetches.len()
    );
}

/// Starvation: 3 parts of 10 batches of one 20 KiB record each, 32 KiB per
/// part. A part's reply is 2 batches (the second crosses the limit), so 3
/// parts need about 120 KiB of a 96 KiB request budget, and the third part
/// gets nothing below its high watermark. The consumer fetches it alone
/// next. Records what Redpanda sends per part and per reply.
#[compio::test]
async fn starved() {
    let broker = Redpanda::start().await;
    broker.create_topic("starved", 3).await;
    let now = now_ms();
    produce_all(
        broker.addr(),
        &[("starved", 3)],
        &records_at(
            &(0..30)
                .map(|at| (at % 3, now + at as i64, 20 << 10))
                .collect::<Vec<_>>(),
        ),
    )
    .await;
    let ends = high_watermarks(&broker, "starved").await;
    assert_eq!(ends, vec![10, 10, 10]);
    let limit = 32 << 10;
    let consumed = consume_to(
        broker.addr(),
        &["starved"],
        consumer_config(Start::Timestamp(0), None, limit),
        &ends,
    )
    .await;
    let replies: Vec<(Vec<usize>, Vec<usize>, usize)> = consumed
        .fetches
        .iter()
        .map(|reply| {
            let bytes: Vec<usize> = reply.parts.iter().map(|part| part.bytes).collect();
            // Before `bytes` moves into the tuple.
            let total = bytes.iter().sum();
            (
                reply.parts.iter().map(|part| part.part).collect(),
                bytes,
                total,
            )
        })
        .collect();
    eprintln!(
        "starved: limit {limit}, max_bytes = limit x parts; Fetch replies (parts, bytes, total): {replies:?}"
    );
    let limit = limit as usize;
    for (parts, bytes, total) in &replies {
        assert!(
            *total <= (parts.len() + 1) * limit,
            "the fetch_reply_bytes bound: {bytes:?}"
        );
        assert!(
            bytes.iter().all(|&bytes| bytes < 2 * limit),
            "a part holds less than twice the limit: {bytes:?}"
        );
    }
    let (first_parts, first_bytes, _) = &replies[0];
    assert_eq!(
        first_parts,
        &vec![0, 1, 2],
        "the first Fetch asks for every part"
    );
    assert_eq!(first_bytes[2], 0, "the third part starves: {first_bytes:?}");
    assert_eq!(
        replies[1].0,
        vec![2],
        "the starved part is fetched alone next"
    );
    assert_eq!(
        offsets(&consumed),
        vec![(0..10).collect::<Vec<i64>>(); 3],
        "every record once"
    );
}
