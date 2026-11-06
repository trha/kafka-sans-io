//! Plan test 2 of `plans/consumer.md`: partition counts the conn learns
//! from the first Metadata reply and checks on each later one, and the
//! Fetch reply fields it passes on.

use kafka_sans_io::{
    conn::{Config, Conn, ConnIn, ConnOut, FetchLimits, NetCmd, NetEvent, Reply},
    error::Fatal,
    frame::RxBuf,
    limits::MAX_PARTS,
    testing::{
        model::{Broker, Connection},
        record::OwnedRecord,
    },
    time::{Duration, Instant},
};

fn config(topics: &[&str]) -> Config {
    Config {
        client_id: "conn",
        topics: topics.iter().map(|&name| name.to_string()).collect(),
        request_timeout: Duration::from_secs(5),
        backoff_min: Duration::from_millis(1),
        backoff_max: Duration::from_millis(8),
        max_in_flight: 1,
        max_batch_bytes: 0,
        produce_parts: 0,
    }
}

/// One conn on the model broker. Every request is answered at once.
struct Run {
    broker: Broker,
    connection: Option<Connection>,
    net: Option<NetEvent>,
    conn: Conn,
    conn_out: ConnOut,
    rx: RxBuf,
    step: u64,
}

impl Run {
    fn new(broker: Broker, config: Config) -> Self {
        let conn = Conn::new(config);
        let conn_out = ConnOut::new(&conn);
        Run {
            broker,
            connection: None,
            net: None,
            conn,
            conn_out,
            rx: RxBuf::new(1 << 20),
            step: 0,
        }
    }

    fn now(&self) -> Instant {
        Instant(self.step * 1_000_000)
    }

    /// One conn step, then the network. Returns whether the conn went up.
    fn step(&mut self) -> Result<bool, Fatal> {
        self.step += 1;
        let now = self.now();
        self.conn.step(
            ConnIn {
                now,
                net: self.net.take(),
                rx: &mut self.rx,
            },
            &mut self.conn_out,
        )?;
        Ok(self.conn_out.up)
    }

    fn network(&mut self) {
        match self.conn_out.cmd {
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
        connection.receive(self.conn_out.tx.pending());
        self.conn_out.tx.consume(self.conn_out.tx.pending().len());
        while connection.handle_one(&mut self.broker, None) {}
        let count = connection.deliver(usize::MAX, self.rx.spare());
        self.rx.filled(count);
    }

    /// Steps until the conn is `Up`, or a fatal.
    fn up(&mut self) -> Result<(), Fatal> {
        for _ in 0..100 {
            if self.step()? {
                return Ok(());
            }
            self.network();
        }
        panic!("conn not Up after 100 steps");
    }

    /// Drops the connection and steps until the next `Up`, or a fatal.
    fn reconnect(&mut self) -> Result<(), Fatal> {
        self.conn.close(self.now(), &mut self.conn_out);
        self.network();
        self.up()
    }
}

#[test]
fn learned_counts_come_from_metadata() {
    let mut run = Run::new(
        Broker::new(&[("a", 3), ("b", 1), ("c", 24)]),
        config(&["a", "b", "c"]),
    );
    assert_eq!(run.conn.parts(), None, "unknown before the first Up");
    run.up().expect("counts learned");
    assert_eq!(run.conn.parts(), Some(28));
    assert_eq!(run.conn.part(0, 2), 2);
    assert_eq!(run.conn.part(1, 0), 3);
    assert_eq!(run.conn.part(2, 23), 27);
    for part in 0..28 {
        let (topic, partition) = run.conn.topic_partition(part);
        assert_eq!(run.conn.part(topic, partition), part, "part {part}");
    }
    assert_eq!(run.conn.partitions(), Some(&[3, 1, 24][..]));
    run.reconnect()
        .expect("the same counts on a new connection");
    assert_eq!(run.conn.parts(), Some(28));
}

#[test]
#[should_panic(expected = "a Produce above produce_parts")]
fn produce_above_produce_parts_panics() {
    let mut run = Run::new(Broker::new(&[("a", 2)]), config(&["a"]));
    run.up().expect("count learned");
    run.conn.send_produce(
        run.now(),
        [(0, &[0u8; 8][..])].into_iter(),
        &mut run.conn_out,
    );
}

#[test]
fn a_learned_count_that_changes_is_fatal() {
    let mut run = Run::new(Broker::new(&[("a", 3)]), config(&["a"]));
    run.up().expect("count learned");
    run.broker.wrong_partitions = true;
    assert_eq!(run.reconnect(), Err(Fatal::Metadata));
}

#[test]
fn learned_total_above_max_parts_is_fatal() {
    let mut run = Run::new(
        Broker::new(&[("big", MAX_PARTS as i32 + 1)]),
        config(&["big"]),
    );
    assert_eq!(run.up(), Err(Fatal::Metadata), "one topic above MAX_PARTS");

    let mut run = Run::new(Broker::new(&[("a", 40), ("b", 25)]), config(&["a", "b"]));
    assert_eq!(run.up(), Err(Fatal::Metadata), "two topics above MAX_PARTS");

    let mut run = Run::new(Broker::new(&[("a", 40), ("b", 24)]), config(&["a", "b"]));
    run.up().expect("exactly MAX_PARTS");
    assert_eq!(run.conn.parts(), Some(MAX_PARTS));
}

#[test]
fn a_learned_topic_without_partitions_is_fatal() {
    let mut run = Run::new(
        Broker::new(&[("a", 1), ("empty", 0)]),
        config(&["a", "empty"]),
    );
    assert_eq!(run.up(), Err(Fatal::Metadata));
}

/// The conn passes each part's high watermark and log start, and the
/// limits reach the broker.
#[test]
fn fetch_part_has_watermark_and_log_start() {
    let mut broker = Broker::new(&[("a", 2)]);
    let record = OwnedRecord {
        timestamp_ms: 1,
        key: None,
        value: Some(vec![1; 8]),
        headers: Vec::new(),
    };
    for _ in 0..3 {
        broker.put("a", 1, std::slice::from_ref(&record));
    }
    let mut run = Run::new(broker, config(&["a"]));
    run.up().expect("count learned");
    run.conn.send_fetch(
        run.now(),
        FetchLimits {
            partition_max_bytes: 1 << 20,
            max_wait_ms: 0,
            min_bytes: 1,
        },
        [(0, 0), (1, 1)].into_iter(),
        &mut run.conn_out,
    );
    run.network();
    run.step().expect("Fetch reply reads");
    let [Reply::Fetch { code: 0, parts }] = &run.conn_out.replies[..] else {
        panic!("one Fetch reply: {:?}", run.conn_out.replies);
    };
    assert_eq!(
        parts
            .iter()
            .map(|part| {
                (
                    part.part,
                    part.code,
                    part.high_watermark,
                    part.log_start_offset,
                )
            })
            .collect::<Vec<_>>(),
        vec![(0, 0, 0, 0), (1, 0, 3, 0)]
    );
}
