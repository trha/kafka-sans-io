# kafka-sans-io consumer, and atc without rdkafka

Status: Plan

Parent plans: `plans/kafka-sans-io.md` ("conn", "wire", "recovery",
"Property tests", "Integration tests"). Where they differ, this plan wins.
"Changes to other plans" lists the changes.

## Goal

atc (`~/atc`) reads Kafka through kafka-sans-io and no longer links
librdkafka. The crate gets a consumer job: start each partition at a
timestamp or a few records before the end, optionally stop at a second
timestamp, long-poll Fetch, and report when each partition is caught up.
In atc, `kafkax` gets a tokio driver and a merge by timestamp. The six
servers move over, and `rdkafka` leaves `Cargo.toml`, `flake.nix`,
`shell.nix`, and `/share/infra/nix/atc.nix`.

## Out of scope

- Consumer groups, offset commits, and rebalancing. atc sets `group.id`, but
  nothing commits, and each server picks its own offsets.
- Compressed batches, transactions, SASL, and TLS. The live cluster has none
  (see "Facts").
- Publishing kafka-sans-io to the asm-rust registry. atc uses a path
  dependency for now.
- fleeb-kafka in `~/simple`. It only changes `partitions: n` to
  `partitions: Some(n)`.

## Facts this plan rests on

### What atc uses rdkafka for

Every use is a consumer. Nothing in atc produces. The users are `kafkax`,
`health-server`, `latency-server`, `pnl-server`, `markout-server`,
`umdf-server`, and `log-server`.

| Server | Topics | Start | Stop | Order |
|---|---|---|---|---|
| health | `fleeb-counters` (all partitions) | market open | none | merged by timestamp |
| latency | `ep-latency` (all partitions) | market open | none | merged by timestamp |
| markout | `ep-messages` (all) and the umdf topics (partition 0) | market open | none | merged by timestamp |
| pnl | `ep-messages` (all) | market open | none | arrival |
| umdf, main | `channel-72`, `channel-78`, `channel-70` | market open | none | arrival, per topic |
| umdf, closing | the same topics | yesterday's close start | yesterday's close end | arrival |
| log, params | `strategy-params` (all) | market open | none | arrival |
| log, tail | `strategy-logs`, one partition per gRPC call | 4 before the end | none | arrival |

Each server reads the partition count from Metadata
(`fetch_metadata(...).partitions().len()`). It turns the start timestamp
into offsets with `offsets_for_times`, then assigns and streams. Health,
latency, and markout make one `StreamConsumer` per partition and merge them
with `kafkax::merge_partitions`. The merge waits until every stream has a
record or is at `PartitionEOF`, then yields the record with the smallest
timestamp (`kafkax/src/lib.rs`). `umdf-server/src/processors.rs` reads the
closing window: it reassigns without a partition once that partition
reaches its end offset, and the stream ends when none is left. A record
reads key, payload, timestamp, topic, partition, and offset. No server
reads headers. umdf finds a record's topic with `rec.topic()`.

The deployed config (`/share/infra/machines/b3-colo-1/atc.nix`) is
`bootstrap.servers = 192.168.181.11:9092`, `enable.partition.eof = true`,
and `group.id = atc`. There is no SASL or TLS. All servers run in the one
`atc-server` process on tokio 1.47. `release.sh` builds the release with
local cargo, so a path dependency builds there. `/share/infra/nix/atc.nix`
lists `rdkafka` in `buildInputs` for `autoPatchelfHook`.

### Live cluster (b3-colo-1, Redpanda at 192.168.181.11:9092, 2026-11-05)

- `log_compression_type = producer`, every topic has
  `compression.type=producer`, and `log_message_timestamp_type =
  CreateTime`. A scan of every retained batch of the topics atc reads
  (about 57 GB) found only magic 2, compression none, and timestamp type
  CreateTime. Earlier samples of the first 300 and last 50 records per
  partition found no control batches.
- The largest batch is 394 005 bytes (`channel-72`, offset 12691873959,
  2742 records). Only 14 batches of `channel-72` are above 256 KiB. The
  scan output is in
  `/tmp/claude-1000/-home-anh-atc/b0ca38a3-a672-48af-bbba-38fcf3cb28c6/scratchpad/sizes.out`.
- `kafka_batch_max_bytes = 1048576`, and `max.message.bytes = 1048576` on
  the topics. So no batch is above 1 MiB, now or later.
- Partition counts: `ep-messages`, `strategy-logs`, and `strategy-params`
  have 24, and every `channel-*` and stamps topic has 1. Only partitions 2,
  3, and 8 of the 24-partition topics hold records. markout has the most
  parts: 24 + 3 = 27, below `MAX_PARTS = 64`.
- `ep-latency` and `fleeb-counters` do not exist on the cluster yet.

### kafka-support links librdkafka (2026-11-05)

- `kafka-support` 0.1.3 (asm-rust registry), which atc used for the Schema
  Registry client and decoding, depends on `rdkafka` 0.38 with `tracing`
  and `dynamic-linking`, with no feature to turn it off, for its
  `poll_producer` and `Encoder` over `BaseRecord`. Its source exists only
  in `~/.cargo/registry`. So step 8 moved the parts atc needs into
  `kafkax::schema_registry`.

### kafka-sans-io today

- `Conn` speaks ApiVersions 3, Metadata 8, ListOffsets 5 (any timestamp,
  -1 is the end), and Fetch 11 (`src/conn.rs`, `src/wire/recovery.rs`).
  `Request::ListOffsets { timestamp }` lists every part at one timestamp.
- `TopicConfig.partitions` is a fixed `u16`. The Metadata check fails with
  `Fatal::Metadata` on another count. `Conn::new` sizes `tx` from the counts
  (`tx_capacity`).
- `wire::fetch_request` hard-codes `max_wait_ms: 0` and `min_bytes: 1`, and
  sets `max_bytes` to `partition_max_bytes` times the part count.
- The Fetch reply reader has `high_watermark` and `log_start_offset` in
  `FetchPartitionHead`, but `FetchPart` keeps only `part`, `code`, and
  `records`.
- `RxBuf::spare` and `clear` end every `RxSpan`. One frame must fit the
  whole `RxBuf`, or it is `Fatal::Protocol`.
- `batch::split_batch` and `read_batch` read batches in place. `read_batch`
  rejects compression and checks the CRC. `RecordView` has `offset`,
  `timestamp_ms`, `key`, `value`, and `headers`.
- The model broker (`src/testing/model.rs`) asserts `max_wait_ms == 0`, has
  no log start, and has no earliest ListOffsets (-2).
- Recovery scenario 5 showed that Redpanda does not return a batch whole
  when it is above the partition limit and is not in the first partition of
  the Fetch. So the partition limit must be at least the largest batch.
- fleeb-kafka (`~/simple/fleeb-kafka/src/world.rs`) builds a `TopicConfig`
  and depends on `../../kafka-sans-io`.

### Measured on a local Redpanda 25.3.17 (test 7, 2026-11-05)

- ListOffsets with a timestamp after every record answers -1 in every
  partition (`stop_window`). The model broker now answers the same.
- An idle Fetch with `max_wait_ms = 500` and `min_bytes = 1` comes back
  after 500.4 to 501.2 ms, with code 0, an empty records blob for each
  part, and the high watermark set (`idle_reply`, 5 replies).
- Over 2 s idle at `max_wait_ms = 500`, the proxy saw 4 Fetches. A record
  produced while the Fetch was held arrived 0 to 0.3 ms after its
  Produce ack (`long_poll`, 3 runs). So `min_bytes = 1` ends the wait at
  once.
- After `rpk topic trim-prefix -o 150` past the consumer's next offset (3),
  the next Fetch got partition code 1 with `high_watermark = 200`,
  `log_start_offset = 150`, and an empty records blob. ListOffsets -2 then
  answered 150, the offset asked for (`trimmed`).
- Redpanda's Fetch rule, from `starved` and the two failed
  `consumer_broker_restart` runs (step 5a): each part gets whole batches
  from the one that holds the fetch offset while its bytes are below the
  part limit, so the batch that crosses the limit comes whole and nothing
  follows it. No batch was cut short. In request order, a part whose
  reply would push the total past `max_bytes` (the limit times the parts)
  gets nothing at all, even when a smaller part of it would fit. The
  first part with records always gets its batches.
  - `starved`: 20 557-byte batches and a 32 768-byte limit give 41 114
    bytes per part (25 % over the limit). With 3 parts, parts 0 and 1
    filled 82 228 of the 98 304-byte budget, and part 2 got 0 bytes below
    a high watermark of 10, in every round. Fetched alone, part 2 got
    41 114 bytes, above that request's own `max_bytes` of 32 768.
  - `consumer_broker_restart` (seeds 1793909178940249579 and
    1793909154507114763): 33 061 + 32 290 bytes for parts 0 and 1 and 0
    for part 2 below its high watermark, with 32 953 bytes of the budget
    left. So a part is dropped whole, not trimmed to the rest of the
    budget.
  - With a part limit of 4096 bytes and batches of about 3 to 6 KiB, the
    first part got a 6381-byte batch whole.
  - No reply was seen above `max_bytes` with more than one part. So the
    records of one reply stay below the limit times `parts + 1`, when
    every batch is at most the limit. `fetch_reply_bytes` uses that
    bound.
- `replay_rate`: 1 GiB in one partition (8738 batches of one 120 KiB
  record), one Fetch in flight, 1 MiB per part: 1093 Fetch replies in
  1.27 s, 848 MB/s, with the release build. The debug build reads it at
  639 MB/s. Both include the CRC check of every batch in `delivered`.

## Design

### Layers

```text
atc server -> kafkax::Source (tokio) -> Conn <-> Consumer
                                         |
                              RxBuf: Fetch replies, read in place,
                              then copied once per part into a Chunk
```

`Conn` keeps the socket state and the requests in flight. Its one new job
is to learn partition counts. `Consumer` is a third job next to `Recovery`
and `Producer`, with one Fetch in flight. It reads replies in place and
reports `Delivery` spans. `kafkax::Source` owns the socket, the conn, the
consumer, and the `RxBuf`. After each step it copies each delivered span
into `bytes::Bytes`, so atc's records live beyond the next socket read.

### conn: learned partition counts, Fetch limits

Step 5 replaces the mixed fixed and learned counts of step 1: the conn
always learns. The text below is the design after step 5.

```rust
pub struct Config {
    pub client_id: &'static str,
    /// Topic names. Part numbers count through them in this order once the
    /// counts are known: topic 0 has parts `0..count`, topic 1 the next.
    pub topics: Box<[String]>,
    pub request_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub max_in_flight: u8,
    /// Bound of one sealed batch, header included.
    pub max_batch_bytes: usize,
    /// The most parts one Produce carries: 0 for a consumer, the
    /// accumulator's part count for a producer. Sizes the send buffer.
    pub produce_parts: usize,
}

/// One Fetch's bounds and wait.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchLimits {
    /// Bytes per part. At least the largest batch (broker cap: 1 MiB).
    pub partition_max_bytes: i32,
    /// The broker holds the reply this long for `min_bytes`. Below
    /// `Config::request_timeout`.
    pub max_wait_ms: i32,
    pub min_bytes: i32,
}

pub struct FetchPart {
    pub part: usize,
    pub code: i16,
    pub high_watermark: i64,
    pub log_start_offset: i64,
    pub records: RxSpan,
}

impl Conn {
    /// The partition count of each topic, in config order, from the first
    /// `Up` on. `None` before it.
    pub fn partitions(&self) -> Option<&[u16]>;
    /// Parts over all topics, from the first `Up` on.
    pub fn parts(&self) -> Option<usize>;
    /// The part of `partition` in `topics[topic]`. Panics before the first
    /// `Up` or outside the counts.
    pub fn part(&self, topic: usize, partition: u16) -> usize;
    /// The topic index and partition of `part`.
    pub fn topic_partition(&self, part: usize) -> (usize, u16);
    /// One send method per request type (step 5b).
    pub fn send_init_producer_id(&mut self, now: Instant, out: &mut ConnOut);
    /// Every part at `timestamp`: -1 the end, -2 the log start.
    pub fn send_list_offsets(&mut self, now: Instant, timestamp: i64, out: &mut ConnOut);
    pub fn send_produce<'b>(
        &mut self,
        now: Instant,
        batches: impl Clone + Iterator<Item = (usize, &'b [u8])>,
        out: &mut ConnOut,
    );
    pub fn send_fetch(
        &mut self,
        now: Instant,
        limits: FetchLimits,
        offsets: impl Clone + Iterator<Item = (usize, i64)>,
        out: &mut ConnOut,
    );
}
```

`Conn` keeps `partitions: Box<[u16]>` and `first_part: Box<[usize]>`,
both sized to the topic count in `new`, plus `learned: bool`. `metadata()`
fills them on the first reply and sets `learned`. It compares them on each
later reply. A changed count, a total above `MAX_PARTS`, or a topic with 0
partitions is `Fatal::Metadata`. `part_of`, `targets`, and
`every_partition` read `self.partitions`. `Config::parts()`,
`Config::part()`, and `TopicConfig` go away, so `Conn::part` is the one
numbering. A job that needs a layout checks `Conn::partitions()` on
`ConnOut::up`. fleeb-kafka checks `Some(&[1])` and stops otherwise.

`tx_capacity` sizes ListOffsets and Fetch for `MAX_PARTS` parts, each in
its own topic entry with the longest configured name. That is an upper
bound of about 30 bytes per part. It sizes Produce for `produce_parts`
batches of `max_batch_bytes`. `send_produce` asserts at most
`produce_parts` batches. `Conn::new` asserts 1 to `MAX_PARTS` topics and
`produce_parts <= MAX_PARTS`.

`Request` and `Conn::send` are gone. The four send methods share one
private helper, `fn request(&mut self, now, api, write: impl Fn(&Conn,
&mut Writer<'_>), out)`, which checks the room, takes the correlation id,
pushes the frame, and adds the request in flight. `write` gets the conn
as an argument, because it reads the topics and counts while `request`
holds `&mut self`.

`Recovery` gets its per-part state the way `Consumer` does: a
`heapless::Vec<PartRead, MAX_PARTS>` filled in a `Waiting` phase from
`RecoveryIn::partitions` (`Conn::partitions()`). `Recovery::new` no longer
takes `parts`.

A broker answers the requests of one connection in order, so a request
sent behind a held Fetch waits up to `max_wait_ms`. A conn that long-polls
therefore carries only its consumer. In atc each `Source` owns its socket,
and the producer's conn uses only recovery Fetches with `max_wait_ms: 0`.

`wire::fetch_request(writer, limits, targets)` writes `limits.max_wait_ms`
and `limits.min_bytes`. `max_bytes` stays `partition_max_bytes` times the
part count, so one part never starves another. Recovery passes
`FetchLimits { partition_max_bytes: fetch_max_bytes, max_wait_ms: 0,
min_bytes: 1 }`, so its behavior stays the same.

### consumer: the consumer job (`src/consumer.rs`)

```rust
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

pub struct ConsumerConfig {
    pub select: Select,
    pub start: Start,
    /// Each part stops before the first offset with a timestamp at or above
    /// this. When no record has one: at the end found by `ListEnd`.
    pub stop_timestamp_ms: Option<i64>,
    pub limits: FetchLimits,
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
}

pub struct Consumer {
    config: ConsumerConfig,
    phase: Phase,
    /// The request of this round is out and its reply not yet read.
    waiting: bool,
    parts: heapless::Vec<Part, MAX_PARTS>,
}

pub struct ConsumerIn<'a> {
    pub now: Instant,
    /// `Conn::room()`.
    pub room: usize,
    /// `Conn::parts()`.
    pub parts: Option<usize>,
    pub replies: &'a [Reply],
    pub lost: bool,
    pub rx: &'a RxBuf,
}

/// New records of one part, in the receive buffer. Read them with
/// `delivered` before the next `RxBuf::spare`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub part: usize,
    pub records: RxSpan,
    /// Offsets in `from..until`, without control records.
    pub from: i64,
    pub until: i64,
}

#[derive(Default)]
pub struct ConsumerOut {
    /// Pass the timestamp to `Conn::send_list_offsets`.
    pub list: Option<i64>,
    /// Pass `limits()` and `fetches()` to `Conn::send_fetch`.
    pub fetch: bool,
    /// A retriable error: close the conn, and the round goes again.
    pub close: bool,
    pub deliveries: heapless::Vec<Delivery, MAX_PARTS>,
}

impl Consumer {
    pub fn new(config: ConsumerConfig) -> Self;
    pub fn step(&mut self, input: ConsumerIn<'_>, out: &mut ConsumerOut) -> Result<(), Fatal>;
    pub fn phase(&self) -> Phase;
    pub fn limits(&self) -> FetchLimits;
    /// (part, next) of the next Fetch, in part order: the first starved
    /// part alone, else every selected part below its stop.
    pub fn fetches(&self) -> impl Clone + Iterator<Item = (usize, i64)>;
    /// The part's next offset is at its last high watermark.
    pub fn caught_up(&self, part: usize) -> bool;
    /// The part reached its stop.
    pub fn done(&self, part: usize) -> bool;
    /// The selected parts.
    pub fn selected(&self) -> impl Iterator<Item = usize>;
}

/// The records of `delivery` in offset order: batches read with
/// `batch::read_batch` (CRC, no compression), control batches skipped,
/// records outside `from..until` skipped.
pub fn delivered<'a>(
    rx: &'a RxBuf,
    delivery: &Delivery,
) -> impl Iterator<Item = Result<RecordView<'a>, Fatal>>;

/// Bytes of the largest Fetch reply frame for `parts` parts of `conn`.
/// Size the `RxBuf` with it.
pub fn fetch_reply_bytes(conn: &Conn, parts: usize, limits: FetchLimits) -> usize;
```

`step` is one `match` on the phase.

- `Waiting`: when `parts` is `Some(n)`, fill `parts` with `n` entries and
  mark the selected ones. `Select::One` outside the counts is
  `Fatal::Metadata`. Go to `ListEnd`.
- `ListEnd`: send ListOffsets -1 and store each `end`.
- `ListStart`: for `Start::Timestamp(ts)`, send ListOffsets `ts`, and set
  `next` to the answer, or to `end` when the answer is -1. For
  `Start::Tail(count)`, set `next = (end - count).max(0)` with no request.
  A start below the log start fixes itself through `ListEarliest`.
- `ListStop`: only with `stop_timestamp_ms`. Set `stop` to the answer, or
  to `end` when it is -1. A part with `next >= stop` is done at once.
- `Fetch`: when `room > 0` and no request is out, send one Fetch for
  `fetches()`. For each `FetchPart` in the reply:
  - Code 0: store `high_watermark`. Walk the records blob with
    `split_batch`. Skip a batch whose last offset is below `next`. Stop at
    a cut-short last batch. For whole batches, set `next` to the last
    offset plus one. Push `Delivery { from: old next, until: next.min(stop) }`
    when `until > from`. If no whole batch moved `next` and `next` is below
    the high watermark, the part is starved: the reply budget ran out on
    the parts before it. Mark it, and `fetches()` gives the first starved
    part alone in the next round, which gets its first batch whole. Only
    a Fetch of one part with no whole batch below the high watermark is
    `Fatal::Fetch`, as in recovery: the batch is broken or above the
    broker's cap. Normal rounds resume once no part is starved.
  - Code 1 (`OFFSET_OUT_OF_RANGE`): set `out_of_range`, and go to
    `ListEarliest` after the reply.
  - A code in `RETRIABLE`: set `close`, so the round goes again on the next
    connection. Deliveries already pushed from this reply stay, and their
    `next` moved, so nothing is delivered twice.
  - Any other code, or a request-level code other than 0: `Fatal::Fetch`.
- `ListEarliest`: send ListOffsets -2. For each `out_of_range` part: if
  `next < earliest`, then `next = earliest`, because retention dropped
  records or `Tail` started too far back. Otherwise `next` is above the
  end, which is `Fatal::Fetch`. Then go back to `Fetch`.
- `Done` when every selected part has `next >= stop`.

`lost` repeats the round's request. The offsets move only on a reply, so a
resend asks for the same offsets. A `ConnOut::down` keeps `parts`. On the
next `Up` the conn checks the counts, and the consumer goes on in the same
phase.

Example trace (pnl, 24 parts, `Start::Timestamp(open)`): `Waiting` until
`Up` gives 24. `ListEnd` gives `end[2] = 26736189`. `ListStart` gives
`next[2] = 26499000` and -1 for empty parts, which then get `next = end`.
`Fetch` with 1 MiB per part returns about 1 MiB for part 2 and nothing for
part 0. The reply gives `Delivery { part: 2, from: 26499000, until:
26501200 }`, and part 0 is caught up. The next Fetch asks for part 2 at
26501200. Once every part is caught up, the broker holds the Fetch for
`max_wait_ms` and answers when a record arrives or the wait ends.

Allocation: `parts` and `deliveries` are fixed `heapless::Vec`s, and
`delivered` reads in place. So the consumer adds no allocation after
`Conn::new`, and test 6 covers it.

### kafkax: tokio driver and merge (`~/atc/kafkax`)

```rust
pub struct SourceConfig {
    pub addr: SocketAddr,
    pub topics: Vec<String>,
    pub consumer: ConsumerConfig,
}

/// One part's new records, copied out of the RxBuf once.
pub struct Chunk {
    pub part: usize,
    bytes: Bytes,
    from: i64,
    until: i64,
}

/// A record: key and value are slices of the chunk's bytes, made with
/// `Bytes::slice_ref` on the `RecordView` slices that
/// `consumer::delivered(&chunk.bytes, chunk.from, chunk.until)` gives.
#[derive(Clone, Debug)]
pub struct Record {
    /// One `Arc` per topic, cloned into each record: umdf and the error
    /// contexts name the topic.
    pub topic: Arc<str>,
    pub partition: i32,
    pub part: usize,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
}

pub struct Source {
    socket: Option<tokio::net::TcpStream>,
    conn: Conn,
    conn_out: ConnOut,
    consumer: Consumer,
    consumer_out: ConsumerOut,
    rx: RxBuf,
    /// CLOCK_MONOTONIC base for `kafka_sans_io::time::Instant`.
    base: std::time::Instant,
}

impl Source {
    pub fn new(config: SourceConfig) -> Self;
    /// Runs the machines until a Fetch reply or `Done`. Pushes the reply's
    /// chunks to `out`. Returns false once the consumer is `Done`.
    pub async fn next_chunks(&mut self, out: &mut Vec<Chunk>) -> anyhow::Result<bool>;
    /// Topic name and partition of `part`.
    pub fn topic_partition(&self, part: usize) -> (&str, i32);
    pub fn caught_up(&self, part: usize) -> bool;
    pub fn selected(&self) -> impl Iterator<Item = usize>;
}

impl Chunk {
    pub fn records(&self) -> impl Iterator<Item = anyhow::Result<Record>>;
}

/// Records in arrival order. Ends when the consumer is `Done`.
pub fn records(source: Source) -> impl Stream<Item = anyhow::Result<Record>>;
/// Records by timestamp across parts. See "merge".
pub fn merged(source: Source) -> impl Stream<Item = anyhow::Result<Record>>;
```

The driver loop runs the Composition of the parent plan with `Consumer`
in place of `Recovery`, the same loop as `testing::driver::consume`. It
steps the conn and the consumer (`ConsumerIn::partitions` is
`conn.partitions()`). It acts on `list` with `send_list_offsets`, on
`fetch` with `send_fetch(now, consumer.limits(), consumer.fetches(), ..)`,
and on `close` with `conn.close`. It handles `NetCmd::Connect` and
`NetCmd::Close`, writes `tx`, and reads into `rx.spare()` with a timeout
at `conn.deadline()`. A read of 0 bytes or an error is `NetEvent::Closed`.
The conn config is `topics` (names), `max_batch_bytes: 0`, and
`produce_parts: 0`, because a source never produces.

Cancel safety: umdf polls the stream inside `select!`, so the future of a
`next()` can be dropped at any `await`. `Source` keeps all its state in
itself, and the only `await`s are the socket connect, read, and write,
and the deadline timer. Tokio's read and write are readiness-based, so a
dropped one moves no bytes. Each poll copies the deliveries of a step into
chunks before it returns or awaits again, so a dropped future loses no
delivery. A pending connect is kept in `Source` across polls, so a drop
does not restart it. Test 9 drops the stream's future at random points and
checks that every record still arrives once.
On the first `ConnOut::up` it replaces the 64 KiB handshake `RxBuf` with
one of `fetch_reply_bytes(conn, selected, limits)`. The buffer is empty at
that point, because no job request is out yet. Right after each step it
copies each `Delivery` span into a `Chunk`, before the next `spare`. The
stream is the driver, so a slow reader stops the Fetch loop. That gives
backpressure.

`limits` for every atc source: `partition_max_bytes = 1 << 20`,
`max_wait_ms = 500`, `min_bytes = 1`, and `request_timeout = 10 s`. The
receive buffer for 24 parts is about 25 MiB (`fetch_reply_bytes` bounds the
records at the limit times `parts + 1`), and about 28 MiB for markout.

Merge: per selected part, a `VecDeque<Record>` filled from chunks. Each
round yields the head with the smallest `(timestamp_ms, part)` once every
selected part has a head or is caught up. Otherwise it calls
`next_chunks`. This is the rule of `merge_partitions`, with
`caught_up` in place of `PartitionEOF`, and ties broken by part.
`ChornoOrderedMessage`, the `bitvec` use, and `BorrowedMessage` go away.
`strategy()` takes a `&Record`.

### atc servers

| Server | Change |
|---|---|
| pnl | One `Source` over `ep-messages`, `records()`. The `KafkaError` match goes, because the conn retries retriable codes itself. |
| log | Params: one `Source`, `Start::Timestamp(open)`. Tail: one `Source` per call, `Select::One`, `Start::Tail(4)`. `StreamLog` holds a `SourceConfig` in place of `ClientConfig`. |
| umdf | Main: `Source` over the umdf topics from open. Closing: `Source` with `stop_timestamp_ms`, so its stream ends by itself. The reassign code in `closing_prices_by_sec_id` goes. |
| health, latency | One `Source` over the topic, `merged()`. |
| markout | One `Source` over `ep-messages` and the umdf topics, `merged()`. Today the umdf topics are read at partition 0 only, and every one of them has 1 partition. |

Config: `[kafka]` stays a map in `atc-server/src/main.rs`, and atc reads
only `bootstrap.servers` from it. Other keys are ignored with one `warn!`
line, so the deployed `atc.nix` keeps working before it is edited.

One behavior change: umdf's closing window now excludes the record at the
end offset in every partition. Today it depends on which partition ends
last.

## Tests

In kafka-sans-io, all under `nix develop`:

1. Wire (`tests/wire.rs`): `fetch_request` with random `FetchLimits`
   decodes with `kafka-protocol` to the same `max_wait_ms`, `min_bytes`,
   and `max_bytes`. The Fetch reply reader fills `high_watermark` and
   `log_start_offset`.
2. Conn counts (`tests/conn.rs`, new): learned counts come from the
   Metadata reply. A later Metadata reply with another count is
   `Fatal::Metadata`. A learned total above `MAX_PARTS` is
   `Fatal::Metadata`. Fixed counts behave as today. The old suite passes
   unchanged, apart from `Some(n)`.
3. Consumer property (`tests/consumer.rs`, new) against the model broker.
   The generator draws topics with learned counts, records with random
   timestamps and batch sizes up to `partition_max_bytes`, a log start
   (trim) per partition, `Start`, `Select`, an optional stop, records
   appended while the run goes, and faults (drops, lost replies, retriable
   codes, cut-short batches). Invariants: per selected part, the delivered
   offsets are exactly the stored offsets in `[start, stop)` in order, each
   once. `start` is from the ListOffsets rule, or log start, or end minus
   the tail count. `caught_up` is true exactly when `next` equals the
   model's high watermark. `Done` holds exactly when every part is at its
   stop. No part outside `Select` is fetched.
4. Recovery and producer tests 2 to 4 of the parent plan still pass with
   `FetchLimits`.
5. Mutation audit: each mutation must fail test 3.
   - deliver from the batch start in place of `next`;
   - ignore `stop`;
   - map -1 to 0 in place of `end`;
   - skip `ListEarliest` and fetch from `next` again;
   - move `next` on a cut-short batch.
6. Allocation (`tests/alloc.rs`): a consumer run with drops and retriable
   codes allocates nothing after `Conn::new`.
7. Integration (`tests/integration.rs`, the compio driver gets `consume`).
   Real Redpanda checks:
   - `consume_from_time`: 4 partitions, 10 000 records, start at a middle
     timestamp, compared with `rpk`.
   - `long_poll`: caught up, then one record is produced. It arrives within
     `max_wait_ms` plus 100 ms, and the proxy sees at most
     `2000 / max_wait_ms + 2` Fetches over 2 s idle.
   - `tail`: `Tail(4)` on 10 records gives the last 4, and `Tail(100)`
     gives all 10.
   - `stop_window`: start and stop timestamps give exactly the records
     between them, then `Done`.
   - `learned_counts`: 24 partitions give `parts() == Some(24)`.
   - `trimmed`: `rpk topic trim-prefix` past `next` gives code 1, and the
     consumer goes on from the new log start. The test records what
     Redpanda sends.
   - `consumer_broker_restart`: each record once.
   - `idle_reply`: a caught-up consumer gets an empty reply per part after
     about `max_wait_ms`, with the high watermark set.
   - `replay_rate`: read 1 GB in one partition with one Fetch in flight
     and 1 MiB per part, and record the rate. Step 9 records the same on
     `channel-72` from market open.

In atc:

8. `kafkax` merge unit test: synthetic chunks; the merge yields by
   timestamp, waits for a part that is neither caught up nor buffered, and
   breaks ties by part.
9. `kafkax` driver test: the kafka-sans-io model broker serves a tokio
   `TcpListener` (dev-dependency with feature `testing`). `records()` and
   `merged()` read every record once across a dropped connection, and
   when a `select!` against a random short timer drops the `next()`
   future at random points (the umdf pattern).
10. `cargo build --release -p atc-server` and `ldd` on the binary show no
    `librdkafka`.

## Steps

1. Conn: `TopicConfig.partitions: Option<u16>`, learned counts,
   `Conn::parts`, `Conn::part`, `Conn::topic_partition`, and `FetchLimits`
   in `send_fetch` and `fetch_request`. `FetchPart` gets `high_watermark`
   and `log_start_offset`. Recovery passes its limits. fleeb-kafka in
   `~/simple` gets `Some(n)`. Runs tests 1, 2, and 4, plus `cargo test -p
   fleeb-kafka` in `~/simple`.

   Done (2026-11-05). `FetchLimits` lives in `wire` (`wire::FetchLimits`,
   re-exported as `conn::FetchLimits`), because `wire::fetch_request` takes
   it and `wire` imports nothing from `conn`. `Recovery::fetch_max_bytes()`
   became `Recovery::limits()`, so fleeb-kafka's one `send_fetch` call also
   changed, next to `Some(1)`. `Conn::new` now asserts 1 to `MAX_PARTS`
   topics, because the Metadata check indexes topics in a `MAX_PARTS`
   array. A Metadata reply with a moving leader does not store learned
   counts; only the reply that brings the conn `Up` does. Tests: `wire` 9,
   `conn` 7 (new: fixed counts, learned counts, changed count, total above
   `MAX_PARTS` in three shapes, a topic with 0 partitions, `FetchPart`
   high watermark and log start), `trace` 1, `recovery` 2, `alloc` 2,
   `batch` 1, `accumulator` 1, lib 2: all pass. fleeb-kafka: 23 tests pass.
   Clippy is clean.
2. Model broker: any `max_wait_ms` (it answers at once), a log start per
   partition with `trim`, ListOffsets -2, and `OFFSET_OUT_OF_RANGE` below
   the log start. Runs the existing property tests.

   Done (2026-11-05). `Broker::log_start` per partition, `Broker::trim`
   (moves up only, at most the end), ListOffsets -2, timestamps searched
   from the log start, Fetch replies carry `log_start_offset`, and a fetch
   offset below the log start is code 1. Records below the log start stay
   in `logs` and `batches`, so a Fetch at the log start may still return
   the whole batch that holds it, as a segment-based broker does. The
   existing property tests (`trace`, `recovery`) and `alloc` pass.
3. `src/consumer.rs`, tests 3, 5, and 6.

   Done (2026-11-05). Changes from the design, each because the code
   needed it:
   - `ConsumerIn` has `partitions: Option<&[u16]>` (new
     `Conn::partitions()`, the count of each topic) in place of `parts:
     Option<usize>`, because `Select::One { topic, partition }` needs the
     topic layout to find its part.
   - New `Consumer::next(part)`, so test 3 can check `caught_up` against
     the reply's high watermark. New `Conn::config()`, read by
     `fetch_reply_bytes` for the topic names.
   - `Delivery::records` holds the whole batches from the first one that
     reaches `from` (new `RxSpan::subspan`), not the whole records blob, so
     a copy into a `Chunk` skips old batches and the cut-short tail.
   - ListOffsets replies read only the selected parts. A fault or a -1 on
     another part is not this consumer's concern, and the first test run
     failed with `Fatal::Protocol` on exactly that.
   - A retriable request-level Fetch code closes the conn, as in recovery.
     Other request-level codes are `Fatal::Fetch`.
   - The model broker now answers ListOffsets by timestamp with -1 when no
     record has one, as Kafka does. It answered the end before, so mutation
     "map -1 to 0" survived test 3. The recovery tests still pass, because
     recovery maps -1 to `Empty`. Step 4 checks Redpanda.

   Test 3 (`tests/consumer.rs`, 2048 cases, a clean run and a faulted run
   per case, plus `select_outside_the_counts_is_fatal`): pass, about 7 s.
   Appended records have timestamps above every drawn start and stop, and
   trims during the run wait until the consumer reached `Fetch`, so the
   oracle can compute `[start, stop)` from the log at the start. Test 5:
   each mutation fails test 3: from the batch start (offsets below the
   start), ignore `stop` (offsets at the stop), -1 to 0 (offsets below the
   start, after the model fix), skip `ListEarliest` (no progress), move
   `next` on a cut-short batch (missing offsets). Test 6
   (`consumer_run_allocates_nothing` in `tests/alloc.rs`): 3000 steps with
   learned counts, 8 drops, 7 retriable codes, and 7 rounds of appends
   allocate nothing, and every record is read once. Clippy is clean.
4. Compio driver `consume` and the scenarios of test 7.

   Done (2026-11-05). `testing::driver::consume` runs Conn + Consumer on
   compio, grows the receive buffer to `fetch_reply_bytes` at the first
   `Up`, and calls an async visitor after each step, so a scenario can
   trim, restart the broker, or stop. All 9 scenarios of test 7 pass, and
   the 8 older scenarios still pass (17 in `tests/integration.rs`, about
   16 s). `replay_rate` takes `REPLAY_MB` (default 1024). The measured
   numbers are in "Facts" under "Measured on a local Redpanda". Notes:
   - `consume_from_time` uses the random timestamps of the produce plan,
     which go up and down, and Redpanda's ListOffsets agreed with "the
     first record at or above the timestamp" in every partition.
   - `consumer_broker_restart` first ran with a 4096-byte part limit, below
     its batches, and stopped with `Fatal::Fetch` (see "Facts"). It now
     uses 32 KiB.
   - The full suite (`cargo test --features testing`, all 10 test binaries)
     passes, and clippy is clean.
5. Drop fixed partition counts, as "conn" describes: `Config.topics` is
   names only, `Config.produce_parts` sizes Produce, and
   `Config::part`, `Config::parts`, `TopicConfig`, and the 0 marker go
   away. `Recovery` fills its parts on `Up`. The tests that build configs
   (trace, alloc, recovery, conn, integration) and the compio driver
   follow. fleeb-kafka in `~/simple` passes names, sets `produce_parts:
   1`, and checks `partitions() == Some(&[1])` on `Up`. Tests 2 and 4 to 7
   run, the recovery mutations of the parent plan's "Mutation audit" run
   again, and `cargo test -p fleeb-kafka` runs in `~/simple`.

   Done (2026-11-05). Changes from the design, each because the code
   needed it:
   - `tx_capacity` gives each Produce batch its own topic entry with the
     longest name too, as for ListOffsets and Fetch. Two names of that
     length take turns, so `topic_array` never merges two neighbours.
   - `Recovery` gets a `Waiting` phase, as `Consumer` has, and its part
     list is a `heapless::Vec` of `MAX_PARTS` entries, about 3 KiB inline.
     Clippy then flagged the `Phase` enum of `tests/recovery.rs`, so that
     test boxes its `Recovery`.
   - The producer has no layout check of its own, so each producer caller
     checks `Conn::partitions()` on `Up`, as fleeb-kafka does: the trace
     test against `[3, 1]`, and the compio `driver::produce` against the
     new `Options::partitions`. So integration scenario 6
     (`wrong_partitions`) still ends with `Fatal::Metadata`, now from the
     driver. A count that changes after the first `Up` is still the conn's
     own `Fatal::Metadata`.
   - The compio `driver::recover` grows its receive buffer on the first
     `Up`, as `consume` does, because the part count is unknown before it.
   - fleeb-kafka stops with an error when `partitions()` is not
     `Some(&[1])` on `Up`.

   Tests: `cargo test --features testing` passes all 10 binaries: lib 2,
   accumulator 1, alloc 3, batch 1, conn 6, consumer 2, integration 17,
   recovery 2, trace 1, wire 9. In `tests/conn.rs` the fixed-count tests
   went away, and a new one checks that a Produce above `produce_parts`
   panics. Clippy is clean. fleeb-kafka: 23 tests pass, and its clippy is
   clean. Consumer mutations a to e each fail test 3 again. Recovery
   mutations 8, 9, and 10 of the parent plan each fail `tests/recovery.rs`.
   One parallel run of the integration suite failed in
   `consumer_broker_restart`, inside the harness's wait for the restarted
   Redpanda (`src/testing/redpanda.rs:271`, `wait_ready`), after about
   15 s. So the broker exited before it was ready, because the 60 s
   timeout had not passed. The scenario passed 3 times alone and in 7
   parallel runs of the suite after that. The failure message was not
   kept.
5a. Fix the two integration flakes of step 5.

   Done (2026-11-05). Bug 1, the consumer: `consumer_broker_restart` ended
   with `Fatal::Fetch`, because Redpanda gave a part no bytes below its
   high watermark when the request budget ran out (see "Facts"). The step
   called that an oversized batch. A part with no whole batch below its
   high watermark in a Fetch with other parts is now starved, and the next
   round fetches the first starved part alone. Only a one-part Fetch with
   no whole batch below the high watermark is `Fatal::Fetch`.
   `fetch_reply_bytes` bounds the records at the limit times `parts + 1`,
   because a part may hold the batch that crosses the limit. The model
   broker got `redpanda_fetch`, the measured rule, and test 3 draws it, so
   test 3 now makes parts starve. Test 3 also checks the reply bound on
   every reply, and that a starved part is fetched alone next. New
   integration scenario `starved` reproduces starvation in every round.
   Bug 2, the harness: `free_ports` took ports from port 0, so a parallel
   test could take one before Redpanda bound it ("Address already in
   use"). Now each process takes ports from its own block of 120 below
   the ephemeral range (12 000 + (pid mod 160) x 120), one by one, and
   skips a port that does not bind. `Redpanda::start` also retries with
   new ports, and `restart` with the same ports after 1 s, up to 5 times,
   on that error. The retry did not run in the 10 runs, because the block
   scheme had no clash.

   Tests: 10 runs in a row of `cargo test --features testing --test
   integration` pass, 18 tests each (log:
   `/tmp/claude-1000/-home-anh-atc/b0ca38a3-a672-48af-bbba-38fcf3cb28c6/scratchpad/integration-loop.log`).
   The full suite passes (lib 2, accumulator 1, alloc 3, batch 1, conn 6,
   consumer 2, integration 18, recovery 2, trace 1, wire 9), and clippy is
   clean. fleeb-kafka: 23 pass. Mutations of test 3: a to e fail again.
   f, "starved is fatal", fails with `Fatal::Fetch`. g, "fetch starved
   parts with the others", survived until test 3 checked that a starved
   part is fetched alone next. Plain rounds still make progress in the
   model, because the parts before a starved one catch up and free the
   budget. Now g fails.

5b. One model Fetch rule, and one send method per request type.

   Done (2026-11-05). A: the model broker has one Fetch rule, the one
   measured on Redpanda (see "Facts"), and `redpanda_fetch` is gone. A
   batch cut short at the end of a part, which Apache Kafka sends and
   Redpanda does not, is the fault `Fault::CutShort(Index)`: one part of a
   Fetch reply with two batches or more keeps its last batch to half its
   bytes, at least the batch header. Test 3 draws it as an action. With
   the action removed, mutation e passes test 3; with it, e fails. The two
   regression lines that named `redpanda_fetch` were dropped from
   `tests/consumer.proptest-regressions`. The recovery tests drew
   `fetch_max_bytes` of 1 to 300 bytes, below their batches, which breaks
   the recovery's contract. The old model hid that, because it returned
   each part's first batch whole. Under the Redpanda rule, such a part
   loses the request budget and the recovery stops with `Fatal::Fetch`,
   as parent scenario 5 measured. So test 3 of the parent plan now draws
   the largest stored batch plus 0 to 200 bytes (or 1 MiB), and test 4
   draws `SLOT_BYTES` plus 0 to 300 (or 1 MiB). The recovery needed no
   cut-short batch: it fetches the last batch, and nothing follows it.

   B: `Request` and `Conn::send` are gone. `send_init_producer_id` and
   `send_list_offsets` join `send_fetch` and `send_produce`, and all four
   go through the private `request` helper (see "conn"). Its `write`
   closure takes `&Conn`, because a closure that borrowed `self` could
   not be passed next to `&mut self`. The callers in the tests, the compio
   driver, and fleeb-kafka moved, as did the doc comments in `consumer`,
   `producer`, and `recovery`.

   Tests: `cargo test --features testing` passes all 10 binaries (lib 2,
   accumulator 1, alloc 3, batch 1, conn 6, consumer 2, integration 18,
   recovery 2, trace 1, wire 9), and clippy is clean. Five integration
   runs in a row pass, 18 tests each (log:
   `/tmp/claude-1000/-home-anh-atc/b0ca38a3-a672-48af-bbba-38fcf3cb28c6/scratchpad/integration-loop-5b.log`).
   fleeb-kafka: 23 pass, and its clippy is clean. Consumer mutations a to g
   each fail test 3. Recovery mutations 8, 9, and 10 each fail
   `tests/recovery.rs`.

6. atc: path dependency `kafka-sans-io = { path = "../kafka-sans-io" }` in
   the workspace, `kafkax` driver and merge, tests 8 and 9.

   Done (2026-11-05). `kafkax` has `source.rs` (`SourceConfig`, `Source`,
   `Chunk`, `Record`, `records()`, `bootstrap()`), `merge.rs` (`merged()`
   and the pure `Merge` the stream runs), and `lib.rs` (`LIMITS`,
   re-exports of `ConsumerConfig`, `Select`, `Start`, `FetchLimits`, and
   `strategy(&Record)`). The driver loop is the one of
   `testing::driver::consume`, on tokio: `exchange` waits for one of the
   connect future (kept in `Source`), `TcpStream::ready` followed by
   `try_read` and `try_write`, or a `sleep_until` at `conn.deadline()`.
   `records()` and `merged()` are `stream::unfold`s, so the stream keeps
   its pending future when a `next()` is dropped. Changes from the design:
   - `Chunk` also holds `topic` and `partition`, because a `Record` names
     them and the chunk outlives the `Source` borrow.
   - `Source::caught_up(part)` is `caught_up || done`, so the merge does
     not wait for a part that reached its stop.
   - `merged()` treats every part as idle once the consumer is `Done`, so
     the buffered heads drain and the stream ends.
   - `bootstrap(&kafka)` turns atc's `[kafka]` map into the address and
     logs the one `warn!`, so atc-server calls it once.
   - Test 8 drives `Merge` with synthetic `Record`s in place of chunks:
     a `Chunk` holds encoded batches, and the merge rule does not read
     them.
   Tests: test 8 (3 merge tests: by timestamp, waits for a part neither
   caught up nor buffered, ties by part) and the old strategy test pass.
   Test 9 (`kafkax/tests/driver.rs`): the model broker serves a tokio
   `TcpListener` and closes each connection after 3 to 12 requests before
   it answers the last ones. `records()` and `merged()` each read 480
   records of 4 parts once, in order, with the stored values, with and
   without a `select!` against a timer of up to 300 µs that drops `next()`.
   `merged()` comes out in timestamp order, a stop timestamp ends the
   stream by itself, and each run saw at least one reconnect. 5 tests, 5
   runs in a row pass. No kafka-sans-io change was needed.
7. atc: move pnl, log, umdf, health, latency, and markout, one commit each.
   Each builds and runs its own unit tests.

   Done (2026-11-05), left uncommitted as asked. atc-server reads the
   address once with `kafkax::bootstrap`, and each server's `Config.kafka`
   is a `SocketAddr`. Each source uses `kafkax::LIMITS`. Per server:
   - pnl: one `Source` over `ep-messages` from the open, `records()`.
   - log: params, one `Source` from the open, `records()`. Tail, one
     `Source` per call, `Select::One` and `Start::Tail(4)`. `StreamLog`
     holds the broker address in place of a `ClientConfig`, because each
     call needs its own `ConsumerConfig`.
   - umdf: main, one `Source` over the umdf topics from the open,
     `records()`. Closing, one `Source` from yesterday's close start with
     `stop_timestamp_ms` at its end, so the stream ends by itself.
     `get_assignments` and the reassign code are gone.
   - health, latency: one `Source` over the topic from the open,
     `merged()`. The binding is `kafka_source`, because `source` is the
     server's channel.
   - markout: one `Source` over `ep-messages` and the umdf topics,
     `merged()`.
   Behavior changes:
   - `wire` no longer fetches Metadata or offsets, so a broker that is down
     at start no longer fails `wire`. The source connects in the
     background, retries with backoff (100 ms to 5 s), and stops only on a
     `Fatal`.
   - pnl skipped every non-fatal rdkafka error. Now the conn retries the
     retriable codes itself, and a `Fatal` ends the server's future.
   - umdf's closing window excludes the record at the stop offset in every
     partition (see "atc servers").
   - log tail with a partition outside the topic is `Fatal::Metadata` on
     the stream, where rdkafka waited.
   - `group.id` and `enable.partition.eof` are ignored, with one `warn!`.
   Tests: `cargo test --workspace` passes (17 tests), and `cargo clippy
   --workspace --all-targets` warns only in code this step did not touch:
   the generated tonic code of `rpc`, `umdf-core/src/instrument.rs:76`,
   and `umdf-server/src/chan.rs:38`. Two files of atc use CRLF line
   endings (`umdf-server/src/processors.rs`, `log-server/src/consumer.rs`);
   they keep them.
8. atc: drop `rdkafka` from the workspace and crate `Cargo.toml` files
   (log-server pins its own 0.36.2), from `flake.nix` `buildInputs`, and
   from `shell.nix`. Test 10. Change `/share/infra/nix/atc.nix`: drop
   `rdkafka`, and the user deploys it.

   Done (2026-11-05), except `/share/infra/nix/atc.nix`: it pins an older
   release that still links librdkafka, so its `rdkafka` input changes with
   the version bump the user makes. `rdkafka` left the workspace
   dependencies, every crate's `Cargo.toml` (log-server's own 0.36.2 too),
   `flake.nix` `buildInputs`, and `shell.nix`. `kafka-support` (asm-rust
   0.1.3) also linked librdkafka (see "Facts"), so its rdkafka-free part
   moved into `kafkax::schema_registry`: `SrClient` (`new`,
   `post_key_schema`, `post_value_schema`, `fetch_all_topic_schemas`),
   `decode_srs`, `decode_srs_seed`, `strip_sr_header`, and an `Encoder`
   that writes the key and the value into its own buffers and returns
   them, in place of filling an rdkafka `BaseRecord`. Two changes from
   kafka-support: `Encoder::encode` returns an error for a key or value
   that does not fit its schema, where kafka-support panicked; and
   `fetch_all_topic_schemas` returns `impl Stream + use<>`, because in
   edition 2024 the stream would capture `&self`, and the servers spawn it
   with `spawn_pinned`. `kafka-support` left every `Cargo.toml`, and
   `Cargo.lock` has no `rdkafka` or `rdkafka-sys`. New tests: an encode and
   decode round trip, and `strip_sr_header` errors. Test 10: `cargo build
   --release --target x86_64-unknown-linux-gnu -p atc-server` in `nix
   develop` (now without `rdkafka`) builds, and `ldd` on the binary lists
   only `libgcc_s`, `libc`, and the loader, with no `rd_kafka` symbol.
   `cargo test --workspace`: 17 pass.
9. Live check, read-only against production. Build `atc-server` with
   `cargo build --release --target x86_64-unknown-linux-gnu -p
   atc-server`, as `release.sh` does. Copy the binary and a config to
   b3-colo-1 with `scp` into the user's home directory there, because the
   VPN link is slow and the broker runs on that host. The config is the
   deployed `atc-server.toml` with another `port`. Run it there next to the
   deployed `atc-server-*` service, and compare its pnl, log, and umdf
   answers for one session. Nothing is installed or changed in the host
   config.

## Work breakdown

Each package is owned by the main agent, in this order: WP-01 (step 1),
WP-02 (step 2), WP-03 (step 3), WP-04 (step 4), WP-05 (step 5),
WP-06 (step 6), WP-07 (step 7), WP-08 (step 8), WP-09 (step 9). WP-02 can run in parallel
with WP-01, because it only edits `src/testing/model.rs`. Every other
package depends on the one before it. Each package ends with its tests in
"Steps" passing and one commit in its repo.

## Changes to other plans

- `plans/kafka-sans-io.md` "Goal" and "Layers": the crate also holds a
  consumer job, see `plans/consumer.md`.
- "conn": `Config.topics` is names only, Metadata learns every count, and
  `Config.produce_parts` sizes Produce. `Recovery::new` takes no part
  count. `send_fetch` takes `FetchLimits`. `FetchPart` has
  `high_watermark` and `log_start_offset`. `ConnOut.events` and
  `ConnEvent` are replaced by `ConnOut::up` and `ConnOut::down`.
- "wire": the Fetch row reads "every recovery round and every consumer
  round", and `fetch_request` writes `max_wait_ms` and `min_bytes` from
  `FetchLimits`.
- "Model broker": answers Fetch for any `max_wait_ms`, keeps a log start,
  and answers ListOffsets -2.

## Decisions to confirm

- **Long-poll wait of 500 ms, with `request_timeout` 10 s.** With
  `min_bytes = 1`, the broker answers as soon as any selected part has a
  record. So the wait adds no delay to a new record. It only sets how often
  an idle source asks again: one Fetch per wait. atc has about 7 sources
  that run all day (pnl, log params, umdf main, health, latency, markout)
  plus one for each open log tail. At 500 ms that is about 14 requests a
  second in total, which costs the broker nothing.

  The wait does not set how fast a dead broker is found. The conn closes
  when the oldest request passes `request_timeout`, and a long-polled
  Fetch must be allowed its whole wait. So `request_timeout` must stay
  above `max_wait_ms`, and a longer wait forces a longer timeout and a
  slower reconnect after a broker that stops answering without closing
  the socket. A clean close or a broker restart is seen at once,
  whatever the wait.

  The `caught_up` flag that the merge reads only changes on a reply. A
  part that is idle stays caught up while the Fetch waits, and a new
  record in any part ends the wait. So the wait does not slow the merge.

  librdkafka's `fetch.wait.max.ms` defaults to 500 ms, and atc never set
  it. So 500 ms keeps today's request rate. Test 7 `long_poll` checks
  that Redpanda keeps to the wait. Other choices: 100 ms gives about
  70 idle requests a second and lets `request_timeout` drop toward 1 s.
  5 s gives almost no idle requests but needs `request_timeout` above
  5 s.

## Settled

- **The tokio driver lives in atc `kafkax`** (2026-11-05). kafka-sans-io
  stays free of tokio and IO, and atc is the only tokio user.
- **Conn learns partition counts from Metadata** (2026-11-05). atc config
  keeps no counts, and a count that changes during a run stays
  `Fatal::Metadata`.
- **atc uses a path dependency for now** (2026-11-05). The user publishes to
  the asm-rust registry later. `release.sh` builds locally, so the path
  builds there.
- **The scope covers the crate and the atc migration** (2026-11-05).
- **The `[kafka]` map stays, and atc reads only `bootstrap.servers`**
  (2026-11-05). The deployed `atc.nix` keeps working with no edit.
- **The model matches Redpanda; behaviors Redpanda does not show are
  faults** (2026-11-05). The model broker has one Fetch rule, the measured
  one. A batch cut short at the end of a part, legal in Apache Kafka, is
  `Fault::CutShort`, which the property tests draw like the other faults.
- **One send method per request type** (2026-11-05):
  `send_init_producer_id`, `send_list_offsets`, `send_fetch`, and
  `send_produce`, because no caller holds a `Request`, and Fetch and
  Produce take borrowed iterators.
- **The conn always learns partition counts** (2026-11-05). The fixed
  `Option<u16>` of step 1 gave two numberings (`Config::part` and
  `Conn::part`), a 0 marker for "unknown", and a Produce buffer sized for
  `MAX_PARTS` on a learned topic. The fixed count only checked fleeb-kafka's
  layout, which fleeb-kafka now checks on `Up`. `produce_parts` sizes the
  send buffer in its place.
- **Step 9 runs read-only against production on b3-colo-1** (2026-11-05).
  The binary is built here and uploaded to b3-colo-1, and the run happens
  there, because the VPN link is slow.
- **The partition limit is 1 MiB** (2026-11-05). The broker caps a batch at
  1 MiB (`kafka_batch_max_bytes`), and the largest batch seen is 394 005
  bytes. Redpanda does not return an oversized batch whole after the first
  partition (parent scenario 5).
- **`ConnOut` reports `up` and `down` as two bools** (2026-11-05). One step
  can go up and then down, and a `close` after it adds another down, so an
  `Option<ConnEvent>` would lose the `Up` that the layout check and the
  reconnect count read. The old `ArrayVec<ConnEvent, 8>` dropped events past
  8 with a comment that called them "for logs", which stopped being true
  once `Up` set the partition counts.
