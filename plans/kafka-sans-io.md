# kafka-sans-io: Kafka producer and recovery state machines

Status: Plan

Parent plans, in `../simple`: `plans/fleeb-kafka.md` (sections "Batch", "Producer",
"Offsets"), `plans/fleeb.md`. This plan moves the Kafka parts of fleeb-kafka
into their own crate and verifies them alone. The fleeb-kafka wiring (`Tail`,
`Decode`, Avro, partitioning, schema registration) is out of scope.

## Goal

A crate `kafka-sans-io` that holds pure state machines for:

- an idempotent producer, and
- a recovery job that finds, after a restart, the last record each partition
  committed. The producer then resumes after it.

The crate does no IO. The caller feeds it time, socket events, and received
bytes. It returns socket commands, bytes to send, acks, and recovery results.

Verification:

1. Property tests with the `proptest` crate, against a model broker.
2. Integration tests against a real Redpanda that the test starts on this
   machine. The production cluster is never used (it is not reachable from
   here).

## Facts this plan rests on

- Redpanda in production is 25.3.17, one node. `nix/redpanda.nix` in this
  repo is a stripped copy of `/share/infra/nix/redpanda.nix`: `redpanda` and
  `rpk` only, same `.deb` files and hashes. Both build, and both report
  v25.3.17. Keep its version equal to `/share/infra`.
- Redpanda 25.3.17 ApiVersions ranges (raw ApiVersions v0 request, step 1):
  Produce 0-7, Fetch 4-13, ListOffsets 0-5, Metadata 0-12, ApiVersions 0-4,
  InitProducerId 0-3.
- `kafka-protocol` 0.18.0: feature `client` encodes requests and decodes
  replies; feature `broker` does the reverse. The caller writes the 4-byte
  frame length. Its decode allocates, so the crate codes every message by
  hand (see "wire") and uses `kafka-protocol` only in tests, as the
  reference codec and in the model broker. `PartitionProduceData.records`
  is raw `Option<Bytes>`. `RecordBatchDecoder` decodes RecordBatch v2.
- Our batches are uncompressed, and the tests build `kafka-protocol`
  without compression. A compressed batch gives `Fatal::Fetch`, so a wrong
  assumption fails loudly.
- The old producers write the same topics before the cutover. Their
  records have no `fleeb-pos` header. On the cutover day, the last record
  of a partition can be one of theirs, and the recovery then stops with
  `Fatal::Fetch` (see "Restart rule"). Cut over before the first record of
  a day, so that the floor is after every old record and each partition is
  `Empty`.
- Plain `cargo` outside `nix develop` failed to link in an earlier spike. Run
  all cargo work inside `nix develop`.
- A fresh Redpanda can answer InitProducerId with `NOT_COORDINATOR` (16)
  while its producer id service starts. The full integration suite saw it.
- Right after a broker restart, Metadata v8 answers every partition with
  `LEADER_NOT_AVAILABLE` (5) for about 40 ms.
- ListOffsets with a timestamp after every record answers offset -1 with
  code 0 (scenario 5). The recovery reads -1 and the end offset the same
  way.
- A Fetch whose partition limit (1024 bytes) is below the batch that holds
  the fetch offset (16 305 bytes, second partition of the request) did not
  return that batch whole. The recovery stopped with `Fatal::Fetch`
  (scenario 5). So `fetch_max_bytes` must be at least the largest batch.
- Redpanda never sent `UNKNOWN_PRODUCER_ID` (59) after a broker restart
  (scenario 3). After a drop with 5 sets in flight, all 5 resent copies got
  code 0 (scenario 8). Its duplicate window holds 5 batches.

## Restart rule

This replaces the offset file of `plans/fleeb-kafka.md` ("Offsets").

The contract: every record in every topic carries a position header
(fleeb-kafka: `fleeb-pos`). Each topic has one fixed schema, and
fleeb-kafka's counters go to stdout, so no record is written without the
header. The caller's positions are totally ordered, and per partition the
records go out in position order.

So the last record of a partition holds its newest position, and the
recovery reads only that record. A last record without the header breaks
the contract and gives `Fatal::Fetch`.

At startup:

1. The recovery reads, for each partition, the position in the header of
   the last committed record at or after the floor. Call it `p[i]`, or
   none.
2. The caller replays its source from the start of the day. It rebuilds its
   own state (fleeb-kafka: the Decode dedup table and epoch pair) from every
   record.
3. For each output record, it appends to partition `i` only when the
   position is above `p[i]`.

Why this gives each record once per partition:

- Records of one source position that go to one partition are appended in
  one go, between two seals. So they are in one batch, and a batch commits
  whole. `p[i]` then means "everything at or below it is in partition `i`".
- The producer resends a lost batch with the same producer id, so the broker
  drops a copy within one run.
- One gap stays: a Produce sent by the old process just before it died can
  commit after the recovery read the tail. The broker handles a request in
  milliseconds, and systemd waits at least `RestartSec` (1 s) before the
  restart, so this needs a request stuck in the broker for over a second.
  The `fleeb-pos` header lets a recovery drop such a duplicate.

The old offset file goes away. The broker is the one record of what was
committed.

## Crate

```text
kafka-sans-io/
  Cargo.toml
  src/
    lib.rs
    limits.rs      MAX_IN_FLIGHT, MAX_PARTS
    time.rs        Instant (u64 ns on CLOCK_MONOTONIC), Duration helpers
    frame.rs       4-byte length frame split on the receive buffer
    conn.rs        connection: connect, handshake, requests in flight, retry
    read.rs        Cursor: reads reply and record fields in place
    wire.rs        hand codecs of every request and reply: Api (keys and
                   pinned versions), request and response headers
    wire/
      write.rs     Writer: writes request fields, or counts them
      handshake.rs ApiVersions v3, Metadata v8
      produce.rs   InitProducerId v3, Produce v7
      recovery.rs  ListOffsets v5, Fetch v11
    batch.rs       RecordBatch v2 writer into a fixed buffer, and reader in place
    accumulator.rs per-partition buffers, sealed set, acks
    producer.rs    producer job: InitProducerId, Produce
    recovery.rs    recovery job: ListOffsets, Fetch, header of the last record per partition
    error.rs       RETRIABLE codes, Fatal reasons
    testing.rs     helpers shared by the tests, behind feature `testing`
    testing/
      record.rs      OwnedRecord, batch decoder, record generator
      model.rs       model broker for the property tests
      redpanda.rs    process harness, std socket driver
      redpanda/      frame proxy; tests.rs: smoke tests of the harness and proxy
      driver.rs      compio socket driver of the machines
  tests/
    batch.rs       batch format property: writer, then both readers against the source
    trace.rs       producer trace property
    recovery.rs    recovery property and restart property
    wire.rs        hand codecs against kafka-protocol
    alloc.rs       zero allocations after Conn::new
    integration.rs real-broker scenarios
```

Dependencies: `crc32c`, `heapless`, `zerocopy` (the fixed layouts of the
RecordBatch header and of the wire messages). Dev: `kafka-protocol`
(`default-features = false`, `features = ["client", "broker"]`: the
reference codec of the wire tests, and the model broker decodes requests
and encodes replies), `bytes`, `proptest`, `serde_json` (reads `rpk`
output), `tempfile`.

The crate depends on nothing in `../simple`. fleeb-kafka converts its own
`Instant` at the call site.

Allocation: every buffer is sized at `new()` from the config. After
`Conn::new` the conn makes zero heap allocations over its whole life:
connect, handshake, every request and reply, close, and reconnect. See
"wire".

### Layers

```text
caller IO  <->  Conn  <->  Job (Recovery, then Producer)
                            Producer <-> Accumulator <- caller appends
```

`Conn` owns the socket state and the requests in flight. A job makes
request bodies and reads reply bodies. Both jobs share one conn and one
handshake. The caller runs the `Recovery` job to its end, then the
`Producer` job.

### conn: the connection

```rust
pub const MAX_IN_FLIGHT: usize = 5;

pub struct Config {
    pub client_id: &'static str,
    pub topics: Box<[TopicConfig]>,   // name, partition count
    pub request_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    /// Requests in flight, 1..=MAX_IN_FLIGHT. See "Pipelining".
    pub max_in_flight: u8,
}

impl Config {
    /// Parts count through the topics in order: topic 0 has parts
    /// 0..partitions, topic 1 the next ones. The caller picks a record's
    /// partition and turns it into a part here.
    pub fn part(&self, topic: usize, partition: u16) -> usize;
}

pub struct Conn {
    state: ConnState,
    correlation: i32,
    backoff: Duration,
}

enum ConnState {
    Down { retry_at: Instant },
    Connecting { deadline: Instant },
    /// ApiVersions v3 sent.
    Versions { correlation: i32, deadline: Instant },
    /// Metadata v8 sent for the configured topics.
    Metadata { correlation: i32, deadline: Instant },
    /// Job requests in send order. The broker replies in the same order.
    Up { in_flight: ArrayVec<Sent, MAX_IN_FLIGHT> },
}

struct Sent { api: Api, correlation: i32, deadline: Instant }

pub struct ConnIn<'a> {
    pub now: Instant,
    pub net: Option<NetEvent>,     // Connected, ConnectFailed, Closed
    pub rx: &'a mut RxBuf,         // received bytes; frames are taken off the front
}

pub struct ConnOut {
    pub cmd: Option<NetCmd>,  // Connect, Close
    pub tx: TxBuf,                    // request frames to send
    /// Replies in send order: Produce, InitProducerId, ListOffsets, Fetch.
    pub replies: ArrayVec<Reply, MAX_IN_FLIGHT>,
    /// The connection dropped with requests in flight. All of them are lost.
    pub lost: bool,
    pub events: ArrayVec<ConnEvent, 8>,
}

impl Conn {
    pub fn step(&mut self, input: ConnIn, out: &mut ConnOut) -> Result<(), Fatal>;
    /// `Up` and fewer than `max_in_flight` requests out.
    pub fn can_send(&self) -> bool;
    /// Encodes the job's request. Only when `can_send()`.
    pub fn send(&mut self, now: Instant, request: Request, out: &mut ConnOut);
    /// Close at once. Replies not yet read are dropped. Sets `lost`.
    pub fn close(&mut self, now: Instant, out: &mut ConnOut);
}
```

- `Down -> Connecting -> Versions -> Metadata -> Up`.
- Versions: each pinned version must be in the broker's range. Else
  `Fatal::Version`. Pins: ApiVersions 3, Metadata 8, InitProducerId 3,
  Produce 7, ListOffsets 5, Fetch 11. ListOffsets and Fetch use the newest
  version below the flexible format (KIP-482: compact lengths, tagged
  fields, request header v2). Produce 7, Redpanda's maximum, is below it
  too, so all data requests use one format. The newer versions add nothing
  this crate uses. Fetch returns RecordBatch v2 from v4 in both formats. In
  `kafka-protocol` 0.18 ListOffsets is flexible from v6 and Fetch from v12.
  Redpanda has ListOffsets up to 5 and Fetch up to 13.
- Metadata: each topic exists, has the configured partition count, and has
  the connected broker as leader of every partition. Else `Fatal::Metadata`.
  One exception: a partition with code 5 (`LEADER_NOT_AVAILABLE`) or 6
  (`NOT_LEADER_OR_FOLLOWER`) means a leader is still being chosen. The conn
  then closes, goes `Down`, backs off, and tries the handshake again. The
  topic checks come first, so a missing topic (topic code 3) or a wrong
  partition count stays fatal. A partition code other than 0, 5, or 6, and
  a leader other than the connected broker, stay fatal.
- A reply must match the correlation id at the head of `in_flight`. The
  Kafka protocol guarantees that a broker handles the requests of one
  connection in order and replies in that order. A mismatch, or a frame that
  does not decode, is `Fatal::Protocol`.
- The oldest deadline passed, closed socket, connect failure: `Close`, go
  `Down`, set `lost` when anything was in flight, back off from
  `backoff_min` doubling to `backoff_max`. The backoff resets at `Up`.

### wire: hand codecs

`kafka-protocol` 0.18 allocates when it decodes from `&[u8]`: it copies each
bytes field with `Bytes::copy_from_slice` and builds a `Vec` per array. A
Fetch reply decode copies each partition's whole records blob. Every
message is therefore written by hand, and the crate does not depend on
`kafka-protocol`:

| message | when | request | reply |
|---|---|---|---|
| ApiVersions v3 | each connect | `api_versions_request` | `api_versions_response` |
| Metadata v8 | each connect | `metadata_request` | `metadata_response` |
| InitProducerId v3 | once per process | `init_producer_id_request` | `init_producer_id_response` |
| Produce v7 | every set | `produce_request`, one copy of each batch into `tx` | `produce_response` |
| ListOffsets v5 | twice per recovery | `list_offsets_request` | `list_offsets_response` |
| Fetch v11 | once per recovery, again after a lost or retriable round | `fetch_request` | `fetch_response` |

`request_header` writes request header v1, or v2 for a flexible API.
`response_header` reads the correlation id, and the tagged fields of
header v1. `Api` names the six APIs with their keys and pinned versions.

- Formats. Metadata 8, Produce 7, ListOffsets 5, and Fetch 11 are below the
  flexible format: arrays and bytes have an i32 length, strings an i16
  length, and there are no tagged fields. ApiVersions 3 and InitProducerId
  3 are flexible (KIP-482): compact arrays and strings have an unsigned
  varint length plus one (0 is null), each struct ends with a tagged field
  section, and the request header is v2 (client id still an i16 string,
  then tagged fields). The InitProducerId reply has response header v1.
  ApiVersions answers with response header v0 at every version, so a client
  can read the error of a version the broker lacks.
- Fixed layouts are `zerocopy` big-endian structs (`#[repr(C)]`, alignment
  1): the request header head, the Produce, ListOffsets, and Fetch request
  heads and partitions, the InitProducerId fields, and the fixed parts of
  the replies (API range, partitions, InitProducerId reply, Fetch head). A reader takes them in place with
  `ref_from_prefix`; a writer puts them with `as_bytes`.
- Writers. `Writer` puts fields into a `&mut [u8]` in order, or only counts
  them: `Writer::size(write)` runs `write` on a writer with no slice and
  returns the count. `TxBuf` owns a fixed `Box<[u8]>` with `start..end`
  pending. `TxBuf::push(write)` counts the body with `Writer::size(write)`,
  checks the room, moves the pending bytes to the front when the back is
  short, writes the length field, and runs `write` again on a `Writer` over
  exactly that many bytes; it asserts the writer filled them. One closure
  does both passes, so the count and the write cannot differ. `Conn::new` sizes `tx` with the same count passes over every part
  (the handshake plus `max_in_flight` of the largest request).
- Readers. A reader walks the body with a cursor. Array-shaped replies call
  a visitor per entry (per API range, per broker, topic, and partition of
  Metadata, per partition of the rest); fixed replies return a struct. A
  short field, a negative length, a malformed varint, or bytes after the
  last field give `Fatal::Protocol`. It never panics. It skips every tagged
  field, known or not, by its size. The Metadata reader gives only the
  broker node ids, each topic's error code, name, and partition count, and
  each partition's error code, index, leader, and leader epoch; it skips
  the rest.
- An ApiVersions reply with an error code other than 0 ends the read at the
  code: the broker then answers in v0, which has another layout. The conn
  turns it into `Fatal::Version`.
- The Fetch reader skips the aborted transactions (the recovery reads
  uncommitted). It returns each partition's records as a place in the body.
  `FetchPart::records` is an `RxSpan`: a place in the `RxBuf`. The recovery
  reads it there with `RxBuf::slice`, so the records are never copied.
  `RecoveryIn` carries the `RxBuf` for this. A span ends at the next
  `RxBuf::spare` or `clear`, which may move the bytes; `slice` panics on an
  ended span.
- Module graph: `read` (crate-visible `Cursor`) imports only `error`.
  `wire` imports `read`, `error`, and `limits`; its submodules import their
  siblings and never `wire.rs`. `batch` imports `read` and `error`.
  `frame` imports `wire::Writer`; `conn` imports `wire` and `frame`;
  `recovery` imports `batch`, `error`, `frame`, `conn`, and `time`.

### batch: RecordBatch v2, both ways

`batch` owns the format: the writer for the producer, the reader for the
recovery. Free functions over a byte slice and a plain `Copy` struct. No
owned buffer.

Writer:

```rust
#[derive(Clone, Copy, Default)]
pub struct BatchMeta { len: u32, count: i32, base_ts: i64, max_ts: i64 }

pub struct Record<'a> {
    pub timestamp_ms: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [(&'a str, Option<&'a [u8]>)],
}

/// Writes the record in its final form after the 61-byte header room.
/// `Full` leaves `buf` and `meta` unchanged.
pub fn append(buf: &mut [u8], meta: &mut BatchMeta, record: &Record) -> Result<(), Full>;
/// Writes the header: base offset 0, timestamps, producer id and epoch, base
/// sequence, count, CRC32C. Returns the batch bytes.
pub fn seal<'a>(buf: &'a mut [u8], meta: &BatchMeta, id: ProducerId, base_sequence: i32) -> &'a [u8];
```

The record size is computed before the write, so `append` never writes a
partial record.

Reader, in place and with no allocation. Every read error is
`Fatal::Protocol`; the recovery reports it as `Fatal::Fetch`.

```rust
pub struct SplitBatch<'a> { pub batch: &'a [u8], pub rest: &'a [u8] }
/// The first whole batch. `None` when the bytes are fewer than its length
/// says: a Fetch may end with a batch cut short.
pub fn split_batch(records: &[u8]) -> Result<Option<SplitBatch<'_>>, Fatal>;

pub struct BatchView<'a> { pub last_offset: i64, pub control: bool, pub count: i32, pub records: Records<'a> }
/// Checks exactly one whole batch: its length, magic 2, the CRC from
/// `attributes`, and no compression.
pub fn read_batch(batch: &[u8]) -> Result<BatchView<'_>, Fatal>;

/// `Records`: Iterator<Item = Result<RecordView, Fatal>>, `count` items.
pub struct RecordView<'a> {
    pub offset: i64,          // base offset + offset delta
    pub timestamp_ms: i64,    // base timestamp + timestamp delta
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Headers<'a>, // Iterator<Item = Result<RecordHeader, Fatal>>
}
pub struct RecordHeader<'a> { pub key: &'a [u8], pub value: Option<&'a [u8]> }
```

Varints (zigzag, 7 bits per byte, low group first, at most 10 bytes) and
varint-length nullable bytes (-1 is null) are `Cursor` readers in `read`.

### accumulator: the sealed sets

`Accumulator` sits between the caller's appends and the producer:

1. Builds batches. `append(part, record)` writes the record into that
   partition's batch in its final RecordBatch v2 form. `Full` makes the
   caller stop reading its source.
2. Seals them at one time. `seal` closes every non-empty partition's batch
   together into one set: header, producer id, base sequence, CRC. It
   advances each partition's sequence and keeps the caller's position with
   the set.
3. Keeps sets until acked. The producer sends the sealed bytes, and sends
   the same bytes again after a lost reply. `ack` frees each acked
   partition of the oldest set, and returns its position when the set is
   done. Sets finish in seal order.

One allocation holds every batch. `Part` is plain numbers.

```rust
pub const MAX_SLOTS: usize = MAX_IN_FLIGHT + 1;

pub struct Accumulator<T> {
    /// `parts.len() * slots` slots of `slot_bytes`, `slots = max_in_flight + 1`.
    /// Part `i`, slot `k` is at `(slots * i + k) * slot_bytes`.
    arena: Box<[u8]>,
    slot_bytes: usize,
    slots: u8,
    parts: Box<[Part]>,
    /// Sealed sets, oldest first.
    sets: ArrayDeque<Set<T>, MAX_IN_FLIGHT>,
    /// Number of the next set to seal. Counts up for the life of the process.
    next_set: u32,
}

#[derive(Clone, Copy, Default)]
struct Part {
    next_sequence: i32,
    /// Ring of slots: `sealed` sealed slots from `head`, then the filling slot.
    head: u8,
    sealed: u8,
    meta: [BatchMeta; MAX_SLOTS],
    /// Set number of each sealed slot.
    set: [u32; MAX_SLOTS],
}

struct Set<T> {
    number: u32,
    token: T,
    /// Parts not yet acked.
    pending: PartSet,
}
```

- `append(part, &Record) -> Result<(), Full>` and `free(part)`. The caller
  checks space in every part of a source record before it appends.
- `seal(id, token)`: when fewer than `slots - 1` sets are sealed and any
  part has records. Each non-empty part writes the header in its filling
  slot with `base_sequence = next_sequence`, sets `next_sequence += count`,
  and the slot joins the sealed ring. The next slot starts empty.
- `ack(set, parts) -> Option<T>`: `set` must be the oldest. Drops acked
  parts from `pending` and frees their slots. When `pending` is empty, pops
  the set and returns its token. The token is for the caller's stats;
  restarts use the recovery.
- `sets()`: the sealed sets, oldest first. `batches(set)`: an iterator of
  `(part, &[u8])` over its pending parts, sliced from the arena.

With `max_in_flight = 1` this is the two halves per part of the first draft.

### producer: the produce job

```rust
pub enum Producer {
    /// No producer id. Send InitProducerId when the conn can send.
    NeedId,
    IdInFlight,
    /// `sent`: sealed sets out on this connection, oldest first. Each is one
    /// Produce v7 with the set's pending parts, `acks = -1`.
    Running { id: ProducerId, sent: u8 },
}

pub struct ProducerIn<'a, T> {
    pub now: Instant,
    pub can_send: bool,
    pub replies: &'a [Reply],
    pub lost: bool,
    pub accumulator: &'a Accumulator<T>,
}

pub struct ProducerOut {
    pub requests: ArrayVec<Request, MAX_IN_FLIGHT>,  // the caller passes each to `Conn::send`
    /// Acks for the oldest sets, in order.
    pub acked: ArrayVec<(u32, PartSet), MAX_IN_FLIGHT>,
    /// Close the conn now. Replies after the failed one are not read.
    pub close: bool,
}
```

- The id is kept for the life of the process. A lost InitProducerId goes back
  to `NeedId`.
- InitProducerId with a retriable code, or with 14
  (`COORDINATOR_LOAD_IN_PROGRESS`), 15 (`COORDINATOR_NOT_AVAILABLE`), or 16
  (`NOT_COORDINATOR`), sets `close` and
  goes back to `NeedId`. The conn backs off, and the job asks again on the
  next connection. Any other code is fatal. Codes 14, 15, and 16 are kept out of
  the shared retriable list: they name the coordinator, which only
  InitProducerId uses. A Produce, ListOffsets, or Fetch part with them means
  a broken broker, and stays fatal.
- Send: while the conn can send and `sent` < the number of sealed sets, send
  the next set and count it.
- Reply for the oldest sent set, per partition: no error or
  `DUPLICATE_SEQUENCE_NUMBER` (46) acks the part.
- **Close on the first failure.** Any other result (a retriable code 5, 6, 7,
  19, 20, or `OUT_OF_ORDER_SEQUENCE_NUMBER` 45 that follows it in the same
  reply) sets `close`, acks the parts that succeeded, and sets `sent = 0`.
  The replies after it are never read. On the new connection, every unacked
  set goes out again in seal order with the same bytes. A copy of a batch
  the broker has is a duplicate; the rest commit in sequence.
- `lost`: `sent = 0`, the same resend.
- Fatal: a code outside the retriable list, or 45 on the first reply after a
  resend (that means the broker lost committed data).

### Pipelining

`max_in_flight` = `K`, from 1 to 5.

- The bound 5 comes from the broker. Kafka keeps the last 5 batches per
  (producer id, partition) for duplicate checks. A resend inside that window
  gets success with the first offset. An older one gets 45. Each set has at
  most one batch per partition, so `K <= 5` keeps every resend inside the
  window. Scenario 8 checks Redpanda's window.
- fleeb-kafka starts with `K = 1`. A seal happens at most once per tick, and a
  round trip to the one Redpanda node is shorter than the 10 ms tick, so
  `K = 1` already sends one set per tick. `K > 1` pays when round trips
  grow longer than a tick (a remote or multi-node cluster).
- The crate supports every `K` from the start, and the tests draw `K` from
  1 to 5. Raising it in fleeb-kafka is a config change.
- rdkafka recovers in a live connection (drain, sort retries, epoch bump).
  Close on the first failure plus a full resend in order replaces all of
  that.

### recovery: the recovery job

```rust
pub struct RecoveryConfig {
    pub header_key: &'static str,
    /// No record older than this is read (fleeb-kafka: start of the day).
    pub floor_timestamp_ms: i64,
    /// Fetch bytes per part: at least the largest batch in the topics.
    pub fetch_max_bytes: i32,
}

pub enum Phase {
    /// ListOffsets with timestamp -1 (end) for every partition.
    ListEnd,
    /// ListOffsets with `floor_timestamp_ms` for every partition.
    ListFloor,
    /// One Fetch of the last record of every part with a record at or after
    /// the floor.
    Fetch,
    Done,
}

/// Per partition.
enum PartRead {
    Unknown,
    Listed { end: i64 },
    Fetching { last: i64 },   // end - 1
    Found(Found),             // offset, header value in a fixed buffer
    Empty,                    // no record in [floor, end)
}
```

- `ListEnd`, then `ListFloor`. A part whose floor offset is -1 (no record
  at or after the floor timestamp) or at or above its end is `Empty`. Else
  it is `Fetching { last: end - 1 }`.
- `Fetch` sends one Fetch over all `Fetching` parts at `last`, isolation
  `READ_UNCOMMITTED`, `max_wait_ms = 0`. Per part, the reply's batches are
  read in order. Batches that end before `last` are skipped. The first
  batch with its last offset at or above `last` must hold the record at
  `last`, and that record's last header with the key is the result.
  Records appended after `ListEnd` are ignored.
- `Fatal::Fetch` for a broken contract or a broker reply that cannot hold
  the record:
  - the record at `last` has no header with the key;
  - no whole batch holds `last`: the reply is empty, or the batch is cut
    short. `ListEnd` read the end, and a Fetch below the high watermark
    returns the batch that holds the offset, so this happens when the
    batch is larger than `fetch_max_bytes` (scenario 5). A high watermark
    that moved back would look the same. Both need a person. The recovery
    does not retry in place; a process restart retries after `RestartSec`;
  - the batch is a control batch. The producer is not transactional, so
    its topics hold none;
  - a compressed or corrupt batch, or a header value above
    `MAX_HEADER_VALUE`.
- A lost request repeats the same round. Errors in ListOffsets or Fetch:
  retriable codes as in the producer, others fatal. A retriable code closes
  the conn, and the round goes again on the next connection. This covers a
  part with 5 or 6 after a broker restart.
- `Done` when no part is `Fetching`. The caller reads `results()`, an
  iterator of `Option<Found>` per part, read from the part states, and
  parses each header value as its position.

`recovery.rs` holds only this: per part its read state, and the copy of the
header value (up to `MAX_HEADER_VALUE`). Its `last_record` reads a Fetch
records blob with `batch::split_batch` and `batch::read_batch`; the caller
of `last_record` maps every error to `Fatal::Fetch` in one place. A Fetch
reply is read in place, so recovery allocates nothing.

### Composition

```rust
conn.step(ConnIn { now, net, rx }, &mut conn_out)?;
match phase {
    Phase::Recover => {
        recovery.step(RecoveryIn { now, can_send: conn.can_send(), replies, lost }, &mut c_out)?;
        if let Some(request) = c_out.request { conn.send(now, request, &mut conn_out) }
        if recovery.done() { skip = recovery.results(); phase = Phase::Produce }
    }
    Phase::Produce => {
        producer.step(ProducerIn { now, can_send: conn.can_send(), replies, lost, accumulator: &acc }, &mut p_out)?;
        for (set, parts) in p_out.acked { acc.ack(set, parts); }
        if p_out.close { conn.close(now, &mut conn_out) }
        for request in p_out.requests { conn.send(now, request, &mut conn_out) }
        if let Some(id) = producer.id() { acc.seal(id, position) }
    }
}
```

## Property tests (proptest)

### Model broker

A `Vec` of committed batches per partition, each a list of decoded records
with offsets. It applies the Kafka idempotence rule: it keeps the last
sequence per (producer id, partition) and the last 5 batches. A batch at
the next sequence commits. A copy of one of the last 5 batches answers
success with its first offset and commits nothing. Any other sequence answers
45. It handles the requests of one connection in order and replies in that
order. A dropped connection may have handled any prefix of the requests in
flight. ListOffsets
answers the end offset and, for a timestamp, the first offset with a
timestamp at or above it. Fetch returns whole stored batches from the one
that holds the fetch offset, up to the byte limit, always at least one batch.

It decodes requests with `kafka-protocol` (`broker` feature) and batches
with `RecordBatchDecoder`. Its shape (lists of decoded records) differs from
the crate's (byte buffers), so the tests check the rules, and the crate's
layout is free to change.

### Generator shape

Proptest has no state-dependent generator with good shrinking, so each test
generates a `Vec<Action>` and interprets each action in the current state.
Vec shrinking then drops steps from the end and the middle. The first action
in the weights is `Tick { small }`, so a shrink turns an action into a no-op.

| action | meaning |
|---|---|
| `Append { part, records }` | the caller appends records, with free-space check |
| `Tick { gap }` | time moves: small, or past the deadline |
| `Deliver { chunk }` | the next `chunk` bytes of pending replies reach `rx` |
| `Reply(Ok)` | the broker handles the oldest request in flight |
| `Reply(PartError { part, code })` | one partition of the oldest request gets a retriable error |
| `InitError { code }` | the broker handles the oldest request; an InitProducerId answers 14, 15, or 16 |
| `LeaderMoving { part, code }` | the broker handles the oldest request; one partition of a Metadata reply answers 5 or 6 with no leader |
| `Drop { handled }` | the broker handles the first `handled` requests in flight, then the socket closes before any reply |
| `ConnectFails` | the next connect fails |
| `WrongPartitions` | the next Metadata reply has a different count |
| `Crash` | the process dies: drop every machine, keep the broker (restart test only) |

`InitError` and `LeaderMoving` apply only when the oldest request is of
their kind. On another request they act as `Reply`. Both have a low weight.
Test 2 and test 4 use both. Test 3 uses `LeaderMoving`; it sends no
InitProducerId.

### Test 1: batch format

Generate up to 200 records: nullable key and value of 0-300 bytes, 0-3
headers, timestamps that go up, down, and repeat. Append into a byte slice
of a generated capacity, and seal with a generated producer id, epoch, and
base sequence. The source is the records `append` accepted, in order. One
property, `batch_format`, reads the sealed batch with `RecordBatchDecoder`
and with the crate's reader (`split_batch`, `read_batch`). It checks each
reader against the source, never one reader against the other: two readers
that agree can share a wrong encoding.

- `full rule`: `append` returns `Full` only when the record does not fit, and
  the batch never exceeds the capacity.
- `kafka-protocol`: the decoder accepts the batch (so the CRC is right).
  Timestamps, keys, values, and headers equal the source. Offsets are `0..n`,
  sequences are `base..base + n`, and the count, producer id, and epoch
  equal the inputs.
- `reader`: `split` (the whole batch, nothing after it), `count`, `control
  bit` (false), `last offset`, `offsets` (`0..n`), and timestamps, keys,
  values, and headers equal the source.
- Bad batches, on the crate's reader: `crc` (one flipped byte after the CRC
  field fails), `compression` (a codec in the attributes, CRC made over it,
  fails), and `truncated` (a cut-short batch splits as `None` and reads as an
  error, with no panic).

Mutation check of the reader, full suite with `--no-fail-fast`,
`PROPTEST_MAX_SHRINK_ITERS=50`:

| mutation | caught by |
|---|---|
| varint: zigzag decode without the sign negation | `batch_format` `reader: offsets`; test 3 `last header`; test 4 `exactly once across restarts`; scenarios 4 and 5 (`Fatal::Fetch`) |
| `read_batch`: skip the CRC check | `batch_format` `crc` |
| header value: take one byte more than the length | `batch_format` `reader: records`; tests 3 and 4 `fatal` (`Fatal::Fetch`); test 6; scenarios 4 and 5 |

### Test 2: producer trace

Conn + Producer + Accumulator against the model broker, without `Crash`.
`max_in_flight` is drawn from 1 to 5.
Each invariant is its own assertion:

- `exactly once`: after a healthy drain phase, each appended record is
  committed once.
- `order`: per partition, commit order equals append order.
- `in flight`: at most `max_in_flight` requests without their reply.
- `token order`: `ack` returns tokens in seal order.
- `same bytes`: a resent batch is byte-identical to its first send.
- `handshake`: on each connection, no job request before the Versions and
  Metadata replies, and InitProducerId once per process.
- `progress`: in the drain phase (healthy broker, time moves), every
  appended record commits within a bounded number of ticks.
- `fatal`: `Fatal::Metadata` comes from `WrongPartitions` only. After any
  fatal, the trace ends. `InitError` and `LeaderMoving` never give a fatal,
  and `progress` holds after them.

### Test 3: recovery

Generate partition contents in the model broker directly: batches of
records with timestamps that rise. Every record has the header; some have
a null header value, and some have another header before it. In about one
case in ten, the last record of one partition has no header: a deliberate
break of the contract. Also generate the floor timestamp, the fetch byte
limit, and conn faults. Run Conn + Recovery to `Done` or a fatal.

- `last record`: the outcome equals the rule computed straight from the
  model's list: per partition, the header of the last record at or after
  the floor, or none. `Fatal::Fetch` when that record has no header.
- `bounded`: the recovery sends at most 3 requests (ListEnd, ListFloor, one
  Fetch), plus one for each step that lost a request or closed the conn.
- `faults`: lost requests and retriable errors change no outcome.

### Test 4: restart

Conn + Producer + Accumulator with `Crash`. The source is a list of
positions; each position gives 1-3 records over random partitions, each
with the position header. After each `Crash`, a new process runs the
recovery, then replays the source from the start with the skip rule
(`position > p[i]`).

- `exactly once across restarts`: at the end, per partition, each source
  record is committed once, in order.
- `skip safe`: the skip rule never drops a record that is not yet committed.

### Mutation audit

Apply each mutation by hand, run the tests with `PROPTEST_MAX_SHRINK_ITERS`
low, check that a test fails, and record the result in this plan:

1. Producer: send InitProducerId again on each reconnect.
2. Accumulator: reset `next_sequence` to 0 at seal.
3. Producer: ack a part on a retriable error.
4. Accumulator: seal again while a set is pending.
5. Conn: send one request more than `max_in_flight`.
6. Accumulator: clear the sealed buffer before its ack.
7. Conn: ignore the correlation id.
8. Recovery: Fetch at `end` in place of `end - 1`.
9. Recovery: take the first record of the batch in place of the record at
   `end - 1`.
10. Recovery: ignore the floor (every part with records is fetched).
11. Accumulator: allow a seal between two appends of one source position.
12. Producer: keep reading the replies after a failed one, in place of a
    close.
13. Producer: after a close, resend only the failed set and not the sets
    after it.
14. Producer: InitProducerId code 15 or 16 is fatal (the bug before the
    fix).
15. Conn: a Metadata partition with code 5 or 6 is fatal (the bug before
    the fix).

Results for test 2 (`tests/trace.rs`), 256 cases, `PROPTEST_MAX_SHRINK_ITERS=50`:

| # | caught | by |
|---|---|---|
| 1 | yes | `handshake`: the producer id changes after a reconnect |
| 2 | yes | `same bytes`: a second batch goes out with base sequence 0 |
| 3 | yes | `exactly once`: a record of the failed part never commits |
| 4 | yes | the model cannot decode the sent batch (the extra seal resets the oldest sealed slot); with `K = 5` the accumulator's own `unreachable!` fires first |
| 5 | no | The accumulator holds at most `K` sealed sets, and InitProducerId never shares the conn with a Produce. So the producer never asks for more than `K` sends, and the conn limit is a second guard. Still not caught at 5000 cases. The recovery leg shares the conn and may reach it. |
| 6 | yes | the model cannot decode the sent batch |
| 7 | no | The model broker replies in order, as Kafka does, and the conn clears `rx` on each connect, so no reply reaches the wrong request. Still not caught at 5000 cases. The check guards against a broken broker only. |
| 12 | yes | `fatal`: the next reply matches the failed set, `Fatal::Protocol` |
| 13 | yes | `fatal`: a later reply matches the wrong set, `Fatal::Protocol` |
| 14 | yes | `fatal`: `Fatal::Produce { code: 15 }` after an `InitError`. Test 4 catches it too. |
| 15 | yes | `fatal`: `Fatal::Metadata` after a `LeaderMoving`, with no wrong Metadata delivered. Tests 3 and 4 catch it too. |

Results for tests 3 and 4 (`tests/recovery.rs`), 256 cases,
`PROPTEST_MAX_SHRINK_ITERS=50`, and test 6 (`tests/alloc.rs`) for 8 to 10:

| # | caught | by |
|---|---|---|
| 8 | yes | test 3 `last record`: the Fetch at `end` reads nothing, `Fatal::Fetch` where the rule gives a record. Test 4 `fatal`: `Fatal::Fetch`. Test 6: `Fatal::Fetch` with a healthy broker. |
| 9 | yes | test 3 `last record`: a batch of several records gives its first offset. Test 4 `exactly once across restarts`: the replay sends records again. Test 6: part 0 gives the wrong offset. |
| 10 | yes | test 3 `last record`: a floor after every record gives a record where the rule gives none. Test 4 does not catch it: its floor timestamp is 0. Test 6 neither: its floor is 0. |
| 11 | yes | test 4 `skip safe`: a record of a position is skipped while its other batch is not committed. The mutation is in the test caller, which seals after each append; the accumulator cannot see positions. |

Mutations 8 to 10 replace three mutations of the old backward window scan
(keep the first header record of a window, step back by `window - 1` or
`window + 1`, read records before the cursor as new). That scan went away
with the contract that every record carries the header. Each new mutation
was applied alone to `recovery.rs`, tests 3, 4, and 6 ran, and the file was
restored byte for byte (`cmp`).

### Test 5: hand codecs (`tests/wire.rs`)

`kafka-protocol` with the `client` and `broker` features is the reference.
512 cases each.

- Replies: `kafka-protocol` encodes a random reply after its response
  header (v1 with random unknown tagged fields for InitProducerId), and the
  hand reader reads the frame through `response_header`. Each fact is its
  own check:
  - ApiVersions v3: `correlation id`, `error code`, `ranges`. Unknown tagged
    fields in each range and at the end, and the known tagged fields
    (finalized features epoch, zk migration ready), are skipped.
  - Metadata v8: `broker node ids`, then in reply order each topic's error
    code, name (null drawn), and partition count, followed by its
    partitions' error code, index, leader, and leader epoch. Hosts, racks,
    cluster id, replica lists, and authorized operations are drawn and
    skipped.
  - InitProducerId v3: `correlation id`, `throttle`, `error code`, `producer
    id`, `producer epoch`, with tagged fields in the header and the body.
  - ListOffsets v5: `throttle`, `partition count`, and per partition `topic
    name`, `partition index`, `error code`, `timestamp`, `offset`, `leader
    epoch`.
  - Produce v7: `throttle`, `partition count`, and per partition `topic
    names`, `partition index`, `error code`, `base offset`, `log append
    time`, `log start offset`.
  - Fetch v11: the head (`throttle`, `request error code`, `session id`)
    and each partition (`topic name`, `partition index`, `error code`, the
    three offsets, `aborted transactions skipped`, `preferred read
    replica`, `records in place`). Null and empty records and null aborted
    arrays are drawn.
- Every reader: every cut-short frame gives `Fatal::Protocol`, and so does
  one byte too many.
- `garbage_never_panics`: random bytes, and each valid reply with one byte
  changed, give a result or `Fatal::Protocol` in all six readers, never a
  panic.
- `api_versions_error_ends_the_read`: a v0 reply with error 35 reads as code
  35 and no range.
- `requests_decode`: one random client id, correlation id, and set of
  topics with (partition, offset, batch) targets drive all six requests.
  Each is written by hand, header included, and decoded by
  `RequestHeader::decode` at the header version `kafka-protocol` gives for
  the API, then by the message decoder. Per request: `size pass` (the count
  pass equals the bytes written), header `api key`, `api version`,
  `correlation id`, `client id`, no bytes left, and the body facts:
  software name and version; Metadata topic names and three false flags;
  InitProducerId null transactional id, timeout, producer id and epoch -1;
  ListOffsets replica id, isolation, topics and partitions, timestamp,
  leader epoch -1; Produce no transactional id, acks -1, timeout, topics,
  partitions, and batches; Fetch replica id, no wait, isolation, no
  session, max bytes, topics, partitions, offsets, and partition max bytes.

### Test 6: zero allocations (`tests/alloc.rs`)

Its own test binary with a counting `#[global_allocator]`. The count is per
thread, and only while a crate call runs. The model broker, the network
copy, and the checks run outside the count.

- `counter_counts`: one `Box` counts as one allocation.
- `conn_lifecycle_allocates_nothing`, with `max_in_flight` 1 and 5. Every
  buffer and machine is made before the count. 4 parts, each with 60
  batches of 3 records, every record with the header. One counted window
  runs: connect, ApiVersions, Metadata, both ListOffsets rounds, the Fetch
  to `Done`, a close, a reconnect with a new handshake, InitProducerId, and
  2000 producer steps (`Conn::step`, `Producer::step`, the acks,
  `send_produce`, one append per part, and a seal). The window makes 0
  allocations. It did real work: at least one Fetch step, each part finds
  its last record (offset 179) and its header value, exactly 2 connects,
  at least 950 sets acked, and the broker committed every record of the
  acked sets.

Mutation check of the hand codecs, one at a time.

First round, with only the Produce reply and the Fetch request and reply
by hand, full suite:

| mutation | caught by |
|---|---|
| Fetch reply: read the last stable offset before the high watermark | test 5 `high watermark, last stable, log start` |
| strings: take one byte more than the length | test 5 (both readers), tests 2, 3, 4, 6, and 6 of 8 Redpanda scenarios: `Fatal::Protocol` |
| Produce reply: skip the log start offset | test 5 `log start offset`, tests 2, 4, 6, and 6 Redpanda scenarios |
| Produce reply: also decode with `ProduceResponse::decode` | test 6: 5000 allocations in the producer window |

Second round, with every message by hand, tests 2 to 6,
`PROPTEST_MAX_SHRINK_ITERS=20`:

| mutation | caught by |
|---|---|
| compact string: write the length, without the plus one | test 5 `requests_decode` (ApiVersions body does not decode); tests 2, 3, 4, 6: the model broker cannot decode ApiVersions |
| ApiVersions reply: skip the tagged field section of each range | test 5 `api_versions_reply_reads_back` (a whole reply does not read); tests 2, 3, 4, 6: `Fatal::Protocol` on the model's reply |
| Metadata reply: leader id before partition index | test 5 `metadata_reply_reads_back` (partition index, leader); tests 2, 3, 4: `Fatal::Metadata` with no wrong Metadata delivered; test 6 |
| request header: v1 for InitProducerId (no tagged fields) | test 5 `requests_decode` (InitProducerId body does not decode); tests 2, 4, 6: the model broker cannot decode InitProducerId. Test 3 sends no InitProducerId. |

## Integration tests (real Redpanda)

### Nix

`nix/redpanda.nix` goes into the dev shell:

```nix
redpanda = callPackage ./nix/redpanda.nix { };
# devShell nativeBuildInputs += [ redpanda.redpanda redpanda.rpk ]
```

### Harness (`src/testing/redpanda.rs`)

- `Redpanda::start()`: a temp dir, three free ports on `127.0.0.1` (bind
  three listeners to port 0, read the ports, close), and two files. Step 1
  confirmed this setup. `redpanda.yaml` holds the node properties:

  ```yaml
  redpanda:
    data_directory: <dir>/data
    developer_mode: true
    seed_servers: []
    rpc_server: { address: 127.0.0.1, port: <rpc> }
    kafka_api: [{ address: 127.0.0.1, port: <kafka> }]
    advertised_kafka_api: [{ address: 127.0.0.1, port: <kafka> }]
    admin: [{ address: 127.0.0.1, port: <admin> }]
  ```

  `.bootstrap.yaml` in the same dir holds the cluster property
  `auto_create_topics_enabled: false`. Redpanda reads cluster properties
  from that file at the first start, and logs "Unknown property" for them
  in `redpanda.yaml`. Then spawn:

  ```text
  redpanda --redpanda-cfg <dir>/redpanda.yaml --smp 1 --memory 1G \
    --overprovisioned --unsafe-bypass-fsync=true \
    --max-networking-io-control-blocks=1000
  ```

  The default of 10 000 network IOCBs per broker uses up `fs.aio-max-nr`
  (65 536) at about six brokers, and each test runs its own broker.
  Wait until `GET /v1/status/ready` on the admin port returns 200 and the
  Kafka port accepts a connection, with a 60 s limit. The admin port can
  answer 200 before the Kafka port listens. The broker is ready in about
  1 s. The readiness check is polled every 100 ms, since it has no event;
  the poll is raced against the process exit and the limit with
  `select` and `timeout`. `Drop` sends `SIGKILL` and removes the dir; the
  dropped exit future reaps the process.
- `restart()`: `SIGTERM`, wait up to 30 s (then `SIGKILL`), start again on
  the same data dir and ports. The shutdown is clean in about 1 s.
- Topics: `rpk topic create -X brokers=<addr> <name> -p <n>`.
- Read back: `rpk topic consume -X brokers=<addr> <name> --offset start
  --num <k> --format <f>`, parsed with `serde_json`: partition, offset,
  key, value, headers, timestamp. `rpk` is a reader independent of the
  crate's recovery. `--format json` replaces bytes that are not UTF-8 with
  U+FFFD, so `<f>` prints one JSON object per line with the bytes in hex
  (`%k{hex}`, `%v{hex}`, `%h{..}`). `rpk` prints a null key or value the
  same as an empty one, so the tests compare null as empty.
- Runtime: `compio` 0.19.2 (io_uring), one runtime per test through
  `#[compio::test]`. All test IO runs on that one thread. Concurrent work
  is futures joined or raced inside the scope that owns them. The harness `Redpanda` methods are `async`.
- Child processes: `redpanda` is a `compio::process` child with its output
  in a log file. The harness keeps its pid and its exit future
  (`child.wait()`); the ready and stop waits race that future, and a timeout
  sends a signal by pid. `rpk` is a
  `compio::process` child. Its stdin write and its stdout and stderr reads
  are three futures joined in `rpk()`, raced against the 60 s limit; on the
  limit, a kill closes the pipes and the joined futures still finish.
  `compio` waits for each child exit on its own blocking pool, since the
  pidfd wait needs a nightly feature.
- Driver: a `compio` `TcpStream` and a 1 ms tick loop. Each tick, the
  driver does IO until the tick deadline, then steps the machines. It maps
  `NetCmd` to connect and close, reads into `rx`, and writes `tx`. A
  connect, a read, and a write each stay in flight across ticks until they
  complete: the tick timer and the ops are polled together, and the timer
  never drops an op. Dropping an io_uring read loses the bytes the kernel
  already wrote to its buffer. A read takes at most the spare room of `rx`
  and copies into it on completion; a write sends a copy of `tx.pending()`
  and consumes the count written. A close drops the ops in flight, since
  the conn drops every byte of a closed connection.
- Proxy: futures between the driver and the broker that split frames by
  their 4-byte length. `Proxy::serve(work)` runs the accept loop and one
  future per connection (requests and replies raced) next to `work`, in a
  `FuturesUnordered` inside `serve`. When `work` ends, `serve` drops them
  and every proxy connection closes. The accept stays in flight between
  polls, so no client is lost. It counts request frames by API key, over its whole
  life. `drop_after(key, n)` closes both sides after it forwards request
  number `n` of that key, before the reply. `drop_after_range(key, first,
  last)` forwards requests `first` to `last`, then closes. It never forwards
  a reply to any of them, even one the broker sends before `last` arrives
  (scenario 8).
- Each test starts its own Redpanda: a broker shared across the test
  threads would need cross-thread sharing. The 8 scenarios run in about
  3 s. Scenario 3 joins the produce run with a future that restarts the
  broker after 30 acked sets.

### Scenarios

1. `round_trip`: topic A with 4 partitions and topic B with 1. Append 10 000
   records with random keys, values, headers, and timestamps, in about 50
   sealed sets. Read back with `rpk`. Check per partition: count, order, key,
   value, headers, CreateTime. This also checks the pinned versions against a
   real ApiVersions reply.
2. `lost_reply`: the proxy drops the connection after it forwards the 2nd
   Produce and before the reply. The job resends. Read back: each record
   once. The producer got one producer id. A fresh broker may answer the
   first InitProducerId with 15 or 16; the producer then asks again, so
   the proxy can see more than one InitProducerId.
3. `broker_restart`: restart Redpanda while sets are in flight. The conn
   backs off and reconnects, and the job finishes. Read back: each record
   once. If Redpanda answers `UNKNOWN_PRODUCER_ID` (59), the test fails, and
   the rule for 59 is a decision for the user. Result: Redpanda never sent
   59. It did send Metadata code 5 for about 40 ms after the restart; the
   Metadata rule under "conn" covers it.
4. `process_restart`: produce positions 1..N with the header. The proxy
   drops the connection after it forwards a Produce in the middle (the
   broker may or may not commit it). Drop every machine. A new process runs
   the recovery, gets `p[i]`, and produces positions 1..N again with the
   skip rule. Read back: each position once per partition. The header value
   is the position as a big-endian u64. The new process starts 1 s after
   the drop, as systemd's `RestartSec` does.
5. `recover_last`: 4 partitions, every record with the header. Partition 0
   ends with 50 batches of 10 records. Partition 1 ends with one batch just
   below `fetch_max_bytes` (16 KiB), and it is second in the Fetch, so the
   broker must return it whole as a later partition. Partition 2 is empty.
   Partition 3 has records only before the floor timestamp. The recovery
   finds the last record of partitions 0 and 1 with its position, and
   `Empty` for 2 and 3. A second run with a floor after every record gives
   `Empty` for all four, and records what ListOffsets answers (-1, see
   "Facts"). A third run with a fetch limit of 1024 bytes, below partition
   1's last batch, records what the broker does: Redpanda did not return
   the batch whole, and the recovery stopped with `Fatal::Fetch` (see
   "Facts"). The test accepts that or the right result.
6. `wrong_partitions`: the config says 4 partitions and the topic has 2.
   `Fatal::Metadata`, and the proxy saw no Produce.
7. `missing_topic`: the config names a topic that does not exist.
   `Fatal::Metadata`, no Produce.
8. `pipeline_drop`: `max_in_flight = 5`. The proxy drops the connection with
   5 sets in flight, after it forwards all 5 and before any reply. The job
   resends all 5. Read back: each record once. This checks that Redpanda's
   duplicate window holds 5 batches. If it holds fewer, `MAX_IN_FLIGHT`
   drops to Redpanda's number.

### Run

Inside `nix develop`: `cargo test -p kafka-sans-io`. The integration tests
fail with a clear message when `redpanda` is not on `PATH`. Proptest
failures print the minimal case and the seed; keep the full output (the
`AGENTS.md` rule on property test output applies).

## Steps

1. Done. `nix/redpanda.nix` is in the dev shell. Redpanda started by hand
   with the command and files under "Harness". `ready`, `rpk topic create`,
   produce, and consume of one record with headers worked. A raw ApiVersions
   v0 request gave the ranges under "Facts"; the pins are under "conn".
   `rpk cluster config list` shows `enable_idempotence true` and no setting
   for the duplicate window, so scenario 8 still checks it. The harness and
   its smoke test (`src/testing/redpanda/tests.rs`) are also done.
2. Crate skeleton, workspace member, `batch.rs`, test 1.
3. `frame.rs`, `conn.rs`, `accumulator.rs`, `producer.rs`, the model broker,
   test 2.
4. `recovery.rs`, tests 3 and 4.
5. Mutation audit.
6. Harness, driver, proxy, scenarios 1-8.
7. Update `plans/fleeb-kafka.md`: `Batch` and `Producer` use this crate, and
   the restart rule replaces `Offsets`.
8. Done. Every record carries the header, and fleeb-kafka's counters go to
   stdout. The recovery reads only the last record of each partition: one
   Fetch round in place of the backward window scan. `RecoveryConfig` lost
   `window`. Tests 3, 4, and 6, scenario 5 (`recover_window` became
   `recover_last`), and mutations 8 to 10 follow.

## Decisions to confirm

- `UNKNOWN_PRODUCER_ID` after a broker restart: answered by scenario 3.
  Redpanda never sent 59, so it stays fatal. Scenario 8 showed that all 5
  resent copies got code 0, so Redpanda's duplicate window is 5 and
  `MAX_IN_FLIGHT = 5` stays.
- The restart rule replaces the offset file. The gap: a Produce that commits
  more than `RestartSec` after the old process died can duplicate.
- Decided: every record in every topic carries the `fleeb-pos` header, and
  each topic keeps one fixed schema. fleeb-kafka's counters (acked
  position, lag, reconnects, resends) go only to stdout, one text line per
  second, and no stats record goes to Kafka. The recovery then reads only
  the last record of each partition, in one Fetch round. A last record
  without the header is `Fatal::Fetch`.
- Cutover: on the cutover day the last record can be an old producer's
  record without the header. Cut over before a day's first record (see
  "Facts").
