//! Model broker for the property tests, and one model connection.
//!
//! The broker keeps, per partition, a list of decoded records. It applies
//! the Kafka idempotence rule per (producer id, partition): a batch at the
//! next sequence commits; a copy of one of the last 5 batches answers
//! success with its first offset and commits nothing; any other sequence
//! answers `OUT_OF_ORDER_SEQUENCE_NUMBER` (45).
//!
//! It also keeps each partition's batches as stored bytes, base offset set,
//! for Fetch, and a log start that `trim` moves up. ListOffsets answers the
//! end offset (-1), the log start (-2), or for a timestamp the first offset
//! at or above the log start with a timestamp at or above it (-1 when none
//! has, as Kafka answers). Fetch answers at once for any `max_wait_ms`, by
//! the rule Redpanda 25.3 follows (see `fetch`, and "Facts" in
//! `plans/consumer.md`). A fetch offset below the log start or above the
//! end is `OFFSET_OUT_OF_RANGE`. Kafka behavior Redpanda does not show,
//! such as a batch cut short at the end of a part, is a `Fault`.
//!
//! It decodes requests with `kafka-protocol` (`broker` feature) and batches
//! with `RecordBatchDecoder`. A request the crate must never send panics.

#![allow(dead_code)]

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use kafka_protocol::{
    indexmap::IndexMap,
    messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
        InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse,
        MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
        ResponseHeader, TopicName,
        api_versions_response::ApiVersion,
        fetch_response::{FetchableTopicResponse, PartitionData},
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
    protocol::{Decodable, Encodable, StrBytes},
    records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
};
use proptest::sample::Index;

use super::record::{OwnedRecord, decode};
use crate::batch::HEADER_BYTES;

pub struct Broker {
    topics: Vec<(String, i32)>,
    /// Records per (topic, partition), in offset order.
    pub logs: BTreeMap<(String, i32), Vec<Committed>>,
    /// The same records per (topic, partition), as stored batches.
    pub batches: BTreeMap<(String, i32), Vec<StoredBatch>>,
    /// The first offset kept per (topic, partition). Records below it stay
    /// in `logs` and `batches`, but Fetch and ListOffsets act as if they
    /// were gone.
    pub log_start: BTreeMap<(String, i32), i64>,
    sequences: BTreeMap<(i64, String, i32), Sequence>,
    next_producer: i64,
    /// The next Metadata reply has one partition more in the first topic.
    pub wrong_partitions: bool,
}

impl Broker {
    pub fn new(topics: &[(&str, i32)]) -> Self {
        let topics: Vec<(String, i32)> = topics
            .iter()
            .map(|&(name, count)| (name.to_string(), count))
            .collect();
        let logs = topics
            .iter()
            .flat_map(|(name, count)| {
                (0..*count).map(|partition| ((name.clone(), partition), Vec::new()))
            })
            .collect::<BTreeMap<_, _>>();
        let batches = logs.keys().map(|key| (key.clone(), Vec::new())).collect();
        let log_start = logs.keys().map(|key| (key.clone(), 0)).collect();
        Broker {
            topics,
            logs,
            batches,
            log_start,
            sequences: BTreeMap::new(),
            next_producer: 1000,
            wrong_partitions: false,
        }
    }

    /// Handles one request frame body. A `fault` of this request's kind
    /// replaces part of the reply with an error. Returns the reply frame.
    pub fn handle(&mut self, frame: &[u8], fault: Option<Fault>) -> ReplyFrame {
        let kind = Broker::inspect(frame);
        let (header, mut body) = split(frame);
        let version = header.request_api_version;
        let api = ApiKey::try_from(header.request_api_key).expect("known API key");
        let correlation = header.correlation_id;
        let mut wrong = false;
        let mut faulted = false;
        let part_fault = match fault {
            Some(Fault::Part(index, code)) => Some((index, code)),
            _ => None,
        };
        let bytes = match api {
            ApiKey::ApiVersions => {
                ApiVersionsRequest::decode(&mut body, version).expect("ApiVersions decodes");
                let api_keys = RANGES
                    .iter()
                    .map(|&(api, min, max)| {
                        ApiVersion::default()
                            .with_api_key(api as i16)
                            .with_min_version(min)
                            .with_max_version(max)
                    })
                    .collect();
                frame_of(
                    api,
                    version,
                    correlation,
                    &ApiVersionsResponse::default().with_api_keys(api_keys),
                )
            }
            ApiKey::Metadata => {
                let request =
                    MetadataRequest::decode(&mut body, version).expect("Metadata decodes");
                assert!(
                    !request.allow_auto_topic_creation,
                    "Metadata may create topics"
                );
                wrong = std::mem::take(&mut self.wrong_partitions);
                let leader_fault = match fault {
                    Some(Fault::Leader(index, code)) => {
                        let count = self.topics.iter().map(|(_, count)| *count as usize).sum();
                        Some((index.index(count), code))
                    }
                    _ => None,
                };
                faulted = leader_fault.is_some();
                let mut at = 0;
                let topics = request
                    .topics
                    .expect("Metadata names its topics")
                    .into_iter()
                    .map(|topic| {
                        let name = topic.name.expect("topic by name").to_string();
                        let index = self.topics.iter().position(|(known, _)| *known == name);
                        let mut reply = MetadataResponseTopic::default()
                            .with_name(Some(TopicName(StrBytes::from_string(name))));
                        match index {
                            None => reply = reply.with_error_code(3),
                            Some(index) => {
                                let count = self.topics[index].1 + i32::from(wrong && index == 0);
                                reply = reply.with_partitions(
                                    (0..count)
                                        .map(|partition| {
                                            let reply = MetadataResponsePartition::default()
                                                .with_partition_index(partition)
                                                .with_replica_nodes(vec![BrokerId(NODE)])
                                                .with_isr_nodes(vec![BrokerId(NODE)]);
                                            let reply = match leader_fault {
                                                Some((faulted, code)) if faulted == at => reply
                                                    .with_error_code(code)
                                                    .with_leader_id(BrokerId(-1)),
                                                _ => reply.with_leader_id(BrokerId(NODE)),
                                            };
                                            at += 1;
                                            reply
                                        })
                                        .collect(),
                                );
                            }
                        }
                        reply
                    })
                    .collect();
                let reply = MetadataResponse::default()
                    .with_brokers(vec![
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId(NODE))
                            .with_host(StrBytes::from_static_str("model"))
                            .with_port(9092),
                    ])
                    .with_controller_id(BrokerId(NODE))
                    .with_topics(topics);
                frame_of(api, version, correlation, &reply)
            }
            ApiKey::InitProducerId => {
                let request = InitProducerIdRequest::decode(&mut body, version)
                    .expect("InitProducerId decodes");
                assert!(request.transactional_id.is_none(), "transactional producer");
                let reply = match fault {
                    Some(Fault::Init(code)) => {
                        faulted = true;
                        InitProducerIdResponse::default()
                            .with_error_code(code)
                            .with_producer_id((-1).into())
                            .with_producer_epoch(-1)
                    }
                    _ => {
                        let id = self.next_producer;
                        self.next_producer += 1;
                        InitProducerIdResponse::default()
                            .with_producer_id(id.into())
                            .with_producer_epoch(0)
                    }
                };
                frame_of(api, version, correlation, &reply)
            }
            ApiKey::Produce => {
                let request = ProduceRequest::decode(&mut body, version).expect("Produce decodes");
                assert_eq!(request.acks, -1, "acks");
                assert!(request.transactional_id.is_none(), "transactional Produce");
                let Kind::Produce(batches) = &kind else {
                    unreachable!()
                };
                let fault = part_fault.map(|(index, code)| (index.index(batches.len()), code));
                faulted = fault.is_some();
                let mut responses: Vec<TopicProduceResponse> = Vec::new();
                for (at, batch) in batches.iter().enumerate() {
                    let (code, base_offset) = match fault {
                        Some((faulted, code)) if faulted == at => (code, -1),
                        _ => self.append(batch),
                    };
                    let partition = PartitionProduceResponse::default()
                        .with_index(batch.partition)
                        .with_error_code(code)
                        .with_base_offset(base_offset)
                        .with_log_append_time_ms(-1);
                    match responses
                        .iter_mut()
                        .find(|topic| topic.name.as_str() == batch.topic)
                    {
                        Some(topic) => topic.partition_responses.push(partition),
                        None => responses.push(
                            TopicProduceResponse::default()
                                .with_name(TopicName(StrBytes::from_string(batch.topic.clone())))
                                .with_partition_responses(vec![partition]),
                        ),
                    }
                }
                frame_of(
                    api,
                    version,
                    correlation,
                    &ProduceResponse::default().with_responses(responses),
                )
            }
            ApiKey::ListOffsets => {
                let request =
                    ListOffsetsRequest::decode(&mut body, version).expect("ListOffsets decodes");
                assert_eq!(*request.replica_id, -1, "ListOffsets as a consumer");
                let count = request
                    .topics
                    .iter()
                    .map(|topic| topic.partitions.len())
                    .sum();
                let fault = part_fault.map(|(index, code)| (index.index(count), code));
                faulted = fault.is_some();
                let mut at = 0;
                let topics = request
                    .topics
                    .into_iter()
                    .map(|topic| {
                        let name = topic.name.to_string();
                        let partitions = topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                let index = partition.partition_index;
                                let reply = ListOffsetsPartitionResponse::default()
                                    .with_partition_index(index)
                                    .with_timestamp(-1);
                                let reply = match fault {
                                    Some((faulted, code)) if faulted == at => {
                                        reply.with_error_code(code).with_offset(-1)
                                    }
                                    _ => reply.with_offset(self.list_offset(
                                        &name,
                                        index,
                                        partition.timestamp,
                                    )),
                                };
                                at += 1;
                                reply
                            })
                            .collect();
                        ListOffsetsTopicResponse::default()
                            .with_name(topic.name)
                            .with_partitions(partitions)
                    })
                    .collect();
                frame_of(
                    api,
                    version,
                    correlation,
                    &ListOffsetsResponse::default().with_topics(topics),
                )
            }
            ApiKey::Fetch => {
                let request = FetchRequest::decode(&mut body, version).expect("Fetch decodes");
                assert_eq!(request.isolation_level, 0, "Fetch READ_UNCOMMITTED");
                let count = request
                    .topics
                    .iter()
                    .map(|topic| topic.partitions.len())
                    .sum();
                // Read before `fault` names the part fault below.
                let cut = match fault {
                    Some(Fault::CutShort(index)) => Some(index.index(count)),
                    _ => None,
                };
                let fault = part_fault.map(|(index, code)| (index.index(count), code));
                faulted = fault.is_some();
                let mut at = 0;
                // Record bytes of the reply so far, for the request budget.
                let mut used = 0;
                let mut cut_done = false;
                let max_bytes = usize::try_from(request.max_bytes).expect("a positive max_bytes");
                let responses = request
                    .topics
                    .into_iter()
                    .map(|topic| {
                        let name = topic.topic.to_string();
                        let partitions = topic
                            .partitions
                            .into_iter()
                            .map(|partition| {
                                let index = partition.partition;
                                let key = (name.clone(), index);
                                let end = self.logs[&key].len() as i64;
                                let reply = PartitionData::default()
                                    .with_partition_index(index)
                                    .with_high_watermark(end)
                                    .with_last_stable_offset(end)
                                    .with_log_start_offset(self.log_start[&key]);
                                let reply = match fault {
                                    Some((faulted, code)) if faulted == at => {
                                        reply.with_error_code(code)
                                    }
                                    _ => match self.fetch(
                                        &key,
                                        partition.fetch_offset,
                                        partition.partition_max_bytes,
                                    ) {
                                        // Past the request budget, a part
                                        // gets nothing, unless the reply
                                        // has no record yet.
                                        Ok((records, _))
                                            if used > 0 && used + records.len() > max_bytes =>
                                        {
                                            reply.with_records(Some(Bytes::new()))
                                        }
                                        Ok((mut records, last)) => {
                                            if cut == Some(at) && last < records.len() {
                                                records.truncate(
                                                    records.len() - last
                                                        + (last / 2).max(HEADER_BYTES),
                                                );
                                                cut_done = true;
                                            }
                                            used += records.len();
                                            reply.with_records(Some(records))
                                        }
                                        Err(code) => reply.with_error_code(code),
                                    },
                                };
                                at += 1;
                                reply
                            })
                            .collect();
                        FetchableTopicResponse::default()
                            .with_topic(topic.topic)
                            .with_partitions(partitions)
                    })
                    .collect();
                faulted |= cut_done;
                frame_of(
                    api,
                    version,
                    correlation,
                    &FetchResponse::default().with_responses(responses),
                )
            }
            other => panic!("the producer sent {other:?}"),
        };
        ReplyFrame {
            bytes,
            kind,
            wrong,
            faulted,
        }
    }

    /// Reads a request frame body (no length field) without handling it.
    pub fn inspect(frame: &[u8]) -> Kind {
        let (header, mut body) = split(frame);
        let version = header.request_api_version;
        match ApiKey::try_from(header.request_api_key).expect("known API key") {
            ApiKey::ApiVersions => Kind::ApiVersions,
            ApiKey::Metadata => Kind::Metadata,
            ApiKey::InitProducerId => Kind::InitProducerId,
            ApiKey::ListOffsets => Kind::ListOffsets,
            ApiKey::Fetch => Kind::Fetch,
            ApiKey::Produce => {
                let request = ProduceRequest::decode(&mut body, version).expect("Produce decodes");
                assert!(body.is_empty(), "bytes after the Produce body");
                let mut batches = Vec::new();
                for topic in request.topic_data {
                    for partition in topic.partition_data {
                        let bytes = partition.records.expect("Produce with records").to_vec();
                        let (info, _) = decode(&bytes).expect("one batch per partition");
                        batches.push(SentBatch {
                            topic: topic.name.to_string(),
                            partition: partition.index,
                            producer_id: info.producer_id,
                            base_sequence: info.base_sequence,
                            bytes,
                        });
                    }
                }
                Kind::Produce(batches)
            }
            other => panic!("the producer sent {other:?}"),
        }
    }

    /// Appends one batch of `records` to a partition, written by
    /// `RecordBatchEncoder` with no producer id. For tests that fill the log
    /// directly.
    pub fn put(&mut self, topic: &str, partition: i32, records: &[OwnedRecord]) {
        assert!(!records.is_empty(), "a batch has records");
        let key = (topic.to_string(), partition);
        let first = self.logs[&key].len() as i64;
        let encoded: Vec<Record> = records
            .iter()
            .enumerate()
            .map(|(delta, plain)| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: first + delta as i64,
                // The encoder starts a new batch where offset minus sequence
                // changes.
                sequence: delta as i32,
                timestamp: plain.timestamp_ms,
                key: plain.key.clone().map(Bytes::from),
                value: plain.value.clone().map(Bytes::from),
                headers: plain
                    .headers
                    .iter()
                    .map(|(key, value)| {
                        (
                            StrBytes::from_string(key.clone()),
                            value.clone().map(Bytes::from),
                        )
                    })
                    .collect::<IndexMap<_, _>>(),
            })
            .collect();
        let mut bytes = Vec::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut bytes, &encoded, &options).expect("batch encodes");
        let log = self.logs.get_mut(&key).expect("known partition");
        for (delta, plain) in records.iter().enumerate() {
            log.push(Committed {
                offset: first + delta as i64,
                producer_id: -1,
                sequence: -1,
                record: plain.clone(),
            });
        }
        self.batches
            .get_mut(&key)
            .expect("known partition")
            .push(StoredBatch {
                base: first,
                last: first + records.len() as i64 - 1,
                bytes,
            });
    }

    /// Moves the log start of a partition up to `offset`, at most the end,
    /// as `rpk topic trim-prefix` does. A lower `offset` changes nothing.
    pub fn trim(&mut self, topic: &str, partition: i32, offset: i64) {
        let key = (topic.to_string(), partition);
        let start = self.log_start.get_mut(&key).expect("known partition");
        *start = (*start).max(offset.min(self.logs[&key].len() as i64));
    }

    /// The idempotence rule for one batch. Returns the error code and the
    /// first offset.
    fn append(&mut self, batch: &SentBatch) -> (i16, i64) {
        assert!(
            (1000..self.next_producer).contains(&batch.producer_id),
            "batch with a producer id the broker never gave"
        );
        let key = (batch.topic.clone(), batch.partition);
        assert!(
            self.logs.contains_key(&key),
            "Produce to an unknown partition {key:?}"
        );
        let (_, records) = decode(&batch.bytes).expect("batch decodes");
        let count = records.len() as i32;
        let sequence = self
            .sequences
            .entry((batch.producer_id, batch.topic.clone(), batch.partition))
            .or_default();
        if let Some(&(_, _, first)) = sequence
            .window
            .iter()
            .find(|&&(base, seen, _)| base == batch.base_sequence && seen == count)
        {
            return (0, first);
        }
        if batch.base_sequence != sequence.next {
            return (OUT_OF_ORDER_SEQUENCE_NUMBER, -1);
        }
        let log = self.logs.get_mut(&key).expect("checked above");
        let first = log.len() as i64;
        // The broker sets the base offset; the CRC does not cover it.
        let mut stored = batch.bytes.clone();
        stored[..8].copy_from_slice(&first.to_be_bytes());
        self.batches
            .get_mut(&key)
            .expect("known partition")
            .push(StoredBatch {
                base: first,
                last: first + i64::from(count) - 1,
                bytes: stored,
            });
        for (delta, decoded) in records.into_iter().enumerate() {
            log.push(Committed {
                offset: first + delta as i64,
                producer_id: batch.producer_id,
                sequence: decoded.sequence,
                record: decoded.record,
            });
        }
        sequence
            .window
            .push_back((batch.base_sequence, count, first));
        if sequence.window.len() > WINDOW {
            sequence.window.pop_front();
        }
        sequence.next = sequence.next.wrapping_add(count);
        (0, first)
    }

    /// ListOffsets for one partition: the end, the log start, or the first
    /// offset at or above the log start with a timestamp at or above
    /// `timestamp` (-1 when none has).
    fn list_offset(&self, topic: &str, partition: i32, timestamp: i64) -> i64 {
        let key = (topic.to_string(), partition);
        let log = &self.logs[&key];
        let start = self.log_start[&key];
        match timestamp {
            LATEST => log.len() as i64,
            EARLIEST => start,
            timestamp => log[start as usize..]
                .iter()
                .find(|committed| committed.record.timestamp_ms >= timestamp)
                .map_or(-1, |committed| committed.offset),
        }
    }

    /// Fetch for one partition, as Redpanda 25.3 answers: whole batches
    /// from the one holding `offset` while the bytes so far are below
    /// `limit`, so the batch that crosses the limit comes whole and nothing
    /// follows it. The caller applies the request budget: a part that would
    /// push the reply past `max_bytes` gets nothing, unless the reply has no
    /// record yet. Returns the records and the bytes of the last batch.
    fn fetch(&self, key: &(String, i32), offset: i64, limit: i32) -> Result<(Bytes, usize), i16> {
        let batches = &self.batches[key];
        let end = self.logs[key].len() as i64;
        if !(self.log_start[key]..=end).contains(&offset) {
            return Err(OFFSET_OUT_OF_RANGE);
        }
        let limit = usize::try_from(limit).expect("a positive byte limit");
        let first = batches.partition_point(|batch| batch.last < offset);
        let mut bytes = Vec::new();
        let mut last = 0;
        for batch in &batches[first..] {
            if bytes.len() >= limit {
                break;
            }
            bytes.extend_from_slice(&batch.bytes);
            last = batch.bytes.len();
        }
        Ok((Bytes::from(bytes), last))
    }
}

/// An error the broker answers in place of its normal reply. Each variant
/// applies to one request kind; on another kind the broker answers as
/// normal.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// One partition of a Produce, ListOffsets, or Fetch, picked by index
    /// over its partitions, gets this code.
    Part(Index, i16),
    /// InitProducerId answers this code and no producer id.
    Init(i16),
    /// One partition of a Metadata reply, picked by index over all
    /// partitions, gets this code and no leader.
    Leader(Index, i16),
    /// One partition of a Fetch reply, picked by index over its partitions,
    /// ends with its last batch cut short to half its bytes, at least the
    /// batch header, as Apache Kafka sends at the byte limit. Only a part
    /// with two batches or more: the first stays whole.
    CutShort(Index),
}

/// A reply frame, length field included, and what it answers.
#[derive(Clone, Debug)]
pub struct ReplyFrame {
    pub bytes: Vec<u8>,
    pub kind: Kind,
    /// A Metadata reply with a wrong partition count.
    pub wrong: bool,
    /// The reply carries the injected error.
    pub faulted: bool,
}

/// What a request is, read without handling it.
#[derive(Clone, Debug)]
pub enum Kind {
    ApiVersions,
    Metadata,
    InitProducerId,
    Produce(Vec<SentBatch>),
    ListOffsets,
    Fetch,
}

impl Kind {
    /// A request a job sends, after the handshake.
    pub fn is_job(&self) -> bool {
        !matches!(self, Kind::ApiVersions | Kind::Metadata)
    }
}

/// One batch of a Produce, as sent.
#[derive(Clone, Debug)]
pub struct SentBatch {
    pub topic: String,
    pub partition: i32,
    pub producer_id: i64,
    pub base_sequence: i32,
    pub bytes: Vec<u8>,
}

/// A record in a partition log.
#[derive(Clone, Debug)]
pub struct Committed {
    pub offset: i64,
    pub producer_id: i64,
    pub sequence: i32,
    pub record: OwnedRecord,
}

/// A batch as stored in a partition log, base offset set.
#[derive(Clone, Debug)]
pub struct StoredBatch {
    pub base: i64,
    /// Offset of its last record.
    pub last: i64,
    pub bytes: Vec<u8>,
}

/// One TCP connection between the crate and the broker.
#[derive(Default)]
pub struct Connection {
    /// Client bytes not yet a whole frame.
    inbound: Vec<u8>,
    /// Request frame bodies the broker has not handled, oldest first.
    pub unhandled: VecDeque<Vec<u8>>,
    /// Reply frames not yet fully delivered, and the bytes of the front one
    /// already delivered.
    outbound: VecDeque<ReplyFrame>,
    front_sent: usize,
    pub requests: usize,
    pub versions_delivered: bool,
    pub metadata_delivered: bool,
    pub wrong_metadata_delivered: bool,
    pub jobs_received: usize,
    pub jobs_delivered: usize,
    /// Fetch replies delivered whole, without an injected error.
    pub clean_fetches_delivered: usize,
}

impl Connection {
    /// Takes client bytes. Returns the request frames they complete.
    pub fn receive(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.inbound.extend_from_slice(bytes);
        let mut frames = Vec::new();
        while let Some(length) = self.inbound.first_chunk::<4>() {
            let length = i32::from_be_bytes(*length) as usize;
            if self.inbound.len() < 4 + length {
                break;
            }
            let frame: Vec<u8> = self.inbound.drain(..4 + length).skip(4).collect();
            self.requests += 1;
            if Broker::inspect(&frame).is_job() {
                self.jobs_received += 1;
            }
            self.unhandled.push_back(frame.clone());
            frames.push(frame);
        }
        frames
    }

    /// Handles the oldest unhandled request. False when there is none.
    pub fn handle_one(&mut self, broker: &mut Broker, fault: Option<Fault>) -> bool {
        let Some(frame) = self.unhandled.pop_front() else {
            return false;
        };
        let reply = broker.handle(&frame, fault);
        self.outbound.push_back(reply);
        true
    }

    /// Copies up to `chunk` reply bytes into `spare`. Returns the count.
    pub fn deliver(&mut self, chunk: usize, spare: &mut [u8]) -> usize {
        let mut copied = 0;
        while copied < chunk.min(spare.len()) {
            let Some(front) = self.outbound.front() else {
                break;
            };
            let take = (front.bytes.len() - self.front_sent).min(chunk.min(spare.len()) - copied);
            spare[copied..copied + take]
                .copy_from_slice(&front.bytes[self.front_sent..self.front_sent + take]);
            copied += take;
            self.front_sent += take;
            if self.front_sent == front.bytes.len() {
                let done = self.outbound.pop_front().expect("front exists");
                self.front_sent = 0;
                match done.kind {
                    Kind::ApiVersions => self.versions_delivered = true,
                    Kind::Metadata => {
                        self.metadata_delivered = true;
                        self.wrong_metadata_delivered |= done.wrong;
                    }
                    Kind::Fetch => {
                        self.jobs_delivered += 1;
                        self.clean_fetches_delivered += usize::from(!done.faulted);
                    }
                    Kind::InitProducerId | Kind::Produce(_) | Kind::ListOffsets => {
                        self.jobs_delivered += 1
                    }
                }
            }
        }
        copied
    }

    /// Reply bytes the broker wrote and the client did not get yet.
    pub fn undelivered(&self) -> usize {
        self.outbound
            .iter()
            .map(|reply| reply.bytes.len())
            .sum::<usize>()
            - self.front_sent
    }
}

/// Batches per (producer id, partition) the broker keeps for duplicate checks.
pub const WINDOW: usize = 5;

pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;

pub const OFFSET_OUT_OF_RANGE: i16 = 1;

#[derive(Default)]
struct Sequence {
    next: i32,
    /// (base sequence, count, first offset) of the last `WINDOW` batches.
    window: VecDeque<(i32, i32, i64)>,
}

/// The request header and the body after it.
fn split(frame: &[u8]) -> (RequestHeader, &[u8]) {
    let key = i16::from_be_bytes(frame[0..2].try_into().expect("api key"));
    let version = i16::from_be_bytes(frame[2..4].try_into().expect("api version"));
    let api = ApiKey::try_from(key).expect("known API key");
    let &(_, min, max) = RANGES
        .iter()
        .find(|(known, _, _)| *known == api)
        .unwrap_or_else(|| panic!("the broker does not serve {api:?}"));
    assert!(
        (min..=max).contains(&version),
        "{api:?} v{version} outside the broker's range"
    );
    let mut body = frame;
    let header = RequestHeader::decode(&mut body, api.request_header_version(version))
        .expect("request header decodes");
    (header, body)
}

fn frame_of<M: Encodable>(api: ApiKey, version: i16, correlation: i32, body: &M) -> Vec<u8> {
    let mut bytes = vec![0; 4];
    ResponseHeader::default()
        .with_correlation_id(correlation)
        .encode(&mut bytes, api.response_header_version(version))
        .expect("response header encodes");
    body.encode(&mut bytes, version)
        .expect("response body encodes");
    let length = (bytes.len() - 4) as i32;
    bytes[..4].copy_from_slice(&length.to_be_bytes());
    bytes
}

const NODE: i32 = 1;

/// Redpanda 25.3 version ranges, from the plan's facts.
const RANGES: [(ApiKey, i16, i16); 6] = [
    (ApiKey::ApiVersions, 0, 4),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::Produce, 0, 7),
    (ApiKey::InitProducerId, 0, 3),
    (ApiKey::ListOffsets, 0, 5),
    (ApiKey::Fetch, 4, 13),
];

/// ListOffsets timestamps with a meaning of their own.
const LATEST: i64 = -1;

const EARLIEST: i64 = -2;
