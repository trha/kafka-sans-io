//! The hand codecs of `wire` against `kafka-protocol`.
//!
//! Replies: random replies, encoded by `kafka-protocol` after their
//! response header, must read back to the same facts. Unknown tagged fields
//! are drawn where the format has them. A cut-short reply, or one byte too
//! many, is `Fatal::Protocol`, and random bytes never panic. Requests: each
//! hand-written request, header included, must decode with `kafka-protocol`
//! to the same facts with no byte left, and its count pass must equal the
//! bytes written.

use std::collections::BTreeMap;

use bytes::Bytes;
use kafka_protocol::{
    messages::{
        ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse,
        InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse,
        MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, RequestHeader,
        ResponseHeader, TopicName,
        api_versions_response::ApiVersion,
        fetch_response::{AbortedTransaction, FetchableTopicResponse, PartitionData},
        list_offsets_response::{ListOffsetsPartitionResponse, ListOffsetsTopicResponse},
        metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        },
        produce_response::{PartitionProduceResponse, TopicProduceResponse},
    },
    protocol::{Decodable, Encodable, StrBytes},
};
use kafka_sans_io::{
    error::Fatal,
    limits::MAX_PARTS,
    wire::{
        self, Api, FetchLimits, FetchTarget, MetadataItem, ProduceTarget, VersionRange, Writer,
    },
};
use proptest::{collection::vec, option, prelude::*, sample::Index};

fn topic_name() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9._-]{0,24}"
}

fn name(topic: &str) -> TopicName {
    TopicName(StrBytes::from_string(topic.to_string()))
}

fn text(value: &str) -> StrBytes {
    StrBytes::from_string(value.to_string())
}

fn api_key(api: Api) -> ApiKey {
    ApiKey::try_from(api.key()).expect("a known API key")
}

/// Unknown tagged fields, with tags no message here knows.
fn tagged() -> impl Strategy<Value = BTreeMap<i32, Bytes>> {
    let value = vec(any::<u8>(), 0..6).prop_map(Bytes::from);
    proptest::collection::btree_map(10..300, value, 0..3)
}

/// A reply frame without its length field: the response header of `api`,
/// with `tags` when the header has a tagged field section, then `body`.
fn reply_frame<M: Encodable>(
    api: Api,
    correlation: i32,
    tags: &BTreeMap<i32, Bytes>,
    body: &M,
) -> Vec<u8> {
    let header_version = api_key(api).response_header_version(api.version());
    let mut header = ResponseHeader::default().with_correlation_id(correlation);
    if header_version >= 1 {
        header = header.with_unknown_tagged_fields(tags.clone());
    }
    let mut bytes = Vec::new();
    header
        .encode(&mut bytes, header_version)
        .expect("response header encodes");
    body.encode(&mut bytes, api.version())
        .expect("reply body encodes");
    bytes
}

/// Every cut-short frame, and one byte too many, give `Fatal::Protocol`.
fn bounds<T: std::fmt::Debug>(
    frame: &[u8],
    read: impl Fn(&[u8]) -> Result<T, Fatal>,
) -> Result<(), TestCaseError> {
    for cut in 0..frame.len() {
        prop_assert_eq!(
            read(&frame[..cut]).err(),
            Some(Fatal::Protocol),
            "cut at {}",
            cut
        );
    }
    let mut longer = frame.to_vec();
    longer.push(0);
    prop_assert_eq!(read(&longer).err(), Some(Fatal::Protocol), "trailing byte");
    Ok(())
}

// --------------------------------------------------------------- ApiVersions

#[derive(Clone, Debug)]
struct VersionsReply {
    correlation: i32,
    ranges: Vec<(i16, i16, i16, BTreeMap<i32, Bytes>)>,
    throttle: i32,
    finalized_features_epoch: i64,
    zk_migration_ready: bool,
    tags: BTreeMap<i32, Bytes>,
}

fn versions_reply() -> impl Strategy<Value = VersionsReply> {
    let range = (any::<i16>(), any::<i16>(), any::<i16>(), tagged());
    (
        any::<i32>(),
        vec(range, 0..8),
        any::<i32>(),
        any::<i64>(),
        any::<bool>(),
        tagged(),
    )
        .prop_map(
            |(
                correlation,
                ranges,
                throttle,
                finalized_features_epoch,
                zk_migration_ready,
                tags,
            )| {
                VersionsReply {
                    correlation,
                    ranges,
                    throttle,
                    finalized_features_epoch,
                    zk_migration_ready,
                    tags,
                }
            },
        )
}

fn encode_versions(reply: &VersionsReply) -> Vec<u8> {
    let ranges = reply
        .ranges
        .iter()
        .map(|(key, min, max, tags)| {
            ApiVersion::default()
                .with_api_key(*key)
                .with_min_version(*min)
                .with_max_version(*max)
                .with_unknown_tagged_fields(tags.clone())
        })
        .collect();
    // Known tagged fields the reader skips as well.
    let body = ApiVersionsResponse::default()
        .with_api_keys(ranges)
        .with_throttle_time_ms(reply.throttle)
        .with_finalized_features_epoch(reply.finalized_features_epoch)
        .with_zk_migration_ready(reply.zk_migration_ready)
        .with_unknown_tagged_fields(reply.tags.clone());
    reply_frame(Api::ApiVersions, reply.correlation, &BTreeMap::new(), &body)
}

#[derive(Debug)]
struct VersionsRead {
    correlation: i32,
    code: i16,
    ranges: Vec<VersionRange>,
}

fn read_versions(frame: &[u8]) -> Result<VersionsRead, Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::ApiVersions)?;
    let mut ranges = Vec::new();
    let code = wire::api_versions_response(body, |range| ranges.push(*range))?;
    Ok(VersionsRead {
        correlation,
        code,
        ranges,
    })
}

// ------------------------------------------------------------------ Metadata

#[derive(Clone, Debug)]
struct MetadataPart {
    code: i16,
    index: i32,
    leader: i32,
    leader_epoch: i32,
    /// Replica nodes, isr nodes, offline replicas.
    nodes: [Vec<i32>; 3],
}

#[derive(Clone, Debug)]
struct MetadataTopic {
    code: i16,
    name: Option<String>,
    internal: bool,
    parts: Vec<MetadataPart>,
    authorized: i32,
}

#[derive(Clone, Debug)]
struct MetadataReply {
    correlation: i32,
    throttle: i32,
    /// Node id, host, port, rack.
    brokers: Vec<(i32, String, i32, Option<String>)>,
    cluster_id: Option<String>,
    controller: i32,
    topics: Vec<MetadataTopic>,
    authorized: i32,
}

fn metadata_reply() -> impl Strategy<Value = MetadataReply> {
    let nodes = || vec(any::<i32>(), 0..3);
    let part = (
        any::<i16>(),
        any::<i32>(),
        any::<i32>(),
        any::<i32>(),
        [nodes(), nodes(), nodes()],
    )
        .prop_map(|(code, index, leader, leader_epoch, nodes)| MetadataPart {
            code,
            index,
            leader,
            leader_epoch,
            nodes,
        });
    let topic = (
        any::<i16>(),
        option::of(topic_name()),
        any::<bool>(),
        vec(part, 0..4),
        any::<i32>(),
    )
        .prop_map(|(code, name, internal, parts, authorized)| MetadataTopic {
            code,
            name,
            internal,
            parts,
            authorized,
        });
    let broker = (
        any::<i32>(),
        "[a-z0-9.]{0,12}",
        any::<i32>(),
        option::of("[a-z0-9]{0,6}"),
    );
    (
        (any::<i32>(), any::<i32>(), vec(broker, 0..3)),
        option::of("[a-zA-Z0-9]{0,10}"),
        any::<i32>(),
        vec(topic, 0..4),
        any::<i32>(),
    )
        .prop_map(
            |((correlation, throttle, brokers), cluster_id, controller, topics, authorized)| {
                MetadataReply {
                    correlation,
                    throttle,
                    brokers,
                    cluster_id,
                    controller,
                    topics,
                    authorized,
                }
            },
        )
}

fn encode_metadata(reply: &MetadataReply) -> Vec<u8> {
    let brokers = reply
        .brokers
        .iter()
        .map(|(node, host, port, rack)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(*node))
                .with_host(text(host))
                .with_port(*port)
                .with_rack(rack.as_deref().map(text))
        })
        .collect();
    let ids = |nodes: &[i32]| nodes.iter().map(|&node| BrokerId(node)).collect();
    let topics = reply
        .topics
        .iter()
        .map(|topic| {
            let parts = topic
                .parts
                .iter()
                .map(|part| {
                    MetadataResponsePartition::default()
                        .with_error_code(part.code)
                        .with_partition_index(part.index)
                        .with_leader_id(BrokerId(part.leader))
                        .with_leader_epoch(part.leader_epoch)
                        .with_replica_nodes(ids(&part.nodes[0]))
                        .with_isr_nodes(ids(&part.nodes[1]))
                        .with_offline_replicas(ids(&part.nodes[2]))
                })
                .collect();
            MetadataResponseTopic::default()
                .with_error_code(topic.code)
                .with_name(topic.name.as_deref().map(name))
                .with_is_internal(topic.internal)
                .with_partitions(parts)
                .with_topic_authorized_operations(topic.authorized)
        })
        .collect();
    let body = MetadataResponse::default()
        .with_throttle_time_ms(reply.throttle)
        .with_brokers(brokers)
        .with_cluster_id(reply.cluster_id.as_deref().map(text))
        .with_controller_id(BrokerId(reply.controller))
        .with_topics(topics)
        .with_cluster_authorized_operations(reply.authorized);
    reply_frame(Api::Metadata, reply.correlation, &BTreeMap::new(), &body)
}

/// What the hand reader saw, in its own shape.
#[derive(Clone, Debug, PartialEq, Eq)]
enum MetadataRow {
    Broker(i32),
    Topic(i16, Option<Vec<u8>>, usize),
    /// Error code, index, leader, leader epoch.
    Partition(i16, i32, i32, i32),
}

fn read_metadata(frame: &[u8]) -> Result<(i32, Vec<MetadataRow>), Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::Metadata)?;
    let mut rows = Vec::new();
    wire::metadata_response(body, |item| {
        rows.push(match item {
            MetadataItem::Broker { node_id } => MetadataRow::Broker(node_id),
            MetadataItem::Topic {
                error_code,
                name,
                partitions,
            } => MetadataRow::Topic(error_code, name.map(<[u8]>::to_vec), partitions),
            MetadataItem::Partition(partition) => MetadataRow::Partition(
                partition.error_code.get(),
                partition.partition_index.get(),
                partition.leader_id.get(),
                partition.leader_epoch.get(),
            ),
        });
        Ok(())
    })?;
    Ok((correlation, rows))
}

// ------------------------------------------------------------ InitProducerId

#[derive(Clone, Debug)]
struct InitReply {
    correlation: i32,
    header_tags: BTreeMap<i32, Bytes>,
    throttle: i32,
    code: i16,
    id: i64,
    epoch: i16,
    tags: BTreeMap<i32, Bytes>,
}

fn init_reply() -> impl Strategy<Value = InitReply> {
    (
        any::<i32>(),
        tagged(),
        any::<i32>(),
        any::<i16>(),
        any::<i64>(),
        any::<i16>(),
        tagged(),
    )
        .prop_map(
            |(correlation, header_tags, throttle, code, id, epoch, tags)| InitReply {
                correlation,
                header_tags,
                throttle,
                code,
                id,
                epoch,
                tags,
            },
        )
}

fn encode_init(reply: &InitReply) -> Vec<u8> {
    let body = InitProducerIdResponse::default()
        .with_throttle_time_ms(reply.throttle)
        .with_error_code(reply.code)
        .with_producer_id(reply.id.into())
        .with_producer_epoch(reply.epoch)
        .with_unknown_tagged_fields(reply.tags.clone());
    reply_frame(
        Api::InitProducerId,
        reply.correlation,
        &reply.header_tags,
        &body,
    )
}

fn read_init(frame: &[u8]) -> Result<(i32, wire::InitProducerIdReply), Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::InitProducerId)?;
    Ok((correlation, wire::init_producer_id_response(body)?))
}

// --------------------------------------------------------------- ListOffsets

#[derive(Clone, Debug)]
struct ListPart {
    index: i32,
    code: i16,
    timestamp: i64,
    offset: i64,
    leader_epoch: i32,
}

#[derive(Clone, Debug)]
struct ListReply {
    correlation: i32,
    throttle: i32,
    topics: Vec<(String, Vec<ListPart>)>,
}

fn list_reply() -> impl Strategy<Value = ListReply> {
    let part = (
        any::<i32>(),
        any::<i16>(),
        any::<i64>(),
        any::<i64>(),
        any::<i32>(),
    )
        .prop_map(|(index, code, timestamp, offset, leader_epoch)| ListPart {
            index,
            code,
            timestamp,
            offset,
            leader_epoch,
        });
    let topic = (topic_name(), vec(part, 0..5));
    (any::<i32>(), any::<i32>(), vec(topic, 0..5)).prop_map(|(correlation, throttle, topics)| {
        ListReply {
            correlation,
            throttle,
            topics,
        }
    })
}

fn encode_list(reply: &ListReply) -> Vec<u8> {
    let topics = reply
        .topics
        .iter()
        .map(|(topic, parts)| {
            let parts = parts
                .iter()
                .map(|part| {
                    ListOffsetsPartitionResponse::default()
                        .with_partition_index(part.index)
                        .with_error_code(part.code)
                        .with_timestamp(part.timestamp)
                        .with_offset(part.offset)
                        .with_leader_epoch(part.leader_epoch)
                })
                .collect();
            ListOffsetsTopicResponse::default()
                .with_name(name(topic))
                .with_partitions(parts)
        })
        .collect();
    let body = ListOffsetsResponse::default()
        .with_throttle_time_ms(reply.throttle)
        .with_topics(topics);
    reply_frame(Api::ListOffsets, reply.correlation, &BTreeMap::new(), &body)
}

/// The rows of a reply with a throttle time, one row per partition.
#[derive(Debug)]
struct RowsRead<Row> {
    correlation: i32,
    throttle: i32,
    rows: Vec<Row>,
}

#[derive(Debug)]
struct ListRow {
    topic: Vec<u8>,
    index: i32,
    code: i16,
    timestamp: i64,
    offset: i64,
    leader_epoch: i32,
}

fn read_list(frame: &[u8]) -> Result<RowsRead<ListRow>, Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::ListOffsets)?;
    let mut rows = Vec::new();
    let throttle = wire::list_offsets_response(body, |topic, partition| {
        rows.push(ListRow {
            topic: topic.to_vec(),
            index: partition.partition_index.get(),
            code: partition.error_code.get(),
            timestamp: partition.timestamp.get(),
            offset: partition.offset.get(),
            leader_epoch: partition.leader_epoch.get(),
        });
        Ok(())
    })?;
    Ok(RowsRead {
        correlation,
        throttle,
        rows,
    })
}

// ------------------------------------------------------------ Produce reply

#[derive(Clone, Debug)]
struct ProducePart {
    index: i32,
    code: i16,
    base_offset: i64,
    append_time: i64,
    start_offset: i64,
}

#[derive(Clone, Debug)]
struct ProduceReply {
    correlation: i32,
    topics: Vec<(String, Vec<ProducePart>)>,
    throttle: i32,
}

fn produce_reply() -> impl Strategy<Value = ProduceReply> {
    let part = (
        any::<i32>(),
        any::<i16>(),
        any::<i64>(),
        any::<i64>(),
        any::<i64>(),
    )
        .prop_map(
            |(index, code, base_offset, append_time, start_offset)| ProducePart {
                index,
                code,
                base_offset,
                append_time,
                start_offset,
            },
        );
    let topic = (topic_name(), vec(part, 0..5));
    (any::<i32>(), vec(topic, 0..5), any::<i32>()).prop_map(|(correlation, topics, throttle)| {
        ProduceReply {
            correlation,
            topics,
            throttle,
        }
    })
}

fn encode_produce(reply: &ProduceReply) -> Vec<u8> {
    let responses = reply
        .topics
        .iter()
        .map(|(topic, parts)| {
            let parts = parts
                .iter()
                .map(|part| {
                    PartitionProduceResponse::default()
                        .with_index(part.index)
                        .with_error_code(part.code)
                        .with_base_offset(part.base_offset)
                        .with_log_append_time_ms(part.append_time)
                        .with_log_start_offset(part.start_offset)
                })
                .collect();
            TopicProduceResponse::default()
                .with_name(name(topic))
                .with_partition_responses(parts)
        })
        .collect();
    let body = ProduceResponse::default()
        .with_responses(responses)
        .with_throttle_time_ms(reply.throttle);
    reply_frame(Api::Produce, reply.correlation, &BTreeMap::new(), &body)
}

#[derive(Debug)]
struct ProduceRow {
    topic: Vec<u8>,
    index: i32,
    code: i16,
    base_offset: i64,
    append_time: i64,
    start_offset: i64,
}

fn read_produce(frame: &[u8]) -> Result<RowsRead<ProduceRow>, Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::Produce)?;
    let mut rows = Vec::new();
    let throttle = wire::produce_response(body, |topic, partition| {
        rows.push(ProduceRow {
            topic: topic.to_vec(),
            index: partition.index.get(),
            code: partition.error_code.get(),
            base_offset: partition.base_offset.get(),
            append_time: partition.log_append_time_ms.get(),
            start_offset: partition.log_start_offset.get(),
        });
        Ok(())
    })?;
    Ok(RowsRead {
        correlation,
        throttle,
        rows,
    })
}

// -------------------------------------------------------------- Fetch reply

#[derive(Clone, Debug)]
struct FetchPart {
    index: i32,
    code: i16,
    high_watermark: i64,
    last_stable: i64,
    log_start: i64,
    aborted: Option<Vec<(i64, i64)>>,
    preferred: i32,
    records: Option<Vec<u8>>,
}

#[derive(Clone, Debug)]
struct FetchReply {
    correlation: i32,
    throttle: i32,
    code: i16,
    session: i32,
    topics: Vec<(String, Vec<FetchPart>)>,
}

fn fetch_reply() -> impl Strategy<Value = FetchReply> {
    let offsets = (any::<i64>(), any::<i64>(), any::<i64>());
    let aborted = option::of(vec((any::<i64>(), any::<i64>()), 0..4));
    let records = option::of(vec(any::<u8>(), 0..120));
    let part = (
        any::<i32>(),
        any::<i16>(),
        offsets,
        aborted,
        any::<i32>(),
        records,
    )
        .prop_map(
            |(
                index,
                code,
                (high_watermark, last_stable, log_start),
                aborted,
                preferred,
                records,
            )| {
                FetchPart {
                    index,
                    code,
                    high_watermark,
                    last_stable,
                    log_start,
                    aborted,
                    preferred,
                    records,
                }
            },
        );
    let topic = (topic_name(), vec(part, 0..5));
    (
        any::<i32>(),
        any::<i32>(),
        any::<i16>(),
        any::<i32>(),
        vec(topic, 0..5),
    )
        .prop_map(
            |(correlation, throttle, code, session, topics)| FetchReply {
                correlation,
                throttle,
                code,
                session,
                topics,
            },
        )
}

fn encode_fetch(reply: &FetchReply) -> Vec<u8> {
    let responses = reply
        .topics
        .iter()
        .map(|(topic, parts)| {
            let parts = parts
                .iter()
                .map(|part| {
                    let aborted = part.aborted.as_ref().map(|aborted| {
                        aborted
                            .iter()
                            .map(|&(producer, first)| {
                                AbortedTransaction::default()
                                    .with_producer_id(producer.into())
                                    .with_first_offset(first)
                            })
                            .collect()
                    });
                    PartitionData::default()
                        .with_partition_index(part.index)
                        .with_error_code(part.code)
                        .with_high_watermark(part.high_watermark)
                        .with_last_stable_offset(part.last_stable)
                        .with_log_start_offset(part.log_start)
                        .with_aborted_transactions(aborted)
                        .with_preferred_read_replica(BrokerId(part.preferred))
                        .with_records(part.records.clone().map(Bytes::from))
                })
                .collect();
            FetchableTopicResponse::default()
                .with_topic(name(topic))
                .with_partitions(parts)
        })
        .collect();
    let body = FetchResponse::default()
        .with_throttle_time_ms(reply.throttle)
        .with_error_code(reply.code)
        .with_session_id(reply.session)
        .with_responses(responses);
    reply_frame(Api::Fetch, reply.correlation, &BTreeMap::new(), &body)
}

/// One row per partition, records copied out of the body.
#[derive(Debug, PartialEq, Eq)]
struct FetchRow {
    topic: Vec<u8>,
    index: i32,
    code: i16,
    offsets: (i64, i64, i64),
    aborted: Option<usize>,
    preferred: i32,
    records: Option<Vec<u8>>,
}

fn read_fetch(frame: &[u8]) -> Result<(i32, wire::FetchHead, Vec<FetchRow>), Fatal> {
    let (correlation, body) = wire::response_header(frame, Api::Fetch)?;
    let mut rows = Vec::new();
    let head = wire::fetch_response(body, |partition| {
        rows.push(FetchRow {
            topic: partition.topic.to_vec(),
            index: partition.head.partition_index.get(),
            code: partition.head.error_code.get(),
            offsets: (
                partition.head.high_watermark.get(),
                partition.head.last_stable_offset.get(),
                partition.head.log_start_offset.get(),
            ),
            aborted: partition.aborted_transactions,
            preferred: partition.preferred_read_replica,
            records: partition.records.map(|range| body[range].to_vec()),
        });
        Ok(())
    })?;
    Ok((correlation, head, rows))
}

// ------------------------------------------------------------------ Requests

/// Writes a request with the hand codecs: the header of `api`, then `body`.
/// Returns the count pass, and the bytes the write pass wrote.
fn write_request(
    api: Api,
    correlation: i32,
    client_id: &str,
    body: impl Fn(&mut Writer<'_>),
) -> (usize, Vec<u8>) {
    let request = |writer: &mut Writer<'_>| {
        wire::request_header(writer, api, correlation, client_id);
        body(writer);
    };
    let size = Writer::size(request);
    // Room for more than the count, so a write pass that puts more shows.
    let mut bytes = vec![0; size + 64];
    let written = {
        let mut writer = Writer::new(&mut bytes);
        request(&mut writer);
        writer.written()
    };
    bytes.truncate(written);
    (size, bytes)
}

/// Decodes a hand-written request with `kafka-protocol`: the header, then
/// the body of `api`. Checks the header and that no byte is left.
fn decode_request<M: Decodable>(
    api: Api,
    bytes: &[u8],
    correlation: i32,
    client_id: &str,
) -> Result<M, TestCaseError> {
    let mut buf = bytes;
    let header_version = api_key(api).request_header_version(api.version());
    let header = RequestHeader::decode(&mut buf, header_version)
        .map_err(|error| TestCaseError::fail(format!("{api:?} header: {error}")))?;
    prop_assert_eq!(header.request_api_key, api.key(), "{:?} api key", api);
    prop_assert_eq!(
        header.request_api_version,
        api.version(),
        "{:?} api version",
        api
    );
    prop_assert_eq!(
        header.correlation_id,
        correlation,
        "{:?} correlation id",
        api
    );
    prop_assert_eq!(
        header.client_id.as_deref(),
        Some(client_id),
        "{:?} client id",
        api
    );
    let message = M::decode(&mut buf, api.version())
        .map_err(|error| TestCaseError::fail(format!("{api:?} body: {error}")))?;
    prop_assert!(buf.is_empty(), "{:?}: bytes after the request", api);
    Ok(message)
}

/// Topics with distinct names, each with its (partition, offset, batch)
/// targets.
type Targets = Vec<(String, Vec<(i32, i64, Vec<u8>)>)>;

fn targets() -> impl Strategy<Value = Targets> {
    let partitions = vec((any::<i32>(), any::<i64>(), vec(any::<u8>(), 0..40)), 1..6);
    proptest::collection::btree_map(topic_name(), partitions, 0..8)
        .prop_map(|topics| topics.into_iter().collect::<Vec<_>>())
        .prop_filter("at most MAX_PARTS targets", |topics| {
            topics.iter().map(|(_, parts)| parts.len()).sum::<usize>() <= MAX_PARTS
        })
}

/// Per topic, (partition, records) of a Produce request.
type Produced = Vec<(String, Vec<(i32, Option<Vec<u8>>)>)>;

/// (topic, partition) pairs per topic, in the shape of the targets.
fn partitions_of(topics: &Targets) -> Vec<(String, Vec<i32>)> {
    topics
        .iter()
        .map(|(topic, parts)| {
            let partitions = parts.iter().map(|(partition, _, _)| *partition).collect();
            (topic.clone(), partitions)
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn api_versions_reply_reads_back(reply in versions_reply()) {
        let frame = encode_versions(&reply);
        let read = read_versions(&frame).expect("a whole reply reads");
        let expected: Vec<VersionRange> = reply
            .ranges
            .iter()
            .map(|&(key, min, max, _)| VersionRange {
                api_key: key.into(),
                min_version: min.into(),
                max_version: max.into(),
            })
            .collect();
        prop_assert_eq!(read.correlation, reply.correlation, "correlation id");
        prop_assert_eq!(read.code, 0, "error code");
        prop_assert_eq!(read.ranges, expected, "ranges");
        bounds(&frame, read_versions)?;
    }

    #[test]
    fn metadata_reply_reads_back(reply in metadata_reply()) {
        let frame = encode_metadata(&reply);
        let (correlation, rows) = read_metadata(&frame).expect("a whole reply reads");
        prop_assert_eq!(correlation, reply.correlation, "correlation id");
        // Brokers come first, then each topic followed by its partitions.
        let (brokers, rest) = rows.split_at(reply.brokers.len().min(rows.len()));
        let expected: Vec<MetadataRow> =
            reply.brokers.iter().map(|broker| MetadataRow::Broker(broker.0)).collect();
        prop_assert_eq!(brokers, expected.as_slice(), "broker node ids");
        let expected: Vec<MetadataRow> = reply
            .topics
            .iter()
            .flat_map(|topic| {
                let name = topic.name.as_ref().map(|name| name.as_bytes().to_vec());
                let head = MetadataRow::Topic(topic.code, name, topic.parts.len());
                let parts = topic.parts.iter().map(|part| {
                    MetadataRow::Partition(part.code, part.index, part.leader, part.leader_epoch)
                });
                std::iter::once(head).chain(parts)
            })
            .collect();
        prop_assert_eq!(
            rest,
            expected.as_slice(),
            "topic error code, name, partition count; partition error code, index, leader, leader epoch"
        );
        bounds(&frame, read_metadata)?;
    }

    #[test]
    fn init_producer_id_reply_reads_back(reply in init_reply()) {
        let frame = encode_init(&reply);
        let (correlation, read) = read_init(&frame).expect("a whole reply reads");
        prop_assert_eq!(correlation, reply.correlation, "correlation id");
        prop_assert_eq!(read.throttle_time_ms.get(), reply.throttle, "throttle");
        prop_assert_eq!(read.error_code.get(), reply.code, "error code");
        prop_assert_eq!(read.producer_id.get(), reply.id, "producer id");
        prop_assert_eq!(read.producer_epoch.get(), reply.epoch, "producer epoch");
        bounds(&frame, read_init)?;
    }

    #[test]
    fn list_offsets_reply_reads_back(reply in list_reply()) {
        let frame = encode_list(&reply);
        let read = read_list(&frame).expect("a whole reply reads");
        let expected: Vec<ListRow> = reply
            .topics
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().map(move |part| ListRow {
                    topic: topic.as_bytes().to_vec(),
                    index: part.index,
                    code: part.code,
                    timestamp: part.timestamp,
                    offset: part.offset,
                    leader_epoch: part.leader_epoch,
                })
            })
            .collect();
        prop_assert_eq!(read.correlation, reply.correlation, "correlation id");
        prop_assert_eq!(read.throttle, reply.throttle, "throttle");
        prop_assert_eq!(read.rows.len(), expected.len(), "partition count");
        for (row, expected) in read.rows.iter().zip(&expected) {
            prop_assert_eq!(&row.topic, &expected.topic, "topic name");
            prop_assert_eq!(row.index, expected.index, "partition index");
            prop_assert_eq!(row.code, expected.code, "error code");
            prop_assert_eq!(row.timestamp, expected.timestamp, "timestamp");
            prop_assert_eq!(row.offset, expected.offset, "offset");
            prop_assert_eq!(row.leader_epoch, expected.leader_epoch, "leader epoch");
        }
        bounds(&frame, read_list)?;
    }

    #[test]
    fn produce_reply_reads_back(reply in produce_reply()) {
        let frame = encode_produce(&reply);
        let read = read_produce(&frame).expect("a whole reply reads");
        let expected: Vec<ProduceRow> = reply
            .topics
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().map(move |part| ProduceRow {
                    topic: topic.as_bytes().to_vec(),
                    index: part.index,
                    code: part.code,
                    base_offset: part.base_offset,
                    append_time: part.append_time,
                    start_offset: part.start_offset,
                })
            })
            .collect();
        prop_assert_eq!(read.correlation, reply.correlation, "correlation id");
        prop_assert_eq!(read.throttle, reply.throttle, "throttle");
        prop_assert_eq!(read.rows.len(), expected.len(), "partition count");
        for (row, expected) in read.rows.iter().zip(&expected) {
            prop_assert_eq!(&row.topic, &expected.topic, "topic name");
            prop_assert_eq!(row.index, expected.index, "partition index");
            prop_assert_eq!(row.code, expected.code, "error code");
            prop_assert_eq!(row.base_offset, expected.base_offset, "base offset");
            prop_assert_eq!(row.append_time, expected.append_time, "log append time");
            prop_assert_eq!(row.start_offset, expected.start_offset, "log start offset");
        }
        bounds(&frame, read_produce)?;
    }

    #[test]
    fn fetch_reply_reads_back(reply in fetch_reply()) {
        let frame = encode_fetch(&reply);
        let (correlation, head, rows) = read_fetch(&frame).expect("a whole reply reads");
        let expected: Vec<FetchRow> = reply
            .topics
            .iter()
            .flat_map(|(topic, parts)| {
                parts.iter().map(move |part| FetchRow {
                    topic: topic.as_bytes().to_vec(),
                    index: part.index,
                    code: part.code,
                    offsets: (part.high_watermark, part.last_stable, part.log_start),
                    aborted: part.aborted.as_ref().map(Vec::len),
                    preferred: part.preferred,
                    records: part.records.clone(),
                })
            })
            .collect();
        prop_assert_eq!(correlation, reply.correlation, "correlation id");
        prop_assert_eq!(head.throttle_time_ms.get(), reply.throttle, "throttle");
        prop_assert_eq!(head.error_code.get(), reply.code, "request error code");
        prop_assert_eq!(head.session_id.get(), reply.session, "session id");
        prop_assert_eq!(rows.len(), expected.len(), "partition count");
        for (row, expected) in rows.iter().zip(&expected) {
            prop_assert_eq!(&row.topic, &expected.topic, "topic name");
            prop_assert_eq!(row.index, expected.index, "partition index");
            prop_assert_eq!(row.code, expected.code, "partition error code");
            prop_assert_eq!(row.offsets, expected.offsets, "high watermark, last stable, log start");
            prop_assert_eq!(row.aborted, expected.aborted, "aborted transactions skipped");
            prop_assert_eq!(row.preferred, expected.preferred, "preferred read replica");
            prop_assert_eq!(&row.records, &expected.records, "records in place");
        }
        bounds(&frame, read_fetch)?;
    }

    /// Random bytes, and valid replies with one byte changed, read to a
    /// result or `Fatal::Protocol` in every reader, never a panic.
    #[test]
    fn garbage_never_panics(
        noise in vec(any::<u8>(), 0..300),
        versions in versions_reply(),
        metadata in metadata_reply(),
        init in init_reply(),
        list in list_reply(),
        produce in produce_reply(),
        fetch in fetch_reply(),
        at in any::<Index>(),
        byte in any::<u8>(),
    ) {
        let frames = [
            noise,
            encode_versions(&versions),
            encode_metadata(&metadata),
            encode_init(&init),
            encode_list(&list),
            encode_produce(&produce),
            encode_fetch(&fetch),
        ];
        for frame in frames {
            let mut changed = frame.clone();
            if !changed.is_empty() {
                let at = at.index(changed.len());
                changed[at] = byte;
            }
            for frame in [&frame, &changed] {
                let results = [
                    read_versions(frame).err(),
                    read_metadata(frame).err(),
                    read_init(frame).err(),
                    read_list(frame).err(),
                    read_produce(frame).err(),
                    read_fetch(frame).err(),
                ];
                for result in results {
                    prop_assert!(matches!(result, None | Some(Fatal::Protocol)), "{:?}", result);
                }
            }
        }
    }

    /// Every request, written by hand, decodes with `kafka-protocol`.
    #[test]
    fn requests_decode(
        client_id in "[a-zA-Z0-9._-]{0,24}",
        correlation in any::<i32>(),
        topics in targets(),
        timestamp in any::<i64>(),
        timeout_ms in any::<i32>(),
        partition_max_bytes in 0..i32::MAX,
        max_wait_ms in any::<i32>(),
        min_bytes in any::<i32>(),
    ) {
        let limits = FetchLimits { partition_max_bytes, max_wait_ms, min_bytes };
        let partitions = partitions_of(&topics);
        let names: Vec<&str> = topics.iter().map(|(topic, _)| topic.as_str()).collect();
        let count: usize = topics.iter().map(|(_, parts)| parts.len()).sum();
        let pairs = || {
            topics.iter().flat_map(|(topic, parts)| {
                parts.iter().map(move |(partition, offset, batch)| {
                    (topic.as_str(), *partition, *offset, batch.as_slice())
                })
            })
        };

        // ApiVersions
        let (size, bytes) = write_request(Api::ApiVersions, correlation, &client_id, wire::api_versions_request);
        prop_assert_eq!(size, bytes.len(), "ApiVersions size pass");
        let request: ApiVersionsRequest = decode_request(Api::ApiVersions, &bytes, correlation, &client_id)?;
        prop_assert_eq!(request.client_software_name.as_str(), "kafka-sans-io", "software name");
        prop_assert_eq!(
            request.client_software_version.as_str(),
            env!("CARGO_PKG_VERSION"),
            "software version"
        );

        // Metadata
        let (size, bytes) = write_request(Api::Metadata, correlation, &client_id, |writer| {
            wire::metadata_request(writer, names.iter().copied());
        });
        prop_assert_eq!(size, bytes.len(), "Metadata size pass");
        let request: MetadataRequest = decode_request(Api::Metadata, &bytes, correlation, &client_id)?;
        let requested: Vec<Option<String>> = request
            .topics
            .unwrap_or_default()
            .iter()
            .map(|topic| topic.name.as_ref().map(|name| name.to_string()))
            .collect();
        let expected: Vec<Option<String>> = names.iter().map(|name| Some(name.to_string())).collect();
        prop_assert_eq!(requested, expected, "Metadata topic names");
        prop_assert!(!request.allow_auto_topic_creation, "no topic creation");
        prop_assert!(!request.include_cluster_authorized_operations, "no cluster operations");
        prop_assert!(!request.include_topic_authorized_operations, "no topic operations");

        // InitProducerId
        let (size, bytes) = write_request(Api::InitProducerId, correlation, &client_id, wire::init_producer_id_request);
        prop_assert_eq!(size, bytes.len(), "InitProducerId size pass");
        let request: InitProducerIdRequest =
            decode_request(Api::InitProducerId, &bytes, correlation, &client_id)?;
        prop_assert!(request.transactional_id.is_none(), "no transactional id");
        prop_assert_eq!(request.transaction_timeout_ms, 60_000, "transaction timeout");
        prop_assert_eq!(*request.producer_id, -1, "broker chooses the id");
        prop_assert_eq!(request.producer_epoch, -1, "broker chooses the epoch");

        // ListOffsets
        let (size, bytes) = write_request(Api::ListOffsets, correlation, &client_id, |writer| {
            let targets = pairs().map(|(topic, partition, _, _)| (topic, partition));
            wire::list_offsets_request(writer, timestamp, targets);
        });
        prop_assert_eq!(size, bytes.len(), "ListOffsets size pass");
        let request: ListOffsetsRequest =
            decode_request(Api::ListOffsets, &bytes, correlation, &client_id)?;
        prop_assert_eq!(*request.replica_id, -1, "ListOffsets consumer replica id");
        prop_assert_eq!(request.isolation_level, 0, "ListOffsets read uncommitted");
        let listed: Vec<(String, Vec<i32>)> = request
            .topics
            .iter()
            .map(|topic| {
                let partitions = topic.partitions.iter().map(|partition| partition.partition_index).collect();
                (topic.name.to_string(), partitions)
            })
            .collect();
        prop_assert_eq!(&listed, &partitions, "ListOffsets topics and partitions");
        for partition in request.topics.iter().flat_map(|topic| &topic.partitions) {
            prop_assert_eq!(partition.timestamp, timestamp, "ListOffsets timestamp");
            prop_assert_eq!(partition.current_leader_epoch, -1, "ListOffsets unknown leader epoch");
        }

        // Produce
        let (size, bytes) = write_request(Api::Produce, correlation, &client_id, |writer| {
            let targets = pairs().map(|(topic, partition, _, batch)| ProduceTarget { topic, partition, batch });
            wire::produce_request(writer, timeout_ms, targets);
        });
        prop_assert_eq!(size, bytes.len(), "Produce size pass");
        let request: ProduceRequest = decode_request(Api::Produce, &bytes, correlation, &client_id)?;
        prop_assert!(request.transactional_id.is_none(), "Produce no transactional id");
        prop_assert_eq!(request.acks, -1, "acks from all replicas");
        prop_assert_eq!(request.timeout_ms, timeout_ms, "Produce timeout");
        let produced: Produced = request
            .topic_data
            .iter()
            .map(|topic| {
                let parts = topic
                    .partition_data
                    .iter()
                    .map(|partition| (partition.index, partition.records.as_ref().map(|records| records.to_vec())))
                    .collect();
                (topic.name.to_string(), parts)
            })
            .collect();
        let expected: Produced = topics
            .iter()
            .map(|(topic, parts)| {
                let parts = parts.iter().map(|(partition, _, batch)| (*partition, Some(batch.clone()))).collect();
                (topic.clone(), parts)
            })
            .collect();
        prop_assert_eq!(produced, expected, "Produce topics, partitions, and batches");

        // Fetch
        let (size, bytes) = write_request(Api::Fetch, correlation, &client_id, |writer| {
            let targets = pairs().map(|(topic, partition, fetch_offset, _)| FetchTarget { topic, partition, fetch_offset });
            wire::fetch_request(writer, limits, targets);
        });
        prop_assert_eq!(size, bytes.len(), "Fetch size pass");
        let request: FetchRequest = decode_request(Api::Fetch, &bytes, correlation, &client_id)?;
        prop_assert_eq!(*request.replica_id, -1, "Fetch consumer replica id");
        prop_assert_eq!(request.max_wait_ms, max_wait_ms, "max wait");
        prop_assert_eq!(request.min_bytes, min_bytes, "min bytes");
        prop_assert_eq!(request.isolation_level, 0, "Fetch read uncommitted");
        prop_assert_eq!(request.session_epoch, -1, "no session");
        prop_assert_eq!(request.max_bytes, partition_max_bytes.saturating_mul(count as i32), "max bytes");
        let fetched: Vec<(String, Vec<(i32, i64)>)> = request
            .topics
            .iter()
            .map(|topic| {
                let parts = topic.partitions.iter().map(|partition| (partition.partition, partition.fetch_offset)).collect();
                (topic.topic.to_string(), parts)
            })
            .collect();
        let expected: Vec<(String, Vec<(i32, i64)>)> = topics
            .iter()
            .map(|(topic, parts)| {
                let parts = parts.iter().map(|(partition, offset, _)| (*partition, *offset)).collect();
                (topic.clone(), parts)
            })
            .collect();
        prop_assert_eq!(fetched, expected, "Fetch topics, partitions, and offsets");
        for partition in request.topics.iter().flat_map(|topic| &topic.partitions) {
            prop_assert_eq!(partition.partition_max_bytes, partition_max_bytes, "partition max bytes");
        }
    }
}

/// A broker without ApiVersions v3 answers in v0: an error code, then the
/// v0 layout. The reader returns the code and reads no range.
#[test]
fn api_versions_error_ends_the_read() {
    let mut frame = Vec::new();
    ResponseHeader::default()
        .with_correlation_id(9)
        .encode(&mut frame, 0)
        .expect("header encodes");
    ApiVersionsResponse::default()
        .with_error_code(35)
        .with_api_keys(vec![ApiVersion::default().with_api_key(18)])
        .encode(&mut frame, 0)
        .expect("v0 reply encodes");
    let read = read_versions(&frame).expect("a v0 error reply reads");
    assert_eq!(read.correlation, 9, "correlation id");
    assert_eq!(read.code, 35, "error code");
    assert_eq!(read.ranges, Vec::new(), "no range");
}
