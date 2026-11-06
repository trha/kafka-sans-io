//! InitProducerId v3 and Produce v7: the produce job.

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    big_endian::{I16, I32, I64},
};

use super::write::{Writer, topic_array};
use crate::{error::Fatal, read::Cursor};

/// Writes an InitProducerId v3 request body for an idempotent producer: no
/// transactional id, and the broker chooses the id.
///
/// Layout (flexible): transactional id compact nullable string, fields,
/// tagged fields.
pub fn init_producer_id_request(writer: &mut Writer<'_>) {
    writer.null_compact_string();
    writer.put(&InitProducerIdFields {
        transaction_timeout_ms: 60_000.into(),
        producer_id: (-1).into(),
        producer_epoch: (-1).into(),
    });
    writer.no_tagged_fields();
}

/// The fixed fields of an InitProducerId v3 request after the
/// transactional id.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct InitProducerIdFields {
    transaction_timeout_ms: I32,
    producer_id: I64,
    producer_epoch: I16,
}

/// Reads an InitProducerId v3 reply body.
///
/// Layout (flexible): reply, tagged fields.
pub fn init_producer_id_response(body: &[u8]) -> Result<InitProducerIdReply, Fatal> {
    let mut cursor = Cursor::new(body);
    let reply = *cursor.read::<InitProducerIdReply>()?;
    cursor.tagged_fields()?;
    cursor.end()?;
    Ok(reply)
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct InitProducerIdReply {
    pub throttle_time_ms: I32,
    pub error_code: I16,
    pub producer_id: I64,
    pub producer_epoch: I16,
}

/// Writes a Produce v7 request body: no transactional id, acks from all
/// replicas. Targets of one topic must be next to each other; each run of
/// one topic is one topic entry. At most `MAX_PARTS` targets.
///
/// The batches go into the send buffer in one copy, from the accumulator.
///
/// Layout: head, topics array of (name string, partitions array of (index
/// i32, records nullable bytes)).
pub fn produce_request<'t, 'b>(
    writer: &mut Writer<'_>,
    timeout_ms: i32,
    targets: impl Clone + Iterator<Item = ProduceTarget<'t, 'b>>,
) {
    writer.put(&ProduceHead {
        transactional_id_length: (-1).into(),
        acks: (-1).into(),
        timeout_ms: timeout_ms.into(),
    });
    topic_array(
        writer,
        targets,
        |target| target.topic,
        |writer, target| {
            writer.i32(target.partition);
            writer.nullable_bytes(target.batch);
        },
    );
}

/// One batch to produce.
#[derive(Clone, Copy, Debug)]
pub struct ProduceTarget<'t, 'b> {
    pub topic: &'t str,
    pub partition: i32,
    /// RecordBatch bytes.
    pub batch: &'b [u8],
}

/// The fixed fields at the start of a Produce v7 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ProduceHead {
    /// -1: a null string.
    transactional_id_length: I16,
    acks: I16,
    timeout_ms: I32,
}

/// Reads a Produce v7 reply body. Calls `visit` with the topic name and
/// each partition in reply order, and returns the throttle time. An error
/// from `visit` stops the read.
///
/// Layout: responses array of (name string, partition responses array of
/// partition), throttle time i32.
pub fn produce_response<'a>(
    body: &'a [u8],
    mut visit: impl FnMut(&'a [u8], &'a ProducePartition) -> Result<(), Fatal>,
) -> Result<i32, Fatal> {
    let mut cursor = Cursor::new(body);
    for _ in 0..cursor.array()? {
        let topic = cursor.string()?;
        for _ in 0..cursor.array()? {
            visit(topic, cursor.read()?)?;
        }
    }
    let throttle_time_ms = cursor.i32()?;
    cursor.end()?;
    Ok(throttle_time_ms)
}

/// The fields of one partition of a Produce v7 reply.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct ProducePartition {
    pub index: I32,
    pub error_code: I16,
    pub base_offset: I64,
    pub log_append_time_ms: I64,
    pub log_start_offset: I64,
}
