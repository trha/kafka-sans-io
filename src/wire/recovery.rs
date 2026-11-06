//! ListOffsets v5 and Fetch v11: the recovery and consumer jobs.

use core::ops::Range;

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    big_endian::{I16, I32, I64},
};

use super::write::{Writer, topic_array};
use crate::{error::Fatal, read::Cursor};

/// Writes a ListOffsets v5 request body for `targets` (topic name,
/// partition index) at `timestamp`, as a consumer at READ_UNCOMMITTED: the
/// end is the high watermark. Timestamp -1 asks for the end offset.
/// Targets of one topic must be next to each other. At most `MAX_PARTS`
/// targets.
///
/// Layout: head, topics array of (name string, partitions array of
/// target).
pub fn list_offsets_request<'t>(
    writer: &mut Writer<'_>,
    timestamp: i64,
    targets: impl Clone + Iterator<Item = (&'t str, i32)>,
) {
    writer.put(&ListOffsetsHead {
        replica_id: (-1).into(),
        isolation_level: 0,
    });
    topic_array(
        writer,
        targets,
        |&(topic, _)| topic,
        |writer, (_, partition)| {
            writer.put(&ListOffsetsTarget {
                partition_index: partition.into(),
                current_leader_epoch: (-1).into(),
                timestamp: timestamp.into(),
            });
        },
    );
}

/// The fixed fields at the start of a ListOffsets v5 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ListOffsetsHead {
    replica_id: I32,
    isolation_level: i8,
}

/// One partition of a ListOffsets v5 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct ListOffsetsTarget {
    partition_index: I32,
    current_leader_epoch: I32,
    timestamp: I64,
}

/// Reads a ListOffsets v5 reply body. Calls `visit` with the topic name and
/// each partition in reply order, and returns the throttle time. An error
/// from `visit` stops the read.
///
/// Layout: throttle time i32, topics array of (name string, partitions
/// array of partition).
pub fn list_offsets_response<'a>(
    body: &'a [u8],
    mut visit: impl FnMut(&'a [u8], &'a ListOffsetsPartition) -> Result<(), Fatal>,
) -> Result<i32, Fatal> {
    let mut cursor = Cursor::new(body);
    let throttle_time_ms = cursor.i32()?;
    for _ in 0..cursor.array()? {
        let topic = cursor.string()?;
        for _ in 0..cursor.array()? {
            visit(topic, cursor.read()?)?;
        }
    }
    cursor.end()?;
    Ok(throttle_time_ms)
}

/// One partition of a ListOffsets v5 reply.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct ListOffsetsPartition {
    pub partition_index: I32,
    pub error_code: I16,
    pub timestamp: I64,
    /// -1 when the broker has no offset for the timestamp.
    pub offset: I64,
    pub leader_epoch: I32,
}

/// Writes a Fetch v11 request body.
///
/// A full Fetch with no session: replica id -1, READ_UNCOMMITTED, and the
/// wait and min bytes of `limits`. `limits.partition_max_bytes` bounds each
/// partition, and the whole reply is bounded by it times the partition
/// count, so one partition never starves another. Targets of one
/// topic must be next to each other; each run of one topic is one topic
/// entry. At most `MAX_PARTS` targets.
///
/// Layout: head, topics array of (topic string, partitions array of
/// partition), forgotten topics array, rack id string.
pub fn fetch_request<'t>(
    writer: &mut Writer<'_>,
    limits: FetchLimits,
    targets: impl Clone + Iterator<Item = FetchTarget<'t>>,
) {
    let total = i32::try_from(targets.clone().count()).expect("at most MAX_PARTS targets");
    writer.put(&FetchRequestHead {
        replica_id: (-1).into(),
        max_wait_ms: limits.max_wait_ms.into(),
        min_bytes: limits.min_bytes.into(),
        max_bytes: limits.partition_max_bytes.saturating_mul(total).into(),
        isolation_level: 0,
        session_id: 0.into(),
        // A full fetch without a session.
        session_epoch: (-1).into(),
    });
    topic_array(
        writer,
        targets,
        |target| target.topic,
        |writer, target| {
            writer.put(&FetchRequestPartition {
                partition: target.partition.into(),
                current_leader_epoch: (-1).into(),
                fetch_offset: target.fetch_offset.into(),
                // A consumer has none.
                log_start_offset: (-1).into(),
                partition_max_bytes: limits.partition_max_bytes.into(),
            });
        },
    );
    writer.array(0); // forgotten topics
    writer.string(""); // rack id
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

/// One partition to fetch.
#[derive(Clone, Copy, Debug)]
pub struct FetchTarget<'t> {
    pub topic: &'t str,
    pub partition: i32,
    pub fetch_offset: i64,
}

/// The fixed fields at the start of a Fetch v11 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FetchRequestHead {
    replica_id: I32,
    max_wait_ms: I32,
    min_bytes: I32,
    max_bytes: I32,
    isolation_level: i8,
    session_id: I32,
    session_epoch: I32,
}

/// One partition of a Fetch v11 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FetchRequestPartition {
    partition: I32,
    current_leader_epoch: I32,
    fetch_offset: I64,
    log_start_offset: I64,
    partition_max_bytes: I32,
}

/// Reads a Fetch v11 reply body. Calls `visit` for each partition in reply
/// order, and returns the head. An error from `visit` stops the read.
///
/// Layout: head, responses array of (topic string, partitions array of
/// (partition head, aborted transactions nullable array, preferred read
/// replica i32, records nullable bytes)).
pub fn fetch_response<'a>(
    body: &'a [u8],
    mut visit: impl FnMut(FetchPartition<'a>) -> Result<(), Fatal>,
) -> Result<FetchHead, Fatal> {
    let mut cursor = Cursor::new(body);
    let head = *cursor.read::<FetchHead>()?;
    for _ in 0..cursor.array()? {
        let topic = cursor.string()?;
        for _ in 0..cursor.array()? {
            let partition_head = cursor.read()?;
            let aborted_transactions = cursor.nullable_array()?;
            cursor.skip_items::<AbortedTransaction>(aborted_transactions.unwrap_or(0))?;
            visit(FetchPartition {
                topic,
                head: partition_head,
                aborted_transactions,
                preferred_read_replica: cursor.i32()?,
                records: cursor.nullable_bytes()?,
            })?;
        }
    }
    cursor.end()?;
    Ok(head)
}

/// The fixed fields at the start of a Fetch v11 reply.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct FetchHead {
    pub throttle_time_ms: I32,
    /// Error code of the whole request.
    pub error_code: I16,
    pub session_id: I32,
}

/// One partition of a Fetch v11 reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchPartition<'a> {
    pub topic: &'a [u8],
    pub head: &'a FetchPartitionHead,
    /// Count of aborted transactions, `None` for a null array. The entries
    /// are skipped: the recovery reads uncommitted.
    pub aborted_transactions: Option<usize>,
    pub preferred_read_replica: i32,
    /// Where the records are in the body, `None` for null.
    pub records: Option<Range<usize>>,
}

/// The fixed fields at the start of one partition of a Fetch v11 reply.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct FetchPartitionHead {
    pub partition_index: I32,
    pub error_code: I16,
    pub high_watermark: I64,
    pub last_stable_offset: I64,
    pub log_start_offset: I64,
}

/// One aborted transaction of a Fetch v11 reply.
#[derive(FromBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AbortedTransaction {
    _producer_id: I64,
    _first_offset: I64,
}
