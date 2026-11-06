//! ApiVersions v3 and Metadata v8: the handshake on each connection.

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    big_endian::{I16, I32},
};

use super::write::Writer;
use crate::{error::Fatal, read::Cursor};

/// Writes an ApiVersions v3 request body.
///
/// Layout (flexible): client software name compact string, client software
/// version compact string, tagged fields.
pub fn api_versions_request(writer: &mut Writer<'_>) {
    writer.compact_string(SOFTWARE_NAME);
    writer.compact_string(SOFTWARE_VERSION);
    writer.no_tagged_fields();
}

/// Reads an ApiVersions v3 reply body and returns its error code. A code
/// other than 0 ends the read: a broker without v3 answers in v0, which has
/// another layout after the code. Otherwise calls `visit` for each range.
///
/// Layout (flexible): error code i16, api keys compact array of (range,
/// tagged fields), throttle time i32, tagged fields.
pub fn api_versions_response<'a>(
    body: &'a [u8],
    mut visit: impl FnMut(&'a VersionRange),
) -> Result<i16, Fatal> {
    let mut cursor = Cursor::new(body);
    let error_code = cursor.i16()?;
    if error_code != 0 {
        return Ok(error_code);
    }
    for _ in 0..cursor.compact_array()? {
        visit(cursor.read()?);
        cursor.tagged_fields()?;
    }
    let _throttle_time_ms = cursor.i32()?;
    cursor.tagged_fields()?;
    cursor.end()?;
    Ok(error_code)
}

/// The versions the broker serves for one API.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct VersionRange {
    pub api_key: I16,
    pub min_version: I16,
    pub max_version: I16,
}

/// Writes a Metadata v8 request body for `topics`. The broker creates no
/// topic.
///
/// Layout: topics array of (name string), flags.
pub fn metadata_request<'t>(
    writer: &mut Writer<'_>,
    topics: impl ExactSizeIterator<Item = &'t str>,
) {
    writer.array(topics.len());
    for topic in topics {
        writer.string(topic);
    }
    writer.put(&MetadataFlags {
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false,
    });
}

/// The three flags at the end of a Metadata v8 request.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct MetadataFlags {
    allow_auto_topic_creation: bool,
    include_cluster_authorized_operations: bool,
    include_topic_authorized_operations: bool,
}

/// Reads a Metadata v8 reply body and calls `visit` for each broker, topic,
/// and partition, in reply order. An error from `visit` stops the read.
///
/// Layout: throttle time i32, brokers array of (node id i32, host string,
/// port i32, rack nullable string), cluster id nullable string, controller
/// id i32, topics array of (error code i16, name nullable string, is
/// internal bool, partitions array of (partition, replica nodes array of
/// i32, isr nodes array of i32, offline replicas array of i32), topic
/// authorized operations i32), cluster authorized operations i32.
pub fn metadata_response<'a>(
    body: &'a [u8],
    mut visit: impl FnMut(MetadataItem<'a>) -> Result<(), Fatal>,
) -> Result<(), Fatal> {
    let mut cursor = Cursor::new(body);
    let _throttle_time_ms = cursor.i32()?;
    for _ in 0..cursor.array()? {
        let node_id = cursor.i32()?;
        let _host = cursor.string()?;
        let _port = cursor.i32()?;
        let _rack = cursor.nullable_string()?;
        visit(MetadataItem::Broker { node_id })?;
    }
    let _cluster_id = cursor.nullable_string()?;
    let _controller_id = cursor.i32()?;
    for _ in 0..cursor.array()? {
        let error_code = cursor.i16()?;
        let name = cursor.nullable_string()?;
        let _is_internal = cursor.read::<u8>()?;
        let partitions = cursor.array()?;
        visit(MetadataItem::Topic {
            error_code,
            name,
            partitions,
        })?;
        for _ in 0..partitions {
            visit(MetadataItem::Partition(cursor.read()?))?;
            // Replica nodes, isr nodes, offline replicas.
            for _ in 0..3 {
                let count = cursor.array()?;
                cursor.skip_items::<I32>(count)?;
            }
        }
        let _topic_authorized_operations = cursor.i32()?;
    }
    let _cluster_authorized_operations = cursor.i32()?;
    cursor.end()
}

/// One part of a Metadata v8 reply, in reply order. The partitions of a
/// topic follow it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetadataItem<'a> {
    Broker {
        node_id: i32,
    },
    Topic {
        error_code: i16,
        name: Option<&'a [u8]>,
        partitions: usize,
    },
    Partition(&'a MetadataPartition),
}

/// The fixed fields at the start of one partition of a Metadata v8 reply.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
pub struct MetadataPartition {
    pub error_code: I16,
    pub partition_index: I32,
    pub leader_id: I32,
    pub leader_epoch: I32,
}

/// Name and version of this client in ApiVersions. The broker checks each
/// against `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`.
const SOFTWARE_NAME: &str = "kafka-sans-io";

const SOFTWARE_VERSION: &str = env!("CARGO_PKG_VERSION");
