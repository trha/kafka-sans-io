//! Hand codecs for every message this crate sends and reads.
//!
//! `kafka-protocol` decodes from `&[u8]` with a copy of every bytes field,
//! and builds a `Vec` per array. These codecs read the frame in place and
//! write into the send buffer, so the conn makes no heap allocation after
//! `Conn::new`. Every number is big-endian.
//!
//! Two formats are in use. Produce 7, Fetch 11, ListOffsets 5, and
//! Metadata 8 are below the flexible format: arrays and bytes have an i32
//! length, strings an i16 length, and there are no tagged fields.
//! ApiVersions 3 and InitProducerId 3 are flexible (KIP-482): compact
//! arrays and strings have an unsigned varint length plus one (0 is null),
//! and each struct ends with a tagged field section.
//!
//! A reader returns `Fatal::Protocol` on a short or malformed body, and on
//! bytes left after the last field. It skips every tagged field.

mod handshake;
mod produce;
mod recovery;
mod write;

use zerocopy::{
    Immutable, IntoBytes, KnownLayout, Unaligned,
    big_endian::{I16, I32},
};

pub use self::{
    handshake::{
        MetadataItem, MetadataPartition, VersionRange, api_versions_request, api_versions_response,
        metadata_request, metadata_response,
    },
    produce::{
        InitProducerIdReply, ProducePartition, ProduceTarget, init_producer_id_request,
        init_producer_id_response, produce_request, produce_response,
    },
    recovery::{
        FetchHead, FetchLimits, FetchPartition, FetchPartitionHead, FetchTarget,
        ListOffsetsPartition, fetch_request, fetch_response, list_offsets_request,
        list_offsets_response,
    },
    write::Writer,
};
use crate::{error::Fatal, read::Cursor};

/// The APIs this crate sends, each at its pinned version. Each pinned
/// version must be in the broker's range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Api {
    ApiVersions,
    Metadata,
    InitProducerId,
    Produce,
    ListOffsets,
    Fetch,
}

impl Api {
    pub const ALL: [Api; 6] = [
        Api::ApiVersions,
        Api::Metadata,
        Api::InitProducerId,
        Api::Produce,
        Api::ListOffsets,
        Api::Fetch,
    ];

    pub const fn key(self) -> i16 {
        match self {
            Api::Produce => 0,
            Api::Fetch => 1,
            Api::ListOffsets => 2,
            Api::Metadata => 3,
            Api::ApiVersions => 18,
            Api::InitProducerId => 22,
        }
    }

    /// ListOffsets 5 and Fetch 11 are the newest versions below the
    /// flexible format, so every data request has the format of Produce 7.
    pub const fn version(self) -> i16 {
        match self {
            Api::ApiVersions => 3,
            Api::Metadata => 8,
            Api::InitProducerId => 3,
            Api::Produce => 7,
            Api::ListOffsets => 5,
            Api::Fetch => 11,
        }
    }

    /// The pinned version has the flexible format.
    const fn flexible(self) -> bool {
        matches!(self, Api::ApiVersions | Api::InitProducerId)
    }
}

/// Writes request header v1, or v2 for a flexible API.
///
/// Layout: api key i16, api version i16, correlation id i32, client id
/// string (an i16 length in v2 too), and in v2 a tagged field section.
pub fn request_header(writer: &mut Writer<'_>, api: Api, correlation: i32, client_id: &str) {
    writer.put(&RequestHead {
        api_key: api.key().into(),
        api_version: api.version().into(),
        correlation_id: correlation.into(),
    });
    writer.string(client_id);
    if api.flexible() {
        writer.no_tagged_fields();
    }
}

/// The fixed fields at the start of every request header.
#[derive(IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RequestHead {
    api_key: I16,
    api_version: I16,
    correlation_id: I32,
}

/// Reads the response header of a reply to `api`. Returns the correlation
/// id and the body after the header.
///
/// Layout: correlation id i32, and in v1 a tagged field section. A flexible
/// API answers with v1, except ApiVersions: it answers with v0 at every
/// version, so a client can read the error of a version the broker lacks.
pub fn response_header(frame: &[u8], api: Api) -> Result<(i32, &[u8]), Fatal> {
    let mut cursor = Cursor::new(frame);
    let correlation = cursor.i32()?;
    if api.flexible() && api != Api::ApiVersions {
        cursor.tagged_fields()?;
    }
    Ok((correlation, cursor.rest()))
}
