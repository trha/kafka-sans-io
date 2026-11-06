//! Shared helpers for the batch and accumulator properties. Sizes here come
//! from the RecordBatch v2 spec, written apart from the crate.

#![allow(dead_code)]

use crate::batch::Record;
use bytes::Bytes;
use kafka_protocol::records::{BatchDecodeInfo, RecordBatchDecoder};
use proptest::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedRecord {
    pub timestamp_ms: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
}

impl OwnedRecord {
    pub fn with_borrowed<R>(&self, body: impl FnOnce(&Record<'_>) -> R) -> R {
        let headers: Vec<(&str, Option<&[u8]>)> = self
            .headers
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_deref()))
            .collect();
        body(&Record {
            timestamp_ms: self.timestamp_ms,
            key: self.key.as_deref(),
            value: self.value.as_deref(),
            headers: &headers,
        })
    }

    /// Bytes on the wire in a batch whose first timestamp is `base_ts`, at
    /// offset delta `offset_delta`, length field included. Spec: length
    /// varint, attributes int8, timestamp delta varlong, offset delta
    /// varint, key, value, header count varint, then each header. Varints
    /// are zigzag LEB128.
    pub fn wire_size(&self, base_ts: i64, offset_delta: i64) -> usize {
        fn varint(value: i64) -> usize {
            let mut rest = ((value << 1) ^ (value >> 63)) as u64;
            let mut size = 1;
            while rest >= 0x80 {
                rest >>= 7;
                size += 1;
            }
            size
        }
        fn nullable(bytes: &Option<Vec<u8>>) -> usize {
            bytes
                .as_ref()
                .map_or(varint(-1), |bytes| varint(bytes.len() as i64) + bytes.len())
        }
        let body = 1
            + varint(self.timestamp_ms - base_ts)
            + varint(offset_delta)
            + nullable(&self.key)
            + nullable(&self.value)
            + varint(self.headers.len() as i64)
            + self
                .headers
                .iter()
                .map(|(key, value)| varint(key.len() as i64) + key.len() + nullable(value))
                .sum::<usize>();
        varint(body as i64) + body
    }
}

/// A decoded record with its numbering.
#[derive(Clone, Debug)]
pub struct Decoded {
    pub offset: i64,
    pub sequence: i32,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub record: OwnedRecord,
}

/// Decodes exactly one batch from `bytes`. Errors on trailing bytes.
pub fn decode(bytes: &[u8]) -> Result<(BatchDecodeInfo, Vec<Decoded>), String> {
    let mut info_buf = Bytes::copy_from_slice(bytes);
    let info =
        RecordBatchDecoder::decode_batch_info(&mut info_buf).map_err(|error| error.to_string())?;
    let mut buf = Bytes::copy_from_slice(bytes);
    let sets = RecordBatchDecoder::decode_all(&mut buf).map_err(|error| error.to_string())?;
    if sets.len() != 1 || info.len() != 1 {
        return Err(format!("{} batches in the bytes", sets.len()));
    }
    let records = sets
        .into_iter()
        .flat_map(|set| set.records)
        .map(|record| Decoded {
            offset: record.offset,
            sequence: record.sequence,
            producer_id: record.producer_id,
            producer_epoch: record.producer_epoch,
            record: OwnedRecord {
                timestamp_ms: record.timestamp,
                key: record.key.map(|key| key.to_vec()),
                value: record.value.map(|value| value.to_vec()),
                headers: record
                    .headers
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.map(|value| value.to_vec())))
                    .collect(),
            },
        })
        .collect();
    Ok((info.into_iter().next().expect("one batch"), records))
}

/// A record with 0..=`max_bytes` key and value bytes and 0-3 headers with
/// distinct keys (the decoder keeps headers in a map). Timestamps come from
/// the caller.
pub fn plain(
    timestamp_ms: impl Strategy<Value = i64>,
    max_bytes: usize,
) -> impl Strategy<Value = OwnedRecord> {
    let bytes = move || proptest::option::of(proptest::collection::vec(any::<u8>(), 0..=max_bytes));
    (
        timestamp_ms,
        bytes(),
        bytes(),
        proptest::collection::btree_map("[a-z-]{0,8}", bytes(), 0..=3),
    )
        .prop_map(|(timestamp_ms, key, value, headers)| OwnedRecord {
            timestamp_ms,
            key,
            value,
            headers: headers.into_iter().collect(),
        })
}
