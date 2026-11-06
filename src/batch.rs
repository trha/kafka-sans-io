//! RecordBatch v2, both ways.
//!
//! Writer: records are written in their final form after the header room as
//! they arrive. `seal` then writes the header in front of them. The batch is
//! uncompressed, has base offset 0 and CreateTime timestamps.
//!
//! Reader: `split_batch` takes the next whole batch off a Fetch records blob,
//! and `read_batch` checks it and reads its records in place. Every read error
//! is `Fatal::Protocol`.

use core::mem::{offset_of, size_of};

use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    big_endian::{I16, I32, I64, U32},
};

use crate::{error::Fatal, read::Cursor};

#[derive(Clone, Copy, Debug)]
pub struct Record<'a> {
    pub timestamp_ms: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: &'a [(&'a str, Option<&'a [u8]>)],
}

/// The state of one batch being filled.
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchMeta {
    /// Record bytes after the header room.
    len: u32,
    count: i32,
    /// Timestamp of the first record. Deltas are taken from it.
    base_ts: i64,
    max_ts: i64,
}

impl BatchMeta {
    pub fn count(&self) -> i32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Bytes of the batch once sealed, header included.
    pub fn bytes(&self) -> usize {
        HEADER_BYTES + self.len as usize
    }
}

/// Bytes `record` takes when appended next to `meta`, length field included.
pub fn record_size(meta: &BatchMeta, record: &Record<'_>) -> usize {
    let body = body_size(meta, record);
    varint_size(body as i64) + body
}

/// Writes the record in its final form after the header room. `Full` leaves
/// `buf` and `meta` unchanged.
pub fn append(buf: &mut [u8], meta: &mut BatchMeta, record: &Record<'_>) -> Result<(), Full> {
    let body = body_size(meta, record);
    let start = HEADER_BYTES + meta.len as usize;
    let end = start + varint_size(body as i64) + body;
    if end > buf.len() || meta.count == i32::MAX {
        return Err(Full);
    }
    let base_ts = if meta.is_empty() {
        record.timestamp_ms
    } else {
        meta.base_ts
    };
    let mut writer = Writer {
        buf: &mut buf[start..end],
        at: 0,
    };
    writer.varint(body as i64);
    // Record attributes, unused.
    writer.bytes(&[0]);
    writer.varint(record.timestamp_ms.wrapping_sub(base_ts));
    writer.varint(i64::from(meta.count));
    writer.nullable(record.key);
    writer.nullable(record.value);
    writer.varint(record.headers.len() as i64);
    for (key, value) in record.headers {
        writer.varint(key.len() as i64);
        writer.bytes(key.as_bytes());
        writer.nullable(*value);
    }
    debug_assert_eq!(
        writer.at,
        end - start,
        "record size and record write differ"
    );

    meta.max_ts = if meta.is_empty() {
        record.timestamp_ms
    } else {
        meta.max_ts.max(record.timestamp_ms)
    };
    meta.base_ts = base_ts;
    meta.len += (end - start) as u32;
    meta.count += 1;
    Ok(())
}

/// The record does not fit the free room.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Full;

/// Writes the header: base offset 0, timestamps, producer id and epoch, base
/// sequence, count, CRC32C. Returns the batch bytes.
pub fn seal<'a>(
    buf: &'a mut [u8],
    meta: &BatchMeta,
    id: ProducerId,
    base_sequence: i32,
) -> &'a [u8] {
    let end = meta.bytes();
    let batch = &mut buf[..end];
    let header = BatchHeader {
        base_offset: 0.into(),
        length: ((end - PREFIX_BYTES) as i32).into(),
        // The broker sets it; producers write -1.
        partition_leader_epoch: (-1).into(),
        magic: MAGIC,
        crc: 0.into(),
        // No compression, CreateTime, not transactional, not control.
        attributes: 0.into(),
        last_offset_delta: (meta.count - 1).into(),
        base_timestamp: meta.base_ts.into(),
        max_timestamp: meta.max_ts.into(),
        producer_id: id.id.into(),
        producer_epoch: id.epoch.into(),
        base_sequence: base_sequence.into(),
        count: meta.count.into(),
    };
    batch[..HEADER_BYTES].copy_from_slice(header.as_bytes());
    let crc = crc32c::crc32c(&batch[CRC_FROM..]);
    let (header, _) = BatchHeader::mut_from_prefix(batch).expect("a batch holds its header");
    header.crc = crc.into();
    batch
}

/// The id and epoch the broker gave in InitProducerId.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProducerId {
    pub id: i64,
    pub epoch: i16,
}

/// The sequence `count` records after `sequence`. Kafka sequences wrap from
/// `i32::MAX` to 0.
pub fn next_sequence(sequence: i32, count: i32) -> i32 {
    ((i64::from(sequence) + i64::from(count)) % (i64::from(i32::MAX) + 1)) as i32
}

/// Takes the first whole batch off `records`. `None` when `records` holds
/// less than the batch length says: a Fetch may end with a batch cut short.
pub fn split_batch(records: &[u8]) -> Result<Option<SplitBatch<'_>>, Fatal> {
    // A whole batch holds at least a header, so less is a batch cut short.
    let Ok((header, _)) = BatchHeader::ref_from_prefix(records) else {
        return Ok(None);
    };
    let length = usize::try_from(header.length.get()).map_err(|_| Fatal::Protocol)?;
    let bytes = PREFIX_BYTES + length;
    if bytes < HEADER_BYTES {
        return Err(Fatal::Protocol);
    }
    if records.len() < bytes {
        return Ok(None);
    }
    let (batch, rest) = records.split_at(bytes);
    Ok(Some(SplitBatch { batch, rest }))
}

/// The first whole batch of a records blob, and the bytes after it.
#[derive(Clone, Copy, Debug)]
pub struct SplitBatch<'a> {
    pub batch: &'a [u8],
    pub rest: &'a [u8],
}

/// Checks `batch`, exactly one whole batch: its length, magic 2, the CRC,
/// and no compression.
pub fn read_batch(batch: &[u8]) -> Result<BatchView<'_>, Fatal> {
    let mut cursor = Cursor::new(batch);
    let header = cursor.read::<BatchHeader>()?;
    if usize::try_from(header.length.get()).ok() != Some(batch.len() - PREFIX_BYTES) {
        return Err(Fatal::Protocol);
    }
    if header.magic != MAGIC {
        return Err(Fatal::Protocol);
    }
    if header.crc.get() != crc32c::crc32c(&batch[CRC_FROM..]) {
        return Err(Fatal::Protocol);
    }
    let attributes = header.attributes.get();
    if attributes & COMPRESSION_MASK != 0 {
        return Err(Fatal::Protocol);
    }
    let base_offset = header.base_offset.get();
    let last_offset = base_offset
        .checked_add(i64::from(header.last_offset_delta.get()))
        .ok_or(Fatal::Protocol)?;
    let count = header.count.get();
    Ok(BatchView {
        last_offset,
        control: attributes & CONTROL_BIT != 0,
        count,
        records: Records {
            cursor,
            base_offset,
            base_timestamp: header.base_timestamp.get(),
            remaining: count,
        },
    })
}

/// A checked batch. Its records are read as `records` is iterated.
#[derive(Clone)]
pub struct BatchView<'a> {
    /// Offset of the last record the batch was written with.
    /// Compaction can remove records and keeps this offset.
    pub last_offset: i64,
    pub control: bool,
    pub count: i32,
    pub records: Records<'a>,
}

/// The records of a batch, in batch order. After an error it yields nothing.
#[derive(Clone)]
pub struct Records<'a> {
    cursor: Cursor<'a>,
    base_offset: i64,
    base_timestamp: i64,
    remaining: i32,
}

impl<'a> Iterator for Records<'a> {
    type Item = Result<RecordView<'a>, Fatal>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining <= 0 {
            return None;
        }
        self.remaining -= 1;
        let record = read_record(&mut self.cursor, self.base_offset, self.base_timestamp);
        if record.is_err() {
            self.remaining = 0;
        }
        Some(record)
    }
}

/// One record, read in place.
#[derive(Clone)]
pub struct RecordView<'a> {
    pub offset: i64,
    pub timestamp_ms: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub headers: Headers<'a>,
}

/// The headers of a record, in record order. After an error it yields
/// nothing.
#[derive(Clone)]
pub struct Headers<'a> {
    cursor: Cursor<'a>,
    remaining: i64,
}

impl<'a> Iterator for Headers<'a> {
    type Item = Result<RecordHeader<'a>, Fatal>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining <= 0 {
            return None;
        }
        self.remaining -= 1;
        let header = read_header(&mut self.cursor);
        if header.is_err() {
            self.remaining = 0;
        }
        Some(header)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordHeader<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}

/// The RecordBatch v2 header, as it lies in the batch. Every field is
/// big-endian with alignment 1, so the struct has no padding.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub struct BatchHeader {
    pub base_offset: I64,
    /// Bytes after this field to the end of the batch.
    pub length: I32,
    pub partition_leader_epoch: I32,
    pub magic: u8,
    /// CRC32C from `attributes` to the end of the batch.
    pub crc: U32,
    pub attributes: I16,
    pub last_offset_delta: I32,
    pub base_timestamp: I64,
    pub max_timestamp: I64,
    pub producer_id: I64,
    pub producer_epoch: I16,
    pub base_sequence: I32,
    pub count: I32,
}

/// Bytes of the header, from the base offset to the record count.
pub const HEADER_BYTES: usize = size_of::<BatchHeader>();

const _: () = assert!(HEADER_BYTES == 61, "RecordBatch v2 header is 61 bytes");

/// Bytes up to and including `length`.
pub const PREFIX_BYTES: usize = offset_of!(BatchHeader, partition_leader_epoch);

/// Start of the bytes the CRC covers.
pub const CRC_FROM: usize = offset_of!(BatchHeader, attributes);

pub const MAGIC: u8 = 2;

/// Attribute bits of the compression codec. 0 is none.
pub const COMPRESSION_MASK: i16 = 0x7;

/// Attribute bit of a control batch (a transaction marker).
pub const CONTROL_BIT: i16 = 0x20;

struct Writer<'a> {
    buf: &'a mut [u8],
    at: usize,
}

impl Writer<'_> {
    fn bytes(&mut self, bytes: &[u8]) {
        self.buf[self.at..self.at + bytes.len()].copy_from_slice(bytes);
        self.at += bytes.len();
    }

    /// Zigzag, then 7 bits per byte, low group first.
    fn varint(&mut self, value: i64) {
        let mut rest = zigzag(value);
        while rest >= 0x80 {
            self.bytes(&[(rest as u8) | 0x80]);
            rest >>= 7;
        }
        self.bytes(&[rest as u8]);
    }

    /// A length, -1 for none, then the bytes.
    fn nullable(&mut self, bytes: Option<&[u8]>) {
        match bytes {
            Some(bytes) => {
                self.varint(bytes.len() as i64);
                self.bytes(bytes);
            }
            None => self.varint(-1),
        }
    }
}

/// Record bytes after the length field.
fn body_size(meta: &BatchMeta, record: &Record<'_>) -> usize {
    let base_ts = if meta.is_empty() {
        record.timestamp_ms
    } else {
        meta.base_ts
    };
    let headers: usize = record
        .headers
        .iter()
        .map(|(key, value)| varint_size(key.len() as i64) + key.len() + nullable_size(*value))
        .sum();
    1 + varint_size(record.timestamp_ms.wrapping_sub(base_ts))
        + varint_size(i64::from(meta.count))
        + nullable_size(record.key)
        + nullable_size(record.value)
        + varint_size(record.headers.len() as i64)
        + headers
}

fn nullable_size(bytes: Option<&[u8]>) -> usize {
    match bytes {
        Some(bytes) => varint_size(bytes.len() as i64) + bytes.len(),
        None => varint_size(-1),
    }
}

fn varint_size(value: i64) -> usize {
    let bits = 64 - zigzag(value).leading_zeros() as usize;
    bits.div_ceil(7).max(1)
}

fn zigzag(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)) as u64
}

/// Layout: length varint, attributes i8, timestamp delta varint, offset
/// delta varint, key, value, header count varint, then the headers. A key,
/// a value, and each header key and value is a varint length (-1 for null)
/// and the bytes.
fn read_record<'a>(
    cursor: &mut Cursor<'a>,
    base_offset: i64,
    base_timestamp: i64,
) -> Result<RecordView<'a>, Fatal> {
    let size = usize::try_from(cursor.varint()?).map_err(|_| Fatal::Protocol)?;
    let mut record = Cursor::new(cursor.take(size)?);
    let _attributes = record.read::<u8>()?;
    let timestamp_delta = record.varint()?;
    let offset = base_offset
        .checked_add(record.varint()?)
        .ok_or(Fatal::Protocol)?;
    let key = record.varint_bytes()?;
    let value = record.varint_bytes()?;
    let remaining = record.varint()?;
    Ok(RecordView {
        offset,
        timestamp_ms: base_timestamp.wrapping_add(timestamp_delta),
        key,
        value,
        headers: Headers {
            cursor: record,
            remaining,
        },
    })
}

/// A header key is never null.
fn read_header<'a>(cursor: &mut Cursor<'a>) -> Result<RecordHeader<'a>, Fatal> {
    let key = cursor.varint_bytes()?.ok_or(Fatal::Protocol)?;
    let value = cursor.varint_bytes()?;
    Ok(RecordHeader { key, value })
}
