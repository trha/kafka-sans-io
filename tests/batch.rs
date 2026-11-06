//! Plan test 1: batch format. Records go through `append` and `seal`, then
//! through `RecordBatchDecoder` and through the crate's own reader. Each
//! reader is checked against the source records, never against the other.

use kafka_sans_io::{
    batch::{self, BatchMeta, COMPRESSION_MASK, CRC_FROM, HEADER_BYTES, ProducerId, RecordHeader},
    error::Fatal,
    testing::record::{OwnedRecord, decode, plain},
};
use proptest::{prelude::*, sample::Index, test_runner::TestCaseError};

/// Timestamps that go up, down, and repeat, from a start near today.
fn records() -> impl Strategy<Value = Vec<OwnedRecord>> {
    let step = prop_oneof![Just(0i64), -5_000i64..5_000, -(1i64 << 40)..(1i64 << 40)];
    (
        1_600_000_000_000i64..1_900_000_000_000,
        proptest::collection::vec(plain(step, 300), 0..=200),
    )
        .prop_map(|(start, mut records)| {
            let mut timestamp_ms = start;
            for record in &mut records {
                timestamp_ms += record.timestamp_ms;
                record.timestamp_ms = timestamp_ms;
            }
            records
        })
}

fn capacity() -> impl Strategy<Value = usize> {
    prop_oneof![
        HEADER_BYTES..HEADER_BYTES + 400,
        HEADER_BYTES..20_000,
        Just(200_000)
    ]
}

/// Every read of a batch: each record, and every header of each.
fn read_all(bytes: &[u8]) -> Result<Vec<OwnedRecord>, Fatal> {
    let view = batch::read_batch(bytes)?;
    view.records
        .map(|record| {
            let record = record?;
            let headers = record
                .headers
                .map(|header| {
                    let RecordHeader { key, value } = header?;
                    Ok((
                        String::from_utf8_lossy(key).into_owned(),
                        value.map(<[u8]>::to_vec),
                    ))
                })
                .collect::<Result<_, _>>()?;
            Ok(OwnedRecord {
                timestamp_ms: record.timestamp_ms,
                key: record.key.map(<[u8]>::to_vec),
                value: record.value.map(<[u8]>::to_vec),
                headers,
            })
        })
        .collect()
}

proptest! {
    #[test]
    fn batch_format(
        records in records(),
        capacity in capacity(),
        id in any::<i64>(),
        epoch in any::<i16>(),
        base_sequence in 0..=i32::MAX,
        flip_at in any::<Index>(),
        flip_mask in 1..=u8::MAX,
        codec in 1..=COMPRESSION_MASK,
        cut_at in any::<Index>(),
    ) {
        let mut buf = vec![0u8; capacity];
        let mut meta = BatchMeta::default();
        // The source: the records the writer took, in order.
        let mut taken: Vec<OwnedRecord> = Vec::new();
        // Batch bytes by the spec, from the records taken so far.
        let mut expected_bytes = HEADER_BYTES;
        let mut full_rule_breaks = Vec::new();
        for record in &records {
            let base_ts = taken.first().unwrap_or(record).timestamp_ms;
            let fits = expected_bytes + record.wire_size(base_ts, taken.len() as i64) <= capacity;
            let accepted = record.with_borrowed(|record| batch::append(&mut buf, &mut meta, record)).is_ok();
            if accepted != fits {
                full_rule_breaks.push((taken.len(), fits, accepted));
            }
            if accepted {
                expected_bytes += record.wire_size(base_ts, taken.len() as i64);
                taken.push(record.clone());
            }
        }
        let identity = ProducerId { id, epoch };
        let sealed = batch::seal(&mut buf, &meta, identity, base_sequence).to_vec();

        prop_assert!(full_rule_breaks.is_empty(), "full rule: (index, fits, accepted) {:?}", full_rule_breaks);
        prop_assert!(sealed.len() <= capacity, "full rule: {} bytes over capacity {}", sealed.len(), capacity);

        // kafka-protocol against the source.
        let decoded = decode(&sealed);
        prop_assert!(decoded.is_ok(), "kafka-protocol: rejects the batch: {:?}", decoded.as_ref().err());
        let (info, decoded) = decoded.unwrap();
        let records_out: Vec<OwnedRecord> = decoded.iter().map(|decoded| decoded.record.clone()).collect();
        same_records("kafka-protocol", &records_out, &taken)?;

        let offsets: Vec<i64> = decoded.iter().map(|decoded| decoded.offset).collect();
        let sequences: Vec<i32> = decoded.iter().map(|decoded| decoded.sequence).collect();
        let expected_offsets: Vec<i64> = (0..taken.len() as i64).collect();
        let expected_sequences: Vec<i32> =
            (0..taken.len() as i32).map(|step| base_sequence.wrapping_add(step)).collect();
        prop_assert_eq!(&offsets, &expected_offsets, "kafka-protocol: offsets");
        prop_assert_eq!(sequences, expected_sequences, "kafka-protocol: sequences");
        prop_assert_eq!(info.base_sequence, base_sequence, "kafka-protocol: base sequence");
        prop_assert_eq!(info.record_count as usize, taken.len(), "kafka-protocol: count");

        let identities: Vec<(i64, i16)> = decoded
            .iter()
            .map(|decoded| (decoded.producer_id, decoded.producer_epoch))
            .chain([(info.producer_id, info.producer_epoch)])
            .collect();
        prop_assert!(identities.iter().all(|&pair| pair == (id, epoch)), "kafka-protocol: identity {:?}", identities);

        // The crate's reader against the source.
        let split = batch::split_batch(&sealed);
        prop_assert!(
            matches!(split, Ok(Some(split)) if split.batch == sealed.as_slice() && split.rest.is_empty()),
            "reader: split gives the whole batch, nothing after it"
        );

        let view = batch::read_batch(&sealed);
        prop_assert!(view.is_ok(), "reader: {:?}", view.as_ref().err());
        let view = view.unwrap();
        prop_assert_eq!(view.count as usize, taken.len(), "reader: count");
        prop_assert!(!view.control, "reader: control bit");
        prop_assert_eq!(view.last_offset, taken.len() as i64 - 1, "reader: last offset");

        let offsets: Result<Vec<i64>, _> =
            view.records.clone().map(|record| record.map(|record| record.offset)).collect();
        prop_assert_eq!(offsets, Ok(expected_offsets), "reader: offsets");

        let read = read_all(&sealed);
        prop_assert!(read.is_ok(), "reader: records {:?}", read.as_ref().err());
        same_records("reader", &read.unwrap(), &taken)?;

        // The reader rejects bad batches. The CRC field ends at byte 21.
        // Every later byte is covered.
        let mut flipped = sealed.clone();
        let at = 21 + flip_at.index(flipped.len() - 21);
        flipped[at] ^= flip_mask;
        prop_assert!(batch::read_batch(&flipped).is_err(), "crc: a flipped byte at {} reads", at);

        // A codec in the attributes, with the CRC made over it.
        let mut compressed = sealed.clone();
        compressed[CRC_FROM + 1] |= codec as u8;
        let crc = crc32c::crc32c(&compressed[CRC_FROM..]);
        compressed[CRC_FROM - 4..CRC_FROM].copy_from_slice(&crc.to_be_bytes());
        prop_assert!(batch::read_batch(&compressed).is_err(), "compression: codec {} reads", codec);

        let cut = &sealed[..cut_at.index(sealed.len())];
        prop_assert!(matches!(batch::split_batch(cut), Ok(None)), "truncated: split of {} bytes", cut.len());
        prop_assert!(read_all(cut).is_err(), "truncated: read of {} bytes", cut.len());
    }
}

/// `got` equals the source `want`, one field at a time so a failure names
/// the field.
fn same_records(
    reader: &str,
    got: &[OwnedRecord],
    want: &[OwnedRecord],
) -> Result<(), TestCaseError> {
    let timestamps = |records: &[OwnedRecord]| {
        records
            .iter()
            .map(|record| record.timestamp_ms)
            .collect::<Vec<_>>()
    };
    let keys = |records: &[OwnedRecord]| {
        records
            .iter()
            .map(|record| record.key.clone())
            .collect::<Vec<_>>()
    };
    let values = |records: &[OwnedRecord]| {
        records
            .iter()
            .map(|record| record.value.clone())
            .collect::<Vec<_>>()
    };
    let headers = |records: &[OwnedRecord]| {
        records
            .iter()
            .map(|record| record.headers.clone())
            .collect::<Vec<_>>()
    };
    prop_assert_eq!(timestamps(got), timestamps(want), "{}: timestamps", reader);
    prop_assert_eq!(keys(got), keys(want), "{}: keys", reader);
    prop_assert_eq!(values(got), values(want), "{}: values", reader);
    prop_assert_eq!(headers(got), headers(want), "{}: headers", reader);
    Ok(())
}
