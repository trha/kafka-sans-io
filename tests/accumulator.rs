//! Accumulator: append, seal, and ack against a list model. The producer
//! trace test covers it again through the producer.

use std::collections::{BTreeMap, BTreeSet};

use kafka_sans_io::{
    accumulator::{Accumulator, PartSet},
    batch::{HEADER_BYTES, ProducerId},
    testing::record::{OwnedRecord, decode, plain},
};
use proptest::{prelude::*, sample::Index};

const PARTS: usize = 3;
const ID: ProducerId = ProducerId { id: 7, epoch: 1 };

#[derive(Clone, Debug)]
enum Action {
    Append {
        part: Index,
        records: Vec<OwnedRecord>,
    },
    Seal,
    /// Acks the oldest set for the pending parts in this mask.
    Ack {
        mask: u64,
    },
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        3 => (any::<Index>(), proptest::collection::vec(plain(0i64..1_000, 40), 1..=3))
            .prop_map(|(part, records)| Action::Append { part, records }),
        2 => Just(Action::Seal),
        2 => (0u64..1 << PARTS).prop_map(|mask| Action::Ack { mask }),
    ]
}

/// A sealed set as the model sees it.
#[derive(Debug)]
struct ModelSet {
    number: u32,
    token: u32,
    /// Base sequence and records of each part sealed into the set.
    parts: BTreeMap<usize, (i32, Vec<OwnedRecord>)>,
    pending: BTreeSet<usize>,
}

fn filled_bytes(records: &[OwnedRecord]) -> usize {
    let base_ts = records.first().map_or(0, |record| record.timestamp_ms);
    HEADER_BYTES
        + records
            .iter()
            .enumerate()
            .map(|(delta, record)| record.wire_size(base_ts, delta as i64))
            .sum::<usize>()
}

proptest! {
    #[test]
    fn accumulator(
        max_in_flight in 1u8..=5,
        slot_bytes in HEADER_BYTES + 1..1_500,
        actions in proptest::collection::vec(action(), 0..80),
    ) {
        let limit = usize::from(max_in_flight);
        let mut accumulator = Accumulator::<u32>::new(PARTS, max_in_flight, slot_bytes);
        let mut filling: Vec<Vec<OwnedRecord>> = vec![Vec::new(); PARTS];
        let mut next_sequence = [0i32; PARTS];
        let mut sets: Vec<ModelSet> = Vec::new();
        let mut next_token = 100u32;

        for (step, action) in actions.into_iter().enumerate() {
            match action {
                Action::Append { part, records } => {
                    let part = part.index(PARTS);
                    for record in records {
                        let mut after = filling[part].clone();
                        after.push(record.clone());
                        let fits = filled_bytes(&after) <= slot_bytes;
                        let accepted = record.with_borrowed(|record| accumulator.append(part, record)).is_ok();
                        prop_assert_eq!(accepted, fits, "full rule at step {}", step);
                        if accepted {
                            filling[part] = after;
                        }
                    }
                }
                Action::Seal => {
                    let expected = sets.len() < limit && filling.iter().any(|records| !records.is_empty());
                    let token = next_token;
                    next_token += 1;
                    let sealed = accumulator.seal(ID, token);
                    prop_assert_eq!(sealed.is_some(), expected, "seal rule at step {}", step);
                    if let Some(number) = sealed {
                        let mut parts = BTreeMap::new();
                        for (part, records) in filling.iter_mut().enumerate() {
                            if !records.is_empty() {
                                parts.insert(part, (next_sequence[part], std::mem::take(records)));
                                next_sequence[part] += parts[&part].1.len() as i32;
                            }
                        }
                        let pending = parts.keys().copied().collect();
                        sets.push(ModelSet { number, token, parts, pending });
                    }
                }
                Action::Ack { mask } => {
                    if let Some(oldest) = sets.first_mut() {
                        let acked: BTreeSet<usize> =
                            oldest.pending.iter().copied().filter(|&part| mask >> part & 1 == 1).collect();
                        let mut parts = PartSet::default();
                        acked.iter().for_each(|&part| parts.insert(part));
                        oldest.pending.retain(|part| !acked.contains(part));
                        let done = oldest.pending.is_empty();
                        let returned = accumulator.ack(oldest.number, parts);
                        let expected = done.then_some(oldest.token);
                        prop_assert_eq!(returned, expected, "token order at step {}", step);
                        if done {
                            sets.remove(0);
                        }
                    }
                }
            }

            prop_assert!(accumulator.sets().count() <= limit, "bound at step {}", step);

            let listed: Vec<(u32, u32, BTreeSet<usize>)> = accumulator
                .sets()
                .map(|set| (set.number(), *set.token(), set.pending().iter().collect()))
                .collect();
            let modeled: Vec<(u32, u32, BTreeSet<usize>)> =
                sets.iter().map(|set| (set.number, set.token, set.pending.clone())).collect();
            prop_assert_eq!(listed, modeled, "seal order at step {}", step);

            for set in &sets {
                let batches: Vec<(usize, Vec<u8>)> =
                    accumulator.batches(set.number).map(|(part, bytes)| (part, bytes.to_vec())).collect();
                let parts: BTreeSet<usize> = batches.iter().map(|(part, _)| *part).collect();
                prop_assert_eq!(&parts, &set.pending, "batches cover the pending parts at step {}", step);
                for (part, bytes) in batches {
                    let (base_sequence, records) = &set.parts[&part];
                    let decoded = decode(&bytes);
                    prop_assert!(decoded.is_ok(), "sealed bytes decode at step {}: {:?}", step, decoded.as_ref().err());
                    let (_, decoded) = decoded.unwrap();
                    let records_out: Vec<OwnedRecord> = decoded.iter().map(|decoded| decoded.record.clone()).collect();
                    prop_assert_eq!(&records_out, records, "records of set {} part {} at step {}", set.number, part, step);
                    let sequences: Vec<i32> = decoded.iter().map(|decoded| decoded.sequence).collect();
                    let expected: Vec<i32> = (0..records.len() as i32).map(|step| base_sequence + step).collect();
                    prop_assert_eq!(sequences, expected, "contiguous sequences, set {} part {} at step {}", set.number, part, step);
                }
            }
        }
    }
}
