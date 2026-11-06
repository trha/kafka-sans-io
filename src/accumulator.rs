//! Per-partition batches, sealed together into sets that wait for their acks.
//!
//! The caller appends records. `seal` closes every non-empty batch at one
//! time into one set. The producer sends each set's batches, and sends the
//! same bytes again after a lost reply. `ack` frees the acked parts of the
//! oldest set. Sets finish in seal order.

use heapless::Deque;

use crate::{
    batch::{self, BatchMeta, Full, HEADER_BYTES, ProducerId, Record},
    limits::{MAX_IN_FLIGHT, MAX_PARTS},
};

pub struct Accumulator<T> {
    /// `parts.len() * slots` slots of `slot_bytes`, `slots = max_in_flight + 1`.
    /// Part `i`, slot `k` is at `(slots * i + k) * slot_bytes`.
    arena: Box<[u8]>,
    slot_bytes: usize,
    slots: u8,
    parts: Box<[Part]>,
    /// Sealed sets, oldest first.
    sets: Deque<Set<T>, MAX_IN_FLIGHT>,
    /// Number of the next set to seal. Counts up for the life of the process.
    next_set: u32,
}

impl<T> Accumulator<T> {
    /// `max_in_flight` sets may be sealed at once, 1 to `MAX_IN_FLIGHT`.
    /// `slot_bytes` bounds one batch, header included.
    pub fn new(parts: usize, max_in_flight: u8, slot_bytes: usize) -> Self {
        assert!(
            (1..=MAX_IN_FLIGHT).contains(&usize::from(max_in_flight)),
            "max_in_flight out of range"
        );
        assert!((1..=MAX_PARTS).contains(&parts), "part count out of range");
        assert!(
            slot_bytes > HEADER_BYTES,
            "a slot needs room for a record after the header"
        );
        let slots = max_in_flight + 1;
        Accumulator {
            arena: vec![0; parts * usize::from(slots) * slot_bytes].into_boxed_slice(),
            slot_bytes,
            slots,
            parts: vec![Part::default(); parts].into_boxed_slice(),
            sets: Deque::new(),
            next_set: 0,
        }
    }

    /// Writes the record into the part's filling batch. `Full` leaves the
    /// part unchanged.
    pub fn append(&mut self, part: usize, record: &Record<'_>) -> Result<(), Full> {
        let slot = self.filling(part);
        let range = self.slot_range(part, slot);
        batch::append(
            &mut self.arena[range],
            &mut self.parts[part].meta[slot],
            record,
        )
    }

    /// Free bytes in the part's filling batch. A record of
    /// `batch::record_size` bytes or fewer fits.
    pub fn free(&self, part: usize) -> usize {
        self.slot_bytes - self.parts[part].meta[self.filling(part)].bytes()
    }

    /// Fewer than `max_in_flight` sets are sealed, and a part has records.
    pub fn can_seal(&self) -> bool {
        self.sets.len() < usize::from(self.slots - 1)
            && (0..self.parts.len())
                .any(|part| !self.parts[part].meta[self.filling(part)].is_empty())
    }

    /// Seals every non-empty part into one set that keeps `token`. Returns
    /// the set number, or `None` (and drops `token`) when `can_seal` is false.
    pub fn seal(&mut self, id: ProducerId, token: T) -> Option<u32> {
        if !self.can_seal() {
            return None;
        }
        let number = self.next_set;
        let mut pending = PartSet::default();
        for index in 0..self.parts.len() {
            let slot = self.filling(index);
            let meta = self.parts[index].meta[slot];
            if meta.is_empty() {
                continue;
            }
            let range = self.slot_range(index, slot);
            let part = &mut self.parts[index];
            batch::seal(&mut self.arena[range], &meta, id, part.next_sequence);
            part.next_sequence = batch::next_sequence(part.next_sequence, meta.count());
            part.set[slot] = number;
            part.sealed += 1;
            let next =
                (usize::from(part.head) + usize::from(part.sealed)) % usize::from(self.slots);
            part.meta[next] = BatchMeta::default();
            pending.insert(index);
        }
        if self
            .sets
            .push_back(Set {
                number,
                token,
                pending,
            })
            .is_err()
        {
            unreachable!("can_seal bounds the sets below MAX_IN_FLIGHT");
        }
        self.next_set = self.next_set.wrapping_add(1);
        Some(number)
    }

    /// Frees the acked `parts` of the oldest set. Returns its token when no
    /// part is left pending, and drops the set.
    pub fn ack(&mut self, set: u32, parts: PartSet) -> Option<T> {
        let oldest = self.sets.front_mut().expect("ack with no sealed set");
        assert_eq!(oldest.number, set, "ack for a set other than the oldest");
        assert_eq!(
            parts.0 & !oldest.pending.0,
            0,
            "ack for a part that is not pending"
        );
        for index in parts.iter() {
            let part = &mut self.parts[index];
            debug_assert_eq!(
                part.set[usize::from(part.head)],
                set,
                "oldest sealed slot of another set"
            );
            part.head = (part.head + 1) % self.slots;
            part.sealed -= 1;
            oldest.pending.remove(index);
        }
        if oldest.pending.is_empty() {
            self.sets.pop_front().map(|done| done.token)
        } else {
            None
        }
    }

    /// The sealed sets, oldest first.
    pub fn sets(&self) -> impl Iterator<Item = &Set<T>> {
        self.sets.iter()
    }

    /// The sealed batch bytes of each pending part of `set`, by part.
    pub fn batches(&self, set: u32) -> impl Clone + Iterator<Item = (usize, &[u8])> {
        let pending = self
            .sets
            .iter()
            .find(|sealed| sealed.number == set)
            .map_or(PartSet::default(), |sealed| sealed.pending);
        pending.iter().map(move |index| {
            let part = &self.parts[index];
            let slot = (0..part.sealed)
                .map(|step| (usize::from(part.head) + usize::from(step)) % usize::from(self.slots))
                .find(|&slot| part.set[slot] == set)
                .expect("a pending part has a sealed slot for its set");
            let start = self.slot_range(index, slot).start;
            (index, &self.arena[start..start + part.meta[slot].bytes()])
        })
    }

    fn filling(&self, part: usize) -> usize {
        let part = &self.parts[part];
        (usize::from(part.head) + usize::from(part.sealed)) % usize::from(self.slots)
    }

    fn slot_range(&self, part: usize, slot: usize) -> core::ops::Range<usize> {
        let start = (usize::from(self.slots) * part + slot) * self.slot_bytes;
        start..start + self.slot_bytes
    }
}

/// A set of part numbers, below `MAX_PARTS`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartSet(pub u64);

impl PartSet {
    pub fn contains(self, part: usize) -> bool {
        self.0 >> part & 1 == 1
    }

    pub fn insert(&mut self, part: usize) {
        self.0 |= 1 << part;
    }

    pub fn remove(&mut self, part: usize) {
        self.0 &= !(1 << part);
    }

    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// Parts in increasing order.
    pub fn iter(self) -> impl Clone + Iterator<Item = usize> {
        (0..MAX_PARTS).filter(move |&part| self.contains(part))
    }
}

pub struct Set<T> {
    number: u32,
    token: T,
    /// Parts not yet acked.
    pending: PartSet,
}

impl<T> Set<T> {
    pub fn number(&self) -> u32 {
        self.number
    }

    pub fn token(&self) -> &T {
        &self.token
    }

    /// Parts not yet acked.
    pub fn pending(&self) -> PartSet {
        self.pending
    }
}

/// Slots per part: one per set in flight, plus the filling slot.
pub const MAX_SLOTS: usize = MAX_IN_FLIGHT + 1;

#[derive(Clone, Copy, Default)]
struct Part {
    next_sequence: i32,
    /// Ring of slots: `sealed` sealed slots from `head`, then the filling slot.
    head: u8,
    sealed: u8,
    meta: [BatchMeta; MAX_SLOTS],
    /// Set number of each sealed slot.
    set: [u32; MAX_SLOTS],
}
