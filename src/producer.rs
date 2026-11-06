//! The produce job: get a producer id, then send each sealed set as one
//! Produce and turn the replies into acks.
//!
//! On the first failed reply the job closes the connection. On the next
//! connection every unacked set goes out again in seal order, with the same
//! bytes. The broker answers a copy of a batch it has with success, and the
//! rest commit in sequence.

use heapless::Vec as ArrayVec;

use crate::{
    accumulator::{Accumulator, PartSet},
    batch::ProducerId,
    conn::{PartCode, Reply},
    error::{Fatal, RETRIABLE},
    limits::MAX_IN_FLIGHT,
    time::Instant,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Producer {
    /// No producer id. Send InitProducerId when the conn can send.
    NeedId,
    IdInFlight,
    Running {
        id: ProducerId,
        /// Sealed sets out on this connection, oldest first. Each is one
        /// Produce with the set's pending parts.
        sent: u8,
        /// No Produce reply read since the sets were last sent from the
        /// oldest one.
        fresh: bool,
    },
}

impl Producer {
    pub fn new() -> Self {
        Producer::NeedId
    }

    pub fn step<T>(
        &mut self,
        input: ProducerIn<'_, T>,
        out: &mut ProducerOut,
    ) -> Result<(), Fatal> {
        out.clear();
        // Oldest first. A reply takes the next set; the sets fully acked in
        // this step stay until the caller pops them after the step.
        let mut sets = input.accumulator.sets();

        for reply in input.replies {
            match (&mut *self, reply) {
                (Producer::IdInFlight, Reply::InitProducerId { code: 0, id }) => {
                    *self = Producer::Running {
                        id: *id,
                        sent: 0,
                        fresh: true,
                    };
                }
                (Producer::IdInFlight, Reply::InitProducerId { code, .. }) => {
                    let coordinator = [
                        COORDINATOR_LOAD_IN_PROGRESS,
                        COORDINATOR_NOT_AVAILABLE,
                        NOT_COORDINATOR,
                    ];
                    if !RETRIABLE.contains(code) && !coordinator.contains(code) {
                        return Err(Fatal::Produce { code: *code });
                    }
                    *self = Producer::NeedId;
                    out.close = true;
                    break;
                }
                (Producer::Running { sent, fresh, .. }, Reply::Produce { codes }) => {
                    // The caller passes only replies to this job's requests.
                    assert!(*sent > 0, "a Produce reply with no set out");
                    let set = sets
                        .next()
                        .expect("a set out is a sealed set not yet acked");
                    let (number, pending) = (set.number(), set.pending());
                    let mut answered = PartSet::default();
                    codes.iter().for_each(|code| answered.insert(code.part));
                    if answered != pending {
                        return Err(Fatal::Protocol);
                    }
                    let retriable = codes.iter().any(|code| RETRIABLE.contains(&code.code));
                    let mut acked = PartSet::default();
                    for &PartCode { part, code } in codes {
                        match code {
                            0 | DUPLICATE_SEQUENCE_NUMBER => acked.insert(part),
                            OUT_OF_ORDER_SEQUENCE_NUMBER if *fresh && !retriable => {
                                // The resend starts at the oldest unacked set,
                                // so the broker lost a batch it committed.
                                return Err(Fatal::Produce { code });
                            }
                            OUT_OF_ORDER_SEQUENCE_NUMBER => {}
                            code if RETRIABLE.contains(&code) => {}
                            code => return Err(Fatal::Produce { code }),
                        }
                    }
                    *fresh = false;
                    if !acked.is_empty() {
                        let _ = out.acked.push((number, acked));
                    }
                    if acked != pending {
                        out.close = true;
                        *sent = 0;
                        *fresh = true;
                        break;
                    }
                    *sent -= 1;
                }
                (state, reply) => unreachable!("reply {reply:?} in state {state:?}"),
            }
        }

        if input.lost && !out.close {
            match self {
                Producer::IdInFlight => *self = Producer::NeedId,
                Producer::Running { sent, fresh, .. } => {
                    *sent = 0;
                    *fresh = true;
                }
                Producer::NeedId => {}
            }
        }
        if out.close {
            return Ok(());
        }

        match self {
            Producer::NeedId if input.room > 0 => {
                out.init = true;
                *self = Producer::IdInFlight;
            }
            Producer::Running { sent, .. } => {
                for set in sets.skip(usize::from(*sent)).take(input.room) {
                    out.produce
                        .push(set.number())
                        .expect("the accumulator holds at most MAX_IN_FLIGHT sets");
                    *sent += 1;
                }
            }
            Producer::NeedId | Producer::IdInFlight => {}
        }
        Ok(())
    }

    /// The id, kept for the life of the process once the broker gave it.
    pub fn id(&self) -> Option<ProducerId> {
        match *self {
            Producer::Running { id, .. } => Some(id),
            Producer::NeedId | Producer::IdInFlight => None,
        }
    }
}

impl Default for Producer {
    fn default() -> Self {
        Producer::new()
    }
}

pub struct ProducerIn<'a, T> {
    pub now: Instant,
    /// `Conn::room()`: requests the conn takes now.
    pub room: usize,
    pub replies: &'a [Reply],
    pub lost: bool,
    pub accumulator: &'a Accumulator<T>,
}

#[derive(Default)]
pub struct ProducerOut {
    /// Call `Conn::send_init_producer_id`.
    pub init: bool,
    /// Sets to pass to `Conn::send_produce`, in this order, after the acks.
    pub produce: ArrayVec<u32, MAX_IN_FLIGHT>,
    /// Acks for the oldest sets, in order. Apply them before the sends.
    pub acked: ArrayVec<(u32, PartSet), MAX_IN_FLIGHT>,
    /// Close the conn now. Replies after the failed one are not read.
    pub close: bool,
}

impl ProducerOut {
    pub fn clear(&mut self) {
        let Self {
            init,
            produce,
            acked,
            close,
        } = self;
        *init = false;
        produce.clear();
        acked.clear();
        *close = false;
    }
}

/// The broker has this batch already.
const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;

/// The batch does not follow the last sequence the broker has.
const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;

/// InitProducerId codes that a fresh broker sends while its producer id
/// service starts. They name the coordinator, so they apply to
/// InitProducerId only. A Produce part with them stays fatal.
const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;

const COORDINATOR_NOT_AVAILABLE: i16 = 15;

const NOT_COORDINATOR: i16 = 16;
