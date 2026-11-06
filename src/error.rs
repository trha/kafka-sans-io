//! Broker error codes the machines retry, and the reasons they stop. Each
//! stop reason needs a person to look.

use core::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fatal {
    /// The broker does not support the pinned version of API key `api`.
    Version { api: i16 },
    /// A topic is missing, has another partition count, or a partition has
    /// another leader than the connected broker.
    Metadata,
    /// A frame does not fit the receive buffer, does not decode, or does not
    /// match the request at the head of the queue.
    Protocol,
    /// A Produce error code outside the retriable list, or
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER` on the first reply after a resend.
    Produce { code: i16 },
    /// A ListOffsets or Fetch reply the recovery cannot read: an error code
    /// outside the retriable list, a compressed or corrupt batch, a header
    /// value above `recovery::MAX_HEADER_VALUE`, no whole batch that holds
    /// the last record, a control batch there, or a last record without the
    /// header.
    Fetch,
}

impl fmt::Display for Fatal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fatal::Version { api } => {
                write!(formatter, "broker lacks the pinned version of API {api}")
            }
            Fatal::Metadata => write!(formatter, "topic metadata differs from the config"),
            Fatal::Protocol => write!(formatter, "broker frame breaks the protocol"),
            Fatal::Produce { code } => write!(formatter, "produce failed with error code {code}"),
            Fatal::Fetch => write!(formatter, "fetch reply cannot be read"),
        }
    }
}

impl std::error::Error for Fatal {}

/// Error codes a request may fail with and succeed on a new connection.
pub const RETRIABLE: [i16; 5] = [
    5,  // LEADER_NOT_AVAILABLE
    6,  // NOT_LEADER_OR_FOLLOWER
    7,  // REQUEST_TIMED_OUT
    19, // NOT_ENOUGH_REPLICAS
    20, // NOT_ENOUGH_REPLICAS_AFTER_APPEND
];
