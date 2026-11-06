//! Fixed bounds that size the crate's arrays. A leaf module: it imports
//! nothing from the crate.

/// Requests in flight on one connection, and sealed sets kept until acked.
/// Kafka keeps the last 5 batches per (producer id, partition) for duplicate
/// checks, so a resend of any of them is still a duplicate.
pub const MAX_IN_FLIGHT: usize = 5;

/// Parts (partitions over all topics) one conn, accumulator or `PartSet`
/// handles.
pub const MAX_PARTS: usize = 64;
