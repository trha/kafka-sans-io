//! Kafka producer, recovery, and consumer jobs as sans-IO state machines.
//! See `plans/kafka-sans-io.md` and `plans/consumer.md`.

pub mod accumulator;
pub mod batch;
pub mod conn;
pub mod consumer;
pub mod error;
pub mod frame;
pub mod limits;
pub mod producer;
mod read;
pub mod recovery;
pub mod time;
pub mod wire;

#[cfg(feature = "testing")]
pub mod testing;
