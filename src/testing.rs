//! Helpers shared by the tests in `tests/`: `record` (owned records and a
//! batch decoder), `model` (model broker), `redpanda` (real broker harness
//! and frame proxy), and `driver` (compio socket driver). Feature `testing`.

pub mod driver;
pub mod model;
pub mod record;
pub mod redpanda;
