//! Time as the caller gives it. The crate never reads a clock.

pub use core::time::Duration;

/// A point on the caller's `CLOCK_MONOTONIC`, in nanoseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Instant(pub u64);

impl Instant {
    /// Saturates at the end of time.
    pub fn plus(self, duration: Duration) -> Self {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        Instant(self.0.saturating_add(nanos))
    }

    /// Zero when `earlier` is later than `self`.
    pub fn since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}
