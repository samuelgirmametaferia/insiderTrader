//! Injected live and simulated clocks.

#![forbid(unsafe_code)]

use core::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use insider_common_types::{MonoTime, WallTime};

/// Source of monotonic deadline time and corresponding observable UTC time.
pub trait Clock: Send + Sync {
    /// Returns monotonic time for deadlines and expiry.
    fn mono_now(&self) -> MonoTime;

    /// Returns wall time for external correlation and display.
    fn wall_now(&self) -> WallTime;
}

/// Error returned when wall time cannot be represented as signed Unix nanoseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SystemClockError;

impl fmt::Display for SystemClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("system wall time is outside the supported Unix-nanosecond range")
    }
}

impl core::error::Error for SystemClockError {}

/// Live clock anchored at construction so monotonic values are process-relative.
pub struct SystemClock {
    mono_epoch: Instant,
    wall_epoch: WallTime,
}

impl SystemClock {
    /// Captures matching monotonic and wall-clock epochs.
    ///
    /// # Errors
    /// Returns an error if system time predates Unix epoch or exceeds `i64` nanoseconds.
    pub fn new() -> Result<Self, SystemClockError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SystemClockError)?;
        let nanos = i64::try_from(duration.as_nanos()).map_err(|_| SystemClockError)?;
        Ok(Self {
            mono_epoch: Instant::now(),
            wall_epoch: WallTime::from_unix_nanos(nanos),
        })
    }
}

impl Clock for SystemClock {
    fn mono_now(&self) -> MonoTime {
        let nanos = u64::try_from(self.mono_epoch.elapsed().as_nanos()).unwrap_or(u64::MAX);
        MonoTime::from_nanos(nanos)
    }

    fn wall_now(&self) -> WallTime {
        let elapsed = i64::try_from(self.mono_epoch.elapsed().as_nanos()).unwrap_or(i64::MAX);
        WallTime::from_unix_nanos(self.wall_epoch.as_unix_nanos().saturating_add(elapsed))
    }
}

/// Thread-safe clock advanced explicitly by replay/tests rather than real time.
pub struct SimClock {
    mono_ns: AtomicU64,
    wall_ns: AtomicI64,
}

impl SimClock {
    /// Creates a simulated clock with independently specified epochs.
    #[must_use]
    pub const fn new(mono: MonoTime, wall: WallTime) -> Self {
        Self {
            mono_ns: AtomicU64::new(mono.as_nanos()),
            wall_ns: AtomicI64::new(wall.as_unix_nanos()),
        }
    }

    /// Advances both clocks by the same duration.
    ///
    /// # Errors
    /// Returns [`AdvanceError`] without changing either value if either would overflow.
    pub fn advance(&self, duration_ns: u64) -> Result<(), AdvanceError> {
        let wall_delta = i64::try_from(duration_ns).map_err(|_| AdvanceError::Overflow)?;
        let mono = self.mono_ns.load(Ordering::SeqCst);
        let wall = self.wall_ns.load(Ordering::SeqCst);
        let next_mono = mono
            .checked_add(duration_ns)
            .ok_or(AdvanceError::Overflow)?;
        let next_wall = wall.checked_add(wall_delta).ok_or(AdvanceError::Overflow)?;
        self.mono_ns.store(next_mono, Ordering::SeqCst);
        self.wall_ns.store(next_wall, Ordering::SeqCst);
        Ok(())
    }
}

impl Clock for SimClock {
    fn mono_now(&self) -> MonoTime {
        MonoTime::from_nanos(self.mono_ns.load(Ordering::SeqCst))
    }

    fn wall_now(&self) -> WallTime {
        WallTime::from_unix_nanos(self.wall_ns.load(Ordering::SeqCst))
    }
}

/// Failure while advancing simulated time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdvanceError {
    /// The requested duration exceeded a timestamp representation.
    Overflow,
}

impl fmt::Display for AdvanceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("simulated clock advance would overflow")
    }
}

impl core::error::Error for AdvanceError {}

#[cfg(test)]
mod tests {
    use insider_common_types::{MonoTime, WallTime};

    use super::{AdvanceError, Clock, SimClock};

    #[test]
    fn simulated_clock_advances_both_domains_deterministically() {
        let clock = SimClock::new(MonoTime::from_nanos(10), WallTime::from_unix_nanos(1_000));
        assert_eq!(clock.advance(25), Ok(()));
        assert_eq!(clock.mono_now().as_nanos(), 35);
        assert_eq!(clock.wall_now().as_unix_nanos(), 1_025);
    }

    #[test]
    fn failed_advance_is_atomic() {
        let clock = SimClock::new(MonoTime::from_nanos(u64::MAX), WallTime::from_unix_nanos(7));
        assert_eq!(clock.advance(1), Err(AdvanceError::Overflow));
        assert_eq!(clock.mono_now().as_nanos(), u64::MAX);
        assert_eq!(clock.wall_now().as_unix_nanos(), 7);
    }
}
