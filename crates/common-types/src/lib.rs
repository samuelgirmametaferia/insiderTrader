//! Dependency-free identities and timestamps shared by all `InsiderTrader` domains.

#![forbid(unsafe_code)]

use core::fmt;
use core::num::ParseIntError;
use core::str::FromStr;

macro_rules! define_id {
    ($name:ident, $prefix:literal) => {
        #[doc = concat!("Stable ", stringify!($name), " value.")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u128);

        impl $name {
            /// Creates an identity from its non-zero canonical value.
            ///
            /// # Errors
            /// Returns [`IdError::Zero`] when `value` is zero.
            #[must_use = "identity construction can fail and its result must be handled"]
            pub const fn new(value: u128) -> Result<Self, IdError> {
                if value == 0 {
                    Err(IdError::Zero)
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the canonical numeric representation.
            #[must_use]
            pub const fn get(self) -> u128 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, concat!($prefix, "_{:032x}"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let numeric = value
                    .strip_prefix(concat!($prefix, "_"))
                    .ok_or(IdError::Prefix)?;
                if numeric.len() != 32 {
                    return Err(IdError::Length);
                }
                Self::new(u128::from_str_radix(numeric, 16).map_err(IdError::InvalidHex)?)
            }
        }
    };
}

/// Failure while constructing or parsing an identity.
#[derive(Debug)]
pub enum IdError {
    /// Zero is reserved as an invalid/uninitialized value.
    Zero,
    /// The domain prefix did not match the identity type.
    Prefix,
    /// The hexadecimal body was not exactly 32 characters.
    Length,
    /// The hexadecimal body was malformed.
    InvalidHex(ParseIntError),
}

impl fmt::Display for IdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("identity value must be non-zero"),
            Self::Prefix => formatter.write_str("identity prefix does not match its type"),
            Self::Length => formatter.write_str("identity body must contain 32 hexadecimal digits"),
            Self::InvalidHex(error) => write!(formatter, "identity contains invalid hex: {error}"),
        }
    }
}

impl core::error::Error for IdError {}

define_id!(TraceId, "trace");
define_id!(EventId, "event");
define_id!(InstrumentId, "instrument");
define_id!(AccountId, "account");
define_id!(CommandId, "command");
define_id!(ProposalId, "proposal");

/// Monotonic process/replay time in nanoseconds from an injected epoch.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
pub struct MonoTime(u64);

impl MonoTime {
    /// Creates a monotonic timestamp.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Returns elapsed nanoseconds from the injected epoch.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Adds a duration, rejecting overflow.
    #[must_use]
    pub const fn checked_add(self, duration_ns: u64) -> Option<Self> {
        match self.0.checked_add(duration_ns) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// UTC Unix timestamp in nanoseconds. It is not valid for deadline measurement.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WallTime(i64);

impl WallTime {
    /// Creates a wall-clock timestamp from Unix nanoseconds.
    #[must_use]
    pub const fn from_unix_nanos(nanos: i64) -> Self {
        Self(nanos)
    }

    /// Returns Unix nanoseconds.
    #[must_use]
    pub const fn as_unix_nanos(self) -> i64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use core::str::FromStr;

    use super::{IdError, MonoTime, TraceId};

    #[test]
    fn identity_has_domain_prefix_and_round_trips() {
        let id = TraceId::new(0xabc).ok();
        let encoded = id.map(|value| value.to_string());
        assert_eq!(
            encoded.as_deref(),
            Some("trace_00000000000000000000000000000abc")
        );
        assert_eq!(
            encoded
                .as_deref()
                .and_then(|value| TraceId::from_str(value).ok()),
            id
        );
    }

    #[test]
    fn identity_rejects_zero_wrong_prefix_and_wrong_length() {
        assert!(matches!(TraceId::new(0), Err(IdError::Zero)));
        assert!(matches!(
            TraceId::from_str("event_00000000000000000000000000000001"),
            Err(IdError::Prefix)
        ));
        assert!(matches!(
            TraceId::from_str("trace_01"),
            Err(IdError::Length)
        ));
    }

    #[test]
    fn monotonic_addition_is_checked() {
        assert_eq!(
            MonoTime::from_nanos(7)
                .checked_add(5)
                .map(MonoTime::as_nanos),
            Some(12)
        );
        assert_eq!(MonoTime::from_nanos(u64::MAX).checked_add(1), None);
    }
}
