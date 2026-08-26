//! Incremental, immutable feature snapshots keyed by canonical instrument IDs.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};

use insider_common_types::{InstrumentId, MonoTime};

/// Feature-state mutation error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FeatureError {
    /// Feature names cannot be empty.
    EmptyName,
    /// Non-finite values cannot enter deterministic feature state.
    NonFinite,
    /// The configured history bound is invalid.
    InvalidCapacity,
}

/// Online bounded numeric feature window.
#[derive(Clone, Debug, PartialEq)]
pub struct RollingWindow {
    capacity: usize,
    samples: VecDeque<(MonoTime, f64)>,
}

impl RollingWindow {
    /// Creates a window with a fixed sample bound.
    ///
    /// # Errors
    /// Returns [`FeatureError::InvalidCapacity`] when `capacity` is zero.
    pub fn new(capacity: usize) -> Result<Self, FeatureError> {
        if capacity == 0 {
            return Err(FeatureError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            samples: VecDeque::with_capacity(capacity),
        })
    }

    /// Appends a finite, monotonic sample and evicts the oldest sample.
    ///
    /// # Errors
    /// Returns [`FeatureError::NonFinite`] for invalid values. Out-of-order
    /// samples are ignored and return success so replay can remain idempotent.
    pub fn push(&mut self, time: MonoTime, value: f64) -> Result<(), FeatureError> {
        if !value.is_finite() {
            return Err(FeatureError::NonFinite);
        }
        if self.samples.back().is_some_and(|(last, _)| time < *last) {
            return Ok(());
        }
        self.samples.push_back((time, value));
        while self.samples.len() > self.capacity {
            self.samples.pop_front();
        }
        Ok(())
    }

    /// Number of samples currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no samples are available.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Returns `(mean, population_variance, minimum, maximum)` when populated.
    #[must_use]
    pub fn statistics(&self) -> Option<(f64, f64, f64, f64)> {
        if self.samples.is_empty() {
            return None;
        }
        let count = f64::from(u32::try_from(self.samples.len()).unwrap_or(u32::MAX));
        let mean = self.samples.iter().map(|(_, value)| value).sum::<f64>() / count;
        let variance = self
            .samples
            .iter()
            .map(|(_, value)| {
                let delta = *value - mean;
                delta * delta
            })
            .sum::<f64>()
            / count;
        let min = self
            .samples
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::INFINITY, f64::min);
        let max = self
            .samples
            .iter()
            .map(|(_, value)| *value)
            .fold(f64::NEG_INFINITY, f64::max);
        Some((mean, variance, min, max))
    }
}

/// Immutable feature values for one instrument/time.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Monotonic generation time.
    pub generated_mono: MonoTime,
    /// Named feature values.
    pub values: BTreeMap<String, f64>,
}

impl Snapshot {
    /// Creates a snapshot, rejecting invalid feature names/values.
    ///
    /// # Errors
    /// Returns [`FeatureError`] for empty names or non-finite values.
    pub fn new(
        instrument_id: InstrumentId,
        generated_mono: MonoTime,
        values: BTreeMap<String, f64>,
    ) -> Result<Self, FeatureError> {
        if values.keys().any(|name| name.trim().is_empty()) {
            return Err(FeatureError::EmptyName);
        }
        if values.values().any(|value| !value.is_finite()) {
            return Err(FeatureError::NonFinite);
        }
        Ok(Self {
            instrument_id,
            generated_mono,
            values,
        })
    }
}

/// Bounded history of feature snapshots for replay and diagnostics.
pub struct Store {
    capacity: usize,
    history: BTreeMap<InstrumentId, VecDeque<Snapshot>>,
}

impl Store {
    /// Creates a bounded feature store.
    ///
    /// # Errors
    /// Returns [`FeatureError::InvalidCapacity`] for zero capacity.
    pub fn new(capacity: usize) -> Result<Self, FeatureError> {
        if capacity == 0 {
            return Err(FeatureError::InvalidCapacity);
        }
        Ok(Self {
            capacity,
            history: BTreeMap::new(),
        })
    }

    /// Publishes a snapshot and evicts only the oldest snapshot for that instrument.
    ///
    /// # Errors
    /// Returns validation errors from the snapshot.
    pub fn publish(&mut self, snapshot: Snapshot) -> Result<(), FeatureError> {
        let queue = self.history.entry(snapshot.instrument_id).or_default();
        if queue
            .back()
            .is_some_and(|last| snapshot.generated_mono < last.generated_mono)
        {
            return Ok(());
        }
        queue.push_back(snapshot);
        while queue.len() > self.capacity {
            queue.pop_front();
        }
        Ok(())
    }

    /// Returns the newest snapshot for an instrument.
    #[must_use]
    pub fn latest(&self, instrument_id: InstrumentId) -> Option<&Snapshot> {
        self.history.get(&instrument_id).and_then(VecDeque::back)
    }

    /// Returns snapshots in generation order for replay.
    #[must_use]
    pub fn history(&self, instrument_id: InstrumentId) -> Vec<&Snapshot> {
        self.history
            .get(&instrument_id)
            .map_or_else(Vec::new, |values| values.iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use insider_common_types::{InstrumentId, MonoTime};

    use super::{FeatureError, RollingWindow, Snapshot, Store};

    fn snapshot(time: u64, value: f64) -> Option<Snapshot> {
        Snapshot::new(
            InstrumentId::new(1).ok()?,
            MonoTime::from_nanos(time),
            BTreeMap::from([(String::from("spread"), value)]),
        )
        .ok()
    }

    #[test]
    fn store_is_bounded_and_ignores_out_of_order_state() {
        let Ok(mut store) = Store::new(2) else {
            return;
        };
        let Some(first) = snapshot(1, 1.0) else {
            return;
        };
        let Some(second) = snapshot(2, 2.0) else {
            return;
        };
        let Some(third) = snapshot(3, 3.0) else {
            return;
        };
        let Some(old) = snapshot(0, 0.0) else {
            return;
        };
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        assert!(store.publish(first).is_ok());
        assert!(store.publish(second).is_ok());
        assert!(store.publish(third).is_ok());
        assert!(store.publish(old).is_ok());
        assert_eq!(store.history(instrument_id).len(), 2);
        assert_eq!(
            store
                .latest(instrument_id)
                .map(|item| item.generated_mono.as_nanos()),
            Some(3)
        );
        assert_eq!(
            Snapshot::new(
                instrument_id,
                MonoTime::from_nanos(1),
                BTreeMap::from([(String::new(), 1.0)])
            ),
            Err(FeatureError::EmptyName)
        );
    }

    #[test]
    fn rolling_window_evicts_and_computes_online_statistics() {
        let Ok(mut window) = RollingWindow::new(3) else {
            return;
        };
        assert!(window.push(MonoTime::from_nanos(1), 1.0).is_ok());
        assert!(window.push(MonoTime::from_nanos(2), 2.0).is_ok());
        assert!(window.push(MonoTime::from_nanos(3), 3.0).is_ok());
        assert!(window.push(MonoTime::from_nanos(4), 4.0).is_ok());
        assert_eq!(window.len(), 3);
        assert_eq!(window.statistics(), Some((3.0, 2.0 / 3.0, 2.0, 4.0)));
        assert!(window.push(MonoTime::from_nanos(2), 100.0).is_ok());
        assert_eq!(window.statistics().map(|stats| stats.2), Some(2.0));
        assert!(window.push(MonoTime::from_nanos(5), f64::NAN).is_err());
    }
}
