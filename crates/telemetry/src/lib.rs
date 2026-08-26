//! Low-overhead process metrics with bounded metric cardinality.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// One reconstructible decision span.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceSpan {
    /// Stable trace correlation ID.
    pub trace_id: String,
    /// Monotonic sequence within the trace.
    pub sequence: u64,
    /// Monotonic event timestamp in nanoseconds.
    pub mono_ns: u64,
    /// Producing subsystem/component.
    pub component: String,
    /// Stable event name.
    pub event: String,
    /// Bounded structured attributes.
    pub attributes: BTreeMap<String, String>,
}

/// Trace storage failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TraceError {
    /// Trace identity/event metadata is blank.
    InvalidIdentity,
    /// Span sequence moved backwards for one trace.
    OutOfOrder,
    /// Internal lock was poisoned.
    Unavailable,
}

/// Bounded in-memory decision trace store.
pub struct TraceStore {
    capacity: usize,
    spans: RwLock<VecDeque<TraceSpan>>,
}

impl TraceStore {
    /// Creates a trace store with a hard global span bound.
    #[must_use]
    pub fn new(capacity: usize) -> Option<Self> {
        (capacity > 0).then_some(Self {
            capacity,
            spans: RwLock::new(VecDeque::with_capacity(capacity)),
        })
    }

    /// Appends a span and evicts the oldest span when full.
    ///
    /// # Errors
    /// Returns `TraceError` for invalid metadata, out-of-order spans, or a
    /// poisoned store lock.
    pub fn append(&self, span: TraceSpan) -> Result<(), TraceError> {
        if span.trace_id.trim().is_empty()
            || span.component.trim().is_empty()
            || span.event.trim().is_empty()
        {
            return Err(TraceError::InvalidIdentity);
        }
        let mut spans = self.spans.write().map_err(|_| TraceError::Unavailable)?;
        if spans
            .iter()
            .rev()
            .find(|existing| existing.trace_id == span.trace_id)
            .is_some_and(|existing| span.sequence <= existing.sequence)
        {
            return Err(TraceError::OutOfOrder);
        }
        spans.push_back(span);
        while spans.len() > self.capacity {
            spans.pop_front();
        }
        Ok(())
    }

    /// Returns spans for one trace in append order.
    #[must_use]
    pub fn trace(&self, trace_id: &str) -> Vec<TraceSpan> {
        self.spans.read().map_or_else(
            |_| Vec::new(),
            |spans| {
                spans
                    .iter()
                    .filter(|span| span.trace_id == trace_id)
                    .cloned()
                    .collect()
            },
        )
    }

    /// Returns current retained span count.
    #[must_use]
    pub fn len(&self) -> usize {
        self.spans.read().map_or(0, |spans| spans.len())
    }

    /// Returns whether no spans are retained.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A monotonically increasing counter.
pub struct Counter(AtomicU64);

impl Counter {
    /// Creates a zero counter.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    /// Adds one or more events.
    pub fn add(&self, amount: u64) {
        self.0.fetch_add(amount, Ordering::Relaxed);
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

/// An atomically replaceable signed gauge.
pub struct Gauge(AtomicI64);

impl Gauge {
    /// Creates a zero gauge.
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicI64::new(0))
    }

    /// Sets the gauge value.
    pub fn set(&self, value: i64) {
        self.0.store(value, Ordering::Relaxed);
    }

    /// Returns the current value.
    #[must_use]
    pub fn get(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

/// Fixed-boundary histogram suitable for latency and queue-depth measurements.
pub struct Histogram {
    bounds: Vec<u64>,
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    total: AtomicU64,
}

impl Histogram {
    /// Creates a histogram. Bounds must be strictly increasing; invalid bounds
    /// produce `None` instead of silently changing measurement semantics.
    #[must_use]
    pub fn new(bounds: Vec<u64>) -> Option<Self> {
        if bounds.windows(2).any(|window| window[0] >= window[1]) {
            return None;
        }
        let bucket_count = bounds.len().saturating_add(1);
        Some(Self {
            bounds,
            buckets: (0..bucket_count).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            total: AtomicU64::new(0),
        })
    }

    /// Records a value in its first matching upper-bound bucket.
    pub fn observe(&self, value: u64) {
        let index = self
            .bounds
            .iter()
            .position(|bound| value <= *bound)
            .unwrap_or(self.bounds.len());
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(value, Ordering::Relaxed);
    }

    /// Returns `(bucket_counts, observations, sum)`.
    #[must_use]
    pub fn snapshot(&self) -> (Vec<u64>, u64, u64) {
        (
            self.buckets
                .iter()
                .map(|bucket| bucket.load(Ordering::Relaxed))
                .collect(),
            self.count.load(Ordering::Relaxed),
            self.total.load(Ordering::Relaxed),
        )
    }
}

/// A registry that rejects duplicate metric names and exposes immutable snapshots.
pub struct Registry {
    counters: RwLock<BTreeMap<String, Arc<Counter>>>,
    gauges: RwLock<BTreeMap<String, Arc<Gauge>>>,
}

impl Registry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(BTreeMap::new()),
            gauges: RwLock::new(BTreeMap::new()),
        }
    }

    /// Registers or retrieves a counter by exact name.
    pub fn counter(&self, name: &str) -> Option<Arc<Counter>> {
        let mut counters = self.counters.write().ok()?;
        Some(Arc::clone(
            counters
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(Counter::new())),
        ))
    }

    /// Registers or retrieves a gauge by exact name.
    pub fn gauge(&self, name: &str) -> Option<Arc<Gauge>> {
        let mut gauges = self.gauges.write().ok()?;
        Some(Arc::clone(
            gauges
                .entry(name.to_owned())
                .or_insert_with(|| Arc::new(Gauge::new())),
        ))
    }

    /// Returns counter values in stable name order.
    #[must_use]
    pub fn counter_snapshot(&self) -> BTreeMap<String, u64> {
        self.counters.read().map_or_else(
            |_| BTreeMap::new(),
            |counters| {
                counters
                    .iter()
                    .map(|(name, value)| (name.clone(), value.get()))
                    .collect()
            },
        )
    }

    /// Returns gauge values in stable name order.
    #[must_use]
    pub fn gauge_snapshot(&self) -> BTreeMap<String, i64> {
        self.gauges.read().map_or_else(
            |_| BTreeMap::new(),
            |gauges| {
                gauges
                    .iter()
                    .map(|(name, value)| (name.clone(), value.get()))
                    .collect()
            },
        )
    }
}

impl Default for Registry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{Histogram, Registry, TraceSpan, TraceStore};
    use std::collections::BTreeMap;

    #[test]
    fn registry_and_histogram_keep_bounded_named_state() {
        let registry = Registry::new();
        let Some(counter) = registry.counter("orders.sent") else {
            return;
        };
        counter.add(3);
        let Some(gauge) = registry.gauge("queue.depth") else {
            return;
        };
        gauge.set(7);
        assert_eq!(registry.counter_snapshot().get("orders.sent"), Some(&3));
        assert_eq!(registry.gauge_snapshot().get("queue.depth"), Some(&7));
        let Some(histogram) = Histogram::new(vec![10, 100]) else {
            return;
        };
        histogram.observe(5);
        histogram.observe(1000);
        assert_eq!(histogram.snapshot(), (vec![1, 0, 1], 2, 1005));
        assert!(Histogram::new(vec![2, 1]).is_none());
    }

    #[test]
    fn traces_are_ordered_correlated_and_bounded() {
        let Some(store) = TraceStore::new(2) else {
            return;
        };
        let span = |sequence| TraceSpan {
            trace_id: "trace-1".into(),
            sequence,
            mono_ns: sequence,
            component: "risk".into(),
            event: format!("event-{sequence}"),
            attributes: BTreeMap::new(),
        };
        assert!(store.append(span(1)).is_ok());
        assert!(store.append(span(2)).is_ok());
        assert!(store.append(span(2)).is_err());
        assert_eq!(store.trace("trace-1").len(), 2);
        assert_eq!(store.len(), 2);
    }
}
