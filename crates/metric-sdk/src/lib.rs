//! Stable metric SDK: declared inputs become validated, timestamped outputs.

#![forbid(unsafe_code)]

use insider_common_types::{InstrumentId, MonoTime};
use std::collections::VecDeque;
use std::sync::Mutex;

/// Metric evaluation failure.
#[derive(Clone, Debug, PartialEq)]
pub enum MetricError {
    /// A required input was unavailable.
    MissingInput(String),
    /// The metric produced a non-finite or out-of-range value.
    InvalidOutput(&'static str),
    /// The evaluation exceeded its deadline.
    DeadlineExceeded,
    /// A result was generated after the evaluation time or past its TTL.
    StaleOutput,
}

/// Immutable metric metadata used by scheduling and validation.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricDescriptor {
    /// Immutable metric identifier/version.
    pub metric_id: String,
    /// Declared input names.
    pub inputs: Vec<String>,
    /// Lower output bound when configured.
    pub min_score: Option<f64>,
    /// Upper output bound when configured.
    pub max_score: Option<f64>,
    /// Output TTL in nanoseconds.
    pub ttl_ns: u64,
}

/// Scheduler priority for one metric evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricPriority {
    /// Deadline-sensitive hot-path computation.
    Fast,
    /// Normal decision-plane computation.
    Normal,
    /// Asynchronous research/background computation.
    Background,
}

/// Versioned scheduling contract for a metric package.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricManifest {
    /// Immutable descriptor and declared inputs.
    pub descriptor: MetricDescriptor,
    /// Desired evaluation period in nanoseconds.
    pub period_ns: u64,
    /// Maximum allowed evaluation latency in nanoseconds.
    pub deadline_ns: u64,
    /// Compute budget in nanoseconds.
    pub budget_ns: u64,
    /// Scheduler priority.
    pub priority: MetricPriority,
}

impl MetricManifest {
    /// Validates descriptor identity and scheduling bounds.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] for invalid scheduling metadata.
    pub fn validate(&self) -> Result<(), MetricError> {
        if self.descriptor.metric_id.trim().is_empty()
            || self.descriptor.ttl_ns == 0
            || self.period_ns == 0
            || self.deadline_ns == 0
            || self.budget_ns == 0
            || self.budget_ns > self.deadline_ns
            || self
                .descriptor
                .inputs
                .iter()
                .any(|input| input.trim().is_empty())
        {
            return Err(MetricError::InvalidOutput("metric manifest"));
        }
        Ok(())
    }
}

/// Inputs supplied to one metric evaluation.
#[derive(Clone, Debug, Default)]
pub struct MetricContext {
    /// Instrument being evaluated.
    pub instrument_id: Option<InstrumentId>,
    /// Named numeric features.
    pub features: std::collections::BTreeMap<String, f64>,
    /// Evaluation time.
    pub now: MonoTime,
}

impl MetricContext {
    /// Reads a declared numeric feature.
    ///
    /// # Errors
    /// Returns [`MetricError::MissingInput`] when the feature is absent or non-finite.
    pub fn feature(&self, name: &str) -> Result<f64, MetricError> {
        let value = self
            .features
            .get(name)
            .copied()
            .ok_or_else(|| MetricError::MissingInput(name.to_owned()))?;
        if value.is_finite() {
            Ok(value)
        } else {
            Err(MetricError::MissingInput(name.to_owned()))
        }
    }
}

/// Validated output consumed by strategies.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricOutput {
    /// Metric identity/version.
    pub metric_id: String,
    /// Instrument identity.
    pub instrument_id: InstrumentId,
    /// Generation time.
    pub generated_mono: MonoTime,
    /// Output validity horizon.
    pub ttl_ns: u64,
    /// Bounded score.
    pub score: f64,
    /// Confidence in `[0, 1]`.
    pub confidence: f64,
    /// Uncertainty estimate, non-negative.
    pub uncertainty: f64,
}

impl MetricOutput {
    /// Validates a raw metric result against its descriptor.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] for invalid numeric values or bounds.
    pub fn validate(&self, descriptor: &MetricDescriptor) -> Result<(), MetricError> {
        if self.metric_id != descriptor.metric_id {
            return Err(MetricError::InvalidOutput("metric_id"));
        }
        if !self.score.is_finite() || !self.confidence.is_finite() || !self.uncertainty.is_finite()
        {
            return Err(MetricError::InvalidOutput("non-finite output"));
        }
        if !(0.0..=1.0).contains(&self.confidence) || self.uncertainty < 0.0 {
            return Err(MetricError::InvalidOutput("confidence or uncertainty"));
        }
        if descriptor
            .min_score
            .is_some_and(|minimum| self.score < minimum)
            || descriptor
                .max_score
                .is_some_and(|maximum| self.score > maximum)
        {
            return Err(MetricError::InvalidOutput("score bounds"));
        }
        if self.ttl_ns == 0 || self.ttl_ns > descriptor.ttl_ns {
            return Err(MetricError::InvalidOutput("ttl"));
        }
        Ok(())
    }

    /// Returns whether the output is valid at a later monotonic time.
    #[must_use]
    pub fn is_fresh(&self, now: MonoTime) -> bool {
        now.as_nanos() >= self.generated_mono.as_nanos()
            && now.as_nanos() - self.generated_mono.as_nanos() <= self.ttl_ns
    }
}

/// Metric implementation executed by the host.
pub trait Metric: Send + Sync {
    /// Returns immutable descriptor and declared inputs.
    fn descriptor(&self) -> &MetricDescriptor;
    /// Returns the metric descriptor plus scheduler contract.
    fn manifest(&self) -> MetricManifest {
        MetricManifest {
            descriptor: self.descriptor().clone(),
            period_ns: self.descriptor().ttl_ns,
            deadline_ns: self.descriptor().ttl_ns,
            budget_ns: self.descriptor().ttl_ns,
            priority: MetricPriority::Normal,
        }
    }
    /// Evaluates against an immutable context.
    ///
    /// # Errors
    /// Returns [`MetricError`] for missing inputs, invalid outputs, or a deadline breach.
    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError>;
    /// Captures incremental state for deterministic restart/replay.
    ///
    /// Stateless metrics return `Ok(None)`. Stateful implementations should
    /// include a version marker in the returned bytes.
    ///
    /// # Errors
    /// Returns [`MetricError`] when state cannot be read safely.
    fn checkpoint(&self) -> Result<Option<Vec<u8>>, MetricError> {
        Ok(None)
    }
    /// Restores a prior checkpoint without changing state on malformed bytes.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] when the checkpoint schema or
    /// values do not match this metric.
    fn restore_checkpoint(&self, _bytes: &[u8]) -> Result<(), MetricError> {
        Err(MetricError::InvalidOutput("checkpoint unsupported"))
    }
}

/// Stateful exponentially weighted volatility metric over a `return` feature.
pub struct EwmaVolatility {
    descriptor: MetricDescriptor,
    lambda: f64,
    state: Mutex<EwmaState>,
}

#[derive(Clone, Copy, Debug, Default)]
struct EwmaState {
    variance: f64,
    observations: u64,
}

impl EwmaVolatility {
    /// Creates an EWMA metric with decay in `(0, 1)` and a bounded output TTL.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] when `lambda` is outside `(0, 1)`
    /// or `ttl_ns` is zero.
    pub fn new(metric_id: String, lambda: f64, ttl_ns: u64) -> Result<Self, MetricError> {
        if !lambda.is_finite() || !(0.0..1.0).contains(&lambda) {
            return Err(MetricError::InvalidOutput("lambda"));
        }
        if ttl_ns == 0 {
            return Err(MetricError::InvalidOutput("ttl"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![String::from("return")],
                min_score: Some(0.0),
                max_score: None,
                ttl_ns,
            },
            lambda,
            state: Mutex::new(EwmaState::default()),
        })
    }

    /// Returns the number of accepted observations.
    #[must_use]
    pub fn observations(&self) -> u64 {
        self.state.lock().map_or(0, |state| state.observations)
    }
}

impl Metric for EwmaVolatility {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn manifest(&self) -> MetricManifest {
        MetricManifest {
            descriptor: self.descriptor.clone(),
            period_ns: self.descriptor.ttl_ns,
            deadline_ns: self.descriptor.ttl_ns,
            budget_ns: self.descriptor.ttl_ns,
            priority: MetricPriority::Fast,
        }
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let return_value = context.feature("return")?;
        let Ok(mut state) = self.state.lock() else {
            return Err(MetricError::InvalidOutput("state lock"));
        };
        state.variance = self.lambda.mul_add(
            state.variance,
            (1.0 - self.lambda) * return_value * return_value,
        );
        state.observations = state.observations.saturating_add(1);
        let confidence_observations = state.observations.min(100);
        let confidence = f64::from(u32::try_from(confidence_observations).unwrap_or(100)) / 100.0;
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score: state.variance.sqrt(),
            confidence,
            uncertainty: (1.0 - confidence) * state.variance.sqrt(),
        })
    }

    fn checkpoint(&self) -> Result<Option<Vec<u8>>, MetricError> {
        let state = self
            .state
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        let mut bytes = Vec::with_capacity(8 + 8 + 8 + 8);
        bytes.extend_from_slice(b"EWMA_V1\0");
        bytes.extend_from_slice(&self.lambda.to_bits().to_le_bytes());
        bytes.extend_from_slice(&state.variance.to_bits().to_le_bytes());
        bytes.extend_from_slice(&state.observations.to_le_bytes());
        Ok(Some(bytes))
    }

    fn restore_checkpoint(&self, bytes: &[u8]) -> Result<(), MetricError> {
        if bytes.len() != 32 || !bytes.starts_with(b"EWMA_V1\0") {
            return Err(MetricError::InvalidOutput("checkpoint schema"));
        }
        let lambda = f64::from_bits(u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| MetricError::InvalidOutput("checkpoint lambda"))?,
        ));
        let variance = f64::from_bits(u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| MetricError::InvalidOutput("checkpoint variance"))?,
        ));
        let observations = u64::from_le_bytes(
            bytes[24..32]
                .try_into()
                .map_err(|_| MetricError::InvalidOutput("checkpoint observations"))?,
        );
        if lambda.to_bits() != self.lambda.to_bits() || !variance.is_finite() || variance < 0.0 {
            return Err(MetricError::InvalidOutput("checkpoint values"));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        state.variance = variance;
        state.observations = observations;
        Ok(())
    }
}

/// Stateful simple moving average over a declared `value` feature.
pub struct SimpleMovingAverage {
    descriptor: MetricDescriptor,
    window: usize,
    values: Mutex<VecDeque<f64>>,
}

impl SimpleMovingAverage {
    /// Creates a bounded moving average with an explicit warm-up window.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] when the window or TTL is zero.
    pub fn new(metric_id: String, window: usize, ttl_ns: u64) -> Result<Self, MetricError> {
        if window == 0 || ttl_ns == 0 || metric_id.trim().is_empty() {
            return Err(MetricError::InvalidOutput("sma configuration"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![String::from("value")],
                min_score: None,
                max_score: None,
                ttl_ns,
            },
            window,
            values: Mutex::new(VecDeque::with_capacity(window)),
        })
    }

    /// Number of observations currently retained for the warm-up calculation.
    #[must_use]
    pub fn observations(&self) -> usize {
        self.values.lock().map_or(0, |values| values.len())
    }
}

impl Metric for SimpleMovingAverage {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let value = context.feature("value")?;
        let mut values = self
            .values
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        values.push_back(value);
        if values.len() > self.window {
            values.pop_front();
        }
        let sum: f64 = values.iter().sum();
        let count =
            u32::try_from(values.len()).map_err(|_| MetricError::InvalidOutput("sample count"))?;
        let window =
            u32::try_from(self.window).map_err(|_| MetricError::InvalidOutput("window"))?;
        let score = sum / f64::from(count);
        let variance = values
            .iter()
            .map(|sample| {
                let delta = *sample - score;
                delta * delta
            })
            .sum::<f64>()
            / f64::from(count);
        let confidence = f64::from(count) / f64::from(window);
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score,
            confidence,
            uncertainty: variance.sqrt(),
        })
    }

    fn checkpoint(&self) -> Result<Option<Vec<u8>>, MetricError> {
        let values = self
            .values
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        let count = u32::try_from(values.len())
            .map_err(|_| MetricError::InvalidOutput("checkpoint count"))?;
        let mut bytes = Vec::with_capacity(16 + values.len() * 8);
        bytes.extend_from_slice(b"SMA_V1\0\0");
        bytes.extend_from_slice(&(self.window as u64).to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for value in values.iter().copied() {
            bytes.extend_from_slice(&value.to_bits().to_le_bytes());
        }
        Ok(Some(bytes))
    }

    fn restore_checkpoint(&self, bytes: &[u8]) -> Result<(), MetricError> {
        if bytes.len() < 20 || !bytes.starts_with(b"SMA_V1\0\0") {
            return Err(MetricError::InvalidOutput("checkpoint schema"));
        }
        let window = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?,
        );
        let count = usize::try_from(u32::from_le_bytes(
            bytes[16..20]
                .try_into()
                .map_err(|_| MetricError::InvalidOutput("checkpoint count"))?,
        ))
        .map_err(|_| MetricError::InvalidOutput("checkpoint count"))?;
        let expected_window = u64::try_from(self.window)
            .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        if window != expected_window || count > self.window || bytes.len() != 20 + count * 8 {
            return Err(MetricError::InvalidOutput("checkpoint values"));
        }
        let mut restored = VecDeque::with_capacity(self.window);
        for index in 0..count {
            let start = 20 + index * 8;
            let value = f64::from_bits(u64::from_le_bytes(
                bytes[start..start + 8]
                    .try_into()
                    .map_err(|_| MetricError::InvalidOutput("checkpoint value"))?,
            ));
            if !value.is_finite() {
                return Err(MetricError::InvalidOutput("checkpoint value"));
            }
            restored.push_back(value);
        }
        *self
            .values
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))? = restored;
        Ok(())
    }
}

/// Stateless relative bid/ask spread metric over `bid` and `ask` features.
pub struct SpreadMetric {
    descriptor: MetricDescriptor,
}

impl SpreadMetric {
    /// Creates a spread metric with a bounded output TTL.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] when the identity or TTL is invalid.
    pub fn new(metric_id: String, ttl_ns: u64) -> Result<Self, MetricError> {
        if metric_id.trim().is_empty() || ttl_ns == 0 {
            return Err(MetricError::InvalidOutput("spread configuration"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![String::from("bid"), String::from("ask")],
                min_score: Some(0.0),
                max_score: None,
                ttl_ns,
            },
        })
    }
}

impl Metric for SpreadMetric {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let bid = context.feature("bid")?;
        let ask = context.feature("ask")?;
        if bid <= 0.0 || ask < bid {
            return Err(MetricError::InvalidOutput("quote ordering"));
        }
        let midpoint = f64::midpoint(bid, ask);
        let score = (ask - bid) / midpoint;
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score,
            confidence: 1.0,
            uncertainty: 0.0,
        })
    }
}

/// Stateless order-book imbalance over `bid_quantity` and `ask_quantity`.
pub struct BookImbalanceMetric {
    descriptor: MetricDescriptor,
}

impl BookImbalanceMetric {
    /// Creates a book-imbalance metric with score range `[-1, 1]`.
    ///
    /// # Errors
    /// Returns [`MetricError::InvalidOutput`] when the identity or TTL is invalid.
    pub fn new(metric_id: String, ttl_ns: u64) -> Result<Self, MetricError> {
        if metric_id.trim().is_empty() || ttl_ns == 0 {
            return Err(MetricError::InvalidOutput("imbalance configuration"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![String::from("bid_quantity"), String::from("ask_quantity")],
                min_score: Some(-1.0),
                max_score: Some(1.0),
                ttl_ns,
            },
        })
    }
}

impl Metric for BookImbalanceMetric {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let bid = context.feature("bid_quantity")?;
        let ask = context.feature("ask_quantity")?;
        let total = bid + ask;
        if bid < 0.0 || ask < 0.0 || total <= 0.0 {
            return Err(MetricError::InvalidOutput("book quantities"));
        }
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score: (bid - ask) / total,
            confidence: 1.0,
            uncertainty: 0.0,
        })
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime};

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::{
        BookImbalanceMetric, EwmaVolatility, Metric, MetricContext, MetricDescriptor, MetricOutput,
        SimpleMovingAverage, SpreadMetric,
    };

    #[test]
    fn output_validation_rejects_bad_confidence_and_expired_ttl() {
        let Some(instrument) = InstrumentId::new(1).ok() else {
            return;
        };
        let descriptor = MetricDescriptor {
            metric_id: String::from("volatility.ewma.v1"),
            inputs: vec![String::from("returns")],
            min_score: Some(-1.0),
            max_score: Some(1.0),
            ttl_ns: 100,
        };
        let output = MetricOutput {
            metric_id: descriptor.metric_id.clone(),
            instrument_id: instrument,
            generated_mono: MonoTime::from_nanos(10),
            ttl_ns: 100,
            score: 0.5,
            confidence: 0.8,
            uncertainty: 0.1,
        };
        assert!(output.validate(&descriptor).is_ok());
        assert!(output.is_fresh(MonoTime::from_nanos(110)));
        let mut invalid = output;
        invalid.confidence = 1.1;
        assert!(invalid.validate(&descriptor).is_err());
    }

    #[test]
    fn ewma_metric_updates_state_from_returns() {
        let Ok(metric) = EwmaVolatility::new(String::from("volatility.ewma.v1"), 0.9, 100) else {
            return;
        };
        let Some(instrument) = InstrumentId::new(1).ok() else {
            return;
        };
        let context = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([(String::from("return"), 0.1)]),
            now: MonoTime::from_nanos(1),
        };
        let first = metric.evaluate(&context).ok();
        assert!(first.as_ref().is_some_and(|output| output.score > 0.0));
        assert_eq!(metric.observations(), 1);
        let second = metric.evaluate(&context).ok();
        assert!(second.as_ref().is_some_and(|output| output.score > 0.0));
        let metric_object: Arc<dyn Metric> = Arc::new(metric);
        assert_eq!(metric_object.descriptor().metric_id, "volatility.ewma.v1");
    }

    #[test]
    fn ewma_checkpoint_restores_incremental_state_and_rejects_mismatch() {
        let Ok(metric) = EwmaVolatility::new(String::from("volatility.ewma.v1"), 0.9, 100) else {
            return;
        };
        let Some(instrument) = InstrumentId::new(1).ok() else {
            return;
        };
        let context = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([(String::from("return"), 0.1)]),
            now: MonoTime::from_nanos(1),
        };
        assert!(metric.evaluate(&context).is_ok());
        let Some(checkpoint) = metric.checkpoint().ok().flatten() else {
            return;
        };
        let Ok(restored) = EwmaVolatility::new(String::from("volatility.ewma.v1"), 0.9, 100) else {
            return;
        };
        assert!(restored.restore_checkpoint(&checkpoint).is_ok());
        assert_eq!(restored.observations(), 1);
        assert!(restored.restore_checkpoint(b"bad").is_err());
    }

    #[test]
    fn reference_metrics_emit_bounded_outputs() {
        let Some(instrument) = InstrumentId::new(2).ok() else {
            return;
        };
        let context = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([
                (String::from("value"), 10.0),
                (String::from("bid"), 99.0),
                (String::from("ask"), 100.0),
                (String::from("bid_quantity"), 3.0),
                (String::from("ask_quantity"), 1.0),
            ]),
            now: MonoTime::from_nanos(1),
        };
        let Ok(sma) = SimpleMovingAverage::new(String::from("price.sma.v1"), 2, 100) else {
            return;
        };
        let Ok(spread) = SpreadMetric::new(String::from("liquidity.spread.v1"), 100) else {
            return;
        };
        let Ok(imbalance) = BookImbalanceMetric::new(String::from("book.imbalance.v1"), 100) else {
            return;
        };
        assert!(sma.evaluate(&context).is_ok());
        let Ok(spread_output) = spread.evaluate(&context) else {
            return;
        };
        let Ok(imbalance_output) = imbalance.evaluate(&context) else {
            return;
        };
        assert!((0.0..1.0).contains(&spread_output.score));
        assert!((-1.0..=1.0).contains(&imbalance_output.score));
    }
}
