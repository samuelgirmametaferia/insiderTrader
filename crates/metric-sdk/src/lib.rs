//! Stable metric SDK: declared inputs become validated, timestamped outputs.

#![forbid(unsafe_code)]

use insider_common_types::{InstrumentId, MonoTime};
use std::collections::{BTreeMap, VecDeque};
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

const MAX_STARTER_WINDOW: usize = 4_096;
const DEFAULT_MAX_STARTER_INSTRUMENTS: usize = 4_096;
const MAX_STARTER_STATE_SAMPLES: usize = 262_144;

fn checkpoint_field<const SIZE: usize>(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; SIZE], MetricError> {
    let end = cursor
        .checked_add(SIZE)
        .ok_or(MetricError::InvalidOutput("checkpoint length"))?;
    let field = bytes
        .get(*cursor..end)
        .ok_or(MetricError::InvalidOutput("checkpoint length"))?;
    *cursor = end;
    field
        .try_into()
        .map_err(|_| MetricError::InvalidOutput("checkpoint field"))
}

/// Bounded statistical value returned by a batch reference calculation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MetricEstimate {
    /// Cross-asset normalized metric value.
    pub score: f64,
    /// Warm-up confidence in `[0, 1]`.
    pub confidence: f64,
    /// Non-negative dispersion estimate in the same normalized units.
    pub uncertainty: f64,
}

/// One validated OHLC observation for batch ATR calculations.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OhlcSample {
    /// Highest traded price in the interval.
    pub high: f64,
    /// Lowest traded price in the interval.
    pub low: f64,
    /// Closing price in the interval.
    pub close: f64,
}

fn validate_window(window: usize) -> Result<(), MetricError> {
    if window == 0 || window > MAX_STARTER_WINDOW {
        return Err(MetricError::InvalidOutput("rolling window"));
    }
    Ok(())
}

fn validate_ohlc(sample: OhlcSample) -> Result<(), MetricError> {
    if !sample.high.is_finite()
        || !sample.low.is_finite()
        || !sample.close.is_finite()
        || sample.low <= 0.0
        || sample.low > sample.close
        || sample.close > sample.high
    {
        return Err(MetricError::InvalidOutput("ohlc sample"));
    }
    Ok(())
}

fn standard_deviation(values: &[f64]) -> Result<f64, MetricError> {
    if values.is_empty() {
        return Ok(0.0);
    }
    let count =
        u32::try_from(values.len()).map_err(|_| MetricError::InvalidOutput("sample count"))?;
    let count = f64::from(count);
    let mean = values.iter().sum::<f64>() / count;
    let variance = values
        .iter()
        .map(|value| {
            let difference = value - mean;
            difference * difference
        })
        .sum::<f64>()
        / count;
    if !variance.is_finite() || variance < 0.0 {
        return Err(MetricError::InvalidOutput("sample dispersion"));
    }
    Ok(variance.sqrt())
}

fn exponential_average(values: &[f64], window: usize) -> Result<f64, MetricError> {
    let Some((&first, remaining)) = values.split_first() else {
        return Err(MetricError::MissingInput(String::from("close_price")));
    };
    let window = u32::try_from(window).map_err(|_| MetricError::InvalidOutput("ema window"))?;
    let alpha = 2.0 / (f64::from(window) + 1.0);
    let average = remaining
        .iter()
        .fold(first, |average, value| average + alpha * (value - average));
    if !average.is_finite() || average <= 0.0 {
        return Err(MetricError::InvalidOutput("ema value"));
    }
    Ok(average)
}

/// Computes the batch reference value for a normalized fast/slow EMA trend.
///
/// The calculation consumes at most the most recent `slow_window` prices, so
/// incremental and replay implementations share the same bounded state.
///
/// # Errors
/// Returns [`MetricError`] for an empty/non-finite price series or invalid
/// window relationship.
pub fn normalized_ema_trend_batch(
    closes: &[f64],
    fast_window: usize,
    slow_window: usize,
) -> Result<MetricEstimate, MetricError> {
    validate_window(fast_window)?;
    validate_window(slow_window)?;
    if fast_window >= slow_window {
        return Err(MetricError::InvalidOutput("ema window relationship"));
    }
    if closes.is_empty() {
        return Err(MetricError::MissingInput(String::from("close_price")));
    }
    if closes
        .iter()
        .any(|close| !close.is_finite() || *close <= 0.0)
    {
        return Err(MetricError::InvalidOutput("close price"));
    }
    let retained = &closes[closes.len().saturating_sub(slow_window)..];
    let fast = exponential_average(retained, fast_window)?;
    let slow = exponential_average(retained, slow_window)?;
    let raw_score = (fast - slow) / slow;
    if !raw_score.is_finite() {
        return Err(MetricError::InvalidOutput("ema trend"));
    }
    let returns = retained
        .windows(2)
        .map(|pair| pair[1] / pair[0] - 1.0)
        .collect::<Vec<_>>();
    if returns.iter().any(|value| !value.is_finite()) {
        return Err(MetricError::InvalidOutput("ema returns"));
    }
    let observations =
        u32::try_from(retained.len()).map_err(|_| MetricError::InvalidOutput("sample count"))?;
    let slow_window =
        u32::try_from(slow_window).map_err(|_| MetricError::InvalidOutput("ema window"))?;
    Ok(MetricEstimate {
        score: raw_score.clamp(-1.0, 1.0),
        confidence: f64::from(observations) / f64::from(slow_window),
        uncertainty: standard_deviation(&returns)?,
    })
}

/// Computes the batch reference value for normalized average true range.
///
/// True range is divided by each bar's close, making the result comparable
/// across assets with different price scales. At most `window + 1` trailing
/// bars affect the value; the extra bar supplies the prior close.
///
/// # Errors
/// Returns [`MetricError`] for invalid OHLC data, an empty slice, or an
/// unbounded window.
pub fn normalized_average_true_range_batch(
    bars: &[OhlcSample],
    window: usize,
) -> Result<MetricEstimate, MetricError> {
    validate_window(window)?;
    if bars.is_empty() {
        return Err(MetricError::MissingInput(String::from("ohlc")));
    }
    for sample in bars {
        validate_ohlc(*sample)?;
    }
    let retained = &bars[bars.len().saturating_sub(window.saturating_add(1))..];
    let start = retained.len().saturating_sub(window);
    let mut ranges = Vec::with_capacity(retained.len().saturating_sub(start));
    for index in start..retained.len() {
        let bar = retained[index];
        let previous_close = if index == 0 {
            bar.close
        } else {
            retained[index - 1].close
        };
        let true_range = (bar.high - bar.low)
            .max((bar.high - previous_close).abs())
            .max((bar.low - previous_close).abs());
        let normalized = true_range / bar.close;
        if !normalized.is_finite() || normalized < 0.0 {
            return Err(MetricError::InvalidOutput("normalized true range"));
        }
        ranges.push(normalized);
    }
    let count =
        u32::try_from(ranges.len()).map_err(|_| MetricError::InvalidOutput("sample count"))?;
    let window = u32::try_from(window).map_err(|_| MetricError::InvalidOutput("atr window"))?;
    let score = ranges.iter().sum::<f64>() / f64::from(count);
    if !score.is_finite() {
        return Err(MetricError::InvalidOutput("normalized atr"));
    }
    Ok(MetricEstimate {
        score,
        confidence: f64::from(count) / f64::from(window),
        uncertainty: standard_deviation(&ranges)?,
    })
}

#[allow(clippy::cast_possible_truncation)]
fn bar_index(context: &MetricContext) -> Result<i64, MetricError> {
    let value = context.feature("bar_index")?;
    if value.fract() != 0.0 || value < f64::from(i32::MIN) || value > f64::from(i32::MAX) {
        return Err(MetricError::InvalidOutput("bar index"));
    }
    // The authoritative engine emits an i32 bar ordinal as f64. Range and
    // integrality checks above make the narrowing conversion lossless.
    Ok(i64::from(value as i32))
}

#[derive(Clone, Copy, Debug)]
struct IndexedClose {
    bar_index: i64,
    close: f64,
}

#[derive(Clone, Debug, Default)]
struct EmaInstrumentState {
    closes: VecDeque<IndexedClose>,
}

/// Bounded per-instrument normalized fast/slow EMA trend metric.
///
/// Re-evaluating the latest bar replaces it, which gives live corrections and
/// point-in-time replay the same behavior instead of double-counting a bar.
pub struct NormalizedEmaTrend {
    descriptor: MetricDescriptor,
    fast_window: usize,
    slow_window: usize,
    max_instruments: usize,
    states: Mutex<BTreeMap<InstrumentId, EmaInstrumentState>>,
}

impl NormalizedEmaTrend {
    /// Creates a normalized EMA trend metric with a bounded default universe.
    ///
    /// # Errors
    /// Returns [`MetricError`] when identity, windows, or TTL are invalid.
    pub fn new(
        metric_id: String,
        fast_window: usize,
        slow_window: usize,
        ttl_ns: u64,
    ) -> Result<Self, MetricError> {
        Self::new_with_instrument_capacity(
            metric_id,
            fast_window,
            slow_window,
            ttl_ns,
            DEFAULT_MAX_STARTER_INSTRUMENTS,
        )
    }

    /// Creates the metric with an explicit hard instrument-state capacity.
    ///
    /// # Errors
    /// Returns [`MetricError`] when any bound is zero or invalid.
    pub fn new_with_instrument_capacity(
        metric_id: String,
        fast_window: usize,
        slow_window: usize,
        ttl_ns: u64,
        max_instruments: usize,
    ) -> Result<Self, MetricError> {
        validate_window(fast_window)?;
        validate_window(slow_window)?;
        if metric_id.trim().is_empty()
            || fast_window >= slow_window
            || ttl_ns == 0
            || max_instruments == 0
            || slow_window
                .checked_mul(max_instruments)
                .is_none_or(|samples| samples > MAX_STARTER_STATE_SAMPLES)
        {
            return Err(MetricError::InvalidOutput("ema trend configuration"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![String::from("bar_index"), String::from("close_price")],
                min_score: Some(-1.0),
                max_score: Some(1.0),
                ttl_ns,
            },
            fast_window,
            slow_window,
            max_instruments,
            states: Mutex::new(BTreeMap::new()),
        })
    }

    /// Returns retained observations for one canonical instrument.
    #[must_use]
    pub fn observations(&self, instrument_id: InstrumentId) -> usize {
        self.states.lock().map_or(0, |states| {
            states
                .get(&instrument_id)
                .map_or(0, |state| state.closes.len())
        })
    }
}

impl Metric for NormalizedEmaTrend {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let index = bar_index(context)?;
        let close = context.feature("close_price")?;
        if close <= 0.0 {
            return Err(MetricError::InvalidOutput("close price"));
        }
        let mut states = self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        if !states.contains_key(&instrument_id) && states.len() >= self.max_instruments {
            return Err(MetricError::InvalidOutput("instrument capacity"));
        }
        let state = states.entry(instrument_id).or_default();
        match state.closes.back_mut() {
            Some(last) if index < last.bar_index => {
                return Err(MetricError::InvalidOutput("out-of-order bar"));
            }
            Some(last) if index == last.bar_index => last.close = close,
            _ => state.closes.push_back(IndexedClose {
                bar_index: index,
                close,
            }),
        }
        while state.closes.len() > self.slow_window {
            state.closes.pop_front();
        }
        let closes = state
            .closes
            .iter()
            .map(|sample| sample.close)
            .collect::<Vec<_>>();
        let estimate = normalized_ema_trend_batch(&closes, self.fast_window, self.slow_window)?;
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score: estimate.score,
            confidence: estimate.confidence,
            uncertainty: estimate.uncertainty,
        })
    }

    fn checkpoint(&self) -> Result<Option<Vec<u8>>, MetricError> {
        let states = self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        let fast_window = u32::try_from(self.fast_window)
            .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let slow_window = u32::try_from(self.slow_window)
            .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let instrument_count = u32::try_from(states.len())
            .map_err(|_| MetricError::InvalidOutput("checkpoint instruments"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"EMAT_V1\0");
        bytes.extend_from_slice(&fast_window.to_le_bytes());
        bytes.extend_from_slice(&slow_window.to_le_bytes());
        bytes.extend_from_slice(&instrument_count.to_le_bytes());
        for (instrument_id, state) in states.iter() {
            bytes.extend_from_slice(&instrument_id.get().to_le_bytes());
            let count = u32::try_from(state.closes.len())
                .map_err(|_| MetricError::InvalidOutput("checkpoint samples"))?;
            bytes.extend_from_slice(&count.to_le_bytes());
            for sample in &state.closes {
                bytes.extend_from_slice(&sample.bar_index.to_le_bytes());
                bytes.extend_from_slice(&sample.close.to_bits().to_le_bytes());
            }
        }
        Ok(Some(bytes))
    }

    fn restore_checkpoint(&self, bytes: &[u8]) -> Result<(), MetricError> {
        if !bytes.starts_with(b"EMAT_V1\0") {
            return Err(MetricError::InvalidOutput("checkpoint schema"));
        }
        let mut cursor = 8;
        let fast_window =
            usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let slow_window =
            usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let instrument_count =
            usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint instruments"))?;
        if fast_window != self.fast_window
            || slow_window != self.slow_window
            || instrument_count > self.max_instruments
        {
            return Err(MetricError::InvalidOutput("checkpoint configuration"));
        }
        let mut restored = BTreeMap::new();
        let mut total_samples = 0_usize;
        for _ in 0..instrument_count {
            let raw_id = u128::from_le_bytes(checkpoint_field(bytes, &mut cursor)?);
            let instrument_id = InstrumentId::new(raw_id)
                .map_err(|_| MetricError::InvalidOutput("checkpoint instrument"))?;
            let count = usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint samples"))?;
            total_samples = total_samples
                .checked_add(count)
                .ok_or(MetricError::InvalidOutput("checkpoint samples"))?;
            if count > self.slow_window || total_samples > MAX_STARTER_STATE_SAMPLES {
                return Err(MetricError::InvalidOutput("checkpoint samples"));
            }
            let mut closes = VecDeque::with_capacity(count);
            let mut previous_index = None;
            for _ in 0..count {
                let sample = IndexedClose {
                    bar_index: i64::from_le_bytes(checkpoint_field(bytes, &mut cursor)?),
                    close: f64::from_bits(u64::from_le_bytes(checkpoint_field(
                        bytes,
                        &mut cursor,
                    )?)),
                };
                if !sample.close.is_finite()
                    || sample.close <= 0.0
                    || previous_index.is_some_and(|index| sample.bar_index <= index)
                {
                    return Err(MetricError::InvalidOutput("checkpoint sample"));
                }
                previous_index = Some(sample.bar_index);
                closes.push_back(sample);
            }
            if restored
                .insert(instrument_id, EmaInstrumentState { closes })
                .is_some()
            {
                return Err(MetricError::InvalidOutput("checkpoint instrument"));
            }
        }
        if cursor != bytes.len() {
            return Err(MetricError::InvalidOutput("checkpoint trailing bytes"));
        }
        *self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))? = restored;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct IndexedOhlc {
    bar_index: i64,
    value: OhlcSample,
}

#[derive(Clone, Debug, Default)]
struct AtrInstrumentState {
    bars: VecDeque<IndexedOhlc>,
}

/// Bounded per-instrument normalized average-true-range metric.
pub struct NormalizedAverageTrueRange {
    descriptor: MetricDescriptor,
    window: usize,
    max_instruments: usize,
    states: Mutex<BTreeMap<InstrumentId, AtrInstrumentState>>,
}

impl NormalizedAverageTrueRange {
    /// Creates a normalized ATR metric with a bounded default universe.
    ///
    /// # Errors
    /// Returns [`MetricError`] when identity, window, or TTL are invalid.
    pub fn new(metric_id: String, window: usize, ttl_ns: u64) -> Result<Self, MetricError> {
        Self::new_with_instrument_capacity(
            metric_id,
            window,
            ttl_ns,
            DEFAULT_MAX_STARTER_INSTRUMENTS,
        )
    }

    /// Creates the metric with an explicit hard instrument-state capacity.
    ///
    /// # Errors
    /// Returns [`MetricError`] when any bound is zero or invalid.
    pub fn new_with_instrument_capacity(
        metric_id: String,
        window: usize,
        ttl_ns: u64,
        max_instruments: usize,
    ) -> Result<Self, MetricError> {
        validate_window(window)?;
        if metric_id.trim().is_empty()
            || ttl_ns == 0
            || max_instruments == 0
            || window
                .saturating_add(1)
                .checked_mul(max_instruments)
                .is_none_or(|samples| samples > MAX_STARTER_STATE_SAMPLES)
        {
            return Err(MetricError::InvalidOutput("atr configuration"));
        }
        Ok(Self {
            descriptor: MetricDescriptor {
                metric_id,
                inputs: vec![
                    String::from("bar_index"),
                    String::from("high_price"),
                    String::from("low_price"),
                    String::from("close_price"),
                ],
                min_score: Some(0.0),
                max_score: None,
                ttl_ns,
            },
            window,
            max_instruments,
            states: Mutex::new(BTreeMap::new()),
        })
    }

    /// Returns retained observations for one canonical instrument.
    #[must_use]
    pub fn observations(&self, instrument_id: InstrumentId) -> usize {
        self.states.lock().map_or(0, |states| {
            states
                .get(&instrument_id)
                .map_or(0, |state| state.bars.len())
        })
    }
}

impl Metric for NormalizedAverageTrueRange {
    fn descriptor(&self) -> &MetricDescriptor {
        &self.descriptor
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
        let index = bar_index(context)?;
        let value = OhlcSample {
            high: context.feature("high_price")?,
            low: context.feature("low_price")?,
            close: context.feature("close_price")?,
        };
        validate_ohlc(value)?;
        let mut states = self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        if !states.contains_key(&instrument_id) && states.len() >= self.max_instruments {
            return Err(MetricError::InvalidOutput("instrument capacity"));
        }
        let state = states.entry(instrument_id).or_default();
        match state.bars.back_mut() {
            Some(last) if index < last.bar_index => {
                return Err(MetricError::InvalidOutput("out-of-order bar"));
            }
            Some(last) if index == last.bar_index => last.value = value,
            _ => state.bars.push_back(IndexedOhlc {
                bar_index: index,
                value,
            }),
        }
        while state.bars.len() > self.window.saturating_add(1) {
            state.bars.pop_front();
        }
        let bars = state
            .bars
            .iter()
            .map(|sample| sample.value)
            .collect::<Vec<_>>();
        let estimate = normalized_average_true_range_batch(&bars, self.window)?;
        Ok(MetricOutput {
            metric_id: self.descriptor.metric_id.clone(),
            instrument_id,
            generated_mono: context.now,
            ttl_ns: self.descriptor.ttl_ns,
            score: estimate.score,
            confidence: estimate.confidence,
            uncertainty: estimate.uncertainty,
        })
    }

    fn checkpoint(&self) -> Result<Option<Vec<u8>>, MetricError> {
        let states = self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))?;
        let window = u32::try_from(self.window)
            .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let instrument_count = u32::try_from(states.len())
            .map_err(|_| MetricError::InvalidOutput("checkpoint instruments"))?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"NATR_V1\0");
        bytes.extend_from_slice(&window.to_le_bytes());
        bytes.extend_from_slice(&instrument_count.to_le_bytes());
        for (instrument_id, state) in states.iter() {
            bytes.extend_from_slice(&instrument_id.get().to_le_bytes());
            let count = u32::try_from(state.bars.len())
                .map_err(|_| MetricError::InvalidOutput("checkpoint samples"))?;
            bytes.extend_from_slice(&count.to_le_bytes());
            for sample in &state.bars {
                bytes.extend_from_slice(&sample.bar_index.to_le_bytes());
                bytes.extend_from_slice(&sample.value.high.to_bits().to_le_bytes());
                bytes.extend_from_slice(&sample.value.low.to_bits().to_le_bytes());
                bytes.extend_from_slice(&sample.value.close.to_bits().to_le_bytes());
            }
        }
        Ok(Some(bytes))
    }

    fn restore_checkpoint(&self, bytes: &[u8]) -> Result<(), MetricError> {
        if !bytes.starts_with(b"NATR_V1\0") {
            return Err(MetricError::InvalidOutput("checkpoint schema"));
        }
        let mut cursor = 8;
        let window = usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
            .map_err(|_| MetricError::InvalidOutput("checkpoint window"))?;
        let instrument_count =
            usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint instruments"))?;
        if window != self.window || instrument_count > self.max_instruments {
            return Err(MetricError::InvalidOutput("checkpoint configuration"));
        }
        let mut restored = BTreeMap::new();
        let mut total_samples = 0_usize;
        for _ in 0..instrument_count {
            let raw_id = u128::from_le_bytes(checkpoint_field(bytes, &mut cursor)?);
            let instrument_id = InstrumentId::new(raw_id)
                .map_err(|_| MetricError::InvalidOutput("checkpoint instrument"))?;
            let count = usize::try_from(u32::from_le_bytes(checkpoint_field(bytes, &mut cursor)?))
                .map_err(|_| MetricError::InvalidOutput("checkpoint samples"))?;
            total_samples = total_samples
                .checked_add(count)
                .ok_or(MetricError::InvalidOutput("checkpoint samples"))?;
            if count > self.window.saturating_add(1) || total_samples > MAX_STARTER_STATE_SAMPLES {
                return Err(MetricError::InvalidOutput("checkpoint samples"));
            }
            let mut bars = VecDeque::with_capacity(count);
            let mut previous_index = None;
            for _ in 0..count {
                let bar_index = i64::from_le_bytes(checkpoint_field(bytes, &mut cursor)?);
                let value = OhlcSample {
                    high: f64::from_bits(u64::from_le_bytes(checkpoint_field(bytes, &mut cursor)?)),
                    low: f64::from_bits(u64::from_le_bytes(checkpoint_field(bytes, &mut cursor)?)),
                    close: f64::from_bits(u64::from_le_bytes(checkpoint_field(
                        bytes,
                        &mut cursor,
                    )?)),
                };
                validate_ohlc(value)?;
                if previous_index.is_some_and(|index| bar_index <= index) {
                    return Err(MetricError::InvalidOutput("checkpoint sample"));
                }
                previous_index = Some(bar_index);
                bars.push_back(IndexedOhlc { bar_index, value });
            }
            if restored
                .insert(instrument_id, AtrInstrumentState { bars })
                .is_some()
            {
                return Err(MetricError::InvalidOutput("checkpoint instrument"));
            }
        }
        if cursor != bytes.len() {
            return Err(MetricError::InvalidOutput("checkpoint trailing bytes"));
        }
        *self
            .states
            .lock()
            .map_err(|_| MetricError::InvalidOutput("state lock"))? = restored;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime};

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::{
        BookImbalanceMetric, EwmaVolatility, Metric, MetricContext, MetricDescriptor, MetricOutput,
        NormalizedAverageTrueRange, NormalizedEmaTrend, OhlcSample, SimpleMovingAverage,
        SpreadMetric, normalized_average_true_range_batch, normalized_ema_trend_batch,
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

    #[test]
    fn normalized_ema_incremental_matches_batch_and_replaces_latest_correction() {
        let Some(instrument) = InstrumentId::new(11).ok() else {
            return;
        };
        let Ok(metric) = NormalizedEmaTrend::new(String::from("trend.v1"), 2, 4, 100) else {
            return;
        };
        let mut closes = vec![100.0, 101.0, 102.0, 103.0];
        let mut output = None;
        for (offset, close) in closes.iter().copied().enumerate() {
            let Ok(offset) = u32::try_from(offset) else {
                return;
            };
            let context = MetricContext {
                instrument_id: Some(instrument),
                features: BTreeMap::from([
                    (String::from("bar_index"), f64::from(offset + 1)),
                    (String::from("close_price"), close),
                ]),
                now: MonoTime::from_nanos(u64::from(offset) + 1),
            };
            output = metric.evaluate(&context).ok();
        }
        let Ok(batch) = normalized_ema_trend_batch(&closes, 2, 4) else {
            return;
        };
        assert_eq!(output.as_ref().map(|value| value.score), Some(batch.score));
        assert_eq!(
            output.as_ref().map(|value| value.confidence),
            Some(batch.confidence)
        );
        let corrected = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([
                (String::from("bar_index"), 4.0),
                (String::from("close_price"), 104.0),
            ]),
            now: MonoTime::from_nanos(5),
        };
        let Ok(corrected_output) = metric.evaluate(&corrected) else {
            return;
        };
        closes[3] = 104.0;
        let Ok(corrected_batch) = normalized_ema_trend_batch(&closes, 2, 4) else {
            return;
        };
        assert_eq!(
            corrected_output.score.to_bits(),
            corrected_batch.score.to_bits()
        );
        assert_eq!(metric.observations(instrument), 4);

        let out_of_order = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([
                (String::from("bar_index"), 3.0),
                (String::from("close_price"), 103.0),
            ]),
            now: MonoTime::from_nanos(6),
        };
        assert_eq!(
            metric.evaluate(&out_of_order),
            Err(super::MetricError::InvalidOutput("out-of-order bar"))
        );
    }

    #[test]
    fn normalized_atr_incremental_matches_batch_and_is_instrument_isolated() {
        let Some(first_instrument) = InstrumentId::new(21).ok() else {
            return;
        };
        let Some(second_instrument) = InstrumentId::new(22).ok() else {
            return;
        };
        let Ok(metric) = NormalizedAverageTrueRange::new(String::from("atr.v1"), 3, 100) else {
            return;
        };
        let bars = [
            OhlcSample {
                high: 101.0,
                low: 99.0,
                close: 100.0,
            },
            OhlcSample {
                high: 104.0,
                low: 100.0,
                close: 103.0,
            },
            OhlcSample {
                high: 106.0,
                low: 102.0,
                close: 105.0,
            },
        ];
        let mut output = None;
        for (offset, bar) in bars.iter().copied().enumerate() {
            let Ok(offset) = u32::try_from(offset) else {
                return;
            };
            output = metric
                .evaluate(&MetricContext {
                    instrument_id: Some(first_instrument),
                    features: BTreeMap::from([
                        (String::from("bar_index"), f64::from(offset + 1)),
                        (String::from("high_price"), bar.high),
                        (String::from("low_price"), bar.low),
                        (String::from("close_price"), bar.close),
                    ]),
                    now: MonoTime::from_nanos(u64::from(offset) + 1),
                })
                .ok();
        }
        let Ok(batch) = normalized_average_true_range_batch(&bars, 3) else {
            return;
        };
        assert_eq!(output.as_ref().map(|value| value.score), Some(batch.score));
        assert_eq!(metric.observations(first_instrument), 3);
        assert_eq!(metric.observations(second_instrument), 0);
        let second = MetricContext {
            instrument_id: Some(second_instrument),
            features: BTreeMap::from([
                (String::from("bar_index"), 1.0),
                (String::from("high_price"), 10.1),
                (String::from("low_price"), 9.9),
                (String::from("close_price"), 10.0),
            ]),
            now: MonoTime::from_nanos(4),
        };
        assert!(metric.evaluate(&second).is_ok());
        assert_eq!(metric.observations(first_instrument), 3);
        assert_eq!(metric.observations(second_instrument), 1);
    }

    #[test]
    fn starter_metric_state_capacity_and_freshness_boundary_fail_closed() {
        let Some(first) = InstrumentId::new(31).ok() else {
            return;
        };
        let Some(second) = InstrumentId::new(32).ok() else {
            return;
        };
        let Ok(metric) =
            NormalizedEmaTrend::new_with_instrument_capacity(String::from("trend.v1"), 2, 3, 10, 1)
        else {
            return;
        };
        let context = |instrument_id, index| MetricContext {
            instrument_id: Some(instrument_id),
            features: BTreeMap::from([
                (String::from("bar_index"), index),
                (String::from("close_price"), 100.0),
            ]),
            now: MonoTime::from_nanos(10),
        };
        let Ok(output) = metric.evaluate(&context(first, 1.0)) else {
            return;
        };
        assert!(output.is_fresh(MonoTime::from_nanos(20)));
        assert!(!output.is_fresh(MonoTime::from_nanos(21)));
        assert_eq!(
            metric.evaluate(&context(second, 1.0)),
            Err(super::MetricError::InvalidOutput("instrument capacity"))
        );
    }

    #[test]
    fn starter_metric_checkpoints_restore_identical_subsequent_outputs() {
        let Some(instrument) = InstrumentId::new(41).ok() else {
            return;
        };
        let ema_context = |index: u32, close: f64, now: u64| MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([
                (String::from("bar_index"), f64::from(index)),
                (String::from("close_price"), close),
            ]),
            now: MonoTime::from_nanos(now),
        };
        let Ok(live_ema) = NormalizedEmaTrend::new(String::from("trend.v1"), 2, 4, 100) else {
            return;
        };
        assert!(live_ema.evaluate(&ema_context(1, 100.0, 1)).is_ok());
        assert!(live_ema.evaluate(&ema_context(2, 101.0, 2)).is_ok());
        let Some(ema_checkpoint) = live_ema.checkpoint().ok().flatten() else {
            return;
        };
        let Ok(replay_ema) = NormalizedEmaTrend::new(String::from("trend.v1"), 2, 4, 100) else {
            return;
        };
        assert!(replay_ema.restore_checkpoint(&ema_checkpoint).is_ok());
        assert_eq!(
            live_ema.evaluate(&ema_context(3, 103.0, 3)),
            replay_ema.evaluate(&ema_context(3, 103.0, 3))
        );
        let mut malformed = ema_checkpoint;
        malformed.push(0);
        assert!(replay_ema.restore_checkpoint(&malformed).is_err());
        assert_eq!(replay_ema.observations(instrument), 3);

        let atr_context = |index: u32, high: f64, low: f64, close: f64, now: u64| MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::from([
                (String::from("bar_index"), f64::from(index)),
                (String::from("high_price"), high),
                (String::from("low_price"), low),
                (String::from("close_price"), close),
            ]),
            now: MonoTime::from_nanos(now),
        };
        let Ok(live_atr) = NormalizedAverageTrueRange::new(String::from("atr.v1"), 3, 100) else {
            return;
        };
        assert!(
            live_atr
                .evaluate(&atr_context(1, 101.0, 99.0, 100.0, 1))
                .is_ok()
        );
        assert!(
            live_atr
                .evaluate(&atr_context(2, 104.0, 100.0, 103.0, 2))
                .is_ok()
        );
        let Some(atr_checkpoint) = live_atr.checkpoint().ok().flatten() else {
            return;
        };
        let Ok(replay_atr) = NormalizedAverageTrueRange::new(String::from("atr.v1"), 3, 100) else {
            return;
        };
        assert!(replay_atr.restore_checkpoint(&atr_checkpoint).is_ok());
        assert_eq!(
            live_atr.evaluate(&atr_context(3, 106.0, 102.0, 105.0, 3)),
            replay_atr.evaluate(&atr_context(3, 106.0, 102.0, 105.0, 3))
        );
    }
}
