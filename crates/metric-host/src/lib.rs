//! Runtime host for validated, independently quarantinable metrics.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Duration;

use insider_metric_sdk::{Metric, MetricContext, MetricError, MetricManifest, MetricOutput};
use insider_scheduler::{Priority, Scheduler, SubmitError, Work};

const PYTHON_MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_METRIC_DISCOVERY_DEPTH: usize = 32;
const MAX_DISCOVERED_METRICS: usize = 4_096;
const MAX_MANIFEST_BYTES: u64 = 1_048_576;

#[allow(clippy::needless_pass_by_value)]
fn read_worker_frames(
    mut stdout: ChildStdout,
    sender: SyncSender<Vec<u8>>,
    max_frame_bytes: usize,
) {
    loop {
        let mut header = [0_u8; 4];
        if stdout.read_exact(&mut header).is_err() {
            return;
        }
        let length = usize::try_from(u32::from_le_bytes(header)).unwrap_or(usize::MAX);
        if length == 0 || length > max_frame_bytes {
            return;
        }
        let mut payload = vec![0_u8; length];
        if stdout.read_exact(&mut payload).is_err() || sender.send(payload).is_err() {
            return;
        }
    }
}

/// A Python metric process using the bounded framed worker protocol.
///
/// The process is single-flight: the host serializes requests per worker and
/// still validates every response through the normal `MetricHost` path.
pub struct PythonMetricProcess {
    descriptor: insider_metric_sdk::MetricDescriptor,
    manifest: MetricManifest,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    responses: Mutex<Receiver<Vec<u8>>>,
    max_frame_bytes: usize,
    deadline_ns: u64,
}

impl PythonMetricProcess {
    /// Starts a worker command with an entrypoint and immutable descriptor.
    ///
    /// # Errors
    /// Returns a string diagnostic when the worker cannot be spawned or the
    /// descriptor/manifest is invalid.
    pub fn spawn(
        mut command: Command,
        entrypoint: &str,
        manifest: MetricManifest,
    ) -> Result<Arc<Self>, String> {
        if entrypoint.trim().is_empty() || manifest.validate().is_err() {
            return Err("invalid Python metric worker configuration".into());
        }
        let descriptor = manifest.descriptor.clone();
        let max_frame_bytes = PYTHON_MAX_FRAME_BYTES;
        let deadline_ns = manifest.deadline_ns;
        command
            .arg("--entrypoint")
            .arg(entrypoint)
            .arg("--metric-id")
            .arg(&descriptor.metric_id)
            .arg("--ttl-ns")
            .arg(descriptor.ttl_ns.to_string());
        for input in &descriptor.inputs {
            command.arg("--input").arg(input);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|error| format!("spawn Python metric worker: {error}"))?;
        let Some(stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Python worker stdin unavailable".into());
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Python worker stdout unavailable".into());
        };
        let (response_tx, response_rx) = sync_channel(64);
        std::thread::Builder::new()
            .name(format!("metric-worker-reader-{}", descriptor.metric_id))
            .spawn(move || read_worker_frames(stdout, response_tx, max_frame_bytes))
            .map_err(|error| format!("start Python metric reader: {error}"))?;
        Ok(Arc::new(Self {
            descriptor,
            manifest,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            responses: Mutex::new(response_rx),
            max_frame_bytes,
            deadline_ns,
        }))
    }

    fn request(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        let instrument_id = context
            .instrument_id
            .ok_or(MetricError::MissingInput("instrument_id".into()))?;
        let request = serde_json::json!({
            "instrument_id": instrument_id.get().to_string(),
            "now_mono_ns": context.now.as_nanos(),
            "features": context.features,
        });
        let payload = serde_json::to_vec(&request)
            .map_err(|_| MetricError::InvalidOutput("python request"))?;
        if payload.is_empty() || payload.len() > self.max_frame_bytes {
            return Err(MetricError::InvalidOutput("python request bounds"));
        }
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| MetricError::InvalidOutput("python worker lock"))?;
        stdin
            .write_all(
                &(u32::try_from(payload.len())
                    .map_err(|_| MetricError::InvalidOutput("python request length"))?)
                .to_le_bytes(),
            )
            .and_then(|()| stdin.write_all(&payload))
            .and_then(|()| stdin.flush())
            .map_err(|_| MetricError::InvalidOutput("python worker write"))?;
        drop(stdin);
        let response = self
            .responses
            .lock()
            .map_err(|_| MetricError::InvalidOutput("python worker lock"))?
            .recv_timeout(Duration::from_nanos(self.deadline_ns))
            .map_err(|_| {
                if let Ok(mut child) = self.child.lock() {
                    let _ = child.kill();
                }
                MetricError::DeadlineExceeded
            })?;
        if response.is_empty() || response.len() > self.max_frame_bytes {
            return Err(MetricError::InvalidOutput("python response bounds"));
        }
        let value: serde_json::Value = serde_json::from_slice(&response)
            .map_err(|_| MetricError::InvalidOutput("python response JSON"))?;
        if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(MetricError::InvalidOutput("python metric failure"));
        }
        let metric = value
            .get("metric")
            .ok_or(MetricError::InvalidOutput("python metric missing"))?;
        let output = MetricOutput {
            metric_id: metric
                .get("metric_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(MetricError::InvalidOutput("python metric ID"))?
                .to_owned(),
            instrument_id: insider_common_types::InstrumentId::new(
                metric
                    .get("instrument_id")
                    .and_then(serde_json::Value::as_str)
                    .ok_or(MetricError::InvalidOutput("python instrument ID"))?
                    .parse::<u128>()
                    .map_err(|_| MetricError::InvalidOutput("python instrument ID"))?,
            )
            .map_err(|_| MetricError::InvalidOutput("python instrument ID"))?,
            generated_mono: insider_common_types::MonoTime::from_nanos(
                metric
                    .get("generated_mono_ns")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or(MetricError::InvalidOutput("python generated time"))?,
            ),
            ttl_ns: metric
                .get("ttl_ns")
                .and_then(serde_json::Value::as_u64)
                .ok_or(MetricError::InvalidOutput("python ttl"))?,
            score: metric
                .get("score")
                .and_then(serde_json::Value::as_f64)
                .ok_or(MetricError::InvalidOutput("python score"))?,
            confidence: metric
                .get("confidence")
                .and_then(serde_json::Value::as_f64)
                .ok_or(MetricError::InvalidOutput("python confidence"))?,
            uncertainty: metric
                .get("uncertainty")
                .and_then(serde_json::Value::as_f64)
                .ok_or(MetricError::InvalidOutput("python uncertainty"))?,
        };
        output.validate(&self.descriptor)?;
        Ok(output)
    }
}

impl Drop for PythonMetricProcess {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Metric for PythonMetricProcess {
    fn descriptor(&self) -> &insider_metric_sdk::MetricDescriptor {
        &self.descriptor
    }

    fn manifest(&self) -> MetricManifest {
        self.manifest.clone()
    }

    fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
        self.request(context)
    }
}

/// A validated metric package discovered from disk.
#[derive(Clone, Debug, PartialEq)]
pub struct DiscoveredMetric {
    /// Package manifest path.
    pub manifest_path: PathBuf,
    /// Validated metric manifest.
    pub manifest: MetricManifest,
    /// Implementation language declared by the package.
    pub language: String,
    /// Python module entrypoint when the package is out of process.
    pub entrypoint: Option<String>,
}

/// Metric discovery failure.
#[derive(Debug)]
pub enum DiscoveryError {
    /// Filesystem traversal/read failure.
    Io(std::io::Error),
    /// Manifest parse or validation failure.
    Invalid {
        /// Manifest that failed validation.
        path: PathBuf,
        /// Machine-readable parsing or validation reason.
        reason: String,
    },
    /// A manifest exceeded the bounded parser input size.
    BoundsExceeded {
        /// Filesystem path at which the bound was reached.
        path: PathBuf,
        /// Bound name (`depth`, `count`, or `manifest_bytes`).
        bound: &'static str,
    },
}

impl From<std::io::Error> for DiscoveryError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

/// Finds and validates every `metric.manifest` below `root` in path order.
///
/// # Errors
/// Returns [`DiscoveryError`] for traversal, parsing, or manifest validation
/// failures. No invalid package is returned.
pub fn discover_metric_packages(
    root: impl AsRef<Path>,
) -> Result<Vec<DiscoveredMetric>, DiscoveryError> {
    let mut paths = Vec::new();
    let mut visited = BTreeSet::new();
    collect_metric_manifests(root.as_ref(), 0, &mut paths, &mut visited)?;
    paths.sort();
    let mut ids = BTreeSet::new();
    paths
        .into_iter()
        .map(|path| {
            let text = read_manifest_text(&path)?;
            let (manifest, language, entrypoint) =
                parse_metric_manifest(&text).map_err(|reason| DiscoveryError::Invalid {
                    path: path.clone(),
                    reason,
                })?;
            if !ids.insert(manifest.descriptor.metric_id.clone()) {
                return Err(DiscoveryError::Invalid {
                    path,
                    reason: format!("duplicate metric id: {}", manifest.descriptor.metric_id),
                });
            }
            Ok(DiscoveredMetric {
                manifest_path: path,
                manifest,
                language,
                entrypoint,
            })
        })
        .collect()
}

fn read_manifest_text(path: &Path) -> Result<String, DiscoveryError> {
    let file = std::fs::File::open(path)?;
    let mut reader = file.take(MAX_MANIFEST_BYTES.saturating_add(1));
    let mut text = String::new();
    reader.read_to_string(&mut text)?;
    if text.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(DiscoveryError::BoundsExceeded {
            path: path.to_path_buf(),
            bound: "manifest_bytes",
        });
    }
    Ok(text)
}

fn collect_metric_manifests(
    path: &Path,
    depth: usize,
    output: &mut Vec<PathBuf>,
    visited: &mut BTreeSet<PathBuf>,
) -> Result<(), DiscoveryError> {
    if depth > MAX_METRIC_DISCOVERY_DEPTH {
        return Err(DiscoveryError::BoundsExceeded {
            path: path.to_path_buf(),
            bound: "depth",
        });
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.is_file() {
        if path
            .file_name()
            .is_some_and(|name| name == "metric.manifest")
        {
            if output.len() >= MAX_DISCOVERED_METRICS {
                return Err(DiscoveryError::BoundsExceeded {
                    path: path.to_path_buf(),
                    bound: "count",
                });
            }
            output.push(path.to_path_buf());
        }
        return Ok(());
    }
    if !metadata.is_dir() {
        return Ok(());
    }
    let canonical = path.canonicalize()?;
    if !visited.insert(canonical) {
        return Ok(());
    }
    for entry in std::fs::read_dir(path)? {
        collect_metric_manifests(&entry?.path(), depth + 1, output, visited)?;
    }
    Ok(())
}

fn parse_metric_manifest(text: &str) -> Result<(MetricManifest, String, Option<String>), String> {
    let mut id = None;
    let mut language = String::from("rust");
    let mut entrypoint = None;
    let mut inputs = Vec::new();
    let mut min_score = None;
    let mut max_score = None;
    let mut ttl_ns = None;
    let mut period_ns = None;
    let mut deadline_ns = None;
    let mut budget_ns = None;
    let mut priority = None;
    let mut seen = BTreeSet::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line == "metric:" {
            continue;
        }
        let (key, value) = line
            .split_once(':')
            .or_else(|| line.split_once('='))
            .ok_or_else(|| format!("invalid manifest line: {line}"))?;
        let key = key.trim();
        if !seen.insert(key) {
            return Err(format!("duplicate manifest field: {key}"));
        }
        let value = value.trim().trim_matches('"').trim_matches('\'');
        match key {
            "id" | "metric_id" => id = Some(value.to_owned()),
            "language" => language = value.to_ascii_lowercase(),
            "entrypoint" => entrypoint = Some(value.to_owned()),
            "inputs" => inputs = parse_list(value)?,
            "min_score" => min_score = Some(parse_float(value, key)?),
            "max_score" => max_score = Some(parse_float(value, key)?),
            "ttl_ns" => ttl_ns = Some(parse_positive(value, key)?),
            "period_ns" => period_ns = Some(parse_positive(value, key)?),
            "deadline_ns" => deadline_ns = Some(parse_positive(value, key)?),
            "budget_ns" => budget_ns = Some(parse_positive(value, key)?),
            "priority" => {
                priority = Some(match value.to_ascii_lowercase().as_str() {
                    "fast" => insider_metric_sdk::MetricPriority::Fast,
                    "normal" => insider_metric_sdk::MetricPriority::Normal,
                    "background" => insider_metric_sdk::MetricPriority::Background,
                    _ => return Err("priority must be fast, normal, or background".into()),
                });
            }
            _ => return Err(format!("unknown manifest field: {key}")),
        }
    }
    let manifest = MetricManifest {
        descriptor: insider_metric_sdk::MetricDescriptor {
            metric_id: id.ok_or("missing id")?,
            inputs,
            min_score,
            max_score,
            ttl_ns: ttl_ns.ok_or("missing ttl_ns")?,
        },
        period_ns: period_ns.ok_or("missing period_ns")?,
        deadline_ns: deadline_ns.ok_or("missing deadline_ns")?,
        budget_ns: budget_ns.ok_or("missing budget_ns")?,
        priority: priority.ok_or("missing priority")?,
    };
    manifest
        .validate()
        .map_err(|error| format!("invalid manifest: {error:?}"))?;
    if manifest
        .descriptor
        .min_score
        .zip(manifest.descriptor.max_score)
        .is_some_and(|(minimum, maximum)| minimum > maximum)
    {
        return Err("min_score exceeds max_score".into());
    }
    if !matches!(language.as_str(), "rust" | "python") {
        return Err("language must be rust or python".into());
    }
    if language == "python" && entrypoint.as_deref().is_none_or(str::is_empty) {
        return Err("python metrics require entrypoint".into());
    }
    Ok((manifest, language, entrypoint))
}

fn parse_list(value: &str) -> Result<Vec<String>, String> {
    let value = value.trim().trim_start_matches('[').trim_end_matches(']');
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|item| {
            let item = item.trim().trim_matches('"').trim_matches('\'');
            if item.is_empty() {
                Err("list contains an empty value".into())
            } else {
                Ok(item.to_owned())
            }
        })
        .collect()
}

fn parse_positive(value: &str, field: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{field} must be an integer"))?;
    if parsed == 0 {
        return Err(format!("{field} must be positive"));
    }
    Ok(parsed)
}

fn parse_float(value: &str, field: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("{field} must be numeric"))?;
    if !parsed.is_finite() {
        return Err(format!("{field} must be finite"));
    }
    Ok(parsed)
}

/// One metric evaluation admitted to the shared bounded scheduler.
#[derive(Clone, Debug)]
pub struct ScheduledMetric {
    /// Registered metric identity.
    pub metric_id: String,
    /// Immutable input snapshot captured at admission.
    pub context: MetricContext,
}

/// Failure while admitting scheduled metric work.
#[derive(Debug)]
pub enum ScheduleError {
    /// Metric is absent or quarantined.
    Unavailable(String),
    /// Scheduler queue rejected the work at capacity or due to lock failure.
    QueueFull,
}

/// Registration or lifecycle failure.
#[derive(Clone, Debug, PartialEq)]
pub enum HostError {
    /// A metric ID was already registered.
    Duplicate(String),
    /// The metric manifest is invalid or disagrees with its descriptor.
    InvalidManifest(String),
    /// Metric is absent or has been quarantined.
    Unavailable(String),
    /// Metric evaluation failed or produced invalid output.
    Evaluation(MetricError),
}

/// Lifecycle state for one hosted metric.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    /// Eligible for evaluation.
    Ready,
    /// Isolated after repeated failures.
    Quarantined,
}

/// Artifact promotion lifecycle for a metric package.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    /// Newly discovered research artifact.
    Research,
    /// Validation evidence accepted but not live eligible.
    Validated,
    /// Evaluated for comparison only.
    Shadow,
    /// Bounded live eligibility.
    Canary,
    /// Fully live eligible.
    Production,
    /// Explicitly paused.
    Paused,
    /// Permanently retired.
    Retired,
}

struct Entry {
    metric: Arc<dyn Metric>,
    manifest: MetricManifest,
    state: State,
    lifecycle: Lifecycle,
    lifecycle_evidence_ref: String,
    failures: u32,
}

/// In-process metric host. Python/out-of-process workers can implement the same
/// SDK trait through an adapter without changing coordinator semantics.
pub struct Host {
    failure_limit: u32,
    entries: BTreeMap<String, Entry>,
}

impl Host {
    /// Creates a host with the number of consecutive failures allowed before quarantine.
    #[must_use]
    pub const fn new(failure_limit: u32) -> Self {
        Self {
            failure_limit,
            entries: BTreeMap::new(),
        }
    }

    /// Registers a metric using the descriptor's immutable ID.
    ///
    /// # Errors
    /// Returns [`HostError::Duplicate`] when the ID is already registered.
    pub fn register(&mut self, metric: Arc<dyn Metric>) -> Result<(), HostError> {
        self.register_with_lifecycle(metric, Lifecycle::Production)
    }

    fn register_with_lifecycle(
        &mut self,
        metric: Arc<dyn Metric>,
        lifecycle: Lifecycle,
    ) -> Result<(), HostError> {
        let id = metric.descriptor().metric_id.clone();
        if self.entries.contains_key(&id) {
            return Err(HostError::Duplicate(id));
        }
        let manifest = metric.manifest();
        if manifest.descriptor.metric_id != id {
            return Err(HostError::InvalidManifest(id));
        }
        manifest
            .validate()
            .map_err(|_| HostError::InvalidManifest(id.clone()))?;
        self.entries.insert(
            id,
            Entry {
                metric,
                manifest,
                state: State::Ready,
                lifecycle,
                lifecycle_evidence_ref: String::from("registration"),
                failures: 0,
            },
        );
        Ok(())
    }

    /// Loads one discovered manifest through a caller-owned implementation
    /// factory, then verifies that the implementation's descriptor and full
    /// scheduling manifest exactly match the file before registration.
    ///
    /// The factory is the only extensibility point: package code cannot bypass
    /// host validation or acquire undeclared inputs by registering itself.
    ///
    /// # Errors
    /// Returns [`HostError::InvalidManifest`] for a manifest/implementation
    /// mismatch or the normal registration errors for duplicate IDs.
    pub fn register_discovered<F>(
        &mut self,
        discovered: &DiscoveredMetric,
        factory: F,
    ) -> Result<(), HostError>
    where
        F: FnOnce(&MetricManifest) -> Result<Arc<dyn Metric>, HostError>,
    {
        let metric = factory(&discovered.manifest)?;
        if metric.manifest() != discovered.manifest
            || metric.descriptor().metric_id != discovered.manifest.descriptor.metric_id
        {
            return Err(HostError::InvalidManifest(
                discovered.manifest.descriptor.metric_id.clone(),
            ));
        }
        self.register_with_lifecycle(metric, Lifecycle::Research)
    }

    /// Registers a discovered Python metric through the isolated worker
    /// protocol. The manifest metadata is checked before a process is started.
    ///
    /// # Errors
    /// Returns [`HostError::InvalidManifest`] when the package is not a Python
    /// package or has no entrypoint, and a bounded worker startup failure as a
    /// metric registration error.
    pub fn register_discovered_python(
        &mut self,
        discovered: &DiscoveredMetric,
        command: std::process::Command,
    ) -> Result<(), HostError> {
        if discovered.language != "python" {
            return Err(HostError::InvalidManifest(
                discovered.manifest.descriptor.metric_id.clone(),
            ));
        }
        let entrypoint = discovered.entrypoint.as_deref().ok_or_else(|| {
            HostError::InvalidManifest(discovered.manifest.descriptor.metric_id.clone())
        })?;
        let metric = PythonMetricProcess::spawn(command, entrypoint, discovered.manifest.clone())
            .map_err(HostError::Unavailable)?;
        self.register_with_lifecycle(metric, Lifecycle::Research)
    }

    /// Evaluates one metric and validates its output against its descriptor.
    ///
    /// # Errors
    /// Returns [`HostError`] for unavailable/quarantined metrics, evaluation
    /// failures, or invalid output. Repeated failures quarantine only that metric.
    pub fn evaluate(
        &mut self,
        metric_id: &str,
        context: &MetricContext,
    ) -> Result<MetricOutput, HostError> {
        self.evaluate_internal(metric_id, context, false)
    }

    /// Evaluates a Shadow metric for diagnostics without making it live
    /// eligible. The returned output must not be admitted to a live snapshot.
    ///
    /// # Errors
    /// Returns [`HostError::Unavailable`] unless the metric is in Shadow
    /// lifecycle, or normal evaluation/validation errors.
    pub fn evaluate_shadow(
        &mut self,
        metric_id: &str,
        context: &MetricContext,
    ) -> Result<MetricOutput, HostError> {
        self.evaluate_internal(metric_id, context, true)
    }

    fn evaluate_internal(
        &mut self,
        metric_id: &str,
        context: &MetricContext,
        shadow: bool,
    ) -> Result<MetricOutput, HostError> {
        let Some(entry) = self.entries.get_mut(metric_id) else {
            return Err(HostError::Unavailable(metric_id.to_owned()));
        };
        if entry.state == State::Quarantined {
            return Err(HostError::Unavailable(metric_id.to_owned()));
        }
        if (!shadow && !matches!(entry.lifecycle, Lifecycle::Canary | Lifecycle::Production))
            || (shadow && entry.lifecycle != Lifecycle::Shadow)
        {
            return Err(HostError::Unavailable(metric_id.to_owned()));
        }
        let scoped_features = context
            .features
            .iter()
            .filter(|(name, _)| {
                entry
                    .manifest
                    .descriptor
                    .inputs
                    .iter()
                    .any(|id| id == *name)
            })
            .map(|(name, value)| (name.clone(), *value))
            .collect();
        let scoped_context = MetricContext {
            instrument_id: context.instrument_id,
            features: scoped_features,
            now: context.now,
        };
        let output = match entry.metric.evaluate(&scoped_context) {
            Ok(output) => output,
            Err(error) => {
                entry.failures = entry.failures.saturating_add(1);
                if entry.failures >= self.failure_limit.max(1) {
                    entry.state = State::Quarantined;
                }
                return Err(HostError::Evaluation(error));
            }
        };
        if let Err(error) = output.validate(entry.metric.descriptor()) {
            entry.failures = entry.failures.saturating_add(1);
            if entry.failures >= self.failure_limit.max(1) {
                entry.state = State::Quarantined;
            }
            return Err(HostError::Evaluation(error));
        }
        if !output.is_fresh(context.now) {
            entry.failures = entry.failures.saturating_add(1);
            if entry.failures >= self.failure_limit.max(1) {
                entry.state = State::Quarantined;
            }
            return Err(HostError::Evaluation(MetricError::StaleOutput));
        }
        entry.failures = 0;
        Ok(output)
    }

    /// Admits one metric evaluation using its manifest-derived deadline and
    /// priority. Admission is bounded; no work is silently dropped.
    ///
    /// # Errors
    /// Returns [`ScheduleError`] when the metric is unavailable or the queue
    /// cannot admit another task.
    pub fn schedule(
        &self,
        scheduler: &Scheduler<ScheduledMetric>,
        metric_id: &str,
        context: MetricContext,
    ) -> Result<u64, ScheduleError> {
        let Some(entry) = self.entries.get(metric_id) else {
            return Err(ScheduleError::Unavailable(metric_id.to_owned()));
        };
        if entry.state == State::Quarantined {
            return Err(ScheduleError::Unavailable(metric_id.to_owned()));
        }
        if !matches!(entry.lifecycle, Lifecycle::Canary | Lifecycle::Production) {
            return Err(ScheduleError::Unavailable(metric_id.to_owned()));
        }
        let deadline = context
            .now
            .checked_add(entry.manifest.deadline_ns)
            .ok_or(ScheduleError::QueueFull)?;
        let priority = match entry.manifest.priority {
            insider_metric_sdk::MetricPriority::Fast => Priority::Fast,
            insider_metric_sdk::MetricPriority::Normal => Priority::Standard,
            insider_metric_sdk::MetricPriority::Background => Priority::Batch,
        };
        scheduler
            .submit(Work {
                task_id: 0,
                priority,
                deadline,
                payload: ScheduledMetric {
                    metric_id: metric_id.to_owned(),
                    context,
                },
            })
            .map_err(|error| match error {
                SubmitError::Full(_) | SubmitError::Unavailable(_) => ScheduleError::QueueFull,
            })
    }

    /// Pops one ready scheduled task and evaluates it through normal host
    /// validation/quarantine handling.
    pub fn evaluate_ready(
        &mut self,
        scheduler: &Scheduler<ScheduledMetric>,
        now: insider_common_types::MonoTime,
    ) -> Option<(bool, Result<MetricOutput, HostError>)> {
        let ready = scheduler.pop_ready(now)?;
        let late = ready.late;
        let task = ready.work.payload;
        if late {
            // A late task is retained in scheduler diagnostics by the caller,
            // but it must never mutate state or become a valid snapshot.
            return Some((true, Err(HostError::Evaluation(MetricError::StaleOutput))));
        }
        Some((false, self.evaluate(&task.metric_id, &task.context)))
    }

    /// Captures a metric's incremental state for durable checkpointing.
    ///
    /// # Errors
    /// Returns [`HostError`] when the metric is unavailable or cannot capture
    /// its state.
    pub fn checkpoint(&self, metric_id: &str) -> Result<Option<Vec<u8>>, HostError> {
        let entry = self
            .entries
            .get(metric_id)
            .ok_or_else(|| HostError::Unavailable(metric_id.to_owned()))?;
        if entry.state == State::Quarantined {
            return Err(HostError::Unavailable(metric_id.to_owned()));
        }
        entry.metric.checkpoint().map_err(HostError::Evaluation)
    }

    /// Restores a previously captured metric checkpoint.
    ///
    /// # Errors
    /// Returns [`HostError`] when the metric is unavailable/quarantined or the
    /// checkpoint does not match its versioned state schema.
    pub fn restore_checkpoint(&mut self, metric_id: &str, bytes: &[u8]) -> Result<(), HostError> {
        let entry = self
            .entries
            .get_mut(metric_id)
            .ok_or_else(|| HostError::Unavailable(metric_id.to_owned()))?;
        if entry.state == State::Quarantined {
            return Err(HostError::Unavailable(metric_id.to_owned()));
        }
        entry
            .metric
            .restore_checkpoint(bytes)
            .map_err(HostError::Evaluation)
    }

    /// Returns current lifecycle state for a metric.
    #[must_use]
    pub fn state(&self, metric_id: &str) -> Option<State> {
        self.entries.get(metric_id).map(|entry| entry.state)
    }

    /// Returns bounded aggregate counts for operational supervision.
    #[must_use]
    pub fn health_counts(&self) -> (usize, usize) {
        let total = self.entries.len();
        let quarantined = self
            .entries
            .values()
            .filter(|entry| entry.state == State::Quarantined)
            .count();
        (total, quarantined)
    }

    /// Returns the artifact promotion lifecycle.
    #[must_use]
    pub fn lifecycle(&self, metric_id: &str) -> Option<Lifecycle> {
        self.entries.get(metric_id).map(|entry| entry.lifecycle)
    }

    /// Restores a journaled metric lifecycle and its evidence reference.
    pub fn restore_lifecycle_with_evidence(
        &mut self,
        metric_id: &str,
        lifecycle: Lifecycle,
        evidence_ref: &str,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(metric_id) else {
            return false;
        };
        entry.lifecycle = lifecycle;
        evidence_ref.clone_into(&mut entry.lifecycle_evidence_ref);
        true
    }

    /// Checks whether a lifecycle transition is permitted.
    #[must_use]
    pub fn can_transition_lifecycle(&self, metric_id: &str, next: Lifecycle) -> bool {
        let Some(entry) = self.entries.get(metric_id) else {
            return false;
        };
        matches!(
            (entry.lifecycle, next),
            (
                Lifecycle::Research,
                Lifecycle::Validated | Lifecycle::Retired
            ) | (Lifecycle::Validated, Lifecycle::Shadow | Lifecycle::Retired)
                | (Lifecycle::Shadow, Lifecycle::Canary | Lifecycle::Retired)
                | (
                    Lifecycle::Canary,
                    Lifecycle::Production | Lifecycle::Paused | Lifecycle::Retired
                )
                | (
                    Lifecycle::Production,
                    Lifecycle::Paused | Lifecycle::Retired
                )
                | (
                    Lifecycle::Paused,
                    Lifecycle::Canary | Lifecycle::Production | Lifecycle::Retired
                )
        )
    }

    /// Applies a validated lifecycle transition.
    pub fn transition_lifecycle(&mut self, metric_id: &str, next: Lifecycle) -> bool {
        self.transition_lifecycle_with_evidence(metric_id, next, "operator")
    }

    /// Applies a validated lifecycle transition and stores its evidence.
    pub fn transition_lifecycle_with_evidence(
        &mut self,
        metric_id: &str,
        next: Lifecycle,
        evidence_ref: &str,
    ) -> bool {
        if !self.can_transition_lifecycle(metric_id, next) {
            return false;
        }
        let Some(entry) = self.entries.get_mut(metric_id) else {
            return false;
        };
        entry.lifecycle = next;
        evidence_ref.clone_into(&mut entry.lifecycle_evidence_ref);
        true
    }

    /// Returns the evidence reference associated with the lifecycle.
    #[must_use]
    pub fn lifecycle_evidence(&self, metric_id: &str) -> Option<&str> {
        self.entries
            .get(metric_id)
            .map(|entry| entry.lifecycle_evidence_ref.as_str())
    }

    /// Returns the immutable scheduler manifest for a registered metric.
    #[must_use]
    pub fn manifest(&self, metric_id: &str) -> Option<&MetricManifest> {
        self.entries.get(metric_id).map(|entry| &entry.manifest)
    }

    /// Returns registered metric IDs in deterministic order for scheduler
    /// admission. The returned list is bounded by the host's package set.
    #[must_use]
    pub fn metric_ids(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Resumes a quarantined metric after an operator or supervisor decision.
    pub fn resume(&mut self, metric_id: &str) -> bool {
        let Some(entry) = self.entries.get_mut(metric_id) else {
            return false;
        };
        entry.state = State::Ready;
        entry.failures = 0;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;

    use insider_common_types::{InstrumentId, MonoTime};
    use insider_metric_sdk::{Metric, MetricContext, MetricDescriptor, MetricError, MetricOutput};

    use super::{DiscoveryError, Host, HostError, Lifecycle, State, discover_metric_packages};

    struct FixedMetric {
        descriptor: MetricDescriptor,
        fail: bool,
    }

    impl Metric for FixedMetric {
        fn descriptor(&self) -> &MetricDescriptor {
            &self.descriptor
        }
        fn evaluate(&self, context: &MetricContext) -> Result<MetricOutput, MetricError> {
            if self.fail {
                return Err(MetricError::DeadlineExceeded);
            }
            let instrument_id = context
                .instrument_id
                .ok_or_else(|| MetricError::MissingInput(String::from("instrument_id")))?;
            Ok(MetricOutput {
                metric_id: self.descriptor.metric_id.clone(),
                instrument_id,
                generated_mono: context.now,
                ttl_ns: 10,
                score: 0.5,
                confidence: 0.8,
                uncertainty: 0.1,
            })
        }
    }

    fn descriptor(id: &str) -> MetricDescriptor {
        MetricDescriptor {
            metric_id: id.to_owned(),
            inputs: Vec::new(),
            min_score: Some(-1.0),
            max_score: Some(1.0),
            ttl_ns: 10,
        }
    }

    #[test]
    fn valid_metric_evaluates_and_failures_quarantine_independently() {
        let Some(instrument) = InstrumentId::new(1).ok() else {
            return;
        };
        let mut host = Host::new(2);
        assert!(
            host.register(Arc::new(FixedMetric {
                descriptor: descriptor("good"),
                fail: false
            }))
            .is_ok()
        );
        assert!(
            host.register(Arc::new(FixedMetric {
                descriptor: descriptor("bad"),
                fail: true
            }))
            .is_ok()
        );
        let context = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::new(),
            now: MonoTime::from_nanos(1),
        };
        assert!(host.evaluate("good", &context).is_ok());
        assert!(matches!(
            host.evaluate("bad", &context),
            Err(HostError::Evaluation(MetricError::DeadlineExceeded))
        ));
        assert!(matches!(
            host.evaluate("bad", &context),
            Err(HostError::Evaluation(MetricError::DeadlineExceeded))
        ));
        assert_eq!(host.state("bad"), Some(State::Quarantined));
        assert!(matches!(
            host.evaluate("bad", &context),
            Err(HostError::Unavailable(_))
        ));
        assert_eq!(host.state("good"), Some(State::Ready));
    }

    #[test]
    fn discovered_metric_requires_promotion_before_evaluation() {
        let Some(instrument) = InstrumentId::new(1).ok() else {
            return;
        };
        let metric = Arc::new(FixedMetric {
            descriptor: descriptor("discovered"),
            fail: false,
        });
        let manifest = metric.manifest();
        let discovered = super::DiscoveredMetric {
            manifest_path: std::path::PathBuf::from("metric.manifest"),
            manifest,
            language: String::from("rust"),
            entrypoint: None,
        };
        let mut host = Host::new(2);
        assert!(
            host.register_discovered(&discovered, |_| Ok(metric))
                .is_ok()
        );
        let context = MetricContext {
            instrument_id: Some(instrument),
            features: BTreeMap::new(),
            now: MonoTime::from_nanos(1),
        };
        assert!(matches!(
            host.evaluate("discovered", &context),
            Err(HostError::Unavailable(_))
        ));
        assert!(host.transition_lifecycle("discovered", Lifecycle::Validated));
        assert!(host.transition_lifecycle("discovered", Lifecycle::Shadow));
        assert!(host.evaluate_shadow("discovered", &context).is_ok());
        assert!(matches!(
            host.evaluate("discovered", &context),
            Err(HostError::Unavailable(_))
        ));
        assert!(host.transition_lifecycle("discovered", Lifecycle::Canary));
        assert!(host.evaluate("discovered", &context).is_ok());
    }

    #[test]
    fn discovery_is_recursive_deterministic_and_bounded() {
        let root = std::env::temp_dir().join(format!(
            "insidertrader-metric-discovery-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        assert!(fs::create_dir_all(root.join("zeta")).is_ok());
        assert!(fs::create_dir_all(root.join("alpha/nested")).is_ok());
        let manifest = "metric:\nid: \"demo.v1\"\ninputs: [price]\nmin_score: 0\nmax_score: 1\nttl_ns: 10\nperiod_ns: 20\ndeadline_ns: 5\nbudget_ns: 5\npriority: fast\n";
        assert!(fs::write(root.join("zeta/metric.manifest"), manifest).is_ok());
        assert!(
            fs::write(
                root.join("alpha/nested/metric.manifest"),
                manifest.replace("demo.v1", "demo.v2")
            )
            .is_ok()
        );
        let Ok(discovered) = discover_metric_packages(&root) else {
            let _ = fs::remove_dir_all(&root);
            return;
        };
        assert_eq!(discovered.len(), 2);
        assert!(discovered[0].manifest_path < discovered[1].manifest_path);
        assert!(fs::create_dir_all(root.join("duplicate")).is_ok());
        assert!(fs::write(root.join("duplicate/metric.manifest"), manifest).is_ok());
        assert!(matches!(
            discover_metric_packages(&root),
            Err(DiscoveryError::Invalid { reason, .. }) if reason.contains("duplicate metric id")
        ));
        let oversized = root.join("oversized");
        assert!(fs::create_dir_all(&oversized).is_ok());
        assert!(fs::write(oversized.join("metric.manifest"), "x".repeat(1_048_577)).is_ok());
        assert!(matches!(
            discover_metric_packages(&oversized),
            Err(DiscoveryError::BoundsExceeded {
                bound: "manifest_bytes",
                ..
            })
        ));
        let duplicate_field = root.join("duplicate-field");
        assert!(fs::create_dir_all(&duplicate_field).is_ok());
        assert!(
            fs::write(
                duplicate_field.join("metric.manifest"),
                format!("{manifest}priority: normal\n")
            )
            .is_ok()
        );
        assert!(matches!(
            discover_metric_packages(&duplicate_field),
            Err(DiscoveryError::Invalid { reason, .. })
                if reason.contains("duplicate manifest field")
        ));
        assert!(matches!(
            discover_metric_packages(root.join("missing")),
            Err(DiscoveryError::Io(_))
        ));
        let _ = fs::remove_dir_all(root);
    }
}
