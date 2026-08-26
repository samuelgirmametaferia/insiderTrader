//! Runtime host for independently evaluated and quarantinable strategies.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::time::Duration;

use insider_scheduler::{Priority, Scheduler, SubmitError, Work};
use insider_strategy_sdk::{Proposal, ProposalError, Strategy, StrategyContext, StrategyManifest};

const PYTHON_MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_STRATEGY_DISCOVERY_DEPTH: usize = 32;
const MAX_DISCOVERED_STRATEGIES: usize = 4_096;
const MAX_MANIFEST_BYTES: u64 = 1_048_576;

#[allow(clippy::needless_pass_by_value)]
fn read_worker_frames(mut stdout: ChildStdout, sender: SyncSender<Vec<u8>>, max: usize) {
    loop {
        let mut header = [0_u8; 4];
        if stdout.read_exact(&mut header).is_err() {
            return;
        }
        let length = usize::try_from(u32::from_le_bytes(header)).unwrap_or(usize::MAX);
        if length == 0 || length > max {
            return;
        }
        let mut payload = vec![0_u8; length];
        if stdout.read_exact(&mut payload).is_err() || sender.send(payload).is_err() {
            return;
        }
    }
}

/// Python strategy process implementing the same typed proposal boundary as
/// in-process strategies.
pub struct PythonStrategyProcess {
    strategy_id: String,
    manifest: StrategyManifest,
    child: Mutex<Child>,
    stdin: Mutex<ChildStdin>,
    responses: Mutex<Receiver<Vec<u8>>>,
    max_frame_bytes: usize,
}

impl PythonStrategyProcess {
    /// Starts an isolated Python strategy worker.
    ///
    /// # Errors
    /// Returns a bounded configuration, process-spawn, or pipe setup error.
    pub fn spawn(
        mut command: Command,
        entrypoint: &str,
        manifest: StrategyManifest,
    ) -> Result<Arc<Self>, String> {
        if entrypoint.trim().is_empty() || manifest.validate().is_err() {
            return Err("invalid Python strategy worker configuration".into());
        }
        let strategy_id = manifest.strategy_id.clone();
        let max = PYTHON_MAX_FRAME_BYTES;
        command
            .arg("--entrypoint")
            .arg(entrypoint)
            .arg("--strategy-id")
            .arg(&strategy_id)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let mut child = command
            .spawn()
            .map_err(|error| format!("spawn Python strategy worker: {error}"))?;
        let Some(stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Python strategy stdin unavailable".into());
        };
        let Some(stdout) = child.stdout.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Python strategy stdout unavailable".into());
        };
        let (sender, receiver) = sync_channel(64);
        std::thread::Builder::new()
            .name(format!("strategy-worker-reader-{strategy_id}"))
            .spawn(move || read_worker_frames(stdout, sender, max))
            .map_err(|error| format!("start Python strategy reader: {error}"))?;
        Ok(Arc::new(Self {
            strategy_id,
            manifest,
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            responses: Mutex::new(receiver),
            max_frame_bytes: max,
        }))
    }

    fn evaluate_process(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
        let metrics: Vec<serde_json::Value> = context
            .metrics
            .iter()
            .map(|metric| {
                serde_json::json!({
                    "metric_id": metric.metric_id,
                    "instrument_id": metric.instrument_id.get().to_string(),
                    "generated_mono_ns": metric.generated_mono.as_nanos(),
                    "ttl_ns": metric.ttl_ns,
                    "score": metric.score,
                    "confidence": metric.confidence,
                    "uncertainty": metric.uncertainty,
                })
            })
            .collect();
        let request = serde_json::json!({
            "instrument_id": context.instrument_id.get().to_string(),
            "now_mono_ns": context.now.as_nanos(),
            "metrics": metrics,
        });
        let payload = serde_json::to_vec(&request).map_err(|_| ProposalError::InvalidAction)?;
        if payload.is_empty() || payload.len() > self.max_frame_bytes {
            return Err(ProposalError::InvalidAction);
        }
        let mut stdin = self
            .stdin
            .lock()
            .map_err(|_| ProposalError::InvalidAction)?;
        stdin
            .write_all(
                &u32::try_from(payload.len())
                    .map_err(|_| ProposalError::InvalidAction)?
                    .to_le_bytes(),
            )
            .and_then(|()| stdin.write_all(&payload))
            .and_then(|()| stdin.flush())
            .map_err(|_| ProposalError::InvalidAction)?;
        drop(stdin);
        let response = self
            .responses
            .lock()
            .map_err(|_| ProposalError::InvalidAction)?
            .recv_timeout(Duration::from_nanos(self.manifest.deadline_ns))
            .map_err(|_| {
                if let Ok(mut child) = self.child.lock() {
                    let _ = child.kill();
                }
                ProposalError::InvalidHorizon
            })?;
        let value: serde_json::Value =
            serde_json::from_slice(&response).map_err(|_| ProposalError::InvalidAction)?;
        if value.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            return Err(ProposalError::InvalidAction);
        }
        parse_python_proposal(
            value.get("proposal").ok_or(ProposalError::InvalidAction)?,
            context,
            &self.strategy_id,
        )
    }
}

impl Drop for PythonStrategyProcess {
    fn drop(&mut self) {
        if let Ok(mut child) = self.child.lock() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Strategy for PythonStrategyProcess {
    fn strategy_id(&self) -> &str {
        &self.strategy_id
    }

    fn manifest(&self) -> StrategyManifest {
        self.manifest.clone()
    }

    fn evaluate(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
        self.evaluate_process(context)
    }
}

fn parse_python_proposal(
    value: &serde_json::Value,
    context: &StrategyContext<'_>,
    strategy_id: &str,
) -> Result<Proposal, ProposalError> {
    let action = value.get("action").ok_or(ProposalError::InvalidAction)?;
    let action_type = action
        .get("type")
        .and_then(serde_json::Value::as_str)
        .ok_or(ProposalError::InvalidAction)?;
    let number = || action.get("value").and_then(serde_json::Value::as_i64);
    let action = match action_type {
        "no_action" => insider_strategy_sdk::Action::NoAction,
        "target_quantity" => insider_strategy_sdk::Action::TargetQuantity {
            quantity_ticks: number().ok_or(ProposalError::InvalidAction)?,
        },
        "increase" => insider_strategy_sdk::Action::Increase {
            quantity_ticks: number().ok_or(ProposalError::InvalidAction)?,
        },
        "decrease" => insider_strategy_sdk::Action::Decrease {
            quantity_ticks: number().ok_or(ProposalError::InvalidAction)?,
        },
        "close" => insider_strategy_sdk::Action::Close,
        "target_weight" => insider_strategy_sdk::Action::TargetWeight {
            weight: action
                .get("value")
                .and_then(serde_json::Value::as_f64)
                .ok_or(ProposalError::InvalidAction)?,
        },
        _ => return Err(ProposalError::InvalidAction),
    };
    let proposal_id = insider_common_types::ProposalId::new(
        value
            .get("proposal_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(ProposalError::MissingIdentity)?
            .parse::<u128>()
            .map_err(|_| ProposalError::MissingIdentity)?,
    )
    .map_err(|_| ProposalError::MissingIdentity)?;
    let evidence = value
        .get("evidence")
        .and_then(serde_json::Value::as_array)
        .ok_or(ProposalError::InvalidAction)?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or(ProposalError::InvalidAction)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let proposal = Proposal {
        proposal_id,
        strategy_id: value
            .get("strategy_id")
            .and_then(serde_json::Value::as_str)
            .ok_or(ProposalError::MissingIdentity)?
            .to_owned(),
        instrument_id: insider_common_types::InstrumentId::new(
            value
                .get("instrument_id")
                .and_then(serde_json::Value::as_str)
                .ok_or(ProposalError::MissingIdentity)?
                .parse::<u128>()
                .map_err(|_| ProposalError::MissingIdentity)?,
        )
        .map_err(|_| ProposalError::MissingIdentity)?,
        action,
        confidence: value
            .get("confidence")
            .and_then(serde_json::Value::as_f64)
            .ok_or(ProposalError::InvalidConfidence)?,
        horizon_ns: value
            .get("horizon_ns")
            .and_then(serde_json::Value::as_u64)
            .ok_or(ProposalError::InvalidHorizon)?,
        ttl_ns: value
            .get("ttl_ns")
            .and_then(serde_json::Value::as_u64)
            .ok_or(ProposalError::InvalidHorizon)?,
        evidence,
        generated_mono: insider_common_types::MonoTime::from_nanos(
            value
                .get("generated_mono_ns")
                .and_then(serde_json::Value::as_u64)
                .ok_or(ProposalError::InvalidHorizon)?,
        ),
    };
    if proposal.strategy_id != strategy_id || proposal.instrument_id != context.instrument_id {
        return Err(ProposalError::MissingIdentity);
    }
    proposal.validate(context.now)?;
    Ok(proposal)
}

/// A validated strategy package discovered from the filesystem.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiscoveredStrategy {
    /// Package manifest path.
    pub manifest_path: PathBuf,
    /// Validated immutable strategy manifest.
    pub manifest: StrategyManifest,
    /// Implementation language declared by the package.
    pub language: String,
    /// Out-of-process entrypoint for Python packages.
    pub entrypoint: Option<String>,
}

/// Errors raised while discovering or parsing strategy packages.
#[derive(Debug)]
pub enum DiscoveryError {
    /// Filesystem traversal failed.
    Io(std::io::Error),
    /// A manifest is malformed or violates the strategy contract.
    Invalid {
        /// Manifest that failed validation.
        path: PathBuf,
        /// Machine-readable parsing or validation reason.
        reason: String,
    },
    /// The package tree exceeds a defensive discovery bound.
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

/// Discovers `strategy.manifest` files in deterministic path order.
///
/// # Errors
/// Returns [`DiscoveryError`] when traversal, file reads, parsing, or manifest
/// validation fails.
pub fn discover_strategy_packages(
    root: impl AsRef<Path>,
) -> Result<Vec<DiscoveredStrategy>, DiscoveryError> {
    let mut paths = Vec::new();
    let mut visited = BTreeSet::new();
    collect_manifest_paths(root.as_ref(), 0, &mut paths, &mut visited)?;
    paths.sort();
    let mut discovered = Vec::with_capacity(paths.len());
    let mut ids = BTreeSet::new();
    for path in paths {
        let text = read_manifest_text(&path)?;
        let (manifest, language, entrypoint) =
            parse_strategy_manifest(&text).map_err(|reason| DiscoveryError::Invalid {
                path: path.clone(),
                reason,
            })?;
        if !ids.insert(manifest.strategy_id.clone()) {
            return Err(DiscoveryError::Invalid {
                path,
                reason: format!("duplicate strategy id: {}", manifest.strategy_id),
            });
        }
        discovered.push(DiscoveredStrategy {
            manifest_path: path,
            manifest,
            language,
            entrypoint,
        });
    }
    Ok(discovered)
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

fn collect_manifest_paths(
    path: &Path,
    depth: usize,
    output: &mut Vec<PathBuf>,
    visited: &mut BTreeSet<PathBuf>,
) -> Result<(), DiscoveryError> {
    if depth > MAX_STRATEGY_DISCOVERY_DEPTH {
        return Err(DiscoveryError::BoundsExceeded {
            path: path.to_path_buf(),
            bound: "depth",
        });
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.is_file() {
        if path
            .file_name()
            .is_some_and(|name| name == "strategy.manifest")
        {
            if output.len() >= MAX_DISCOVERED_STRATEGIES {
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
        collect_manifest_paths(&entry?.path(), depth + 1, output, visited)?;
    }
    Ok(())
}

fn parse_strategy_manifest(
    text: &str,
) -> Result<(StrategyManifest, String, Option<String>), String> {
    let mut id = None;
    let mut language = String::from("rust");
    let mut entrypoint = None;
    let mut mode = None;
    let mut metrics = Vec::new();
    let mut dependencies = Vec::new();
    let mut horizon = None;
    let mut ttl = None;
    let mut period = None;
    let mut deadline = None;
    let mut priority = None;
    let mut seen = BTreeSet::new();
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') || line == "strategy:" {
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
            "id" | "strategy_id" => id = Some(value.to_owned()),
            "language" => language = value.to_ascii_lowercase(),
            "entrypoint" => entrypoint = Some(value.to_owned()),
            "mode" => {
                mode = Some(match value.to_ascii_lowercase().as_str() {
                    "deterministic" => insider_strategy_sdk::StrategyMode::Deterministic,
                    "contextual" => insider_strategy_sdk::StrategyMode::Contextual,
                    _ => return Err("mode must be deterministic or contextual".into()),
                });
            }
            "metrics" | "metric_ids" => metrics = parse_list(value)?,
            "dependencies" | "strategy_dependencies" => dependencies = parse_list(value)?,
            "horizon_ns" => horizon = Some(parse_positive(value, key)?),
            "ttl_ns" => ttl = Some(parse_positive(value, key)?),
            "period_ns" => period = Some(parse_positive(value, key)?),
            "deadline_ns" => deadline = Some(parse_positive(value, key)?),
            "priority" => {
                priority = Some(match value.to_ascii_lowercase().as_str() {
                    "fast" => insider_strategy_sdk::StrategyPriority::Fast,
                    "normal" => insider_strategy_sdk::StrategyPriority::Normal,
                    "background" => insider_strategy_sdk::StrategyPriority::Background,
                    _ => return Err("priority must be fast, normal, or background".into()),
                });
            }
            _ => return Err(format!("unknown manifest field: {key}")),
        }
    }
    let manifest = StrategyManifest {
        strategy_id: id.ok_or("missing id")?,
        mode: mode.ok_or("missing mode")?,
        metric_ids: metrics,
        strategy_dependencies: dependencies,
        horizon_ns: horizon.ok_or("missing horizon_ns")?,
        ttl_ns: ttl.ok_or("missing ttl_ns")?,
        period_ns: period.ok_or("missing period_ns")?,
        deadline_ns: deadline.ok_or("missing deadline_ns")?,
        priority: priority.ok_or("missing priority")?,
    };
    manifest
        .validate()
        .map_err(|error| format!("invalid manifest: {error:?}"))?;
    if !matches!(language.as_str(), "rust" | "python") {
        return Err("language must be rust or python".into());
    }
    if language == "python" && entrypoint.as_deref().is_none_or(str::is_empty) {
        return Err("python strategies require entrypoint".into());
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

/// One strategy evaluation admitted to the shared bounded scheduler.
#[derive(Clone, Debug)]
pub struct ScheduledStrategy {
    /// Registered strategy identity.
    pub strategy_id: String,
    /// Immutable input snapshot captured at admission.
    pub context: StrategyContextOwned,
}

/// Owned strategy context suitable for queued work.
#[derive(Clone, Debug)]
pub struct StrategyContextOwned {
    /// Evaluation time.
    pub now: insider_common_types::MonoTime,
    /// Instrument identity.
    pub instrument_id: insider_common_types::InstrumentId,
    /// Metric snapshot.
    pub metrics: Vec<insider_metric_sdk::MetricOutput>,
}

impl StrategyContextOwned {
    fn as_context(&self) -> StrategyContext<'_> {
        StrategyContext {
            now: self.now,
            instrument_id: self.instrument_id,
            metrics: &self.metrics,
        }
    }
}

/// Failure while admitting scheduled strategy work.
#[derive(Debug)]
pub enum ScheduleError {
    /// Strategy is absent or quarantined.
    Unavailable(String),
    /// Scheduler queue rejected work at capacity or due to lock failure.
    QueueFull,
}

/// Strategy host failure.
#[derive(Clone, Debug, PartialEq)]
pub enum HostError {
    /// Strategy ID was already registered.
    Duplicate(String),
    /// The strategy manifest is invalid or disagrees with the strategy ID.
    InvalidManifest(String),
    /// Strategy is absent or quarantined.
    Unavailable(String),
    /// Evaluation or proposal validation failed.
    Evaluation(ProposalError),
}

/// Hosted strategy lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum State {
    /// Evaluation is allowed.
    Ready,
    /// Evaluation is isolated after repeated failures.
    Quarantined,
}

/// Artifact lifecycle state independent from worker health.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    /// Newly discovered and not eligible for live decisions.
    Research,
    /// Validation evidence accepted; not live eligible.
    Validated,
    /// Receiving shadow evaluations only.
    Shadow,
    /// Bounded live/canary eligibility.
    Canary,
    /// Fully production eligible.
    Production,
    /// Explicitly paused by an operator.
    Paused,
    /// Permanently retired.
    Retired,
}

struct Entry {
    strategy: Arc<dyn Strategy>,
    manifest: StrategyManifest,
    state: State,
    lifecycle: Lifecycle,
    lifecycle_evidence_ref: String,
    failures: u32,
}

/// Strategy host with independent failure isolation.
pub struct Host {
    failure_limit: u32,
    entries: BTreeMap<String, Entry>,
}

impl Host {
    /// Creates a host with a per-strategy failure limit.
    #[must_use]
    pub const fn new(failure_limit: u32) -> Self {
        Self {
            failure_limit,
            entries: BTreeMap::new(),
        }
    }

    /// Registers a strategy by its immutable ID.
    ///
    /// # Errors
    /// Returns [`HostError::Duplicate`] if the ID already exists.
    pub fn register(&mut self, strategy: Arc<dyn Strategy>) -> Result<(), HostError> {
        self.register_with_lifecycle(strategy, Lifecycle::Production)
    }

    fn register_with_lifecycle(
        &mut self,
        strategy: Arc<dyn Strategy>,
        lifecycle: Lifecycle,
    ) -> Result<(), HostError> {
        let id = strategy.strategy_id().to_owned();
        if id.trim().is_empty() {
            return Err(HostError::Duplicate(id));
        }
        if self.entries.contains_key(&id) {
            return Err(HostError::Duplicate(id));
        }
        let manifest = strategy.manifest();
        if manifest.strategy_id != id {
            return Err(HostError::InvalidManifest(id));
        }
        manifest
            .validate()
            .map_err(|_| HostError::InvalidManifest(id.clone()))?;
        self.entries.insert(
            id,
            Entry {
                strategy,
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
    /// factory and admits it only when the implementation reproduces the
    /// manifest exactly. This keeps package discovery separate from hard-coded
    /// registration while preserving capability and scheduling checks.
    ///
    /// # Errors
    /// Returns [`HostError::InvalidManifest`] for a manifest/implementation
    /// mismatch or normal registration errors for duplicate IDs.
    pub fn register_discovered<F>(
        &mut self,
        discovered: &DiscoveredStrategy,
        factory: F,
    ) -> Result<(), HostError>
    where
        F: FnOnce(&StrategyManifest) -> Result<Arc<dyn Strategy>, HostError>,
    {
        let strategy = factory(&discovered.manifest)?;
        if strategy.manifest() != discovered.manifest
            || strategy.strategy_id() != discovered.manifest.strategy_id
        {
            return Err(HostError::InvalidManifest(
                discovered.manifest.strategy_id.clone(),
            ));
        }
        self.register_with_lifecycle(strategy, Lifecycle::Research)
    }

    /// Registers a discovered Python strategy through its isolated worker.
    ///
    /// # Errors
    /// Returns [`HostError::InvalidManifest`] for non-Python or incomplete
    /// packages, or [`HostError::Unavailable`] when the worker cannot start.
    pub fn register_discovered_python(
        &mut self,
        discovered: &DiscoveredStrategy,
        command: std::process::Command,
    ) -> Result<(), HostError> {
        if discovered.language != "python" {
            return Err(HostError::InvalidManifest(
                discovered.manifest.strategy_id.clone(),
            ));
        }
        let entrypoint = discovered
            .entrypoint
            .as_deref()
            .ok_or_else(|| HostError::InvalidManifest(discovered.manifest.strategy_id.clone()))?;
        let strategy =
            PythonStrategyProcess::spawn(command, entrypoint, discovered.manifest.clone())
                .map_err(HostError::Unavailable)?;
        self.register_with_lifecycle(strategy, Lifecycle::Research)
    }

    /// Evaluates a strategy and validates its resulting proposal.
    ///
    /// # Errors
    /// Returns [`HostError`] for unavailable strategies, evaluation failures,
    /// or invalid proposals. Repeated failures quarantine only that strategy.
    pub fn evaluate(
        &mut self,
        strategy_id: &str,
        context: &StrategyContext<'_>,
    ) -> Result<Proposal, HostError> {
        self.evaluate_internal(strategy_id, context, false)
    }

    /// Evaluates a strategy in Shadow lifecycle without admitting its proposal
    /// to the coordinator. The caller may compare the returned proposal with
    /// live decisions, but it cannot pass this result directly to execution.
    ///
    /// # Errors
    /// Returns [`HostError::Unavailable`] unless the strategy is specifically
    /// in Shadow lifecycle, or the same validation/evaluation errors as
    /// [`Self::evaluate`].
    pub fn evaluate_shadow(
        &mut self,
        strategy_id: &str,
        context: &StrategyContext<'_>,
    ) -> Result<Proposal, HostError> {
        self.evaluate_internal(strategy_id, context, true)
    }

    fn evaluate_internal(
        &mut self,
        strategy_id: &str,
        context: &StrategyContext<'_>,
        shadow: bool,
    ) -> Result<Proposal, HostError> {
        let Some(entry) = self.entries.get_mut(strategy_id) else {
            return Err(HostError::Unavailable(strategy_id.to_owned()));
        };
        if entry.state == State::Quarantined {
            return Err(HostError::Unavailable(strategy_id.to_owned()));
        }
        if (!shadow && !matches!(entry.lifecycle, Lifecycle::Canary | Lifecycle::Production))
            || (shadow && entry.lifecycle != Lifecycle::Shadow)
        {
            return Err(HostError::Unavailable(strategy_id.to_owned()));
        }
        let scoped_metrics = context
            .metrics
            .iter()
            .filter(|metric| {
                entry
                    .manifest
                    .metric_ids
                    .iter()
                    .any(|id| id == &metric.metric_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        let scoped_context = StrategyContext {
            now: context.now,
            instrument_id: context.instrument_id,
            metrics: &scoped_metrics,
        };
        let proposal = match entry.strategy.evaluate(&scoped_context) {
            Ok(proposal) => proposal,
            Err(error) => {
                entry.failures = entry.failures.saturating_add(1);
                if entry.failures >= self.failure_limit.max(1) {
                    entry.state = State::Quarantined;
                }
                return Err(HostError::Evaluation(error));
            }
        };
        if let Err(error) = proposal.validate(context.now) {
            entry.failures = entry.failures.saturating_add(1);
            if entry.failures >= self.failure_limit.max(1) {
                entry.state = State::Quarantined;
            }
            return Err(HostError::Evaluation(error));
        }
        entry.failures = 0;
        Ok(proposal)
    }

    /// Returns a strategy lifecycle state.
    #[must_use]
    pub fn state(&self, strategy_id: &str) -> Option<State> {
        self.entries.get(strategy_id).map(|entry| entry.state)
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

    /// Returns the artifact lifecycle state.
    #[must_use]
    pub fn lifecycle(&self, strategy_id: &str) -> Option<Lifecycle> {
        self.entries.get(strategy_id).map(|entry| entry.lifecycle)
    }

    /// Restores a journaled lifecycle exactly after registration.
    pub fn restore_lifecycle(&mut self, strategy_id: &str, lifecycle: Lifecycle) -> bool {
        self.restore_lifecycle_with_evidence(strategy_id, lifecycle, "legacy-journal")
    }

    /// Restores a journaled lifecycle and its promotion evidence.
    pub fn restore_lifecycle_with_evidence(
        &mut self,
        strategy_id: &str,
        lifecycle: Lifecycle,
        evidence_ref: &str,
    ) -> bool {
        let Some(entry) = self.entries.get_mut(strategy_id) else {
            return false;
        };
        entry.lifecycle = lifecycle;
        evidence_ref.clone_into(&mut entry.lifecycle_evidence_ref);
        true
    }

    /// Checks whether an operator-requested lifecycle transition is valid.
    #[must_use]
    pub fn can_transition_lifecycle(&self, strategy_id: &str, next: Lifecycle) -> bool {
        let Some(entry) = self.entries.get(strategy_id) else {
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

    /// Transitions an artifact through the controlled promotion path.
    pub fn transition_lifecycle(&mut self, strategy_id: &str, next: Lifecycle) -> bool {
        self.transition_lifecycle_with_evidence(strategy_id, next, "operator")
    }

    /// Applies a validated lifecycle transition and records its evidence.
    pub fn transition_lifecycle_with_evidence(
        &mut self,
        strategy_id: &str,
        next: Lifecycle,
        evidence_ref: &str,
    ) -> bool {
        if !self.can_transition_lifecycle(strategy_id, next) {
            return false;
        }
        let Some(entry) = self.entries.get_mut(strategy_id) else {
            return false;
        };
        entry.lifecycle = next;
        evidence_ref.clone_into(&mut entry.lifecycle_evidence_ref);
        true
    }

    /// Returns the evidence reference associated with the current lifecycle.
    #[must_use]
    pub fn lifecycle_evidence(&self, strategy_id: &str) -> Option<&str> {
        self.entries
            .get(strategy_id)
            .map(|entry| entry.lifecycle_evidence_ref.as_str())
    }

    /// Admits one strategy evaluation using manifest-derived deadline/priority.
    ///
    /// # Errors
    /// Returns [`ScheduleError`] when the strategy is unavailable or the queue
    /// cannot admit another task.
    pub fn schedule(
        &self,
        scheduler: &Scheduler<ScheduledStrategy>,
        strategy_id: &str,
        context: StrategyContextOwned,
    ) -> Result<u64, ScheduleError> {
        let Some(entry) = self.entries.get(strategy_id) else {
            return Err(ScheduleError::Unavailable(strategy_id.to_owned()));
        };
        if entry.state == State::Quarantined {
            return Err(ScheduleError::Unavailable(strategy_id.to_owned()));
        }
        if !matches!(entry.lifecycle, Lifecycle::Canary | Lifecycle::Production) {
            return Err(ScheduleError::Unavailable(strategy_id.to_owned()));
        }
        let deadline = context
            .now
            .checked_add(entry.manifest.deadline_ns)
            .ok_or(ScheduleError::QueueFull)?;
        let priority = match entry.manifest.priority {
            insider_strategy_sdk::StrategyPriority::Fast => Priority::Fast,
            insider_strategy_sdk::StrategyPriority::Normal => Priority::Standard,
            insider_strategy_sdk::StrategyPriority::Background => Priority::Batch,
        };
        scheduler
            .submit(Work {
                task_id: 0,
                priority,
                deadline,
                payload: ScheduledStrategy {
                    strategy_id: strategy_id.to_owned(),
                    context,
                },
            })
            .map_err(|error| match error {
                SubmitError::Full(_) | SubmitError::Unavailable(_) => ScheduleError::QueueFull,
            })
    }

    /// Pops one ready strategy task and evaluates it through normal validation
    /// and quarantine handling.
    pub fn evaluate_ready(
        &mut self,
        scheduler: &Scheduler<ScheduledStrategy>,
        now: insider_common_types::MonoTime,
    ) -> Option<(bool, Result<Proposal, HostError>)> {
        let ready = scheduler.pop_ready(now)?;
        let late = ready.late;
        let task = ready.work.payload;
        let context = task.context.as_context();
        Some((late, self.evaluate(&task.strategy_id, &context)))
    }

    /// Returns the immutable manifest declared by a registered strategy.
    #[must_use]
    pub fn manifest(&self, strategy_id: &str) -> Option<&StrategyManifest> {
        self.entries.get(strategy_id).map(|entry| &entry.manifest)
    }

    /// Returns registered strategy IDs in deterministic order for scheduler
    /// admission. The returned list is bounded by the host's package set.
    #[must_use]
    pub fn strategy_ids(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Resumes a quarantined strategy after external review.
    pub fn resume(&mut self, strategy_id: &str) -> bool {
        let Some(entry) = self.entries.get_mut(strategy_id) else {
            return false;
        };
        entry.state = State::Ready;
        entry.failures = 0;
        true
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use insider_common_types::{InstrumentId, MonoTime, ProposalId};
    use insider_strategy_sdk::{Action, Proposal, ProposalError, Strategy, StrategyContext};

    use super::{DiscoveryError, Host, HostError, Lifecycle, State, discover_strategy_packages};

    struct FixedStrategy {
        id: String,
        fail: bool,
    }

    impl Strategy for FixedStrategy {
        fn strategy_id(&self) -> &str {
            &self.id
        }
        fn evaluate(&self, context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
            if self.fail {
                return Err(ProposalError::InvalidAction);
            }
            let Some(proposal_id) = ProposalId::new(1).ok() else {
                return Err(ProposalError::MissingIdentity);
            };
            Ok(Proposal {
                proposal_id,
                strategy_id: self.id.clone(),
                instrument_id: context.instrument_id,
                action: Action::NoAction,
                confidence: 0.5,
                horizon_ns: 100,
                ttl_ns: 10,
                evidence: Vec::new(),
                generated_mono: context.now,
            })
        }
    }

    #[test]
    fn healthy_strategy_runs_while_failed_strategy_quarantines() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let mut host = Host::new(2);
        assert!(
            host.register(Arc::new(FixedStrategy {
                id: String::from("good.v1"),
                fail: false
            }))
            .is_ok()
        );
        assert!(
            host.register(Arc::new(FixedStrategy {
                id: String::from("bad.v1"),
                fail: true
            }))
            .is_ok()
        );
        let context = StrategyContext {
            now: MonoTime::from_nanos(1),
            instrument_id,
            metrics: &[],
        };
        assert!(host.evaluate("good.v1", &context).is_ok());
        assert!(matches!(
            host.evaluate("bad.v1", &context),
            Err(HostError::Evaluation(_))
        ));
        assert!(matches!(
            host.evaluate("bad.v1", &context),
            Err(HostError::Evaluation(_))
        ));
        assert_eq!(host.state("bad.v1"), Some(State::Quarantined));
        assert_eq!(host.state("good.v1"), Some(State::Ready));
    }

    #[test]
    fn discovery_is_recursive_deterministic_and_bounded() {
        let root = std::env::temp_dir().join(format!(
            "insidertrader-strategy-discovery-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        assert!(fs::create_dir_all(root.join("zeta")).is_ok());
        assert!(fs::create_dir_all(root.join("alpha/nested")).is_ok());
        let manifest = "strategy:\nid: \"demo.v1\"\nmode: deterministic\nhorizon_ns: 100\nttl_ns: 10\nperiod_ns: 20\ndeadline_ns: 5\npriority: fast\n";
        assert!(fs::write(root.join("zeta/strategy.manifest"), manifest).is_ok());
        assert!(
            fs::write(
                root.join("alpha/nested/strategy.manifest"),
                manifest.replace("demo.v1", "demo.v2")
            )
            .is_ok()
        );
        let Ok(discovered) = discover_strategy_packages(&root) else {
            let _ = fs::remove_dir_all(root);
            return;
        };
        assert_eq!(discovered.len(), 2);
        assert!(discovered[0].manifest_path < discovered[1].manifest_path);
        assert!(fs::create_dir_all(root.join("duplicate")).is_ok());
        assert!(fs::write(root.join("duplicate/strategy.manifest"), manifest).is_ok());
        assert!(matches!(
            discover_strategy_packages(&root),
            Err(DiscoveryError::Invalid { reason, .. }) if reason.contains("duplicate strategy id")
        ));
        let oversized = root.join("oversized");
        assert!(fs::create_dir_all(&oversized).is_ok());
        assert!(fs::write(oversized.join("strategy.manifest"), "x".repeat(1_048_577)).is_ok());
        assert!(matches!(
            discover_strategy_packages(&oversized),
            Err(DiscoveryError::BoundsExceeded {
                bound: "manifest_bytes",
                ..
            })
        ));
        let duplicate_field = root.join("duplicate-field");
        assert!(fs::create_dir_all(&duplicate_field).is_ok());
        assert!(
            fs::write(
                duplicate_field.join("strategy.manifest"),
                format!("{manifest}priority: normal\n")
            )
            .is_ok()
        );
        assert!(matches!(
            discover_strategy_packages(&duplicate_field),
            Err(DiscoveryError::Invalid { reason, .. })
                if reason.contains("duplicate manifest field")
        ));
        assert!(matches!(
            discover_strategy_packages(root.join("missing")),
            Err(DiscoveryError::Io(_))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn shadow_lifecycle_evaluates_without_becoming_live_eligible() {
        let Some(instrument_id) = InstrumentId::new(1).ok() else {
            return;
        };
        let mut host = Host::new(2);
        assert!(
            host.register(Arc::new(FixedStrategy {
                id: String::from("shadow.v1"),
                fail: false,
            }))
            .is_ok()
        );
        assert!(host.restore_lifecycle("shadow.v1", Lifecycle::Shadow));
        let context = StrategyContext {
            now: MonoTime::from_nanos(1),
            instrument_id,
            metrics: &[],
        };
        assert!(host.evaluate_shadow("shadow.v1", &context).is_ok());
        assert!(matches!(
            host.evaluate("shadow.v1", &context),
            Err(HostError::Unavailable(_))
        ));
    }
}
