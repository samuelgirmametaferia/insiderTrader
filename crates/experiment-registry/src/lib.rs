//! experiment-registry subsystem for `InsiderTrader`.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "experiment_registry";

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::PathBuf;

const MAX_BUNDLE_BYTES: u64 = 64 * 1024 * 1024;

/// Research run lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    /// Metadata registered but not started.
    Created,
    /// Work is executing.
    Running,
    /// Run completed and artifacts are eligible for comparison.
    Succeeded,
    /// Run failed and is not promotable.
    Failed,
    /// Run was intentionally cancelled.
    Cancelled,
}

/// Hash-addressed output artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Artifact {
    /// Artifact category, e.g. model or report.
    pub kind: String,
    /// Content hash.
    pub hash: String,
    /// Local/object-store path.
    pub path: String,
}

/// Decision and data lineage attached to an experiment run.
///
/// Every field is bounded so a malformed or accidentally unbounded research
/// payload cannot exhaust journal or registry memory.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExperimentProvenance {
    /// Strategy identity used by the run.
    pub strategy_id: Option<String>,
    /// Exact strategy version used by the run.
    pub strategy_version: Option<String>,
    /// Point-in-time news dataset snapshot hash.
    pub news_dataset_hash: Option<String>,
    /// News clustering algorithm/version.
    pub news_clustering_version: Option<String>,
    /// Context graph snapshot/version.
    pub graph_snapshot_version: Option<String>,
    /// LLM provider identifier, when applicable.
    pub llm_provider: Option<String>,
    /// LLM model identifier, when applicable.
    pub llm_model: Option<String>,
    /// Version of the prompt/template used.
    pub prompt_version: Option<String>,
    /// Version of the tool schema exposed to the model.
    pub tool_schema_version: Option<String>,
    /// Content IDs for cached LLM outputs consumed by the run.
    pub llm_cache_ids: Vec<String>,
    /// Hash of the autonomy policy/configuration, when applicable.
    pub autonomy_config_hash: Option<String>,
}

impl ExperimentProvenance {
    /// Validates bounded provenance values during journal replay.
    #[must_use]
    pub fn valid_for_replay(&self) -> bool {
        self.valid()
    }

    fn valid(&self) -> bool {
        const MAX_VALUE: usize = 512;
        const MAX_CACHE_IDS: usize = 256;
        let values = [
            &self.strategy_id,
            &self.strategy_version,
            &self.news_dataset_hash,
            &self.news_clustering_version,
            &self.graph_snapshot_version,
            &self.llm_provider,
            &self.llm_model,
            &self.prompt_version,
            &self.tool_schema_version,
            &self.autonomy_config_hash,
        ];
        values.iter().all(|value| {
            value
                .as_ref()
                .is_none_or(|text| !text.trim().is_empty() && text.len() <= MAX_VALUE)
        }) && self.llm_cache_ids.len() <= MAX_CACHE_IDS
            && self
                .llm_cache_ids
                .iter()
                .all(|id| !id.trim().is_empty() && id.len() <= MAX_VALUE)
            && self.llm_cache_ids.windows(2).all(|pair| pair[0] < pair[1])
    }
}

/// Complete immutable provenance for a research run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExperimentBundle {
    /// Stable experiment identifier.
    pub run_id: String,
    /// Source revision hash.
    pub code_hash: String,
    /// Resolved configuration hash.
    pub config_hash: String,
    /// Point-in-time data snapshot hash.
    pub dataset_hash: String,
    /// Schema hashes used to decode the run.
    pub schema_hashes: BTreeMap<String, String>,
    /// Model artifact hashes used by the run.
    pub model_hashes: BTreeMap<String, String>,
    /// Prompt/template hashes used by the run.
    pub prompt_hashes: BTreeMap<String, String>,
    /// Environment identity (runtime, OS, compiler, dependency lock hash, etc.).
    pub environment: BTreeMap<String, String>,
    /// Exact command and arguments used to produce the run.
    pub command: Vec<String>,
    /// Explicit deterministic seed.
    pub seed: u64,
    /// Output artifacts and their content hashes.
    pub artifacts: Vec<Artifact>,
    /// Hash of the canonical report payload.
    pub report_hash: String,
}

impl ExperimentBundle {
    /// Validates required provenance fields and artifact hashes.
    fn validate(&self) -> Result<(), BundleError> {
        const MAX_FIELD_BYTES: usize = 4_096;
        const MAX_MAP_ENTRIES: usize = 2_048;
        const MAX_COMMAND_ARGS: usize = 512;
        const MAX_ARTIFACTS: usize = 4_096;
        let valid_text = |value: &str| {
            !value.trim().is_empty()
                && value.len() <= MAX_FIELD_BYTES
                && !value.chars().any(char::is_control)
        };
        let required = [
            &self.run_id,
            &self.code_hash,
            &self.config_hash,
            &self.dataset_hash,
            &self.report_hash,
        ];
        if required.iter().any(|value| !valid_text(value))
            || self.command.is_empty()
            || self.command.len() > MAX_COMMAND_ARGS
            || self.command.iter().any(|value| !valid_text(value))
            || self.schema_hashes.len() > MAX_MAP_ENTRIES
            || self.model_hashes.len() > MAX_MAP_ENTRIES
            || self.prompt_hashes.len() > MAX_MAP_ENTRIES
            || self.environment.len() > MAX_MAP_ENTRIES
            || self
                .schema_hashes
                .iter()
                .any(|(key, value)| !valid_text(key) || !valid_text(value))
            || self
                .model_hashes
                .iter()
                .any(|(key, value)| !valid_text(key) || !valid_text(value))
            || self
                .prompt_hashes
                .iter()
                .any(|(key, value)| !valid_text(key) || !valid_text(value))
            || self
                .environment
                .iter()
                .any(|(key, value)| !valid_text(key) || !valid_text(value))
            || self.artifacts.len() > MAX_ARTIFACTS
            || self.artifacts.iter().any(|artifact| {
                !valid_text(&artifact.kind)
                    || !valid_text(&artifact.hash)
                    || !valid_text(&artifact.path)
            })
        {
            return Err(BundleError::InvalidMetadata);
        }
        Ok(())
    }

    /// Encodes the bundle into a deterministic, line-oriented manifest.
    ///
    /// Keys are sorted by `BTreeMap`; artifact order is part of the manifest and
    /// must therefore be stable for a given run.
    ///
    /// # Errors
    /// Returns [`BundleError::InvalidMetadata`] when required provenance is absent.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, BundleError> {
        self.validate()?;
        let mut output = String::new();
        let mut line = |key: &str, value: &str| {
            output.push_str(key);
            output.push('=');
            output.push_str(value);
            output.push('\n');
        };
        line("format", "insidertrader-experiment-bundle-v1");
        line("run_id", &self.run_id);
        line("code_hash", &self.code_hash);
        line("config_hash", &self.config_hash);
        line("dataset_hash", &self.dataset_hash);
        line("seed", &self.seed.to_string());
        line("report_hash", &self.report_hash);
        for (key, value) in &self.schema_hashes {
            line(&format!("schema.{key}"), value);
        }
        for (key, value) in &self.model_hashes {
            line(&format!("model.{key}"), value);
        }
        for (key, value) in &self.prompt_hashes {
            line(&format!("prompt.{key}"), value);
        }
        for (key, value) in &self.environment {
            line(&format!("environment.{key}"), value);
        }
        for (index, argument) in self.command.iter().enumerate() {
            line(&format!("command.{index}"), argument);
        }
        for (index, artifact) in self.artifacts.iter().enumerate() {
            line(&format!("artifact.{index}.kind"), &artifact.kind);
            line(&format!("artifact.{index}.hash"), &artifact.hash);
            line(&format!("artifact.{index}.path"), &artifact.path);
        }
        Ok(output.into_bytes())
    }

    /// Computes the content address of this canonical bundle.
    ///
    /// # Errors
    /// Returns [`BundleError::InvalidMetadata`] when validation fails.
    pub fn content_hash(&self) -> Result<String, BundleError> {
        Ok(insider_journal::hex_digest(&insider_journal::sha256(
            &self.canonical_bytes()?,
        )))
    }
}

/// Durable content-addressed experiment bundle store.
#[derive(Clone, Debug)]
pub struct BundleStore {
    root: PathBuf,
}

impl BundleStore {
    /// Creates a store rooted at an existing or newly-created directory.
    ///
    /// # Errors
    /// Returns [`BundleError::Io`] if the directory cannot be created.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, BundleError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(BundleError::Io)?;
        Ok(Self { root })
    }

    /// Writes one immutable bundle and returns its content hash.
    ///
    /// Existing bundle paths are never overwritten. Publication uses a temporary
    /// file plus an atomic rename, so readers observe either no bundle or a full
    /// manifest and its digest sidecar.
    ///
    /// # Errors
    /// Returns [`BundleError`] when validation, I/O, or immutability checks fail.
    pub fn publish(&self, bundle: &ExperimentBundle) -> Result<String, BundleError> {
        let bytes = bundle.canonical_bytes()?;
        if bytes.len() as u64 > MAX_BUNDLE_BYTES {
            return Err(BundleError::TooLarge(bytes.len() as u64));
        }
        let hash = insider_journal::hex_digest(&insider_journal::sha256(&bytes));
        let destination = self.root.join(format!("{hash}.bundle"));
        let sidecar = self.root.join(format!("{hash}.sha256"));
        if destination.exists() || sidecar.exists() {
            return Err(BundleError::AlreadyExists);
        }
        let temporary = self.root.join(format!(".{hash}.bundle.tmp"));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(BundleError::Io)?;
        file.write_all(&bytes).map_err(BundleError::Io)?;
        file.sync_all().map_err(BundleError::Io)?;
        fs::rename(&temporary, &destination).map_err(|error| {
            let _ = fs::remove_file(&temporary);
            BundleError::Io(error)
        })?;
        let mut digest = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&sidecar)
            .map_err(BundleError::Io)?;
        digest
            .write_all(hash.as_bytes())
            .and_then(|()| digest.write_all(b"\n"))
            .and_then(|()| digest.sync_all())
            .map_err(BundleError::Io)?;
        File::open(&self.root)
            .and_then(|directory| directory.sync_all())
            .map_err(BundleError::Io)?;
        Ok(hash)
    }

    /// Verifies a published bundle and returns its canonical bytes.
    ///
    /// # Errors
    /// Returns [`BundleError::Corrupt`] if the manifest or digest sidecar differs.
    pub fn verify(&self, hash: &str) -> Result<Vec<u8>, BundleError> {
        if !is_content_hash(hash) {
            return Err(BundleError::InvalidMetadata);
        }
        let bytes = read_bounded_bundle(&self.root.join(format!("{hash}.bundle")))?;
        let actual = insider_journal::hex_digest(&insider_journal::sha256(&bytes));
        let expected = read_bounded_sidecar(&self.root.join(format!("{hash}.sha256")))?;
        if actual != hash || expected.trim() != hash {
            return Err(BundleError::Corrupt);
        }
        Ok(bytes)
    }

    /// Returns the on-disk manifest path for a content hash.
    #[must_use]
    pub fn manifest_path(&self, hash: &str) -> PathBuf {
        self.root.join(format!("{hash}.bundle"))
    }
}

fn is_content_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Failures publishing or validating immutable experiment bundles.
#[derive(Debug)]
pub enum BundleError {
    /// Filesystem operation failed.
    Io(io::Error),
    /// Required provenance is absent or malformed.
    InvalidMetadata,
    /// A content-addressed path already exists and cannot be overwritten.
    AlreadyExists,
    /// Manifest bytes and digest sidecar disagree.
    Corrupt,
    /// Bundle bytes exceed the immutable artifact bound.
    TooLarge(u64),
}

fn read_bounded_bundle(path: &std::path::Path) -> Result<Vec<u8>, BundleError> {
    let file = File::open(path).map_err(BundleError::Io)?;
    let size = file.metadata().map_err(BundleError::Io)?.len();
    if size > MAX_BUNDLE_BYTES {
        return Err(BundleError::TooLarge(size));
    }
    let mut bytes = Vec::new();
    file.take(MAX_BUNDLE_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(BundleError::Io)?;
    if bytes.len() as u64 > MAX_BUNDLE_BYTES {
        return Err(BundleError::TooLarge(bytes.len() as u64));
    }
    Ok(bytes)
}

fn read_bounded_sidecar(path: &std::path::Path) -> Result<String, BundleError> {
    const MAX_SIDECAR_BYTES: u64 = 256;
    let file = File::open(path).map_err(BundleError::Io)?;
    let mut bytes = Vec::new();
    file.take(MAX_SIDECAR_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(BundleError::Io)?;
    if bytes.len() as u64 > MAX_SIDECAR_BYTES {
        return Err(BundleError::Corrupt);
    }
    String::from_utf8(bytes).map_err(|_| BundleError::Corrupt)
}

/// Immutable experiment lineage and mutable lifecycle/results.
#[derive(Clone, Debug, PartialEq)]
pub struct ExperimentRun {
    /// Stable run identifier.
    pub run_id: String,
    /// Source/code revision hash.
    pub code_hash: String,
    /// Fully resolved configuration hash.
    pub config_hash: String,
    /// Point-in-time dataset snapshot hash.
    pub dataset_hash: String,
    /// Strategy/news/graph/LLM/autonomy lineage for this run.
    pub provenance: ExperimentProvenance,
    /// Current lifecycle status.
    pub status: RunStatus,
    /// Scalar metrics with stable names.
    pub metrics: BTreeMap<String, f64>,
    /// Produced artifacts.
    pub artifacts: Vec<Artifact>,
}

/// Registry mutation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// Required identity/hash is blank or metric is non-finite.
    InvalidMetadata,
    /// Run ID already exists.
    Duplicate,
    /// Run ID is unknown.
    NotFound,
    /// Lifecycle transition is invalid.
    InvalidTransition,
}

/// In-memory registry; callers persist changes through the journal.
#[derive(Clone, Default)]
pub struct Registry {
    runs: BTreeMap<String, ExperimentRun>,
}

impl Registry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a new run with immutable lineage hashes.
    ///
    /// # Errors
    /// Returns `RegistryError` for invalid metadata or duplicate IDs.
    pub fn create(&mut self, run: ExperimentRun) -> Result<(), RegistryError> {
        if run.run_id.trim().is_empty()
            || run.code_hash.trim().is_empty()
            || run.config_hash.trim().is_empty()
            || run.dataset_hash.trim().is_empty()
            || run.status != RunStatus::Created
            || !run.provenance.valid()
            || run.metrics.values().any(|value| !value.is_finite())
        {
            return Err(RegistryError::InvalidMetadata);
        }
        if self.runs.contains_key(&run.run_id) {
            return Err(RegistryError::Duplicate);
        }
        self.runs.insert(run.run_id.clone(), run);
        Ok(())
    }

    /// Advances a run into execution.
    ///
    /// # Errors
    /// Returns `RegistryError` when the run is absent or not created.
    pub fn start(&mut self, run_id: &str) -> Result<(), RegistryError> {
        let run = self.runs.get_mut(run_id).ok_or(RegistryError::NotFound)?;
        if run.status != RunStatus::Created {
            return Err(RegistryError::InvalidTransition);
        }
        run.status = RunStatus::Running;
        Ok(())
    }

    /// Completes a run successfully with finite metrics.
    ///
    /// # Errors
    /// Returns `RegistryError` when the run is not running or a metric is invalid.
    pub fn succeed(
        &mut self,
        run_id: &str,
        metrics: BTreeMap<String, f64>,
    ) -> Result<(), RegistryError> {
        if metrics.keys().any(|key| key.trim().is_empty())
            || metrics.values().any(|value| !value.is_finite())
        {
            return Err(RegistryError::InvalidMetadata);
        }
        let run = self.runs.get_mut(run_id).ok_or(RegistryError::NotFound)?;
        if run.status != RunStatus::Running {
            return Err(RegistryError::InvalidTransition);
        }
        run.metrics = metrics;
        run.status = RunStatus::Succeeded;
        Ok(())
    }

    /// Marks a running run failed.
    ///
    /// # Errors
    /// Returns `RegistryError` when the run is absent or not running.
    pub fn fail(&mut self, run_id: &str) -> Result<(), RegistryError> {
        let run = self.runs.get_mut(run_id).ok_or(RegistryError::NotFound)?;
        if run.status != RunStatus::Running {
            return Err(RegistryError::InvalidTransition);
        }
        run.status = RunStatus::Failed;
        Ok(())
    }

    /// Attaches an immutable artifact to a running or succeeded run.
    ///
    /// # Errors
    /// Returns `RegistryError` when metadata is invalid or lifecycle disallows artifacts.
    pub fn add_artifact(&mut self, run_id: &str, artifact: Artifact) -> Result<(), RegistryError> {
        if artifact.kind.trim().is_empty()
            || artifact.hash.trim().is_empty()
            || artifact.path.trim().is_empty()
        {
            return Err(RegistryError::InvalidMetadata);
        }
        let run = self.runs.get_mut(run_id).ok_or(RegistryError::NotFound)?;
        if !matches!(run.status, RunStatus::Running | RunStatus::Succeeded) {
            return Err(RegistryError::InvalidTransition);
        }
        if !run.artifacts.iter().any(|existing| existing == &artifact) {
            run.artifacts.push(artifact);
        }
        Ok(())
    }

    /// Returns one run by ID.
    #[must_use]
    pub fn get(&self, run_id: &str) -> Option<&ExperimentRun> {
        self.runs.get(run_id)
    }

    /// Returns every run in deterministic run-ID order.
    #[must_use]
    pub fn all(&self) -> Vec<ExperimentRun> {
        self.runs.values().cloned().collect()
    }

    /// Restores one previously journaled run after validating its complete
    /// immutable lineage and lifecycle projection.
    ///
    /// # Errors
    /// Returns [`RegistryError`] when the run is malformed. Replayed snapshots
    /// replace an earlier lifecycle projection for the same run ID.
    pub fn restore(&mut self, run: ExperimentRun) -> Result<(), RegistryError> {
        if run.run_id.trim().is_empty()
            || run.code_hash.trim().is_empty()
            || run.config_hash.trim().is_empty()
            || run.dataset_hash.trim().is_empty()
            || run.metrics.keys().any(|key| key.trim().is_empty())
            || run.metrics.values().any(|value| !value.is_finite())
            || run.artifacts.iter().any(|artifact| {
                artifact.kind.trim().is_empty()
                    || artifact.hash.trim().is_empty()
                    || artifact.path.trim().is_empty()
            })
        {
            return Err(RegistryError::InvalidMetadata);
        }
        self.runs.insert(run.run_id.clone(), run);
        Ok(())
    }

    /// Returns successful runs with a requested metric, sorted by run ID.
    #[must_use]
    pub fn successful_with_metric(&self, metric: &str) -> Vec<&ExperimentRun> {
        self.runs
            .values()
            .filter(|run| run.status == RunStatus::Succeeded && run.metrics.contains_key(metric))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Artifact, BundleStore, ExperimentBundle, ExperimentProvenance, ExperimentRun,
        MAX_BUNDLE_BYTES, Registry, RunStatus, SUBSYSTEM_ID,
    };
    use std::collections::BTreeMap;

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn bundle_publication_is_content_addressed_and_immutable() {
        let root =
            std::env::temp_dir().join(format!("insider-experiment-bundle-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = BundleStore::open(&root).ok();
        let Some(store) = store else {
            return;
        };
        let bundle = ExperimentBundle {
            run_id: "run-1".into(),
            code_hash: "code".into(),
            config_hash: "config".into(),
            dataset_hash: "data".into(),
            schema_hashes: BTreeMap::from([(String::from("market"), String::from("schema"))]),
            model_hashes: BTreeMap::new(),
            prompt_hashes: BTreeMap::new(),
            environment: BTreeMap::from([(String::from("rust"), String::from("1.98"))]),
            command: vec!["research".into(), "--seed".into(), "7".into()],
            seed: 7,
            artifacts: vec![Artifact {
                kind: "report".into(),
                hash: "report".into(),
                path: "report.json".into(),
            }],
            report_hash: "report".into(),
        };
        let hash = bundle.content_hash().ok();
        assert!(hash.is_some());
        let Some(hash) = hash else {
            return;
        };
        assert_eq!(store.publish(&bundle).ok(), Some(hash.clone()));
        assert!(store.verify(&hash).is_ok());
        assert!(std::fs::write(root.join(format!("{hash}.sha256")), "x".repeat(257)).is_ok());
        assert!(matches!(
            store.verify(&hash),
            Err(super::BundleError::Corrupt)
        ));
        assert!(matches!(
            store.verify("../outside"),
            Err(super::BundleError::InvalidMetadata)
        ));
        assert!(matches!(
            store.verify(&"A".repeat(64)),
            Err(super::BundleError::InvalidMetadata)
        ));
        assert!(store.publish(&bundle).is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn oversized_bundle_is_rejected_before_verification_buffering() {
        let root = std::env::temp_dir().join(format!(
            "insider-experiment-bundle-oversized-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        assert!(std::fs::create_dir_all(&root).is_ok());
        let hash = "0".repeat(64);
        let path = root.join(format!("{hash}.bundle"));
        let file = std::fs::File::create(&path).ok();
        assert!(file.is_some());
        if let Some(file) = file {
            assert!(file.set_len(MAX_BUNDLE_BYTES + 1).is_ok());
        }
        let store = BundleStore::open(&root).ok();
        assert!(store.is_some());
        assert!(matches!(
            store.and_then(|store| store.verify(&hash).err()),
            Some(super::BundleError::TooLarge(size)) if size == MAX_BUNDLE_BYTES + 1
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn oversized_bundle_metadata_is_rejected_before_serialization() {
        let mut bundle = ExperimentBundle {
            run_id: "run-oversized".into(),
            code_hash: "code".into(),
            config_hash: "config".into(),
            dataset_hash: "data".into(),
            schema_hashes: BTreeMap::new(),
            model_hashes: BTreeMap::new(),
            prompt_hashes: BTreeMap::new(),
            environment: BTreeMap::new(),
            command: vec!["research".into()],
            seed: 1,
            artifacts: Vec::new(),
            report_hash: "report".into(),
        };
        bundle.environment.insert("host".into(), "x".repeat(4_097));
        assert!(matches!(
            bundle.canonical_bytes(),
            Err(super::BundleError::InvalidMetadata)
        ));
    }

    #[test]
    fn control_characters_are_rejected_from_line_manifest_fields() {
        let mut bundle = ExperimentBundle {
            run_id: "run\nforged".into(),
            code_hash: "code".into(),
            config_hash: "config".into(),
            dataset_hash: "data".into(),
            schema_hashes: BTreeMap::new(),
            model_hashes: BTreeMap::new(),
            prompt_hashes: BTreeMap::new(),
            environment: BTreeMap::new(),
            command: vec!["research".into()],
            seed: 1,
            artifacts: Vec::new(),
            report_hash: "report".into(),
        };
        assert!(matches!(
            bundle.canonical_bytes(),
            Err(super::BundleError::InvalidMetadata)
        ));
        bundle.run_id = "run-1".into();
        bundle.command = vec!["research\r--unsafe".into()];
        assert!(matches!(
            bundle.canonical_bytes(),
            Err(super::BundleError::InvalidMetadata)
        ));
    }

    #[test]
    fn run_lineage_and_artifacts_survive_lifecycle_transitions() {
        let mut registry = Registry::new();
        assert!(
            registry
                .create(ExperimentRun {
                    run_id: "run-1".into(),
                    code_hash: "code".into(),
                    config_hash: "config".into(),
                    dataset_hash: "data".into(),
                    provenance: ExperimentProvenance::default(),
                    status: RunStatus::Created,
                    metrics: BTreeMap::new(),
                    artifacts: Vec::new()
                })
                .is_ok()
        );
        assert!(registry.start("run-1").is_ok());
        assert!(
            registry
                .add_artifact(
                    "run-1",
                    Artifact {
                        kind: "predictions".into(),
                        hash: "hash".into(),
                        path: "artifacts/predictions".into()
                    }
                )
                .is_ok()
        );
        assert!(
            registry
                .succeed("run-1", BTreeMap::from([("sharpe".into(), 1.2)]))
                .is_ok()
        );
        assert_eq!(
            registry.get("run-1").map(|run| run.status),
            Some(RunStatus::Succeeded)
        );
        assert_eq!(registry.successful_with_metric("sharpe").len(), 1);
    }
}
