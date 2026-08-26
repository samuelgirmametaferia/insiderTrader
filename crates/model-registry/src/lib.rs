//! Versioned model registry with explicit challenger promotion.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "model_registry";

use std::collections::BTreeMap;

/// Model lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    /// Registered research artifact with no promotion evidence yet.
    Research,
    /// Passed validation and is eligible for shadow evaluation.
    Validated,
    /// Receiving shadow/evaluation traffic without production impact.
    Shadow,
    /// Receiving bounded canary traffic under explicit limits.
    Canary,
    /// Selected for production inference.
    Production,
    /// No longer eligible for new inference.
    Retired,
}

impl Status {
    /// Compatibility name for pre-promotion registry clients.
    #[allow(non_upper_case_globals)]
    pub const Candidate: Self = Self::Research;
    /// Compatibility name for pre-promotion registry clients.
    #[allow(non_upper_case_globals)]
    pub const Challenger: Self = Self::Shadow;
    /// Compatibility name for pre-promotion registry clients.
    #[allow(non_upper_case_globals)]
    pub const Active: Self = Self::Production;
}

/// Immutable model metadata and provenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelRecord {
    /// Stable logical model name.
    pub model_id: String,
    /// Immutable semantic version.
    pub version: String,
    /// Artifact content hash.
    pub artifact_hash: String,
    /// Feature/input schema hash.
    pub input_schema_hash: String,
    /// Output schema hash.
    pub output_schema_hash: String,
    /// Expected feature vector width.
    pub input_width: usize,
    /// Lifecycle state.
    pub status: Status,
}

/// Immutable provenance bundle for a model artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactManifest {
    /// Hash of the executable/model code or container image.
    pub code_hash: String,
    /// Hash of the training dataset snapshot.
    pub training_data_hash: String,
    /// Hash of model parameters and calibration configuration.
    pub config_hash: String,
    /// Hash of the feature definition and preprocessing graph.
    pub feature_hash: String,
    /// Hash of calibration/validation artifacts.
    pub calibration_hash: String,
    /// Aggregate artifact hash recorded in [`ModelRecord`].
    pub artifact_hash: String,
}

/// Durable registry image used by the journal/read-model layer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegistrySnapshot {
    /// All immutable model records in deterministic key order.
    pub records: Vec<ModelRecord>,
    /// Provenance manifests paired by model ID and version.
    pub manifests: Vec<((String, String), ArtifactManifest)>,
    /// Active production version per logical model.
    pub active: Vec<(String, String)>,
}

impl ArtifactManifest {
    /// Validates that every immutable provenance component is present.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidMetadata`] for blank or mismatched
    /// aggregate identity.
    pub fn validate_for(&self, record: &ModelRecord) -> Result<(), RegistryError> {
        if self.artifact_hash != record.artifact_hash
            || [
                self.code_hash.as_str(),
                self.training_data_hash.as_str(),
                self.config_hash.as_str(),
                self.feature_hash.as_str(),
                self.calibration_hash.as_str(),
                self.artifact_hash.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty())
        {
            return Err(RegistryError::InvalidMetadata);
        }
        Ok(())
    }
}

/// Registry mutation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// Required identity or hash is blank.
    InvalidMetadata,
    /// Model version already exists.
    Duplicate,
    /// Requested model/version is absent.
    NotFound,
    /// Promotion is not legal from the current state.
    InvalidTransition,
    /// A promotion step requires non-empty operator/evidence authorization.
    AuthorizationRequired,
}

/// In-memory registry; persistence is supplied by the journal/service host.
#[derive(Clone, Default)]
pub struct Registry {
    records: BTreeMap<(String, String), ModelRecord>,
    manifests: BTreeMap<(String, String), ArtifactManifest>,
    active: BTreeMap<String, String>,
}

impl Registry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Exports a deterministic immutable registry image.
    #[must_use]
    pub fn snapshot(&self) -> RegistrySnapshot {
        RegistrySnapshot {
            records: self.records.values().cloned().collect(),
            manifests: self
                .manifests
                .iter()
                .map(|(key, manifest)| (key.clone(), manifest.clone()))
                .collect(),
            active: self
                .active
                .iter()
                .map(|(model, version)| (model.clone(), version.clone()))
                .collect(),
        }
    }

    /// Restores a registry image atomically after validating all references.
    ///
    /// # Errors
    /// Returns [`RegistryError`] when records are invalid/duplicated, manifests
    /// do not match registered records, or active pointers are not production
    /// records. The existing registry remains unchanged on failure.
    pub fn restore_snapshot(&mut self, snapshot: RegistrySnapshot) -> Result<(), RegistryError> {
        let mut candidate = Self::new();
        for record in snapshot.records {
            let status = record.status;
            let mut registration = record.clone();
            registration.status = Status::Research;
            candidate.register(registration)?;
            if let Some(stored) = candidate
                .records
                .get_mut(&(record.model_id, record.version))
            {
                stored.status = status;
            }
        }
        for ((model_id, version), manifest) in snapshot.manifests {
            let Some(record) = candidate.records.get(&(model_id.clone(), version.clone())) else {
                return Err(RegistryError::NotFound);
            };
            manifest.validate_for(record)?;
            if candidate
                .manifests
                .insert((model_id, version), manifest)
                .is_some()
            {
                return Err(RegistryError::Duplicate);
            }
        }
        // A production record is executable authority, so a snapshot that
        // omits its immutable provenance must never become active after a
        // restart. Research/validated records may be staged without a
        // manifest, but production state is fail-closed.
        if candidate.records.iter().any(|(key, record)| {
            record.status == Status::Production && !candidate.manifests.contains_key(key)
        }) {
            return Err(RegistryError::InvalidMetadata);
        }
        for (model_id, version) in snapshot.active {
            let Some(record) = candidate.records.get(&(model_id.clone(), version.clone())) else {
                return Err(RegistryError::NotFound);
            };
            if record.status != Status::Production {
                return Err(RegistryError::InvalidTransition);
            }
            if candidate.active.insert(model_id, version).is_some() {
                return Err(RegistryError::Duplicate);
            }
        }
        self.records = candidate.records;
        self.manifests = candidate.manifests;
        self.active = candidate.active;
        Ok(())
    }

    /// Registers an immutable model record.
    ///
    /// # Errors
    /// Returns [`RegistryError`] for invalid metadata or duplicate versions.
    pub fn register(&mut self, record: ModelRecord) -> Result<(), RegistryError> {
        if record.model_id.trim().is_empty()
            || record.version.trim().is_empty()
            || record.artifact_hash.trim().is_empty()
            || record.input_schema_hash.trim().is_empty()
            || record.output_schema_hash.trim().is_empty()
            || record.input_width == 0
        {
            return Err(RegistryError::InvalidMetadata);
        }
        if record.status != Status::Research {
            return Err(RegistryError::InvalidTransition);
        }
        let key = (record.model_id.clone(), record.version.clone());
        if self.records.contains_key(&key) {
            return Err(RegistryError::Duplicate);
        }
        self.records.insert(key, record);
        Ok(())
    }

    /// Registers a model together with its immutable provenance bundle.
    ///
    /// This is the production registration path. The record is inserted only
    /// after all code/data/config/feature/calibration identities match.
    ///
    /// # Errors
    /// Returns [`RegistryError`] for invalid metadata, duplicate versions, or
    /// a mismatched artifact aggregate hash.
    pub fn register_verified(
        &mut self,
        record: ModelRecord,
        manifest: ArtifactManifest,
    ) -> Result<(), RegistryError> {
        manifest.validate_for(&record)?;
        let key = (record.model_id.clone(), record.version.clone());
        self.register(record)?;
        self.manifests.insert(key, manifest);
        Ok(())
    }

    /// Marks a validated artifact as shadow-eligible.
    ///
    /// # Errors
    /// Returns [`RegistryError`] when the version is absent or not a candidate.
    pub fn start_challenger(&mut self, model_id: &str, version: &str) -> Result<(), RegistryError> {
        self.validate(model_id, version, "legacy-validation")?;
        self.start_shadow(model_id, version)
    }

    /// Records validation evidence and moves Research to Validated.
    ///
    /// # Errors
    /// Returns [`RegistryError::AuthorizationRequired`] when evidence is blank
    /// or [`RegistryError::InvalidTransition`] when the version is not Research.
    pub fn validate(
        &mut self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), RegistryError> {
        if evidence_id.trim().is_empty() {
            return Err(RegistryError::AuthorizationRequired);
        }
        let record = self
            .records
            .get_mut(&(model_id.to_owned(), version.to_owned()))
            .ok_or(RegistryError::NotFound)?;
        if record.status != Status::Research {
            return Err(RegistryError::InvalidTransition);
        }
        record.status = Status::Validated;
        Ok(())
    }

    /// Starts shadow evaluation after validation.
    ///
    /// # Errors
    /// Returns [`RegistryError::InvalidTransition`] unless the version is
    /// Validated.
    pub fn start_shadow(&mut self, model_id: &str, version: &str) -> Result<(), RegistryError> {
        let record = self
            .records
            .get_mut(&(model_id.to_owned(), version.to_owned()))
            .ok_or(RegistryError::NotFound)?;
        if record.status != Status::Validated {
            return Err(RegistryError::InvalidTransition);
        }
        record.status = Status::Shadow;
        Ok(())
    }

    /// Starts bounded canary evaluation after shadow validation.
    ///
    /// # Errors
    /// Returns [`RegistryError::AuthorizationRequired`] for blank evidence or
    /// [`RegistryError::InvalidTransition`] for an incorrect lifecycle state.
    pub fn start_canary(
        &mut self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), RegistryError> {
        if evidence_id.trim().is_empty() {
            return Err(RegistryError::AuthorizationRequired);
        }
        let record = self
            .records
            .get_mut(&(model_id.to_owned(), version.to_owned()))
            .ok_or(RegistryError::NotFound)?;
        if record.status != Status::Shadow {
            return Err(RegistryError::InvalidTransition);
        }
        record.status = Status::Canary;
        Ok(())
    }

    /// Promotes a challenger and retires the previous active version.
    ///
    /// # Errors
    /// Returns [`RegistryError`] when the challenger is absent or not eligible.
    pub fn promote(&mut self, model_id: &str, version: &str) -> Result<(), RegistryError> {
        let key = (model_id.to_owned(), version.to_owned());
        let record = self.records.get(&key).ok_or(RegistryError::NotFound)?;
        if record.status != Status::Canary {
            return Err(RegistryError::InvalidTransition);
        }
        if !self.manifests.contains_key(&key) {
            return Err(RegistryError::InvalidMetadata);
        }
        if let Some(previous) = self.active.insert(model_id.to_owned(), version.to_owned())
            && let Some(old) = self.records.get_mut(&(model_id.to_owned(), previous))
        {
            old.status = Status::Retired;
        }
        self.records
            .get_mut(&key)
            .ok_or(RegistryError::NotFound)?
            .status = Status::Production;
        Ok(())
    }

    /// Returns the active record for a logical model.
    #[must_use]
    pub fn active(&self, model_id: &str) -> Option<&ModelRecord> {
        let version = self.active.get(model_id)?;
        self.records.get(&(model_id.to_owned(), version.clone()))
    }

    /// Returns a registered record.
    #[must_use]
    pub fn get(&self, model_id: &str, version: &str) -> Option<&ModelRecord> {
        self.records.get(&(model_id.to_owned(), version.to_owned()))
    }

    /// Returns the immutable provenance manifest for a registered version.
    #[must_use]
    pub fn manifest(&self, model_id: &str, version: &str) -> Option<&ArtifactManifest> {
        self.manifests
            .get(&(model_id.to_owned(), version.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::{ArtifactManifest, ModelRecord, Registry, SUBSYSTEM_ID, Status};

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn challenger_promotion_retires_previous_active_version() {
        let record = |version: &str| ModelRecord {
            model_id: "alpha".into(),
            version: version.into(),
            artifact_hash: format!("artifact-{version}"),
            input_schema_hash: "in-v1".into(),
            output_schema_hash: "out-v1".into(),
            input_width: 4,
            status: Status::Candidate,
        };
        let manifest = |version: &str| ArtifactManifest {
            code_hash: format!("code-{version}"),
            training_data_hash: format!("data-{version}"),
            config_hash: format!("config-{version}"),
            feature_hash: format!("feature-{version}"),
            calibration_hash: format!("calibration-{version}"),
            artifact_hash: format!("artifact-{version}"),
        };
        let mut registry = Registry::new();
        assert!(
            registry
                .register_verified(record("1.0.0"), manifest("1.0.0"))
                .is_ok()
        );
        assert!(registry.start_challenger("alpha", "1.0.0").is_ok());
        assert!(
            registry
                .start_canary("alpha", "1.0.0", "shadow-evidence")
                .is_ok()
        );
        assert!(registry.promote("alpha", "1.0.0").is_ok());
        assert!(
            registry
                .register_verified(record("1.1.0"), manifest("1.1.0"))
                .is_ok()
        );
        assert!(registry.start_challenger("alpha", "1.1.0").is_ok());
        assert!(
            registry
                .start_canary("alpha", "1.1.0", "shadow-evidence-2")
                .is_ok()
        );
        assert!(registry.promote("alpha", "1.1.0").is_ok());
        assert_eq!(
            registry.active("alpha").map(|model| model.version.as_str()),
            Some("1.1.0")
        );
        assert_eq!(
            registry.get("alpha", "1.0.0").map(|model| model.status),
            Some(Status::Retired)
        );
    }

    #[test]
    fn restore_rejects_production_without_provenance_manifest() {
        let record = ModelRecord {
            model_id: "alpha".into(),
            version: "1.0.0".into(),
            artifact_hash: "artifact".into(),
            input_schema_hash: "input".into(),
            output_schema_hash: "output".into(),
            input_width: 1,
            status: Status::Production,
        };
        let snapshot = super::RegistrySnapshot {
            records: vec![record],
            manifests: vec![],
            active: vec![("alpha".into(), "1.0.0".into())],
        };
        let mut registry = Registry::new();
        assert_eq!(
            registry.restore_snapshot(snapshot),
            Err(super::RegistryError::InvalidMetadata)
        );
        assert!(registry.active("alpha").is_none());
    }

    #[test]
    fn unverified_record_cannot_be_promoted() {
        let record = ModelRecord {
            model_id: "alpha".into(),
            version: "1.0.0".into(),
            artifact_hash: "artifact".into(),
            input_schema_hash: "input".into(),
            output_schema_hash: "output".into(),
            input_width: 1,
            status: Status::Research,
        };
        let mut registry = Registry::new();
        assert!(registry.register(record).is_ok());
        assert!(registry.start_challenger("alpha", "1.0.0").is_ok());
        assert!(registry.start_canary("alpha", "1.0.0", "evidence").is_ok());
        assert_eq!(
            registry.promote("alpha", "1.0.0"),
            Err(super::RegistryError::InvalidMetadata)
        );
        assert!(registry.active("alpha").is_none());
    }
}
