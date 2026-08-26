//! Validated inference routing for active versioned models.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "model_runtime";

use std::collections::BTreeMap;
use std::sync::Arc;

use insider_model_registry::{ArtifactManifest, ModelRecord, Registry, RegistryError};

/// Validated model inference output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Output {
    /// Bounded model score.
    pub score: f64,
    /// Confidence in `[0,1]`.
    pub confidence: f64,
    /// Non-negative uncertainty estimate.
    pub uncertainty: f64,
}

/// Inference implementation supplied by Rust/Python/model adapters.
pub trait Model: Send + Sync {
    /// Computes one output from the declared feature vector.
    ///
    /// # Errors
    /// Returns an adapter-specific error when inference cannot complete.
    fn infer(&self, input: &[f64]) -> Result<Output, String>;
}

/// Runtime failure.
#[derive(Debug)]
pub enum RuntimeError {
    /// Registry state rejected the operation.
    Registry(RegistryError),
    /// No active implementation exists.
    NotActive,
    /// Input vector violates the registered schema.
    InvalidInput,
    /// Model returned an invalid output or execution error.
    Inference(String),
}

impl From<RegistryError> for RuntimeError {
    fn from(error: RegistryError) -> Self {
        Self::Registry(error)
    }
}

/// Registry-backed inference runtime.
#[derive(Default)]
pub struct Runtime {
    registry: Registry,
    models: BTreeMap<(String, String), Arc<dyn Model>>,
}

impl Runtime {
    /// Creates an empty model runtime.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers metadata and its executable implementation atomically.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] for invalid metadata or duplicate versions.
    pub fn register(
        &mut self,
        record: ModelRecord,
        model: Arc<dyn Model>,
    ) -> Result<(), RuntimeError> {
        let key = (record.model_id.clone(), record.version.clone());
        self.registry.register(record)?;
        self.models.insert(key, model);
        Ok(())
    }

    /// Registers a model implementation with immutable artifact provenance.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] when metadata, hashes, or version
    /// identity are invalid.
    pub fn register_verified(
        &mut self,
        record: ModelRecord,
        manifest: ArtifactManifest,
        model: Arc<dyn Model>,
    ) -> Result<(), RuntimeError> {
        let key = (record.model_id.clone(), record.version.clone());
        self.registry.register_verified(record, manifest)?;
        self.models.insert(key, model);
        Ok(())
    }

    /// Starts challenger traffic for a registered version.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] when the transition is invalid.
    pub fn start_challenger(&mut self, model_id: &str, version: &str) -> Result<(), RuntimeError> {
        self.registry
            .start_challenger(model_id, version)
            .map_err(Into::into)
    }

    /// Records validation evidence for a Research model.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] when the lifecycle or evidence is
    /// invalid.
    pub fn validate(
        &mut self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), RuntimeError> {
        self.registry
            .validate(model_id, version, evidence_id)
            .map_err(Into::into)
    }

    /// Starts shadow evaluation for a Validated model.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] for an invalid lifecycle transition.
    pub fn start_shadow(&mut self, model_id: &str, version: &str) -> Result<(), RuntimeError> {
        self.registry
            .start_shadow(model_id, version)
            .map_err(Into::into)
    }

    /// Starts bounded canary evaluation with explicit evidence.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] when the lifecycle or evidence is
    /// invalid.
    pub fn start_canary(
        &mut self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), RuntimeError> {
        self.registry
            .start_canary(model_id, version, evidence_id)
            .map_err(Into::into)
    }

    /// Promotes a challenger to active inference traffic.
    ///
    /// # Errors
    /// Returns [`RuntimeError::Registry`] when the transition is invalid.
    pub fn promote(&mut self, model_id: &str, version: &str) -> Result<(), RuntimeError> {
        self.registry.promote(model_id, version).map_err(Into::into)
    }

    /// Runs the active model after checking schema width and finite values.
    ///
    /// # Errors
    /// Returns [`RuntimeError`] for missing active versions, invalid input, or
    /// invalid model output.
    pub fn infer(&self, model_id: &str, input: &[f64]) -> Result<Output, RuntimeError> {
        let record = self
            .registry
            .active(model_id)
            .ok_or(RuntimeError::NotActive)?;
        if input.len() != record.input_width || input.iter().any(|value| !value.is_finite()) {
            return Err(RuntimeError::InvalidInput);
        }
        let model = self
            .models
            .get(&(record.model_id.clone(), record.version.clone()))
            .ok_or(RuntimeError::NotActive)?;
        let output = model.infer(input).map_err(RuntimeError::Inference)?;
        if !output.score.is_finite()
            || !output.confidence.is_finite()
            || !output.uncertainty.is_finite()
            || !(0.0..=1.0).contains(&output.confidence)
            || output.uncertainty < 0.0
        {
            return Err(RuntimeError::Inference("invalid model output".into()));
        }
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::{ArtifactManifest, Model, ModelRecord, Output, Runtime, SUBSYSTEM_ID};
    use std::sync::Arc;

    struct SumModel;
    impl Model for SumModel {
        fn infer(&self, input: &[f64]) -> Result<Output, String> {
            Ok(Output {
                score: input.iter().sum(),
                confidence: 0.8,
                uncertainty: 0.1,
            })
        }
    }

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn runtime_routes_only_promoted_version_and_checks_width() {
        let record = ModelRecord {
            model_id: "sum".into(),
            version: "1".into(),
            artifact_hash: "a".into(),
            input_schema_hash: "i".into(),
            output_schema_hash: "o".into(),
            input_width: 2,
            status: insider_model_registry::Status::Candidate,
        };
        let mut runtime = Runtime::new();
        let manifest = ArtifactManifest {
            code_hash: "code".into(),
            training_data_hash: "data".into(),
            config_hash: "config".into(),
            feature_hash: "features".into(),
            calibration_hash: "calibration".into(),
            artifact_hash: "a".into(),
        };
        assert!(
            runtime
                .register_verified(record, manifest, Arc::new(SumModel))
                .is_ok()
        );
        assert!(runtime.infer("sum", &[1.0, 2.0]).is_err());
        assert!(runtime.start_challenger("sum", "1").is_ok());
        assert!(runtime.start_canary("sum", "1", "shadow-evidence").is_ok());
        assert!(runtime.promote("sum", "1").is_ok());
        assert_eq!(
            runtime
                .infer("sum", &[1.0, 2.0])
                .ok()
                .map(|output| output.score),
            Some(3.0)
        );
        assert!(runtime.infer("sum", &[1.0]).is_err());
    }
}
