//! ensemble subsystem for `InsiderTrader`.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "ensemble";

use std::f64;

/// One validated member contribution.
#[derive(Clone, Debug, PartialEq)]
pub struct Member {
    /// Stable metric/model identifier.
    pub id: String,
    /// Bounded or model-defined score.
    pub score: f64,
    /// Member confidence in the unit interval.
    pub confidence: f64,
    /// Non-negative member uncertainty.
    pub uncertainty: f64,
    /// Positive allocation weight.
    pub weight: f64,
}

/// Combined ensemble output.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Output {
    /// Weighted score.
    pub score: f64,
    /// Confidence reduced by disagreement.
    pub confidence: f64,
    /// Weighted uncertainty plus score disagreement.
    pub uncertainty: f64,
    /// Number of members included.
    pub member_count: usize,
}

/// Ensemble failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EnsembleError {
    /// No members were supplied.
    Empty,
    /// A member contains invalid numeric data or identity.
    InvalidMember,
    /// Weights cannot be normalized safely.
    InvalidWeights,
}

/// Combines member outputs with positive weights.
///
/// # Errors
/// Returns `EnsembleError` for empty members, invalid values, or unusable weights.
pub fn combine(members: &[Member]) -> Result<Output, EnsembleError> {
    if members.is_empty() {
        return Err(EnsembleError::Empty);
    }
    if members.iter().any(|member| {
        member.id.trim().is_empty()
            || !member.score.is_finite()
            || !member.confidence.is_finite()
            || !member.uncertainty.is_finite()
            || !(0.0..=1.0).contains(&member.confidence)
            || member.uncertainty < 0.0
            || !member.weight.is_finite()
            || member.weight <= 0.0
    }) {
        return Err(EnsembleError::InvalidMember);
    }
    let weight_sum = members.iter().map(|member| member.weight).sum::<f64>();
    if !weight_sum.is_finite() || weight_sum <= 0.0 {
        return Err(EnsembleError::InvalidWeights);
    }
    let score = members
        .iter()
        .map(|member| member.score * member.weight)
        .sum::<f64>()
        / weight_sum;
    let confidence = members
        .iter()
        .map(|member| member.confidence * member.weight)
        .sum::<f64>()
        / weight_sum;
    let base_uncertainty = members
        .iter()
        .map(|member| member.uncertainty * member.weight)
        .sum::<f64>()
        / weight_sum;
    let disagreement = members
        .iter()
        .map(|member| member.weight * (member.score - score).powi(2))
        .sum::<f64>()
        / weight_sum;
    let disagreement = disagreement.sqrt();
    let uncertainty = (base_uncertainty + disagreement).max(0.0);
    let confidence = (confidence * (1.0 - disagreement.abs().min(1.0))).clamp(0.0, 1.0);
    if !score.is_finite() || !uncertainty.is_finite() || !confidence.is_finite() {
        return Err(EnsembleError::InvalidWeights);
    }
    Ok(Output {
        score,
        confidence,
        uncertainty,
        member_count: members.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::{Member, SUBSYSTEM_ID, combine};

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn ensemble_combines_scores_and_penalizes_disagreement() {
        let members = [
            Member {
                id: "fast".into(),
                score: 1.0,
                confidence: 0.9,
                uncertainty: 0.1,
                weight: 1.0,
            },
            Member {
                id: "slow".into(),
                score: 0.0,
                confidence: 0.8,
                uncertainty: 0.2,
                weight: 1.0,
            },
        ];
        let output = combine(&members);
        assert!(output.is_ok_and(|output| output.member_count == 2
            && (output.score - 0.5).abs() < f64::EPSILON
            && output.uncertainty > 0.4
            && output.confidence < 0.9));
    }
}
