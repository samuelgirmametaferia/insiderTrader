//! Deterministic proposal collection, conflict reporting, and resolution.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};

use insider_common_types::{InstrumentId, ProposalId};
use insider_strategy_sdk::{Action, Proposal, ProposalError, StrategyManifest};

/// Conflict resolution policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Policy {
    /// Keep every valid proposal and expose conflicts to the portfolio optimizer.
    IsolatedBooks,
    /// Select the lexicographically first strategy for opposing requests.
    Priority,
    /// Net signed quantity using confidence as the allocation weight.
    WeightedNet,
}

/// Opposing proposals for one instrument.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Conflict {
    /// Instrument with opposing requests.
    pub instrument_id: InstrumentId,
    /// Proposal IDs involved in the conflict.
    pub proposal_ids: Vec<ProposalId>,
}

/// Attribution from one resolved proposal to its immutable source proposals.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribution {
    /// Resulting proposal ID.
    pub result_proposal_id: ProposalId,
    /// Instrument represented by the result.
    pub instrument_id: InstrumentId,
    /// Source IDs in deterministic strategy/ID order.
    pub source_proposal_ids: Vec<ProposalId>,
}

/// Coordinator output retains both accepted proposals and diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub struct ResultSet {
    /// Proposals allowed through the configured policy.
    pub accepted: Vec<Proposal>,
    /// Explicit conflicts observed during resolution.
    pub conflicts: Vec<Conflict>,
    /// Proposals that were dropped because their TTL had expired at resolve time.
    pub expired: Vec<ProposalId>,
    /// Source mapping for every accepted result, including synthetic netted
    /// proposals.
    pub attributions: Vec<Attribution>,
}

/// Hard per-strategy quantity budget applied during proposal resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StrategyBudget {
    /// Maximum aggregate absolute quantity the strategy may request in one
    /// resolution cycle. Closing an existing position is never blocked.
    pub max_abs_quantity_ticks: i64,
}

impl StrategyBudget {
    /// Creates a valid positive budget.
    #[must_use]
    pub const fn new(max_abs_quantity_ticks: i64) -> Option<Self> {
        if max_abs_quantity_ticks > 0 {
            Some(Self {
                max_abs_quantity_ticks,
            })
        } else {
            None
        }
    }
}

/// One deterministic budget resize made during resolution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetAdjustment {
    /// Proposal that was resized.
    pub proposal_id: ProposalId,
    /// Original signed quantity or delta represented by the proposal.
    pub before_quantity_ticks: i64,
    /// Bounded signed quantity or delta after the budget was applied.
    pub after_quantity_ticks: i64,
}

/// Resolution result with explicit per-strategy budget diagnostics.
#[derive(Clone, Debug, PartialEq)]
pub struct BudgetedResultSet {
    /// Normal conflict/lifecycle resolution output.
    pub result: ResultSet,
    /// Every proposal whose requested quantity was reduced.
    pub adjustments: Vec<BudgetAdjustment>,
}

/// Lifecycle of an immutable proposal record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProposalState {
    /// Accepted into the coordinator result for this resolution cycle.
    Accepted,
    /// Waiting for the next resolution cycle.
    Pending,
    /// Rejected by policy or invalidated by a conflict.
    Rejected,
    /// Replaced by a netted or higher-priority proposal.
    Superseded,
    /// No longer valid at the injected decision time.
    Expired,
}

/// Immutable proposal plus its coordinator lifecycle state.
#[derive(Clone, Debug, PartialEq)]
pub struct ProposalRecord {
    /// Original proposal bytes represented by the typed object.
    pub proposal: Proposal,
    /// Current lifecycle state.
    pub state: ProposalState,
}

/// Coordinator submission failure.
#[derive(Clone, Debug, PartialEq)]
pub enum SubmitError {
    /// The proposal ID already exists in the immutable store.
    DuplicateProposal(ProposalId),
    /// Proposal failed SDK validation at the supplied decision time.
    InvalidProposal(ProposalError),
}

/// Failure while applying a proposal to a strategy virtual book.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AllocationError {
    /// The proposal does not identify a strategy.
    MissingStrategy,
    /// The action needs portfolio equity/mark data and cannot be applied by
    /// the quantity-only virtual-book boundary.
    TargetWeightUnsupported,
    /// Equity or mark context was missing, non-positive, non-finite, or
    /// produced a quantity outside canonical integer bounds.
    InvalidWeightContext,
    /// `NoAction` does not change a virtual book.
    NoAction,
    /// The resulting signed position exceeded canonical quantity bounds.
    Overflow,
}

/// One immutable virtual-book position change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualBookChange {
    /// Source proposal identity.
    pub proposal_id: ProposalId,
    /// Strategy virtual-book identity.
    pub strategy_id: String,
    /// Instrument changed.
    pub instrument_id: InstrumentId,
    /// Position before applying the proposal.
    pub before_quantity_ticks: i64,
    /// Position after applying the proposal.
    pub after_quantity_ticks: i64,
}

/// Deterministic per-strategy virtual position ledger.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct VirtualBookLedger {
    positions: BTreeMap<(String, InstrumentId), i64>,
}

impl VirtualBookLedger {
    /// Creates an empty virtual-book ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies one proposal to its strategy book without touching live
    /// portfolio or broker state.
    ///
    /// # Errors
    /// Returns [`AllocationError`] when the action cannot be represented by a
    /// quantity-only book or arithmetic would overflow.
    pub fn apply(&mut self, proposal: &Proposal) -> Result<VirtualBookChange, AllocationError> {
        self.apply_with_context(proposal, None)
    }

    /// Applies one proposal with optional portfolio context for target weights.
    ///
    /// `equity_ticks` is reporting-currency equity and `mark_price_ticks` is
    /// the trusted instrument mark. The conversion is rounded to the nearest
    /// canonical quantity tick and is performed before mutating the ledger.
    ///
    /// # Errors
    /// Returns [`AllocationError`] when the action is invalid or the supplied
    /// weight context cannot produce a bounded quantity.
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    pub fn apply_with_context(
        &mut self,
        proposal: &Proposal,
        weight_context: Option<(i128, i64)>,
    ) -> Result<VirtualBookChange, AllocationError> {
        if proposal.strategy_id.trim().is_empty() {
            return Err(AllocationError::MissingStrategy);
        }
        let key = (proposal.strategy_id.clone(), proposal.instrument_id);
        let before = self.positions.get(&key).copied().unwrap_or(0);
        let after = match proposal.action {
            Action::NoAction => return Err(AllocationError::NoAction),
            Action::TargetQuantity { quantity_ticks } => quantity_ticks,
            Action::Increase { quantity_ticks } => before
                .checked_add(quantity_ticks)
                .ok_or(AllocationError::Overflow)?,
            Action::Decrease { quantity_ticks } => before
                .checked_sub(quantity_ticks)
                .ok_or(AllocationError::Overflow)?,
            Action::Close => 0,
            Action::TargetWeight { weight } => {
                let Some((equity_ticks, mark_price_ticks)) = weight_context else {
                    return Err(AllocationError::TargetWeightUnsupported);
                };
                if equity_ticks <= 0 || mark_price_ticks <= 0 || !weight.is_finite() {
                    return Err(AllocationError::InvalidWeightContext);
                }
                let quantity = (weight * (equity_ticks as f64) / (mark_price_ticks as f64)).round();
                if !quantity.is_finite() || quantity < i64::MIN as f64 || quantity > i64::MAX as f64
                {
                    return Err(AllocationError::InvalidWeightContext);
                }
                quantity as i64
            }
        };
        if after == 0 {
            self.positions.remove(&key);
        } else {
            self.positions.insert(key, after);
        }
        Ok(VirtualBookChange {
            proposal_id: proposal.proposal_id,
            strategy_id: proposal.strategy_id.clone(),
            instrument_id: proposal.instrument_id,
            before_quantity_ticks: before,
            after_quantity_ticks: after,
        })
    }

    /// Returns one strategy's current virtual position, defaulting to flat.
    #[must_use]
    pub fn position(&self, strategy_id: &str, instrument_id: InstrumentId) -> i64 {
        self.positions
            .get(&(strategy_id.to_owned(), instrument_id))
            .copied()
            .unwrap_or(0)
    }

    /// Returns all non-flat positions in deterministic strategy/instrument
    /// order.
    pub fn positions(&self) -> impl Iterator<Item = (&str, InstrumentId, i64)> {
        self.positions
            .iter()
            .map(|((strategy_id, instrument_id), quantity)| {
                (strategy_id.as_str(), *instrument_id, *quantity)
            })
    }
}

/// Failure while constructing the strategy dependency graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphError {
    /// A strategy was registered more than once.
    DuplicateNode(String),
    /// A declared dependency has not been registered.
    MissingDependency {
        /// Strategy declaring the dependency.
        strategy: String,
        /// Missing dependency ID.
        dependency: String,
    },
    /// A cycle prevents deterministic evaluation order; the path closes at its
    /// first repeated node.
    Cycle(Vec<String>),
}

/// Validated DAG of metric/strategy dependencies.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DependencyGraph {
    dependencies: BTreeMap<String, BTreeSet<String>>,
}

/// Immutable, validated package catalog used to assemble the runtime.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PackageCatalog {
    metric_ids: BTreeSet<String>,
    strategies: BTreeMap<String, StrategyManifest>,
    evaluation_order: Vec<String>,
}

impl PackageCatalog {
    /// Builds a catalog from discovered metric and strategy package manifests.
    /// All validation and dependency checks complete before the catalog is
    /// published, so callers never observe a partially loaded package set.
    ///
    /// # Errors
    /// Returns [`GraphError`] for duplicate IDs, missing metric references,
    /// missing strategy dependencies, or cycles.
    pub fn from_discovered(
        metrics: &[insider_metric_host::DiscoveredMetric],
        strategies: &[insider_strategy_host::DiscoveredStrategy],
    ) -> Result<Self, GraphError> {
        let mut metric_ids = BTreeSet::new();
        for metric in metrics {
            let id = metric.manifest.descriptor.metric_id.clone();
            if !metric_ids.insert(id.clone()) {
                return Err(GraphError::DuplicateNode(id));
            }
        }
        let mut strategy_map = BTreeMap::new();
        let mut graph = DependencyGraph::new();
        for strategy in strategies {
            let manifest = strategy.manifest.clone();
            if manifest
                .metric_ids
                .iter()
                .any(|metric_id| !metric_ids.contains(metric_id))
            {
                let missing = manifest
                    .metric_ids
                    .iter()
                    .find(|metric_id| !metric_ids.contains(*metric_id))
                    .cloned()
                    .unwrap_or_default();
                return Err(GraphError::MissingDependency {
                    strategy: manifest.strategy_id.clone(),
                    dependency: missing,
                });
            }
            if strategy_map
                .insert(manifest.strategy_id.clone(), manifest.clone())
                .is_some()
            {
                return Err(GraphError::DuplicateNode(manifest.strategy_id));
            }
            graph.register(
                manifest.strategy_id.clone(),
                manifest.strategy_dependencies.clone(),
            )?;
        }
        let evaluation_order = graph.topological_order()?;
        Ok(Self {
            metric_ids,
            strategies: strategy_map,
            evaluation_order,
        })
    }

    /// Returns whether a metric ID is present in the catalog.
    #[must_use]
    pub fn has_metric(&self, metric_id: &str) -> bool {
        self.metric_ids.contains(metric_id)
    }

    /// Returns a strategy manifest by immutable ID.
    #[must_use]
    pub fn strategy(&self, strategy_id: &str) -> Option<&StrategyManifest> {
        self.strategies.get(strategy_id)
    }

    /// Returns deterministic dependency order for strategy evaluation.
    #[must_use]
    pub fn evaluation_order(&self) -> &[String] {
        &self.evaluation_order
    }
}

impl DependencyGraph {
    /// Creates an empty dependency graph.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one strategy and its declared dependencies.
    ///
    /// Registration is atomic: invalid IDs or duplicate nodes do not modify
    /// the graph. Dependencies may be registered in any order, but the graph
    /// cannot be evaluated until every dependency has a node.
    ///
    /// # Errors
    /// Returns [`GraphError::DuplicateNode`] for a duplicate/blank strategy ID.
    pub fn register<I, S>(&mut self, strategy: S, dependencies: I) -> Result<(), GraphError>
    where
        I: IntoIterator,
        I::Item: Into<String>,
        S: Into<String>,
    {
        let strategy = strategy.into();
        if strategy.trim().is_empty() || self.dependencies.contains_key(&strategy) {
            return Err(GraphError::DuplicateNode(strategy));
        }
        let declared = dependencies
            .into_iter()
            .map(Into::into)
            .collect::<BTreeSet<_>>();
        if declared
            .iter()
            .any(|dependency| dependency.trim().is_empty())
        {
            return Err(GraphError::MissingDependency {
                strategy,
                dependency: String::new(),
            });
        }
        self.dependencies.insert(strategy, declared);
        Ok(())
    }

    /// Returns a deterministic topological evaluation order.
    ///
    /// # Errors
    /// Returns a missing-dependency or cycle error; no partial order is
    /// returned because executing one would be unsafe.
    pub fn topological_order(&self) -> Result<Vec<String>, GraphError> {
        for (strategy, dependencies) in &self.dependencies {
            for dependency in dependencies {
                if !self.dependencies.contains_key(dependency) {
                    return Err(GraphError::MissingDependency {
                        strategy: strategy.clone(),
                        dependency: dependency.clone(),
                    });
                }
            }
        }
        let mut state = BTreeMap::<String, VisitState>::new();
        let mut stack = Vec::new();
        let mut order = Vec::with_capacity(self.dependencies.len());
        for strategy in self.dependencies.keys() {
            visit_node(strategy, self, &mut state, &mut stack, &mut order)?;
        }
        Ok(order)
    }

    /// Returns the number of registered nodes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.dependencies.len()
    }

    /// Returns whether no nodes are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.dependencies.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum VisitState {
    Visiting,
    Visited,
}

fn visit_node(
    node: &str,
    graph: &DependencyGraph,
    state: &mut BTreeMap<String, VisitState>,
    stack: &mut Vec<String>,
    order: &mut Vec<String>,
) -> Result<(), GraphError> {
    match state.get(node) {
        Some(VisitState::Visited) => return Ok(()),
        Some(VisitState::Visiting) => {
            let start = stack.iter().position(|item| item == node).unwrap_or(0);
            let mut cycle = stack[start..].to_vec();
            cycle.push(node.to_owned());
            return Err(GraphError::Cycle(cycle));
        }
        None => {}
    }
    state.insert(node.to_owned(), VisitState::Visiting);
    stack.push(node.to_owned());
    if let Some(dependencies) = graph.dependencies.get(node) {
        for dependency in dependencies {
            visit_node(dependency, graph, state, stack, order)?;
        }
    }
    stack.pop();
    state.insert(node.to_owned(), VisitState::Visited);
    order.push(node.to_owned());
    Ok(())
}

/// Central proposal coordinator.
#[derive(Clone, Default)]
pub struct Coordinator {
    pending: Vec<Proposal>,
    graph: DependencyGraph,
    records: BTreeMap<ProposalId, ProposalRecord>,
}

impl Coordinator {
    /// Creates an empty coordinator.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a validated proposal for later resolution.
    pub fn submit(&mut self, proposal: Proposal) {
        if self.records.contains_key(&proposal.proposal_id) {
            return;
        }
        self.records.insert(
            proposal.proposal_id,
            ProposalRecord {
                proposal: proposal.clone(),
                state: ProposalState::Pending,
            },
        );
        self.pending.push(proposal);
    }

    /// Inserts one proposal into the immutable lifecycle store.
    ///
    /// # Errors
    /// Returns [`SubmitError`] for duplicate IDs or invalid proposal fields.
    pub fn submit_unique(
        &mut self,
        proposal: Proposal,
        now: insider_common_types::MonoTime,
    ) -> Result<(), SubmitError> {
        proposal
            .validate(now)
            .map_err(SubmitError::InvalidProposal)?;
        if self.records.contains_key(&proposal.proposal_id) {
            return Err(SubmitError::DuplicateProposal(proposal.proposal_id));
        }
        self.records.insert(
            proposal.proposal_id,
            ProposalRecord {
                proposal: proposal.clone(),
                state: ProposalState::Pending,
            },
        );
        self.pending.push(proposal);
        Ok(())
    }

    /// Registers a strategy dependency declaration.
    ///
    /// # Errors
    /// Returns [`GraphError`] when the declaration is duplicate or cannot form
    /// a complete acyclic graph.
    pub fn register_dependencies<I, S>(
        &mut self,
        strategy: S,
        dependencies: I,
    ) -> Result<(), GraphError>
    where
        I: IntoIterator,
        I::Item: Into<String>,
        S: Into<String>,
    {
        let mut candidate = self.graph.clone();
        candidate.register(strategy, dependencies)?;
        self.graph = candidate;
        Ok(())
    }

    /// Registers the strategy DAG edges declared by one validated manifest.
    ///
    /// The graph update is atomic; a missing dependency or cycle leaves the
    /// coordinator's previous graph unchanged.
    ///
    /// # Errors
    /// Returns [`GraphError`] when the manifest ID is invalid or its declared
    /// dependency graph cannot be evaluated safely.
    pub fn register_manifest(&mut self, manifest: &StrategyManifest) -> Result<(), GraphError> {
        manifest
            .validate()
            .map_err(|_| GraphError::DuplicateNode(manifest.strategy_id.clone()))?;
        self.register_dependencies(
            manifest.strategy_id.clone(),
            manifest.strategy_dependencies.clone(),
        )
    }

    /// Returns the validated evaluation order for registered strategies.
    ///
    /// # Errors
    /// Returns [`GraphError::MissingDependency`] or [`GraphError::Cycle`] if
    /// declarations are incomplete or cyclic.
    pub fn evaluation_order(&self) -> Result<Vec<String>, GraphError> {
        self.graph.topological_order()
    }

    /// Validates and queues one proposal against the injected decision time.
    ///
    /// # Errors
    /// Returns [`SubmitError`] without mutating the pending queue.
    pub fn submit_checked(
        &mut self,
        proposal: Proposal,
        now: insider_common_types::MonoTime,
    ) -> Result<(), SubmitError> {
        self.submit_unique(proposal, now)
    }

    /// Expires invalid/old proposals before deterministic conflict resolution.
    #[must_use]
    pub fn resolve_at(&mut self, policy: Policy, now: insider_common_types::MonoTime) -> ResultSet {
        let mut retained = Vec::with_capacity(self.pending.len());
        let mut expired = Vec::new();
        for proposal in self.pending.drain(..) {
            if proposal.validate(now).is_ok() {
                retained.push(proposal);
            } else {
                expired.push(proposal.proposal_id);
                if let Some(record) = self.records.get_mut(&proposal.proposal_id) {
                    record.state = ProposalState::Expired;
                }
            }
        }
        self.pending = retained;
        let mut result = self.resolve(policy);
        result.expired = expired;
        result
    }

    /// Resolves proposals and applies deterministic per-strategy quantity
    /// budgets after conflict policy resolution. Budgets are consumed in the
    /// coordinator's stable accepted order; absent strategies remain unlimited.
    /// Close actions are preserved because reducing exposure is a safe action.
    #[must_use]
    pub fn resolve_at_with_budgets(
        &mut self,
        policy: Policy,
        now: insider_common_types::MonoTime,
        budgets: &BTreeMap<String, StrategyBudget>,
    ) -> BudgetedResultSet {
        let mut result = self.resolve_at(policy, now);
        let mut remaining = budgets
            .iter()
            .map(|(strategy, budget)| (strategy.clone(), budget.max_abs_quantity_ticks))
            .collect::<BTreeMap<_, _>>();
        let mut adjustments = Vec::new();
        for proposal in &mut result.accepted {
            let Some(available) = remaining.get_mut(&proposal.strategy_id) else {
                continue;
            };
            let (requested, original_action) = match proposal.action {
                Action::TargetQuantity { quantity_ticks }
                | Action::Increase { quantity_ticks }
                | Action::Decrease { quantity_ticks } => (quantity_ticks, proposal.action.clone()),
                Action::Close | Action::NoAction | Action::TargetWeight { .. } => continue,
            };
            let cap = requested.unsigned_abs().min((*available).unsigned_abs());
            let bounded = i64::try_from(cap)
                .ok()
                .and_then(|value| requested.signum().checked_mul(value))
                .unwrap_or(0);
            *available = available.saturating_sub(i64::try_from(cap).unwrap_or(i64::MAX));
            if bounded != requested {
                adjustments.push(BudgetAdjustment {
                    proposal_id: proposal.proposal_id,
                    before_quantity_ticks: requested,
                    after_quantity_ticks: bounded,
                });
                proposal.action = bounded_action(original_action, bounded);
            }
        }
        BudgetedResultSet {
            result,
            adjustments,
        }
    }

    /// Removes all pending proposals and resolves conflicts deterministically.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn resolve(&mut self, policy: Policy) -> ResultSet {
        let submitted = self
            .pending
            .iter()
            .map(|proposal| proposal.proposal_id)
            .collect::<Vec<_>>();
        let mut grouped: BTreeMap<InstrumentId, Vec<Proposal>> = BTreeMap::new();
        for proposal in self.pending.drain(..) {
            grouped
                .entry(proposal.instrument_id)
                .or_default()
                .push(proposal);
        }
        let mut accepted = Vec::new();
        let mut conflicts = Vec::new();
        let mut attributions = Vec::new();
        for (instrument_id, mut proposals) in grouped {
            proposals.sort_by(|left, right| {
                left.strategy_id
                    .cmp(&right.strategy_id)
                    .then(left.proposal_id.cmp(&right.proposal_id))
            });
            let positive = proposals
                .iter()
                .any(|proposal| direction(&proposal.action) > 0);
            let negative = proposals
                .iter()
                .any(|proposal| direction(&proposal.action) < 0);
            if positive && negative {
                conflicts.push(Conflict {
                    instrument_id,
                    proposal_ids: proposals
                        .iter()
                        .map(|proposal| proposal.proposal_id)
                        .collect(),
                });
                if policy == Policy::Priority {
                    let winner_direction = direction(&proposals[0].action);
                    for proposal in proposals
                        .into_iter()
                        .filter(|proposal| direction(&proposal.action) == winner_direction)
                    {
                        let proposal_id = proposal.proposal_id;
                        accepted.push(proposal);
                        attributions.push(Attribution {
                            result_proposal_id: proposal_id,
                            instrument_id,
                            source_proposal_ids: vec![proposal_id],
                        });
                    }
                    continue;
                }
            }
            if policy == Policy::WeightedNet {
                if let Some(net) = weighted_net(instrument_id, &proposals) {
                    let result_proposal_id = net.proposal_id;
                    attributions.push(Attribution {
                        result_proposal_id,
                        instrument_id,
                        source_proposal_ids: proposals
                            .iter()
                            .map(|proposal| proposal.proposal_id)
                            .collect(),
                    });
                    accepted.push(net);
                }
                continue;
            }
            for proposal in proposals {
                let proposal_id = proposal.proposal_id;
                accepted.push(proposal);
                attributions.push(Attribution {
                    result_proposal_id: proposal_id,
                    instrument_id,
                    source_proposal_ids: vec![proposal_id],
                });
            }
        }
        let accepted_ids = accepted
            .iter()
            .map(|proposal| proposal.proposal_id)
            .collect::<BTreeSet<_>>();
        let conflict_ids = conflicts
            .iter()
            .flat_map(|conflict| conflict.proposal_ids.iter().copied())
            .collect::<BTreeSet<_>>();
        for proposal_id in submitted {
            if let Some(record) = self.records.get_mut(&proposal_id) {
                record.state = if accepted_ids.contains(&proposal_id) {
                    ProposalState::Accepted
                } else if conflict_ids.contains(&proposal_id) {
                    ProposalState::Superseded
                } else {
                    ProposalState::Rejected
                };
            }
        }
        ResultSet {
            accepted,
            conflicts,
            expired: Vec::new(),
            attributions,
        }
    }

    /// Returns immutable source IDs for one resolved proposal.
    #[must_use]
    pub fn attribution(
        result: &ResultSet,
        result_proposal_id: ProposalId,
    ) -> Option<&[ProposalId]> {
        result
            .attributions
            .iter()
            .find(|attribution| attribution.result_proposal_id == result_proposal_id)
            .map(|attribution| attribution.source_proposal_ids.as_slice())
    }

    /// Returns the immutable lifecycle record for a proposal ID.
    #[must_use]
    pub fn record(&self, proposal_id: ProposalId) -> Option<&ProposalRecord> {
        self.records.get(&proposal_id)
    }

    /// Returns immutable proposal records in deterministic proposal-ID order.
    pub fn records(&self) -> impl Iterator<Item = &ProposalRecord> {
        self.records.values()
    }
}

fn bounded_action(original: Action, quantity_ticks: i64) -> Action {
    match original {
        Action::TargetQuantity { .. } => Action::TargetQuantity { quantity_ticks },
        Action::Increase { .. } => Action::Increase { quantity_ticks },
        Action::Decrease { .. } => Action::Decrease { quantity_ticks },
        other => other,
    }
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
fn weighted_net(instrument_id: InstrumentId, proposals: &[Proposal]) -> Option<Proposal> {
    let first = proposals.first()?.clone();
    let mut weighted_quantity = 0.0_f64;
    let mut total_confidence = 0.0_f64;
    let mut evidence = Vec::new();
    for proposal in proposals {
        let signed_quantity = match proposal.action {
            Action::TargetQuantity { quantity_ticks } | Action::Increase { quantity_ticks } => {
                quantity_ticks as f64
            }
            Action::Decrease { quantity_ticks } => -(quantity_ticks as f64),
            // A target weight cannot be netted safely without portfolio
            // equity, instrument mark, and precision context. The caller must
            // resolve it through the portfolio optimizer instead of inventing
            // a quantity scale here.
            Action::TargetWeight { .. } => return None,
            Action::Close | Action::NoAction => 0.0,
        };
        weighted_quantity += signed_quantity * proposal.confidence;
        total_confidence += proposal.confidence;
        evidence.extend(proposal.evidence.iter().cloned());
    }
    if !weighted_quantity.is_finite() || !total_confidence.is_finite() {
        return None;
    }
    let rounded = weighted_quantity
        .round()
        .clamp(i64::MIN as f64, i64::MAX as f64) as i64;
    let action = if rounded == 0 {
        Action::NoAction
    } else {
        Action::TargetQuantity {
            quantity_ticks: rounded,
        }
    };
    Some(Proposal {
        proposal_id: first.proposal_id,
        strategy_id: "coordinator.weighted_net.v1".into(),
        instrument_id,
        action,
        confidence: (total_confidence / proposals.len() as f64).clamp(0.0, 1.0),
        horizon_ns: proposals.iter().map(|proposal| proposal.horizon_ns).min()?,
        ttl_ns: proposals.iter().map(|proposal| proposal.ttl_ns).min()?,
        evidence,
        generated_mono: first.generated_mono,
    })
}

fn direction(action: &Action) -> i8 {
    match action {
        Action::TargetQuantity { quantity_ticks } | Action::Increase { quantity_ticks } => {
            sign_i64(*quantity_ticks)
        }
        Action::Decrease { quantity_ticks } => -sign_i64(*quantity_ticks),
        Action::TargetWeight { weight } => sign_f64(*weight),
        Action::Close | Action::NoAction => 0,
    }
}

fn sign_i64(value: i64) -> i8 {
    match value.cmp(&0) {
        std::cmp::Ordering::Greater => 1,
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
    }
}

fn sign_f64(value: f64) -> i8 {
    if value > 0.0 {
        1
    } else if value < 0.0 {
        -1
    } else {
        0
    }
}

#[cfg(test)]
mod tests {
    use insider_common_types::{InstrumentId, MonoTime, ProposalId};
    use insider_strategy_sdk::{Action, Proposal};
    use std::collections::BTreeMap;

    use super::{
        AllocationError, Coordinator, GraphError, Policy, ProposalState, StrategyBudget,
        VirtualBookLedger,
    };

    fn proposal(id: u128, strategy_id: &str, action: Action) -> Option<Proposal> {
        Some(Proposal {
            proposal_id: ProposalId::new(id).ok()?,
            strategy_id: strategy_id.to_owned(),
            instrument_id: InstrumentId::new(1).ok()?,
            action,
            confidence: 0.7,
            horizon_ns: 100,
            ttl_ns: 10,
            evidence: Vec::new(),
            generated_mono: MonoTime::from_nanos(1),
        })
    }

    #[test]
    fn opposing_requests_are_recorded_and_priority_is_deterministic() {
        let Some(buy) = proposal(1, "buy.v1", Action::TargetWeight { weight: 0.2 }) else {
            return;
        };
        let Some(sell) = proposal(2, "sell.v1", Action::TargetWeight { weight: -0.2 }) else {
            return;
        };
        let mut coordinator = Coordinator::new();
        coordinator.submit(sell);
        coordinator.submit(buy);
        let result = coordinator.resolve(Policy::Priority);
        assert_eq!(result.conflicts.len(), 1);
        assert_eq!(result.accepted.len(), 1);
        assert_eq!(result.accepted[0].strategy_id, "buy.v1");
    }

    #[test]
    fn strategy_budget_resizes_without_losing_attribution() {
        let Some(first) = proposal(
            11,
            "bounded.v1",
            Action::TargetQuantity { quantity_ticks: 8 },
        ) else {
            return;
        };
        let Some(second) = proposal(
            12,
            "bounded.v1",
            Action::TargetQuantity { quantity_ticks: 7 },
        ) else {
            return;
        };
        let mut coordinator = Coordinator::new();
        coordinator.submit(first);
        coordinator.submit(second);
        let mut budgets = BTreeMap::new();
        let Some(budget) = StrategyBudget::new(10) else {
            return;
        };
        budgets.insert("bounded.v1".into(), budget);
        let result = coordinator.resolve_at_with_budgets(
            Policy::IsolatedBooks,
            MonoTime::from_nanos(2),
            &budgets,
        );
        assert_eq!(result.result.accepted.len(), 2);
        assert_eq!(result.adjustments.len(), 1);
        assert_eq!(
            result.result.accepted[1].action,
            Action::TargetQuantity { quantity_ticks: 2 }
        );
        assert_eq!(
            Coordinator::attribution(&result.result, result.result.accepted[1].proposal_id),
            Some(&[result.result.accepted[1].proposal_id][..])
        );
    }

    #[test]
    fn weighted_net_produces_one_attributed_target() {
        let Some(buy) = proposal(1, "buy.v1", Action::Increase { quantity_ticks: 10 }) else {
            return;
        };
        let Some(sell) = proposal(2, "sell.v1", Action::Decrease { quantity_ticks: 4 }) else {
            return;
        };
        let mut coordinator = Coordinator::new();
        coordinator.submit(buy);
        coordinator.submit(sell);
        let result = coordinator.resolve(Policy::WeightedNet);
        assert_eq!(result.accepted.len(), 1);
        assert!(
            matches!(result.accepted[0].action, Action::TargetQuantity { quantity_ticks } if quantity_ticks == 4)
        );
        assert_eq!(result.conflicts.len(), 1);
    }

    #[test]
    fn dependency_graph_returns_deterministic_order_and_cycle_path() {
        let mut coordinator = Coordinator::new();
        assert!(
            coordinator
                .register_dependencies("strategy.a", ["metric.x"])
                .is_ok()
        );
        assert!(
            coordinator
                .register_dependencies("metric.x", std::iter::empty::<&str>())
                .is_ok()
        );
        assert_eq!(
            coordinator.evaluation_order().ok(),
            Some(vec![String::from("metric.x"), String::from("strategy.a")])
        );

        let mut cyclic = Coordinator::new();
        assert!(cyclic.register_dependencies("a", ["b"]).is_ok());
        assert!(cyclic.register_dependencies("b", ["a"]).is_ok());
        assert!(matches!(
            cyclic.evaluation_order(),
            Err(GraphError::Cycle(path)) if path == vec!["a".to_owned(), "b".to_owned(), "a".to_owned()]
        ));
    }

    #[test]
    fn proposal_records_are_immutable_and_expire_at_injected_boundary() {
        let Some(proposal) = proposal(7, "one.v1", Action::NoAction) else {
            return;
        };
        let mut coordinator = Coordinator::new();
        assert!(
            coordinator
                .submit_unique(proposal.clone(), MonoTime::from_nanos(1))
                .is_ok()
        );
        let result = coordinator.resolve_at(Policy::IsolatedBooks, MonoTime::from_nanos(12));
        assert_eq!(result.expired, vec![proposal.proposal_id]);
        assert_eq!(
            coordinator
                .record(proposal.proposal_id)
                .map(|record| record.state),
            Some(ProposalState::Expired)
        );
    }

    #[test]
    fn virtual_books_apply_targets_and_reject_unrepresentable_weights() {
        let Some(instrument) = InstrumentId::new(9).ok() else {
            return;
        };
        let Some(mut proposal) = proposal(9, "book.v1", Action::Increase { quantity_ticks: 4 })
        else {
            return;
        };
        proposal.instrument_id = instrument;
        let mut books = VirtualBookLedger::new();
        assert_eq!(
            books
                .apply(&proposal)
                .ok()
                .map(|change| change.after_quantity_ticks),
            Some(4)
        );
        proposal.action = Action::Close;
        assert_eq!(
            books
                .apply(&proposal)
                .ok()
                .map(|change| change.after_quantity_ticks),
            Some(0)
        );
        proposal.action = Action::TargetWeight { weight: 0.2 };
        assert_eq!(
            books.apply(&proposal),
            Err(AllocationError::TargetWeightUnsupported)
        );
        assert_eq!(
            books
                .apply_with_context(&proposal, Some((10_000, 100)))
                .ok()
                .map(|change| change.after_quantity_ticks),
            Some(20)
        );
        assert_eq!(
            books.apply_with_context(&proposal, Some((0, 100))),
            Err(AllocationError::InvalidWeightContext)
        );
    }
}
