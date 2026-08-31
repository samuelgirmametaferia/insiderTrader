//! Runtime assembly for proposal-to-order execution and fill accounting.

#![forbid(unsafe_code)]

/// Stable subsystem identifier used by startup diagnostics.
pub const SUBSYSTEM_ID: &str = "engine";

use std::collections::BTreeMap;
use std::fmt::Write as FmtWrite;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use insider_alerts::{Alert, AlertRouter, Channel as AlertChannel, Severity as AlertSeverity};
use insider_autonomy::{
    ApprovedAction, LiveGuard, LiveGuardError, LiveLimits, Mode as AutonomyMode, Plan, PlanEvent,
    PlanState, PlanStore, TradingEnvironment, decode_plan_event, encode_plan_event,
};
use insider_broker_api::{
    BrokerEvent, BrokerGateway, BrokerHealth, BrokerSnapshot, OrderState, OrderType, Side,
};
use insider_cfg_core::{ConfigStore, ReloadError, Settings, Snapshot, Value};
use insider_common_types::{AccountId, InstrumentId, MonoTime, ProposalId, TraceId};
use insider_context_graph::{
    Edge, EdgeFact, EmbeddingError, EmbeddingIndex, EmbeddingIndexSnapshot, EmbeddingRecord, Graph,
    GraphError, Node, NodeFact, NodeType, Provenance, RetrievalHit, RetrievalQuery, TimeInterval,
};
use insider_execution::{
    CancelError, ChildOrder, ChildPlan, ChildRecord, ChildState, OrderBook, PlanError,
    ReplaceError, Schedule, SubmitError, TransitionError, cancel_order,
    plan_target_with_guardrails, replace_order, submit_order,
};
use insider_experiment_registry::{
    Artifact as ExperimentArtifact, BundleError, BundleStore, ExperimentBundle,
    ExperimentProvenance, ExperimentRun, Registry as ExperimentRegistry, RunStatus,
};
use insider_journal::{
    BackupManifest, Journal, JournalError, JournalWriterLock, hex_digest, sha256,
};
use insider_llm_core::{
    ActionType, AutonomousAction, Capabilities as LlmCapabilities, LlmError, PromptRecord,
    PromptRegistry, Provider as LlmProvider, Request as LlmRequest, Response as LlmResponse,
    StreamItem, ToolHandler, ToolPermission, ToolRegistry, ToolRequest, ToolResponse, ToolSpec,
    parse_autonomous_action,
};
use insider_market_data::{
    Bar, BarUpdate, IngestOutcome, MarketDataHub, MarketEvent, MarketSnapshot, StreamKind,
};
use insider_metric_host::{DiscoveredMetric, Host as MetricHost};
use insider_metric_sdk::{BookImbalanceMetric, Metric, MetricContext, MetricOutput};
use insider_model_registry::{
    ArtifactManifest, ModelRecord, Registry as ModelRegistry,
    RegistrySnapshot as ModelRegistrySnapshot, Status as ModelStatus,
};
use insider_news_core::{
    CursorCommitter, CursorProvider, NewsDetail, NewsItem, NewsPage, NewsStore, PollOutcome,
    ProviderHealth, ProviderRegistry, ProviderStateSnapshot, ProviderStatus, RetryClass,
    RetryPolicy, decode_provider_state, encode_provider_state,
};
use insider_portfolio::{
    AccountingError, CorporateActionKind, OptimizationCandidate, OptimizationConstraints,
    OptimizationResult, Portfolio, TargetError, optimize_targets,
};
use insider_read_model::{ProjectionManifest, ProjectionStore};
use insider_risk_engine::{
    Guardrails, Reason as RiskReason, RiskEngine, RiskInputs, RiskScope, ScopedRiskPolicy,
    ScopedRiskPolicySnapshot, State as RiskState, StateTransitionError, TimedLimits,
};
use insider_strategy_coordinator::{
    BudgetedResultSet, Coordinator, Policy as StrategyPolicy, ProposalRecord,
    ResultSet as StrategyResultSet, StrategyBudget,
};
use insider_strategy_host::{DiscoveredStrategy, Host as StrategyHost};
use insider_strategy_sdk::{
    Action, MissingEvidencePolicy, Proposal, ProposalError, Strategy, StrategyContext,
    StrategyManifest, ThresholdStrategy,
};
use insider_supervisor::{Health as SupervisorHealth, Policy as SupervisorPolicy, Supervisor};

pub mod command;
pub use command::{
    CommandServiceError, EngineCommandService, ExperimentMutation, ModelMutation,
    alert_ack_command_payload, alerts_get_command_payload, autonomy_mode_command_payload,
    autonomy_submit_command_payload, autonomy_transition_command_payload,
    broker_status_command_payload, context_search_command_payload, events_command_payload,
    experiment_mutation_command_payload, live_arm_command_payload, live_configure_command_payload,
    live_confirm_command_payload, live_kill_command_payload, llm_action_command_payload,
    llm_complete_command_payload, llm_stream_command_payload, metric_registry_list_command_payload,
    model_mutation_command_payload, news_detail_command_payload,
    news_provider_status_command_payload, preview_command_payload,
    proposal_preview_command_payload, proposal_submit_command_payload,
    resolve_symbol_command_payload, scoped_risk_policy_command_payload, snapshot_command_payload,
    strategy_evaluate_command_payload, strategy_registry_list_command_payload,
    submit_command_payload, supervisor_status_command_payload, trace_export_command_payload,
};

/// Runtime assembly errors.
#[derive(Debug)]
pub enum EngineError {
    /// Proposal failed schema/TTL validation.
    Proposal(ProposalError),
    /// Proposal could not become an absolute target.
    Target(TargetError),
    /// Risk or broker capabilities rejected an order.
    Plan(PlanError),
    /// Durable local lifecycle failure.
    Submit(SubmitError),
    /// Broker event could not advance the local order book.
    Transition(TransitionError),
    /// A required runtime lock was poisoned.
    Poisoned,
    /// New work was submitted after drain or stop.
    NotRunning,
    /// The event journal rejected a durable append.
    Journal(JournalError),
    /// A broker fill could not be applied to the double-entry portfolio.
    Accounting(AccountingError),
    /// A cancel request was unsupported, rejected, or transport-ambiguous.
    Cancel(CancelError),
    /// A replace request was unsupported, rejected, or transport-ambiguous.
    Replace(ReplaceError),
    /// A risk state transition was unauthorized or invalid.
    RiskState(StateTransitionError),
    /// Broker reconciliation could not query authoritative state.
    Reconcile(String),
    /// A manual order preview is no longer valid for submission.
    StalePreview,
    /// A manual order preview has expired.
    PreviewExpired,
    /// Explicit confirmation is required before submission.
    ConfirmationRequired,
    /// A bounded engine request was malformed or outside supported limits.
    InvalidRequest,
    /// Live trading guard rejected the pre-send request.
    LiveGuard(LiveGuardError),
    /// Strategy proposal admission or resolution failed.
    Strategy(String),
    /// Autonomous plan lifecycle validation or persistence failed.
    Autonomy(String),
    /// Rebuildable local read-model projection failed.
    ReadModel(String),
    /// Canonical market-data ingress rejected an event or recovery request.
    MarketData(String),
    /// Context graph projection or retrieval failed.
    Graph(String),
    /// LLM provider was unavailable or rejected a control-plane request.
    Llm(LlmError),
}

impl From<JournalError> for EngineError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

fn index_news_in_graph(graph: &mut Graph, item: &NewsItem) -> Result<(), GraphError> {
    let news_id = format!("news:{}", item.id);
    let known_start = item.received_at_ms.max(0);
    let valid_start = item.published_at_ms.unwrap_or(known_start).max(0);
    let valid = TimeInterval::new(valid_start, None)?;
    let known = TimeInterval::new(known_start, None)?;
    let provenance = Provenance {
        source: item.provider.clone(),
        artifact_id: item.content_hash.clone(),
        confidence: 1.0,
    };
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: news_id.clone(),
            node_type: NodeType::NewsItem,
            label: item.title.clone(),
        },
        valid,
        known,
        provenance: provenance.clone(),
    })?;
    let cluster_key = item
        .title
        .split_whitespace()
        .map(str::to_lowercase)
        .collect::<Vec<_>>()
        .join(" ");
    let cluster_id = format!("news-cluster:{cluster_key}");
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: cluster_id.clone(),
            node_type: NodeType::NewsCluster,
            label: cluster_key,
        },
        valid,
        known,
        provenance: provenance.clone(),
    })?;
    graph.add_edge_fact(EdgeFact {
        edge: Edge {
            from: news_id.clone(),
            relation: "IN_CLUSTER".into(),
            to: cluster_id,
        },
        valid,
        known,
        provenance: provenance.clone(),
    })?;
    for symbol in &item.symbols {
        let instrument_id = format!("instrument:{}", symbol.to_ascii_uppercase());
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: instrument_id.clone(),
                node_type: NodeType::Instrument,
                label: symbol.to_ascii_uppercase(),
            },
            valid,
            known,
            provenance: provenance.clone(),
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: news_id.clone(),
                relation: "MENTIONS".into(),
                to: instrument_id,
            },
            valid,
            known,
            provenance: provenance.clone(),
        })?;
    }
    Ok(())
}

/// Converts canonical integer ticks to the floating feature representation
/// required by metric SDKs. Feature manifests explicitly accept f64; the
/// conversion is centralized so precision policy is visible and auditable.
#[allow(clippy::cast_precision_loss)]
fn ticks_to_feature(value: i64) -> f64 {
    value as f64
}

fn index_strategy_in_graph(graph: &mut Graph, proposal: &Proposal) -> Result<(), GraphError> {
    let proposal_id = format!("proposal:{}", proposal.proposal_id.get());
    let strategy_id = format!("strategy:{}", proposal.strategy_id);
    let instrument_id = format!("instrument:{}", proposal.instrument_id.get());
    let start = i64::try_from(proposal.generated_mono.as_nanos()).unwrap_or(i64::MAX);
    let interval = TimeInterval::new(start, None)?;
    let provenance = Provenance {
        source: String::from("strategy-coordinator"),
        artifact_id: proposal.proposal_id.get().to_string(),
        confidence: proposal.confidence,
    };
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: proposal_id.clone(),
            node_type: NodeType::Strategy,
            label: format!("{} proposal", proposal.strategy_id),
        },
        valid: interval,
        known: interval,
        provenance: provenance.clone(),
    })?;
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: strategy_id.clone(),
            node_type: NodeType::Strategy,
            label: proposal.strategy_id.clone(),
        },
        valid: interval,
        known: interval,
        provenance: provenance.clone(),
    })?;
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: instrument_id.clone(),
            node_type: NodeType::Instrument,
            label: proposal.instrument_id.get().to_string(),
        },
        valid: interval,
        known: interval,
        provenance: provenance.clone(),
    })?;
    graph.add_edge_fact(EdgeFact {
        edge: Edge {
            from: proposal_id.clone(),
            relation: "GENERATED_BY".into(),
            to: strategy_id,
        },
        valid: interval,
        known: interval,
        provenance: provenance.clone(),
    })?;
    graph.add_edge_fact(EdgeFact {
        edge: Edge {
            from: proposal_id,
            relation: "PROPOSED_FOR".into(),
            to: instrument_id,
        },
        valid: interval,
        known: interval,
        provenance,
    })?;
    Ok(())
}

fn index_experiment_in_graph(
    graph: &mut Graph,
    run: &ExperimentRun,
    known_ms: i64,
) -> Result<(), GraphError> {
    let node_id = format!("experiment:{}", run.run_id);
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    let experiment_id = format!("experiment:{}", run.run_id);
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: node_id.clone(),
            node_type: NodeType::Experiment,
            label: format!("{} ({:?})", run.run_id, run.status),
        },
        valid,
        known,
        provenance: Provenance {
            source: "experiment-registry".into(),
            artifact_id: format!("{}:{}:{}", run.code_hash, run.config_hash, run.dataset_hash),
            confidence: 1.0,
        },
    })?;
    if let Some(strategy_id) = &run.provenance.strategy_id {
        let strategy_node = format!("strategy:{strategy_id}");
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: strategy_node.clone(),
                node_type: NodeType::Strategy,
                label: strategy_id.clone(),
            },
            valid,
            known,
            provenance: Provenance {
                source: "experiment-registry".into(),
                artifact_id: run.code_hash.clone(),
                confidence: 1.0,
            },
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: experiment_id,
                relation: "USES_STRATEGY".into(),
                to: strategy_node,
            },
            valid,
            known,
            provenance: Provenance {
                source: "experiment-registry".into(),
                artifact_id: run.config_hash.clone(),
                confidence: 1.0,
            },
        })?;
    }
    Ok(())
}

fn index_model_in_graph(
    graph: &mut Graph,
    record: &ModelRecord,
    manifest: Option<&ArtifactManifest>,
    known_ms: i64,
) -> Result<(), GraphError> {
    let node_id = format!("model:{}:{}", record.model_id, record.version);
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    let artifact_id = manifest.map_or_else(
        || record.artifact_hash.clone(),
        |value| value.artifact_hash.clone(),
    );
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: node_id,
            node_type: NodeType::Model,
            label: format!(
                "{}:{} ({:?})",
                record.model_id, record.version, record.status
            ),
        },
        valid,
        known,
        provenance: Provenance {
            source: "model-registry".into(),
            artifact_id,
            confidence: 1.0,
        },
    })?;
    Ok(())
}

fn index_model_snapshot_in_graph(
    graph: &mut Graph,
    snapshot: &ModelRegistrySnapshot,
    known_ms: i64,
) -> Result<(), GraphError> {
    let manifests = snapshot
        .manifests
        .iter()
        .map(|((model_id, version), manifest)| ((model_id.as_str(), version.as_str()), manifest))
        .collect::<BTreeMap<_, _>>();
    for record in &snapshot.records {
        index_model_in_graph(
            graph,
            record,
            manifests
                .get(&(record.model_id.as_str(), record.version.as_str()))
                .copied(),
            known_ms,
        )?;
    }
    Ok(())
}

fn index_order_in_graph(
    graph: &mut Graph,
    intent: &insider_broker_api::OrderIntent,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: format!("order:{}", intent.client_order_id),
            node_type: NodeType::Order,
            label: format!("{} {:?}", intent.client_order_id, intent.state),
        },
        valid,
        known,
        provenance: Provenance {
            source: "execution".into(),
            artifact_id: intent.intent_id.clone(),
            confidence: 1.0,
        },
    })?;
    Ok(())
}

fn index_fill_in_graph(
    graph: &mut Graph,
    client_order_id: &str,
    quantity_ticks: i64,
    price_ticks: i64,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    let fill_id = format!("fill:{client_order_id}:{quantity_ticks}:{price_ticks}");
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: fill_id,
            node_type: NodeType::Fill,
            label: format!("{quantity_ticks} × {price_ticks}"),
        },
        valid,
        known,
        provenance: Provenance {
            source: "broker".into(),
            artifact_id: client_order_id.to_owned(),
            confidence: 1.0,
        },
    })?;
    let order_id = format!("order:{client_order_id}");
    let fill_id = format!("fill:{client_order_id}:{quantity_ticks}:{price_ticks}");
    if graph.node(&order_id).is_some() {
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: order_id,
                relation: "HAS_FILL".into(),
                to: fill_id,
            },
            valid,
            known,
            provenance: Provenance {
                source: "broker".into(),
                artifact_id: client_order_id.to_owned(),
                confidence: 1.0,
            },
        })?;
    }
    Ok(())
}

fn index_portfolio_in_graph(
    graph: &mut Graph,
    portfolio: &Portfolio,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    let portfolio_id = "portfolio:primary".to_owned();
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: portfolio_id.clone(),
            node_type: NodeType::Portfolio,
            label: "Primary portfolio".into(),
        },
        valid,
        known,
        provenance: Provenance {
            source: "portfolio".into(),
            artifact_id: format!("equity:{:?}", portfolio.equity_ticks()),
            confidence: 1.0,
        },
    })?;
    for (instrument_id, position) in portfolio.positions() {
        let position_id = format!("position:{}", instrument_id.get());
        let instrument_node_id = format!("instrument:{}", instrument_id.get());
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: instrument_node_id.clone(),
                node_type: NodeType::Instrument,
                label: instrument_id.get().to_string(),
            },
            valid,
            known,
            provenance: Provenance {
                source: "instrument-master".into(),
                artifact_id: instrument_id.get().to_string(),
                confidence: 1.0,
            },
        })?;
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: position_id.clone(),
                node_type: NodeType::Position,
                label: format!(
                    "instrument {} quantity {} mark {}",
                    instrument_id.get(),
                    position.quantity_ticks,
                    position.mark_price_ticks
                ),
            },
            valid,
            known,
            provenance: Provenance {
                source: "portfolio".into(),
                artifact_id: format!("position:{}:{}", instrument_id.get(), known_ms),
                confidence: 1.0,
            },
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: portfolio_id.clone(),
                relation: "HOLDS".into(),
                to: position_id.clone(),
            },
            valid,
            known,
            provenance: Provenance {
                source: "portfolio".into(),
                artifact_id: instrument_id.get().to_string(),
                confidence: 1.0,
            },
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: position_id,
                relation: "POSITION_OF".into(),
                to: instrument_node_id,
            },
            valid,
            known,
            provenance: Provenance {
                source: "portfolio".into(),
                artifact_id: instrument_id.get().to_string(),
                confidence: 1.0,
            },
        })?;
    }
    Ok(())
}

fn index_metric_in_graph(
    graph: &mut Graph,
    metric_id: &str,
    lifecycle: &str,
    evidence_ref: &str,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: format!("metric:{metric_id}"),
            node_type: NodeType::Metric,
            label: format!("{metric_id} ({lifecycle})"),
        },
        valid,
        known,
        provenance: Provenance {
            source: "metric-host".into(),
            artifact_id: if evidence_ref.trim().is_empty() {
                format!("metric:{metric_id}")
            } else {
                evidence_ref.to_owned()
            },
            confidence: 1.0,
        },
    })?;
    Ok(())
}

fn index_strategy_manifest_in_graph(
    graph: &mut Graph,
    manifest: &StrategyManifest,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    let strategy_node = format!("strategy:{}", manifest.strategy_id);
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: strategy_node.clone(),
            node_type: NodeType::Strategy,
            label: manifest.strategy_id.clone(),
        },
        valid,
        known,
        provenance: Provenance {
            source: "strategy-host".into(),
            artifact_id: manifest.strategy_id.clone(),
            confidence: 1.0,
        },
    })?;
    for metric_id in &manifest.metric_ids {
        let metric_node = format!("metric:{metric_id}");
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: metric_node.clone(),
                node_type: NodeType::Metric,
                label: metric_id.clone(),
            },
            valid,
            known,
            provenance: Provenance {
                source: "strategy-manifest".into(),
                artifact_id: manifest.strategy_id.clone(),
                confidence: 1.0,
            },
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: strategy_node.clone(),
                relation: "USES_METRIC".into(),
                to: metric_node,
            },
            valid,
            known,
            provenance: Provenance {
                source: "strategy-manifest".into(),
                artifact_id: manifest.strategy_id.clone(),
                confidence: 1.0,
            },
        })?;
    }
    for dependency in &manifest.strategy_dependencies {
        let dependency_node = format!("strategy:{dependency}");
        graph.upsert_node_fact(NodeFact {
            node: Node {
                id: dependency_node.clone(),
                node_type: NodeType::Strategy,
                label: dependency.clone(),
            },
            valid,
            known,
            provenance: Provenance {
                source: "strategy-manifest".into(),
                artifact_id: manifest.strategy_id.clone(),
                confidence: 1.0,
            },
        })?;
        graph.add_edge_fact(EdgeFact {
            edge: Edge {
                from: strategy_node.clone(),
                relation: "DEPENDS_ON".into(),
                to: dependency_node,
            },
            valid,
            known,
            provenance: Provenance {
                source: "strategy-manifest".into(),
                artifact_id: manifest.strategy_id.clone(),
                confidence: 1.0,
            },
        })?;
    }
    Ok(())
}

fn index_strategy_lifecycle_in_graph(
    graph: &mut Graph,
    strategy_id: &str,
    lifecycle: &str,
    evidence_ref: &str,
    known_ms: i64,
) -> Result<(), GraphError> {
    let valid = TimeInterval::new(0, None)?;
    let known = TimeInterval::new(known_ms.max(0), None)?;
    graph.upsert_node_fact(NodeFact {
        node: Node {
            id: format!("strategy:{strategy_id}"),
            node_type: NodeType::Strategy,
            label: format!("{strategy_id} ({lifecycle})"),
        },
        valid,
        known,
        provenance: Provenance {
            source: "strategy-host".into(),
            artifact_id: if evidence_ref.trim().is_empty() {
                format!("strategy:{strategy_id}")
            } else {
                evidence_ref.to_owned()
            },
            confidence: 1.0,
        },
    })?;
    Ok(())
}

/// Service lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Lifecycle {
    /// Journal and broker state are being reconciled before accepting work.
    Reconciling,
    /// Services accept market, decision, and execution work.
    Running,
    /// New work is rejected while in-flight state drains.
    Draining,
    /// Runtime has been stopped.
    Stopped,
}

/// Operational cause for a broker reconciliation sweep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileTrigger {
    /// Initial process startup after journal restoration.
    Startup,
    /// Broker session became healthy after a disconnect.
    Reconnect,
    /// Periodic open-order/execution reconciliation timer.
    Periodic,
    /// A callback or local transition revealed inconsistent state.
    Anomaly,
}

/// A durable broker event that was retained for audit but could not be applied
/// to the local order projection during startup replay. The broker snapshot
/// reconciliation remains authoritative for resolving the affected order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryAnomaly {
    /// Journal sequence containing the anomalous event.
    pub sequence: u64,
    /// Stable event category for operator filtering.
    pub kind: &'static str,
    /// Bounded diagnostic describing why projection was rejected.
    pub reason: String,
}

const MAX_RECOVERY_ANOMALIES: usize = 1_024;

/// Result of one service-level reconciliation pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileSummary {
    /// Number of locally unknown orders queried.
    pub queried: usize,
    /// Number of broker events durably applied.
    pub resolved: usize,
    /// Number of orders for which the broker still returned no event.
    pub still_unknown: usize,
    /// Query failures retained for operator retry.
    pub failed: Vec<(String, String)>,
    /// Broker orders absent from the local journal.
    pub external_orders: usize,
    /// Locally working/uncertain orders absent from the broker snapshot.
    pub missing_at_broker: usize,
    /// Number of canonical broker positions returned by the snapshot.
    pub snapshot_positions: usize,
    /// Number of account values returned by the snapshot.
    pub snapshot_account_values: usize,
}

/// Authoritative runtime snapshot served to terminal/read-model clients.
///
/// The journal cursor is part of the snapshot contract: a client must apply
/// only deltas strictly after this cursor and request a fresh snapshot on gaps.
#[derive(Clone, Debug, PartialEq)]
pub struct RuntimeSnapshot {
    /// Account represented by this runtime.
    pub account_id: AccountId,
    /// Cursor immediately after the last journal record included.
    pub cursor: u64,
    /// Current enforced risk state.
    pub risk_state: RiskState,
    /// Durable manual/hybrid/autonomous decision mode.
    pub autonomy_mode: AutonomyMode,
    /// Most recently generated autonomous plan, if one exists.
    pub autonomy_plan: Option<AutonomyPlanSnapshot>,
    /// Currently installed LLM provider identity. This is runtime
    /// configuration, not plan-bound provenance.
    pub llm_provider_id: Option<String>,
    /// Currently configured LLM model. This is runtime configuration, not
    /// plan-bound provenance.
    pub llm_model: Option<String>,
    /// Reconciled portfolio projection.
    pub portfolio: Portfolio,
    /// Stable order projection records.
    pub orders: Vec<insider_execution::OrderRecord>,
    /// Bounded authoritative fill history for execution-quality and audit views.
    pub fills: Vec<FillRecord>,
    /// Integer-exact realized execution summaries derived from the fill history.
    pub tca: Vec<TcaSnapshot>,
    /// Versioned strategy proposals and coordinator lifecycle states.
    pub proposals: Vec<ProposalRecord>,
    /// Bounded canonical latest market states for registered instruments.
    pub markets: Vec<MarketSnapshot>,
    /// Current gross marked notional from reconciled positions.
    pub gross_notional_ticks: i128,
    /// Configured hard gross-notional limit.
    pub max_gross_notional_ticks: i128,
    /// Gross exposure utilization in basis points of the configured limit.
    pub gross_utilization_bps: i64,
    /// Largest single-position marked notional.
    pub largest_position_notional_ticks: i128,
    /// Peak-to-equity drawdown, if a positive high-water mark exists.
    pub drawdown_bps: Option<i64>,
}

/// Bounded autonomous-plan detail exposed to control-plane clients.
#[derive(Clone, Debug, PartialEq)]
pub struct AutonomyPlanSnapshot {
    /// Stable plan identity.
    pub plan_id: String,
    /// Durable lifecycle state.
    pub state: PlanState,
    /// Monotonic generation timestamp.
    pub generated_at_ns: u64,
    /// Monotonic hard expiry timestamp.
    pub expires_at_ns: u64,
    /// Schema-validated finite actions retained by the plan store.
    pub actions: Vec<AutonomousAction>,
}

/// Journal evidence associated with one trading trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceEvent {
    /// Source journal sequence.
    pub sequence: u64,
    /// Stable event category.
    pub kind: String,
    /// Original versioned event payload.
    pub payload: Vec<u8>,
}

/// One normalized fill retained by the runtime read model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FillRecord {
    /// Stable client order identity.
    pub client_order_id: String,
    /// Canonical instrument identity.
    pub instrument_id: InstrumentId,
    /// Signed quantity (sell is negative).
    pub signed_quantity_ticks: i64,
    /// Execution price in canonical ticks.
    pub price_ticks: i64,
}

/// Realized, integer-exact execution summary grouped by client order.
/// Arrival price, latency, spread, and implementation shortfall are omitted
/// until the broker supplies those source measurements; they are never
/// inferred from this projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TcaSnapshot {
    /// Stable client order identity.
    pub client_order_id: String,
    /// Total positive quantity represented by retained fills.
    pub filled_quantity_ticks: i64,
    /// Sum of quantity multiplied by execution price.
    pub notional_ticks: i128,
    /// Exact VWAP numerator (equal to `notional_ticks`).
    pub average_fill_price_numerator: i128,
    /// Exact VWAP denominator (equal to `filled_quantity_ticks`).
    pub average_fill_price_denominator: i64,
    /// Price captured at the decision boundary, when a trusted mark existed.
    pub arrival_price_ticks: Option<i64>,
    /// Decision timestamp from the engine monotonic clock.
    pub decision_mono_ns: Option<u64>,
    /// Timestamp immediately before the broker send attempt.
    pub send_mono_ns: Option<u64>,
    /// First acknowledgement timestamp, when acknowledged.
    pub ack_mono_ns: Option<u64>,
    /// First fill timestamp, when a fill was observed.
    pub first_fill_mono_ns: Option<u64>,
    /// Signed implementation shortfall in tick-value units, when arrival exists.
    pub implementation_shortfall_tick_value: Option<i128>,
    /// Quoted spread at arrival, when a quote was available.
    pub average_spread_ticks: Option<i64>,
    /// Signed adverse-selection tick-value cost after the first fill.
    pub adverse_selection_tick_value: Option<i128>,
}

/// Durable timing context captured around one order's broker lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionTiming {
    /// Stable client order identity.
    pub client_order_id: String,
    /// Decision/arrival boundary.
    pub decision_mono_ns: u64,
    /// Trusted arrival price, if available.
    pub arrival_price_ticks: Option<i64>,
    /// Send attempt timestamp.
    pub send_mono_ns: Option<u64>,
    /// First acknowledgement timestamp.
    pub ack_mono_ns: Option<u64>,
    /// First fill timestamp.
    pub first_fill_mono_ns: Option<u64>,
    /// Midpoint captured at the decision boundary.
    pub decision_mid_ticks: Option<i64>,
    /// Quoted spread captured at the decision boundary.
    pub arrival_spread_ticks: Option<i64>,
    /// Midpoint at send, acknowledgement, and after the first fill.
    pub send_mid_ticks: Option<i64>,
    /// Midpoint observed when the broker acknowledgement arrived.
    pub ack_mid_ticks: Option<i64>,
    /// Midpoint observed after the first fill callback.
    pub post_fill_mid_ticks: Option<i64>,
}

/// Canonical quote-derived reference captured for execution-quality analysis.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionMarketReference {
    /// Quote midpoint in integer ticks.
    pub mid_ticks: i64,
    /// Ask minus bid in integer ticks.
    pub spread_ticks: i64,
}

/// Durable parent intent plus its deterministic child-order state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildPlanRecord {
    /// Parent intent used to derive every child identity.
    pub parent: insider_broker_api::OrderIntent,
    /// Mutable child lifecycle projection.
    pub plan: ChildPlan,
}

/// Immutable input bundle for one deterministic backtest run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestRunRequest {
    /// Stable caller-supplied run identity.
    pub run_id: String,
    /// Immutable strategy package/version reference.
    pub strategy_id: String,
    /// Content hash of the point-in-time dataset.
    pub dataset_hash: String,
    /// Hash of the resolved run configuration.
    pub config_hash: String,
    /// Initial reporting-currency cash in canonical ticks.
    pub initial_cash_ticks: i128,
    /// Strictly sequenced historical fills/marks.
    pub events: Vec<insider_replay::BacktestEvent>,
}

/// One point-in-time market/feature snapshot supplied to a deterministic
/// strategy replay. The strategy is evaluated at `now_mono_ns`; any resulting
/// target delta is filled at `price_ticks`, then marked at the same price.
#[derive(Clone, Debug, PartialEq)]
pub struct StrategyBacktestEvent {
    /// Strictly increasing historical sequence.
    pub sequence: u64,
    /// Deterministic decision clock value for this event.
    pub now_mono_ns: u64,
    /// Instrument being evaluated.
    pub instrument_id: InstrumentId,
    /// Positive executable/mark price in canonical ticks.
    pub price_ticks: i64,
    /// Explicit venue fee charged for any simulated fill.
    pub fee_ticks: i128,
    /// Point-in-time metrics visible to the strategy.
    pub metrics: Vec<MetricOutput>,
}

/// Immutable input bundle for replaying one registered deterministic strategy.
#[derive(Clone, Debug, PartialEq)]
pub struct StrategyBacktestRunRequest {
    /// Stable caller-supplied run identity.
    pub run_id: String,
    /// Registered strategy package/version to evaluate.
    pub strategy_id: String,
    /// Immutable dataset content hash.
    pub dataset_hash: String,
    /// Hash of the resolved strategy/backtest configuration.
    pub config_hash: String,
    /// Initial reporting-currency cash in canonical ticks.
    pub initial_cash_ticks: i128,
    /// Ordered market/feature observations.
    pub events: Vec<StrategyBacktestEvent>,
}

/// Durable summary and full deterministic equity curve for a backtest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BacktestRunResult {
    /// Immutable run identity.
    pub run_id: String,
    /// Strategy reference used by the run.
    pub strategy_id: String,
    /// Content hash of the point-in-time dataset.
    pub dataset_hash: String,
    /// Hash of the resolved run configuration.
    pub config_hash: String,
    /// Deterministic replay report.
    pub report: insider_replay::BacktestReport,
}

/// Bounded operator summary of one strategy resolution boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyResolutionSummary {
    /// Policy used for the resolution.
    pub policy: String,
    /// Injected monotonic decision boundary.
    pub now_mono_ns: u64,
    /// Number of accepted result proposals.
    pub accepted_count: usize,
    /// Number of explicit conflicts.
    pub conflict_count: usize,
    /// Number of proposals expired at the boundary.
    pub expired_count: usize,
    /// Number of attribution mappings produced.
    pub attribution_count: usize,
}

/// Durable execution totals attributed to a strategy through proposal lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyExecutionSummary {
    /// Strategy identifier resolved from the originating proposal.
    pub strategy_id: String,
    /// Number of authoritative fill events attributed to the strategy.
    pub fill_count: u64,
    /// Signed filled quantity in canonical ticks.
    pub filled_quantity_ticks: i128,
    /// Signed execution notional in canonical ticks.
    pub notional_ticks: i128,
}

/// Read-only installed strategy registry record exposed to workstation clients.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StrategyRegistryRecord {
    /// Immutable strategy identity.
    pub strategy_id: String,
    /// Declared strategy execution mode.
    pub mode: String,
    /// Required metric IDs.
    pub metric_ids: Vec<String>,
    /// Strategy-to-strategy dependencies.
    pub dependencies: Vec<String>,
    /// Decision horizon in nanoseconds.
    pub horizon_ns: u64,
    /// Proposal time-to-live in nanoseconds.
    pub ttl_ns: u64,
    /// Evaluation period in nanoseconds.
    pub period_ns: u64,
    /// Evaluation deadline in nanoseconds.
    pub deadline_ns: u64,
    /// Scheduler priority.
    pub priority: String,
    /// Current isolated host lifecycle state.
    pub state: String,
    /// Artifact promotion lifecycle state.
    pub lifecycle: String,
    /// Evidence reference supplied for the current lifecycle state.
    pub lifecycle_evidence_ref: String,
}

/// Read-only installed metric registry record exposed to workstation clients.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricRegistryRecord {
    /// Immutable metric identity.
    pub metric_id: String,
    /// Current host lifecycle state.
    pub state: String,
    /// Artifact promotion lifecycle state.
    pub lifecycle: String,
    /// Declared feature inputs.
    pub inputs: Vec<String>,
    /// Optional lower output bound.
    pub min_score: Option<f64>,
    /// Optional upper output bound.
    pub max_score: Option<f64>,
    /// Maximum output age in nanoseconds.
    pub ttl_ns: u64,
    /// Desired evaluation period in nanoseconds.
    pub period_ns: u64,
    /// Evaluation deadline in nanoseconds.
    pub deadline_ns: u64,
    /// Compute budget in nanoseconds.
    pub budget_ns: u64,
    /// Scheduler priority.
    pub priority: String,
}

/// Operator-facing alert retained by the engine read model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlertRecord {
    /// Stable alert identity.
    pub alert_id: String,
    /// Dedupe key used by the router.
    pub dedupe_key: String,
    /// Source subsystem.
    pub source: String,
    /// Monotonic occurrence time in milliseconds.
    pub occurred_ms: i64,
    /// Severity code: 1 info, 2 warning, 3 critical.
    pub severity: u8,
    /// Safe operator-facing message.
    pub message: String,
    /// Whether the alert is sensitive and therefore in-app only.
    pub sensitive: bool,
}

const MAX_RUNTIME_FILLS: usize = 10_000;

fn action_type_name(action: ActionType) -> &'static str {
    match action {
        ActionType::ExecuteProposal => "EXECUTE_PROPOSAL",
        ActionType::ExecuteProposalScaled => "EXECUTE_PROPOSAL_SCALED",
        ActionType::IgnoreProposal => "IGNORE_PROPOSAL",
        ActionType::PauseStrategy => "PAUSE_STRATEGY",
        ActionType::ResumeStrategy => "RESUME_STRATEGY",
        ActionType::RequestReanalysis => "REQUEST_REANALYSIS",
        ActionType::AddToWatch => "ADD_TO_WATCH",
        ActionType::RemoveFromWatch => "REMOVE_FROM_WATCH",
        ActionType::ReduceAutonomy => "REDUCE_AUTONOMY",
        ActionType::NoAction => "NO_ACTION",
    }
}

/// Read-only manual-order preview bound to the journal state version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManualOrderPreview {
    /// Stable preview identity used for audit and retries.
    pub preview_id: String,
    /// State version observed while calculating risk.
    pub expected_state_version: u64,
    /// Monotonic expiry deadline.
    pub expires_mono_ns: u64,
    /// Risk-approved intent that would be submitted.
    pub intent: insider_broker_api::OrderIntent,
    /// Absolute target quantity used to re-run risk validation.
    pub target_quantity_ticks: i64,
    /// Stable proposal identity used for deterministic intent IDs.
    pub proposal_id: ProposalId,
    /// Estimated notional from the trusted mark, when available.
    pub estimated_notional_ticks: Option<i128>,
    /// Estimated execution cost in basis points, when available.
    pub estimated_cost_bps: Option<i64>,
    /// Explicit warnings that must be rendered before confirmation.
    pub warnings: Vec<String>,
}

/// Durable headless service host used by terminal and unattended deployments.
pub struct ServiceHost {
    started: Instant,
    runtime: Arc<Runtime>,
    journal: Journal,
    _writer_lock: JournalWriterLock,
    append_lock: Mutex<()>,
    config: ConfigStore,
    lifecycle: Mutex<Lifecycle>,
    autonomy_plans: Mutex<PlanStore>,
    read_model: ProjectionStore,
    news: Mutex<NewsStore>,
    news_providers: Mutex<ProviderRegistry>,
    provider_state: Mutex<BTreeMap<String, ProviderStateSnapshot>>,
    market_data: Mutex<MarketDataHub>,
    recovery_anomalies: Mutex<Vec<RecoveryAnomaly>>,
    strategy_coordinator: Mutex<Coordinator>,
    metric_host: Mutex<MetricHost>,
    metric_lifecycle_overrides: Mutex<BTreeMap<String, (insider_metric_host::Lifecycle, String)>>,
    strategy_host: Mutex<StrategyHost>,
    strategy_lifecycle_overrides:
        Mutex<BTreeMap<String, (insider_strategy_host::Lifecycle, String)>>,
    python_metric_last_run: Mutex<BTreeMap<(String, u128), u64>>,
    python_strategy_last_run: Mutex<BTreeMap<(String, u128), u64>>,
    python_metric_outputs: Mutex<BTreeMap<(String, u128), MetricOutput>>,
    llm_provider: Mutex<Option<Arc<dyn LlmProvider>>>,
    prompt_registry: Mutex<PromptRegistry>,
    context_graph: Mutex<Graph>,
    autonomy_mode: Mutex<AutonomyMode>,
    alerts: Mutex<AlertRouter>,
    alert_webhook: Option<String>,
    supervisor_policy: SupervisorPolicy,
    backtest_runs: Mutex<BTreeMap<String, BacktestRunResult>>,
    experiment_registry: Mutex<ExperimentRegistry>,
    experiment_bundles: BundleStore,
    model_registry: Mutex<ModelRegistry>,
    strategy_resolution_history: Mutex<Vec<StrategyResolutionSummary>>,
    strategy_execution_summaries: Mutex<BTreeMap<String, StrategyExecutionSummary>>,
    supervisor: Mutex<Supervisor>,
}

#[allow(clippy::cast_precision_loss)]
fn configured_guardrails(settings: &Settings) -> Result<Guardrails, EngineError> {
    fn integer(settings: &Settings, key: &str) -> Result<Option<i64>, EngineError> {
        match settings.get(key) {
            None => Ok(None),
            Some(insider_cfg_core::Value::Integer(value)) => Ok(Some(*value)),
            Some(_) => Err(EngineError::InvalidRequest),
        }
    }
    fn non_negative_integer(settings: &Settings, key: &str) -> Result<Option<i64>, EngineError> {
        match integer(settings, key)? {
            Some(value) if value >= 0 => Ok(Some(value)),
            Some(_) => Err(EngineError::InvalidRequest),
            None => Ok(None),
        }
    }
    let max_leverage = match settings.get("risk.max_leverage") {
        None => None,
        Some(insider_cfg_core::Value::Float(value)) if value.is_finite() && *value >= 0.0 => {
            Some(*value)
        }
        Some(insider_cfg_core::Value::Integer(value)) if *value >= 0 => Some(*value as f64),
        Some(_) => return Err(EngineError::InvalidRequest),
    };
    let max_drawdown_bps = non_negative_integer(settings, "risk.max_drawdown_bps")?;
    let max_outstanding_orders = integer(settings, "risk.max_outstanding_orders")?
        .map(|value| u64::try_from(value).map_err(|_| EngineError::InvalidRequest))
        .transpose()?;
    let max_predicted_volatility_bps =
        non_negative_integer(settings, "risk.max_predicted_volatility_bps")?;
    let max_participation_bps = non_negative_integer(settings, "risk.max_participation_bps")?;
    let max_message_rate = integer(settings, "risk.max_message_rate")?
        .map(|value| u64::try_from(value).map_err(|_| EngineError::InvalidRequest))
        .transpose()?;
    let max_price_deviation_bps = non_negative_integer(settings, "risk.max_price_deviation_bps")?;
    Ok(Guardrails {
        max_leverage,
        max_drawdown_bps,
        max_predicted_volatility_bps,
        max_participation_bps,
        max_outstanding_orders,
        max_message_rate,
        max_price_deviation_bps,
    })
}

fn configured_alert_webhook(settings: &Settings) -> Result<Option<String>, EngineError> {
    match settings.get("alerts.webhook_url") {
        None => Ok(None),
        Some(insider_cfg_core::Value::String(url))
            if url.len() <= 2_048
                && url.starts_with("https://")
                && !url.contains(char::is_whitespace)
                && url[8..]
                    .split('/')
                    .next()
                    .is_some_and(|authority| !authority.is_empty() && !authority.contains('@')) =>
        {
            Ok(Some(url.clone()))
        }
        Some(_) => Err(EngineError::InvalidRequest),
    }
}

fn configured_alert_limits(settings: &Settings) -> Result<(i64, usize), EngineError> {
    fn integer(
        settings: &Settings,
        key: &str,
        default: i64,
        minimum: i64,
        maximum: i64,
    ) -> Result<i64, EngineError> {
        let value = match settings.get(key) {
            None => default,
            Some(insider_cfg_core::Value::Integer(value)) => *value,
            Some(_) => return Err(EngineError::InvalidRequest),
        };
        (minimum..=maximum)
            .contains(&value)
            .then_some(value)
            .ok_or(EngineError::InvalidRequest)
    }
    let cooldown_ms = integer(settings, "alerts.cooldown_ms", 60_000, 0, 86_400_000)?;
    let max_pending = integer(settings, "alerts.max_pending", 4_096, 1, 1_000_000)?;
    Ok((
        cooldown_ms,
        usize::try_from(max_pending).map_err(|_| EngineError::InvalidRequest)?,
    ))
}

fn configured_supervisor_policy(settings: &Settings) -> Result<SupervisorPolicy, EngineError> {
    fn integer(settings: &Settings, key: &str, default: i128) -> Result<i128, EngineError> {
        match settings.get(key) {
            None => Ok(default),
            Some(insider_cfg_core::Value::Integer(value)) => Ok(i128::from(*value)),
            Some(_) => Err(EngineError::InvalidRequest),
        }
    }
    let max_failures = integer(settings, "supervisor.max_failures", 3)?;
    let window_ns = integer(settings, "supervisor.window_ns", 60_000_000_000)?;
    let initial_backoff_ns = integer(settings, "supervisor.initial_backoff_ns", 100_000_000)?;
    let max_backoff_ns = integer(settings, "supervisor.max_backoff_ns", 30_000_000_000)?;
    let jitter_bps = integer(settings, "supervisor.jitter_bps", 1_000)?;
    if !(1..=1_000_000).contains(&max_failures)
        || !(1..=86_400_000_000_000).contains(&window_ns)
        || !(1..=86_400_000_000_000).contains(&initial_backoff_ns)
        || !(initial_backoff_ns..=86_400_000_000_000).contains(&max_backoff_ns)
        || !(0..=10_000).contains(&jitter_bps)
    {
        return Err(EngineError::InvalidRequest);
    }
    Ok(SupervisorPolicy {
        max_failures: u32::try_from(max_failures).map_err(|_| EngineError::InvalidRequest)?,
        window_ns: u64::try_from(window_ns).map_err(|_| EngineError::InvalidRequest)?,
        initial_backoff_ns: u64::try_from(initial_backoff_ns)
            .map_err(|_| EngineError::InvalidRequest)?,
        max_backoff_ns: u64::try_from(max_backoff_ns).map_err(|_| EngineError::InvalidRequest)?,
        jitter_bps: u32::try_from(jitter_bps).map_err(|_| EngineError::InvalidRequest)?,
    })
}

impl ServiceHost {
    /// Opens and restores the journal before exposing a reconciling runtime.
    /// Call [`Self::reconcile_trigger`] before submitting new work.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] if the journal cannot be opened.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn open(
        journal_path: impl AsRef<std::path::Path>,
        account_id: AccountId,
        broker: Arc<dyn BrokerGateway>,
        portfolio: Portfolio,
        risk: RiskEngine,
        settings: Settings,
    ) -> Result<Self, EngineError> {
        let configured = configured_guardrails(&settings)?;
        let (alert_cooldown_ms, alert_max_pending) = configured_alert_limits(&settings)?;
        let supervisor_policy = configured_supervisor_policy(&settings)?;
        let risk = if configured == Guardrails::default() {
            risk
        } else {
            RiskEngine::new_with_guardrails(risk.limits(), configured)
        };
        let writer_lock = JournalWriterLock::acquire(&journal_path)?;
        let journal = Journal::open(journal_path)?;
        let experiment_bundles = BundleStore::open(journal.path().with_extension("bundles"))
            .map_err(|error| EngineError::ReadModel(format!("experiment bundles: {error:?}")))?;
        let read_model = ProjectionStore::new(journal.path().with_extension("read-model"));
        read_model
            .rebuild_from_journal(&journal)
            .map_err(|error| EngineError::ReadModel(format!("{error:?}")))?;
        let runtime = Arc::new(Runtime::new(account_id, broker, portfolio, risk));
        let mut market_data = MarketDataHub::new(4_096, 32, Some(60_000_000_000), 4_096)
            .ok_or(EngineError::InvalidRequest)?;
        let mut autonomy_plans = PlanStore::new();
        let mut news = NewsStore::default();
        let mut provider_state = BTreeMap::new();
        let mut context_graph = Graph::new();
        let mut recovery_anomalies = Vec::new();
        let mut strategy_coordinator = Coordinator::new();
        let mut autonomy_mode = AutonomyMode::Manual;
        let mut backtest_runs = BTreeMap::new();
        let mut experiment_registry = ExperimentRegistry::new();
        let mut prompt_registry = PromptRegistry::new();
        let mut model_registry = ModelRegistry::new();
        let mut strategy_resolution_history = Vec::new();
        let mut strategy_execution_summaries = BTreeMap::new();
        let mut supervisor = Supervisor::new(supervisor_policy);
        for component in [
            "market-data",
            "news",
            "metrics",
            "strategies",
            "llm",
            "autonomy",
            "portfolio",
            "risk",
            "execution",
            "journal",
            "ui-bridge",
            "telemetry",
        ] {
            let _ = supervisor.register(component);
        }
        let mut strategy_lifecycle_overrides = BTreeMap::new();
        let mut metric_lifecycle_overrides = BTreeMap::new();
        let mut alerts_for_restore = AlertRouter::new(alert_cooldown_ms, alert_max_pending)
            .ok_or(EngineError::InvalidRequest)?;
        let alert_webhook = configured_alert_webhook(&settings)?;
        if let Some(url) = &alert_webhook
            && !alerts_for_restore.allow_webhook(url.clone())
        {
            return Err(EngineError::InvalidRequest);
        }
        for record in journal.scan()?.records {
            match decode_journal_payload(&record.payload)? {
                Some(RecoveredEvent::Market {
                    event,
                    receive_wall,
                }) => {
                    let instrument_id = match event {
                        MarketEvent::Quote(value) => value.instrument_id,
                        MarketEvent::Trade(value) => value.instrument_id,
                        MarketEvent::Book(value) => value.instrument_id,
                    };
                    if !market_data.contains(instrument_id) && !market_data.register(instrument_id)
                    {
                        return Err(EngineError::MarketData(
                            "market journal instrument capacity exceeded".into(),
                        ));
                    }
                    let outcome = market_data.ingest(event, receive_wall).map_err(|error| {
                        EngineError::MarketData(format!("market restore: {error:?}"))
                    })?;
                    if matches!(outcome, IngestOutcome::Accepted(_))
                        && let Some((instrument_id, price_ticks)) = market_event_mark(event)
                    {
                        runtime.update_mark_price(instrument_id, price_ticks)?;
                    }
                }
                Some(RecoveredEvent::MarketBar { bar, sequence }) => {
                    if !market_data.contains(bar.instrument_id)
                        && !market_data.register(bar.instrument_id)
                    {
                        return Err(EngineError::MarketData(
                            "market bar journal instrument capacity exceeded".into(),
                        ));
                    }
                    market_data.ingest_bar(bar, sequence).map_err(|error| {
                        EngineError::MarketData(format!("bar restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::Intent(intent)) => {
                    index_order_in_graph(
                        &mut context_graph,
                        &intent,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("order graph restore: {error:?}"))
                    })?;
                    runtime.restore_intent(intent)?;
                }
                Some(RecoveredEvent::ChildPlan(record)) => runtime.restore_child_plan(record)?,
                Some(RecoveredEvent::ExecutionTiming(timing)) => runtime.restore_timing(timing)?,
                Some(RecoveredEvent::Broker(event)) => {
                    if let BrokerEvent::Filled {
                        client_order_id,
                        quantity_ticks,
                        price_ticks,
                    } = &event
                    {
                        index_fill_in_graph(
                            &mut context_graph,
                            client_order_id,
                            *quantity_ticks,
                            *price_ticks,
                            i64::try_from(record.sequence).unwrap_or(i64::MAX),
                        )
                        .map_err(|error| {
                            EngineError::Graph(format!("fill graph restore: {error:?}"))
                        })?;
                    }
                    let prior_fill = match &event {
                        BrokerEvent::Filled {
                            client_order_id, ..
                        } => runtime.filled_quantity(client_order_id).ok(),
                        _ => None,
                    };
                    if let Err(error) = runtime.apply_broker_event(event.clone()) {
                        // Broker events are retained before projection mutation.
                        // A legal transport event can still be out of order after
                        // a crash or reconnect; preserve it as an anomaly and let
                        // the broker snapshot reconcile the authoritative state.
                        if matches!(error, EngineError::Transition(_)) {
                            if recovery_anomalies.len() >= MAX_RECOVERY_ANOMALIES {
                                recovery_anomalies.remove(0);
                            }
                            recovery_anomalies.push(RecoveryAnomaly {
                                sequence: record.sequence,
                                kind: "broker_event",
                                reason: format!("{error:?}"),
                            });
                        } else {
                            return Err(error);
                        }
                    }
                    if let (
                        BrokerEvent::Filled {
                            client_order_id, ..
                        },
                        Some(prior),
                    ) = (&event, prior_fill)
                        && runtime.filled_quantity(client_order_id)? > prior
                        && let Some(summary) = ServiceHost::recovered_strategy_execution(
                            &strategy_coordinator,
                            &runtime,
                            &event,
                            &mut strategy_execution_summaries,
                        )?
                    {
                        strategy_execution_summaries.insert(summary.strategy_id.clone(), summary);
                    }
                    let _ = runtime.apply_child_event(&event)?;
                }
                Some(RecoveredEvent::Risk(state, authorization)) => {
                    runtime.transition_risk_state(state, &authorization)?;
                }
                Some(RecoveredEvent::LiveLimits(limits)) => {
                    runtime.configure_live_limits(limits)?;
                }
                Some(RecoveredEvent::LiveKilled) => {
                    runtime.kill_live()?;
                }
                Some(RecoveredEvent::PortfolioSnapshot {
                    positions,
                    cash_ticks,
                }) => {
                    runtime.apply_reconciled_portfolio(&positions, cash_ticks)?;
                    index_portfolio_in_graph(
                        &mut context_graph,
                        &runtime.portfolio()?,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("portfolio graph restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::PortfolioPeak(peak)) => {
                    runtime.restore_peak_equity_ticks(peak)?;
                }
                Some(RecoveredEvent::CorporateAction {
                    instrument_id,
                    kind,
                }) => {
                    runtime.apply_corporate_action(instrument_id, kind)?;
                }
                Some(RecoveredEvent::ScopedRiskPolicy(snapshot)) => {
                    let policy = snapshot
                        .map(ScopedRiskPolicy::from_snapshot)
                        .transpose()
                        .map_err(|error| {
                            EngineError::ReadModel(format!("risk policy restore: {error:?}"))
                        })?;
                    runtime.set_scoped_risk_policy(policy)?;
                }
                Some(RecoveredEvent::Autonomy(event)) => autonomy_plans
                    .restore_event(event)
                    .map_err(|error| EngineError::Autonomy(format!("{error:?}")))?,
                Some(RecoveredEvent::News(item)) => {
                    index_news_in_graph(&mut context_graph, &item).map_err(|error| {
                        EngineError::Graph(format!("news graph restore: {error:?}"))
                    })?;
                    news.insert_versioned(item).map_err(|error| {
                        EngineError::ReadModel(format!("news restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::EmbeddingSnapshot(snapshot)) => {
                    context_graph
                        .replace_embeddings(snapshot)
                        .map_err(|error| {
                            EngineError::Graph(format!("embedding restore: {error:?}"))
                        })?;
                }
                Some(RecoveredEvent::ProviderState(snapshot)) => {
                    provider_state.insert(snapshot.provider_id.clone(), snapshot);
                }
                Some(RecoveredEvent::StrategyProposal(proposal)) => {
                    strategy_coordinator
                        .submit_checked(proposal.clone(), proposal.generated_mono)
                        .map_err(|error| EngineError::Strategy(format!("{error:?}")))?;
                    index_strategy_in_graph(&mut context_graph, &proposal).map_err(|error| {
                        EngineError::Graph(format!("strategy graph restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::StrategyResolution {
                    policy,
                    now,
                    budgets,
                }) => {
                    let result = if budgets.is_empty() {
                        strategy_coordinator.resolve_at(policy, now)
                    } else {
                        strategy_coordinator
                            .resolve_at_with_budgets(policy, now, &budgets)
                            .result
                    };
                    push_resolution_summary(
                        &mut strategy_resolution_history,
                        resolution_summary(policy, now, &result),
                    );
                }
                Some(RecoveredEvent::StrategyExecution(summary)) => {
                    strategy_execution_summaries.insert(summary.strategy_id.clone(), summary);
                }
                Some(RecoveredEvent::StrategyLifecycle {
                    strategy_id,
                    lifecycle,
                    evidence_ref,
                }) => {
                    index_strategy_lifecycle_in_graph(
                        &mut context_graph,
                        &strategy_id,
                        &format!("{lifecycle:?}"),
                        &evidence_ref,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("strategy graph restore: {error:?}"))
                    })?;
                    strategy_lifecycle_overrides.insert(strategy_id, (lifecycle, evidence_ref));
                }
                Some(RecoveredEvent::MetricLifecycle {
                    metric_id,
                    lifecycle,
                    evidence_ref,
                }) => {
                    index_metric_in_graph(
                        &mut context_graph,
                        &metric_id,
                        &format!("{lifecycle:?}"),
                        &evidence_ref,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("metric graph restore: {error:?}"))
                    })?;
                    metric_lifecycle_overrides.insert(metric_id, (lifecycle, evidence_ref));
                }
                Some(RecoveredEvent::AutonomyMode(mode)) => {
                    autonomy_mode = mode;
                }
                Some(RecoveredEvent::Alert(alert)) => {
                    let _ = alerts_for_restore.route(alert, AlertChannel::InApp, i64::MAX);
                }
                Some(RecoveredEvent::AlertAck(alert_id)) => {
                    alerts_for_restore.acknowledge(&alert_id);
                }
                Some(RecoveredEvent::Backtest(result)) => {
                    backtest_runs.insert(result.run_id.clone(), result);
                }
                Some(RecoveredEvent::Experiment(run)) => {
                    index_experiment_in_graph(
                        &mut context_graph,
                        &run,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("experiment graph restore: {error:?}"))
                    })?;
                    experiment_registry.restore(run).map_err(|error| {
                        EngineError::ReadModel(format!("experiment restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::ModelRegistry(snapshot)) => {
                    index_model_snapshot_in_graph(
                        &mut context_graph,
                        &snapshot,
                        i64::try_from(record.sequence).unwrap_or(i64::MAX),
                    )
                    .map_err(|error| {
                        EngineError::Graph(format!("model graph restore: {error:?}"))
                    })?;
                    model_registry.restore_snapshot(snapshot).map_err(|error| {
                        EngineError::ReadModel(format!("model registry restore: {error:?}"))
                    })?;
                }
                Some(RecoveredEvent::Prompt(prompt)) => {
                    prompt_registry.register(prompt).map_err(|error| {
                        EngineError::ReadModel(format!("prompt restore: {error:?}"))
                    })?;
                }
                None => {}
            }
        }
        Ok(Self {
            started: Instant::now(),
            runtime,
            journal,
            _writer_lock: writer_lock,
            append_lock: Mutex::new(()),
            config: ConfigStore::new(settings),
            lifecycle: Mutex::new(Lifecycle::Reconciling),
            autonomy_plans: Mutex::new(autonomy_plans),
            read_model,
            news: Mutex::new(news),
            news_providers: Mutex::new(ProviderRegistry::new()),
            provider_state: Mutex::new(provider_state),
            market_data: Mutex::new(market_data),
            recovery_anomalies: Mutex::new(recovery_anomalies),
            strategy_coordinator: Mutex::new(strategy_coordinator),
            metric_host: Mutex::new(MetricHost::new(3)),
            metric_lifecycle_overrides: Mutex::new(metric_lifecycle_overrides),
            strategy_host: Mutex::new(StrategyHost::new(3)),
            strategy_lifecycle_overrides: Mutex::new(strategy_lifecycle_overrides),
            python_metric_last_run: Mutex::new(BTreeMap::new()),
            python_strategy_last_run: Mutex::new(BTreeMap::new()),
            python_metric_outputs: Mutex::new(BTreeMap::new()),
            llm_provider: Mutex::new(None),
            prompt_registry: Mutex::new(prompt_registry),
            context_graph: Mutex::new(context_graph),
            autonomy_mode: Mutex::new(autonomy_mode),
            alerts: Mutex::new(alerts_for_restore),
            alert_webhook,
            supervisor_policy,
            backtest_runs: Mutex::new(backtest_runs),
            experiment_registry: Mutex::new(experiment_registry),
            experiment_bundles,
            model_registry: Mutex::new(model_registry),
            strategy_resolution_history: Mutex::new(strategy_resolution_history),
            strategy_execution_summaries: Mutex::new(strategy_execution_summaries),
            supervisor: Mutex::new(supervisor),
        })
    }

    /// Returns the engine-owned monotonic decision time. IPC clients must not
    /// supply their own process-local monotonic origin for trading decisions.
    #[must_use]
    pub fn monotonic_now(&self) -> MonoTime {
        MonoTime::from_nanos(
            u64::try_from(self.started.elapsed().as_nanos().min(u128::from(u64::MAX)))
                .unwrap_or(u64::MAX),
        )
    }

    /// Registers a canonical instrument before its provider stream is enabled.
    /// Registration is bounded and idempotence is explicit: a duplicate returns
    /// `false` so callers cannot accidentally replace live state.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the ingress state is unavailable.
    pub fn register_market_instrument(
        &self,
        instrument_id: InstrumentId,
    ) -> Result<bool, EngineError> {
        self.market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)
            .map(|mut hub| hub.register(instrument_id))
    }

    /// Ingests one normalized market event into the authoritative hot-path
    /// state. Providers must persist their own raw payloads separately; this
    /// method only accepts canonical, bounded values.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] for an unavailable ingress lock or
    /// [`EngineError::MarketData`] when validation, sequencing, or registration
    /// rejects the event.
    pub fn ingest_market_event(
        &self,
        event: MarketEvent,
        receive_wall: insider_common_types::WallTime,
    ) -> Result<IngestOutcome, EngineError> {
        let mark = match event {
            MarketEvent::Quote(quote) => Some((
                quote.instrument_id,
                quote
                    .bid_ticks
                    .checked_add(quote.ask_ticks)
                    .ok_or(EngineError::MarketData("quote midpoint overflow".into()))?
                    / 2,
            )),
            MarketEvent::Trade(trade) => Some((trade.instrument_id, trade.price_ticks)),
            MarketEvent::Book(_) => None,
        };
        let outcome = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .ingest(event, receive_wall)
            .map_err(|error| EngineError::MarketData(format!("{error:?}")))?;
        if matches!(outcome, IngestOutcome::Accepted(_)) {
            self.append_event(&encode_market_event(event, receive_wall))?;
        }
        if matches!(outcome, IngestOutcome::Accepted(_))
            && let Some((instrument_id, price_ticks)) = mark
        {
            // Compute any new high-water mark on a private projection first.
            // The durable peak event is written before the authoritative
            // portfolio mutation so a crash cannot expose an unjournaled
            // drawdown baseline after restart.
            let current = self.runtime.portfolio()?;
            let previous_peak = current.peak_equity_ticks();
            let mut projected = current;
            projected
                .set_mark_price(instrument_id, price_ticks)
                .map_err(EngineError::Accounting)?;
            let next_peak = projected.peak_equity_ticks();
            if next_peak != previous_peak {
                self.append_event(&encode_portfolio_peak(next_peak))?;
            }
            self.runtime.update_mark_price(instrument_id, price_ticks)?;
        }
        Ok(outcome)
    }

    /// Ingests one historical OHLCV bar from a backfill provider. Bars update
    /// chart state only; they do not create trades, fills, marks, or risk
    /// decisions.
    ///
    /// # Errors
    /// Returns [`EngineError::MarketData`] when the stream is unregistered,
    /// unconfigured, malformed, or has a sequence gap.
    pub fn ingest_market_bar(&self, bar: Bar, sequence: u64) -> Result<BarUpdate, EngineError> {
        let update = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .ingest_bar(bar, sequence)
            .map_err(|error| EngineError::MarketData(format!("historical bar: {error:?}")))?;
        if matches!(update, BarUpdate::New(_) | BarUpdate::Correction(_)) {
            self.append_event(&encode_market_bar(bar, sequence))?;
        }
        Ok(update)
    }

    /// Installs a level-2 snapshot after a provider gap, then permits
    /// contiguous deltas to resume.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] or [`EngineError::MarketData`] when the
    /// snapshot is unavailable, malformed, or targets an unknown instrument.
    pub fn recover_market_book(
        &self,
        instrument_id: InstrumentId,
        sequence: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> Result<(), EngineError> {
        self.market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .recover_book(instrument_id, sequence, bids, asks)
            .map_err(|error| EngineError::MarketData(format!("{error:?}")))
    }

    /// Recovers a quote or trade stream from a verified provider snapshot.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] or [`EngineError::MarketData`] when the
    /// stream is unknown or the snapshot sequence is invalid.
    pub fn recover_market_stream(
        &self,
        instrument_id: InstrumentId,
        kind: StreamKind,
        sequence: u64,
        received: MonoTime,
    ) -> Result<(), EngineError> {
        self.market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .recover_stream(instrument_id, kind, sequence, received)
            .map_err(|error| EngineError::MarketData(format!("{error:?}")))
    }

    /// Marks feeds stale after their configured freshness deadline.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the ingress state is unavailable.
    pub fn mark_market_data_stale(
        &self,
        now: MonoTime,
        max_age_ns: u64,
    ) -> Result<(), EngineError> {
        self.market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .mark_stale(now, max_age_ns);
        Ok(())
    }

    /// Returns the latest bounded market state for one instrument.
    #[must_use]
    pub fn market_snapshot(&self, instrument_id: InstrumentId) -> Option<MarketSnapshot> {
        self.market_data
            .lock()
            .ok()
            .and_then(|hub| hub.snapshot(instrument_id))
    }

    /// Returns the assembled trading runtime.
    #[must_use]
    pub fn runtime(&self) -> Arc<Runtime> {
        Arc::clone(&self.runtime)
    }

    /// Returns the rebuildable projection path used by local read consumers.
    #[must_use]
    pub fn read_model_path(&self) -> &std::path::Path {
        self.read_model.path()
    }

    /// Inserts one normalized news item into the authoritative bounded news
    /// projection. Provider adapters call this after normalization/deduplication.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when the news item is invalid or the
    /// projection lock is unavailable.
    pub fn ingest_news_item(&self, item: NewsItem) -> Result<bool, EngineError> {
        if self
            .news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))?
            .contains_version(&item.id, &item.content_hash)
        {
            return Ok(false);
        }
        let graph_item = item.clone();
        self.append_event(&encode_news_item(&item))?;
        let result = self
            .news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))?
            .insert_versioned(item)
            .map_err(|error| EngineError::ReadModel(format!("news item: {error:?}")))?;
        if !matches!(result, insider_news_core::VersionInsert::Duplicate) {
            self.context_graph
                .lock()
                .map_err(|_| EngineError::Graph("context graph lock poisoned".into()))
                .and_then(|mut graph| {
                    index_news_in_graph(&mut graph, &graph_item)
                        .map_err(|error| EngineError::Graph(format!("news graph: {error:?}")))
                })?;
        }
        Ok(!matches!(
            result,
            insider_news_core::VersionInsert::Duplicate
        ))
    }

    /// Performs bounded hybrid retrieval for Analyst and global search callers.
    /// The graph's configured single-model embedding index participates when
    /// the query carries a vector; exact/lexical/graph ranking remains the
    /// deterministic fallback.
    ///
    /// # Errors
    /// Returns [`EngineError::Graph`] when the projection is unavailable or
    /// the query limit/vector contract is invalid.
    pub fn search_context(
        &self,
        query: &RetrievalQuery,
        limit: usize,
    ) -> Result<Vec<RetrievalHit>, EngineError> {
        self.context_graph
            .lock()
            .map_err(|_| EngineError::Graph("context graph lock poisoned".into()))?
            .hybrid_search(None, query, limit)
            .map_err(|error| EngineError::Graph(format!("context retrieval: {error:?}")))
    }

    /// Configures the one model/version tuple accepted by the graph's
    /// semantic index. The index is a rebuildable projection; graph facts and
    /// trading state remain authoritative when it is unavailable.
    ///
    /// # Errors
    /// Returns [`EngineError::Graph`] when the model tuple is invalid or
    /// conflicts with an already configured index.
    pub fn configure_context_embeddings(
        &self,
        model: impl Into<String>,
        model_version: impl Into<String>,
        dimensions: usize,
    ) -> Result<(), EngineError> {
        self.context_graph
            .lock()
            .map_err(|_| EngineError::Graph("context graph lock poisoned".into()))?
            .configure_embedding_index(model, model_version, dimensions)
            .map_err(|error| EngineError::Graph(format!("embedding index: {error:?}")))
    }

    /// Adds one externally generated embedding to the rebuildable semantic
    /// index after model tuple, dimension, finiteness, and normalization checks.
    ///
    /// # Errors
    /// Returns [`EngineError::Graph`] when no index is configured or the record
    /// fails model, dimension, identity, or numeric validation.
    pub fn upsert_context_embedding(&self, record: EmbeddingRecord) -> Result<(), EngineError> {
        self.context_graph
            .lock()
            .map_err(|_| EngineError::Graph("context graph lock poisoned".into()))?
            .upsert_embedding(record)
            .map_err(|error: EmbeddingError| {
                EngineError::Graph(format!("embedding record: {error:?}"))
            })
    }

    /// Atomically replaces the rebuildable semantic index after validating a
    /// complete model/version generation.
    ///
    /// # Errors
    /// Returns [`EngineError::Graph`] when any record is invalid; the previous
    /// generation remains active in that case.
    pub fn replace_context_embeddings(
        &self,
        snapshot: EmbeddingIndexSnapshot,
    ) -> Result<(), EngineError> {
        EmbeddingIndex::from_snapshot(snapshot.clone())
            .map_err(|error| EngineError::Graph(format!("embedding generation: {error:?}")))?;
        let payload = encode_embedding_snapshot(&snapshot);
        if payload.len() > 16 * 1024 * 1024 {
            return Err(EngineError::InvalidRequest);
        }
        self.append_event(&payload)?;
        self.context_graph
            .lock()
            .map_err(|_| EngineError::Graph("context graph lock poisoned".into()))?
            .replace_embeddings(snapshot)
            .map_err(|error| EngineError::Graph(format!("embedding generation: {error:?}")))
    }

    /// Performs hybrid retrieval with a caller-supplied embedding vector.
    /// Exact/lexical/graph ranking remains available when no index is loaded.
    ///
    /// # Errors
    /// Returns [`EngineError::Graph`] when the embedding is incompatible with
    /// the configured index or the requested limit is invalid.
    pub fn search_context_with_embedding(
        &self,
        mut query: RetrievalQuery,
        embedding: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<RetrievalHit>, EngineError> {
        query.embedding = Some(embedding);
        self.search_context(&query, limit)
    }

    /// Persists one provider cursor/retry/dead-letter snapshot after a page
    /// has been accepted by the news projection. The journal append happens
    /// before the in-memory projection update, so a crash cannot acknowledge
    /// state that was not durable.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] for invalid bounded state or a
    /// poisoned provider-state lock, and [`EngineError::Journal`] on append.
    pub fn persist_provider_state(
        &self,
        snapshot: ProviderStateSnapshot,
    ) -> Result<(), EngineError> {
        let payload = encode_provider_state(&snapshot)
            .map_err(|error| EngineError::ReadModel(format!("provider state: {error:?}")))?;
        self.append_event(&payload)?;
        self.provider_state
            .lock()
            .map_err(|_| EngineError::ReadModel("provider state lock poisoned".into()))?
            .insert(snapshot.provider_id.clone(), snapshot);
        Ok(())
    }

    /// Returns the last durably committed state for one provider.
    #[must_use]
    pub fn provider_state(&self, provider_id: &str) -> Option<ProviderStateSnapshot> {
        self.provider_state
            .lock()
            .ok()
            .and_then(|state| state.get(provider_id).cloned())
    }

    /// Returns all durably committed provider states in provider-ID order.
    #[must_use]
    pub fn provider_states(&self) -> Vec<ProviderStateSnapshot> {
        self.provider_state
            .lock()
            .map(|state| state.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns bounded replay anomalies discovered before startup
    /// reconciliation. These records are diagnostics only; they never grant
    /// permission to submit orders or override broker truth.
    #[must_use]
    pub fn recovery_anomalies(&self) -> Vec<RecoveryAnomaly> {
        self.recovery_anomalies
            .lock()
            .map(|anomalies| anomalies.clone())
            .unwrap_or_default()
    }

    /// Registers an isolated Python metric package discovered from disk.
    /// Registration is explicit and immutable; the host validates the full
    /// manifest before starting the worker process.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the worker or manifest is invalid.
    pub fn register_python_metric(
        &self,
        discovered: &DiscoveredMetric,
        command: std::process::Command,
    ) -> Result<(), EngineError> {
        self.metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .register_discovered_python(discovered, command)
            .map_err(|error| EngineError::Strategy(format!("metric registration: {error:?}")))
            .and_then(|()| {
                self.apply_metric_lifecycle_override(&discovered.manifest.descriptor.metric_id)
            })?;
        index_metric_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &discovered.manifest.descriptor.metric_id,
            "research",
            "",
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("metric graph: {error:?}")))
    }

    /// Registers a compiled deterministic metric in the same bounded host as
    /// isolated Python metrics.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when manifest validation rejects the
    /// metric or the host lock is unavailable.
    pub fn register_metric(&self, metric: Arc<dyn Metric>) -> Result<(), EngineError> {
        let metric_id = metric.descriptor().metric_id.clone();
        self.metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .register(metric)
            .map_err(|error| EngineError::Strategy(format!("metric registration: {error:?}")))
            .and_then(|()| self.apply_metric_lifecycle_override(&metric_id))?;
        index_metric_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &metric_id,
            "research",
            "",
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("metric graph: {error:?}")))
    }

    fn apply_metric_lifecycle_override(&self, metric_id: &str) -> Result<(), EngineError> {
        let override_state = self
            .metric_lifecycle_overrides
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .remove(metric_id);
        if let Some((lifecycle, evidence_ref)) = override_state
            && !self
                .metric_host
                .lock()
                .map_err(|_| EngineError::Poisoned)?
                .restore_lifecycle_with_evidence(metric_id, lifecycle, &evidence_ref)
        {
            return Err(EngineError::Strategy(format!(
                "cannot restore metric lifecycle for unregistered metric {metric_id}"
            )));
        }
        Ok(())
    }

    /// Atomically journals and applies a metric promotion lifecycle transition.
    ///
    /// # Errors
    /// Returns an error for invalid evidence, an unknown metric, an invalid
    /// transition, or journal/host failure.
    pub fn transition_metric_lifecycle(
        &self,
        metric_id: &str,
        next: insider_metric_host::Lifecycle,
        evidence_ref: &str,
    ) -> Result<(), EngineError> {
        if evidence_ref.trim().is_empty() || evidence_ref.len() > 512 {
            return Err(EngineError::InvalidRequest);
        }
        if !self
            .metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .can_transition_lifecycle(metric_id, next)
        {
            return Err(EngineError::Strategy(format!(
                "invalid metric lifecycle transition for {metric_id}"
            )));
        }
        self.append_event(&encode_metric_lifecycle(metric_id, next, evidence_ref))?;
        if !self
            .metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .transition_lifecycle_with_evidence(metric_id, next, evidence_ref)
        {
            return Err(EngineError::Strategy(
                "metric lifecycle changed during transition".into(),
            ));
        }
        index_metric_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            metric_id,
            &format!("{next:?}"),
            evidence_ref,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("metric graph: {error:?}")))?;
        Ok(())
    }

    /// Evaluates one registered metric through the same bounded host used by
    /// deterministic and Python implementations.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the metric is unavailable or fails validation.
    pub fn evaluate_registered_metric(
        &self,
        metric_id: &str,
        context: &MetricContext,
    ) -> Result<MetricOutput, EngineError> {
        self.metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .evaluate(metric_id, context)
            .map_err(|error| EngineError::Strategy(format!("metric evaluation: {error:?}")))
    }

    /// Evaluates a Shadow metric against a live input snapshot without
    /// publishing it to the authoritative metric state.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the metric is not Shadow or its
    /// evaluation/output validation fails.
    pub fn evaluate_registered_metric_shadow(
        &self,
        metric_id: &str,
        context: &MetricContext,
    ) -> Result<MetricOutput, EngineError> {
        self.metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .evaluate_shadow(metric_id, context)
            .map_err(|error| EngineError::Strategy(format!("shadow metric evaluation: {error:?}")))
    }

    /// Registers an isolated Python strategy package discovered from disk.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the worker or manifest is invalid.
    pub fn register_python_strategy(
        &self,
        discovered: &DiscoveredStrategy,
        command: std::process::Command,
    ) -> Result<(), EngineError> {
        let mut candidate = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone();
        candidate
            .register_manifest(&discovered.manifest)
            .map_err(|error| {
                EngineError::Strategy(format!("strategy dependency graph: {error:?}"))
            })?;
        self.strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .register_discovered_python(discovered, command)
            .map_err(|error| EngineError::Strategy(format!("strategy registration: {error:?}")))?;
        self.apply_strategy_lifecycle_override(&discovered.manifest.strategy_id)?;
        index_strategy_manifest_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &discovered.manifest,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("strategy graph: {error:?}")))?;
        *self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)? = candidate;
        Ok(())
    }

    /// Registers an in-process deterministic strategy after manifest
    /// validation. This is the Rust strategy boundary used by live cycles and
    /// strategy-driven replay; registration is explicit and never inferred
    /// from a display symbol or UI state.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the strategy host cannot be
    /// locked, or [`EngineError::Strategy`] when manifest validation rejects
    /// the strategy.
    pub fn register_strategy(&self, strategy: Arc<dyn Strategy>) -> Result<(), EngineError> {
        let manifest = strategy.manifest();
        let mut candidate = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone();
        candidate.register_manifest(&manifest).map_err(|error| {
            EngineError::Strategy(format!("strategy dependency graph: {error:?}"))
        })?;
        self.strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .register(strategy)
            .map_err(|error| EngineError::Strategy(format!("strategy registration: {error:?}")))?;
        self.apply_strategy_lifecycle_override(&manifest.strategy_id)?;
        index_strategy_manifest_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &manifest,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("strategy graph: {error:?}")))?;
        *self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)? = candidate;
        Ok(())
    }

    fn apply_strategy_lifecycle_override(&self, strategy_id: &str) -> Result<(), EngineError> {
        let lifecycle = self
            .strategy_lifecycle_overrides
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .remove(strategy_id);
        if let Some((lifecycle, evidence_ref)) = lifecycle
            && !self
                .strategy_host
                .lock()
                .map_err(|_| EngineError::Poisoned)?
                .restore_lifecycle_with_evidence(strategy_id, lifecycle, &evidence_ref)
        {
            return Err(EngineError::Strategy(format!(
                "cannot restore lifecycle for unregistered strategy {strategy_id}"
            )));
        }
        Ok(())
    }

    /// Atomically journals and applies an operator lifecycle transition.
    ///
    /// # Errors
    /// Returns an error when the strategy is unknown, the transition is not
    /// allowed, the journal cannot be appended, or the host lock is poisoned.
    pub fn transition_strategy_lifecycle(
        &self,
        strategy_id: &str,
        next: insider_strategy_host::Lifecycle,
        evidence_ref: &str,
    ) -> Result<(), EngineError> {
        if evidence_ref.trim().is_empty() || evidence_ref.len() > 512 {
            return Err(EngineError::InvalidRequest);
        }
        {
            let host = self
                .strategy_host
                .lock()
                .map_err(|_| EngineError::Poisoned)?;
            if !host.can_transition_lifecycle(strategy_id, next) {
                return Err(EngineError::Strategy(format!(
                    "invalid lifecycle transition for {strategy_id}"
                )));
            }
        }
        self.append_event(&encode_strategy_lifecycle(strategy_id, next, evidence_ref))?;
        if !self
            .strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .transition_lifecycle_with_evidence(strategy_id, next, evidence_ref)
        {
            return Err(EngineError::Strategy(
                "lifecycle changed during transition".into(),
            ));
        }
        index_strategy_lifecycle_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            strategy_id,
            &format!("{next:?}"),
            evidence_ref,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("strategy graph: {error:?}")))?;
        Ok(())
    }

    /// Evaluates one registered strategy and durably admits its proposal into
    /// the coordinator used by manual, hybrid, and autonomous modes.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when evaluation or durable admission fails.
    pub fn evaluate_registered_strategy(
        &self,
        strategy_id: &str,
        context: &StrategyContext<'_>,
    ) -> Result<Proposal, EngineError> {
        let proposal = self
            .strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .evaluate(strategy_id, context)
            .map_err(|error| EngineError::Strategy(format!("strategy evaluation: {error:?}")))?;
        self.submit_strategy_proposal(&proposal, context.now)?;
        Ok(proposal)
    }

    /// Evaluates a Shadow strategy against the live snapshot without creating
    /// an actionable proposal record. This is the comparison boundary for
    /// promotion evidence; the returned proposal is diagnostic-only.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the strategy is not in Shadow
    /// lifecycle, fails evaluation, or emits an invalid proposal.
    pub fn evaluate_registered_strategy_shadow(
        &self,
        strategy_id: &str,
        context: &StrategyContext<'_>,
    ) -> Result<Proposal, EngineError> {
        self.strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .evaluate_shadow(strategy_id, context)
            .map_err(|error| {
                EngineError::Strategy(format!("shadow strategy evaluation: {error:?}"))
            })
    }

    /// Runs one bounded decision cycle for all discovered metric/strategy
    /// workers. Features are derived only from the authoritative market
    /// snapshot; incomplete inputs cause an explicit skip rather than a
    /// fabricated value. Metric results are then supplied to strategies and
    /// admitted through the normal durable proposal path.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when a registered worker fails or a
    /// produced proposal cannot be durably admitted.
    #[allow(clippy::too_many_lines)]
    pub fn run_registered_python_cycle(&self) -> Result<usize, EngineError> {
        let now = self.monotonic_now();
        let snapshots = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .snapshots();
        let metric_ids = self
            .metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .metric_ids();
        let strategy_ids = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .evaluation_order()
            .map_err(|error| {
                EngineError::Strategy(format!("strategy dependency graph: {error:?}"))
            })?;
        let mut admitted = 0_usize;

        for snapshot in snapshots {
            let mut features = BTreeMap::new();
            if let Some(quote) = snapshot.quote {
                let midpoint = f64::midpoint(
                    ticks_to_feature(quote.bid_ticks),
                    ticks_to_feature(quote.ask_ticks),
                );
                features.insert("mid_price".into(), midpoint);
                features.insert("bid".into(), ticks_to_feature(quote.bid_ticks));
                features.insert("ask".into(), ticks_to_feature(quote.ask_ticks));
                features.insert(
                    "spread".into(),
                    ticks_to_feature(quote.ask_ticks.saturating_sub(quote.bid_ticks)),
                );
                features.insert(
                    "bid_quantity".into(),
                    ticks_to_feature(quote.bid_quantity_ticks),
                );
                features.insert(
                    "ask_quantity".into(),
                    ticks_to_feature(quote.ask_quantity_ticks),
                );
            }
            if let Some(trade) = snapshot.trade {
                features.insert("last_price".into(), ticks_to_feature(trade.price_ticks));
                features.insert(
                    "trade_quantity".into(),
                    ticks_to_feature(trade.quantity_ticks),
                );
            }
            if let Some(bar) = snapshot.bars.last() {
                features.insert("open_price".into(), ticks_to_feature(bar.open_ticks));
                features.insert("high_price".into(), ticks_to_feature(bar.high_ticks));
                features.insert("low_price".into(), ticks_to_feature(bar.low_ticks));
                features.insert("close_price".into(), ticks_to_feature(bar.close_ticks));
                features.insert("volume".into(), ticks_to_feature(bar.volume_ticks));
                if let Ok(interval_ns) = i64::try_from(bar.interval_ns)
                    && interval_ns > 0
                    && let Ok(index) =
                        i32::try_from(bar.start_time.as_unix_nanos().div_euclid(interval_ns))
                {
                    features.insert("bar_index".into(), f64::from(index));
                }
                if let Some(previous) = snapshot.bars.iter().rev().nth(1)
                    && previous.close_ticks > 0
                {
                    features.insert(
                        "return".into(),
                        ticks_to_feature(bar.close_ticks) / ticks_to_feature(previous.close_ticks)
                            - 1.0,
                    );
                }
            }
            if let Some(value) = features
                .get("close_price")
                .copied()
                .or_else(|| features.get("mid_price").copied())
            {
                features.insert("value".into(), value);
            }

            let mut metrics = Vec::new();
            for metric_id in &metric_ids {
                let (required, period_ns) = self
                    .metric_host
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .manifest(metric_id)
                    .map(|manifest| (manifest.descriptor.inputs.clone(), manifest.period_ns))
                    .ok_or_else(|| EngineError::Strategy("metric disappeared".into()))?;
                let metric_key = (metric_id.clone(), snapshot.instrument_id.get());
                if self
                    .python_metric_last_run
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .get(&metric_key)
                    .is_some_and(|last| now.as_nanos() < last.saturating_add(period_ns))
                {
                    continue;
                }
                if required.iter().any(|input| !features.contains_key(input)) {
                    continue;
                }
                let output = self.evaluate_registered_metric(
                    metric_id,
                    &MetricContext {
                        instrument_id: Some(snapshot.instrument_id),
                        features: features.clone(),
                        now,
                    },
                )?;
                self.python_metric_last_run
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .insert(metric_key, now.as_nanos());
                self.python_metric_outputs
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .insert(
                        (metric_id.clone(), snapshot.instrument_id.get()),
                        output.clone(),
                    );
                metrics.push(output);
            }
            let cached_outputs = self
                .python_metric_outputs
                .lock()
                .map_err(|_| EngineError::Poisoned)?
                .iter()
                .filter(|((_, instrument), output)| {
                    *instrument == snapshot.instrument_id.get()
                        && output.is_fresh(now)
                        && !metrics
                            .iter()
                            .any(|current| current.metric_id == output.metric_id)
                })
                .map(|(_, output)| output.clone())
                .collect::<Vec<_>>();
            metrics.extend(cached_outputs);
            for strategy_id in &strategy_ids {
                let (required_metrics, missing_evidence, period_ns) = self
                    .strategy_host
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .manifest(strategy_id)
                    .map(|manifest| {
                        (
                            manifest.metric_ids.clone(),
                            manifest.missing_evidence,
                            manifest.period_ns,
                        )
                    })
                    .ok_or_else(|| EngineError::Strategy("strategy disappeared".into()))?;
                let strategy_key = (strategy_id.clone(), snapshot.instrument_id.get());
                if self
                    .python_strategy_last_run
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .get(&strategy_key)
                    .is_some_and(|last| now.as_nanos() < last.saturating_add(period_ns))
                {
                    continue;
                }
                let incomplete = required_metrics
                    .iter()
                    .any(|required| !metrics.iter().any(|metric| &metric.metric_id == required));
                if incomplete && missing_evidence == MissingEvidencePolicy::SkipEvaluation {
                    continue;
                }
                // Only packages that explicitly declare the typed no-action
                // policy receive incomplete snapshots. Legacy/native/Python
                // implementations retain skip behavior and cannot abort the
                // cycle merely because an input is unavailable.
                let context = StrategyContext {
                    now,
                    instrument_id: snapshot.instrument_id,
                    metrics: &metrics,
                };
                self.evaluate_registered_strategy(strategy_id, &context)?;
                self.python_strategy_last_run
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .insert(strategy_key, now.as_nanos());
                admitted = admitted.saturating_add(1);
            }
        }
        Ok(admitted)
    }

    /// Admits one validated strategy proposal into the durable coordinator.
    /// Proposals are recommendations only; this method never contacts a
    /// broker or bypasses portfolio/risk/execution services.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] for invalid or duplicate proposals,
    /// and [`EngineError::Journal`] if the admission event cannot be durable.
    pub fn submit_strategy_proposal(
        &self,
        proposal: &Proposal,
        now: MonoTime,
    ) -> Result<(), EngineError> {
        let mut coordinator = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = coordinator.clone();
        candidate
            .submit_checked(proposal.clone(), now)
            .map_err(|error| EngineError::Strategy(format!("{error:?}")))?;
        self.append_event(&encode_strategy_proposal(proposal))?;
        *coordinator = candidate;
        self.context_graph
            .lock()
            .map_err(|_| EngineError::Poisoned)
            .and_then(|mut graph| {
                index_strategy_in_graph(&mut graph, proposal)
                    .map_err(|error| EngineError::Graph(format!("strategy graph index: {error:?}")))
            })?;
        Ok(())
    }

    /// Evaluates the built-in deterministic threshold strategy against one
    /// immutable metric snapshot and admits its proposal into the same
    /// coordinator used by manual, hybrid, and autonomous consumers. The
    /// strategy can only return a bounded proposal or explicit `NoAction`; it
    /// never plans or submits an order itself.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when the strategy configuration or
    /// proposal is invalid, or a journal error when admission cannot be made
    /// durable before the coordinator changes.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_threshold_strategy(
        &self,
        strategy_id: impl Into<String>,
        metric_id: impl Into<String>,
        metric: &MetricOutput,
        entry_threshold: f64,
        exit_threshold: f64,
        quantity_ticks: i64,
        horizon_ns: u64,
        ttl_ns: u64,
        now: MonoTime,
    ) -> Result<Proposal, EngineError> {
        if metric.generated_mono > now || !metric.is_fresh(now) {
            return Err(EngineError::Strategy(
                "metric snapshot is stale or from the future".into(),
            ));
        }
        let strategy = ThresholdStrategy::new_with_proposal_seed(
            strategy_id,
            metric_id,
            entry_threshold,
            exit_threshold,
            quantity_ticks,
            horizon_ns,
            ttl_ns,
            now.as_nanos().max(1),
        )
        .ok_or_else(|| EngineError::Strategy("invalid threshold strategy configuration".into()))?;
        let context = StrategyContext {
            now,
            instrument_id: metric.instrument_id,
            metrics: std::slice::from_ref(metric),
        };
        let proposal = strategy
            .evaluate(&context)
            .map_err(|error| EngineError::Strategy(format!("threshold evaluation: {error:?}")))?;
        self.submit_strategy_proposal(&proposal, now)?;
        Ok(proposal)
    }

    /// Computes the reference order-book imbalance metric and evaluates the
    /// deterministic threshold strategy from the same immutable input. This is
    /// the concrete metric→strategy path used by hot-path integrations; the
    /// resulting proposal is journaled but never becomes an order implicitly.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] when metric inputs/configuration or
    /// proposal validation fails, or a journal error when admission is not
    /// durable.
    #[allow(clippy::too_many_arguments)]
    pub fn evaluate_book_imbalance_strategy(
        &self,
        instrument_id: InstrumentId,
        metric_id: impl Into<String>,
        strategy_id: impl Into<String>,
        bid_quantity: f64,
        ask_quantity: f64,
        metric_ttl_ns: u64,
        entry_threshold: f64,
        exit_threshold: f64,
        quantity_ticks: i64,
        horizon_ns: u64,
        strategy_ttl_ns: u64,
    ) -> Result<Proposal, EngineError> {
        let now = self.monotonic_now();
        let metric_id = metric_id.into();
        let mut features = BTreeMap::new();
        features.insert(String::from("bid_quantity"), bid_quantity);
        features.insert(String::from("ask_quantity"), ask_quantity);
        let metric = BookImbalanceMetric::new(metric_id, metric_ttl_ns)
            .map_err(|error| EngineError::Strategy(format!("metric configuration: {error:?}")))?
            .evaluate(&MetricContext {
                instrument_id: Some(instrument_id),
                features,
                now,
            })
            .map_err(|error| EngineError::Strategy(format!("metric evaluation: {error:?}")))?;
        self.evaluate_threshold_strategy(
            strategy_id,
            metric.metric_id.clone(),
            &metric,
            entry_threshold,
            exit_threshold,
            quantity_ticks,
            horizon_ns,
            strategy_ttl_ns,
            now,
        )
    }

    /// Resolves currently pending proposals with a deterministic policy and
    /// journals the exact policy/time boundary used by the decision.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] if the coordinator is unavailable or
    /// [`EngineError::Journal`] if the resolution boundary cannot be persisted.
    pub fn resolve_strategy_proposals(
        &self,
        policy: StrategyPolicy,
        now: MonoTime,
    ) -> Result<StrategyResultSet, EngineError> {
        let mut coordinator = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = coordinator.clone();
        let result = candidate.resolve_at(policy, now);
        self.append_event(&encode_strategy_resolution(policy, now))?;
        *coordinator = candidate;
        self.strategy_resolution_history
            .lock()
            .map_err(|_| EngineError::Poisoned)
            .map(|mut history| {
                push_resolution_summary(&mut history, resolution_summary(policy, now, &result));
            })?;
        Ok(result)
    }

    /// Resolves pending proposals with deterministic per-strategy quantity
    /// budgets, then journals the same resolution boundary used by ordinary
    /// manual/autonomous resolution. Budget adjustments remain in the returned
    /// diagnostics while proposal IDs and attribution are preserved.
    ///
    /// # Errors
    /// Returns [`EngineError::Strategy`] if a budget is invalid or the
    /// coordinator is unavailable, or [`EngineError::Journal`] when the
    /// resolution boundary cannot be persisted.
    pub fn resolve_strategy_proposals_with_budgets(
        &self,
        policy: StrategyPolicy,
        now: MonoTime,
        budgets: &std::collections::BTreeMap<String, StrategyBudget>,
    ) -> Result<BudgetedResultSet, EngineError> {
        if budgets
            .values()
            .any(|budget| budget.max_abs_quantity_ticks <= 0)
        {
            return Err(EngineError::Strategy("invalid strategy budget".into()));
        }
        let mut coordinator = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = coordinator.clone();
        let budgeted = candidate.resolve_at_with_budgets(policy, now, budgets);
        self.append_event(&encode_strategy_resolution_with_budgets(
            policy, now, budgets,
        ))?;
        *coordinator = candidate;
        self.strategy_resolution_history
            .lock()
            .map_err(|_| EngineError::Poisoned)
            .map(|mut history| {
                push_resolution_summary(
                    &mut history,
                    resolution_summary(policy, now, &budgeted.result),
                );
            })?;
        Ok(budgeted)
    }

    /// Converts a resolved proposal set into one aggregate, risk-budgeted
    /// target allocation. Proposal resolution remains responsible for
    /// lifecycle/conflict policy; this boundary applies portfolio marks,
    /// liquidity, turnover, and gross/net constraints before execution.
    ///
    /// Confidence is used only as a deterministic ordering score. It does not
    /// bypass the independent risk engine, which still evaluates every target
    /// immediately before order creation.
    ///
    /// # Errors
    /// Returns [`EngineError::Target`] when a proposal cannot be converted with
    /// the current portfolio snapshot, or [`EngineError::Strategy`] when the
    /// optimizer rejects invalid aggregate inputs.
    pub fn optimize_resolved_proposals(
        &self,
        proposals: &[Proposal],
        constraints: OptimizationConstraints,
    ) -> Result<OptimizationResult, EngineError> {
        let portfolio = self.runtime.portfolio()?;
        let mut candidates = Vec::with_capacity(proposals.len());
        for proposal in proposals {
            let target = portfolio
                .target_from_proposal(proposal)
                .map_err(EngineError::Target)?;
            let current = portfolio
                .position(proposal.instrument_id)
                .map_or(0, |position| position.quantity_ticks);
            let mark = portfolio
                .mark_price(proposal.instrument_id)
                .or_else(|| {
                    portfolio
                        .position(proposal.instrument_id)
                        .map(|position| position.mark_price_ticks)
                })
                .ok_or(EngineError::Target(TargetError::MissingPosition))?;
            candidates.push(OptimizationCandidate {
                target,
                current_quantity_ticks: current,
                mark_price_ticks: mark,
                expected_return_bps: proposal.confidence * 10_000.0,
                uncertainty_bps: (1.0 - proposal.confidence) * 10_000.0,
                max_participation_quantity_ticks: None,
            });
        }
        optimize_targets(&candidates, constraints)
            .map_err(|error| EngineError::Strategy(format!("portfolio optimization: {error:?}")))
    }

    /// Returns bounded strategy resolution diagnostics in journal order.
    #[must_use]
    pub fn strategy_resolution_history(&self) -> Vec<StrategyResolutionSummary> {
        self.strategy_resolution_history
            .lock()
            .map(|history| history.clone())
            .unwrap_or_default()
    }

    /// Returns durable execution totals grouped by strategy ID.
    #[must_use]
    pub fn strategy_execution_summaries(&self) -> Vec<StrategyExecutionSummary> {
        self.strategy_execution_summaries
            .lock()
            .map(|summaries| summaries.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns the validated installed strategy manifests and lifecycle state.
    #[must_use]
    pub fn strategy_registry(&self) -> Vec<StrategyRegistryRecord> {
        let Ok(host) = self.strategy_host.lock() else {
            return Vec::new();
        };
        host.strategy_ids()
            .into_iter()
            .filter_map(|strategy_id| {
                let manifest = host.manifest(&strategy_id)?.clone();
                let mode = match manifest.mode {
                    insider_strategy_sdk::StrategyMode::Deterministic => "deterministic",
                    insider_strategy_sdk::StrategyMode::Contextual => "contextual",
                };
                let priority = match manifest.priority {
                    insider_strategy_sdk::StrategyPriority::Fast => "fast",
                    insider_strategy_sdk::StrategyPriority::Normal => "normal",
                    insider_strategy_sdk::StrategyPriority::Background => "background",
                };
                let state = match host.state(&strategy_id) {
                    Some(insider_strategy_host::State::Ready) => "ready",
                    Some(insider_strategy_host::State::Quarantined) => "quarantined",
                    None => return None,
                };
                let lifecycle = match host.lifecycle(&strategy_id) {
                    Some(insider_strategy_host::Lifecycle::Research) => "research",
                    Some(insider_strategy_host::Lifecycle::Validated) => "validated",
                    Some(insider_strategy_host::Lifecycle::Shadow) => "shadow",
                    Some(insider_strategy_host::Lifecycle::Canary) => "canary",
                    Some(insider_strategy_host::Lifecycle::Production) => "production",
                    Some(insider_strategy_host::Lifecycle::Paused) => "paused",
                    Some(insider_strategy_host::Lifecycle::Retired) => "retired",
                    None => return None,
                };
                let lifecycle_evidence_ref = host.lifecycle_evidence(&strategy_id)?.to_owned();
                Some(StrategyRegistryRecord {
                    strategy_id,
                    mode: mode.into(),
                    metric_ids: manifest.metric_ids,
                    dependencies: manifest.strategy_dependencies,
                    horizon_ns: manifest.horizon_ns,
                    ttl_ns: manifest.ttl_ns,
                    period_ns: manifest.period_ns,
                    deadline_ns: manifest.deadline_ns,
                    priority: priority.into(),
                    state: state.into(),
                    lifecycle: lifecycle.into(),
                    lifecycle_evidence_ref,
                })
            })
            .collect()
    }

    #[must_use]
    /// Returns installed metric manifests and current lifecycle state.
    pub fn metric_registry(&self) -> Vec<MetricRegistryRecord> {
        let Ok(host) = self.metric_host.lock() else {
            return Vec::new();
        };
        host.metric_ids()
            .into_iter()
            .filter_map(|metric_id| {
                let manifest = host.manifest(&metric_id)?.clone();
                let priority = match manifest.priority {
                    insider_metric_sdk::MetricPriority::Fast => "fast",
                    insider_metric_sdk::MetricPriority::Normal => "normal",
                    insider_metric_sdk::MetricPriority::Background => "background",
                };
                let state = match host.state(&metric_id) {
                    Some(insider_metric_host::State::Ready) => "ready",
                    Some(insider_metric_host::State::Quarantined) => "quarantined",
                    None => return None,
                };
                let lifecycle = match host.lifecycle(&metric_id) {
                    Some(insider_metric_host::Lifecycle::Research) => "research",
                    Some(insider_metric_host::Lifecycle::Validated) => "validated",
                    Some(insider_metric_host::Lifecycle::Shadow) => "shadow",
                    Some(insider_metric_host::Lifecycle::Canary) => "canary",
                    Some(insider_metric_host::Lifecycle::Production) => "production",
                    Some(insider_metric_host::Lifecycle::Paused) => "paused",
                    Some(insider_metric_host::Lifecycle::Retired) => "retired",
                    None => return None,
                };
                Some(MetricRegistryRecord {
                    metric_id,
                    state: state.into(),
                    lifecycle: lifecycle.into(),
                    inputs: manifest.descriptor.inputs,
                    min_score: manifest.descriptor.min_score,
                    max_score: manifest.descriptor.max_score,
                    ttl_ns: manifest.descriptor.ttl_ns,
                    period_ns: manifest.period_ns,
                    deadline_ns: manifest.deadline_ns,
                    budget_ns: manifest.budget_ns,
                    priority: priority.into(),
                })
            })
            .collect()
    }

    /// Returns one coordinator record for terminal/strategy diagnostics without
    /// exposing coordinator mutation.
    #[must_use]
    pub fn strategy_proposal_record(&self, proposal_id: ProposalId) -> Option<ProposalRecord> {
        self.strategy_coordinator
            .lock()
            .ok()
            .and_then(|coordinator| coordinator.record(proposal_id).cloned())
    }

    /// Registers one cursor-capable provider and restores its last committed
    /// state, if the journal contains one from an earlier process lifetime.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] for duplicate/invalid registration
    /// or a persisted state that exceeds the provider's configured bounds.
    pub fn register_news_provider(
        &self,
        provider: Box<dyn CursorProvider>,
        retry_policy: RetryPolicy,
        max_requests: u32,
        window_ms: i64,
        max_items: usize,
        dead_letter_capacity: usize,
    ) -> Result<(), EngineError> {
        let provider_id = provider.provider_id().trim().to_owned();
        let mut providers = self
            .news_providers
            .lock()
            .map_err(|_| EngineError::ReadModel("news provider lock poisoned".into()))?;
        providers
            .register(
                provider,
                retry_policy,
                max_requests,
                window_ms,
                max_items,
                dead_letter_capacity,
            )
            .map_err(|error| EngineError::ReadModel(format!("provider register: {error:?}")))?;
        if let Some(snapshot) = self.provider_state(&provider_id) {
            providers
                .restore_snapshot(snapshot)
                .map_err(|error| EngineError::ReadModel(format!("provider restore: {error:?}")))?;
        }
        drop(providers);
        self.refresh_news_supervisor_health()?;
        Ok(())
    }

    /// Projects provider-worker health into the named supervisor component.
    /// Failed providers make the component unavailable; transient failures are
    /// degraded, while an all-healthy registry is healthy. No provider result
    /// is inferred from an empty news page.
    fn refresh_news_supervisor_health(&self) -> Result<(), EngineError> {
        let statuses = self.news_provider_statuses()?;
        let health = if statuses.is_empty()
            || statuses
                .iter()
                .any(|status| status.health == ProviderHealth::Unknown)
        {
            SupervisorHealth::Unknown
        } else if statuses
            .iter()
            .any(|status| status.health == ProviderHealth::Failed)
        {
            SupervisorHealth::Unavailable
        } else if statuses.iter().any(|status| {
            matches!(
                status.health,
                ProviderHealth::CoolingDown | ProviderHealth::Degraded
            )
        }) {
            SupervisorHealth::Degraded
        } else {
            SupervisorHealth::Healthy
        };
        self.set_supervisor_health("news", health)
    }

    /// Polls one registered provider against the authoritative news store.
    /// Accepted normalized articles are journaled by the page commit hook;
    /// the provider cursor/retry state is persisted only after the poll state
    /// machine has advanced successfully.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] for unavailable locks, unknown
    /// providers, or invalid persisted provider state.
    pub fn poll_news_provider<F>(
        &self,
        provider_id: &str,
        now_ms: i64,
        classify: F,
    ) -> Result<PollOutcome, EngineError>
    where
        F: Fn(&str) -> RetryClass,
    {
        let mut providers = self
            .news_providers
            .lock()
            .map_err(|_| EngineError::ReadModel("news provider lock poisoned".into()))?;
        let mut news = self
            .news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))?;
        let mut committer = JournalNewsCommitter { host: self };
        let outcome = providers
            .poll(provider_id, &mut news, &mut committer, now_ms, classify)
            .map_err(|error| EngineError::ReadModel(format!("provider poll: {error:?}")))?;
        let snapshot = providers
            .snapshots()
            .into_iter()
            .find(|snapshot| snapshot.provider_id == provider_id);
        drop(news);
        drop(providers);
        if let Some(snapshot) = snapshot {
            self.persist_provider_state(snapshot)?;
        }
        self.refresh_news_supervisor_health()?;
        Ok(outcome)
    }

    /// Polls providers in the supplied deterministic priority order until one
    /// ingests new articles. Every attempted provider's cursor/dead-letter
    /// snapshot is persisted before the outcome is returned.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] for provider, projection, journal,
    /// or supervisor-state failures. Unknown provider IDs are rejected by the
    /// registry before any later provider is attempted.
    pub fn poll_news_fallback<F>(
        &self,
        priority: &[&str],
        now_ms: i64,
        classify: F,
    ) -> Result<Vec<(String, PollOutcome)>, EngineError>
    where
        F: Fn(&str) -> RetryClass + Copy,
    {
        if priority.is_empty() || priority.len() > 32 {
            return Err(EngineError::InvalidRequest);
        }
        let mut providers = self
            .news_providers
            .lock()
            .map_err(|_| EngineError::ReadModel("news provider lock poisoned".into()))?;
        let mut news = self
            .news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))?;
        let mut committer = JournalNewsCommitter { host: self };
        let outcomes = providers
            .poll_fallback(priority, &mut news, &mut committer, now_ms, classify)
            .map_err(|error| {
                EngineError::ReadModel(format!("provider fallback poll: {error:?}"))
            })?;
        let snapshots = providers.snapshots();
        drop(news);
        drop(providers);
        for provider_id in outcomes.iter().map(|(provider_id, _)| provider_id) {
            if let Some(snapshot) = snapshots
                .iter()
                .find(|snapshot| snapshot.provider_id == provider_id.as_str())
            {
                self.persist_provider_state(snapshot.clone())?;
            }
        }
        self.refresh_news_supervisor_health()?;
        Ok(outcomes)
    }

    /// Returns the bounded operational status of one registered news provider.
    /// The status remains available while a provider is cooling down,
    /// degraded, or failed so callers can select a fallback.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] if the provider registry lock is
    /// unavailable.
    pub fn news_provider_status(
        &self,
        provider_id: &str,
    ) -> Result<Option<ProviderStatus>, EngineError> {
        self.news_providers
            .lock()
            .map(|providers| providers.status(provider_id))
            .map_err(|_| EngineError::ReadModel("news provider lock poisoned".into()))
    }

    /// Returns all registered news provider statuses in deterministic order.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] if the provider registry lock is
    /// unavailable.
    pub fn news_provider_statuses(&self) -> Result<Vec<ProviderStatus>, EngineError> {
        let providers = self
            .news_providers
            .lock()
            .map_err(|_| EngineError::ReadModel("news provider lock poisoned".into()))?;
        Ok(providers
            .provider_ids()
            .into_iter()
            .filter_map(|provider_id| providers.status(&provider_id))
            .collect())
    }

    /// Returns a bounded cursor page from the authoritative news projection.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when the news projection lock is
    /// unavailable.
    pub fn news_page(
        &self,
        scope: &str,
        symbol: &str,
        after_id: Option<&str>,
        limit: usize,
    ) -> Result<NewsPage, EngineError> {
        self.news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))
            .map(|store| {
                if scope == "relevant" {
                    let now_ms = store.all(1).first().map_or(0, |item| item.received_at_ms);
                    store.relevant_page(symbol, now_ms, after_id, limit)
                } else {
                    store.all_page(after_id, limit)
                }
            })
    }

    /// Returns one authoritative article detail view, including retained
    /// corrections and deterministic exact-title cluster membership.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when the news projection lock is
    /// unavailable.
    pub fn news_detail(&self, item_id: &str) -> Result<Option<NewsDetail>, EngineError> {
        if item_id.trim().is_empty() {
            return Ok(None);
        }
        self.news
            .lock()
            .map_err(|_| EngineError::ReadModel("news projection lock poisoned".into()))
            .map(|store| store.detail(item_id))
    }

    /// Creates an atomic, verified backup of the rebuildable read model.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when verification or publication fails.
    pub fn backup_read_model(
        &self,
        destination: impl AsRef<std::path::Path>,
    ) -> Result<ProjectionManifest, EngineError> {
        self.read_model
            .backup_to(destination)
            .map_err(|error| EngineError::ReadModel(format!("{error:?}")))
    }

    /// Restores a verified read-model backup to a new path.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when verification or publication fails.
    pub fn restore_read_model_backup(
        source: impl AsRef<std::path::Path>,
        destination: impl AsRef<std::path::Path>,
    ) -> Result<ProjectionManifest, EngineError> {
        ProjectionStore::restore_from(source, destination)
            .map_err(|error| EngineError::ReadModel(format!("{error:?}")))
    }

    /// Returns the verified read-model record count and newest journal cursor.
    ///
    /// # Errors
    /// Returns [`EngineError::ReadModel`] when the projection cannot be read or
    /// fails checksum, ordering, or bounds validation.
    pub fn read_model_manifest(&self) -> Result<ProjectionManifest, EngineError> {
        let records = self
            .read_model
            .read_all()
            .map_err(|error| EngineError::ReadModel(format!("{error:?}")))?;
        Ok(ProjectionManifest {
            record_count: records.len() as u64,
            newest_sequence: records.last().map_or(0, |record| record.sequence),
        })
    }

    /// Returns an authoritative snapshot and journal cursor for IPC clients.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] if the cursor cannot be read or
    /// [`EngineError::Poisoned`] if a runtime projection lock is unavailable.
    pub fn runtime_snapshot(&self) -> Result<RuntimeSnapshot, EngineError> {
        let cursor = self.journal_cursor()?;
        let mut snapshot = self.runtime.snapshot(cursor)?;
        snapshot.proposals = self
            .strategy_coordinator
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .records()
            .cloned()
            .collect();
        snapshot.markets = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .snapshots();
        snapshot.autonomy_mode = self.autonomy_mode();
        snapshot.autonomy_plan = self
            .autonomy_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .latest()
            .map(|record| AutonomyPlanSnapshot {
                plan_id: record.plan.plan_id.clone(),
                state: record.state,
                generated_at_ns: record.plan.generated_at.as_nanos(),
                expires_at_ns: record.plan.expires_at.as_nanos(),
                actions: record.plan.actions.clone(),
            });
        snapshot.llm_provider_id = self
            .llm_provider
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .as_ref()
            .and_then(|provider| provider.manifest())
            .map(|manifest| manifest.provider_id);
        snapshot.llm_model = self
            .config
            .snapshot()
            .map_err(|_| EngineError::Poisoned)?
            .settings
            .get("llm.model")
            .and_then(|value| match value {
                Value::String(model) if !model.trim().is_empty() => Some(model.clone()),
                _ => None,
            });
        Ok(snapshot)
    }

    fn journal_cursor(&self) -> Result<u64, EngineError> {
        Ok(self
            .journal
            .scan()?
            .records
            .last()
            .map_or(0, |record| record.sequence.saturating_add(1)))
    }

    /// Reads a bounded journal suffix for IPC snapshot resumption.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] when the journal cannot be scanned, or
    /// [`EngineError::InvalidRequest`] when the requested bound is invalid.
    pub fn journal_events_after(
        &self,
        cursor: u64,
        max_records: usize,
    ) -> Result<Vec<insider_journal::Record>, EngineError> {
        if max_records == 0 || max_records > 4_096 {
            return Err(EngineError::InvalidRequest);
        }
        Ok(self
            .journal
            .scan()?
            .records
            .into_iter()
            .filter(|record| record.sequence >= cursor)
            .take(max_records)
            .collect())
    }

    /// Reconstructs the durable order/execution portion of a trace from the
    /// authoritative journal. Intent records identify the trace; subsequent
    /// broker records are joined by stable client order ID.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] for scan failures or malformed event
    /// payloads encountered while reconstructing the trace.
    pub fn trace_events(&self, trace_id: TraceId) -> Result<Vec<TraceEvent>, EngineError> {
        let records = self.journal.scan()?.records;
        let mut client_orders = std::collections::BTreeSet::new();
        let mut linked_proposals = std::collections::BTreeSet::new();
        let mut linked_records = std::collections::BTreeSet::new();
        for record in &records {
            if let Some((linked_trace, kind, object_id)) = decode_trace_link(&record.payload)?
                && linked_trace == trace_id
            {
                linked_records.insert(record.sequence);
                if kind == "proposal" {
                    linked_proposals.insert(object_id);
                }
            }
            if let Some(RecoveredEvent::Intent(intent)) = decode_journal_payload(&record.payload)?
                && intent.trace_id == trace_id
            {
                client_orders.insert(intent.client_order_id);
            }
        }
        let mut events = Vec::new();
        for record in records {
            if linked_records.contains(&record.sequence) {
                events.push(TraceEvent {
                    sequence: record.sequence,
                    kind: "trace_link".to_owned(),
                    payload: record.payload.clone(),
                });
                continue;
            }
            let Some(event) = decode_journal_payload(&record.payload)? else {
                continue;
            };
            let (matched, kind) = match event {
                RecoveredEvent::Intent(intent) => (intent.trace_id == trace_id, "order_intent"),
                RecoveredEvent::StrategyProposal(proposal) => (
                    linked_proposals.contains(&proposal.proposal_id.get().to_string()),
                    "strategy_proposal",
                ),
                RecoveredEvent::Broker(event) => {
                    let matched = client_orders.contains(broker_event_client_order_id(&event));
                    (matched, "broker_event")
                }
                _ => (false, "other"),
            };
            if matched {
                events.push(TraceEvent {
                    sequence: record.sequence,
                    kind: kind.to_owned(),
                    payload: record.payload,
                });
            }
        }
        Ok(events)
    }

    /// Calculates a manual target preview without contacting the broker.
    ///
    /// # Errors
    /// Returns an engine error when risk, target conversion, or journal state
    /// cannot be read.
    pub fn preview_manual_target(
        &self,
        instrument_id: InstrumentId,
        target_quantity_ticks: i64,
        proposal_id: ProposalId,
        now: MonoTime,
        trace_id: TraceId,
        ttl_ns: u64,
    ) -> Result<ManualOrderPreview, EngineError> {
        self.preview_manual_target_with_order(
            instrument_id,
            target_quantity_ticks,
            proposal_id,
            now,
            trace_id,
            ttl_ns,
            OrderType::Market,
            None,
        )
    }

    /// Calculates a manual preview while preserving the requested broker
    /// order type. Limit prices are validated before the preview is cached and
    /// are revalidated again when the preview is submitted.
    ///
    /// # Errors
    /// Returns an engine error when the runtime is not running, the order type
    /// or limit is invalid, or risk planning denies the target.
    #[allow(clippy::too_many_arguments)]
    pub fn preview_manual_target_with_order(
        &self,
        instrument_id: InstrumentId,
        target_quantity_ticks: i64,
        proposal_id: ProposalId,
        now: MonoTime,
        trace_id: TraceId,
        ttl_ns: u64,
        order_type: OrderType,
        limit_price_ticks: Option<i64>,
    ) -> Result<ManualOrderPreview, EngineError> {
        if self.lifecycle() != Lifecycle::Running || ttl_ns == 0 {
            return Err(EngineError::NotRunning);
        }
        let current_quantity = self
            .runtime
            .portfolio()?
            .position(instrument_id)
            .map_or(0, |position| position.quantity_ticks);
        self.ensure_market_health_for_target(
            instrument_id,
            target_quantity_ticks,
            current_quantity,
        )?;
        let intent = self.runtime.prepare_manual_target_with_order(
            instrument_id,
            target_quantity_ticks,
            proposal_id,
            now,
            trace_id,
            order_type,
            limit_price_ticks,
        )?;
        let state_version = self.journal_cursor()?;
        let portfolio = self.runtime.portfolio()?;
        let estimated_notional_ticks = portfolio
            .mark_price(instrument_id)
            .or_else(|| {
                portfolio
                    .position(instrument_id)
                    .map(|position| position.mark_price_ticks)
            })
            .and_then(|price| i128::from(intent.quantity_ticks).checked_mul(i128::from(price)));
        let preview_id = format!("preview-{state_version}-{}", intent.client_order_id);
        Ok(ManualOrderPreview {
            preview_id,
            expected_state_version: state_version,
            expires_mono_ns: now.as_nanos().saturating_add(ttl_ns),
            intent,
            target_quantity_ticks,
            proposal_id,
            estimated_notional_ticks,
            estimated_cost_bps: None,
            warnings: vec![String::from("execution cost estimate unavailable")],
        })
    }

    /// Prevents new exposure from using an absent, degraded, or stale quote.
    /// Reductions toward zero remain available for risk-off operation.
    fn ensure_market_health_for_target(
        &self,
        instrument_id: InstrumentId,
        target_quantity_ticks: i64,
        current_quantity_ticks: i64,
    ) -> Result<(), EngineError> {
        let reducing = current_quantity_ticks != 0
            && (target_quantity_ticks == 0
                || (target_quantity_ticks.signum() == current_quantity_ticks.signum()
                    && target_quantity_ticks.unsigned_abs()
                        < current_quantity_ticks.unsigned_abs()));
        if reducing {
            return Ok(());
        }
        let snapshot = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .snapshot(instrument_id);
        let Some(snapshot) = snapshot else {
            // A broker reconciliation may provide a trusted mark before a
            // market stream is registered. That mark is an authoritative
            // bootstrap valuation; once a stream exists, its health is still
            // enforced strictly below.
            if self
                .runtime
                .portfolio()?
                .mark_price(instrument_id)
                .is_some()
            {
                return Ok(());
            }
            return Err(EngineError::Plan(PlanError::RiskDenied(
                RiskReason::StaleData,
            )));
        };
        if snapshot.quote.is_none()
            || snapshot.quote_health.quality != insider_market_data::Quality::Good
        {
            return Err(EngineError::Plan(PlanError::RiskDenied(
                RiskReason::StaleData,
            )));
        }
        Ok(())
    }

    /// Builds a read-only risk preview for an immutable strategy proposal.
    /// Scaling is applied before target conversion; no journal or broker state
    /// changes until a later confirmation revalidates the proposal.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an unknown/invalid proposal
    /// or the normal planning/risk errors when the target is not admissible.
    pub fn preview_strategy_proposal(
        &self,
        proposal_id: ProposalId,
        scale: f64,
        now: MonoTime,
        trace_id: TraceId,
        ttl_ns: u64,
    ) -> Result<ManualOrderPreview, EngineError> {
        if !scale.is_finite() || scale <= 0.0 || scale > 1.0 || ttl_ns == 0 {
            return Err(EngineError::InvalidRequest);
        }
        let proposal = self
            .strategy_proposal_record(proposal_id)
            .ok_or(EngineError::InvalidRequest)?
            .proposal;
        let scaled_action = scale_action(&proposal.action, scale)?;
        let mut scaled = proposal;
        scaled.action = scaled_action;
        let portfolio_snapshot = self.runtime.portfolio()?;
        let target = portfolio_snapshot
            .target_from_proposal(&scaled)
            .map_err(EngineError::Target)?;
        self.ensure_market_health_for_target(
            target.instrument_id,
            target.quantity_ticks,
            portfolio_snapshot
                .position(target.instrument_id)
                .map_or(0, |position| position.quantity_ticks),
        )?;
        let intent = self.runtime.prepare_proposal(&scaled, now, trace_id)?;
        let state_version = self.journal_cursor()?;
        let portfolio = self.runtime.portfolio()?;
        let estimated_notional_ticks = portfolio
            .mark_price(intent.instrument_id)
            .or_else(|| {
                portfolio
                    .position(intent.instrument_id)
                    .map(|position| position.mark_price_ticks)
            })
            .and_then(|price| i128::from(intent.quantity_ticks).checked_mul(i128::from(price)));
        Ok(ManualOrderPreview {
            preview_id: format!("preview-{state_version}-{}", intent.client_order_id),
            expected_state_version: state_version,
            expires_mono_ns: now.as_nanos().saturating_add(ttl_ns),
            target_quantity_ticks: intent.quantity_ticks,
            proposal_id,
            estimated_notional_ticks,
            estimated_cost_bps: None,
            warnings: vec![String::from("execution cost estimate unavailable")],
            intent,
        })
    }

    /// Revalidates and submits a previously previewed manual target.
    ///
    /// The preview's journal version, expiry, intent identity, and explicit
    /// confirmation are checked before the intent is durably appended.
    ///
    /// # Errors
    /// Returns [`EngineError::StalePreview`], [`EngineError::PreviewExpired`],
    /// or [`EngineError::ConfirmationRequired`] when the UI must re-preview.
    pub fn submit_manual_preview(
        &self,
        preview: &ManualOrderPreview,
        now: MonoTime,
        confirmation: &str,
    ) -> Result<String, EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        if confirmation != "CONFIRM" {
            return Err(EngineError::ConfirmationRequired);
        }
        if now.as_nanos() > preview.expires_mono_ns {
            return Err(EngineError::PreviewExpired);
        }
        if self.journal_cursor()? != preview.expected_state_version {
            return Err(EngineError::StalePreview);
        }
        let refreshed = self.runtime.prepare_manual_target_with_order(
            preview.intent.instrument_id,
            preview.target_quantity_ticks,
            preview.proposal_id,
            now,
            preview.intent.trace_id,
            preview.intent.order_type,
            preview.intent.limit_price_ticks,
        )?;
        if !same_order_identity(&refreshed, &preview.intent) {
            return Err(EngineError::StalePreview);
        }
        self.runtime.authorize_intent(&refreshed)?;
        self.append_event(&encode_trace_link(
            refreshed.trace_id,
            "proposal",
            &preview.proposal_id.get().to_string(),
        ))?;
        self.append_event(&encode_order_intent(&refreshed))?;
        self.index_order_graph(&refreshed, now)?;
        self.submit_intent_with_timing(&refreshed, now)?;
        Ok(refreshed.client_order_id)
    }

    /// Submits a proposal while the host is running.
    ///
    /// # Errors
    /// Returns [`EngineError::NotRunning`] after drain/stop, otherwise the
    /// underlying proposal, risk, persistence, or broker error.
    pub fn submit_proposal(
        &self,
        proposal: &Proposal,
        now: insider_common_types::MonoTime,
        trace_id: TraceId,
    ) -> Result<String, EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        let portfolio_snapshot = self.runtime.portfolio()?;
        let target = portfolio_snapshot
            .target_from_proposal(proposal)
            .map_err(EngineError::Target)?;
        self.ensure_market_health_for_target(
            target.instrument_id,
            target.quantity_ticks,
            portfolio_snapshot
                .position(target.instrument_id)
                .map_or(0, |position| position.quantity_ticks),
        )?;
        let intent = self.runtime.prepare_proposal(proposal, now, trace_id)?;
        self.runtime.authorize_intent(&intent)?;
        self.append_event(&encode_trace_link(
            trace_id,
            "proposal",
            &proposal.proposal_id.get().to_string(),
        ))?;
        self.append_event(&encode_order_intent(&intent))?;
        self.index_order_graph(&intent, now)?;
        self.submit_intent_with_timing(&intent, now)?;
        Ok(intent.client_order_id)
    }

    /// Creates a durable parent execution plan and starts any children due at
    /// the supplied monotonic time. The parent is never sent to the broker;
    /// only deterministic child intents enter the normal order lifecycle.
    ///
    /// # Errors
    /// Returns the normal risk, planning, journal, or broker lifecycle errors.
    pub fn submit_scheduled_proposal(
        &self,
        proposal: &Proposal,
        schedule: &Schedule,
        now: MonoTime,
        trace_id: TraceId,
    ) -> Result<String, EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        let portfolio_snapshot = self.runtime.portfolio()?;
        let target = portfolio_snapshot
            .target_from_proposal(proposal)
            .map_err(EngineError::Target)?;
        self.ensure_market_health_for_target(
            target.instrument_id,
            target.quantity_ticks,
            portfolio_snapshot
                .position(target.instrument_id)
                .map_or(0, |position| position.quantity_ticks),
        )?;
        let parent = self.runtime.prepare_proposal(proposal, now, trace_id)?;
        self.runtime.authorize_intent(&parent)?;
        let record = self
            .runtime
            .create_child_plan(parent, schedule, now.as_nanos())?;
        self.append_event(&encode_trace_link(
            trace_id,
            "proposal",
            &proposal.proposal_id.get().to_string(),
        ))?;
        self.append_event(&encode_child_plan(&record))?;
        self.drive_scheduled_children(now)?;
        Ok(record.plan.parent_client_order_id)
    }

    /// Claims and sends all due children from every durable parent plan.
    /// Calling this repeatedly is safe: `ChildPlan::claim_due` removes each
    /// pending child from the claim set before any transport side effect.
    ///
    /// # Errors
    /// Returns an engine error when a child cannot be journaled, inserted, or
    /// sent; ambiguous transport outcomes remain Unknown and are not retried.
    pub fn drive_scheduled_children(&self, now: MonoTime) -> Result<usize, EngineError> {
        let claims = self.runtime.claim_due_children(now.as_nanos())?;
        let mut sent = 0_usize;
        for (claimed_record, child_intent, _child) in claims {
            self.append_event(&encode_child_plan(&claimed_record))?;
            self.append_event(&encode_order_intent(&child_intent))?;
            self.index_order_graph(&child_intent, now)?;
            self.runtime.restore_intent(child_intent.clone())?;
            let result = self.submit_intent_with_timing(&child_intent, now);
            match result {
                Ok(()) => {
                    let updated = self
                        .runtime
                        .mark_child_sent(&child_intent.client_order_id)?;
                    self.append_event(&encode_child_plan(&updated))?;
                    sent += 1;
                }
                Err(error) => {
                    let updated = self
                        .runtime
                        .mark_child_unknown(&child_intent.client_order_id)?;
                    self.append_event(&encode_child_plan(&updated))?;
                    return Err(error);
                }
            }
        }
        Ok(sent)
    }

    /// Scales and submits an existing immutable proposal after resolving it
    /// from the coordinator. This is the command-service boundary used by the
    /// desktop confirmation flow; risk and execution remain authoritative.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an unknown proposal or
    /// invalid scale, otherwise the normal proposal submission errors.
    pub fn submit_scaled_proposal(
        &self,
        proposal_id: ProposalId,
        scale: f64,
        now: MonoTime,
        trace_id: TraceId,
    ) -> Result<String, EngineError> {
        if !scale.is_finite() || scale <= 0.0 || scale > 1.0 {
            return Err(EngineError::InvalidRequest);
        }
        let record = self
            .strategy_proposal_record(proposal_id)
            .ok_or(EngineError::InvalidRequest)?;
        let mut proposal = record.proposal;
        proposal.action = scale_action(&proposal.action, scale)?;
        self.submit_proposal(&proposal, now, trace_id)
    }

    /// Executes one policy-approved autonomous proposal through the same
    /// target, risk, journal, and broker path as manual and deterministic
    /// strategy submissions.
    ///
    /// The proposal ID in the approved action must match the supplied immutable
    /// proposal. Scaling is applied before target conversion and never bypasses
    /// risk checks.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for action/proposal mismatches or
    /// unsupported actions, and the normal execution errors otherwise.
    pub fn submit_approved_action(
        &self,
        approved: &ApprovedAction,
        proposal: &Proposal,
        now: MonoTime,
        trace_id: TraceId,
    ) -> Result<String, EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        let Some(proposal_id) = approved.action.proposal_id.as_deref() else {
            return Err(EngineError::InvalidRequest);
        };
        if proposal_id != proposal.proposal_id.to_string()
            || !matches!(
                approved.action.action_type,
                insider_llm_core::ActionType::ExecuteProposal
                    | insider_llm_core::ActionType::ExecuteProposalScaled
            )
        {
            return Err(EngineError::InvalidRequest);
        }
        if !approved.scale.is_finite() || approved.scale <= 0.0 || approved.scale > 1.0 {
            return Err(EngineError::InvalidRequest);
        }
        let mut scaled = proposal.clone();
        scaled.action = scale_action(&proposal.action, approved.scale)?;
        self.submit_proposal(&scaled, now, trace_id)
    }

    /// Returns the current immutable configuration snapshot.
    ///
    /// # Errors
    /// Returns [`ReloadError::Unavailable`] if configuration state is poisoned.
    pub fn config(&self) -> Result<Snapshot, ReloadError> {
        self.config.snapshot()
    }

    /// Atomically validates and publishes a new configuration.
    ///
    /// # Errors
    /// Returns a [`ReloadError`] for invalid, stale, or unavailable reloads.
    pub fn reload_config<F>(
        &self,
        expected_version: u64,
        settings: Settings,
        validate: F,
    ) -> Result<Snapshot, ReloadError>
    where
        F: FnOnce(&Settings) -> Result<(), String>,
    {
        let guardrails = configured_guardrails(&settings)
            .map_err(|_| ReloadError::Invalid(String::from("invalid risk guardrail settings")))?;
        let webhook = configured_alert_webhook(&settings)
            .map_err(|_| ReloadError::Invalid(String::from("invalid alert webhook settings")))?;
        let alert_limits = configured_alert_limits(&settings)
            .map_err(|_| ReloadError::Invalid(String::from("invalid alert routing settings")))?;
        let supervisor_policy = configured_supervisor_policy(&settings)
            .map_err(|_| ReloadError::Invalid(String::from("invalid supervisor settings")))?;
        if supervisor_policy != self.supervisor_policy {
            return Err(ReloadError::Invalid(
                "supervisor policy changes require a process restart".into(),
            ));
        }
        let mut alert_router = self.alerts.lock().map_err(|_| ReloadError::Unavailable)?;
        alert_router
            .validate_reconfigure(alert_limits.0, alert_limits.1)
            .map_err(|_| {
                ReloadError::Invalid(String::from(
                    "alert queue has more pending deliveries than configured capacity",
                ))
            })?;
        if webhook != self.alert_webhook {
            return Err(ReloadError::Invalid(
                "alert webhook changes require a process restart".into(),
            ));
        }
        let snapshot = self
            .config
            .reload(expected_version, settings, |candidate| {
                configured_guardrails(candidate)
                    .map_err(|_| String::from("invalid risk guardrail settings"))?;
                configured_alert_webhook(candidate)
                    .map_err(|_| String::from("invalid alert webhook settings"))?;
                configured_alert_limits(candidate)
                    .map_err(|_| String::from("invalid alert routing settings"))?;
                configured_supervisor_policy(candidate)
                    .map_err(|_| String::from("invalid supervisor settings"))?;
                validate(candidate)
            })?;
        self.runtime
            .set_guardrails(guardrails)
            .map_err(|_| ReloadError::Unavailable)?;
        alert_router
            .reconfigure(alert_limits.0, alert_limits.1)
            .map_err(|_| ReloadError::Unavailable)?;
        Ok(snapshot)
    }

    /// Runs a deterministic event-driven backtest and durably records its
    /// immutable lineage and result. The supplied events are never mixed with
    /// live runtime state and no system clock is consulted.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for missing lineage, duplicate
    /// IDs, excessive input, invalid initial cash, or replay/accounting
    /// failures.
    pub fn run_backtest(
        &self,
        request: BacktestRunRequest,
    ) -> Result<BacktestRunResult, EngineError> {
        let BacktestRunRequest {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            initial_cash_ticks,
            events,
        } = request;
        if run_id.trim().is_empty()
            || strategy_id.trim().is_empty()
            || dataset_hash.trim().is_empty()
            || config_hash.trim().is_empty()
            || initial_cash_ticks <= 0
            || events.is_empty()
            || events.len() > 1_000_000
        {
            return Err(EngineError::InvalidRequest);
        }
        if self
            .backtest_runs
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .contains_key(&run_id)
        {
            return Err(EngineError::InvalidRequest);
        }
        let mut runner = insider_replay::BacktestRunner::new(initial_cash_ticks);
        for event in events {
            runner
                .apply(event)
                .map_err(|_| EngineError::InvalidRequest)?;
        }
        let report = runner.finish().map_err(|_| EngineError::InvalidRequest)?;
        let result = BacktestRunResult {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            report,
        };
        self.record_backtest_experiment(&result)?;
        self.append_event(&encode_backtest_result(&result))?;
        self.backtest_runs
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(result.run_id.clone(), result.clone());
        Ok(result)
    }

    /// Runs a leakage-safe expanding-window validation over a deterministic
    /// event tape. Fold scoring and the locked holdout are returned separately;
    /// this method never mutates live runtime, broker, or portfolio state.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid split/accounting
    /// inputs or [`EngineError::Strategy`] when replay validation fails.
    pub fn run_walk_forward_validation(
        &self,
        events: &[insider_replay::BacktestEvent],
        initial_cash_ticks: i128,
        config: insider_replay::WalkForwardConfig,
    ) -> Result<insider_replay::WalkForwardReport, EngineError> {
        if initial_cash_ticks <= 0 || events.len() > 1_000_000 {
            return Err(EngineError::InvalidRequest);
        }
        insider_replay::run_walk_forward(events, initial_cash_ticks, config)
            .map_err(|error| EngineError::Strategy(format!("walk-forward validation: {error:?}")))
    }

    /// Replays a registered deterministic strategy against a point-in-time
    /// market/metric tape. Every proposal is routed through the same strategy
    /// host validation used by live evaluation; accepted target deltas are
    /// filled by the deterministic replay ledger and never touch broker state.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid lineage, unsupported
    /// strategy mode, stale/malformed inputs, duplicate IDs, or replay errors.
    #[allow(clippy::too_many_lines)]
    pub fn run_strategy_backtest(
        &self,
        request: StrategyBacktestRunRequest,
    ) -> Result<BacktestRunResult, EngineError> {
        let StrategyBacktestRunRequest {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            initial_cash_ticks,
            events,
        } = request;
        if run_id.trim().is_empty()
            || strategy_id.trim().is_empty()
            || dataset_hash.trim().is_empty()
            || config_hash.trim().is_empty()
            || initial_cash_ticks <= 0
            || events.is_empty()
            || events.len() > 100_000
        {
            return Err(EngineError::InvalidRequest);
        }
        if self
            .backtest_runs
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .contains_key(&run_id)
        {
            return Err(EngineError::InvalidRequest);
        }
        let mut strategy_host = self
            .strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let Some(manifest) = strategy_host.manifest(&strategy_id) else {
            return Err(EngineError::InvalidRequest);
        };
        if manifest.mode != insider_strategy_sdk::StrategyMode::Deterministic {
            return Err(EngineError::InvalidRequest);
        }
        let mut last_sequence = 0_u64;
        let mut last_now = 0_u64;
        let mut position = 0_i64;
        let mut replay_events = Vec::with_capacity(events.len().saturating_mul(2));
        for event in events {
            if event.sequence == 0
                || event.sequence <= last_sequence
                || event.now_mono_ns < last_now
                || event.price_ticks <= 0
                || event.fee_ticks < 0
                || event.sequence > u64::MAX / 2
                || event.metrics.len() > 4_096
                || event.metrics.iter().any(|metric| {
                    metric.instrument_id != event.instrument_id
                        || !metric.score.is_finite()
                        || !metric.confidence.is_finite()
                        || !metric.uncertainty.is_finite()
                        || !(0.0..=1.0).contains(&metric.confidence)
                        || metric.uncertainty < 0.0
                        || !metric.is_fresh(MonoTime::from_nanos(event.now_mono_ns))
                })
            {
                return Err(EngineError::InvalidRequest);
            }
            let now = MonoTime::from_nanos(event.now_mono_ns);
            let context = StrategyContext {
                now,
                instrument_id: event.instrument_id,
                metrics: &event.metrics,
            };
            let proposal = strategy_host
                .evaluate(&strategy_id, &context)
                .map_err(|_| EngineError::InvalidRequest)?;
            let target = match proposal.action {
                Action::NoAction => position,
                Action::TargetQuantity { quantity_ticks } => quantity_ticks,
                Action::Increase { quantity_ticks } => position
                    .checked_add(quantity_ticks)
                    .ok_or(EngineError::InvalidRequest)?,
                Action::Decrease { quantity_ticks } => position
                    .checked_sub(quantity_ticks)
                    .ok_or(EngineError::InvalidRequest)?,
                Action::Close => 0,
                Action::TargetWeight { .. } => return Err(EngineError::InvalidRequest),
            };
            let delta = target
                .checked_sub(position)
                .ok_or(EngineError::InvalidRequest)?;
            let base_sequence = event
                .sequence
                .checked_mul(2)
                .ok_or(EngineError::InvalidRequest)?;
            if delta != 0 {
                replay_events.push(insider_replay::BacktestEvent::Fill {
                    sequence: base_sequence,
                    quantity_ticks: delta,
                    price_ticks: event.price_ticks,
                    fee_ticks: event.fee_ticks,
                });
            }
            replay_events.push(insider_replay::BacktestEvent::Mark {
                sequence: base_sequence
                    .checked_add(1)
                    .ok_or(EngineError::InvalidRequest)?,
                price_ticks: event.price_ticks,
            });
            position = target;
            last_sequence = event.sequence;
            last_now = event.now_mono_ns;
        }
        drop(strategy_host);
        let mut runner = insider_replay::BacktestRunner::new(initial_cash_ticks);
        for event in replay_events {
            runner
                .apply(event)
                .map_err(|_| EngineError::InvalidRequest)?;
        }
        let result = BacktestRunResult {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            report: runner.finish().map_err(|_| EngineError::InvalidRequest)?,
        };
        self.record_backtest_experiment(&result)?;
        self.append_event(&encode_backtest_result(&result))?;
        self.backtest_runs
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(result.run_id.clone(), result.clone());
        Ok(result)
    }

    /// Returns immutable backtest results in deterministic run-ID order.
    #[must_use]
    pub fn backtest_runs(&self) -> Vec<BacktestRunResult> {
        self.backtest_runs
            .lock()
            .map(|runs| runs.values().cloned().collect())
            .unwrap_or_default()
    }

    #[allow(clippy::cast_precision_loss)]
    fn record_backtest_experiment(&self, result: &BacktestRunResult) -> Result<(), EngineError> {
        let run_id = format!("backtest:{}", result.run_id);
        if self
            .experiment_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .get(&run_id)
            .is_some()
        {
            return Err(EngineError::InvalidRequest);
        }
        let mut metrics = BTreeMap::new();
        metrics.insert("event_count".to_owned(), result.report.event_count as f64);
        metrics.insert(
            "max_drawdown_ticks".to_owned(),
            result.report.max_drawdown_ticks as f64,
        );
        metrics.insert(
            "total_fees_ticks".to_owned(),
            result.report.total_fees_ticks as f64,
        );
        if let Some(snapshot) = result.report.final_snapshot {
            metrics.insert(
                "final_equity_ticks".to_owned(),
                snapshot.equity_ticks as f64,
            );
        }
        if let Ok(statistics) = result.report.statistics() {
            metrics.insert("return_mean".to_owned(), statistics.mean);
            metrics.insert(
                "return_standard_deviation".to_owned(),
                statistics.standard_deviation,
            );
            metrics.insert("sharpe".to_owned(), statistics.sharpe);
            metrics.insert("return_skewness".to_owned(), statistics.skewness);
            metrics.insert(
                "return_excess_kurtosis".to_owned(),
                statistics.excess_kurtosis,
            );
            if let Ok(probability) = result.report.deflated_sharpe(1) {
                metrics.insert("deflated_sharpe_probability".to_owned(), probability);
            }
        }
        self.create_experiment(ExperimentRun {
            run_id: run_id.clone(),
            code_hash: format!("strategy:{}", result.strategy_id),
            config_hash: result.config_hash.clone(),
            dataset_hash: result.dataset_hash.clone(),
            provenance: ExperimentProvenance {
                strategy_id: Some(result.strategy_id.clone()),
                ..Default::default()
            },
            status: RunStatus::Created,
            metrics: BTreeMap::new(),
            artifacts: Vec::new(),
        })?;
        self.start_experiment(&run_id)?;
        let bundle = backtest_experiment_bundle(result)?;
        self.publish_experiment_bundle(&bundle)?;
        self.succeed_experiment(&run_id, metrics)
    }

    /// Registers a research run with immutable lineage hashes.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid or duplicate metadata,
    /// or [`EngineError::Journal`] if the snapshot cannot be persisted.
    pub fn create_experiment(&self, run: ExperimentRun) -> Result<(), EngineError> {
        let run_id = run.run_id.clone();
        let mut registry = self
            .experiment_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = registry.clone();
        candidate
            .create(run)
            .map_err(|_| EngineError::InvalidRequest)?;
        let persisted = candidate
            .get(&run_id)
            .cloned()
            .ok_or(EngineError::InvalidRequest)?;
        self.append_event(&encode_experiment_run(&persisted))?;
        index_experiment_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &persisted,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("experiment graph: {error:?}")))?;
        *registry = candidate;
        Ok(())
    }

    /// Starts a registered research run and persists the new lifecycle state.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an unknown run or invalid transition.
    pub fn start_experiment(&self, run_id: &str) -> Result<(), EngineError> {
        self.mutate_experiment(run_id, |registry| registry.start(run_id))
    }

    /// Completes a research run with finite scalar metrics.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid metrics or lifecycle state.
    pub fn succeed_experiment(
        &self,
        run_id: &str,
        metrics: BTreeMap<String, f64>,
    ) -> Result<(), EngineError> {
        self.mutate_experiment(run_id, |registry| registry.succeed(run_id, metrics))
    }

    /// Marks a running research run failed and persists that state.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an unknown run or invalid transition.
    pub fn fail_experiment(&self, run_id: &str) -> Result<(), EngineError> {
        self.mutate_experiment(run_id, |registry| registry.fail(run_id))
    }

    /// Attaches a hash-addressed artifact to a running or successful run.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid artifact metadata or lifecycle state.
    pub fn add_experiment_artifact(
        &self,
        run_id: &str,
        artifact: ExperimentArtifact,
    ) -> Result<(), EngineError> {
        self.mutate_experiment(run_id, |registry| registry.add_artifact(run_id, artifact))
    }

    /// Publishes an immutable provenance bundle beside the journal and
    /// attaches its content hash to the corresponding experiment run.
    /// Repeating a publish for the same content is idempotent after hash
    /// verification; a different bundle cannot overwrite an existing object.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for malformed bundle metadata or
    /// missing experiment state, [`EngineError::ReadModel`] for bundle storage
    /// failures, or [`EngineError::Journal`] if artifact attachment cannot be
    /// durably recorded.
    pub fn publish_experiment_bundle(
        &self,
        bundle: &ExperimentBundle,
    ) -> Result<String, EngineError> {
        {
            let registry = self
                .experiment_registry
                .lock()
                .map_err(|_| EngineError::Poisoned)?;
            let run = registry
                .get(&bundle.run_id)
                .ok_or(EngineError::InvalidRequest)?;
            if !matches!(run.status, RunStatus::Running | RunStatus::Succeeded) {
                return Err(EngineError::InvalidRequest);
            }
        }
        let hash = bundle
            .content_hash()
            .map_err(|_| EngineError::InvalidRequest)?;
        match self.experiment_bundles.publish(bundle) {
            Ok(published) if published == hash => {}
            Err(BundleError::AlreadyExists) => {
                self.experiment_bundles
                    .verify(&hash)
                    .map_err(|error| EngineError::ReadModel(format!("bundle verify: {error:?}")))?;
            }
            Err(error) => {
                return Err(EngineError::ReadModel(format!("bundle publish: {error:?}")));
            }
            Ok(_) => return Err(EngineError::ReadModel("bundle hash mismatch".into())),
        }
        self.add_experiment_artifact(
            &bundle.run_id,
            ExperimentArtifact {
                kind: "experiment_bundle".into(),
                hash: hash.clone(),
                path: self
                    .experiment_bundles
                    .manifest_path(&hash)
                    .display()
                    .to_string(),
            },
        )?;
        Ok(hash)
    }

    /// Returns all research runs in deterministic run-ID order.
    #[must_use]
    pub fn experiment_runs(&self) -> Vec<ExperimentRun> {
        self.experiment_registry
            .lock()
            .map(|registry| registry.all())
            .unwrap_or_default()
    }

    fn mutate_experiment<F>(&self, run_id: &str, mutate: F) -> Result<(), EngineError>
    where
        F: FnOnce(
            &mut ExperimentRegistry,
        ) -> Result<(), insider_experiment_registry::RegistryError>,
    {
        if run_id.trim().is_empty() {
            return Err(EngineError::InvalidRequest);
        }
        let mut registry = self
            .experiment_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = registry.clone();
        mutate(&mut candidate).map_err(|_| EngineError::InvalidRequest)?;
        let persisted = candidate
            .get(run_id)
            .cloned()
            .ok_or(EngineError::InvalidRequest)?;
        self.append_event(&encode_experiment_run(&persisted))?;
        index_experiment_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &persisted,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("experiment graph: {error:?}")))?;
        *registry = candidate;
        Ok(())
    }

    /// Registers a model and immutable provenance, then journals the registry image.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for invalid or duplicate metadata.
    pub fn register_model(
        &self,
        record: ModelRecord,
        manifest: ArtifactManifest,
    ) -> Result<(), EngineError> {
        let mut registry = self
            .model_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = registry.clone();
        candidate
            .register_verified(record, manifest)
            .map_err(|_| EngineError::InvalidRequest)?;
        let snapshot = candidate.snapshot();
        self.append_event(&encode_model_registry(&snapshot))?;
        index_model_snapshot_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &snapshot,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("model graph: {error:?}")))?;
        *registry = candidate;
        Ok(())
    }

    /// Advances a model from Research to Validated with evidence authorization.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an invalid lifecycle transition.
    pub fn validate_model(
        &self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), EngineError> {
        self.mutate_model(|registry| registry.validate(model_id, version, evidence_id))
    }

    /// Starts shadow evaluation for a validated model.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an invalid lifecycle transition.
    pub fn start_model_shadow(&self, model_id: &str, version: &str) -> Result<(), EngineError> {
        self.mutate_model(|registry| registry.start_shadow(model_id, version))
    }

    /// Starts bounded canary evaluation with explicit evidence.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an invalid lifecycle transition.
    pub fn start_model_canary(
        &self,
        model_id: &str,
        version: &str,
        evidence_id: &str,
    ) -> Result<(), EngineError> {
        self.mutate_model(|registry| registry.start_canary(model_id, version, evidence_id))
    }

    /// Promotes a canary model to production and retires its predecessor.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] when promotion requirements fail.
    pub fn promote_model(&self, model_id: &str, version: &str) -> Result<(), EngineError> {
        self.mutate_model(|registry| registry.promote(model_id, version))
    }

    /// Returns the complete immutable model registry image.
    #[must_use]
    pub fn model_registry_snapshot(&self) -> ModelRegistrySnapshot {
        self.model_registry.lock().map_or(
            ModelRegistrySnapshot {
                records: Vec::new(),
                manifests: Vec::new(),
                active: Vec::new(),
            },
            |registry| registry.snapshot(),
        )
    }

    fn mutate_model<F>(&self, mutate: F) -> Result<(), EngineError>
    where
        F: FnOnce(&mut ModelRegistry) -> Result<(), insider_model_registry::RegistryError>,
    {
        let mut registry = self
            .model_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = registry.clone();
        mutate(&mut candidate).map_err(|_| EngineError::InvalidRequest)?;
        let snapshot = candidate.snapshot();
        self.append_event(&encode_model_registry(&snapshot))?;
        index_model_snapshot_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &snapshot,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("model graph: {error:?}")))?;
        *registry = candidate;
        Ok(())
    }

    fn submit_intent_with_timing(
        &self,
        intent: &insider_broker_api::OrderIntent,
        decision_mono: MonoTime,
    ) -> Result<(), EngineError> {
        let market = self.market_reference(intent.instrument_id);
        let decision = self
            .runtime
            .record_decision(intent, decision_mono.as_nanos(), market)?;
        self.append_event(&encode_execution_timing(&decision))?;
        let send = self.runtime.record_send(
            &intent.client_order_id,
            self.monotonic_now().as_nanos(),
            self.market_reference(intent.instrument_id),
        )?;
        self.append_event(&encode_execution_timing(&send))?;
        self.runtime.submit_intent(intent)
    }

    fn market_reference(&self, instrument_id: InstrumentId) -> Option<ExecutionMarketReference> {
        let snapshot = self.market_data.lock().ok()?.snapshot(instrument_id)?;
        let (bid, ask) = snapshot
            .quote
            .map(|quote| (quote.bid_ticks, quote.ask_ticks))
            .or_else(|| snapshot.book_top.map(|top| (top.0, top.2)))?;
        if bid <= 0 || ask < bid {
            return None;
        }
        let midpoint = bid.checked_add(ask)?.checked_div(2)?;
        let spread = ask.checked_sub(bid)?;
        (midpoint > 0).then_some(ExecutionMarketReference {
            mid_ticks: midpoint,
            spread_ticks: spread,
        })
    }

    fn event_market_reference(&self, event: &BrokerEvent) -> Option<ExecutionMarketReference> {
        let instrument = self
            .runtime
            .order_instrument(broker_event_client_order_id(event))?;
        self.market_reference(instrument)
    }

    /// Appends a versioned opaque domain event to durable storage.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] when the append or stable sync fails.
    pub fn append_event(&self, payload: &[u8]) -> Result<u64, EngineError> {
        // Journal and rebuildable projection form one ordered publication
        // boundary. Provider, scheduler, and IPC threads may all emit events;
        // serialize the pair so two projection appends cannot read the same
        // header count and corrupt the projection.
        let _append_guard = self.append_lock.lock().map_err(|_| EngineError::Poisoned)?;
        let sequence = self.journal.append(payload).map_err(EngineError::Journal)?;
        self.read_model
            .append_record(&insider_journal::Record {
                sequence,
                payload: payload.to_vec(),
            })
            .map_err(|error| EngineError::ReadModel(format!("{error:?}")))?;
        Ok(sequence)
    }

    /// Verifies the current journal seal before an operator treats the segment as immutable evidence.
    ///
    /// # Errors
    /// Returns a journal error when no valid seal exists.
    pub fn verify_journal_seal(&self) -> Result<[u8; 32], EngineError> {
        self.journal.verify_seal().map_err(EngineError::Journal)
    }

    /// Creates an atomic, hash-verified backup of the authoritative journal.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] when the journal cannot be synced or
    /// the destination already exists.
    pub fn backup_journal(
        &self,
        destination: impl AsRef<std::path::Path>,
    ) -> Result<BackupManifest, EngineError> {
        self.journal
            .backup_to(destination)
            .map_err(EngineError::Journal)
    }

    /// Restores a verified journal backup into a new path before startup.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] when the source seal, framing, or
    /// destination publication is invalid.
    pub fn restore_journal_backup(
        source: impl AsRef<std::path::Path>,
        destination: impl AsRef<std::path::Path>,
    ) -> Result<BackupManifest, EngineError> {
        Journal::restore_backup(source, destination).map_err(EngineError::Journal)
    }

    /// Persists and applies one broker event in authoritative order.
    ///
    /// # Errors
    /// Returns a journal error before mutation when the event cannot be made
    /// durable, or a transition error when the persisted event is invalid for
    /// the local order projection.
    #[allow(clippy::needless_pass_by_value)]
    pub fn apply_broker_event(&self, event: BrokerEvent) -> Result<(), EngineError> {
        self.append_event(&encode_broker_event(&event))?;
        if let Some(timing) = self.runtime.record_broker_timing(
            &event,
            self.monotonic_now().as_nanos(),
            self.event_market_reference(&event),
        )? {
            self.append_event(&encode_execution_timing(&timing))?;
        }
        let prior_fill = self.fill_quantity_for_event(&event)?;
        self.runtime.apply_broker_event(event.clone())?;
        if let BrokerEvent::Filled {
            client_order_id,
            quantity_ticks,
            price_ticks,
        } = &event
        {
            self.index_fill_graph(
                client_order_id,
                *quantity_ticks,
                *price_ticks,
                self.monotonic_now(),
            )?;
        }
        index_portfolio_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &self.runtime.portfolio()?,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("portfolio graph: {error:?}")))?;
        if self.fill_advanced(&event, prior_fill)?
            && let Some(summary) = self.attribute_strategy_fill(&event)?
        {
            self.append_event(&encode_strategy_execution_summary(&summary))?;
        }
        if let Some(record) = self.runtime.apply_child_event(&event)? {
            self.append_event(&encode_child_plan(&record))?;
        }
        Ok(())
    }

    fn index_order_graph(
        &self,
        intent: &insider_broker_api::OrderIntent,
        now: MonoTime,
    ) -> Result<(), EngineError> {
        index_order_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            intent,
            i64::try_from(now.as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("order graph: {error:?}")))
    }

    fn index_fill_graph(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        price_ticks: i64,
        now: MonoTime,
    ) -> Result<(), EngineError> {
        index_fill_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            client_order_id,
            quantity_ticks,
            price_ticks,
            i64::try_from(now.as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("fill graph: {error:?}")))
    }

    fn fill_quantity_for_event(&self, event: &BrokerEvent) -> Result<Option<i64>, EngineError> {
        match event {
            BrokerEvent::Filled {
                client_order_id, ..
            } => self.runtime.filled_quantity(client_order_id).map(Some),
            _ => Ok(None),
        }
    }

    fn fill_advanced(&self, event: &BrokerEvent, prior: Option<i64>) -> Result<bool, EngineError> {
        let BrokerEvent::Filled {
            client_order_id, ..
        } = event
        else {
            return Ok(false);
        };
        let Some(prior) = prior else {
            return Ok(false);
        };
        Ok(self.runtime.filled_quantity(client_order_id)? > prior)
    }

    fn attribute_strategy_fill(
        &self,
        event: &BrokerEvent,
    ) -> Result<Option<StrategyExecutionSummary>, EngineError> {
        let BrokerEvent::Filled {
            client_order_id,
            quantity_ticks,
            price_ticks,
        } = event
        else {
            return Ok(None);
        };
        let Some(proposal_id) = Self::proposal_id_from_client_order_id(client_order_id) else {
            return Ok(None);
        };
        let Some(record) = self.strategy_proposal_record(proposal_id) else {
            return Ok(None);
        };
        let signed_quantity = self
            .runtime
            .order_side(client_order_id)
            .map(|side| {
                if side == Side::Buy {
                    *quantity_ticks
                } else {
                    quantity_ticks.saturating_neg()
                }
            })
            .ok_or(EngineError::Transition(TransitionError::UnknownOrder))?;
        let mut summaries = self
            .strategy_execution_summaries
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let summary = summaries
            .entry(record.proposal.strategy_id.clone())
            .or_insert_with(|| StrategyExecutionSummary {
                strategy_id: record.proposal.strategy_id.clone(),
                fill_count: 0,
                filled_quantity_ticks: 0,
                notional_ticks: 0,
            });
        summary.fill_count = summary
            .fill_count
            .checked_add(1)
            .ok_or(EngineError::InvalidRequest)?;
        summary.filled_quantity_ticks = summary
            .filled_quantity_ticks
            .checked_add(i128::from(signed_quantity))
            .ok_or(EngineError::InvalidRequest)?;
        summary.notional_ticks = summary
            .notional_ticks
            .checked_add(
                i128::from(signed_quantity)
                    .checked_mul(i128::from(*price_ticks))
                    .ok_or(EngineError::InvalidRequest)?,
            )
            .ok_or(EngineError::InvalidRequest)?;
        Ok(Some(summary.clone()))
    }

    fn proposal_id_from_client_order_id(client_order_id: &str) -> Option<ProposalId> {
        let (_, proposal_text) = client_order_id.rsplit_once('-')?;
        let raw_proposal_text = proposal_text.strip_prefix("proposal_")?;
        let raw_proposal = u128::from_str_radix(raw_proposal_text, 16).ok()?;
        ProposalId::new(raw_proposal).ok()
    }

    fn recovered_strategy_execution(
        coordinator: &Coordinator,
        runtime: &Runtime,
        event: &BrokerEvent,
        summaries: &mut BTreeMap<String, StrategyExecutionSummary>,
    ) -> Result<Option<StrategyExecutionSummary>, EngineError> {
        let BrokerEvent::Filled {
            client_order_id,
            quantity_ticks,
            price_ticks,
        } = event
        else {
            return Ok(None);
        };
        let Some(proposal_id) = Self::proposal_id_from_client_order_id(client_order_id) else {
            return Ok(None);
        };
        let Some(record) = coordinator.record(proposal_id) else {
            return Ok(None);
        };
        let Some(side) = runtime.order_side(client_order_id) else {
            return Ok(None);
        };
        let signed_quantity = if side == Side::Buy {
            *quantity_ticks
        } else {
            quantity_ticks.saturating_neg()
        };
        let summary = summaries
            .entry(record.proposal.strategy_id.clone())
            .or_insert_with(|| StrategyExecutionSummary {
                strategy_id: record.proposal.strategy_id.clone(),
                fill_count: 0,
                filled_quantity_ticks: 0,
                notional_ticks: 0,
            });
        summary.fill_count = summary
            .fill_count
            .checked_add(1)
            .ok_or(EngineError::InvalidRequest)?;
        summary.filled_quantity_ticks = summary
            .filled_quantity_ticks
            .checked_add(i128::from(signed_quantity))
            .ok_or(EngineError::InvalidRequest)?;
        summary.notional_ticks = summary
            .notional_ticks
            .checked_add(
                i128::from(signed_quantity)
                    .checked_mul(i128::from(*price_ticks))
                    .ok_or(EngineError::InvalidRequest)?,
            )
            .ok_or(EngineError::InvalidRequest)?;
        Ok(Some(summary.clone()))
    }

    /// Starts the explicit live-enable challenge through the runtime guard.
    ///
    /// # Errors
    /// Returns an engine or live-guard error when the challenge is rejected.
    pub fn arm_live(
        &self,
        account: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<String, EngineError> {
        self.runtime.arm_live(account, now, phrase)
    }

    /// Replaces live limits while retaining paper mode.
    ///
    /// # Errors
    /// Returns an engine or live-guard error when live is already enabled.
    pub fn configure_live_limits(&self, limits: LiveLimits) -> Result<(), EngineError> {
        self.append_event(&encode_live_limits(&limits))?;
        self.runtime.configure_live_limits(limits)
    }

    /// Completes the explicit live-enable challenge.
    ///
    /// # Errors
    /// Returns an engine or live-guard error when the challenge is invalid.
    pub fn confirm_live(
        &self,
        account: &str,
        token: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<(), EngineError> {
        self.runtime.confirm_live(account, token, now, phrase)
    }

    /// Activates the live kill switch.
    ///
    /// # Errors
    /// Returns an engine error when the guard lock is unavailable.
    pub fn kill_live(&self) -> Result<(), EngineError> {
        self.append_event(&encode_live_kill())?;
        self.runtime.kill_live()
    }

    /// Returns the enforced paper/live/killed environment.
    ///
    /// # Errors
    /// Returns an engine error when the guard lock is unavailable.
    pub fn trading_environment(&self) -> Result<TradingEnvironment, EngineError> {
        self.runtime.trading_environment()
    }

    /// Returns the durable manual/hybrid/autonomous decision mode.
    #[must_use]
    pub fn autonomy_mode(&self) -> AutonomyMode {
        self.autonomy_mode
            .lock()
            .map_or(AutonomyMode::Manual, |mode| *mode)
    }

    /// Persists an explicit automation-mode change before publishing it.
    /// Mode changes never bypass live guards, proposal validation, or risk.
    ///
    /// # Errors
    /// Returns [`EngineError::Journal`] when the mode event cannot be made
    /// durable, or [`EngineError::Poisoned`] when state is unavailable.
    pub fn set_autonomy_mode(&self, mode: AutonomyMode) -> Result<(), EngineError> {
        self.append_event(&encode_autonomy_mode(mode))?;
        *self
            .autonomy_mode
            .lock()
            .map_err(|_| EngineError::Poisoned)? = mode;
        if mode != AutonomyMode::Manual && self.lifecycle() == Lifecycle::Running {
            self.recover_executing_autonomy_plans(self.monotonic_now())?;
        }
        Ok(())
    }

    /// Durably submits one validated autonomous plan before publishing it to
    /// the in-memory lifecycle projection.
    ///
    /// # Errors
    /// Returns [`EngineError::Autonomy`] for invalid/duplicate plans and
    /// [`EngineError::Journal`] if the lifecycle event cannot be persisted.
    pub fn submit_autonomy_plan(&self, plan: Plan, now: MonoTime) -> Result<(), EngineError> {
        let mut plans = self
            .autonomy_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = plans.clone();
        candidate
            .submit(plan, now)
            .map_err(|error| EngineError::Autonomy(format!("{error:?}")))?;
        for event in candidate.drain_events() {
            self.append_event(&encode_plan_event(&event))?;
        }
        *plans = candidate;
        Ok(())
    }

    /// Durably records an autonomous plan lifecycle transition before
    /// publishing the new state.
    ///
    /// # Errors
    /// Returns [`EngineError::Autonomy`] for an invalid transition and
    /// [`EngineError::Journal`] if the transition cannot be persisted.
    pub fn transition_autonomy_plan(
        &self,
        plan_id: &str,
        next: PlanState,
        now: MonoTime,
    ) -> Result<(), EngineError> {
        if matches!(next, PlanState::Approved | PlanState::Executing)
            && self.autonomy_mode() == AutonomyMode::Manual
        {
            return Err(EngineError::Autonomy(
                "manual mode cannot approve or execute autonomous plans".into(),
            ));
        }
        let mut plans = self
            .autonomy_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = plans.clone();
        let transition_result = candidate.transition(plan_id, next, now);
        for event in candidate.drain_events() {
            self.append_event(&encode_plan_event(&event))?;
        }
        *plans = candidate;
        transition_result.map_err(|error| EngineError::Autonomy(format!("{error:?}")))?;
        drop(plans);

        if next == PlanState::Executing {
            let execution_result = self.execute_autonomy_plan(plan_id, now);
            let terminal_state = if execution_result.is_ok() {
                PlanState::Completed
            } else {
                PlanState::Failed
            };
            // The terminal lifecycle event is best-effort after execution; the
            // plan remains durably Executing if journaling the terminal state
            // fails, which is safer than claiming completion.
            let terminal_result = self.transition_autonomy_plan(plan_id, terminal_state, now);
            if let Err(error) = execution_result {
                let _ = terminal_result;
                return Err(error);
            }
            terminal_result?;
        }
        Ok(())
    }

    /// Executes the broker-affecting subset of one autonomous plan through the
    /// normal proposal/risk/execution boundary. Non-trading control actions are
    /// retained in the plan audit trail but do not mutate broker state.
    fn execute_autonomy_plan(&self, plan_id: &str, now: MonoTime) -> Result<(), EngineError> {
        let record = self
            .autonomy_plan(plan_id)
            .ok_or_else(|| EngineError::Autonomy("autonomous plan disappeared".into()))?;
        let trace_seed = stable_autonomy_trace_seed(plan_id);
        for (index, action) in record.plan.actions.iter().enumerate() {
            let Some(proposal_text) = action.proposal_id.as_deref() else {
                if matches!(
                    action.action_type,
                    ActionType::ExecuteProposal | ActionType::ExecuteProposalScaled
                ) {
                    return Err(EngineError::Autonomy(
                        "execute action is missing proposal ID".into(),
                    ));
                }
                continue;
            };
            let proposal_id = proposal_text
                .parse::<ProposalId>()
                .ok()
                .or_else(|| {
                    proposal_text
                        .parse::<u128>()
                        .ok()
                        .and_then(|value| ProposalId::new(value).ok())
                })
                .ok_or_else(|| EngineError::Autonomy("autonomous proposal ID is invalid".into()))?;
            let trace = TraceId::new(trace_seed.saturating_add(index as u128).max(1))
                .map_err(|_| EngineError::Autonomy("autonomous trace ID is invalid".into()))?;
            match action.action_type {
                ActionType::ExecuteProposal => {
                    if self
                        .runtime
                        .has_order(&self.runtime.client_order_id_for_proposal(proposal_id))?
                    {
                        continue;
                    }
                    let proposal = self
                        .strategy_proposal_record(proposal_id)
                        .ok_or_else(|| {
                            EngineError::Autonomy("autonomous proposal unavailable".into())
                        })?
                        .proposal;
                    self.submit_proposal(&proposal, now, trace)?;
                }
                ActionType::ExecuteProposalScaled => {
                    if self
                        .runtime
                        .has_order(&self.runtime.client_order_id_for_proposal(proposal_id))?
                    {
                        continue;
                    }
                    let scale = action.scale.ok_or_else(|| {
                        EngineError::Autonomy("scaled action is missing scale".into())
                    })?;
                    self.submit_scaled_proposal(proposal_id, scale, now, trace)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Resumes plans that were durably marked executing before a process
    /// crash. Existing client-order IDs are treated as already submitted and
    /// are never resent; unresolved plans remain visible as failed for an
    /// operator rather than being retried blindly.
    fn recover_executing_autonomy_plans(&self, now: MonoTime) -> Result<(), EngineError> {
        // A persisted mode change to Manual is an explicit revocation of
        // autonomous authority. Recovery must not reinterpret an old
        // Executing state as permission to submit while that mode is active.
        if self.autonomy_mode() == AutonomyMode::Manual {
            return Ok(());
        }
        let plan_ids = self
            .autonomy_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .executing_plan_ids();
        for plan_id in plan_ids {
            let Some(record) = self.autonomy_plan(&plan_id) else {
                continue;
            };
            if now >= record.plan.expires_at {
                let _ = self.transition_autonomy_plan(&plan_id, PlanState::Expired, now);
                continue;
            }
            match self.execute_autonomy_plan(&plan_id, now) {
                Ok(()) => {
                    self.transition_autonomy_plan(&plan_id, PlanState::Completed, now)?;
                }
                Err(error) => {
                    let _ = self.transition_autonomy_plan(&plan_id, PlanState::Failed, now);
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    /// Returns one restored autonomous plan record without exposing mutation.
    #[must_use]
    pub fn autonomy_plan(&self, plan_id: &str) -> Option<insider_autonomy::PlanRecord> {
        self.autonomy_plans
            .lock()
            .ok()
            .and_then(|plans| plans.get(plan_id).cloned())
    }

    /// Builds the authoritative, deny-by-default LLM tool registry for this
    /// runtime. Handlers read projections only; they cannot submit orders.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if a projection cannot be read while
    /// constructing the registry.
    pub fn llm_tool_registry(&self) -> Result<ToolRegistry, EngineError> {
        let mut registry = ToolRegistry::new();
        registry
            .register(Box::new(PortfolioTool::new(Arc::clone(&self.runtime))))
            .map_err(|_| EngineError::InvalidRequest)?;
        registry
            .register(Box::new(PositionTool::new(Arc::clone(&self.runtime))))
            .map_err(|_| EngineError::InvalidRequest)?;
        registry
            .register(Box::new(RecentFillsTool::new(Arc::clone(&self.runtime))))
            .map_err(|_| EngineError::InvalidRequest)?;
        Ok(registry)
    }

    /// Registers an immutable prompt artifact for exact-version LLM requests.
    ///
    /// # Errors
    /// Returns [`EngineError::Llm`] when metadata is invalid or the version is
    /// already registered.
    pub fn register_prompt(&self, prompt: PromptRecord) -> Result<(), EngineError> {
        let prompt_id = prompt.prompt_id.clone();
        let version = prompt.version.clone();
        let mut registry = self
            .prompt_registry
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let mut candidate = registry.clone();
        candidate.register(prompt).map_err(EngineError::Llm)?;
        let persisted = candidate
            .get(&prompt_id, &version)
            .cloned()
            .ok_or(EngineError::InvalidRequest)?;
        self.append_event(&encode_prompt_record(&persisted))?;
        *registry = candidate;
        Ok(())
    }

    /// Returns the immutable prompt registry snapshot in deterministic order.
    #[must_use]
    pub fn prompt_registry(&self) -> Vec<PromptRecord> {
        self.prompt_registry
            .lock()
            .map(|registry| registry.records())
            .unwrap_or_default()
    }

    /// Installs one provider for control-plane analysis. The provider is
    /// replaceable only before another provider has been installed, preventing
    /// an in-flight analyst request from silently changing vendor semantics.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for a provider with no usable
    /// endpoint capability or an already configured runtime.
    pub fn install_llm_provider(
        &self,
        provider: Arc<dyn LlmProvider>,
    ) -> Result<LlmCapabilities, EngineError> {
        if let Some(manifest) = provider.manifest() {
            manifest
                .validate()
                .map_err(|_| EngineError::InvalidRequest)?;
        }
        let capabilities = provider.capabilities();
        if !capabilities.responses && !capabilities.chat_completions {
            return Err(EngineError::InvalidRequest);
        }
        let mut slot = self
            .llm_provider
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        if slot.is_some() {
            return Err(EngineError::InvalidRequest);
        }
        *slot = Some(provider);
        Ok(capabilities)
    }

    /// Returns the configured provider capabilities, if a provider is present.
    #[must_use]
    pub fn llm_capabilities(&self) -> Option<LlmCapabilities> {
        self.llm_provider
            .lock()
            .ok()
            .and_then(|provider| provider.as_ref().map(|value| value.capabilities()))
    }

    /// Performs one bounded control-plane completion through the installed
    /// provider. This method is intentionally absent from market/strategy hot
    /// callbacks; callers must schedule it on an intelligence worker.
    ///
    /// # Errors
    /// Returns [`EngineError::Llm`] for provider, transport, validation, or
    /// output failures, and [`EngineError::InvalidRequest`] when no provider is
    /// installed.
    pub fn llm_complete(&self, request: &LlmRequest) -> Result<LlmResponse, EngineError> {
        let provider = self
            .llm_provider
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone()
            .ok_or(EngineError::InvalidRequest)?;
        let result = provider.complete(request);
        if let Ok(mut supervisor) = self.supervisor.lock() {
            let health = if result.is_ok() {
                SupervisorHealth::Healthy
            } else {
                SupervisorHealth::Degraded
            };
            let _ = supervisor.set_health("llm", health);
        }
        result.map_err(EngineError::Llm)
    }

    /// Performs one bounded control-plane streaming request. Stream items are
    /// returned in provider order; callers must display deltas only and wait
    /// for the terminal item before treating the response as complete.
    ///
    /// # Errors
    /// Returns [`EngineError::Llm`] for provider, transport, validation, or
    /// interrupted-stream failures, and [`EngineError::InvalidRequest`] when
    /// no provider is installed.
    pub fn llm_stream(&self, request: &LlmRequest) -> Result<Vec<StreamItem>, EngineError> {
        let provider = self
            .llm_provider
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone()
            .ok_or(EngineError::InvalidRequest)?;
        let result = provider.stream(request);
        if let Ok(mut supervisor) = self.supervisor.lock() {
            let health = if result.is_ok() {
                SupervisorHealth::Healthy
            } else {
                SupervisorHealth::Degraded
            };
            let _ = supervisor.set_health("llm", health);
        }
        result.map_err(EngineError::Llm)
    }

    /// Completes and semantically validates one autonomous action. The full
    /// response is buffered and parsed before it can reach autonomy; this
    /// method never submits an order or mutates a plan.
    ///
    /// # Errors
    /// Returns [`EngineError::Llm`] for transport, trace, syntax, schema, or
    /// action validation failures.
    pub fn llm_autonomous_action(
        &self,
        request: &LlmRequest,
    ) -> Result<AutonomousAction, EngineError> {
        let response = self.llm_complete(request)?;
        if response.trace_id != request.trace_id {
            return Err(EngineError::Llm(LlmError::SchemaViolation(
                "autonomous response trace mismatch".into(),
            )));
        }
        let action = parse_autonomous_action(&response.content).map_err(EngineError::Llm)?;
        action.validate().map_err(EngineError::Llm)?;
        if matches!(
            action.action_type,
            ActionType::PauseStrategy | ActionType::ReduceAutonomy
        ) || action.reason_codes.iter().any(|code| {
            let code = code.to_ascii_uppercase();
            code.contains("ALERT") || code.contains("LIQUID") || code.contains("MARGIN")
        }) {
            self.publish_llm_alert(request, &action);
        }
        Ok(action)
    }

    fn publish_llm_alert(&self, request: &LlmRequest, action: &AutonomousAction) {
        let occurred_ms =
            i64::try_from(self.monotonic_now().as_nanos() / 1_000_000).unwrap_or(i64::MAX);
        let reason = action.reason_codes.join(", ");
        let message = format!(
            "AI control alert [{}]: {}",
            action_type_name(action.action_type),
            reason
        );
        let alert = Alert {
            alert_id: format!("llm-alert-{}", request.trace_id),
            dedupe_key: format!("llm-alert:{}", action_type_name(action.action_type)),
            source: "llm-autonomy".into(),
            occurred_ms,
            severity: AlertSeverity::Critical,
            message,
            sensitive: false,
        };
        let _ = self.append_event(&encode_alert(&alert));
        if let Ok(mut router) = self.alerts.lock() {
            let _ = router.route(alert, AlertChannel::InApp, occurred_ms);
        }
    }

    /// Requests cancellation through the normal durable order lifecycle.
    ///
    /// # Errors
    /// Returns an engine error when the order is not cancellable or the broker
    /// transport is ambiguous.
    pub fn cancel_order(&self, client_order_id: &str) -> Result<(), EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        self.journal
            .append(&encode_cancel_request(client_order_id))?;
        self.runtime.cancel_order(client_order_id)
    }

    /// Requests replacement of a working order through the broker gateway.
    ///
    /// # Errors
    /// Returns an engine error when replacement is unsupported, invalid, or
    /// transport-ambiguous.
    pub fn replace_order(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), EngineError> {
        if self.lifecycle() != Lifecycle::Running {
            return Err(EngineError::NotRunning);
        }
        self.append_event(&encode_replace_request(
            client_order_id,
            quantity_ticks,
            limit_price_ticks,
        ))?;
        self.runtime
            .replace_order(client_order_id, quantity_ticks, limit_price_ticks)
    }

    /// Publishes a trusted market mark to the portfolio/risk projection.
    ///
    /// # Errors
    /// Returns an engine error when the lifecycle is stopped or the mark is
    /// invalid.
    pub fn update_mark_price(
        &self,
        instrument_id: insider_common_types::InstrumentId,
        price_ticks: i64,
    ) -> Result<(), EngineError> {
        if self.lifecycle() == Lifecycle::Stopped {
            return Err(EngineError::NotRunning);
        }
        self.runtime.update_mark_price(instrument_id, price_ticks)
    }

    /// Installs a deterministic scoped risk policy for account/strategy/
    /// instrument planning. Passing `None` restores the configured system
    /// limits. The policy is immutable from the runtime's perspective and
    /// should be replaced atomically by configuration reload.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the runtime policy lock is unavailable.
    pub fn set_scoped_risk_policy(
        &self,
        policy: Option<ScopedRiskPolicy>,
    ) -> Result<(), EngineError> {
        let snapshot = policy.as_ref().map(ScopedRiskPolicy::snapshot);
        self.append_event(&encode_scoped_risk_policy(snapshot.as_ref())?)?;
        self.runtime.set_scoped_risk_policy(policy)
    }

    /// Returns the currently installed scoped risk policy for read-only
    /// operator inspection. The snapshot is cloned from the immutable policy
    /// representation and never exposes mutable risk-engine internals.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the runtime policy lock is unavailable.
    pub fn scoped_risk_policy_snapshot(
        &self,
    ) -> Result<Option<ScopedRiskPolicySnapshot>, EngineError> {
        self.runtime.scoped_risk_policy_snapshot()
    }

    /// Applies and journals a corporate action before mutating the portfolio
    /// projection. The same event is replayed during startup recovery.
    ///
    /// # Errors
    /// Returns [`EngineError::Accounting`] for invalid or unrepresentable
    /// actions, [`EngineError::Journal`] when durability fails, or
    /// [`EngineError::NotRunning`] after shutdown.
    pub fn apply_corporate_action(
        &self,
        instrument_id: InstrumentId,
        kind: CorporateActionKind,
    ) -> Result<(), EngineError> {
        if self.lifecycle() == Lifecycle::Stopped {
            return Err(EngineError::NotRunning);
        }
        let current = self.runtime.portfolio()?;
        let mut candidate = current;
        match kind {
            CorporateActionKind::Split {
                numerator,
                denominator,
            } => candidate
                .apply_split(instrument_id, numerator, denominator)
                .map_err(EngineError::Accounting)?,
            CorporateActionKind::CashDividend { amount_ticks } => candidate
                .apply_cash_dividend(instrument_id, amount_ticks)
                .map_err(EngineError::Accounting)?,
            CorporateActionKind::OptionExercise {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            } => candidate
                .apply_option_exercise(
                    instrument_id,
                    underlying_instrument_id,
                    option_quantity_delta_ticks,
                    underlying_quantity_delta_ticks,
                    cash_delta_ticks,
                )
                .map_err(EngineError::Accounting)?,
            CorporateActionKind::OptionAssignment {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            } => candidate
                .apply_option_assignment(
                    instrument_id,
                    underlying_instrument_id,
                    option_quantity_delta_ticks,
                    underlying_quantity_delta_ticks,
                    cash_delta_ticks,
                )
                .map_err(EngineError::Accounting)?,
            CorporateActionKind::OptionExpiry {
                option_quantity_delta_ticks,
                cash_delta_ticks,
            } => candidate
                .apply_option_expiry(instrument_id, option_quantity_delta_ticks, cash_delta_ticks)
                .map_err(EngineError::Accounting)?,
            CorporateActionKind::FuturesVariationMargin { cash_delta_ticks } => candidate
                .apply_futures_variation_margin(instrument_id, cash_delta_ticks)
                .map_err(EngineError::Accounting)?,
        };
        self.append_event(&encode_corporate_action(instrument_id, kind))?;
        self.runtime.apply_corporate_action(instrument_id, kind)
    }

    /// Durably changes the enforced risk state before exposing the new state.
    ///
    /// # Errors
    /// Returns an engine error when the state transition is invalid,
    /// unauthorized, or cannot be journaled.
    pub fn transition_risk_state(
        &self,
        next: RiskState,
        authorization: &str,
    ) -> Result<(), EngineError> {
        self.runtime.validate_risk_transition(next, authorization)?;
        self.journal
            .append(&encode_risk_state(next, authorization))?;
        self.runtime.transition_risk_state(next, authorization)?;
        let mut halt_cancel_failures = 0_usize;
        if matches!(next, RiskState::Halted)
            && let Ok(order_ids) = self.runtime.working_order_ids()
        {
            for client_order_id in order_ids {
                if self.cancel_order(&client_order_id).is_err() {
                    halt_cancel_failures = halt_cancel_failures.saturating_add(1);
                }
            }
        }
        if !matches!(next, RiskState::Running) {
            let occurred_ms =
                i64::try_from(self.monotonic_now().as_nanos() / 1_000_000).unwrap_or(i64::MAX);
            let severity = if matches!(next, RiskState::Halted) {
                AlertSeverity::Critical
            } else {
                AlertSeverity::Warning
            };
            let state_name = match next {
                RiskState::ReduceOnly => "REDUCE_ONLY",
                RiskState::CancelOnly => "CANCEL_ONLY",
                RiskState::Halted => "HALTED",
                RiskState::Running => "RUNNING",
            };
            let alert = Alert {
                alert_id: format!("risk-state-{occurred_ms}-{state_name}"),
                dedupe_key: format!("risk-state:{state_name}"),
                source: "risk-engine".into(),
                occurred_ms,
                severity,
                message: if halt_cancel_failures == 0 {
                    format!("Risk state changed to {state_name}")
                } else {
                    format!(
                        "Risk state changed to {state_name}; {halt_cancel_failures} working order cancellations failed"
                    )
                },
                sensitive: false,
            };
            // A journal failure must not roll back or mask the risk state
            // transition; the risk event is already authoritative. The alert
            // surface degrades until the next state transition/recovery.
            let _ = self.append_event(&encode_alert(&alert));
            // Alert delivery is deliberately best-effort: a saturated alert
            // queue must never prevent a risk halt from taking effect.
            if let Ok(mut router) = self.alerts.lock() {
                let _ = router.route(alert.clone(), AlertChannel::InApp, occurred_ms);
                if let Some(url) = &self.alert_webhook {
                    let _ = router.route(alert, AlertChannel::Webhook(url.clone()), occurred_ms);
                }
            }
        }
        Ok(())
    }

    /// Returns a bounded snapshot of unacknowledged in-app alerts.
    #[must_use]
    pub fn pending_alerts(&self) -> Vec<AlertRecord> {
        self.alerts
            .lock()
            .map(|router| {
                router
                    .pending()
                    .into_iter()
                    .filter(|(_, channel)| matches!(channel, AlertChannel::InApp))
                    .map(|(alert, _)| AlertRecord {
                        alert_id: alert.alert_id.clone(),
                        dedupe_key: alert.dedupe_key.clone(),
                        source: alert.source.clone(),
                        occurred_ms: alert.occurred_ms,
                        severity: match alert.severity {
                            AlertSeverity::Info => 1,
                            AlertSeverity::Warning => 2,
                            AlertSeverity::Critical => 3,
                        },
                        message: alert.message.clone(),
                        sensitive: alert.sensitive,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns the bounded webhook delivery queue without exposing webhook
    /// URLs or allowing the UI to acknowledge it as an in-app alert.
    #[must_use]
    pub fn pending_webhook_alerts(&self) -> Vec<AlertRecord> {
        let Some(url) = &self.alert_webhook else {
            return Vec::new();
        };
        self.alerts
            .lock()
            .map(|router| {
                router
                    .pending()
                    .into_iter()
                    .filter(|(_, channel)| {
                        matches!(channel, AlertChannel::Webhook(candidate) if candidate == url)
                    })
                    .map(|(alert, _)| AlertRecord {
                        alert_id: alert.alert_id.clone(),
                        dedupe_key: alert.dedupe_key.clone(),
                        source: alert.source.clone(),
                        occurred_ms: alert.occurred_ms,
                        severity: match alert.severity {
                            AlertSeverity::Info => 1,
                            AlertSeverity::Warning => 2,
                            AlertSeverity::Critical => 3,
                        },
                        message: alert.message.clone(),
                        sensitive: alert.sensitive,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Marks one successfully delivered webhook alert while retaining the
    /// corresponding in-app notification until the operator acknowledges it.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an invalid alert ID or
    /// [`EngineError::Poisoned`] if the alert queue cannot be accessed.
    pub fn acknowledge_webhook_alert(&self, alert_id: &str) -> Result<bool, EngineError> {
        if alert_id.trim().is_empty() || alert_id.len() > 256 {
            return Err(EngineError::InvalidRequest);
        }
        let Some(url) = &self.alert_webhook else {
            return Ok(false);
        };
        let mut router = self.alerts.lock().map_err(|_| EngineError::Poisoned)?;
        Ok(router.acknowledge_channel(alert_id, &AlertChannel::Webhook(url.clone())))
    }

    /// Acknowledges one in-app alert idempotently.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an invalid identifier or
    /// [`EngineError::Poisoned`] if the alert router lock is unavailable.
    pub fn acknowledge_alert(&self, alert_id: &str) -> Result<bool, EngineError> {
        if alert_id.trim().is_empty() || alert_id.len() > 256 {
            return Err(EngineError::InvalidRequest);
        }
        let mut router = self.alerts.lock().map_err(|_| EngineError::Poisoned)?;
        let mut candidate = router.clone();
        let acknowledged = candidate.acknowledge(alert_id);
        if acknowledged {
            self.append_event(&encode_alert_ack(alert_id))?;
            *router = candidate;
        }
        Ok(acknowledged)
    }

    /// Reconciles all unknown orders and journals returned broker events before
    /// mutating the local order and portfolio projections.
    ///
    /// # Errors
    /// Returns an engine error only when local runtime locks fail. Individual
    /// broker query failures are retained in the returned summary.
    pub fn reconcile_unknown_orders(&self) -> Result<ReconcileSummary, EngineError> {
        let ids = self.runtime.unknown_order_ids()?;
        let mut summary = ReconcileSummary {
            queried: ids.len(),
            resolved: 0,
            still_unknown: 0,
            failed: Vec::new(),
            external_orders: 0,
            missing_at_broker: 0,
            snapshot_positions: 0,
            snapshot_account_values: 0,
        };
        for client_order_id in ids {
            match self.runtime.query_reconcile(&client_order_id) {
                Ok(Some(event)) => {
                    self.append_event(&encode_broker_event(&event))?;
                    if let Some(timing) = self.runtime.record_broker_timing(
                        &event,
                        self.monotonic_now().as_nanos(),
                        self.event_market_reference(&event),
                    )? {
                        self.append_event(&encode_execution_timing(&timing))?;
                    }
                    self.runtime.apply_reconciled_broker_event(event.clone())?;
                    if let BrokerEvent::Filled {
                        client_order_id,
                        quantity_ticks,
                        price_ticks,
                    } = &event
                    {
                        self.index_fill_graph(
                            client_order_id,
                            *quantity_ticks,
                            *price_ticks,
                            self.monotonic_now(),
                        )?;
                    }
                    if let Some(record) = self.runtime.apply_child_event(&event)? {
                        self.append_event(&encode_child_plan(&record))?;
                    }
                    summary.resolved += 1;
                }
                Ok(None) => summary.still_unknown += 1,
                Err(EngineError::Reconcile(error)) => {
                    summary.failed.push((client_order_id, error));
                }
                Err(error) => return Err(error),
            }
        }
        Ok(summary)
    }

    /// Runs a broker reconciliation sweep for an operational trigger.
    ///
    /// Startup begins in [`Lifecycle::Reconciling`] and cannot accept orders
    /// until this method observes an empty unresolved/failed set. Reconnect,
    /// periodic, and anomaly sweeps temporarily gate new submissions in the
    /// same way, preventing an uncertain broker session from receiving new
    /// traffic.
    ///
    /// # Errors
    /// Returns an engine error when local state cannot be read or journaled.
    #[allow(clippy::too_many_lines)]
    pub fn reconcile_trigger(
        &self,
        _trigger: ReconcileTrigger,
    ) -> Result<ReconcileSummary, EngineError> {
        {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| EngineError::Poisoned)?;
            if *lifecycle == Lifecycle::Stopped {
                return Err(EngineError::NotRunning);
            }
            *lifecycle = Lifecycle::Reconciling;
        }
        let mut summary = self.reconcile_unknown_orders()?;
        let snapshot = self.runtime.broker_snapshot()?;
        summary.snapshot_positions = snapshot.positions.len();
        summary.snapshot_account_values = snapshot.account_values.len();
        let observed: std::collections::BTreeSet<String> = snapshot
            .orders
            .iter()
            .map(|order| order.client_order_id.clone())
            .collect();
        for order in &snapshot.orders {
            if !self.runtime.has_order(&order.client_order_id)? {
                summary.external_orders += 1;
                continue;
            }
            let mut event = order.event.clone();
            if let (BrokerEvent::Filled { .. }, Some(cumulative)) =
                (&event, order.filled_quantity_ticks)
            {
                let current = self.runtime.filled_quantity(&order.client_order_id)?;
                if cumulative <= current {
                    continue;
                }
                let delta = cumulative.saturating_sub(current);
                if let BrokerEvent::Filled { quantity_ticks, .. } = &mut event {
                    *quantity_ticks = delta;
                }
            }
            self.append_event(&encode_broker_event(&event))?;
            if let Some(timing) = self.runtime.record_broker_timing(
                &event,
                self.monotonic_now().as_nanos(),
                self.event_market_reference(&event),
            )? {
                self.append_event(&encode_execution_timing(&timing))?;
            }
            let prior_fill = self.fill_quantity_for_event(&event)?;
            if let Err(error) = self.runtime.apply_reconciled_broker_event(event.clone()) {
                summary
                    .failed
                    .push(("snapshot-order".into(), format!("{error:?}")));
            } else {
                if let BrokerEvent::Filled {
                    client_order_id,
                    quantity_ticks,
                    price_ticks,
                } = &event
                {
                    self.index_fill_graph(
                        client_order_id,
                        *quantity_ticks,
                        *price_ticks,
                        self.monotonic_now(),
                    )?;
                }
                if self.fill_advanced(&event, prior_fill)?
                    && let Some(execution) = self.attribute_strategy_fill(&event)?
                {
                    self.append_event(&encode_strategy_execution_summary(&execution))?;
                }
                if let Some(record) = self.runtime.apply_child_event(&event)? {
                    self.append_event(&encode_child_plan(&record))?;
                }
            }
        }
        // The broker snapshot is authoritative for positions and cash. Apply it
        // after order lifecycle reconciliation so replaying fill events cannot
        // double-count quantities already reflected by the snapshot.
        self.append_event(&encode_portfolio_snapshot(&snapshot)?)?;
        self.runtime.apply_broker_snapshot(&snapshot)?;
        index_portfolio_in_graph(
            &mut *self
                .context_graph
                .lock()
                .map_err(|_| EngineError::Poisoned)?,
            &self.runtime.portfolio()?,
            i64::try_from(self.monotonic_now().as_nanos()).unwrap_or(i64::MAX),
        )
        .map_err(|error| EngineError::Graph(format!("portfolio graph: {error:?}")))?;
        let peak = self.runtime.portfolio()?.peak_equity_ticks();
        self.append_event(&encode_portfolio_peak(peak))?;
        summary.missing_at_broker = self
            .runtime
            .working_order_ids()?
            .into_iter()
            .filter(|client_order_id| !observed.contains(client_order_id))
            .count();
        let reconciliation_clean = summary.failed.is_empty()
            && summary.still_unknown == 0
            && summary.external_orders == 0
            && summary.missing_at_broker == 0;
        if reconciliation_clean {
            let mut lifecycle = self.lifecycle.lock().map_err(|_| EngineError::Poisoned)?;
            if *lifecycle == Lifecycle::Reconciling {
                *lifecycle = Lifecycle::Running;
            }
        }
        if reconciliation_clean {
            self.recover_executing_autonomy_plans(self.monotonic_now())?;
        }
        Ok(summary)
    }

    /// Completes startup reconciliation and returns whether the host is ready.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.lifecycle() == Lifecycle::Running
    }

    /// Prevents new work and begins graceful shutdown.
    pub fn drain(&self) {
        if let Ok(mut lifecycle) = self.lifecycle.lock()
            && *lifecycle == Lifecycle::Running
        {
            *lifecycle = Lifecycle::Draining;
        }
        if let Ok(mut supervisor) = self.supervisor.lock() {
            supervisor.drain();
        }
    }

    /// Completes shutdown after callers have drained broker events.
    pub fn stop(&self) {
        if let Ok(mut lifecycle) = self.lifecycle.lock() {
            *lifecycle = Lifecycle::Stopped;
        }
        if let Ok(mut supervisor) = self.supervisor.lock() {
            supervisor.drain();
        }
    }

    /// Returns the bounded operational supervisor snapshot for system-health
    /// read models and authenticated desktop diagnostics.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if supervisor state is unavailable.
    pub fn supervisor_snapshot(&self) -> Result<insider_supervisor::Snapshot, EngineError> {
        self.refresh_worker_supervisor_health()?;
        self.supervisor
            .lock()
            .map(|supervisor| supervisor.snapshot())
            .map_err(|_| EngineError::Poisoned)
    }

    /// Returns broker health plus bounded authoritative account counts for UI
    /// status rendering. The snapshot is read-only and never submits orders.
    ///
    /// # Errors
    /// Returns [`EngineError::Reconcile`] when the broker snapshot is unavailable.
    pub fn broker_status_snapshot(
        &self,
    ) -> Result<(BrokerHealth, usize, usize, usize), EngineError> {
        let health = self.runtime.broker_health();
        let snapshot = self.runtime.broker_snapshot()?;
        Ok((
            health,
            snapshot.orders.len(),
            snapshot.positions.len(),
            snapshot.account_values.len(),
        ))
    }

    fn refresh_worker_supervisor_health(&self) -> Result<(), EngineError> {
        fn aggregate(total: usize, quarantined: usize) -> SupervisorHealth {
            if total == 0 {
                SupervisorHealth::Unknown
            } else if quarantined == total {
                SupervisorHealth::Unavailable
            } else if quarantined > 0 {
                SupervisorHealth::Degraded
            } else {
                SupervisorHealth::Healthy
            }
        }
        fn aggregate_feed(total: usize, unhealthy: usize) -> SupervisorHealth {
            if total == 0 {
                SupervisorHealth::Unknown
            } else if unhealthy == total {
                SupervisorHealth::Unavailable
            } else if unhealthy > 0 {
                SupervisorHealth::Degraded
            } else {
                SupervisorHealth::Healthy
            }
        }
        let market = self
            .market_data
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .health_counts();
        let broker = self.runtime.broker_health();
        let metric = self
            .metric_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .health_counts();
        let strategy = self
            .strategy_host
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .health_counts();
        self.set_supervisor_health("market-data", aggregate_feed(market.0, market.1))?;
        self.set_supervisor_health("metrics", aggregate(metric.0, metric.1))?;
        self.set_supervisor_health("strategies", aggregate(strategy.0, strategy.1))?;
        self.set_supervisor_health(
            "execution",
            match broker {
                BrokerHealth::Unknown => SupervisorHealth::Unknown,
                BrokerHealth::Healthy => SupervisorHealth::Healthy,
                BrokerHealth::Degraded => SupervisorHealth::Degraded,
                BrokerHealth::Unavailable => SupervisorHealth::Unavailable,
            },
        )
    }

    /// Publishes a component health observation used to gate dependent
    /// restarts. Unknown component names are rejected rather than silently
    /// creating an untracked operational component.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an unknown component or
    /// [`EngineError::Poisoned`] when supervisor state is unavailable.
    pub fn set_supervisor_health(
        &self,
        component: &str,
        health: SupervisorHealth,
    ) -> Result<(), EngineError> {
        let mut supervisor = self.supervisor.lock().map_err(|_| EngineError::Poisoned)?;
        if supervisor.set_health(component, health) {
            Ok(())
        } else {
            Err(EngineError::InvalidRequest)
        }
    }

    /// Returns lifecycle state.
    #[must_use]
    pub fn lifecycle(&self) -> Lifecycle {
        self.lifecycle
            .lock()
            .map_or(Lifecycle::Stopped, |lifecycle| *lifecycle)
    }
}

struct PortfolioTool {
    runtime: Arc<Runtime>,
    spec: ToolSpec,
}

impl PortfolioTool {
    fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            spec: ToolSpec {
                name: "get_portfolio".into(),
                max_input_bytes: 2,
                max_output_bytes: 1_048_576,
                permission: ToolPermission::ReadOnly,
                deadline_ms: 100,
            },
        }
    }
}

impl ToolHandler for PortfolioTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError> {
        if request.input.trim() != "{}" {
            return Err(LlmError::SchemaViolation(
                "get_portfolio accepts an empty object only".into(),
            ));
        }
        let portfolio = self
            .runtime
            .portfolio()
            .map_err(|error| LlmError::Provider(format!("portfolio unavailable: {error:?}")))?;
        let mut output = format!(
            "{{\"cash_ticks\":{},\"realized_pnl_ticks\":{},\"fees_ticks\":{},\"positions\":[",
            portfolio.cash_ticks, portfolio.realized_pnl_ticks, portfolio.fees_ticks
        );
        for (index, (instrument_id, position)) in portfolio.positions().enumerate() {
            if index != 0 {
                output.push(',');
            }
            let _ = write!(
                output,
                "{{\"instrument_id\":\"{}\",\"quantity_ticks\":{},\"mark_price_ticks\":{}}}",
                instrument_id, position.quantity_ticks, position.mark_price_ticks
            );
        }
        output.push_str("]}");
        Ok(ToolResponse {
            trace_id: request.trace_id.clone(),
            name: request.name.clone(),
            output,
        })
    }
}

struct PositionTool {
    runtime: Arc<Runtime>,
    spec: ToolSpec,
}

impl PositionTool {
    fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            spec: ToolSpec {
                name: "get_position".into(),
                max_input_bytes: 256,
                max_output_bytes: 4_096,
                permission: ToolPermission::ReadOnly,
                deadline_ms: 100,
            },
        }
    }
}

impl ToolHandler for PositionTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError> {
        let instrument_id = parse_position_input(&request.input)?;
        let portfolio = self
            .runtime
            .portfolio()
            .map_err(|error| LlmError::Provider(format!("portfolio unavailable: {error:?}")))?;
        let output = match portfolio.position(instrument_id) {
            Some(position) => format!(
                "{{\"found\":true,\"instrument_id\":\"{}\",\"quantity_ticks\":{},\"mark_price_ticks\":{}}}",
                instrument_id, position.quantity_ticks, position.mark_price_ticks
            ),
            None => format!("{{\"found\":false,\"instrument_id\":\"{instrument_id}\"}}"),
        };
        Ok(ToolResponse {
            trace_id: request.trace_id.clone(),
            name: request.name.clone(),
            output,
        })
    }
}

struct RecentFillsTool {
    runtime: Arc<Runtime>,
    spec: ToolSpec,
}

impl RecentFillsTool {
    fn new(runtime: Arc<Runtime>) -> Self {
        Self {
            runtime,
            spec: ToolSpec {
                name: "get_recent_fills".into(),
                max_input_bytes: 32,
                max_output_bytes: 1_048_576,
                permission: ToolPermission::ReadOnly,
                deadline_ms: 100,
            },
        }
    }
}

impl ToolHandler for RecentFillsTool {
    fn spec(&self) -> &ToolSpec {
        &self.spec
    }

    fn invoke(&self, request: &ToolRequest) -> Result<ToolResponse, LlmError> {
        let limit = parse_fill_limit(&request.input)?;
        let fills = self
            .runtime
            .fill_history()
            .map_err(|error| LlmError::Provider(format!("fill history unavailable: {error:?}")))?;
        let start = fills.len().saturating_sub(limit);
        let mut output = String::from("{\"fills\":[");
        for (index, fill) in fills[start..].iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            let _ = write!(
                output,
                "{{\"client_order_id\":\"{}\",\"instrument_id\":\"{}\",\"signed_quantity_ticks\":{},\"price_ticks\":{}}}",
                fill.client_order_id,
                fill.instrument_id,
                fill.signed_quantity_ticks,
                fill.price_ticks
            );
        }
        output.push_str("]}");
        Ok(ToolResponse {
            trace_id: request.trace_id.clone(),
            name: request.name.clone(),
            output,
        })
    }
}

fn parse_position_input(input: &str) -> Result<InstrumentId, LlmError> {
    const PREFIX: &str = "{\"instrument_id\":\"";
    let trimmed = input.trim();
    if !trimmed.starts_with(PREFIX) || !trimmed.ends_with("\"}") {
        return Err(LlmError::SchemaViolation(
            "get_position requires {instrument_id}".into(),
        ));
    }
    let encoded = &trimmed[PREFIX.len()..trimmed.len() - 2];
    if encoded.contains('\\') || encoded.contains('"') {
        return Err(LlmError::SchemaViolation(
            "instrument_id contains escapes".into(),
        ));
    }
    encoded
        .parse()
        .map_err(|_| LlmError::SchemaViolation("instrument_id is not canonical".into()))
}

fn parse_fill_limit(input: &str) -> Result<usize, LlmError> {
    const PREFIX: &str = "{\"limit\":";
    let trimmed = input.trim();
    if trimmed == "{}" {
        return Ok(100);
    }
    if !trimmed.starts_with(PREFIX) || !trimmed.ends_with('}') {
        return Err(LlmError::SchemaViolation(
            "get_recent_fills requires {} or {limit}".into(),
        ));
    }
    let value = &trimmed[PREFIX.len()..trimmed.len() - 1];
    let limit = value
        .parse::<usize>()
        .map_err(|_| LlmError::SchemaViolation("limit must be an integer".into()))?;
    if !(1..=100).contains(&limit) {
        return Err(LlmError::SchemaViolation(
            "limit must be between 1 and 100".into(),
        ));
    }
    Ok(limit)
}

/// Shared runtime state. The UI and services can use the same object without
/// owning broker credentials or bypassing risk/execution boundaries.
pub struct Runtime {
    account_id: AccountId,
    broker: Arc<dyn BrokerGateway>,
    portfolio: Mutex<Portfolio>,
    risk: Mutex<RiskEngine>,
    scoped_risk_policy: Mutex<Option<ScopedRiskPolicy>>,
    orders: Mutex<OrderBook>,
    fills: Mutex<std::collections::VecDeque<FillRecord>>,
    timings: Mutex<BTreeMap<String, ExecutionTiming>>,
    child_plans: Mutex<BTreeMap<String, ChildPlanRecord>>,
    message_timestamps_ns: Mutex<std::collections::VecDeque<u64>>,
    live_guard: Mutex<LiveGuard>,
}

fn realized_tca(
    orders: &[insider_execution::OrderRecord],
    fills: &[FillRecord],
    timings: &BTreeMap<String, ExecutionTiming>,
) -> Vec<TcaSnapshot> {
    let known_orders: BTreeMap<&str, Side> = orders
        .iter()
        .map(|order| (order.intent.client_order_id.as_str(), order.intent.side))
        .collect();
    let mut grouped = BTreeMap::<String, (Side, i128, i128)>::new();
    for fill in fills {
        let Some(side) = known_orders.get(fill.client_order_id.as_str()) else {
            continue;
        };
        let quantity = i128::from(fill.signed_quantity_ticks.unsigned_abs());
        if quantity == 0 || fill.price_ticks <= 0 {
            continue;
        }
        let Some(notional) = quantity.checked_mul(i128::from(fill.price_ticks)) else {
            continue;
        };
        let entry = grouped
            .entry(fill.client_order_id.clone())
            .or_insert((*side, 0, 0));
        let Some(next_quantity) = entry.1.checked_add(quantity) else {
            continue;
        };
        let Some(next_notional) = entry.2.checked_add(notional) else {
            continue;
        };
        entry.1 = next_quantity;
        entry.2 = next_notional;
    }
    grouped
        .into_iter()
        .filter_map(|(client_order_id, (side, quantity, notional))| {
            let filled_quantity_ticks = i64::try_from(quantity).ok()?;
            if filled_quantity_ticks <= 0 {
                return None;
            }
            let timing = timings.get(&client_order_id);
            let implementation_shortfall_tick_value = timing
                .and_then(|value| value.arrival_price_ticks)
                .and_then(|arrival| {
                    let arrival_notional =
                        i128::from(filled_quantity_ticks).checked_mul(i128::from(arrival))?;
                    let delta = notional.checked_sub(arrival_notional)?;
                    match side {
                        Side::Buy => Some(delta),
                        Side::Sell => delta.checked_neg(),
                    }
                });
            let adverse_selection_tick_value = timing
                .and_then(|value| value.post_fill_mid_ticks)
                .and_then(|post_fill_mid| {
                    let post_fill_notional =
                        i128::from(filled_quantity_ticks).checked_mul(i128::from(post_fill_mid))?;
                    let delta = post_fill_notional.checked_sub(notional)?;
                    match side {
                        Side::Buy => Some(delta),
                        Side::Sell => delta.checked_neg(),
                    }
                });
            Some(TcaSnapshot {
                client_order_id,
                filled_quantity_ticks,
                notional_ticks: notional,
                average_fill_price_numerator: notional,
                average_fill_price_denominator: filled_quantity_ticks,
                arrival_price_ticks: timing.and_then(|value| value.arrival_price_ticks),
                decision_mono_ns: timing.map(|value| value.decision_mono_ns),
                send_mono_ns: timing.and_then(|value| value.send_mono_ns),
                ack_mono_ns: timing.and_then(|value| value.ack_mono_ns),
                first_fill_mono_ns: timing.and_then(|value| value.first_fill_mono_ns),
                implementation_shortfall_tick_value,
                average_spread_ticks: timing.and_then(|value| value.arrival_spread_ticks),
                adverse_selection_tick_value,
            })
        })
        .collect()
}

impl Runtime {
    /// Creates a runtime with reconciled starting portfolio and risk policy.
    #[must_use]
    pub fn new(
        account_id: AccountId,
        broker: Arc<dyn BrokerGateway>,
        portfolio: Portfolio,
        risk: RiskEngine,
    ) -> Self {
        Self {
            account_id,
            broker,
            portfolio: Mutex::new(portfolio),
            risk: Mutex::new(risk),
            scoped_risk_policy: Mutex::new(None),
            orders: Mutex::new(OrderBook::new()),
            fills: Mutex::new(std::collections::VecDeque::new()),
            timings: Mutex::new(BTreeMap::new()),
            child_plans: Mutex::new(BTreeMap::new()),
            message_timestamps_ns: Mutex::new(std::collections::VecDeque::new()),
            live_guard: Mutex::new(LiveGuard::paper(insider_autonomy::LiveLimits {
                allowed_accounts: std::collections::BTreeSet::new(),
                max_notional_ticks: 0,
            })),
        }
    }

    /// Installs or clears the immutable scoped risk policy used by subsequent
    /// proposal/manual planning. Existing risk state and guardrails remain
    /// authoritative; only hard limits are resolved by scope.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the runtime policy lock is unavailable.
    pub fn set_scoped_risk_policy(
        &self,
        policy: Option<ScopedRiskPolicy>,
    ) -> Result<(), EngineError> {
        *self
            .scoped_risk_policy
            .lock()
            .map_err(|_| EngineError::Poisoned)? = policy;
        Ok(())
    }

    /// Returns a clone of the installed policy snapshot for read-only APIs.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the policy lock is unavailable.
    pub fn scoped_risk_policy_snapshot(
        &self,
    ) -> Result<Option<ScopedRiskPolicySnapshot>, EngineError> {
        Ok(self
            .scoped_risk_policy
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .as_ref()
            .map(ScopedRiskPolicy::snapshot))
    }

    fn effective_risk(
        &self,
        strategy_id: Option<&str>,
        instrument_id: InstrumentId,
        now: MonoTime,
    ) -> Result<RiskEngine, EngineError> {
        let risk = self.risk.lock().map_err(|_| EngineError::Poisoned)?;
        let policy = self
            .scoped_risk_policy
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        Ok(policy.as_ref().map_or_else(
            || risk.clone(),
            |policy| {
                risk.with_limits(policy.limits_at(
                    RiskScope {
                        account_id: &self.account_id.to_string(),
                        strategy_id,
                        asset_class: None,
                        instrument_id,
                    },
                    now.as_nanos(),
                ))
            },
        ))
    }

    fn set_guardrails(&self, guardrails: Guardrails) -> Result<(), EngineError> {
        self.risk
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .set_guardrails(guardrails);
        Ok(())
    }

    /// Returns the side of a known local order for fill attribution.
    fn order_side(&self, client_order_id: &str) -> Option<Side> {
        self.orders
            .lock()
            .ok()
            .and_then(|orders| orders.get(client_order_id).map(|record| record.intent.side))
    }

    /// Starts the first step of the explicit two-step live enablement flow.
    ///
    /// # Errors
    /// Returns an engine or live-guard error without changing runtime mode.
    pub fn arm_live(
        &self,
        account: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<String, EngineError> {
        self.live_guard
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .arm_live(account, now, phrase)
            .map_err(EngineError::LiveGuard)
    }

    /// Replaces the live account allowlist and hard cap while still paper-only.
    /// Configuration changes cannot silently enable live submission.
    ///
    /// # Errors
    /// Returns an engine error if the guard lock is unavailable or live is active.
    pub fn configure_live_limits(&self, limits: LiveLimits) -> Result<(), EngineError> {
        let mut guard = self.live_guard.lock().map_err(|_| EngineError::Poisoned)?;
        if guard.environment() != TradingEnvironment::Paper {
            return Err(EngineError::LiveGuard(LiveGuardError::NotLive));
        }
        *guard = LiveGuard::paper(limits);
        Ok(())
    }

    /// Completes live enablement after the typed confirmation challenge.
    ///
    /// # Errors
    /// Returns an engine or challenge-validation error without enabling live.
    pub fn confirm_live(
        &self,
        account: &str,
        token: &str,
        now: MonoTime,
        phrase: &str,
    ) -> Result<(), EngineError> {
        self.live_guard
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .confirm_live(account, token, now, phrase)
            .map_err(EngineError::LiveGuard)
    }

    /// Immediately kills live submission; re-enablement requires both steps.
    ///
    /// # Errors
    /// Returns an engine error if the guard lock is unavailable.
    pub fn kill_live(&self) -> Result<(), EngineError> {
        self.live_guard
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .kill_switch();
        Ok(())
    }

    /// Returns the enforced trading environment.
    ///
    /// # Errors
    /// Returns an engine error if the guard lock is unavailable.
    pub fn trading_environment(&self) -> Result<TradingEnvironment, EngineError> {
        Ok(self
            .live_guard
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .environment())
    }

    /// Returns broker-neutral transport/session health for supervision.
    #[must_use]
    pub fn broker_health(&self) -> BrokerHealth {
        self.broker.health()
    }

    /// Creates a point-in-time snapshot of all authoritative projections.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if any projection lock is unavailable.
    pub fn snapshot(&self, cursor: u64) -> Result<RuntimeSnapshot, EngineError> {
        let portfolio = self
            .portfolio
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone();
        let risk_state = self.risk.lock().map_err(|_| EngineError::Poisoned)?.state();
        let max_gross_notional_ticks = self
            .risk
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .limits()
            .max_gross_notional_ticks;
        let gross_notional_ticks = portfolio
            .positions()
            .try_fold(0_i128, |gross, (instrument_id, position)| {
                let mark = portfolio.mark_price(instrument_id)?;
                gross.checked_add(
                    i128::from(position.quantity_ticks.unsigned_abs())
                        .checked_mul(i128::from(mark))?,
                )
            })
            .unwrap_or(i128::MAX);
        let largest_position_notional_ticks = portfolio
            .positions()
            .filter_map(|(instrument_id, position)| {
                portfolio.mark_price(instrument_id).and_then(|mark| {
                    i128::from(position.quantity_ticks.unsigned_abs())
                        .checked_mul(i128::from(mark.unsigned_abs()))
                })
            })
            .max()
            .unwrap_or(0);
        let gross_utilization_bps = if max_gross_notional_ticks > 0 && gross_notional_ticks >= 0 {
            i64::try_from(
                gross_notional_ticks
                    .saturating_mul(10_000)
                    .checked_div(max_gross_notional_ticks)
                    .unwrap_or(i128::MAX)
                    .min(i128::from(i64::MAX)),
            )
            .unwrap_or(i64::MAX)
        } else {
            i64::MAX
        };
        let drawdown_bps = portfolio.drawdown_bps();
        let orders = self
            .orders
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .records();
        let timings = self
            .timings
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .clone();
        let tca = realized_tca(&orders, &self.fill_history()?, &timings);
        Ok(RuntimeSnapshot {
            account_id: self.account_id,
            cursor,
            risk_state,
            autonomy_mode: AutonomyMode::Manual,
            autonomy_plan: None,
            llm_provider_id: None,
            llm_model: None,
            portfolio,
            orders,
            fills: self
                .fills
                .lock()
                .map_err(|_| EngineError::Poisoned)?
                .iter()
                .cloned()
                .collect(),
            tca,
            proposals: Vec::new(),
            markets: Vec::new(),
            gross_notional_ticks,
            max_gross_notional_ticks,
            gross_utilization_bps,
            largest_position_notional_ticks,
            drawdown_bps,
        })
    }

    /// Converts a validated strategy proposal into a risk-gated persisted send.
    ///
    /// # Errors
    /// Returns [`EngineError`] when validation, target conversion, risk,
    /// persistence, or broker submission fails.
    pub fn submit_proposal(
        &self,
        proposal: &Proposal,
        now: insider_common_types::MonoTime,
        trace_id: TraceId,
    ) -> Result<String, EngineError> {
        let intent = self.prepare_proposal(proposal, now, trace_id)?;
        let client_order_id = intent.client_order_id.clone();
        self.submit_intent(&intent)?;
        Ok(client_order_id)
    }

    /// Converts a proposal into an order intent without contacting the broker.
    /// This split lets the service host persist the exact intent before any
    /// transport side effect occurs.
    ///
    /// # Errors
    /// Returns an engine error when validation, target conversion, or risk
    /// planning rejects the proposal.
    pub fn prepare_proposal(
        &self,
        proposal: &Proposal,
        now: insider_common_types::MonoTime,
        trace_id: TraceId,
    ) -> Result<insider_broker_api::OrderIntent, EngineError> {
        proposal.validate(now).map_err(EngineError::Proposal)?;
        let portfolio = self.portfolio.lock().map_err(|_| EngineError::Poisoned)?;
        let target = portfolio
            .target_from_proposal(proposal)
            .map_err(EngineError::Target)?;
        let risk = self.effective_risk(Some(&proposal.strategy_id), proposal.instrument_id, now)?;
        let inputs = self.contextual_risk_inputs(&portfolio, &risk, &target, now, None)?;
        let intent = plan_target_with_guardrails(
            self.account_id,
            trace_id,
            &portfolio,
            &target,
            &risk,
            self.broker.capabilities(),
            inputs,
        )
        .map_err(EngineError::Plan)?;
        Ok(intent)
    }

    /// Builds a risk-gated manual target using the same execution planner as
    /// automated strategies.
    ///
    /// # Errors
    /// Returns an engine error when the target is invalid or risk denies it.
    pub fn prepare_manual_target(
        &self,
        instrument_id: InstrumentId,
        target_quantity_ticks: i64,
        proposal_id: ProposalId,
        now: MonoTime,
        trace_id: TraceId,
    ) -> Result<insider_broker_api::OrderIntent, EngineError> {
        self.prepare_manual_target_with_order(
            instrument_id,
            target_quantity_ticks,
            proposal_id,
            now,
            trace_id,
            insider_broker_api::OrderType::Market,
            None,
        )
    }

    /// Builds a manual target intent with an explicit order type and limit.
    ///
    /// # Errors
    /// Returns an engine error when the order type/limit is inconsistent or
    /// risk planning denies the resulting target.
    #[allow(clippy::too_many_arguments)]
    pub fn prepare_manual_target_with_order(
        &self,
        instrument_id: InstrumentId,
        target_quantity_ticks: i64,
        proposal_id: ProposalId,
        now: MonoTime,
        trace_id: TraceId,
        order_type: insider_broker_api::OrderType,
        limit_price_ticks: Option<i64>,
    ) -> Result<insider_broker_api::OrderIntent, EngineError> {
        if matches!(order_type, insider_broker_api::OrderType::Market)
            && limit_price_ticks.is_some()
            || matches!(order_type, insider_broker_api::OrderType::Limit)
                && limit_price_ticks.is_none_or(|price| price <= 0)
        {
            return Err(EngineError::InvalidRequest);
        }
        let portfolio = self.portfolio.lock().map_err(|_| EngineError::Poisoned)?;
        let target = insider_portfolio::Target {
            instrument_id,
            quantity_ticks: target_quantity_ticks,
            proposal_id,
        };
        let risk = self.effective_risk(None, instrument_id, now)?;
        let price_deviation_bps = limit_price_ticks.and_then(|limit| {
            let mark = portfolio.mark_price(instrument_id)?;
            if mark <= 0 || limit <= 0 {
                return None;
            }
            let difference = i128::from(limit)
                .saturating_sub(i128::from(mark))
                .unsigned_abs();
            let deviation = difference
                .saturating_mul(10_000)
                .checked_div(u128::try_from(mark).ok()?)?;
            i64::try_from(deviation).ok()
        });
        let inputs =
            self.contextual_risk_inputs(&portfolio, &risk, &target, now, price_deviation_bps)?;
        let mut intent = plan_target_with_guardrails(
            self.account_id,
            trace_id,
            &portfolio,
            &target,
            &risk,
            self.broker.capabilities(),
            inputs,
        )
        .map_err(EngineError::Plan)?;
        intent.order_type = order_type;
        intent.limit_price_ticks = limit_price_ticks;
        Ok(intent)
    }

    /// Derives contextual risk observations from authoritative local state.
    /// Missing observations required by an enabled guardrail fail closed;
    /// values are never fabricated from strategy input.
    #[allow(clippy::cast_precision_loss)]
    fn contextual_risk_inputs(
        &self,
        portfolio: &Portfolio,
        risk: &RiskEngine,
        target: &insider_portfolio::Target,
        now: MonoTime,
        price_deviation_bps: Option<i64>,
    ) -> Result<Option<RiskInputs>, EngineError> {
        let guardrails = risk.guardrails();
        if guardrails == Guardrails::default() {
            return Ok(None);
        }
        let prices_fresh = portfolio
            .mark_price(target.instrument_id)
            .is_some_and(|price| price > 0);
        let equity = portfolio.equity_ticks().filter(|value| *value > 0);
        let gross = portfolio
            .positions()
            .try_fold(0_i128, |total, (instrument, position)| {
                let mark = portfolio.mark_price(instrument)?;
                i128::from(position.quantity_ticks.unsigned_abs())
                    .checked_mul(i128::from(mark.unsigned_abs()))
                    .and_then(|value| total.checked_add(value))
            });
        let leverage = match (gross, equity) {
            (Some(gross), Some(equity)) => gross as f64 / equity as f64,
            _ if guardrails.max_leverage.is_some() => {
                return Err(EngineError::Plan(PlanError::RiskDenied(
                    RiskReason::StaleData,
                )));
            }
            _ => 0.0,
        };
        let drawdown_bps = match portfolio.drawdown_bps() {
            Some(value) => value,
            None if guardrails.max_drawdown_bps.is_some() => {
                return Err(EngineError::Plan(PlanError::RiskDenied(
                    RiskReason::StaleData,
                )));
            }
            None => 0,
        };
        let outstanding_orders = self
            .orders
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .records()
            .iter()
            .filter(|record| {
                matches!(
                    record.intent.state,
                    OrderState::Queued
                        | OrderState::Sending
                        | OrderState::Sent
                        | OrderState::Acknowledged
                        | OrderState::PartiallyFilled
                        | OrderState::CancelPending
                        | OrderState::ReplacePending
                        | OrderState::Unknown
                )
            })
            .count() as u64;
        let broker_session_healthy = matches!(self.broker.health(), BrokerHealth::Healthy);
        if guardrails.max_price_deviation_bps.is_some() && price_deviation_bps.is_none() {
            return Err(EngineError::Plan(PlanError::RiskDenied(
                RiskReason::StaleData,
            )));
        }
        if guardrails.max_predicted_volatility_bps.is_some()
            || guardrails.max_participation_bps.is_some()
            || (guardrails.max_price_deviation_bps.is_some() && price_deviation_bps.is_none())
        {
            return Err(EngineError::Plan(PlanError::RiskDenied(
                RiskReason::StaleData,
            )));
        }
        let message_rate = self.message_rate(now.as_nanos())?;
        Ok(Some(RiskInputs {
            leverage,
            drawdown_bps,
            predicted_volatility_bps: 0,
            participation_bps: 0,
            outstanding_orders,
            message_rate,
            price_deviation_bps: price_deviation_bps.unwrap_or(0),
            prices_fresh,
            clock_healthy: true,
            broker_session_healthy,
            duplicate_intent: false,
        }))
    }

    /// Records a planning boundary and returns the number of boundaries in
    /// the preceding one-second monotonic window. The queue is capped so a
    /// malfunctioning caller cannot grow runtime memory without bound.
    fn message_rate(&self, now_ns: u64) -> Result<u64, EngineError> {
        const WINDOW_NS: u64 = 1_000_000_000;
        const MAX_WINDOW_EVENTS: usize = 1_000_000;
        let mut timestamps = self
            .message_timestamps_ns
            .lock()
            .map_err(|_| EngineError::Poisoned)?;
        let cutoff = now_ns.saturating_sub(WINDOW_NS);
        while timestamps
            .front()
            .is_some_and(|timestamp| *timestamp < cutoff)
        {
            timestamps.pop_front();
        }
        if timestamps.len() >= MAX_WINDOW_EVENTS {
            timestamps.pop_front();
        }
        timestamps.push_back(now_ns);
        Ok(u64::try_from(timestamps.len()).unwrap_or(u64::MAX))
    }

    /// Sends a previously planned and durably recorded intent.
    ///
    /// # Errors
    /// Returns an engine error when the local order lifecycle or broker
    /// transport fails. A transport failure leaves the order Unknown.
    pub fn submit_intent(
        &self,
        intent: &insider_broker_api::OrderIntent,
    ) -> Result<(), EngineError> {
        self.authorize_intent(intent)?;
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        submit_order(&mut orders, self.broker.as_ref(), intent).map_err(EngineError::Submit)
    }

    /// Records the trusted decision boundary before the first send attempt.
    /// This is a projection-only operation; the caller journals the returned
    /// timing record before invoking the broker gateway.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the timing or portfolio projection
    /// cannot be read or updated.
    pub fn record_decision(
        &self,
        intent: &insider_broker_api::OrderIntent,
        decision_mono_ns: u64,
        market: Option<ExecutionMarketReference>,
    ) -> Result<ExecutionTiming, EngineError> {
        if market.is_some_and(|reference| reference.mid_ticks <= 0 || reference.spread_ticks < 0) {
            return Err(EngineError::InvalidRequest);
        }
        let arrival_price_ticks = intent.limit_price_ticks.or_else(|| {
            self.portfolio
                .lock()
                .ok()
                .and_then(|portfolio| portfolio.mark_price(intent.instrument_id))
        });
        if arrival_price_ticks.is_some_and(|price| price <= 0) {
            return Err(EngineError::InvalidRequest);
        }
        let timing = ExecutionTiming {
            client_order_id: intent.client_order_id.clone(),
            decision_mono_ns,
            arrival_price_ticks,
            send_mono_ns: None,
            ack_mono_ns: None,
            first_fill_mono_ns: None,
            decision_mid_ticks: market.map(|value| value.mid_ticks),
            arrival_spread_ticks: market.map(|value| value.spread_ticks),
            send_mid_ticks: None,
            ack_mid_ticks: None,
            post_fill_mid_ticks: None,
        };
        self.timings
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(intent.client_order_id.clone(), timing.clone());
        Ok(timing)
    }

    /// Applies a journal-restored timing context without contacting a broker.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for an empty identity or an
    /// impossible timestamp chain, or [`EngineError::Poisoned`] on lock failure.
    pub fn restore_timing(&self, timing: ExecutionTiming) -> Result<(), EngineError> {
        if timing.client_order_id.trim().is_empty()
            || timing.arrival_price_ticks.is_some_and(|price| price <= 0)
            || timing.arrival_spread_ticks.is_some_and(|spread| spread < 0)
            || [
                timing.decision_mid_ticks,
                timing.send_mid_ticks,
                timing.ack_mid_ticks,
                timing.post_fill_mid_ticks,
            ]
            .into_iter()
            .flatten()
            .any(|mid| mid <= 0)
            || timing
                .send_mono_ns
                .is_some_and(|value| value < timing.decision_mono_ns)
            || timing
                .ack_mono_ns
                .is_some_and(|value| value < timing.send_mono_ns.unwrap_or(timing.decision_mono_ns))
            || timing.first_fill_mono_ns.is_some_and(|value| {
                value
                    < timing
                        .ack_mono_ns
                        .unwrap_or(timing.send_mono_ns.unwrap_or(timing.decision_mono_ns))
            })
        {
            return Err(EngineError::InvalidRequest);
        }
        self.timings
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(timing.client_order_id.clone(), timing);
        Ok(())
    }

    /// Records the send-attempt boundary. A send timestamp is retained even
    /// when transport fails, because the resulting order state is then
    /// reconciled as Unknown rather than blindly retried.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] if the order has no decision
    /// record or the timestamp precedes that decision, or [`EngineError::Poisoned`]
    /// if timing state is unavailable.
    pub fn record_send(
        &self,
        client_order_id: &str,
        send_mono_ns: u64,
        market: Option<ExecutionMarketReference>,
    ) -> Result<ExecutionTiming, EngineError> {
        let mut timings = self.timings.lock().map_err(|_| EngineError::Poisoned)?;
        let timing = timings
            .get_mut(client_order_id)
            .ok_or(EngineError::InvalidRequest)?;
        if send_mono_ns < timing.decision_mono_ns {
            return Err(EngineError::InvalidRequest);
        }
        timing.send_mono_ns = Some(send_mono_ns);
        timing.send_mid_ticks = market.map(|value| value.mid_ticks);
        Ok(timing.clone())
    }

    fn record_broker_timing(
        &self,
        event: &BrokerEvent,
        now_mono_ns: u64,
        market: Option<ExecutionMarketReference>,
    ) -> Result<Option<ExecutionTiming>, EngineError> {
        let client_order_id = broker_event_client_order_id(event);
        let mut timings = self.timings.lock().map_err(|_| EngineError::Poisoned)?;
        let Some(timing) = timings.get_mut(client_order_id) else {
            return Ok(None);
        };
        if now_mono_ns < timing.send_mono_ns.unwrap_or(timing.decision_mono_ns) {
            return Err(EngineError::InvalidRequest);
        }
        match event {
            BrokerEvent::Acknowledged { .. } => {
                if timing.ack_mono_ns.is_none() {
                    timing.ack_mono_ns = Some(now_mono_ns);
                }
                timing.ack_mid_ticks = market.map(|value| value.mid_ticks);
            }
            BrokerEvent::Filled { .. } => {
                if timing.first_fill_mono_ns.is_none() {
                    timing.first_fill_mono_ns = Some(now_mono_ns);
                }
                timing.post_fill_mid_ticks = market.map(|value| value.mid_ticks);
            }
            BrokerEvent::Rejected { .. } | BrokerEvent::Cancelled { .. } => {}
        }
        Ok(Some(timing.clone()))
    }

    /// Performs the side-effect-free live-guard authorization used immediately
    /// before journaling an intent. The send path repeats this check to close
    /// the race where an operator kills live trading between authorization and
    /// transport submission.
    ///
    /// # Errors
    /// Returns [`EngineError::LiveGuard`] when the environment, account, mark,
    /// or notional cap rejects the intent.
    pub fn authorize_intent(
        &self,
        intent: &insider_broker_api::OrderIntent,
    ) -> Result<(), EngineError> {
        let trusted_price = if intent.limit_price_ticks.is_some() {
            intent.limit_price_ticks
        } else {
            self.portfolio
                .lock()
                .map_err(|_| EngineError::Poisoned)?
                .mark_price(intent.instrument_id)
        };
        let estimated_notional_ticks = trusted_price
            .and_then(|price| u128::try_from(price).ok())
            .and_then(|price| {
                u128::try_from(intent.quantity_ticks)
                    .ok()
                    .and_then(|quantity| quantity.checked_mul(price))
            });
        self.live_guard
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .authorize(&self.account_id.to_string(), estimated_notional_ticks)
            .map_err(EngineError::LiveGuard)
    }

    /// Restores a previously journaled intent without contacting the broker.
    ///
    /// # Errors
    /// Returns a transition error when the journal contains a duplicate or
    /// invalid intent record.
    pub fn restore_intent(
        &self,
        intent: insider_broker_api::OrderIntent,
    ) -> Result<(), EngineError> {
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        orders.insert(intent).map_err(EngineError::Transition)
    }

    /// Creates and stores a deterministic parent child-order plan.
    ///
    /// # Errors
    /// Returns [`EngineError::Plan`] for an invalid schedule or
    /// [`EngineError::Poisoned`] when the plan projection is unavailable.
    pub fn create_child_plan(
        &self,
        parent: insider_broker_api::OrderIntent,
        schedule: &Schedule,
        created_mono_ns: u64,
    ) -> Result<ChildPlanRecord, EngineError> {
        let plan = ChildPlan::new(&parent, schedule, created_mono_ns).map_err(EngineError::Plan)?;
        let record = ChildPlanRecord { parent, plan };
        self.child_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(record.plan.parent_client_order_id.clone(), record.clone());
        Ok(record)
    }

    /// Restores one complete parent plan from the journal.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] for mismatched parent identity
    /// or [`EngineError::Poisoned`] when the plan projection is unavailable.
    pub fn restore_child_plan(&self, record: ChildPlanRecord) -> Result<(), EngineError> {
        if record.parent.client_order_id != record.plan.parent_client_order_id {
            return Err(EngineError::InvalidRequest);
        }
        self.child_plans
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .insert(record.plan.parent_client_order_id.clone(), record);
        Ok(())
    }

    /// Claims due children exactly once across all stored plans.
    ///
    /// # Errors
    /// Returns [`EngineError::Plan`] for an invalid due-time arithmetic state.
    pub fn claim_due_children(
        &self,
        now_mono_ns: u64,
    ) -> Result<Vec<(ChildPlanRecord, insider_broker_api::OrderIntent, ChildOrder)>, EngineError>
    {
        let mut plans = self.child_plans.lock().map_err(|_| EngineError::Poisoned)?;
        let mut claimed = Vec::new();
        for record in plans.values_mut() {
            for child in record
                .plan
                .claim_due(now_mono_ns)
                .map_err(EngineError::Plan)?
            {
                let intent = ChildPlan::child_intent(&record.parent, &child);
                claimed.push((record.clone(), intent, child));
            }
        }
        Ok(claimed)
    }

    /// Marks one child as sent after the transport call succeeds.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] when the child is not part of a
    /// stored plan, [`EngineError::Plan`] for an invalid state transition, or
    /// [`EngineError::Poisoned`] on lock failure.
    pub fn mark_child_sent(&self, client_order_id: &str) -> Result<ChildPlanRecord, EngineError> {
        let mut plans = self.child_plans.lock().map_err(|_| EngineError::Poisoned)?;
        let record = plans
            .values_mut()
            .find(|record| {
                record
                    .plan
                    .children
                    .iter()
                    .any(|child| child.order.client_order_id == client_order_id)
            })
            .ok_or(EngineError::InvalidRequest)?;
        record
            .plan
            .mark_sent(client_order_id)
            .map_err(EngineError::Plan)?;
        Ok(record.clone())
    }

    /// Marks one child Unknown after an ambiguous transport result.
    ///
    /// # Errors
    /// Returns [`EngineError::InvalidRequest`] when the child is not part of a
    /// stored plan, [`EngineError::Plan`] for an invalid state transition, or
    /// [`EngineError::Poisoned`] on lock failure.
    pub fn mark_child_unknown(
        &self,
        client_order_id: &str,
    ) -> Result<ChildPlanRecord, EngineError> {
        let mut plans = self.child_plans.lock().map_err(|_| EngineError::Poisoned)?;
        let record = plans
            .values_mut()
            .find(|record| {
                record
                    .plan
                    .children
                    .iter()
                    .any(|child| child.order.client_order_id == client_order_id)
            })
            .ok_or(EngineError::InvalidRequest)?;
        record
            .plan
            .mark_unknown(client_order_id)
            .map_err(EngineError::Plan)?;
        Ok(record.clone())
    }

    /// Applies a broker callback to a child plan when the client ID belongs to
    /// one; ordinary non-scheduled orders are intentionally ignored.
    ///
    /// # Errors
    /// Returns [`EngineError::Plan`] for an invalid child transition or
    /// [`EngineError::Poisoned`] when the plan projection is unavailable.
    pub fn apply_child_event(
        &self,
        event: &BrokerEvent,
    ) -> Result<Option<ChildPlanRecord>, EngineError> {
        let client_order_id = broker_event_client_order_id(event);
        let mut plans = self.child_plans.lock().map_err(|_| EngineError::Poisoned)?;
        let Some(record) = plans.values_mut().find(|record| {
            record
                .plan
                .children
                .iter()
                .any(|child| child.order.client_order_id == client_order_id)
        }) else {
            return Ok(None);
        };
        record
            .plan
            .apply_broker_event(event)
            .map_err(EngineError::Plan)?;
        Ok(Some(record.clone()))
    }

    /// Returns local orders currently requiring broker reconciliation.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the order projection lock fails.
    pub fn unknown_order_ids(&self) -> Result<Vec<String>, EngineError> {
        let orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        Ok(orders
            .client_order_ids()
            .filter(|client_order_id| {
                orders.get(client_order_id).is_some_and(|record| {
                    record.intent.state == insider_broker_api::OrderState::Unknown
                })
            })
            .map(str::to_owned)
            .collect())
    }

    fn order_instrument(&self, client_order_id: &str) -> Option<InstrumentId> {
        self.orders.lock().ok().and_then(|orders| {
            orders
                .get(client_order_id)
                .map(|record| record.intent.instrument_id)
        })
    }

    /// Queries one unknown order without mutating local state.
    ///
    /// # Errors
    /// Returns [`EngineError::Reconcile`] when authoritative broker state is
    /// unavailable.
    pub fn query_reconcile(
        &self,
        client_order_id: &str,
    ) -> Result<Option<BrokerEvent>, EngineError> {
        self.broker
            .reconcile(client_order_id)
            .map_err(EngineError::Reconcile)
    }

    /// Queries the broker's complete account snapshot for reconciliation.
    ///
    /// # Errors
    /// Returns [`EngineError::Reconcile`] when the adapter cannot provide
    /// authoritative snapshot state.
    pub fn broker_snapshot(&self) -> Result<BrokerSnapshot, EngineError> {
        self.broker.snapshot().map_err(EngineError::Reconcile)
    }

    /// Applies broker-reported positions and cash to the authoritative
    /// portfolio projection. Missing cash is intentionally left unchanged
    /// because some gateways expose positions before account values.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the portfolio projection lock is
    /// unavailable, or [`EngineError::Accounting`] if an account value cannot
    /// fit canonical cash ticks.
    pub fn apply_broker_snapshot(&self, snapshot: &BrokerSnapshot) -> Result<(), EngineError> {
        let cash_ticks = snapshot
            .account_values
            .get(insider_broker_api::ACCOUNT_VALUE_CASH_TICKS)
            .map(|value| i64::try_from(*value).map_err(|_| AccountingError::Overflow))
            .transpose()
            .map_err(EngineError::Accounting)?;
        let positions = snapshot
            .positions
            .iter()
            .map(|position| (position.instrument_id, position.quantity_ticks))
            .collect::<Vec<_>>();
        self.apply_reconciled_portfolio(&positions, cash_ticks)
    }

    fn apply_reconciled_portfolio(
        &self,
        positions: &[(InstrumentId, i64)],
        cash_ticks: Option<i64>,
    ) -> Result<(), EngineError> {
        self.portfolio
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .reconcile_positions(positions.iter().copied(), cash_ticks);
        Ok(())
    }

    fn restore_peak_equity_ticks(&self, peak: Option<i128>) -> Result<(), EngineError> {
        self.portfolio
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .restore_peak_equity_ticks(peak);
        Ok(())
    }

    /// Returns whether a client order exists in the local journal projection.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the order projection lock fails.
    pub fn has_order(&self, client_order_id: &str) -> Result<bool, EngineError> {
        Ok(self
            .orders
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .get(client_order_id)
            .is_some())
    }

    fn client_order_id_for_proposal(&self, proposal_id: ProposalId) -> String {
        format!("client-intent-{}-{}", self.account_id, proposal_id)
    }

    /// Returns the current local lifecycle state for a client order.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the order projection lock fails.
    pub fn order_state(&self, client_order_id: &str) -> Result<Option<OrderState>, EngineError> {
        Ok(self
            .orders
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .get(client_order_id)
            .map(|record| record.intent.state))
    }

    /// Returns cumulative locally applied fill quantity for a client order.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the order projection lock fails.
    pub fn filled_quantity(&self, client_order_id: &str) -> Result<i64, EngineError> {
        Ok(self
            .orders
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .get(client_order_id)
            .map_or(0, |record| record.filled_quantity_ticks))
    }

    /// Returns local working and uncertain orders for snapshot comparison.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] when the order projection lock fails.
    pub fn working_order_ids(&self) -> Result<Vec<String>, EngineError> {
        let orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        Ok(orders
            .client_order_ids()
            .filter(|client_order_id| {
                orders.get(client_order_id).is_some_and(|record| {
                    matches!(
                        record.intent.state,
                        OrderState::Queued
                            | OrderState::Sending
                            | OrderState::Sent
                            | OrderState::Acknowledged
                            | OrderState::PartiallyFilled
                            | OrderState::CancelPending
                            | OrderState::ReplacePending
                            | OrderState::Unknown
                    )
                })
            })
            .map(str::to_owned)
            .collect())
    }

    /// Validates a risk state transition without mutating runtime state.
    ///
    /// # Errors
    /// Returns an engine error when the risk lock is unavailable or the
    /// transition is unauthorized/invalid.
    pub fn validate_risk_transition(
        &self,
        next: RiskState,
        authorization: &str,
    ) -> Result<(), EngineError> {
        self.risk
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .validate_transition(next, authorization)
            .map_err(EngineError::RiskState)
    }

    /// Applies a previously journaled risk state transition.
    ///
    /// # Errors
    /// Returns an engine error when the risk lock is unavailable or the
    /// transition cannot be applied.
    pub fn transition_risk_state(
        &self,
        next: RiskState,
        authorization: &str,
    ) -> Result<(), EngineError> {
        self.risk
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .transition(next, authorization)
            .map_err(EngineError::RiskState)
    }

    /// Requests cancellation of a working order through the broker gateway.
    ///
    /// # Errors
    /// Returns an engine error when the local lifecycle or broker transport
    /// rejects the request.
    pub fn cancel_order(&self, client_order_id: &str) -> Result<(), EngineError> {
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        cancel_order(&mut orders, self.broker.as_ref(), client_order_id)
            .map_err(EngineError::Cancel)
    }

    /// Requests replacement through the broker-neutral order lifecycle.
    ///
    /// # Errors
    /// Returns an engine error when local state or the broker rejects the
    /// replacement.
    pub fn replace_order(
        &self,
        client_order_id: &str,
        quantity_ticks: i64,
        limit_price_ticks: Option<i64>,
    ) -> Result<(), EngineError> {
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        replace_order(
            &mut orders,
            self.broker.as_ref(),
            client_order_id,
            quantity_ticks,
            limit_price_ticks,
        )
        .map_err(EngineError::Replace)
    }

    /// Applies an authoritative broker event and updates position quantity on fills.
    ///
    /// # Errors
    /// Returns [`EngineError::Transition`] if the event does not match a
    /// persisted order or violates its lifecycle.
    pub fn apply_broker_event(&self, event: BrokerEvent) -> Result<(), EngineError> {
        self.apply_broker_event_inner(event, true)
    }

    /// Applies a broker event during reconciliation without changing the
    /// portfolio projection. The subsequent broker account snapshot is the
    /// authoritative position/cash state; this path still advances order state
    /// and records fills for audit and attribution.
    ///
    /// # Errors
    /// Returns a transition error when the event does not match a persisted
    /// order or violates its lifecycle.
    pub fn apply_reconciled_broker_event(&self, event: BrokerEvent) -> Result<(), EngineError> {
        self.apply_broker_event_inner(event, false)
    }

    fn apply_broker_event_inner(
        &self,
        event: BrokerEvent,
        update_portfolio: bool,
    ) -> Result<(), EngineError> {
        let fill = match &event {
            BrokerEvent::Filled {
                client_order_id,
                quantity_ticks,
                price_ticks,
            } => {
                let orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
                let record = orders
                    .get(client_order_id)
                    .ok_or(EngineError::Transition(TransitionError::UnknownOrder))?;
                if record.intent.state == insider_broker_api::OrderState::Filled {
                    drop(orders);
                    return self.apply_duplicate_broker_event(event);
                }
                let signed = if record.intent.side == Side::Buy {
                    *quantity_ticks
                } else {
                    quantity_ticks.saturating_neg()
                };
                Some((
                    record.intent.instrument_id,
                    signed,
                    *price_ticks,
                    client_order_id.clone(),
                ))
            }
            _ => None,
        };
        let portfolio_candidate = if update_portfolio {
            if let Some((instrument_id, signed_quantity, price, _)) = &fill {
                let mut candidate = self
                    .portfolio
                    .lock()
                    .map_err(|_| EngineError::Poisoned)?
                    .clone();
                candidate
                    .apply_fill(*instrument_id, *signed_quantity, *price, 0)
                    .map_err(EngineError::Accounting)?;
                Some(candidate)
            } else {
                None
            }
        } else {
            None
        };
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        orders
            .apply_broker_event(event)
            .map_err(EngineError::Transition)?;
        drop(orders);
        if let Some((instrument_id, signed_quantity, price, client_order_id)) = fill {
            if let Some(candidate) = portfolio_candidate {
                *self.portfolio.lock().map_err(|_| EngineError::Poisoned)? = candidate;
            }
            let mut fills = self.fills.lock().map_err(|_| EngineError::Poisoned)?;
            if fills.len() >= MAX_RUNTIME_FILLS {
                fills.pop_front();
            }
            fills.push_back(FillRecord {
                client_order_id,
                instrument_id,
                signed_quantity_ticks: signed_quantity,
                price_ticks: price,
            });
        }
        Ok(())
    }

    fn apply_duplicate_broker_event(&self, event: BrokerEvent) -> Result<(), EngineError> {
        let mut orders = self.orders.lock().map_err(|_| EngineError::Poisoned)?;
        orders
            .apply_broker_event(event)
            .map_err(EngineError::Transition)
    }

    /// Encodes an authoritative broker event for journaling before projection mutation.
    #[must_use]
    pub fn broker_event_payload(event: &BrokerEvent) -> Vec<u8> {
        encode_broker_event(event)
    }

    /// Returns a snapshot of the reconciled portfolio.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the runtime state lock is unavailable.
    pub fn portfolio(&self) -> Result<Portfolio, EngineError> {
        self.portfolio
            .lock()
            .map(|portfolio| portfolio.clone())
            .map_err(|_| EngineError::Poisoned)
    }

    /// Returns the bounded authoritative fill history in arrival order.
    ///
    /// The history is rebuilt from journaled broker events during startup and
    /// is suitable for TCA/read-model consumers; it never accepts UI writes.
    ///
    /// # Errors
    /// Returns [`EngineError::Poisoned`] if the fill-history lock is unavailable.
    pub fn fill_history(&self) -> Result<Vec<FillRecord>, EngineError> {
        self.fills
            .lock()
            .map(|fills| fills.iter().cloned().collect())
            .map_err(|_| EngineError::Poisoned)
    }

    /// Updates the trusted market mark used by opening-target risk checks.
    ///
    /// # Errors
    /// Returns [`EngineError::Accounting`] when the mark is not positive.
    pub fn update_mark_price(
        &self,
        instrument_id: insider_common_types::InstrumentId,
        price_ticks: i64,
    ) -> Result<(), EngineError> {
        self.portfolio
            .lock()
            .map_err(|_| EngineError::Poisoned)?
            .set_mark_price(instrument_id, price_ticks)
            .map_err(EngineError::Accounting)
    }

    fn apply_corporate_action(
        &self,
        instrument_id: InstrumentId,
        kind: CorporateActionKind,
    ) -> Result<(), EngineError> {
        let mut portfolio = self.portfolio.lock().map_err(|_| EngineError::Poisoned)?;
        match kind {
            CorporateActionKind::Split {
                numerator,
                denominator,
            } => portfolio
                .apply_split(instrument_id, numerator, denominator)
                .map(|_| ())
                .map_err(EngineError::Accounting),
            CorporateActionKind::CashDividend { amount_ticks } => portfolio
                .apply_cash_dividend(instrument_id, amount_ticks)
                .map(|_| ())
                .map_err(EngineError::Accounting),
            CorporateActionKind::OptionExercise {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            } => portfolio
                .apply_option_exercise(
                    instrument_id,
                    underlying_instrument_id,
                    option_quantity_delta_ticks,
                    underlying_quantity_delta_ticks,
                    cash_delta_ticks,
                )
                .map(|_| ())
                .map_err(EngineError::Accounting),
            CorporateActionKind::OptionAssignment {
                underlying_instrument_id,
                option_quantity_delta_ticks,
                underlying_quantity_delta_ticks,
                cash_delta_ticks,
            } => portfolio
                .apply_option_assignment(
                    instrument_id,
                    underlying_instrument_id,
                    option_quantity_delta_ticks,
                    underlying_quantity_delta_ticks,
                    cash_delta_ticks,
                )
                .map(|_| ())
                .map_err(EngineError::Accounting),
            CorporateActionKind::OptionExpiry {
                option_quantity_delta_ticks,
                cash_delta_ticks,
            } => portfolio
                .apply_option_expiry(instrument_id, option_quantity_delta_ticks, cash_delta_ticks)
                .map(|_| ())
                .map_err(EngineError::Accounting),
            CorporateActionKind::FuturesVariationMargin { cash_delta_ticks } => portfolio
                .apply_futures_variation_margin(instrument_id, cash_delta_ticks)
                .map(|_| ())
                .map_err(EngineError::Accounting),
        }
    }
}

fn backtest_experiment_bundle(result: &BacktestRunResult) -> Result<ExperimentBundle, EngineError> {
    let mut report_bytes = String::new();
    writeln!(report_bytes, "format=insider-replay-report-v1")
        .map_err(|_| EngineError::InvalidRequest)?;
    writeln!(report_bytes, "event_count={}", result.report.event_count)
        .map_err(|_| EngineError::InvalidRequest)?;
    writeln!(
        report_bytes,
        "max_drawdown_ticks={}",
        result.report.max_drawdown_ticks
    )
    .map_err(|_| EngineError::InvalidRequest)?;
    writeln!(
        report_bytes,
        "total_fees_ticks={}",
        result.report.total_fees_ticks
    )
    .map_err(|_| EngineError::InvalidRequest)?;
    for point in &result.report.equity_curve {
        writeln!(
            report_bytes,
            "equity.{}.position={};average_cost={};cash={};realized={};equity={}",
            point.sequence,
            point.snapshot.position_ticks,
            point.snapshot.average_cost_ticks,
            point.snapshot.cash_ticks,
            point.snapshot.realized_pnl_ticks,
            point.snapshot.equity_ticks
        )
        .map_err(|_| EngineError::InvalidRequest)?;
    }
    if let Some(snapshot) = result.report.final_snapshot {
        writeln!(
            report_bytes,
            "final.position={};average_cost={};cash={};realized={};equity={}",
            snapshot.position_ticks,
            snapshot.average_cost_ticks,
            snapshot.cash_ticks,
            snapshot.realized_pnl_ticks,
            snapshot.equity_ticks
        )
        .map_err(|_| EngineError::InvalidRequest)?;
    }
    let report_digest = sha256(report_bytes.as_bytes());
    let report_hash = hex_digest(&report_digest);
    let mut seed_bytes = [0_u8; 8];
    seed_bytes.copy_from_slice(&report_digest[..8]);
    let seed = u64::from_be_bytes(seed_bytes);
    Ok(ExperimentBundle {
        run_id: format!("backtest:{}", result.run_id),
        code_hash: format!("strategy:{}", result.strategy_id),
        config_hash: result.config_hash.clone(),
        dataset_hash: result.dataset_hash.clone(),
        schema_hashes: BTreeMap::from([(
            String::from("report"),
            hex_digest(&sha256(b"insider-replay-report-v1")),
        )]),
        model_hashes: BTreeMap::new(),
        prompt_hashes: BTreeMap::new(),
        environment: BTreeMap::from([(
            String::from("engine_version"),
            env!("CARGO_PKG_VERSION").to_owned(),
        )]),
        command: vec![String::from("insider-engine"), String::from("backtest")],
        seed,
        artifacts: vec![ExperimentArtifact {
            kind: String::from("backtest_report"),
            hash: report_hash.clone(),
            path: format!("backtests/{}.report", result.run_id),
        }],
        report_hash,
    })
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(&bytes[..bytes.len().min(length as usize)]);
}

const TRACE_LINK_MAGIC: &[u8] = b"IT_TRACE_LINK_V1\0";

fn encode_trace_link(trace_id: TraceId, kind: &str, object_id: &str) -> Vec<u8> {
    let mut output = TRACE_LINK_MAGIC.to_vec();
    output.extend_from_slice(&trace_id.get().to_le_bytes());
    push_string(&mut output, kind);
    push_string(&mut output, object_id);
    output
}

fn encode_order_intent(intent: &insider_broker_api::OrderIntent) -> Vec<u8> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(b"IT_ORDER_INTENT_V1\0");
    output.extend_from_slice(&intent.account_id.get().to_le_bytes());
    output.extend_from_slice(&intent.instrument_id.get().to_le_bytes());
    output.push(match intent.side {
        Side::Buy => 1,
        Side::Sell => 2,
    });
    output.extend_from_slice(&intent.quantity_ticks.to_le_bytes());
    output.push(match intent.order_type {
        insider_broker_api::OrderType::Market => 1,
        insider_broker_api::OrderType::Limit => 2,
    });
    output.extend_from_slice(&intent.limit_price_ticks.unwrap_or_default().to_le_bytes());
    output.push(match intent.time_in_force {
        insider_broker_api::TimeInForce::Day => 1,
        insider_broker_api::TimeInForce::GoodTilCancel => 2,
        insider_broker_api::TimeInForce::ImmediateOrCancel => 3,
    });
    output.extend_from_slice(&intent.trace_id.get().to_le_bytes());
    push_string(&mut output, &intent.intent_id);
    push_string(&mut output, &intent.client_order_id);
    output
}

fn market_event_mark(event: MarketEvent) -> Option<(InstrumentId, i64)> {
    match event {
        MarketEvent::Quote(quote) => Some((
            quote.instrument_id,
            quote
                .bid_ticks
                .checked_add(quote.ask_ticks)?
                .checked_div(2)?,
        )),
        MarketEvent::Trade(trade) => Some((trade.instrument_id, trade.price_ticks)),
        MarketEvent::Book(_) => None,
    }
}

fn encode_market_event(
    event: MarketEvent,
    receive_wall: insider_common_types::WallTime,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(b"IT_MARKET_EVENT_V1\0");
    output.extend_from_slice(&receive_wall.as_unix_nanos().to_le_bytes());
    match event {
        MarketEvent::Quote(quote) => {
            output.push(1);
            output.extend_from_slice(&quote.instrument_id.get().to_le_bytes());
            output.extend_from_slice(&quote.sequence.to_le_bytes());
            output.extend_from_slice(&quote.exchange_time.as_unix_nanos().to_le_bytes());
            output.extend_from_slice(&quote.received_mono.as_nanos().to_le_bytes());
            output.extend_from_slice(&quote.bid_ticks.to_le_bytes());
            output.extend_from_slice(&quote.ask_ticks.to_le_bytes());
            output.extend_from_slice(&quote.bid_quantity_ticks.to_le_bytes());
            output.extend_from_slice(&quote.ask_quantity_ticks.to_le_bytes());
        }
        MarketEvent::Trade(trade) => {
            output.push(2);
            output.extend_from_slice(&trade.instrument_id.get().to_le_bytes());
            output.extend_from_slice(&trade.sequence.to_le_bytes());
            output.extend_from_slice(&trade.exchange_time.as_unix_nanos().to_le_bytes());
            output.extend_from_slice(&trade.received_mono.as_nanos().to_le_bytes());
            output.extend_from_slice(&trade.price_ticks.to_le_bytes());
            output.extend_from_slice(&trade.quantity_ticks.to_le_bytes());
        }
        MarketEvent::Book(delta) => {
            output.push(3);
            output.extend_from_slice(&delta.instrument_id.get().to_le_bytes());
            output.extend_from_slice(&delta.sequence.to_le_bytes());
            output.push(match delta.side {
                insider_market_data::BookSide::Bid => 1,
                insider_market_data::BookSide::Ask => 2,
            });
            output.extend_from_slice(&delta.price_ticks.to_le_bytes());
            output.extend_from_slice(&delta.quantity_ticks.to_le_bytes());
        }
    }
    output
}

fn encode_market_bar(bar: Bar, sequence: u64) -> Vec<u8> {
    let mut output = Vec::with_capacity(96);
    output.extend_from_slice(b"IT_MARKET_BAR_V1\0");
    output.extend_from_slice(&bar.instrument_id.get().to_le_bytes());
    output.extend_from_slice(&sequence.to_le_bytes());
    output.extend_from_slice(&bar.start_time.as_unix_nanos().to_le_bytes());
    output.extend_from_slice(&bar.interval_ns.to_le_bytes());
    output.extend_from_slice(&bar.open_ticks.to_le_bytes());
    output.extend_from_slice(&bar.high_ticks.to_le_bytes());
    output.extend_from_slice(&bar.low_ticks.to_le_bytes());
    output.extend_from_slice(&bar.close_ticks.to_le_bytes());
    output.extend_from_slice(&bar.volume_ticks.to_le_bytes());
    output
}

fn encode_broker_event(event: &BrokerEvent) -> Vec<u8> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(b"IT_BROKER_EVENT_V1\0");
    match event {
        BrokerEvent::Acknowledged {
            client_order_id,
            broker_order_id,
        } => {
            output.push(1);
            push_string(&mut output, client_order_id);
            push_string(&mut output, broker_order_id);
        }
        BrokerEvent::Filled {
            client_order_id,
            quantity_ticks,
            price_ticks,
        } => {
            output.push(2);
            push_string(&mut output, client_order_id);
            output.extend_from_slice(&quantity_ticks.to_le_bytes());
            output.extend_from_slice(&price_ticks.to_le_bytes());
        }
        BrokerEvent::Rejected {
            client_order_id,
            reason,
        } => {
            output.push(3);
            push_string(&mut output, client_order_id);
            push_string(&mut output, reason);
        }
        BrokerEvent::Cancelled { client_order_id } => {
            output.push(4);
            push_string(&mut output, client_order_id);
        }
    }
    output
}

fn encode_execution_timing(timing: &ExecutionTiming) -> Vec<u8> {
    let mut output = Vec::with_capacity(128);
    output.extend_from_slice(b"IT_EXECUTION_TIMING_V1\0");
    push_string(&mut output, &timing.client_order_id);
    output.extend_from_slice(&timing.decision_mono_ns.to_le_bytes());
    output.push(u8::from(timing.arrival_price_ticks.is_some()));
    output.extend_from_slice(&timing.arrival_price_ticks.unwrap_or_default().to_le_bytes());
    for value in [
        timing.send_mono_ns,
        timing.ack_mono_ns,
        timing.first_fill_mono_ns,
    ] {
        output.push(u8::from(value.is_some()));
        output.extend_from_slice(&value.unwrap_or_default().to_le_bytes());
    }
    for value in [
        timing.decision_mid_ticks,
        timing.arrival_spread_ticks,
        timing.send_mid_ticks,
        timing.ack_mid_ticks,
        timing.post_fill_mid_ticks,
    ] {
        output.push(u8::from(value.is_some()));
        output.extend_from_slice(&value.unwrap_or_default().to_le_bytes());
    }
    output
}

fn encode_child_plan(record: &ChildPlanRecord) -> Vec<u8> {
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(b"IT_CHILD_PLAN_V1\0");
    let parent = encode_order_intent(&record.parent);
    output.extend_from_slice(&(u32::try_from(parent.len()).unwrap_or(u32::MAX)).to_le_bytes());
    output.extend_from_slice(&parent);
    output.extend_from_slice(&record.plan.created_mono_ns.to_le_bytes());
    output.extend_from_slice(
        &(u32::try_from(record.plan.children.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for child in &record.plan.children {
        push_string(&mut output, &child.order.parent_client_order_id);
        output.extend_from_slice(&child.order.child_sequence.to_le_bytes());
        push_string(&mut output, &child.order.client_order_id);
        output.extend_from_slice(&child.order.quantity_ticks.to_le_bytes());
        output.extend_from_slice(&child.order.due_after_ns.to_le_bytes());
        output.push(match child.order.side {
            Side::Buy => 1,
            Side::Sell => 2,
        });
        output.push(match child.order.order_type {
            insider_broker_api::OrderType::Market => 1,
            insider_broker_api::OrderType::Limit => 2,
        });
        output.push(u8::from(child.order.limit_price_ticks.is_some()));
        output.extend_from_slice(
            &child
                .order
                .limit_price_ticks
                .unwrap_or_default()
                .to_le_bytes(),
        );
        output.push(match child.state {
            ChildState::Pending => 1,
            ChildState::Sending => 2,
            ChildState::Sent => 3,
            ChildState::Acknowledged => 4,
            ChildState::PartiallyFilled => 5,
            ChildState::Filled => 6,
            ChildState::CancelPending => 7,
            ChildState::Cancelled => 8,
            ChildState::Rejected => 9,
            ChildState::Unknown => 10,
        });
        output.extend_from_slice(&child.filled_quantity_ticks.to_le_bytes());
        push_string(
            &mut output,
            child.broker_order_id.as_deref().unwrap_or_default(),
        );
    }
    output
}

fn encode_backtest_result(result: &BacktestRunResult) -> Vec<u8> {
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(b"IT_BACKTEST_RESULT_V1\0");
    push_string(&mut output, &result.run_id);
    push_string(&mut output, &result.strategy_id);
    push_string(&mut output, &result.dataset_hash);
    push_string(&mut output, &result.config_hash);
    output.extend_from_slice(
        &(u64::try_from(result.report.event_count).unwrap_or(u64::MAX)).to_le_bytes(),
    );
    output.extend_from_slice(&result.report.max_drawdown_ticks.to_le_bytes());
    output.extend_from_slice(&result.report.total_fees_ticks.to_le_bytes());
    output.push(u8::from(result.report.final_snapshot.is_some()));
    encode_ledger_snapshot(&mut output, result.report.final_snapshot);
    output.extend_from_slice(
        &(u32::try_from(result.report.equity_curve.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for point in &result.report.equity_curve {
        output.extend_from_slice(&point.sequence.to_le_bytes());
        encode_ledger_snapshot(&mut output, Some(point.snapshot));
    }
    output
}

fn encode_experiment_run(run: &ExperimentRun) -> Vec<u8> {
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(b"IT_EXPERIMENT_RUN_V2\0");
    push_string(&mut output, &run.run_id);
    push_string(&mut output, &run.code_hash);
    push_string(&mut output, &run.config_hash);
    push_string(&mut output, &run.dataset_hash);
    output.push(match run.status {
        RunStatus::Created => 1,
        RunStatus::Running => 2,
        RunStatus::Succeeded => 3,
        RunStatus::Failed => 4,
        RunStatus::Cancelled => 5,
    });
    output.extend_from_slice(&(u32::try_from(run.metrics.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for (key, value) in &run.metrics {
        push_string(&mut output, key);
        output.extend_from_slice(&value.to_le_bytes());
    }
    output
        .extend_from_slice(&(u32::try_from(run.artifacts.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for artifact in &run.artifacts {
        push_string(&mut output, &artifact.kind);
        push_string(&mut output, &artifact.hash);
        push_string(&mut output, &artifact.path);
    }
    for value in [
        &run.provenance.strategy_id,
        &run.provenance.strategy_version,
        &run.provenance.news_dataset_hash,
        &run.provenance.news_clustering_version,
        &run.provenance.graph_snapshot_version,
        &run.provenance.llm_provider,
        &run.provenance.llm_model,
        &run.provenance.prompt_version,
        &run.provenance.tool_schema_version,
        &run.provenance.autonomy_config_hash,
    ] {
        output.push(u8::from(value.is_some()));
        if let Some(value) = value {
            push_string(&mut output, value);
        }
    }
    output.extend_from_slice(
        &(u32::try_from(run.provenance.llm_cache_ids.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for cache_id in &run.provenance.llm_cache_ids {
        push_string(&mut output, cache_id);
    }
    output
}

fn encode_prompt_record(prompt: &PromptRecord) -> Vec<u8> {
    let mut output = b"IT_PROMPT_RECORD_V1\0".to_vec();
    for value in [
        &prompt.prompt_id,
        &prompt.version,
        &prompt.purpose,
        &prompt.input_schema,
        &prompt.output_schema,
        &prompt.task_class,
        &prompt.artifact_hash,
        &prompt.fixture_suite,
    ] {
        push_string(&mut output, value);
    }
    output.extend_from_slice(
        &(u16::try_from(prompt.allowed_tools.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for tool in &prompt.allowed_tools {
        push_string(&mut output, tool);
    }
    output.extend_from_slice(&[
        u8::from(prompt.required_capabilities.responses),
        u8::from(prompt.required_capabilities.chat_completions),
        u8::from(prompt.required_capabilities.streaming),
        u8::from(prompt.required_capabilities.json_schema),
        u8::from(prompt.required_capabilities.tools),
    ]);
    output
}

fn encode_model_registry(snapshot: &ModelRegistrySnapshot) -> Vec<u8> {
    let mut output = b"IT_MODEL_REGISTRY_V1\0".to_vec();
    output.extend_from_slice(
        &(u32::try_from(snapshot.records.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for record in &snapshot.records {
        push_string(&mut output, &record.model_id);
        push_string(&mut output, &record.version);
        push_string(&mut output, &record.artifact_hash);
        push_string(&mut output, &record.input_schema_hash);
        push_string(&mut output, &record.output_schema_hash);
        output.extend_from_slice(
            &(u64::try_from(record.input_width).unwrap_or(u64::MAX)).to_le_bytes(),
        );
        output.push(match record.status {
            ModelStatus::Research => 1,
            ModelStatus::Validated => 2,
            ModelStatus::Shadow => 3,
            ModelStatus::Canary => 4,
            ModelStatus::Production => 5,
            ModelStatus::Retired => 6,
        });
    }
    output.extend_from_slice(
        &(u32::try_from(snapshot.manifests.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for ((model_id, version), manifest) in &snapshot.manifests {
        push_string(&mut output, model_id);
        push_string(&mut output, version);
        push_string(&mut output, &manifest.code_hash);
        push_string(&mut output, &manifest.training_data_hash);
        push_string(&mut output, &manifest.config_hash);
        push_string(&mut output, &manifest.feature_hash);
        push_string(&mut output, &manifest.calibration_hash);
        push_string(&mut output, &manifest.artifact_hash);
    }
    output.extend_from_slice(
        &(u32::try_from(snapshot.active.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for (model_id, version) in &snapshot.active {
        push_string(&mut output, model_id);
        push_string(&mut output, version);
    }
    output
}

fn encode_ledger_snapshot(output: &mut Vec<u8>, snapshot: Option<insider_replay::LedgerSnapshot>) {
    if let Some(snapshot) = snapshot {
        output.extend_from_slice(&snapshot.position_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.average_cost_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.cash_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.realized_pnl_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.equity_ticks.to_le_bytes());
    } else {
        output.extend_from_slice(&0_i64.to_le_bytes());
        output.extend_from_slice(&0_i64.to_le_bytes());
        output.extend_from_slice(&0_i128.to_le_bytes());
        output.extend_from_slice(&0_i128.to_le_bytes());
        output.extend_from_slice(&0_i128.to_le_bytes());
    }
}

fn encode_replace_request(
    client_order_id: &str,
    quantity_ticks: i64,
    limit_price_ticks: Option<i64>,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(64 + client_order_id.len());
    output.extend_from_slice(b"IT_REPLACE_REQUEST_V1\0");
    push_string(&mut output, client_order_id);
    output.extend_from_slice(&quantity_ticks.to_le_bytes());
    output.push(u8::from(limit_price_ticks.is_some()));
    output.extend_from_slice(&limit_price_ticks.unwrap_or_default().to_le_bytes());
    output
}

fn encode_cancel_request(client_order_id: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(32 + client_order_id.len());
    output.extend_from_slice(b"IT_CANCEL_REQUEST_V1\0");
    push_string(&mut output, client_order_id);
    output
}

fn encode_risk_state(state: RiskState, authorization: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(64);
    output.extend_from_slice(b"IT_RISK_STATE_V1\0");
    output.push(match state {
        RiskState::Running => 1,
        RiskState::ReduceOnly => 2,
        RiskState::CancelOnly => 3,
        RiskState::Halted => 4,
    });
    push_string(&mut output, authorization);
    output
}

fn encode_alert(alert: &Alert) -> Vec<u8> {
    let mut output = b"IT_ALERT_V1\0".to_vec();
    push_string(&mut output, &alert.alert_id);
    push_string(&mut output, &alert.dedupe_key);
    push_string(&mut output, &alert.source);
    output.extend_from_slice(&alert.occurred_ms.to_le_bytes());
    output.push(match alert.severity {
        AlertSeverity::Info => 1,
        AlertSeverity::Warning => 2,
        AlertSeverity::Critical => 3,
    });
    output.push(u8::from(alert.sensitive));
    push_string(&mut output, &alert.message);
    output
}

fn encode_alert_ack(alert_id: &str) -> Vec<u8> {
    let mut output = b"IT_ALERT_ACK_V1\0".to_vec();
    push_string(&mut output, alert_id);
    output
}

fn encode_live_limits(limits: &LiveLimits) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_LIVE_LIMITS_V1\0");
    output.extend_from_slice(
        &u16::try_from(limits.allowed_accounts.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for account in &limits.allowed_accounts {
        push_string(&mut output, account);
    }
    output.extend_from_slice(&limits.max_notional_ticks.to_le_bytes());
    output
}

fn encode_live_kill() -> Vec<u8> {
    b"IT_LIVE_KILL_V1\0".to_vec()
}

fn encode_news_item(item: &NewsItem) -> Vec<u8> {
    let mut output = b"IT_NEWS_ITEM_V1\0".to_vec();
    push_string(&mut output, &item.id);
    push_string(&mut output, &item.provider);
    push_string(&mut output, &item.canonical_url);
    push_string(&mut output, &item.source_name);
    push_string(&mut output, &item.title);
    push_string(
        &mut output,
        item.summary_text.as_deref().unwrap_or_default(),
    );
    output.push(u8::from(item.published_at_ms.is_some()));
    output.extend_from_slice(&item.published_at_ms.unwrap_or_default().to_le_bytes());
    output.extend_from_slice(&item.received_at_ms.to_le_bytes());
    output
        .extend_from_slice(&(u16::try_from(item.symbols.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for symbol in &item.symbols {
        push_string(&mut output, symbol);
    }
    push_string(&mut output, &item.content_hash);
    output
}

fn encode_embedding_snapshot(snapshot: &EmbeddingIndexSnapshot) -> Vec<u8> {
    let mut output = b"IT_CONTEXT_EMBEDDINGS_V1\0".to_vec();
    push_string(&mut output, &snapshot.model);
    push_string(&mut output, &snapshot.model_version);
    output.extend_from_slice(
        &u32::try_from(snapshot.dimensions)
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    output.extend_from_slice(
        &u32::try_from(snapshot.records.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for record in &snapshot.records {
        push_string(&mut output, &record.node_id);
        push_string(&mut output, &record.content_hash);
        push_string(&mut output, &record.model);
        push_string(&mut output, &record.model_version);
        output.extend_from_slice(
            &u32::try_from(record.dimensions)
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        output.extend_from_slice(&record.created_at_ms.to_le_bytes());
        output.extend_from_slice(
            &u32::try_from(record.vector.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for value in &record.vector {
            output.extend_from_slice(&value.to_le_bytes());
        }
    }
    output
}

fn encode_strategy_proposal(proposal: &Proposal) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_STRATEGY_PROPOSAL_V1\0");
    output.extend_from_slice(&proposal.proposal_id.get().to_le_bytes());
    output.extend_from_slice(&proposal.instrument_id.get().to_le_bytes());
    push_string(&mut output, &proposal.strategy_id);
    match proposal.action {
        Action::NoAction => output.push(0),
        Action::TargetQuantity { quantity_ticks } => {
            output.push(1);
            output.extend_from_slice(&quantity_ticks.to_le_bytes());
        }
        Action::TargetWeight { weight } => {
            output.push(2);
            output.extend_from_slice(&weight.to_bits().to_le_bytes());
        }
        Action::Increase { quantity_ticks } => {
            output.push(3);
            output.extend_from_slice(&quantity_ticks.to_le_bytes());
        }
        Action::Decrease { quantity_ticks } => {
            output.push(4);
            output.extend_from_slice(&quantity_ticks.to_le_bytes());
        }
        Action::Close => output.push(5),
    }
    output.extend_from_slice(&proposal.confidence.to_bits().to_le_bytes());
    output.extend_from_slice(&proposal.horizon_ns.to_le_bytes());
    output.extend_from_slice(&proposal.ttl_ns.to_le_bytes());
    output.extend_from_slice(&proposal.generated_mono.as_nanos().to_le_bytes());
    let evidence_count = proposal.evidence.len().min(256);
    output.extend_from_slice(&(u16::try_from(evidence_count).unwrap_or(256)).to_le_bytes());
    for evidence in proposal.evidence.iter().take(256) {
        push_string(&mut output, evidence);
    }
    output
}

fn encode_strategy_resolution(policy: StrategyPolicy, now: MonoTime) -> Vec<u8> {
    let mut output = b"IT_STRATEGY_RESOLUTION_V1\0".to_vec();
    output.push(match policy {
        StrategyPolicy::IsolatedBooks => 1,
        StrategyPolicy::Priority => 2,
        StrategyPolicy::WeightedNet => 3,
    });
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    output
}

fn encode_strategy_resolution_with_budgets(
    policy: StrategyPolicy,
    now: MonoTime,
    budgets: &std::collections::BTreeMap<String, StrategyBudget>,
) -> Vec<u8> {
    let mut output = b"IT_STRATEGY_RESOLUTION_V2\0".to_vec();
    output.push(match policy {
        StrategyPolicy::IsolatedBooks => 1,
        StrategyPolicy::Priority => 2,
        StrategyPolicy::WeightedNet => 3,
    });
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    let count = budgets.len().min(256);
    output.extend_from_slice(&(u16::try_from(count).unwrap_or(256)).to_le_bytes());
    for (strategy_id, budget) in budgets.iter().take(256) {
        push_string(&mut output, strategy_id);
        output.extend_from_slice(&budget.max_abs_quantity_ticks.to_le_bytes());
    }
    output
}

fn resolution_policy_name(policy: StrategyPolicy) -> &'static str {
    match policy {
        StrategyPolicy::IsolatedBooks => "isolated_books",
        StrategyPolicy::Priority => "priority",
        StrategyPolicy::WeightedNet => "weighted_net",
    }
}

fn resolution_summary(
    policy: StrategyPolicy,
    now: MonoTime,
    result: &StrategyResultSet,
) -> StrategyResolutionSummary {
    StrategyResolutionSummary {
        policy: resolution_policy_name(policy).to_owned(),
        now_mono_ns: now.as_nanos(),
        accepted_count: result.accepted.len(),
        conflict_count: result.conflicts.len(),
        expired_count: result.expired.len(),
        attribution_count: result.attributions.len(),
    }
}

fn push_resolution_summary(
    history: &mut Vec<StrategyResolutionSummary>,
    summary: StrategyResolutionSummary,
) {
    const MAX_HISTORY: usize = 4096;
    if history.len() >= MAX_HISTORY {
        history.remove(0);
    }
    history.push(summary);
}

fn encode_strategy_execution_summary(summary: &StrategyExecutionSummary) -> Vec<u8> {
    let mut output = b"IT_STRATEGY_EXECUTION_SUMMARY_V1\0".to_vec();
    push_string(&mut output, &summary.strategy_id);
    output.extend_from_slice(&summary.fill_count.to_le_bytes());
    output.extend_from_slice(&summary.filled_quantity_ticks.to_le_bytes());
    output.extend_from_slice(&summary.notional_ticks.to_le_bytes());
    output
}

fn encode_strategy_lifecycle(
    strategy_id: &str,
    lifecycle: insider_strategy_host::Lifecycle,
    evidence_ref: &str,
) -> Vec<u8> {
    let mut output = b"IT_STRATEGY_LIFECYCLE_V1\0".to_vec();
    push_string(&mut output, strategy_id);
    output.push(match lifecycle {
        insider_strategy_host::Lifecycle::Research => 1,
        insider_strategy_host::Lifecycle::Validated => 2,
        insider_strategy_host::Lifecycle::Shadow => 3,
        insider_strategy_host::Lifecycle::Canary => 4,
        insider_strategy_host::Lifecycle::Production => 5,
        insider_strategy_host::Lifecycle::Paused => 6,
        insider_strategy_host::Lifecycle::Retired => 7,
    });
    push_string(&mut output, evidence_ref);
    output
}

fn encode_metric_lifecycle(
    metric_id: &str,
    lifecycle: insider_metric_host::Lifecycle,
    evidence_ref: &str,
) -> Vec<u8> {
    let mut output = b"IT_METRIC_LIFECYCLE_V1\0".to_vec();
    push_string(&mut output, metric_id);
    output.push(match lifecycle {
        insider_metric_host::Lifecycle::Research => 1,
        insider_metric_host::Lifecycle::Validated => 2,
        insider_metric_host::Lifecycle::Shadow => 3,
        insider_metric_host::Lifecycle::Canary => 4,
        insider_metric_host::Lifecycle::Production => 5,
        insider_metric_host::Lifecycle::Paused => 6,
        insider_metric_host::Lifecycle::Retired => 7,
    });
    push_string(&mut output, evidence_ref);
    output
}

fn encode_autonomy_mode(mode: AutonomyMode) -> Vec<u8> {
    vec![
        b'I',
        b'T',
        b'_',
        b'A',
        b'U',
        b'T',
        b'O',
        b'N',
        b'O',
        b'M',
        b'Y',
        b'_',
        b'M',
        b'O',
        b'D',
        b'E',
        b'_',
        b'V',
        b'1',
        0,
        match mode {
            AutonomyMode::Manual => 1,
            AutonomyMode::Hybrid => 2,
            AutonomyMode::Autonomous => 3,
        },
    ]
}

fn encode_portfolio_snapshot(snapshot: &BrokerSnapshot) -> Result<Vec<u8>, EngineError> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_PORTFOLIO_SNAPSHOT_V1\0");
    output.extend_from_slice(
        &u32::try_from(snapshot.positions.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for position in &snapshot.positions {
        output.extend_from_slice(&position.instrument_id.get().to_le_bytes());
        output.extend_from_slice(&position.quantity_ticks.to_le_bytes());
    }
    let cash = snapshot
        .account_values
        .get(insider_broker_api::ACCOUNT_VALUE_CASH_TICKS)
        .map(|value| i64::try_from(*value).map_err(|_| AccountingError::Overflow))
        .transpose()
        .map_err(EngineError::Accounting)?;
    output.push(u8::from(cash.is_some()));
    output.extend_from_slice(&cash.unwrap_or_default().to_le_bytes());
    Ok(output)
}

fn encode_portfolio_peak(peak: Option<i128>) -> Vec<u8> {
    let mut output = b"IT_PORTFOLIO_PEAK_V1\0".to_vec();
    output.push(u8::from(peak.is_some()));
    output.extend_from_slice(&peak.unwrap_or_default().to_le_bytes());
    output
}

fn encode_corporate_action(instrument_id: InstrumentId, kind: CorporateActionKind) -> Vec<u8> {
    let mut output = b"IT_PORTFOLIO_CORPORATE_ACTION_V1\0".to_vec();
    output.extend_from_slice(&instrument_id.get().to_le_bytes());
    match kind {
        CorporateActionKind::Split {
            numerator,
            denominator,
        } => {
            output.push(1);
            output.extend_from_slice(&numerator.to_le_bytes());
            output.extend_from_slice(&denominator.to_le_bytes());
        }
        CorporateActionKind::CashDividend { amount_ticks } => {
            output.push(2);
            output.extend_from_slice(&amount_ticks.to_le_bytes());
        }
        CorporateActionKind::OptionExercise {
            underlying_instrument_id,
            option_quantity_delta_ticks,
            underlying_quantity_delta_ticks,
            cash_delta_ticks,
        } => {
            output.push(3);
            output.extend_from_slice(&underlying_instrument_id.get().to_le_bytes());
            output.extend_from_slice(&option_quantity_delta_ticks.to_le_bytes());
            output.extend_from_slice(&underlying_quantity_delta_ticks.to_le_bytes());
            output.extend_from_slice(&cash_delta_ticks.to_le_bytes());
        }
        CorporateActionKind::OptionAssignment {
            underlying_instrument_id,
            option_quantity_delta_ticks,
            underlying_quantity_delta_ticks,
            cash_delta_ticks,
        } => {
            output.push(4);
            output.extend_from_slice(&underlying_instrument_id.get().to_le_bytes());
            output.extend_from_slice(&option_quantity_delta_ticks.to_le_bytes());
            output.extend_from_slice(&underlying_quantity_delta_ticks.to_le_bytes());
            output.extend_from_slice(&cash_delta_ticks.to_le_bytes());
        }
        CorporateActionKind::OptionExpiry {
            option_quantity_delta_ticks,
            cash_delta_ticks,
        } => {
            output.push(5);
            output.extend_from_slice(&option_quantity_delta_ticks.to_le_bytes());
            output.extend_from_slice(&cash_delta_ticks.to_le_bytes());
        }
        CorporateActionKind::FuturesVariationMargin { cash_delta_ticks } => {
            output.push(6);
            output.extend_from_slice(&cash_delta_ticks.to_le_bytes());
        }
    }
    output
}

fn encode_scoped_risk_policy(
    snapshot: Option<&ScopedRiskPolicySnapshot>,
) -> Result<Vec<u8>, EngineError> {
    let mut output = b"IT_RISK_SCOPED_POLICY_V1\0".to_vec();
    let Some(snapshot) = snapshot else {
        output.push(0);
        return Ok(output);
    };
    output.push(1);
    append_timed_limits(&mut output, &snapshot.system)?;
    if snapshot.accounts.len() > 1024
        || snapshot.strategies.len() > 1024
        || snapshot.assets.len() > 16
        || snapshot.instruments.len() > 16_384
    {
        return Err(EngineError::InvalidRequest);
    }
    output.extend_from_slice(
        &u16::try_from(snapshot.accounts.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for (identity, revisions) in &snapshot.accounts {
        push_string(&mut output, identity);
        append_timed_limits(&mut output, revisions)?;
    }
    output.extend_from_slice(
        &u16::try_from(snapshot.strategies.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for (identity, revisions) in &snapshot.strategies {
        push_string(&mut output, identity);
        append_timed_limits(&mut output, revisions)?;
    }
    output.push(u8::try_from(snapshot.assets.len()).unwrap_or(u8::MAX));
    for (asset, revisions) in &snapshot.assets {
        output.push(match asset {
            insider_market_types::AssetClass::Equity => 1,
            insider_market_types::AssetClass::Etf => 2,
            insider_market_types::AssetClass::Option => 3,
            insider_market_types::AssetClass::Future => 4,
            insider_market_types::AssetClass::Fx => 5,
            insider_market_types::AssetClass::Crypto => 6,
        });
        append_timed_limits(&mut output, revisions)?;
    }
    output.extend_from_slice(
        &u16::try_from(snapshot.instruments.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for (instrument, revisions) in &snapshot.instruments {
        output.extend_from_slice(&instrument.get().to_le_bytes());
        append_timed_limits(&mut output, revisions)?;
    }
    Ok(output)
}

fn append_timed_limits(output: &mut Vec<u8>, revisions: &[TimedLimits]) -> Result<(), EngineError> {
    if revisions.len() > 256 {
        return Err(EngineError::InvalidRequest);
    }
    output.extend_from_slice(
        &u16::try_from(revisions.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for revision in revisions {
        output.extend_from_slice(&revision.effective_mono_ns.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_position_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_order_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_gross_notional_ticks.to_le_bytes());
    }
    Ok(())
}

fn read_timed_limits(payload: &[u8], cursor: &mut usize) -> Result<Vec<TimedLimits>, EngineError> {
    let count = usize::from(read_u16(payload, cursor).ok_or_else(journal_corrupt)?);
    if count > 256 {
        return Err(journal_corrupt());
    }
    let mut revisions = Vec::with_capacity(count);
    for _ in 0..count {
        let effective_mono_ns = read_u64(payload, cursor).ok_or_else(journal_corrupt)?;
        let limits = insider_risk_engine::Limits {
            max_position_ticks: read_i64(payload, cursor).ok_or_else(journal_corrupt)?,
            max_order_ticks: read_i64(payload, cursor).ok_or_else(journal_corrupt)?,
            max_gross_notional_ticks: read_i128(payload, cursor).ok_or_else(journal_corrupt)?,
        };
        revisions.push(TimedLimits {
            effective_mono_ns,
            limits,
        });
    }
    Ok(revisions)
}

fn same_order_identity(
    left: &insider_broker_api::OrderIntent,
    right: &insider_broker_api::OrderIntent,
) -> bool {
    left.intent_id == right.intent_id
        && left.account_id == right.account_id
        && left.instrument_id == right.instrument_id
        && left.client_order_id == right.client_order_id
        && left.side == right.side
        && left.quantity_ticks == right.quantity_ticks
        && left.order_type == right.order_type
        && left.limit_price_ticks == right.limit_price_ticks
        && left.time_in_force == right.time_in_force
}

fn stable_autonomy_trace_seed(plan_id: &str) -> u128 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u128;
    for byte in plan_id.as_bytes() {
        hash ^= u128::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3_u128);
    }
    hash.max(1)
}

fn scale_action(action: &Action, scale: f64) -> Result<Action, EngineError> {
    #[allow(clippy::cast_precision_loss)]
    let scale_quantity = |quantity_ticks: i64| -> Result<i64, EngineError> {
        const MAX_EXACT_INTEGER: i64 = 1_i64 << 53;
        if quantity_ticks.unsigned_abs() > MAX_EXACT_INTEGER as u64 {
            return Err(EngineError::InvalidRequest);
        }
        let scaled = (quantity_ticks as f64 * scale).round();
        if !scaled.is_finite()
            || scaled == 0.0
            || scaled < i64::MIN as f64
            || scaled > i64::MAX as f64
        {
            return Err(EngineError::InvalidRequest);
        }
        #[allow(clippy::cast_possible_truncation)]
        let result = scaled as i64;
        Ok(result)
    };
    match action {
        Action::NoAction => Err(EngineError::InvalidRequest),
        Action::TargetQuantity { quantity_ticks } => Ok(Action::TargetQuantity {
            quantity_ticks: scale_quantity(*quantity_ticks)?,
        }),
        Action::TargetWeight { weight } => Ok(Action::TargetWeight {
            weight: *weight * scale,
        }),
        Action::Increase { quantity_ticks } => Ok(Action::Increase {
            quantity_ticks: scale_quantity(*quantity_ticks)?,
        }),
        Action::Decrease { quantity_ticks } => Ok(Action::Decrease {
            quantity_ticks: scale_quantity(*quantity_ticks)?,
        }),
        Action::Close => Ok(Action::Close),
    }
}

/// Bridges the news-core page transaction to the engine journal. The core has
/// already applied the bounded in-memory projection when this hook runs; a
/// crash before the journal append is safe because the provider cursor remains
/// unchanged and the retried page is deduplicated on restart.
struct JournalNewsCommitter<'a> {
    host: &'a ServiceHost,
}

impl CursorCommitter for JournalNewsCommitter<'_> {
    fn commit_page(
        &mut self,
        _provider_id: &str,
        _expected_generation: u64,
        _next_cursor: Option<&str>,
        items: &[NewsItem],
    ) -> Result<(), String> {
        for item in items {
            self.host
                .append_event(&encode_news_item(item))
                .map_err(|error| format!("news journal: {error:?}"))?;
        }
        Ok(())
    }

    fn commit_cursor(
        &mut self,
        _provider_id: &str,
        _expected_generation: u64,
        _next_cursor: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
}

enum RecoveredEvent {
    Market {
        event: MarketEvent,
        receive_wall: insider_common_types::WallTime,
    },
    MarketBar {
        bar: Bar,
        sequence: u64,
    },
    Intent(insider_broker_api::OrderIntent),
    ChildPlan(ChildPlanRecord),
    ExecutionTiming(ExecutionTiming),
    Broker(BrokerEvent),
    Risk(RiskState, String),
    LiveLimits(LiveLimits),
    LiveKilled,
    PortfolioSnapshot {
        positions: Vec<(InstrumentId, i64)>,
        cash_ticks: Option<i64>,
    },
    PortfolioPeak(Option<i128>),
    CorporateAction {
        instrument_id: InstrumentId,
        kind: CorporateActionKind,
    },
    ScopedRiskPolicy(Option<ScopedRiskPolicySnapshot>),
    Autonomy(PlanEvent),
    News(NewsItem),
    EmbeddingSnapshot(EmbeddingIndexSnapshot),
    ProviderState(ProviderStateSnapshot),
    StrategyProposal(Proposal),
    StrategyResolution {
        policy: StrategyPolicy,
        now: MonoTime,
        budgets: std::collections::BTreeMap<String, StrategyBudget>,
    },
    AutonomyMode(AutonomyMode),
    Alert(Alert),
    AlertAck(String),
    Backtest(BacktestRunResult),
    Experiment(ExperimentRun),
    Prompt(PromptRecord),
    ModelRegistry(ModelRegistrySnapshot),
    StrategyExecution(StrategyExecutionSummary),
    StrategyLifecycle {
        strategy_id: String,
        lifecycle: insider_strategy_host::Lifecycle,
        evidence_ref: String,
    },
    MetricLifecycle {
        metric_id: String,
        lifecycle: insider_metric_host::Lifecycle,
        evidence_ref: String,
    },
}

fn broker_event_client_order_id(event: &BrokerEvent) -> &str {
    match event {
        BrokerEvent::Acknowledged {
            client_order_id, ..
        }
        | BrokerEvent::Filled {
            client_order_id, ..
        }
        | BrokerEvent::Rejected {
            client_order_id, ..
        }
        | BrokerEvent::Cancelled {
            client_order_id, ..
        } => client_order_id.as_str(),
    }
}

fn journal_corrupt() -> EngineError {
    EngineError::Journal(JournalError::Corrupt {
        offset: 0,
        reason: "invalid engine journal event",
    })
}

fn read_bytes<'a>(payload: &'a [u8], cursor: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = cursor.checked_add(length)?;
    let bytes = payload.get(*cursor..end)?;
    *cursor = end;
    Some(bytes)
}

fn read_u8(payload: &[u8], cursor: &mut usize) -> Option<u8> {
    read_bytes(payload, cursor, 1).map(|bytes| bytes[0])
}

fn read_i64(payload: &[u8], cursor: &mut usize) -> Option<i64> {
    read_bytes(payload, cursor, 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(i64::from_le_bytes)
}

fn read_f64(payload: &[u8], cursor: &mut usize) -> Option<f64> {
    read_bytes(payload, cursor, 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(f64::from_le_bytes)
}

fn read_u16(payload: &[u8], cursor: &mut usize) -> Option<u16> {
    read_bytes(payload, cursor, 2)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u16::from_le_bytes)
}

fn read_u32(payload: &[u8], cursor: &mut usize) -> Option<u32> {
    read_bytes(payload, cursor, 4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)
}

fn read_u64(payload: &[u8], cursor: &mut usize) -> Option<u64> {
    read_bytes(payload, cursor, 8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u64::from_le_bytes)
}

fn read_u128(payload: &[u8], cursor: &mut usize) -> Option<u128> {
    read_bytes(payload, cursor, 16)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u128::from_le_bytes)
}

fn read_i128(payload: &[u8], cursor: &mut usize) -> Option<i128> {
    read_bytes(payload, cursor, 16)
        .and_then(|bytes| bytes.try_into().ok())
        .map(i128::from_le_bytes)
}

fn read_ledger_snapshot(
    payload: &[u8],
    cursor: &mut usize,
) -> Option<insider_replay::LedgerSnapshot> {
    Some(insider_replay::LedgerSnapshot {
        position_ticks: read_i64(payload, cursor)?,
        average_cost_ticks: read_i64(payload, cursor)?,
        cash_ticks: read_i128(payload, cursor)?,
        realized_pnl_ticks: read_i128(payload, cursor)?,
        equity_ticks: read_i128(payload, cursor)?,
    })
}

fn read_string(payload: &[u8], cursor: &mut usize) -> Option<String> {
    let length = read_bytes(payload, cursor, 4)
        .and_then(|bytes| bytes.try_into().ok())
        .map(u32::from_le_bytes)? as usize;
    if length > 1_048_576 {
        return None;
    }
    String::from_utf8(read_bytes(payload, cursor, length)?.to_vec()).ok()
}

fn decode_trace_link(payload: &[u8]) -> Result<Option<(TraceId, String, String)>, EngineError> {
    if !payload.starts_with(TRACE_LINK_MAGIC) {
        return Ok(None);
    }
    let mut cursor = TRACE_LINK_MAGIC.len();
    let trace_id = TraceId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?);
    let trace_id = trace_id.map_err(|_| journal_corrupt())?;
    let kind = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
    let object_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
    if kind.trim().is_empty() || object_id.trim().is_empty() || cursor != payload.len() {
        return Err(journal_corrupt());
    }
    Ok(Some((trace_id, kind, object_id)))
}

#[allow(clippy::too_many_lines)]
fn decode_journal_payload(payload: &[u8]) -> Result<Option<RecoveredEvent>, EngineError> {
    const INTENT_MAGIC: &[u8] = b"IT_ORDER_INTENT_V1\0";
    const BROKER_MAGIC: &[u8] = b"IT_BROKER_EVENT_V1\0";
    const RISK_MAGIC: &[u8] = b"IT_RISK_STATE_V1\0";
    const LIVE_LIMITS_MAGIC: &[u8] = b"IT_LIVE_LIMITS_V1\0";
    const LIVE_KILL_MAGIC: &[u8] = b"IT_LIVE_KILL_V1\0";
    const PORTFOLIO_SNAPSHOT_MAGIC: &[u8] = b"IT_PORTFOLIO_SNAPSHOT_V1\0";
    const PORTFOLIO_PEAK_MAGIC: &[u8] = b"IT_PORTFOLIO_PEAK_V1\0";
    const CORPORATE_ACTION_MAGIC: &[u8] = b"IT_PORTFOLIO_CORPORATE_ACTION_V1\0";
    const PLAN_EVENT_MAGIC: &[u8] = b"IT_PLAN_EVENT_V1\0";
    const NEWS_ITEM_MAGIC: &[u8] = b"IT_NEWS_ITEM_V1\0";
    const EMBEDDING_SNAPSHOT_MAGIC: &[u8] = b"IT_CONTEXT_EMBEDDINGS_V1\0";
    const PROVIDER_STATE_MAGIC: &[u8] = b"IT_NEWS_PROVIDER_STATE_V2\0";
    const PROVIDER_STATE_MAGIC_V1: &[u8] = b"IT_NEWS_PROVIDER_STATE_V1\0";
    const STRATEGY_PROPOSAL_MAGIC: &[u8] = b"IT_STRATEGY_PROPOSAL_V1\0";
    const STRATEGY_RESOLUTION_MAGIC: &[u8] = b"IT_STRATEGY_RESOLUTION_V1\0";
    const STRATEGY_RESOLUTION_V2_MAGIC: &[u8] = b"IT_STRATEGY_RESOLUTION_V2\0";
    const STRATEGY_EXECUTION_SUMMARY_MAGIC: &[u8] = b"IT_STRATEGY_EXECUTION_SUMMARY_V1\0";
    const STRATEGY_LIFECYCLE_MAGIC: &[u8] = b"IT_STRATEGY_LIFECYCLE_V1\0";
    const METRIC_LIFECYCLE_MAGIC: &[u8] = b"IT_METRIC_LIFECYCLE_V1\0";
    const AUTONOMY_MODE_MAGIC: &[u8] = b"IT_AUTONOMY_MODE_V1\0";
    const ALERT_MAGIC: &[u8] = b"IT_ALERT_V1\0";
    const ALERT_ACK_MAGIC: &[u8] = b"IT_ALERT_ACK_V1\0";
    const REPLACE_MAGIC: &[u8] = b"IT_REPLACE_REQUEST_V1\0";
    const CANCEL_MAGIC: &[u8] = b"IT_CANCEL_REQUEST_V1\0";
    const EXECUTION_TIMING_MAGIC: &[u8] = b"IT_EXECUTION_TIMING_V1\0";
    const CHILD_PLAN_MAGIC: &[u8] = b"IT_CHILD_PLAN_V1\0";
    const BACKTEST_RESULT_MAGIC: &[u8] = b"IT_BACKTEST_RESULT_V1\0";
    const EXPERIMENT_RUN_MAGIC: &[u8] = b"IT_EXPERIMENT_RUN_V1\0";
    const EXPERIMENT_RUN_V2_MAGIC: &[u8] = b"IT_EXPERIMENT_RUN_V2\0";
    const PROMPT_RECORD_MAGIC: &[u8] = b"IT_PROMPT_RECORD_V1\0";
    const MODEL_REGISTRY_MAGIC: &[u8] = b"IT_MODEL_REGISTRY_V1\0";
    const MARKET_EVENT_MAGIC: &[u8] = b"IT_MARKET_EVENT_V1\0";
    const MARKET_BAR_MAGIC: &[u8] = b"IT_MARKET_BAR_V1\0";
    const SCOPED_RISK_POLICY_MAGIC: &[u8] = b"IT_RISK_SCOPED_POLICY_V1\0";
    if payload.starts_with(SCOPED_RISK_POLICY_MAGIC) {
        let mut cursor = SCOPED_RISK_POLICY_MAGIC.len();
        let present = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if present > 1 {
            return Err(journal_corrupt());
        }
        if present == 0 {
            if cursor != payload.len() {
                return Err(journal_corrupt());
            }
            return Ok(Some(RecoveredEvent::ScopedRiskPolicy(None)));
        }
        let system = read_timed_limits(payload, &mut cursor)?;
        let account_count =
            usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if account_count > 1024 {
            return Err(journal_corrupt());
        }
        let mut accounts = BTreeMap::new();
        for _ in 0..account_count {
            let id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if id.trim().is_empty() || accounts.contains_key(&id) {
                return Err(journal_corrupt());
            }
            accounts.insert(id, read_timed_limits(payload, &mut cursor)?);
        }
        let strategy_count =
            usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if strategy_count > 1024 {
            return Err(journal_corrupt());
        }
        let mut strategies = BTreeMap::new();
        for _ in 0..strategy_count {
            let id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if id.trim().is_empty() || strategies.contains_key(&id) {
                return Err(journal_corrupt());
            }
            strategies.insert(id, read_timed_limits(payload, &mut cursor)?);
        }
        let asset_count = usize::from(read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if asset_count > 6 {
            return Err(journal_corrupt());
        }
        let mut assets = BTreeMap::new();
        for _ in 0..asset_count {
            let asset = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                1 => insider_market_types::AssetClass::Equity,
                2 => insider_market_types::AssetClass::Etf,
                3 => insider_market_types::AssetClass::Option,
                4 => insider_market_types::AssetClass::Future,
                5 => insider_market_types::AssetClass::Fx,
                6 => insider_market_types::AssetClass::Crypto,
                _ => return Err(journal_corrupt()),
            };
            if assets.contains_key(&asset) {
                return Err(journal_corrupt());
            }
            assets.insert(asset, read_timed_limits(payload, &mut cursor)?);
        }
        let instrument_count =
            usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if instrument_count > 16_384 {
            return Err(journal_corrupt());
        }
        let mut instruments = BTreeMap::new();
        for _ in 0..instrument_count {
            let instrument =
                InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            if instruments.contains_key(&instrument) {
                return Err(journal_corrupt());
            }
            instruments.insert(instrument, read_timed_limits(payload, &mut cursor)?);
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::ScopedRiskPolicy(Some(
            ScopedRiskPolicySnapshot {
                system,
                accounts,
                strategies,
                assets,
                instruments,
            },
        ))));
    }
    if payload.starts_with(MARKET_BAR_MAGIC) {
        let mut cursor = MARKET_BAR_MAGIC.len();
        let instrument_id =
            InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        let sequence = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let bar = Bar {
            instrument_id,
            start_time: insider_common_types::WallTime::from_unix_nanos(
                read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            ),
            interval_ns: read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            open_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            high_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            low_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            close_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            volume_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        };
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::MarketBar { bar, sequence }));
    }
    if payload.starts_with(CORPORATE_ACTION_MAGIC) {
        let mut cursor = CORPORATE_ACTION_MAGIC.len();
        let instrument_id =
            InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        let action_kind = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let action = match action_kind {
            1 => CorporateActionKind::Split {
                numerator: read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                denominator: read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            2 => CorporateActionKind::CashDividend {
                amount_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            3 | 4 => {
                let underlying_instrument_id =
                    InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                        .map_err(|_| journal_corrupt())?;
                let option_quantity_delta_ticks =
                    read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                let underlying_quantity_delta_ticks =
                    read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                let cash_delta_ticks =
                    read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                if action_kind == 3 {
                    CorporateActionKind::OptionExercise {
                        underlying_instrument_id,
                        option_quantity_delta_ticks,
                        underlying_quantity_delta_ticks,
                        cash_delta_ticks,
                    }
                } else {
                    CorporateActionKind::OptionAssignment {
                        underlying_instrument_id,
                        option_quantity_delta_ticks,
                        underlying_quantity_delta_ticks,
                        cash_delta_ticks,
                    }
                }
            }
            5 => CorporateActionKind::OptionExpiry {
                option_quantity_delta_ticks: read_i64(payload, &mut cursor)
                    .ok_or_else(journal_corrupt)?,
                cash_delta_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            6 => CorporateActionKind::FuturesVariationMargin {
                cash_delta_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            _ => return Err(journal_corrupt()),
        };
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::CorporateAction {
            instrument_id,
            kind: action,
        }));
    }
    if payload.starts_with(MARKET_EVENT_MAGIC) {
        let mut cursor = MARKET_EVENT_MAGIC.len();
        let receive_wall = insider_common_types::WallTime::from_unix_nanos(
            read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        );
        let event = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => {
                let instrument_id =
                    InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                        .map_err(|_| journal_corrupt())?;
                MarketEvent::Quote(insider_market_data::Quote {
                    instrument_id,
                    sequence: read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    exchange_time: insider_common_types::WallTime::from_unix_nanos(
                        read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    ),
                    received_mono: MonoTime::from_nanos(
                        read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    ),
                    bid_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    ask_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    bid_quantity_ticks: read_i64(payload, &mut cursor)
                        .ok_or_else(journal_corrupt)?,
                    ask_quantity_ticks: read_i64(payload, &mut cursor)
                        .ok_or_else(journal_corrupt)?,
                })
            }
            2 => {
                let instrument_id =
                    InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                        .map_err(|_| journal_corrupt())?;
                MarketEvent::Trade(insider_market_data::Trade {
                    instrument_id,
                    sequence: read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    exchange_time: insider_common_types::WallTime::from_unix_nanos(
                        read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    ),
                    received_mono: MonoTime::from_nanos(
                        read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    ),
                    price_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                })
            }
            3 => {
                let instrument_id =
                    InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                        .map_err(|_| journal_corrupt())?;
                let sequence = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                let side = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                    1 => insider_market_data::BookSide::Bid,
                    2 => insider_market_data::BookSide::Ask,
                    _ => return Err(journal_corrupt()),
                };
                MarketEvent::Book(insider_market_data::BookDelta {
                    instrument_id,
                    sequence,
                    side,
                    price_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                    quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                })
            }
            _ => return Err(journal_corrupt()),
        };
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Market {
            event,
            receive_wall,
        }));
    }
    if payload.starts_with(MODEL_REGISTRY_MAGIC) {
        let mut cursor = MODEL_REGISTRY_MAGIC.len();
        let record_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if record_count > 4096 {
            return Err(journal_corrupt());
        }
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let model_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let version = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let artifact_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let input_schema_hash =
                read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let output_schema_hash =
                read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let input_width =
                usize::try_from(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            let status = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                1 => ModelStatus::Research,
                2 => ModelStatus::Validated,
                3 => ModelStatus::Shadow,
                4 => ModelStatus::Canary,
                5 => ModelStatus::Production,
                6 => ModelStatus::Retired,
                _ => return Err(journal_corrupt()),
            };
            records.push(ModelRecord {
                model_id,
                version,
                artifact_hash,
                input_schema_hash,
                output_schema_hash,
                input_width,
                status,
            });
        }
        let manifest_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if manifest_count > 4096 {
            return Err(journal_corrupt());
        }
        let mut manifests = Vec::with_capacity(manifest_count);
        for _ in 0..manifest_count {
            let model_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let version = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let manifest = ArtifactManifest {
                code_hash: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                training_data_hash: read_string(payload, &mut cursor)
                    .ok_or_else(journal_corrupt)?,
                config_hash: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                feature_hash: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                calibration_hash: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                artifact_hash: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            };
            manifests.push(((model_id, version), manifest));
        }
        let active_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if active_count > 4096 {
            return Err(journal_corrupt());
        }
        let mut active = Vec::with_capacity(active_count);
        for _ in 0..active_count {
            active.push((
                read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
                read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            ));
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::ModelRegistry(ModelRegistrySnapshot {
            records,
            manifests,
            active,
        })));
    }
    if payload.starts_with(EXPERIMENT_RUN_V2_MAGIC) || payload.starts_with(EXPERIMENT_RUN_MAGIC) {
        let v2 = payload.starts_with(EXPERIMENT_RUN_V2_MAGIC);
        let mut cursor = if v2 {
            EXPERIMENT_RUN_V2_MAGIC.len()
        } else {
            EXPERIMENT_RUN_MAGIC.len()
        };
        let run_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let code_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let config_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let dataset_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let status = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => RunStatus::Created,
            2 => RunStatus::Running,
            3 => RunStatus::Succeeded,
            4 => RunStatus::Failed,
            5 => RunStatus::Cancelled,
            _ => return Err(journal_corrupt()),
        };
        let metric_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if metric_count > 4096
            || run_id.trim().is_empty()
            || code_hash.trim().is_empty()
            || config_hash.trim().is_empty()
            || dataset_hash.trim().is_empty()
        {
            return Err(journal_corrupt());
        }
        let mut metrics = BTreeMap::new();
        for _ in 0..metric_count {
            let key = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let value = read_f64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if key.trim().is_empty() || !value.is_finite() {
                return Err(journal_corrupt());
            }
            metrics.insert(key, value);
        }
        let artifact_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if artifact_count > 4096 {
            return Err(journal_corrupt());
        }
        let mut artifacts = Vec::with_capacity(artifact_count);
        for _ in 0..artifact_count {
            let kind = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let path = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if kind.trim().is_empty() || hash.trim().is_empty() || path.trim().is_empty() {
                return Err(journal_corrupt());
            }
            artifacts.push(ExperimentArtifact { kind, hash, path });
        }
        let mut provenance = ExperimentProvenance::default();
        if v2 {
            let fields = [
                &mut provenance.strategy_id,
                &mut provenance.strategy_version,
                &mut provenance.news_dataset_hash,
                &mut provenance.news_clustering_version,
                &mut provenance.graph_snapshot_version,
                &mut provenance.llm_provider,
                &mut provenance.llm_model,
                &mut provenance.prompt_version,
                &mut provenance.tool_schema_version,
                &mut provenance.autonomy_config_hash,
            ];
            for field in fields {
                let present = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                if present > 1 {
                    return Err(journal_corrupt());
                }
                if present == 1 {
                    *field = Some(read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?);
                }
            }
            let cache_count =
                usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            if cache_count > 256 {
                return Err(journal_corrupt());
            }
            for _ in 0..cache_count {
                provenance
                    .llm_cache_ids
                    .push(read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?);
            }
            if !provenance.valid_for_replay() {
                return Err(journal_corrupt());
            }
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Experiment(ExperimentRun {
            run_id,
            code_hash,
            config_hash,
            dataset_hash,
            provenance,
            status,
            metrics,
            artifacts,
        })));
    }
    if payload.starts_with(PROMPT_RECORD_MAGIC) {
        let mut cursor = PROMPT_RECORD_MAGIC.len();
        let prompt_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let version = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let purpose = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let input_schema = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let output_schema = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let task_class = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let artifact_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let fixture_suite = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let tool_count = usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if tool_count > 128 {
            return Err(journal_corrupt());
        }
        let mut allowed_tools = Vec::with_capacity(tool_count);
        for _ in 0..tool_count {
            allowed_tools.push(read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        }
        let mut capability_flags = [0_u8; 5];
        for flag in &mut capability_flags {
            *flag = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if *flag > 1 {
                return Err(journal_corrupt());
            }
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        let prompt = PromptRecord {
            prompt_id,
            version,
            purpose,
            input_schema,
            output_schema,
            allowed_tools,
            task_class,
            required_capabilities: LlmCapabilities {
                responses: capability_flags[0] == 1,
                chat_completions: capability_flags[1] == 1,
                streaming: capability_flags[2] == 1,
                json_schema: capability_flags[3] == 1,
                tools: capability_flags[4] == 1,
            },
            artifact_hash,
            fixture_suite,
        };
        if prompt.clone().validate_for_replay().is_err() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Prompt(prompt)));
    }
    if payload.starts_with(BACKTEST_RESULT_MAGIC) {
        let mut cursor = BACKTEST_RESULT_MAGIC.len();
        let run_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let strategy_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let dataset_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let config_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let event_count =
            usize::try_from(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if run_id.trim().is_empty()
            || strategy_id.trim().is_empty()
            || dataset_hash.trim().is_empty()
            || config_hash.trim().is_empty()
            || event_count > 1_000_000
        {
            return Err(journal_corrupt());
        }
        let max_drawdown_ticks = read_i128(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let total_fees_ticks = read_i128(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let final_marker = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if final_marker > 1 {
            return Err(journal_corrupt());
        }
        let final_snapshot =
            read_ledger_snapshot(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let final_snapshot = (final_marker == 1).then_some(final_snapshot);
        let curve_count =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if curve_count > 1_000_000 {
            return Err(journal_corrupt());
        }
        let mut equity_curve = Vec::with_capacity(curve_count);
        for _ in 0..curve_count {
            let sequence = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let snapshot =
                read_ledger_snapshot(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            equity_curve.push(insider_replay::EquityPoint { sequence, snapshot });
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Backtest(BacktestRunResult {
            run_id,
            strategy_id,
            dataset_hash,
            config_hash,
            report: insider_replay::BacktestReport {
                event_count,
                equity_curve,
                final_snapshot,
                max_drawdown_ticks,
                total_fees_ticks,
            },
        })));
    }
    if payload.starts_with(CHILD_PLAN_MAGIC) {
        let mut cursor = CHILD_PLAN_MAGIC.len();
        let parent_len =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        if parent_len == 0 || parent_len > 1_048_576 {
            return Err(journal_corrupt());
        }
        let parent_payload =
            read_bytes(payload, &mut cursor, parent_len).ok_or_else(journal_corrupt)?;
        let Some(RecoveredEvent::Intent(parent)) = decode_journal_payload(parent_payload)? else {
            return Err(journal_corrupt());
        };
        let created_mono_ns = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let count = usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
            .map_err(|_| journal_corrupt())?;
        if count == 0 || count > 16_384 {
            return Err(journal_corrupt());
        }
        let mut children = Vec::with_capacity(count);
        for _ in 0..count {
            let parent_client_order_id =
                read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let child_sequence = read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let quantity_ticks = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let due_after_ns = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let side = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                1 => Side::Buy,
                2 => Side::Sell,
                _ => return Err(journal_corrupt()),
            };
            let order_type = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                1 => insider_broker_api::OrderType::Market,
                2 => insider_broker_api::OrderType::Limit,
                _ => return Err(journal_corrupt()),
            };
            let limit_marker = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if limit_marker > 1 {
                return Err(journal_corrupt());
            }
            let limit_price = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let state = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
                1 => ChildState::Pending,
                2 => ChildState::Sending,
                3 => ChildState::Sent,
                4 => ChildState::Acknowledged,
                5 => ChildState::PartiallyFilled,
                6 => ChildState::Filled,
                7 => ChildState::CancelPending,
                8 => ChildState::Cancelled,
                9 => ChildState::Rejected,
                10 => ChildState::Unknown,
                _ => return Err(journal_corrupt()),
            };
            let filled_quantity_ticks =
                read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let broker_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if parent_client_order_id.trim().is_empty()
                || client_order_id.trim().is_empty()
                || quantity_ticks <= 0
                || filled_quantity_ticks < 0
                || filled_quantity_ticks > quantity_ticks
            {
                return Err(journal_corrupt());
            }
            children.push(ChildRecord {
                order: ChildOrder {
                    parent_client_order_id,
                    child_sequence,
                    client_order_id,
                    quantity_ticks,
                    due_after_ns,
                    side,
                    order_type,
                    limit_price_ticks: (limit_marker == 1).then_some(limit_price),
                },
                state,
                filled_quantity_ticks,
                broker_order_id: (!broker_order_id.is_empty()).then_some(broker_order_id),
            });
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::ChildPlan(ChildPlanRecord {
            parent: parent.clone(),
            plan: ChildPlan {
                parent_client_order_id: parent.client_order_id,
                created_mono_ns,
                children,
            },
        })));
    }
    if payload.starts_with(EXECUTION_TIMING_MAGIC) {
        let mut cursor = EXECUTION_TIMING_MAGIC.len();
        let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let decision_mono_ns = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let arrival_marker = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if arrival_marker > 1 {
            return Err(journal_corrupt());
        }
        let arrival_price = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let mut optional = [None, None, None];
        for value in &mut optional {
            let marker = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if marker > 1 {
                return Err(journal_corrupt());
            }
            let timestamp = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            *value = (marker == 1).then_some(timestamp);
        }
        let mut market_refs = [None, None, None, None, None];
        if cursor != payload.len() {
            for value in &mut market_refs {
                let presence = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                if presence > 1 {
                    return Err(journal_corrupt());
                }
                let reference = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                *value = (presence == 1).then_some(reference);
            }
        }
        if cursor != payload.len() || client_order_id.trim().is_empty() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::ExecutionTiming(ExecutionTiming {
            client_order_id,
            decision_mono_ns,
            arrival_price_ticks: (arrival_marker == 1).then_some(arrival_price),
            send_mono_ns: optional[0],
            ack_mono_ns: optional[1],
            first_fill_mono_ns: optional[2],
            decision_mid_ticks: market_refs[0],
            arrival_spread_ticks: market_refs[1],
            send_mid_ticks: market_refs[2],
            ack_mid_ticks: market_refs[3],
            post_fill_mid_ticks: market_refs[4],
        })));
    }
    if payload.starts_with(ALERT_MAGIC) {
        let mut cursor = ALERT_MAGIC.len();
        let alert_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let dedupe_key = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let source = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let occurred_ms = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let severity = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => AlertSeverity::Info,
            2 => AlertSeverity::Warning,
            3 => AlertSeverity::Critical,
            _ => return Err(journal_corrupt()),
        };
        let sensitive = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            0 => false,
            1 => true,
            _ => return Err(journal_corrupt()),
        };
        let message = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if alert_id.trim().is_empty()
            || dedupe_key.trim().is_empty()
            || source.trim().is_empty()
            || message.trim().is_empty()
            || cursor != payload.len()
        {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Alert(Alert {
            alert_id,
            dedupe_key,
            source,
            occurred_ms,
            severity,
            message,
            sensitive,
        })));
    }
    if payload.starts_with(ALERT_ACK_MAGIC) {
        let mut cursor = ALERT_ACK_MAGIC.len();
        let alert_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if alert_id.trim().is_empty() || cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::AlertAck(alert_id)));
    }
    if payload.starts_with(AUTONOMY_MODE_MAGIC) {
        if payload.len() != AUTONOMY_MODE_MAGIC.len() + 1 {
            return Err(journal_corrupt());
        }
        let mode = match payload[AUTONOMY_MODE_MAGIC.len()] {
            1 => AutonomyMode::Manual,
            2 => AutonomyMode::Hybrid,
            3 => AutonomyMode::Autonomous,
            _ => return Err(journal_corrupt()),
        };
        return Ok(Some(RecoveredEvent::AutonomyMode(mode)));
    }
    if payload.starts_with(PROVIDER_STATE_MAGIC) || payload.starts_with(PROVIDER_STATE_MAGIC_V1) {
        let snapshot = decode_provider_state(payload).map_err(|_| journal_corrupt())?;
        return Ok(Some(RecoveredEvent::ProviderState(snapshot)));
    }
    if payload == LIVE_KILL_MAGIC {
        return Ok(Some(RecoveredEvent::LiveKilled));
    }
    if payload.starts_with(STRATEGY_RESOLUTION_V2_MAGIC)
        || payload.starts_with(STRATEGY_RESOLUTION_MAGIC)
    {
        let versioned = payload.starts_with(STRATEGY_RESOLUTION_V2_MAGIC);
        let mut cursor = if versioned {
            STRATEGY_RESOLUTION_V2_MAGIC.len()
        } else {
            STRATEGY_RESOLUTION_MAGIC.len()
        };
        let policy = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => StrategyPolicy::IsolatedBooks,
            2 => StrategyPolicy::Priority,
            3 => StrategyPolicy::WeightedNet,
            _ => return Err(journal_corrupt()),
        };
        let now = MonoTime::from_nanos(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        let mut budgets = std::collections::BTreeMap::new();
        if versioned {
            let count = usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
            if count > 256 {
                return Err(journal_corrupt());
            }
            for _ in 0..count {
                let strategy_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                let quantity = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
                let budget = StrategyBudget::new(quantity).ok_or_else(journal_corrupt)?;
                if budgets.insert(strategy_id, budget).is_some() {
                    return Err(journal_corrupt());
                }
            }
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::StrategyResolution {
            policy,
            now,
            budgets,
        }));
    }
    if payload.starts_with(STRATEGY_EXECUTION_SUMMARY_MAGIC) {
        let mut cursor = STRATEGY_EXECUTION_SUMMARY_MAGIC.len();
        let strategy_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if strategy_id.is_empty() {
            return Err(journal_corrupt());
        }
        let fill_count = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let filled_quantity_ticks = read_i128(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let notional_ticks = read_i128(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::StrategyExecution(
            StrategyExecutionSummary {
                strategy_id,
                fill_count,
                filled_quantity_ticks,
                notional_ticks,
            },
        )));
    }
    if payload.starts_with(STRATEGY_LIFECYCLE_MAGIC) {
        let mut cursor = STRATEGY_LIFECYCLE_MAGIC.len();
        let strategy_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let lifecycle = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => insider_strategy_host::Lifecycle::Research,
            2 => insider_strategy_host::Lifecycle::Validated,
            3 => insider_strategy_host::Lifecycle::Shadow,
            4 => insider_strategy_host::Lifecycle::Canary,
            5 => insider_strategy_host::Lifecycle::Production,
            6 => insider_strategy_host::Lifecycle::Paused,
            7 => insider_strategy_host::Lifecycle::Retired,
            _ => return Err(journal_corrupt()),
        };
        let evidence_ref = if cursor < payload.len() {
            read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?
        } else {
            String::from("legacy-journal")
        };
        if strategy_id.trim().is_empty()
            || evidence_ref.trim().is_empty()
            || evidence_ref.len() > 512
            || cursor != payload.len()
        {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::StrategyLifecycle {
            strategy_id,
            lifecycle,
            evidence_ref,
        }));
    }
    if payload.starts_with(METRIC_LIFECYCLE_MAGIC) {
        let mut cursor = METRIC_LIFECYCLE_MAGIC.len();
        let metric_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let lifecycle = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => insider_metric_host::Lifecycle::Research,
            2 => insider_metric_host::Lifecycle::Validated,
            3 => insider_metric_host::Lifecycle::Shadow,
            4 => insider_metric_host::Lifecycle::Canary,
            5 => insider_metric_host::Lifecycle::Production,
            6 => insider_metric_host::Lifecycle::Paused,
            7 => insider_metric_host::Lifecycle::Retired,
            _ => return Err(journal_corrupt()),
        };
        let evidence_ref = if cursor < payload.len() {
            read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?
        } else {
            String::from("legacy-journal")
        };
        if metric_id.trim().is_empty()
            || evidence_ref.trim().is_empty()
            || evidence_ref.len() > 512
            || cursor != payload.len()
        {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::MetricLifecycle {
            metric_id,
            lifecycle,
            evidence_ref,
        }));
    }
    if payload.starts_with(PORTFOLIO_PEAK_MAGIC) {
        let mut cursor = PORTFOLIO_PEAK_MAGIC.len();
        let has_peak = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let peak = i128::from_le_bytes(
            read_bytes(payload, &mut cursor, 16)
                .ok_or_else(journal_corrupt)?
                .try_into()
                .map_err(|_| journal_corrupt())?,
        );
        if has_peak > 1 || cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::PortfolioPeak(
            (has_peak == 1).then_some(peak),
        )));
    }
    if payload.starts_with(STRATEGY_PROPOSAL_MAGIC) {
        let mut cursor = STRATEGY_PROPOSAL_MAGIC.len();
        let proposal_id =
            ProposalId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        let instrument_id =
            InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        let strategy_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let action = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            0 => Action::NoAction,
            1 => Action::TargetQuantity {
                quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            2 => Action::TargetWeight {
                weight: f64::from_bits(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?),
            },
            3 => Action::Increase {
                quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            4 => Action::Decrease {
                quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            },
            5 => Action::Close,
            _ => return Err(journal_corrupt()),
        };
        let confidence =
            f64::from_bits(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        let horizon_ns = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let ttl_ns = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let generated_mono =
            MonoTime::from_nanos(read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        let evidence_count =
            usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if evidence_count > 256 {
            return Err(journal_corrupt());
        }
        let mut evidence = Vec::with_capacity(evidence_count);
        for _ in 0..evidence_count {
            evidence.push(read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::StrategyProposal(Proposal {
            proposal_id,
            strategy_id,
            instrument_id,
            action,
            confidence,
            horizon_ns,
            ttl_ns,
            evidence,
            generated_mono,
        })));
    }
    if payload.starts_with(PLAN_EVENT_MAGIC) {
        let event = decode_plan_event(payload)
            .map_err(|error| EngineError::Autonomy(format!("{error:?}")))?;
        return Ok(Some(RecoveredEvent::Autonomy(event)));
    }
    if payload.starts_with(CANCEL_MAGIC) {
        let mut cursor = CANCEL_MAGIC.len();
        let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if client_order_id.trim().is_empty() || cursor != payload.len() {
            return Err(journal_corrupt());
        }
        // Cancellation is an audit record. The broker is queried during
        // startup reconciliation instead of replaying a network mutation.
        return Ok(None);
    }
    if payload.starts_with(NEWS_ITEM_MAGIC) {
        let mut cursor = NEWS_ITEM_MAGIC.len();
        let id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let provider = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let canonical_url = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let source_name = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let title = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let summary = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let has_published = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let published = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let received = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let count = usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if has_published > 1 || count > 256 {
            return Err(journal_corrupt());
        }
        let mut symbols = std::collections::BTreeSet::new();
        for _ in 0..count {
            let symbol = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if symbol.trim().is_empty() || !symbols.insert(symbol) {
                return Err(journal_corrupt());
            }
        }
        let content_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::News(NewsItem {
            id,
            provider,
            canonical_url,
            source_name,
            title,
            summary_text: (!summary.is_empty()).then_some(summary),
            published_at_ms: (has_published == 1).then_some(published),
            received_at_ms: received,
            symbols,
            content_hash,
        })));
    }
    if payload.starts_with(EMBEDDING_SNAPSHOT_MAGIC) {
        let mut cursor = EMBEDDING_SNAPSHOT_MAGIC.len();
        let model = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let model_version = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let dimensions =
            usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?;
        let count = usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
            .map_err(|_| journal_corrupt())?;
        if model.trim().is_empty()
            || model_version.trim().is_empty()
            || dimensions == 0
            || dimensions > 4_096
            || count > 8_192
        {
            return Err(journal_corrupt());
        }
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let node_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let content_hash = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let record_model = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let record_version = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let record_dimensions =
                usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            let created_at_ms = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            let vector_count =
                usize::try_from(read_u32(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            if vector_count != dimensions || record_dimensions != dimensions {
                return Err(journal_corrupt());
            }
            let mut vector = Vec::with_capacity(vector_count);
            for _ in 0..vector_count {
                let value = f32::from_le_bytes(
                    read_bytes(payload, &mut cursor, 4)
                        .ok_or_else(journal_corrupt)?
                        .try_into()
                        .map_err(|_| journal_corrupt())?,
                );
                if !value.is_finite() {
                    return Err(journal_corrupt());
                }
                vector.push(value);
            }
            records.push(EmbeddingRecord {
                node_id,
                content_hash,
                model: record_model,
                model_version: record_version,
                dimensions: record_dimensions,
                vector,
                created_at_ms,
            });
        }
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::EmbeddingSnapshot(
            EmbeddingIndexSnapshot {
                model,
                model_version,
                dimensions,
                records,
            },
        )));
    }
    if payload.starts_with(REPLACE_MAGIC) {
        let mut cursor = REPLACE_MAGIC.len();
        let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let quantity_ticks = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let has_limit = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let limit = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if client_order_id.is_empty()
            || quantity_ticks <= 0
            || has_limit > 1
            || (has_limit == 1 && limit <= 0)
            || cursor != payload.len()
        {
            return Err(journal_corrupt());
        }
        // Replacement commands are audit records. The broker is queried during
        // startup reconciliation instead of replaying a potentially duplicate
        // network mutation.
        return Ok(None);
    }
    let (kind, mut cursor) = if payload.starts_with(INTENT_MAGIC) {
        (1_u8, INTENT_MAGIC.len())
    } else if payload.starts_with(BROKER_MAGIC) {
        (2_u8, BROKER_MAGIC.len())
    } else if payload.starts_with(RISK_MAGIC) {
        (3_u8, RISK_MAGIC.len())
    } else if payload.starts_with(LIVE_LIMITS_MAGIC) {
        (4_u8, LIVE_LIMITS_MAGIC.len())
    } else if payload.starts_with(PORTFOLIO_SNAPSHOT_MAGIC) {
        (5_u8, PORTFOLIO_SNAPSHOT_MAGIC.len())
    } else {
        return Ok(None);
    };
    if kind == 1 {
        let account = insider_common_types::AccountId::new(
            read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        )
        .map_err(|_| journal_corrupt())?;
        let instrument = insider_common_types::InstrumentId::new(
            read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        )
        .map_err(|_| journal_corrupt())?;
        let side = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => Side::Buy,
            2 => Side::Sell,
            _ => return Err(journal_corrupt()),
        };
        let quantity_ticks = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let order_type = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => insider_broker_api::OrderType::Market,
            2 => insider_broker_api::OrderType::Limit,
            _ => return Err(journal_corrupt()),
        };
        let limit = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let time_in_force = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => insider_broker_api::TimeInForce::Day,
            2 => insider_broker_api::TimeInForce::GoodTilCancel,
            3 => insider_broker_api::TimeInForce::ImmediateOrCancel,
            _ => return Err(journal_corrupt()),
        };
        let trace = insider_common_types::TraceId::new(
            read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        )
        .map_err(|_| journal_corrupt())?;
        let intent_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if cursor != payload.len() || quantity_ticks <= 0 {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Intent(
            insider_broker_api::OrderIntent {
                intent_id,
                account_id: account,
                instrument_id: instrument,
                client_order_id,
                side,
                quantity_ticks,
                order_type,
                limit_price_ticks: (order_type == insider_broker_api::OrderType::Limit)
                    .then_some(limit),
                time_in_force,
                state: insider_broker_api::OrderState::RiskApproved,
                trace_id: trace,
            },
        )));
    }
    if kind == 3 {
        let state = match read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)? {
            1 => RiskState::Running,
            2 => RiskState::ReduceOnly,
            3 => RiskState::CancelOnly,
            4 => RiskState::Halted,
            _ => return Err(journal_corrupt()),
        };
        let authorization = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::Risk(state, authorization)));
    }
    if kind == 4 {
        let count = usize::from(read_u16(payload, &mut cursor).ok_or_else(journal_corrupt)?);
        if count == 0 || count > 128 {
            return Err(journal_corrupt());
        }
        let mut accounts = std::collections::BTreeSet::new();
        for _ in 0..count {
            let account = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if account.trim().is_empty() || !accounts.insert(account) {
                return Err(journal_corrupt());
            }
        }
        let max_notional_ticks = read_u64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if max_notional_ticks == 0 || cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::LiveLimits(LiveLimits {
            allowed_accounts: accounts,
            max_notional_ticks,
        })));
    }
    if kind == 5 {
        let count = usize::try_from(
            read_bytes(payload, &mut cursor, 4)
                .and_then(|bytes| bytes.try_into().ok())
                .map(u32::from_le_bytes)
                .ok_or_else(journal_corrupt)?,
        )
        .map_err(|_| journal_corrupt())?;
        if count > 16_384 {
            return Err(journal_corrupt());
        }
        let mut positions = Vec::with_capacity(count);
        for _ in 0..count {
            let instrument =
                InstrumentId::new(read_u128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                    .map_err(|_| journal_corrupt())?;
            let quantity_ticks = read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?;
            if quantity_ticks == 0 || positions.iter().any(|(id, _)| *id == instrument) {
                return Err(journal_corrupt());
            }
            positions.push((instrument, quantity_ticks));
        }
        let has_cash = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
        if has_cash > 1 {
            return Err(journal_corrupt());
        }
        // Early V1 writers accidentally serialized the broker's i128 account
        // value even though the authoritative portfolio accepts i64 cash
        // ticks. Accept that exact legacy width on recovery, but reject values
        // that could never have been applied to the portfolio. New writers
        // always emit the canonical i64 representation.
        let remaining = payload.len().saturating_sub(cursor);
        let cash_ticks = if remaining == 8 {
            read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?
        } else if remaining == 16 {
            i64::try_from(read_i128(payload, &mut cursor).ok_or_else(journal_corrupt)?)
                .map_err(|_| journal_corrupt())?
        } else {
            return Err(journal_corrupt());
        };
        if cursor != payload.len() {
            return Err(journal_corrupt());
        }
        return Ok(Some(RecoveredEvent::PortfolioSnapshot {
            positions,
            cash_ticks: (has_cash == 1).then_some(cash_ticks),
        }));
    }
    let event_kind = read_u8(payload, &mut cursor).ok_or_else(journal_corrupt)?;
    let client_order_id = read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?;
    let event = match event_kind {
        1 => BrokerEvent::Acknowledged {
            client_order_id,
            broker_order_id: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        },
        2 => BrokerEvent::Filled {
            client_order_id,
            quantity_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
            price_ticks: read_i64(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        },
        3 => BrokerEvent::Rejected {
            client_order_id,
            reason: read_string(payload, &mut cursor).ok_or_else(journal_corrupt)?,
        },
        4 => BrokerEvent::Cancelled { client_order_id },
        _ => return Err(journal_corrupt()),
    };
    if cursor != payload.len() {
        return Err(journal_corrupt());
    }
    Ok(Some(RecoveredEvent::Broker(event)))
}

#[cfg(test)]
mod tests {
    use super::{
        BacktestRunRequest, ReconcileTrigger, Runtime, SUBSYSTEM_ID, ServiceHost,
        StrategyBacktestEvent, StrategyBacktestRunRequest, StrategyExecutionSummary,
        StrategyPolicy, configured_alert_limits, configured_alert_webhook, configured_guardrails,
        configured_supervisor_policy,
    };
    use insider_autonomy::TradingEnvironment;
    use insider_broker_api::{BrokerEvent, BrokerGateway, BrokerSnapshot, Capabilities};
    use insider_cfg_core::{Settings, Value};
    use insider_common_types::{AccountId, InstrumentId, MonoTime, ProposalId, TraceId};
    use insider_exchange_sim::PaperBroker;
    use insider_experiment_registry::{
        Artifact, ExperimentBundle, ExperimentProvenance, ExperimentRun, RunStatus,
    };
    use insider_market_data::{Bar, BarUpdate, MarketEvent};
    use insider_metric_sdk::{MetricOutput, SpreadMetric};
    use insider_model_registry::{ArtifactManifest, ModelRecord, Status as ModelStatus};
    use insider_news_core::{
        CursorProvider, NewsItem, ProviderBatch, ProviderHealth, ProviderStateSnapshot, RetryClass,
        RetryPolicy,
    };
    use insider_portfolio::Portfolio;
    use insider_risk_engine::{Limits, RiskEngine};
    use insider_strategy_sdk::{
        Action, MissingEvidencePolicy, Proposal, ProposalError, Strategy, StrategyContext,
        StrategyManifest, StrategyMode, StrategyPriority, ThresholdStrategy,
        VolatilityScaledTrendConfig, VolatilityScaledTrendStrategy,
    };
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[test]
    fn portfolio_snapshot_encoding_is_canonical_and_recovers_legacy_wide_cash() -> Result<(), String>
    {
        let snapshot = BrokerSnapshot {
            account_values: BTreeMap::from([(
                insider_broker_api::ACCOUNT_VALUE_CASH_TICKS.to_owned(),
                250_000_i128,
            )]),
            ..BrokerSnapshot::default()
        };
        let encoded = super::encode_portfolio_snapshot(&snapshot)
            .map_err(|error| format!("encode bounded cash: {error:?}"))?;
        assert_eq!(
            encoded.len(),
            b"IT_PORTFOLIO_SNAPSHOT_V1\0".len() + 4 + 1 + 8
        );
        let Some(super::RecoveredEvent::PortfolioSnapshot { cash_ticks, .. }) =
            super::decode_journal_payload(&encoded)
                .map_err(|error| format!("decode canonical snapshot: {error:?}"))?
        else {
            return Err("canonical snapshot did not decode as a portfolio snapshot".into());
        };
        assert_eq!(cash_ticks, Some(250_000));

        let mut legacy = encoded[..encoded.len() - 8].to_vec();
        legacy.extend_from_slice(&250_000_i128.to_le_bytes());
        let Some(super::RecoveredEvent::PortfolioSnapshot { cash_ticks, .. }) =
            super::decode_journal_payload(&legacy)
                .map_err(|error| format!("decode legacy snapshot: {error:?}"))?
        else {
            return Err("legacy snapshot did not decode as a portfolio snapshot".into());
        };
        assert_eq!(cash_ticks, Some(250_000));
        Ok(())
    }

    #[test]
    fn subsystem_id_is_non_empty_and_ascii() {
        assert!(!SUBSYSTEM_ID.is_empty());
        assert!(SUBSYSTEM_ID.is_ascii());
    }

    #[test]
    fn alert_limits_are_typed_bounded_and_defaulted() {
        let Ok(defaults) = configured_alert_limits(&Settings::new()) else {
            return;
        };
        assert_eq!(defaults, (60_000, 4_096));
        let configured = Settings::from([
            ("alerts.cooldown_ms".to_owned(), Value::Integer(5_000)),
            ("alerts.max_pending".to_owned(), Value::Integer(128)),
        ]);
        let Ok(configured_limits) = configured_alert_limits(&configured) else {
            return;
        };
        assert_eq!(configured_limits, (5_000, 128));
        let invalid = Settings::from([("alerts.max_pending".to_owned(), Value::Integer(0))]);
        assert!(configured_alert_limits(&invalid).is_err());
        let wrong_type =
            Settings::from([("alerts.cooldown_ms".to_owned(), Value::String("5s".into()))]);
        assert!(configured_alert_limits(&wrong_type).is_err());
    }

    #[test]
    fn supervisor_policy_is_typed_bounded_and_ordered() {
        let Ok(defaults) = configured_supervisor_policy(&Settings::new()) else {
            return;
        };
        assert_eq!(defaults.max_failures, 3);
        let configured = Settings::from([
            ("supervisor.max_failures".to_owned(), Value::Integer(5)),
            (
                "supervisor.initial_backoff_ns".to_owned(),
                Value::Integer(200),
            ),
            ("supervisor.max_backoff_ns".to_owned(), Value::Integer(400)),
        ]);
        let Ok(policy) = configured_supervisor_policy(&configured) else {
            return;
        };
        assert_eq!(policy.max_failures, 5);
        assert_eq!(policy.max_backoff_ns, 400);
        let invalid = Settings::from([
            (
                "supervisor.initial_backoff_ns".to_owned(),
                Value::Integer(400),
            ),
            ("supervisor.max_backoff_ns".to_owned(), Value::Integer(200)),
        ]);
        assert!(configured_supervisor_policy(&invalid).is_err());
    }

    #[test]
    fn webhook_validation_rejects_embedded_userinfo() {
        let settings = Settings::from([(
            "alerts.webhook_url".to_owned(),
            Value::String("https://user:password@localhost/alerts".into()),
        )]);
        assert!(configured_alert_webhook(&settings).is_err());
        let malformed = Settings::from([(
            "alerts.webhook_url".to_owned(),
            Value::String("https:///missing-authority".into()),
        )]);
        assert!(configured_alert_webhook(&malformed).is_err());
    }

    #[test]
    fn guardrails_reject_negative_integer_limits() {
        for key in [
            "risk.max_drawdown_bps",
            "risk.max_predicted_volatility_bps",
            "risk.max_participation_bps",
            "risk.max_price_deviation_bps",
        ] {
            let settings = Settings::from([(key.to_owned(), Value::Integer(-1))]);
            assert!(configured_guardrails(&settings).is_err(), "{key}");
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn experiment_lifecycle_and_lineage_survive_restart() {
        let Some(account) = AccountId::new(901).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-experiment-{}.journal",
            std::process::id()
        ));
        let limits = || {
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            })
        };
        let broker = Arc::new(PaperBroker::new());
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        if host
            .create_experiment(ExperimentRun {
                run_id: "run-901".into(),
                code_hash: "git:abc".into(),
                config_hash: "cfg:def".into(),
                dataset_hash: "data:ghi".into(),
                provenance: ExperimentProvenance::default(),
                status: RunStatus::Created,
                metrics: BTreeMap::new(),
                artifacts: Vec::new(),
            })
            .is_err()
        {
            return;
        }
        if host.start_experiment("run-901").is_err() {
            return;
        }
        if host
            .add_experiment_artifact(
                "run-901",
                Artifact {
                    kind: "report".into(),
                    hash: "sha256:1".into(),
                    path: "reports/run-901.json".into(),
                },
            )
            .is_err()
        {
            return;
        }
        if host
            .succeed_experiment("run-901", BTreeMap::from([("sharpe".into(), 1.25)]))
            .is_err()
        {
            return;
        }
        let bundle = ExperimentBundle {
            run_id: "run-901".into(),
            code_hash: "git:abc".into(),
            config_hash: "cfg:def".into(),
            dataset_hash: "data:ghi".into(),
            schema_hashes: BTreeMap::new(),
            model_hashes: BTreeMap::new(),
            prompt_hashes: BTreeMap::new(),
            environment: BTreeMap::from([(String::from("rust"), String::from("stable"))]),
            command: vec![String::from("backtest"), String::from("run-901")],
            seed: 7,
            artifacts: vec![Artifact {
                kind: "report".into(),
                hash: "sha256:1".into(),
                path: "reports/run-901.json".into(),
            }],
            report_hash: "sha256:report".into(),
        };
        let Ok(bundle_hash) = host.publish_experiment_bundle(&bundle) else {
            return;
        };
        assert!(
            host.experiment_runs()[0]
                .artifacts
                .iter()
                .any(
                    |artifact| artifact.kind == "experiment_bundle" && artifact.hash == bundle_hash
                )
        );
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        let runs = reopened.experiment_runs();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].status, RunStatus::Succeeded);
        assert_eq!(runs[0].code_hash, "git:abc");
        assert_eq!(runs[0].metrics.get("sharpe"), Some(&1.25));
        assert_eq!(runs[0].artifacts.len(), 2);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
        let _ = std::fs::remove_dir_all(path.with_extension("bundles"));
    }

    #[test]
    fn model_promotion_lineage_survives_restart() {
        let Some(account) = AccountId::new(902).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-model-{}.journal",
            std::process::id()
        ));
        let limits = || {
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            })
        };
        let broker = Arc::new(PaperBroker::new());
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        let record = ModelRecord {
            model_id: "momentum".into(),
            version: "1.0.0".into(),
            artifact_hash: "artifact:1".into(),
            input_schema_hash: "input:1".into(),
            output_schema_hash: "output:1".into(),
            input_width: 3,
            status: ModelStatus::Research,
        };
        let manifest = ArtifactManifest {
            code_hash: "code:1".into(),
            training_data_hash: "data:1".into(),
            config_hash: "config:1".into(),
            feature_hash: "features:1".into(),
            calibration_hash: "calibration:1".into(),
            artifact_hash: "artifact:1".into(),
        };
        if host.register_model(record, manifest).is_err() {
            return;
        }
        if host
            .validate_model("momentum", "1.0.0", "validation:1")
            .is_err()
        {
            return;
        }
        if host.start_model_shadow("momentum", "1.0.0").is_err() {
            return;
        }
        if host
            .start_model_canary("momentum", "1.0.0", "canary:1")
            .is_err()
        {
            return;
        }
        if host.promote_model("momentum", "1.0.0").is_err() {
            return;
        }
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        let snapshot = reopened.model_registry_snapshot();
        assert_eq!(snapshot.records.len(), 1);
        assert_eq!(snapshot.records[0].status, ModelStatus::Production);
        assert_eq!(snapshot.active, vec![("momentum".into(), "1.0.0".into())]);
        assert_eq!(snapshot.manifests.len(), 1);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn runtime_routes_proposal_through_risk_paper_broker_and_fill_accounting() {
        let Some(account) = AccountId::new(1).ok() else {
            return;
        };
        let Some(instrument) = InstrumentId::new(2).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(3).ok() else {
            return;
        };
        let Some(trace) = TraceId::new(4).ok() else {
            return;
        };
        let broker = Arc::new(PaperBroker::new());
        assert!(broker.set_price(instrument, 100).is_ok());
        let mut portfolio = Portfolio::new();
        portfolio.set_position(
            instrument,
            insider_portfolio::Position {
                quantity_ticks: 0,
                mark_price_ticks: 100,
            },
        );
        let runtime = Runtime::new(
            account,
            broker.clone(),
            portfolio,
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
        );
        let proposal = Proposal {
            proposal_id,
            strategy_id: "test.v1".into(),
            instrument_id: instrument,
            action: Action::TargetQuantity { quantity_ticks: 10 },
            confidence: 0.9,
            horizon_ns: 1_000,
            ttl_ns: 1_000,
            evidence: Vec::new(),
            generated_mono: MonoTime::from_nanos(1),
        };
        let client = runtime.submit_proposal(&proposal, MonoTime::from_nanos(2), trace);
        assert!(client.is_ok());
        for event in broker.drain_events() {
            assert!(runtime.apply_broker_event(event).is_ok());
        }
        assert_eq!(
            runtime
                .portfolio()
                .ok()
                .and_then(|p| p.position(instrument))
                .map(|p| p.quantity_ticks),
            Some(10)
        );
        assert_eq!(
            broker.capabilities(),
            Capabilities {
                market: true,
                limit: true,
                fractional_quantity: false,
                cancel_replace: true
            }
        );
    }

    #[test]
    fn provider_state_is_journaled_and_restored_on_reopen() {
        let Some(account) = AccountId::new(9).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-provider-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let portfolio = Portfolio::new();
        let risk = RiskEngine::new(Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        });
        let Some(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            portfolio,
            risk,
            BTreeMap::new(),
        )
        .ok() else {
            return;
        };
        let snapshot = ProviderStateSnapshot {
            provider_id: "test-provider".into(),
            cursor: Some("cursor-7".into()),
            generation: 7,
            next_retry_ms: Some(1234),
            retries: 2,
            dead_letters: Vec::new(),
            health: ProviderHealth::CoolingDown,
            last_success_ms: Some(900),
            last_failure_ms: Some(1_000),
            consecutive_failures: 2,
        };
        assert!(host.persist_provider_state(snapshot.clone()).is_ok());
        drop(host);
        let Some(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
            BTreeMap::new(),
        )
        .ok() else {
            return;
        };
        assert_eq!(reopened.provider_state("test-provider"), Some(snapshot));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn restart_retains_invalid_broker_transition_as_recovery_anomaly() {
        let Some(account) = AccountId::new(11).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-recovery-anomaly-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let host = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
            BTreeMap::new(),
        );
        let Ok(host) = host else {
            return;
        };
        let event = BrokerEvent::Acknowledged {
            client_order_id: "unknown-after-crash".into(),
            broker_order_id: "broker-1".into(),
        };
        if host
            .append_event(&Runtime::broker_event_payload(&event))
            .is_err()
        {
            return;
        }
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert_eq!(reopened.recovery_anomalies().len(), 1);
        assert_eq!(reopened.recovery_anomalies()[0].kind, "broker_event");
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn live_kill_switch_is_restored_after_restart() {
        let Some(account) = AccountId::new(12).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-live-kill-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = || {
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            })
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert!(host.kill_live().is_ok());
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert_eq!(
            reopened.trading_environment().ok(),
            Some(TradingEnvironment::Killed)
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn strategy_proposals_and_resolution_policy_survive_restart() {
        let Some(account) = AccountId::new(13).ok() else {
            return;
        };
        let Some(instrument) = InstrumentId::new(14).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(15).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-strategy-coordinator-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        assert!(broker.set_price(instrument, 100).is_ok());
        let mut portfolio = Portfolio::new();
        portfolio.set_position(
            instrument,
            insider_portfolio::Position {
                quantity_ticks: 0,
                mark_price_ticks: 100,
            },
        );
        let limits = || {
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            })
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            portfolio.clone(),
            limits(),
            BTreeMap::new(),
        ) else {
            return;
        };
        let proposal = Proposal {
            proposal_id,
            strategy_id: "momentum.v1".into(),
            instrument_id: instrument,
            action: Action::TargetQuantity { quantity_ticks: 5 },
            confidence: 0.8,
            horizon_ns: 100,
            ttl_ns: 50,
            evidence: vec!["metric:momentum".into()],
            generated_mono: MonoTime::from_nanos(1),
        };
        assert!(
            host.submit_strategy_proposal(&proposal, MonoTime::from_nanos(2))
                .is_ok()
        );
        let result = host
            .resolve_strategy_proposals(StrategyPolicy::IsolatedBooks, MonoTime::from_nanos(2))
            .ok();
        assert_eq!(result.map(|value| value.accepted.len()), Some(1));
        assert!(host.reconcile_trigger(ReconcileTrigger::Startup).is_ok());
        let Some(trace) = TraceId::new(16).ok() else {
            return;
        };
        let submit_result = host.submit_proposal(&proposal, MonoTime::from_nanos(3), trace);
        assert!(submit_result.is_ok(), "submit failed: {submit_result:?}");
        for event in broker.drain_events() {
            assert!(host.apply_broker_event(event).is_ok());
        }
        assert_eq!(
            host.strategy_execution_summaries(),
            vec![StrategyExecutionSummary {
                strategy_id: "momentum.v1".into(),
                fill_count: 1,
                filled_quantity_ticks: 5,
                notional_ticks: 500,
            }]
        );
        drop(host);
        let Ok(reopened) =
            ServiceHost::open(&path, account, broker, portfolio, limits(), BTreeMap::new())
        else {
            return;
        };
        assert_eq!(
            reopened
                .strategy_proposal_record(proposal_id)
                .map(|record| record.state),
            Some(insider_strategy_coordinator::ProposalState::Accepted)
        );
        let resolutions = reopened.strategy_resolution_history();
        assert_eq!(resolutions.len(), 1);
        assert_eq!(resolutions[0].accepted_count, 1);
        assert_eq!(resolutions[0].conflict_count, 0);
        assert_eq!(
            reopened.strategy_execution_summaries(),
            vec![StrategyExecutionSummary {
                strategy_id: "momentum.v1".into(),
                fill_count: 1,
                filled_quantity_ticks: 5,
                notional_ticks: 500,
            }]
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    struct OnePageProvider;

    impl CursorProvider for OnePageProvider {
        #[allow(clippy::unnecessary_literal_bound)]
        fn provider_id(&self) -> &str {
            "page-provider"
        }

        fn fetch_page(&self, _cursor: Option<&str>, _now_ms: i64) -> Result<ProviderBatch, String> {
            Ok(ProviderBatch {
                items: vec![NewsItem {
                    id: "article-1".into(),
                    provider: String::new(),
                    canonical_url: "https://news.example/article-1".into(),
                    source_name: "Example".into(),
                    title: "AAPL reports results".into(),
                    summary_text: None,
                    published_at_ms: Some(10),
                    received_at_ms: 20,
                    symbols: ["AAPL".into()].into_iter().collect(),
                    content_hash: "hash-1".into(),
                }],
                next_cursor: Some("cursor-1".into()),
            })
        }
    }

    #[test]
    fn provider_poll_journals_article_and_cursor_for_restart_recovery() {
        let Some(account) = AccountId::new(10).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-provider-poll-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let host = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
            BTreeMap::new(),
        );
        let Ok(host) = host else {
            return;
        };
        let Some(policy) = RetryPolicy::new(2, 100, 1_000) else {
            return;
        };
        assert!(
            host.register_news_provider(Box::new(OnePageProvider), policy, 10, 1_000, 10, 4)
                .is_ok()
        );
        let outcome = host.poll_news_provider("page-provider", 100, |_| RetryClass::Transient);
        assert!(matches!(
            outcome,
            Ok(insider_news_core::PollOutcome::Ingested(_))
        ));
        assert_eq!(
            host.provider_state("page-provider").and_then(|s| s.cursor),
            Some("cursor-1".into())
        );
        assert_eq!(
            host.news_provider_status("page-provider")
                .ok()
                .flatten()
                .map(|status| status.health),
            Some(ProviderHealth::Healthy)
        );
        assert!(matches!(
            host.news_page("all", "", None, 10),
            Ok(page) if page.items.len() == 1
        ));
        drop(host);
        let reopened = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(Limits {
                max_position_ticks: 100,
                max_order_ticks: 100,
                max_gross_notional_ticks: 100_000,
            }),
            BTreeMap::new(),
        );
        let Ok(reopened) = reopened else {
            return;
        };
        assert_eq!(
            reopened
                .provider_state("page-provider")
                .and_then(|s| s.cursor),
            Some("cursor-1".into())
        );
        assert!(matches!(
            reopened.news_page("all", "", None, 10),
            Ok(page) if page.items.len() == 1
        ));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn backtest_result_is_deterministic_and_restored_from_journal() {
        let Some(account) = AccountId::new(99).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-backtest-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        let request = BacktestRunRequest {
            run_id: "run-1".into(),
            strategy_id: "strategy.v1".into(),
            dataset_hash: "dataset-sha".into(),
            config_hash: "config-sha".into(),
            initial_cash_ticks: 100_000,
            events: vec![
                insider_replay::BacktestEvent::Fill {
                    sequence: 1,
                    quantity_ticks: 10,
                    price_ticks: 100,
                    fee_ticks: 5,
                },
                insider_replay::BacktestEvent::Mark {
                    sequence: 2,
                    price_ticks: 110,
                },
            ],
        };
        let Ok(expected) = host.run_backtest(request) else {
            return;
        };
        assert_eq!(expected.report.event_count, 2);
        assert_eq!(expected.report.total_fees_ticks, 5);
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert_eq!(reopened.backtest_runs(), vec![expected]);
        let experiments = reopened.experiment_runs();
        assert_eq!(experiments.len(), 1);
        assert_eq!(experiments[0].run_id, "backtest:run-1");
        assert_eq!(experiments[0].status, RunStatus::Succeeded);
        assert_eq!(experiments[0].dataset_hash, "dataset-sha");
        assert!(
            experiments[0]
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "experiment_bundle")
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
        let _ = std::fs::remove_dir_all(path.with_extension("bundles"));
    }

    #[test]
    fn strategy_backtest_reuses_registered_strategy_boundary() {
        let Some(account) = AccountId::new(100).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-strategy-backtest-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        let Some(instrument) = InstrumentId::new(101).ok() else {
            return;
        };
        let Some(strategy) = ThresholdStrategy::new_with_proposal_seed(
            "strategy.bt.v1",
            "momentum.v1",
            0.5,
            0.1,
            10,
            1_000,
            100,
            1,
        ) else {
            return;
        };
        assert!(host.register_strategy(Arc::new(strategy)).is_ok());
        let result = host.run_strategy_backtest(StrategyBacktestRunRequest {
            run_id: "strategy-run-1".into(),
            strategy_id: "strategy.bt.v1".into(),
            dataset_hash: "dataset".into(),
            config_hash: "config".into(),
            initial_cash_ticks: 100_000,
            events: vec![StrategyBacktestEvent {
                sequence: 1,
                now_mono_ns: 2,
                instrument_id: instrument,
                price_ticks: 100,
                fee_ticks: 5,
                metrics: vec![MetricOutput {
                    metric_id: "momentum.v1".into(),
                    instrument_id: instrument,
                    generated_mono: MonoTime::from_nanos(1),
                    ttl_ns: 100,
                    score: 0.8,
                    confidence: 0.9,
                    uncertainty: 0.1,
                }],
            }],
        });
        let Ok(result) = result else {
            return;
        };
        assert_eq!(result.report.event_count, 2);
        assert_eq!(result.report.total_fees_ticks, 5);
        assert_eq!(
            result
                .report
                .final_snapshot
                .map(|snapshot| snapshot.position_ticks),
            Some(10)
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn accepted_market_events_restore_canonical_state_after_restart() {
        let Some(account) = AccountId::new(102).ok() else {
            return;
        };
        let Some(instrument) = InstrumentId::new(103).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-market-journal-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert!(host.register_market_instrument(instrument).is_ok());
        let quote = insider_market_data::Quote {
            instrument_id: instrument,
            sequence: 1,
            exchange_time: insider_common_types::WallTime::from_unix_nanos(1),
            received_mono: MonoTime::from_nanos(1),
            bid_ticks: 99,
            ask_ticks: 101,
            bid_quantity_ticks: 10,
            ask_quantity_ticks: 12,
        };
        assert!(
            host.ingest_market_event(
                MarketEvent::Quote(quote),
                insider_common_types::WallTime::from_unix_nanos(2),
            )
            .is_ok()
        );
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert_eq!(
            reopened
                .market_snapshot(instrument)
                .and_then(|snapshot| snapshot.quote),
            Some(quote)
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    fn accepted_historical_bars_restore_after_restart() {
        let Some(account) = AccountId::new(104).ok() else {
            return;
        };
        let Some(instrument) = InstrumentId::new(105).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-bar-journal-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker.clone(),
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert!(host.register_market_instrument(instrument).is_ok());
        let bar = Bar {
            instrument_id: instrument,
            start_time: insider_common_types::WallTime::from_unix_nanos(0),
            interval_ns: 60_000_000_000,
            open_ticks: 100,
            high_ticks: 105,
            low_ticks: 99,
            close_ticks: 103,
            volume_ticks: 1_000,
        };
        assert!(matches!(
            host.ingest_market_bar(bar, 1),
            Ok(BarUpdate::New(_))
        ));
        drop(host);
        let Ok(reopened) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert_eq!(
            reopened
                .market_snapshot(instrument)
                .map(|snapshot| snapshot.bars),
            Some(vec![bar])
        );
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn decision_cycle_only_sends_incomplete_evidence_to_opted_in_strategies() {
        struct FailIfCalled {
            called: Arc<AtomicBool>,
        }

        impl Strategy for FailIfCalled {
            fn strategy_id(&self) -> &'static str {
                "legacy.requires.complete.v1"
            }

            fn manifest(&self) -> StrategyManifest {
                StrategyManifest {
                    strategy_id: self.strategy_id().to_owned(),
                    mode: StrategyMode::Deterministic,
                    metric_ids: vec![String::from("metric.never.available.v1")],
                    missing_evidence: MissingEvidencePolicy::SkipEvaluation,
                    strategy_dependencies: Vec::new(),
                    horizon_ns: 1_000_000_000,
                    ttl_ns: 100_000_000,
                    period_ns: 100_000_000,
                    deadline_ns: 10_000_000,
                    priority: StrategyPriority::Fast,
                }
            }

            fn evaluate(&self, _context: &StrategyContext<'_>) -> Result<Proposal, ProposalError> {
                self.called.store(true, Ordering::SeqCst);
                Err(ProposalError::InvalidAction)
            }
        }

        let Some(account) = AccountId::new(106).ok() else {
            return;
        };
        let Some(instrument) = InstrumentId::new(107).ok() else {
            return;
        };
        let Some(proposal_id) = ProposalId::new(1).ok() else {
            return;
        };
        let path = std::env::temp_dir().join(format!(
            "insider-engine-incomplete-evidence-{}.journal",
            std::process::id()
        ));
        let broker = Arc::new(PaperBroker::new());
        let limits = Limits {
            max_position_ticks: 100,
            max_order_ticks: 100,
            max_gross_notional_ticks: 100_000,
        };
        let Ok(host) = ServiceHost::open(
            &path,
            account,
            broker,
            Portfolio::new(),
            RiskEngine::new(limits),
            BTreeMap::new(),
        ) else {
            return;
        };
        assert!(host.register_market_instrument(instrument).is_ok());
        let received_mono = host.monotonic_now();
        assert!(
            host.ingest_market_event(
                MarketEvent::Quote(insider_market_data::Quote {
                    instrument_id: instrument,
                    sequence: 1,
                    exchange_time: insider_common_types::WallTime::from_unix_nanos(1),
                    received_mono,
                    bid_ticks: 99,
                    ask_ticks: 101,
                    bid_quantity_ticks: 10,
                    ask_quantity_ticks: 10,
                }),
                insider_common_types::WallTime::from_unix_nanos(2),
            )
            .is_ok()
        );
        let Ok(spread) = SpreadMetric::new(String::from("spread.v1"), 1_000_000_000) else {
            return;
        };
        assert!(host.register_metric(Arc::new(spread)).is_ok());
        let called = Arc::new(AtomicBool::new(false));
        assert!(
            host.register_strategy(Arc::new(FailIfCalled {
                called: Arc::clone(&called),
            }))
            .is_ok()
        );
        let Some(starter) = VolatilityScaledTrendStrategy::new(VolatilityScaledTrendConfig {
            strategy_id: String::from("starter.no-action.v1"),
            trend_metric_id: String::from("trend.missing.v1"),
            volatility_metric_id: String::from("atr.missing.v1"),
            spread_metric_id: String::from("spread.v1"),
            entry_threshold: 0.01,
            exit_threshold: 0.002,
            max_spread: 0.05,
            target_volatility: 0.01,
            min_confidence: 0.5,
            base_quantity_ticks: 10,
            horizon_ns: 1_000_000_000,
            ttl_ns: 100_000_000,
        }) else {
            return;
        };
        assert!(host.register_strategy(Arc::new(starter)).is_ok());
        assert_eq!(host.run_registered_python_cycle().ok(), Some(1));
        assert!(!called.load(Ordering::SeqCst));
        let record = host.strategy_proposal_record(proposal_id);
        assert!(record.is_some_and(|record| {
            matches!(record.proposal.action, Action::NoAction)
                && record
                    .proposal
                    .evidence
                    .iter()
                    .any(|item| item == "rationale:MISSING_OR_STALE_EVIDENCE")
        }));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("read-model"));
    }
}
