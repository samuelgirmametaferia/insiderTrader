//! Authenticated IPC command service for the headless engine and terminal clients.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use insider_common_types::{InstrumentId, MonoTime, ProposalId, TraceId};
use insider_context_graph::RetrievalQuery;
use insider_experiment_registry::{
    Artifact as ExperimentArtifact, ExperimentProvenance, ExperimentRun, RunStatus,
};
use insider_instrument_master::Catalog;
use insider_ipc::{
    CapabilityPolicy, CommandDispatcher, CommandEnvelope, CommandResponse, DispatchError,
};
use insider_llm_core::{ActionType, AutonomousAction, Endpoint, Request as LlmRequest, StreamItem};
use insider_market_types::AssetClass;
use insider_metric_sdk::MetricOutput;
use insider_model_registry::{ArtifactManifest, ModelRecord};
use insider_model_registry::{RegistrySnapshot as ModelRegistrySnapshot, Status as ModelStatus};
use insider_risk_engine::{
    Limits as RiskLimits, ScopedRiskPolicy, ScopedRiskPolicySnapshot, State as RiskState,
    TimedLimits,
};
use insider_strategy_coordinator::{BudgetedResultSet, Policy as StrategyPolicy, StrategyBudget};

use crate::{
    BacktestRunRequest, ManualOrderPreview, RecoveredEvent, ServiceHost, StrategyBacktestEvent,
    StrategyBacktestRunRequest, decode_journal_payload, encode_order_intent,
};
use insider_execution::Schedule;

const SNAPSHOT: u8 = 1;
const PREVIEW: u8 = 2;
const SUBMIT: u8 = 3;
const CANCEL: u8 = 17;
const REPLACE: u8 = 18;
const RESOLVE_SYMBOL: u8 = 4;
const EVENTS: u8 = 5;
const LIVE_CONFIGURE: u8 = 6;
const LIVE_ARM: u8 = 7;
const LIVE_CONFIRM: u8 = 8;
const LIVE_KILL: u8 = 9;
const AUTONOMY_SUBMIT: u8 = 10;
const AUTONOMY_TRANSITION: u8 = 11;
const READ_MODEL_BACKUP: u8 = 12;
const READ_MODEL_RESTORE: u8 = 13;
const READ_MODEL_STATUS: u8 = 14;
const TRACE_EVENTS: u8 = 15;
const TRACE_EXPORT: u8 = 44;
const NEWS_PAGE: u8 = 16;
const NEWS_DETAIL: u8 = 19;
const LLM_COMPLETE: u8 = 20;
const CONTEXT_SEARCH: u8 = 21;
const LLM_ACTION: u8 = 22;
const LLM_STREAM: u8 = 42;
const STRATEGY_EVALUATE: u8 = 23;
const PROPOSAL_PREVIEW: u8 = 24;
const PROPOSAL_SUBMIT: u8 = 25;
const SCHEDULED_PROPOSAL_SUBMIT: u8 = 29;
const AUTONOMY_MODE: u8 = 26;
const ALERTS_GET: u8 = 27;
const ALERT_ACK: u8 = 28;
const BACKTEST_RUN: u8 = 30;
const BACKTEST_LIST: u8 = 31;
const STRATEGY_BACKTEST_RUN: u8 = 32;
const EXPERIMENT_LIST: u8 = 33;
const MODEL_LIST: u8 = 34;
const EXPERIMENT_MUTATE: u8 = 35;
const MODEL_MUTATE: u8 = 36;
const STRATEGY_RESOLUTION_LIST: u8 = 37;
const STRATEGY_EXECUTION_LIST: u8 = 38;
const STRATEGY_REGISTRY_LIST: u8 = 43;
const JOURNAL_BACKUP: u8 = 39;
const JOURNAL_RESTORE: u8 = 40;
const RISK_STATE_TRANSITION: u8 = 41;
const METRIC_REGISTRY_LIST: u8 = 45;
const STRATEGY_LIFECYCLE_TRANSITION: u8 = 46;
const METRIC_LIFECYCLE_TRANSITION: u8 = 47;
const STRATEGY_RESOLUTION_BUDGETED: u8 = 48;
const NEWS_PROVIDER_STATUS: u8 = 49;
const RISK_POLICY_SET: u8 = 50;
const SUPERVISOR_STATUS: u8 = 51;
const RISK_POLICY_STATUS: u8 = 52;
const BROKER_STATUS: u8 = 53;
const CONFIG_STATUS: u8 = 54;
const CONFIG_RELOAD: u8 = 55;
const PREVIEW_MAGIC: &[u8] = b"IT_CMD_PREVIEW_V1\0";
const SUBMIT_MAGIC: &[u8] = b"IT_CMD_SUBMIT_V1\0";
const RESOLVE_MAGIC: &[u8] = b"IT_CMD_RESOLVE_V1\0";
const LIVE_CONFIGURE_MAGIC: &[u8] = b"IT_CMD_LIVE_CONFIGURE_V1\0";
const LIVE_ARM_MAGIC: &[u8] = b"IT_CMD_LIVE_ARM_V1\0";
const LIVE_CONFIRM_MAGIC: &[u8] = b"IT_CMD_LIVE_CONFIRM_V1\0";
const CANCEL_MAGIC: &[u8] = b"IT_CMD_CANCEL_V1\0";
const REPLACE_MAGIC: &[u8] = b"IT_CMD_REPLACE_V1\0";
const LLM_COMPLETE_MAGIC: &[u8] = b"IT_CMD_LLM_COMPLETE_V1\0";
const CONTEXT_SEARCH_MAGIC: &[u8] = b"IT_CMD_CONTEXT_SEARCH_V1\0";
const CONTEXT_SEARCH_VECTOR_MAGIC: &[u8] = b"IT_CMD_CONTEXT_SEARCH_V2\0";
const LLM_ACTION_MAGIC: &[u8] = b"IT_CMD_LLM_ACTION_V1\0";
const LLM_STREAM_MAGIC: &[u8] = b"IT_CMD_LLM_STREAM_V1\0";
const STRATEGY_EVALUATE_MAGIC: &[u8] = b"IT_CMD_STRATEGY_EVALUATE_V1\0";
const PROPOSAL_PREVIEW_MAGIC: &[u8] = b"IT_CMD_PROPOSAL_PREVIEW_V1\0";
const PROPOSAL_SUBMIT_MAGIC: &[u8] = b"IT_CMD_PROPOSAL_SUBMIT_V1\0";
const SCHEDULED_PROPOSAL_MAGIC: &[u8] = b"IT_CMD_SCHEDULED_PROPOSAL_V1\0";
const AUTONOMY_MODE_MAGIC: &[u8] = b"IT_CMD_AUTONOMY_MODE_V1\0";
const ALERT_ACK_MAGIC: &[u8] = b"IT_CMD_ALERT_ACK_V1\0";
const BACKTEST_RUN_MAGIC: &[u8] = b"IT_CMD_BACKTEST_RUN_V1\0";
const STRATEGY_BACKTEST_RUN_MAGIC: &[u8] = b"IT_CMD_STRATEGY_BACKTEST_RUN_V1\0";
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const EXPERIMENT_MUTATE_MAGIC: &[u8] = b"IT_CMD_EXPERIMENT_MUTATE_V1\0";
const MODEL_MUTATE_MAGIC: &[u8] = b"IT_CMD_MODEL_MUTATE_V1\0";
const STRATEGY_LIFECYCLE_TRANSITION_MAGIC: &[u8] = b"IT_CMD_STRATEGY_LIFECYCLE_TRANSITION_V1\0";
const METRIC_LIFECYCLE_TRANSITION_MAGIC: &[u8] = b"IT_CMD_METRIC_LIFECYCLE_TRANSITION_V1\0";
const STRATEGY_RESOLUTION_BUDGETED_MAGIC: &[u8] = b"IT_CMD_STRATEGY_RESOLUTION_BUDGETED_V1\0";
const NEWS_PROVIDER_STATUS_MAGIC: &[u8] = b"IT_CMD_NEWS_PROVIDER_STATUS_V1\0";
const RISK_POLICY_SET_MAGIC: &[u8] = b"IT_CMD_RISK_POLICY_SET_V1\0";
const SUPERVISOR_STATUS_MAGIC: &[u8] = b"IT_CMD_SUPERVISOR_STATUS_V1\0";
const RISK_POLICY_STATUS_MAGIC: &[u8] = b"IT_CMD_RISK_POLICY_STATUS_V1\0";
const BROKER_STATUS_MAGIC: &[u8] = b"IT_CMD_BROKER_STATUS_V1\0";
const CONFIG_RELOAD_MAGIC: &[u8] = b"IT_CMD_CONFIG_RELOAD_V1\0";

/// Explicit model lifecycle mutation sent through authenticated IPC.
#[allow(missing_docs)]
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub enum ModelMutation {
    Register {
        record: ModelRecord,
        manifest: ArtifactManifest,
    },
    Validate {
        model_id: String,
        version: String,
        evidence_id: String,
    },
    Shadow {
        model_id: String,
        version: String,
    },
    Canary {
        model_id: String,
        version: String,
        evidence_id: String,
    },
    Promote {
        model_id: String,
        version: String,
    },
}

/// Explicit research-run mutation sent through authenticated IPC.
#[allow(missing_docs)]
#[derive(Clone, Debug, PartialEq)]
pub enum ExperimentMutation {
    /// Registers immutable run lineage.
    Create {
        run_id: String,
        code_hash: String,
        config_hash: String,
        dataset_hash: String,
    },
    /// Registers immutable run lineage including decision/data provenance.
    CreateWithProvenance {
        run_id: String,
        code_hash: String,
        config_hash: String,
        dataset_hash: String,
        provenance: Box<ExperimentProvenance>,
    },
    /// Starts a registered run.
    Start { run_id: String },
    /// Completes a run with scalar results.
    Succeed {
        run_id: String,
        metrics: BTreeMap<String, f64>,
    },
    /// Marks a running run failed.
    Fail { run_id: String },
    /// Adds a hash-addressed artifact.
    AddArtifact {
        run_id: String,
        artifact: ExperimentArtifact,
    },
}

/// Typed command-service failure returned through the IPC dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandServiceError {
    /// The command payload is malformed or exceeds a bound.
    InvalidPayload,
    /// The engine projection lock or journal failed.
    Engine(String),
}

/// Authenticated engine command router.
pub struct EngineCommandService {
    host: Arc<ServiceHost>,
    catalog: Arc<Catalog>,
    dispatcher: Mutex<CommandDispatcher>,
}

impl EngineCommandService {
    /// Creates a command service with deny-by-default capability policy.
    /// Callers must grant capabilities to authenticated actors before use.
    #[must_use]
    pub fn new(
        host: Arc<ServiceHost>,
        catalog: Arc<Catalog>,
        policy: CapabilityPolicy,
        cache_capacity: usize,
    ) -> Option<Self> {
        Some(Self {
            host,
            catalog,
            dispatcher: Mutex::new(CommandDispatcher::new(policy, cache_capacity)?),
        })
    }

    /// Dispatches a bounded command after capability, version, and idempotency
    /// checks. Payloads use the versioned binary forms documented below.
    ///
    /// # Errors
    /// Returns [`DispatchError`] when authorization, optimistic concurrency,
    /// idempotency, payload validation, or command handling fails.
    pub fn dispatch(&self, command: CommandEnvelope) -> Result<CommandResponse, DispatchError> {
        let kind = command_kind(&command.payload)
            .ok_or_else(|| DispatchError::Handler("command kind missing or unknown".into()))?;
        let capability = capability_for(kind)
            .ok_or_else(|| DispatchError::Handler("unknown command kind".into()))?;
        if command.payload.len() > MAX_COMMAND_BYTES {
            return Err(DispatchError::Handler(
                "command payload exceeds bound".into(),
            ));
        }
        let current = self
            .host
            .journal_cursor()
            .map_err(|error| DispatchError::Handler(format!("state version: {error:?}")))?;
        // Observational requests do not mutate state and therefore cannot
        // conflict with provider/scheduler journal progress. Bind them to the
        // current cursor at dispatch time; optimistic concurrency remains
        // mandatory for every mutating command.
        let mut command = command;
        if matches!(
            kind,
            SNAPSHOT
                | EVENTS
                | RESOLVE_SYMBOL
                | READ_MODEL_STATUS
                | TRACE_EVENTS
                | TRACE_EXPORT
                | NEWS_PAGE
                | NEWS_DETAIL
                | LLM_COMPLETE
                | LLM_ACTION
                | LLM_STREAM
                | STRATEGY_EVALUATE
                | PROPOSAL_PREVIEW
                | CONTEXT_SEARCH
                | EXPERIMENT_LIST
                | MODEL_LIST
                | STRATEGY_RESOLUTION_LIST
                | STRATEGY_EXECUTION_LIST
                | STRATEGY_REGISTRY_LIST
                | METRIC_REGISTRY_LIST
                | CONFIG_STATUS
                | NEWS_PROVIDER_STATUS
                | SUPERVISOR_STATUS
                | RISK_POLICY_STATUS
                | BROKER_STATUS
                | ALERTS_GET
        ) {
            command.expected_state_version = current;
        }
        let mut dispatcher = self
            .dispatcher
            .lock()
            .map_err(|_| DispatchError::Handler("dispatcher poisoned".into()))?;
        dispatcher.dispatch(command, capability, current, |command| {
            self.handle(kind, command)
        })
    }

    #[allow(clippy::too_many_lines)]
    fn handle(&self, kind: u8, command: &CommandEnvelope) -> Result<CommandResponse, String> {
        match kind {
            CONFIG_STATUS => {
                if command.payload != [CONFIG_STATUS] {
                    return Err("invalid config status payload".into());
                }
                let snapshot = self
                    .host
                    .config()
                    .map_err(|error| format!("config: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_config_snapshot(&snapshot),
                })
            }
            CONFIG_RELOAD => {
                let (expected, text) = decode_config_reload(&command.payload)?;
                let settings = insider_cfg_core::parse_cfg(&text)
                    .map_err(|error| format!("config parse: {error}"))?;
                let snapshot = self
                    .host
                    .reload_config(expected, settings, |_| Ok(()))
                    .map_err(|error| format!("config reload: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_config_snapshot(&snapshot),
                })
            }
            SNAPSHOT => {
                if command.payload.len() != 1 {
                    return Err("snapshot payload must contain only command kind".into());
                }
                let snapshot = self
                    .host
                    .runtime_snapshot()
                    .map_err(|error| format!("snapshot: {error:?}"))?;
                let state_version = snapshot.cursor;
                Ok(CommandResponse {
                    state_version,
                    payload: encode_snapshot(&snapshot),
                })
            }
            EVENTS => {
                let (cursor, limit) = decode_events_request(&command.payload)?;
                let records = self
                    .host
                    .journal_events_after(cursor, limit)
                    .map_err(|error| format!("events: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_events_response(&records),
                })
            }
            PREVIEW => {
                let (instrument, target, proposal, now, trace, ttl, order_type, limit_price) =
                    decode_preview_request(&command.payload)?;
                let preview = self
                    .host
                    .preview_manual_target_with_order(
                        instrument,
                        target,
                        proposal,
                        now,
                        trace,
                        ttl,
                        order_type,
                        limit_price,
                    )
                    .map_err(|error| format!("preview: {error:?}"))?;
                let state_version = preview.expected_state_version;
                Ok(CommandResponse {
                    state_version,
                    payload: encode_preview(&preview),
                })
            }
            SUBMIT => {
                let (preview, confirmation, now) = decode_submit_request(&command.payload)?;
                let order_id = self
                    .host
                    .submit_manual_preview(&preview, now, &confirmation)
                    .map_err(|error| format!("submit: {error:?}"))?;
                let state_version = self
                    .host
                    .journal_cursor()
                    .map_err(|error| format!("state version: {error:?}"))?;
                let mut payload = Vec::new();
                push_string(&mut payload, &order_id);
                Ok(CommandResponse {
                    state_version,
                    payload,
                })
            }
            CANCEL => {
                let client_order_id = decode_cancel_request(&command.payload)?;
                self.host
                    .cancel_order(&client_order_id)
                    .map_err(|error| format!("cancel: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_CANCEL_OK_V1\0".to_vec(),
                })
            }
            REPLACE => {
                let (client_order_id, quantity_ticks, limit_price_ticks) =
                    decode_replace_request(&command.payload)?;
                self.host
                    .replace_order(&client_order_id, quantity_ticks, limit_price_ticks)
                    .map_err(|error| format!("replace: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_REPLACE_OK_V1\0".to_vec(),
                })
            }
            RESOLVE_SYMBOL => {
                let (symbol, day, supported_assets) = decode_resolve_request(&command.payload)?;
                let instrument = self
                    .catalog
                    .resolve_symbol(&symbol, day, &supported_assets)
                    .map_err(|error| format!("resolve: {error:?}"))?;
                let mut payload = Vec::new();
                payload.extend_from_slice(b"IT_RESOLVED_INSTRUMENT_V1\0");
                payload.extend_from_slice(&instrument.id.get().to_le_bytes());
                payload.push(asset_code(instrument.asset_class));
                push_string(&mut payload, &instrument.symbol);
                push_string(&mut payload, &instrument.venue);
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload,
                })
            }
            LIVE_CONFIGURE => {
                let (accounts, max_notional_ticks) = decode_live_configure(&command.payload)?;
                self.host
                    .configure_live_limits(insider_autonomy::LiveLimits {
                        allowed_accounts: accounts.into_iter().collect(),
                        max_notional_ticks,
                    })
                    .map_err(|error| format!("live configure: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: live_environment_payload(
                        self.host
                            .trading_environment()
                            .map_err(|error| format!("environment: {error:?}"))?,
                    ),
                })
            }
            LIVE_ARM => {
                let (account, now, phrase) = decode_live_arm(&command.payload)?;
                let token = self
                    .host
                    .arm_live(&account, now, &phrase)
                    .map_err(|error| format!("live arm: {error:?}"))?;
                let mut payload = live_environment_payload(
                    self.host
                        .trading_environment()
                        .map_err(|error| format!("environment: {error:?}"))?,
                );
                push_string(&mut payload, &token);
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload,
                })
            }
            LIVE_CONFIRM => {
                let (account, token, now, phrase) = decode_live_confirm(&command.payload)?;
                self.host
                    .confirm_live(&account, &token, now, &phrase)
                    .map_err(|error| format!("live confirm: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: live_environment_payload(
                        self.host
                            .trading_environment()
                            .map_err(|error| format!("environment: {error:?}"))?,
                    ),
                })
            }
            LIVE_KILL => {
                if command.payload != [LIVE_KILL] {
                    return Err("invalid live kill payload".into());
                }
                self.host
                    .kill_live()
                    .map_err(|error| format!("live kill: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: live_environment_payload(
                        self.host
                            .trading_environment()
                            .map_err(|error| format!("environment: {error:?}"))?,
                    ),
                })
            }
            AUTONOMY_SUBMIT => {
                let mut plan = decode_autonomy_submit(&command.payload)?;
                let now = self.host.monotonic_now();
                if plan.generated_at.as_nanos() == 0 {
                    let ttl = plan.expires_at.as_nanos();
                    plan.generated_at = now;
                    plan.expires_at = now
                        .checked_add(ttl)
                        .ok_or("autonomy plan expiry overflow")?;
                }
                self.host
                    .submit_autonomy_plan(plan, now)
                    .map_err(|error| format!("autonomy submit: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_AUTONOMY_SUBMIT_OK_V1\0".to_vec(),
                })
            }
            AUTONOMY_TRANSITION => {
                let (plan_id, state, _client_now) = decode_autonomy_transition(&command.payload)?;
                self.host
                    .transition_autonomy_plan(&plan_id, state, self.host.monotonic_now())
                    .map_err(|error| format!("autonomy transition: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_AUTONOMY_TRANSITION_OK_V1\0".to_vec(),
                })
            }
            READ_MODEL_BACKUP => {
                let destination = decode_path_command(&command.payload, READ_MODEL_BACKUP)?;
                let manifest = self
                    .host
                    .backup_read_model(destination)
                    .map_err(|error| format!("read-model backup: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_projection_manifest(
                        b"IT_CMD_READ_MODEL_BACKUP_OK_V1\0",
                        &manifest,
                    ),
                })
            }
            READ_MODEL_RESTORE => {
                let (source, destination) = decode_restore_command(&command.payload)?;
                let manifest = ServiceHost::restore_read_model_backup(source, destination)
                    .map_err(|error| format!("read-model restore: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_projection_manifest(
                        b"IT_CMD_READ_MODEL_RESTORE_OK_V1\0",
                        &manifest,
                    ),
                })
            }
            JOURNAL_BACKUP => {
                let destination = decode_path_command(&command.payload, JOURNAL_BACKUP)?;
                let manifest = self
                    .host
                    .backup_journal(destination)
                    .map_err(|error| format!("journal backup: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_backup_manifest(b"IT_CMD_JOURNAL_BACKUP_OK_V1\0", &manifest),
                })
            }
            JOURNAL_RESTORE => {
                let (source, destination) =
                    decode_restore_command_kind(&command.payload, JOURNAL_RESTORE)?;
                let manifest = ServiceHost::restore_journal_backup(source, destination)
                    .map_err(|error| format!("journal restore: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_backup_manifest(b"IT_CMD_JOURNAL_RESTORE_OK_V1\0", &manifest),
                })
            }
            RISK_STATE_TRANSITION => {
                let (next, authorization) = decode_risk_state_command(&command.payload)?;
                self.host
                    .transition_risk_state(next, &authorization)
                    .map_err(|error| format!("risk state transition: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_risk_state_response(next),
                })
            }
            RISK_POLICY_SET => {
                let policy = decode_scoped_risk_policy_command(&command.payload)?;
                self.host
                    .set_scoped_risk_policy(policy)
                    .map_err(|error| format!("risk policy set: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_RISK_POLICY_SET_OK_V1\0".to_vec(),
                })
            }
            SUPERVISOR_STATUS => {
                if command.payload != SUPERVISOR_STATUS_MAGIC
                    && command.payload != [SUPERVISOR_STATUS]
                {
                    return Err("invalid supervisor status request".into());
                }
                let snapshot = self
                    .host
                    .supervisor_snapshot()
                    .map_err(|error| format!("supervisor status: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_supervisor_snapshot(&snapshot),
                })
            }
            RISK_POLICY_STATUS => {
                if command.payload != RISK_POLICY_STATUS_MAGIC
                    && command.payload != [RISK_POLICY_STATUS]
                {
                    return Err("invalid risk policy status request".into());
                }
                let policy = self
                    .host
                    .scoped_risk_policy_snapshot()
                    .map_err(|error| format!("risk policy status: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_risk_policy_snapshot(policy.as_ref()),
                })
            }
            BROKER_STATUS => {
                if command.payload != BROKER_STATUS_MAGIC && command.payload != [BROKER_STATUS] {
                    return Err("invalid broker status request".into());
                }
                let status = self
                    .host
                    .broker_status_snapshot()
                    .map_err(|error| format!("broker status: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_broker_status(&status),
                })
            }
            READ_MODEL_STATUS => {
                if command.payload != [READ_MODEL_STATUS] {
                    return Err("invalid read-model status payload".into());
                }
                let manifest = self
                    .host
                    .read_model_manifest()
                    .map_err(|error| format!("read-model status: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_projection_manifest(
                        b"IT_CMD_READ_MODEL_STATUS_V1\0",
                        &manifest,
                    ),
                })
            }
            TRACE_EVENTS => {
                let trace = decode_trace_request(&command.payload)?;
                let events = self
                    .host
                    .trace_events(trace)
                    .map_err(|error| format!("trace events: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_trace_events(&events),
                })
            }
            TRACE_EXPORT => {
                let trace = decode_trace_request(&command.payload)?;
                let events = self
                    .host
                    .trace_events(trace)
                    .map_err(|error| format!("trace export: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_trace_export(&events),
                })
            }
            NEWS_PAGE => {
                let (scope, symbol, after) = decode_news_page_request(&command.payload)?;
                let page = self
                    .host
                    .news_page(&scope, &symbol, after.as_deref(), 100)
                    .map_err(|error| format!("news page: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_news_page(&page),
                })
            }
            NEWS_PROVIDER_STATUS => {
                if command.payload != NEWS_PROVIDER_STATUS_MAGIC
                    && command.payload != [NEWS_PROVIDER_STATUS]
                {
                    return Err("invalid news provider status request".into());
                }
                let statuses = self
                    .host
                    .news_provider_statuses()
                    .map_err(|error| format!("news provider status: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_news_provider_statuses(&statuses),
                })
            }
            NEWS_DETAIL => {
                let item_id = decode_news_detail_request(&command.payload)?;
                let detail = self
                    .host
                    .news_detail(&item_id)
                    .map_err(|error| format!("news detail: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_news_detail(detail.as_ref()),
                })
            }
            LLM_COMPLETE => {
                let request = decode_llm_request(&command.payload)?;
                let response = self
                    .host
                    .llm_complete(&request)
                    .map_err(|error| format!("llm complete: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_llm_response(&response),
                })
            }
            LLM_STREAM => {
                let request = decode_llm_request_with_magic(&command.payload, LLM_STREAM_MAGIC)?;
                let items = self
                    .host
                    .llm_stream(&request)
                    .map_err(|error| format!("llm stream: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_llm_stream_response(&request.trace_id, &items),
                })
            }
            LLM_ACTION => {
                let request = decode_llm_request_with_magic(&command.payload, LLM_ACTION_MAGIC)?;
                let action = self
                    .host
                    .llm_autonomous_action(&request)
                    .map_err(|error| format!("llm autonomous action: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_llm_action_response(&request.trace_id, &action),
                })
            }
            STRATEGY_EVALUATE => {
                let mut request = decode_strategy_evaluate_request(&command.payload)?;
                // Monotonic timestamps belong to the engine process. The
                // desktop request carries metric values, but cannot smuggle a
                // different clock origin into freshness or replay semantics.
                let now = self.host.monotonic_now();
                request.metric.generated_mono = now;
                request.now = now;
                let proposal = self
                    .host
                    .evaluate_threshold_strategy(
                        request.strategy_id,
                        request.metric_id,
                        &request.metric,
                        request.entry_threshold,
                        request.exit_threshold,
                        request.quantity_ticks,
                        request.horizon_ns,
                        request.strategy_ttl_ns,
                        request.now,
                    )
                    .map_err(|error| format!("strategy evaluation: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_strategy_proposal_response(&proposal),
                })
            }
            PROPOSAL_PREVIEW => {
                let (proposal_id, scale, _requested_now, trace_id, ttl_ns) =
                    decode_proposal_preview_request(&command.payload)?;
                let now = self.host.monotonic_now();
                let preview = self
                    .host
                    .preview_strategy_proposal(proposal_id, scale, now, trace_id, ttl_ns)
                    .map_err(|error| format!("proposal preview: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_preview(&preview),
                })
            }
            PROPOSAL_SUBMIT => {
                let (proposal_id, scale, confirmation, trace_id) =
                    decode_proposal_submit_request(&command.payload)?;
                if confirmation != "CONFIRM" {
                    return Err("proposal confirmation is required".into());
                }
                let client_order_id = self
                    .host
                    .submit_scaled_proposal(proposal_id, scale, self.host.monotonic_now(), trace_id)
                    .map_err(|error| format!("proposal submit: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: {
                        let mut out = b"IT_CMD_PROPOSAL_SUBMIT_RESPONSE_V1\0".to_vec();
                        push_string(&mut out, &client_order_id);
                        out
                    },
                })
            }
            SCHEDULED_PROPOSAL_SUBMIT => {
                let (proposal_id, schedule, confirmation, trace_id) =
                    decode_scheduled_proposal_request(&command.payload)?;
                if confirmation != "CONFIRM" {
                    return Err("scheduled proposal confirmation is required".into());
                }
                let parent_id = self
                    .host
                    .submit_scheduled_proposal(
                        &self
                            .host
                            .strategy_proposal_record(proposal_id)
                            .ok_or_else(|| "unknown proposal".to_owned())?
                            .proposal,
                        &schedule,
                        self.host.monotonic_now(),
                        trace_id,
                    )
                    .map_err(|error| format!("scheduled proposal submit: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: {
                        let mut out = b"IT_CMD_SCHEDULED_PROPOSAL_RESPONSE_V1\0".to_vec();
                        push_string(&mut out, &parent_id);
                        out
                    },
                })
            }
            BACKTEST_RUN => {
                let request = decode_backtest_run_request(&command.payload)?;
                let result = self
                    .host
                    .run_backtest(request)
                    .map_err(|error| format!("backtest: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_backtest_run_response(&result),
                })
            }
            BACKTEST_LIST => {
                if command.payload != [BACKTEST_LIST] {
                    return Err("invalid backtest list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_backtest_list_response(&self.host.backtest_runs()),
                })
            }
            STRATEGY_BACKTEST_RUN => {
                let request = decode_strategy_backtest_request(&command.payload)?;
                let result = self
                    .host
                    .run_strategy_backtest(request)
                    .map_err(|error| format!("strategy backtest: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_backtest_run_response(&result),
                })
            }
            EXPERIMENT_LIST => {
                if command.payload != [EXPERIMENT_LIST] {
                    return Err("invalid experiment list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_experiment_list_response(&self.host.experiment_runs()),
                })
            }
            MODEL_LIST => {
                if command.payload != [MODEL_LIST] {
                    return Err("invalid model registry payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_model_list_response(&self.host.model_registry_snapshot()),
                })
            }
            EXPERIMENT_MUTATE => {
                let mutation = decode_experiment_mutation(&command.payload)?;
                match mutation {
                    ExperimentMutation::Create {
                        run_id,
                        code_hash,
                        config_hash,
                        dataset_hash,
                    } => self.host.create_experiment(ExperimentRun {
                        run_id,
                        code_hash,
                        config_hash,
                        dataset_hash,
                        provenance: ExperimentProvenance::default(),
                        status: RunStatus::Created,
                        metrics: BTreeMap::new(),
                        artifacts: Vec::new(),
                    }),
                    ExperimentMutation::CreateWithProvenance {
                        run_id,
                        code_hash,
                        config_hash,
                        dataset_hash,
                        provenance,
                    } => self.host.create_experiment(ExperimentRun {
                        run_id,
                        code_hash,
                        config_hash,
                        dataset_hash,
                        provenance: *provenance,
                        status: RunStatus::Created,
                        metrics: BTreeMap::new(),
                        artifacts: Vec::new(),
                    }),
                    ExperimentMutation::Start { run_id } => self.host.start_experiment(&run_id),
                    ExperimentMutation::Succeed { run_id, metrics } => {
                        self.host.succeed_experiment(&run_id, metrics)
                    }
                    ExperimentMutation::Fail { run_id } => self.host.fail_experiment(&run_id),
                    ExperimentMutation::AddArtifact { run_id, artifact } => {
                        self.host.add_experiment_artifact(&run_id, artifact)
                    }
                }
                .map_err(|error| format!("experiment mutation: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_EXPERIMENT_MUTATE_OK_V1\0".to_vec(),
                })
            }
            MODEL_MUTATE => {
                let mutation = decode_model_mutation(&command.payload)?;
                match mutation {
                    ModelMutation::Register { record, manifest } => {
                        self.host.register_model(record, manifest)
                    }
                    ModelMutation::Validate {
                        model_id,
                        version,
                        evidence_id,
                    } => self.host.validate_model(&model_id, &version, &evidence_id),
                    ModelMutation::Shadow { model_id, version } => {
                        self.host.start_model_shadow(&model_id, &version)
                    }
                    ModelMutation::Canary {
                        model_id,
                        version,
                        evidence_id,
                    } => self
                        .host
                        .start_model_canary(&model_id, &version, &evidence_id),
                    ModelMutation::Promote { model_id, version } => {
                        self.host.promote_model(&model_id, &version)
                    }
                }
                .map_err(|error| format!("model mutation: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_MODEL_MUTATE_OK_V1\0".to_vec(),
                })
            }
            STRATEGY_RESOLUTION_LIST => {
                if command.payload != [STRATEGY_RESOLUTION_LIST] {
                    return Err("invalid strategy resolution list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_resolution_list_response(
                        &self.host.strategy_resolution_history(),
                    ),
                })
            }
            STRATEGY_RESOLUTION_BUDGETED => {
                let (policy, now, budgets) = decode_strategy_resolution_budgeted(&command.payload)?;
                let result = self
                    .host
                    .resolve_strategy_proposals_with_budgets(policy, now, &budgets)
                    .map_err(|error| format!("budgeted strategy resolution: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_budgeted_resolution_response(&result),
                })
            }
            STRATEGY_EXECUTION_LIST => {
                if command.payload != [STRATEGY_EXECUTION_LIST] {
                    return Err("invalid strategy execution list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_strategy_execution_list_response(
                        &self.host.strategy_execution_summaries(),
                    ),
                })
            }
            STRATEGY_REGISTRY_LIST => {
                if command.payload != [STRATEGY_REGISTRY_LIST] {
                    return Err("invalid strategy registry list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_strategy_registry_list_response(&self.host.strategy_registry()),
                })
            }
            METRIC_REGISTRY_LIST => {
                if command.payload != [METRIC_REGISTRY_LIST] {
                    return Err("invalid metric registry list payload".into());
                }
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_metric_registry_list_response(&self.host.metric_registry()),
                })
            }
            STRATEGY_LIFECYCLE_TRANSITION => {
                let (strategy_id, lifecycle, confirmation, evidence_ref) =
                    decode_strategy_lifecycle_transition(&command.payload)?;
                if confirmation != "CONFIRM" {
                    return Err("lifecycle transition requires CONFIRM".into());
                }
                self.host
                    .transition_strategy_lifecycle(&strategy_id, lifecycle, &evidence_ref)
                    .map_err(|error| format!("strategy lifecycle transition: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_STRATEGY_LIFECYCLE_TRANSITION_OK_V1\0".to_vec(),
                })
            }
            METRIC_LIFECYCLE_TRANSITION => {
                let (metric_id, lifecycle, confirmation, evidence_ref) =
                    decode_metric_lifecycle_transition(&command.payload)?;
                if confirmation != "CONFIRM" {
                    return Err("metric lifecycle transition requires CONFIRM".into());
                }
                self.host
                    .transition_metric_lifecycle(&metric_id, lifecycle, &evidence_ref)
                    .map_err(|error| format!("metric lifecycle transition: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: b"IT_CMD_METRIC_LIFECYCLE_TRANSITION_OK_V1\0".to_vec(),
                })
            }
            AUTONOMY_MODE => {
                let mode = decode_autonomy_mode_request(&command.payload)?;
                self.host
                    .set_autonomy_mode(mode)
                    .map_err(|error| format!("set autonomy mode: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: autonomy_mode_response(mode),
                })
            }
            ALERTS_GET => Ok(CommandResponse {
                state_version: self
                    .host
                    .journal_cursor()
                    .map_err(|error| format!("state version: {error:?}"))?,
                payload: encode_alerts_response(&self.host.pending_alerts()),
            }),
            ALERT_ACK => {
                let alert_id = decode_alert_ack(&command.payload)?;
                let acknowledged = self
                    .host
                    .acknowledge_alert(&alert_id)
                    .map_err(|error| format!("alert acknowledge: {error:?}"))?;
                let mut payload = b"IT_CMD_ALERT_ACK_RESPONSE_V1\0".to_vec();
                payload.push(u8::from(acknowledged));
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload,
                })
            }
            CONTEXT_SEARCH => {
                let (query, limit) = decode_context_search_request(&command.payload)?;
                let hits = self
                    .host
                    .search_context(&query, limit)
                    .map_err(|error| format!("context search: {error:?}"))?;
                Ok(CommandResponse {
                    state_version: self
                        .host
                        .journal_cursor()
                        .map_err(|error| format!("state version: {error:?}"))?,
                    payload: encode_context_search_response(&hits),
                })
            }
            _ => Err("unknown command kind".into()),
        }
    }
}

/// Builds the payload for a read-only runtime snapshot command.
#[must_use]
pub const fn snapshot_command_payload() -> [u8; 1] {
    [SNAPSHOT]
}

/// Builds a read-only pending-alert query command.
#[must_use]
pub const fn alerts_get_command_payload() -> [u8; 1] {
    [ALERTS_GET]
}

/// Builds an idempotent in-app alert acknowledgement command.
#[must_use]
pub fn alert_ack_command_payload(alert_id: &str) -> Vec<u8> {
    let mut output = b"IT_CMD_ALERT_ACK_V1\0".to_vec();
    push_string(&mut output, alert_id);
    output
}

/// Builds a bounded cursor-resumption command payload.
#[must_use]
pub fn events_command_payload(cursor: u64, limit: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(13);
    payload.push(EVENTS);
    payload.extend_from_slice(&cursor.to_le_bytes());
    payload.extend_from_slice(&limit.to_le_bytes());
    payload
}

fn decode_events_request(payload: &[u8]) -> Result<(u64, usize), String> {
    if payload.len() != 13 || payload[0] != EVENTS {
        return Err("invalid events payload".into());
    }
    let cursor = u64::from_le_bytes(
        payload[1..9]
            .try_into()
            .map_err(|_| "invalid events cursor")?,
    );
    let limit_u32 = u32::from_le_bytes(
        payload[9..13]
            .try_into()
            .map_err(|_| "invalid events limit")?,
    );
    let limit = usize::try_from(limit_u32).map_err(|_| "invalid events limit")?;
    if limit == 0 || limit > 4_096 {
        return Err("events limit out of bounds".into());
    }
    Ok((cursor, limit))
}

/// Builds a versioned manual-preview command payload.
#[must_use]
pub fn preview_command_payload(
    instrument_id: InstrumentId,
    target_quantity_ticks: i64,
    proposal_id: ProposalId,
    now: MonoTime,
    trace_id: TraceId,
    ttl_ns: u64,
) -> Vec<u8> {
    preview_command_payload_with_order(
        instrument_id,
        target_quantity_ticks,
        proposal_id,
        now,
        trace_id,
        ttl_ns,
        insider_broker_api::OrderType::Market,
        None,
    )
}

/// Builds a manual-preview payload preserving order type and limit price.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn preview_command_payload_with_order(
    instrument_id: InstrumentId,
    target_quantity_ticks: i64,
    proposal_id: ProposalId,
    now: MonoTime,
    trace_id: TraceId,
    ttl_ns: u64,
    order_type: insider_broker_api::OrderType,
    limit_price_ticks: Option<i64>,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(PREVIEW_MAGIC.len() + 66);
    output.extend_from_slice(PREVIEW_MAGIC);
    output.extend_from_slice(&instrument_id.get().to_le_bytes());
    output.extend_from_slice(&target_quantity_ticks.to_le_bytes());
    output.extend_from_slice(&proposal_id.get().to_le_bytes());
    output.push(match order_type {
        insider_broker_api::OrderType::Market => 1,
        insider_broker_api::OrderType::Limit => 2,
    });
    output.push(u8::from(limit_price_ticks.is_some()));
    output.extend_from_slice(&limit_price_ticks.unwrap_or_default().to_le_bytes());
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    output.extend_from_slice(&trace_id.get().to_le_bytes());
    output.extend_from_slice(&ttl_ns.to_le_bytes());
    output
}

/// Builds a read-only preview request for an existing strategy proposal.
#[must_use]
pub fn proposal_preview_command_payload(
    proposal_id: ProposalId,
    scale: f64,
    now: MonoTime,
    trace_id: TraceId,
    ttl_ns: u64,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(PROPOSAL_PREVIEW_MAGIC);
    output.extend_from_slice(&proposal_id.get().to_le_bytes());
    output.extend_from_slice(&scale.to_le_bytes());
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    output.extend_from_slice(&trace_id.get().to_le_bytes());
    output.extend_from_slice(&ttl_ns.to_le_bytes());
    output
}

/// Builds an explicit confirmation request for a strategy proposal.
#[must_use]
pub fn proposal_submit_command_payload(
    proposal_id: ProposalId,
    scale: f64,
    confirmation: &str,
    trace_id: TraceId,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(PROPOSAL_SUBMIT_MAGIC);
    output.extend_from_slice(&proposal_id.get().to_le_bytes());
    output.extend_from_slice(&scale.to_le_bytes());
    push_string(&mut output, confirmation);
    output.extend_from_slice(&trace_id.get().to_le_bytes());
    output
}

/// Builds an explicit confirmation request for a scheduled proposal.
#[must_use]
pub fn scheduled_proposal_command_payload(
    proposal_id: ProposalId,
    schedule: &Schedule,
    confirmation: &str,
    trace_id: TraceId,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(SCHEDULED_PROPOSAL_MAGIC);
    output.extend_from_slice(&proposal_id.get().to_le_bytes());
    match schedule {
        Schedule::Immediate => output.push(0),
        Schedule::Twap {
            slices,
            interval_ns,
        } => {
            output.push(1);
            output.extend_from_slice(&(u32::try_from(*slices).unwrap_or(u32::MAX)).to_le_bytes());
            output.extend_from_slice(&interval_ns.to_le_bytes());
        }
        Schedule::Vwap { weights } => {
            output.push(2);
            output.extend_from_slice(
                &(u32::try_from(weights.len()).unwrap_or(u32::MAX)).to_le_bytes(),
            );
            for weight in weights {
                output.extend_from_slice(&weight.to_le_bytes());
            }
        }
        Schedule::Pov {
            participation_bps,
            interval_ns,
            market_volume_ticks,
        } => {
            output.push(3);
            output.extend_from_slice(&participation_bps.to_le_bytes());
            output.extend_from_slice(&interval_ns.to_le_bytes());
            output.extend_from_slice(
                &(u32::try_from(market_volume_ticks.len()).unwrap_or(u32::MAX)).to_le_bytes(),
            );
            for volume in market_volume_ticks {
                output.extend_from_slice(&volume.to_le_bytes());
            }
        }
        Schedule::ImplementationShortfall {
            slices,
            interval_ns,
            urgency_bps,
        } => {
            output.push(4);
            output.extend_from_slice(&(u32::try_from(*slices).unwrap_or(u32::MAX)).to_le_bytes());
            output.extend_from_slice(&interval_ns.to_le_bytes());
            output.extend_from_slice(&urgency_bps.to_le_bytes());
        }
        Schedule::Adaptive {
            slices,
            interval_ns,
            urgency_bps,
            spread_ticks,
            max_spread_ticks,
            volatility_bps,
            max_volatility_bps,
            market_volume_ticks,
        } => {
            output.push(5);
            output.extend_from_slice(&(u32::try_from(*slices).unwrap_or(u32::MAX)).to_le_bytes());
            output.extend_from_slice(&interval_ns.to_le_bytes());
            output.extend_from_slice(&urgency_bps.to_le_bytes());
            output.extend_from_slice(&spread_ticks.to_le_bytes());
            output.extend_from_slice(&max_spread_ticks.to_le_bytes());
            output.extend_from_slice(&volatility_bps.to_le_bytes());
            output.extend_from_slice(&max_volatility_bps.to_le_bytes());
            output.extend_from_slice(
                &(u32::try_from(market_volume_ticks.len()).unwrap_or(u32::MAX)).to_le_bytes(),
            );
            for volume in market_volume_ticks {
                output.extend_from_slice(&volume.to_le_bytes());
            }
        }
    }
    push_string(&mut output, confirmation);
    output.extend_from_slice(&trace_id.get().to_le_bytes());
    output
}

/// Builds a bounded research backtest command. Event sequences must be strictly
/// increasing; the engine performs the authoritative replay and journaling.
#[must_use]
pub fn backtest_run_command_payload(request: &BacktestRunRequest) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(BACKTEST_RUN_MAGIC);
    push_string(&mut output, &request.run_id);
    push_string(&mut output, &request.strategy_id);
    push_string(&mut output, &request.dataset_hash);
    push_string(&mut output, &request.config_hash);
    output.extend_from_slice(&request.initial_cash_ticks.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(request.events.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for event in &request.events {
        match event {
            insider_replay::BacktestEvent::Fill {
                sequence,
                quantity_ticks,
                price_ticks,
                fee_ticks,
            } => {
                output.push(1);
                output.extend_from_slice(&sequence.to_le_bytes());
                output.extend_from_slice(&quantity_ticks.to_le_bytes());
                output.extend_from_slice(&price_ticks.to_le_bytes());
                output.extend_from_slice(&fee_ticks.to_le_bytes());
            }
            insider_replay::BacktestEvent::Mark {
                sequence,
                price_ticks,
            } => {
                output.push(2);
                output.extend_from_slice(&sequence.to_le_bytes());
                output.extend_from_slice(&price_ticks.to_le_bytes());
            }
        }
    }
    output
}

fn decode_backtest_run_request(payload: &[u8]) -> Result<BacktestRunRequest, String> {
    if !payload.starts_with(BACKTEST_RUN_MAGIC) {
        return Err("invalid backtest command magic".into());
    }
    let mut cursor = BACKTEST_RUN_MAGIC.len();
    let run_id = read_string(payload, &mut cursor)?;
    let strategy_id = read_string(payload, &mut cursor)?;
    let dataset_hash = read_string(payload, &mut cursor)?;
    let config_hash = read_string(payload, &mut cursor)?;
    let initial_cash_ticks = read_i128(payload, &mut cursor)?;
    let count = usize::try_from(read_u32(payload, &mut cursor)?)
        .map_err(|_| "invalid backtest event count")?;
    if count == 0 || count > 1_000_000 {
        return Err("backtest event count is outside bounds".into());
    }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let kind = read_u8(payload, &mut cursor)?;
        let sequence = read_u64(payload, &mut cursor)?;
        let event = match kind {
            1 => insider_replay::BacktestEvent::Fill {
                sequence,
                quantity_ticks: read_i64(payload, &mut cursor)?,
                price_ticks: read_i64(payload, &mut cursor)?,
                fee_ticks: read_i128(payload, &mut cursor)?,
            },
            2 => insider_replay::BacktestEvent::Mark {
                sequence,
                price_ticks: read_i64(payload, &mut cursor)?,
            },
            _ => return Err("unknown backtest event kind".into()),
        };
        events.push(event);
    }
    if cursor != payload.len() {
        return Err("backtest command has trailing bytes".into());
    }
    Ok(BacktestRunRequest {
        run_id,
        strategy_id,
        dataset_hash,
        config_hash,
        initial_cash_ticks,
        events,
    })
}

/// Builds a deterministic strategy replay command from a point-in-time tape.
#[must_use]
pub fn strategy_backtest_command_payload(request: &StrategyBacktestRunRequest) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(STRATEGY_BACKTEST_RUN_MAGIC);
    push_string(&mut output, &request.run_id);
    push_string(&mut output, &request.strategy_id);
    push_string(&mut output, &request.dataset_hash);
    push_string(&mut output, &request.config_hash);
    output.extend_from_slice(&request.initial_cash_ticks.to_le_bytes());
    output.extend_from_slice(
        &u32::try_from(request.events.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for event in &request.events {
        output.extend_from_slice(&event.sequence.to_le_bytes());
        output.extend_from_slice(&event.now_mono_ns.to_le_bytes());
        output.extend_from_slice(&event.instrument_id.get().to_le_bytes());
        output.extend_from_slice(&event.price_ticks.to_le_bytes());
        output.extend_from_slice(&event.fee_ticks.to_le_bytes());
        output.extend_from_slice(
            &u16::try_from(event.metrics.len())
                .unwrap_or(u16::MAX)
                .to_le_bytes(),
        );
        for metric in &event.metrics {
            push_string(&mut output, &metric.metric_id);
            output.extend_from_slice(&metric.generated_mono.as_nanos().to_le_bytes());
            output.extend_from_slice(&metric.ttl_ns.to_le_bytes());
            output.extend_from_slice(&metric.score.to_le_bytes());
            output.extend_from_slice(&metric.confidence.to_le_bytes());
            output.extend_from_slice(&metric.uncertainty.to_le_bytes());
        }
    }
    output
}

fn decode_strategy_backtest_request(payload: &[u8]) -> Result<StrategyBacktestRunRequest, String> {
    if !payload.starts_with(STRATEGY_BACKTEST_RUN_MAGIC) {
        return Err("invalid strategy backtest command magic".into());
    }
    let mut cursor = STRATEGY_BACKTEST_RUN_MAGIC.len();
    let run_id = read_string(payload, &mut cursor)?;
    let strategy_id = read_string(payload, &mut cursor)?;
    let dataset_hash = read_string(payload, &mut cursor)?;
    let config_hash = read_string(payload, &mut cursor)?;
    let initial_cash_ticks = read_i128(payload, &mut cursor)?;
    let count = usize::try_from(read_u32(payload, &mut cursor)?)
        .map_err(|_| "invalid strategy backtest event count")?;
    if count == 0 || count > 100_000 {
        return Err("strategy backtest event count is outside bounds".into());
    }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let sequence = read_u64(payload, &mut cursor)?;
        let now_mono_ns = read_u64(payload, &mut cursor)?;
        let instrument_id = InstrumentId::new(read_u128(payload, &mut cursor)?)
            .map_err(|_| "invalid strategy backtest instrument")?;
        let price_ticks = read_i64(payload, &mut cursor)?;
        let fee_ticks = read_i128(payload, &mut cursor)?;
        let metric_count = usize::from(read_u16(payload, &mut cursor)?);
        if metric_count > 4_096 {
            return Err("too many strategy backtest metrics".into());
        }
        let mut metrics = Vec::with_capacity(metric_count);
        for _ in 0..metric_count {
            metrics.push(insider_metric_sdk::MetricOutput {
                metric_id: read_string(payload, &mut cursor)?,
                instrument_id,
                generated_mono: MonoTime::from_nanos(read_u64(payload, &mut cursor)?),
                ttl_ns: read_u64(payload, &mut cursor)?,
                score: read_f64(payload, &mut cursor)?,
                confidence: read_f64(payload, &mut cursor)?,
                uncertainty: read_f64(payload, &mut cursor)?,
            });
        }
        events.push(StrategyBacktestEvent {
            sequence,
            now_mono_ns,
            instrument_id,
            price_ticks,
            fee_ticks,
            metrics,
        });
    }
    if cursor != payload.len() {
        return Err("strategy backtest command has trailing bytes".into());
    }
    Ok(StrategyBacktestRunRequest {
        run_id,
        strategy_id,
        dataset_hash,
        config_hash,
        initial_cash_ticks,
        events,
    })
}

fn encode_backtest_run_response(result: &crate::BacktestRunResult) -> Vec<u8> {
    let mut output = Vec::with_capacity(256);
    output.extend_from_slice(b"IT_CMD_BACKTEST_RUN_RESPONSE_V1\0");
    push_string(&mut output, &result.run_id);
    push_string(&mut output, &result.strategy_id);
    push_string(&mut output, &result.dataset_hash);
    push_string(&mut output, &result.config_hash);
    output.extend_from_slice(
        &u64::try_from(result.report.event_count)
            .unwrap_or(u64::MAX)
            .to_le_bytes(),
    );
    output.extend_from_slice(&result.report.max_drawdown_ticks.to_le_bytes());
    output.extend_from_slice(&result.report.total_fees_ticks.to_le_bytes());
    output.push(u8::from(result.report.final_snapshot.is_some()));
    if let Some(snapshot) = result.report.final_snapshot {
        output.extend_from_slice(&snapshot.position_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.average_cost_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.cash_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.realized_pnl_ticks.to_le_bytes());
        output.extend_from_slice(&snapshot.equity_ticks.to_le_bytes());
    }
    output
}

fn encode_backtest_list_response(results: &[crate::BacktestRunResult]) -> Vec<u8> {
    let mut output = b"IT_CMD_BACKTEST_LIST_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(
        &u32::try_from(results.len().min(4_096))
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for result in results.iter().take(4_096) {
        push_string(&mut output, &result.run_id);
        push_string(&mut output, &result.strategy_id);
        push_string(&mut output, &result.dataset_hash);
        push_string(&mut output, &result.config_hash);
        output.extend_from_slice(
            &u64::try_from(result.report.event_count)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        output.extend_from_slice(&result.report.max_drawdown_ticks.to_le_bytes());
        output.extend_from_slice(&result.report.total_fees_ticks.to_le_bytes());
        output.push(u8::from(result.report.final_snapshot.is_some()));
        if let Some(snapshot) = result.report.final_snapshot {
            output.extend_from_slice(&snapshot.equity_ticks.to_le_bytes());
        }
    }
    output
}

fn encode_experiment_list_response(runs: &[ExperimentRun]) -> Vec<u8> {
    let mut output = b"IT_CMD_EXPERIMENT_LIST_RESPONSE_V2\0".to_vec();
    output.extend_from_slice(&(u32::try_from(runs.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for run in runs {
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
        output.extend_from_slice(
            &(u32::try_from(run.metrics.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for (key, value) in &run.metrics {
            push_string(&mut output, key);
            output.extend_from_slice(&value.to_le_bytes());
        }
        output.extend_from_slice(
            &(u32::try_from(run.artifacts.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for artifact in &run.artifacts {
            push_string(&mut output, &artifact.kind);
            push_string(&mut output, &artifact.hash);
            push_string(&mut output, &artifact.path);
        }
        encode_experiment_provenance(&mut output, &run.provenance);
    }
    output
}

fn encode_model_list_response(snapshot: &ModelRegistrySnapshot) -> Vec<u8> {
    let mut output = b"IT_CMD_MODEL_LIST_RESPONSE_V1\0".to_vec();
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
        &(u32::try_from(snapshot.active.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for (model_id, version) in &snapshot.active {
        push_string(&mut output, model_id);
        push_string(&mut output, version);
    }
    output
}

fn encode_resolution_list_response(summaries: &[crate::StrategyResolutionSummary]) -> Vec<u8> {
    let mut output = b"IT_CMD_STRATEGY_RESOLUTION_LIST_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(summaries.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for summary in summaries {
        push_string(&mut output, &summary.policy);
        output.extend_from_slice(&summary.now_mono_ns.to_le_bytes());
        output.extend_from_slice(
            &(u32::try_from(summary.accepted_count).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        output.extend_from_slice(
            &(u32::try_from(summary.conflict_count).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        output.extend_from_slice(
            &(u32::try_from(summary.expired_count).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        output.extend_from_slice(
            &(u32::try_from(summary.attribution_count).unwrap_or(u32::MAX)).to_le_bytes(),
        );
    }
    output
}

fn encode_strategy_execution_list_response(
    summaries: &[crate::StrategyExecutionSummary],
) -> Vec<u8> {
    let mut output = b"IT_CMD_STRATEGY_EXECUTION_LIST_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(summaries.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for summary in summaries {
        push_string(&mut output, &summary.strategy_id);
        output.extend_from_slice(&summary.fill_count.to_le_bytes());
        output.extend_from_slice(&summary.filled_quantity_ticks.to_le_bytes());
        output.extend_from_slice(&summary.notional_ticks.to_le_bytes());
    }
    output
}

fn encode_strategy_registry_list_response(records: &[crate::StrategyRegistryRecord]) -> Vec<u8> {
    let mut output = b"IT_CMD_STRATEGY_REGISTRY_LIST_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(records.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for record in records {
        push_string(&mut output, &record.strategy_id);
        push_string(&mut output, &record.mode);
        push_string(&mut output, &record.state);
        push_string(&mut output, &record.lifecycle);
        push_string(&mut output, &record.lifecycle_evidence_ref);
        push_string(&mut output, &record.priority);
        output.extend_from_slice(&record.horizon_ns.to_le_bytes());
        output.extend_from_slice(&record.ttl_ns.to_le_bytes());
        output.extend_from_slice(&record.period_ns.to_le_bytes());
        output.extend_from_slice(&record.deadline_ns.to_le_bytes());
        output.extend_from_slice(
            &(u32::try_from(record.metric_ids.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for metric_id in &record.metric_ids {
            push_string(&mut output, metric_id);
        }
        output.extend_from_slice(
            &(u32::try_from(record.dependencies.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for dependency in &record.dependencies {
            push_string(&mut output, dependency);
        }
    }
    output
}

fn encode_metric_registry_list_response(records: &[crate::MetricRegistryRecord]) -> Vec<u8> {
    let mut output = b"IT_CMD_METRIC_REGISTRY_LIST_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(records.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for record in records {
        push_string(&mut output, &record.metric_id);
        push_string(&mut output, &record.state);
        push_string(&mut output, &record.lifecycle);
        push_string(&mut output, &record.priority);
        for value in [
            record.ttl_ns,
            record.period_ns,
            record.deadline_ns,
            record.budget_ns,
        ] {
            output.extend_from_slice(&value.to_le_bytes());
        }
        for value in [record.min_score, record.max_score] {
            output.push(u8::from(value.is_some()));
            output.extend_from_slice(&value.unwrap_or_default().to_le_bytes());
        }
        output.extend_from_slice(
            &(u32::try_from(record.inputs.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for input in &record.inputs {
            push_string(&mut output, input);
        }
    }
    output
}

/// Builds a bounded read-only strategy registry command.
#[must_use]
pub fn strategy_registry_list_command_payload() -> [u8; 1] {
    [STRATEGY_REGISTRY_LIST]
}

/// Builds a bounded read-only backtest history command.
#[must_use]
pub const fn backtest_list_command_payload() -> [u8; 1] {
    [BACKTEST_LIST]
}

/// Builds a bounded read-only experiment registry command.
#[must_use]
pub const fn experiment_list_command_payload() -> [u8; 1] {
    [EXPERIMENT_LIST]
}

/// Builds a bounded read-only model registry command.
#[must_use]
pub const fn model_list_command_payload() -> [u8; 1] {
    [MODEL_LIST]
}

/// Builds a bounded read-only strategy-resolution history command.
#[must_use]
pub const fn strategy_resolution_list_command_payload() -> [u8; 1] {
    [STRATEGY_RESOLUTION_LIST]
}

/// Builds a bounded read-only strategy execution-attribution command.
#[must_use]
pub const fn strategy_execution_list_command_payload() -> [u8; 1] {
    [STRATEGY_EXECUTION_LIST]
}

/// Builds a bounded read-only metric registry command.
#[must_use]
pub fn metric_registry_list_command_payload() -> [u8; 1] {
    [METRIC_REGISTRY_LIST]
}

/// Builds an authenticated operator lifecycle transition request.
#[must_use]
pub fn strategy_lifecycle_transition_command_payload(
    strategy_id: &str,
    lifecycle: insider_strategy_host::Lifecycle,
    confirmation: &str,
    evidence_ref: &str,
) -> Vec<u8> {
    let mut output = STRATEGY_LIFECYCLE_TRANSITION_MAGIC.to_vec();
    push_string(&mut output, strategy_id);
    output.push(strategy_lifecycle_code(lifecycle));
    push_string(&mut output, confirmation);
    push_string(&mut output, evidence_ref);
    output
}

fn strategy_lifecycle_code(lifecycle: insider_strategy_host::Lifecycle) -> u8 {
    match lifecycle {
        insider_strategy_host::Lifecycle::Research => 1,
        insider_strategy_host::Lifecycle::Validated => 2,
        insider_strategy_host::Lifecycle::Shadow => 3,
        insider_strategy_host::Lifecycle::Canary => 4,
        insider_strategy_host::Lifecycle::Production => 5,
        insider_strategy_host::Lifecycle::Paused => 6,
        insider_strategy_host::Lifecycle::Retired => 7,
    }
}

fn decode_strategy_lifecycle_transition(
    payload: &[u8],
) -> Result<(String, insider_strategy_host::Lifecycle, String, String), String> {
    if !payload.starts_with(STRATEGY_LIFECYCLE_TRANSITION_MAGIC) {
        return Err("invalid strategy lifecycle transition magic".into());
    }
    let mut cursor = STRATEGY_LIFECYCLE_TRANSITION_MAGIC.len();
    let strategy_id = read_string(payload, &mut cursor)?;
    let lifecycle = match read_u8(payload, &mut cursor)? {
        1 => insider_strategy_host::Lifecycle::Research,
        2 => insider_strategy_host::Lifecycle::Validated,
        3 => insider_strategy_host::Lifecycle::Shadow,
        4 => insider_strategy_host::Lifecycle::Canary,
        5 => insider_strategy_host::Lifecycle::Production,
        6 => insider_strategy_host::Lifecycle::Paused,
        7 => insider_strategy_host::Lifecycle::Retired,
        _ => return Err("invalid strategy lifecycle".into()),
    };
    let confirmation = read_string(payload, &mut cursor)?;
    let evidence_ref = read_string(payload, &mut cursor)?;
    if strategy_id.trim().is_empty()
        || evidence_ref.trim().is_empty()
        || evidence_ref.len() > 512
        || cursor != payload.len()
    {
        return Err("invalid strategy lifecycle transition bounds".into());
    }
    Ok((strategy_id, lifecycle, confirmation, evidence_ref))
}

/// Builds an authenticated operator metric lifecycle transition request.
#[must_use]
pub fn metric_lifecycle_transition_command_payload(
    metric_id: &str,
    lifecycle: insider_metric_host::Lifecycle,
    confirmation: &str,
    evidence_ref: &str,
) -> Vec<u8> {
    let mut output = METRIC_LIFECYCLE_TRANSITION_MAGIC.to_vec();
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
    push_string(&mut output, confirmation);
    push_string(&mut output, evidence_ref);
    output
}

fn decode_metric_lifecycle_transition(
    payload: &[u8],
) -> Result<(String, insider_metric_host::Lifecycle, String, String), String> {
    if !payload.starts_with(METRIC_LIFECYCLE_TRANSITION_MAGIC) {
        return Err("invalid metric lifecycle transition magic".into());
    }
    let mut cursor = METRIC_LIFECYCLE_TRANSITION_MAGIC.len();
    let metric_id = read_string(payload, &mut cursor)?;
    let lifecycle = match read_u8(payload, &mut cursor)? {
        1 => insider_metric_host::Lifecycle::Research,
        2 => insider_metric_host::Lifecycle::Validated,
        3 => insider_metric_host::Lifecycle::Shadow,
        4 => insider_metric_host::Lifecycle::Canary,
        5 => insider_metric_host::Lifecycle::Production,
        6 => insider_metric_host::Lifecycle::Paused,
        7 => insider_metric_host::Lifecycle::Retired,
        _ => return Err("invalid metric lifecycle".into()),
    };
    let confirmation = read_string(payload, &mut cursor)?;
    let evidence_ref = read_string(payload, &mut cursor)?;
    if metric_id.trim().is_empty()
        || evidence_ref.trim().is_empty()
        || evidence_ref.len() > 512
        || cursor != payload.len()
    {
        return Err("invalid metric lifecycle transition bounds".into());
    }
    Ok((metric_id, lifecycle, confirmation, evidence_ref))
}

/// Builds a bounded authenticated experiment mutation request.
#[must_use]
pub fn experiment_mutation_command_payload(mutation: &ExperimentMutation) -> Vec<u8> {
    let mut output = EXPERIMENT_MUTATE_MAGIC.to_vec();
    match mutation {
        ExperimentMutation::Create {
            run_id,
            code_hash,
            config_hash,
            dataset_hash,
        } => {
            output.push(1);
            push_string(&mut output, run_id);
            push_string(&mut output, code_hash);
            push_string(&mut output, config_hash);
            push_string(&mut output, dataset_hash);
        }
        ExperimentMutation::CreateWithProvenance {
            run_id,
            code_hash,
            config_hash,
            dataset_hash,
            provenance,
        } => {
            output.push(6);
            push_string(&mut output, run_id);
            push_string(&mut output, code_hash);
            push_string(&mut output, config_hash);
            push_string(&mut output, dataset_hash);
            encode_experiment_provenance(&mut output, provenance);
        }
        ExperimentMutation::Start { run_id } => {
            output.push(2);
            push_string(&mut output, run_id);
        }
        ExperimentMutation::Succeed { run_id, metrics } => {
            output.push(3);
            push_string(&mut output, run_id);
            output.extend_from_slice(
                &(u32::try_from(metrics.len()).unwrap_or(u32::MAX)).to_le_bytes(),
            );
            for (key, value) in metrics {
                push_string(&mut output, key);
                output.extend_from_slice(&value.to_le_bytes());
            }
        }
        ExperimentMutation::Fail { run_id } => {
            output.push(4);
            push_string(&mut output, run_id);
        }
        ExperimentMutation::AddArtifact { run_id, artifact } => {
            output.push(5);
            push_string(&mut output, run_id);
            push_string(&mut output, &artifact.kind);
            push_string(&mut output, &artifact.hash);
            push_string(&mut output, &artifact.path);
        }
    }
    output
}

fn encode_experiment_provenance(output: &mut Vec<u8>, provenance: &ExperimentProvenance) {
    let fields = [
        &provenance.strategy_id,
        &provenance.strategy_version,
        &provenance.news_dataset_hash,
        &provenance.news_clustering_version,
        &provenance.graph_snapshot_version,
        &provenance.llm_provider,
        &provenance.llm_model,
        &provenance.prompt_version,
        &provenance.tool_schema_version,
        &provenance.autonomy_config_hash,
    ];
    for field in fields {
        output.push(u8::from(field.is_some()));
        if let Some(value) = field {
            push_string(output, value);
        }
    }
    output.extend_from_slice(
        &u32::try_from(provenance.llm_cache_ids.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for cache_id in &provenance.llm_cache_ids {
        push_string(output, cache_id);
    }
}

fn decode_experiment_provenance(
    payload: &[u8],
    cursor: &mut usize,
) -> Result<ExperimentProvenance, String> {
    let mut provenance = ExperimentProvenance::default();
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
        let present = read_u8(payload, cursor)?;
        if present > 1 {
            return Err("invalid experiment provenance presence flag".into());
        }
        if present == 1 {
            *field = Some(read_string(payload, cursor)?);
        }
    }
    let count = usize::try_from(read_u32(payload, cursor)?)
        .map_err(|_| "invalid experiment cache ID count")?;
    if count > 256 {
        return Err("too many experiment cache IDs".into());
    }
    for _ in 0..count {
        provenance.llm_cache_ids.push(read_string(payload, cursor)?);
    }
    if !provenance.valid_for_replay() {
        return Err("invalid experiment provenance bounds or ordering".into());
    }
    Ok(provenance)
}

fn decode_experiment_mutation(payload: &[u8]) -> Result<ExperimentMutation, String> {
    if !payload.starts_with(EXPERIMENT_MUTATE_MAGIC) {
        return Err("invalid experiment mutation magic".into());
    }
    let mut cursor = EXPERIMENT_MUTATE_MAGIC.len();
    let operation = read_u8(payload, &mut cursor)?;
    let run_id = read_string(payload, &mut cursor)?;
    if run_id.trim().is_empty() {
        return Err("experiment run ID is required".into());
    }
    let mutation = match operation {
        1 => ExperimentMutation::Create {
            run_id,
            code_hash: read_string(payload, &mut cursor)?,
            config_hash: read_string(payload, &mut cursor)?,
            dataset_hash: read_string(payload, &mut cursor)?,
        },
        6 => ExperimentMutation::CreateWithProvenance {
            run_id,
            code_hash: read_string(payload, &mut cursor)?,
            config_hash: read_string(payload, &mut cursor)?,
            dataset_hash: read_string(payload, &mut cursor)?,
            provenance: Box::new(decode_experiment_provenance(payload, &mut cursor)?),
        },
        2 => ExperimentMutation::Start { run_id },
        3 => {
            let count = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid experiment metric count")?;
            if count > 4096 {
                return Err("too many experiment metrics".into());
            }
            let mut metrics = BTreeMap::new();
            for _ in 0..count {
                let key = read_string(payload, &mut cursor)?;
                let value = read_f64(payload, &mut cursor)?;
                if key.trim().is_empty() || !value.is_finite() {
                    return Err("invalid experiment metric".into());
                }
                metrics.insert(key, value);
            }
            ExperimentMutation::Succeed { run_id, metrics }
        }
        4 => ExperimentMutation::Fail { run_id },
        5 => ExperimentMutation::AddArtifact {
            run_id,
            artifact: ExperimentArtifact {
                kind: read_string(payload, &mut cursor)?,
                hash: read_string(payload, &mut cursor)?,
                path: read_string(payload, &mut cursor)?,
            },
        },
        _ => return Err("unknown experiment mutation".into()),
    };
    if cursor != payload.len() {
        return Err("trailing experiment mutation bytes".into());
    }
    Ok(mutation)
}

/// Builds an authenticated model lifecycle mutation request.
#[must_use]
pub fn model_mutation_command_payload(mutation: &ModelMutation) -> Vec<u8> {
    let mut output = MODEL_MUTATE_MAGIC.to_vec();
    match mutation {
        ModelMutation::Register { record, manifest } => {
            output.push(1);
            push_string(&mut output, &record.model_id);
            push_string(&mut output, &record.version);
            push_string(&mut output, &record.artifact_hash);
            push_string(&mut output, &record.input_schema_hash);
            push_string(&mut output, &record.output_schema_hash);
            output.extend_from_slice(
                &(u64::try_from(record.input_width).unwrap_or(u64::MAX)).to_le_bytes(),
            );
            push_string(&mut output, &manifest.code_hash);
            push_string(&mut output, &manifest.training_data_hash);
            push_string(&mut output, &manifest.config_hash);
            push_string(&mut output, &manifest.feature_hash);
            push_string(&mut output, &manifest.calibration_hash);
            push_string(&mut output, &manifest.artifact_hash);
        }
        ModelMutation::Validate {
            model_id,
            version,
            evidence_id,
        } => {
            output.push(2);
            push_string(&mut output, model_id);
            push_string(&mut output, version);
            push_string(&mut output, evidence_id);
        }
        ModelMutation::Shadow { model_id, version } => {
            output.push(3);
            push_string(&mut output, model_id);
            push_string(&mut output, version);
        }
        ModelMutation::Canary {
            model_id,
            version,
            evidence_id,
        } => {
            output.push(4);
            push_string(&mut output, model_id);
            push_string(&mut output, version);
            push_string(&mut output, evidence_id);
        }
        ModelMutation::Promote { model_id, version } => {
            output.push(5);
            push_string(&mut output, model_id);
            push_string(&mut output, version);
        }
    }
    output
}

fn decode_model_mutation(payload: &[u8]) -> Result<ModelMutation, String> {
    if !payload.starts_with(MODEL_MUTATE_MAGIC) {
        return Err("invalid model mutation magic".into());
    }
    let mut cursor = MODEL_MUTATE_MAGIC.len();
    let operation = read_u8(payload, &mut cursor)?;
    let read_identity = |cursor: &mut usize| -> Result<(String, String), String> {
        let model_id = read_string(payload, cursor)?;
        let version = read_string(payload, cursor)?;
        if model_id.trim().is_empty() || version.trim().is_empty() {
            return Err("model identity is required".into());
        }
        Ok((model_id, version))
    };
    let mutation = match operation {
        1 => {
            let (model_id, version) = read_identity(&mut cursor)?;
            let artifact_hash = read_string(payload, &mut cursor)?;
            let input_schema_hash = read_string(payload, &mut cursor)?;
            let output_schema_hash = read_string(payload, &mut cursor)?;
            let input_width = usize::try_from(read_u64(payload, &mut cursor)?)
                .map_err(|_| "invalid model input width")?;
            let manifest = ArtifactManifest {
                code_hash: read_string(payload, &mut cursor)?,
                training_data_hash: read_string(payload, &mut cursor)?,
                config_hash: read_string(payload, &mut cursor)?,
                feature_hash: read_string(payload, &mut cursor)?,
                calibration_hash: read_string(payload, &mut cursor)?,
                artifact_hash: read_string(payload, &mut cursor)?,
            };
            ModelMutation::Register {
                record: ModelRecord {
                    model_id,
                    version,
                    artifact_hash,
                    input_schema_hash,
                    output_schema_hash,
                    input_width,
                    status: ModelStatus::Research,
                },
                manifest,
            }
        }
        2 => {
            let (model_id, version) = read_identity(&mut cursor)?;
            ModelMutation::Validate {
                model_id,
                version,
                evidence_id: read_string(payload, &mut cursor)?,
            }
        }
        3 => {
            let (model_id, version) = read_identity(&mut cursor)?;
            ModelMutation::Shadow { model_id, version }
        }
        4 => {
            let (model_id, version) = read_identity(&mut cursor)?;
            ModelMutation::Canary {
                model_id,
                version,
                evidence_id: read_string(payload, &mut cursor)?,
            }
        }
        5 => {
            let (model_id, version) = read_identity(&mut cursor)?;
            ModelMutation::Promote { model_id, version }
        }
        _ => return Err("unknown model mutation".into()),
    };
    if cursor != payload.len() {
        return Err("trailing model mutation bytes".into());
    }
    Ok(mutation)
}

fn decode_proposal_preview_request(
    payload: &[u8],
) -> Result<(ProposalId, f64, MonoTime, TraceId, u64), String> {
    if !payload.starts_with(PROPOSAL_PREVIEW_MAGIC) {
        return Err("invalid proposal preview magic".into());
    }
    let mut cursor = PROPOSAL_PREVIEW_MAGIC.len();
    let proposal =
        ProposalId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid proposal ID")?;
    let scale = read_f64(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let trace = TraceId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid trace ID")?;
    let ttl = read_u64(payload, &mut cursor)?;
    if cursor != payload.len() || !scale.is_finite() || !(0.0..=1.0).contains(&scale) || ttl == 0 {
        return Err("invalid proposal preview bounds".into());
    }
    Ok((proposal, scale, now, trace, ttl))
}

fn decode_proposal_submit_request(
    payload: &[u8],
) -> Result<(ProposalId, f64, String, TraceId), String> {
    if !payload.starts_with(PROPOSAL_SUBMIT_MAGIC) {
        return Err("invalid proposal submit magic".into());
    }
    let mut cursor = PROPOSAL_SUBMIT_MAGIC.len();
    let proposal =
        ProposalId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid proposal ID")?;
    let scale = read_f64(payload, &mut cursor)?;
    let confirmation = read_string(payload, &mut cursor)?;
    let trace = TraceId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid trace ID")?;
    if cursor != payload.len() || !scale.is_finite() || !(0.0..=1.0).contains(&scale) {
        return Err("invalid proposal submit bounds".into());
    }
    Ok((proposal, scale, confirmation, trace))
}

fn decode_scheduled_proposal_request(
    payload: &[u8],
) -> Result<(ProposalId, Schedule, String, TraceId), String> {
    if !payload.starts_with(SCHEDULED_PROPOSAL_MAGIC) {
        return Err("invalid scheduled proposal magic".into());
    }
    let mut cursor = SCHEDULED_PROPOSAL_MAGIC.len();
    let proposal =
        ProposalId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid proposal ID")?;
    let schedule = match read_u8(payload, &mut cursor)? {
        0 => Schedule::Immediate,
        1 => Schedule::Twap {
            slices: usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid TWAP slices")?,
            interval_ns: read_u64(payload, &mut cursor)?,
        },
        2 => {
            let count = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid VWAP count")?;
            if count == 0 || count > 16_384 {
                return Err("VWAP count is outside bounds".into());
            }
            let mut weights = Vec::with_capacity(count);
            for _ in 0..count {
                weights.push(read_u32(payload, &mut cursor)?);
            }
            Schedule::Vwap { weights }
        }
        3 => {
            let participation_bps = read_u32(payload, &mut cursor)?;
            let interval_ns = read_u64(payload, &mut cursor)?;
            let count = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid POV count")?;
            if count == 0 || count > 16_384 {
                return Err("POV count is outside bounds".into());
            }
            let mut market_volume_ticks = Vec::with_capacity(count);
            for _ in 0..count {
                market_volume_ticks.push(read_i64(payload, &mut cursor)?);
            }
            Schedule::Pov {
                participation_bps,
                interval_ns,
                market_volume_ticks,
            }
        }
        4 => Schedule::ImplementationShortfall {
            slices: usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid IS slices")?,
            interval_ns: read_u64(payload, &mut cursor)?,
            urgency_bps: read_u32(payload, &mut cursor)?,
        },
        5 => {
            let slices = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid adaptive slices")?;
            let interval_ns = read_u64(payload, &mut cursor)?;
            let urgency_bps = read_u32(payload, &mut cursor)?;
            let spread_ticks = read_i64(payload, &mut cursor)?;
            let max_spread_ticks = read_i64(payload, &mut cursor)?;
            let volatility_bps = read_u32(payload, &mut cursor)?;
            let max_volatility_bps = read_u32(payload, &mut cursor)?;
            let count = usize::try_from(read_u32(payload, &mut cursor)?)
                .map_err(|_| "invalid adaptive volume count")?;
            if count == 0 || count > 16_384 {
                return Err("adaptive volume count is outside bounds".into());
            }
            let mut market_volume_ticks = Vec::with_capacity(count);
            for _ in 0..count {
                market_volume_ticks.push(read_i64(payload, &mut cursor)?);
            }
            Schedule::Adaptive {
                slices,
                interval_ns,
                urgency_bps,
                spread_ticks,
                max_spread_ticks,
                volatility_bps,
                max_volatility_bps,
                market_volume_ticks,
            }
        }
        _ => return Err("unknown execution schedule".into()),
    };
    let confirmation = read_string(payload, &mut cursor)?;
    let trace = TraceId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid trace ID")?;
    if cursor != payload.len() || confirmation.trim().is_empty() {
        return Err("invalid scheduled proposal bounds".into());
    }
    Ok((proposal, schedule, confirmation, trace))
}

fn decode_autonomy_mode_request(payload: &[u8]) -> Result<insider_autonomy::Mode, String> {
    if payload.len() != AUTONOMY_MODE_MAGIC.len() + 1 || !payload.starts_with(AUTONOMY_MODE_MAGIC) {
        return Err("invalid autonomy mode payload".into());
    }
    match payload[AUTONOMY_MODE_MAGIC.len()] {
        1 => Ok(insider_autonomy::Mode::Manual),
        2 => Ok(insider_autonomy::Mode::Hybrid),
        3 => Ok(insider_autonomy::Mode::Autonomous),
        _ => Err("invalid autonomy mode".into()),
    }
}

/// Builds an authenticated persisted trading-mode change.
#[must_use]
pub fn autonomy_mode_command_payload(mode: insider_autonomy::Mode) -> Vec<u8> {
    let mut output = AUTONOMY_MODE_MAGIC.to_vec();
    output.push(match mode {
        insider_autonomy::Mode::Manual => 1,
        insider_autonomy::Mode::Hybrid => 2,
        insider_autonomy::Mode::Autonomous => 3,
    });
    output
}

fn autonomy_mode_response(mode: insider_autonomy::Mode) -> Vec<u8> {
    vec![
        b'I',
        b'T',
        b'_',
        b'C',
        b'M',
        b'D',
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
        b'R',
        b'E',
        b'S',
        b'P',
        b'O',
        b'N',
        b'S',
        b'E',
        b'_',
        b'V',
        b'1',
        0,
        match mode {
            insider_autonomy::Mode::Manual => 1,
            insider_autonomy::Mode::Hybrid => 2,
            insider_autonomy::Mode::Autonomous => 3,
        },
    ]
}

/// Builds a versioned manual-submit command payload from a previously returned
/// preview. The current monotonic time is included so expiry cannot be bypassed
/// by a transport adapter.
#[must_use]
pub fn submit_command_payload(
    preview: &ManualOrderPreview,
    now: MonoTime,
    confirmation: &str,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(SUBMIT_MAGIC);
    push_string(&mut output, confirmation);
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    push_string(&mut output, &preview.preview_id);
    output.extend_from_slice(&preview.expected_state_version.to_le_bytes());
    output.extend_from_slice(&preview.expires_mono_ns.to_le_bytes());
    output.extend_from_slice(&preview.target_quantity_ticks.to_le_bytes());
    output.extend_from_slice(&preview.proposal_id.get().to_le_bytes());
    let intent = encode_order_intent(&preview.intent);
    output.extend_from_slice(&(u32::try_from(intent.len()).unwrap_or(u32::MAX)).to_le_bytes());
    output.extend_from_slice(&intent);
    output.extend_from_slice(
        &(u16::try_from(preview.warnings.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for warning in &preview.warnings {
        push_string(&mut output, warning);
    }
    output
}

/// Builds an idempotent cancellation command for a durable client order ID.
#[must_use]
pub fn cancel_command_payload(client_order_id: &str) -> Vec<u8> {
    let mut output = Vec::with_capacity(CANCEL_MAGIC.len() + client_order_id.len() + 2);
    output.extend_from_slice(CANCEL_MAGIC);
    push_string(&mut output, client_order_id);
    output
}

/// Builds a replacement command for a working order. A missing limit price
/// preserves market-order semantics; the engine and broker capability matrix
/// remain authoritative for whether the replacement is allowed.
#[must_use]
pub fn replace_command_payload(
    client_order_id: &str,
    quantity_ticks: i64,
    limit_price_ticks: Option<i64>,
) -> Vec<u8> {
    let mut output = Vec::with_capacity(REPLACE_MAGIC.len() + client_order_id.len() + 19);
    output.extend_from_slice(REPLACE_MAGIC);
    push_string(&mut output, client_order_id);
    output.extend_from_slice(&quantity_ticks.to_le_bytes());
    output.push(u8::from(limit_price_ticks.is_some()));
    output.extend_from_slice(&limit_price_ticks.unwrap_or_default().to_le_bytes());
    output
}

/// Builds a bounded read-only AI Analyst completion request.
#[must_use]
pub fn llm_complete_command_payload(request: &LlmRequest) -> Vec<u8> {
    llm_command_payload(LLM_COMPLETE_MAGIC, request)
}

/// Builds a bounded request for strict autonomous-action validation. This
/// command returns a validated action and never submits an order.
#[must_use]
pub fn llm_action_command_payload(request: &LlmRequest) -> Vec<u8> {
    llm_command_payload(LLM_ACTION_MAGIC, request)
}

/// Builds a bounded display-only streaming request. Partial stream items are
/// never parsed as autonomous actions.
#[must_use]
pub fn llm_stream_command_payload(request: &LlmRequest) -> Vec<u8> {
    llm_command_payload(LLM_STREAM_MAGIC, request)
}

fn llm_command_payload(magic: &[u8], request: &LlmRequest) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(magic);
    push_string(&mut output, &request.trace_id);
    push_string(&mut output, &request.prompt_version);
    push_string(&mut output, &request.model);
    push_string(&mut output, &request.task);
    push_string(&mut output, &request.context_hash);
    push_string(&mut output, &request.input);
    output.extend_from_slice(&request.max_output_tokens.to_le_bytes());
    output.push(match request.endpoint {
        Endpoint::Responses => 1,
        Endpoint::ChatCompletions => 2,
    });
    output
}

/// Builds an authenticated, bounded strategy-resolution request with explicit
/// per-strategy quantity budgets.
#[must_use]
pub fn strategy_resolution_budgeted_command_payload(
    policy: StrategyPolicy,
    now: MonoTime,
    budgets: &BTreeMap<String, StrategyBudget>,
) -> Vec<u8> {
    let mut output = STRATEGY_RESOLUTION_BUDGETED_MAGIC.to_vec();
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

fn decode_strategy_resolution_budgeted(
    payload: &[u8],
) -> Result<(StrategyPolicy, MonoTime, BTreeMap<String, StrategyBudget>), String> {
    if !payload.starts_with(STRATEGY_RESOLUTION_BUDGETED_MAGIC) {
        return Err("invalid budgeted strategy resolution magic".into());
    }
    let mut cursor = STRATEGY_RESOLUTION_BUDGETED_MAGIC.len();
    let policy = match read_u8(payload, &mut cursor)? {
        1 => StrategyPolicy::IsolatedBooks,
        2 => StrategyPolicy::Priority,
        3 => StrategyPolicy::WeightedNet,
        _ => return Err("invalid strategy policy".into()),
    };
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let count = usize::from(read_u16(payload, &mut cursor)?);
    if count > 256 {
        return Err("too many strategy budgets".into());
    }
    let mut budgets = BTreeMap::new();
    for _ in 0..count {
        let strategy_id = read_string(payload, &mut cursor)?;
        let quantity = read_i64(payload, &mut cursor)?;
        let budget = StrategyBudget::new(quantity).ok_or("strategy budget must be positive")?;
        if budgets.insert(strategy_id, budget).is_some() {
            return Err("duplicate strategy budget".into());
        }
    }
    if cursor != payload.len() {
        return Err("budgeted strategy resolution has trailing bytes".into());
    }
    Ok((policy, now, budgets))
}

fn encode_budgeted_resolution_response(result: &BudgetedResultSet) -> Vec<u8> {
    let mut output = b"IT_CMD_STRATEGY_RESOLUTION_BUDGETED_RESPONSE_V1\0".to_vec();
    let accepted = result
        .result
        .accepted
        .iter()
        .take(4_096)
        .collect::<Vec<_>>();
    output.extend_from_slice(&(u16::try_from(accepted.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for proposal in accepted {
        output.extend_from_slice(&proposal.proposal_id.get().to_le_bytes());
        match proposal.action {
            insider_strategy_sdk::Action::TargetQuantity { quantity_ticks } => {
                output.push(1);
                output.extend_from_slice(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::Increase { quantity_ticks } => {
                output.push(2);
                output.extend_from_slice(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::Decrease { quantity_ticks } => {
                output.push(3);
                output.extend_from_slice(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::Close => {
                output.push(4);
                output.extend_from_slice(&0_i64.to_le_bytes());
            }
            insider_strategy_sdk::Action::NoAction => {
                output.push(5);
                output.extend_from_slice(&0_i64.to_le_bytes());
            }
            insider_strategy_sdk::Action::TargetWeight { weight } => {
                output.push(6);
                output.extend_from_slice(&weight.to_bits().to_le_bytes());
            }
        }
    }
    let adjustments = result.adjustments.iter().take(4_096).collect::<Vec<_>>();
    output.extend_from_slice(&(u16::try_from(adjustments.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for adjustment in adjustments {
        output.extend_from_slice(&adjustment.proposal_id.get().to_le_bytes());
        output.extend_from_slice(&adjustment.before_quantity_ticks.to_le_bytes());
        output.extend_from_slice(&adjustment.after_quantity_ticks.to_le_bytes());
    }
    output
}

fn encode_llm_action_response(trace_id: &str, action: &AutonomousAction) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_CMD_LLM_ACTION_RESPONSE_V1\0");
    push_string(&mut output, trace_id);
    output.push(match action.action_type {
        ActionType::ExecuteProposal => 1,
        ActionType::ExecuteProposalScaled => 2,
        ActionType::IgnoreProposal => 3,
        ActionType::PauseStrategy => 4,
        ActionType::ResumeStrategy => 5,
        ActionType::RequestReanalysis => 6,
        ActionType::AddToWatch => 7,
        ActionType::RemoveFromWatch => 8,
        ActionType::ReduceAutonomy => 9,
        ActionType::NoAction => 10,
    });
    push_string(
        &mut output,
        action.proposal_id.as_deref().unwrap_or_default(),
    );
    output.extend_from_slice(&action.scale.unwrap_or_default().to_le_bytes());
    let reasons = action.reason_codes.iter().take(256).collect::<Vec<_>>();
    output.extend_from_slice(
        &u16::try_from(reasons.len())
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for reason in reasons {
        push_string(&mut output, reason);
    }
    output
}

/// Builds a bounded exact/lexical/graph search request.
#[must_use]
pub fn context_search_command_payload(
    text: &str,
    graph_root: Option<&str>,
    max_depth: usize,
    limit: usize,
) -> Vec<u8> {
    context_search_command_payload_with_embedding(text, graph_root, max_depth, limit, None)
}

/// Builds a bounded context-search request with an optional query vector.
#[must_use]
pub fn context_search_command_payload_with_embedding(
    text: &str,
    graph_root: Option<&str>,
    max_depth: usize,
    limit: usize,
    embedding: Option<&[f32]>,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(if embedding.is_some() {
        CONTEXT_SEARCH_VECTOR_MAGIC
    } else {
        CONTEXT_SEARCH_MAGIC
    });
    push_string(&mut output, text);
    push_string(&mut output, graph_root.unwrap_or_default());
    output.extend_from_slice(&u16::try_from(max_depth).unwrap_or(u16::MAX).to_le_bytes());
    output.extend_from_slice(&u16::try_from(limit).unwrap_or(u16::MAX).to_le_bytes());
    if let Some(embedding) = embedding {
        output.extend_from_slice(
            &u16::try_from(embedding.len())
                .unwrap_or(u16::MAX)
                .to_le_bytes(),
        );
        for value in embedding {
            output.extend_from_slice(&value.to_le_bytes());
        }
    }
    output
}

/// Converts a previously returned preview wire payload into a submit payload.
/// Native terminal adapters use this to retain the exact risk-approved intent bytes;
/// they cannot invent quantities, client IDs, or warning fields at submit time.
///
/// # Errors
/// Returns an error when the preview is malformed, oversized, or has trailing
/// bytes. The resulting payload remains subject to normal engine revalidation.
pub fn submit_preview_payload(
    preview_payload: &[u8],
    now: MonoTime,
    confirmation: &str,
) -> Result<Vec<u8>, String> {
    if !preview_payload.starts_with(PREVIEW_MAGIC) || confirmation.trim().is_empty() {
        return Err("invalid preview submit input".into());
    }
    let mut cursor = PREVIEW_MAGIC.len();
    let preview_id = read_string(preview_payload, &mut cursor)?;
    let expected = read_u64(preview_payload, &mut cursor)?;
    let expires = read_u64(preview_payload, &mut cursor)?;
    let target = read_i64(preview_payload, &mut cursor)?;
    let proposal = read_u128(preview_payload, &mut cursor)?;
    let intent_length = usize::try_from(read_u32(preview_payload, &mut cursor)?)
        .map_err(|_| "invalid preview intent length")?;
    if intent_length == 0 || intent_length > MAX_COMMAND_BYTES {
        return Err("invalid preview intent length".into());
    }
    let intent = read_bytes(preview_payload, &mut cursor, intent_length)?.to_vec();
    // Estimates are informational and are not copied into the submit wire
    // format, but their bytes must still be present and bounded.
    let _estimated_notional = read_bytes(preview_payload, &mut cursor, 16)?;
    let _estimated_cost = read_i64(preview_payload, &mut cursor)?;
    let warning_count = usize::from(read_u16(preview_payload, &mut cursor)?);
    if warning_count > 128 {
        return Err("too many preview warnings".into());
    }
    let mut warnings = Vec::with_capacity(warning_count);
    for _ in 0..warning_count {
        warnings.push(read_string(preview_payload, &mut cursor)?);
    }
    if cursor != preview_payload.len() || preview_id.trim().is_empty() {
        return Err("invalid preview trailing bytes".into());
    }
    let mut output = Vec::new();
    output.extend_from_slice(SUBMIT_MAGIC);
    push_string(&mut output, confirmation);
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    push_string(&mut output, &preview_id);
    output.extend_from_slice(&expected.to_le_bytes());
    output.extend_from_slice(&expires.to_le_bytes());
    output.extend_from_slice(&target.to_le_bytes());
    output.extend_from_slice(&proposal.to_le_bytes());
    output.extend_from_slice(&(u32::try_from(intent.len()).unwrap_or(u32::MAX)).to_le_bytes());
    output.extend_from_slice(&intent);
    output.extend_from_slice(&(u16::try_from(warnings.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for warning in warnings {
        push_string(&mut output, &warning);
    }
    Ok(output)
}

fn capability_for(kind: u8) -> Option<&'static str> {
    match kind {
        SNAPSHOT
        | EVENTS
        | READ_MODEL_STATUS
        | TRACE_EVENTS
        | TRACE_EXPORT
        | NEWS_PAGE
        | NEWS_PROVIDER_STATUS
        | SUPERVISOR_STATUS
        | RISK_POLICY_STATUS
        | BROKER_STATUS
        | NEWS_DETAIL
        | STRATEGY_RESOLUTION_LIST
        | STRATEGY_EXECUTION_LIST
        | STRATEGY_REGISTRY_LIST
        | METRIC_REGISTRY_LIST
        | CONTEXT_SEARCH
        | CONFIG_STATUS => Some("runtime.read"),
        CONFIG_RELOAD => Some("config.write"),
        STRATEGY_RESOLUTION_BUDGETED => Some("strategy.resolve.write"),
        STRATEGY_LIFECYCLE_TRANSITION => Some("strategy.lifecycle.write"),
        METRIC_LIFECYCLE_TRANSITION => Some("metric.lifecycle.write"),
        LLM_COMPLETE | LLM_ACTION | LLM_STREAM => Some("llm.analyze"),
        STRATEGY_EVALUATE => Some("strategy.evaluate"),
        PROPOSAL_PREVIEW | PREVIEW => Some("order.preview"),
        PROPOSAL_SUBMIT | SCHEDULED_PROPOSAL_SUBMIT | SUBMIT => Some("order.submit"),
        BACKTEST_RUN
        | BACKTEST_LIST
        | STRATEGY_BACKTEST_RUN
        | EXPERIMENT_LIST
        | MODEL_LIST
        | EXPERIMENT_MUTATE
        | MODEL_MUTATE => Some("research.backtest"),
        AUTONOMY_MODE => Some("autonomy.mode.write"),
        CANCEL | REPLACE => Some("order.manage"),
        RESOLVE_SYMBOL => Some("instrument.read"),
        LIVE_CONFIGURE => Some("live.configure"),
        LIVE_ARM | LIVE_CONFIRM => Some("live.enable"),
        LIVE_KILL => Some("live.kill"),
        AUTONOMY_SUBMIT | AUTONOMY_TRANSITION => Some("autonomy.plan.write"),
        ALERTS_GET => Some("alerts.read"),
        ALERT_ACK => Some("alerts.ack"),
        READ_MODEL_BACKUP | READ_MODEL_RESTORE => Some("read_model.backup.write"),
        JOURNAL_BACKUP | JOURNAL_RESTORE => Some("journal.backup.write"),
        RISK_STATE_TRANSITION => Some("risk.state.write"),
        RISK_POLICY_SET => Some("risk.policy.write"),
        _ => None,
    }
}

/// Resolves the numeric command kind for both compact one-byte commands and
/// the legacy self-describing magic payloads. Keeping this compatibility layer
/// here lets existing clients remain wire-compatible while authorization still
/// occurs on a single canonical numeric kind.
fn command_kind(payload: &[u8]) -> Option<u8> {
    if payload.len() == 1
        || payload
            .first()
            .is_some_and(|kind| capability_for(*kind).is_some())
    {
        return Some(payload[0]);
    }
    [
        (PREVIEW_MAGIC, PREVIEW),
        (SUBMIT_MAGIC, SUBMIT),
        (RESOLVE_MAGIC, RESOLVE_SYMBOL),
        (LIVE_CONFIGURE_MAGIC, LIVE_CONFIGURE),
        (LIVE_ARM_MAGIC, LIVE_ARM),
        (LIVE_CONFIRM_MAGIC, LIVE_CONFIRM),
        (CANCEL_MAGIC, CANCEL),
        (REPLACE_MAGIC, REPLACE),
        (LLM_COMPLETE_MAGIC, LLM_COMPLETE),
        (CONTEXT_SEARCH_MAGIC, CONTEXT_SEARCH),
        (CONTEXT_SEARCH_VECTOR_MAGIC, CONTEXT_SEARCH),
        (LLM_ACTION_MAGIC, LLM_ACTION),
        (LLM_STREAM_MAGIC, LLM_STREAM),
        (STRATEGY_EVALUATE_MAGIC, STRATEGY_EVALUATE),
        (PROPOSAL_PREVIEW_MAGIC, PROPOSAL_PREVIEW),
        (PROPOSAL_SUBMIT_MAGIC, PROPOSAL_SUBMIT),
        (SCHEDULED_PROPOSAL_MAGIC, SCHEDULED_PROPOSAL_SUBMIT),
        (AUTONOMY_MODE_MAGIC, AUTONOMY_MODE),
        (ALERT_ACK_MAGIC, ALERT_ACK),
        (BACKTEST_RUN_MAGIC, BACKTEST_RUN),
        (STRATEGY_BACKTEST_RUN_MAGIC, STRATEGY_BACKTEST_RUN),
        (
            STRATEGY_RESOLUTION_BUDGETED_MAGIC,
            STRATEGY_RESOLUTION_BUDGETED,
        ),
        (NEWS_PROVIDER_STATUS_MAGIC, NEWS_PROVIDER_STATUS),
        (SUPERVISOR_STATUS_MAGIC, SUPERVISOR_STATUS),
        (RISK_POLICY_STATUS_MAGIC, RISK_POLICY_STATUS),
        (BROKER_STATUS_MAGIC, BROKER_STATUS),
        (CONFIG_RELOAD_MAGIC, CONFIG_RELOAD),
        (RISK_POLICY_SET_MAGIC, RISK_POLICY_SET),
        (EXPERIMENT_MUTATE_MAGIC, EXPERIMENT_MUTATE),
        (MODEL_MUTATE_MAGIC, MODEL_MUTATE),
    ]
    .into_iter()
    .find_map(|(magic, kind)| payload.starts_with(magic).then_some(kind))
}

fn decode_llm_request(payload: &[u8]) -> Result<LlmRequest, String> {
    decode_llm_request_with_magic(payload, LLM_COMPLETE_MAGIC)
}

fn decode_llm_request_with_magic(payload: &[u8], magic: &[u8]) -> Result<LlmRequest, String> {
    if !payload.starts_with(magic) {
        return Err("invalid LLM command magic".into());
    }
    let mut cursor = magic.len();
    let trace_id = read_string(payload, &mut cursor)?;
    let prompt_version = read_string(payload, &mut cursor)?;
    let model = read_string(payload, &mut cursor)?;
    let task = read_string(payload, &mut cursor)?;
    let context_hash = read_string(payload, &mut cursor)?;
    let input = read_string(payload, &mut cursor)?;
    let max_output_tokens = read_u32(payload, &mut cursor)?;
    let endpoint = match read_bytes(payload, &mut cursor, 1)?[0] {
        1 => Endpoint::Responses,
        2 => Endpoint::ChatCompletions,
        _ => return Err("invalid LLM endpoint".into()),
    };
    if cursor != payload.len() {
        return Err("LLM request has trailing bytes".into());
    }
    let request = LlmRequest {
        trace_id,
        prompt_version,
        model,
        task,
        context_hash,
        input,
        max_output_tokens,
        endpoint,
    };
    request
        .validate()
        .map_err(|error| format!("invalid LLM request: {error:?}"))?;
    Ok(request)
}

struct StrategyEvaluateRequest {
    strategy_id: String,
    metric_id: String,
    metric: MetricOutput,
    entry_threshold: f64,
    exit_threshold: f64,
    quantity_ticks: i64,
    horizon_ns: u64,
    strategy_ttl_ns: u64,
    now: MonoTime,
}

fn decode_strategy_evaluate_request(payload: &[u8]) -> Result<StrategyEvaluateRequest, String> {
    if !payload.starts_with(STRATEGY_EVALUATE_MAGIC) {
        return Err("invalid strategy evaluation magic".into());
    }
    let mut cursor = STRATEGY_EVALUATE_MAGIC.len();
    let strategy_id = read_string(payload, &mut cursor)?;
    let metric_id = read_string(payload, &mut cursor)?;
    let instrument_id = InstrumentId::new(read_u128(payload, &mut cursor)?)
        .map_err(|_| "invalid metric instrument")?;
    let generated_mono = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let metric_ttl_ns = read_u64(payload, &mut cursor)?;
    let score = read_f64(payload, &mut cursor)?;
    let confidence = read_f64(payload, &mut cursor)?;
    let uncertainty = read_f64(payload, &mut cursor)?;
    let entry_threshold = read_f64(payload, &mut cursor)?;
    let exit_threshold = read_f64(payload, &mut cursor)?;
    let quantity_ticks = read_i64(payload, &mut cursor)?;
    let horizon_ns = read_u64(payload, &mut cursor)?;
    let strategy_ttl_ns = read_u64(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    if strategy_id.trim().is_empty()
        || metric_id.trim().is_empty()
        || metric_ttl_ns == 0
        || !score.is_finite()
        || !confidence.is_finite()
        || !uncertainty.is_finite()
        || cursor != payload.len()
    {
        return Err("strategy evaluation request is outside bounds".into());
    }
    Ok(StrategyEvaluateRequest {
        strategy_id,
        metric_id: metric_id.clone(),
        metric: MetricOutput {
            metric_id,
            instrument_id,
            generated_mono,
            ttl_ns: metric_ttl_ns,
            score,
            confidence,
            uncertainty,
        },
        entry_threshold,
        exit_threshold,
        quantity_ticks,
        horizon_ns,
        strategy_ttl_ns,
        now,
    })
}

/// Builds a strategy evaluation command from one immutable metric snapshot.
#[must_use]
#[allow(clippy::too_many_arguments)]
pub fn strategy_evaluate_command_payload(
    strategy_id: &str,
    metric_id: &str,
    metric: &MetricOutput,
    entry_threshold: f64,
    exit_threshold: f64,
    quantity_ticks: i64,
    horizon_ns: u64,
    strategy_ttl_ns: u64,
    now: MonoTime,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(STRATEGY_EVALUATE_MAGIC);
    push_string(&mut output, strategy_id);
    push_string(&mut output, metric_id);
    output.extend_from_slice(&metric.instrument_id.get().to_le_bytes());
    output.extend_from_slice(&metric.generated_mono.as_nanos().to_le_bytes());
    output.extend_from_slice(&metric.ttl_ns.to_le_bytes());
    for value in [
        metric.score,
        metric.confidence,
        metric.uncertainty,
        entry_threshold,
        exit_threshold,
    ] {
        output.extend_from_slice(&value.to_le_bytes());
    }
    output.extend_from_slice(&quantity_ticks.to_le_bytes());
    output.extend_from_slice(&horizon_ns.to_le_bytes());
    output.extend_from_slice(&strategy_ttl_ns.to_le_bytes());
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    output
}

fn encode_strategy_proposal_response(proposal: &insider_strategy_sdk::Proposal) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_CMD_STRATEGY_PROPOSAL_RESPONSE_V1\0");
    output.extend_from_slice(&proposal.proposal_id.get().to_le_bytes());
    push_string(&mut output, &proposal.strategy_id);
    output.extend_from_slice(&proposal.instrument_id.get().to_le_bytes());
    output.push(match proposal.action {
        insider_strategy_sdk::Action::NoAction => 0,
        insider_strategy_sdk::Action::TargetQuantity { .. } => 1,
        insider_strategy_sdk::Action::TargetWeight { .. } => 2,
        insider_strategy_sdk::Action::Increase { .. } => 3,
        insider_strategy_sdk::Action::Decrease { .. } => 4,
        insider_strategy_sdk::Action::Close => 5,
    });
    let quantity = match proposal.action {
        insider_strategy_sdk::Action::TargetQuantity { quantity_ticks }
        | insider_strategy_sdk::Action::Increase { quantity_ticks }
        | insider_strategy_sdk::Action::Decrease { quantity_ticks } => quantity_ticks,
        _ => 0,
    };
    output.extend_from_slice(&quantity.to_le_bytes());
    let weight = match proposal.action {
        insider_strategy_sdk::Action::TargetWeight { weight } => weight,
        _ => 0.0,
    };
    output.extend_from_slice(&weight.to_le_bytes());
    output.extend_from_slice(&proposal.confidence.to_le_bytes());
    output.extend_from_slice(&proposal.generated_mono.as_nanos().to_le_bytes());
    output.extend_from_slice(&proposal.ttl_ns.to_le_bytes());
    output
}

fn decode_context_search_request(payload: &[u8]) -> Result<(RetrievalQuery, usize), String> {
    let (magic, has_embedding) = if payload.starts_with(CONTEXT_SEARCH_MAGIC) {
        (CONTEXT_SEARCH_MAGIC, false)
    } else if payload.starts_with(CONTEXT_SEARCH_VECTOR_MAGIC) {
        (CONTEXT_SEARCH_VECTOR_MAGIC, true)
    } else {
        return Err("invalid context search magic".into());
    };
    let mut cursor = magic.len();
    let text = read_string(payload, &mut cursor)?;
    let root = read_string(payload, &mut cursor)?;
    let max_depth = usize::from(read_u16(payload, &mut cursor)?);
    let limit = usize::from(read_u16(payload, &mut cursor)?);
    let embedding = if has_embedding {
        let dimensions = usize::from(read_u16(payload, &mut cursor)?);
        if dimensions == 0 || dimensions > 4_096 {
            return Err("context embedding dimensions are outside bounds".into());
        }
        let mut values = Vec::with_capacity(dimensions);
        for _ in 0..dimensions {
            let value = f32::from_le_bytes(
                read_bytes(payload, &mut cursor, 4)?
                    .try_into()
                    .map_err(|_| "context embedding value")?,
            );
            if !value.is_finite() {
                return Err("context embedding contains a non-finite value".into());
            }
            values.push(value);
        }
        Some(values)
    } else {
        None
    };
    if text.trim().is_empty()
        || text.len() > 16_384
        || max_depth > 8
        || limit == 0
        || limit > 256
        || cursor != payload.len()
    {
        return Err("context search request is outside bounds".into());
    }
    Ok((
        RetrievalQuery {
            text,
            embedding,
            graph_root: (!root.trim().is_empty()).then_some(root),
            max_depth,
        },
        limit,
    ))
}

fn encode_context_search_response(hits: &[insider_context_graph::RetrievalHit]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_CMD_CONTEXT_SEARCH_RESPONSE_V1\0");
    output.extend_from_slice(&u16::try_from(hits.len()).unwrap_or(u16::MAX).to_le_bytes());
    for hit in hits.iter().take(256) {
        push_string(&mut output, &hit.node_id);
        output.extend_from_slice(&hit.score.to_le_bytes());
        output.extend_from_slice(&hit.exact_score.to_le_bytes());
        output.extend_from_slice(&hit.lexical_score.to_le_bytes());
        output.extend_from_slice(&hit.vector_score.to_le_bytes());
        output.extend_from_slice(
            &u16::try_from(hit.evidence_path.len())
                .unwrap_or(u16::MAX)
                .to_le_bytes(),
        );
        for node in hit.evidence_path.iter().take(32) {
            push_string(&mut output, node);
        }
    }
    output
}

fn encode_llm_response(response: &insider_llm_core::Response) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_CMD_LLM_COMPLETE_RESPONSE_V1\0");
    push_string(&mut output, &response.trace_id);
    push_string(&mut output, &response.finish_reason);
    push_string(&mut output, &response.content);
    output
}

fn encode_llm_stream_response(trace_id: &str, items: &[StreamItem]) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_CMD_LLM_STREAM_RESPONSE_V1\0");
    push_string(&mut output, trace_id);
    let bounded = items.iter().take(4_096);
    let count = u32::try_from(items.len().min(4_096)).unwrap_or(4_096);
    output.extend_from_slice(&count.to_le_bytes());
    for item in bounded {
        match item {
            StreamItem::Delta(delta) => {
                output.push(1);
                push_string(&mut output, delta);
            }
            StreamItem::Done(reason) => {
                output.push(2);
                push_string(&mut output, reason);
            }
        }
    }
    output
}

fn decode_cancel_request(payload: &[u8]) -> Result<String, String> {
    if !payload.starts_with(CANCEL_MAGIC) {
        return Err("invalid cancel command magic".into());
    }
    let mut cursor = CANCEL_MAGIC.len();
    let client_order_id = read_string(payload, &mut cursor)?;
    if client_order_id.trim().is_empty() || cursor != payload.len() {
        return Err("invalid cancel request bounds".into());
    }
    Ok(client_order_id)
}

fn decode_replace_request(payload: &[u8]) -> Result<(String, i64, Option<i64>), String> {
    if !payload.starts_with(REPLACE_MAGIC) {
        return Err("invalid replace command magic".into());
    }
    let mut cursor = REPLACE_MAGIC.len();
    let client_order_id = read_string(payload, &mut cursor)?;
    let quantity_ticks = read_i64(payload, &mut cursor)?;
    let has_limit = read_bytes(payload, &mut cursor, 1)?
        .first()
        .copied()
        .ok_or("replace limit marker truncated")?;
    let limit_price_ticks = read_i64(payload, &mut cursor)?;
    if client_order_id.trim().is_empty()
        || quantity_ticks <= 0
        || has_limit > 1
        || (has_limit == 1 && limit_price_ticks <= 0)
        || cursor != payload.len()
    {
        return Err("invalid replace request bounds".into());
    }
    Ok((
        client_order_id,
        quantity_ticks,
        (has_limit == 1).then_some(limit_price_ticks),
    ))
}

/// Builds a bounded autonomous-plan submission command from a validated plan.
#[must_use]
pub fn autonomy_submit_command_payload(plan: &insider_autonomy::Plan) -> Vec<u8> {
    let mut output = vec![AUTONOMY_SUBMIT];
    output.extend_from_slice(&insider_autonomy::encode_plan_event(
        &insider_autonomy::PlanEvent::Submitted(plan.clone()),
    ));
    output
}

/// Builds a lifecycle transition command with injected monotonic time.
#[must_use]
pub fn autonomy_transition_command_payload(
    plan_id: &str,
    state: insider_autonomy::PlanState,
    now: MonoTime,
) -> Vec<u8> {
    let mut output = vec![AUTONOMY_TRANSITION, plan_state_code(state)];
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    push_string(&mut output, plan_id);
    output
}

/// Builds an authenticated read-model backup command.
#[must_use]
pub fn read_model_backup_command_payload(destination: &str) -> Vec<u8> {
    let mut output = vec![READ_MODEL_BACKUP];
    push_string(&mut output, destination);
    output
}

/// Builds an authenticated read-model restore command.
#[must_use]
pub fn read_model_restore_command_payload(source: &str, destination: &str) -> Vec<u8> {
    let mut output = vec![READ_MODEL_RESTORE];
    push_string(&mut output, source);
    push_string(&mut output, destination);
    output
}

/// Builds an authenticated journal backup command.
#[must_use]
pub fn journal_backup_command_payload(destination: &str) -> Vec<u8> {
    let mut output = vec![JOURNAL_BACKUP];
    push_string(&mut output, destination);
    output
}

/// Builds an authenticated journal restore command.
#[must_use]
pub fn journal_restore_command_payload(source: &str, destination: &str) -> Vec<u8> {
    let mut output = vec![JOURNAL_RESTORE];
    push_string(&mut output, source);
    push_string(&mut output, destination);
    output
}

/// Builds an authenticated risk-state transition command.
#[must_use]
pub fn risk_state_transition_command_payload(state: RiskState, authorization: &str) -> Vec<u8> {
    let mut output = vec![RISK_STATE_TRANSITION, risk_state_code(state)];
    push_string(&mut output, authorization);
    output
}

/// Builds an authenticated, bounded scoped-risk-policy replacement command.
/// `None` explicitly clears scoped overrides and restores system limits.
///
/// # Errors
///
/// Returns an error when a scope/revision count exceeds the protocol bounds or
/// when a hard risk limit is non-positive.
pub fn scoped_risk_policy_command_payload(
    snapshot: Option<&ScopedRiskPolicySnapshot>,
) -> Result<Vec<u8>, String> {
    let mut output = RISK_POLICY_SET_MAGIC.to_vec();
    let Some(snapshot) = snapshot else {
        output.push(0);
        return Ok(output);
    };
    output.push(1);
    append_command_revisions(&mut output, &snapshot.system)?;
    if snapshot.accounts.len() > 1024
        || snapshot.strategies.len() > 1024
        || snapshot.assets.len() > 16
        || snapshot.instruments.len() > 16_384
    {
        return Err("scoped risk policy exceeds bounds".into());
    }
    output.extend_from_slice(
        &(u16::try_from(snapshot.accounts.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for (identity, revisions) in &snapshot.accounts {
        push_string(&mut output, identity);
        append_command_revisions(&mut output, revisions)?;
    }
    output.extend_from_slice(
        &(u16::try_from(snapshot.strategies.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for (identity, revisions) in &snapshot.strategies {
        push_string(&mut output, identity);
        append_command_revisions(&mut output, revisions)?;
    }
    output.push(u8::try_from(snapshot.assets.len()).unwrap_or(u8::MAX));
    for (asset, revisions) in &snapshot.assets {
        output.push(asset_class_code(*asset));
        append_command_revisions(&mut output, revisions)?;
    }
    output.extend_from_slice(
        &(u16::try_from(snapshot.instruments.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for (instrument, revisions) in &snapshot.instruments {
        output.extend_from_slice(&instrument.get().to_le_bytes());
        append_command_revisions(&mut output, revisions)?;
    }
    Ok(output)
}

fn append_command_revisions(output: &mut Vec<u8>, revisions: &[TimedLimits]) -> Result<(), String> {
    if revisions.len() > 256 {
        return Err("risk policy revision count exceeds bound".into());
    }
    output.extend_from_slice(&(u16::try_from(revisions.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for revision in revisions {
        if revision.limits.max_position_ticks <= 0
            || revision.limits.max_order_ticks <= 0
            || revision.limits.max_gross_notional_ticks <= 0
        {
            return Err("risk policy limits must be positive".into());
        }
        output.extend_from_slice(&revision.effective_mono_ns.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_position_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_order_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_gross_notional_ticks.to_le_bytes());
    }
    Ok(())
}

fn asset_class_code(asset: AssetClass) -> u8 {
    match asset {
        AssetClass::Equity => 1,
        AssetClass::Etf => 2,
        AssetClass::Option => 3,
        AssetClass::Future => 4,
        AssetClass::Fx => 5,
        AssetClass::Crypto => 6,
    }
}

fn decode_scoped_risk_policy_command(payload: &[u8]) -> Result<Option<ScopedRiskPolicy>, String> {
    if !payload.starts_with(RISK_POLICY_SET_MAGIC) {
        return Err("invalid risk policy command magic".into());
    }
    let mut cursor = RISK_POLICY_SET_MAGIC.len();
    let present = read_u8(payload, &mut cursor)?;
    if present > 1 {
        return Err("invalid risk policy presence".into());
    }
    if present == 0 {
        if cursor != payload.len() {
            return Err("risk policy clear has trailing bytes".into());
        }
        return Ok(None);
    }
    let system = read_command_revisions(payload, &mut cursor)?;
    let accounts = read_command_revision_map(payload, &mut cursor, 1024)?;
    let strategies = read_command_revision_map(payload, &mut cursor, 1024)?;
    let asset_count = usize::from(read_u8(payload, &mut cursor)?);
    if asset_count > 6 {
        return Err("asset policy count exceeds bound".into());
    }
    let mut assets = BTreeMap::new();
    for _ in 0..asset_count {
        let asset = match read_u8(payload, &mut cursor)? {
            1 => AssetClass::Equity,
            2 => AssetClass::Etf,
            3 => AssetClass::Option,
            4 => AssetClass::Future,
            5 => AssetClass::Fx,
            6 => AssetClass::Crypto,
            _ => return Err("invalid asset policy code".into()),
        };
        if assets
            .insert(asset, read_command_revisions(payload, &mut cursor)?)
            .is_some()
        {
            return Err("duplicate asset policy".into());
        }
    }
    let instrument_count = usize::from(read_u16(payload, &mut cursor)?);
    if instrument_count > 16_384 {
        return Err("instrument policy count exceeds bound".into());
    }
    let mut instruments = BTreeMap::new();
    for _ in 0..instrument_count {
        let instrument = InstrumentId::new(read_u128(payload, &mut cursor)?)
            .map_err(|_| "invalid instrument policy identity")?;
        if instruments
            .insert(instrument, read_command_revisions(payload, &mut cursor)?)
            .is_some()
        {
            return Err("duplicate instrument policy".into());
        }
    }
    if cursor != payload.len() {
        return Err("risk policy command has trailing bytes".into());
    }
    ScopedRiskPolicy::from_snapshot(ScopedRiskPolicySnapshot {
        system,
        accounts,
        strategies,
        assets,
        instruments,
    })
    .map(Some)
    .map_err(|error| format!("invalid scoped risk policy: {error:?}"))
}

fn read_command_revisions(payload: &[u8], cursor: &mut usize) -> Result<Vec<TimedLimits>, String> {
    let count = usize::from(read_u16(payload, cursor)?);
    if count > 256 {
        return Err("risk policy revisions exceed bound".into());
    }
    let mut revisions = Vec::with_capacity(count);
    for _ in 0..count {
        let revision = TimedLimits {
            effective_mono_ns: read_u64(payload, cursor)?,
            limits: RiskLimits {
                max_position_ticks: read_i64(payload, cursor)?,
                max_order_ticks: read_i64(payload, cursor)?,
                max_gross_notional_ticks: read_i128(payload, cursor)?,
            },
        };
        if revision.limits.max_position_ticks <= 0
            || revision.limits.max_order_ticks <= 0
            || revision.limits.max_gross_notional_ticks <= 0
        {
            return Err("risk policy limits must be positive".into());
        }
        revisions.push(revision);
    }
    Ok(revisions)
}

fn read_command_revision_map(
    payload: &[u8],
    cursor: &mut usize,
    maximum: usize,
) -> Result<BTreeMap<String, Vec<TimedLimits>>, String> {
    let count = usize::from(read_u16(payload, cursor)?);
    if count > maximum {
        return Err("risk policy scope count exceeds bound".into());
    }
    let mut output = BTreeMap::new();
    for _ in 0..count {
        let identity = read_string(payload, cursor)?;
        if identity.trim().is_empty()
            || output
                .insert(identity, read_command_revisions(payload, cursor)?)
                .is_some()
        {
            return Err("invalid or duplicate risk policy identity".into());
        }
    }
    Ok(output)
}

/// Builds a read-only read-model health and cursor query.
#[must_use]
pub const fn read_model_status_command_payload() -> [u8; 1] {
    [READ_MODEL_STATUS]
}

/// Builds a bounded trace reconstruction query.
#[must_use]
pub fn trace_events_command_payload(trace_id: TraceId) -> [u8; 17] {
    let mut output = [0_u8; 17];
    output[0] = TRACE_EVENTS;
    output[1..17].copy_from_slice(&trace_id.get().to_le_bytes());
    output
}

/// Builds a bounded redacted trace export query.
#[must_use]
pub fn trace_export_command_payload(trace_id: TraceId) -> [u8; 17] {
    let mut output = [0_u8; 17];
    output[0] = TRACE_EXPORT;
    output[1..17].copy_from_slice(&trace_id.get().to_le_bytes());
    output
}

/// Builds a bounded all-news cursor query. Scope/symbol are retained in the
/// transport contract for future deterministic relevance filtering.
#[must_use]
pub fn news_page_command_payload(scope: &str, symbol: &str, after_cursor: Option<&str>) -> Vec<u8> {
    let mut output = vec![NEWS_PAGE];
    push_string(&mut output, scope);
    push_string(&mut output, symbol);
    push_string(&mut output, after_cursor.unwrap_or_default());
    output
}

/// Builds a read-only provider health query.
#[must_use]
pub fn news_provider_status_command_payload() -> [u8; 1] {
    [NEWS_PROVIDER_STATUS]
}

/// Builds an authenticated supervisor operational-status request.
#[must_use]
pub const fn supervisor_status_command_payload() -> [u8; 1] {
    [SUPERVISOR_STATUS]
}

/// Builds an authenticated scoped risk-policy status request.
#[must_use]
pub const fn risk_policy_status_command_payload() -> [u8; 1] {
    [RISK_POLICY_STATUS]
}

/// Builds an authenticated broker session/account health request.
#[must_use]
pub const fn broker_status_command_payload() -> [u8; 1] {
    [BROKER_STATUS]
}

/// Builds a bounded authoritative news-detail query.
#[must_use]
pub fn news_detail_command_payload(item_id: &str) -> Vec<u8> {
    let mut output = vec![NEWS_DETAIL];
    push_string(&mut output, item_id);
    output
}

fn decode_news_page_request(payload: &[u8]) -> Result<(String, String, Option<String>), String> {
    if payload.first().copied() != Some(NEWS_PAGE) {
        return Err("invalid news page command".into());
    }
    let mut cursor = 1;
    let scope = read_string(payload, &mut cursor)?;
    let symbol = read_string(payload, &mut cursor)?;
    let after = read_string(payload, &mut cursor)?;
    if (scope != "all" && scope != "relevant")
        || !symbol.is_empty()
            && !symbol
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || ".-".contains(character))
        || cursor != payload.len()
    {
        return Err("invalid news page bounds".into());
    }
    Ok((scope, symbol, (!after.is_empty()).then_some(after)))
}

fn decode_news_detail_request(payload: &[u8]) -> Result<String, String> {
    if payload.first().copied() != Some(NEWS_DETAIL) {
        return Err("invalid news detail command".into());
    }
    let mut cursor = 1;
    let item_id = read_string(payload, &mut cursor)?;
    if item_id.trim().is_empty() || cursor != payload.len() {
        return Err("invalid news detail bounds".into());
    }
    Ok(item_id)
}

fn encode_news_page(page: &insider_news_core::NewsPage) -> Vec<u8> {
    let mut output = b"IT_CMD_NEWS_PAGE_V2\0".to_vec();
    output.extend_from_slice(&(u32::try_from(page.items.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for item in &page.items {
        push_string(&mut output, &item.id);
        push_string(&mut output, &item.title);
        push_string(&mut output, &item.source_name);
        push_string(&mut output, &item.canonical_url);
        output.extend_from_slice(&item.published_at_ms.unwrap_or_default().to_le_bytes());
        output.extend_from_slice(&item.received_at_ms.to_le_bytes());
        output.extend_from_slice(
            &(u16::try_from(item.symbols.len()).unwrap_or(u16::MAX)).to_le_bytes(),
        );
        for symbol in &item.symbols {
            push_string(&mut output, symbol);
        }
        output.extend_from_slice(
            &page
                .relevance_scores_bps
                .get(&item.id)
                .copied()
                .unwrap_or(0)
                .to_le_bytes(),
        );
    }
    push_string(&mut output, page.next_cursor.as_deref().unwrap_or_default());
    output
}

fn encode_news_provider_statuses(statuses: &[insider_news_core::ProviderStatus]) -> Vec<u8> {
    let mut output = b"IT_CMD_NEWS_PROVIDER_STATUS_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(
        &u16::try_from(statuses.len().min(1024))
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for status in statuses.iter().take(1024) {
        push_string(&mut output, &status.provider_id);
        output.push(match status.health {
            insider_news_core::ProviderHealth::Unknown => 0,
            insider_news_core::ProviderHealth::Healthy => 1,
            insider_news_core::ProviderHealth::CoolingDown => 2,
            insider_news_core::ProviderHealth::Degraded => 3,
            insider_news_core::ProviderHealth::Failed => 4,
        });
        for timestamp in [
            status.last_success_ms,
            status.last_failure_ms,
            status.next_retry_ms,
        ] {
            output.push(u8::from(timestamp.is_some()));
            output.extend_from_slice(&timestamp.unwrap_or_default().to_le_bytes());
        }
        output.extend_from_slice(
            &u64::try_from(status.dead_letter_count)
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        output.extend_from_slice(&status.consecutive_failures.to_le_bytes());
    }
    output
}

fn encode_supervisor_snapshot(snapshot: &insider_supervisor::Snapshot) -> Vec<u8> {
    let mut output = b"IT_CMD_SUPERVISOR_STATUS_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(
        &u16::try_from(snapshot.components.len().min(128))
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for component in snapshot.components.iter().take(128) {
        push_string(&mut output, &component.name);
        output.push(match component.state {
            insider_supervisor::State::Running => 1,
            insider_supervisor::State::Backoff => 2,
            insider_supervisor::State::Quarantined => 3,
            insider_supervisor::State::Draining => 4,
        });
        output.push(match component.health {
            insider_supervisor::Health::Unknown => 0,
            insider_supervisor::Health::Healthy => 1,
            insider_supervisor::Health::Degraded => 2,
            insider_supervisor::Health::Unavailable => 3,
        });
        output.extend_from_slice(&component.failures.to_le_bytes());
        output.extend_from_slice(&component.retry_at_ns.to_le_bytes());
        output.extend_from_slice(&component.backoff_ns.to_le_bytes());
    }
    output
}

fn encode_broker_status(
    status: &(insider_broker_api::BrokerHealth, usize, usize, usize),
) -> Vec<u8> {
    let mut output = b"IT_CMD_BROKER_STATUS_RESPONSE_V1\0".to_vec();
    output.push(match status.0 {
        insider_broker_api::BrokerHealth::Unknown => 0,
        insider_broker_api::BrokerHealth::Healthy => 1,
        insider_broker_api::BrokerHealth::Degraded => 2,
        insider_broker_api::BrokerHealth::Unavailable => 3,
    });
    for count in [status.1, status.2, status.3] {
        output.extend_from_slice(
            &u32::try_from(count.min(1_000_000))
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
    }
    output
}

/// Encodes a bounded, flattened risk-policy view. Flattening keeps the
/// read-only wire contract stable while preserving every revision's scope and
/// effective timestamp; the authoritative policy remains journal-backed.
fn encode_risk_policy_snapshot(snapshot: Option<&ScopedRiskPolicySnapshot>) -> Vec<u8> {
    let mut rows: Vec<(&str, String, &TimedLimits)> = Vec::new();
    if let Some(snapshot) = snapshot {
        for revision in &snapshot.system {
            if rows.len() >= 1_024 {
                break;
            }
            rows.push(("system", String::new(), revision));
        }
        for (identity, revisions) in &snapshot.accounts {
            for revision in revisions {
                if rows.len() >= 1_024 {
                    break;
                }
                rows.push(("account", identity.clone(), revision));
            }
            if rows.len() >= 1_024 {
                break;
            }
        }
        for (identity, revisions) in &snapshot.strategies {
            for revision in revisions {
                if rows.len() >= 1_024 {
                    break;
                }
                rows.push(("strategy", identity.clone(), revision));
            }
            if rows.len() >= 1_024 {
                break;
            }
        }
        for (asset, revisions) in &snapshot.assets {
            for revision in revisions {
                if rows.len() >= 1_024 {
                    break;
                }
                rows.push(("asset", format!("{asset:?}"), revision));
            }
            if rows.len() >= 1_024 {
                break;
            }
        }
        for (instrument, revisions) in &snapshot.instruments {
            for revision in revisions {
                if rows.len() >= 1_024 {
                    break;
                }
                rows.push(("instrument", instrument.get().to_string(), revision));
            }
            if rows.len() >= 1_024 {
                break;
            }
        }
    }
    let mut output = b"IT_CMD_RISK_POLICY_STATUS_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(
        &u16::try_from(rows.len().min(1_024))
            .unwrap_or(u16::MAX)
            .to_le_bytes(),
    );
    for (scope, identity, revision) in rows.into_iter().take(1_024) {
        push_string(&mut output, scope);
        push_string(&mut output, &identity);
        output.extend_from_slice(&revision.effective_mono_ns.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_position_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_order_ticks.to_le_bytes());
        output.extend_from_slice(&revision.limits.max_gross_notional_ticks.to_le_bytes());
    }
    output
}

fn encode_news_detail(detail: Option<&insider_news_core::NewsDetail>) -> Vec<u8> {
    let mut output = b"IT_CMD_NEWS_DETAIL_V1\0".to_vec();
    let Some(detail) = detail else {
        output.push(0);
        return output;
    };
    output.push(1);
    encode_news_item_detail(&mut output, &detail.current);
    output.extend_from_slice(
        &(u16::try_from(detail.versions.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for version in &detail.versions {
        encode_news_item_detail(&mut output, version);
    }
    push_string(&mut output, &detail.cluster_id);
    output.extend_from_slice(
        &(u16::try_from(detail.related_item_ids.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for item_id in &detail.related_item_ids {
        push_string(&mut output, item_id);
    }
    output
}

fn encode_news_item_detail(output: &mut Vec<u8>, item: &insider_news_core::NewsItem) {
    push_string(output, &item.id);
    push_string(output, &item.provider);
    push_string(output, &item.canonical_url);
    push_string(output, &item.source_name);
    push_string(output, &item.title);
    output.push(u8::from(item.summary_text.is_some()));
    push_string(output, item.summary_text.as_deref().unwrap_or_default());
    output.push(u8::from(item.published_at_ms.is_some()));
    output.extend_from_slice(&item.published_at_ms.unwrap_or_default().to_le_bytes());
    output.extend_from_slice(&item.received_at_ms.to_le_bytes());
    output
        .extend_from_slice(&(u16::try_from(item.symbols.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for symbol in &item.symbols {
        push_string(output, symbol);
    }
    push_string(output, &item.content_hash);
}

fn decode_trace_request(payload: &[u8]) -> Result<TraceId, String> {
    if payload.len() != 17 || !matches!(payload[0], TRACE_EVENTS | TRACE_EXPORT) {
        return Err("invalid trace request".into());
    }
    TraceId::new(u128::from_le_bytes(
        payload[1..17].try_into().map_err(|_| "invalid trace ID")?,
    ))
    .map_err(|_| "invalid trace ID".into())
}

fn encode_trace_events(events: &[crate::TraceEvent]) -> Vec<u8> {
    let mut output = b"IT_CMD_TRACE_EVENTS_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(events.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for event in events {
        output.extend_from_slice(&event.sequence.to_le_bytes());
        push_string(&mut output, &event.kind);
        output.extend_from_slice(
            &(u32::try_from(event.payload.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        output.extend_from_slice(&event.payload);
    }
    output
}

fn encode_trace_export(events: &[crate::TraceEvent]) -> Vec<u8> {
    let mut output = b"IT_CMD_TRACE_EXPORT_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(events.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for event in events {
        output.extend_from_slice(&event.sequence.to_le_bytes());
        push_string(&mut output, &event.kind);
        output.extend_from_slice(
            &(u32::try_from(event.payload.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
    }
    output
}

fn decode_path_command(payload: &[u8], kind: u8) -> Result<std::path::PathBuf, String> {
    if payload.first().copied() != Some(kind) {
        return Err("invalid backup command kind".into());
    }
    let mut cursor = 1;
    let path = read_string(payload, &mut cursor)?;
    if path.trim().is_empty() || cursor != payload.len() {
        return Err("invalid backup destination".into());
    }
    Ok(std::path::PathBuf::from(path))
}

fn decode_restore_command(
    payload: &[u8],
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    decode_restore_command_kind(payload, READ_MODEL_RESTORE)
}

fn decode_restore_command_kind(
    payload: &[u8],
    kind: u8,
) -> Result<(std::path::PathBuf, std::path::PathBuf), String> {
    if payload.first().copied() != Some(kind) {
        return Err("invalid backup restore kind".into());
    }
    let mut cursor = 1;
    let source = read_string(payload, &mut cursor)?;
    let destination = read_string(payload, &mut cursor)?;
    if source.trim().is_empty() || destination.trim().is_empty() || cursor != payload.len() {
        return Err("invalid backup restore paths".into());
    }
    Ok((
        std::path::PathBuf::from(source),
        std::path::PathBuf::from(destination),
    ))
}

fn encode_projection_manifest(
    prefix: &[u8],
    manifest: &insider_read_model::ProjectionManifest,
) -> Vec<u8> {
    let mut output = prefix.to_vec();
    output.extend_from_slice(&manifest.record_count.to_le_bytes());
    output.extend_from_slice(&manifest.newest_sequence.to_le_bytes());
    output
}

fn encode_backup_manifest(prefix: &[u8], manifest: &insider_journal::BackupManifest) -> Vec<u8> {
    let mut output = prefix.to_vec();
    push_string(&mut output, &manifest.source.to_string_lossy());
    push_string(&mut output, &manifest.destination.to_string_lossy());
    output.extend_from_slice(&manifest.byte_len.to_le_bytes());
    output.extend_from_slice(&manifest.sha256);
    output
}

fn decode_risk_state_command(payload: &[u8]) -> Result<(RiskState, String), String> {
    if payload.first().copied() != Some(RISK_STATE_TRANSITION) {
        return Err("invalid risk-state command kind".into());
    }
    let state = decode_risk_state(*payload.get(1).ok_or("risk-state command is truncated")?)?;
    let mut cursor = 2;
    let authorization = read_string(payload, &mut cursor)?;
    if authorization.len() > 256 || cursor != payload.len() {
        return Err("invalid risk-state authorization".into());
    }
    Ok((state, authorization))
}

fn risk_state_code(state: RiskState) -> u8 {
    match state {
        RiskState::Running => 1,
        RiskState::ReduceOnly => 2,
        RiskState::CancelOnly => 3,
        RiskState::Halted => 4,
    }
}

fn decode_risk_state(code: u8) -> Result<RiskState, String> {
    match code {
        1 => Ok(RiskState::Running),
        2 => Ok(RiskState::ReduceOnly),
        3 => Ok(RiskState::CancelOnly),
        4 => Ok(RiskState::Halted),
        _ => Err("unknown risk-state code".into()),
    }
}

fn encode_risk_state_response(state: RiskState) -> Vec<u8> {
    let mut output = b"IT_CMD_RISK_STATE_OK_V1\0".to_vec();
    output.push(risk_state_code(state));
    output
}

fn decode_autonomy_submit(payload: &[u8]) -> Result<insider_autonomy::Plan, String> {
    if payload.len() <= 1 || payload[0] != AUTONOMY_SUBMIT {
        return Err("invalid autonomy submit payload".into());
    }
    match insider_autonomy::decode_plan_event(&payload[1..])
        .map_err(|_| "invalid autonomy plan event".to_owned())?
    {
        insider_autonomy::PlanEvent::Submitted(plan) => Ok(plan),
        insider_autonomy::PlanEvent::Transition { .. } => {
            Err("autonomy submit requires a submitted plan".into())
        }
    }
}

fn decode_alert_ack(payload: &[u8]) -> Result<String, String> {
    const MAGIC: &[u8] = b"IT_CMD_ALERT_ACK_V1\0";
    if !payload.starts_with(MAGIC) {
        return Err("invalid alert acknowledge payload".into());
    }
    let mut cursor = MAGIC.len();
    let alert_id = read_string(payload, &mut cursor)?;
    if alert_id.trim().is_empty() || alert_id.len() > 256 || cursor != payload.len() {
        return Err("invalid alert ID".into());
    }
    Ok(alert_id)
}

fn encode_alerts_response(alerts: &[crate::AlertRecord]) -> Vec<u8> {
    let mut output = b"IT_CMD_ALERTS_RESPONSE_V1\0".to_vec();
    output.extend_from_slice(&(u32::try_from(alerts.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for alert in alerts {
        push_string(&mut output, &alert.alert_id);
        push_string(&mut output, &alert.dedupe_key);
        push_string(&mut output, &alert.source);
        output.extend_from_slice(&alert.occurred_ms.to_le_bytes());
        output.push(alert.severity);
        output.push(u8::from(alert.sensitive));
        push_string(&mut output, &alert.message);
    }
    output
}

fn decode_autonomy_transition(
    payload: &[u8],
) -> Result<(String, insider_autonomy::PlanState, MonoTime), String> {
    if payload.len() < 10 || payload[0] != AUTONOMY_TRANSITION {
        return Err("invalid autonomy transition payload".into());
    }
    let state = decode_plan_state(payload[1])?;
    let now = MonoTime::from_nanos(u64::from_le_bytes(
        payload[2..10]
            .try_into()
            .map_err(|_| "invalid transition time")?,
    ));
    let mut cursor = 10;
    let plan_id = read_string(payload, &mut cursor)?;
    if plan_id.trim().is_empty() || cursor != payload.len() {
        return Err("invalid autonomy plan ID".into());
    }
    Ok((plan_id, state, now))
}

fn plan_state_code(state: insider_autonomy::PlanState) -> u8 {
    match state {
        insider_autonomy::PlanState::Pending => 1,
        insider_autonomy::PlanState::Approved => 2,
        insider_autonomy::PlanState::Rejected => 3,
        insider_autonomy::PlanState::Expired => 4,
        insider_autonomy::PlanState::Executing => 5,
        insider_autonomy::PlanState::Completed => 6,
        insider_autonomy::PlanState::Failed => 7,
    }
}

fn decode_plan_state(code: u8) -> Result<insider_autonomy::PlanState, String> {
    match code {
        1 => Ok(insider_autonomy::PlanState::Pending),
        2 => Ok(insider_autonomy::PlanState::Approved),
        3 => Ok(insider_autonomy::PlanState::Rejected),
        4 => Ok(insider_autonomy::PlanState::Expired),
        5 => Ok(insider_autonomy::PlanState::Executing),
        6 => Ok(insider_autonomy::PlanState::Completed),
        7 => Ok(insider_autonomy::PlanState::Failed),
        _ => Err("invalid autonomy plan state".into()),
    }
}

/// Builds a deterministic live-limit configuration command.
#[must_use]
pub fn live_configure_command_payload(accounts: &[String], max_notional_ticks: u64) -> Vec<u8> {
    let mut sorted = accounts.to_vec();
    sorted.sort();
    let mut output = Vec::new();
    output.extend_from_slice(LIVE_CONFIGURE_MAGIC);
    output.extend_from_slice(&(u16::try_from(sorted.len()).unwrap_or(u16::MAX)).to_le_bytes());
    for account in sorted.iter().take(128) {
        push_string(&mut output, account);
    }
    output.extend_from_slice(&max_notional_ticks.to_le_bytes());
    output
}

/// Builds the first live-enable challenge command.
#[must_use]
pub fn live_arm_command_payload(account: &str, now: MonoTime, phrase: &str) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(LIVE_ARM_MAGIC);
    push_string(&mut output, account);
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    push_string(&mut output, phrase);
    output
}

/// Builds the second live-enable confirmation command.
#[must_use]
pub fn live_confirm_command_payload(
    account: &str,
    token: &str,
    now: MonoTime,
    phrase: &str,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(LIVE_CONFIRM_MAGIC);
    push_string(&mut output, account);
    push_string(&mut output, token);
    output.extend_from_slice(&now.as_nanos().to_le_bytes());
    push_string(&mut output, phrase);
    output
}

/// Builds the live kill-switch command.
#[must_use]
pub const fn live_kill_command_payload() -> [u8; 1] {
    [LIVE_KILL]
}

fn decode_live_configure(payload: &[u8]) -> Result<(Vec<String>, u64), String> {
    if !payload.starts_with(LIVE_CONFIGURE_MAGIC) {
        return Err("invalid live configure magic".into());
    }
    let mut cursor = LIVE_CONFIGURE_MAGIC.len();
    let count = usize::from(read_u16(payload, &mut cursor)?);
    if count == 0 || count > 128 {
        return Err("live account list out of bounds".into());
    }
    let mut accounts = Vec::with_capacity(count);
    for _ in 0..count {
        let account = read_string(payload, &mut cursor)?;
        if account.trim().is_empty() {
            return Err("empty live account".into());
        }
        accounts.push(account);
    }
    let cap = read_u64(payload, &mut cursor)?;
    if cap == 0 || cursor != payload.len() || accounts.windows(2).any(|w| w[0] == w[1]) {
        return Err("invalid live limits".into());
    }
    Ok((accounts, cap))
}

fn decode_live_arm(payload: &[u8]) -> Result<(String, MonoTime, String), String> {
    if !payload.starts_with(LIVE_ARM_MAGIC) {
        return Err("invalid live arm magic".into());
    }
    let mut cursor = LIVE_ARM_MAGIC.len();
    let account = read_string(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let phrase = read_string(payload, &mut cursor)?;
    if account.trim().is_empty() || phrase.trim().is_empty() || cursor != payload.len() {
        return Err("invalid live arm request".into());
    }
    Ok((account, now, phrase))
}

fn decode_live_confirm(payload: &[u8]) -> Result<(String, String, MonoTime, String), String> {
    if !payload.starts_with(LIVE_CONFIRM_MAGIC) {
        return Err("invalid live confirm magic".into());
    }
    let mut cursor = LIVE_CONFIRM_MAGIC.len();
    let account = read_string(payload, &mut cursor)?;
    let token = read_string(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let phrase = read_string(payload, &mut cursor)?;
    if account.trim().is_empty()
        || token.trim().is_empty()
        || phrase.trim().is_empty()
        || cursor != payload.len()
    {
        return Err("invalid live confirm request".into());
    }
    Ok((account, token, now, phrase))
}

fn live_environment_payload(environment: insider_autonomy::TradingEnvironment) -> Vec<u8> {
    vec![
        b'I',
        b'T',
        b'_',
        b'L',
        b'I',
        b'V',
        b'E',
        b'_',
        b'V',
        b'1',
        0,
        match environment {
            insider_autonomy::TradingEnvironment::Paper => 1,
            insider_autonomy::TradingEnvironment::Live => 2,
            insider_autonomy::TradingEnvironment::Killed => 3,
        },
    ]
}

/// Builds an instrument-resolution command payload for a display symbol.
#[must_use]
pub fn resolve_symbol_command_payload(
    symbol: &str,
    day: u32,
    supported_asset_mask: u16,
) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(RESOLVE_MAGIC);
    push_string(&mut output, symbol);
    output.extend_from_slice(&day.to_le_bytes());
    output.extend_from_slice(&supported_asset_mask.to_le_bytes());
    output
}

fn decode_resolve_request(payload: &[u8]) -> Result<(String, u32, BTreeSet<AssetClass>), String> {
    if !payload.starts_with(RESOLVE_MAGIC) {
        return Err("invalid resolve command magic".into());
    }
    let mut cursor = RESOLVE_MAGIC.len();
    let symbol = read_string(payload, &mut cursor)?;
    let day = read_u32(payload, &mut cursor)?;
    let mask = read_u16(payload, &mut cursor)?;
    if symbol.trim().is_empty() || cursor != payload.len() {
        return Err("invalid resolve request bounds".into());
    }
    let mut assets = BTreeSet::new();
    for (bit, asset) in [
        (0, AssetClass::Equity),
        (1, AssetClass::Etf),
        (2, AssetClass::Option),
        (3, AssetClass::Future),
        (4, AssetClass::Fx),
        (5, AssetClass::Crypto),
    ] {
        if mask & (1 << bit) != 0 {
            assets.insert(asset);
        }
    }
    if assets.is_empty() {
        return Err("supported asset mask is empty".into());
    }
    Ok((symbol, day, assets))
}

fn asset_code(asset: AssetClass) -> u8 {
    match asset {
        AssetClass::Equity => 1,
        AssetClass::Etf => 2,
        AssetClass::Option => 3,
        AssetClass::Future => 4,
        AssetClass::Fx => 5,
        AssetClass::Crypto => 6,
    }
}

#[allow(clippy::type_complexity)]
fn decode_preview_request(
    payload: &[u8],
) -> Result<
    (
        InstrumentId,
        i64,
        ProposalId,
        MonoTime,
        TraceId,
        u64,
        insider_broker_api::OrderType,
        Option<i64>,
    ),
    String,
> {
    if !payload.starts_with(PREVIEW_MAGIC) {
        return Err("invalid preview command magic".into());
    }
    let mut cursor = PREVIEW_MAGIC.len();
    let instrument =
        InstrumentId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid instrument")?;
    let target = read_i64(payload, &mut cursor)?;
    let proposal =
        ProposalId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid proposal")?;
    let order_type = match read_u8(payload, &mut cursor)? {
        1 => insider_broker_api::OrderType::Market,
        2 => insider_broker_api::OrderType::Limit,
        _ => return Err("invalid order type".into()),
    };
    let limit_marker = read_u8(payload, &mut cursor)?;
    if limit_marker > 1 {
        return Err("invalid limit marker".into());
    }
    let limit_price = read_i64(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let trace = TraceId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid trace")?;
    let ttl = read_u64(payload, &mut cursor)?;
    if cursor != payload.len() || ttl == 0 {
        return Err("invalid preview request bounds".into());
    }
    let limit_price = (limit_marker == 1).then_some(limit_price);
    Ok((
        instrument,
        target,
        proposal,
        now,
        trace,
        ttl,
        order_type,
        limit_price,
    ))
}

fn decode_submit_request(payload: &[u8]) -> Result<(ManualOrderPreview, String, MonoTime), String> {
    if !payload.starts_with(SUBMIT_MAGIC) {
        return Err("invalid submit command magic".into());
    }
    let mut cursor = SUBMIT_MAGIC.len();
    let confirmation = read_string(payload, &mut cursor)?;
    let now = MonoTime::from_nanos(read_u64(payload, &mut cursor)?);
    let preview_id = read_string(payload, &mut cursor)?;
    let expected = read_u64(payload, &mut cursor)?;
    let expires = read_u64(payload, &mut cursor)?;
    let target = read_i64(payload, &mut cursor)?;
    let proposal =
        ProposalId::new(read_u128(payload, &mut cursor)?).map_err(|_| "invalid proposal")?;
    let intent_length = read_u32(payload, &mut cursor)? as usize;
    if intent_length == 0 || intent_length > MAX_COMMAND_BYTES {
        return Err("invalid intent length".into());
    }
    let intent_payload = read_bytes(payload, &mut cursor, intent_length)?;
    let Some(RecoveredEvent::Intent(intent)) =
        decode_journal_payload(intent_payload).map_err(|_| "invalid intent payload")?
    else {
        return Err("submit payload is not an order intent".into());
    };
    let warning_count = usize::from(read_u16(payload, &mut cursor)?);
    if warning_count > 128 {
        return Err("too many warnings".into());
    }
    let mut warnings = Vec::with_capacity(warning_count);
    for _ in 0..warning_count {
        warnings.push(read_string(payload, &mut cursor)?);
    }
    if cursor != payload.len() || preview_id.trim().is_empty() || confirmation.trim().is_empty() {
        return Err("invalid submit request bounds".into());
    }
    Ok((
        ManualOrderPreview {
            preview_id,
            expected_state_version: expected,
            expires_mono_ns: expires,
            intent,
            target_quantity_ticks: target,
            proposal_id: proposal,
            estimated_notional_ticks: None,
            estimated_cost_bps: None,
            warnings,
        },
        confirmation,
        now,
    ))
}

fn encode_preview(preview: &ManualOrderPreview) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(PREVIEW_MAGIC);
    push_string(&mut output, &preview.preview_id);
    output.extend_from_slice(&preview.expected_state_version.to_le_bytes());
    output.extend_from_slice(&preview.expires_mono_ns.to_le_bytes());
    output.extend_from_slice(&preview.target_quantity_ticks.to_le_bytes());
    output.extend_from_slice(&preview.proposal_id.get().to_le_bytes());
    let intent = encode_order_intent(&preview.intent);
    output.extend_from_slice(&(u32::try_from(intent.len()).unwrap_or(u32::MAX)).to_le_bytes());
    output.extend_from_slice(&intent);
    output.extend_from_slice(
        &preview
            .estimated_notional_ticks
            .unwrap_or_default()
            .to_le_bytes(),
    );
    output.extend_from_slice(&preview.estimated_cost_bps.unwrap_or_default().to_le_bytes());
    output.extend_from_slice(
        &(u16::try_from(preview.warnings.len()).unwrap_or(u16::MAX)).to_le_bytes(),
    );
    for warning in &preview.warnings {
        push_string(&mut output, warning);
    }
    output
}

#[allow(clippy::too_many_lines)]
fn encode_snapshot(snapshot: &crate::RuntimeSnapshot) -> Vec<u8> {
    let mut output = Vec::new();
    output.extend_from_slice(b"IT_RUNTIME_SNAPSHOT_V14\0");
    output.extend_from_slice(&snapshot.account_id.get().to_le_bytes());
    output.extend_from_slice(&snapshot.cursor.to_le_bytes());
    output.push(match snapshot.risk_state {
        insider_risk_engine::State::Running => 1,
        insider_risk_engine::State::ReduceOnly => 2,
        insider_risk_engine::State::CancelOnly => 3,
        insider_risk_engine::State::Halted => 4,
    });
    output.push(match snapshot.autonomy_mode {
        insider_autonomy::Mode::Manual => 1,
        insider_autonomy::Mode::Hybrid => 2,
        insider_autonomy::Mode::Autonomous => 3,
    });
    match &snapshot.autonomy_plan {
        Some(plan) => {
            output.push(1);
            push_string(&mut output, &plan.plan_id);
            output.push(plan_state_code(plan.state));
            output.extend_from_slice(&plan.generated_at_ns.to_le_bytes());
            output.extend_from_slice(&plan.expires_at_ns.to_le_bytes());
            let actions = plan.actions.iter().take(4_096).collect::<Vec<_>>();
            output.extend_from_slice(
                &u16::try_from(actions.len())
                    .unwrap_or(u16::MAX)
                    .to_le_bytes(),
            );
            for action in actions {
                output.push(match action.action_type {
                    ActionType::ExecuteProposal => 1,
                    ActionType::ExecuteProposalScaled => 2,
                    ActionType::IgnoreProposal => 3,
                    ActionType::PauseStrategy => 4,
                    ActionType::ResumeStrategy => 5,
                    ActionType::RequestReanalysis => 6,
                    ActionType::AddToWatch => 7,
                    ActionType::RemoveFromWatch => 8,
                    ActionType::ReduceAutonomy => 9,
                    ActionType::NoAction => 10,
                });
                push_string(
                    &mut output,
                    action.proposal_id.as_deref().unwrap_or_default(),
                );
                output.push(u8::from(action.scale.is_some()));
                output.extend_from_slice(&action.scale.unwrap_or_default().to_le_bytes());
                let reasons = action.reason_codes.iter().take(256).collect::<Vec<_>>();
                output.extend_from_slice(
                    &u16::try_from(reasons.len())
                        .unwrap_or(u16::MAX)
                        .to_le_bytes(),
                );
                for reason in reasons {
                    push_string(&mut output, reason);
                }
            }
        }
        None => output.push(0),
    }
    for identity in [&snapshot.llm_provider_id, &snapshot.llm_model] {
        output.push(u8::from(identity.is_some()));
        if let Some(value) = identity {
            push_string(&mut output, value);
        }
    }
    output.extend_from_slice(&snapshot.portfolio.cash_ticks.to_le_bytes());
    output.extend_from_slice(&snapshot.portfolio.realized_pnl_ticks.to_le_bytes());
    output.extend_from_slice(&snapshot.portfolio.fees_ticks.to_le_bytes());
    output.extend_from_slice(&snapshot.gross_notional_ticks.to_le_bytes());
    output.extend_from_slice(&snapshot.max_gross_notional_ticks.to_le_bytes());
    output.extend_from_slice(&snapshot.gross_utilization_bps.to_le_bytes());
    output.extend_from_slice(&snapshot.largest_position_notional_ticks.to_le_bytes());
    output.push(u8::from(snapshot.drawdown_bps.is_some()));
    output.extend_from_slice(&snapshot.drawdown_bps.unwrap_or_default().to_le_bytes());
    let positions = snapshot.portfolio.positions().collect::<Vec<_>>();
    output.extend_from_slice(&(u32::try_from(positions.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for (instrument_id, position) in positions {
        output.extend_from_slice(&instrument_id.get().to_le_bytes());
        output.extend_from_slice(&position.quantity_ticks.to_le_bytes());
        output.extend_from_slice(&position.mark_price_ticks.to_le_bytes());
        output.extend_from_slice(
            &snapshot
                .portfolio
                .average_cost_price(instrument_id)
                .unwrap_or(position.mark_price_ticks)
                .to_le_bytes(),
        );
    }
    output.extend_from_slice(
        &(u32::try_from(snapshot.orders.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for order in &snapshot.orders {
        let intent = encode_order_intent(&order.intent);
        output.extend_from_slice(&(u32::try_from(intent.len()).unwrap_or(u32::MAX)).to_le_bytes());
        output.extend_from_slice(&intent);
        push_string(
            &mut output,
            order.broker_order_id.as_deref().unwrap_or_default(),
        );
        output.extend_from_slice(&order.filled_quantity_ticks.to_le_bytes());
        output.push(order_state_code(order.intent.state));
    }
    output.extend_from_slice(
        &(u32::try_from(snapshot.fills.len()).unwrap_or(u32::MAX)).to_le_bytes(),
    );
    for fill in &snapshot.fills {
        push_string(&mut output, &fill.client_order_id);
        output.extend_from_slice(&fill.instrument_id.get().to_le_bytes());
        output.extend_from_slice(&fill.signed_quantity_ticks.to_le_bytes());
        output.extend_from_slice(&fill.price_ticks.to_le_bytes());
    }
    output
        .extend_from_slice(&(u32::try_from(snapshot.tca.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for tca in &snapshot.tca {
        push_string(&mut output, &tca.client_order_id);
        output.extend_from_slice(&tca.filled_quantity_ticks.to_le_bytes());
        output.extend_from_slice(&tca.notional_ticks.to_le_bytes());
        output.extend_from_slice(&tca.average_fill_price_numerator.to_le_bytes());
        output.extend_from_slice(&tca.average_fill_price_denominator.to_le_bytes());
        output.push(u8::from(tca.arrival_price_ticks.is_some()));
        output.extend_from_slice(&tca.arrival_price_ticks.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.decision_mono_ns.is_some()));
        output.extend_from_slice(&tca.decision_mono_ns.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.send_mono_ns.is_some()));
        output.extend_from_slice(&tca.send_mono_ns.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.ack_mono_ns.is_some()));
        output.extend_from_slice(&tca.ack_mono_ns.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.first_fill_mono_ns.is_some()));
        output.extend_from_slice(&tca.first_fill_mono_ns.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.implementation_shortfall_tick_value.is_some()));
        output.extend_from_slice(
            &tca.implementation_shortfall_tick_value
                .unwrap_or_default()
                .to_le_bytes(),
        );
        output.push(u8::from(tca.average_spread_ticks.is_some()));
        output.extend_from_slice(&tca.average_spread_ticks.unwrap_or_default().to_le_bytes());
        output.push(u8::from(tca.adverse_selection_tick_value.is_some()));
        output.extend_from_slice(
            &tca.adverse_selection_tick_value
                .unwrap_or_default()
                .to_le_bytes(),
        );
    }
    encode_snapshot_proposals(&mut output, &snapshot.proposals);
    encode_snapshot_markets(&mut output, &snapshot.markets);
    output
}

fn encode_snapshot_proposals(
    output: &mut Vec<u8>,
    records: &[insider_strategy_coordinator::ProposalRecord],
) {
    output.extend_from_slice(&(u32::try_from(records.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for record in records {
        let proposal = &record.proposal;
        output.extend_from_slice(&proposal.proposal_id.get().to_le_bytes());
        output.extend_from_slice(&proposal.instrument_id.get().to_le_bytes());
        push_string(output, &proposal.strategy_id);
        match proposal.action {
            insider_strategy_sdk::Action::NoAction => output.push(0),
            insider_strategy_sdk::Action::TargetQuantity { quantity_ticks }
            | insider_strategy_sdk::Action::Increase { quantity_ticks }
            | insider_strategy_sdk::Action::Decrease { quantity_ticks } => {
                output.push(match proposal.action {
                    insider_strategy_sdk::Action::TargetQuantity { .. } => 1,
                    insider_strategy_sdk::Action::Increase { .. } => 3,
                    _ => 4,
                });
                output.extend_from_slice(&quantity_ticks.to_le_bytes());
            }
            insider_strategy_sdk::Action::TargetWeight { weight } => {
                output.push(2);
                output.extend_from_slice(&weight.to_bits().to_le_bytes());
            }
            insider_strategy_sdk::Action::Close => output.push(5),
        }
        output.extend_from_slice(&proposal.confidence.to_bits().to_le_bytes());
        output.extend_from_slice(&proposal.generated_mono.as_nanos().to_le_bytes());
        output.extend_from_slice(&proposal.ttl_ns.to_le_bytes());
        output.push(match record.state {
            insider_strategy_coordinator::ProposalState::Accepted => 1,
            insider_strategy_coordinator::ProposalState::Pending => 2,
            insider_strategy_coordinator::ProposalState::Rejected => 3,
            insider_strategy_coordinator::ProposalState::Superseded => 4,
            insider_strategy_coordinator::ProposalState::Expired => 5,
        });
    }
}

fn encode_snapshot_markets(output: &mut Vec<u8>, markets: &[insider_market_data::MarketSnapshot]) {
    output.extend_from_slice(&(u32::try_from(markets.len()).unwrap_or(u32::MAX)).to_le_bytes());
    for market in markets {
        output.extend_from_slice(&market.instrument_id.get().to_le_bytes());
        if let Some(quote) = market.quote {
            output.push(1);
            output.extend_from_slice(&quote.sequence.to_le_bytes());
            output.extend_from_slice(&quote.received_mono.as_nanos().to_le_bytes());
            output.extend_from_slice(&quote.bid_ticks.to_le_bytes());
            output.extend_from_slice(&quote.ask_ticks.to_le_bytes());
            output.extend_from_slice(&quote.bid_quantity_ticks.to_le_bytes());
            output.extend_from_slice(&quote.ask_quantity_ticks.to_le_bytes());
        } else {
            output.push(0);
        }
        if let Some(trade) = market.trade {
            output.push(1);
            output.extend_from_slice(&trade.sequence.to_le_bytes());
            output.extend_from_slice(&trade.price_ticks.to_le_bytes());
        } else {
            output.push(0);
        }
        output.push(quality_code(market.quote_health.quality));
        output.push(quality_code(market.trade_health.quality));
        output.push(market.book_health.map_or(0, quality_code));
        if let Some((bid, bid_quantity, ask, ask_quantity)) = market.book_top {
            output.push(1);
            output.extend_from_slice(&bid.to_le_bytes());
            output.extend_from_slice(&bid_quantity.to_le_bytes());
            output.extend_from_slice(&ask.to_le_bytes());
            output.extend_from_slice(&ask_quantity.to_le_bytes());
        } else {
            output.push(0);
        }
        output.extend_from_slice(
            &(u16::try_from(market.trades.len()).unwrap_or(u16::MAX)).to_le_bytes(),
        );
        for trade in market.trades.iter().rev().take(512).rev() {
            output.extend_from_slice(&trade.sequence.to_le_bytes());
            output.extend_from_slice(&trade.exchange_time.as_unix_nanos().to_le_bytes());
            output.extend_from_slice(&trade.received_mono.as_nanos().to_le_bytes());
            output.extend_from_slice(&trade.price_ticks.to_le_bytes());
            output.extend_from_slice(&trade.quantity_ticks.to_le_bytes());
        }
        output.extend_from_slice(
            &(u32::try_from(market.bars.len()).unwrap_or(u32::MAX)).to_le_bytes(),
        );
        for bar in &market.bars {
            output.extend_from_slice(&bar.start_time.as_unix_nanos().to_le_bytes());
            output.extend_from_slice(&bar.interval_ns.to_le_bytes());
            output.extend_from_slice(&bar.open_ticks.to_le_bytes());
            output.extend_from_slice(&bar.high_ticks.to_le_bytes());
            output.extend_from_slice(&bar.low_ticks.to_le_bytes());
            output.extend_from_slice(&bar.close_ticks.to_le_bytes());
            output.extend_from_slice(&bar.volume_ticks.to_le_bytes());
        }
    }
}

fn quality_code(quality: insider_market_data::Quality) -> u8 {
    match quality {
        insider_market_data::Quality::Good => 1,
        insider_market_data::Quality::Degraded => 2,
        insider_market_data::Quality::Stale => 3,
    }
}

fn order_state_code(state: insider_broker_api::OrderState) -> u8 {
    match state {
        insider_broker_api::OrderState::Created => 13,
        insider_broker_api::OrderState::RiskApproved => 1,
        insider_broker_api::OrderState::Queued => 2,
        insider_broker_api::OrderState::Sending => 3,
        insider_broker_api::OrderState::Sent => 4,
        insider_broker_api::OrderState::Acknowledged => 5,
        insider_broker_api::OrderState::PartiallyFilled => 6,
        insider_broker_api::OrderState::Filled => 7,
        insider_broker_api::OrderState::Rejected => 8,
        insider_broker_api::OrderState::CancelPending => 9,
        insider_broker_api::OrderState::Cancelled => 10,
        insider_broker_api::OrderState::ReplacePending => 11,
        insider_broker_api::OrderState::Unknown => 12,
        insider_broker_api::OrderState::Expired => 14,
    }
}

fn encode_events_response(records: &[insider_journal::Record]) -> Vec<u8> {
    let mut output = Vec::with_capacity(32);
    output.extend_from_slice(b"IT_CMD_EVENTS_V1\0");
    output.extend_from_slice(
        &u32::try_from(records.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for record in records {
        output.extend_from_slice(&record.sequence.to_le_bytes());
        let length = u32::try_from(record.payload.len()).unwrap_or(u32::MAX);
        output.extend_from_slice(&length.to_le_bytes());
        output.extend_from_slice(&record.payload[..usize::try_from(length).unwrap_or(0)]);
    }
    output
}

fn push_string(output: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(&bytes[..usize::from(length).min(bytes.len())]);
}

fn read_bytes<'a>(
    payload: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], String> {
    let end = cursor.checked_add(length).ok_or("payload overflow")?;
    let bytes = payload.get(*cursor..end).ok_or("payload truncated")?;
    *cursor = end;
    Ok(bytes)
}
fn read_u8(payload: &[u8], cursor: &mut usize) -> Result<u8, String> {
    read_bytes(payload, cursor, 1).map(|bytes| bytes[0])
}
fn read_u16(payload: &[u8], cursor: &mut usize) -> Result<u16, String> {
    read_bytes(payload, cursor, 2).and_then(|bytes| {
        bytes
            .try_into()
            .map(u16::from_le_bytes)
            .map_err(|_| "invalid u16".into())
    })
}
fn read_u32(payload: &[u8], cursor: &mut usize) -> Result<u32, String> {
    read_bytes(payload, cursor, 4).and_then(|bytes| {
        bytes
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| "invalid u32".into())
    })
}
fn read_u64(payload: &[u8], cursor: &mut usize) -> Result<u64, String> {
    read_bytes(payload, cursor, 8).and_then(|bytes| {
        bytes
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| "invalid u64".into())
    })
}
fn read_i64(payload: &[u8], cursor: &mut usize) -> Result<i64, String> {
    read_bytes(payload, cursor, 8).and_then(|bytes| {
        bytes
            .try_into()
            .map(i64::from_le_bytes)
            .map_err(|_| "invalid i64".into())
    })
}

fn read_i128(payload: &[u8], cursor: &mut usize) -> Result<i128, String> {
    let bytes = read_bytes(payload, cursor, 16)?;
    let array: [u8; 16] = bytes.try_into().map_err(|_| "invalid i128 field")?;
    Ok(i128::from_le_bytes(array))
}
fn read_f64(payload: &[u8], cursor: &mut usize) -> Result<f64, String> {
    read_bytes(payload, cursor, 8).and_then(|bytes| {
        bytes
            .try_into()
            .map(f64::from_le_bytes)
            .map_err(|_| "invalid f64".into())
    })
}
fn read_u128(payload: &[u8], cursor: &mut usize) -> Result<u128, String> {
    read_bytes(payload, cursor, 16).and_then(|bytes| {
        bytes
            .try_into()
            .map(u128::from_le_bytes)
            .map_err(|_| "invalid u128".into())
    })
}
fn read_string(payload: &[u8], cursor: &mut usize) -> Result<String, String> {
    let length = usize::from(read_u16(payload, cursor)?);
    String::from_utf8(read_bytes(payload, cursor, length)?.to_vec())
        .map_err(|_| "invalid UTF-8".into())
}

fn decode_config_reload(payload: &[u8]) -> Result<(u64, String), String> {
    if !payload.starts_with(CONFIG_RELOAD_MAGIC) {
        return Err("invalid config reload magic".into());
    }
    let mut cursor = CONFIG_RELOAD_MAGIC.len();
    let expected = read_u64(payload, &mut cursor)?;
    let text = read_string(payload, &mut cursor)?;
    if text.is_empty() || cursor != payload.len() {
        return Err("configuration text is empty or trailing bytes exist".into());
    }
    Ok((expected, text))
}

fn encode_config_snapshot(snapshot: &insider_cfg_core::Snapshot) -> Vec<u8> {
    let mut output = b"IT_CMD_CONFIG_SNAPSHOT_V1\0".to_vec();
    output.extend_from_slice(&snapshot.version.to_le_bytes());
    let text = insider_cfg_core::render_cfg(&snapshot.settings);
    push_string(&mut output, &text);
    output
}

/// Builds a configuration reload request from deterministic `.cfg` text.
#[must_use]
pub fn config_reload_command_payload(expected_version: u64, cfg_text: &str) -> Vec<u8> {
    let mut output = CONFIG_RELOAD_MAGIC.to_vec();
    output.extend_from_slice(&expected_version.to_le_bytes());
    push_string(&mut output, cfg_text);
    output
}

/// Builds a read-only configuration snapshot request.
#[must_use]
pub const fn config_status_command_payload() -> [u8; 1] {
    [CONFIG_STATUS]
}

#[cfg(test)]
mod experiment_command_tests {
    use super::{
        ExperimentMutation, decode_experiment_mutation, experiment_mutation_command_payload,
    };
    use insider_experiment_registry::ExperimentProvenance;

    #[test]
    fn provenance_mutation_round_trips_through_authenticated_payload() {
        let mutation = ExperimentMutation::CreateWithProvenance {
            run_id: "run-1".into(),
            code_hash: "code-1".into(),
            config_hash: "config-1".into(),
            dataset_hash: "data-1".into(),
            provenance: Box::new(ExperimentProvenance {
                strategy_id: Some("strategy-1".into()),
                strategy_version: Some("7".into()),
                llm_cache_ids: vec!["cache-a".into(), "cache-b".into()],
                ..Default::default()
            }),
        };
        let payload = experiment_mutation_command_payload(&mutation);
        assert_eq!(decode_experiment_mutation(&payload), Ok(mutation));
    }
}
