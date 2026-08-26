#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use insider_common_types::{InstrumentId, MonoTime, ProposalId, TraceId};
use insider_engine::command::{
    alert_ack_command_payload, alerts_get_command_payload,
    broker_status_command_payload,
    cancel_command_payload, live_arm_command_payload, live_configure_command_payload,
    live_confirm_command_payload, live_kill_command_payload, llm_complete_command_payload,
    llm_stream_command_payload,
    llm_action_command_payload,
    context_search_command_payload_with_embedding,
    news_page_command_payload, news_detail_command_payload,
    news_provider_status_command_payload,
    supervisor_status_command_payload,
    risk_policy_status_command_payload,
    preview_command_payload_with_order,
    proposal_preview_command_payload, proposal_submit_command_payload,
    scheduled_proposal_command_payload, replace_command_payload,
    resolve_symbol_command_payload, snapshot_command_payload, strategy_evaluate_command_payload,
    strategy_registry_list_command_payload,
    metric_registry_list_command_payload,
    submit_preview_payload, trace_events_command_payload, trace_export_command_payload, autonomy_mode_command_payload,
    autonomy_submit_command_payload, autonomy_transition_command_payload,
    backtest_run_command_payload, strategy_backtest_command_payload,
    experiment_mutation_command_payload, ExperimentMutation,
    model_mutation_command_payload, ModelMutation,
    journal_backup_command_payload, journal_restore_command_payload,
    risk_state_transition_command_payload,
    strategy_lifecycle_transition_command_payload,
    metric_lifecycle_transition_command_payload,
    strategy_resolution_budgeted_command_payload,
    config_reload_command_payload, config_status_command_payload,
};
use insider_ipc::{CommandEnvelope, UnixSocketClient};
use insider_llm_core::{ActionType, AutonomousAction, Endpoint, Request as LlmRequest};
use insider_metric_sdk::MetricOutput;
use insider_execution::Schedule;
use insider_autonomy::Mode as AutonomyMode;
use insider_autonomy::{Plan, PlanState};
use insider_engine::{BacktestRunRequest, StrategyBacktestEvent, StrategyBacktestRunRequest};
use insider_replay::BacktestEvent;
use insider_risk_engine::State as RiskState;
use serde::{Deserialize, Serialize};
use tauri::State;

const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MANUAL_PREVIEW_TTL_MS: i64 = 30_000;
const MAX_CACHED_PREVIEWS: usize = 1_024;
const PREVIEW_TTL_NS: u64 = 30_000_000_000;

struct CachedPreview {
    payload: Vec<u8>,
    created_mono_ns: u64,
}

/// Returns a bounded Unix-epoch timestamp suitable for the command envelope.
/// System clocks can move backwards, so pre-epoch values fail closed to zero;
/// the engine still uses its monotonic clock for deadlines and expiry.
fn wall_clock_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64
}

fn command_envelope(id: u64, expected_state_version: u64, idempotency_key: String, payload: Vec<u8>) -> CommandEnvelope {
    CommandEnvelope {
        command_id: format!("tauri-command-{id}"),
        trace_id: format!("tauri-trace-{id}"),
        actor: "desktop".into(),
        issued_wall_ns: wall_clock_ns(),
        expected_state_version,
        idempotency_key,
        payload,
    }
}

struct AppState {
    client: UnixSocketClient,
    next_id: AtomicU64,
    state_version: AtomicU64,
    started: Instant,
    previews: Mutex<BTreeMap<String, CachedPreview>>,
}

impl AppState {
    fn new(socket: PathBuf) -> Result<Self, String> {
        let client = UnixSocketClient::new(socket, MAX_PAYLOAD_BYTES)
            .map_err(|error| format!("engine IPC configuration: {error:?}"))?;
        Ok(Self {
            client,
            next_id: AtomicU64::new(1),
            state_version: AtomicU64::new(0),
            started: Instant::now(),
            previews: Mutex::new(BTreeMap::new()),
        })
    }

    fn now(&self) -> MonoTime {
        MonoTime::from_nanos(self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64)
    }

    fn prune_previews(&self, previews: &mut BTreeMap<String, CachedPreview>, now_ns: u64) {
        previews
            .retain(|_, preview| now_ns.saturating_sub(preview.created_mono_ns) < PREVIEW_TTL_NS);
        while previews.len() >= MAX_CACHED_PREVIEWS {
            let Some(oldest_id) = previews
                .iter()
                .min_by_key(|(_, preview)| preview.created_mono_ns)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            previews.remove(&oldest_id);
        }
    }

    fn request(
        &self,
        payload: Vec<u8>,
        expected_state_version: Option<u64>,
    ) -> Result<Vec<u8>, String> {
        let id = self.next_id.load(Ordering::Relaxed);
        self.request_with_key(
            payload,
            expected_state_version,
            format!("tauri-idempotency-{id}"),
        )
    }

    fn request_with_key(
        &self,
        payload: Vec<u8>,
        expected_state_version: Option<u64>,
        idempotency_key: String,
    ) -> Result<Vec<u8>, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let expected =
            expected_state_version.unwrap_or_else(|| self.state_version.load(Ordering::Acquire));
        let command = command_envelope(id, expected, idempotency_key, payload);
        let response = self
            .client
            .request(&command)
            .map_err(|error| format!("engine IPC request: {error:?}"))?;
        self.state_version
            .store(response.state_version, Ordering::Release);
        Ok(response.payload)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct OrderDraftRequest {
    symbol: String,
    instrument_id: Option<String>,
    side: String,
    #[serde(rename = "type")]
    order_type: String,
    quantity_ticks: i64,
    limit_price_ticks: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PreviewOrderRequest {
    draft: OrderDraftRequest,
    instrument_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubmitOrderRequest {
    preview_id: String,
    confirmation_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderRequest {
    client_order_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProposalPreviewRequest {
    proposal_id: String,
    scale: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ProposalSubmitRequest {
    proposal_id: String,
    confirmation_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScheduledProposalRequest {
    proposal_id: String,
    confirmation_token: String,
    schedule: String,
    slices: Option<usize>,
    interval_ns: Option<u64>,
    weights: Option<Vec<u32>>,
    participation_bps: Option<u32>,
    market_volume_ticks: Option<Vec<i64>>,
    urgency_bps: Option<u32>,
    spread_ticks: Option<i64>,
    max_spread_ticks: Option<i64>,
    volatility_bps: Option<u32>,
    max_volatility_bps: Option<u32>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BacktestEventRequest {
    kind: String,
    sequence: u64,
    quantity_ticks: Option<i64>,
    price_ticks: i64,
    fee_ticks: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BacktestRunRequestDto {
    run_id: String,
    strategy_id: String,
    dataset_hash: String,
    config_hash: String,
    initial_cash_ticks: String,
    events: Vec<BacktestEventRequest>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BacktestRunResponse {
    run_id: String,
    strategy_id: String,
    dataset_hash: String,
    config_hash: String,
    event_count: usize,
    max_drawdown_ticks: String,
    total_fees_ticks: String,
    final_equity_ticks: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyBacktestMetricRequest {
    metric_id: String,
    generated_mono_ns: u64,
    ttl_ns: u64,
    score: f64,
    confidence: f64,
    uncertainty: f64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyBacktestEventRequest {
    sequence: u64,
    now_mono_ns: u64,
    instrument_id: String,
    price_ticks: i64,
    fee_ticks: String,
    metrics: Vec<StrategyBacktestMetricRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyBacktestRunRequestDto {
    run_id: String,
    strategy_id: String,
    dataset_hash: String,
    config_hash: String,
    initial_cash_ticks: String,
    events: Vec<StrategyBacktestEventRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TradingModeRequest {
    mode: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RiskStateRequest {
    state: String,
    authorization: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutonomousActionRequest {
    action_type: String,
    proposal_id: Option<String>,
    scale: Option<f64>,
    reason_codes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutonomousPlanRequest {
    plan_id: String,
    expires_after_ms: u64,
    actions: Vec<AutonomousActionRequest>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutonomousPlanTransitionRequest {
    plan_id: String,
    state: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AlertAcknowledgeRequest {
    alert_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AlertResponse {
    alert_id: String,
    dedupe_key: String,
    source: String,
    occurred_ms: i64,
    severity: u8,
    sensitive: bool,
    message: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplaceOrderRequest {
    client_order_id: String,
    quantity_ticks: i64,
    limit_price_ticks: Option<i64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnalystRequest {
    task: String,
    input: String,
    context_hash: String,
    model: String,
    prompt_version: String,
    max_output_tokens: u32,
    endpoint: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AnalystResponse {
    trace_id: String,
    finish_reason: String,
    content: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AnalystStreamChunk {
    trace_id: String,
    kind: String,
    text: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TraceEventResponse {
    sequence: u64,
    kind: String,
    payload_hex: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TraceExportEventResponse {
    sequence: u64,
    kind: String,
    payload_bytes: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AutonomousActionResponse {
    trace_id: String,
    action_type: String,
    proposal_id: Option<String>,
    scale: Option<f64>,
    reason_codes: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyEvaluateRequest {
    strategy_id: String,
    metric_id: String,
    instrument_id: String,
    metric_ttl_ns: u64,
    score: f64,
    confidence: f64,
    uncertainty: f64,
    entry_threshold: f64,
    exit_threshold: f64,
    quantity_ticks: i64,
    horizon_ns: u64,
    strategy_ttl_ns: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StrategyProposalResponse {
    proposal_id: String,
    strategy_id: String,
    instrument_id: String,
    action: String,
    quantity_ticks: i64,
    weight: f64,
    confidence: f64,
    generated_mono_ns: u64,
    ttl_ns: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContextSearchRequest {
    text: String,
    graph_root: Option<String>,
    max_depth: u16,
    limit: u16,
    embedding: Option<Vec<f32>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextSearchHit {
    node_id: String,
    score: f64,
    exact_score: f64,
    lexical_score: f64,
    vector_score: f64,
    evidence_path: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LiveConfigureRequest {
    accounts: Vec<String>,
    max_notional_ticks: u64,
}

#[derive(Deserialize)]
struct LiveArmRequest {
    account: String,
    phrase: String,
}

#[derive(Deserialize)]
struct LiveConfirmRequest {
    account: String,
    token: String,
    phrase: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PreviewResponse {
    preview_id: String,
    expected_state_version: u64,
    expires_at_ms: i64,
    estimated_notional_ticks: Option<i64>,
    estimated_cost_bps: Option<i64>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
struct TradingEnvironmentResponse {
    environment: &'static str,
}

#[derive(Serialize)]
struct LiveArmResponse {
    environment: &'static str,
    token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct BackupRequest {
    path: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RestoreBackupRequest {
    source: String,
    destination: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct JournalBackupResponse {
    source: String,
    destination: String,
    byte_len: u64,
    sha256: String,
}

fn read_u16(bytes: &[u8], offset: &mut usize) -> Result<u16, String> {
    let end = offset.checked_add(2).ok_or("preview offset overflow")?;
    let value = bytes.get(*offset..end).ok_or("preview truncated")?;
    *offset = end;
    Ok(u16::from_le_bytes([value[0], value[1]]))
}

fn read_u8(bytes: &[u8], offset: &mut usize) -> Result<u8, String> {
    let value = *bytes.get(*offset).ok_or("response truncated")?;
    *offset = (*offset).checked_add(1).ok_or("response offset overflow")?;
    Ok(value)
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let end = offset.checked_add(8).ok_or("preview offset overflow")?;
    let value = bytes.get(*offset..end).ok_or("preview truncated")?;
    *offset = end;
    Ok(u64::from_le_bytes(
        value.try_into().map_err(|_| "preview integer")?,
    ))
}

fn read_u128(bytes: &[u8], offset: &mut usize) -> Result<u128, String> {
    Ok(u128::from_le_bytes(
        read_bytes(bytes, offset, 16)?
            .try_into()
            .map_err(|_| "preview integer")?,
    ))
}

fn read_f64(bytes: &[u8], offset: &mut usize) -> Result<f64, String> {
    Ok(f64::from_le_bytes(
        read_bytes(bytes, offset, 8)?
            .try_into()
            .map_err(|_| "preview float")?,
    ))
}

fn read_i64(bytes: &[u8], offset: &mut usize) -> Result<i64, String> {
    Ok(i64::from_le_bytes(
        read_bytes(bytes, offset, 8)?
            .try_into()
            .map_err(|_| "preview integer")?,
    ))
}

fn read_i128(bytes: &[u8], offset: &mut usize) -> Result<i128, String> {
    let end = offset.checked_add(16).ok_or("response offset overflow")?;
    let value = bytes.get(*offset..end).ok_or("response truncated")?;
    *offset = end;
    let array: [u8; 16] = value.try_into().map_err(|_| "invalid i128 response")?;
    Ok(i128::from_le_bytes(array))
}

fn read_bytes<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Result<&'a [u8], String> {
    let end = offset
        .checked_add(length)
        .ok_or("preview offset overflow")?;
    let value = bytes.get(*offset..end).ok_or("preview truncated")?;
    *offset = end;
    Ok(value)
}

fn read_string(bytes: &[u8], offset: &mut usize) -> Result<String, String> {
    let length = usize::from(read_u16(bytes, offset)?);
    String::from_utf8(read_bytes(bytes, offset, length)?.to_vec())
        .map_err(|_| "preview UTF-8".into())
}

fn decode_preview(
    payload: &[u8],
    now_wall_ms: i64,
    display_ttl_ms: i64,
) -> Result<PreviewResponse, String> {
    if display_ttl_ms <= 0 {
        return Err("preview display TTL is invalid".into());
    }
    const MAGIC: &[u8] = b"IT_CMD_PREVIEW_V1\0";
    if !payload.starts_with(MAGIC) {
        return Err("engine returned an invalid preview response".into());
    }
    let mut offset = MAGIC.len();
    let preview_id = read_string(payload, &mut offset)?;
    let expected_state_version = read_u64(payload, &mut offset)?;
    // The engine expiry is an absolute monotonic timestamp. It is retained in
    // the cached wire payload for authoritative submit-time validation, but it
    // cannot be converted to wall time because the UI and engine have separate
    // monotonic origins. Display the bounded request TTL instead.
    let _expires_mono_ns = read_u64(payload, &mut offset)?;
    let _target = read_i64(payload, &mut offset)?;
    let _proposal = read_bytes(payload, &mut offset, 16)?;
    let intent_length = usize::try_from(u32::from_le_bytes(
        read_bytes(payload, &mut offset, 4)?
            .try_into()
            .map_err(|_| "preview length")?,
    ))
    .map_err(|_| "preview length")?;
    let _intent = read_bytes(payload, &mut offset, intent_length)?;
    let estimate_bytes = read_bytes(payload, &mut offset, 16)?;
    let estimate = i128::from_le_bytes(estimate_bytes.try_into().map_err(|_| "preview estimate")?);
    let estimated_notional_ticks = i64::try_from(estimate).ok();
    let cost = read_i64(payload, &mut offset)?;
    let warning_count = usize::from(read_u16(payload, &mut offset)?);
    if warning_count > 128 {
        return Err("engine returned too many preview warnings".into());
    }
    let warnings = (0..warning_count)
        .map(|_| read_string(payload, &mut offset))
        .collect::<Result<Vec<_>, _>>()?;
    if offset != payload.len() {
        return Err("engine preview has trailing bytes".into());
    }
    Ok(PreviewResponse {
        preview_id,
        expected_state_version,
        expires_at_ms: now_wall_ms.saturating_add(display_ttl_ms),
        estimated_notional_ticks,
        estimated_cost_bps: (cost != 0).then_some(cost),
        warnings,
    })
}

fn wall_now_ms() -> Result<i64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock before epoch")?;
    i64::try_from(duration.as_millis()).map_err(|_| "system clock exceeds supported range".into())
}

fn decode_environment(payload: &[u8]) -> Result<&'static str, String> {
    const MAGIC: &[u8] = b"IT_LIVE_V1\0";
    if payload.len() != MAGIC.len() + 1 || !payload.starts_with(MAGIC) {
        return Err("engine returned an invalid trading environment".into());
    }
    match payload[MAGIC.len()] {
        1 => Ok("paper"),
        2 => Ok("live"),
        3 => Ok("killed"),
        _ => Err("engine returned an unknown trading environment".into()),
    }
}

fn decode_environment_with_token(payload: &[u8]) -> Result<LiveArmResponse, String> {
    let environment = decode_environment(&payload[..payload.len().min(12)])?;
    let mut offset = 12;
    let token = read_string(payload, &mut offset)?;
    if offset != payload.len() || token.trim().is_empty() {
        return Err("engine returned an invalid live token".into());
    }
    Ok(LiveArmResponse { environment, token })
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewsPageRequest {
    scope: String,
    symbol: String,
    after_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct NewsDetailRequest {
    item_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResolveRequest {
    symbol: String,
    day: u32,
    supported_asset_mask: u8,
}

#[tauri::command]
fn get_runtime_snapshot(state: State<'_, AppState>) -> Result<Vec<u8>, String> {
    state.request(snapshot_command_payload().to_vec(), None)
}

#[tauri::command]
fn get_news_page(state: State<'_, AppState>, request: NewsPageRequest) -> Result<Vec<u8>, String> {
    if request.scope != "all" && request.scope != "relevant" {
        return Err("news scope is invalid".into());
    }
    state.request(
        news_page_command_payload(
            &request.scope,
            &request.symbol,
            request.after_cursor.as_deref(),
        ),
        None,
    )
}

#[tauri::command]
fn get_news_detail(
    state: State<'_, AppState>,
    request: NewsDetailRequest,
) -> Result<Vec<u8>, String> {
    if request.item_id.trim().is_empty() || request.item_id.len() > 256 {
        return Err("news item ID is invalid".into());
    }
    state.request(news_detail_command_payload(&request.item_id), None)
}

#[tauri::command]
fn get_news_provider_status(state: State<'_, AppState>) -> Result<Vec<u8>, String> {
    state.request(news_provider_status_command_payload().to_vec(), None)
}

#[tauri::command]
fn get_supervisor_status(state: State<'_, AppState>) -> Result<Vec<u8>, String> {
    state.request(supervisor_status_command_payload().to_vec(), None)
}

#[tauri::command]
fn get_broker_status(state: State<'_, AppState>) -> Result<Vec<u8>, String> {
    state.request(broker_status_command_payload().to_vec(), None)
}

#[tauri::command]
fn get_risk_policy_status(state: State<'_, AppState>) -> Result<Vec<u8>, String> {
    state.request(risk_policy_status_command_payload().to_vec(), None)
}

#[tauri::command]
fn resolve_instrument(
    state: State<'_, AppState>,
    request: ResolveRequest,
) -> Result<Vec<u8>, String> {
    state.request(
        resolve_symbol_command_payload(
            &request.symbol,
            request.day,
            u16::from(request.supported_asset_mask),
        ),
        None,
    )
}

#[tauri::command]
fn preview_order(
    state: State<'_, AppState>,
    request: PreviewOrderRequest,
) -> Result<PreviewResponse, String> {
    if request.draft.quantity_ticks <= 0
        || (request.draft.side != "buy" && request.draft.side != "sell")
        || (request.draft.order_type != "market" && request.draft.order_type != "limit")
        || request.draft.symbol.trim().is_empty()
        || request.draft.instrument_id.as_deref() != Some(request.instrument_id.as_str())
        || (request.draft.order_type == "market" && request.draft.limit_price_ticks.is_some())
        || (request.draft.order_type == "limit"
            && request.draft.limit_price_ticks.is_none_or(|price| price <= 0))
    {
        return Err("order draft is invalid".into());
    }
    let instrument = request
        .instrument_id
        .parse::<u128>()
        .map_err(|_| "instrument ID is invalid".to_owned())?;
    let instrument = insider_common_types::InstrumentId::new(instrument)
        .map_err(|_| "instrument ID is invalid".to_owned())?;
    let target = if request.draft.side == "sell" {
        request
            .draft
            .quantity_ticks
            .checked_neg()
            .ok_or("quantity overflow")?
    } else {
        request.draft.quantity_ticks
    };
    let id = state.next_id.load(Ordering::Relaxed).max(1);
    let proposal =
        ProposalId::new(u128::from(id)).map_err(|_| "proposal ID is invalid".to_owned())?;
    let trace = TraceId::new(u128::from(id)).map_err(|_| "trace ID is invalid".to_owned())?;
    let payload = preview_command_payload_with_order(
        instrument,
        target,
        proposal,
        state.now(),
        trace,
        30_000_000_000,
        if request.draft.order_type == "limit" {
            insider_broker_api::OrderType::Limit
        } else {
            insider_broker_api::OrderType::Market
        },
        request.draft.limit_price_ticks,
    );
    let response = state.request(payload, None)?;
    let decoded = decode_preview(&response, wall_now_ms()?, MANUAL_PREVIEW_TTL_MS)?;
    let now_ns = state.now().as_nanos();
    let mut previews = state
        .previews
        .lock()
        .map_err(|_| "preview store is unavailable".to_owned())?;
    state.prune_previews(&mut previews, now_ns);
    previews.insert(
        decoded.preview_id.clone(),
        CachedPreview {
            payload: response,
            created_mono_ns: now_ns,
        },
    );
    Ok(decoded)
}

#[tauri::command]
fn preview_proposal(
    state: State<'_, AppState>,
    request: ProposalPreviewRequest,
) -> Result<PreviewResponse, String> {
    let proposal_id = request
        .proposal_id
        .parse::<u128>()
        .ok()
        .and_then(|value| ProposalId::new(value).ok())
        .ok_or("proposal ID is invalid")?;
    if !request.scale.is_finite() || request.scale <= 0.0 || request.scale > 1.0 {
        return Err("proposal scale is invalid".into());
    }
    let id = state.next_id.load(Ordering::Relaxed).max(1);
    let trace = TraceId::new(u128::from(id)).map_err(|_| "trace ID is invalid")?;
    let response = state.request(
        proposal_preview_command_payload(
            proposal_id,
            request.scale,
            MonoTime::from_nanos(0),
            trace,
            30_000_000_000,
        ),
        None,
    )?;
    decode_preview(&response, wall_now_ms()?, MANUAL_PREVIEW_TTL_MS)
}

#[tauri::command]
fn submit_proposal(
    state: State<'_, AppState>,
    request: ProposalSubmitRequest,
) -> Result<String, String> {
    let proposal_id = request
        .proposal_id
        .parse::<u128>()
        .ok()
        .and_then(|value| ProposalId::new(value).ok())
        .ok_or("proposal ID is invalid")?;
    if request.confirmation_token != "CONFIRM" {
        return Err("explicit CONFIRM is required".into());
    }
    let id = state.next_id.load(Ordering::Relaxed).max(1);
    let trace = TraceId::new(u128::from(id)).map_err(|_| "trace ID is invalid")?;
    let response = state.request(
        proposal_submit_command_payload(proposal_id, 1.0, &request.confirmation_token, trace),
        None,
    )?;
    const MAGIC: &[u8] = b"IT_CMD_PROPOSAL_SUBMIT_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid proposal submission response".into());
    }
    let mut offset = MAGIC.len();
    let client_order_id = read_string(&response, &mut offset)?;
    if offset != response.len() || client_order_id.trim().is_empty() {
        return Err("engine returned an invalid client order ID".into());
    }
    Ok(client_order_id)
}

#[tauri::command]
fn submit_scheduled_proposal(
    state: State<'_, AppState>,
    request: ScheduledProposalRequest,
) -> Result<String, String> {
    let proposal_id = request
        .proposal_id
        .parse::<u128>()
        .ok()
        .and_then(|value| ProposalId::new(value).ok())
        .ok_or("proposal ID is invalid")?;
    if request.confirmation_token != "CONFIRM" {
        return Err("explicit CONFIRM is required".into());
    }
    let interval_ns = request.interval_ns.unwrap_or(0);
    let schedule = match request.schedule.trim().to_ascii_lowercase().as_str() {
        "immediate" => Schedule::Immediate,
        "twap" => Schedule::Twap {
            slices: request.slices.unwrap_or(0),
            interval_ns,
        },
        "vwap" => Schedule::Vwap {
            weights: request.weights.unwrap_or_default(),
        },
        "pov" => Schedule::Pov {
            participation_bps: request.participation_bps.unwrap_or(0),
            interval_ns,
            market_volume_ticks: request.market_volume_ticks.unwrap_or_default(),
        },
        "implementation_shortfall" => Schedule::ImplementationShortfall {
            slices: request.slices.unwrap_or(0),
            interval_ns,
            urgency_bps: request.urgency_bps.unwrap_or(u32::MAX),
        },
        "adaptive" => Schedule::Adaptive {
            slices: request.slices.unwrap_or(0),
            interval_ns,
            urgency_bps: request.urgency_bps.unwrap_or(0),
            spread_ticks: request.spread_ticks.unwrap_or(-1),
            max_spread_ticks: request.max_spread_ticks.unwrap_or(0),
            volatility_bps: request.volatility_bps.unwrap_or(u32::MAX),
            max_volatility_bps: request.max_volatility_bps.unwrap_or(0),
            market_volume_ticks: request.market_volume_ticks.unwrap_or_default(),
        },
        _ => return Err("schedule must be immediate, twap, vwap, pov, implementation_shortfall, or adaptive".into()),
    };
    let id = state.next_id.load(Ordering::Relaxed).max(1);
    let trace = TraceId::new(u128::from(id)).map_err(|_| "trace ID is invalid")?;
    let response = state.request(
        scheduled_proposal_command_payload(
            proposal_id,
            &schedule,
            &request.confirmation_token,
            trace,
        ),
        None,
    )?;
    const MAGIC: &[u8] = b"IT_CMD_SCHEDULED_PROPOSAL_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid scheduled submission response".into());
    }
    let mut offset = MAGIC.len();
    let parent_id = read_string(&response, &mut offset)?;
    if offset != response.len() || parent_id.trim().is_empty() {
        return Err("engine returned an invalid parent order ID".into());
    }
    Ok(parent_id)
}

#[tauri::command]
fn run_backtest(
    state: State<'_, AppState>,
    request: BacktestRunRequestDto,
) -> Result<BacktestRunResponse, String> {
    if request.events.is_empty() || request.events.len() > 1_000_000 {
        return Err("backtest event count is outside bounds".into());
    }
    let initial_cash_ticks = request
        .initial_cash_ticks
        .parse::<i128>()
        .map_err(|_| "initial cash ticks must be an integer")?;
    let mut events = Vec::with_capacity(request.events.len());
    for event in request.events {
        let kind = event.kind.trim().to_ascii_lowercase();
        events.push(match kind.as_str() {
            "fill" => BacktestEvent::Fill {
                sequence: event.sequence,
                quantity_ticks: event
                    .quantity_ticks
                    .ok_or("fill quantity ticks are required")?,
                price_ticks: event.price_ticks,
                fee_ticks: event
                    .fee_ticks
                    .as_deref()
                    .unwrap_or("0")
                    .parse::<i128>()
                    .map_err(|_| "fill fee ticks must be an integer")?,
            },
            "mark" => BacktestEvent::Mark {
                sequence: event.sequence,
                price_ticks: event.price_ticks,
            },
            _ => return Err("backtest event kind must be fill or mark".into()),
        });
    }
    let payload = backtest_run_command_payload(&BacktestRunRequest {
        run_id: request.run_id,
        strategy_id: request.strategy_id,
        dataset_hash: request.dataset_hash,
        config_hash: request.config_hash,
        initial_cash_ticks,
        events,
    });
    let response = state.request(payload, None)?;
    const MAGIC: &[u8] = b"IT_CMD_BACKTEST_RUN_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid backtest response".into());
    }
    let mut offset = MAGIC.len();
    let run_id = read_string(&response, &mut offset)?;
    let strategy_id = read_string(&response, &mut offset)?;
    let dataset_hash = read_string(&response, &mut offset)?;
    let config_hash = read_string(&response, &mut offset)?;
    let event_count = read_u64(&response, &mut offset)? as usize;
    let max_drawdown_ticks = read_i128(&response, &mut offset)?.to_string();
    let total_fees_ticks = read_i128(&response, &mut offset)?.to_string();
    let final_marker = read_u8(&response, &mut offset)?;
    if final_marker > 1 {
        return Err("engine returned an invalid final snapshot marker".into());
    }
    let final_equity_ticks = if final_marker == 1 {
        let _position = read_i64(&response, &mut offset)?;
        let _average_cost = read_i64(&response, &mut offset)?;
        let _cash = read_i128(&response, &mut offset)?;
        let _realized = read_i128(&response, &mut offset)?;
        Some(read_i128(&response, &mut offset)?.to_string())
    } else {
        None
    };
    if offset != response.len() {
        return Err("engine returned trailing backtest bytes".into());
    }
    Ok(BacktestRunResponse {
        run_id,
        strategy_id,
        dataset_hash,
        config_hash,
        event_count,
        max_drawdown_ticks,
        total_fees_ticks,
        final_equity_ticks,
    })
}

#[tauri::command]
fn run_strategy_backtest(
    state: State<'_, AppState>,
    request: StrategyBacktestRunRequestDto,
) -> Result<BacktestRunResponse, String> {
    if request.events.is_empty() || request.events.len() > 100_000 {
        return Err("strategy backtest event count is outside bounds".into());
    }
    let initial_cash_ticks = request
        .initial_cash_ticks
        .parse::<i128>()
        .map_err(|_| "initial cash ticks must be an integer")?;
    let mut events = Vec::with_capacity(request.events.len());
    for event in request.events {
        let instrument_id = event
            .instrument_id
            .parse::<u128>()
            .ok()
            .and_then(|value| insider_common_types::InstrumentId::new(value).ok())
            .ok_or("strategy backtest instrument ID is invalid")?;
        if event.metrics.len() > 4_096 {
            return Err("strategy backtest metrics are outside bounds".into());
        }
        let metrics = event
            .metrics
            .into_iter()
            .map(|metric| insider_metric_sdk::MetricOutput {
                metric_id: metric.metric_id,
                instrument_id,
                generated_mono: MonoTime::from_nanos(metric.generated_mono_ns),
                ttl_ns: metric.ttl_ns,
                score: metric.score,
                confidence: metric.confidence,
                uncertainty: metric.uncertainty,
            })
            .collect();
        events.push(StrategyBacktestEvent {
            sequence: event.sequence,
            now_mono_ns: event.now_mono_ns,
            instrument_id,
            price_ticks: event.price_ticks,
            fee_ticks: event
                .fee_ticks
                .parse::<i128>()
                .map_err(|_| "strategy backtest fee ticks must be an integer")?,
            metrics,
        });
    }
    let payload = strategy_backtest_command_payload(&StrategyBacktestRunRequest {
        run_id: request.run_id,
        strategy_id: request.strategy_id,
        dataset_hash: request.dataset_hash,
        config_hash: request.config_hash,
        initial_cash_ticks,
        events,
    });
    let response = state.request(payload, None)?;
    const MAGIC: &[u8] = b"IT_CMD_BACKTEST_RUN_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid strategy backtest response".into());
    }
    decode_backtest_response(&response, MAGIC)
}

fn decode_backtest_response(
    response: &[u8],
    magic: &[u8],
) -> Result<BacktestRunResponse, String> {
    let mut offset = magic.len();
    let run_id = read_string(response, &mut offset)?;
    let strategy_id = read_string(response, &mut offset)?;
    let dataset_hash = read_string(response, &mut offset)?;
    let config_hash = read_string(response, &mut offset)?;
    let event_count = usize::try_from(read_u64(response, &mut offset)?)
        .map_err(|_| "invalid backtest event count")?;
    let max_drawdown_ticks = read_i128(response, &mut offset)?.to_string();
    let total_fees_ticks = read_i128(response, &mut offset)?.to_string();
    let marker = read_u8(response, &mut offset)?;
    if marker > 1 {
        return Err("invalid backtest final snapshot marker".into());
    }
    let final_equity_ticks = if marker == 1 {
        let _position = read_i64(response, &mut offset)?;
        let _average_cost = read_i64(response, &mut offset)?;
        let _cash = read_i128(response, &mut offset)?;
        let _realized = read_i128(response, &mut offset)?;
        Some(read_i128(response, &mut offset)?.to_string())
    } else {
        None
    };
    if offset != response.len() {
        return Err("engine returned trailing strategy backtest bytes".into());
    }
    Ok(BacktestRunResponse {
        run_id,
        strategy_id,
        dataset_hash,
        config_hash,
        event_count,
        max_drawdown_ticks,
        total_fees_ticks,
        final_equity_ticks,
    })
}

#[tauri::command]
fn list_backtests(state: State<'_, AppState>) -> Result<Vec<BacktestRunResponse>, String> {
    let response = state.request(vec![31], None)?;
    const MAGIC: &[u8] = b"IT_CMD_BACKTEST_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid backtest list response".into());
    }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid backtest count")?;
    if count > 4_096 {
        return Err("engine returned too many backtests".into());
    }
    let mut results = Vec::with_capacity(count);
    for _ in 0..count {
        let run_id = read_string(&response, &mut offset)?;
        let strategy_id = read_string(&response, &mut offset)?;
        let dataset_hash = read_string(&response, &mut offset)?;
        let config_hash = read_string(&response, &mut offset)?;
        let event_count = usize::try_from(read_u64(&response, &mut offset)?).map_err(|_| "invalid event count")?;
        let max_drawdown_ticks = read_i128(&response, &mut offset)?.to_string();
        let total_fees_ticks = read_i128(&response, &mut offset)?.to_string();
        let marker = read_u8(&response, &mut offset)?;
        if marker > 1 {
            return Err("invalid backtest snapshot marker".into());
        }
        let final_equity_ticks = if marker == 1 {
            Some(read_i128(&response, &mut offset)?.to_string())
        } else {
            None
        };
        results.push(BacktestRunResponse { run_id, strategy_id, dataset_hash, config_hash, event_count, max_drawdown_ticks, total_fees_ticks, final_equity_ticks });
    }
    if offset != response.len() {
        return Err("engine returned trailing backtest list bytes".into());
    }
    Ok(results)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExperimentArtifactResponse {
    kind: String,
    hash: String,
    path: String,
}

#[derive(Clone, Debug, Deserialize)]
struct ExperimentMutationRequest {
    operation: String,
    run_id: String,
    code_hash: Option<String>,
    config_hash: Option<String>,
    dataset_hash: Option<String>,
    metrics: Option<BTreeMap<String, f64>>,
    artifact: Option<ExperimentArtifactResponse>,
}

#[tauri::command]
fn mutate_experiment(state: State<'_, AppState>, request: ExperimentMutationRequest) -> Result<(), String> {
    let operation = request.operation.trim().to_ascii_lowercase();
    let mutation = match operation.as_str() {
        "create" => ExperimentMutation::Create { run_id: request.run_id, code_hash: request.code_hash.ok_or("code hash is required")?, config_hash: request.config_hash.ok_or("config hash is required")?, dataset_hash: request.dataset_hash.ok_or("dataset hash is required")? },
        "start" => ExperimentMutation::Start { run_id: request.run_id },
        "succeed" => ExperimentMutation::Succeed { run_id: request.run_id, metrics: request.metrics.unwrap_or_default() },
        "fail" => ExperimentMutation::Fail { run_id: request.run_id },
        "artifact" => { let artifact = request.artifact.ok_or("artifact is required")?; ExperimentMutation::AddArtifact { run_id: request.run_id, artifact: insider_experiment_registry::Artifact { kind: artifact.kind, hash: artifact.hash, path: artifact.path } } }
        _ => return Err("experiment operation must be create, start, succeed, fail, or artifact".into()),
    };
    let response = state.request(experiment_mutation_command_payload(&mutation), None)?;
    if response != b"IT_CMD_EXPERIMENT_MUTATE_OK_V1\0" { return Err("engine rejected experiment mutation".into()); }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExperimentRunResponse {
    run_id: String,
    code_hash: String,
    config_hash: String,
    dataset_hash: String,
    status: String,
    metrics: BTreeMap<String, f64>,
    artifacts: Vec<ExperimentArtifactResponse>,
    provenance: ExperimentProvenanceResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ExperimentProvenanceResponse {
    strategy_id: Option<String>,
    strategy_version: Option<String>,
    news_dataset_hash: Option<String>,
    news_clustering_version: Option<String>,
    graph_snapshot_version: Option<String>,
    llm_provider: Option<String>,
    llm_model: Option<String>,
    prompt_version: Option<String>,
    tool_schema_version: Option<String>,
    llm_cache_ids: Vec<String>,
    autonomy_config_hash: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ModelRecordResponse {
    model_id: String,
    version: String,
    artifact_hash: String,
    input_schema_hash: String,
    output_schema_hash: String,
    input_width: usize,
    status: String,
    active: bool,
}

#[tauri::command]
fn list_models(state: State<'_, AppState>) -> Result<Vec<ModelRecordResponse>, String> {
    let response = state.request(vec![34], None)?;
    const MAGIC: &[u8] = b"IT_CMD_MODEL_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned an invalid model list response".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid model count")?;
    if count > 4096 { return Err("engine returned too many models".into()); }
    let mut rows = Vec::with_capacity(count);
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let model_id = read_string(&response, &mut offset)?; let version = read_string(&response, &mut offset)?; let artifact_hash = read_string(&response, &mut offset)?; let input_schema_hash = read_string(&response, &mut offset)?; let output_schema_hash = read_string(&response, &mut offset)?; let input_width = usize::try_from(read_u64(&response, &mut offset)?).map_err(|_| "invalid model width")?;
        let status = match read_u8(&response, &mut offset)? { 1 => "research", 2 => "validated", 3 => "shadow", 4 => "canary", 5 => "production", 6 => "retired", _ => return Err("invalid model status".into()) }.to_owned();
        records.push((model_id, version, artifact_hash, input_schema_hash, output_schema_hash, input_width, status));
    }
    let active_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid active model count")?;
    if active_count > 4096 { return Err("too many active model pointers".into()); }
    let mut active = std::collections::BTreeSet::new();
    for _ in 0..active_count { active.insert((read_string(&response, &mut offset)?, read_string(&response, &mut offset)?)); }
    for (model_id, version, artifact_hash, input_schema_hash, output_schema_hash, input_width, status) in records { rows.push(ModelRecordResponse { active: active.contains(&(model_id.clone(), version.clone())), model_id, version, artifact_hash, input_schema_hash, output_schema_hash, input_width, status }); }
    if offset != response.len() { return Err("engine returned trailing model bytes".into()); }
    Ok(rows)
}

#[derive(Clone, Debug, Deserialize)]
struct ModelMutationRequest {
    operation: String,
    model_id: String,
    version: String,
    evidence_id: Option<String>,
    artifact_hash: Option<String>,
    input_schema_hash: Option<String>,
    output_schema_hash: Option<String>,
    input_width: Option<usize>,
    code_hash: Option<String>,
    training_data_hash: Option<String>,
    config_hash: Option<String>,
    feature_hash: Option<String>,
    calibration_hash: Option<String>,
}

#[tauri::command]
fn mutate_model(state: State<'_, AppState>, request: ModelMutationRequest) -> Result<(), String> {
    let operation = request.operation.trim().to_ascii_lowercase();
    let identity = (request.model_id, request.version);
    let artifact_hash = request.artifact_hash.clone();
    let mutation = match operation.as_str() {
        "register" => ModelMutation::Register {
            record: insider_model_registry::ModelRecord { model_id: identity.0.clone(), version: identity.1.clone(), artifact_hash: artifact_hash.clone().ok_or("artifact hash is required")?, input_schema_hash: request.input_schema_hash.ok_or("input schema hash is required")?, output_schema_hash: request.output_schema_hash.ok_or("output schema hash is required")?, input_width: request.input_width.ok_or("input width is required")?, status: insider_model_registry::Status::Research },
            manifest: insider_model_registry::ArtifactManifest { code_hash: request.code_hash.ok_or("code hash is required")?, training_data_hash: request.training_data_hash.ok_or("training data hash is required")?, config_hash: request.config_hash.ok_or("config hash is required")?, feature_hash: request.feature_hash.ok_or("feature hash is required")?, calibration_hash: request.calibration_hash.ok_or("calibration hash is required")?, artifact_hash: artifact_hash.ok_or("artifact hash is required")? },
        },
        "validate" => ModelMutation::Validate { model_id: identity.0, version: identity.1, evidence_id: request.evidence_id.ok_or("evidence ID is required")? },
        "shadow" => ModelMutation::Shadow { model_id: identity.0, version: identity.1 },
        "canary" => ModelMutation::Canary { model_id: identity.0, version: identity.1, evidence_id: request.evidence_id.ok_or("evidence ID is required")? },
        "promote" => ModelMutation::Promote { model_id: identity.0, version: identity.1 },
        _ => return Err("model operation must be register, validate, shadow, canary, or promote".into()),
    };
    let response = state.request(model_mutation_command_payload(&mutation), None)?;
    if response != b"IT_CMD_MODEL_MUTATE_OK_V1\0" { return Err("engine rejected model mutation".into()); }
    Ok(())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StrategyResolutionResponse {
    policy: String,
    now_mono_ns: u64,
    accepted_count: u32,
    conflict_count: u32,
    expired_count: u32,
    attribution_count: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyResolutionBudgetedRequest {
    policy: String,
    budgets: BTreeMap<String, i64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StrategyResolutionBudgetedResponse {
    accepted_count: u16,
    adjustment_count: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StrategyExecutionResponse {
    strategy_id: String,
    fill_count: u64,
    filled_quantity_ticks: String,
    notional_ticks: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct StrategyRegistryResponse {
    strategy_id: String,
    mode: String,
    state: String,
    lifecycle: String,
    lifecycle_evidence_ref: String,
    priority: String,
    horizon_ns: u64,
    ttl_ns: u64,
    period_ns: u64,
    deadline_ns: u64,
    metric_ids: Vec<String>,
    dependencies: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StrategyLifecycleTransitionRequest {
    strategy_id: String,
    lifecycle: String,
    confirmation: String,
    evidence_ref: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetricLifecycleTransitionRequest {
    metric_id: String,
    lifecycle: String,
    confirmation: String,
    evidence_ref: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct MetricRegistryResponse {
    metric_id: String,
    state: String,
    lifecycle: String,
    priority: String,
    ttl_ns: u64,
    period_ns: u64,
    deadline_ns: u64,
    budget_ns: u64,
    min_score: Option<f64>,
    max_score: Option<f64>,
    inputs: Vec<String>,
}

#[tauri::command]
fn list_strategy_resolutions(state: State<'_, AppState>) -> Result<Vec<StrategyResolutionResponse>, String> {
    let response = state.request(vec![37], None)?;
    const MAGIC: &[u8] = b"IT_CMD_STRATEGY_RESOLUTION_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned an invalid strategy resolution response".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid resolution count")?;
    if count > 4096 { return Err("too many strategy resolutions".into()); }
    let mut summaries = Vec::with_capacity(count);
    for _ in 0..count { summaries.push(StrategyResolutionResponse { policy: read_string(&response, &mut offset)?, now_mono_ns: read_u64(&response, &mut offset)?, accepted_count: read_u32(&response, &mut offset)?, conflict_count: read_u32(&response, &mut offset)?, expired_count: read_u32(&response, &mut offset)?, attribution_count: read_u32(&response, &mut offset)? }); }
    if offset != response.len() { return Err("engine returned trailing strategy resolution bytes".into()); }
    Ok(summaries)
}

#[tauri::command]
fn resolve_strategy_with_budgets(
    state: State<'_, AppState>,
    request: StrategyResolutionBudgetedRequest,
) -> Result<StrategyResolutionBudgetedResponse, String> {
    let policy = match request.policy.as_str() {
        "isolated_books" => insider_strategy_coordinator::Policy::IsolatedBooks,
        "priority" => insider_strategy_coordinator::Policy::Priority,
        "weighted_net" => insider_strategy_coordinator::Policy::WeightedNet,
        _ => return Err("strategy policy is invalid".into()),
    };
    if request.budgets.len() > 256
        || request
            .budgets
            .keys()
            .any(|id| id.trim().is_empty() || id.len() > 256)
    {
        return Err("strategy budgets are invalid".into());
    }
    let mut budgets = BTreeMap::new();
    for (strategy_id, quantity) in request.budgets {
        let budget = insider_strategy_coordinator::StrategyBudget::new(quantity)
            .ok_or("strategy budgets must be positive")?;
        budgets.insert(strategy_id, budget);
    }
    let response = state.request(
        strategy_resolution_budgeted_command_payload(policy, state.now(), &budgets),
        None,
    )?;
    const MAGIC: &[u8] = b"IT_CMD_STRATEGY_RESOLUTION_BUDGETED_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) {
        return Err("engine returned an invalid budgeted strategy response".into());
    }
    let mut offset = MAGIC.len();
    let accepted_count = read_u16(&response, &mut offset)?;
    for _ in 0..accepted_count {
        let _proposal_id = read_u128(&response, &mut offset)?;
        let _action = read_u8(&response, &mut offset)?;
        let _value = read_i64(&response, &mut offset)?;
    }
    let adjustment_count = read_u16(&response, &mut offset)?;
    for _ in 0..adjustment_count {
        let _proposal_id = read_u128(&response, &mut offset)?;
        let _before = read_i64(&response, &mut offset)?;
        let _after = read_i64(&response, &mut offset)?;
    }
    if offset != response.len() {
        return Err("engine returned trailing budgeted strategy bytes".into());
    }
    Ok(StrategyResolutionBudgetedResponse {
        accepted_count,
        adjustment_count,
    })
}

#[tauri::command]
fn list_strategy_execution_summaries(state: State<'_, AppState>) -> Result<Vec<StrategyExecutionResponse>, String> {
    let response = state.request(vec![38], None)?;
    const MAGIC: &[u8] = b"IT_CMD_STRATEGY_EXECUTION_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned an invalid strategy execution response".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid strategy execution count")?;
    if count > 4096 { return Err("too many strategy execution summaries".into()); }
    let mut summaries = Vec::with_capacity(count);
    for _ in 0..count {
        summaries.push(StrategyExecutionResponse {
            strategy_id: read_string(&response, &mut offset)?,
            fill_count: read_u64(&response, &mut offset)?,
            filled_quantity_ticks: read_i128(&response, &mut offset)?.to_string(),
            notional_ticks: read_i128(&response, &mut offset)?.to_string(),
        });
    }
    if offset != response.len() { return Err("engine returned trailing strategy execution bytes".into()); }
    Ok(summaries)
}

#[tauri::command]
fn list_strategies(state: State<'_, AppState>) -> Result<Vec<StrategyRegistryResponse>, String> {
    let response = state.request(strategy_registry_list_command_payload().to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_STRATEGY_REGISTRY_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned an invalid strategy registry response".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid strategy registry count")?;
    if count > 4096 { return Err("too many installed strategies".into()); }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let strategy_id = read_string(&response, &mut offset)?;
        let mode = read_string(&response, &mut offset)?;
        let state_name = read_string(&response, &mut offset)?;
        let lifecycle = read_string(&response, &mut offset)?;
        let lifecycle_evidence_ref = read_string(&response, &mut offset)?;
        let priority = read_string(&response, &mut offset)?;
        let horizon_ns = read_u64(&response, &mut offset)?;
        let ttl_ns = read_u64(&response, &mut offset)?;
        let period_ns = read_u64(&response, &mut offset)?;
        let deadline_ns = read_u64(&response, &mut offset)?;
        let metric_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid strategy metric count")?;
        if metric_count > 4096 { return Err("too many strategy metrics".into()); }
        let mut metric_ids = Vec::with_capacity(metric_count);
        for _ in 0..metric_count { metric_ids.push(read_string(&response, &mut offset)?); }
        let dependency_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid strategy dependency count")?;
        if dependency_count > 4096 { return Err("too many strategy dependencies".into()); }
        let mut dependencies = Vec::with_capacity(dependency_count);
        for _ in 0..dependency_count { dependencies.push(read_string(&response, &mut offset)?); }
        records.push(StrategyRegistryResponse { strategy_id, mode, state: state_name, lifecycle, lifecycle_evidence_ref, priority, horizon_ns, ttl_ns, period_ns, deadline_ns, metric_ids, dependencies });
    }
    if offset != response.len() { return Err("engine returned trailing strategy registry bytes".into()); }
    Ok(records)
}

#[tauri::command]
fn transition_strategy_lifecycle(
    state: State<'_, AppState>,
    request: StrategyLifecycleTransitionRequest,
) -> Result<(), String> {
    if request.strategy_id.trim().is_empty() || request.strategy_id.len() > 256 {
        return Err("strategy ID is invalid".into());
    }
    let lifecycle = match request.lifecycle.as_str() {
        "research" => insider_strategy_host::Lifecycle::Research,
        "validated" => insider_strategy_host::Lifecycle::Validated,
        "shadow" => insider_strategy_host::Lifecycle::Shadow,
        "canary" => insider_strategy_host::Lifecycle::Canary,
        "production" => insider_strategy_host::Lifecycle::Production,
        "paused" => insider_strategy_host::Lifecycle::Paused,
        "retired" => insider_strategy_host::Lifecycle::Retired,
        _ => return Err("unknown strategy lifecycle".into()),
    };
    let response = state.request(
        strategy_lifecycle_transition_command_payload(
            &request.strategy_id,
            lifecycle,
            &request.confirmation,
            &request.evidence_ref,
        ),
        None,
    )?;
    if response != b"IT_CMD_STRATEGY_LIFECYCLE_TRANSITION_OK_V1\0" {
        return Err("engine rejected strategy lifecycle transition".into());
    }
    Ok(())
}

#[tauri::command]
fn transition_metric_lifecycle(
    state: State<'_, AppState>,
    request: MetricLifecycleTransitionRequest,
) -> Result<(), String> {
    if request.metric_id.trim().is_empty() || request.metric_id.len() > 256 {
        return Err("metric ID is invalid".into());
    }
    let lifecycle = match request.lifecycle.as_str() {
        "research" => insider_metric_host::Lifecycle::Research,
        "validated" => insider_metric_host::Lifecycle::Validated,
        "shadow" => insider_metric_host::Lifecycle::Shadow,
        "canary" => insider_metric_host::Lifecycle::Canary,
        "production" => insider_metric_host::Lifecycle::Production,
        "paused" => insider_metric_host::Lifecycle::Paused,
        "retired" => insider_metric_host::Lifecycle::Retired,
        _ => return Err("unknown metric lifecycle".into()),
    };
    let response = state.request(
        metric_lifecycle_transition_command_payload(
            &request.metric_id,
            lifecycle,
            &request.confirmation,
            &request.evidence_ref,
        ),
        None,
    )?;
    if response != b"IT_CMD_METRIC_LIFECYCLE_TRANSITION_OK_V1\0" {
        return Err("engine rejected metric lifecycle transition".into());
    }
    Ok(())
}

#[tauri::command]
fn list_metrics(state: State<'_, AppState>) -> Result<Vec<MetricRegistryResponse>, String> {
    let response = state.request(metric_registry_list_command_payload().to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_METRIC_REGISTRY_LIST_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned an invalid metric registry response".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid metric registry count")?;
    if count > 4096 { return Err("too many installed metrics".into()); }
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let metric_id = read_string(&response, &mut offset)?;
        let state_name = read_string(&response, &mut offset)?;
        let lifecycle = read_string(&response, &mut offset)?;
        let priority = read_string(&response, &mut offset)?;
        let ttl_ns = read_u64(&response, &mut offset)?; let period_ns = read_u64(&response, &mut offset)?;
        let deadline_ns = read_u64(&response, &mut offset)?; let budget_ns = read_u64(&response, &mut offset)?;
        let min_score = if read_u8(&response, &mut offset)? != 0 { Some(read_f64(&response, &mut offset)?) } else { let _ = read_f64(&response, &mut offset)?; None };
        let max_score = if read_u8(&response, &mut offset)? != 0 { Some(read_f64(&response, &mut offset)?) } else { let _ = read_f64(&response, &mut offset)?; None };
        let input_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid metric input count")?;
        if input_count > 4096 { return Err("too many metric inputs".into()); }
        let mut inputs = Vec::with_capacity(input_count); for _ in 0..input_count { inputs.push(read_string(&response, &mut offset)?); }
        records.push(MetricRegistryResponse { metric_id, state: state_name, lifecycle, priority, ttl_ns, period_ns, deadline_ns, budget_ns, min_score, max_score, inputs });
    }
    if offset != response.len() { return Err("engine returned trailing metric registry bytes".into()); }
    Ok(records)
}

#[tauri::command]
fn list_experiments(state: State<'_, AppState>) -> Result<Vec<ExperimentRunResponse>, String> {
    let response = state.request(vec![33], None)?;
    const V1_MAGIC: &[u8] = b"IT_CMD_EXPERIMENT_LIST_RESPONSE_V1\0";
    const V2_MAGIC: &[u8] = b"IT_CMD_EXPERIMENT_LIST_RESPONSE_V2\0";
    let v2 = response.starts_with(V2_MAGIC);
    let magic = if v2 { V2_MAGIC } else { V1_MAGIC };
    if !response.starts_with(magic) { return Err("engine returned an invalid experiment list response".into()); }
    let mut offset = magic.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid experiment count")?;
    if count > 4096 { return Err("engine returned too many experiments".into()); }
    let mut runs = Vec::with_capacity(count);
    for _ in 0..count {
        let run_id = read_string(&response, &mut offset)?;
        let code_hash = read_string(&response, &mut offset)?;
        let config_hash = read_string(&response, &mut offset)?;
        let dataset_hash = read_string(&response, &mut offset)?;
        let status = match read_u8(&response, &mut offset)? { 1 => "created", 2 => "running", 3 => "succeeded", 4 => "failed", 5 => "cancelled", _ => return Err("invalid experiment status".into()) }.to_owned();
        let metric_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid metric count")?;
        if metric_count > 4096 { return Err("too many experiment metrics".into()); }
        let mut metrics = BTreeMap::new();
        for _ in 0..metric_count { let key = read_string(&response, &mut offset)?; let value = read_f64(&response, &mut offset)?; if !value.is_finite() { return Err("non-finite experiment metric".into()); } metrics.insert(key, value); }
        let artifact_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid artifact count")?;
        if artifact_count > 4096 { return Err("too many experiment artifacts".into()); }
        let mut artifacts = Vec::with_capacity(artifact_count);
        for _ in 0..artifact_count { artifacts.push(ExperimentArtifactResponse { kind: read_string(&response, &mut offset)?, hash: read_string(&response, &mut offset)?, path: read_string(&response, &mut offset)? }); }
        let provenance = if v2 {
            let read_optional = |response: &[u8], offset: &mut usize| -> Result<Option<String>, String> {
                if read_u8(response, offset)? == 0 { return Ok(None); }
                Ok(Some(read_string(response, offset)?))
            };
            let strategy_id = read_optional(&response, &mut offset)?;
            let strategy_version = read_optional(&response, &mut offset)?;
            let news_dataset_hash = read_optional(&response, &mut offset)?;
            let news_clustering_version = read_optional(&response, &mut offset)?;
            let graph_snapshot_version = read_optional(&response, &mut offset)?;
            let llm_provider = read_optional(&response, &mut offset)?;
            let llm_model = read_optional(&response, &mut offset)?;
            let prompt_version = read_optional(&response, &mut offset)?;
            let tool_schema_version = read_optional(&response, &mut offset)?;
            let autonomy_config_hash = read_optional(&response, &mut offset)?;
            let cache_count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid cache ID count")?;
            if cache_count > 256 { return Err("too many experiment cache IDs".into()); }
            let mut llm_cache_ids = Vec::with_capacity(cache_count);
            for _ in 0..cache_count { llm_cache_ids.push(read_string(&response, &mut offset)?); }
            ExperimentProvenanceResponse { strategy_id, strategy_version, news_dataset_hash, news_clustering_version, graph_snapshot_version, llm_provider, llm_model, prompt_version, tool_schema_version, llm_cache_ids, autonomy_config_hash }
        } else {
            ExperimentProvenanceResponse { strategy_id: None, strategy_version: None, news_dataset_hash: None, news_clustering_version: None, graph_snapshot_version: None, llm_provider: None, llm_model: None, prompt_version: None, tool_schema_version: None, llm_cache_ids: Vec::new(), autonomy_config_hash: None }
        };
        runs.push(ExperimentRunResponse { run_id, code_hash, config_hash, dataset_hash, status, metrics, artifacts, provenance });
    }
    if offset != response.len() { return Err("engine returned trailing experiment bytes".into()); }
    Ok(runs)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ConfigSnapshotResponse {
    version: u64,
    cfg_text: String,
}

#[tauri::command]
fn get_config(state: State<'_, AppState>) -> Result<ConfigSnapshotResponse, String> {
    let response = state.request(config_status_command_payload().to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_CONFIG_SNAPSHOT_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine returned invalid config snapshot".into()); }
    let mut offset = MAGIC.len();
    let version = read_u64(&response, &mut offset)?;
    let cfg_text = read_string(&response, &mut offset)?;
    if offset != response.len() { return Err("engine returned trailing config bytes".into()); }
    Ok(ConfigSnapshotResponse { version, cfg_text })
}

#[derive(Clone, Debug, Deserialize)]
struct ConfigReloadRequest { expected_version: u64, cfg_text: String }

#[tauri::command]
fn reload_config(state: State<'_, AppState>, request: ConfigReloadRequest) -> Result<ConfigSnapshotResponse, String> {
    if request.cfg_text.trim().is_empty() || request.cfg_text.len() > 1_048_576 {
        return Err("configuration text is empty or exceeds 1 MiB".into());
    }
    let response = state.request(config_reload_command_payload(request.expected_version, &request.cfg_text), None)?;
    const MAGIC: &[u8] = b"IT_CMD_CONFIG_SNAPSHOT_V1\0";
    if !response.starts_with(MAGIC) { return Err("engine rejected configuration reload".into()); }
    let mut offset = MAGIC.len();
    let version = read_u64(&response, &mut offset)?;
    let cfg_text = read_string(&response, &mut offset)?;
    if offset != response.len() { return Err("engine returned trailing config bytes".into()); }
    Ok(ConfigSnapshotResponse { version, cfg_text })
}

#[tauri::command]
fn submit_manual_order(
    state: State<'_, AppState>,
    request: SubmitOrderRequest,
) -> Result<String, String> {
    if request.preview_id.trim().is_empty() || request.confirmation_token != "CONFIRM" {
        return Err("explicit CONFIRM is required".into());
    }
    let now_ns = state.now().as_nanos();
    let mut previews = state
        .previews
        .lock()
        .map_err(|_| "preview store is unavailable".to_owned())?;
    previews.retain(|_, preview| now_ns.saturating_sub(preview.created_mono_ns) < PREVIEW_TTL_NS);
    let preview = previews
        .get(&request.preview_id)
        .map(|cached| cached.payload.clone())
        .ok_or_else(|| "preview is missing or expired".to_owned())?;
    drop(previews);
    let payload = submit_preview_payload(&preview, state.now(), &request.confirmation_token)?;
    let response = state.request_with_key(
        payload,
        None,
        format!("manual-submit-{}", request.preview_id),
    )?;
    let mut offset = 0;
    let order_id = read_string(&response, &mut offset)?;
    if offset != response.len() || order_id.trim().is_empty() {
        return Err("engine returned an invalid order ID".into());
    }
    state
        .previews
        .lock()
        .map_err(|_| "preview store is unavailable".to_owned())?
        .remove(&request.preview_id);
    Ok(order_id)
}

#[tauri::command]
fn cancel_order(
    state: State<'_, AppState>,
    request: CancelOrderRequest,
) -> Result<(), String> {
    if request.client_order_id.trim().is_empty() {
        return Err("client order ID is required".into());
    }
    let response = state.request(cancel_command_payload(&request.client_order_id), None)?;
    if response != b"IT_CMD_CANCEL_OK_V1\0" {
        return Err("engine returned an invalid cancel response".into());
    }
    Ok(())
}

#[tauri::command]
fn replace_order(
    state: State<'_, AppState>,
    request: ReplaceOrderRequest,
) -> Result<(), String> {
    if request.client_order_id.trim().is_empty()
        || request.quantity_ticks <= 0
        || request.limit_price_ticks.is_some_and(|price| price <= 0)
    {
        return Err("replacement order is invalid".into());
    }
    let response = state.request(
        replace_command_payload(
            &request.client_order_id,
            request.quantity_ticks,
            request.limit_price_ticks,
        ),
        None,
    )?;
    if response != b"IT_CMD_REPLACE_OK_V1\0" {
        return Err("engine returned an invalid replace response".into());
    }
    Ok(())
}

#[tauri::command]
fn set_trading_mode(
    state: State<'_, AppState>,
    request: TradingModeRequest,
) -> Result<String, String> {
    let mode = match request.mode.as_str() {
        "manual" => AutonomyMode::Manual,
        "hybrid" => AutonomyMode::Hybrid,
        "autonomous" => AutonomyMode::Autonomous,
        _ => return Err("unsupported trading mode".into()),
    };
    let response = state.request(autonomy_mode_command_payload(mode), None)?;
    const MAGIC: &[u8] = b"IT_CMD_AUTONOMY_MODE_RESPONSE_V1\0";
    if response.len() != MAGIC.len() + 1 || !response.starts_with(MAGIC) {
        return Err("engine returned an invalid trading mode response".into());
    }
    let value = match response[MAGIC.len()] {
        1 => "manual",
        2 => "hybrid",
        3 => "autonomous",
        _ => return Err("engine returned an unknown trading mode".into()),
    };
    Ok(value.to_owned())
}

#[tauri::command]
fn submit_autonomous_plan(
    state: State<'_, AppState>,
    request: AutonomousPlanRequest,
) -> Result<String, String> {
    if request.plan_id.trim().is_empty()
        || request.plan_id.len() > 256
        || request.expires_after_ms == 0
        || request.expires_after_ms > 86_400_000
        || request.actions.is_empty()
        || request.actions.len() > 4_096
    {
        return Err("autonomous plan is outside bounds".into());
    }
    let actions = request
        .actions
        .into_iter()
        .map(|action| {
            let action_type = match action.action_type.as_str() {
                "EXECUTE_PROPOSAL" => ActionType::ExecuteProposal,
                "EXECUTE_PROPOSAL_SCALED" => ActionType::ExecuteProposalScaled,
                "IGNORE_PROPOSAL" => ActionType::IgnoreProposal,
                "PAUSE_STRATEGY" => ActionType::PauseStrategy,
                "RESUME_STRATEGY" => ActionType::ResumeStrategy,
                "REQUEST_REANALYSIS" => ActionType::RequestReanalysis,
                "ADD_TO_WATCH" => ActionType::AddToWatch,
                "REMOVE_FROM_WATCH" => ActionType::RemoveFromWatch,
                "REDUCE_AUTONOMY" => ActionType::ReduceAutonomy,
                "NO_ACTION" => ActionType::NoAction,
                _ => return Err("unknown autonomous action type".to_owned()),
            };
            let value = AutonomousAction { action_type, proposal_id: action.proposal_id, scale: action.scale, reason_codes: action.reason_codes };
            value.validate().map_err(|error| format!("invalid autonomous action: {error:?}"))?;
            Ok(value)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let ttl_ns = request.expires_after_ms.checked_mul(1_000_000).ok_or("plan TTL overflow")?;
    let plan = Plan { plan_id: request.plan_id.clone(), generated_at: MonoTime::from_nanos(0), expires_at: MonoTime::from_nanos(ttl_ns), actions };
    let response = state.request(autonomy_submit_command_payload(&plan), None)?;
    const MAGIC: &[u8] = b"IT_CMD_AUTONOMY_SUBMIT_OK_V1\0";
    if response != MAGIC { return Err("engine rejected autonomous plan".into()); }
    Ok(request.plan_id)
}

#[tauri::command]
fn transition_autonomous_plan(
    state: State<'_, AppState>,
    request: AutonomousPlanTransitionRequest,
) -> Result<String, String> {
    if request.plan_id.trim().is_empty() || request.plan_id.len() > 256 {
        return Err("autonomous plan ID is invalid".into());
    }
    let next = match request.state.as_str() {
        "pending" => PlanState::Pending,
        "approved" => PlanState::Approved,
        "rejected" => PlanState::Rejected,
        "expired" => PlanState::Expired,
        "executing" => PlanState::Executing,
        "completed" => PlanState::Completed,
        "failed" => PlanState::Failed,
        _ => return Err("unsupported autonomous plan state".into()),
    };
    let response = state.request(
        autonomy_transition_command_payload(&request.plan_id, next, MonoTime::from_nanos(0)),
        None,
    )?;
    const MAGIC: &[u8] = b"IT_CMD_AUTONOMY_TRANSITION_OK_V1\0";
    if response != MAGIC {
        return Err("engine rejected autonomous plan transition".into());
    }
    Ok(request.state)
}

#[tauri::command]
fn get_alerts(state: State<'_, AppState>) -> Result<Vec<AlertResponse>, String> {
    let response = state.request(alerts_get_command_payload().to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_ALERTS_RESPONSE_V1\0";
    if !response.starts_with(MAGIC) || response.len() < MAGIC.len() + 4 {
        return Err("engine returned an invalid alert response".into());
    }
    let mut offset = MAGIC.len();
    let count = u32::from_le_bytes(
        response[offset..offset + 4]
            .try_into()
            .map_err(|_| "invalid alert count")?,
    ) as usize;
    offset += 4;
    if count > 4_096 {
        return Err("engine returned too many alerts".into());
    }
    let mut alerts = Vec::with_capacity(count);
    for _ in 0..count {
        let alert_id = read_wire_string(&response, &mut offset)?;
        let dedupe_key = read_wire_string(&response, &mut offset)?;
        let source = read_wire_string(&response, &mut offset)?;
        let occurred_ms = read_wire_i64(&response, &mut offset)?;
        let severity = *response.get(offset).ok_or("alert severity truncated")?;
        offset += 1;
        let sensitive = match response.get(offset) {
            Some(0) => false,
            Some(1) => true,
            _ => return Err("invalid alert sensitivity".into()),
        };
        offset += 1;
        let message = read_wire_string(&response, &mut offset)?;
        if !(1..=3).contains(&severity) {
            return Err("invalid alert severity".into());
        }
        alerts.push(AlertResponse { alert_id, dedupe_key, source, occurred_ms, severity, sensitive, message });
    }
    if offset != response.len() {
        return Err("trailing alert response bytes".into());
    }
    Ok(alerts)
}

#[tauri::command]
fn acknowledge_alert(
    state: State<'_, AppState>,
    request: AlertAcknowledgeRequest,
) -> Result<bool, String> {
    if request.alert_id.trim().is_empty() || request.alert_id.len() > 256 {
        return Err("alert ID is invalid".into());
    }
    let response = state.request(alert_ack_command_payload(&request.alert_id), None)?;
    const MAGIC: &[u8] = b"IT_CMD_ALERT_ACK_RESPONSE_V1\0";
    if response.len() != MAGIC.len() + 1 || !response.starts_with(MAGIC) {
        return Err("engine returned an invalid alert acknowledgement".into());
    }
    match response[MAGIC.len()] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err("engine returned an invalid acknowledgement marker".into()),
    }
}

fn decode_journal_backup_response(
    response: &[u8],
    magic: &[u8],
) -> Result<JournalBackupResponse, String> {
    if !response.starts_with(magic) {
        return Err("engine returned an invalid journal backup response".into());
    }
    let mut offset = magic.len();
    let source = read_string(response, &mut offset)?;
    let destination = read_string(response, &mut offset)?;
    let byte_len = read_u64(response, &mut offset)?;
    let digest = read_bytes(response, &mut offset, 32)?;
    if source.trim().is_empty() || destination.trim().is_empty() || offset != response.len() {
        return Err("engine returned malformed journal backup metadata".into());
    }
    Ok(JournalBackupResponse {
        source,
        destination,
        byte_len,
        sha256: hex_encode(digest),
    })
}

#[tauri::command]
fn backup_journal(
    state: State<'_, AppState>,
    request: BackupRequest,
) -> Result<JournalBackupResponse, String> {
    if request.path.trim().is_empty() || request.path.len() > 4_096 {
        return Err("backup path is invalid".into());
    }
    let response = state.request(journal_backup_command_payload(&request.path), None)?;
    decode_journal_backup_response(&response, b"IT_CMD_JOURNAL_BACKUP_OK_V1\0")
}

#[tauri::command]
fn restore_journal(
    state: State<'_, AppState>,
    request: RestoreBackupRequest,
) -> Result<JournalBackupResponse, String> {
    if request.source.trim().is_empty()
        || request.destination.trim().is_empty()
        || request.source.len() > 4_096
        || request.destination.len() > 4_096
    {
        return Err("restore paths are invalid".into());
    }
    let response = state.request(
        journal_restore_command_payload(&request.source, &request.destination),
        None,
    )?;
    decode_journal_backup_response(&response, b"IT_CMD_JOURNAL_RESTORE_OK_V1\0")
}

#[tauri::command]
fn transition_risk_state(
    state: State<'_, AppState>,
    request: RiskStateRequest,
) -> Result<String, String> {
    if request.authorization.len() > 256 {
        return Err("risk authorization is too long".into());
    }
    let next = match request.state.as_str() {
        "running" => RiskState::Running,
        "reduce_only" => RiskState::ReduceOnly,
        "cancel_only" => RiskState::CancelOnly,
        "halted" => RiskState::Halted,
        _ => return Err("unsupported risk state".into()),
    };
    let response = state.request(
        risk_state_transition_command_payload(next, &request.authorization),
        None,
    )?;
    const MAGIC: &[u8] = b"IT_CMD_RISK_STATE_OK_V1\0";
    if response.len() != MAGIC.len() + 1 || !response.starts_with(MAGIC) {
        return Err("engine returned an invalid risk-state response".into());
    }
    let state = match response[MAGIC.len()] {
        1 => "running",
        2 => "reduce_only",
        3 => "cancel_only",
        4 => "halted",
        _ => return Err("engine returned an unknown risk state".into()),
    };
    Ok(state.into())
}

fn read_wire_string(bytes: &[u8], offset: &mut usize) -> Result<String, String> {
    let end_len = (*offset).checked_add(4).ok_or("alert string length overflow")?;
    let length = u32::from_le_bytes(
        bytes
            .get(*offset..end_len)
            .ok_or("alert string length truncated")?
            .try_into()
            .map_err(|_| "alert string length truncated")?,
    ) as usize;
    *offset = end_len;
    if length > 1_048_576 {
        return Err("alert string exceeds bound".into());
    }
    let end = (*offset).checked_add(length).ok_or("alert string overflow")?;
    let value = String::from_utf8(bytes.get(*offset..end).ok_or("alert string truncated")?.to_vec())
        .map_err(|_| "alert string is not UTF-8")?;
    *offset = end;
    Ok(value)
}

fn read_wire_i64(bytes: &[u8], offset: &mut usize) -> Result<i64, String> {
    let end = (*offset).checked_add(8).ok_or("alert timestamp overflow")?;
    let value = i64::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or("alert timestamp truncated")?
            .try_into()
            .map_err(|_| "alert timestamp truncated")?,
    );
    *offset = end;
    Ok(value)
}

fn read_wire_u64(bytes: &[u8], offset: &mut usize) -> Result<u64, String> {
    let end = (*offset).checked_add(8).ok_or("trace sequence overflow")?;
    let value = u64::from_le_bytes(
        bytes
            .get(*offset..end)
            .ok_or("trace sequence truncated")?
            .try_into()
            .map_err(|_| "trace sequence truncated")?,
    );
    *offset = end;
    Ok(value)
}

fn read_wire_bytes(bytes: &[u8], offset: &mut usize, max: usize) -> Result<Vec<u8>, String> {
    let end_len = (*offset).checked_add(4).ok_or("trace payload length overflow")?;
    let length = u32::from_le_bytes(
        bytes
            .get(*offset..end_len)
            .ok_or("trace payload length truncated")?
            .try_into()
            .map_err(|_| "trace payload length truncated")?,
    ) as usize;
    *offset = end_len;
    if length > max { return Err("trace payload exceeds bound".into()); }
    let end = (*offset).checked_add(length).ok_or("trace payload overflow")?;
    let value = bytes.get(*offset..end).ok_or("trace payload truncated")?.to_vec();
    *offset = end;
    Ok(value)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[tauri::command]
fn configure_live_limits(
    state: State<'_, AppState>,
    request: LiveConfigureRequest,
) -> Result<TradingEnvironmentResponse, String> {
    if request.accounts.is_empty()
        || request.accounts.len() > 128
        || request.max_notional_ticks == 0
    {
        return Err("live limits are invalid".into());
    }
    let payload = live_configure_command_payload(&request.accounts, request.max_notional_ticks);
    let environment = decode_environment(&state.request(payload, None)?)?;
    Ok(TradingEnvironmentResponse { environment })
}

#[tauri::command]
fn arm_live(
    state: State<'_, AppState>,
    request: LiveArmRequest,
) -> Result<LiveArmResponse, String> {
    if request.account.trim().is_empty() || request.phrase != "ARM LIVE" {
        return Err("type ARM LIVE exactly".into());
    }
    let payload = live_arm_command_payload(&request.account, state.now(), &request.phrase);
    decode_environment_with_token(&state.request(payload, None)?)
}

#[tauri::command]
fn confirm_live(
    state: State<'_, AppState>,
    request: LiveConfirmRequest,
) -> Result<TradingEnvironmentResponse, String> {
    if request.account.trim().is_empty()
        || request.token.trim().is_empty()
        || request.phrase != "ENABLE LIVE"
    {
        return Err("type ENABLE LIVE exactly and provide the challenge token".into());
    }
    let payload = live_confirm_command_payload(
        &request.account,
        &request.token,
        state.now(),
        &request.phrase,
    );
    let environment = decode_environment(&state.request(payload, None)?)?;
    Ok(TradingEnvironmentResponse { environment })
}

#[tauri::command]
fn kill_live(state: State<'_, AppState>) -> Result<TradingEnvironmentResponse, String> {
    let environment =
        decode_environment(&state.request(live_kill_command_payload().to_vec(), None)?)?;
    Ok(TradingEnvironmentResponse { environment })
}

#[tauri::command]
fn analyze(state: State<'_, AppState>, request: AnalystRequest) -> Result<AnalystResponse, String> {
    if request.task.trim().is_empty()
        || request.task.len() > 128
        || request.input.trim().is_empty()
        || request.input.len() > 1_048_576
        || request.context_hash.trim().is_empty()
        || request.context_hash.len() > 256
        || request.model.trim().is_empty()
        || request.model.len() > 256
        || request.prompt_version.trim().is_empty()
        || request.prompt_version.len() > 256
        || request.max_output_tokens == 0
        || request.max_output_tokens > 16_384
    {
        return Err("analyst request exceeds required bounds".into());
    }
    let endpoint = match request.endpoint.as_str() {
        "responses" => Endpoint::Responses,
        "chat_completions" => Endpoint::ChatCompletions,
        _ => return Err("unsupported analyst endpoint".into()),
    };
    let trace_id = format!("tauri-llm-{}", state.next_id.load(Ordering::Relaxed));
    let payload = llm_complete_command_payload(&LlmRequest {
        trace_id,
        prompt_version: request.prompt_version,
        model: request.model,
        task: request.task,
        context_hash: request.context_hash,
        input: request.input,
        max_output_tokens: request.max_output_tokens,
        endpoint,
    });
    decode_analyst_response(&state.request(payload, None)?)
}

#[tauri::command]
fn analyze_stream(
    state: State<'_, AppState>,
    request: AnalystRequest,
) -> Result<Vec<AnalystStreamChunk>, String> {
    if request.task.trim().is_empty()
        || request.task.len() > 128
        || request.input.trim().is_empty()
        || request.input.len() > 1_048_576
        || request.context_hash.trim().is_empty()
        || request.context_hash.len() > 256
        || request.model.trim().is_empty()
        || request.model.len() > 256
        || request.prompt_version.trim().is_empty()
        || request.prompt_version.len() > 256
        || request.max_output_tokens == 0
        || request.max_output_tokens > 16_384
    {
        return Err("analyst request exceeds required bounds".into());
    }
    let endpoint = match request.endpoint.as_str() {
        "responses" => Endpoint::Responses,
        "chat_completions" => Endpoint::ChatCompletions,
        _ => return Err("unsupported analyst endpoint".into()),
    };
    let trace_id = format!("tauri-llm-{}", state.next_id.load(Ordering::Relaxed));
    let payload = llm_stream_command_payload(&LlmRequest {
        trace_id,
        prompt_version: request.prompt_version,
        model: request.model,
        task: request.task,
        context_hash: request.context_hash,
        input: request.input,
        max_output_tokens: request.max_output_tokens,
        endpoint,
    });
    decode_analyst_stream(&state.request(payload, None)?)
}

fn decode_analyst_stream(value: &[u8]) -> Result<Vec<AnalystStreamChunk>, String> {
    const MAGIC: &[u8] = b"IT_CMD_LLM_STREAM_RESPONSE_V1\0";
    if !value.starts_with(MAGIC) {
        return Err("engine returned an invalid analyst stream".into());
    }
    let mut offset = MAGIC.len();
    let trace_id = read_string(value, &mut offset)?;
    let count_bytes = read_bytes(value, &mut offset, 4)?;
    let count = u32::from_le_bytes(count_bytes.try_into().map_err(|_| "stream count")?) as usize;
    if trace_id.trim().is_empty() || count == 0 || count > 4_096 {
        return Err("analyst stream has invalid bounds".into());
    }
    let mut chunks = Vec::with_capacity(count);
    let mut terminal = false;
    for _ in 0..count {
        let kind = *value.get(offset).ok_or("stream kind truncated")?;
        offset += 1;
        let text = read_string(value, &mut offset)?;
        let kind_name = match kind {
            1 => "delta",
            2 => {
                terminal = true;
                "done"
            }
            _ => return Err("analyst stream kind is invalid".into()),
        };
        chunks.push(AnalystStreamChunk {
            trace_id: trace_id.clone(),
            kind: kind_name.into(),
            text,
        });
    }
    if offset != value.len() || !terminal {
        return Err("analyst stream is incomplete or has trailing bytes".into());
    }
    Ok(chunks)
}

fn decode_analyst_response(value: &[u8]) -> Result<AnalystResponse, String> {
    const MAGIC: &[u8] = b"IT_CMD_LLM_COMPLETE_RESPONSE_V1\0";
    if !value.starts_with(MAGIC) {
        return Err("engine returned an invalid analyst response".into());
    }
    let mut offset = MAGIC.len();
    let trace_id = read_string(value, &mut offset)?;
    let finish_reason = read_string(value, &mut offset)?;
    let content = read_string(value, &mut offset)?;
    if offset != value.len() || trace_id.trim().is_empty() || content.is_empty() {
        return Err("analyst response is empty, missing trace, or malformed".into());
    }
    Ok(AnalystResponse {
        trace_id,
        finish_reason,
        content,
    })
}

#[tauri::command]
fn get_trace_events(
    state: State<'_, AppState>,
    trace_id: String,
) -> Result<Vec<TraceEventResponse>, String> {
    let raw = trace_id.trim();
    let numeric = raw
        .strip_prefix("tauri-trace-")
        .or_else(|| raw.strip_prefix("trace-"))
        .unwrap_or(raw)
        .parse::<u128>()
        .map_err(|_| "trace ID must be numeric".to_owned())?;
    let trace = TraceId::new(numeric).map_err(|_| "trace ID is invalid".to_owned())?;
    let response = state.request(trace_events_command_payload(trace).to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_TRACE_EVENTS_V1\0";
    if !response.starts_with(MAGIC) || response.len() < MAGIC.len() + 4 {
        return Err("engine returned an invalid trace response".into());
    }
    let mut offset = MAGIC.len();
    let count = u32::from_le_bytes(
        response[offset..offset + 4]
            .try_into()
            .map_err(|_| "trace count truncated")?,
    ) as usize;
    offset += 4;
    if count > 4_096 {
        return Err("trace contains too many events".into());
    }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let sequence = read_wire_u64(&response, &mut offset)?;
        let kind = read_wire_string(&response, &mut offset)?;
        let payload = read_wire_bytes(&response, &mut offset, 1_048_576)?;
        events.push(TraceEventResponse {
            sequence,
            kind,
            payload_hex: hex_encode(&payload),
        });
    }
    if offset != response.len() {
        return Err("trace response has trailing bytes".into());
    }
    Ok(events)
}

#[tauri::command]
fn export_trace(
    state: State<'_, AppState>,
    trace_id: String,
) -> Result<Vec<TraceExportEventResponse>, String> {
    let raw = trace_id.trim();
    let numeric = raw.strip_prefix("tauri-trace-").or_else(|| raw.strip_prefix("trace-")).unwrap_or(raw)
        .parse::<u128>().map_err(|_| "trace ID must be numeric".to_owned())?;
    let trace = TraceId::new(numeric).map_err(|_| "trace ID is invalid".to_owned())?;
    let response = state.request(trace_export_command_payload(trace).to_vec(), None)?;
    const MAGIC: &[u8] = b"IT_CMD_TRACE_EXPORT_V1\0";
    if !response.starts_with(MAGIC) || response.len() < MAGIC.len() + 4 { return Err("engine returned an invalid trace export".into()); }
    let mut offset = MAGIC.len();
    let count = usize::try_from(read_u32(&response, &mut offset)?).map_err(|_| "invalid trace export count")?;
    if count > 4096 { return Err("trace export contains too many events".into()); }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        events.push(TraceExportEventResponse { sequence: read_wire_u64(&response, &mut offset)?, kind: read_wire_string(&response, &mut offset)?, payload_bytes: read_u32(&response, &mut offset)? });
    }
    if offset != response.len() { return Err("trace export has trailing bytes".into()); }
    Ok(events)
}

#[tauri::command]
fn evaluate_threshold_strategy(
    state: State<'_, AppState>,
    request: StrategyEvaluateRequest,
) -> Result<StrategyProposalResponse, String> {
    if request.strategy_id.trim().is_empty()
        || request.strategy_id.len() > 256
        || request.metric_id.trim().is_empty()
        || request.metric_id.len() > 256
        || request.metric_ttl_ns == 0
        || request.horizon_ns == 0
        || request.strategy_ttl_ns == 0
        || !request.score.is_finite()
        || !request.confidence.is_finite()
        || !request.uncertainty.is_finite()
        || !request.entry_threshold.is_finite()
        || !request.exit_threshold.is_finite()
    {
        return Err("strategy evaluation request exceeds bounds".into());
    }
    let instrument_id = request
        .instrument_id
        .parse::<u128>()
        .ok()
        .and_then(|value| InstrumentId::new(value).ok())
        .ok_or("instrument ID must be a canonical numeric identity")?;
    let metric = MetricOutput {
        metric_id: request.metric_id.clone(),
        instrument_id,
        // The engine replaces this client placeholder with its own clock.
        generated_mono: MonoTime::from_nanos(0),
        ttl_ns: request.metric_ttl_ns,
        score: request.score,
        confidence: request.confidence,
        uncertainty: request.uncertainty,
    };
    let payload = strategy_evaluate_command_payload(
        &request.strategy_id,
        &request.metric_id,
        &metric,
        request.entry_threshold,
        request.exit_threshold,
        request.quantity_ticks,
        request.horizon_ns,
        request.strategy_ttl_ns,
        MonoTime::from_nanos(0),
    );
    decode_strategy_proposal_response(&state.request(payload, None)?)
}

fn decode_strategy_proposal_response(value: &[u8]) -> Result<StrategyProposalResponse, String> {
    const MAGIC: &[u8] = b"IT_CMD_STRATEGY_PROPOSAL_RESPONSE_V1\0";
    if !value.starts_with(MAGIC) {
        return Err("engine returned an invalid strategy proposal".into());
    }
    let mut offset = MAGIC.len();
    let proposal_id = read_u128(value, &mut offset)?.to_string();
    let strategy_id = read_string(value, &mut offset)?;
    let instrument_id = read_u128(value, &mut offset)?.to_string();
    let action = match read_bytes(value, &mut offset, 1)?[0] {
        0 => "NO_ACTION",
        1 => "TARGET_QUANTITY",
        2 => "TARGET_WEIGHT",
        3 => "INCREASE",
        4 => "DECREASE",
        5 => "CLOSE",
        _ => return Err("engine returned an unknown strategy action".into()),
    }
    .to_owned();
    let quantity_ticks = read_i64(value, &mut offset)?;
    let weight = read_f64(value, &mut offset)?;
    let confidence = read_f64(value, &mut offset)?;
    let generated_mono_ns = read_u64(value, &mut offset)?;
    let ttl_ns = read_u64(value, &mut offset)?;
    if offset != value.len() || proposal_id == "0" || strategy_id.trim().is_empty()
        || !confidence.is_finite() || !(0.0..=1.0).contains(&confidence)
    {
        return Err("engine returned malformed strategy proposal".into());
    }
    Ok(StrategyProposalResponse { proposal_id, strategy_id, instrument_id, action, quantity_ticks, weight, confidence, generated_mono_ns, ttl_ns })
}

#[tauri::command]
fn validate_autonomous_action(
    state: State<'_, AppState>,
    request: AnalystRequest,
) -> Result<AutonomousActionResponse, String> {
    let endpoint = match request.endpoint.as_str() {
        "responses" => Endpoint::Responses,
        "chat_completions" => Endpoint::ChatCompletions,
        _ => return Err("unsupported autonomous action endpoint".into()),
    };
    if request.input.trim().is_empty() || request.input.len() > 4 * 1024 * 1024 {
        return Err("autonomous action input exceeds bounds".into());
    }
    let payload = llm_action_command_payload(&LlmRequest {
        trace_id: format!("tauri-action-{}", state.next_id.load(Ordering::Relaxed)),
        prompt_version: request.prompt_version,
        model: request.model,
        task: request.task,
        context_hash: request.context_hash,
        input: request.input,
        max_output_tokens: request.max_output_tokens,
        endpoint,
    });
    decode_autonomous_action_response(&state.request(payload, None)?)
}

fn decode_autonomous_action_response(value: &[u8]) -> Result<AutonomousActionResponse, String> {
    const MAGIC: &[u8] = b"IT_CMD_LLM_ACTION_RESPONSE_V1\0";
    if !value.starts_with(MAGIC) { return Err("invalid autonomous action response".into()); }
    let mut offset = MAGIC.len();
    let action_type = match read_bytes(value, &mut offset, 1)?[0] {
        1 => "EXECUTE_PROPOSAL", 2 => "EXECUTE_PROPOSAL_SCALED", 3 => "IGNORE_PROPOSAL",
        4 => "PAUSE_STRATEGY", 5 => "RESUME_STRATEGY", 6 => "REQUEST_REANALYSIS",
        7 => "ADD_TO_WATCH", 8 => "REMOVE_FROM_WATCH", 9 => "REDUCE_AUTONOMY",
        10 => "NO_ACTION", _ => return Err("unknown autonomous action".into()),
    }.to_owned();
    let proposal = read_string(value, &mut offset)?;
    let scale = f64::from_le_bytes(read_bytes(value, &mut offset, 8)?.try_into().map_err(|_| "action scale")?);
    let count = usize::from(read_u16(value, &mut offset)?);
    if count > 256 || !scale.is_finite() { return Err("malformed autonomous action".into()); }
    let reason_codes = (0..count).map(|_| read_string(value, &mut offset)).collect::<Result<Vec<_>, _>>()?;
    if offset != value.len() { return Err("autonomous action response has trailing bytes".into()); }
    Ok(AutonomousActionResponse { trace_id, action_type, proposal_id: (!proposal.is_empty()).then_some(proposal), scale: (scale != 0.0).then_some(scale), reason_codes })
}

#[tauri::command]
fn search_context(
    state: State<'_, AppState>,
    request: ContextSearchRequest,
) -> Result<Vec<ContextSearchHit>, String> {
    if request.text.trim().is_empty()
        || request.text.len() > 16_384
        || request.max_depth > 8
        || request.limit == 0
        || request.limit > 256
        || request.embedding.as_ref().is_some_and(|values| {
            values.is_empty() || values.len() > 4_096 || values.iter().any(|value| !value.is_finite())
        })
    {
        return Err("context search request exceeds bounds".into());
    }
    let payload = context_search_command_payload_with_embedding(
        &request.text,
        request.graph_root.as_deref(),
        usize::from(request.max_depth),
        usize::from(request.limit),
        request.embedding.as_deref(),
    );
    decode_context_search_response(&state.request(payload, None)?)
}

fn decode_context_search_response(value: &[u8]) -> Result<Vec<ContextSearchHit>, String> {
    const MAGIC: &[u8] = b"IT_CMD_CONTEXT_SEARCH_RESPONSE_V1\0";
    if !value.starts_with(MAGIC) {
        return Err("engine returned an invalid context search response".into());
    }
    let mut offset = MAGIC.len();
    let count = usize::from(read_u16(value, &mut offset)?);
    if count > 256 {
        return Err("engine returned too many context hits".into());
    }
    let mut hits = Vec::with_capacity(count);
    for _ in 0..count {
        let node_id = read_string(value, &mut offset)?;
        let score = f64::from_le_bytes(read_bytes(value, &mut offset, 8)?.try_into().map_err(|_| "context score")?);
        let exact_score = f64::from_le_bytes(read_bytes(value, &mut offset, 8)?.try_into().map_err(|_| "context exact score")?);
        let lexical_score = f64::from_le_bytes(read_bytes(value, &mut offset, 8)?.try_into().map_err(|_| "context lexical score")?);
        let vector_score = f64::from_le_bytes(read_bytes(value, &mut offset, 8)?.try_into().map_err(|_| "context vector score")?);
        let path_count = usize::from(read_u16(value, &mut offset)?);
        if path_count > 32 || node_id.trim().is_empty() || !score.is_finite() {
            return Err("engine returned malformed context hit".into());
        }
        let evidence_path = (0..path_count)
            .map(|_| read_string(value, &mut offset))
            .collect::<Result<Vec<_>, _>>()?;
        hits.push(ContextSearchHit {
            node_id,
            score,
            exact_score,
            lexical_score,
            vector_score,
            evidence_path,
        });
    }
    if offset != value.len() {
        return Err("context search response has trailing bytes".into());
    }
    Ok(hits)
}

pub fn run() -> Result<(), String> {
    let socket = std::env::var_os("IT_ENGINE_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/insidertrader-engine.sock"));
    let state = AppState::new(socket)?;
    tauri::Builder::default()
        .manage(state)
        .invoke_handler(tauri::generate_handler![
            get_runtime_snapshot,
            get_news_page,
            get_news_detail,
            get_news_provider_status,
            get_supervisor_status,
            get_broker_status,
            get_risk_policy_status,
            analyze,
            analyze_stream,
            evaluate_threshold_strategy,
            validate_autonomous_action,
            search_context,
            resolve_instrument,
            preview_order,
            preview_proposal,
            submit_proposal,
            submit_scheduled_proposal,
            run_backtest,
            run_strategy_backtest,
            list_backtests,
            list_experiments,
            get_config,
            reload_config,
            mutate_experiment,
            list_models,
            mutate_model,
            list_strategy_resolutions,
            resolve_strategy_with_budgets,
            list_strategy_execution_summaries,
            list_strategies,
            transition_strategy_lifecycle,
            transition_metric_lifecycle,
            list_metrics,
            submit_manual_order,
            cancel_order,
            replace_order,
            set_trading_mode,
            submit_autonomous_plan,
            transition_autonomous_plan,
            get_alerts,
            acknowledge_alert,
            backup_journal,
            restore_journal,
            transition_risk_state,
            get_trace_events,
            export_trace,
            configure_live_limits,
            arm_live,
            confirm_live,
            kill_live
        ])
        .run(tauri::generate_context!())
        .map_err(|error| format!("Tauri runtime: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{command_envelope, wall_clock_ns};

    #[test]
    fn wall_clock_is_bounded_and_currently_nonzero() {
        let timestamp = wall_clock_ns();
        assert!(timestamp > 0);
    }

    #[test]
    fn command_envelope_contains_issuance_metadata() {
        let command = command_envelope(7, 3, "idem-7".into(), vec![1, 2, 3]);
        assert_eq!(command.command_id, "tauri-command-7");
        assert_eq!(command.trace_id, "tauri-trace-7");
        assert_eq!(command.expected_state_version, 3);
        assert!(command.issued_wall_ns > 0);
    }
}
