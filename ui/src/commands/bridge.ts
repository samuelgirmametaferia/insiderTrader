import type {
  OrderPreview,
  OrderDraft,
  ProposalSnapshot,
  RuntimeSnapshot,
  RuntimeState,
  TradePrintSnapshot,
  TcaSnapshot,
  RuntimeStore,
  OrderSnapshot,
  NewsPageSnapshot,
} from "../stores/runtime-store";
import { validateOrderDraft } from "../stores/runtime-store";

export interface RuntimeBridge {
  invoke<T>(command: string, payload?: unknown): Promise<T>;
  listen<T>(event: string, handler: (payload: T) => void): Promise<() => void>;
}

export interface TradingCommands {
  loadNewsPage(scope: "relevant" | "all", symbol: string, afterCursor?: string): Promise<NewsPageSnapshot>;
  getNewsProviderStatuses(): Promise<readonly NewsProviderStatusSnapshot[]>;
  getSupervisorStatuses(): Promise<readonly SupervisorStatusSnapshot[]>;
  getBrokerStatus(): Promise<BrokerStatusSnapshot>;
  getRiskPolicyStatus(): Promise<readonly RiskPolicyRevisionSnapshot[]>;
  getNewsDetail(itemId: string): Promise<NewsDetailSnapshot | undefined>;
  searchContext(text: string, graphRoot?: string, maxDepth?: number, limit?: number, embedding?: readonly number[]): Promise<readonly ContextSearchHit[]>;
  analyze(request: AnalystRequest): Promise<AnalystResponse>;
  analyzeStream(request: AnalystRequest): Promise<readonly AnalystStreamChunk[]>;
  evaluateThresholdStrategy(request: StrategyEvaluateRequest): Promise<StrategyProposalResponse>;
  validateAutonomousAction(request: AnalystRequest): Promise<AutonomousActionResponse>;
  submitAutonomousPlan(request: AutonomousPlanRequest): Promise<string>;
  transitionAutonomousPlan(request: AutonomousPlanTransitionRequest): Promise<string>;
  getAlerts(): Promise<readonly AlertSnapshot[]>;
  acknowledgeAlert(alertId: string): Promise<boolean>;
  getTraceEvents(traceId: string): Promise<readonly TraceEventSnapshot[]>;
  exportTrace(traceId: string): Promise<readonly TraceExportEventSnapshot[]>;
  resolveInstrument(symbol: string): Promise<InstrumentResolution>;
  previewOrder(draft: OrderDraft): Promise<OrderPreview>;
  submitManualOrder(draft: OrderDraft, confirmationToken: string): Promise<string>;
  cancelOrder(clientOrderId: string): Promise<void>;
  replaceOrder(clientOrderId: string, quantityTicks: number, limitPriceTicks?: number): Promise<void>;
  previewProposal(proposalId: string, scale?: number): Promise<unknown>;
  submitProposal(proposalId: string, confirmationToken: string): Promise<string>;
  submitScheduledProposal(proposalId: string, schedule: ExecutionScheduleRequest, confirmationToken: string): Promise<string>;
  runBacktest(request: BacktestRunRequest): Promise<BacktestRunResponse>;
  runStrategyBacktest(request: StrategyBacktestRunRequest): Promise<BacktestRunResponse>;
  listBacktests(): Promise<readonly BacktestRunResponse[]>;
  listExperiments(): Promise<readonly ExperimentRunResponse[]>;
  getConfig(): Promise<ConfigSnapshotResponse>;
  reloadConfig(request: ConfigReloadRequest): Promise<ConfigSnapshotResponse>;
  mutateExperiment(request: ExperimentMutationRequest): Promise<void>;
  listModels(): Promise<readonly ModelRecordResponse[]>;
  mutateModel(request: ModelMutationRequest): Promise<void>;
  listStrategyResolutions(): Promise<readonly StrategyResolutionResponse[]>;
  resolveStrategyWithBudgets(policy: "isolated_books" | "priority" | "weighted_net", budgets: Readonly<Record<string, number>>): Promise<{ readonly acceptedCount: number; readonly adjustmentCount: number }>;
  listStrategyExecutionSummaries(): Promise<readonly StrategyExecutionResponse[]>;
  listStrategies(): Promise<readonly StrategyRegistryResponse[]>;
  transitionStrategyLifecycle(strategyId: string, lifecycle: string, confirmation: string, evidenceRef: string): Promise<void>;
  transitionMetricLifecycle(metricId: string, lifecycle: string, confirmation: string, evidenceRef: string): Promise<void>;
  listMetrics(): Promise<readonly MetricRegistryResponse[]>;
  setTradingMode(mode: "manual" | "hybrid" | "autonomous"): Promise<void>;
  configureLiveLimits(accounts: readonly string[], maxNotionalTicks: number): Promise<TradingEnvironment>;
  armLive(account: string, phrase: string): Promise<{ readonly environment: TradingEnvironment; readonly token: string }>;
  confirmLive(account: string, token: string, phrase: string): Promise<TradingEnvironment>;
  killLive(): Promise<TradingEnvironment>;
  backupJournal(path: string): Promise<JournalBackupResponse>;
  restoreJournal(source: string, destination: string): Promise<JournalBackupResponse>;
  transitionRiskState(state: "running" | "reduce_only" | "cancel_only" | "halted", authorization: string): Promise<string>;
}

export type TradingEnvironment = "paper" | "live" | "killed";

export interface ConfigSnapshotResponse { readonly version: number; readonly cfg_text: string }
export interface ConfigReloadRequest { readonly expected_version: number; readonly cfg_text: string }

export interface JournalBackupResponse {
  readonly source: string;
  readonly destination: string;
  readonly byteLen: number;
  readonly sha256: string;
}

export type ExecutionScheduleRequest =
  | { readonly type: "immediate" }
  | { readonly type: "twap"; readonly slices: number; readonly intervalNs: number }
  | { readonly type: "vwap"; readonly weights: readonly number[] }
  | { readonly type: "pov"; readonly participationBps: number; readonly intervalNs: number; readonly marketVolumeTicks: readonly number[] }
  | { readonly type: "implementation_shortfall"; readonly slices: number; readonly intervalNs: number; readonly urgencyBps: number }
  | { readonly type: "adaptive"; readonly slices: number; readonly intervalNs: number; readonly urgencyBps: number; readonly spreadTicks: number; readonly maxSpreadTicks: number; readonly volatilityBps: number; readonly maxVolatilityBps: number; readonly marketVolumeTicks: readonly number[] };

export type BacktestEventRequest =
  | { readonly kind: "fill"; readonly sequence: number; readonly quantityTicks: number; readonly priceTicks: number; readonly feeTicks?: string }
  | { readonly kind: "mark"; readonly sequence: number; readonly priceTicks: number };

export interface BacktestRunRequest {
  readonly runId: string;
  readonly strategyId: string;
  readonly datasetHash: string;
  readonly configHash: string;
  readonly initialCashTicks: string;
  readonly events: readonly BacktestEventRequest[];
}

export interface StrategyBacktestMetricRequest {
  readonly metricId: string;
  readonly generatedMonoNs: number;
  readonly ttlNs: number;
  readonly score: number;
  readonly confidence: number;
  readonly uncertainty: number;
}

export interface StrategyBacktestEventRequest {
  readonly sequence: number;
  readonly nowMonoNs: number;
  readonly instrumentId: string;
  readonly priceTicks: number;
  readonly feeTicks: string;
  readonly metrics: readonly StrategyBacktestMetricRequest[];
}

export interface StrategyBacktestRunRequest {
  readonly runId: string;
  readonly strategyId: string;
  readonly datasetHash: string;
  readonly configHash: string;
  readonly initialCashTicks: string;
  readonly events: readonly StrategyBacktestEventRequest[];
}

export interface BacktestRunResponse {
  readonly runId: string;
  readonly strategyId: string;
  readonly datasetHash: string;
  readonly configHash: string;
  readonly eventCount: number;
  readonly maxDrawdownTicks: string;
  readonly totalFeesTicks: string;
  readonly finalEquityTicks?: string;
}

export interface ExperimentArtifactResponse {
  readonly kind: string;
  readonly hash: string;
  readonly path: string;
}

export type ExperimentMutationRequest =
  | { readonly operation: "create"; readonly run_id: string; readonly code_hash: string; readonly config_hash: string; readonly dataset_hash: string }
  | { readonly operation: "start" | "fail"; readonly run_id: string }
  | { readonly operation: "succeed"; readonly run_id: string; readonly metrics?: Readonly<Record<string, number>> }
  | { readonly operation: "artifact"; readonly run_id: string; readonly artifact: ExperimentArtifactResponse };

export interface ExperimentRunResponse {
  readonly run_id: string;
  readonly code_hash: string;
  readonly config_hash: string;
  readonly dataset_hash: string;
  readonly status: "created" | "running" | "succeeded" | "failed" | "cancelled";
  readonly metrics: Readonly<Record<string, number>>;
  readonly artifacts: readonly ExperimentArtifactResponse[];
  readonly provenance: ExperimentProvenanceResponse;
}

export interface ExperimentProvenanceResponse {
  readonly strategy_id?: string;
  readonly strategy_version?: string;
  readonly news_dataset_hash?: string;
  readonly news_clustering_version?: string;
  readonly graph_snapshot_version?: string;
  readonly llm_provider?: string;
  readonly llm_model?: string;
  readonly prompt_version?: string;
  readonly tool_schema_version?: string;
  readonly llm_cache_ids: readonly string[];
  readonly autonomy_config_hash?: string;
}

export interface ModelRecordResponse {
  readonly model_id: string;
  readonly version: string;
  readonly artifact_hash: string;
  readonly input_schema_hash: string;
  readonly output_schema_hash: string;
  readonly input_width: number;
  readonly status: "research" | "validated" | "shadow" | "canary" | "production" | "retired";
  readonly active: boolean;
}

export interface ModelMutationRequest {
  readonly operation: "register" | "validate" | "shadow" | "canary" | "promote";
  readonly model_id: string;
  readonly version: string;
  readonly evidence_id?: string;
  readonly artifact_hash?: string;
  readonly input_schema_hash?: string;
  readonly output_schema_hash?: string;
  readonly input_width?: number;
  readonly code_hash?: string;
  readonly training_data_hash?: string;
  readonly config_hash?: string;
  readonly feature_hash?: string;
  readonly calibration_hash?: string;
}

export interface StrategyResolutionResponse {
  readonly policy: string;
  readonly now_mono_ns: number;
  readonly accepted_count: number;
  readonly conflict_count: number;
  readonly expired_count: number;
  readonly attribution_count: number;
}

export interface StrategyExecutionResponse {
  readonly strategy_id: string;
  readonly fill_count: number;
  readonly filled_quantity_ticks: string;
  readonly notional_ticks: string;
}

export interface StrategyRegistryResponse {
  readonly strategy_id: string;
  readonly mode: string;
  readonly state: string;
  readonly lifecycle: string;
  readonly lifecycle_evidence_ref: string;
  readonly priority: string;
  readonly horizon_ns: number;
  readonly ttl_ns: number;
  readonly period_ns: number;
  readonly deadline_ns: number;
  readonly metric_ids: readonly string[];
  readonly dependencies: readonly string[];
}

export interface MetricRegistryResponse {
  metricId: string;
  state: string;
  lifecycle: string;
  priority: string;
  ttlNs: number;
  periodNs: number;
  deadlineNs: number;
  budgetNs: number;
  minScore: number | null;
  maxScore: number | null;
  inputs: readonly string[];
}

export interface InstrumentResolution {
  readonly instrumentId: string;
  readonly symbol: string;
  readonly venue: string;
  readonly assetClass: "equity" | "etf" | "option" | "future" | "fx" | "crypto";
}

export interface NewsDetailSnapshot {
  readonly current: NewsDetailVersion;
  readonly versions: readonly NewsDetailVersion[];
  readonly clusterId: string;
  readonly relatedItemIds: readonly string[];
}

export interface NewsProviderStatusSnapshot {
  readonly providerId: string;
  readonly health: "unknown" | "healthy" | "cooling_down" | "degraded" | "failed";
  readonly lastSuccessMs?: number;
  readonly lastFailureMs?: number;
  readonly nextRetryMs?: number;
  readonly deadLetterCount: number;
  readonly consecutiveFailures: number;
}

export interface SupervisorStatusSnapshot {
  readonly name: string;
  readonly state: "running" | "backoff" | "quarantined" | "draining";
  readonly health: "unknown" | "healthy" | "degraded" | "unavailable";
  readonly failures: number;
  readonly retryAtNs: number;
  readonly backoffNs: number;
}

export interface BrokerStatusSnapshot {
  readonly health: "unknown" | "healthy" | "degraded" | "unavailable";
  readonly orderCount: number;
  readonly positionCount: number;
  readonly accountValueCount: number;
}

export interface RiskPolicyRevisionSnapshot {
  readonly scope: "system" | "account" | "strategy" | "asset" | "instrument";
  readonly identity: string;
  readonly effectiveMonoNs: number;
  readonly maxPositionTicks: number;
  readonly maxOrderTicks: number;
  readonly maxGrossNotionalTicks: number | string;
}

export interface NewsDetailVersion extends NewsItemSnapshot {
  readonly provider: string;
  readonly summaryText?: string;
  readonly contentHash: string;
}

export interface AnalystRequest {
  readonly task: string;
  readonly input: string;
  readonly contextHash: string;
  readonly model: string;
  readonly promptVersion: string;
  readonly maxOutputTokens: number;
  readonly endpoint: "responses" | "chat_completions";
}

export interface AnalystResponse {
  readonly traceId: string;
  readonly finishReason: string;
  readonly content: string;
}

export interface AnalystStreamChunk {
  readonly traceId: string;
  readonly kind: "delta" | "done";
  readonly text: string;
}

export interface AutonomousActionResponse {
  readonly traceId: string;
  readonly actionType: string;
  readonly proposalId?: string;
  readonly scale?: number;
  readonly reasonCodes: readonly string[];
}

export interface AutonomousActionRequest {
  readonly actionType: string;
  readonly proposalId?: string;
  readonly scale?: number;
  readonly reasonCodes: readonly string[];
}

export interface AutonomousPlanRequest {
  readonly planId: string;
  readonly expiresAfterMs: number;
  readonly actions: readonly AutonomousActionRequest[];
}

export interface AutonomousPlanTransitionRequest {
  readonly planId: string;
  readonly state: "pending" | "approved" | "rejected" | "expired" | "executing" | "completed" | "failed";
}

export interface AlertSnapshot {
  readonly alertId: string;
  readonly dedupeKey: string;
  readonly source: string;
  readonly occurredMs: number;
  readonly severity: 1 | 2 | 3;
  readonly sensitive: boolean;
  readonly message: string;
}

export interface TraceEventSnapshot {
  readonly sequence: number;
  readonly kind: string;
  readonly payloadHex: string;
}

export interface TraceExportEventSnapshot {
  readonly sequence: number;
  readonly kind: string;
  readonly payloadBytes: number;
}

export interface StrategyEvaluateRequest {
  readonly strategyId: string;
  readonly metricId: string;
  readonly instrumentId: string;
  readonly metricTtlNs: number;
  readonly score: number;
  readonly confidence: number;
  readonly uncertainty: number;
  readonly entryThreshold: number;
  readonly exitThreshold: number;
  readonly quantityTicks: number;
  readonly horizonNs: number;
  readonly strategyTtlNs: number;
}

export interface StrategyProposalResponse {
  readonly proposalId: string;
  readonly strategyId: string;
  readonly instrumentId: string;
  readonly action: string;
  readonly quantityTicks: number;
  readonly weight: number;
  readonly confidence: number;
  readonly generatedMonoNs: number;
  readonly ttlNs: number;
}

export interface ContextSearchHit {
  readonly nodeId: string;
  readonly score: number;
  readonly exactScore: number;
  readonly lexicalScore: number;
  readonly vectorScore: number;
  readonly evidencePath: readonly string[];
}

function draftKey(draft: OrderDraft): string {
  const canonical = `${draft.instrumentId ?? ""}|${draft.symbol}|${draft.side}|${draft.type}|${draft.quantityTicks}|${draft.limitPriceTicks ?? ""}`;
  let hash = 2166136261;
  for (let index = 0; index < canonical.length; index += 1) {
    hash ^= canonical.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `manual-${(hash >>> 0).toString(16).padStart(8, "0")}`;
}

function sameDraft(left: OrderDraft, right: OrderDraft): boolean {
  return draftKey(left) === draftKey(right);
}

function parseInstrumentResolution(value: unknown, symbol: string): InstrumentResolution {
  const binary = parseBinaryInstrumentResolution(value, symbol);
  if (binary) return binary;
  if (!value || typeof value !== "object") throw new Error("runtime returned an invalid instrument resolution");
  const candidate = value as Record<string, unknown>;
  const instrumentId = candidate.instrumentId;
  const canonicalSymbol = candidate.symbol;
  const venue = candidate.venue;
  const assetClass = candidate.assetClass;
  if (typeof instrumentId !== "string" || !instrumentId.trim() || typeof canonicalSymbol !== "string" || typeof venue !== "string") {
    throw new Error(`instrument resolution failed for ${symbol}`);
  }
  if (!["equity", "etf", "option", "future", "fx", "crypto"].includes(String(assetClass))) {
    throw new Error("instrument resolution returned an unsupported asset class");
  }
  return { instrumentId, symbol: canonicalSymbol, venue, assetClass: assetClass as InstrumentResolution["assetClass"] };
}

const INSTRUMENT_MAGIC = new TextEncoder().encode("IT_RESOLVED_INSTRUMENT_V1\0");

function parseBinaryInstrumentResolution(value: unknown, requestedSymbol: string): InstrumentResolution | undefined {
  const bytes = binaryResponse(value);
  if (!bytes || !hasPrefix(bytes, INSTRUMENT_MAGIC)) return undefined;
  const body = new BinaryReader(bytes.subarray(INSTRUMENT_MAGIC.length));
  const instrumentId = body.u128String();
  const assetClass = ({ 1: "equity", 2: "etf", 3: "option", 4: "future", 5: "fx", 6: "crypto" } as const)[body.u8() as 1 | 2 | 3 | 4 | 5 | 6];
  const symbol = body.string();
  const venue = body.string();
  body.finish();
  if (!instrumentId || !assetClass || !symbol.trim() || !venue.trim()) throw new Error(`instrument resolution failed for ${requestedSymbol}`);
  return { instrumentId, symbol, venue, assetClass };
}

function parsePreview(value: unknown, draft: OrderDraft): OrderPreview {
  if (!value || typeof value !== "object") throw new Error("runtime returned an invalid order preview");
  const candidate = value as Record<string, unknown>;
  if (typeof candidate.previewId !== "string" || !candidate.previewId.trim()) throw new Error("preview ID is missing");
  if (!Number.isSafeInteger(candidate.expectedStateVersion) || (candidate.expectedStateVersion as number) < 0) throw new Error("preview state version is invalid");
  if (!Number.isSafeInteger(candidate.expiresAtMs) || (candidate.expiresAtMs as number) <= Date.now()) throw new Error("preview is already expired");
  const warnings = candidate.warnings === undefined ? [] : candidate.warnings;
  if (!Array.isArray(warnings) || warnings.some((warning) => typeof warning !== "string")) throw new Error("preview warnings are invalid");
  return {
    previewId: candidate.previewId,
    draft,
    expectedStateVersion: candidate.expectedStateVersion as number,
    expiresAtMs: candidate.expiresAtMs as number,
    estimatedNotionalTicks: typeof candidate.estimatedNotionalTicks === "number" ? candidate.estimatedNotionalTicks : undefined,
    estimatedCostBps: typeof candidate.estimatedCostBps === "number" ? candidate.estimatedCostBps : undefined,
    warnings: warnings as string[],
  };
}

function parseTradingEnvironment(value: unknown): TradingEnvironment {
  if (!value || typeof value !== "object") throw new Error("runtime returned an invalid trading environment");
  const environment = (value as Record<string, unknown>).environment;
  if (environment !== "paper" && environment !== "live" && environment !== "killed") {
    throw new Error("runtime returned an unsupported trading environment");
  }
  return environment;
}

const NEWS_PAGE_MAGIC_V1 = new TextEncoder().encode("IT_CMD_NEWS_PAGE_V1\0");
const NEWS_PAGE_MAGIC = new TextEncoder().encode("IT_CMD_NEWS_PAGE_V2\0");
const NEWS_DETAIL_MAGIC = new TextEncoder().encode("IT_CMD_NEWS_DETAIL_V1\0");
const NEWS_PROVIDER_STATUS_MAGIC = new TextEncoder().encode("IT_CMD_NEWS_PROVIDER_STATUS_RESPONSE_V1\0");
const RISK_POLICY_STATUS_MAGIC = new TextEncoder().encode("IT_CMD_RISK_POLICY_STATUS_RESPONSE_V1\0");

class BinaryReader {
  #bytes: Uint8Array;
  #offset = 0;
  readonly #decoder = new TextDecoder("utf-8", { fatal: true });

  constructor(value: Uint8Array) {
    this.#bytes = value;
  }

  #take(length: number): Uint8Array {
    if (!Number.isSafeInteger(length) || length < 0 || length > 16 * 1024 * 1024 || this.#offset + length > this.#bytes.length) {
      throw new Error("runtime binary response is truncated or exceeds bounds");
    }
    const result = this.#bytes.subarray(this.#offset, this.#offset + length);
    this.#offset += length;
    return result;
  }

  u16(): number {
    const bytes = this.#take(2);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint16(0, true);
  }

  u8(): number { return this.#take(1)[0] ?? 0; }

  u32(): number {
    const bytes = this.#take(4);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
  }

  u64(): number {
    const bytes = this.#take(8);
    const value = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(0, true);
    const number = Number(value);
    if (!Number.isSafeInteger(number)) throw new Error("runtime integer exceeds JavaScript safe range");
    return number;
  }

  f64(): number {
    const bytes = this.#take(8);
    return new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getFloat64(0, true);
  }

  i128Number(): number | string {
    const bytes = this.#take(16);
    let value = 0n;
    for (let index = bytes.length - 1; index >= 0; index -= 1) value = (value << 8n) | BigInt(bytes[index] ?? 0);
    if ((value & (1n << 127n)) !== 0n) value -= 1n << 128n;
    const number = Number(value);
    return Number.isSafeInteger(number) ? number : value.toString(10);
  }

  i64(): number {
    const bytes = this.#take(8);
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const value = view.getBigInt64(0, true);
    const number = Number(value);
    if (!Number.isSafeInteger(number)) throw new Error("runtime timestamp exceeds JavaScript safe range");
    return number;
  }

  u128String(): string {
    const bytes = this.#take(16);
    let value = 0n;
    for (let index = bytes.length - 1; index >= 0; index -= 1) value = (value << 8n) | BigInt(bytes[index] ?? 0);
    return value.toString(10);
  }

  string(): string {
    const value = this.#decoder.decode(this.#take(this.u16()));
    if (value.length > 1_048_576) throw new Error("runtime string exceeds bounds");
    return value;
  }

  bytes(length: number): Uint8Array {
    return this.#take(length);
  }

  finish(): void {
    if (this.#offset !== this.#bytes.length) throw new Error("runtime binary response has trailing bytes");
  }

}

const RUNTIME_SNAPSHOT_MAGIC_V2 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V2\0");
const RUNTIME_SNAPSHOT_MAGIC_V3 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V3\0");
const RUNTIME_SNAPSHOT_MAGIC_V4 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V4\0");
const RUNTIME_SNAPSHOT_MAGIC_V5 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V5\0");
const RUNTIME_SNAPSHOT_MAGIC_V6 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V6\0");
const RUNTIME_SNAPSHOT_MAGIC_V7 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V7\0");
const RUNTIME_SNAPSHOT_MAGIC_V8 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V8\0");
const RUNTIME_SNAPSHOT_MAGIC_V9 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V9\0");
const RUNTIME_SNAPSHOT_MAGIC_V10 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V10\0");
const RUNTIME_SNAPSHOT_MAGIC_V11 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V11\0");
const RUNTIME_SNAPSHOT_MAGIC_V12 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V12\0");
const RUNTIME_SNAPSHOT_MAGIC_V13 = new TextEncoder().encode("IT_RUNTIME_SNAPSHOT_V13\0");
const ORDER_INTENT_MAGIC = new TextEncoder().encode("IT_ORDER_INTENT_V1\0");

/** Decodes the bounded native snapshot wire format into the UI read model. */
function parseRuntimeSnapshot(value: unknown, current: RuntimeState): RuntimeSnapshot {
  const bytes = binaryResponse(value);
  const v13 = Boolean(bytes && hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13));
  const v12 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v11 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V11) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v10 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V10) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V11) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v9 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V9) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V10) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V11) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v8 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V8) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V9) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V10) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V11) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v7 = Boolean(bytes && (hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V7) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V8) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V9) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V10) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V11) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V12) || hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V13)));
  const v6 = Boolean(bytes && hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V6));
  const v5 = Boolean(bytes && hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V5));
  const v4 = Boolean(bytes && hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V4));
  const v3 = Boolean(bytes && hasPrefix(bytes, RUNTIME_SNAPSHOT_MAGIC_V3));
  const magic = v13 ? RUNTIME_SNAPSHOT_MAGIC_V13 : v12 ? RUNTIME_SNAPSHOT_MAGIC_V12 : v11 ? RUNTIME_SNAPSHOT_MAGIC_V11 : v10 ? RUNTIME_SNAPSHOT_MAGIC_V10 : v9 ? RUNTIME_SNAPSHOT_MAGIC_V9 : v8 ? RUNTIME_SNAPSHOT_MAGIC_V8 : v7 ? RUNTIME_SNAPSHOT_MAGIC_V7 : v6 ? RUNTIME_SNAPSHOT_MAGIC_V6 : v5 ? RUNTIME_SNAPSHOT_MAGIC_V5 : v4 ? RUNTIME_SNAPSHOT_MAGIC_V4 : v3 ? RUNTIME_SNAPSHOT_MAGIC_V3 : RUNTIME_SNAPSHOT_MAGIC_V2;
  if (!bytes || !hasPrefix(bytes, magic)) {
    if (!value || typeof value !== "object") throw new Error("runtime returned an invalid snapshot");
    const candidate = value as Record<string, unknown>;
    if (!Number.isSafeInteger(candidate.cursor) || !candidate.state || typeof candidate.state !== "object") {
      throw new Error("runtime returned a malformed snapshot");
    }
    return value as RuntimeSnapshot;
  }
  const body = new BinaryReader(bytes.subarray(magic.length));
  body.u128String(); // account identity is authoritative on the engine side.
  const cursor = body.u64();
  const riskCode = body.u8();
  const riskState = ({ 1: "running", 2: "reduce_only", 3: "cancel_only", 4: "halted" } as const)[riskCode as 1 | 2 | 3 | 4];
  if (!riskState) throw new Error("runtime returned an invalid risk state");
  const modeCode = v8 ? body.u8() : undefined;
  const autonomyMode = modeCode === undefined ? current.autonomy.mode : ({ 1: "manual", 2: "hybrid", 3: "autonomous" } as const)[modeCode as 1 | 2 | 3];
  if (!autonomyMode) throw new Error("runtime returned an invalid autonomy mode");
  let autonomyPlanId: string | undefined;
  let autonomyPlanState: RuntimeState["autonomy"]["planState"];
  let autonomyPlanExpiresMonoNs: number | undefined;
  if (v9) {
    const hasPlan = body.u8();
    if (hasPlan === 1) {
      autonomyPlanId = body.string();
      const planStateCode = body.u8();
      autonomyPlanState = ({ 1: "pending", 2: "approved", 3: "rejected", 4: "expired", 5: "executing", 6: "completed", 7: "failed" } as const)[planStateCode as 1 | 2 | 3 | 4 | 5 | 6 | 7];
      if (!autonomyPlanId.trim() || !autonomyPlanState) throw new Error("runtime returned an invalid autonomous plan");
      autonomyPlanExpiresMonoNs = body.u64();
    } else if (hasPlan !== 0) {
      throw new Error("runtime returned an invalid autonomous plan marker");
    }
  }
  const cashTicks = body.i64();
  const realizedPnlTicks = body.i64();
  const feesTicks = body.i64();
  const grossNotionalTicks = body.i128Number();
  const maxGrossNotionalTicks = body.i128Number();
  const grossUtilizationBps = v7 ? body.i64() : 0;
  const largestPositionNotionalTicks = v7 ? body.i128Number() : 0;
  const hasDrawdown = body.u8();
  const drawdownBps = body.i64();
  if (hasDrawdown > 1) throw new Error("runtime returned an invalid drawdown marker");
  const positionCount = body.u32();
  if (positionCount > 16_384) throw new Error("runtime returned too many positions");
  const positions = Array.from({ length: positionCount }, () => {
    const instrument = body.u128String();
    const quantityTicks = body.i64();
    const markTicks = body.i64();
    const averageCostTicks = v6 ? body.i64() : markTicks;
    const pnlTicks = (markTicks - averageCostTicks) * quantityTicks;
    if (!Number.isSafeInteger(pnlTicks)) throw new Error("runtime returned an unsafe position P&L");
    return {
      symbol: instrument,
      quantityTicks,
      markTicks,
      averageCostTicks,
      pnlTicks,
    };
  });
  const orderCount = body.u32();
  if (orderCount > 16_384) throw new Error("runtime returned too many orders");
  const orders: OrderSnapshot[] = [];
  for (let index = 0; index < orderCount; index += 1) {
    const intentLength = body.u32();
    const intent = parseOrderIntent(body.bytes(intentLength));
    const brokerOrderId = body.string();
    const filledQuantityTicks = body.i64();
    const stateCode = body.u8();
    const state = ({ 1: "risk_approved", 2: "queued", 3: "sending", 4: "sent", 5: "acknowledged", 6: "partially_filled", 7: "filled", 8: "rejected", 9: "cancel_pending", 10: "cancelled", 11: "replace_pending", 12: "unknown", 13: "created", 14: "expired" } as const)[stateCode as 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14];
    if (!state) throw new Error("runtime returned an invalid order state");
    orders.push({
      ...intent,
      ...(brokerOrderId ? { brokerOrderId } : {}),
      filledQuantityTicks,
      state,
    });
  }
  const fillCount = body.u32();
  if (fillCount > 10_000) throw new Error("runtime returned too many fills");
  for (let index = 0; index < fillCount; index += 1) {
    body.string();
    body.u128String();
    body.i64();
    body.i64();
  }
  const tca: TcaSnapshot[] = [];
  if (v10) {
    const tcaCount = body.u32();
    if (tcaCount > 16_384) throw new Error("runtime returned too many TCA records");
    for (let index = 0; index < tcaCount; index += 1) {
      const clientOrderId = body.string();
      const filledQuantityTicks = body.i64();
      const notionalTicks = body.i128Number();
      const averageFillPriceNumerator = body.i128Number();
      const averageFillPriceDenominator = body.i64();
      const readOptionalI64 = (): number | undefined => {
        const marker = body.u8();
        const value = body.i64();
        if (marker > 1) throw new Error("runtime returned an invalid TCA marker");
        return marker === 1 ? value : undefined;
      };
      const readOptionalU64 = (): number | undefined => {
        const marker = body.u8();
        const value = body.u64();
        if (marker > 1) throw new Error("runtime returned an invalid TCA marker");
        return marker === 1 ? value : undefined;
      };
      const arrivalPriceTicks = readOptionalI64();
      const decisionMonoNs = readOptionalU64();
      const sendMonoNs = readOptionalU64();
      const ackMonoNs = readOptionalU64();
      const firstFillMonoNs = readOptionalU64();
      const shortfallMarker = body.u8();
      if (shortfallMarker > 1) throw new Error("runtime returned an invalid TCA marker");
      const implementationShortfallTickValue = shortfallMarker === 1 ? body.i128Number() : undefined;
      let averageSpreadTicks: number | undefined;
      let adverseSelectionTickValue: number | string | undefined;
      if (v11) {
        const spreadMarker = body.u8();
        averageSpreadTicks = spreadMarker === 1 ? body.i64() : undefined;
        const adverseMarker = body.u8();
        adverseSelectionTickValue = adverseMarker === 1 ? body.i128Number() : undefined;
        if (spreadMarker > 1 || adverseMarker > 1) throw new Error("runtime returned an invalid TCA marker");
      }
      if (!clientOrderId.trim() || filledQuantityTicks <= 0 || averageFillPriceDenominator <= 0) {
        throw new Error("runtime returned malformed TCA record");
      }
      tca.push({
        clientOrderId,
        filledQuantityTicks,
        notionalTicks,
        averageFillPriceNumerator,
        averageFillPriceDenominator,
        ...(arrivalPriceTicks === undefined ? {} : { arrivalPriceTicks }),
        ...(decisionMonoNs === undefined ? {} : { decisionMonoNs }),
        ...(sendMonoNs === undefined ? {} : { sendMonoNs }),
        ...(ackMonoNs === undefined ? {} : { ackMonoNs }),
        ...(firstFillMonoNs === undefined ? {} : { firstFillMonoNs }),
        ...(implementationShortfallTickValue === undefined ? {} : { implementationShortfallTickValue }),
        ...(averageSpreadTicks === undefined ? {} : { averageSpreadTicks }),
        ...(adverseSelectionTickValue === undefined ? {} : { adverseSelectionTickValue }),
      });
    }
  }
  const proposals: ProposalSnapshot[] = [];
  if (v3 || v4) {
    const proposalCount = body.u32();
    if (proposalCount > 4_096) throw new Error("runtime returned too many proposals");
    for (let index = 0; index < proposalCount; index += 1) {
      const proposalId = body.u128String();
      const instrument = body.u128String();
      const strategyId = body.string();
      const actionCode = body.u8();
      let action: ProposalSnapshot["action"];
      let quantityTicks: number | undefined;
      if (actionCode === 0) action = "no_action";
      else if (actionCode === 1) {
        action = "target";
        quantityTicks = body.i64();
      } else if (actionCode === 2) {
        action = "target";
        body.f64(); // target weight is rendered through the strategy inspector.
      } else if (actionCode === 3) {
        action = "increase";
        quantityTicks = body.i64();
      } else if (actionCode === 4) {
        action = "decrease";
        quantityTicks = body.i64();
      } else if (actionCode === 5) action = "close";
      else throw new Error("runtime returned an invalid proposal action");
      const confidence = body.f64();
      const generated = body.u64();
      const ttl = body.u64();
      const lifecycle = body.u8();
      if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1 || lifecycle < 1 || lifecycle > 5) {
        throw new Error("runtime returned an invalid proposal confidence/state");
      }
      const expiresAtMs = Date.now() + Math.min(Math.floor(ttl / 1_000_000), 86_400_000);
      proposals.push({
        proposalId,
        strategyId,
        symbol: instrument,
        action,
        ...(quantityTicks === undefined ? {} : { quantityTicks }),
        confidence,
        expiresAtMs,
        rationaleCodes: Object.freeze([`coordinator_state:${lifecycle}`, `generated_mono:${generated}`]),
      });
    }
  }
  const quotes = { ...current.quotes };
  const tradesBySymbol: Record<string, readonly TradePrintSnapshot[]> = { ...current.tradesBySymbol };
  const chartCandles: Candle[] = [];
  if (v4 || v5 || v6 || v7) {
    const marketCount = body.u32();
    if (marketCount > 4_096) throw new Error("runtime returned too many market states");
    for (let index = 0; index < marketCount; index += 1) {
      const symbol = body.u128String();
      const hasQuote = body.u8();
      let quote: QuoteSnapshot | undefined;
      if (hasQuote === 1) {
        const sequence = body.u64();
        body.u64(); // monotonic receive timestamp has no wall-clock conversion.
        const bidTicks = body.i64();
        const askTicks = body.i64();
        const bidQuantityTicks = body.i64();
        const askQuantityTicks = body.i64();
        quote = {
          symbol,
          bidTicks,
          askTicks,
          lastTicks: Math.floor((bidTicks + askTicks) / 2),
          sequence,
          receivedAtMs: Date.now(),
          ...(v12 ? { bidQuantityTicks, askQuantityTicks } : {}),
        };
      } else if (hasQuote !== 0) throw new Error("runtime returned an invalid quote marker");
      const hasTrade = body.u8();
      if (hasTrade === 1) {
        const tradeSequence = body.u64();
        const tradePrice = body.i64();
        if (quote && tradeSequence >= quote.sequence) quote = { ...quote, lastTicks: tradePrice, sequence: tradeSequence };
      } else if (hasTrade !== 0) throw new Error("runtime returned an invalid trade marker");
      body.u8();
      body.u8();
      body.u8();
      if (v12) {
        const bookMarker = body.u8();
        if (bookMarker > 1) throw new Error("runtime returned an invalid book marker");
        if (bookMarker === 1) {
          const bookTop: [number, number, number, number] = [body.i64(), body.i64(), body.i64(), body.i64()];
          if (quote) quote = { ...quote, bookTop };
        }
      }
      if (v13) {
        const tradeHistoryCount = body.u16();
        if (tradeHistoryCount > 512) throw new Error("runtime returned too many trade prints");
        const trades: TradePrintSnapshot[] = [];
        for (let tradeIndex = 0; tradeIndex < tradeHistoryCount; tradeIndex += 1) {
          trades.push({
            sequence: body.u64(),
            exchangeTimeNs: body.i64(),
            receivedMonoNs: body.u64(),
            priceTicks: body.i64(),
            quantityTicks: body.i64(),
          });
        }
        tradesBySymbol[symbol] = Object.freeze(trades);
      }
      if (v5 || v6 || v7) {
        const barCount = body.u32();
        if (barCount > 4_096) throw new Error("runtime returned too many bars");
        for (let barIndex = 0; barIndex < barCount; barIndex += 1) {
          const startNs = body.i64();
          body.u64(); // interval is retained by the engine; chart uses start time.
          const openTicks = body.i64();
          const highTicks = body.i64();
          const lowTicks = body.i64();
          const closeTicks = body.i64();
          const volumeTicks = body.i64();
          if (symbol === current.selectedSymbol && chartCandles.length < 20_000) {
            chartCandles.push({
              timeMs: Math.max(0, Math.floor(startNs / 1_000_000)),
              openTicks,
              highTicks,
              lowTicks,
              closeTicks,
              volumeTicks,
              sequence: barIndex + 1,
            });
          }
        }
      }
      if (quote) quotes[symbol] = quote;
    }
  }
  body.finish();
  const state: RuntimeState = {
    ...current,
    positions: Object.freeze(positions),
    orders: Object.freeze(orders),
    tca: Object.freeze(tca),
    quotes: Object.freeze(quotes),
    tradesBySymbol: Object.freeze(tradesBySymbol),
    proposals: Object.freeze(proposals),
    risk: {
      ...current.risk,
      state: riskState,
      grossNotionalTicks,
      maxGrossNotionalTicks,
      grossUtilizationBps,
      largestPositionNotionalTicks,
      ...(hasDrawdown === 1 ? { drawdownBps } : {}),
    },
    // These values are retained in the typed runtime projection for future
    // panels; current risk limits remain the configured server snapshot.
    autonomy: {
      ...current.autonomy,
      mode: autonomyMode,
      ...(v9 ? { planId: autonomyPlanId, planState: autonomyPlanState, planExpiresMonoNs: autonomyPlanExpiresMonoNs } : {}),
    },
    chart: chartCandles.length > 0
      ? { ...current.chart, candles: Object.freeze(chartCandles), lastSequence: chartCandles.length, requiresRecovery: false }
      : current.chart,
  };
  void cashTicks;
  void realizedPnlTicks;
  void feesTicks;
  return { cursor, state };
}

function parseOrderIntent(value: Uint8Array): Pick<OrderSnapshot, "instrumentId" | "side" | "quantityTicks" | "clientOrderId"> {
  if (!hasPrefix(value, ORDER_INTENT_MAGIC)) throw new Error("runtime returned an invalid order intent");
  const body = new BinaryReader(value.subarray(ORDER_INTENT_MAGIC.length));
  body.u128String(); // account identity is not rendered in the order panel.
  const instrumentId = body.u128String();
  const sideCode = body.u8();
  const side = sideCode === 1 ? "buy" : sideCode === 2 ? "sell" : undefined;
  const quantityTicks = body.i64();
  body.u8(); // order type
  body.i64(); // optional limit price
  body.u8(); // time in force
  body.u128String(); // trace ID
  body.string(); // intent ID
  const clientOrderId = body.string();
  body.finish();
  if (!side || quantityTicks <= 0 || !clientOrderId.trim()) throw new Error("runtime returned malformed order intent");
  return { instrumentId, side, quantityTicks, clientOrderId };
}

function binaryResponse(value: unknown): Uint8Array | undefined {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (Array.isArray(value) && value.length <= 16 * 1024 * 1024 && value.every((item) => Number.isInteger(item) && item >= 0 && item <= 255)) {
    return Uint8Array.from(value);
  }
  return undefined;
}

function hasPrefix(bytes: Uint8Array, prefix: Uint8Array): boolean {
  return bytes.length >= prefix.length && prefix.every((value, index) => bytes[index] === value);
}

function parseBinaryNewsPage(value: unknown): NewsPageSnapshot | undefined {
  const bytes = binaryResponse(value);
  if (!bytes || (!hasPrefix(bytes, NEWS_PAGE_MAGIC) && !hasPrefix(bytes, NEWS_PAGE_MAGIC_V1))) return undefined;
  const version = hasPrefix(bytes, NEWS_PAGE_MAGIC) ? 2 : 1;
  // Decode from the payload boundary after the fixed magic.
  const payload = bytes.subarray((version === 2 ? NEWS_PAGE_MAGIC : NEWS_PAGE_MAGIC_V1).length);
  const body = new BinaryReader(payload);
  const count = body.u32();
  if (count > 500) throw new Error("runtime returned too many news items");
  const items: NewsItemSnapshot[] = [];
  for (let index = 0; index < count; index += 1) {
    const id = body.string();
    const title = body.string();
    const source = body.string();
    const canonicalUrl = body.string();
    const publishedAtMs = body.i64();
    const receivedAtMs = body.i64();
    const symbolCount = body.u16();
    if (!id.trim() || !title.trim() || !source.trim() || !canonicalUrl.trim() || symbolCount > 1_024 || receivedAtMs < 0) {
      throw new Error("runtime returned malformed news item");
    }
    const symbols = Array.from({ length: symbolCount }, () => body.string());
    const relevanceScore = version === 2 ? body.u16() / 10_000 : 0;
    items.push({
      id,
      title,
      source,
      canonicalUrl,
      receivedAtMs,
      ...(publishedAtMs === 0 ? {} : { publishedAtMs }),
      symbols: Object.freeze(symbols),
      relevanceScore,
    });
  }
  const nextCursor = body.string();
  body.finish();
  return { items, ...(nextCursor ? { nextCursor } : {}) };
}

function parseNewsProviderStatuses(value: unknown): readonly NewsProviderStatusSnapshot[] {
  const bytes = binaryResponse(value);
  if (!bytes || !hasPrefix(bytes, NEWS_PROVIDER_STATUS_MAGIC)) {
    throw new Error("runtime returned an invalid provider status response");
  }
  const body = new BinaryReader(bytes.subarray(NEWS_PROVIDER_STATUS_MAGIC.length));
  const count = body.u16();
  if (count > 1_024) throw new Error("runtime returned too many provider statuses");
  const health = ["unknown", "healthy", "cooling_down", "degraded", "failed"] as const;
  const statuses: NewsProviderStatusSnapshot[] = [];
  for (let index = 0; index < count; index += 1) {
    const providerId = body.string();
    const healthCode = body.u8();
    if (!providerId.trim() || healthCode >= health.length) {
      throw new Error("runtime returned malformed provider status");
    }
    const optionalTimestamp = (): number | undefined => {
      const present = body.u8();
      const timestamp = body.i64();
      if (present > 1) throw new Error("runtime returned malformed provider timestamp");
      return present === 1 ? timestamp : undefined;
    };
    const lastSuccessMs = optionalTimestamp();
    const lastFailureMs = optionalTimestamp();
    const nextRetryMs = optionalTimestamp();
    const deadLetterCount = body.u64();
    const consecutiveFailures = body.u32();
    statuses.push({
      providerId,
      health: health[healthCode] ?? "unknown",
      ...(lastSuccessMs === undefined ? {} : { lastSuccessMs }),
      ...(lastFailureMs === undefined ? {} : { lastFailureMs }),
      ...(nextRetryMs === undefined ? {} : { nextRetryMs }),
      deadLetterCount,
      consecutiveFailures,
    });
  }
  body.finish();
  return Object.freeze(statuses);
}

function parseSupervisorStatuses(value: unknown): readonly SupervisorStatusSnapshot[] {
  const bytes = binaryResponse(value);
  const magic = new TextEncoder().encode("IT_CMD_SUPERVISOR_STATUS_RESPONSE_V1\0");
  if (!bytes || !hasPrefix(bytes, magic)) throw new Error("runtime returned an invalid supervisor status response");
  const body = new BinaryReader(bytes.subarray(magic.length));
  const count = body.u16();
  if (count > 128) throw new Error("runtime returned too many supervisor components");
  const states = ["running", "backoff", "quarantined", "draining"] as const;
  const health = ["unknown", "healthy", "degraded", "unavailable"] as const;
  const statuses: SupervisorStatusSnapshot[] = [];
  for (let index = 0; index < count; index += 1) {
    const name = body.string();
    const stateCode = body.u8();
    const healthCode = body.u8();
    if (!name.trim() || stateCode >= states.length || healthCode >= health.length) throw new Error("runtime returned malformed supervisor status");
    statuses.push({ name, state: states[stateCode], health: health[healthCode], failures: body.u32(), retryAtNs: body.u64(), backoffNs: body.u64() });
  }
  body.finish();
  return statuses;
}

function parseBrokerStatus(value: unknown): BrokerStatusSnapshot {
  const bytes = binaryResponse(value);
  const magic = new TextEncoder().encode("IT_CMD_BROKER_STATUS_RESPONSE_V1\0");
  if (!bytes || !hasPrefix(bytes, magic)) throw new Error("runtime returned an invalid broker status response");
  const body = new BinaryReader(bytes.subarray(magic.length));
  const health = ["unknown", "healthy", "degraded", "unavailable"] as const;
  const healthCode = body.u8();
  if (healthCode >= health.length) throw new Error("runtime returned malformed broker health");
  const result = { health: health[healthCode], orderCount: body.u32(), positionCount: body.u32(), accountValueCount: body.u32() };
  body.finish();
  return result;
}

function parseRiskPolicyStatus(value: unknown): readonly RiskPolicyRevisionSnapshot[] {
  const bytes = binaryResponse(value);
  if (!bytes || !hasPrefix(bytes, RISK_POLICY_STATUS_MAGIC)) throw new Error("runtime returned an invalid risk policy response");
  const body = new BinaryReader(bytes.subarray(RISK_POLICY_STATUS_MAGIC.length));
  const count = body.u16();
  if (count > 1_024) throw new Error("runtime returned too many risk policy revisions");
  const scopes = ["system", "account", "strategy", "asset", "instrument"] as const;
  const revisions: RiskPolicyRevisionSnapshot[] = [];
  for (let index = 0; index < count; index += 1) {
    const scope = body.string();
    const identity = body.string();
    if (!scopes.includes(scope as (typeof scopes)[number])) throw new Error("runtime returned malformed risk policy scope");
    revisions.push({
      scope: scope as RiskPolicyRevisionSnapshot["scope"],
      identity,
      effectiveMonoNs: body.u64(),
      maxPositionTicks: body.i64(),
      maxOrderTicks: body.i64(),
      maxGrossNotionalTicks: body.i128Number(),
    });
  }
  body.finish();
  return Object.freeze(revisions);
}

function parseNewsPage(value: unknown): NewsPageSnapshot {
  if (!value || typeof value !== "object") throw new Error("runtime returned an invalid news page");
  const binary = parseBinaryNewsPage(value);
  if (binary) return binary;
  if (!value || typeof value !== "object") throw new Error("runtime returned an invalid news page");
  const candidate = value as Record<string, unknown>;
  if (!Array.isArray(candidate.items)) throw new Error("runtime returned invalid news items");
  const items = candidate.items.map((raw) => {
    if (!raw || typeof raw !== "object") throw new Error("runtime returned an invalid news item");
    const item = raw as Record<string, unknown>;
    const id = item.id;
    const title = item.title;
    const source = item.source ?? item.sourceName;
    const canonicalUrl = item.canonicalUrl ?? item.canonical_url;
    const receivedAtMs = item.receivedAtMs ?? item.received_at_ms;
    const publishedAtMs = item.publishedAtMs ?? item.published_at_ms;
    const symbols = item.symbols;
    if (typeof id !== "string" || !id.trim() || typeof title !== "string" || typeof source !== "string"
      || typeof canonicalUrl !== "string" || !Number.isSafeInteger(receivedAtMs)
      || (publishedAtMs !== undefined && !Number.isSafeInteger(publishedAtMs))
      || !Array.isArray(symbols) || symbols.some((symbol) => typeof symbol !== "string")) {
      throw new Error("runtime returned malformed news item");
    }
    const relevanceScore = item.relevanceScore ?? item.relevance_score ?? 0;
    if (typeof relevanceScore !== "number" || !Number.isFinite(relevanceScore)) throw new Error("news relevance is invalid");
    return {
      id,
      title,
      source,
      canonicalUrl,
      receivedAtMs,
      ...(publishedAtMs === undefined ? {} : { publishedAtMs }),
      symbols: Object.freeze([...symbols] as string[]),
      relevanceScore,
      ...(typeof item.clusterId === "string" ? { clusterId: item.clusterId } : {}),
    };
  });
  const nextCursor = candidate.nextCursor ?? candidate.next_cursor;
  if (nextCursor !== undefined && typeof nextCursor !== "string") throw new Error("runtime returned an invalid news cursor");
  return { items, ...(nextCursor === undefined || nextCursor === "" ? {} : { nextCursor }) };
}

function parseNewsDetail(value: unknown): NewsDetailSnapshot | undefined {
  const bytes = binaryResponse(value);
  if (!bytes || !hasPrefix(bytes, NEWS_DETAIL_MAGIC)) {
    if (value === undefined || value === null) return undefined;
    throw new Error("runtime returned an invalid news detail");
  }
  const body = new BinaryReader(bytes.subarray(NEWS_DETAIL_MAGIC.length));
  const present = body.u8();
  if (present > 1) throw new Error("runtime returned an invalid news detail marker");
  if (present === 0) {
    body.finish();
    return undefined;
  }
  const decodeVersion = (): NewsDetailVersion => {
    const id = body.string();
    const provider = body.string();
    const canonicalUrl = body.string();
    const source = body.string();
    const title = body.string();
    const hasSummary = body.u8();
    const summaryText = body.string();
    const hasPublished = body.u8();
    const publishedAtMs = body.i64();
    const receivedAtMs = body.i64();
    const symbolCount = body.u16();
    if (!id.trim() || !provider.trim() || !canonicalUrl.trim() || !source.trim() || !title.trim()
      || hasSummary > 1 || hasPublished > 1 || symbolCount > 1_024 || receivedAtMs < 0) {
      throw new Error("runtime returned malformed news detail version");
    }
    const symbols = Array.from({ length: symbolCount }, () => body.string());
    const contentHash = body.string();
    if (!contentHash.trim()) throw new Error("runtime returned an empty news content hash");
    return {
      id,
      provider,
      title,
      source,
      canonicalUrl,
      receivedAtMs,
      symbols: Object.freeze(symbols),
      relevanceScore: 0,
      contentHash,
      ...(hasSummary === 1 ? { summaryText } : {}),
      ...(hasPublished === 1 ? { publishedAtMs } : {}),
    };
  };
  const current = decodeVersion();
  const versionCount = body.u16();
  if (versionCount > 32) throw new Error("runtime returned too many news versions");
  const versions = Array.from({ length: versionCount }, decodeVersion);
  const clusterId = body.string();
  const relatedCount = body.u16();
  if (!clusterId.trim() || relatedCount > 1_024) throw new Error("runtime returned invalid news cluster");
  const relatedItemIds = Array.from({ length: relatedCount }, () => body.string());
  body.finish();
  return { current, versions: Object.freeze(versions), clusterId, relatedItemIds: Object.freeze(relatedItemIds) };
}

function validateLiveAccount(account: string): string {
  const normalized = account.trim();
  if (!normalized || normalized.length > 128 || /[\u0000-\u001f]/.test(normalized)) throw new Error("live account is invalid");
  return normalized;
}

/** Reconnecting snapshot/cursor session for authoritative runtime state. */
export class RuntimeSession {
  #stops: Array<() => void> = [];
  #connecting = false;
  #pollTimer: ReturnType<typeof setInterval> | undefined;
  #polling = false;

  constructor(private readonly bridge: RuntimeBridge, private readonly store: RuntimeStore) {}

  /** Connects subscriptions and obtains a gap-free snapshot. */
  async connect(): Promise<void> {
    if (this.#connecting) return;
    this.#connecting = true;
    this.store.setConnection("connecting");
    try {
      await this.refreshSnapshot();
      // The native Tauri shell currently exposes an RPC command boundary, not
      // a broker-event fanout. Polling the authoritative cursor/snapshot keeps
      // the workstation live without fabricating deltas; a future event bridge
      // can replace this timer without changing store semantics.
      this.#pollTimer = setInterval(() => {
        void this.refreshSnapshot();
      }, 1_000);
    } catch (error) {
      for (const stop of this.#stops.splice(0)) stop();
      this.store.setConnection("degraded");
      throw error;
    } finally {
      this.#connecting = false;
    }
  }

  /** Removes event listeners and marks the UI disconnected. */
  disconnect(): void {
    if (this.#pollTimer !== undefined) {
      clearInterval(this.#pollTimer);
      this.#pollTimer = undefined;
    }
    for (const stop of this.#stops.splice(0)) stop();
    this.store.setConnection("disconnected");
  }

  /** Reconnects after a transport interruption. */
  async reconnect(): Promise<void> {
    this.disconnect();
    await this.connect();
  }

  private async refreshSnapshot(): Promise<void> {
    if (this.#polling) return;
    this.#polling = true;
    try {
      const rawSnapshot = await this.bridge.invoke<unknown>("get_runtime_snapshot", {
        cursor: this.store.state.cursor,
      });
      const snapshot = parseRuntimeSnapshot(rawSnapshot, this.store.state);
      this.store.applySnapshot(snapshot);
      if (this.store.state.connection !== "stale") this.store.setConnection("ready");
    } catch {
      this.store.setConnection("degraded");
    } finally {
      this.#polling = false;
    }
  }
}

/** Typed command/event adapter; credentials never enter browser-managed state. */
export function createTradingCommands(bridge: RuntimeBridge, store: RuntimeStore): TradingCommands {
  return {
    loadNewsPage: async (scope, symbol, afterCursor) => {
      const normalized = symbol.trim().toUpperCase();
      if (!/^[A-Z0-9.\-]{1,16}$/.test(normalized)) throw new Error("symbol is invalid");
      const value = await bridge.invoke<unknown>("get_news_page", {
        scope,
        symbol: normalized,
        afterCursor,
        limit: 100,
      });
      const page = parseNewsPage(value);
      store.applyNewsPage(page, !afterCursor);
      return page;
    },
    getNewsProviderStatuses: async () =>
      parseNewsProviderStatuses(await bridge.invoke("get_news_provider_status")),
    getSupervisorStatuses: async () =>
      parseSupervisorStatuses(await bridge.invoke("get_supervisor_status")),
    getBrokerStatus: async () => parseBrokerStatus(await bridge.invoke("get_broker_status")),
    getRiskPolicyStatus: async () =>
      parseRiskPolicyStatus(await bridge.invoke("get_risk_policy_status")),
    getNewsDetail: async (itemId) => {
      if (!itemId.trim() || itemId.length > 256) throw new Error("news item ID is invalid");
      return parseNewsDetail(await bridge.invoke("get_news_detail", { itemId }));
    },
    searchContext: async (text, graphRoot, maxDepth = 3, limit = 50, embedding) => {
      const normalized = text.trim();
      if (!normalized || normalized.length > 16_384 || !Number.isSafeInteger(maxDepth) || maxDepth < 0 || maxDepth > 8
        || !Number.isSafeInteger(limit) || limit <= 0 || limit > 256) throw new Error("context search request is invalid");
      const value = await bridge.invoke<unknown>("search_context", {
        text: normalized,
        graphRoot,
        maxDepth,
        limit,
        embedding,
      });
      if (!Array.isArray(value)) throw new Error("context search response is invalid");
      return Object.freeze(value.map((raw) => {
        if (!raw || typeof raw !== "object") throw new Error("context hit is invalid");
        const hit = raw as Record<string, unknown>;
        const nodeId = hit.nodeId;
        const score = hit.score;
        const exactScore = hit.exactScore;
        const lexicalScore = hit.lexicalScore;
        const vectorScore = hit.vectorScore;
        const evidencePath = hit.evidencePath;
        if (typeof nodeId !== "string" || !nodeId.trim() || typeof score !== "number" || !Number.isFinite(score)
          || typeof exactScore !== "number" || typeof lexicalScore !== "number" || typeof vectorScore !== "number"
          || !Array.isArray(evidencePath) || evidencePath.some((item) => typeof item !== "string")) throw new Error("context hit is malformed");
        return { nodeId, score, exactScore, lexicalScore, vectorScore, evidencePath: Object.freeze([...evidencePath] as string[]) };
      }));
    },
    analyze: async (request) => {
      if (!request.task.trim() || request.task.length > 128 || !request.input.trim() || request.input.length > 1_048_576
        || !request.contextHash.trim() || request.contextHash.length > 256 || !request.model.trim() || request.model.length > 256
        || !request.promptVersion.trim() || request.promptVersion.length > 256
        || !Number.isSafeInteger(request.maxOutputTokens) || request.maxOutputTokens <= 0 || request.maxOutputTokens > 16_384) {
        throw new Error("analyst request exceeds bounds");
      }
      const value = await bridge.invoke<unknown>("analyze", { ...request, contextHash: request.contextHash });
      if (!value || typeof value !== "object") throw new Error("analyst response is invalid");
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.traceId !== "string" || !candidate.traceId.trim()
        || typeof candidate.finishReason !== "string" || typeof candidate.content !== "string" || !candidate.content.trim()) {
        throw new Error("analyst response is malformed");
      }
      return { traceId: candidate.traceId, finishReason: candidate.finishReason, content: candidate.content };
    },
    analyzeStream: async (request) => {
      if (!request.task.trim() || request.task.length > 128 || !request.input.trim() || request.input.length > 1_048_576
        || !request.contextHash.trim() || request.contextHash.length > 256 || !request.model.trim() || request.model.length > 256
        || !request.promptVersion.trim() || request.promptVersion.length > 256
        || !Number.isSafeInteger(request.maxOutputTokens) || request.maxOutputTokens <= 0 || request.maxOutputTokens > 16_384) {
        throw new Error("analyst request exceeds bounds");
      }
      const value = await bridge.invoke<unknown>("analyze_stream", request);
      if (!Array.isArray(value) || value.length === 0 || value.length > 4_096) throw new Error("analyst stream is invalid");
      return Object.freeze(value.map((raw) => {
        if (!raw || typeof raw !== "object") throw new Error("analyst stream chunk is invalid");
        const candidate = raw as Record<string, unknown>;
        if (typeof candidate.traceId !== "string" || !candidate.traceId.trim()
          || (candidate.kind !== "delta" && candidate.kind !== "done") || typeof candidate.text !== "string") {
          throw new Error("analyst stream chunk is malformed");
        }
        return Object.freeze({
          traceId: candidate.traceId,
          kind: candidate.kind,
          text: candidate.text,
        }) as AnalystStreamChunk;
      }));
    },
    evaluateThresholdStrategy: async (request) => {
      if (!request.strategyId.trim() || !request.metricId.trim() || !request.instrumentId.trim()
        || !Number.isSafeInteger(request.metricTtlNs) || request.metricTtlNs <= 0
        || !Number.isFinite(request.score) || !Number.isFinite(request.confidence) || request.confidence < 0 || request.confidence > 1
        || !Number.isFinite(request.uncertainty) || request.uncertainty < 0
        || !Number.isFinite(request.entryThreshold) || !Number.isFinite(request.exitThreshold)
        || !Number.isSafeInteger(request.quantityTicks) || request.quantityTicks === 0
        || !Number.isSafeInteger(request.horizonNs) || request.horizonNs <= 0
        || !Number.isSafeInteger(request.strategyTtlNs) || request.strategyTtlNs <= 0) {
        throw new Error("strategy evaluation request is invalid");
      }
      const value = await bridge.invoke<unknown>("evaluate_threshold_strategy", request);
      if (!value || typeof value !== "object") throw new Error("strategy proposal response is invalid");
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.proposalId !== "string" || !candidate.proposalId.trim()
        || typeof candidate.strategyId !== "string" || typeof candidate.instrumentId !== "string"
        || typeof candidate.action !== "string" || typeof candidate.quantityTicks !== "number"
        || typeof candidate.weight !== "number" || typeof candidate.confidence !== "number"
        || !Number.isFinite(candidate.confidence) || candidate.confidence < 0 || candidate.confidence > 1
        || !Number.isFinite(candidate.weight)) throw new Error("strategy proposal response is malformed");
      return candidate as unknown as StrategyProposalResponse;
    },
    validateAutonomousAction: async (request) => {
      const value = await bridge.invoke<unknown>("validate_autonomous_action", request);
      if (!value || typeof value !== "object") throw new Error("autonomous action response is invalid");
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.traceId !== "string" || !candidate.traceId.trim()
        || typeof candidate.actionType !== "string" || !candidate.actionType.trim()
        || (candidate.proposalId !== undefined && typeof candidate.proposalId !== "string")
        || (candidate.scale !== undefined && (typeof candidate.scale !== "number" || !Number.isFinite(candidate.scale)))
        || !Array.isArray(candidate.reasonCodes) || candidate.reasonCodes.some((reason) => typeof reason !== "string")) {
        throw new Error("autonomous action response is malformed");
      }
      return { traceId: candidate.traceId, actionType: candidate.actionType, proposalId: candidate.proposalId as string | undefined, scale: candidate.scale as number | undefined, reasonCodes: Object.freeze([...(candidate.reasonCodes as string[])]) };
    },
    submitAutonomousPlan: async (request) => {
      if (!request.planId.trim() || request.planId.length > 256
        || !Number.isSafeInteger(request.expiresAfterMs) || request.expiresAfterMs <= 0 || request.expiresAfterMs > 86_400_000
        || request.actions.length === 0 || request.actions.length > 4_096) {
        throw new Error("autonomous plan request is invalid");
      }
      for (const action of request.actions) {
        if (!action.actionType.trim() || !Array.isArray(action.reasonCodes)
          || (action.proposalId !== undefined && typeof action.proposalId !== "string")
          || (action.scale !== undefined && (typeof action.scale !== "number" || !Number.isFinite(action.scale)))) {
          throw new Error("autonomous action request is invalid");
        }
      }
      const value = await bridge.invoke<unknown>("submit_autonomous_plan", request);
      if (typeof value !== "string" || !value.trim()) throw new Error("autonomous plan response is invalid");
      return value;
    },
    transitionAutonomousPlan: async (request) => {
      if (!request.planId.trim() || request.planId.length > 256) throw new Error("autonomous plan ID is invalid");
      const allowed = ["pending", "approved", "rejected", "expired", "executing", "completed", "failed"] as const;
      if (!allowed.includes(request.state)) throw new Error("autonomous plan state is invalid");
      const value = await bridge.invoke<unknown>("transition_autonomous_plan", request);
      if (typeof value !== "string" || value !== request.state) throw new Error("autonomous transition response is invalid");
      return value;
    },
    getAlerts: async () => {
      const value = await bridge.invoke<unknown>("get_alerts");
      if (!Array.isArray(value)) throw new Error("alert response is invalid");
      return Object.freeze(value.map((item): AlertSnapshot => {
        if (!item || typeof item !== "object") throw new Error("alert response item is invalid");
        const candidate = item as Record<string, unknown>;
        if (typeof candidate.alertId !== "string" || !candidate.alertId.trim()
          || typeof candidate.dedupeKey !== "string" || typeof candidate.source !== "string"
          || !Number.isSafeInteger(candidate.occurredMs)
          || (candidate.severity !== 1 && candidate.severity !== 2 && candidate.severity !== 3)
          || typeof candidate.sensitive !== "boolean" || typeof candidate.message !== "string") {
          throw new Error("alert response item is malformed");
        }
        return { alertId: candidate.alertId, dedupeKey: candidate.dedupeKey, source: candidate.source, occurredMs: candidate.occurredMs, severity: candidate.severity, sensitive: candidate.sensitive, message: candidate.message };
      }));
    },
    acknowledgeAlert: async (alertId) => {
      if (!alertId.trim() || alertId.length > 256) throw new Error("alert ID is invalid");
      const value = await bridge.invoke<unknown>("acknowledge_alert", { alertId });
      if (typeof value !== "boolean") throw new Error("alert acknowledgement response is invalid");
      return value;
    },
    getTraceEvents: async (traceId) => {
      if (!traceId.trim() || traceId.length > 256) throw new Error("trace ID is invalid");
      const value = await bridge.invoke<unknown>("get_trace_events", { traceId });
      if (!Array.isArray(value)) throw new Error("trace response is invalid");
      return Object.freeze(value.map((item): TraceEventSnapshot => {
        if (!item || typeof item !== "object") throw new Error("trace event is invalid");
        const candidate = item as Record<string, unknown>;
        if (!Number.isSafeInteger(candidate.sequence) || candidate.sequence < 0
          || typeof candidate.kind !== "string" || !candidate.kind.trim()
          || typeof candidate.payloadHex !== "string" || !/^[0-9a-f]*$/i.test(candidate.payloadHex)) {
          throw new Error("trace event is malformed");
        }
        return { sequence: candidate.sequence, kind: candidate.kind, payloadHex: candidate.payloadHex };
      }));
    },
    exportTrace: async (traceId) => {
      if (!traceId.trim() || traceId.length > 256) throw new Error("trace ID is invalid");
      const value = await bridge.invoke<unknown>("export_trace", { traceId });
      if (!Array.isArray(value)) throw new Error("trace export response is invalid");
      return Object.freeze(value.map((item): TraceExportEventSnapshot => {
        if (!item || typeof item !== "object") throw new Error("trace export event is invalid");
        const candidate = item as Record<string, unknown>;
        if (!Number.isSafeInteger(candidate.sequence) || candidate.sequence < 0
          || typeof candidate.kind !== "string" || !candidate.kind.trim()
          || !Number.isSafeInteger(candidate.payloadBytes) || candidate.payloadBytes < 0) {
          throw new Error("trace export event is malformed");
        }
        return { sequence: candidate.sequence, kind: candidate.kind, payloadBytes: candidate.payloadBytes };
      }));
    },
    resolveInstrument: async (symbol) => {
      const normalized = symbol.trim().toUpperCase();
      if (!/^[A-Z0-9.\-]{1,16}$/.test(normalized)) throw new Error("symbol is invalid");
      return parseInstrumentResolution(await bridge.invoke("resolve_instrument", {
        symbol: normalized,
        day: Math.floor(Date.now() / 86_400_000),
        // Watchlists are observational and may contain any catalog-certified
        // asset class; order preview applies its own stricter policy later.
        supportedAssetMask: 0b11_1111,
      }), normalized);
    },
    previewOrder: async (draft) => {
      const error = validateOrderDraft(draft);
      if (error) throw new Error(error);
      if (store.state.connection !== "ready") throw new Error("runtime is not ready for order preview");
      const current = store.state.orderTicket;
      if (current?.status === "ready" && current.preview && sameDraft(current.preview.draft, draft) && current.preview.expiresAtMs > Date.now()) {
        return current.preview;
      }
      const resolution = draft.instrumentId
        ? { instrumentId: draft.instrumentId }
        : await (async () => {
          const resolved = await bridge.invoke("resolve_instrument", {
            symbol: draft.symbol,
            day: Math.floor(Date.now() / 86_400_000),
            supportedAssetMask: 1,
          });
          return parseInstrumentResolution(resolved, draft.symbol);
        })();
      const resolvedDraft = draft.instrumentId === resolution.instrumentId ? draft : { ...draft, instrumentId: resolution.instrumentId };
      const idempotencyKey = draftKey(resolvedDraft);
      store.setOrderTicket({ status: "previewing", draft: resolvedDraft, idempotencyKey });
      try {
        const preview = parsePreview(await bridge.invoke("preview_order", {
          draft: resolvedDraft,
          instrumentId: resolution.instrumentId,
          idempotencyKey,
          expectedStateVersion: store.state.version,
        }), resolvedDraft);
        store.setOrderTicket({ status: "ready", draft: resolvedDraft, idempotencyKey, preview });
        return preview;
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : "order preview failed";
        store.setOrderTicket({ status: "rejected", draft: resolvedDraft, idempotencyKey, error: message });
        throw new Error(message);
      }
    },
    submitManualOrder: async (draft, confirmationToken) => {
      const error = validateOrderDraft(draft);
      if (error) throw new Error(error);
      if (!confirmationToken.trim()) throw new Error("confirmation is required");
      if (store.state.connection !== "ready") throw new Error("runtime is not ready for order submission");
      const ticket = store.state.orderTicket;
      if (!ticket?.preview || !sameDraft(ticket.preview.draft, draft) || ticket.preview.expiresAtMs <= Date.now()) {
        throw new Error("a fresh risk preview is required before submission");
      }
      if (ticket.status === "submitted" && ticket.submittedOrderId) return ticket.submittedOrderId;
      store.setOrderTicket({ ...ticket, status: "submitting" });
      try {
        const orderId = await bridge.invoke<string>("submit_manual_order", {
          draft,
          confirmationToken,
          previewId: ticket.preview.previewId,
          expectedStateVersion: ticket.preview.expectedStateVersion,
          idempotencyKey: ticket.idempotencyKey ?? draftKey(draft),
        });
        if (typeof orderId !== "string" || !orderId.trim()) throw new Error("runtime returned an invalid order ID");
        store.setOrderTicket({ ...ticket, status: "submitted", submittedOrderId: orderId });
        return orderId;
      } catch (cause) {
        const message = cause instanceof Error ? cause.message : "order submission failed";
        store.setOrderTicket({ ...ticket, status: "rejected", error: message });
        throw new Error(message);
      }
    },
    cancelOrder: async (clientOrderId) => {
      if (!clientOrderId.trim()) throw new Error("client order ID is required");
      if (store.state.connection !== "ready") throw new Error("runtime is not ready for cancellation");
      await bridge.invoke("cancel_order", { clientOrderId });
    },
    replaceOrder: async (clientOrderId, quantityTicks, limitPriceTicks) => {
      if (!clientOrderId.trim()) throw new Error("client order ID is required");
      if (!Number.isSafeInteger(quantityTicks) || quantityTicks <= 0) throw new Error("replacement quantity is invalid");
      if (limitPriceTicks !== undefined && (!Number.isSafeInteger(limitPriceTicks) || limitPriceTicks <= 0)) {
        throw new Error("replacement limit price is invalid");
      }
      if (store.state.connection !== "ready") throw new Error("runtime is not ready for replacement");
      await bridge.invoke("replace_order", { clientOrderId, quantityTicks, limitPriceTicks });
    },
    previewProposal: (proposalId, scale = 1) => {
      if (!Number.isFinite(scale) || scale <= 0 || scale > 1) {
        return Promise.reject(new Error("proposal scale must be in (0,1]"));
      }
      return bridge.invoke("preview_proposal", { proposalId, scale });
    },
    submitProposal: async (proposalId, confirmationToken) => {
      if (!confirmationToken.trim()) throw new Error("confirmation is required");
      const proposal = store.state.proposals.find((item: ProposalSnapshot) => item.proposalId === proposalId);
      if (!proposal || proposal.expiresAtMs <= Date.now()) throw new Error("proposal is missing or expired");
      return bridge.invoke<string>("submit_proposal", { proposalId, confirmationToken });
    },
    submitScheduledProposal: async (proposalId, schedule, confirmationToken) => {
      if (!confirmationToken.trim()) throw new Error("confirmation is required");
      const proposal = store.state.proposals.find((item: ProposalSnapshot) => item.proposalId === proposalId);
      if (!proposal || proposal.expiresAtMs <= Date.now()) throw new Error("proposal is missing or expired");
      if (schedule.type === "twap" && (!Number.isSafeInteger(schedule.slices) || schedule.slices <= 0 || !Number.isSafeInteger(schedule.intervalNs) || schedule.intervalNs <= 0)) {
        throw new Error("TWAP schedule is invalid");
      }
      if (schedule.type === "vwap" && (schedule.weights.length === 0 || schedule.weights.length > 16_384 || schedule.weights.some((weight) => !Number.isSafeInteger(weight) || weight <= 0))) {
        throw new Error("VWAP schedule is invalid");
      }
      if (schedule.type === "pov" && (!Number.isSafeInteger(schedule.participationBps) || schedule.participationBps <= 0 || schedule.participationBps > 10_000 || !Number.isSafeInteger(schedule.intervalNs) || schedule.intervalNs <= 0 || schedule.marketVolumeTicks.length === 0 || schedule.marketVolumeTicks.some((volume) => !Number.isSafeInteger(volume) || volume <= 0))) {
        throw new Error("POV schedule is invalid");
      }
      if (schedule.type === "implementation_shortfall" && (!Number.isSafeInteger(schedule.slices) || schedule.slices <= 0 || !Number.isSafeInteger(schedule.intervalNs) || schedule.intervalNs <= 0 || !Number.isSafeInteger(schedule.urgencyBps) || schedule.urgencyBps < 0 || schedule.urgencyBps > 10_000)) {
        throw new Error("implementation-shortfall schedule is invalid");
      }
      if (schedule.type === "adaptive" && (!Number.isSafeInteger(schedule.slices) || schedule.slices <= 0 || !Number.isSafeInteger(schedule.intervalNs) || schedule.intervalNs <= 0 || !Number.isSafeInteger(schedule.urgencyBps) || schedule.urgencyBps <= 0 || schedule.urgencyBps > 10_000 || !Number.isSafeInteger(schedule.spreadTicks) || schedule.spreadTicks < 0 || !Number.isSafeInteger(schedule.maxSpreadTicks) || schedule.maxSpreadTicks <= 0 || !Number.isSafeInteger(schedule.volatilityBps) || schedule.volatilityBps < 0 || !Number.isSafeInteger(schedule.maxVolatilityBps) || schedule.maxVolatilityBps <= 0 || schedule.marketVolumeTicks.length === 0 || schedule.marketVolumeTicks.length > schedule.slices || schedule.marketVolumeTicks.some((volume) => !Number.isSafeInteger(volume) || volume <= 0))) {
        throw new Error("adaptive schedule is invalid");
      }
      const payload = schedule.type === "immediate"
        ? { schedule: "immediate" }
        : schedule.type === "twap"
          ? { schedule: "twap", slices: schedule.slices, intervalNs: schedule.intervalNs }
          : schedule.type === "vwap"
            ? { schedule: "vwap", weights: [...schedule.weights] }
            : schedule.type === "pov"
              ? { schedule: "pov", participationBps: schedule.participationBps, intervalNs: schedule.intervalNs, marketVolumeTicks: [...schedule.marketVolumeTicks] }
              : schedule.type === "implementation_shortfall"
                ? { schedule: "implementation_shortfall", slices: schedule.slices, intervalNs: schedule.intervalNs, urgencyBps: schedule.urgencyBps }
                : { schedule: "adaptive", slices: schedule.slices, intervalNs: schedule.intervalNs, urgencyBps: schedule.urgencyBps, spreadTicks: schedule.spreadTicks, maxSpreadTicks: schedule.maxSpreadTicks, volatilityBps: schedule.volatilityBps, maxVolatilityBps: schedule.maxVolatilityBps, marketVolumeTicks: [...schedule.marketVolumeTicks] };
      return bridge.invoke<string>("submit_scheduled_proposal", { proposalId, confirmationToken, ...payload });
    },
    runBacktest: async (request) => {
      if (!request.runId.trim() || !request.strategyId.trim() || !request.datasetHash.trim() || !request.configHash.trim()) {
        throw new Error("backtest lineage is required");
      }
      if (!/^-?[0-9]+$/.test(request.initialCashTicks) || BigInt(request.initialCashTicks) <= 0n) {
        throw new Error("initial cash ticks are invalid");
      }
      if (request.events.length === 0 || request.events.length > 1_000_000) throw new Error("backtest events are outside bounds");
      let previous = 0;
      for (const event of request.events) {
        if (!Number.isSafeInteger(event.sequence) || event.sequence === 0 || event.sequence <= previous) throw new Error("backtest sequences must increase");
        previous = event.sequence;
        if (!Number.isSafeInteger(event.priceTicks) || event.priceTicks <= 0) throw new Error("backtest price is invalid");
        if (event.kind === "fill" && (!Number.isSafeInteger(event.quantityTicks) || event.quantityTicks === 0)) throw new Error("backtest fill quantity is invalid");
      }
      return bridge.invoke<BacktestRunResponse>("run_backtest", request);
    },
    runStrategyBacktest: async (request) => {
      if (!request.runId.trim() || !request.strategyId.trim() || !request.datasetHash.trim() || !request.configHash.trim()) throw new Error("strategy backtest lineage is required");
      if (!/^-?[0-9]+$/.test(request.initialCashTicks) || BigInt(request.initialCashTicks) <= 0n) throw new Error("initial cash ticks are invalid");
      if (request.events.length === 0 || request.events.length > 100_000) throw new Error("strategy backtest events are outside bounds");
      let previous = 0;
      for (const event of request.events) {
        if (!Number.isSafeInteger(event.sequence) || event.sequence <= previous || event.sequence <= 0 || !Number.isSafeInteger(event.nowMonoNs) || event.nowMonoNs < 0 || !Number.isSafeInteger(event.priceTicks) || event.priceTicks <= 0) throw new Error("strategy backtest event is invalid");
        previous = event.sequence;
        if (event.metrics.length > 4_096 || event.metrics.some((metric) => !metric.metricId.trim() || !Number.isSafeInteger(metric.generatedMonoNs) || metric.generatedMonoNs < 0 || !Number.isSafeInteger(metric.ttlNs) || metric.ttlNs <= 0 || !Number.isFinite(metric.score) || !Number.isFinite(metric.confidence) || !Number.isFinite(metric.uncertainty))) throw new Error("strategy backtest metric is invalid");
      }
      return bridge.invoke<BacktestRunResponse>("run_strategy_backtest", request);
    },
    listBacktests: () => bridge.invoke<readonly BacktestRunResponse[]>("list_backtests"),
    listExperiments: () => bridge.invoke<readonly ExperimentRunResponse[]>("list_experiments"),
    getConfig: () => bridge.invoke<ConfigSnapshotResponse>("get_config"),
    reloadConfig: (request) => bridge.invoke<ConfigSnapshotResponse>("reload_config", request),
    mutateExperiment: async (request) => { await bridge.invoke("mutate_experiment", request); },
    listModels: () => bridge.invoke<readonly ModelRecordResponse[]>("list_models"),
    mutateModel: async (request) => { await bridge.invoke("mutate_model", request); },
    listStrategyResolutions: () => bridge.invoke<readonly StrategyResolutionResponse[]>("list_strategy_resolutions"),
    resolveStrategyWithBudgets: async (policy, budgets) => {
      if (!["isolated_books", "priority", "weighted_net"].includes(policy)) throw new Error("strategy policy is invalid");
      const entries = Object.entries(budgets);
      if (entries.length > 256 || entries.some(([id, value]) => !id.trim() || id.length > 256 || !Number.isSafeInteger(value) || value <= 0)) throw new Error("strategy budgets are invalid");
      return bridge.invoke<{ readonly acceptedCount: number; readonly adjustmentCount: number }>("resolve_strategy_with_budgets", { policy, budgets });
    },
    listStrategyExecutionSummaries: () => bridge.invoke<readonly StrategyExecutionResponse[]>("list_strategy_execution_summaries"),
    listStrategies: () => bridge.invoke<readonly StrategyRegistryResponse[]>("list_strategies"),
    transitionStrategyLifecycle: async (strategyId, lifecycle, confirmation, evidenceRef) => {
      if (!strategyId.trim() || strategyId.length > 256) throw new Error("strategy ID is invalid");
      if (!["research", "validated", "shadow", "canary", "production", "paused", "retired"].includes(lifecycle)) throw new Error("strategy lifecycle is invalid");
      if (confirmation !== "CONFIRM") throw new Error("type CONFIRM to change strategy lifecycle");
      if (!evidenceRef.trim() || evidenceRef.length > 512) throw new Error("promotion evidence reference is invalid");
      await bridge.invoke("transition_strategy_lifecycle", { strategyId, lifecycle, confirmation, evidenceRef });
    },
    transitionMetricLifecycle: async (metricId, lifecycle, confirmation, evidenceRef) => {
      if (!metricId.trim() || metricId.length > 256) throw new Error("metric ID is invalid");
      if (!["research", "validated", "shadow", "canary", "production", "paused", "retired"].includes(lifecycle)) throw new Error("metric lifecycle is invalid");
      if (confirmation !== "CONFIRM") throw new Error("type CONFIRM to change metric lifecycle");
      if (!evidenceRef.trim() || evidenceRef.length > 512) throw new Error("promotion evidence reference is invalid");
      await bridge.invoke("transition_metric_lifecycle", { metricId, lifecycle, confirmation, evidenceRef });
    },
    listMetrics: () => bridge.invoke<readonly MetricRegistryResponse[]>("list_metrics"),
    setTradingMode: async (mode) => {
      await bridge.invoke("set_trading_mode", { mode });
      store.patch({ autonomy: { ...store.state.autonomy, mode } });
    },
    configureLiveLimits: async (accounts, maxNotionalTicks) => {
      const normalized = [...new Set(accounts.map(validateLiveAccount))].sort();
      if (normalized.length === 0 || normalized.length > 128) throw new Error("live account allowlist is invalid");
      if (!Number.isSafeInteger(maxNotionalTicks) || maxNotionalTicks <= 0) throw new Error("live notional cap is invalid");
      return parseTradingEnvironment(await bridge.invoke("configure_live_limits", { accounts: normalized, maxNotionalTicks }));
    },
    armLive: async (account, phrase) => {
      const normalized = validateLiveAccount(account);
      if (phrase !== "ARM LIVE") throw new Error("type ARM LIVE exactly");
      const value = await bridge.invoke("arm_live", { account: normalized, phrase });
      if (!value || typeof value !== "object") throw new Error("runtime returned an invalid live challenge");
      const environment = parseTradingEnvironment(value);
      const token = (value as Record<string, unknown>).token;
      if (typeof token !== "string" || !token.trim()) throw new Error("runtime returned an invalid live challenge token");
      return { environment, token };
    },
    confirmLive: async (account, token, phrase) => {
      const normalized = validateLiveAccount(account);
      if (!token.trim()) throw new Error("live challenge token is required");
      if (phrase !== "ENABLE LIVE") throw new Error("type ENABLE LIVE exactly");
      return parseTradingEnvironment(await bridge.invoke("confirm_live", { account: normalized, token, phrase }));
    },
    killLive: async () => parseTradingEnvironment(await bridge.invoke("kill_live")),
    backupJournal: async (path) => {
      if (!path.trim() || path.length > 4096) throw new Error("backup path is invalid");
      const value = await bridge.invoke<JournalBackupResponse>("backup_journal", { path });
      if (!value || typeof value.source !== "string" || typeof value.destination !== "string" ||
          !Number.isSafeInteger(value.byteLen) || value.byteLen < 0 ||
          typeof value.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.sha256)) {
        throw new Error("runtime returned invalid journal backup metadata");
      }
      return value;
    },
    restoreJournal: async (source, destination) => {
      if (!source.trim() || !destination.trim() || source.length > 4096 || destination.length > 4096) {
        throw new Error("restore paths are invalid");
      }
      const value = await bridge.invoke<JournalBackupResponse>("restore_journal", { source, destination });
      if (!value || typeof value.source !== "string" || typeof value.destination !== "string" ||
          !Number.isSafeInteger(value.byteLen) || value.byteLen < 0 ||
          typeof value.sha256 !== "string" || !/^[0-9a-f]{64}$/.test(value.sha256)) {
        throw new Error("runtime returned invalid journal restore metadata");
      }
      return value;
    },
    transitionRiskState: async (state, authorization) => {
      if (!authorization || authorization.length > 256) throw new Error("risk authorization is invalid");
      if (!["running", "reduce_only", "cancel_only", "halted"].includes(state)) throw new Error("risk state is invalid");
      const value = await bridge.invoke<string>("transition_risk_state", { state, authorization });
      if (value !== state) throw new Error("runtime returned an invalid risk state");
      return value;
    },
  };
}
