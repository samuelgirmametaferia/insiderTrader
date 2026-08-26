import { createApplicationIdentity } from "./bootstrap";
import type { AlertSnapshot, BacktestRunRequest, BacktestRunResponse, BrokerStatusSnapshot, ExperimentRunResponse, JournalBackupResponse, MetricRegistryResponse, ModelRecordResponse, NewsDetailSnapshot, NewsProviderStatusSnapshot, RiskPolicyRevisionSnapshot, RuntimeBridge, StrategyExecutionResponse, StrategyRegistryResponse, StrategyResolutionResponse, SupervisorStatusSnapshot, TraceEventSnapshot, TraceExportEventSnapshot } from "../commands/bridge";
import { createTradingCommands, RuntimeSession } from "../commands/bridge";
import { RuntimeStore, validateOrderDraft } from "../stores/runtime-store";
import { ALL_PANEL_IDS, completeWorkspaceLayout, createWorkspacePreset, loadWorkspaceLayout, validateWorkspaceLayout, WorkspacePersistence, type WorkspacePreset } from "../layouts/workspace";
import { renderChartSvg, resampleCandles, type ChartDrawing, type ChartGridlineDensity, type ChartRenderMode, type ChartViewWindow } from "../charts/market-chart";

const bridge: RuntimeBridge = {
  invoke: async <T>(command: string, payload?: unknown) => {
    const tauri = (globalThis as { __TAURI__?: { core?: { invoke?: (name: string, args?: unknown) => Promise<T> } } }).__TAURI__;
    if (!tauri?.core?.invoke) throw new Error(`runtime bridge unavailable for ${command}`);
    return tauri.core.invoke(command, payload);
  },
  listen: async <T>(event: string, handler: (payload: T) => void) => {
    const tauri = (globalThis as {
      __TAURI__?: {
        event?: {
          listen?: (name: string, callback: (event: { payload: T }) => void) => Promise<() => void>;
        };
      };
    }).__TAURI__;
    if (!tauri?.event?.listen) throw new Error(`runtime event bridge unavailable for ${event}`);
    return tauri.event.listen(event, (message) => handler(message.payload));
  },
};

const store = new RuntimeStore();
const commands = createTradingCommands(bridge, store);
const session = new RuntimeSession(bridge, store);
const identity = createApplicationIdentity(store.state.autonomy.mode);
const root = document.querySelector<HTMLElement>("#workspace");
let selectedNewsDetail: NewsDetailSnapshot | undefined;
let analystContent = "";
let analystError = "";
let analystBusy = false;
let analystReceivedAtMs: number | undefined;
const ANALYST_DISPLAY_TTL_MS = 300_000;
let analystStaleNoticeShown = false;
type AnalystContextId = "symbol" | "timeframe" | "cursor" | "news" | "strategies" | "positions";
const analystContextEnabled = new Set<AnalystContextId>(["symbol", "timeframe", "cursor", "news", "strategies", "positions"]);
type AnalystEvidenceCard = { readonly contextId: AnalystContextId; readonly label: string; readonly value: string; readonly panelId: import("../stores/runtime-store").PanelId };
let analystEvidence: readonly AnalystEvidenceCard[] = [];
let contextSearchResults: readonly import("../commands/bridge").ContextSearchHit[] = [];
let selectedContextHit: import("../commands/bridge").ContextSearchHit | undefined;
type GlobalSearchResult = { readonly kind: "instrument" | "strategy" | "metric" | "news" | "model" | "experiment" | "order" | "trace"; readonly id: string; readonly label: string; readonly detail: string };
let globalSearchResults: readonly GlobalSearchResult[] = [];
let contextSearchError = "";
let contextSearchBusy = false;
let alerts: readonly AlertSnapshot[] = [];
const messageStationExpiry = new Map<string, number>();
type MessageRecord = { readonly alert: AlertSnapshot; readonly acknowledged: boolean };
let messageHistory: readonly MessageRecord[] = [];
const ALERT_NATIVE_STORAGE_KEY = "insidertrader.alerts.native.v1";
const ALERT_SOUND_STORAGE_KEY = "insidertrader.alerts.sound.v1";
const ALERT_SOUND_SEVERITY_STORAGE_KEY = "insidertrader.alerts.sound-severity.v1";
let nativeAlertsEnabled = false;
let nativeAlertPermissionError = "";
let soundAlertsEnabled = false;
let soundSeverityEnabled: readonly boolean[] = [true, true, true, true];
let alertsDegraded = false;
const ALERT_POLL_DEFAULT_MS = 1_000;
function configuredAlertPollMs(): number {
  const value = Number(configNumericValue(configSnapshot?.cfg_text ?? "", "ui.alert_poll_ms", String(ALERT_POLL_DEFAULT_MS)));
  return Number.isSafeInteger(value) && value >= 500 && value <= 60_000 ? value : ALERT_POLL_DEFAULT_MS;
}
const deliveredAlertIds = new Set<string>();
let alertAudioContext: AudioContext | undefined;
let traceEvents: readonly TraceEventSnapshot[] = [];
let traceExport: readonly TraceExportEventSnapshot[] = [];
let traceError = "";
let newsScrollTop = 0;
let newsScrollScheduled = false;
let newsBusy = false;
let newsError = "";
let newsLastSuccessAtMs: number | undefined;
let newsRetryScope: "relevant" | "all" = "relevant";
let newsRetrySymbol = "";
let newsRetryCursor: string | undefined;
let queuedNewsRequest: { readonly scope: "relevant" | "all"; readonly symbol: string; readonly cursor?: string } | undefined;
const NEWS_STALE_AFTER_MS = 300_000;
function configuredNewsStaleAfterMs(): number {
  const value = Number(configNumericValue(configSnapshot?.cfg_text ?? "", "ui.news_stale_after_ms", String(NEWS_STALE_AFTER_MS)));
  return Number.isSafeInteger(value) && value >= 60_000 && value <= 3_600_000 ? value : NEWS_STALE_AFTER_MS;
}
function configuredAnalystDisplayTtlMs(): number {
  const value = Number(configNumericValue(configSnapshot?.cfg_text ?? "", "ui.analyst_stale_after_ms", String(ANALYST_DISPLAY_TTL_MS)));
  return Number.isSafeInteger(value) && value >= 60_000 && value <= 3_600_000 ? value : ANALYST_DISPLAY_TTL_MS;
}
type NewsView = "relevant" | "all" | "watchlist" | "portfolio";
let newsView: NewsView = "relevant";
const NEWS_VIEW_STORAGE_KEY = "insidertrader.news-view.v1";
function loadNewsView(): NewsView {
  const value = layoutStorage.getItem(NEWS_VIEW_STORAGE_KEY);
  return value === "all" || value === "watchlist" || value === "portfolio" || value === "relevant" ? value : "relevant";
}
function persistNewsView(): void { layoutStorage.setItem(NEWS_VIEW_STORAGE_KEY, newsView); }
let watchlistScrollTop = 0;
let watchlistError = "";
let timeSalesScrollTop = 0;
let tapeScrollScheduled = false;
let screenerQuery = "";
let screenerSort: "symbol" | "last" | "spread" | "confidence" = "symbol";
const SCREENER_PAGE_SIZE = 100;
let screenerVisibleRows = SCREENER_PAGE_SIZE;
let backtestBusy = false;
let backtestError = "";
let backtestResult: BacktestRunResponse | undefined;
let backtestHistory: readonly BacktestRunResponse[] = [];
let experimentHistory: readonly ExperimentRunResponse[] = [];
let configSnapshot: import("../commands/bridge").ConfigSnapshotResponse | undefined;
let configError = "";
let configActionMessage = "";
let configBusy = false;
let modelHistory: readonly ModelRecordResponse[] = [];
let resolutionHistory: readonly StrategyResolutionResponse[] = [];
let executionHistory: readonly StrategyExecutionResponse[] = [];
let strategyRegistry: readonly StrategyRegistryResponse[] = [];
let metricRegistry: readonly MetricRegistryResponse[] = [];
let selectedMetricId: string | undefined;
let newsProviderStatuses: readonly NewsProviderStatusSnapshot[] = [];
let supervisorStatuses: readonly SupervisorStatusSnapshot[] = [];
let brokerStatus: BrokerStatusSnapshot | undefined;
let riskPolicyRevisions: readonly RiskPolicyRevisionSnapshot[] = [];
let backupBusy = false;
let backupError = "";
let backupResult: JournalBackupResponse | undefined;
let commandPaletteOpen = false;
let commandPaletteQuery = "";
let workspaceAddOpen = false;
type WorkspaceDialogMode = "duplicate" | "rename" | "delete";
let workspaceDialog: { readonly mode: WorkspaceDialogMode; readonly initialName: string; error?: string } | undefined;
let scheduleConfirmation: { readonly proposalId: string; readonly kind: "twap" | "implementation_shortfall"; error?: string } | undefined;
let cancelAllConfirmation: { readonly count: number; error?: string } | undefined;
let replaceOrderDialog: { readonly clientOrderId: string; readonly quantity: number; readonly limit?: number; error?: string } | undefined;
let chartTemplateDialog: { readonly mode: "save" | "delete"; readonly initialName: string; error?: string } | undefined;
let lifecycleDialog: { readonly kind: "strategy" | "metric"; readonly id: string; readonly lifecycle: string; error?: string } | undefined;
let modelEvidenceDialog: { readonly modelId: string; readonly version: string; readonly operation: "validate" | "canary"; error?: string } | undefined;
let autonomyModeDialog: { error?: string } | undefined;
let messageStationOpen = false;
type RightDockTab = "positions" | "orders" | "watchlist" | "alerts";
let rightDockTab: RightDockTab = "positions";
const RIGHT_DOCK_TAB_KEY = "insidertrader.right-dock-tab.v1";
function loadRightDockTab(): RightDockTab {
  const value = layoutStorage.getItem(RIGHT_DOCK_TAB_KEY);
  return value === "orders" || value === "watchlist" || value === "alerts" || value === "positions" ? value : "positions";
}
function persistRightDockTab(): void { layoutStorage.setItem(RIGHT_DOCK_TAB_KEY, rightDockTab); }
let toolsRailOpen = false;
let chartView: ChartViewWindow | undefined;
let chartDrag: { startX: number; startStart: number } | undefined;
let chartPanVelocity = 0;
let chartPanLastX = 0;
let chartPanLastAt = 0;
let chartInertiaFrame: number | undefined;
let chartHoverIndex: number | undefined;
let chartContextOpen = false;
let chartHoverRenderFrame: number | undefined;
let chartHoverYPercent: number | undefined;
let chartRenderMs = 0;
const CHART_FRAME_BUDGET_MS = 1000 / 60;
let settingsOpen = false;
let settingsQuery = "";
type HotkeyAction = "commandPalette" | "workspace1" | "workspace2" | "workspace3" | "workspace4" | "workspace5" | "workspace6" | "workspace7" | "workspace8" | "workspace9";
const HOTKEY_STORAGE_KEY = "insidertrader.hotkeys.v1";
const DEFAULT_HOTKEYS: Readonly<Record<HotkeyAction, string>> = {
  commandPalette: "Mod+K", workspace1: "Mod+1", workspace2: "Mod+2", workspace3: "Mod+3", workspace4: "Mod+4",
  workspace5: "Mod+5", workspace6: "Mod+6", workspace7: "Mod+7", workspace8: "Mod+8", workspace9: "Mod+9",
};
const HOTKEY_ACTIONS: readonly HotkeyAction[] = ["commandPalette", "workspace1", "workspace2", "workspace3", "workspace4", "workspace5", "workspace6", "workspace7", "workspace8", "workspace9"];
function normalizeHotkey(value: string): string | undefined {
  const normalized = value.trim().replace(/\s+/g, "").replace(/^Ctrl\+|^Meta\+/i, "Mod+");
  if (!/^Mod\+[A-Z0-9]$/.test(normalized)) return undefined;
  return `Mod+${normalized.slice(4).toUpperCase()}`;
}
function loadHotkeys(): Record<HotkeyAction, string> {
  const result = { ...DEFAULT_HOTKEYS };
  try {
    const parsed: unknown = JSON.parse(layoutStorage.getItem(HOTKEY_STORAGE_KEY) ?? "null");
    if (!parsed || typeof parsed !== "object") return result;
    const candidate = parsed as Record<string, unknown>;
    if (candidate.version !== 1) return result;
    const source = candidate.bindings;
    if (!source || typeof source !== "object") return result;
    const seen = new Set<string>();
    for (const action of HOTKEY_ACTIONS) {
      const value = (source as Record<string, unknown>)[action];
      const normalized = typeof value === "string" ? normalizeHotkey(value) : undefined;
      if (!normalized || seen.has(normalized)) return { ...DEFAULT_HOTKEYS };
      seen.add(normalized);
      result[action] = normalized;
    }
  } catch { /* corrupt presentation state falls back to safe defaults */ }
  return result;
}
let hotkeys: Record<HotkeyAction, string> = { ...DEFAULT_HOTKEYS };
let hotkeyError = "";
function persistHotkeys(): void { layoutStorage.setItem(HOTKEY_STORAGE_KEY, JSON.stringify({ version: 1, bindings: hotkeys })); }
function eventHotkey(event: KeyboardEvent): string | undefined {
  if (!event.ctrlKey && !event.metaKey) return undefined;
  if (event.altKey) return undefined;
  const key = event.key.length === 1 ? event.key.toUpperCase() : undefined;
  return key ? `Mod+${key}` : undefined;
}
const COLORBLIND_STORAGE_KEY = "insidertrader.appearance.colorblind.v1";
const FONT_SCALE_STORAGE_KEY = "insidertrader.appearance.font-scale.v1";
const ORDER_DEFAULTS_STORAGE_KEY = "insidertrader.trading.defaults.v1";
const NEWS_ROW_HEIGHT = 92;
const NEWS_VIEWPORT_HEIGHT = 520;
const WATCHLIST_ROW_HEIGHT = 34;
const WATCHLIST_VIEWPORT_HEIGHT = 360;
const TAPE_ROW_HEIGHT = 30;
const TAPE_VIEWPORT_HEIGHT = 360;

function commandPaletteMarkup(): string {
  if (!commandPaletteOpen) return "";
  const commands = [
    ["workspace:Trading", "Switch workspace · Trading"],
    ["workspace:MultiChart", "Switch workspace · MultiChart"],
    ["workspace:News", "Switch workspace · News"],
    ["workspace:Strategies", "Switch workspace · Strategies"],
    ["workspace:Autonomy", "Switch workspace · Autonomy"],
    ["workspace:Execution", "Switch workspace · Execution"],
    ["workspace:Research", "Switch workspace · Research"],
    ["workspace:Scalping", "Switch workspace · Scalping"],
    ["workspace:Swing", "Switch workspace · Swing"],
    ["workspace:Backtest", "Switch workspace · Backtest"],
    ["panels:restore", "Restore hidden panels"],
    ["focus:global-search", "Search symbols, strategies, news, and traces"],
    ["focus:order-ticket", "Open order ticket"],
    ["focus:alerts", "Create or inspect alerts"],
    ["focus:autonomy", "Open autonomy console"],
    ["focus:news", "Open relevant news"],
    ["focus:watchlist", "Open watchlist"],
    ["focus:metrics", "Open metrics"],
    ["focus:strategy-analysis", "Run strategy analysis"],
    ["focus:backtest", "Run deterministic backtest"],
    ["focus:trace", "Inspect TraceId"],
    ["mode:manual", "Change autonomy state · Manual"],
    ["mode:hybrid", "Change autonomy state · Hybrid"],
    ["mode:autonomous", "Change autonomy state · Autonomous"],
  ] as const;
  const allCommands = [...commands, ...customWorkspaces.map((workspace) => [`workspace:${workspace.name}`, `Switch workspace · ${workspace.name}`] as const)];
  const query = commandPaletteQuery.trim().toLocaleLowerCase();
  const visible = allCommands.filter(([, label]) => !query || label.toLocaleLowerCase().includes(query));
  return `<div class="command-palette-backdrop" data-command-close><section class="command-palette" role="dialog" aria-modal="true" aria-label="Command palette"><header><strong>Command palette</strong><button type="button" data-command-close aria-label="Close command palette">×</button></header><input data-command-search type="search" maxlength="96" value="${escapeHtml(commandPaletteQuery)}" placeholder="Search commands…" aria-label="Search commands" autofocus />${visible.map(([command, label]) => `<button type="button" data-command="${escapeHtml(command)}">${escapeHtml(label)}</button>`).join("") || `<div class="empty">No matching commands</div>`}<p class="muted">Press Esc to close · Ctrl/Cmd+K to toggle</p></section></div>`;
}

function settingsMarkup(): string {
  if (!settingsOpen) return "";
  const query = settingsQuery.toLocaleLowerCase();
  const category = (name: string, body: string): string => `${name} ${body}`.toLocaleLowerCase().includes(query) ? `<section class="settings-category"><h3>${name}</h3>${body}</section>` : "";
  const workspaceHotkeys = HOTKEY_ACTIONS.filter((action) => action !== "commandPalette").map((action, index) => `<label>Workspace ${index + 1}<input data-hotkey-action="${action}" value="${escapeHtml(hotkeys[action])}" maxlength="16" pattern="Mod\+[A-Z0-9]" /></label>`).join("");
  const conflict = hotkeyError ? `<div class="error" role="alert">${escapeHtml(hotkeyError)}</div>` : "";
  return `<div class="settings-backdrop" data-settings-close><section class="settings-modal" role="dialog" aria-modal="true" aria-label="Settings"><header><h2>Settings</h2><button type="button" data-settings-close aria-label="Close settings">×</button></header><input class="settings-search" data-settings-search value="${escapeHtml(settingsQuery)}" placeholder="Search settings…" aria-label="Search settings" />${category("Appearance", `<label><input type="checkbox" data-colorblind-toggle ${colorblindMode ? "checked" : ""} /> Colorblind-safe palette (blue/amber)</label><label>Font size scale<select data-font-scale><option value="0.9" ${fontScale === 0.9 ? "selected" : ""}>90%</option><option value="1" ${fontScale === 1 ? "selected" : ""}>100%</option><option value="1.1" ${fontScale === 1.1 ? "selected" : ""}>110%</option></select></label><label>Default chart style<select data-settings-chart-mode><option value="candles" ${chartPreferences.mode === "candles" ? "selected" : ""}>Candles</option><option value="bars" ${chartPreferences.mode === "bars" ? "selected" : ""}>OHLC bars</option><option value="line" ${chartPreferences.mode === "line" ? "selected" : ""}>Line</option><option value="area" ${chartPreferences.mode === "area" ? "selected" : ""}>Area</option></select></label><label>Gridline density<select data-settings-gridlines><option value="none" ${chartPreferences.gridlineDensity === "none" ? "selected" : ""}>None</option><option value="low" ${chartPreferences.gridlineDensity === "low" ? "selected" : ""}>Low</option><option value="high" ${chartPreferences.gridlineDensity === "high" ? "selected" : ""}>High</option></select></label><button type="button" data-settings-open-panel="chart">Open Chart</button><div class="muted">Dark theme · compact density · tabular numerals</div>`)}${category("Trading", `<label>Default order type<select data-settings-order-type><option value="market" ${defaultOrderType === "market" ? "selected" : ""}>Market</option><option value="limit" ${defaultOrderType === "limit" ? "selected" : ""}>Limit</option></select></label><label>Default order quantity (ticks)<input data-settings-order-quantity type="number" min="1" step="1" value="${defaultOrderQuantity}" /></label><label><input type="checkbox" disabled /> One-click trading <span class="muted">disabled; engine confirmation remains mandatory</span></label><div class="muted">Order confirmation and risk gates remain authoritative in the engine.</div><button type="button" data-settings-open-panel="order-ticket">Open Order Ticket</button>`)}${category("Layout", `<div class="metric"><span>Active workspace</span><span>${escapeHtml(workspaceLayout.name)}</span></div><button type="button" data-settings-reset-layout>Reset this workspace to its template</button><div class="muted">Panel edits, tab order, dock width, and workspace context save silently as presentation state.</div>`)}${category("Data", `<div class="muted">Market/news refresh and provider selection are configured through .cfg.</div><button type="button" data-settings-open-panel="configuration">Open CFG Generator</button>`)}${category("Notifications", `<div class="muted">Use the Alerts panel to configure message-station routing and per-severity sound.</div><button type="button" data-settings-open-panel="alerts">Open Alerts</button>`)}${category("Risk", `<div class="muted">Risk limits are generated and applied through the Configuration panel; state transitions require authorization.</div><button type="button" data-settings-open-panel="risk">Open Risk</button>`)}${category("Hotkeys", `<label>Command palette<input data-hotkey-action="commandPalette" value="${escapeHtml(hotkeys.commandPalette)}" maxlength="16" pattern="Mod\+[A-Z0-9]" /></label>${workspaceHotkeys}${conflict}<button type="button" data-hotkeys-reset>Reset supported hotkeys</button><div class="muted">Use Mod+key (Ctrl on Windows/Linux, Cmd on macOS); legacy Cmd/Ctrl + 1–9 bindings map to the same workspace slots. Duplicate bindings are rejected.</div>`)}${category("Connections / API", `<div class="muted">Credentials never enter UI persistence; provider setup is handled by the engine.</div><button type="button" data-settings-open-panel="broker-status">Open Broker Status</button>`)}</section></div>`;
}

function workspaceDialogMarkup(): string {
  const dialog = workspaceDialog;
  if (!dialog) return "";
  const deleting = dialog.mode === "delete";
  const title = deleting ? "Delete workspace" : dialog.mode === "rename" ? "Rename workspace" : "Duplicate workspace";
  const description = deleting
    ? `Delete the presentation-only workspace “${escapeHtml(dialog.initialName)}”? Trading state, orders, positions, and journal data are unaffected.`
    : "Use 1–32 letters, numbers, spaces, _ or -; names must be unique.";
  return `<div class="workspace-dialog-backdrop" data-workspace-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="workspace-dialog-title"><header><h2 id="workspace-dialog-title">${title}</h2><button type="button" data-workspace-dialog-close aria-label="Close workspace dialog">×</button></header><p>${description}</p>${deleting ? "" : `<label>Workspace name<input data-workspace-dialog-name maxlength="32" value="${escapeHtml(dialog.initialName)}" autocomplete="off" autofocus /></label>`}${dialog.error ? `<div class="error" role="alert">${escapeHtml(dialog.error)}</div>` : ""}<footer><button type="button" data-workspace-dialog-cancel>Cancel</button><button type="button" data-workspace-dialog-submit class="${deleting ? "danger" : ""}">${deleting ? "Delete workspace" : "Continue"}</button></footer></section></div>`;
}

function scheduleConfirmationMarkup(): string {
  const confirmation = scheduleConfirmation;
  if (!confirmation) return "";
  const label = confirmation.kind === "twap" ? "TWAP" : "implementation-shortfall";
  return `<div class="workspace-dialog-backdrop" data-schedule-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="schedule-dialog-title" aria-describedby="schedule-dialog-description"><header><h2 id="schedule-dialog-title">Schedule child orders</h2><button type="button" data-schedule-dialog-close aria-label="Close schedule confirmation">×</button></header><p id="schedule-dialog-description">This submits four ${label} child orders for proposal ${escapeHtml(confirmation.proposalId)}. Type CONFIRM to continue; risk and execution checks still apply.</p><label>Confirmation phrase<input data-schedule-confirmation-input maxlength="7" autocomplete="off" autofocus /></label>${confirmation.error ? `<div class="error" role="alert">${escapeHtml(confirmation.error)}</div>` : ""}<footer><button type="button" data-schedule-dialog-cancel>Cancel</button><button type="button" data-schedule-dialog-submit>Schedule ${label}</button></footer></section></div>`;
}

function cancelAllConfirmationMarkup(): string {
  const confirmation = cancelAllConfirmation;
  if (!confirmation) return "";
  return `<div class="workspace-dialog-backdrop" data-cancel-all-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="cancel-all-dialog-title" aria-describedby="cancel-all-dialog-description"><header><h2 id="cancel-all-dialog-title">Cancel working orders</h2><button type="button" data-cancel-all-dialog-close aria-label="Close cancel-all confirmation">×</button></header><p id="cancel-all-dialog-description">Request cancellation for ${confirmation.count} working order${confirmation.count === 1 ? "" : "s"}? Each request remains subject to broker acknowledgement and reconciliation.</p><label>Confirmation phrase<input data-cancel-all-confirmation-input maxlength="7" autocomplete="off" autofocus /></label>${confirmation.error ? `<div class="error" role="alert">${escapeHtml(confirmation.error)}</div>` : ""}<footer><button type="button" data-cancel-all-dialog-cancel>Cancel</button><button type="button" data-cancel-all-dialog-submit class="danger">Cancel all orders</button></footer></section></div>`;
}

function replaceOrderDialogMarkup(): string {
  const dialog = replaceOrderDialog;
  if (!dialog) return "";
  return `<div class="workspace-dialog-backdrop" data-replace-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="replace-dialog-title" aria-describedby="replace-dialog-description"><header><h2 id="replace-dialog-title">Replace order</h2><button type="button" data-replace-dialog-close aria-label="Close replace order dialog">×</button></header><p id="replace-dialog-description">Edit quantity and limit price for order ${escapeHtml(dialog.clientOrderId)}. Submission remains subject to broker capabilities, risk checks, and reconciliation.</p><label>Replacement quantity (ticks)<input data-replace-quantity-input type="number" min="1" step="1" value="${dialog.quantity}" autofocus /></label><label>Replacement limit price (ticks)<input data-replace-limit-input type="number" min="1" step="1" value="${dialog.limit ?? ""}" placeholder="Leave blank for market" /></label>${dialog.error ? `<div class="error" role="alert">${escapeHtml(dialog.error)}</div>` : ""}<footer><button type="button" data-replace-dialog-cancel>Cancel</button><button type="button" data-replace-dialog-submit>Replace order</button></footer></section></div>`;
}

function chartTemplateDialogMarkup(): string {
  const dialog = chartTemplateDialog;
  if (!dialog) return "";
  const deleting = dialog.mode === "delete";
  return `<div class="workspace-dialog-backdrop" data-chart-template-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="chart-template-dialog-title" aria-describedby="chart-template-dialog-description"><header><h2 id="chart-template-dialog-title">${deleting ? "Delete chart template" : "Save chart template"}</h2><button type="button" data-chart-template-dialog-close aria-label="Close chart template dialog">×</button></header><p id="chart-template-dialog-description">${deleting ? `Delete template “${escapeHtml(dialog.initialName)}”? This only removes chart presentation preferences.` : "Use 1–64 characters; saving an existing name replaces its preferences."}</p>${deleting ? "" : `<label>Template name<input data-chart-template-name maxlength="64" value="${escapeHtml(dialog.initialName)}" autocomplete="off" autofocus /></label>`}${dialog.error ? `<div class="error" role="alert">${escapeHtml(dialog.error)}</div>` : ""}<footer><button type="button" data-chart-template-dialog-cancel>Cancel</button><button type="button" data-chart-template-dialog-submit class="${deleting ? "danger" : ""}">${deleting ? "Delete template" : "Save template"}</button></footer></section></div>`;
}

function lifecycleDialogMarkup(): string {
  const dialog = lifecycleDialog;
  if (!dialog) return "";
  return `<div class="workspace-dialog-backdrop" data-lifecycle-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="lifecycle-dialog-title" aria-describedby="lifecycle-dialog-description"><header><h2 id="lifecycle-dialog-title">Transition ${dialog.kind}</h2><button type="button" data-lifecycle-dialog-close aria-label="Close lifecycle dialog">×</button></header><p id="lifecycle-dialog-description">Transition ${escapeHtml(dialog.id)} to ${escapeHtml(dialog.lifecycle)}. This changes runtime eligibility and requires recorded evidence.</p><label>Confirmation phrase<input data-lifecycle-confirmation maxlength="7" autocomplete="off" autofocus /></label><label>Evidence reference<input data-lifecycle-evidence maxlength="128" placeholder="Run, report, or approval ID" /></label>${dialog.error ? `<div class="error" role="alert">${escapeHtml(dialog.error)}</div>` : ""}<footer><button type="button" data-lifecycle-dialog-cancel>Cancel</button><button type="button" data-lifecycle-dialog-submit>Transition</button></footer></section></div>`;
}

function modelEvidenceDialogMarkup(): string {
  const dialog = modelEvidenceDialog;
  if (!dialog) return "";
  return `<div class="workspace-dialog-backdrop" data-model-evidence-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="model-evidence-dialog-title" aria-describedby="model-evidence-dialog-description"><header><h2 id="model-evidence-dialog-title">${dialog.operation === "canary" ? "Canary" : "Validate"} model</h2><button type="button" data-model-evidence-dialog-close aria-label="Close model evidence dialog">×</button></header><p id="model-evidence-dialog-description">${escapeHtml(dialog.modelId)} version ${escapeHtml(dialog.version)} requires a recorded evidence reference before this operation can proceed.</p><label>Evidence reference<input data-model-evidence-input maxlength="128" autocomplete="off" placeholder="Run, report, or approval ID" autofocus /></label>${dialog.error ? `<div class="error" role="alert">${escapeHtml(dialog.error)}</div>` : ""}<footer><button type="button" data-model-evidence-dialog-cancel>Cancel</button><button type="button" data-model-evidence-dialog-submit>${dialog.operation === "canary" ? "Run canary" : "Validate model"}</button></footer></section></div>`;
}

function autonomyModeDialogMarkup(): string {
  if (!autonomyModeDialog) return "";
  return `<div class="workspace-dialog-backdrop" data-autonomy-dialog-close><section class="workspace-dialog" role="dialog" aria-modal="true" aria-labelledby="autonomy-dialog-title" aria-describedby="autonomy-dialog-description"><header><h2 id="autonomy-dialog-title">Enable autonomous mode</h2><button type="button" data-autonomy-dialog-close aria-label="Close autonomous mode dialog">×</button></header><p id="autonomy-dialog-description">The coordinator may initiate actions, but every plan still passes policy, portfolio, risk, execution, broker, and reconciliation checks. The LLM never bypasses deterministic safeguards.</p><label>Confirmation phrase<input data-autonomy-confirmation maxlength="7" autocomplete="off" autofocus /></label>${autonomyModeDialog.error ? `<div class="error" role="alert">${escapeHtml(autonomyModeDialog.error)}</div>` : ""}<footer><button type="button" data-autonomy-dialog-cancel>Cancel</button><button type="button" data-autonomy-dialog-submit class="danger">Enable autonomous mode</button></footer></section></div>`;
}

async function refreshAlerts(): Promise<void> {
  try {
    alerts = await commands.getAlerts();
    alertsDegraded = false;
    const now = Date.now();
    for (const alert of alerts) {
      if (messageStationExpiry.has(alert.alertId)) continue;
      const durationMs = alert.severity >= 2 ? (alert.severity === 2 ? 8_000 : Number.POSITIVE_INFINITY) : 4_000;
      messageStationExpiry.set(alert.alertId, now + durationMs);
    }
    if (messageStationExpiry.size > 4096) {
      const activeIds = new Set(alerts.map((alert) => alert.alertId));
      for (const id of messageStationExpiry.keys()) if (!activeIds.has(id)) messageStationExpiry.delete(id);
    }
    const known = new Map(messageHistory.map((record) => [record.alert.alertId, record]));
    for (const alert of alerts) if (!known.has(alert.alertId)) known.set(alert.alertId, { alert, acknowledged: false });
    messageHistory = [...known.values()].sort((left, right) => right.alert.occurredMs - left.alert.occurredMs).slice(0, 256);
    deliverDesktopAlerts(alerts);
    render();
  } catch {
    // Alert transport degradation must not interrupt chart/order rendering, but
    // it must remain visible so operators do not mistake stale alerts for safety.
    alertsDegraded = true;
    render();
  }
}

async function refreshNewsProviderStatuses(): Promise<void> {
  try {
    newsProviderStatuses = await commands.getNewsProviderStatuses();
    render();
  } catch {
    // Provider-status degradation must not interrupt trading controls.
  }
}

async function refreshSupervisorStatuses(): Promise<void> {
  try {
    supervisorStatuses = await commands.getSupervisorStatuses();
    render();
  } catch {
    // Supervisor-status degradation must not interrupt trading controls.
  }
}

async function refreshBrokerStatus(): Promise<void> {
  try { brokerStatus = await commands.getBrokerStatus(); render(); } catch { /* broker status is observational */ }
}

async function refreshRiskPolicyStatus(): Promise<void> {
  try {
    riskPolicyRevisions = await commands.getRiskPolicyStatus();
    render();
  } catch {
    // Risk-policy status degradation must not interrupt trading controls.
  }
}

function configuredUiStatusPollMs(): number {
  const value = Number(configNumericValue(configSnapshot?.cfg_text ?? "", "ui.status_poll_ms", "5000"));
  return Number.isSafeInteger(value) && value >= 1_000 && value <= 60_000 ? value : 5_000;
}

let statusRefreshTimer: ReturnType<typeof setTimeout> | undefined;
let alertRefreshTimer: ReturnType<typeof setTimeout> | undefined;
function scheduleAlertRefresh(): void {
  if (alertRefreshTimer !== undefined) clearTimeout(alertRefreshTimer);
  alertRefreshTimer = setTimeout(async () => {
    alertRefreshTimer = undefined;
    await refreshAlerts();
    scheduleAlertRefresh();
  }, configuredAlertPollMs());
}
function scheduleStatusRefresh(): void {
  if (statusRefreshTimer !== undefined) clearTimeout(statusRefreshTimer);
  statusRefreshTimer = setTimeout(async () => {
    statusRefreshTimer = undefined;
    await Promise.allSettled([
      refreshNewsProviderStatuses(),
      refreshSupervisorStatuses(),
      refreshBrokerStatus(),
      refreshRiskPolicyStatus(),
      commands.listStrategyResolutions().then((runs) => { resolutionHistory = runs; render(); }),
      commands.listStrategyExecutionSummaries().then((runs) => { executionHistory = runs; render(); }),
    ]);
    scheduleStatusRefresh();
  }, configuredUiStatusPollMs());
}

function deliverDesktopAlerts(next: readonly AlertSnapshot[]): void {
  for (const alert of next) {
    if (deliveredAlertIds.has(alert.alertId)) continue;
    deliveredAlertIds.add(alert.alertId);
    if (nativeAlertsEnabled && typeof Notification !== "undefined" && Notification.permission === "granted") {
      try {
        new Notification(`${alert.source} · ${alert.severity === 3 ? "Critical" : alert.severity === 2 ? "Warning" : "Info"}`, { body: alert.message, tag: alert.dedupeKey });
      } catch {
        // Browser/desktop notification failures never affect alert acknowledgement.
      }
    }
    if (soundAlertsEnabled && soundSeverityEnabled[alert.severity] !== false && alertAudioContext) {
      try {
        const oscillator = alertAudioContext.createOscillator();
        const gain = alertAudioContext.createGain();
        oscillator.frequency.value = alert.severity === 3 ? 880 : alert.severity === 2 ? 660 : 440;
        gain.gain.setValueAtTime(0.04, alertAudioContext.currentTime);
        gain.gain.exponentialRampToValueAtTime(0.001, alertAudioContext.currentTime + 0.18);
        oscillator.connect(gain).connect(alertAudioContext.destination);
        oscillator.start();
        oscillator.stop(alertAudioContext.currentTime + 0.18);
      } catch {
        // Audio devices may be unavailable or autoplay-blocked.
      }
    }
  }
  // Bound local delivery memory while retaining enough IDs to suppress repeated polls.
  if (deliveredAlertIds.size > 4096) {
    const retained = new Set(next.map((alert) => alert.alertId));
    for (const id of deliveredAlertIds) if (!retained.has(id)) deliveredAlertIds.delete(id);
  }
}

const memoryStorage = new Map<string, string>();
const layoutStorage = {
  getItem: (key: string): string | null => {
    try {
      return globalThis.localStorage?.getItem(key) ?? memoryStorage.get(key) ?? null;
    } catch {
      return memoryStorage.get(key) ?? null;
    }
  },
  setItem: (key: string, value: string): void => {
    memoryStorage.set(key, value);
    try {
      globalThis.localStorage?.setItem(key, value);
    } catch {
      // Private browsing or a locked-down WebView may reject localStorage;
      // the in-memory copy remains valid for this session.
    }
  },
  removeItem: (key: string): void => {
    memoryStorage.delete(key);
    try {
      globalThis.localStorage?.removeItem(key);
    } catch {
      // Private browsing may reject removal; in-memory state remains valid.
    }
  },
};
const ANALYST_CONTEXT_STORAGE_KEY = "insidertrader.analyst.context.v1";
const ANALYST_CONTEXT_SCHEMA_VERSION = 1;
const ANALYST_CONTEXT_IDS: readonly AnalystContextId[] = ["symbol", "timeframe", "cursor", "news", "strategies", "positions"];
function restoreAnalystContext(): void {
  const raw = layoutStorage.getItem(ANALYST_CONTEXT_STORAGE_KEY);
  if (!raw) return;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;
    const candidate = parsed as Record<string, unknown>;
    if (candidate.version !== ANALYST_CONTEXT_SCHEMA_VERSION || !Array.isArray(candidate.enabled)) return;
    const enabled = new Set(candidate.enabled.filter((value): value is AnalystContextId => typeof value === "string" && ANALYST_CONTEXT_IDS.includes(value as AnalystContextId)));
    analystContextEnabled.clear();
    for (const id of ANALYST_CONTEXT_IDS) if (enabled.has(id)) analystContextEnabled.add(id);
  } catch {
    // Corrupt presentation state is ignored; runtime defaults remain safe.
  }
}
function persistAnalystContext(): void {
  layoutStorage.setItem(ANALYST_CONTEXT_STORAGE_KEY, JSON.stringify({ version: ANALYST_CONTEXT_SCHEMA_VERSION, enabled: ANALYST_CONTEXT_IDS.filter((id) => analystContextEnabled.has(id)) }));
}
restoreAnalystContext();
persistAnalystContext();
hotkeys = loadHotkeys();
persistHotkeys();
let colorblindMode = layoutStorage.getItem(COLORBLIND_STORAGE_KEY) === "true";
document.documentElement.dataset.colorblind = colorblindMode ? "true" : "false";
const storedFontScale = Number(layoutStorage.getItem(FONT_SCALE_STORAGE_KEY));
let fontScale = Number.isFinite(storedFontScale) && [0.9, 1, 1.1].includes(storedFontScale) ? storedFontScale : 1;
document.documentElement.style.setProperty("--ui-font-scale", String(fontScale));
const storedOrderDefaults = (() => {
  try {
    const parsed: unknown = JSON.parse(layoutStorage.getItem(ORDER_DEFAULTS_STORAGE_KEY) ?? "null");
    if (!parsed || typeof parsed !== "object") return undefined;
    const candidate = parsed as Record<string, unknown>;
    const type = candidate.type === "limit" ? "limit" : candidate.type === "market" ? "market" : undefined;
    const quantity = typeof candidate.quantity === "number" && Number.isSafeInteger(candidate.quantity) && candidate.quantity >= 1 && candidate.quantity <= 1_000_000 ? candidate.quantity : undefined;
    return type && quantity ? { type, quantity } : undefined;
  } catch { return undefined; }
})();
let defaultOrderType: "market" | "limit" = storedOrderDefaults?.type ?? "market";
let defaultOrderQuantity = storedOrderDefaults?.quantity ?? 1;
const WORKSPACE_PRESETS: readonly WorkspacePreset[] = ["Trading", "MultiChart", "News", "Strategies", "Autonomy", "Execution", "Research", "Scalping", "Swing", "Backtest"];
type CustomWorkspace = { readonly name: string; readonly base: WorkspacePreset };
const CUSTOM_WORKSPACES_KEY = "insidertrader.custom-workspaces.v1";
const WORKSPACE_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9 _-]{0,31}$/;
function loadCustomWorkspaces(): CustomWorkspace[] {
  try {
    const parsed: unknown = JSON.parse(layoutStorage.getItem(CUSTOM_WORKSPACES_KEY) ?? "null");
    const rawValues = Array.isArray(parsed)
      ? parsed
      : parsed && typeof parsed === "object" && (parsed as Record<string, unknown>).version === 1 && Array.isArray((parsed as Record<string, unknown>).workspaces)
        ? (parsed as Record<string, unknown>).workspaces
        : [];
    const seen = new Set<string>(WORKSPACE_PRESETS);
    const values: CustomWorkspace[] = [];
    for (const value of rawValues) {
      if (!value || typeof value !== "object") continue;
      const candidate = value as Record<string, unknown>;
      if (typeof candidate.name !== "string" || !WORKSPACE_NAME_PATTERN.test(candidate.name) || seen.has(candidate.name)) continue;
      if (typeof candidate.base !== "string" || !WORKSPACE_PRESETS.includes(candidate.base as WorkspacePreset)) continue;
      seen.add(candidate.name);
      values.push({ name: candidate.name, base: candidate.base as WorkspacePreset });
      if (values.length >= 8) break;
    }
    return values;
  } catch { return []; }
}
let customWorkspaces = loadCustomWorkspaces();
function persistCustomWorkspaces(): void { layoutStorage.setItem(CUSTOM_WORKSPACES_KEY, JSON.stringify({ version: 1, workspaces: customWorkspaces })); }
const allWorkspaceNames = (): string[] => [...WORKSPACE_PRESETS, ...customWorkspaces.map((workspace) => workspace.name)];
const WORKSPACE_TAB_ORDER_KEY = "insidertrader.workspace-tabs.v1";
function loadWorkspaceTabOrder(): string[] {
  const fallback = allWorkspaceNames();
  const raw = layoutStorage.getItem(WORKSPACE_TAB_ORDER_KEY);
  if (!raw) return fallback;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed) || parsed.length !== fallback.length || parsed.some((value) => typeof value !== "string")) return fallback;
    const values = parsed as string[];
    return fallback.every((preset) => values.includes(preset)) ? values : fallback;
  } catch {
    return fallback;
  }
}
let workspaceTabOrder = loadWorkspaceTabOrder();
layoutStorage.setItem(WORKSPACE_TAB_ORDER_KEY, JSON.stringify(workspaceTabOrder));
let draggedWorkspaceTab: string | undefined;
const RIGHT_DOCK_WIDTH_KEY = "insidertrader.right-dock-width.v1";
const RIGHT_DOCK_MIN_WIDTH = 240;
const RIGHT_DOCK_MAX_WIDTH = 560;
function loadRightDockWidth(): number {
  const value = Number(layoutStorage.getItem(RIGHT_DOCK_WIDTH_KEY));
  return Number.isFinite(value) ? Math.round(Math.max(RIGHT_DOCK_MIN_WIDTH, Math.min(RIGHT_DOCK_MAX_WIDTH, value))) : 320;
}
let rightDockWidth = loadRightDockWidth();
const workspacePersistence = new WorkspacePersistence(layoutStorage);
newsView = loadNewsView();
persistNewsView();
rightDockTab = loadRightDockTab();
persistRightDockTab();
nativeAlertsEnabled = layoutStorage.getItem(ALERT_NATIVE_STORAGE_KEY) === "true";
soundAlertsEnabled = layoutStorage.getItem(ALERT_SOUND_STORAGE_KEY) === "true";
try {
  const storedSeverity = JSON.parse(layoutStorage.getItem(ALERT_SOUND_SEVERITY_STORAGE_KEY) ?? "null") as unknown;
  if (Array.isArray(storedSeverity) && storedSeverity.length === 4 && storedSeverity.every((value) => typeof value === "boolean")) soundSeverityEnabled = Object.freeze([...storedSeverity]);
} catch {
  // Invalid notification presentation preferences fall back to all severities enabled.
}
let workspaceLayout = loadWorkspaceLayout(layoutStorage, "Trading", createWorkspacePreset("Trading"));
workspacePersistence.schedule(workspaceLayout);
window.addEventListener("beforeunload", () => workspacePersistence.flush(), { once: true });

type LinkGroup = "none" | "red" | "blue" | "green" | "yellow";
const LINK_GROUP_STORAGE_KEY = "insidertrader.panel-links.v2";
const LINK_GROUP_LEGACY_STORAGE_KEY = "insidertrader.panel-links.v1";
const LINK_GROUP_SCHEMA_VERSION = 2;
const LINK_GROUPS: readonly LinkGroup[] = ["none", "red", "blue", "green", "yellow"];
const LINKABLE_PANELS = ["chart", "chart-secondary", "chart-tertiary", "chart-quaternary", "watchlist", "global-search", "screener", "heatmap", "news", "news-detail", "order-ticket", "metrics", "strategy-inspector", "depth", "time-sales", "ai-analyst"] as const;
type LinkablePanel = (typeof LINKABLE_PANELS)[number];
function loadPanelLinks(): Record<LinkablePanel, LinkGroup> {
  const fallback = Object.fromEntries(LINKABLE_PANELS.map((panelId) => [panelId, "red"])) as Record<LinkablePanel, LinkGroup>;
  const raw = layoutStorage.getItem(LINK_GROUP_STORAGE_KEY) ?? layoutStorage.getItem(LINK_GROUP_LEGACY_STORAGE_KEY);
  if (!raw) return fallback;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return fallback;
    const source = (parsed as Record<string, unknown>).version === LINK_GROUP_SCHEMA_VERSION
      ? (parsed as Record<string, unknown>).links
      : parsed;
    if (!source || typeof source !== "object") return fallback;
    for (const panelId of LINKABLE_PANELS) {
      const candidate = (source as Record<string, unknown>)[panelId];
      if (typeof candidate === "string" && LINK_GROUPS.includes(candidate as LinkGroup)) fallback[panelId] = candidate as LinkGroup;
    }
  } catch {
    return fallback;
  }
  return fallback;
}
let panelLinks = loadPanelLinks();
persistPanelLinks();
layoutStorage.removeItem(LINK_GROUP_LEGACY_STORAGE_KEY);
const panelSymbols: Partial<Record<LinkablePanel, string>> = {};
for (const panelId of LINKABLE_PANELS) panelSymbols[panelId] = store.state.selectedSymbol;
const panelTimeframes: Partial<Record<LinkablePanel, string>> = {};
for (const panelId of LINKABLE_PANELS) panelTimeframes[panelId] = store.state.selectedTimeframe;
function persistPanelLinks(): void { layoutStorage.setItem(LINK_GROUP_STORAGE_KEY, JSON.stringify({ version: LINK_GROUP_SCHEMA_VERSION, links: panelLinks })); }
function symbolFor(panelId: LinkablePanel): string { return panelSymbols[panelId] ?? store.state.selectedSymbol; }
function timeframeFor(panelId: LinkablePanel): string { return panelTimeframes[panelId] ?? store.state.selectedTimeframe; }
async function loadNewsPage(scope: "relevant" | "all", symbol: string, afterCursor?: string): Promise<void> {
  if (newsBusy) {
    if (afterCursor === undefined) queuedNewsRequest = { scope, symbol };
    return;
  }
  if (afterCursor === undefined) {
    store.resetNewsPage();
    newsLastSuccessAtMs = undefined;
  }
  newsRetryScope = scope;
  newsRetrySymbol = symbol;
  newsRetryCursor = afterCursor;
  newsBusy = true;
  newsError = "";
  render();
  try {
    await commands.loadNewsPage(scope, symbol, afterCursor);
    newsLastSuccessAtMs = Date.now();
  } catch (error) {
    newsError = error instanceof Error ? error.message : "news provider unavailable";
  } finally {
    newsBusy = false;
    render();
    const queued = queuedNewsRequest;
    queuedNewsRequest = undefined;
    if (queued) void loadNewsPage(queued.scope, queued.symbol, queued.cursor);
  }
}
function setPanelSymbol(source: LinkablePanel, symbol: string): void {
  const normalized = symbol.trim().toUpperCase();
  if (!/^[A-Z0-9.\-]{1,16}$/.test(normalized) && !/^\d{1,39}$/.test(normalized)) return;
  const group = panelLinks[source];
  for (const panelId of LINKABLE_PANELS) {
    if (panelId === source || (group !== "none" && panelLinks[panelId] === group)) panelSymbols[panelId] = normalized;
  }
  if (panelSymbols.chart === normalized) {
    store.selectSymbol(normalized);
    chartPreferences = loadChartPreferences();
    store.replaceChartDrawings(loadAndMigrateDrawings());
    void loadNewsPage(store.state.newsScope, normalized);
  }
}
function setPanelTimeframe(source: LinkablePanel, timeframe: string): void {
  if (!/^\d{1,4}(s|m|h|d|w)$/.test(timeframe)) return;
  const group = panelLinks[source];
  for (const panelId of LINKABLE_PANELS) {
    if (panelId === source || (group !== "none" && panelLinks[panelId] === group)) panelTimeframes[panelId] = timeframe;
  }
  if (panelTimeframes.chart === timeframe) {
    store.selectTimeframe(timeframe);
    store.replaceChartDrawings(loadAndMigrateDrawings());
  }
}

const WATCHLIST_STORAGE_KEY = "insidertrader.watchlist.v2";
const WATCHLIST_LEGACY_STORAGE_KEY = "insidertrader.watchlist.v1";
const MAX_WATCHLIST_SYMBOLS = 500;
const WATCHLIST_SCHEMA_VERSION = 2;
function loadWatchlist(): string[] {
  const raw = layoutStorage.getItem(WATCHLIST_STORAGE_KEY) ?? layoutStorage.getItem(WATCHLIST_LEGACY_STORAGE_KEY);
  if (!raw) return ["AAPL"];
  try {
    const parsed: unknown = JSON.parse(raw);
    const values = Array.isArray(parsed)
      ? parsed
      : parsed && typeof parsed === "object" && (parsed as Record<string, unknown>).version === WATCHLIST_SCHEMA_VERSION
        ? (parsed as Record<string, unknown>).symbols
        : undefined;
    if (!Array.isArray(values)) return ["AAPL"];
    const symbols = [...new Set(values.filter((value): value is string => typeof value === "string")
      .map((value) => value.trim().toUpperCase())
      .filter((value) => /^[A-Z0-9.\-]{1,16}$/.test(value)))].slice(0, MAX_WATCHLIST_SYMBOLS);
    return symbols.length ? symbols : ["AAPL"];
  } catch {
    return ["AAPL"];
  }
}
let watchlistSymbols = loadWatchlist();
function persistWatchlist(): void {
  layoutStorage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify({ version: WATCHLIST_SCHEMA_VERSION, symbols: watchlistSymbols.slice(0, MAX_WATCHLIST_SYMBOLS) }));
}
persistWatchlist();
layoutStorage.removeItem(WATCHLIST_LEGACY_STORAGE_KEY);
const TIMEFRAME_STORAGE_KEY = "insidertrader.chart-timeframe.v1";
const VALID_TIMEFRAMES = ["1m", "5m", "15m", "1h", "1d"] as const;
function loadTimeframe(): string {
  const value = layoutStorage.getItem(TIMEFRAME_STORAGE_KEY)?.trim().toLowerCase();
  return value && VALID_TIMEFRAMES.includes(value as (typeof VALID_TIMEFRAMES)[number]) ? value : "1m";
}
store.selectTimeframe(loadTimeframe());

const DRAWING_STORAGE_PREFIX = "insidertrader.chart-drawings.v2:";
const DRAWING_LEGACY_STORAGE_PREFIX = "insidertrader.chart-drawings.v1:";
const DRAWING_SCHEMA_VERSION = 2;
const drawingStorageKey = (): string => `${DRAWING_STORAGE_PREFIX}${store.state.selectedSymbol}:${store.state.selectedTimeframe}`;
const drawingLegacyStorageKey = (): string => `${DRAWING_LEGACY_STORAGE_PREFIX}${store.state.selectedSymbol}:${store.state.selectedTimeframe}`;
const CHART_PREF_STORAGE_PREFIX = "insidertrader.chart-preferences.v2:";
const CHART_PREF_LEGACY_PREFIX = "insidertrader.chart-preferences.v1:";
const CHART_PREF_SCHEMA_VERSION = 2;
type ChartDisplayPreferences = { readonly mode: ChartRenderMode; readonly gridlineDensity: ChartGridlineDensity; readonly showNews: boolean; readonly showStrategies: boolean; readonly showMetrics: boolean };
type ChartTemplate = { readonly name: string; readonly preferences: ChartDisplayPreferences };
const CHART_TEMPLATE_STORAGE_KEY = "insidertrader.chart-templates.v1";
const CHART_TEMPLATE_SCHEMA_VERSION = 1;
const MAX_CHART_TEMPLATES = 32;
function loadChartTemplates(): ChartTemplate[] {
  const raw = layoutStorage.getItem(CHART_TEMPLATE_STORAGE_KEY);
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    const values = parsed && typeof parsed === "object" && (parsed as Record<string, unknown>).version === CHART_TEMPLATE_SCHEMA_VERSION
      ? (parsed as Record<string, unknown>).templates
      : undefined;
    if (!Array.isArray(values) || values.length > MAX_CHART_TEMPLATES) return [];
    return values.filter((value): value is ChartTemplate => {
      if (!value || typeof value !== "object") return false;
      const candidate = value as Record<string, unknown>;
      const preferences = candidate.preferences;
      if (typeof candidate.name !== "string" || candidate.name.trim().length === 0 || candidate.name.length > 64
        || !preferences || typeof preferences !== "object") return false;
      const settings = preferences as Record<string, unknown>;
      return (settings.mode === "candles" || settings.mode === "bars" || settings.mode === "line" || settings.mode === "area")
        && (settings.gridlineDensity === undefined || settings.gridlineDensity === "none" || settings.gridlineDensity === "low" || settings.gridlineDensity === "high")
        && typeof settings.showNews === "boolean" && typeof settings.showStrategies === "boolean" && typeof settings.showMetrics === "boolean";
    }).map((value) => ({ name: value.name.trim(), preferences: { ...value.preferences, gridlineDensity: value.preferences.gridlineDensity ?? "low" } }));
  } catch {
    return [];
  }
}
let chartTemplates = loadChartTemplates();
function persistChartTemplates(): void {
  layoutStorage.setItem(CHART_TEMPLATE_STORAGE_KEY, JSON.stringify({ version: CHART_TEMPLATE_SCHEMA_VERSION, templates: chartTemplates.slice(0, MAX_CHART_TEMPLATES) }));
}
persistChartTemplates();
const chartPreferenceKey = (): string => `${CHART_PREF_STORAGE_PREFIX}${store.state.selectedSymbol}:${store.state.selectedTimeframe}`;
const chartPreferenceLegacyKey = (): string => `${CHART_PREF_LEGACY_PREFIX}${store.state.selectedSymbol}:${store.state.selectedTimeframe}`;
function loadChartPreferences(): ChartDisplayPreferences {
  const raw = layoutStorage.getItem(chartPreferenceKey()) ?? layoutStorage.getItem(chartPreferenceLegacyKey());
  if (!raw) return { mode: "candles", gridlineDensity: "low", showNews: true, showStrategies: true, showMetrics: true };
  try {
    const parsed = JSON.parse(raw) as unknown;
    const candidate = parsed && typeof parsed === "object" && (parsed as Record<string, unknown>).version === CHART_PREF_SCHEMA_VERSION
      ? (parsed as Record<string, unknown>).preferences as Record<string, unknown>
      : parsed as Record<string, unknown>;
    if (!candidate || typeof candidate !== "object") return { mode: "candles", gridlineDensity: "low", showNews: true, showStrategies: true, showMetrics: true };
    const mode = candidate.mode === "bars" || candidate.mode === "line" || candidate.mode === "area" ? candidate.mode : "candles";
    const gridlineDensity: ChartGridlineDensity = candidate.gridlineDensity === "none" || candidate.gridlineDensity === "high" ? candidate.gridlineDensity : "low";
    return { mode, gridlineDensity, showNews: candidate.showNews !== false, showStrategies: candidate.showStrategies !== false, showMetrics: candidate.showMetrics !== false };
  } catch {
    return { mode: "candles", gridlineDensity: "low", showNews: true, showStrategies: true, showMetrics: true };
  }
}
let chartPreferences = loadChartPreferences();
function persistChartPreferences(): void {
  layoutStorage.setItem(chartPreferenceKey(), JSON.stringify({ version: CHART_PREF_SCHEMA_VERSION, preferences: chartPreferences }));
}
persistChartPreferences();
layoutStorage.removeItem(chartPreferenceLegacyKey());
function loadDrawings(): readonly ChartDrawing[] {
  const raw = layoutStorage.getItem(drawingStorageKey()) ?? layoutStorage.getItem(drawingLegacyStorageKey());
  if (!raw) return [];
  try {
    const parsed: unknown = JSON.parse(raw);
    const values = Array.isArray(parsed)
      ? parsed
      : parsed && typeof parsed === "object" && (parsed as Record<string, unknown>).version === DRAWING_SCHEMA_VERSION
        ? (parsed as Record<string, unknown>).drawings
        : undefined;
    if (!Array.isArray(values) || values.length > 256) return [];
    return values.filter((value): value is ChartDrawing => {
      if (!value || typeof value !== "object") return false;
      const drawing = value as Record<string, unknown>;
      return typeof drawing.id === "string" && drawing.id.length <= 128
        && (drawing.kind === "horizontal" || drawing.kind === "trendline")
        && Number.isSafeInteger(drawing.startTimeMs) && Number.isSafeInteger(drawing.startPriceTicks)
        && typeof drawing.color === "string" && /^#[0-9a-f]{6}$/i.test(drawing.color);
    });
  } catch {
    return [];
  }
}
function loadAndMigrateDrawings(): readonly ChartDrawing[] {
  const drawings = loadDrawings();
  // Persist before deleting the legacy key for every symbol/timeframe context,
  // not only the context active during initial application startup.
  layoutStorage.setItem(drawingStorageKey(), JSON.stringify({ version: DRAWING_SCHEMA_VERSION, drawings: drawings.slice(0, 256) }));
  layoutStorage.removeItem(drawingLegacyStorageKey());
  return drawings;
}
let drawingPersistTimer: ReturnType<typeof setTimeout> | undefined;
function persistDrawings(drawings: readonly ChartDrawing[]): void {
  if (drawingPersistTimer !== undefined) clearTimeout(drawingPersistTimer);
  drawingPersistTimer = setTimeout(() => {
    drawingPersistTimer = undefined;
    layoutStorage.setItem(drawingStorageKey(), JSON.stringify({ version: DRAWING_SCHEMA_VERSION, drawings: drawings.slice(0, 256) }));
  }, 250);
}
const initialDrawings = loadAndMigrateDrawings();
store.replaceChartDrawings(initialDrawings);
persistDrawings(store.state.chart.drawings);

const WORKSPACE_CONTEXT_STORAGE_KEY = "insidertrader.workspace-context.v1";
type WorkspaceContext = { readonly symbol: string; readonly timeframe: string };
function loadWorkspaceContexts(): Record<string, WorkspaceContext> {
  const fallback: Record<string, WorkspaceContext> = {};
  const raw = layoutStorage.getItem(WORKSPACE_CONTEXT_STORAGE_KEY);
  if (!raw) return fallback;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return fallback;
    for (const [name, value] of Object.entries(parsed as Record<string, unknown>).slice(0, 16)) {
      if (!value || typeof value !== "object") continue;
      const candidate = value as Record<string, unknown>;
      const symbol = typeof candidate.symbol === "string" ? candidate.symbol.toUpperCase() : "";
      const timeframe = typeof candidate.timeframe === "string" ? candidate.timeframe : "";
      if (/^[A-Z0-9.\-]{1,16}$/.test(symbol) && VALID_TIMEFRAMES.includes(timeframe as (typeof VALID_TIMEFRAMES)[number])) fallback[name] = { symbol, timeframe };
    }
  } catch {
    return {};
  }
  return fallback;
}
let workspaceContexts = loadWorkspaceContexts();
function persistWorkspaceContexts(): void {
  layoutStorage.setItem(WORKSPACE_CONTEXT_STORAGE_KEY, JSON.stringify(Object.fromEntries(Object.entries(workspaceContexts).slice(0, 16))));
}
function switchWorkspace(name: string): void {
  // Commit the previous workspace before changing the persistence key; otherwise
  // a fast tab switch can replace its debounced write with the next workspace.
  workspacePersistence.flush();
  workspaceContexts[workspaceLayout.name] = { symbol: store.state.selectedSymbol, timeframe: store.state.selectedTimeframe };
  const base = WORKSPACE_PRESETS.includes(name as WorkspacePreset)
    ? name as WorkspacePreset
    : customWorkspaces.find((workspace) => workspace.name === name)?.base;
  if (!base) return;
  const preset = createWorkspacePreset(base);
  workspaceLayout = loadWorkspaceLayout(layoutStorage, name, preset);
  const context = workspaceContexts[name] ?? { symbol: store.state.selectedSymbol, timeframe: store.state.selectedTimeframe };
  panelSymbols.chart = context.symbol;
  panelTimeframes.chart = context.timeframe;
  store.selectSymbol(context.symbol);
  store.selectTimeframe(context.timeframe);
  chartPreferences = loadChartPreferences();
  store.replaceChartDrawings(loadAndMigrateDrawings());
  workspaceContexts[name] = context;
  persistWorkspaceContexts();
}

function panel(title: string, className: string, body: string): string {
  const panelId = (ALL_PANEL_IDS as readonly string[]).includes(className) ? ` data-panel-id="${className}" draggable="true"` : "";
  const linkControl = (LINKABLE_PANELS as readonly string[]).includes(className) ? `<label class="panel-link" title="Link symbol and timeframe"><span class="sr-only">Link group</span><select data-link-panel="${className}" aria-label="${title} link group">${LINK_GROUPS.map((group) => `<option value="${group}" ${panelLinks[className as LinkablePanel] === group ? "selected" : ""}>${group === "none" ? "Unlinked" : `Link ${group}`}</option>`).join("")}</select></label>` : "";
  return `<section id="panel-${className}" class="panel ${className}" aria-labelledby="panel-title-${className}"${panelId}><header class="panel-header"><h2 id="panel-title-${className}">${title}</h2><span class="panel-actions">${linkControl}<button type="button" class="panel-popout" data-popout-panel="${className}" aria-label="Pop out ${title}">↗</button><button type="button" class="panel-close" data-close-panel="${className}" aria-label="Close ${title}">×</button></span></header>${body}</section>`;
}

function escapeHtml(value: string): string {
  const entities: Record<string, string> = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#39;",
  };
  return value.replace(/[&<>"']/g, (character) => entities[character] ?? character);
}

function safeExternalUrl(value: string): string {
  if (value.length === 0 || new TextEncoder().encode(value).length > 2048 || /\s/.test(value)) return "#";
  try {
    const parsed = new URL(value);
    if (parsed.protocol !== "https:" || !parsed.hostname || parsed.username || parsed.password) return "#";
    return parsed.toString();
  } catch {
    return "#";
  }
}

function validConfiguredHttpsUrl(value: string): boolean {
  if (value.length === 0 || new TextEncoder().encode(value).length > 2048 || /\s/.test(value)) return false;
  try {
    const parsed = new URL(value);
    return parsed.protocol === "https:" && !!parsed.hostname && !parsed.username && !parsed.password;
  } catch {
    return false;
  }
}

function orderTicketMarkup(): string {
  const ticket = store.state.orderTicket;
  const preview = ticket?.preview;
  const draft = preview?.draft ?? ticket?.draft ?? {
    symbol: symbolFor("order-ticket"),
    side: "buy" as const,
    type: defaultOrderType,
    quantityTicks: defaultOrderQuantity,
  };
  const warnings = preview?.warnings.length
    ? `<ul class="warnings">${preview.warnings.map((warning) => `<li>${escapeHtml(warning)}</li>`).join("")}</ul>`
    : "";
  const previewExpired = preview !== undefined && preview.expiresAtMs <= Date.now();
  const submitDisabled = ticket?.status !== "ready" || !preview || previewExpired;
  return `<div class="metric"><span>State</span><span>${ticket?.status ?? "idle"}</span></div>
    <div class="order-fields">
      <label>Side<select data-order-side><option value="buy" ${draft.side === "buy" ? "selected" : ""}>Buy</option><option value="sell" ${draft.side === "sell" ? "selected" : ""}>Sell</option></select></label>
      <label>Type<select data-order-type><option value="market" ${draft.type === "market" ? "selected" : ""}>Market</option><option value="limit" ${draft.type === "limit" ? "selected" : ""}>Limit</option></select></label>
      <label>Quantity (ticks)<input data-order-quantity type="number" min="1" step="1" value="${draft.quantityTicks}" /></label>
      <label>Limit price (ticks)<input data-order-limit-price type="number" min="1" step="1" value="${draft.limitPriceTicks ?? ""}" ${draft.type === "market" ? "disabled" : ""} /></label>
    </div>
    <div class="metric"><span>Draft</span><span>${draft.side.toUpperCase()} ${escapeHtml(draft.symbol)} × ${draft.quantityTicks}</span></div>
    ${preview ? `<div class="metric"><span>Estimated notional</span><span>${preview.estimatedNotionalTicks ?? "—"}</span></div>
      <div class="metric"><span>Estimated cost</span><span>${preview.estimatedCostBps ?? "—"} bps</span></div><div class="metric"><span>Preview expiry</span><span class="${previewExpired ? "negative" : "muted"}">${previewExpired ? "Expired — preview again" : new Date(preview.expiresAtMs).toISOString()}</span></div>${warnings}` : ""}
    ${ticket?.error ? `<div class="error" role="alert">${escapeHtml(ticket.error)}</div>` : ""}
    ${ticket?.status === "submitted" ? `<div class="positive" role="status">Submitted ${escapeHtml(ticket.submittedOrderId ?? "")}</div>` : ""}
    <button data-manual-order>${ticket?.status === "previewing" ? "Previewing…" : "Preview order"}</button>
    <label class="confirmation">Type CONFIRM to submit
      <input data-confirmation aria-label="Order confirmation" autocomplete="off" />
    </label>
    <button data-submit-order ${submitDisabled ? "disabled" : ""}>Submit confirmed order</button>`;
}

function chartDrawingMarkup(): string {
  const drawings = store.state.chart.drawings;
  const latest = store.state.chart.candles.at(-1);
  const templates = chartTemplates.map((template) => `<option value="${escapeHtml(template.name)}">${escapeHtml(template.name)}</option>`).join("");
  return `<div class="chart-tools"><label>View<select data-chart-mode><option value="candles" ${chartPreferences.mode === "candles" ? "selected" : ""}>Candles</option><option value="line" ${chartPreferences.mode === "line" ? "selected" : ""}>Line</option><option value="area" ${chartPreferences.mode === "area" ? "selected" : ""}>Area</option></select></label><label>Grid<select data-chart-gridlines><option value="none" ${chartPreferences.gridlineDensity === "none" ? "selected" : ""}>None</option><option value="low" ${chartPreferences.gridlineDensity === "low" ? "selected" : ""}>Low</option><option value="high" ${chartPreferences.gridlineDensity === "high" ? "selected" : ""}>High</option></select></label><label>Timeframe<select data-chart-timeframe>${VALID_TIMEFRAMES.map((timeframe) => `<option value="${timeframe}" ${store.state.selectedTimeframe === timeframe ? "selected" : ""}>${timeframe}</option>`).join("")}</select></label><label>Template<select data-chart-template><option value="">Choose template</option>${templates}</select></label><button type="button" data-chart-template-save>Save template</button><button type="button" data-chart-template-delete ${chartTemplates.length ? "" : "disabled"}>Delete templates</button><button type="button" data-drawing-horizontal ${latest ? "" : "disabled"}>Add horizontal level</button><button type="button" data-drawing-clear ${drawings.length ? "" : "disabled"}>Clear drawings</button><label><input type="checkbox" data-chart-news ${chartPreferences.showNews ? "checked" : ""}/> News</label><label><input type="checkbox" data-chart-strategies ${chartPreferences.showStrategies ? "checked" : ""}/> Strategies</label><label><input type="checkbox" data-chart-metrics ${chartPreferences.showMetrics ? "checked" : ""}/> Metrics</label><span class="muted">${drawings.length} saved drawing${drawings.length === 1 ? "" : "s"} · ${chartTemplates.length} template${chartTemplates.length === 1 ? "" : "s"}</span></div>`;
}

function watchlistMarkup(): string {
  const start = Math.max(0, Math.floor(watchlistScrollTop / WATCHLIST_ROW_HEIGHT) - 4);
  const end = Math.min(watchlistSymbols.length, start + Math.ceil(WATCHLIST_VIEWPORT_HEIGHT / WATCHLIST_ROW_HEIGHT) + 8);
  const rows = watchlistSymbols.slice(start, end).map((symbol) => {
    const quote = store.state.quotes[symbol];
    return `<div class="quote"><button data-symbol="${escapeHtml(symbol)}">${escapeHtml(symbol)}</button><span>${quote ? quote.lastTicks : "—"}</span>${quote ? `<span class="muted">#${quote.sequence}</span>` : `<span class="muted">waiting</span>`}<button type="button" data-watchlist-remove="${escapeHtml(symbol)}" aria-label="Remove ${escapeHtml(symbol)}">×</button></div>`;
  }).join("");
  const feed = watchlistSymbols.length === 0
    ? `<div class="empty">Watchlist is empty</div>`
    : `<div class="watchlist-virtual-spacer" style="height:${watchlistSymbols.length * WATCHLIST_ROW_HEIGHT}px;position:relative"><div style="position:absolute;left:0;right:0;top:${start * WATCHLIST_ROW_HEIGHT}px">${rows}</div></div>`;
  return `<form class="watchlist-add" data-watchlist-form><input name="symbol" maxlength="16" pattern="[A-Za-z0-9.\-]{1,16}" placeholder="Add symbol" aria-label="Add symbol" required /><button type="submit">Add</button></form>${watchlistError ? `<div class="error" role="alert">${escapeHtml(watchlistError)}</div>` : ""}<div class="watchlist-viewport" data-watchlist-viewport style="height:${WATCHLIST_VIEWPORT_HEIGHT}px;overflow:auto">${feed}</div><div class="muted">${watchlistSymbols.length}/${MAX_WATCHLIST_SYMBOLS} symbols saved locally.</div>`;
}

function newsMarkup(): string {
  const state = store.state;
  const symbol = symbolFor("news");
  const portfolioSymbols = new Set(state.positions.map((position) => position.symbol));
  const items = state.news
    .filter((item) => newsView === "relevant"
      ? item.symbols.includes(symbol)
      : newsView === "watchlist"
        ? item.symbols.some((itemSymbol) => watchlistSymbols.includes(itemSymbol))
        : newsView === "portfolio"
          ? item.symbols.some((itemSymbol) => portfolioSymbols.has(itemSymbol))
          : true)
    .slice()
    .sort((left, right) => right.relevanceScore - left.relevanceScore || right.receivedAtMs - left.receivedAtMs)
    .slice(0, 100_000);
  const tabs = `<div class="news-tabs" role="tablist" aria-label="News view">
    ${(["relevant", "all", "watchlist", "portfolio"] as const).map((view, index) => `<button id="news-tab-${view}" data-news-view="${view}" role="tab" aria-setsize="4" aria-posinset="${index + 1}" aria-controls="news-feed-panel" aria-selected="${newsView === view}" tabindex="${newsView === view ? "0" : "-1"}">${view[0].toUpperCase()}${view.slice(1)}</button>`).join("")}
  </div>`;
  const start = Math.max(0, Math.floor(newsScrollTop / NEWS_ROW_HEIGHT) - 4);
  const end = Math.min(items.length, start + Math.ceil(NEWS_VIEWPORT_HEIGHT / NEWS_ROW_HEIGHT) + 8);
  const rows = items.slice(start, end).map((item) => {
    const pinned = state.pinnedNews.includes(item.id);
    const published = item.publishedAtMs ? new Date(item.publishedAtMs).toISOString() : "time unavailable";
    return `<article class="news-item" data-news-id="${escapeHtml(item.id)}">
      <div class="news-meta"><span>${escapeHtml(item.source)}</span><time datetime="${escapeHtml(published)}">${escapeHtml(published)}</time></div>
      <a href="${escapeHtml(safeExternalUrl(item.canonicalUrl))}" target="_blank" rel="noopener noreferrer" data-news-link>${escapeHtml(item.title)}</a>
      <button class="pin" data-news-pin="${escapeHtml(item.id)}" aria-label="${pinned ? "Unpin" : "Pin"} ${escapeHtml(item.title)}">${pinned ? "★" : "☆"}</button>
      <button data-news-detail="${escapeHtml(item.id)}" type="button" aria-label="Open details for ${escapeHtml(item.title)}">Details</button>
    </article>`;
  }).join("");
  const feed = items.length === 0
    ? `<div class="empty">No news for this scope</div>`
    : `<div class="news-virtual-spacer" style="height:${items.length * NEWS_ROW_HEIGHT}px;position:relative"><div class="news-virtual-window" style="position:absolute;left:0;right:0;top:${start * NEWS_ROW_HEIGHT}px">${rows}</div></div>`;
  const stale = newsLastSuccessAtMs !== undefined && Date.now() - newsLastSuccessAtMs >= configuredNewsStaleAfterMs();
  const status = newsBusy
    ? `<div class="muted" role="status" aria-live="polite">Loading news…</div>`
    : newsError
      ? `<div class="error" role="alert">News unavailable: ${escapeHtml(newsError)}. Cached items remain visible. <button type="button" data-news-retry aria-label="Retry news for ${escapeHtml(symbol)}">Retry</button></div>`
      : stale
        ? `<div class="warning" role="status">News feed is stale; retrying will refresh provider data.</div>`
        : "";
  const loadMore = state.newsHasMore ? `<button data-news-more type="button" ${newsBusy ? "disabled" : ""}>Load more news</button>` : "";
  const providerStatus = newsProviderStatuses.length
    ? `<div class="news-provider-status" aria-label="News provider health">${newsProviderStatuses.map((provider) => `<span class="provider-chip provider-${escapeHtml(provider.health)}">${escapeHtml(provider.providerId)} · ${escapeHtml(provider.health)}</span>`).join("")}</div>`
    : `<div class="muted">News provider health unavailable</div>`;
  return `${tabs}<div class="muted">Linked context: ${escapeHtml(symbol)} · ${escapeHtml(timeframeFor("news"))}</div>${providerStatus}${status}<div id="news-feed-panel" class="news-feed news-viewport" data-news-viewport role="tabpanel" aria-labelledby="news-tab-${newsView}" style="height:${NEWS_VIEWPORT_HEIGHT}px;overflow:auto" aria-live="polite" aria-busy="${newsBusy}">${feed}</div>${loadMore}`;
}

function newsDetailMarkup(): string {
  const detail = selectedNewsDetail;
  if (!detail) return "";
  const current = detail.current;
  const versions = detail.versions.length > 1
    ? `<div class="muted">${detail.versions.length} retained immutable versions</div>`
    : "";
  const related = detail.relatedItemIds.length
    ? `<div class="muted">Related exact-title items: ${detail.relatedItemIds.map(escapeHtml).join(", ")}</div>`
    : "";
  return panel("News Detail", "news-detail", `<div class="news-meta"><span>${escapeHtml(current.provider)} · ${escapeHtml(current.source)}</span><button data-news-detail-close type="button">Close</button></div><h3>${escapeHtml(current.title)}</h3>${current.summaryText ? `<p>${escapeHtml(current.summaryText)}</p>` : ""}<div class="muted">Cluster ${escapeHtml(detail.clusterId)} · content ${escapeHtml(current.contentHash)}</div>${versions}${related}`);
}

function analystMarkup(): string {
  const state = store.state;
  const analystStale = analystReceivedAtMs !== undefined && Date.now() - analystReceivedAtMs >= configuredAnalystDisplayTtlMs();
  const suggestions = ["Explain move", "Summarize relevant news", "Compare strategies", "Why is risk high?", "What changed since open?", "Analyze this region"] as const;
  const contextValues: Record<AnalystContextId, string> = {
    symbol: state.selectedSymbol,
    timeframe: state.selectedTimeframe,
    cursor: state.cursor,
    news: `${state.news.length} linked news items (${state.pinnedNews.length} pinned)`,
    strategies: `${state.proposals.length} active strategy proposals`,
    positions: `${state.positions.length} reconciled positions`,
  };
  const contextLabels: Record<AnalystContextId, string> = { symbol: "Symbol", timeframe: "Timeframe", cursor: "Cursor", news: "News", strategies: "Strategies", positions: "Positions" };
  const chips = (Object.keys(contextLabels) as AnalystContextId[]).filter((id) => analystContextEnabled.has(id)).map((id) => `<span class="context-chip" data-analyst-context="${id}">${contextLabels[id]}: ${escapeHtml(contextValues[id])}<button type="button" data-analyst-context-remove="${id}" aria-label="Remove ${contextLabels[id]} context">×</button></span>`).join("");
  const evidence = analystEvidence.length === 0 ? `<div class="muted">No source cards yet; run an analysis with selected context.</div>` : analystEvidence.map((card) => `<article class="analyst-evidence-card"><div><strong>${escapeHtml(card.label)}</strong><span class="muted"> · ${escapeHtml(card.value)}</span></div><button type="button" data-analyst-evidence-panel="${card.panelId}">Open source</button></article>`).join("");
  return `<label>Question or context
    <textarea data-analyst-input rows="4" maxlength="1048576" placeholder="Explain the current chart, news, and strategy context…"></textarea>
  </label>
  <div class="analyst-suggestions" aria-label="Suggested analyst actions">${suggestions.map((suggestion) => `<button type="button" data-analyst-suggestion="${escapeHtml(suggestion)}">${escapeHtml(suggestion)}</button>`).join("")}</div>
  <div class="analyst-context" aria-label="Analysis context"><strong>Included context</strong>${chips || `<span class="muted">No runtime context selected</span>`}</div>
  <button data-analyze type="button" ${analystBusy ? "disabled" : ""}>${analystBusy ? "Analyzing…" : "Analyze current context"}</button>
  ${analystError ? `<div class="error" role="alert">${escapeHtml(analystError)}</div>` : ""}
  ${analystContent ? `<article class="analyst-output"><div class="${analystStale ? "warning" : "muted"}">${analystStale ? "Stale analyst response — rerun to refresh" : "Trace-backed provider response"} · model claims are unverified unless supported by a source card</div><p>${escapeHtml(analystContent)}</p><div class="analyst-evidence"><strong>Internal evidence cards</strong>${evidence}</div></article>` : `<div class="empty">No analysis requested</div>`}
  <div class="muted">Context: ${escapeHtml(state.selectedSymbol)} · ${escapeHtml(state.selectedTimeframe)} · cursor ${state.cursor}</div>`;
}

function globalSearchMarkup(): string {
  const selectedGraph = selectedContextHit ? `<article class="context-hit-detail"><strong>${escapeHtml(selectedContextHit.nodeId)}</strong><div class="muted">Score ${selectedContextHit.score.toFixed(3)} · lexical ${selectedContextHit.lexicalScore.toFixed(3)} · vector ${selectedContextHit.vectorScore.toFixed(3)}</div>${selectedContextHit.evidencePath.length ? `<div class="muted">Evidence path: ${selectedContextHit.evidencePath.map(escapeHtml).join(" → ")}</div>` : `<div class="muted">No graph evidence path returned</div>`}</article>` : "";
  return `<label>Search instruments, news, strategies, and graph relationships
    <input data-context-search-input maxlength="16384" placeholder="Search context…" />
  </label>
  <button data-context-search type="button" ${contextSearchBusy ? "disabled" : ""}>${contextSearchBusy ? "Searching…" : "Search"}</button>
  ${contextSearchError ? `<div class="error" role="alert">${escapeHtml(contextSearchError)}</div>` : ""}
  <div class="search-results" aria-live="polite">${selectedGraph}${globalSearchResults.map((result) => `<article class="search-result"><button type="button" data-global-result-kind="${result.kind}" data-global-result-id="${escapeHtml(result.id)}"><strong>${escapeHtml(result.kind.toUpperCase())} · ${escapeHtml(result.id)}</strong></button><div class="muted">${escapeHtml(result.label)} · ${escapeHtml(result.detail)}</div></article>`).join("")}${contextSearchResults.map((hit) => `<article class="search-result"><button type="button" data-context-hit-id="${escapeHtml(hit.nodeId)}"><strong>GRAPH · ${escapeHtml(hit.nodeId)}</strong><span class="muted"> score ${hit.score.toFixed(3)} · lexical ${hit.lexicalScore.toFixed(3)} · vector ${hit.vectorScore.toFixed(3)}</span></button>${hit.evidencePath.length ? `<div class="muted">Path: ${hit.evidencePath.map(escapeHtml).join(" → ")}</div>` : ""}</article>`).join("") || (globalSearchResults.length ? "" : `<div class="empty">No context results</div>`)}</div>`;
}

function localGlobalSearch(text: string): readonly GlobalSearchResult[] {
  const query = text.toLocaleLowerCase();
  const results: GlobalSearchResult[] = [];
  const add = (kind: GlobalSearchResult["kind"], id: string, label: string, detail: string): void => {
    if (`${id} ${label} ${detail}`.toLocaleLowerCase().includes(query)) results.push({ kind, id, label, detail });
  };
  for (const quote of Object.values(store.state.quotes)) add("instrument", quote.symbol, `Last ${quote.lastTicks}`, `bid ${quote.bidTicks} / ask ${quote.askTicks}`);
  for (const record of strategyRegistry) add("strategy", record.strategy_id, `${record.mode} · ${record.state}`, `${record.metric_ids.length} metrics · ${record.dependencies.length} dependencies`);
  for (const record of metricRegistry) add("metric", record.metricId, `${record.state} · ${record.priority}`, `${record.inputs.length} inputs · deadline ${record.deadlineNs}ns`);
  for (const item of store.state.news) add("news", item.id, item.title, `${item.source} · ${item.symbols.join(", ")}`);
  for (const record of modelHistory) add("model", `${record.model_id}:${record.version}`, record.status, record.artifact_hash);
  for (const run of experimentHistory) add("experiment", run.run_id, run.status, run.code_hash);
  for (const order of store.state.orders) add("order", order.clientOrderId, `${order.instrumentId} · ${order.state}`, `quantity ${order.quantityTicks}`);
  for (const event of traceEvents) add("trace", `#${event.sequence}`, event.kind, `${event.payloadHex.length / 2} payload bytes`);
  return results.slice(0, 256);
}

function alertsMarkup(): string {
  const severityLabels = ["Info", "Success", "Warning", "Critical"];
  const nativePermissionBlocked = nativeAlertsEnabled && (typeof Notification === "undefined" || Notification.permission === "denied");
  const permissionMessage = nativeAlertPermissionError || (nativePermissionBlocked ? "Native notifications are unavailable or blocked; alerts remain available in the Message Station." : "");
  const permissionWarning = permissionMessage ? `<div class="warning" role="status" aria-live="polite">${escapeHtml(permissionMessage)}</div>` : "";
  const settings = `<div class="alert-settings"><label><input type="checkbox" data-alert-native ${nativeAlertsEnabled ? "checked" : ""} /> Native notifications</label><label><input type="checkbox" data-alert-sound ${soundAlertsEnabled ? "checked" : ""} /> Sound cues</label><div class="alert-severity-settings" aria-label="Sound severity preferences">${severityLabels.map((label, severity) => `<label><input type="checkbox" data-alert-sound-severity="${severity}" ${soundSeverityEnabled[severity] ? "checked" : ""} /> ${label}</label>`).join("")}</div></div>${permissionWarning}`;
  const degraded = alertsDegraded ? `<div class="warning" role="status" aria-live="polite">Alert service unavailable; displayed alerts may be stale. Retrying automatically. <button type="button" data-alert-retry>Retry now</button></div>` : "";
  if (alerts.length === 0) return `${settings}${degraded}<div class="empty">No pending alerts</div>`;
  return `${settings}${degraded}${alerts.map((alert) => `<article class="alert alert-${alert.severity}" role="alert"><div><strong>${escapeHtml(alert.source)}</strong><span class="alert-severity-label">${severityLabels[Math.max(0, Math.min(severityLabels.length - 1, alert.severity))] ?? "Info"}</span><span class="muted"> · ${new Date(alert.occurredMs).toISOString()}</span></div><p>${escapeHtml(alert.message)}</p><button type="button" data-ack-alert="${escapeHtml(alert.alertId)}">Acknowledge</button></article>`).join("")}`;
}

function messageStationMarkup(): string {
  const now = Date.now();
  const visible = alerts.filter((alert) => (messageStationExpiry.get(alert.alertId) ?? Number.POSITIVE_INFINITY) > now);
  const critical = visible.filter((alert) => alert.severity === 3).length;
  const warning = visible.filter((alert) => alert.severity === 2).length;
  const summary = visible.length === 0 ? "All systems clear" : `${visible.length} pending · ${critical} critical · ${warning} warning`;
  const expanded = messageStationOpen
    ? `<section class="message-station-popover" role="region" aria-label="Message center"><strong>Message center</strong>${messageHistory.slice(0, 8).map((record) => { const label = ["Info", "Success", "Warning", "Critical"][Math.max(0, Math.min(3, record.alert.severity))] ?? "Info"; return `<div class="message-row${record.acknowledged ? " message-row-ack" : ""}"><span class="alert-dot alert-dot-${record.alert.severity}" aria-hidden="true"></span><span><strong class="alert-severity-label">${label}</strong> · ${escapeHtml(record.alert.source)} · ${escapeHtml(record.alert.message)}${record.acknowledged ? " · acknowledged" : ""}</span></div>`; }).join("") || `<div class="muted">No session messages yet.</div>`}<button type="button" data-message-open-alerts>Open Alerts panel</button></section>`
    : "";
  return `<div class="message-station"><button type="button" class="message-station-toggle" data-message-toggle aria-expanded="${messageStationOpen}" aria-live="polite" aria-label="Message station: ${escapeHtml(summary)}"><span class="message-station-label">Messages</span><span class="muted">${escapeHtml(summary)}</span></button>${expanded}</div>`;
}

function traceMarkup(): string {
  return `<label>Trace ID<input data-trace-input maxlength="256" placeholder="trace-…" /></label><button type="button" data-trace-query>Reconstruct trace</button><button type="button" data-trace-export ${traceExport.length ? "" : "disabled"}>Export redacted trace</button>${traceError ? `<div class="error" role="alert">${escapeHtml(traceError)}</div>` : ""}<div class="trace-events">${traceEvents.map((event) => `<article class="trace-event"><strong>#${event.sequence} ${escapeHtml(event.kind)}</strong><code>${escapeHtml(event.payloadHex.slice(0, 256))}${event.payloadHex.length > 256 ? "…" : ""}</code></article>`).join("") || `<div class="empty">No trace loaded</div>`}</div>${traceExport.length ? `<div class="muted">Redacted export: ${traceExport.length} events; raw payloads are omitted.</div>` : ""}`;
}

function strategyInspectorMarkup(): string {
  const proposals = store.state.proposals;
  return proposals.map((proposal) => `<article class="strategy-proposal"><div><strong>${escapeHtml(proposal.strategyId)}</strong><span class="muted"> · ${escapeHtml(proposal.symbol)}</span></div><div class="metric"><span>${escapeHtml(proposal.action)}${proposal.quantityTicks === undefined ? "" : ` × ${proposal.quantityTicks}`}</span><span>confidence ${(proposal.confidence * 100).toFixed(1)}%</span></div><div class="muted">Expires ${new Date(proposal.expiresAtMs).toISOString()} · ${proposal.rationaleCodes.map(escapeHtml).join(", ") || "no rationale codes"}</div><button data-proposal="${escapeHtml(proposal.proposalId)}" aria-label="Draft order from ${escapeHtml(proposal.strategyId)} for ${escapeHtml(proposal.symbol)}">Draft order</button><button data-schedule-proposal="${escapeHtml(proposal.proposalId)}" aria-label="Schedule TWAP from ${escapeHtml(proposal.strategyId)} for ${escapeHtml(proposal.symbol)}">Schedule TWAP</button><button data-schedule-is-proposal="${escapeHtml(proposal.proposalId)}" aria-label="Schedule implementation shortfall from ${escapeHtml(proposal.strategyId)} for ${escapeHtml(proposal.symbol)}">Schedule IS</button></article>`).join("") || `<div class="empty">No active strategy proposals</div>`;
}

function metricsMarkup(): string {
  const symbol = symbolFor("metrics");
  const quote = store.state.quotes[symbol];
  const quoteMarkup = quote ? `<div class="metric"><span>Instrument</span><span>${escapeHtml(quote.symbol)}</span></div><div class="metric"><span>Last</span><span>${quote.lastTicks}</span></div><div class="metric"><span>Spread</span><span>${quote.askTicks - quote.bidTicks}</span></div><div class="metric"><span>Bid / Ask</span><span>${quote.bidTicks} / ${quote.askTicks}</span></div><div class="muted">Quote sequence ${quote.sequence} · received ${new Date(quote.receivedAtMs).toISOString()}</div>` : `<div class="empty">Waiting for metric inputs for ${escapeHtml(symbol)}</div>`;
  const installed = metricRegistry.map((record) => {
    const next = ({ research: "validated", validated: "shadow", shadow: "canary", canary: "production", paused: "canary" } as Record<string, string>)[record.lifecycle];
    const action = next ? `<button type="button" data-metric-lifecycle-id="${escapeHtml(record.metricId)}" data-metric-lifecycle-next="${next}">Advance → ${next}</button>` : "";
    return `<div class="metric"><button type="button" data-metric-inspect="${escapeHtml(record.metricId)}">${escapeHtml(record.metricId)} · health ${escapeHtml(record.state)}</button><span>${escapeHtml(record.lifecycle)} · ${escapeHtml(record.priority)} · ${record.inputs.length} inputs · deadline ${record.deadlineNs}ns ${action}</span></div>`;
  }).join("");
  return `${quoteMarkup}<h3>Installed metrics</h3>${installed || `<div class="empty">No installed metric manifests</div>`}`;
}

function metricInspectorMarkup(): string {
  const record = metricRegistry.find((candidate) => candidate.metricId === selectedMetricId) ?? metricRegistry[0];
  if (!record) return `<div class="empty">Select a metric from the Metrics panel to inspect its manifest.</div>`;
  selectedMetricId = record.metricId;
  const scoreRange = record.minScore === null || record.maxScore === null ? "unbounded" : `${record.minScore} … ${record.maxScore}`;
  return `<div class="metric"><span>Metric</span><strong>${escapeHtml(record.metricId)}</strong></div><div class="metric"><span>Lifecycle</span><span>${escapeHtml(record.lifecycle)} · ${escapeHtml(record.state)}</span></div><div class="metric"><span>Priority</span><span>${escapeHtml(record.priority)}</span></div><div class="metric"><span>Schedule</span><span>period ${record.periodNs}ns · deadline ${record.deadlineNs}ns · budget ${record.budgetNs}ns</span></div><div class="metric"><span>Output</span><span>TTL ${record.ttlNs}ns · score ${escapeHtml(scoreRange)}</span></div><h3>Declared inputs</h3>${record.inputs.map((input) => `<div class="metric"><code>${escapeHtml(input)}</code></div>`).join("") || `<div class="empty">No declared inputs</div>`}<div class="muted">This inspector is read-only; lifecycle changes remain authenticated engine commands.</div>`;
}

function systemHealthMarkup(): string {
  const state = store.state;
  const providers = newsProviderStatuses.map((provider) => {
    const last = provider.lastSuccessMs === undefined ? "never" : new Date(provider.lastSuccessMs).toISOString();
    const retry = provider.nextRetryMs === undefined ? "—" : new Date(provider.nextRetryMs).toISOString();
    return `<div class="metric"><span>${escapeHtml(provider.providerId)} · ${escapeHtml(provider.health)}</span><span>success ${escapeHtml(last)} · retry ${escapeHtml(retry)} · failures ${provider.consecutiveFailures} · DLQ ${provider.deadLetterCount}</span></div>`;
  }).join("");
  const components = supervisorStatuses.map((component) => {
    const retry = component.retryAtNs === 0 ? "—" : `${component.retryAtNs.toLocaleString()} ns`;
    const backoff = component.backoffNs === 0 ? "—" : `${component.backoffNs.toLocaleString()} ns`;
    return `<div class="metric"><span>${escapeHtml(component.name)} · ${escapeHtml(component.health)}</span><span>state ${escapeHtml(component.state)} · failures ${component.failures} · retry ${retry} · backoff ${backoff}</span></div>`;
  }).join("");
  return `<div class="metric"><span>Connection</span><span>${state.connection}</span></div><div class="metric"><span>Runtime cursor</span><span>${state.cursor}</span></div><div class="metric"><span>Snapshot version</span><span>${state.version}</span></div><div class="metric"><span>Selected symbol/timeframe</span><span>${escapeHtml(state.selectedSymbol)} · ${escapeHtml(state.selectedTimeframe)}</span></div><div class="metric"><span>Autonomy analysis</span><span>${state.autonomy.stale ? "stale" : "current"}</span></div><h3>Runtime components</h3>${components || `<div class="empty">Supervisor status unavailable</div>`}<h3>News providers</h3>${providers || `<div class="empty">No news providers registered</div>`}`;
}

function riskMarkup(): string {
  const risk = store.state.risk;
  const policy = riskPolicyRevisions.map((revision) => `<div class="metric"><span>${escapeHtml(revision.scope)}${revision.identity ? ` · ${escapeHtml(revision.identity)}` : ""}</span><span>effective ${revision.effectiveMonoNs.toLocaleString()} ns · position ${revision.maxPositionTicks} · order ${revision.maxOrderTicks} · gross ${revision.maxGrossNotionalTicks}</span></div>`).join("");
  return `<div class="metric"><span>State</span><span>${escapeHtml(risk.state)}</span></div><div class="metric"><span>Gross</span><span>${risk.grossNotionalTicks} / ${risk.maxGrossNotionalTicks}</span></div><div class="metric"><span>Utilization</span><span>${risk.grossUtilizationBps} bps</span></div><div class="metric"><span>Largest position</span><span>${risk.largestPositionNotionalTicks}</span></div><div class="metric"><span>Drawdown</span><span>${risk.drawdownBps ?? "—"}${risk.drawdownBps === undefined ? "" : " bps"}</span></div><h3>Scoped policy revisions</h3>${policy || `<div class="empty">No scoped policy installed</div>`}<label>Operator authorization<input data-risk-authorization maxlength="256" autocomplete="off" placeholder="Required to relax restrictions" /></label><div class="risk-actions"><button type="button" data-risk-state="reduce_only">Reduce only</button><button type="button" data-risk-state="cancel_only">Cancel only</button><button type="button" data-risk-state="halted">Halt</button><button type="button" data-risk-state="running">Resume</button></div>`;
}

function backupMarkup(): string {
  return `<p class="muted">Backups are written atomically by the engine and include a SHA-256 manifest. Existing destination files are never overwritten.</p>
    <label>Backup destination<input data-journal-backup-path maxlength="4096" placeholder="/secure/path/journal.backup" /></label>
    <button type="button" data-journal-backup ${backupBusy ? "disabled" : ""}>${backupBusy ? "Working…" : "Create journal backup"}</button>
    <label>Restore source<input data-journal-restore-source maxlength="4096" placeholder="/secure/path/journal.backup" /></label>
    <label>Restore destination<input data-journal-restore-destination maxlength="4096" placeholder="/secure/path/restored.journal" /></label>
    <button type="button" data-journal-restore ${backupBusy ? "disabled" : ""}>Restore into new file</button>
    ${backupError ? `<div class="error" role="alert">${escapeHtml(backupError)}</div>` : ""}
    ${backupResult ? `<div class="positive" role="status">${escapeHtml(backupResult.destination)} · ${backupResult.byteLen} bytes · ${escapeHtml(backupResult.sha256)}</div>` : `<div class="empty">No backup operation completed this session</div>`}`;
}

function screenerMarkup(): string {
  const state = store.state;
  const proposals = new Map(state.proposals.map((proposal) => [proposal.symbol, proposal]));
  const matchingRows = Object.values(state.quotes)
    .filter((quote) => quote.symbol.toLowerCase().includes(screenerQuery.toLowerCase()))
    .map((quote) => {
      const proposal = proposals.get(quote.symbol);
      return {
        quote,
        proposal,
        spread: quote.askTicks - quote.bidTicks,
        confidence: proposal?.confidence ?? -1,
      };
    })
    .sort((left, right) => {
      if (screenerSort === "last") return right.quote.lastTicks - left.quote.lastTicks || left.quote.symbol.localeCompare(right.quote.symbol);
      if (screenerSort === "spread") return left.spread - right.spread || left.quote.symbol.localeCompare(right.quote.symbol);
      if (screenerSort === "confidence") return right.confidence - left.confidence || left.quote.symbol.localeCompare(right.quote.symbol);
      return left.quote.symbol.localeCompare(right.quote.symbol);
    })
  const rows = matchingRows.slice(0, screenerVisibleRows);
  const body = rows.map(({ quote, proposal, spread, confidence }) => `<div class="metric screener-row"><button data-symbol="${escapeHtml(quote.symbol)}">${escapeHtml(quote.symbol)}</button><span>${quote.lastTicks}</span><span>${quote.bidTicks} / ${quote.askTicks}</span><span>spread ${spread}</span><span>${proposal ? `${escapeHtml(proposal.action)} · ${(confidence * 100).toFixed(1)}%` : "No proposal"}</span></div>`).join("");
  const nextPage = matchingRows.length > rows.length
    ? `<button type="button" data-screener-load-more>Load next ${Math.min(SCREENER_PAGE_SIZE, matchingRows.length - rows.length)} (${rows.length}/${matchingRows.length})</button>`
    : `<span class="muted">All ${matchingRows.length} matching canonical quote records shown</span>`;
  return `<div class="screener-controls"><label>Symbol filter<input data-screener-query value="${escapeHtml(screenerQuery)}" maxlength="32" placeholder="AAPL…" /></label><label>Sort<select data-screener-sort><option value="symbol" ${screenerSort === "symbol" ? "selected" : ""}>Symbol</option><option value="last" ${screenerSort === "last" ? "selected" : ""}>Last price</option><option value="spread" ${screenerSort === "spread" ? "selected" : ""}>Spread</option><option value="confidence" ${screenerSort === "confidence" ? "selected" : ""}>Proposal confidence</option></select></label></div>${body || `<div class="empty">No canonical quotes match the filter</div>`}<div class="screener-pagination">${nextPage}</div><div class="muted">${rows.length} of ${matchingRows.length} canonical quote records rendered; results are paged to keep the UI responsive. Proposal fields are included only when an active strategy proposal exists.</div>`;
}

function brokerStatusMarkup(): string {
  const state = store.state;
  const working = state.orders.filter((order) => !["filled", "cancelled", "rejected", "expired"].includes(order.state)).length;
  const unknown = state.orders.filter((order) => order.state === "unknown").length;
  const health = brokerStatus?.health ?? "unknown";
  return `<div class="metric"><span>Runtime connection</span><span>${state.connection}</span></div><div class="metric"><span>Broker session</span><span class="${health === "healthy" ? "positive" : health === "unknown" ? "muted" : "negative"}">${health}</span></div><div class="metric"><span>Working orders</span><span>${brokerStatus?.orderCount ?? working}</span></div><div class="metric"><span>Unknown orders</span><span class="${unknown ? "negative" : "positive"}">${unknown}</span></div><div class="metric"><span>Broker positions</span><span>${brokerStatus?.positionCount ?? state.positions.length}</span></div><div class="metric"><span>Account values</span><span>${brokerStatus?.accountValueCount ?? "—"}</span></div>${unknown ? `<div class="error" role="alert">Unknown broker state requires reconciliation before resend.</div>` : health === "healthy" ? `<div class="positive">Broker session is healthy.</div>` : `<div class="muted">Broker health is not confirmed; order controls remain risk-gated.</div>`}`;
}

function tcaMarkup(): string {
  const records = store.state.tca;
  const quantity = records.reduce((total, record) => total + record.filledQuantityTicks, 0);
  const rows = records.map((record) => {
    const average = typeof record.averageFillPriceNumerator === "number"
      ? `${record.averageFillPriceNumerator} / ${record.averageFillPriceDenominator}`
      : `${record.averageFillPriceNumerator} / ${record.averageFillPriceDenominator}`;
    const latency = record.sendMonoNs !== undefined && record.ackMonoNs !== undefined
      ? ` · ack ${(record.ackMonoNs - record.sendMonoNs)}ns`
      : "";
    const shortfall = record.implementationShortfallTickValue === undefined
      ? ""
      : ` · shortfall ${record.implementationShortfallTickValue}`;
    const spread = record.averageSpreadTicks === undefined ? "" : ` · spread ${record.averageSpreadTicks}`;
    const adverse = record.adverseSelectionTickValue === undefined ? "" : ` · adverse ${record.adverseSelectionTickValue}`;
    return `<div class="metric"><span>${escapeHtml(record.clientOrderId)}</span><span>${record.filledQuantityTicks} filled · VWAP ${average}${latency}${shortfall}${spread}${adverse}</span></div>`;
  }).join("");
  return `<div class="metric"><span>Orders with realized fills</span><span>${records.length}</span></div><div class="metric"><span>Retained filled quantity</span><span>${quantity}</span></div>${rows || `<div class="empty">No retained fill measurements</div>`}<div class="muted">VWAP, timing, spread, shortfall, and adverse-selection values are shown only when canonical source records exist.</div>`;
}

function backtestMarkup(): string {
  const defaultRequest = {
    runId: `run-${Date.now()}`,
    strategyId: "strategy.example.v1",
    datasetHash: "dataset-sha256",
    configHash: "config-sha256",
    initialCashTicks: "100000",
    events: [
      { kind: "fill", sequence: 1, quantityTicks: 10, priceTicks: 100, feeTicks: "5" },
      { kind: "mark", sequence: 2, priceTicks: 110 },
    ],
  };
  const result = backtestResult
    ? `<div class="positive" role="status">${escapeHtml(backtestResult.runId)} · ${backtestResult.eventCount} events · equity ${escapeHtml(backtestResult.finalEquityTicks ?? "unmarked")} · fees ${escapeHtml(backtestResult.totalFeesTicks)}</div>`
    : `<div class="empty">No backtest run submitted</div>`;
  const history = backtestHistory.slice().reverse().slice(0, 20).map((item) => `<div class="metric"><span>${escapeHtml(item.runId)} · ${escapeHtml(item.strategyId)}</span><span>${item.eventCount} events · equity ${escapeHtml(item.finalEquityTicks ?? "—")}</span></div>`).join("");
  return `<label>Run request JSON<textarea data-backtest-json rows="8" maxlength="16777216">${escapeHtml(JSON.stringify(defaultRequest, null, 2))}</textarea></label><button data-run-backtest type="button" ${backtestBusy ? "disabled" : ""}>${backtestBusy ? "Running…" : "Run deterministic backtest"}</button>${backtestError ? `<div class="error" role="alert">${escapeHtml(backtestError)}</div>` : ""}${result}<h3>Journaled runs</h3>${history || `<div class="empty">No journaled runs</div>`}<div class="muted">Events are point-in-time fills/marks; lineage hashes and results are journaled by the engine.</div>`;
}

function experimentRegistryMarkup(): string {
  const rows = experimentHistory.map((run) => {
    const metrics = Object.entries(run.metrics).map(([key, value]) => `${escapeHtml(key)}=${value}`).join(" · ");
    const lineage = [
      run.provenance.strategy_id && `strategy ${run.provenance.strategy_id}${run.provenance.strategy_version ? `@${run.provenance.strategy_version}` : ""}`,
      run.provenance.news_dataset_hash && `news ${run.provenance.news_dataset_hash}`,
      run.provenance.graph_snapshot_version && `graph ${run.provenance.graph_snapshot_version}`,
      run.provenance.llm_provider && `LLM ${run.provenance.llm_provider}${run.provenance.llm_model ? `/${run.provenance.llm_model}` : ""}`,
      run.provenance.prompt_version && `prompt ${run.provenance.prompt_version}`,
      run.provenance.llm_cache_ids.length > 0 && `cache ${run.provenance.llm_cache_ids.length}`,
      run.provenance.autonomy_config_hash && `autonomy ${run.provenance.autonomy_config_hash}`,
    ].filter((value): value is string => Boolean(value));
    return `<div class="metric"><span>${escapeHtml(run.run_id)} · ${escapeHtml(run.status)}</span><span>code ${escapeHtml(run.code_hash)} · ${run.artifacts.length} artifacts${metrics ? ` · ${metrics}` : ""}</span>${lineage.length ? `<div class="muted">${lineage.map(escapeHtml).join(" · ")}</div>` : `<div class="muted">No extended provenance recorded</div>`}</div>`;
  }).join("");
  return `${rows || `<div class="empty">No registered research experiments</div>`}<div class="muted">Lineage hashes, lifecycle state, scalar results, and artifact references are authoritative engine records.</div>`;
}

const CONFIG_RISK_FIELDS = [
  ["risk.max_leverage", "leverage", "2", false],
  ["risk.max_position_ticks", "max-position", "1000000", true],
  ["risk.max_gross_notional_ticks", "max-notional", "100000000000", true],
  ["risk.max_drawdown_bps", "drawdown", "500", true],
  ["risk.max_outstanding_orders", "orders", "32", true],
  ["risk.max_predicted_volatility_bps", "volatility", "250", true],
  ["risk.max_participation_bps", "participation", "1000", true],
  ["risk.max_message_rate", "message-rate", "20", true],
  ["risk.max_price_deviation_bps", "price-deviation", "75", true],
] as const;
const CONFIG_OPERATIONAL_FIELDS = [
  ["scheduler.python_cycle_ms", "python-cycle", "100"],
  ["scheduler.execution_cycle_ms", "execution-cycle", "25"],
  ["market.max_age_ms", "market-age", "60000"],
  ["market.http_timeout_ms", "market-http-timeout", "30000"],
  ["market.yahoo_interval_ns", "yahoo-interval-ns", "60000000000"],
  ["market.yahoo_price_scale", "yahoo-price-scale", "10000"],
  ["market.yahoo_poll_ms", "yahoo-history-poll", "60000"],
  ["market.yahoo_quote_poll_ms", "yahoo-quote-poll", "5000"],
  ["strategy.reference_enabled", "reference-enabled", "false"],
  ["strategy.reference_entry_threshold", "reference-entry", "0.5"],
  ["strategy.reference_exit_threshold", "reference-exit", "0.1"],
  ["strategy.reference_quantity_ticks", "reference-quantity", "1"],
  ["strategy.reference_horizon_ns", "reference-horizon", "900000000000"],
  ["strategy.reference_ttl_ns", "reference-ttl", "5000000000"],
  ["metric.ewma_lambda", "ewma-lambda", "0.94"],
  ["metric.ewma_ttl_ns", "ewma-ttl", "5000000000"],
  ["metric.reference_ttl_ns", "metric-ttl", "5000000000"],
  ["metric.sma_window", "sma-window", "20"],
  ["broker.ibkr_timeout_ms", "ibkr-timeout", "10000"],
  ["broker.ibkr_market_poll_ms", "ibkr-poll", "1000"],
  ["broker.ibkr_price_scale", "ibkr-scale", "10000"],
  ["python.cpu_seconds", "python-cpu", "3600"],
  ["python.memory_bytes", "python-memory", "536870912"],
  ["news.newsapi_poll_ms", "newsapi-poll", "30000"],
  ["news.yahoo_poll_ms", "yahoo-news-poll", "60000"],
  ["news.http_timeout_ms", "news-http-timeout", "30000"],
  ["news.max_retries", "news-max-retries", "4"],
  ["news.retry_base_ms", "news-retry-base", "1000"],
  ["news.retry_max_ms", "news-retry-max", "60000"],
  ["reconciliation.poll_ms", "reconciliation-poll", "30000"],
  ["alerts.webhook_timeout_ms", "webhook-timeout", "2000"],
  ["alerts.webhook_poll_ms", "webhook-poll", "2000"],
  ["alerts.cooldown_ms", "alert-cooldown", "60000"],
  ["alerts.max_pending", "alert-max-pending", "4096"],
  ["supervisor.max_failures", "supervisor-failures", "3"],
  ["supervisor.window_ns", "supervisor-window", "60000000000"],
  ["supervisor.initial_backoff_ns", "supervisor-initial-backoff", "100000000"],
  ["supervisor.max_backoff_ns", "supervisor-max-backoff", "30000000000"],
  ["supervisor.jitter_bps", "supervisor-jitter", "1000"],
  ["llm.timeout_ms", "llm-timeout", "30000"],
  ["ui.status_poll_ms", "ui-status-poll", "5000"],
  ["ui.alert_poll_ms", "alert-poll", "1000"],
  ["ui.analyst_stale_after_ms", "analyst-stale-after", "300000"],
] as const;
const CONFIG_STRING_FIELDS = [["llm.base_url", "llm-base-url", "https://api.openai.com/v1"], ["market.yahoo_base_url", "yahoo-base-url", "https://query1.finance.yahoo.com"], ["market.yahoo_interval", "yahoo-interval", "1m"], ["market.yahoo_range", "yahoo-range", "1d"], ["broker.mode", "broker-mode", "paper"], ["broker.ibkr_base_url", "ibkr-base-url", "https://127.0.0.1:5000"], ["news.newsapi_base_url", "newsapi-base-url", "https://newsapi.org"], ["news.newsapi_endpoint", "newsapi-endpoint", "everything"], ["strategy.reference_id", "reference-id", "microstructure.imbalance.threshold.v1"], ["strategy.reference_metric_id", "reference-metric-id", "microstructure.imbalance.v1"], ["metric.ewma_id", "ewma-id", "volatility.ewma.v1"], ["metric.sma_id", "sma-id", "trend.sma.v1"], ["metric.spread_id", "spread-id", "liquidity.spread.v1"], ["metric.imbalance_id", "imbalance-id", "microstructure.imbalance.v1"], ["python.executable", "python-executable", "python3"], ["python.workdir", "python-workdir", "data/python-workers"], ["python.metrics_root", "python-metrics-root", "metrics"], ["python.strategies_root", "python-strategies-root", "strategies"]] as const;
const CONFIG_LLM_FIELDS = [["llm.model", "llm-model", "configured-model"], ["llm.prompt_version", "llm-prompt-version", "ai-analyst.v1"]] as const;
const CONFIG_GENERATOR_FIELDS = [...CONFIG_RISK_FIELDS, ...CONFIG_OPERATIONAL_FIELDS, ...CONFIG_STRING_FIELDS, ...CONFIG_LLM_FIELDS] as const;
const CONFIG_OPTIONAL_STRING_KEYS = ["alerts.webhook_url", "market.yahoo_symbols"] as const;

function configNumericValue(cfgText: string, key: string, fallback: string): string {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = cfgText.match(new RegExp(`^\\s*${escaped}\\s*=\\s*([^\\s#]+)`, "m"));
  if (!match || !match[1] || !/^-?(?:\\d+(?:\\.\\d+)?|\\.\\d+)$/.test(match[1])) return fallback;
  return match[1];
}

function configStringValue(cfgText: string, key: string, fallback: string): string {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = cfgText.match(new RegExp(`^\\s*${escaped}\\s*=\\s*"((?:[^"\\\\]|\\\\.)*)"`, "m"));
  if (!match?.[1]) return fallback;
  try {
    const decoded = JSON.parse(`"${match[1]}"`);
    return typeof decoded === "string" && decoded.length <= 2048 ? decoded : fallback;
  } catch {
    return fallback;
  }
}

function configBooleanValue(cfgText: string, key: string, fallback: boolean): boolean {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = cfgText.match(new RegExp(`^\\s*${escaped}\\s*=\\s*(true|false)\\b`, "mi"));
  return match ? match[1].toLowerCase() === "true" : fallback;
}

/** Validate the bounded Yahoo subscription grammar before generating a file.
 *  The engine remains authoritative, but rejecting malformed/ambiguous entries
 *  here prevents a setup file from silently dropping subscriptions at startup.
 */
function validateYahooSymbols(raw: string): string | undefined {
  if (!raw.trim()) return undefined;
  const entries = raw.split(",").map((entry) => entry.trim()).filter(Boolean);
  if (entries.length > 128) return "Yahoo symbol list may contain at most 128 entries.";
  const symbols = new Set<string>();
  const instruments = new Set<string>();
  for (const entry of entries) {
    const parts = entry.split("=");
    if (parts.length !== 2) return "Yahoo symbols must use SYMBOL=INSTRUMENT_ID entries.";
    const symbol = parts[0].trim().toUpperCase();
    const instrument = parts[1].trim();
    if (!/^[A-Z0-9._-]{1,16}$/.test(symbol) || !/^[1-9][0-9]{0,38}$/.test(instrument)) {
      return "Yahoo symbols require a 1–16 character symbol and positive instrument ID.";
    }
    if (symbols.has(symbol) || instruments.has(instrument)) return "Yahoo symbols and instrument IDs must be unique.";
    symbols.add(symbol);
    instruments.add(instrument);
  }
  return undefined;
}

function removeConfigKeys(cfgText: string, keys: readonly string[]): string {
  const keySet = new Set(keys);
  return cfgText
    .split(/\r?\n/)
    .filter((line) => {
      const key = line.match(/^\s*([A-Za-z][A-Za-z0-9_.-]*)\s*=\s*/)?.[1];
      return !key || !keySet.has(key);
    })
    .join("\n");
}

function mergeRiskConfiguration(cfgText: string, values: Readonly<Record<string, string>>): string {
  const seen = new Set<string>();
  const descriptorFor = (key: string): readonly [string, string, string] | undefined => CONFIG_GENERATOR_FIELDS.find(([configKey]) => configKey === key);
  const renderedValue = (key: string): string => {
    const descriptor = descriptorFor(key);
    const raw = values[key] ?? (descriptor ? values[descriptor[1]] : undefined) ?? descriptor?.[2] ?? "";
    if (![...CONFIG_STRING_FIELDS, ...CONFIG_LLM_FIELDS].some(([configKey]) => configKey === key) || raw.startsWith('"')) return raw;
    return JSON.stringify(raw);
  };
  const lines = cfgText.split(/\r?\n/);
  const output: string[] = [];
  for (const line of lines) {
    const match = line.match(/^\s*([A-Za-z][A-Za-z0-9_.-]*)\s*=\s*/);
    const key = match?.[1];
    if (!key || !(key in values)) {
      output.push(line);
      continue;
    }
    if (seen.has(key)) continue;
    seen.add(key);
    const comment = line.match(/\s+(#.*)$/)?.[1] ?? "";
    output.push(`${key} = ${renderedValue(key)}${comment ? ` ${comment}` : ""}`);
  }
  const generatedKeys = CONFIG_GENERATOR_FIELDS.map(([key]) => key);
  const optionalKeys = CONFIG_OPTIONAL_STRING_KEYS.filter((key) => key in values);
  const missing = [...generatedKeys, ...optionalKeys].filter((key, index, keys) => !seen.has(key) && keys.indexOf(key) === index);
  if (missing.length) {
    if (output.length > 0 && output.at(-1) !== "") output.push("");
    output.push("# Generated by InsiderTrader setup");
    for (const key of missing) output.push(`${key} = ${renderedValue(key)}`);
  }
  return `${output.join("\n").replace(/\n+$/, "")}\n`;
}

function configurationMarkup(): string {
  const text = configSnapshot?.cfg_text ?? `# InsiderTrader configuration\nrisk.max_drawdown_bps = 1000\nrisk.max_outstanding_orders = 20\n`;
  const values = Object.fromEntries(CONFIG_GENERATOR_FIELDS.map(([key, field, fallback]) => [field, configNumericValue(text, key, fallback)]));
  const llmBaseUrl = configStringValue(text, "llm.base_url", "https://api.openai.com/v1");
  const yahooBaseUrl = configStringValue(text, "market.yahoo_base_url", "https://query1.finance.yahoo.com");
  const yahooInterval = configStringValue(text, "market.yahoo_interval", "1m");
  const yahooRange = configStringValue(text, "market.yahoo_range", "1d");
  const brokerMode = configStringValue(text, "broker.mode", "paper");
  return `<div class="muted">Version ${configSnapshot?.version ?? "not loaded"} · deterministic .cfg syntax · changes are atomic and version-checked</div><div class="config-generator"><label>Max leverage<input data-config-leverage type="number" min="0" step="0.01" value="${values.leverage}" /></label><label>Max drawdown (bps)<input data-config-drawdown type="number" min="0" value="${values.drawdown}" /></label><label>Max outstanding orders<input data-config-orders type="number" min="0" value="${values.orders}" /></label><label>Max predicted volatility (bps)<input data-config-volatility type="number" min="0" value="${values.volatility}" /></label><label>Max participation (bps)<input data-config-participation type="number" min="0" value="${values.participation}" /></label><label>Max message rate<input data-config-message-rate type="number" min="0" value="${values["message-rate"]}" /></label><label>Max price deviation (bps)<input data-config-price-deviation type="number" min="0" value="${values["price-deviation"]}" /></label><label>Python cycle (ms)<input data-config-python-cycle type="number" min="25" value="${values["python-cycle"]}" /></label><label>Execution cycle (ms)<input data-config-execution-cycle type="number" min="5" value="${values["execution-cycle"]}" /></label><label>Market max age (ms)<input data-config-market-age type="number" min="250" value="${values["market-age"]}" /></label><label>Market HTTP timeout (ms)<input data-config-market-http-timeout type="number" min="1000" value="${values["market-http-timeout"]}" /></label><label>Yahoo base URL<input data-config-yahoo-base-url type="url" maxlength="2048" value="${escapeHtml(yahooBaseUrl)}" /></label><label>Yahoo interval<input data-config-yahoo-interval type="text" maxlength="16" value="${escapeHtml(yahooInterval)}" /></label><label>Yahoo range<input data-config-yahoo-range type="text" maxlength="16" value="${escapeHtml(yahooRange)}" /></label><label>Yahoo interval (ns)<input data-config-yahoo-interval-ns type="number" min="1000000000" value="${values["yahoo-interval-ns"]}" /></label><label>Yahoo price scale<input data-config-yahoo-price-scale type="number" min="1" value="${values["yahoo-price-scale"]}" /></label><label>Yahoo history poll (ms)<input data-config-yahoo-history-poll type="number" min="5000" value="${values["yahoo-history-poll"]}" /></label><label>Yahoo quote poll (ms)<input data-config-yahoo-quote-poll type="number" min="1000" value="${values["yahoo-quote-poll"]}" /></label><label>NewsAPI poll (ms)<input data-config-newsapi-poll type="number" min="1000" value="${values["newsapi-poll"]}" /></label><label>Yahoo news poll (ms)<input data-config-yahoo-news-poll type="number" min="5000" value="${values["yahoo-news-poll"]}" /></label><label>News HTTP timeout (ms)<input data-config-news-http-timeout type="number" min="1000" value="${values["news-http-timeout"]}" /></label><label>News max retries<input data-config-news-max-retries type="number" min="0" max="16" value="${values["news-max-retries"]}" /></label><label>Retry base (ms)<input data-config-news-retry-base type="number" min="1" value="${values["news-retry-base"]}" /></label><label>Retry max (ms)<input data-config-news-retry-max type="number" min="1" value="${values["news-retry-max"]}" /></label><label>Reconciliation poll (ms)<input data-config-reconciliation-poll type="number" min="1000" value="${values["reconciliation-poll"]}" /></label><label>Webhook timeout (ms)<input data-config-webhook-timeout type="number" min="250" value="${values["webhook-timeout"]}" /></label><label>Webhook poll (ms)<input data-config-webhook-poll type="number" min="250" value="${values["webhook-poll"]}" /></label><label>LLM timeout (ms)<input data-config-llm-timeout type="number" min="1000" value="${values["llm-timeout"]}" /></label><label>LLM base URL<input data-config-llm-base-url type="url" maxlength="2048" value="${escapeHtml(llmBaseUrl)}" /></label><button type="button" data-config-generate>Merge setup values into configuration</button></div><textarea data-config-text rows="18" maxlength="1048576" spellcheck="false" aria-label="Configuration file">${escapeHtml(text)}</textarea><div class="config-actions"><button type="button" data-config-copy>Copy .cfg</button><button type="button" data-config-download>Download .cfg</button><button type="button" data-config-reload ${configBusy ? "disabled" : ""}>${configBusy ? "Applying…" : "Validate and apply configuration"}</button></div>${configActionMessage ? `<div class="positive" role="status">${escapeHtml(configActionMessage)}</div>` : ""}${configError ? `<div class="error" role="alert">${escapeHtml(configError)}</div>` : ""}<div class="muted">Use key = value; strings are quoted, booleans are true/false, and # starts a comment. Existing non-generator settings are preserved.</div>`;
}

function modelRegistryMarkup(): string {
  const rows = modelHistory.map((model) => {
    const next = model.status === "research" ? "validate" : model.status === "validated" ? "shadow" : model.status === "shadow" ? "canary" : model.status === "canary" ? "promote" : undefined;
    const action = next ? `<button type="button" data-model-action="${next}" data-model-id="${escapeHtml(model.model_id)}" data-model-version="${escapeHtml(model.version)}">${next === "promote" ? "Promote" : next[0].toUpperCase() + next.slice(1)}</button>` : "";
    return `<div class="metric"><span>${escapeHtml(model.model_id)}:${escapeHtml(model.version)} · ${escapeHtml(model.status)}${model.active ? " · ACTIVE" : ""}</span><span>width ${model.input_width} · ${escapeHtml(model.artifact_hash)} ${action}</span></div>`;
  }).join("");
  return `${rows || `<div class="empty">No registered model artifacts</div>`}<div class="muted">Only journaled lifecycle states with immutable artifact and schema hashes are shown.</div>`;
}

function portfolioMarkup(): string {
  const state = store.state;
  const gross = state.risk.grossNotionalTicks;
  const rows = state.positions.map((position) => {
    const pnlSign = position.pnlTicks > 0 ? "+" : position.pnlTicks < 0 ? "−" : "";
    const pnlClass = position.pnlTicks > 0 ? "positive" : position.pnlTicks < 0 ? "negative" : "muted";
    return `<div class="metric"><span>${escapeHtml(position.symbol)} × ${position.quantityTicks}</span><span>mark ${position.markTicks} · avg ${position.averageCostTicks} · <strong class="${pnlClass}" aria-label="P&L ${position.pnlTicks >= 0 ? "gain" : "loss"}">PnL ${pnlSign}${Math.abs(position.pnlTicks)}</strong></span></div>`;
  }).join("");
  return `<div class="metric"><span>Gross notional</span><span>${gross} / ${state.risk.maxGrossNotionalTicks}</span></div><div class="metric"><span>Utilization</span><span>${state.risk.grossUtilizationBps} bps</span></div>${rows || `<div class="empty">No reconciled positions</div>`}<div class="muted">Positions and risk values are read-only reconciled runtime state.</div>`;
}

function strategyBrowserMarkup(): string {
  const groups = new Map<string, ProposalSnapshot[]>();
  for (const proposal of store.state.proposals) groups.set(proposal.strategyId, [...(groups.get(proposal.strategyId) ?? []), proposal]);
  const rows = [...groups.entries()].sort(([left], [right]) => left.localeCompare(right)).map(([strategy, proposals]) => {
    const confidence = proposals.reduce((sum, proposal) => sum + proposal.confidence, 0) / proposals.length;
    return `<div class="metric"><span>${escapeHtml(strategy)} · ${proposals.length} active proposals</span><span>${(confidence * 100).toFixed(1)}% avg confidence</span></div>`;
  }).join("");
  const latest = resolutionHistory.at(-1);
  const resolution = latest ? `<div class="metric"><span>Last resolution · ${escapeHtml(latest.policy)}</span><span>${latest.accepted_count} accepted · ${latest.conflict_count} conflicts · ${latest.expired_count} expired · ${latest.attribution_count} attributions</span></div>` : `<div class="muted">No strategy resolution boundary recorded</div>`;
  const execution = executionHistory.map((summary) => `<div class="metric"><span>${escapeHtml(summary.strategy_id)} · ${summary.fill_count} fills</span><span>qty ${escapeHtml(summary.filled_quantity_ticks)} · notional ${escapeHtml(summary.notional_ticks)}</span></div>`).join("");
  const installed = strategyRegistry.map((record) => {
    const next = ({ research: "validated", validated: "shadow", shadow: "canary", canary: "production", paused: "canary" } as Record<string, string>)[record.lifecycle];
    const action = next ? `<button type="button" data-strategy-lifecycle-id="${escapeHtml(record.strategy_id)}" data-strategy-lifecycle-next="${next}">Advance → ${next}</button>` : "";
    return `<div class="metric"><span>${escapeHtml(record.strategy_id)} · health ${escapeHtml(record.state)}</span><span>${escapeHtml(record.lifecycle)} · evidence ${escapeHtml(record.lifecycle_evidence_ref)} · ${escapeHtml(record.mode)} · ${escapeHtml(record.priority)} · ${record.metric_ids.length} metrics · ${record.dependencies.length} deps ${action}</span></div>`;
  }).join("");
  return `<h3>Installed strategies</h3>${installed || `<div class="empty">No installed strategy manifests</div>`}${rows || `<div class="empty">No registered strategy proposals</div>`}${resolution}${execution ? `<div class="muted">Authoritative fills by strategy</div>${execution}` : `<div class="muted">No strategy-attributed fills yet</div>`}`;
}

function strategyComparisonMarkup(): string {
  const proposalsByStrategy = new Map<string, number>();
  for (const proposal of store.state.proposals) proposalsByStrategy.set(proposal.strategyId, (proposalsByStrategy.get(proposal.strategyId) ?? 0) + 1);
  const fillsByStrategy = new Map(executionHistory.map((summary) => [summary.strategy_id, summary]));
  const rows = strategyRegistry.map((record) => {
    const fills = fillsByStrategy.get(record.strategy_id);
    return `<div class="strategy-comparison-row"><strong>${escapeHtml(record.strategy_id)}</strong><span>${escapeHtml(record.lifecycle)} · ${escapeHtml(record.state)}</span><span>${escapeHtml(record.mode)}</span><span>${record.metric_ids.length} metrics</span><span>${proposalsByStrategy.get(record.strategy_id) ?? 0} proposals</span><span>${fills?.fill_count ?? 0} fills</span><span>qty ${escapeHtml(fills?.filled_quantity_ticks ?? "0")}</span></div>`;
  }).join("");
  return `<div class="strategy-comparison-table" role="table" aria-label="Strategy comparison"><div class="strategy-comparison-row strategy-comparison-header"><strong>Strategy</strong><span>State</span><span>Mode</span><span>Inputs</span><span>Live proposals</span><span>Fills</span><span>Filled qty</span></div>${rows || `<div class="empty">No installed strategies available for comparison</div>`}</div><div class="muted">Values are read-only registry/proposal/fill projections; no performance value is inferred when an authoritative record is absent.</div>`;
}

function depthMarkup(): string {
  const symbol = symbolFor("depth");
  const quote = store.state.quotes[symbol];
  if (!quote) return `<div class="empty">No quote for ${escapeHtml(symbol)}</div>`;
  const top = quote.bookTop;
  if (!top) return `<div class="empty">Level-2 depth unavailable for ${escapeHtml(symbol)}</div>`;
  return `<div class="metric"><span>Bid</span><span>${top[0]} × ${top[1]}</span></div><div class="metric"><span>Ask</span><span>${top[2]} × ${top[3]}</span></div><div class="muted">Book top sequence ${quote.sequence}; deeper levels are unavailable from the current provider.</div>`;
}

function timeSalesMarkup(): string {
  const symbol = symbolFor("time-sales");
  const quote = store.state.quotes[symbol];
  if (!quote) return `<div class="empty">No prints for ${escapeHtml(symbol)}</div>`;
  const trades = store.state.tradesBySymbol[symbol] ?? [];
  const ordered = trades.slice().reverse();
  const start = Math.max(0, Math.floor(timeSalesScrollTop / TAPE_ROW_HEIGHT) - 4);
  const end = Math.min(ordered.length, start + Math.ceil(TAPE_VIEWPORT_HEIGHT / TAPE_ROW_HEIGHT) + 8);
  const rows = ordered.slice(start, end).map((trade) => `<div class="metric"><span>#${trade.sequence}</span><span>${trade.priceTicks} × ${trade.quantityTicks}</span></div>`).join("");
  const feed = ordered.length === 0
    ? `<div class="metric"><span>Latest print</span><span>${quote.lastTicks}</span></div>`
    : `<div class="tape-virtual-spacer" style="height:${ordered.length * TAPE_ROW_HEIGHT}px;position:relative"><div style="position:absolute;left:0;right:0;top:${start * TAPE_ROW_HEIGHT}px">${rows}</div></div>`;
  return `<div class="time-sales-viewport" data-time-sales-viewport style="height:${TAPE_VIEWPORT_HEIGHT}px;overflow:auto">${feed}</div><div class="metric"><span>Bid / ask</span><span>${quote.bidTicks} / ${quote.askTicks}</span></div><div class="muted">${trades.length} canonical prints retained in the bounded runtime snapshot.</div>`;
}

function correlationMarkup(): string {
  const symbols = Object.keys(store.state.tradesBySymbol).sort();
  if (symbols.length < 2) return `<div class="empty">At least two symbols with retained trade history are required</div>`;
  const returns = new Map<string, number[]>();
  for (const symbol of symbols) {
    const prices = (store.state.tradesBySymbol[symbol] ?? []).map((trade) => trade.priceTicks).filter((price) => price > 0);
    const series: number[] = [];
    for (let index = 1; index < prices.length; index += 1) series.push((prices[index] - prices[index - 1]) / prices[index - 1]);
    if (series.length >= 2) returns.set(symbol, series);
  }
  const available = [...returns.keys()];
  if (available.length < 2) return `<div class="empty">At least two symbols need two consecutive prints</div>`;
  const corr = (left: number[], right: number[]): number | undefined => {
    const count = Math.min(left.length, right.length);
    if (count < 2) return undefined;
    const a = left.slice(-count); const b = right.slice(-count);
    const meanA = a.reduce((sum, value) => sum + value, 0) / count;
    const meanB = b.reduce((sum, value) => sum + value, 0) / count;
    let numerator = 0; let varianceA = 0; let varianceB = 0;
    for (let index = 0; index < count; index += 1) { const da = a[index] - meanA; const db = b[index] - meanB; numerator += da * db; varianceA += da * da; varianceB += db * db; }
    const denominator = Math.sqrt(varianceA * varianceB);
    return denominator > 0 ? numerator / denominator : undefined;
  };
  const header = `<div class="metric"><span></span>${available.map((symbol) => `<strong>${escapeHtml(symbol)}</strong>`).join("")}</div>`;
  const rows = available.map((left) => `<div class="metric"><strong>${escapeHtml(left)}</strong>${available.map((right) => { const value = left === right ? 1 : corr(returns.get(left) ?? [], returns.get(right) ?? []); return `<span>${value === undefined ? "—" : value.toFixed(2)}</span>`; }).join("")}</div>`).join("");
  return `${header}${rows}<div class="muted">Pearson correlation of aligned retained trade returns; values are descriptive and bounded by available history.</div>`;
}

function heatmapMarkup(): string {
  const rows = Object.values(store.state.quotes).sort((left, right) => left.symbol.localeCompare(right.symbol)).map((quote) => {
    const trades = store.state.tradesBySymbol[quote.symbol] ?? [];
    const first = trades[0]?.priceTicks;
    const change = first && first > 0 ? (quote.lastTicks - first) / first * 100 : undefined;
    const state = change === undefined ? "insufficient history" : `${change >= 0 ? "+" : ""}${change.toFixed(2)}%`;
    return `<div class="metric"><span>${escapeHtml(quote.symbol)}</span><span>${state}</span></div>`;
  }).join("");
  return rows || `<div class="empty">No canonical quotes available</div>`;
}

function render(): void {
  if (!root) return;
  const state = store.state;
  const watchlist = watchlistMarkup();
  const positions = state.positions.map((position) => {
    const pnlSign = position.pnlTicks > 0 ? "+" : position.pnlTicks < 0 ? "−" : "";
    const pnlClass = position.pnlTicks > 0 ? "positive" : position.pnlTicks < 0 ? "negative" : "muted";
    return `<div class="metric"><span>${escapeHtml(position.symbol)} × ${position.quantityTicks} @ ${position.averageCostTicks}</span><strong class="${pnlClass}" aria-label="P&L ${position.pnlTicks >= 0 ? "gain" : "loss"}">PnL ${pnlSign}${Math.abs(position.pnlTicks)}</strong><button type="button" data-close-position="${escapeHtml(position.symbol)}" aria-label="Draft close order for ${escapeHtml(position.symbol)}">Close</button></div>`;
  }).join("") || `<div class="empty">No reconciled positions</div>`;
  const workingOrderCount = state.orders.filter((order) => ["created", "risk_approved", "queued", "sending", "sent", "acknowledged", "partially_filled"].includes(order.state)).length;
  const orders = `<div class="orders-toolbar"><button type="button" data-cancel-all-orders aria-live="polite" aria-label="Cancel all ${workingOrderCount} working orders" ${workingOrderCount > 0 ? "" : "disabled"}>Cancel all working orders</button></div>${state.orders.map((order) => {
    const cancellable = ["created", "risk_approved", "queued", "sending", "sent", "acknowledged", "partially_filled"].includes(order.state);
    const orderLabel = `${order.side.toUpperCase()} ${order.instrumentId} ${order.quantityTicks}`;
    return `<div class="metric"><span>${escapeHtml(order.side.toUpperCase())} ${escapeHtml(order.instrumentId)} × ${order.quantityTicks}</span><span>${escapeHtml(order.state)}${order.filledQuantityTicks ? ` · ${order.filledQuantityTicks} filled` : ""}${cancellable ? ` <button data-cancel-order="${escapeHtml(order.clientOrderId)}" aria-label="Cancel ${escapeHtml(orderLabel)} order">Cancel</button><button data-replace-order="${escapeHtml(order.clientOrderId)}" data-replace-quantity="${order.quantityTicks}" aria-label="Replace ${escapeHtml(orderLabel)} order">Replace</button>` : ""}</span></div>`;
  }).join("") || `<div class="empty">No working orders</div>`}`;
  const proposals = state.proposals.map((proposal) => `<div class="metric"><span>${escapeHtml(proposal.strategyId)} · ${escapeHtml(proposal.symbol)}</span><button data-proposal="${escapeHtml(proposal.proposalId)}" aria-label="Preview ${escapeHtml(proposal.strategyId)} proposal for ${escapeHtml(proposal.symbol)}">Preview</button></div>`).join("") || `<div class="empty">No active proposals</div>`;
  const chartCandles = resampleCandles(state.chart.candles, state.selectedTimeframe);
  const chartRenderStarted = typeof performance !== "undefined" ? performance.now() : Date.now();
  const chartSvg = renderChartSvg({
    ...state.chart,
    candles: chartCandles,
    news: chartPreferences.showNews ? state.chart.news : [],
    strategies: chartPreferences.showStrategies ? state.chart.strategies : [],
    metrics: chartPreferences.showMetrics ? state.chart.metrics : [],
  }, { width: 640, height: 220 }, chartPreferences.mode, chartView, chartPreferences.gridlineDensity);
  const chartRenderFinished = typeof performance !== "undefined" ? performance.now() : Date.now();
  chartRenderMs = Math.max(0, chartRenderFinished - chartRenderStarted);
  const change = chartCandles.length >= 2
    ? (() => {
      const first = chartCandles[0].closeTicks;
      const last = chartCandles.at(-1)?.closeTicks ?? first;
      return first > 0 ? ((last - first) / first) * 100 : undefined;
    })()
    : undefined;
  const changeMarkup = change === undefined || !Number.isFinite(change)
    ? `<span class="market-change muted" aria-label="Change unavailable">—</span>`
    : `<span class="market-change ${change >= 0 ? "positive" : "negative"}" aria-label="${change >= 0 ? "Gain" : "Loss"} ${Math.abs(change).toFixed(2)} percent">${change >= 0 ? "+" : "−"}${Math.abs(change).toFixed(2)}%</span>`;
  const chartSpan = Math.max(1, Math.min(chartCandles.length, chartView ? chartView.end - chartView.start : chartCandles.length));
  const chartStart = Math.max(0, Math.min(Math.max(0, chartCandles.length - chartSpan), chartView?.start ?? 0));
  const hoverCandle = chartHoverIndex === undefined ? undefined : chartCandles[Math.max(chartStart, Math.min(chartStart + chartSpan - 1, chartHoverIndex))];
  const chartCrosshair = hoverCandle
    ? `<div class="chart-crosshair-v" style="left:${(((chartHoverIndex! - chartStart + 0.5) / chartSpan) * 100).toFixed(3)}%" aria-hidden="true"></div>${chartHoverYPercent === undefined ? "" : `<div class="chart-crosshair-h" style="top:${chartHoverYPercent.toFixed(3)}%" aria-hidden="true"></div>`}<div class="chart-crosshair-readout"><strong>${new Date(hoverCandle.timeMs).toISOString()}</strong><span>O ${hoverCandle.openTicks} · H ${hoverCandle.highTicks} · L ${hoverCandle.lowTicks} · C ${hoverCandle.closeTicks}</span></div>`
    : "";
  const chartContextMenu = chartContextOpen ? `<div class="chart-context-menu" role="menu"><button type="button" data-chart-context-action="open-alerts">Open alerts</button><button type="button" data-chart-context-action="draw-horizontal">Add horizontal level</button><button type="button" data-chart-context-action="toggle-metrics">${chartPreferences.showMetrics ? "Hide" : "Show"} metric overlays</button><button type="button" data-chart-context-action="clear-drawings">Clear drawings</button><button type="button" data-chart-context-action="reset-view">Reset zoom</button><button type="button" data-chart-context-action="close">Close</button></div>` : "";
  const chartConnectionNotice = state.connection === "ready" ? "" : `<div class="warning chart-connection-notice" role="status" aria-live="polite">Market connection ${escapeHtml(state.connection)}; displayed prices may be stale. New orders remain subject to engine freshness checks.</div>`;
  const selectedProposalIds = new Set(state.autonomy.selectedProposalIds ?? []);
  const selectedAutonomyProposals = state.proposals.filter((proposal) => selectedProposalIds.has(proposal.proposalId));
  const autonomyProposalCards = selectedAutonomyProposals.map((proposal) => `<article class="autonomy-proposal"><strong>${escapeHtml(proposal.strategyId)}</strong><span class="muted"> · ${escapeHtml(proposal.symbol)} · ${escapeHtml(proposal.action)}</span><div class="metric"><span>Confidence</span><span>${(proposal.confidence * 100).toFixed(1)}% · expires ${new Date(proposal.expiresAtMs).toISOString()}</span></div><div class="muted">${proposal.rationaleCodes.map(escapeHtml).join(", ") || "No rationale codes"}</div></article>`).join("");
  const autonomyContext = `symbol ${state.selectedSymbol} · timeframe ${state.selectedTimeframe} · ${state.proposals.length} live proposals · risk ${state.risk.state}`;
  const autonomyTimeline = [
    ["Plan", state.autonomy.planId ?? "No plan"],
    ["Validation", state.autonomy.planState ?? "Not validated"],
    ["Risk gate", state.risk.state === "running" ? "Available for normal risk evaluation" : `Restrictive state: ${state.risk.state}`],
    ["Reconsider", state.autonomy.reconsiderAfterMs ? new Date(state.autonomy.reconsiderAfterMs).toISOString() : "Not scheduled"],
  ].map(([label, value]) => `<div class="metric"><span>${label}</span><span>${escapeHtml(value)}</span></div>`).join("");
  const autonomy = `<label class="mode-selector">Trading mode<select data-trading-mode><option value="manual" ${state.autonomy.mode === "manual" ? "selected" : ""}>Manual</option><option value="hybrid" ${state.autonomy.mode === "hybrid" ? "selected" : ""}>Hybrid</option><option value="autonomous" ${state.autonomy.mode === "autonomous" ? "selected" : ""}>Autonomous</option></select></label><div class="muted">Mode changes are journaled and use the same proposal/risk/execution path. Plan approval never bypasses portfolio, risk, execution, or reconciliation.</div><h3>Policy and context</h3><div class="metric"><span>Policy mode</span><span>${state.autonomy.mode.toUpperCase()} · risk ${state.risk.state}</span></div><div class="metric"><span>Context snapshot</span><span>${escapeHtml(autonomyContext)}</span></div><div class="metric"><span>Model</span><span>${escapeHtml(state.autonomy.model ?? "—")}</span></div><div class="metric"><span>Pending actions</span><span>${state.autonomy.pendingActionCount}</span></div><h3>Plan validation timeline</h3>${autonomyTimeline}<h3>Selected proposals</h3>${autonomyProposalCards || `<div class="empty">No selected proposals in the authoritative runtime snapshot</div>`}<h3>All active proposals</h3>${proposals}`;
  const workspaceTemplates = workspaceAddOpen ? `<div class="workspace-template-menu" role="menu" aria-label="Workspace templates">${(["Scalping", "Swing", "Research", "Backtest"] as const).map((template) => `<button type="button" role="menuitem" data-workspace-create-template="${template}">${template}</button>`).join("")}<button type="button" role="menuitem" data-workspace-duplicate>Duplicate current</button><button type="button" role="menuitem" data-workspace-rename ${WORKSPACE_PRESETS.includes(workspaceLayout.name as WorkspacePreset) ? "disabled" : ""}>Rename current</button><button type="button" role="menuitem" data-workspace-delete ${WORKSPACE_PRESETS.includes(workspaceLayout.name as WorkspacePreset) ? "disabled" : ""}>Delete current</button></div>` : "";
  const workspaceSelector = `<nav class="workspace-tabs" role="tablist" aria-label="Workspaces">${workspaceTabOrder.map((preset, index) => `<button id="workspace-tab-${escapeHtml(preset)}" type="button" role="tab" class="workspace-tab" draggable="true" data-workspace-tab="${escapeHtml(preset)}" tabindex="${workspaceLayout.name === preset ? "0" : "-1"}" aria-setsize="${workspaceTabOrder.length}" aria-posinset="${index + 1}" aria-controls="workspace-main" aria-selected="${workspaceLayout.name === preset}">${index + 1} · ${escapeHtml(preset)}</button>`).join("")}<span class="workspace-add-wrap"><button type="button" class="workspace-tab workspace-tab-add" data-workspace-add aria-expanded="${workspaceAddOpen}" aria-label="Create workspace from template">+</button>${workspaceTemplates}</span></nav>`;
  const dockPanels: readonly RightDockTab[] = ["positions", "orders", "watchlist", "alerts"];
  const availableDockPanels = dockPanels.filter((panelId) => workspaceLayout.panels.includes(panelId));
  if (!availableDockPanels.includes(rightDockTab)) rightDockTab = availableDockPanels[0] ?? "positions";
  const dockBody: Record<RightDockTab, string> = { positions, orders, watchlist, alerts: alertsMarkup() };
  const rightDock = workspaceLayout.name === "Trading" && availableDockPanels.length > 0
    ? `<div class="right-dock-shell"><div class="right-dock-splitter" data-right-dock-splitter role="separator" aria-label="Resize right dock" tabindex="0"></div><aside class="right-dock" aria-label="Trading dock"><nav class="right-dock-tabs" role="tablist" aria-label="Trading panels">${availableDockPanels.map((tab) => `<button id="right-dock-tab-${tab}" type="button" role="tab" tabindex="${rightDockTab === tab ? "0" : "-1"}" aria-controls="right-dock-panel" aria-selected="${rightDockTab === tab}" data-right-dock-tab="${tab}">${tab[0].toUpperCase()}${tab.slice(1)}</button>`).join("")}</nav><div id="right-dock-panel" role="tabpanel" aria-labelledby="right-dock-tab-${rightDockTab}">${panel(rightDockTab, rightDockTab, dockBody[rightDockTab])}</div></aside></div>`
    : "";
  const toolsRail = workspaceLayout.name === "Trading"
    ? `<aside class="tools-rail${toolsRailOpen ? " tools-rail-open" : ""}" aria-label="Trading tools"><button type="button" data-tools-toggle aria-expanded="${toolsRailOpen}" aria-label="${toolsRailOpen ? "Collapse" : "Expand"} tools rail">${toolsRailOpen ? "‹" : "›"}</button><button type="button" data-tool-focus="chart" title="Chart"><span aria-hidden="true">⌁</span><span class="tool-label">Chart</span></button><button type="button" data-tool-focus="order-ticket" title="Order ticket"><span aria-hidden="true">＋</span><span class="tool-label">Order</span></button><button type="button" data-tool-focus="metrics" title="Metrics"><span aria-hidden="true">∿</span><span class="tool-label">Metrics</span></button><button type="button" data-tool-focus="risk" title="Risk"><span aria-hidden="true">△</span><span class="tool-label">Risk</span></button></aside>`
    : "";
  const multiChartPanels = workspaceLayout.name === "MultiChart"
    ? (["chart-secondary", "chart-tertiary", "chart-quaternary"] as const).map((panelId, index) => panel(`Chart ${index + 2}`, panelId, `<div class="chart-surface" data-chart-render-ms="${chartRenderMs.toFixed(2)}">${chartSvg}</div><div class="muted">Linked ${escapeHtml(symbolFor(panelId))} · ${escapeHtml(timeframeFor(panelId))}</div>`)).join("")
    : "";
    root.innerHTML = `${settingsMarkup()}${commandPaletteMarkup()}<div class="shell" data-workspace="${escapeHtml(workspaceLayout.name)}"><header class="topbar"><span class="brand">${identity.name}</span><span class="muted">${escapeHtml(workspaceLayout.name)} · ${escapeHtml(state.selectedSymbol)} · ${escapeHtml(state.selectedTimeframe)}</span><span class="mode">${state.autonomy.mode.toUpperCase()}</span><span class="workspace-actions"><button type="button" data-settings-open aria-label="Open settings">⚙</button><button type="button" data-command-open aria-label="Open command palette">⌘K</button>${workspaceSelector}<button type="button" data-restore-panels>Restore hidden panels</button></span></header><div id="workspace-main" class="workspace-main" role="tabpanel" aria-labelledby="workspace-tab-${escapeHtml(workspaceLayout.name)}" style="--right-dock-width:${rightDockWidth}px">${toolsRail}<div class="grid">${panel("Watchlist", "watchlist", watchlist)}${panel("Chart", "chart", `${chartDrawingMarkup()}<div class="chart-surface" data-chart-render-ms="${chartRenderMs.toFixed(2)}">${chartSvg}${chartCrosshair}${chartContextMenu}</div><div class="metric"><span>Bid / Ask</span><span>${state.quotes[state.selectedSymbol]?.bidTicks ?? "—"} / ${state.quotes[state.selectedSymbol]?.askTicks ?? "—"}</span></div><div class="muted ${chartRenderMs > CHART_FRAME_BUDGET_MS ? "chart-render-over-budget" : ""}" aria-label="Chart render duration">Chart render ${chartRenderMs.toFixed(2)} ms${chartRenderMs > CHART_FRAME_BUDGET_MS ? " · over 60 FPS budget" : ""}</div>`)}${panel("Configuration", "configuration", configurationMarkup())}${panel("Screener", "screener", screenerMarkup())}${panel("Global Search", "global-search", globalSearchMarkup())}${panel("Order Ticket", "order-ticket", orderTicketMarkup())}${panel("Orders", "orders", orders)}${panel("Portfolio", "portfolio", portfolioMarkup())}${panel("Strategy Browser", "strategy-browser", strategyBrowserMarkup())}${panel("Strategy Comparison", "strategy-comparison", strategyComparisonMarkup())}${panel("Strategy Inspector", "strategy-inspector", strategyInspectorMarkup())}${panel("Metrics", "metrics", metricsMarkup())}${panel("Metric Inspector", "metric-inspector", metricInspectorMarkup())}${panel("Risk", "risk", riskMarkup())}${panel("Depth", "depth", depthMarkup())}${panel("Time & Sales", "time-sales", timeSalesMarkup())}${panel("Correlation", "correlation", correlationMarkup())}${panel("Heatmap", "heatmap", heatmapMarkup())}${panel("Broker Status", "broker-status", brokerStatusMarkup())}${panel("TCA", "tca", tcaMarkup())}${panel("Backtest", "backtest", backtestMarkup())}${panel("Experiment Registry", "experiment-registry", experimentRegistryMarkup())}${panel("Model Registry", "model-registry", modelRegistryMarkup())}${panel("Alerts", "alerts", alertsMarkup())}${panel("Logs / Trace", "trace", traceMarkup())}${panel("News", "news", newsMarkup())}${newsDetailMarkup()}${panel("AI Analyst", "ai-analyst", analystMarkup())}${panel("Positions", "positions", positions)}${panel("Autonomy", "autonomy", autonomy)}${panel("System Health", "system-health", systemHealthMarkup())}${panel("Backup & Restore", "backup", backupMarkup())}</div>${rightDock}</div><footer class="statusbar">Runtime ${state.connection} · cursor ${state.cursor} · state v${state.version} · ${state.autonomy.stale ? "analysis stale" : "analysis current"}${messageStationMarkup()}</footer></div>`;
  if (workspaceDialog) {
    root.insertAdjacentHTML("afterbegin", workspaceDialogMarkup());
    const dialogElement = root.querySelector<HTMLElement>(".workspace-dialog");
    dialogElement?.setAttribute("aria-describedby", "workspace-dialog-description");
    dialogElement?.querySelector("p")?.setAttribute("id", "workspace-dialog-description");
  }
  if (scheduleConfirmation) root.insertAdjacentHTML("afterbegin", scheduleConfirmationMarkup());
  if (cancelAllConfirmation) root.insertAdjacentHTML("afterbegin", cancelAllConfirmationMarkup());
  if (replaceOrderDialog) root.insertAdjacentHTML("afterbegin", replaceOrderDialogMarkup());
  if (chartTemplateDialog) root.insertAdjacentHTML("afterbegin", chartTemplateDialogMarkup());
  if (lifecycleDialog) root.insertAdjacentHTML("afterbegin", lifecycleDialogMarkup());
  if (modelEvidenceDialog) root.insertAdjacentHTML("afterbegin", modelEvidenceDialogMarkup());
  if (autonomyModeDialog) root.insertAdjacentHTML("afterbegin", autonomyModeDialogMarkup());
  // Modal surfaces must own keyboard focus and pointer interaction. The
  // workspace remains mounted for stable state, but is inert while a modal is
  // open so background controls cannot be activated by keyboard or assistive
  // technology.
  const modalBackground = root.querySelector<HTMLElement>(".shell");
  if (modalBackground && (settingsOpen || commandPaletteOpen || workspaceDialog || scheduleConfirmation || cancelAllConfirmation || replaceOrderDialog || chartTemplateDialog || lifecycleDialog || modelEvidenceDialog || autonomyModeDialog)) {
    modalBackground.setAttribute("inert", "");
    modalBackground.setAttribute("aria-hidden", "true");
  }
  const chartPanel = root.querySelector<HTMLElement>("#panel-chart");
  if (chartPanel && state.connection !== "ready") {
    chartPanel.querySelector(".panel-header")?.insertAdjacentHTML("afterend", `<div class="warning chart-connection-notice" role="status" aria-live="polite">Market connection ${escapeHtml(state.connection)}; displayed prices may be stale. New orders remain subject to engine freshness checks.</div>`);
  }
  const configGenerator = root.querySelector<HTMLElement>(".config-generator");
  if (configGenerator) {
    const cfgText = configSnapshot?.cfg_text ?? "";
    const brokerMode = configStringValue(cfgText, "broker.mode", "paper");
    const referenceEnabled = configBooleanValue(cfgText, "strategy.reference_enabled", false);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Alert webhook URL (optional HTTPS)<input data-config-webhook-url type="url" maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "alerts.webhook_url", ""))}" placeholder="https://ops.example/alerts" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>UI status poll (ms)<input data-config-ui-status-poll type="number" min="1000" max="60000" value="${configNumericValue(cfgText, "ui.status_poll_ms", "5000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Alert poll (ms)<input data-config-alert-poll type="number" min="500" max="60000" value="${configNumericValue(cfgText, "ui.alert_poll_ms", "1000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>News stale threshold (ms)<input data-config-news-stale-after type="number" min="60000" max="3600000" value="${configNumericValue(cfgText, "ui.news_stale_after_ms", "300000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Analyst stale threshold (ms)<input data-config-analyst-stale-after type="number" min="60000" max="3600000" value="${configNumericValue(cfgText, "ui.analyst_stale_after_ms", "300000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>LLM model<input data-config-llm-model maxlength="256" value="${escapeHtml(configStringValue(cfgText, "llm.model", "configured-model"))}" /></label><label>Analyst prompt version<input data-config-llm-prompt-version maxlength="256" value="${escapeHtml(configStringValue(cfgText, "llm.prompt_version", "ai-analyst.v1"))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Alert dedupe cooldown (ms)<input data-config-alert-cooldown type="number" min="0" max="86400000" value="${configNumericValue(cfgText, "alerts.cooldown_ms", "60000")}" /></label><label>Alert pending capacity<input data-config-alert-max-pending type="number" min="1" max="1000000" value="${configNumericValue(cfgText, "alerts.max_pending", "4096")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Supervisor max failures<input data-config-supervisor-failures type="number" min="1" max="1000000" value="${configNumericValue(cfgText, "supervisor.max_failures", "3")}" /></label><label>Supervisor failure window (ns)<input data-config-supervisor-window type="number" min="1" value="${configNumericValue(cfgText, "supervisor.window_ns", "60000000000")}" /></label><label>Supervisor initial backoff (ns)<input data-config-supervisor-initial-backoff type="number" min="1" value="${configNumericValue(cfgText, "supervisor.initial_backoff_ns", "100000000")}" /></label><label>Supervisor max backoff (ns)<input data-config-supervisor-max-backoff type="number" min="1" value="${configNumericValue(cfgText, "supervisor.max_backoff_ns", "30000000000")}" /></label><label>Supervisor jitter (bps)<input data-config-supervisor-jitter type="number" min="0" max="10000" value="${configNumericValue(cfgText, "supervisor.jitter_bps", "1000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Python executable<input data-config-python-executable maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "python.executable", "python3"))}" /></label><label>Python worker directory<input data-config-python-workdir maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "python.workdir", "data/python-workers"))}" /></label><label>Metrics package root<input data-config-python-metrics-root maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "python.metrics_root", "metrics"))}" /></label><label>Strategies package root<input data-config-python-strategies-root maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "python.strategies_root", "strategies"))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>IBKR price scale<input data-config-ibkr-scale type="number" min="1" max="1000000000" value="${configNumericValue(cfgText, "broker.ibkr_price_scale", "10000")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>IBKR base URL<input data-config-ibkr-base-url maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "broker.ibkr_base_url", "https://127.0.0.1:5000"))}" /></label>`);
    const embeddingsEnabled = configStringValue(cfgText, "embeddings.model", "") !== "" || configStringValue(cfgText, "embeddings.model_version", "") !== "" || configNumericValue(cfgText, "embeddings.dimensions", "") !== "";
    configGenerator.insertAdjacentHTML("afterbegin", `<label><input type="checkbox" data-config-reference-enabled ${referenceEnabled ? "checked" : ""} /> Enable reference strategy</label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label><input type="checkbox" data-config-embeddings-enabled ${embeddingsEnabled ? "checked" : ""} /> Enable context embeddings</label><label>Embedding model<input data-config-embedding-model maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "embeddings.model", ""))}" /></label><label>Embedding version<input data-config-embedding-version maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "embeddings.model_version", ""))}" /></label><label>Embedding dimensions<input data-config-embedding-dimensions type="number" min="1" max="4096" value="${configNumericValue(cfgText, "embeddings.dimensions", "768")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>NewsAPI base URL<input data-config-newsapi-base-url maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "news.newsapi_base_url", "https://newsapi.org"))}" /></label><label>NewsAPI endpoint<select data-config-newsapi-endpoint><option value="everything" ${configStringValue(cfgText, "news.newsapi_endpoint", "everything") === "everything" ? "selected" : ""}>Everything</option><option value="top-headlines" ${configStringValue(cfgText, "news.newsapi_endpoint", "everything") === "top-headlines" ? "selected" : ""}>Top headlines</option></select></label><label>Yahoo symbols (SYMBOL=INSTRUMENT,… optional)<input data-config-yahoo-symbols maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "market.yahoo_symbols", ""))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>IBKR account ID (required in IBKR mode)<input data-config-ibkr-account maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "broker.ibkr_account_id", ""))}" /></label><label>IBKR conid (optional quote polling)<input data-config-ibkr-conid inputmode="numeric" maxlength="32" value="${escapeHtml(configStringValue(cfgText, "broker.ibkr_conid", ""))}" /></label><label>IBKR instrument ID (optional quote polling)<input data-config-ibkr-instrument-id inputmode="numeric" maxlength="40" value="${escapeHtml(configStringValue(cfgText, "broker.ibkr_instrument_id", ""))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>NewsAPI country (top headlines)<input data-config-newsapi-country maxlength="2" value="${escapeHtml(configStringValue(cfgText, "news.newsapi_country", ""))}" /></label><label>NewsAPI category (top headlines)<select data-config-newsapi-category><option value="">None</option>${["business", "entertainment", "general", "health", "science", "sports", "technology"].map((category) => `<option value="${category}" ${configStringValue(cfgText, "news.newsapi_category", "") === category ? "selected" : ""}>${category}</option>`).join("")}</select></label><label>NewsAPI sources (comma-separated)<input data-config-newsapi-sources maxlength="512" value="${escapeHtml(configStringValue(cfgText, "news.newsapi_sources", ""))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label><input type="checkbox" data-config-allow-yahoo-live-marks ${configBooleanValue(cfgText, "market.allow_yahoo_live_marks", false) ? "checked" : ""} /> Allow Yahoo marks in IBKR mode</label><label><input type="checkbox" data-config-allow-ibkr-bootstrap-mark ${configBooleanValue(cfgText, "broker.allow_ibkr_bootstrap_mark", false) ? "checked" : ""} /> Allow synthetic IBKR bootstrap marks</label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>NewsAPI query (optional)<input data-config-newsapi-query maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "news.newsapi_query", ""))}" /></label><label>Yahoo news query (optional)<input data-config-yahoo-query maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "news.yahoo_query", ""))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>EWMA lambda<input data-config-ewma-lambda type="number" min="0.000001" max="0.999999" step="0.000001" value="${configNumericValue(cfgText, "metric.ewma_lambda", "0.94")}" /></label><label>Metric TTL (ns)<input data-config-metric-ttl type="number" min="1" value="${configNumericValue(cfgText, "metric.reference_ttl_ns", "5000000000")}" /></label><label>SMA window<input data-config-sma-window type="number" min="1" max="10000" value="${configNumericValue(cfgText, "metric.sma_window", "20")}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Reference entry threshold<input data-config-reference-entry type="number" step="0.01" value="${configNumericValue(cfgText, "strategy.reference_entry_threshold", "0.5")}" /></label><label>Reference exit threshold<input data-config-reference-exit type="number" step="0.01" value="${configNumericValue(cfgText, "strategy.reference_exit_threshold", "0.1")}" /></label><label>Reference quantity (ticks)<input data-config-reference-quantity type="number" min="1" value="${configNumericValue(cfgText, "strategy.reference_quantity_ticks", "1")}" /></label><label>Reference horizon (ns)<input data-config-reference-horizon type="number" min="1" value="${configNumericValue(cfgText, "strategy.reference_horizon_ns", "900000000000")}" /></label><label>Reference TTL (ns)<input data-config-reference-ttl type="number" min="1" value="${configNumericValue(cfgText, "strategy.reference_ttl_ns", "5000000000")}" /></label><label>Reference strategy ID<input data-config-reference-id maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "strategy.reference_id", "microstructure.imbalance.threshold.v1"))}" /></label><label>Reference metric ID<input data-config-reference-metric-id maxlength="2048" value="${escapeHtml(configStringValue(cfgText, "strategy.reference_metric_id", "microstructure.imbalance.v1"))}" /></label>`);
    configGenerator.insertAdjacentHTML("afterbegin", `<label>Broker mode<select data-config-broker-mode><option value="paper" ${brokerMode === "paper" ? "selected" : ""}>Paper</option><option value="ibkr" ${brokerMode === "ibkr" ? "selected" : ""}>IBKR</option></select></label><label>Max position (ticks)<input data-config-max-position type="number" min="1" value="${configNumericValue(cfgText, "risk.max_position_ticks", "1000000")}" /></label><label>Max gross notional (ticks)<input data-config-max-notional type="number" min="1" value="${configNumericValue(cfgText, "risk.max_gross_notional_ticks", "100000000000")}" /></label><label>IBKR timeout (ms)<input data-config-ibkr-timeout type="number" min="1000" max="120000" value="${configNumericValue(cfgText, "broker.ibkr_timeout_ms", "10000")}" /></label><label>IBKR quote poll (ms)<input data-config-ibkr-poll type="number" min="250" max="60000" value="${configNumericValue(cfgText, "broker.ibkr_market_poll_ms", "1000")}" /></label><label>Python CPU budget (s)<input data-config-python-cpu type="number" min="1" max="86400" value="${configNumericValue(cfgText, "python.cpu_seconds", "3600")}" /></label><label>Python memory budget (bytes)<input data-config-python-memory type="number" min="67108864" value="${configNumericValue(cfgText, "python.memory_bytes", "536870912")}" /></label><label><input type="checkbox" data-config-python-allow-network ${configBooleanValue(cfgText, "python.allow_network", false) ? "checked" : ""} /> Allow Python worker network access</label>`);
  }
  root.querySelectorAll<HTMLSelectElement>("[data-chart-mode], [data-settings-chart-mode]").forEach((select) => {
    if (!select.querySelector('option[value="bars"]')) {
      const option = document.createElement("option");
      option.value = "bars";
      option.textContent = "OHLC bars";
      option.selected = chartPreferences.mode === "bars";
      select.insertBefore(option, select.options[1] ?? null);
    }
  });
  const grid = root.querySelector<HTMLElement>(".grid");
  if (grid && multiChartPanels) grid.insertAdjacentHTML("beforeend", multiChartPanels);
  root.querySelector<HTMLElement>(".topbar .muted")?.insertAdjacentHTML("afterend", changeMarkup);
  if (grid) {
    const panels = new Map<string, HTMLElement>();
    grid.querySelectorAll<HTMLElement>("[data-panel-id]").forEach((element) => panels.set(element.dataset.panelId ?? "", element));
    for (const panelId of workspaceLayout.panels) {
      const element = panels.get(panelId);
      if (element) grid.appendChild(element);
    }
    const visible = new Set(workspaceLayout.panels);
    for (const [panelId, element] of panels) {
      if (!visible.has(panelId)) element.remove();
    }
  }
  const chartSurface = root.querySelector<HTMLElement>(".chart-surface");
  if (chartSurface) {
    const candleCount = resampleCandles(state.chart.candles, state.selectedTimeframe).length;
    const touchPointers = new Map<number, { readonly x: number; readonly y: number }>();
    let pinchDistance: number | undefined;
    let pinchLastCenter: { readonly x: number; readonly y: number } | undefined;
    const normalizedView = (): ChartViewWindow => {
      const span = Math.max(1, Math.min(candleCount, chartView ? chartView.end - chartView.start : candleCount));
      const start = Math.max(0, Math.min(Math.max(0, candleCount - span), chartView?.start ?? 0));
      return { start, end: start + span };
    };
    chartSurface.addEventListener("wheel", (event) => {
      if (candleCount < 2) return;
      event.preventDefault();
      const current = normalizedView();
      if (!event.ctrlKey && Math.abs(event.deltaX) > Math.abs(event.deltaY)) {
        const span = current.end - current.start;
        const pixelsPerCandle = chartSurface.clientWidth / Math.max(1, span);
        const nextStart = Math.max(0, Math.min(candleCount - span, current.start + Math.round(event.deltaX / Math.max(1, pixelsPerCandle))));
        chartView = { start: nextStart, end: nextStart + span };
        render();
        return;
      }
      const rect = chartSurface.getBoundingClientRect();
      const anchor = Math.max(0, Math.min(1, (event.clientX - rect.left) / Math.max(1, rect.width)));
      const scale = event.deltaY < 0 ? 0.8 : 1.25;
      const nextSpan = Math.max(8, Math.min(candleCount, Math.round((current.end - current.start) * scale)));
      const anchorIndex = current.start + anchor * (current.end - current.start);
      const nextStart = Math.round(anchorIndex - anchor * nextSpan);
      chartView = { start: Math.max(0, Math.min(candleCount - nextSpan, nextStart)), end: Math.max(1, Math.min(candleCount, Math.max(0, Math.min(candleCount - nextSpan, nextStart)) + nextSpan)) };
      render();
    }, { passive: false });
    chartSurface.addEventListener("pointerdown", (event) => {
      if (event.button !== 0 || candleCount < 2) return;
      if (event.pointerType === "touch") {
        touchPointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
        if (touchPointers.size >= 2) {
          chartDrag = undefined;
          const points = [...touchPointers.values()];
          pinchDistance = Math.hypot(points[0].x - points[1].x, points[0].y - points[1].y);
          pinchLastCenter = { x: (points[0].x + points[1].x) / 2, y: (points[0].y + points[1].y) / 2 };
          chartSurface.setPointerCapture(event.pointerId);
          return;
        }
      }
      if (chartInertiaFrame !== undefined) {
        globalThis.cancelAnimationFrame(chartInertiaFrame);
        chartInertiaFrame = undefined;
      }
      const current = normalizedView();
      chartDrag = { startX: event.clientX, startStart: current.start };
      chartSurface.style.removeProperty("--chart-pan-translate");
      chartPanVelocity = 0;
      chartPanLastX = event.clientX;
      chartPanLastAt = performance.now();
      chartSurface.setPointerCapture(event.pointerId);
    });
    chartSurface.addEventListener("pointermove", (event) => {
      if (event.pointerType === "touch" && touchPointers.has(event.pointerId)) {
        touchPointers.set(event.pointerId, { x: event.clientX, y: event.clientY });
        if (touchPointers.size >= 2 && pinchDistance && pinchDistance > 0) {
          const points = [...touchPointers.values()];
          const distance = Math.hypot(points[0].x - points[1].x, points[0].y - points[1].y);
          const center = { x: (points[0].x + points[1].x) / 2, y: (points[0].y + points[1].y) / 2 };
          const centerDelta = pinchLastCenter ? center.x - pinchLastCenter.x : 0;
          const scale = pinchDistance / Math.max(1, distance);
          if (Math.abs(scale - 1) > 0.02 || Math.abs(centerDelta) > 0.5) {
            const current = normalizedView();
            const rect = chartSurface.getBoundingClientRect();
            const anchor = Math.max(0, Math.min(1, (center.x - rect.left) / Math.max(1, rect.width)));
            const nextSpan = Math.max(8, Math.min(candleCount, Math.round((current.end - current.start) * scale)));
            const anchorIndex = current.start + anchor * (current.end - current.start);
            const pixelsPerCandle = rect.width / Math.max(1, nextSpan);
            const nextStart = Math.round(anchorIndex - anchor * nextSpan - centerDelta / Math.max(1, pixelsPerCandle));
            const boundedStart = Math.max(0, Math.min(candleCount - nextSpan, nextStart));
            chartView = { start: boundedStart, end: boundedStart + nextSpan };
            pinchDistance = distance;
            pinchLastCenter = center;
            chartSurface.style.setProperty("--chart-pinch-scale", String(Math.max(0.5, Math.min(2, 1 / scale))));
          }
          return;
        }
      }
      if (!chartDrag) return;
      const now = performance.now();
      chartSurface.style.setProperty("--chart-pan-translate", `${event.clientX - chartDrag.startX}px`);
      const elapsed = Math.max(1, now - chartPanLastAt);
      chartPanVelocity = (event.clientX - chartPanLastX) / elapsed;
      chartPanLastX = event.clientX;
      chartPanLastAt = now;
    });
    chartSurface.addEventListener("pointerup", (event) => {
      if (event.pointerType === "touch") {
        const wasPinching = pinchDistance !== undefined;
        touchPointers.delete(event.pointerId);
        if (touchPointers.size < 2) { pinchDistance = undefined; pinchLastCenter = undefined; }
        if (!chartDrag) {
          if (wasPinching) {
            chartSurface.style.removeProperty("--chart-pinch-scale");
            render();
          }
          return;
        }
      }
      if (!chartDrag) return;
      const current = normalizedView();
      const pixelsPerCandle = chartSurface.clientWidth / Math.max(1, current.end - current.start);
      const delta = Math.round((chartDrag.startX - event.clientX) / Math.max(1, pixelsPerCandle));
      const span = current.end - current.start;
      const start = Math.max(0, Math.min(candleCount - span, chartDrag.startStart + delta));
      chartView = { start, end: start + span };
      chartDrag = undefined;
      chartSurface.style.removeProperty("--chart-pan-translate");
      render();
      const initialVelocity = chartPanVelocity / Math.max(1, pixelsPerCandle);
      const reducedMotion = globalThis.matchMedia?.("(prefers-reduced-motion: reduce)").matches ?? false;
      if (!reducedMotion && Math.abs(initialVelocity) > 0.01) {
        let velocity = initialVelocity;
        let last = performance.now();
        const step = (now: number): void => {
          const deltaMs = Math.min(40, Math.max(1, now - last));
          last = now;
          const currentView = normalizedView();
          const nextStart = Math.max(0, Math.min(candleCount - span, Math.round(currentView.start - velocity * deltaMs)));
          chartView = { start: nextStart, end: nextStart + span };
          velocity *= Math.pow(0.001, deltaMs / 350);
          render();
          if (Math.abs(velocity) > 0.01 && nextStart > 0 && nextStart < candleCount - span) chartInertiaFrame = globalThis.requestAnimationFrame(step);
          else chartInertiaFrame = undefined;
        };
        chartInertiaFrame = globalThis.requestAnimationFrame(step);
      }
    });
    chartSurface.addEventListener("pointercancel", (event) => {
      touchPointers.delete(event.pointerId);
      pinchDistance = undefined;
      pinchLastCenter = undefined;
      chartDrag = undefined;
      chartHoverIndex = undefined;
      chartHoverYPercent = undefined;
      chartSurface.style.removeProperty("--chart-pinch-scale");
      chartSurface.style.removeProperty("--chart-pan-translate");
      render();
    });
    chartSurface.addEventListener("dblclick", () => {
      chartView = undefined;
      chartHoverIndex = undefined;
      chartHoverYPercent = undefined;
      render();
    });
    chartSurface.addEventListener("pointermove", (event) => {
      if (candleCount < 1) return;
      const current = normalizedView();
      const rect = chartSurface.getBoundingClientRect();
      const anchor = Math.max(0, Math.min(0.999, (event.clientX - rect.left) / Math.max(1, rect.width)));
      const nextIndex = Math.max(current.start, Math.min(current.end - 1, current.start + Math.round(anchor * Math.max(0, current.end - current.start - 1))));
      const nextYPercent = Math.max(0, Math.min(100, ((event.clientY - rect.top) / Math.max(1, rect.height)) * 100));
      if (nextIndex === chartHoverIndex && chartHoverYPercent !== undefined && Math.abs(nextYPercent - chartHoverYPercent) < 0.1) return;
      chartHoverIndex = nextIndex;
      chartHoverYPercent = nextYPercent;
      if (chartHoverRenderFrame === undefined) {
        chartHoverRenderFrame = globalThis.requestAnimationFrame(() => {
          chartHoverRenderFrame = undefined;
          render();
        });
      }
    });
    chartSurface.addEventListener("pointerleave", () => {
      if (chartHoverIndex === undefined) return;
      chartHoverIndex = undefined;
      chartHoverYPercent = undefined;
      render();
    });
    chartSurface.addEventListener("contextmenu", (event) => {
      event.preventDefault();
      chartContextOpen = true;
      render();
    });
  }
  root.querySelectorAll<HTMLElement>("[data-chart-context-action]").forEach((element) => element.addEventListener("click", () => {
    const action = element.dataset.chartContextAction;
    chartContextOpen = false;
    if (action === "reset-view") chartView = undefined;
    if (action === "draw-horizontal") root.querySelector<HTMLElement>("[data-drawing-horizontal]")?.click();
    if (action === "clear-drawings") root.querySelector<HTMLElement>("[data-drawing-clear]")?.click();
    if (action === "toggle-metrics") {
      chartPreferences = { ...chartPreferences, showMetrics: !chartPreferences.showMetrics };
      persistChartPreferences();
    }
    if (action === "open-alerts") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["alerts", ...workspaceLayout.panels.filter((panelId) => panelId !== "alerts")] });
      workspacePersistence.schedule(workspaceLayout);
    }
    render();
  }));
  let draggedPanel: string | undefined;
  root.querySelectorAll<HTMLElement>("[data-panel-id]").forEach((element) => {
    element.addEventListener("dragstart", (event) => {
      draggedPanel = element.dataset.panelId;
      if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
    });
    element.addEventListener("dragover", (event) => event.preventDefault());
    element.addEventListener("drop", (event) => {
      event.preventDefault();
      const target = element.dataset.panelId;
      if (!draggedPanel || !target || draggedPanel === target) return;
      const panels = [...workspaceLayout.panels];
      const from = panels.indexOf(draggedPanel);
      const to = panels.indexOf(target);
      if (from < 0 || to < 0) return;
      panels.splice(from, 1);
      panels.splice(to, 0, draggedPanel);
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels });
      workspacePersistence.schedule(workspaceLayout);
      render();
    });
  });
  root.querySelectorAll<HTMLButtonElement>("[data-close-panel]").forEach((button) => button.addEventListener("click", () => {
    const panelId = button.dataset.closePanel;
    if (!panelId) return;
    workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: workspaceLayout.panels.filter((value) => value !== panelId) });
    workspacePersistence.schedule(workspaceLayout);
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-restore-panels]")?.addEventListener("click", () => {
    workspaceLayout = completeWorkspaceLayout(workspaceLayout);
    workspacePersistence.schedule(workspaceLayout);
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-workspace-tab]").forEach((element) => {
    element.addEventListener("click", () => {
      const value = element.dataset.workspaceTab ?? "";
      if (!allWorkspaceNames().includes(value)) return;
      switchWorkspace(value);
      workspacePersistence.schedule(workspaceLayout);
      render();
    });
    element.addEventListener("keydown", (event) => {
      if (!(event instanceof KeyboardEvent) || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      const tabs = [...root.querySelectorAll<HTMLElement>("[data-workspace-tab]")];
      const index = tabs.indexOf(element);
      if (index < 0) return;
      const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
      event.preventDefault();
      tabs[next]?.focus();
      tabs[next]?.click();
    });
  });
  root.querySelector<HTMLElement>("[data-workspace-add]")?.addEventListener("click", () => {
    workspaceAddOpen = !workspaceAddOpen;
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-workspace-create-template]").forEach((element) => element.addEventListener("click", () => {
    const template = element.dataset.workspaceCreateTemplate as WorkspacePreset | undefined;
    if (!template || !["Scalping", "Swing", "Research", "Backtest"].includes(template)) return;
    workspaceAddOpen = false;
    switchWorkspace(template);
    workspacePersistence.schedule(workspaceLayout);
    render();
  }));
  root.querySelector<HTMLElement>("[data-workspace-duplicate]")?.addEventListener("click", () => {
    if (customWorkspaces.length >= 8) return;
    workspaceDialog = { mode: "duplicate", initialName: `${workspaceLayout.name} Copy` };
    workspaceAddOpen = false;
    render();
  });
  root.querySelector<HTMLElement>("[data-workspace-rename]")?.addEventListener("click", () => {
    const current = customWorkspaces.find((workspace) => workspace.name === workspaceLayout.name);
    if (!current) return;
    workspaceDialog = { mode: "rename", initialName: current.name };
    render();
  });
  root.querySelector<HTMLElement>("[data-workspace-delete]")?.addEventListener("click", () => {
    const current = customWorkspaces.find((workspace) => workspace.name === workspaceLayout.name);
    if (!current) return;
    workspaceDialog = { mode: "delete", initialName: current.name };
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-workspace-dialog-close], [data-workspace-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (element.hasAttribute("data-workspace-dialog-close") && event.target !== element && !(event.target instanceof HTMLButtonElement)) return;
    workspaceDialog = undefined;
    render();
  }));
  root.querySelector<HTMLElement>("[data-workspace-dialog-submit]")?.addEventListener("click", () => {
    const dialog = workspaceDialog;
    if (!dialog) return;
    const current = customWorkspaces.find((workspace) => workspace.name === workspaceLayout.name);
    if (dialog.mode === "delete") {
      if (!current || current.name !== dialog.initialName) { workspaceDialog = undefined; render(); return; }
      workspacePersistence.flush();
      workspacePersistence.remove(current.name);
      customWorkspaces = customWorkspaces.filter((workspace) => workspace.name !== current.name);
      workspaceTabOrder = workspaceTabOrder.filter((name) => name !== current.name);
      delete workspaceContexts[current.name];
      persistCustomWorkspaces();
      persistWorkspaceContexts();
      layoutStorage.setItem(WORKSPACE_TAB_ORDER_KEY, JSON.stringify(workspaceTabOrder));
      workspaceDialog = undefined;
      switchWorkspace("Trading");
      workspaceAddOpen = false;
      render();
      return;
    }
    const proposed = root.querySelector<HTMLInputElement>("[data-workspace-dialog-name]")?.value.trim() ?? "";
    if (!WORKSPACE_NAME_PATTERN.test(proposed)) { workspaceDialog = { ...dialog, error: "Name must use 1–32 letters, numbers, spaces, _ or -." }; render(); return; }
    if (allWorkspaceNames().some((name) => name.toLocaleLowerCase() === proposed.toLocaleLowerCase() && name !== dialog.initialName)) { workspaceDialog = { ...dialog, error: "That workspace name is already in use." }; render(); return; }
    workspacePersistence.flush();
    if (dialog.mode === "duplicate") {
      if (customWorkspaces.length >= 8) { workspaceDialog = { ...dialog, error: "The maximum of 8 custom workspaces has been reached." }; render(); return; }
      customWorkspaces = [...customWorkspaces, { name: proposed, base: WORKSPACE_PRESETS.includes(workspaceLayout.name as WorkspacePreset) ? workspaceLayout.name as WorkspacePreset : customWorkspaces.find((workspace) => workspace.name === workspaceLayout.name)?.base ?? "Trading" }];
      const copiedContext = workspaceContexts[workspaceLayout.name] ?? { symbol: store.state.selectedSymbol, timeframe: store.state.selectedTimeframe };
      workspaceContexts[proposed] = copiedContext;
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, name: proposed, linkGroup: proposed.toLowerCase() });
    } else if (current) {
      customWorkspaces = customWorkspaces.map((workspace) => workspace.name === current.name ? { ...workspace, name: proposed } : workspace);
      workspaceTabOrder = workspaceTabOrder.map((name) => name === current.name ? proposed : name);
      const renamedContext = workspaceContexts[current.name] ?? { symbol: store.state.selectedSymbol, timeframe: store.state.selectedTimeframe };
      delete workspaceContexts[current.name];
      workspaceContexts[proposed] = renamedContext;
      workspacePersistence.remove(current.name);
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, name: proposed, linkGroup: proposed.toLowerCase() });
    }
    persistCustomWorkspaces();
    persistWorkspaceContexts();
    workspaceTabOrder = workspaceTabOrder.includes(proposed) ? workspaceTabOrder : [...workspaceTabOrder, proposed];
    layoutStorage.setItem(WORKSPACE_TAB_ORDER_KEY, JSON.stringify(workspaceTabOrder));
    workspacePersistence.schedule(workspaceLayout);
    workspaceDialog = undefined;
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-workspace-tab]").forEach((element) => {
    element.addEventListener("dragstart", (event) => {
      const preset = element.dataset.workspaceTab;
      if (!preset) return;
      draggedWorkspaceTab = preset;
      if (event.dataTransfer) event.dataTransfer.effectAllowed = "move";
    });
    element.addEventListener("dragover", (event) => event.preventDefault());
    element.addEventListener("drop", (event) => {
      event.preventDefault();
      const target = element.dataset.workspaceTab;
      if (!draggedWorkspaceTab || !target || draggedWorkspaceTab === target) return;
      const next = [...workspaceTabOrder];
      const from = next.indexOf(draggedWorkspaceTab);
      const to = next.indexOf(target);
      if (from < 0 || to < 0) return;
      next.splice(from, 1);
      next.splice(to, 0, draggedWorkspaceTab);
      workspaceTabOrder = next;
      layoutStorage.setItem(WORKSPACE_TAB_ORDER_KEY, JSON.stringify(workspaceTabOrder));
      draggedWorkspaceTab = undefined;
      render();
    });
    element.addEventListener("dragend", () => { draggedWorkspaceTab = undefined; });
  });
  root.querySelector<HTMLElement>("[data-message-toggle]")?.addEventListener("click", () => {
    messageStationOpen = !messageStationOpen;
    render();
  });
  root.querySelector<HTMLElement>("[data-message-open-alerts]")?.addEventListener("click", () => {
    messageStationOpen = false;
    workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["alerts", ...workspaceLayout.panels.filter((panelId) => panelId !== "alerts")] });
    workspacePersistence.schedule(workspaceLayout);
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-right-dock-tab]").forEach((element) => {
    element.addEventListener("click", () => {
    const tab = element.dataset.rightDockTab as RightDockTab | undefined;
    if (!tab || !["positions", "orders", "watchlist", "alerts"].includes(tab)) return;
    rightDockTab = tab;
    persistRightDockTab();
    render();
    });
    element.addEventListener("keydown", (event) => {
      if (!(event instanceof KeyboardEvent) || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
      const tabs = [...root.querySelectorAll<HTMLElement>("[data-right-dock-tab]")];
      const index = tabs.indexOf(element);
      if (index < 0) return;
      const next = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
      event.preventDefault();
      tabs[next]?.focus();
      tabs[next]?.click();
    });
  });
  const dockSplitter = root.querySelector<HTMLElement>("[data-right-dock-splitter]");
  const workspaceMain = root.querySelector<HTMLElement>(".workspace-main");
  if (dockSplitter && workspaceMain) {
    let startX = 0;
    let startWidth = rightDockWidth;
    dockSplitter.addEventListener("pointerdown", (event) => {
      startX = event.clientX;
      startWidth = rightDockWidth;
      dockSplitter.setPointerCapture(event.pointerId);
      dockSplitter.classList.add("resizing");
    });
    dockSplitter.addEventListener("pointermove", (event) => {
      if (!dockSplitter.hasPointerCapture(event.pointerId)) return;
      rightDockWidth = Math.round(Math.max(RIGHT_DOCK_MIN_WIDTH, Math.min(RIGHT_DOCK_MAX_WIDTH, startWidth - (event.clientX - startX))));
      workspaceMain.style.setProperty("--right-dock-width", `${rightDockWidth}px`);
    });
    const finishResize = (event: PointerEvent): void => {
      if (!dockSplitter.hasPointerCapture(event.pointerId)) return;
      dockSplitter.releasePointerCapture(event.pointerId);
      dockSplitter.classList.remove("resizing");
      layoutStorage.setItem(RIGHT_DOCK_WIDTH_KEY, String(rightDockWidth));
    };
    dockSplitter.addEventListener("pointerup", finishResize);
    dockSplitter.addEventListener("pointercancel", finishResize);
    dockSplitter.addEventListener("keydown", (event) => {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
      event.preventDefault();
      rightDockWidth = Math.round(Math.max(RIGHT_DOCK_MIN_WIDTH, Math.min(RIGHT_DOCK_MAX_WIDTH, rightDockWidth + (event.key === "ArrowLeft" ? -16 : 16))));
      workspaceMain.style.setProperty("--right-dock-width", `${rightDockWidth}px`);
      layoutStorage.setItem(RIGHT_DOCK_WIDTH_KEY, String(rightDockWidth));
    });
  }
  root.querySelector<HTMLElement>("[data-tools-toggle]")?.addEventListener("click", () => {
    toolsRailOpen = !toolsRailOpen;
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-tool-focus]").forEach((element) => element.addEventListener("click", () => {
    const panelId = element.dataset.toolFocus;
    if (!panelId) return;
    root.querySelector<HTMLElement>(`.grid [data-panel-id="${panelId}"]`)?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }));
  root.querySelectorAll<HTMLElement>("[data-command-open]").forEach((element) => element.addEventListener("click", () => {
    commandPaletteOpen = true;
    commandPaletteQuery = "";
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-settings-open]").forEach((element) => element.addEventListener("click", () => {
    settingsOpen = true;
    settingsQuery = "";
    render();
    root.querySelector<HTMLInputElement>("[data-settings-search]")?.focus();
  }));
  root.querySelectorAll<HTMLElement>("[data-settings-close]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && !(event.target instanceof HTMLButtonElement)) return;
    settingsOpen = false;
    render();
  }));
  root.querySelector<HTMLInputElement>("[data-settings-search]")?.addEventListener("input", (event) => {
    settingsQuery = (event.target as HTMLInputElement).value.slice(0, 128);
    render();
  });
  root.querySelectorAll<HTMLInputElement>("[data-hotkey-action]").forEach((input) => input.addEventListener("change", () => {
    const action = input.dataset.hotkeyAction as HotkeyAction | undefined;
    if (!action || !HOTKEY_ACTIONS.includes(action)) return;
    const normalized = normalizeHotkey(input.value);
    if (!normalized) { hotkeyError = "Use a binding such as Mod+K or Mod+1."; render(); return; }
    const duplicate = HOTKEY_ACTIONS.find((candidate) => candidate !== action && hotkeys[candidate] === normalized);
    if (duplicate) { hotkeyError = `${normalized} is already assigned to ${duplicate === "commandPalette" ? "Command palette" : duplicate.replace("workspace", "Workspace ")}.`; render(); return; }
    hotkeys = { ...hotkeys, [action]: normalized };
    hotkeyError = "";
    persistHotkeys();
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-hotkeys-reset]")?.addEventListener("click", () => {
    hotkeys = { ...DEFAULT_HOTKEYS };
    hotkeyError = "";
    persistHotkeys();
    render();
  });
  root.querySelector<HTMLElement>("[data-settings-reset-layout]")?.addEventListener("click", () => {
    switchWorkspace(workspaceLayout.name as WorkspacePreset);
    settingsOpen = false;
    render();
  });
  root.querySelectorAll<HTMLButtonElement>("[data-settings-open-panel]").forEach((button) => button.addEventListener("click", () => {
    const panelId = button.dataset.settingsOpenPanel as import("../stores/runtime-store").PanelId | undefined;
    if (!panelId) return;
    workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: [panelId, ...workspaceLayout.panels.filter((value) => value !== panelId)] });
    workspacePersistence.schedule(workspaceLayout);
    settingsOpen = false;
    render();
    root.querySelector<HTMLElement>(`.grid [data-panel-id="${panelId}"]`)?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }));
  root.querySelector<HTMLInputElement>("[data-colorblind-toggle]")?.addEventListener("change", (event) => {
    colorblindMode = (event.target as HTMLInputElement).checked;
    layoutStorage.setItem(COLORBLIND_STORAGE_KEY, String(colorblindMode));
    document.documentElement.dataset.colorblind = colorblindMode ? "true" : "false";
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-font-scale]")?.addEventListener("change", (event) => {
    const value = Number((event.target as HTMLSelectElement).value);
    if (![0.9, 1, 1.1].includes(value)) return;
    fontScale = value;
    layoutStorage.setItem(FONT_SCALE_STORAGE_KEY, String(fontScale));
    document.documentElement.style.setProperty("--ui-font-scale", String(fontScale));
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-settings-chart-mode]")?.addEventListener("change", (event) => {
    const mode = (event.target as HTMLSelectElement).value;
    if (mode !== "candles" && mode !== "bars" && mode !== "line" && mode !== "area") return;
    chartPreferences = { ...chartPreferences, mode };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-settings-gridlines]")?.addEventListener("change", (event) => {
    const value = (event.target as HTMLSelectElement).value;
    if (value !== "none" && value !== "low" && value !== "high") return;
    chartPreferences = { ...chartPreferences, gridlineDensity: value };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-settings-order-type]")?.addEventListener("change", (event) => {
    const value = (event.target as HTMLSelectElement).value;
    if (value !== "market" && value !== "limit") return;
    defaultOrderType = value;
    layoutStorage.setItem(ORDER_DEFAULTS_STORAGE_KEY, JSON.stringify({ version: 1, type: defaultOrderType, quantity: defaultOrderQuantity }));
    render();
  });
  root.querySelector<HTMLInputElement>("[data-settings-order-quantity]")?.addEventListener("change", (event) => {
    const value = Number((event.target as HTMLInputElement).value);
    if (!Number.isSafeInteger(value) || value < 1 || value > 1_000_000) return;
    defaultOrderQuantity = value;
    layoutStorage.setItem(ORDER_DEFAULTS_STORAGE_KEY, JSON.stringify({ version: 1, type: defaultOrderType, quantity: defaultOrderQuantity }));
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-command-close]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && !(event.target instanceof HTMLButtonElement)) return;
    commandPaletteOpen = false;
    commandPaletteQuery = "";
    render();
  }));
  root.querySelector<HTMLInputElement>("[data-command-search]")?.addEventListener("input", (event) => {
    commandPaletteQuery = (event.target as HTMLInputElement).value.slice(0, 96);
    render();
    root.querySelector<HTMLInputElement>("[data-command-search]")?.focus();
  });
  root.querySelectorAll<HTMLButtonElement>("[data-command]").forEach((element) => element.addEventListener("click", async () => {
    const command = element.dataset.command ?? "";
    if (command.startsWith("workspace:")) {
      const preset = command.slice("workspace:".length);
      if (allWorkspaceNames().includes(preset)) switchWorkspace(preset);
    } else if (command === "panels:restore") workspaceLayout = completeWorkspaceLayout(workspaceLayout);
    else if (command.startsWith("mode:")) {
      const mode = command.slice("mode:" );
      if (mode === "manual" || mode === "hybrid" || mode === "autonomous") {
        if (mode === "autonomous") {
          commandPaletteOpen = false;
          autonomyModeDialog = {};
          render();
        } else {
          try { await commands.setTradingMode(mode); await session.connect(); } catch (error) { traceError = error instanceof Error ? error.message : "trading mode change failed"; }
        }
      }
    }
    else if (command.startsWith("focus:")) {
      const panelId = ({
        "focus:global-search": "global-search",
        "focus:order-ticket": "order-ticket",
        "focus:alerts": "alerts",
        "focus:autonomy": "autonomy",
        "focus:news": "news",
        "focus:watchlist": "watchlist",
        "focus:metrics": "metrics",
        "focus:strategy-analysis": "strategy-inspector",
        "focus:backtest": "backtest",
        "focus:trace": "trace",
      } as Record<string, string>)[command];
      if (panelId) workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: [panelId as import("../stores/runtime-store").PanelId, ...workspaceLayout.panels.filter((value) => value !== panelId)] });
    }
    workspacePersistence.schedule(workspaceLayout);
    commandPaletteOpen = false;
    commandPaletteQuery = "";
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-symbol]").forEach((element) => element.addEventListener("click", () => {
    const symbol = element.dataset.symbol ?? "";
    const sourcePanel = element.closest<HTMLElement>("[data-panel-id]")?.dataset.panelId;
    const source = (LINKABLE_PANELS as readonly string[]).includes(sourcePanel ?? "") ? sourcePanel as LinkablePanel : "chart";
    setPanelSymbol(source, symbol);
    render();
  }));
  root.querySelectorAll<HTMLSelectElement>("[data-link-panel]").forEach((select) => select.addEventListener("change", () => {
    const panelId = select.dataset.linkPanel;
    const group = select.value;
    if (!(LINKABLE_PANELS as readonly string[]).includes(panelId ?? "") || !LINK_GROUPS.includes(group as LinkGroup)) return;
    panelLinks[panelId as LinkablePanel] = group as LinkGroup;
    persistPanelLinks();
    if (group !== "none") {
      setPanelSymbol(panelId as LinkablePanel, symbolFor(panelId as LinkablePanel));
      setPanelTimeframe(panelId as LinkablePanel, timeframeFor(panelId as LinkablePanel));
    }
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-global-result-kind]").forEach((element) => element.addEventListener("click", async () => {
    const kind = element.dataset.globalResultKind;
    const id = element.dataset.globalResultId ?? "";
    if (kind === "instrument") store.selectSymbol(id);
    else if (kind === "news") {
      selectedNewsDetail = await commands.getNewsDetail(id).catch(() => undefined);
    } else if (kind === "order") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["orders", ...workspaceLayout.panels.filter((panelId) => panelId !== "orders")] });
    } else if (kind === "trace") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["trace", ...workspaceLayout.panels.filter((panelId) => panelId !== "trace")] });
    } else if (kind === "strategy") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["strategy-comparison", "strategy-browser", "strategy-inspector", ...workspaceLayout.panels.filter((panelId) => !["strategy-comparison", "strategy-browser", "strategy-inspector"].includes(panelId))] });
    } else if (kind === "metric") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["metrics", ...workspaceLayout.panels.filter((panelId) => panelId !== "metrics")] });
    } else if (kind === "model") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["model-registry", ...workspaceLayout.panels.filter((panelId) => panelId !== "model-registry")] });
    } else if (kind === "experiment") {
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["experiment-registry", ...workspaceLayout.panels.filter((panelId) => panelId !== "experiment-registry")] });
    }
    workspacePersistence.schedule(workspaceLayout);
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-context-hit-id]").forEach((element) => element.addEventListener("click", () => {
    const nodeId = element.dataset.contextHitId;
    if (!nodeId) return;
    selectedContextHit = contextSearchResults.find((hit) => hit.nodeId === nodeId);
    if (!selectedContextHit) return;
    const [kind, ...parts] = nodeId.split(":");
    const identifier = parts.join(":");
    const panelByKind: Record<string, import("../stores/runtime-store").PanelId> = {
      newsitem: "news", news: "news", strategy: "strategy-inspector", metric: "metrics",
      order: "orders", model: "model-registry", experiment: "experiment-registry", position: "positions",
    };
    const panelId = panelByKind[kind.toLowerCase()];
    if (kind.toLowerCase() === "instrument" && /^[A-Z0-9.\-]{1,16}$/.test(identifier)) store.selectSymbol(identifier);
    if (panelId) workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: [panelId, ...workspaceLayout.panels.filter((value) => value !== panelId)] });
    workspacePersistence.schedule(workspaceLayout);
    render();
  }));
  root.querySelector<HTMLInputElement>("[data-chart-news]")?.addEventListener("change", (event) => {
    chartPreferences = { ...chartPreferences, showNews: (event.target as HTMLInputElement).checked };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-chart-timeframe]")?.addEventListener("change", (event) => {
    const timeframe = (event.target as HTMLSelectElement).value;
    if (!VALID_TIMEFRAMES.includes(timeframe as (typeof VALID_TIMEFRAMES)[number])) return;
    setPanelTimeframe("chart", timeframe);
    layoutStorage.setItem(TIMEFRAME_STORAGE_KEY, timeframe);
    chartPreferences = loadChartPreferences();
    store.replaceChartDrawings(loadAndMigrateDrawings());
    void loadNewsPage(store.state.newsScope, symbolFor("news"));
  });
  root.querySelector<HTMLSelectElement>("[data-chart-mode]")?.addEventListener("change", (event) => {
    const mode = (event.target as HTMLSelectElement).value;
    if (mode !== "candles" && mode !== "bars" && mode !== "line" && mode !== "area") return;
    chartPreferences = { ...chartPreferences, mode };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-chart-gridlines]")?.addEventListener("change", (event) => {
    const value = (event.target as HTMLSelectElement).value;
    if (value !== "none" && value !== "low" && value !== "high") return;
    chartPreferences = { ...chartPreferences, gridlineDensity: value };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLInputElement>("[data-chart-strategies]")?.addEventListener("change", (event) => {
    chartPreferences = { ...chartPreferences, showStrategies: (event.target as HTMLInputElement).checked };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLInputElement>("[data-chart-metrics]")?.addEventListener("change", (event) => {
    chartPreferences = { ...chartPreferences, showMetrics: (event.target as HTMLInputElement).checked };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-chart-template]")?.addEventListener("change", (event) => {
    const name = (event.target as HTMLSelectElement).value;
    const template = chartTemplates.find((candidate) => candidate.name === name);
    if (!template) return;
    chartPreferences = { ...template.preferences };
    persistChartPreferences();
    render();
  });
  root.querySelector<HTMLButtonElement>("[data-chart-template-save]")?.addEventListener("click", () => {
    chartTemplateDialog = { mode: "save", initialName: "" };
    render();
  });
  root.querySelector<HTMLButtonElement>("[data-chart-template-delete]")?.addEventListener("click", () => {
    const select = root.querySelector<HTMLSelectElement>("[data-chart-template]");
    const name = select?.value ?? "";
    if (!name || !chartTemplates.some((template) => template.name === name)) return;
    chartTemplateDialog = { mode: "delete", initialName: name };
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-chart-template-dialog-close], [data-chart-template-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.chartTemplateDialogClose !== undefined) return;
    chartTemplateDialog = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-chart-template-dialog-submit]")?.addEventListener("click", () => {
    const dialog = chartTemplateDialog;
    if (!dialog) return;
    if (dialog.mode === "delete") {
      chartTemplates = chartTemplates.filter((template) => template.name !== dialog.initialName);
      persistChartTemplates();
      chartTemplateDialog = undefined;
      render();
      return;
    }
    const name = root.querySelector<HTMLInputElement>("[data-chart-template-name]")?.value.trim() ?? "";
    if (!name || name.length > 64 || /[\u0000-\u001f]/.test(name)) {
      chartTemplateDialog = { ...dialog, error: "Template name must be 1–64 characters without control characters." };
      render();
      return;
    }
    chartTemplates = [{ name, preferences: { ...chartPreferences } }, ...chartTemplates.filter((template) => template.name !== name)].slice(0, MAX_CHART_TEMPLATES);
    persistChartTemplates();
    chartTemplateDialog = undefined;
    render();
  });
  root.querySelector<HTMLFormElement>("[data-watchlist-form]")?.addEventListener("submit", (event) => {
    event.preventDefault();
    const input = (event.currentTarget as HTMLFormElement).elements.namedItem("symbol");
    const symbol = input instanceof HTMLInputElement ? input.value.trim().toUpperCase() : "";
    if (!/^[A-Z0-9.\-]{1,16}$/.test(symbol) || watchlistSymbols.includes(symbol) || watchlistSymbols.length >= MAX_WATCHLIST_SYMBOLS) return;
    watchlistError = "";
    void commands.resolveInstrument(symbol).then((resolution) => {
      const canonicalSymbol = resolution.symbol.toUpperCase();
      if (watchlistSymbols.includes(canonicalSymbol)) throw new Error("symbol is already on the watchlist");
      watchlistSymbols = [...watchlistSymbols, canonicalSymbol];
      persistWatchlist();
    }).catch((error) => {
      watchlistError = error instanceof Error ? error.message : "instrument resolution failed";
    }).finally(() => render());
  });
  root.querySelectorAll<HTMLButtonElement>("[data-watchlist-remove]").forEach((element) => element.addEventListener("click", () => {
    const symbol = element.dataset.watchlistRemove;
    if (!symbol || watchlistSymbols.length <= 1) return;
    watchlistSymbols = watchlistSymbols.filter((candidate) => candidate !== symbol);
    persistWatchlist();
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-strategy-lifecycle-id]").forEach((element) => element.addEventListener("click", () => {
    const strategyId = element.dataset.strategyLifecycleId;
    const lifecycle = element.dataset.strategyLifecycleNext;
    if (!strategyId || !lifecycle) return;
    lifecycleDialog = { kind: "strategy", id: strategyId, lifecycle };
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-metric-inspect]").forEach((element) => element.addEventListener("click", () => {
    const metricId = element.dataset.metricInspect;
    if (!metricId) return;
    selectedMetricId = metricId;
    workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["metric-inspector", ...workspaceLayout.panels.filter((panelId) => panelId !== "metric-inspector")] });
    workspacePersistence.schedule(workspaceLayout);
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-metric-lifecycle-id]").forEach((element) => element.addEventListener("click", () => {
    const metricId = element.dataset.metricLifecycleId;
    const lifecycle = element.dataset.metricLifecycleNext;
    if (!metricId || !lifecycle) return;
    lifecycleDialog = { kind: "metric", id: metricId, lifecycle };
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-lifecycle-dialog-close], [data-lifecycle-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.lifecycleDialogClose !== undefined) return;
    lifecycleDialog = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-lifecycle-dialog-submit]")?.addEventListener("click", async () => {
    const dialog = lifecycleDialog;
    const phrase = root.querySelector<HTMLInputElement>("[data-lifecycle-confirmation]")?.value ?? "";
    const evidence = root.querySelector<HTMLInputElement>("[data-lifecycle-evidence]")?.value.trim() ?? "";
    if (!dialog) return;
    if (phrase !== "CONFIRM" || !evidence) {
      lifecycleDialog = { ...dialog, error: phrase !== "CONFIRM" ? "Type CONFIRM exactly to continue." : "Evidence reference is required." };
      render();
      return;
    }
    lifecycleDialog = undefined;
    render();
    try {
      if (dialog.kind === "strategy") {
        await commands.transitionStrategyLifecycle(dialog.id, dialog.lifecycle, phrase, evidence);
        strategyRegistry = await commands.listStrategies();
      } else {
        await commands.transitionMetricLifecycle(dialog.id, dialog.lifecycle, phrase, evidence);
        metricRegistry = await commands.listMetrics();
      }
      render();
    } catch {
      traceError = `${dialog.kind} lifecycle transition failed`;
      render();
    }
  });
  root.querySelector<HTMLButtonElement>("[data-drawing-horizontal]")?.addEventListener("click", () => {
    const candle = store.state.chart.candles.at(-1);
    if (!candle) return;
    const drawing: ChartDrawing = {
      id: `level-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      kind: "horizontal",
      startTimeMs: candle.timeMs,
      startPriceTicks: candle.closeTicks,
      color: "#4da3ff",
      label: `${store.state.selectedSymbol} ${candle.closeTicks}`,
    };
    store.upsertChartDrawing(drawing);
    persistDrawings(store.state.chart.drawings);
  });
  root.querySelector<HTMLButtonElement>("[data-drawing-clear]")?.addEventListener("click", () => {
    store.replaceChartDrawings([]);
    persistDrawings([]);
  });
  root.querySelectorAll<HTMLButtonElement>("[data-popout-panel]").forEach((button) => button.addEventListener("click", () => {
    const panelId = button.dataset.popoutPanel;
    if (!panelId) return;
    const source = root.querySelector<HTMLElement>(`[data-panel-id="${CSS.escape(panelId)}"]`);
    if (!source) return;
    const popup = globalThis.open("", `insidertrader-${panelId}`, "popup,width=980,height=720,resizable=yes,scrollbars=yes,noopener");
    if (!popup) return;
    const clone = source.cloneNode(true) as HTMLElement;
    clone.querySelectorAll("[data-popout-panel]").forEach((element) => element.remove());
    clone.querySelectorAll<HTMLButtonElement | HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>("button, input, select, textarea").forEach((element) => {
      element.disabled = true;
      element.setAttribute("aria-disabled", "true");
    });
    const disclosure = popup.document.createElement("p");
    disclosure.className = "popout-snapshot-note";
    disclosure.textContent = "Read-only snapshot · return to the main workstation for live updates and actions.";
    clone.prepend(disclosure);
    popup.document.title = source.getAttribute("aria-label") ?? "InsiderTrader panel";
    popup.document.body.innerHTML = `<main class="popout-shell"></main>`;
    popup.document.querySelector("main")?.append(clone);
    popup.document.head.insertAdjacentHTML("beforeend", `<style>body{margin:0;background:#0a0c10;color:#f5f7fb;font:13px system-ui}.popout-shell{padding:16px}.panel{border:1px solid rgba(255,255,255,.12);border-radius:0;padding:12px;background:rgba(18,22,30,.9)}.panel-header{display:flex;align-items:center;justify-content:space-between}.panel-header h2{font-size:14px;margin:0 0 10px}.metric{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.08)}button{color:inherit;background:#1e2430;border:1px solid rgba(255,255,255,.15);border-radius:2px;padding:4px 8px}.popout-snapshot-note{margin:0 0 10px;padding:8px;border:1px solid rgba(240,166,63,.45);color:#f0a63f;font:12px system-ui}</style>`);
  }));
  const newsViewport = root.querySelector<HTMLElement>("[data-news-viewport]");
  if (newsViewport) {
    newsViewport.scrollTop = newsScrollTop;
    newsViewport.addEventListener("scroll", () => {
      newsScrollTop = newsViewport.scrollTop;
      if (newsScrollScheduled) return;
      newsScrollScheduled = true;
      globalThis.requestAnimationFrame(() => {
        newsScrollScheduled = false;
        render();
      });
    }, { passive: true });
  }
  const watchlistViewport = root.querySelector<HTMLElement>("[data-watchlist-viewport]");
  if (watchlistViewport) {
    watchlistViewport.scrollTop = watchlistScrollTop;
    watchlistViewport.addEventListener("scroll", () => {
      watchlistScrollTop = watchlistViewport.scrollTop;
      if (tapeScrollScheduled) return;
      tapeScrollScheduled = true;
      globalThis.requestAnimationFrame(() => {
        tapeScrollScheduled = false;
        render();
      });
    }, { passive: true });
  }
  const timeSalesViewport = root.querySelector<HTMLElement>("[data-time-sales-viewport]");
  if (timeSalesViewport) {
    timeSalesViewport.scrollTop = timeSalesScrollTop;
    timeSalesViewport.addEventListener("scroll", () => {
      timeSalesScrollTop = timeSalesViewport.scrollTop;
      if (tapeScrollScheduled) return;
      tapeScrollScheduled = true;
      globalThis.requestAnimationFrame(() => {
        tapeScrollScheduled = false;
        render();
      });
    }, { passive: true });
  }
  root.querySelector<HTMLInputElement>("[data-screener-query]")?.addEventListener("input", (event) => {
    screenerQuery = (event.target as HTMLInputElement).value.slice(0, 32);
    screenerVisibleRows = SCREENER_PAGE_SIZE;
    render();
  });
  root.querySelector<HTMLSelectElement>("[data-screener-sort]")?.addEventListener("change", (event) => {
    const value = (event.target as HTMLSelectElement).value;
    if (value === "symbol" || value === "last" || value === "spread" || value === "confidence") {
      screenerSort = value;
      screenerVisibleRows = SCREENER_PAGE_SIZE;
      render();
    }
  });
  root.querySelector<HTMLElement>("[data-screener-load-more]")?.addEventListener("click", () => {
    screenerVisibleRows = Math.min(screenerVisibleRows + SCREENER_PAGE_SIZE, Object.keys(store.state.quotes).length);
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-news-view]").forEach((element) => element.addEventListener("click", () => {
    const view = element.dataset.newsView;
    if (view !== "relevant" && view !== "all" && view !== "watchlist" && view !== "portfolio") return;
    newsView = view;
    persistNewsView();
    newsScrollTop = 0;
    const engineScope = view === "relevant" ? "relevant" : "all";
    store.setNewsScope(engineScope);
    store.resetNewsPage();
    void loadNewsPage(engineScope, symbolFor("news"));
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-news-view]").forEach((element) => element.addEventListener("keydown", (event) => {
    if (!(event.key === "ArrowLeft" || event.key === "ArrowRight" || event.key === "Home" || event.key === "End")) return;
    const tabs = [...root.querySelectorAll<HTMLButtonElement>("[data-news-view]")];
    const index = tabs.indexOf(element);
    if (index < 0) return;
    const nextIndex = event.key === "Home" ? 0 : event.key === "End" ? tabs.length - 1 : (index + (event.key === "ArrowRight" ? 1 : -1) + tabs.length) % tabs.length;
    event.preventDefault();
    tabs[nextIndex]?.focus();
    tabs[nextIndex]?.click();
  }));
  root.querySelectorAll<HTMLElement>("[data-news-more]").forEach((element) => element.addEventListener("click", () => {
    const state = store.state;
    if (!state.newsHasMore || !state.newsNextCursor) return;
    element.setAttribute("disabled", "true");
    void loadNewsPage(state.newsScope, symbolFor("news"), state.newsNextCursor);
  }));
  root.querySelector<HTMLButtonElement>("[data-news-retry]")?.addEventListener("click", () => {
    void loadNewsPage(newsRetryScope, newsRetrySymbol || symbolFor("news"), newsRetryCursor);
  });
  root.querySelectorAll<HTMLElement>("[data-news-pin]").forEach((element) => element.addEventListener("click", () => {
    const newsId = element.dataset.newsPin;
    if (newsId) store.togglePinnedNews(newsId);
  }));
  root.querySelectorAll<HTMLElement>("[data-news-detail]").forEach((element) => element.addEventListener("click", async () => {
    const itemId = element.dataset.newsDetail;
    if (!itemId) return;
    element.setAttribute("disabled", "true");
    try {
      selectedNewsDetail = await commands.getNewsDetail(itemId);
      render();
    } catch {
      element.textContent = "Unavailable";
      element.removeAttribute("disabled");
    }
  }));
  root.querySelectorAll<SVGElement>("[data-news-marker]").forEach((element) => element.addEventListener("click", async () => {
    const itemId = element.dataset.newsMarker;
    if (!itemId) return;
    try {
      selectedNewsDetail = await commands.getNewsDetail(itemId);
      render();
    } catch {
      traceError = "The linked news item is unavailable";
      render();
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-news-detail-close]").forEach((element) => element.addEventListener("click", () => {
    selectedNewsDetail = undefined;
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-analyze]").forEach((element) => element.addEventListener("click", async () => {
    const input = root.querySelector<HTMLTextAreaElement>("[data-analyst-input]")?.value.trim() ?? "";
    if (!input || analystBusy) return;
    const state = store.state;
    const context: Partial<Record<AnalystContextId, string>> = {
      symbol: state.selectedSymbol,
      timeframe: state.selectedTimeframe,
      cursor: state.cursor,
      news: `${state.news.length} linked news items (${state.pinnedNews.length} pinned)`,
      strategies: `${state.proposals.length} active strategy proposals`,
      positions: `${state.positions.length} reconciled positions`,
    };
    const selectedContext = (Object.entries(context) as [AnalystContextId, string][]).filter(([id]) => analystContextEnabled.has(id));
    const panelByContext: Record<AnalystContextId, import("../stores/runtime-store").PanelId> = { symbol: "chart", timeframe: "chart", cursor: "chart", news: "news", strategies: "strategy-inspector", positions: "positions" };
    const contextLabels: Record<AnalystContextId, string> = { symbol: "Symbol", timeframe: "Timeframe", cursor: "Cursor", news: "News", strategies: "Strategies", positions: "Positions" };
    const evidenceSnapshot = selectedContext.map(([contextId, value]) => ({ contextId, label: contextLabels[contextId], value, panelId: panelByContext[contextId] }));
    const contextualInput = selectedContext.length === 0 ? input : `${input}\n\n[Included runtime context]\n${selectedContext.map(([id, value]) => `${id}: ${value}`).join("\n")}`;
    if (contextualInput.length > 1_048_576) {
      analystError = "Question plus selected context exceeds the 1 MiB request limit";
      render();
      return;
    }
    analystBusy = true;
    analystError = "";
    render();
    try {
      const chunks = await commands.analyzeStream({
        task: "CHART_CONTEXT",
        input: contextualInput,
        contextHash: selectedContext.map(([id, value]) => `${id}:${value}`).join("|") || "manual-only",
        model: configStringValue(configSnapshot?.cfg_text ?? "", "llm.model", "configured-model"),
        promptVersion: configStringValue(configSnapshot?.cfg_text ?? "", "llm.prompt_version", "ai-analyst.v1"),
        maxOutputTokens: 1_024,
        endpoint: "responses",
      });
      const terminal = chunks.at(-1);
      if (!terminal || terminal.kind !== "done") throw new Error("analysis stream ended without a terminal result");
      analystContent = chunks.filter((chunk) => chunk.kind === "delta").map((chunk) => chunk.text).join("") || terminal.text;
      analystReceivedAtMs = Date.now();
      analystStaleNoticeShown = false;
      analystEvidence = Object.freeze(evidenceSnapshot);
    } catch (error) {
      analystError = error instanceof Error ? error.message : "analysis failed";
    } finally {
      analystBusy = false;
      render();
    }
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-analyst-suggestion]").forEach((button) => button.addEventListener("click", () => {
    const suggestion = button.dataset.analystSuggestion?.slice(0, 128);
    const input = root.querySelector<HTMLTextAreaElement>("[data-analyst-input]");
    if (!suggestion || !input) return;
    input.value = suggestion;
    input.focus();
  }));
  root.querySelectorAll<HTMLElement>("[data-analyst-context-remove]").forEach((element) => element.addEventListener("click", () => {
    const id = element.dataset.analystContextRemove as AnalystContextId | undefined;
    if (!id || !analystContextEnabled.has(id)) return;
    analystContextEnabled.delete(id);
    persistAnalystContext();
    render();
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-analyst-evidence-panel]").forEach((button) => button.addEventListener("click", () => {
    const panelId = button.dataset.analystEvidencePanel as import("../stores/runtime-store").PanelId | undefined;
    if (!panelId) return;
    workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: [panelId, ...workspaceLayout.panels.filter((value) => value !== panelId)] });
    workspacePersistence.schedule(workspaceLayout);
    render();
    root.querySelector<HTMLElement>(`.grid [data-panel-id="${panelId}"]`)?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  }));
  root.querySelectorAll<HTMLElement>("[data-context-search]").forEach((element) => element.addEventListener("click", async () => {
    const text = root.querySelector<HTMLInputElement>("[data-context-search-input]")?.value.trim() ?? "";
    if (!text || contextSearchBusy) return;
    contextSearchBusy = true;
    contextSearchError = "";
    render();
    try {
      globalSearchResults = localGlobalSearch(text);
      contextSearchResults = await commands.searchContext(text, `instrument:${store.state.selectedSymbol}`, 3, 50);
    } catch (error) {
      contextSearchError = error instanceof Error ? error.message : "context search failed";
      contextSearchResults = [];
    } finally {
      contextSearchBusy = false;
      render();
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-run-backtest]").forEach((element) => element.addEventListener("click", async () => {
    if (backtestBusy) return;
    const raw = root.querySelector<HTMLTextAreaElement>("[data-backtest-json]")?.value ?? "";
    let request: BacktestRunRequest;
    try {
      const parsed: unknown = JSON.parse(raw);
      if (!parsed || typeof parsed !== "object") throw new Error("request must be an object");
      request = parsed as BacktestRunRequest;
    } catch (error) {
      backtestError = error instanceof Error ? error.message : "invalid backtest JSON";
      render();
      return;
    }
    backtestBusy = true;
    backtestError = "";
    render();
    try {
      backtestResult = await commands.runBacktest(request);
      backtestHistory = await commands.listBacktests();
    } catch (error) {
      backtestError = error instanceof Error ? error.message : "backtest failed";
    } finally {
      backtestBusy = false;
      render();
    }
  }));
  root.querySelector<HTMLButtonElement>("[data-config-reload]")?.addEventListener("click", async () => {
    const cfgText = root.querySelector<HTMLTextAreaElement>("[data-config-text]")?.value ?? "";
    if (!configSnapshot) {
      configError = "Configuration snapshot is not loaded; reload the panel first.";
      render();
      return;
    }
    configBusy = true;
    configError = "";
    render();
    try {
      configSnapshot = await commands.reloadConfig({ expected_version: configSnapshot.version, cfg_text: cfgText });
    } catch (error) {
      configError = error instanceof Error ? error.message : String(error);
    } finally {
      configBusy = false;
      render();
    }
  });
  root.querySelectorAll<HTMLElement>("[data-journal-backup]").forEach((element) => element.addEventListener("click", async () => {
    const path = root.querySelector<HTMLInputElement>("[data-journal-backup-path]")?.value.trim() ?? "";
    if (!path || backupBusy) return;
    backupBusy = true;
    backupError = "";
    render();
    try {
      backupResult = await commands.backupJournal(path);
    } catch (error) {
      backupError = error instanceof Error ? error.message : "journal backup failed";
    } finally {
      backupBusy = false;
      render();
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-journal-restore]").forEach((element) => element.addEventListener("click", async () => {
    const source = root.querySelector<HTMLInputElement>("[data-journal-restore-source]")?.value.trim() ?? "";
    const destination = root.querySelector<HTMLInputElement>("[data-journal-restore-destination]")?.value.trim() ?? "";
    if (!source || !destination || backupBusy) return;
    backupBusy = true;
    backupError = "";
    render();
    try {
      backupResult = await commands.restoreJournal(source, destination);
    } catch (error) {
      backupError = error instanceof Error ? error.message : "journal restore failed";
    } finally {
      backupBusy = false;
      render();
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-risk-state]").forEach((element) => element.addEventListener("click", async () => {
    const next = element.dataset.riskState as "running" | "reduce_only" | "cancel_only" | "halted" | undefined;
    const authorization = root.querySelector<HTMLInputElement>("[data-risk-authorization]")?.value.trim() ?? "";
    if (!next) return;
    element.setAttribute("disabled", "true");
    try {
      await commands.transitionRiskState(next, authorization);
      await session.connect();
    } catch (error) {
      element.textContent = error instanceof Error ? "Rejected" : "Error";
      element.removeAttribute("disabled");
    }
  }));
  root.querySelectorAll<HTMLButtonElement>("[data-model-action]").forEach((element) => element.addEventListener("click", async () => {
    const operation = element.dataset.modelAction as "validate" | "shadow" | "canary" | "promote" | undefined;
    const modelId = element.dataset.modelId;
    const version = element.dataset.modelVersion;
    if (!operation || !modelId || !version) return;
    if ((operation === "validate" || operation === "canary")) {
      modelEvidenceDialog = { modelId, version, operation };
      render();
      return;
    }
    element.disabled = true;
    try {
      await commands.mutateModel({ operation, model_id: modelId, version });
      modelHistory = await commands.listModels();
      render();
    } catch (error) {
      element.textContent = error instanceof Error ? "Rejected" : "Error";
      element.disabled = false;
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-model-evidence-dialog-close], [data-model-evidence-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.modelEvidenceDialogClose !== undefined) return;
    modelEvidenceDialog = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-model-evidence-dialog-submit]")?.addEventListener("click", async () => {
    const dialog = modelEvidenceDialog;
    const evidence = root.querySelector<HTMLInputElement>("[data-model-evidence-input]")?.value.trim() ?? "";
    if (!dialog) return;
    if (!evidence || evidence.length > 128) {
      modelEvidenceDialog = { ...dialog, error: "A bounded evidence reference is required." };
      render();
      return;
    }
    modelEvidenceDialog = undefined;
    render();
    try {
      await commands.mutateModel({ operation: dialog.operation, model_id: dialog.modelId, version: dialog.version, evidence_id: evidence });
      modelHistory = await commands.listModels();
      render();
    } catch (error) {
      traceError = error instanceof Error ? error.message : "model operation failed";
      render();
    }
  });
  root.querySelector<HTMLSelectElement>("[data-trading-mode]")?.addEventListener("change", async (event) => {
    const next = (event.target as HTMLSelectElement).value;
    if (next !== "manual" && next !== "hybrid" && next !== "autonomous") return;
    if (next === "autonomous") {
      autonomyModeDialog = {};
      render();
      return;
    }
    const select = event.target as HTMLSelectElement;
    select.disabled = true;
    try {
      await commands.setTradingMode(next);
      await session.connect();
    } catch (error) {
      select.disabled = false;
      traceError = error instanceof Error ? error.message : "trading mode change failed";
      render();
    }
  });
  root.querySelectorAll<HTMLElement>("[data-autonomy-dialog-close], [data-autonomy-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.autonomyDialogClose !== undefined) return;
    autonomyModeDialog = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-autonomy-dialog-submit]")?.addEventListener("click", async () => {
    const phrase = root.querySelector<HTMLInputElement>("[data-autonomy-confirmation]")?.value ?? "";
    if (phrase !== "CONFIRM") {
      autonomyModeDialog = { error: "Type CONFIRM exactly to enable autonomous mode." };
      render();
      return;
    }
    autonomyModeDialog = undefined;
    render();
    try {
      await commands.setTradingMode("autonomous");
      await session.connect();
    } catch (error) {
      traceError = error instanceof Error ? error.message : "trading mode change failed";
      render();
    }
  });
  root.querySelector<HTMLButtonElement>("[data-config-generate]")?.addEventListener("click", () => {
    const leverage = Number(root.querySelector<HTMLInputElement>("[data-config-leverage]")?.value ?? "2");
    const maxPosition = Number(root.querySelector<HTMLInputElement>("[data-config-max-position]")?.value ?? "1000000");
    const maxNotional = Number(root.querySelector<HTMLInputElement>("[data-config-max-notional]")?.value ?? "100000000000");
    const ibkrTimeout = Number(root.querySelector<HTMLInputElement>("[data-config-ibkr-timeout]")?.value ?? "10000");
    const ibkrPoll = Number(root.querySelector<HTMLInputElement>("[data-config-ibkr-poll]")?.value ?? "1000");
    const ibkrScale = Number(root.querySelector<HTMLInputElement>("[data-config-ibkr-scale]")?.value ?? "10000");
    const ibkrBaseUrl = root.querySelector<HTMLInputElement>("[data-config-ibkr-base-url]")?.value.trim() ?? "";
    const brokerMode = root.querySelector<HTMLSelectElement>("[data-config-broker-mode]")?.value ?? "paper";
    const referenceEnabled = root.querySelector<HTMLInputElement>("[data-config-reference-enabled]")?.checked ?? false;
    const referenceEntry = Number(root.querySelector<HTMLInputElement>("[data-config-reference-entry]")?.value ?? "0.5");
    const referenceExit = Number(root.querySelector<HTMLInputElement>("[data-config-reference-exit]")?.value ?? "0.1");
    const referenceQuantity = Number(root.querySelector<HTMLInputElement>("[data-config-reference-quantity]")?.value ?? "1");
    const referenceHorizon = Number(root.querySelector<HTMLInputElement>("[data-config-reference-horizon]")?.value ?? "900000000000");
    const referenceTtl = Number(root.querySelector<HTMLInputElement>("[data-config-reference-ttl]")?.value ?? "5000000000");
    const referenceId = root.querySelector<HTMLInputElement>("[data-config-reference-id]")?.value.trim() ?? "";
    const referenceMetricId = root.querySelector<HTMLInputElement>("[data-config-reference-metric-id]")?.value.trim() ?? "";
    const embeddingsEnabled = root.querySelector<HTMLInputElement>("[data-config-embeddings-enabled]")?.checked ?? false;
    const embeddingModel = root.querySelector<HTMLInputElement>("[data-config-embedding-model]")?.value.trim() ?? "";
    const embeddingVersion = root.querySelector<HTMLInputElement>("[data-config-embedding-version]")?.value.trim() ?? "";
    const embeddingDimensions = Number(root.querySelector<HTMLInputElement>("[data-config-embedding-dimensions]")?.value ?? "768");
    const newsapiBaseUrl = root.querySelector<HTMLInputElement>("[data-config-newsapi-base-url]")?.value.trim() ?? "";
    const newsapiEndpoint = root.querySelector<HTMLSelectElement>("[data-config-newsapi-endpoint]")?.value ?? "everything";
    const newsapiCountry = root.querySelector<HTMLInputElement>("[data-config-newsapi-country]")?.value.trim().toLowerCase() ?? "";
    const newsapiCategory = root.querySelector<HTMLSelectElement>("[data-config-newsapi-category]")?.value ?? "";
    const newsapiSources = root.querySelector<HTMLInputElement>("[data-config-newsapi-sources]")?.value.trim() ?? "";
    const allowYahooLiveMarks = root.querySelector<HTMLInputElement>("[data-config-allow-yahoo-live-marks]")?.checked ?? false;
    const allowIbkrBootstrapMark = root.querySelector<HTMLInputElement>("[data-config-allow-ibkr-bootstrap-mark]")?.checked ?? false;
    const newsapiQuery = root.querySelector<HTMLInputElement>("[data-config-newsapi-query]")?.value.trim() ?? "";
    const yahooQuery = root.querySelector<HTMLInputElement>("[data-config-yahoo-query]")?.value.trim() ?? "";
    const yahooSymbols = root.querySelector<HTMLInputElement>("[data-config-yahoo-symbols]")?.value.trim() ?? "";
    const ibkrAccount = root.querySelector<HTMLInputElement>("[data-config-ibkr-account]")?.value.trim() ?? "";
    const ibkrConid = root.querySelector<HTMLInputElement>("[data-config-ibkr-conid]")?.value.trim() ?? "";
    const ibkrInstrumentId = root.querySelector<HTMLInputElement>("[data-config-ibkr-instrument-id]")?.value.trim() ?? "";
    const ewmaLambda = Number(root.querySelector<HTMLInputElement>("[data-config-ewma-lambda]")?.value ?? "0.94");
    const metricTtl = Number(root.querySelector<HTMLInputElement>("[data-config-metric-ttl]")?.value ?? "5000000000");
    const smaWindow = Number(root.querySelector<HTMLInputElement>("[data-config-sma-window]")?.value ?? "20");
    const drawdown = Number(root.querySelector<HTMLInputElement>("[data-config-drawdown]")?.value ?? "500");
    const orders = Number(root.querySelector<HTMLInputElement>("[data-config-orders]")?.value ?? "32");
    const volatility = Number(root.querySelector<HTMLInputElement>("[data-config-volatility]")?.value ?? "250");
    const participation = Number(root.querySelector<HTMLInputElement>("[data-config-participation]")?.value ?? "1000");
    const messageRate = Number(root.querySelector<HTMLInputElement>("[data-config-message-rate]")?.value ?? "20");
    const priceDeviation = Number(root.querySelector<HTMLInputElement>("[data-config-price-deviation]")?.value ?? "75");
    const pythonCycle = Number(root.querySelector<HTMLInputElement>("[data-config-python-cycle]")?.value ?? "100");
    const executionCycle = Number(root.querySelector<HTMLInputElement>("[data-config-execution-cycle]")?.value ?? "25");
    const marketAge = Number(root.querySelector<HTMLInputElement>("[data-config-market-age]")?.value ?? "60000");
    const pythonCpu = Number(root.querySelector<HTMLInputElement>("[data-config-python-cpu]")?.value ?? "3600");
    const pythonMemory = Number(root.querySelector<HTMLInputElement>("[data-config-python-memory]")?.value ?? "536870912");
    const pythonAllowNetwork = root.querySelector<HTMLInputElement>("[data-config-python-allow-network]")?.checked ?? false;
    const pythonExecutable = root.querySelector<HTMLInputElement>("[data-config-python-executable]")?.value.trim() ?? "";
    const pythonWorkdir = root.querySelector<HTMLInputElement>("[data-config-python-workdir]")?.value.trim() ?? "";
    const pythonMetricsRoot = root.querySelector<HTMLInputElement>("[data-config-python-metrics-root]")?.value.trim() ?? "";
    const pythonStrategiesRoot = root.querySelector<HTMLInputElement>("[data-config-python-strategies-root]")?.value.trim() ?? "";
    const marketHttpTimeout = Number(root.querySelector<HTMLInputElement>("[data-config-market-http-timeout]")?.value ?? "30000");
    const yahooIntervalNs = Number(root.querySelector<HTMLInputElement>("[data-config-yahoo-interval-ns]")?.value ?? "60000000000");
    const yahooPriceScale = Number(root.querySelector<HTMLInputElement>("[data-config-yahoo-price-scale]")?.value ?? "10000");
    const yahooHistoryPoll = Number(root.querySelector<HTMLInputElement>("[data-config-yahoo-history-poll]")?.value ?? "60000");
    const yahooQuotePoll = Number(root.querySelector<HTMLInputElement>("[data-config-yahoo-quote-poll]")?.value ?? "5000");
    const newsapiPoll = Number(root.querySelector<HTMLInputElement>("[data-config-newsapi-poll]")?.value ?? "30000");
    const yahooNewsPoll = Number(root.querySelector<HTMLInputElement>("[data-config-yahoo-news-poll]")?.value ?? "60000");
    const newsHttpTimeout = Number(root.querySelector<HTMLInputElement>("[data-config-news-http-timeout]")?.value ?? "30000");
    const newsMaxRetries = Number(root.querySelector<HTMLInputElement>("[data-config-news-max-retries]")?.value ?? "4");
    const newsRetryBase = Number(root.querySelector<HTMLInputElement>("[data-config-news-retry-base]")?.value ?? "1000");
    const newsRetryMax = Number(root.querySelector<HTMLInputElement>("[data-config-news-retry-max]")?.value ?? "60000");
    const reconciliationPoll = Number(root.querySelector<HTMLInputElement>("[data-config-reconciliation-poll]")?.value ?? "30000");
    const webhookTimeout = Number(root.querySelector<HTMLInputElement>("[data-config-webhook-timeout]")?.value ?? "2000");
    const webhookPoll = Number(root.querySelector<HTMLInputElement>("[data-config-webhook-poll]")?.value ?? "2000");
    const webhookUrl = root.querySelector<HTMLInputElement>("[data-config-webhook-url]")?.value.trim() ?? "";
    const alertCooldown = Number(root.querySelector<HTMLInputElement>("[data-config-alert-cooldown]")?.value ?? "60000");
    const alertMaxPending = Number(root.querySelector<HTMLInputElement>("[data-config-alert-max-pending]")?.value ?? "4096");
    const supervisorFailures = Number(root.querySelector<HTMLInputElement>("[data-config-supervisor-failures]")?.value ?? "3");
    const supervisorWindow = Number(root.querySelector<HTMLInputElement>("[data-config-supervisor-window]")?.value ?? "60000000000");
    const supervisorInitialBackoff = Number(root.querySelector<HTMLInputElement>("[data-config-supervisor-initial-backoff]")?.value ?? "100000000");
    const supervisorMaxBackoff = Number(root.querySelector<HTMLInputElement>("[data-config-supervisor-max-backoff]")?.value ?? "30000000000");
    const supervisorJitter = Number(root.querySelector<HTMLInputElement>("[data-config-supervisor-jitter]")?.value ?? "1000");
    const llmTimeout = Number(root.querySelector<HTMLInputElement>("[data-config-llm-timeout]")?.value ?? "30000");
    const uiStatusPoll = Number(root.querySelector<HTMLInputElement>("[data-config-ui-status-poll]")?.value ?? "5000");
    const alertPoll = Number(root.querySelector<HTMLInputElement>("[data-config-alert-poll]")?.value ?? "1000");
    const newsStaleAfter = Number(root.querySelector<HTMLInputElement>("[data-config-news-stale-after]")?.value ?? "300000");
    const analystStaleAfter = Number(root.querySelector<HTMLInputElement>("[data-config-analyst-stale-after]")?.value ?? "300000");
    const llmBaseUrl = root.querySelector<HTMLInputElement>("[data-config-llm-base-url]")?.value.trim() ?? "";
    const llmModel = root.querySelector<HTMLInputElement>("[data-config-llm-model]")?.value.trim() ?? "";
    const llmPromptVersion = root.querySelector<HTMLInputElement>("[data-config-llm-prompt-version]")?.value.trim() ?? "";
    const yahooBaseUrl = root.querySelector<HTMLInputElement>("[data-config-yahoo-base-url]")?.value.trim() ?? "";
    const yahooInterval = root.querySelector<HTMLInputElement>("[data-config-yahoo-interval]")?.value.trim() ?? "";
    const yahooRange = root.querySelector<HTMLInputElement>("[data-config-yahoo-range]")?.value.trim() ?? "";
    const cfgStrings = [ibkrBaseUrl, ibkrAccount, ibkrConid, ibkrInstrumentId, newsapiCountry, newsapiCategory, newsapiSources, referenceId, referenceMetricId, embeddingModel, embeddingVersion,
      newsapiBaseUrl, newsapiQuery, yahooQuery, yahooSymbols, webhookUrl, llmBaseUrl, llmModel, llmPromptVersion,
      yahooBaseUrl, yahooInterval, yahooRange, pythonExecutable, pythonWorkdir, pythonMetricsRoot, pythonStrategiesRoot];
    const yahooSymbolsError = validateYahooSymbols(yahooSymbols);
    const integers = [maxPosition, maxNotional, ibkrTimeout, ibkrPoll, ibkrScale, referenceQuantity, referenceHorizon, referenceTtl, metricTtl, smaWindow, drawdown, orders, volatility, participation, messageRate, priceDeviation, pythonCycle, executionCycle, marketAge, pythonCpu, pythonMemory, marketHttpTimeout, yahooIntervalNs, yahooPriceScale, yahooHistoryPoll, yahooQuotePoll, newsapiPoll, yahooNewsPoll, newsHttpTimeout, newsMaxRetries, newsRetryBase, newsRetryMax, reconciliationPoll, webhookTimeout, webhookPoll, alertCooldown, alertMaxPending, supervisorFailures, supervisorWindow, supervisorInitialBackoff, supervisorMaxBackoff, supervisorJitter, llmTimeout, uiStatusPoll, alertPoll, newsStaleAfter, analystStaleAfter];
    let parsedLlmUrl: URL | undefined;
    let parsedYahooUrl: URL | undefined;
    let parsedWebhookUrl: URL | undefined;
    try { parsedLlmUrl = new URL(llmBaseUrl); } catch { parsedLlmUrl = undefined; }
    try { parsedYahooUrl = new URL(yahooBaseUrl); } catch { parsedYahooUrl = undefined; }
    if (webhookUrl) { try { parsedWebhookUrl = new URL(webhookUrl); } catch { parsedWebhookUrl = undefined; } }
    const safeLocalHttp = parsedLlmUrl?.protocol === "http:" && (parsedLlmUrl.hostname === "localhost" || parsedLlmUrl.hostname === "127.0.0.1");
    if (!Number.isFinite(leverage) || leverage < 0 || !integers.every((value) => Number.isSafeInteger(value) && value >= 0)
      || pythonCycle < 25 || pythonCycle > 60_000 || executionCycle < 5 || executionCycle > 60_000
      || marketAge < 250 || marketAge > 86_400_000 || llmTimeout < 1_000 || llmTimeout > 120_000
      || (brokerMode !== "paper" && brokerMode !== "ibkr")
      || (brokerMode === "ibkr" && (ibkrAccount.length === 0 || ibkrAccount.length > 64 || !/^[A-Za-z0-9._-]+$/.test(ibkrAccount)))
      || ((ibkrConid.length > 0 || ibkrInstrumentId.length > 0) && (!/^[1-9][0-9]*$/.test(ibkrConid) || !/^[1-9][0-9]*$/.test(ibkrInstrumentId)))
      || (embeddingsEnabled && (embeddingModel.length === 0 || embeddingModel.length > 2048 || embeddingVersion.length === 0 || embeddingVersion.length > 2048 || !Number.isSafeInteger(embeddingDimensions) || embeddingDimensions < 1 || embeddingDimensions > 4096))
      || !validConfiguredHttpsUrl(newsapiBaseUrl) || (newsapiEndpoint !== "everything" && newsapiEndpoint !== "top-headlines")
      || (newsapiEndpoint === "top-headlines" && !newsapiCountry && !newsapiCategory && !newsapiSources)
      || (newsapiCountry && !/^[a-z]{2}$/.test(newsapiCountry))
      || (newsapiSources && (newsapiSources.length > 512 || newsapiSources.split(",").some((source) => !source.trim())))
      || !Number.isFinite(ewmaLambda) || ewmaLambda <= 0 || ewmaLambda >= 1 || metricTtl < 1 || smaWindow < 1 || smaWindow > 10_000
      || !Number.isFinite(referenceEntry) || !Number.isFinite(referenceExit) || referenceEntry < -1 || referenceEntry > 1 || referenceExit < -1 || referenceExit > 1 || referenceQuantity < 1 || referenceHorizon < 1 || referenceTtl < 1 || referenceId.length === 0 || referenceId.length > 2048 || referenceMetricId.length === 0 || referenceMetricId.length > 2048
      || maxPosition < 1 || maxPosition > Number.MAX_SAFE_INTEGER || maxNotional < 1 || maxNotional > Number.MAX_SAFE_INTEGER
      || ibkrTimeout < 1_000 || ibkrTimeout > 120_000 || ibkrPoll < 250 || ibkrPoll > 60_000
      || ibkrScale < 1 || ibkrScale > 1_000_000_000
      || !validConfiguredHttpsUrl(ibkrBaseUrl)
      || pythonCpu < 1 || pythonCpu > 86_400 || pythonMemory < 67_108_864 || pythonMemory > 8_589_934_592
      || marketHttpTimeout < 1_000 || marketHttpTimeout > 120_000 || yahooIntervalNs < 1_000_000_000 || yahooIntervalNs > 86_400_000_000_000 || yahooPriceScale < 1 || yahooPriceScale > 1_000_000_000 || yahooHistoryPoll < 5_000 || yahooHistoryPoll > 900_000 || yahooQuotePoll < 1_000 || yahooQuotePoll > 300_000
      || newsapiPoll < 1_000 || newsapiPoll > 300_000 || yahooNewsPoll < 5_000 || yahooNewsPoll > 300_000
      || newsHttpTimeout < 1_000 || newsHttpTimeout > 120_000 || newsMaxRetries > 16 || newsRetryBase < 1 || newsRetryBase > 60_000 || newsRetryMax < newsRetryBase || newsRetryMax > 300_000
      || reconciliationPoll < 1_000 || reconciliationPoll > 300_000 || webhookTimeout < 250 || webhookTimeout > 30_000 || webhookPoll < 250 || webhookPoll > 300_000
      || alertCooldown < 0 || alertCooldown > 86_400_000 || alertMaxPending < 1 || alertMaxPending > 1_000_000
      || supervisorFailures < 1 || supervisorFailures > 1_000_000 || supervisorWindow < 1 || supervisorWindow > 86_400_000_000_000 || supervisorInitialBackoff < 1 || supervisorInitialBackoff > 86_400_000_000_000 || supervisorMaxBackoff < supervisorInitialBackoff || supervisorMaxBackoff > 86_400_000_000_000 || supervisorJitter > 10_000 || uiStatusPoll < 1_000 || uiStatusPoll > 60_000 || alertPoll < 500 || alertPoll > 60_000 || newsStaleAfter < 60_000 || newsStaleAfter > 3_600_000 || analystStaleAfter < 60_000 || analystStaleAfter > 3_600_000
      || yahooSymbolsError !== undefined
      || [pythonExecutable, pythonWorkdir, pythonMetricsRoot, pythonStrategiesRoot].some((value) => value.length === 0 || value.length > 2048)
      || webhookUrl.length > 2048 || (webhookUrl.length > 0 && (parsedWebhookUrl?.protocol !== "https:" || !parsedWebhookUrl.hostname || parsedWebhookUrl.username || parsedWebhookUrl.password))
      || llmBaseUrl.length === 0 || llmBaseUrl.length > 2048 || (!safeLocalHttp && parsedLlmUrl?.protocol !== "https:") || !parsedLlmUrl?.hostname || !!parsedLlmUrl.username || !!parsedLlmUrl.password
      || llmModel.length === 0 || llmModel.length > 256
      || llmPromptVersion.length === 0 || llmPromptVersion.length > 256
      || yahooBaseUrl.length === 0 || yahooBaseUrl.length > 2048 || parsedYahooUrl?.protocol !== "https:" || !parsedYahooUrl?.hostname || !!parsedYahooUrl.username || !!parsedYahooUrl.password
      || cfgStrings.some((value) => new TextEncoder().encode(value).length > 16_384 || /[\u0000-\u001f]/.test(value))
      || !/^[A-Za-z0-9]+$/.test(yahooInterval) || yahooInterval.length > 16 || !/^[0-9]+[A-Za-z0-9]*$/.test(yahooRange) || yahooRange.length > 16) {
      configError = yahooSymbolsError ?? "Generator values must be finite, whole, within engine bounds, and use HTTPS (or localhost HTTP) for the LLM URL.";
      render();
      return;
    }
    const textArea = root.querySelector<HTMLTextAreaElement>("[data-config-text]");
    if (textArea) {
      let baseText = embeddingsEnabled ? textArea.value : removeConfigKeys(textArea.value, ["embeddings.model", "embeddings.model_version", "embeddings.dimensions"]);
      if (!ibkrAccount) baseText = removeConfigKeys(baseText, ["broker.ibkr_account_id"]);
      if (!ibkrConid) baseText = removeConfigKeys(baseText, ["broker.ibkr_conid"]);
      if (!ibkrInstrumentId) baseText = removeConfigKeys(baseText, ["broker.ibkr_instrument_id"]);
      if (!newsapiCountry) baseText = removeConfigKeys(baseText, ["news.newsapi_country"]);
      if (!newsapiCategory) baseText = removeConfigKeys(baseText, ["news.newsapi_category"]);
      if (!newsapiSources) baseText = removeConfigKeys(baseText, ["news.newsapi_sources"]);
      if (!newsapiQuery) baseText = removeConfigKeys(baseText, ["news.newsapi_query"]);
      if (!yahooQuery) baseText = removeConfigKeys(baseText, ["news.yahoo_query"]);
      if (!yahooSymbols) baseText = removeConfigKeys(baseText, ["market.yahoo_symbols"]);
      if (!webhookUrl) baseText = removeConfigKeys(baseText, ["alerts.webhook_url"]);
      const mergedConfiguration = mergeRiskConfiguration(baseText, {
        "risk.max_leverage": String(leverage),
        "risk.max_position_ticks": String(maxPosition),
        "risk.max_gross_notional_ticks": String(maxNotional),
        "broker.ibkr_timeout_ms": String(ibkrTimeout),
        "broker.ibkr_market_poll_ms": String(ibkrPoll),
        "broker.ibkr_price_scale": String(ibkrScale),
        ...(ibkrAccount ? { "broker.ibkr_account_id": JSON.stringify(ibkrAccount) } : {}),
        ...(ibkrConid ? { "broker.ibkr_conid": JSON.stringify(ibkrConid) } : {}),
        ...(ibkrInstrumentId ? { "broker.ibkr_instrument_id": JSON.stringify(ibkrInstrumentId) } : {}),
        "broker.ibkr_base_url": JSON.stringify(ibkrBaseUrl),
        "broker.mode": JSON.stringify(brokerMode),
        "python.allow_network": String(pythonAllowNetwork),
        "python.executable": JSON.stringify(pythonExecutable),
        "python.workdir": JSON.stringify(pythonWorkdir),
        "python.metrics_root": JSON.stringify(pythonMetricsRoot),
        "python.strategies_root": JSON.stringify(pythonStrategiesRoot),
        "strategy.reference_enabled": String(referenceEnabled),
        "strategy.reference_entry_threshold": String(referenceEntry),
        "strategy.reference_exit_threshold": String(referenceExit),
        "strategy.reference_quantity_ticks": String(referenceQuantity),
        "strategy.reference_horizon_ns": String(referenceHorizon),
        "strategy.reference_ttl_ns": String(referenceTtl),
        "strategy.reference_id": JSON.stringify(referenceId),
        "strategy.reference_metric_id": JSON.stringify(referenceMetricId),
        "metric.ewma_lambda": String(ewmaLambda),
        "metric.reference_ttl_ns": String(metricTtl),
        "metric.ewma_ttl_ns": String(metricTtl),
        "metric.sma_window": String(smaWindow),
        ...(embeddingsEnabled ? { "embeddings.model": JSON.stringify(embeddingModel), "embeddings.model_version": JSON.stringify(embeddingVersion), "embeddings.dimensions": String(embeddingDimensions) } : {}),
        "news.newsapi_base_url": JSON.stringify(newsapiBaseUrl),
        "news.newsapi_endpoint": JSON.stringify(newsapiEndpoint),
        ...(newsapiCountry ? { "news.newsapi_country": JSON.stringify(newsapiCountry) } : {}),
        ...(newsapiCategory ? { "news.newsapi_category": JSON.stringify(newsapiCategory) } : {}),
        ...(newsapiSources ? { "news.newsapi_sources": JSON.stringify(newsapiSources) } : {}),
        "market.allow_yahoo_live_marks": String(allowYahooLiveMarks),
        "broker.allow_ibkr_bootstrap_mark": String(allowIbkrBootstrapMark),
        ...(newsapiQuery ? { "news.newsapi_query": JSON.stringify(newsapiQuery) } : {}),
      ...(yahooQuery ? { "news.yahoo_query": JSON.stringify(yahooQuery) } : {}),
        ...(yahooSymbols ? { "market.yahoo_symbols": JSON.stringify(yahooSymbols) } : {}),
        ...(webhookUrl ? { "alerts.webhook_url": JSON.stringify(webhookUrl) } : {}),
        "risk.max_drawdown_bps": String(drawdown),
        "risk.max_outstanding_orders": String(orders),
        "risk.max_predicted_volatility_bps": String(volatility),
        "risk.max_participation_bps": String(participation),
        "risk.max_message_rate": String(messageRate),
        "risk.max_price_deviation_bps": String(priceDeviation),
        "scheduler.python_cycle_ms": String(pythonCycle),
        "scheduler.execution_cycle_ms": String(executionCycle),
        "market.max_age_ms": String(marketAge),
        "market.http_timeout_ms": String(marketHttpTimeout),
        "market.yahoo_interval_ns": String(yahooIntervalNs),
        "market.yahoo_price_scale": String(yahooPriceScale),
        "market.yahoo_poll_ms": String(yahooHistoryPoll),
        "market.yahoo_quote_poll_ms": String(yahooQuotePoll),
        "python.cpu_seconds": String(pythonCpu),
        "python.memory_bytes": String(pythonMemory),
        "news.newsapi_poll_ms": String(newsapiPoll),
        "news.yahoo_poll_ms": String(yahooNewsPoll),
        "news.http_timeout_ms": String(newsHttpTimeout),
        "news.max_retries": String(newsMaxRetries),
        "news.retry_base_ms": String(newsRetryBase),
        "news.retry_max_ms": String(newsRetryMax),
        "reconciliation.poll_ms": String(reconciliationPoll),
        "alerts.webhook_timeout_ms": String(webhookTimeout),
        "alerts.webhook_poll_ms": String(webhookPoll),
        "alerts.cooldown_ms": String(alertCooldown),
        "alerts.max_pending": String(alertMaxPending),
        "supervisor.max_failures": String(supervisorFailures),
        "supervisor.window_ns": String(supervisorWindow),
        "supervisor.initial_backoff_ns": String(supervisorInitialBackoff),
        "supervisor.max_backoff_ns": String(supervisorMaxBackoff),
        "supervisor.jitter_bps": String(supervisorJitter),
        "llm.timeout_ms": String(llmTimeout),
        "llm.base_url": JSON.stringify(llmBaseUrl),
        "ui.status_poll_ms": String(uiStatusPoll),
        "ui.alert_poll_ms": String(alertPoll),
        "ui.news_stale_after_ms": String(newsStaleAfter),
        "ui.analyst_stale_after_ms": String(analystStaleAfter),
        "llm.model": JSON.stringify(llmModel),
        "llm.prompt_version": JSON.stringify(llmPromptVersion),
        "market.yahoo_base_url": JSON.stringify(yahooBaseUrl),
        "market.yahoo_interval": JSON.stringify(yahooInterval),
        "market.yahoo_range": JSON.stringify(yahooRange),
      });
      if (new TextEncoder().encode(mergedConfiguration).length > 1_048_576) {
        configError = "Generated configuration exceeds the 1 MiB input bound";
        render();
        return;
      }
      textArea.value = mergedConfiguration;
    }
    configError = "";
  });
  root.querySelector<HTMLButtonElement>("[data-config-copy]")?.addEventListener("click", async () => {
    const cfgText = root.querySelector<HTMLTextAreaElement>("[data-config-text]")?.value ?? "";
    try {
      await globalThis.navigator?.clipboard?.writeText(cfgText);
      configActionMessage = "Configuration copied to clipboard.";
    } catch {
      configActionMessage = "Clipboard unavailable; select the text manually.";
    }
    render();
  });
  root.querySelector<HTMLButtonElement>("[data-config-download]")?.addEventListener("click", () => {
    const cfgText = root.querySelector<HTMLTextAreaElement>("[data-config-text]")?.value ?? "";
    const url = URL.createObjectURL(new Blob([cfgText], { type: "text/plain;charset=utf-8" }));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "insidertrader.cfg";
    anchor.click();
    URL.revokeObjectURL(url);
    configActionMessage = "Configuration downloaded as insidertrader.cfg.";
    render();
  });
  root.querySelectorAll<HTMLElement>("[data-ack-alert]").forEach((element) => element.addEventListener("click", async () => {
    const alertId = element.dataset.ackAlert;
    if (!alertId) return;
    element.setAttribute("disabled", "true");
    try {
      await commands.acknowledgeAlert(alertId);
      messageHistory = messageHistory.map((record) => record.alert.alertId === alertId ? { ...record, acknowledged: true } : record);
      await refreshAlerts();
    } catch {
      element.removeAttribute("disabled");
    }
  }));
  root.querySelector<HTMLButtonElement>("[data-alert-retry]")?.addEventListener("click", async (event) => {
    const button = event.currentTarget as HTMLButtonElement;
    button.disabled = true;
    await refreshAlerts();
    if (alertsDegraded) button.disabled = false;
  });
  root.querySelector<HTMLInputElement>("[data-alert-native]")?.addEventListener("change", async (event) => {
    nativeAlertsEnabled = (event.target as HTMLInputElement).checked;
    layoutStorage.setItem(ALERT_NATIVE_STORAGE_KEY, String(nativeAlertsEnabled));
    if (nativeAlertsEnabled && typeof Notification !== "undefined" && Notification.permission === "default") {
      try {
        const permission = await Notification.requestPermission();
        if (permission === "denied") nativeAlertPermissionError = "Native notifications are blocked by the operating system; alerts remain available in the Message Station.";
      } catch { nativeAlertPermissionError = "Native notification permission could not be requested; alerts remain available in the Message Station."; }
    } else if (!nativeAlertsEnabled) {
      nativeAlertPermissionError = "";
    } else if (typeof Notification === "undefined" || Notification.permission === "denied") {
      nativeAlertPermissionError = "Native notifications are unavailable or blocked; alerts remain available in the Message Station.";
    }
    render();
  });
  root.querySelector<HTMLInputElement>("[data-alert-sound]")?.addEventListener("change", (event) => {
    soundAlertsEnabled = (event.target as HTMLInputElement).checked;
    layoutStorage.setItem(ALERT_SOUND_STORAGE_KEY, String(soundAlertsEnabled));
    if (soundAlertsEnabled && typeof AudioContext !== "undefined") {
      try {
        alertAudioContext ??= new AudioContext();
        void alertAudioContext.resume();
      } catch { /* audio is optional */ }
    }
  });
  root.querySelectorAll<HTMLInputElement>("[data-alert-sound-severity]").forEach((input) => input.addEventListener("change", (event) => {
    const severity = Number((event.target as HTMLInputElement).dataset.alertSoundSeverity);
    if (!Number.isSafeInteger(severity) || severity < 0 || severity > 3) return;
    const next = [...soundSeverityEnabled];
    next[severity] = (event.target as HTMLInputElement).checked;
    soundSeverityEnabled = Object.freeze(next);
    layoutStorage.setItem(ALERT_SOUND_SEVERITY_STORAGE_KEY, JSON.stringify(soundSeverityEnabled));
  }));
  root.querySelectorAll<HTMLElement>("[data-trace-query]").forEach((element) => element.addEventListener("click", async () => {
    const traceId = root.querySelector<HTMLInputElement>("[data-trace-input]")?.value.trim() ?? "";
    if (!traceId) return;
    element.setAttribute("disabled", "true");
    traceError = "";
    traceExport = [];
    try {
      traceEvents = await commands.getTraceEvents(traceId);
      traceExport = await commands.exportTrace(traceId);
    } catch (error) {
      traceEvents = [];
      traceError = error instanceof Error ? error.message : "trace reconstruction failed";
    } finally {
      element.removeAttribute("disabled");
      render();
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-trace-export]").forEach((element) => element.addEventListener("click", () => {
    if (!traceExport.length) return;
    const traceId = root.querySelector<HTMLInputElement>("[data-trace-input]")?.value.trim() || "trace-export";
    const payload = JSON.stringify({ schema: "insidertrader.trace-export.v1", traceId, redacted: true, events: traceExport }, null, 2);
    const url = URL.createObjectURL(new Blob([payload], { type: "application/json" }));
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${traceId.replace(/[^A-Za-z0-9._-]/g, "_")}.trace.json`;
    anchor.click();
    URL.revokeObjectURL(url);
  }));
  root.querySelectorAll<HTMLElement>("[data-proposal]").forEach((element) => element.addEventListener("click", async () => {
    const proposalId = element.dataset.proposal;
    if (!proposalId) return;
    const proposal = store.state.proposals.find((item) => item.proposalId === proposalId);
    if (!proposal || proposal.expiresAtMs <= Date.now()) {
      element.textContent = "Expired";
      return;
    }
    if (proposal.action === "no_action") {
      element.textContent = "No action";
      return;
    }
    const position = store.state.positions.find((item) => item.symbol === proposal.symbol);
    const rawQuantity = proposal.quantityTicks ?? (position ? Math.abs(position.quantityTicks) : undefined);
    if (!rawQuantity || !Number.isSafeInteger(rawQuantity) || rawQuantity === 0) {
      element.textContent = "No quantity";
      return;
    }
    const direction = proposal.action === "decrease"
      ? -Math.sign(rawQuantity)
      : proposal.action === "close"
        ? -Math.sign(position?.quantityTicks ?? 0)
        : Math.sign(rawQuantity);
    if (direction === 0) {
      element.textContent = "No position";
      return;
    }
    const draft = {
      symbol: state.selectedSymbol,
      instrumentId: proposal.symbol,
      side: direction < 0 ? "sell" : "buy",
      type: "market",
      quantityTicks: Math.abs(rawQuantity),
    } as const;
    store.setOrderTicket({ status: "idle", draft });
    element.textContent = "Drafted";
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-close-position]").forEach((element) => element.addEventListener("click", async () => {
    const symbol = element.dataset.closePosition;
    const position = symbol ? state.positions.find((item) => item.symbol === symbol) : undefined;
    if (!position || !Number.isSafeInteger(position.quantityTicks) || position.quantityTicks === 0) return;
    element.setAttribute("disabled", "true");
    try {
      const resolution = await commands.resolveInstrument(position.symbol);
      store.setOrderTicket({
        status: "idle",
        draft: {
          symbol: resolution.symbol,
          instrumentId: resolution.instrumentId,
          side: position.quantityTicks > 0 ? "sell" : "buy",
          type: "market",
          quantityTicks: Math.abs(position.quantityTicks),
        },
      });
      workspaceLayout = validateWorkspaceLayout({ ...workspaceLayout, panels: ["order-ticket", ...workspaceLayout.panels.filter((panelId) => panelId !== "order-ticket")] });
      workspacePersistence.schedule(workspaceLayout);
      render();
    } catch (error) {
      element.removeAttribute("disabled");
      element.textContent = error instanceof Error ? "Resolve failed" : "Error";
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-schedule-proposal]").forEach((element) => element.addEventListener("click", async () => {
    const proposalId = element.dataset.scheduleProposal;
    if (!proposalId) return;
    scheduleConfirmation = { proposalId, kind: "twap" };
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-schedule-dialog-close], [data-schedule-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (element.hasAttribute("data-schedule-dialog-close") && event.target !== element && !(event.target instanceof HTMLButtonElement)) return;
    scheduleConfirmation = undefined;
    render();
  }));
  root.querySelector<HTMLElement>("[data-schedule-dialog-submit]")?.addEventListener("click", async () => {
    const confirmation = scheduleConfirmation;
    if (!confirmation) return;
    const phrase = root.querySelector<HTMLInputElement>("[data-schedule-confirmation-input]")?.value.trim() ?? "";
    if (phrase !== "CONFIRM") { scheduleConfirmation = { ...confirmation, error: "Type CONFIRM exactly to authorize scheduling." }; render(); return; }
    const button = root.querySelector<HTMLButtonElement>("[data-schedule-dialog-submit]");
    if (button) button.disabled = true;
    try {
      const schedule = confirmation.kind === "twap"
        ? { type: "twap" as const, slices: 4, intervalNs: 1_000_000_000 }
        : { type: "implementation_shortfall" as const, slices: 4, intervalNs: 1_000_000_000, urgencyBps: 7_500 };
      await commands.submitScheduledProposal(confirmation.proposalId, schedule, phrase);
      scheduleConfirmation = undefined;
      render();
    } catch (error) {
      scheduleConfirmation = { ...confirmation, error: error instanceof Error ? error.message : "Scheduling rejected" };
      render();
    }
  });
  root.querySelectorAll<HTMLElement>("[data-schedule-is-proposal]").forEach((element) => element.addEventListener("click", async () => {
    const proposalId = element.dataset.scheduleIsProposal;
    if (!proposalId) return;
    scheduleConfirmation = { proposalId, kind: "implementation_shortfall" };
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-manual-order]").forEach((element) => element.addEventListener("click", async () => {
    try {
      const type = root.querySelector<HTMLSelectElement>("[data-order-type]")?.value === "limit" ? "limit" : "market";
      const quantityTicks = Number(root.querySelector<HTMLInputElement>("[data-order-quantity]")?.value ?? "");
      const limitValue = Number(root.querySelector<HTMLInputElement>("[data-order-limit-price]")?.value ?? "");
      const draft = {
        symbol: state.selectedSymbol,
        side: root.querySelector<HTMLSelectElement>("[data-order-side]")?.value === "sell" ? "sell" : "buy",
        type,
        quantityTicks,
        ...(type === "limit" ? { limitPriceTicks: limitValue } : {}),
      } as const;
      const validation = validateOrderDraft(draft);
      if (validation) throw new Error(validation);
      await commands.previewOrder(draft);
    } catch (error) {
      element.textContent = error instanceof Error ? "Unavailable" : "Error";
    }
  }));
  const invalidateOrderPreview = (): void => {
    const type = root.querySelector<HTMLSelectElement>("[data-order-type]")?.value === "limit" ? "limit" : "market";
    const quantityTicks = Number(root.querySelector<HTMLInputElement>("[data-order-quantity]")?.value ?? "");
    const limitPriceTicks = Number(root.querySelector<HTMLInputElement>("[data-order-limit-price]")?.value ?? "");
    const side = root.querySelector<HTMLSelectElement>("[data-order-side]")?.value === "sell" ? "sell" : "buy";
    store.setOrderTicket({
      status: "idle",
      draft: {
        symbol: symbolFor("order-ticket"), side, type, quantityTicks,
        ...(type === "limit" && Number.isSafeInteger(limitPriceTicks) && limitPriceTicks > 0 ? { limitPriceTicks } : {}),
      },
    });
    render();
  };
  root.querySelectorAll<HTMLElement>("[data-order-side], [data-order-type], [data-order-quantity], [data-order-limit-price]").forEach((field) => {
    field.addEventListener("change", invalidateOrderPreview);
  });
  root.querySelectorAll<HTMLElement>("[data-submit-order]").forEach((element) => element.addEventListener("click", async () => {
    const preview = store.state.orderTicket?.preview;
    const confirmation = root.querySelector<HTMLInputElement>("[data-confirmation]")?.value ?? "";
    if (!preview || confirmation !== "CONFIRM") return;
    try {
      await commands.submitManualOrder(preview.draft, confirmation);
    } catch (error) {
      element.textContent = error instanceof Error ? "Rejected" : "Error";
    }
  }));
  root.querySelectorAll<HTMLElement>("[data-cancel-order]").forEach((element) => element.addEventListener("click", async () => {
    const clientOrderId = element.dataset.cancelOrder;
    if (!clientOrderId) return;
    element.setAttribute("disabled", "true");
    try {
      await commands.cancelOrder(clientOrderId);
      element.textContent = "Cancel requested";
    } catch (error) {
      element.removeAttribute("disabled");
      element.textContent = error instanceof Error ? "Cancel failed" : "Error";
    }
  }));
  root.querySelector<HTMLButtonElement>("[data-cancel-all-orders]")?.addEventListener("click", async (event) => {
    const working = state.orders.filter((order) => ["created", "risk_approved", "queued", "sending", "sent", "acknowledged", "partially_filled"].includes(order.state));
    if (working.length === 0) return;
    cancelAllConfirmation = { count: working.length };
    render();
    return;
  });
  root.querySelectorAll<HTMLElement>("[data-cancel-all-dialog-close], [data-cancel-all-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.cancelAllDialogClose !== undefined) return;
    cancelAllConfirmation = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-cancel-all-dialog-submit]")?.addEventListener("click", async () => {
    const confirmation = cancelAllConfirmation;
    const phrase = root.querySelector<HTMLInputElement>("[data-cancel-all-confirmation-input]")?.value ?? "";
    if (!confirmation || phrase !== "CONFIRM") {
      if (confirmation) cancelAllConfirmation = { ...confirmation, error: "Type CONFIRM exactly to request cancellation." };
      render();
      return;
    }
    const working = store.state.orders.filter((order) => ["created", "risk_approved", "queued", "sending", "sent", "acknowledged", "partially_filled"].includes(order.state));
    cancelAllConfirmation = undefined;
    render();
    let completed = 0;
    let failed = 0;
    for (const order of working) {
      try {
        await commands.cancelOrder(order.clientOrderId);
        completed += 1;
      } catch {
        // Reconciliation remains authoritative; continue cancelling other orders.
        failed += 1;
      }
    }
    const resultButton = root.querySelector<HTMLButtonElement>("[data-cancel-all-orders]");
    if (resultButton) resultButton.textContent = failed === 0
      ? `Cancel requested (${completed}/${working.length})`
      : `Cancel partial (${completed}/${working.length}, ${failed} failed)`;
  });
  root.querySelectorAll<HTMLElement>("[data-replace-order]").forEach((element) => element.addEventListener("click", async () => {
    const clientOrderId = element.dataset.replaceOrder;
    const currentQuantity = Number(element.dataset.replaceQuantity);
    if (!clientOrderId || !Number.isSafeInteger(currentQuantity) || currentQuantity <= 0) return;
    replaceOrderDialog = { clientOrderId, quantity: currentQuantity };
    render();
  }));
  root.querySelectorAll<HTMLElement>("[data-replace-dialog-close], [data-replace-dialog-cancel]").forEach((element) => element.addEventListener("click", (event) => {
    if (event.target !== element && element.dataset.replaceDialogClose !== undefined) return;
    replaceOrderDialog = undefined;
    render();
  }));
  root.querySelector<HTMLButtonElement>("[data-replace-dialog-submit]")?.addEventListener("click", async () => {
    const dialog = replaceOrderDialog;
    if (!dialog) return;
    const quantity = Number(root.querySelector<HTMLInputElement>("[data-replace-quantity-input]")?.value ?? "");
    const limitText = root.querySelector<HTMLInputElement>("[data-replace-limit-input]")?.value.trim() ?? "";
    const limit = limitText === "" ? undefined : Number(limitText);
    if (!Number.isSafeInteger(quantity) || quantity <= 0) {
      replaceOrderDialog = { ...dialog, error: "Quantity must be a positive whole number." };
      render();
      return;
    }
    if (limit !== undefined && (!Number.isSafeInteger(limit) || limit <= 0)) {
      replaceOrderDialog = { ...dialog, error: "Limit price must be blank or a positive whole number." };
      render();
      return;
    }
    replaceOrderDialog = undefined;
    render();
    const button = [...root.querySelectorAll<HTMLButtonElement>("[data-replace-order]")].find((candidate) => candidate.dataset.replaceOrder === dialog.clientOrderId);
    button?.setAttribute("disabled", "true");
    try {
      await commands.replaceOrder(dialog.clientOrderId, quantity, limit);
      if (button) button.textContent = "Replace requested";
    } catch (error) {
      button?.removeAttribute("disabled");
      if (button) button.textContent = error instanceof Error ? "Replace failed" : "Error";
    }
  });
}

let renderScheduled = false;
function scheduleRender(): void {
  if (renderScheduled) return;
  renderScheduled = true;
  const flush = (): void => {
    renderScheduled = false;
    render();
  };
  if (typeof globalThis.requestAnimationFrame === "function") globalThis.requestAnimationFrame(flush);
  else globalThis.setTimeout(flush, 16);
}

// High-frequency runtime updates are batched to one workspace render per frame;
// explicit user interactions still call render() immediately for responsive feedback.
store.subscribe(scheduleRender);
globalThis.addEventListener("keydown", (event) => {
  const target = event.target as HTMLElement | null;
  const editing = target?.tagName === "INPUT" || target?.tagName === "TEXTAREA" || target?.tagName === "SELECT" || target?.isContentEditable;
  const binding = eventHotkey(event);
  if (binding && binding === hotkeys.commandPalette) {
    event.preventDefault();
    commandPaletteOpen = !commandPaletteOpen;
    if (commandPaletteOpen) commandPaletteQuery = "";
    render();
      } else if (event.key === "Tab" && (settingsOpen || commandPaletteOpen || workspaceDialog || scheduleConfirmation || cancelAllConfirmation || replaceOrderDialog || chartTemplateDialog || lifecycleDialog || modelEvidenceDialog || autonomyModeDialog)) {
        const modalSelector = settingsOpen ? ".settings-modal" : commandPaletteOpen ? ".command-palette" : ".workspace-dialog";
    const modal = root?.querySelector<HTMLElement>(modalSelector);
    if (!modal) return;
    const focusable = [...modal.querySelectorAll<HTMLElement>("button, input, select, textarea, [tabindex]")]
      .filter((element) => !element.hasAttribute("disabled") && element.getAttribute("tabindex") !== "-1");
    if (focusable.length === 0) return;
    const first = focusable[0];
    const last = focusable[focusable.length - 1];
    const active = document.activeElement;
    if (event.shiftKey && (active === first || !modal.contains(active))) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && (active === last || !modal.contains(active))) {
      event.preventDefault();
      first.focus();
    }
      } else if (event.key === "Escape" && (commandPaletteOpen || settingsOpen || workspaceDialog || scheduleConfirmation || cancelAllConfirmation || replaceOrderDialog || chartTemplateDialog || lifecycleDialog || modelEvidenceDialog || autonomyModeDialog || messageStationOpen)) {
    commandPaletteOpen = false;
    commandPaletteQuery = "";
    settingsOpen = false;
        workspaceDialog = undefined;
        scheduleConfirmation = undefined;
            cancelAllConfirmation = undefined;
            replaceOrderDialog = undefined;
            chartTemplateDialog = undefined;
            lifecycleDialog = undefined;
            modelEvidenceDialog = undefined;
            autonomyModeDialog = undefined;
    messageStationOpen = false;
    render();
  } else if (!editing && binding) {
    const workspaceIndex = HOTKEY_ACTIONS.slice(1).findIndex((action) => hotkeys[action] === binding);
    if (workspaceIndex < 0) return;
    // Legacy default equivalent: workspaceTabOrder[Number(event.key) - 1].
    const preset = workspaceTabOrder[workspaceIndex];
    if (!preset) return;
    event.preventDefault();
    switchWorkspace(preset);
    workspacePersistence.schedule(workspaceLayout);
    render();
  }
});
void loadNewsPage(store.state.newsScope, symbolFor("news"));
void session.connect().catch(() => undefined);
void refreshAlerts();
void commands.listBacktests().then((runs) => { backtestHistory = runs; render(); }).catch(() => undefined);
void commands.listExperiments().then((runs) => { experimentHistory = runs; render(); }).catch(() => undefined);
void commands.getConfig().then((snapshot) => { configSnapshot = snapshot; render(); }).catch((error) => { configError = error instanceof Error ? error.message : String(error); render(); });
void commands.listModels().then((runs) => { modelHistory = runs; render(); }).catch(() => undefined);
void commands.listStrategyResolutions().then((runs) => { resolutionHistory = runs; render(); }).catch(() => undefined);
void commands.listStrategyExecutionSummaries().then((runs) => { executionHistory = runs; render(); }).catch(() => undefined);
void commands.listStrategies().then((runs) => { strategyRegistry = runs; render(); }).catch(() => undefined);
void commands.listMetrics().then((runs) => { metricRegistry = runs; render(); }).catch(() => undefined);
void refreshNewsProviderStatuses();
void refreshSupervisorStatuses();
void refreshBrokerStatus();
void refreshRiskPolicyStatus();
  scheduleAlertRefresh();
scheduleStatusRefresh();
globalThis.setInterval(() => {
      if (!analystStaleNoticeShown && analystReceivedAtMs !== undefined && Date.now() - analystReceivedAtMs >= configuredAnalystDisplayTtlMs()) {
    analystStaleNoticeShown = true;
    render();
  }
}, 30_000);
