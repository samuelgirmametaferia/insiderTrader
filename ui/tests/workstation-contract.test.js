import { readFile } from "node:fs/promises";
import { test } from "node:test";
import assert from "node:assert/strict";

const source = await readFile(new URL("../src/app/main.ts", import.meta.url), "utf8");
const chartSource = await readFile(new URL("../src/charts/market-chart.ts", import.meta.url), "utf8");
const workspaceSource = await readFile(new URL("../src/layouts/workspace.ts", import.meta.url), "utf8");
const storeSource = await readFile(new URL("../src/stores/runtime-store.ts", import.meta.url), "utf8");
const tokens = await readFile(new URL("../src/theme/tokens.css", import.meta.url), "utf8");
const packageManifest = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8"));
const nodeVersion = (await readFile(new URL("../.node-version", import.meta.url), "utf8")).trim();

test("UI toolchain pins the release Node and npm versions", () => {
  assert.equal(nodeVersion, "22.22.2");
  assert.equal(packageManifest.packageManager, "npm@12.0.2");
  assert.equal(packageManifest.engines?.node, ">=22.22.2 <23");
  assert.equal(packageManifest.engines?.npm, ">=12.0.2 <13");
});

test("external provider links are bounded and credential-free", () => {
  assert.match(source, /new TextEncoder\(\)\.encode\(value\)\.length > 2048/);
  assert.match(source, /parsed\.protocol !== "https:"/);
  assert.match(source, /parsed\.username \|\| parsed\.password/);
  assert.match(source, /!parsed\.hostname/);
  assert.match(source, /parsedWebhookUrl\.username \|\| parsedWebhookUrl\.password/);
  assert.match(source, /parsedLlmUrl\.username/);
  assert.match(source, /function validConfiguredHttpsUrl/);
  assert.match(source, /!validConfiguredHttpsUrl\(newsapiBaseUrl\)/);
  assert.match(source, /!validConfiguredHttpsUrl\(ibkrBaseUrl\)/);
});

test("Trading workstation exposes the required navigation surfaces", () => {
  for (const marker of [
    "data-workspace-tab",
    "role=\"tablist\" aria-label=\"Workspaces\"",
    "workspaceLayout.name === preset ? \"0\" : \"-1\"",
    "data-right-dock-tab",
    "ArrowLeft",
    "ArrowRight",
    "tabindex=\"${rightDockTab === tab ? \"0\" : \"-1\"}\"",
    "data-right-dock-splitter",
    "data-tools-toggle",
    "data-tool-focus",
    "data-message-toggle",
    "aria-live=\"polite\"",
    "Message station:",
    "data-chart-context-action",
    "open-alerts",
    "toggle-metrics",
    "clear-drawings",
    "const initialDrawings = loadAndMigrateDrawings();",
    "Persist before deleting the legacy key",
    "layoutStorage.removeItem(drawingLegacyStorageKey());",
    "data-config-copy",
    "data-config-download",
    "deltaX",
    "chartInertiaFrame",
    "prefers-reduced-motion: reduce",
    "reducedMotion",
    "pinchLastCenter",
    "Math.round(anchor * Math.max(0, current.end - current.start - 1))",
    "data-command-search",
    "focus:global-search",
    "focus:alerts",
    "focus:strategy-analysis",
    "focus:backtest",
    "commandPaletteQuery",
    "metric-inspector",
    "Logs / Trace",
    "data-metric-inspect",
    "metricInspectorMarkup",
    "chart-secondary",
    "chart-tertiary",
    "chart-quaternary",
    "multiChartPanels",
    "market-change",
    "pnlSign",
    "P&L ${position.pnlTicks >= 0 ? \"gain\" : \"loss\"}",
    "Change unavailable",
    "data-font-scale",
    "data-settings-chart-mode",
    'option.value = "bars"',
    'option.textContent = "OHLC bars"',
    "data-settings-gridlines",
    "data-chart-gridlines",
    "ChartGridlineDensity",
    "gridlineDensity",
    "chart-gridline",
    "chart-crosshair-h",
    "chartHoverYPercent",
    "data-settings-order-type",
    "data-settings-order-quantity",
    "ORDER_DEFAULTS_STORAGE_KEY",
    "engine confirmation remains mandatory",
    "HOTKEY_STORAGE_KEY",
    "data-hotkey-action",
    "data-hotkeys-reset",
    "Duplicate bindings are rejected",
    "eventHotkey",
    "candidate.version !== 1",
    "seen.has(normalized)",
    "FONT_SCALE_STORAGE_KEY",
    "--ui-font-scale",
    "popout-snapshot-note",
    "Read-only snapshot",
    "noopener",
    "data-news-view",
    "watchlist",
    "portfolio",
    "NewsView",
    "100_000",
    "AnalystContextId",
    "analystContextEnabled",
    "data-analyst-context-remove",
    "Included runtime context",
    "contextualInput",
    "manual-only",
    "AnalystEvidenceCard",
    "analystEvidence",
    "data-analyst-evidence-panel",
    "Internal evidence cards",
    "model claims are unverified",
    "evidenceSnapshot",
    "data-analyst-suggestion",
    "Explain move",
    "Summarize relevant news",
    "Analyze this region",
    "Plan validation timeline",
    "Context snapshot",
    "Plan approval never bypasses",
    "selectedAutonomyProposals",
    "autonomyProposalCards",
    "mode:manual",
    "mode:hybrid",
    "mode:autonomous",
    "focus:autonomy",
    "focus:news",
    "focus:watchlist",
    "focus:metrics",
    "Change autonomy state",
    "ANALYST_CONTEXT_STORAGE_KEY",
    "ANALYST_CONTEXT_SCHEMA_VERSION",
    "restoreAnalystContext",
    "persistAnalystContext",
    "Corrupt presentation state is ignored",
    "renderScheduled",
    "scheduleRender",
    "requestAnimationFrame",
    "High-frequency runtime updates are batched",
    "messageStationExpiry",
    "durationMs = alert.severity >= 2",
    "All systems clear",
    "messageHistory",
    "alertsDegraded",
    "Alert service unavailable; displayed alerts may be stale",
    "data-alert-retry",
    "nativeAlertPermissionError",
    "Native notifications are unavailable or blocked",
    "nativePermissionBlocked",
    "alert-severity-label",
    "severityLabels[Math.max",
    "messageStationExpiry.size > 4096",
    "activeIds",
    "ALERT_SOUND_SEVERITY_STORAGE_KEY",
    "data-alert-sound-severity",
    "soundSeverityEnabled",
    "alert.severity] !== false",
    "data-settings-open-panel",
    "Open CFG Generator",
    "news.http_timeout_ms",
    "news.max_retries",
    "news.retry_base_ms",
    "news.retry_max_ms",
    "reconciliation.poll_ms",
    "alerts.webhook_timeout_ms",
    "alerts.webhook_poll_ms",
    "alerts.cooldown_ms",
    "alerts.max_pending",
    "data-config-alert-cooldown",
    "data-config-alert-max-pending",
    "supervisor.max_failures",
    "supervisor.window_ns",
    "supervisor.initial_backoff_ns",
    "supervisor.max_backoff_ns",
    "supervisor.jitter_bps",
    "data-config-supervisor-failures",
    "llm.model",
    "data-config-llm-model",
    "llm.prompt_version",
    "ui.status_poll_ms",
    "data-config-ui-status-poll",
    "Generated configuration exceeds the 1 MiB input bound",
    "new TextEncoder().encode(mergedConfiguration).length > 1_048_576",
    "new TextEncoder().encode(value).length > 16_384",
    "cfgStrings.some((value) => new TextEncoder().encode(value).length > 16_384 || /[\\u0000-\\u001f]/.test(value))",
    "configuredUiStatusPollMs",
    "scheduleStatusRefresh",
    "Promise.allSettled",
    "statusRefreshTimer",
    "data-config-llm-prompt-version",
    "configStringValue(configSnapshot?.cfg_text ?? \"\", \"llm.model\"",
    "alerts.webhook_url",
    "data-config-webhook-url",
    "parsedWebhookUrl",
    "market.yahoo_base_url",
    "market.yahoo_interval",
    "market.yahoo_range",
    "python.cpu_seconds",
    "python.memory_bytes",
    "data-config-python-cpu",
    "data-config-python-memory",
    "data-config-yahoo-base-url",
    "data-config-yahoo-interval",
    "data-config-yahoo-range",
    "data-config-yahoo-symbols",
    "validateYahooSymbols",
    "Yahoo symbol list may contain at most 128 entries.",
    "Yahoo symbols and instrument IDs must be unique.",
    "Open Broker Status",
    "Open Order Ticket",
    "workspace-template-menu",
    "data-workspace-create-template",
    "loadWorkspaceLayout(layoutStorage, name, preset)",
    "workspacePersistence.flush()",
    "workspacePersistence.remove(current.name)",
    "CUSTOM_WORKSPACES_KEY",
    "data-workspace-duplicate",
    "data-workspace-rename",
    "data-workspace-delete",
    "persistCustomWorkspaces",
    "WORKSPACE_NAME_PATTERN",
    "copiedContext",
    "renamedContext",
    "delete workspaceContexts[current.name]",
    "Scalping",
    "Swing",
    "Backtest",
    "workspaceAddOpen",
    "workspace:MultiChart",
    "Cmd/Ctrl + 1–9",
    "workspaceTabOrder[Number(event.key) - 1]",
    "touchPointers",
    "pinchDistance",
    "pointercancel",
    "chartHoverIndex = undefined;\n      chartHoverYPercent = undefined;",
    "--chart-pinch-scale",
    "--chart-pan-translate",
    "selectedContextHit",
    "data-context-hit-id",
    "context-hit-detail",
    "panelByKind",
    "Evidence path:",
    "tool-label",
    "invalidateOrderPreview",
    "data-order-side], [data-order-type]",
    "status: \"idle\"",
    "previewExpired",
    "Preview expiry",
    "Expired — preview again",
    "ANALYST_DISPLAY_TTL_MS",
    "analystReceivedAtMs",
    "Stale analyst response",
    "analystStaleNoticeShown",
  ]) assert.match(source, new RegExp(marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), marker);
  assert.doesNotMatch(source, /setInterval\(\(\) => \{ void refresh(?:NewsProvider|Supervisor|Broker|RiskPolicy)Status\(\); \}, 5_000\)/, "diagnostic polls must not use fixed five-second intervals");
  assert.match(tokens, /\.right-dock-splitter/);
  assert.match(tokens, /\.tools-rail/);
  assert.match(tokens, /@supports not \(backdrop-filter: blur\(1px\)\)/);
  assert.doesNotMatch(tokens, /\.chart-surface[^\{]*\{[^}]*backdrop-filter\s*:/s, "chart surfaces must not use backdrop blur");
  assert.match(tokens, /touch-action:\s*none/);
  assert.match(tokens, /--chart-pinch-scale/);
  assert.match(tokens, /@media \(max-width: 1200px\)/);
  assert.match(tokens, /@media \(max-width: 760px\)/);
  assert.match(tokens, /@media \(max-width: 760px\)[\s\S]*body \{ min-width: 0; \}/);
  assert.match(tokens, /@media \(prefers-reduced-motion: reduce\)/);
  for (const token of ["--font-display", "--font-body", "--font-mono", "--radius-panel", "--radius-control", "--glass-blur", "--border-focus"]) {
    assert.match(tokens, new RegExp(`${token}\\s*:`), token);
  }
  for (const token of ["--accent-gain", "--accent-gain-dim", "--accent-loss", "--accent-loss-dim", "--accent-warn", "--blur-panel", "--ease-fast", "--ease-structural"]) {
    assert.match(tokens, new RegExp(`${token}\\s*:`), token);
  }
  assert.match(tokens, /--positive:\s*#16c784/);
  assert.match(tokens, /--negative:\s*#ea3943/);
  assert.match(tokens, /--warning:\s*#f0a63f/);
  assert.match(tokens, /button, select[^\n]*border-radius:\s*var\(--radius-control\)/);
  assert.match(tokens, /button:focus-visible, input:focus-visible, select:focus-visible, textarea:focus-visible/);
  assert.match(tokens, /kbd[^\{]*\{[^}]*border-radius:\s*var\(--radius-control\)/s);
  assert.match(tokens, /\.right-dock-tabs button[^\{]*\{[^}]*border-radius:\s*var\(--radius-control\)/s);
  assert.match(tokens, /\.panel-link select[^\{]*\{[^}]*border-radius:\s*var\(--radius-control\)/s);
  assert.match(tokens, /\.context-chip[^\{]*\{[^}]*border-radius:\s*var\(--radius-control\)/s);
  assert.doesNotMatch(tokens, /\.context-chip[^\{]*\{[^}]*border-radius:\s*999px/s);
  assert.ok(source.indexOf("const layoutStorage =") < source.indexOf("let colorblindMode = layoutStorage.getItem"), "storage must initialize before appearance preferences");
  assert.match(storeSource, /MAX_NEWS_ITEMS\s*=\s*100_000/);
  assert.match(storeSource, /MAX_TRADE_PRINTS_PER_SYMBOL\s*=\s*100_000/);
  assert.match(workspaceSource, /Research: \[.*experiment-registry.*model-registry/s);
  assert.match(workspaceSource, /Strategies: \[.*strategy-browser.*backtest/s);
  assert.match(workspaceSource, /Autonomy: \[.*strategy-inspector/s);
  assert.match(workspaceSource, /Scalping: \[.*depth.*time-sales/s);
  assert.match(workspaceSource, /Swing: \[.*strategy-browser/s);
  assert.match(workspaceSource, /Backtest: \[.*experiment-registry/s);
  assert.match(chartSource, /data-candle-direction/);
  assert.match(chartSource, /ChartRenderMode = "candles" \| "bars"/);
  assert.match(chartSource, /data-bar-direction/);
  assert.match(chartSource, /candle-direction-glyph/);
  assert.match(chartSource, /const glyph = isUp \? "▲" : "▼"/);
  assert.match(chartSource, /candleWidth >= 5/);
  assert.match(chartSource, /mode === "bars" \? barShapes/);
  assert.match(chartSource, /const direction = isUp \? "Up" : "Down"/);
  assert.match(chartSource, /!Number\.isFinite\(metric\.score\)/);
  assert.match(chartSource, /validMarkerTime\(metric\.timeMs\)/);
  assert.match(chartSource, /MAX_RENDER_CANDLES = 4096/);
  assert.match(chartSource, /requestedEnd - MAX_RENDER_CANDLES/);
  assert.match(chartSource, /snapshot\.candles\.filter\(validCandle\)/);
  assert.match(chartSource, /const validCandles = candles\.filter\(validCandle\)/);
  assert.match(chartSource, /Object\.freeze\(validCandles\)/);
  assert.match(source, /chartRenderStarted/);
  assert.match(source, /const chartCandles = resampleCandles\(state\.chart\.candles, state\.selectedTimeframe\)/);
  assert.match(source, /candles: chartCandles/);
  assert.match(source, /data-chart-render-ms/);
  assert.match(source, /data-cancel-all-orders/);
  assert.match(source, /aria-label="Cancel all \$\{workingOrderCount\} working orders"/);
  assert.match(source, /aria-controls="workspace-main" aria-selected/);
  assert.match(source, /aria-setsize="\$\{workspaceTabOrder\.length\}" aria-posinset="\$\{index \+ 1\}"/);
  assert.match(source, /<div id="workspace-main" class="workspace-main"/);
  assert.match(source, /id="workspace-tab-\$\{escapeHtml\(preset\)\}"/);
  assert.match(source, /role="tabpanel" aria-labelledby="workspace-tab-\$\{escapeHtml\(workspaceLayout\.name\)\}"/);
  assert.match(source, /let workspaceTabOrder = loadWorkspaceTabOrder\(\);\s*layoutStorage\.setItem\(WORKSPACE_TAB_ORDER_KEY, JSON\.stringify\(workspaceTabOrder\)\);/);
  assert.match(source, /aria-controls="right-dock-panel" aria-selected/);
  assert.match(source, /id="right-dock-panel" role="tabpanel" aria-labelledby="right-dock-tab-\$\{rightDockTab\}"/);
  assert.match(source, /const RIGHT_DOCK_TAB_KEY = "insidertrader\.right-dock-tab\.v1"/);
  assert.match(source, /rightDockTab = loadRightDockTab\(\);\s*persistRightDockTab\(\);/);
  assert.match(source, /rightDockTab = tab;\s*persistRightDockTab\(\);/);
  assert.match(source, /data-cancel-all-orders aria-live="polite"/);
  assert.match(source, /aria-label="Cancel \$\{escapeHtml\(orderLabel\)\} order"/);
  assert.match(source, /aria-label="Replace \$\{escapeHtml\(orderLabel\)\} order"/);
  assert.match(source, /Request cancellation for \$\{confirmation\.count\}/);
  assert.match(source, /Cancel partial \(\$\{completed\}\/\$\{working\.length\}, \$\{failed\} failed\)/);
  assert.match(source, /data-close-position/);
  assert.match(source, /escapeHtml\(proposal\.strategyId\).*escapeHtml\(proposal\.symbol\)/);
  assert.match(source, /data-proposal="\$\{escapeHtml\(proposal\.proposalId\)\}"/);
  assert.match(source, /aria-label="Preview \$\{escapeHtml\(proposal\.strategyId\)\} proposal for \$\{escapeHtml\(proposal\.symbol\)\}"/);
  assert.match(source, /data-command="\$\{escapeHtml\(command\)\}">\$\{escapeHtml\(label\)\}/);
  assert.match(source, /event\.key === "Escape" && \(commandPaletteOpen \|\| settingsOpen \|\| workspaceDialog \|\| scheduleConfirmation \|\| cancelAllConfirmation \|\| replaceOrderDialog \|\| chartTemplateDialog \|\| lifecycleDialog \|\| modelEvidenceDialog \|\| autonomyModeDialog \|\| messageStationOpen\)/);
  assert.match(source, /settingsOpen = false;/);
  assert.match(source, /modalBackground\.setAttribute\("inert", ""\)/);
  assert.match(source, /modalBackground\.setAttribute\("aria-hidden", "true"\)/);
  assert.match(source, /event\.key === "Tab" && \(settingsOpen \|\| commandPaletteOpen \|\| workspaceDialog \|\| scheduleConfirmation \|\| cancelAllConfirmation \|\| replaceOrderDialog \|\| chartTemplateDialog \|\| lifecycleDialog \|\| modelEvidenceDialog \|\| autonomyModeDialog\)/);
  assert.match(source, /modal\.querySelectorAll<HTMLElement>\("button, input, select, textarea, \[tabindex\]"\)/);
  assert.match(source, /event\.shiftKey && \(active === first \|\| !modal\.contains\(active\)\)/);
  assert.match(source, /settingsOpen = true;[\s\S]*?root\.querySelector<HTMLInputElement>\("\[data-settings-search\]"\)\?\.focus\(\)/);
  assert.match(source, /function workspaceDialogMarkup\(\)/);
  assert.match(source, /role="dialog" aria-modal="true" aria-labelledby="workspace-dialog-title"/);
  assert.match(source, /dialogElement\?\.setAttribute\("aria-describedby", "workspace-dialog-description"\)/);
  assert.match(source, /querySelector\("p"\)\?\.setAttribute\("id", "workspace-dialog-description"\)/);
  assert.match(source, /data-workspace-dialog-submit/);
  assert.match(source, /workspaceDialog = \{ mode: "delete", initialName: current\.name \}/);
  assert.match(source, /function scheduleConfirmationMarkup\(\)/);
  assert.match(source, /data-schedule-confirmation-input/);
  assert.match(source, /phrase !== "CONFIRM"/);
  assert.match(source, /submitScheduledProposal\(confirmation\.proposalId/);
  assert.match(source, /scheduleConfirmation = \{ proposalId, kind: "implementation_shortfall" \}/);
  assert.match(source, /confirmation\.kind === "twap"/);
  assert.match(source, /function cancelAllConfirmationMarkup\(\)/);
  assert.match(source, /data-cancel-all-confirmation-input/);
  assert.match(source, /cancelAllConfirmation = \{ count: working\.length \}/);
  assert.match(source, /Type CONFIRM exactly to request cancellation/);
  assert.match(source, /function replaceOrderDialogMarkup\(\)/);
  assert.match(source, /data-replace-quantity-input/);
  assert.match(source, /data-replace-limit-input/);
  assert.match(source, /replaceOrderDialog = \{ clientOrderId, quantity: currentQuantity \}/);
  assert.match(source, /function chartTemplateDialogMarkup\(\)/);
  assert.match(source, /data-chart-template-name/);
  assert.match(source, /chartTemplateDialog = \{ mode: "delete", initialName: name \}/);
  assert.match(source, /chartTemplates = chartTemplates\.filter\(\(template\) => template\.name !== dialog\.initialName\)/);
  assert.match(source, /function lifecycleDialogMarkup\(\)/);
  assert.match(source, /data-lifecycle-confirmation/);
  assert.match(source, /data-lifecycle-evidence/);
  assert.match(source, /lifecycleDialog = \{ kind: "strategy", id: strategyId, lifecycle \}/);
  assert.match(source, /transitionMetricLifecycle\(dialog\.id, dialog\.lifecycle, phrase, evidence\)/);
  assert.match(source, /function modelEvidenceDialogMarkup\(\)/);
  assert.match(source, /data-model-evidence-input/);
  assert.match(source, /modelEvidenceDialog = \{ modelId, version, operation \}/);
  assert.match(source, /mutateModel\(\{ operation: dialog\.operation, model_id: dialog\.modelId/);
  assert.match(source, /function autonomyModeDialogMarkup\(\)/);
  assert.match(source, /data-autonomy-confirmation/);
  assert.match(source, /autonomyModeDialog = \{\}/);
  assert.match(source, /setTradingMode\("autonomous"\)/);
  assert.match(tokens, /\.workspace-dialog-backdrop/);
  assert.match(tokens, /\.settings-modal, \.workspace-dialog \{ background: rgba\(18, 22, 30, 0\.96\); \}/);
  assert.match(tokens, /button, select[^\{]*\{[^}]*transition: border-color var\(--ease-fast\)/s);
  assert.match(tokens, /\.tools-rail[^\{]*\{[^}]*transition: width var\(--ease-structural\)/s);
  assert.match(tokens, /@media \(prefers-reduced-motion: reduce\)/);
  assert.match(tokens, /\.metric > span:last-child, \.metric > strong[^\{]*\{[^}]*font-family: var\(--font-mono\)/s);
  assert.match(source, /News unavailable: \$\{escapeHtml\(newsError\)\}\. Cached items remain visible/);
  assert.match(source, /data-news-retry/);
  assert.match(source, /aria-label="Retry news for \$\{escapeHtml\(symbol\)\}"/);
  assert.match(source, /aria-live="polite" aria-busy="\$\{newsBusy\}"/);
  assert.match(source, /News provider health/);
  assert.match(source, /provider\.providerId\).*provider\.health/);
  assert.match(source, /role="tab"[\s\S]*aria-controls="news-feed-panel" aria-selected/);
  assert.match(source, /aria-setsize="4" aria-posinset="\$\{index \+ 1\}" aria-controls="news-feed-panel"/);
  assert.match(source, /role="tabpanel" aria-labelledby="news-tab-\$\{newsView\}"/);
  assert.match(source, /tabindex="\$\{newsView === view \? "0" : "-1"\}"/);
  assert.match(source, /event\.key === "ArrowLeft"[\s\S]*event\.key === "Home"[\s\S]*tabs\[nextIndex\]\?\.click\(\)/);
  assert.match(source, /const NEWS_VIEW_STORAGE_KEY = "insidertrader\.news-view\.v1"/);
  assert.match(source, /ui\.news_stale_after_ms/);
  assert.match(source, /data-config-news-stale-after/);
  assert.match(source, /configuredNewsStaleAfterMs\(\)/);
  assert.match(source, /ui\.analyst_stale_after_ms/);
  assert.match(source, /data-config-analyst-stale-after/);
  assert.match(source, /configuredAnalystDisplayTtlMs\(\)/);
  assert.match(source, /ui\.alert_poll_ms/);
  assert.match(source, /data-config-alert-poll/);
  assert.match(source, /configuredAlertPollMs\(\)/);
  assert.doesNotMatch(source, /setInterval\(\(\) => \{ void refreshAlerts\(\); \}, 1_000\)/);
  for (const marker of [
    "data-config-python-executable", "data-config-python-workdir",
    "data-config-python-metrics-root", "data-config-python-strategies-root",
    '"python.executable": JSON.stringify(pythonExecutable)',
    '"python.workdir": JSON.stringify(pythonWorkdir)',
    '"python.metrics_root": JSON.stringify(pythonMetricsRoot)',
    '"python.strategies_root": JSON.stringify(pythonStrategiesRoot)',
  ]) assert.ok(source.includes(marker), marker);
  for (const marker of [
    '"market.http_timeout_ms": String(marketHttpTimeout)',
    '"market.yahoo_interval_ns": String(yahooIntervalNs)',
    '"market.yahoo_price_scale": String(yahooPriceScale)',
    '"market.yahoo_poll_ms": String(yahooHistoryPoll)',
    '"market.yahoo_quote_poll_ms": String(yahooQuotePoll)',
  ]) assert.ok(source.includes(marker), marker);
  assert.match(source, /newsView = loadNewsView\(\);\s*persistNewsView\(\);/);
  assert.match(source, /newsView = view;\s*persistNewsView\(\);/);
  assert.match(tokens, /\.provider-chip::before\s*\{\s*content: "•"/);
  assert.match(tokens, /\.provider-healthy::before/);
  assert.match(tokens, /\.provider-cooling_down::before/);
  assert.match(tokens, /\.provider-failed::before/);
  assert.match(source, /loadNewsPage\(newsRetryScope, newsRetrySymbol \|\| symbolFor\("news"\), newsRetryCursor\)/);
  assert.match(source, /if \(afterCursor === undefined\) \{\s*store\.resetNewsPage\(\);/);
  assert.match(source, /if \(newsBusy\) \{\s*if \(afterCursor === undefined\) queuedNewsRequest = \{ scope, symbol \};/);
  assert.match(source, /const queued = queuedNewsRequest;\s*queuedNewsRequest = undefined;\s*if \(queued\) void loadNewsPage\(queued\.scope, queued\.symbol, queued\.cursor\)/);
  assert.match(source, /News feed is stale; retrying will refresh provider data/);
  assert.match(source, /loadNewsPage\(state\.newsScope, symbolFor\("news"\), state\.newsNextCursor\)/);
  assert.match(source, /aria-label="\$\{pinned \? "Unpin" : "Pin"\} \$\{escapeHtml\(item\.title\)\}"/);
  assert.match(source, /aria-label="Open details for \$\{escapeHtml\(item\.title\)\}"/);
  assert.match(source, /aria-label="Draft order from \$\{escapeHtml\(proposal\.strategyId\)\} for \$\{escapeHtml\(proposal\.symbol\)\}"/);
  assert.match(source, /aria-label="Schedule TWAP from \$\{escapeHtml\(proposal\.strategyId\)\} for \$\{escapeHtml\(proposal\.symbol\)\}"/);
  assert.match(source, /aria-label="Schedule implementation shortfall from \$\{escapeHtml\(proposal\.strategyId\)\} for \$\{escapeHtml\(proposal\.symbol\)\}"/);
  assert.match(source, /escapeHtml\(position\.symbol\)\} ×/);
  assert.match(source, /aria-label="Draft close order for \$\{escapeHtml\(position\.symbol\)\}/);
  assert.match(source, /resolveInstrument\(position\.symbol\)/);
  assert.match(source, /instrumentId: resolution\.instrumentId/);
  const closeHandlerIndex = source.indexOf("[data-close-position]");
  assert.ok(closeHandlerIndex >= 0 && source.indexOf("resolveInstrument(position.symbol)", closeHandlerIndex) < source.indexOf("store.setOrderTicket({", closeHandlerIndex), "close drafting must resolve the catalog identity first");
  assert.match(source, /Chart render duration/);
  assert.match(source, /const CHART_FRAME_BUDGET_MS = 1000 \/ 60/);
  assert.match(source, /chartRenderMs > CHART_FRAME_BUDGET_MS/);
  assert.match(source, /chart-connection-notice/);
  assert.match(source, /displayed prices may be stale/);
  assert.match(source, /New orders remain subject to engine freshness checks/);
  assert.match(tokens, /chart-render-over-budget/);
});

test("all required workstation panels and preset surfaces remain wired", () => {
  const requiredPanels = [
    ["Chart", "chart"], ["Watchlist", "watchlist"], ["Order Ticket", "order-ticket"],
    ["Positions", "positions"], ["Portfolio", "portfolio"], ["Strategy Browser", "strategy-browser"],
    ["Strategy Inspector", "strategy-inspector"], ["Metrics", "metrics"], ["Metric Inspector", "metric-inspector"],
    ["News", "news"], ["News Detail", "news-detail"], ["AI Analyst", "ai-analyst"],
    ["Autonomy", "autonomy"], ["Screener", "screener"], ["Heatmap", "heatmap"], ["Correlation", "correlation"],
    ["Alerts", "alerts"], ["Backtest", "backtest"], ["Experiment Registry", "experiment-registry"],
    ["Model Registry", "model-registry"], ["TCA", "tca"], ["Logs / Trace", "trace"],
  ];
  for (const [title, id] of requiredPanels) {
    assert.match(source, new RegExp(`panel\\("${title}", "${id}"`), `${title} panel must be rendered through the panel shell`);
  }
  for (const preset of ["Trading", "MultiChart", "News", "Strategies", "Autonomy", "Execution", "Research", "Scalping", "Swing", "Backtest"]) {
    assert.match(workspaceSource, new RegExp(`${preset}: \\[`, "m"), `${preset} preset must be declared`);
  }
  assert.match(workspaceSource, /const ALL_PANEL_IDS: readonly PanelId\[\] = \[/);
  assert.match(source, /function panel\(title: string, className: string, body: string\)/);
  assert.match(source, /aria-labelledby="panel-title-\$\{className\}"/);
  assert.match(source, /<h2 id="panel-title-\$\{className\}">\$\{title\}<\/h2>/);
  assert.match(source, /data-panel-id="\$\{className\}" draggable="true"/);
});

test("CFG generator covers every engine guardrail", () => {
  for (const key of [
    "data-config-leverage",
    "data-config-max-position",
    "data-config-max-notional",
    "data-config-ibkr-timeout",
    "data-config-ibkr-poll",
    "data-config-ibkr-scale",
    "data-config-ibkr-base-url",
    "data-config-ibkr-account",
    "data-config-ibkr-conid",
    "data-config-ibkr-instrument-id",
    "data-config-python-allow-network",
    "data-config-newsapi-country",
    "data-config-newsapi-category",
    "data-config-newsapi-sources",
    "data-config-allow-yahoo-live-marks",
    "data-config-allow-ibkr-bootstrap-mark",
    "data-config-broker-mode",
    "data-config-reference-enabled",
    "data-config-reference-entry",
    "data-config-reference-exit",
    "data-config-reference-quantity",
    "data-config-reference-horizon",
    "data-config-reference-ttl",
    "data-config-reference-id",
    "data-config-reference-metric-id",
    "data-config-embeddings-enabled",
    "data-config-embedding-model",
    "data-config-embedding-version",
    "data-config-embedding-dimensions",
    "data-config-ewma-lambda",
    "data-config-metric-ttl",
    "data-config-sma-window",
    "data-config-drawdown",
    "data-config-orders",
    "data-config-volatility",
    "data-config-participation",
    "data-config-message-rate",
    "data-config-price-deviation",
    "data-config-python-cycle",
    "data-config-execution-cycle",
    "data-config-market-age",
    "data-config-newsapi-poll",
    "data-config-yahoo-news-poll",
    "data-config-llm-timeout",
    "data-config-llm-base-url",
    "data-config-newsapi-base-url",
    "data-config-newsapi-endpoint",
    "data-config-newsapi-query",
    "data-config-yahoo-query",
    "risk.max_leverage",
    "risk.max_position_ticks",
    "risk.max_gross_notional_ticks",
    "broker.ibkr_timeout_ms",
    "broker.ibkr_market_poll_ms",
    "broker.ibkr_price_scale",
    "broker.ibkr_base_url",
    "broker.mode",
    "strategy.reference_enabled",
    "strategy.reference_entry_threshold",
    "strategy.reference_exit_threshold",
    "strategy.reference_quantity_ticks",
    "strategy.reference_horizon_ns",
    "strategy.reference_ttl_ns",
    "strategy.reference_id",
    "strategy.reference_metric_id",
    "metric.ewma_lambda",
    "metric.ewma_ttl_ns",
    "metric.reference_ttl_ns",
    "metric.sma_window",
    "metric.ewma_id",
    "metric.sma_id",
    "metric.spread_id",
    "metric.imbalance_id",
    "embeddings.model",
    "embeddings.model_version",
    "embeddings.dimensions",
    "risk.max_drawdown_bps",
    "risk.max_outstanding_orders",
    "risk.max_predicted_volatility_bps",
    "risk.max_participation_bps",
    "risk.max_message_rate",
    "risk.max_price_deviation_bps",
    "scheduler.python_cycle_ms",
    "scheduler.execution_cycle_ms",
    "market.max_age_ms",
    "news.newsapi_poll_ms",
    "news.yahoo_poll_ms",
    "llm.timeout_ms",
    "llm.base_url",
    "news.newsapi_base_url",
    "news.newsapi_endpoint",
    "news.newsapi_query",
    "news.yahoo_query",
    "configStringValue",
    "CONFIG_RISK_FIELDS",
    "configNumericValue",
    "^-?(?:\\\\d+",
    "mergeRiskConfiguration",
    "descriptorFor",
    "renderedValue",
    "removeConfigKeys",
    "Existing non-generator settings are preserved",
  ]) assert.match(source, new RegExp(key.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")), key);
  const riskBlock = source.match(/const CONFIG_RISK_FIELDS = \[(.*?)\] as const;/s)?.[1] ?? "";
  const riskKeys = riskBlock.match(/\[\"risk\.[^\"]+\"/g) ?? [];
  assert.equal(riskKeys.length, 9, "risk generator must expose exactly nine controls");
  assert.equal(new Set(riskKeys).size, riskKeys.length, "risk generator controls must have unique keys");
});

test("AI Analyst request identity comes from CFG snapshot", () => {
  assert.match(source, /model: configStringValue\(configSnapshot\?\.cfg_text/);
  assert.match(source, /promptVersion: configStringValue\(configSnapshot\?\.cfg_text/);
  assert.doesNotMatch(source, /model:\s*"configured-model"/);
  assert.doesNotMatch(source, /promptVersion:\s*"ai-analyst\.v1"/);
});
