import type { PanelId } from "../stores/runtime-store";

export interface WorkspaceLayout {
  readonly schemaVersion: 1;
  readonly name: string;
  readonly panels: readonly PanelId[];
  readonly linkGroup: string;
}

/** Minimal storage contract so persistence is testable without browser globals. */
export interface LayoutStorage {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
}

const STORAGE_PREFIX = "insidertrader.workspace.v1:";

/** Canonical order domain; every rendered panel is represented in this list. */
export const ALL_PANEL_IDS: readonly PanelId[] = [
  "chart", "chart-secondary", "chart-tertiary", "chart-quaternary", "watchlist", "global-search", "order-ticket", "orders", "positions",
  "strategy-inspector", "strategy-comparison", "metrics", "metric-inspector", "news", "news-detail", "ai-analyst", "alerts",
  "trace", "broker-status", "tca", "autonomy", "risk", "system-health", "backup", "screener", "backtest", "experiment-registry", "model-registry", "portfolio", "strategy-browser", "depth", "time-sales", "correlation", "heatmap",
];

/** Built-in workstation presets required by the desktop contract. */
export type WorkspacePreset = "Trading" | "MultiChart" | "News" | "Strategies" | "Autonomy" | "Execution" | "Research" | "Scalping" | "Swing" | "Backtest";

const VALID_PANELS: ReadonlySet<PanelId> = new Set([
  "chart", "chart-secondary", "chart-tertiary", "chart-quaternary", "watchlist", "global-search", "order-ticket", "orders", "positions", "strategy-inspector", "strategy-comparison",
  "metrics", "metric-inspector", "news", "news-detail", "ai-analyst", "alerts", "trace", "broker-status", "tca", "autonomy", "risk", "system-health", "backup", "screener", "backtest", "experiment-registry", "model-registry", "portfolio", "strategy-browser", "depth", "time-sales", "correlation", "heatmap",
]);

export function validateWorkspaceLayout(layout: WorkspaceLayout): WorkspaceLayout {
  if (layout.schemaVersion !== 1 || !layout.name.trim() || !layout.linkGroup.trim()) {
    throw new Error("invalid workspace layout metadata");
  }
  const unique = new Set(layout.panels);
  if (unique.size !== layout.panels.length || [...unique].some((panel) => !VALID_PANELS.has(panel))) {
    throw new Error("workspace contains an invalid or duplicate panel");
  }
  return Object.freeze({ ...layout, panels: Object.freeze([...layout.panels]) });
}

/** Adds panels introduced by a newer UI without changing the user's order. */
export function completeWorkspaceLayout(layout: WorkspaceLayout): WorkspaceLayout {
  const present = new Set(layout.panels);
  return validateWorkspaceLayout({
    ...layout,
    panels: [...layout.panels, ...ALL_PANEL_IDS.filter((panel) => !present.has(panel))],
  });
}

/** Serializes only presentation layout; trading/runtime state is not accepted. */
export function serializeWorkspaceLayout(layout: WorkspaceLayout): string {
  const validated = validateWorkspaceLayout(layout);
  return JSON.stringify({
    schemaVersion: validated.schemaVersion,
    name: validated.name,
    panels: validated.panels,
    linkGroup: validated.linkGroup,
  });
}

function migrateWorkspaceLayout(value: unknown): WorkspaceLayout {
  if (!value || typeof value !== "object") throw new Error("workspace layout must be an object");
  const candidate = value as Record<string, unknown>;
  const name = candidate.name;
  const panels = candidate.panels;
  if (typeof name !== "string" || !Array.isArray(panels) || panels.some((panel) => typeof panel !== "string")) {
    throw new Error("workspace layout fields are invalid");
  }
  // Version 0 had no explicit link group. It is presentation-only and can be
  // migrated deterministically from the workspace name.
  const linkGroup = typeof candidate.linkGroup === "string" && candidate.linkGroup.trim()
    ? candidate.linkGroup
    : name.toLowerCase();
  return validateWorkspaceLayout({
    schemaVersion: 1,
    name,
    panels: panels as PanelId[],
    linkGroup,
  });
}

/** Strictly decodes persisted JSON and returns `undefined` for corrupt data. */
export function deserializeWorkspaceLayout(serialized: string): WorkspaceLayout | undefined {
  try {
    return migrateWorkspaceLayout(JSON.parse(serialized) as unknown);
  } catch {
    return undefined;
  }
}

/** Loads one layout or returns the validated fallback without throwing. */
export function loadWorkspaceLayout(
  storage: LayoutStorage,
  name: string,
  fallback: WorkspaceLayout,
): WorkspaceLayout {
  const persisted = storage.getItem(`${STORAGE_PREFIX}${name}`);
  return persisted ? deserializeWorkspaceLayout(persisted) ?? fallback : fallback;
}

/** Debounced versioned layout persistence with explicit flush on shutdown. */
export class WorkspacePersistence {
  #timer: ReturnType<typeof setTimeout> | undefined;
  #pending: WorkspaceLayout | undefined;

  constructor(
    private readonly storage: LayoutStorage,
    private readonly debounceMs = 250,
  ) {}

  /** Schedules a validated presentation-only layout write. */
  schedule(layout: WorkspaceLayout): void {
    this.#pending = validateWorkspaceLayout(layout);
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = setTimeout(() => {
      this.flush();
    }, this.debounceMs);
  }

  /** Immediately writes the latest layout, suitable for window close. */
  flush(): void {
    if (this.#timer !== undefined) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
    const pending = this.#pending;
    this.#pending = undefined;
    if (pending) this.storage.setItem(`${STORAGE_PREFIX}${pending.name}`, serializeWorkspaceLayout(pending));
  }

  /** Removes one persisted layout without affecting unrelated workspaces. */
  remove(name: string): void {
    if (this.#pending?.name === name) this.cancel();
    this.storage.removeItem(`${STORAGE_PREFIX}${name}`);
  }

  /** Cancels a pending write without mutating already-persisted data. */
  cancel(): void {
    if (this.#timer !== undefined) clearTimeout(this.#timer);
    this.#timer = undefined;
    this.#pending = undefined;
  }
}

/** Returns a validated layout preset; layouts contain presentation state only. */
export function createWorkspacePreset(name: WorkspacePreset): WorkspaceLayout {
  const panels: Record<WorkspacePreset, readonly PanelId[]> = {
    Trading: ["chart", "watchlist", "global-search", "news", "order-ticket", "orders", "positions", "risk", "alerts"],
    MultiChart: ["chart", "chart-secondary", "chart-tertiary", "chart-quaternary", "watchlist", "news", "strategy-inspector", "metrics"],
    News: ["chart", "news", "news-detail", "ai-analyst", "autonomy", "strategy-inspector"],
    Strategies: ["strategy-browser", "strategy-comparison", "strategy-inspector", "metrics", "chart", "positions", "news", "ai-analyst", "backtest", "portfolio"],
    Autonomy: ["autonomy", "strategy-inspector", "chart", "risk", "positions", "alerts", "trace", "system-health", "backup"],
    Execution: ["chart", "watchlist", "global-search", "order-ticket", "orders", "positions", "risk", "alerts", "trace", "system-health", "backup"],
    Research: ["chart", "strategy-inspector", "metrics", "news", "news-detail", "ai-analyst", "backtest", "experiment-registry", "model-registry", "trace", "system-health", "backup"],
    Scalping: ["chart", "watchlist", "depth", "time-sales", "order-ticket", "orders", "risk", "alerts", "tca"],
    Swing: ["chart", "watchlist", "news", "strategy-browser", "strategy-inspector", "portfolio", "risk", "alerts", "ai-analyst"],
    Backtest: ["backtest", "strategy-comparison", "experiment-registry", "model-registry", "chart", "metrics", "portfolio", "trace"],
  };
  return validateWorkspaceLayout({
    schemaVersion: 1,
    name,
    panels: panels[name],
    linkGroup: name.toLowerCase(),
  });
}
