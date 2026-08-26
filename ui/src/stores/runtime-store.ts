import { MarketChartModel, type Candle, type CandleBatch, type ChartDrawing, type ChartSeriesSnapshot } from "../charts/market-chart";

export type PanelId =
  | "chart"
  | "chart-secondary"
  | "chart-tertiary"
  | "chart-quaternary"
  | "watchlist"
  | "global-search"
  | "order-ticket"
  | "orders"
  | "positions"
  | "strategy-inspector"
  | "strategy-comparison"
  | "metrics"
  | "metric-inspector"
  | "news"
  | "news-detail"
  | "ai-analyst"
  | "alerts"
  | "trace"
  | "broker-status"
  | "tca"
  | "autonomy"
  | "risk"
  | "system-health"
  | "screener"
  | "backtest"
  | "experiment-registry"
  | "model-registry"
  | "portfolio"
  | "strategy-browser"
  | "depth"
  | "time-sales"
  | "correlation"
  | "heatmap"
  | "backup";

export type TradingMode = "manual" | "hybrid" | "autonomous";
export type ConnectionState = "disconnected" | "connecting" | "ready" | "stale" | "degraded";
export type NewsScope = "relevant" | "all";

export interface QuoteSnapshot {
  readonly symbol: string;
  readonly bidTicks: number;
  readonly askTicks: number;
  readonly lastTicks: number;
  readonly sequence: number;
  readonly receivedAtMs: number;
  readonly bidQuantityTicks?: number;
  readonly askQuantityTicks?: number;
  readonly bookTop?: readonly [number, number, number, number];
}

export interface TradePrintSnapshot {
  readonly sequence: number;
  readonly exchangeTimeNs: number;
  readonly receivedMonoNs: number;
  readonly priceTicks: number;
  readonly quantityTicks: number;
}

export interface PositionSnapshot {
  readonly symbol: string;
  readonly quantityTicks: number;
  readonly markTicks: number;
  readonly averageCostTicks: number;
  readonly pnlTicks: number;
}

export interface OrderSnapshot {
  readonly clientOrderId: string;
  readonly brokerOrderId?: string;
  readonly instrumentId: string;
  readonly side: OrderSide;
  readonly quantityTicks: number;
  readonly filledQuantityTicks: number;
  readonly state: string;
}

export interface TcaSnapshot {
  readonly clientOrderId: string;
  readonly filledQuantityTicks: number;
  readonly notionalTicks: number | string;
  readonly averageFillPriceNumerator: number | string;
  readonly averageFillPriceDenominator: number;
  readonly arrivalPriceTicks?: number;
  readonly decisionMonoNs?: number;
  readonly sendMonoNs?: number;
  readonly ackMonoNs?: number;
  readonly firstFillMonoNs?: number;
  readonly implementationShortfallTickValue?: number | string;
  readonly averageSpreadTicks?: number;
  readonly adverseSelectionTickValue?: number | string;
}

export interface ProposalSnapshot {
  readonly proposalId: string;
  readonly strategyId: string;
  readonly symbol: string;
  readonly action: "no_action" | "target" | "increase" | "decrease" | "close";
  readonly quantityTicks?: number;
  readonly confidence: number;
  readonly expiresAtMs: number;
  readonly rationaleCodes: readonly string[];
}

export interface NewsItemSnapshot {
  readonly id: string;
  readonly title: string;
  readonly source: string;
  readonly canonicalUrl: string;
  readonly publishedAtMs?: number;
  readonly receivedAtMs: number;
  readonly symbols: readonly string[];
  readonly relevanceScore: number;
  readonly clusterId?: string;
}

export interface NewsPageSnapshot {
  readonly items: readonly NewsItemSnapshot[];
  readonly nextCursor?: string;
}

export interface RiskSnapshot {
  readonly state: "running" | "reduce_only" | "cancel_only" | "halted";
  readonly grossNotionalTicks: number | string;
  readonly maxGrossNotionalTicks: number | string;
  readonly grossUtilizationBps: number;
  readonly largestPositionNotionalTicks: number | string;
  readonly drawdownBps?: number;
}

export interface AutonomySnapshot {
  readonly mode: TradingMode;
  readonly planId?: string;
  readonly model?: string;
  readonly pendingActionCount: number;
  readonly stale: boolean;
  readonly selectedProposalIds?: readonly string[];
  readonly reconsiderAfterMs?: number;
  readonly planState?: "pending" | "approved" | "rejected" | "expired" | "executing" | "completed" | "failed";
  readonly planExpiresMonoNs?: number;
}

export type OrderSide = "buy" | "sell";
export type OrderType = "market" | "limit";
/** Hard bounded client retention for the 100,000-item virtualized news contract. */
const MAX_NEWS_ITEMS = 100_000;
const MAX_TRADE_PRINTS_PER_SYMBOL = 100_000;

export interface OrderDraft {
  readonly symbol: string;
  readonly instrumentId?: string;
  readonly side: OrderSide;
  readonly type: OrderType;
  readonly quantityTicks: number;
  readonly limitPriceTicks?: number;
}

export type OrderTicketStatus = "idle" | "previewing" | "ready" | "submitting" | "submitted" | "rejected";

export interface OrderPreview {
  readonly previewId: string;
  readonly draft: OrderDraft;
  readonly expectedStateVersion: number;
  readonly expiresAtMs: number;
  readonly estimatedNotionalTicks?: number;
  readonly estimatedCostBps?: number;
  readonly warnings: readonly string[];
}

export interface OrderTicketState {
  readonly status: OrderTicketStatus;
  readonly draft?: OrderDraft;
  readonly idempotencyKey?: string;
  readonly preview?: OrderPreview;
  readonly error?: string;
  readonly submittedOrderId?: string;
}

export function validateOrderDraft(draft: OrderDraft): string | undefined {
  if (!/^[A-Z0-9.\-]{1,16}$/.test(draft.symbol)) return "symbol is invalid";
  if (!Number.isSafeInteger(draft.quantityTicks) || draft.quantityTicks <= 0) return "quantity must be a positive integer";
  if (draft.type === "limit" && (!Number.isSafeInteger(draft.limitPriceTicks) || (draft.limitPriceTicks ?? 0) <= 0)) return "limit price is required";
  if (draft.type === "market" && draft.limitPriceTicks !== undefined) return "market orders cannot include a limit price";
  return undefined;
}

export interface RuntimeState {
  readonly selectedSymbol: string;
  readonly selectedTimeframe: string;
  readonly quotes: Readonly<Record<string, QuoteSnapshot>>;
  readonly tradesBySymbol: Readonly<Record<string, readonly TradePrintSnapshot[]>>;
  readonly positions: readonly PositionSnapshot[];
  readonly orders: readonly OrderSnapshot[];
  readonly tca: readonly TcaSnapshot[];
  readonly proposals: readonly ProposalSnapshot[];
  readonly news: readonly NewsItemSnapshot[];
  readonly newsScope: NewsScope;
  readonly pinnedNews: readonly string[];
  readonly newsNextCursor?: string;
  readonly newsHasMore: boolean;
  readonly risk: RiskSnapshot;
  readonly autonomy: AutonomySnapshot;
  readonly openPanels: readonly PanelId[];
  readonly version: number;
  readonly connection: ConnectionState;
  readonly cursor: number;
  readonly orderTicket?: OrderTicketState;
  readonly chart: ChartSeriesSnapshot;
}

export type RuntimePatch = Partial<Omit<RuntimeState, "quotes" | "version">> & {
  readonly quotes?: Readonly<Record<string, QuoteSnapshot>>;
};

export type RuntimeListener = (state: RuntimeState) => void;

const DEFAULT_STATE: RuntimeState = {
  selectedSymbol: "AAPL",
  selectedTimeframe: "1m",
  quotes: {},
  tradesBySymbol: {},
  positions: [],
  orders: [],
  tca: [],
  proposals: [],
  news: [],
  newsScope: "relevant",
  pinnedNews: [],
  newsNextCursor: undefined,
  newsHasMore: false,
  risk: { state: "running", grossNotionalTicks: 0, maxGrossNotionalTicks: 0, grossUtilizationBps: 0, largestPositionNotionalTicks: 0 },
  autonomy: { mode: "manual", pendingActionCount: 0, stale: false },
  openPanels: ["chart", "watchlist", "positions", "risk"],
  version: 0,
  connection: "disconnected",
  cursor: 0,
  orderTicket: { status: "idle" },
  chart: new MarketChartModel().snapshot(),
};

function freezeState(state: RuntimeState): RuntimeState {
  return Object.freeze({
    ...state,
    quotes: Object.freeze({ ...state.quotes }),
    tradesBySymbol: Object.freeze(Object.fromEntries(Object.entries(state.tradesBySymbol ?? {}).map(([symbol, trades]) => [symbol, Object.freeze([...trades].slice(-MAX_TRADE_PRINTS_PER_SYMBOL))]))),
    positions: Object.freeze([...state.positions]),
    orders: Object.freeze([...state.orders]),
    tca: Object.freeze([...state.tca]),
    proposals: Object.freeze([...state.proposals]),
    news: Object.freeze([...state.news]),
    pinnedNews: Object.freeze([...state.pinnedNews]),
    openPanels: Object.freeze([...state.openPanels]),
    chart: state.chart,
    autonomy: state.autonomy
      ? Object.freeze({
        ...state.autonomy,
        ...(state.autonomy.selectedProposalIds
          ? { selectedProposalIds: Object.freeze([...state.autonomy.selectedProposalIds]) }
          : {}),
      })
      : state.autonomy,
    ...(state.orderTicket
      ? {
          orderTicket: Object.freeze({
            ...state.orderTicket,
            ...(state.orderTicket.preview
              ? { preview: Object.freeze({ ...state.orderTicket.preview, warnings: Object.freeze([...state.orderTicket.preview.warnings]) }) }
              : {}),
          }),
        }
      : {}),
  });
}

/** Small immutable store shared by Tauri commands and panel subscriptions. */
export class RuntimeStore {
  #state: RuntimeState = freezeState(DEFAULT_STATE);
  readonly #chartModel = new MarketChartModel();
  readonly #listeners = new Set<RuntimeListener>();

  get state(): RuntimeState {
    return this.#state;
  }

  private syncNewsMarkers(items: readonly NewsItemSnapshot[], selectedSymbol: string): void {
    this.#chartModel.replaceNews(items
      .filter((item) => item.symbols.includes(selectedSymbol))
      .map((item) => ({
        id: item.id,
        timeMs: item.publishedAtMs ?? item.receivedAtMs,
        title: item.title,
        relevance: item.relevanceScore,
        ...(item.clusterId ? { clusterId: item.clusterId } : {}),
      })));
  }

  subscribe(listener: RuntimeListener): () => void {
    this.#listeners.add(listener);
    listener(this.#state);
    return () => this.#listeners.delete(listener);
  }

  patch(patch: RuntimePatch): RuntimeState {
    this.#state = freezeState({ ...this.#state, ...patch, version: this.#state.version + 1 });
    for (const listener of this.#listeners) listener(this.#state);
    return this.#state;
  }

  setConnection(connection: ConnectionState): RuntimeState {
    return this.patch({ connection });
  }

  applySnapshot(snapshot: RuntimeSnapshot): RuntimeState {
    if (!Number.isSafeInteger(snapshot.cursor) || snapshot.cursor < this.#state.cursor) {
      return this.#state;
    }
    if (snapshot.state.chart) {
      try {
        this.#chartModel.recoverAfterSnapshot(
          snapshot.state.chart.lastSequence,
          snapshot.state.chart.candles,
        );
      } catch {
        return this.setConnection("stale");
      }
    }
    this.syncNewsMarkers(snapshot.state.news, snapshot.state.selectedSymbol);
    this.#state = freezeState({
      ...snapshot.state,
      selectedTimeframe: this.#state.selectedTimeframe,
      version: Math.max(this.#state.version + 1, snapshot.state.version),
      connection: "ready",
      cursor: snapshot.cursor,
      chart: this.#chartModel.snapshot(),
    });
    for (const listener of this.#listeners) listener(this.#state);
    return this.#state;
  }

  applyDelta(delta: RuntimeDelta): RuntimeState {
    if (!Number.isSafeInteger(delta.cursor) || delta.cursor !== this.#state.cursor + 1) {
      return this.setConnection("stale");
    }
    let chart = this.#state.chart;
    if (delta.chartRecovery) {
      this.#chartModel.recoverAfterSnapshot(delta.chartRecovery.sequence, delta.chartRecovery.candles);
      chart = this.#chartModel.snapshot();
    } else if (delta.chartBatch) {
      this.#chartModel.apply(delta.chartBatch);
      chart = this.#chartModel.snapshot();
    }
    this.#state = freezeState({
      ...this.#state,
      ...delta.patch,
      chart,
      version: this.#state.version + 1,
      cursor: delta.cursor,
      connection: "ready",
    });
    for (const listener of this.#listeners) listener(this.#state);
    return this.#state;
  }

  selectSymbol(symbol: string): RuntimeState {
    const normalized = symbol.trim().toUpperCase();
    if (!normalized || (!/^[A-Z0-9.\-]{1,16}$/.test(normalized) && !/^\d{1,39}$/.test(normalized))) return this.#state;
    // A chart series is scoped to one instrument. Clear the old series at the
    // selection boundary; the next authoritative snapshot installs the new
    // instrument's candles or leaves the chart explicitly awaiting data.
    this.#chartModel.recoverAfterSnapshot(-1, []);
    this.syncNewsMarkers(this.#state.news, normalized);
    return this.patch({ selectedSymbol: normalized, chart: this.#chartModel.snapshot() });
  }

  /** Changes the UI chart timeframe using a bounded canonical duration token. */
  selectTimeframe(timeframe: string): RuntimeState {
    const normalized = timeframe.trim().toLowerCase();
    if (!/^\d{1,4}(s|m|h|d|w)$/.test(normalized) || normalized === this.#state.selectedTimeframe) return this.#state;
    return this.patch({ selectedTimeframe: normalized });
  }

  /** Replaces presentation-only drawings without changing authoritative trading state. */
  replaceChartDrawings(drawings: readonly ChartDrawing[]): RuntimeState {
    this.#chartModel.replaceDrawings(drawings);
    return this.patch({ chart: this.#chartModel.snapshot() });
  }

  /** Adds or updates one validated presentation-only drawing. */
  upsertChartDrawing(drawing: ChartDrawing): RuntimeState {
    this.#chartModel.upsertDrawing(drawing);
    return this.patch({ chart: this.#chartModel.snapshot() });
  }

  /** Removes one presentation-only drawing by stable ID. */
  removeChartDrawing(id: string): RuntimeState {
    this.#chartModel.removeDrawing(id);
    return this.patch({ chart: this.#chartModel.snapshot() });
  }

  /** Changes the news feed scope without mutating authoritative news data. */
  setNewsScope(newsScope: NewsScope): RuntimeState {
    return this.patch({ newsScope });
  }

  /** Pins or unpins one immutable article identity for later review. */
  togglePinnedNews(newsId: string): RuntimeState {
    if (!newsId.trim() || !this.#state.news.some((item) => item.id === newsId)) return this.#state;
    const pinned = new Set(this.#state.pinnedNews);
    if (pinned.has(newsId)) pinned.delete(newsId);
    else pinned.add(newsId);
    return this.patch({ pinnedNews: [...pinned].sort() });
  }

  /** Replaces or appends one bounded backend news page without duplicating IDs. */
  applyNewsPage(page: NewsPageSnapshot, replace = false): RuntimeState {
    const byId = new Map<string, NewsItemSnapshot>(replace ? [] : this.#state.news.map((item) => [item.id, item]));
    for (const item of page.items) byId.set(item.id, item);
    const ordered = [...byId.values()].sort((left, right) =>
      right.receivedAtMs - left.receivedAtMs || left.id.localeCompare(right.id));
    const retained = new Map(
      ordered.slice(0, MAX_NEWS_ITEMS).map((item) => [item.id, item]),
    );
    // Pinned articles are explicit user state and remain visible even when
    // they fall outside the rolling feed bound. Evict the oldest unpinned
    // item to make room, rather than allowing pins to defeat the hard cap.
    for (const pinnedId of this.#state.pinnedNews) {
      const pinned = byId.get(pinnedId);
      if (!pinned || retained.has(pinnedId)) continue;
      const evict = [...retained.values()]
        .reverse()
        .find((item) => !this.#state.pinnedNews.includes(item.id));
      if (evict) retained.delete(evict.id);
      retained.set(pinnedId, pinned);
    }
    const retainedIds = new Set(retained.keys());
    const news = [...retained.values()];
    this.syncNewsMarkers(news, this.#state.selectedSymbol);
    return this.patch({
      news,
      pinnedNews: this.#state.pinnedNews.filter((id) => retainedIds.has(id)),
      newsNextCursor: page.nextCursor,
      newsHasMore: page.nextCursor !== undefined,
    });
  }

  /** Clears the current feed and cursor before changing scope or symbol. */
  resetNewsPage(): RuntimeState {
    return this.patch({ news: [], newsNextCursor: undefined, newsHasMore: false });
  }

  upsertQuote(quote: QuoteSnapshot): RuntimeState {
    const previous = this.#state.quotes[quote.symbol];
    if (previous && quote.sequence <= previous.sequence) return this.#state;
    return this.patch({ quotes: { ...this.#state.quotes, [quote.symbol]: quote } });
  }

  /** Updates the manual order-ticket lifecycle without mutating trading state. */
  setOrderTicket(ticket: OrderTicketState): RuntimeState {
    return this.patch({ orderTicket: ticket });
  }
}

export interface RuntimeSnapshot {
  readonly cursor: number;
  readonly state: RuntimeState;
}

export interface RuntimeDelta {
  readonly cursor: number;
  readonly patch: RuntimePatch;
  /** Optional batched chart stream, kept out of low-rate state patches. */
  readonly chartBatch?: CandleBatch;
  /** Optional authoritative snapshot used to repair a detected sequence gap. */
  readonly chartRecovery?: { readonly sequence: number; readonly candles: readonly Candle[] };
}
