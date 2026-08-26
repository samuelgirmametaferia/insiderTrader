/**
 * Renderer-neutral chart model for batched market updates.
 *
 * The model deliberately owns no DOM or chart-library objects.  A worker or a
 * Lightweight Charts adapter can consume the immutable snapshots returned by
 * `snapshot()`, while ingestion remains bounded and off the React render path.
 */

export interface Candle {
  readonly timeMs: number;
  readonly openTicks: number;
  readonly highTicks: number;
  readonly lowTicks: number;
  readonly closeTicks: number;
  readonly volumeTicks?: number;
  readonly sequence: number;
}

export interface CandleBatch {
  readonly sequence: number;
  readonly candles: readonly Candle[];
}

export interface NewsMarker {
  readonly id: string;
  readonly timeMs: number;
  readonly title: string;
  readonly relevance: number;
  readonly clusterId?: string;
}

export interface StrategyMarker {
  readonly id: string;
  readonly timeMs: number;
  readonly kind: "entry" | "exit" | "signal";
  readonly label: string;
  readonly proposalId?: string;
}

export interface MetricOverlay {
  readonly id: string;
  readonly timeMs: number;
  readonly score: number;
  readonly color?: string;
  readonly label?: string;
}

/** User-authored, presentation-only drawing anchored to chart data coordinates. */
export interface ChartDrawing {
  readonly id: string;
  readonly kind: "horizontal" | "trendline";
  readonly startTimeMs: number;
  readonly startPriceTicks: number;
  readonly endTimeMs?: number;
  readonly endPriceTicks?: number;
  readonly color: string;
  readonly label?: string;
}

export interface ChartSeriesSnapshot {
  readonly candles: readonly Candle[];
  readonly news: readonly NewsMarker[];
  readonly strategies: readonly StrategyMarker[];
  readonly metrics?: readonly MetricOverlay[];
  readonly drawings: readonly ChartDrawing[];
  readonly lastSequence: number;
  readonly droppedBatches: number;
  readonly requiresRecovery: boolean;
}

export interface BatchResult {
  readonly accepted: number;
  readonly replaced: number;
  readonly rejected: number;
  readonly dropped: boolean;
}

/** Deterministically aggregates canonical minute candles for UI timeframes. */
export function resampleCandles(candles: readonly Candle[], timeframe: string): readonly Candle[] {
  const minutes = ({ "1m": 1, "5m": 5, "15m": 15, "1h": 60, "1d": 1_440 } as Record<string, number>)[timeframe];
  const validCandles = candles.filter(validCandle);
  if (!minutes || minutes === 1) return Object.freeze(validCandles);
  const bucketMs = minutes * 60_000;
  const grouped = new Map<number, Candle>();
  for (const candle of validCandles) {
    const bucket = Math.floor(candle.timeMs / bucketMs) * bucketMs;
    const previous = grouped.get(bucket);
    if (!previous) {
      grouped.set(bucket, { ...candle, timeMs: bucket });
      continue;
    }
    grouped.set(bucket, {
      ...previous,
      highTicks: Math.max(previous.highTicks, candle.highTicks),
      lowTicks: Math.min(previous.lowTicks, candle.lowTicks),
      closeTicks: candle.closeTicks,
      volumeTicks: (previous.volumeTicks ?? 0) + (candle.volumeTicks ?? 0),
      sequence: Math.max(previous.sequence, candle.sequence),
    });
  }
  return Object.freeze([...grouped.values()].sort((left, right) => left.timeMs - right.timeMs));
}

/** Validated dimensions for the renderer; prevents pathological SVG output. */
export interface ChartViewport {
  readonly width: number;
  readonly height: number;
}

/** Bounded candle window selected by user interaction; never mutates source data. */
export interface ChartViewWindow {
  readonly start: number;
  readonly end: number;
}

export type ChartRenderMode = "candles" | "bars" | "line" | "area";
export type ChartGridlineDensity = "none" | "low" | "high";

function escapeXml(value: string): string {
  return value.replace(/[&<>"']/g, (character) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&apos;",
  })[character] ?? character);
}

function validViewport(viewport: ChartViewport): boolean {
  return Number.isSafeInteger(viewport.width)
    && Number.isSafeInteger(viewport.height)
    && viewport.width >= 240
    && viewport.width <= 4096
    && viewport.height >= 120
    && viewport.height <= 4096;
}

/**
 * Produces a deterministic SVG candlestick surface from an immutable chart
 * snapshot. The output contains no external URLs or executable markup and is
 * suitable for insertion into a trusted panel element.
 */
export function renderChartSvg(snapshot: ChartSeriesSnapshot, viewport: ChartViewport, mode: ChartRenderMode = "candles", view?: ChartViewWindow, gridlineDensity: ChartGridlineDensity = "low"): string {
  if (!validViewport(viewport)) throw new Error("chart viewport is outside bounds");
  const { width, height } = viewport;
  const padding = { left: 42, right: 12, top: 12, bottom: 24 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  // Provider/UI boundaries are defensive: malformed snapshots are omitted
  // before any scale calculation so NaN/Infinity cannot enter SVG geometry.
  const allCandles = snapshot.candles.filter(validCandle);
  if (allCandles.length === 0) {
    const message = snapshot.requiresRecovery ? "Recovering candle stream…" : "Awaiting candle stream";
    return `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="Candlestick chart ${message}"><rect width="${width}" height="${height}" fill="transparent"/><text x="${width / 2}" y="${height / 2}" text-anchor="middle" class="chart-empty">${message}</text></svg>`;
  }
  const requestedStart = view ? Math.max(0, Math.min(allCandles.length - 1, Math.floor(view.start))) : 0;
  const requestedEnd = view ? Math.max(requestedStart + 1, Math.min(allCandles.length, Math.floor(view.end))) : allCandles.length;
  const start = Math.max(requestedStart, requestedEnd - MAX_RENDER_CANDLES);
  const end = requestedEnd;
  const candles = allCandles.slice(start, end);
  const hasVolume = candles.some((candle) => (candle.volumeTicks ?? 0) > 0);
  const hasMetricPane = (snapshot.metrics?.length ?? 0) > 0;
  const volumePaneHeight = hasVolume ? 32 : 0;
  const metricPaneHeight = hasMetricPane ? 36 : 0;
  const pricePlotHeight = Math.max(40, plotHeight - volumePaneHeight - metricPaneHeight);
  const low = Math.min(...candles.map((candle) => candle.lowTicks));
  const high = Math.max(...candles.map((candle) => candle.highTicks));
  const range = Math.max(1, high - low);
  const candleWidth = Math.max(2, Math.min(18, plotWidth / candles.length * 0.7));
  const xFor = (index: number): number => padding.left + (index + 0.5) * plotWidth / candles.length;
  const yFor = (price: number): number => padding.top + (high - price) / range * pricePlotHeight;
  const shapes: string[] = [];
  candles.forEach((candle, index) => {
    const x = xFor(index);
    const openY = yFor(candle.openTicks);
    const closeY = yFor(candle.closeTicks);
    const bodyY = Math.min(openY, closeY);
    const bodyHeight = Math.max(1, Math.abs(openY - closeY));
    const isUp = candle.closeTicks >= candle.openTicks;
    const color = isUp ? "var(--positive)" : "var(--negative)";
    const direction = isUp ? "Up" : "Down";
    const label = `${direction} candle ${new Date(candle.timeMs).toISOString()} O ${candle.openTicks} H ${candle.highTicks} L ${candle.lowTicks} C ${candle.closeTicks}`;
    const glyph = isUp ? "▲" : "▼";
    const glyphY = isUp ? Math.max(padding.top + 8, bodyY - 3) : Math.min(padding.top + pricePlotHeight - 2, bodyY + bodyHeight + 9);
    const glyphMarkup = candleWidth >= 5 ? `<text x="${x.toFixed(2)}" y="${glyphY.toFixed(2)}" text-anchor="middle" class="candle-direction-glyph" aria-hidden="true">${glyph}</text>` : "";
    shapes.push(`<g data-candle-direction="${direction.toLowerCase()}" aria-label="${escapeXml(label)}"><title>${escapeXml(label)}</title><line x1="${x.toFixed(2)}" x2="${x.toFixed(2)}" y1="${yFor(candle.highTicks).toFixed(2)}" y2="${yFor(candle.lowTicks).toFixed(2)}" stroke="${color}"/><rect x="${(x - candleWidth / 2).toFixed(2)}" y="${bodyY.toFixed(2)}" width="${candleWidth.toFixed(2)}" height="${bodyHeight.toFixed(2)}" fill="${color}" rx="1"/>${glyphMarkup}</g>`);
  });
  const barWidth = Math.max(4, Math.min(12, candleWidth * 0.8));
  const barShapes = candles.map((candle, index) => {
    const x = xFor(index);
    const up = candle.closeTicks >= candle.openTicks;
    const color = up ? "var(--positive)" : "var(--negative)";
    const label = `${up ? "Up" : "Down"} OHLC bar ${new Date(candle.timeMs).toISOString()} O ${candle.openTicks} H ${candle.highTicks} L ${candle.lowTicks} C ${candle.closeTicks}`;
    const glyph = up ? "▲" : "▼";
    const glyphY = up ? Math.max(padding.top + 8, yFor(candle.highTicks) - 3) : Math.min(padding.top + pricePlotHeight - 2, yFor(candle.lowTicks) + 9);
    const glyphMarkup = candleWidth >= 5 ? `<text x="${x.toFixed(2)}" y="${glyphY.toFixed(2)}" text-anchor="middle" class="candle-direction-glyph" aria-hidden="true">${glyph}</text>` : "";
    return `<g data-bar-direction="${up ? "up" : "down"}" aria-label="${escapeXml(label)}"><title>${escapeXml(label)}</title><line x1="${x.toFixed(2)}" x2="${x.toFixed(2)}" y1="${yFor(candle.highTicks).toFixed(2)}" y2="${yFor(candle.lowTicks).toFixed(2)}" stroke="${color}"/><line x1="${(x - barWidth).toFixed(2)}" x2="${x.toFixed(2)}" y1="${yFor(candle.openTicks).toFixed(2)}" y2="${yFor(candle.openTicks).toFixed(2)}" stroke="${color}"/><line x1="${x.toFixed(2)}" x2="${(x + barWidth).toFixed(2)}" y1="${yFor(candle.closeTicks).toFixed(2)}" y2="${yFor(candle.closeTicks).toFixed(2)}" stroke="${color}"/>${glyphMarkup}</g>`;
  }).join("");
  const closePoints = candles.map((candle, index) => `${xFor(index).toFixed(2)},${yFor(candle.closeTicks).toFixed(2)}`).join(" ");
  const lineShape = `<polyline points="${closePoints}" fill="none" stroke="var(--accent)" stroke-width="2" data-chart-mode="line"/>`;
  const areaShape = `<polygon points="${padding.left},${(padding.top + pricePlotHeight).toFixed(2)} ${closePoints} ${(width - padding.right)},${(padding.top + pricePlotHeight).toFixed(2)}" fill="var(--accent)" opacity="0.16" data-chart-mode="area"/><polyline points="${closePoints}" fill="none" stroke="var(--accent)" stroke-width="2" data-chart-mode="area"/>`;
  const priceShapes = mode === "line" ? lineShape : mode === "area" ? areaShape : mode === "bars" ? barShapes : shapes.join("");
  const gridCount = gridlineDensity === "high" ? 8 : gridlineDensity === "low" ? 4 : 0;
  const gridShapes = Array.from({ length: gridCount }, (_, index) => {
    const fraction = (index + 1) / (gridCount + 1);
    const y = padding.top + fraction * pricePlotHeight;
    return `<line x1="${padding.left}" x2="${width - padding.right}" y1="${y.toFixed(2)}" y2="${y.toFixed(2)}" class="chart-gridline" aria-hidden="true"/>`;
  }).join("");
  const markerShapes = snapshot.news.map((marker) => {
    const index = candles.findIndex((candle) => candle.timeMs >= marker.timeMs);
    if (index < 0) return "";
    return `<circle cx="${xFor(index).toFixed(2)}" cy="${padding.top + 5}" r="3" fill="var(--warning)" data-news-marker="${escapeXml(marker.id)}" aria-label="${escapeXml(marker.title)}"><title>${escapeXml(marker.title)}</title></circle>`;
  }).join("");
  const strategyShapes = snapshot.strategies.map((marker) => {
    const index = candles.findIndex((candle) => candle.timeMs >= marker.timeMs);
    if (index < 0) return "";
    const x = xFor(index);
    const y = marker.kind === "entry" ? padding.top + pricePlotHeight - 8 : padding.top + 18;
    const color = marker.kind === "entry" ? "var(--positive)" : marker.kind === "exit" ? "var(--negative)" : "var(--accent)";
    const label = escapeXml(marker.label);
    return `<path d="M ${(x - 5).toFixed(2)} ${y.toFixed(2)} L ${(x + 5).toFixed(2)} ${y.toFixed(2)} L ${x.toFixed(2)} ${(y + (marker.kind === "entry" ? 7 : -7)).toFixed(2)} Z" fill="${color}" aria-label="${label}"/><title>${label}</title>`;
  }).join("");
  const metricPoints = (snapshot.metrics ?? []).map((metric) => {
    if (!Number.isFinite(metric.score) || !validMarkerTime(metric.timeMs)) return "";
    const index = candles.findIndex((candle) => candle.timeMs >= metric.timeMs);
    if (index < 0) return "";
    const x = xFor(index);
    const normalized = Math.max(-1, Math.min(1, metric.score));
    const metricTop = padding.top + pricePlotHeight + volumePaneHeight;
    const y = metricTop + (1 - (normalized + 1) / 2) * Math.max(1, metricPaneHeight - 8);
    return `${x.toFixed(2)},${y.toFixed(2)}`;
  }).filter(Boolean).join(" ");
  const metricShape = metricPoints
    ? `<polyline points="${metricPoints}" fill="none" stroke="var(--accent)" stroke-width="1.5" opacity="0.9"/><title>Metric overlay</title>`
    : "";
  const drawingShapes = snapshot.drawings.map((drawing) => {
    if (!Number.isFinite(drawing.startPriceTicks) || !validMarkerTime(drawing.startTimeMs)) return "";
    const startIndex = candles.findIndex((candle) => candle.timeMs >= drawing.startTimeMs);
    if (startIndex < 0) return "";
    const color = escapeXml(drawing.color);
    if (drawing.kind === "horizontal") {
      const y = yFor(drawing.startPriceTicks);
      return `<line x1="${padding.left}" x2="${width - padding.right}" y1="${y.toFixed(2)}" y2="${y.toFixed(2)}" stroke="${color}" stroke-dasharray="5 4" data-drawing-id="${escapeXml(drawing.id)}"><title>${escapeXml(drawing.label ?? "Horizontal level")}</title></line>`;
    }
    if (drawing.endTimeMs === undefined || drawing.endPriceTicks === undefined) return "";
    const endIndex = candles.findIndex((candle) => candle.timeMs >= drawing.endTimeMs!);
    if (endIndex < 0) return "";
    return `<line x1="${xFor(startIndex).toFixed(2)}" x2="${xFor(endIndex).toFixed(2)}" y1="${yFor(drawing.startPriceTicks).toFixed(2)}" y2="${yFor(drawing.endPriceTicks).toFixed(2)}" stroke="${color}" data-drawing-id="${escapeXml(drawing.id)}"><title>${escapeXml(drawing.label ?? "Trendline")}</title></line>`;
  }).join("");
  const maxVolume = Math.max(1, ...candles.map((candle) => candle.volumeTicks ?? 0));
  const volumeShapes = hasVolume
    ? candles.map((candle, index) => {
      const volume = candle.volumeTicks ?? 0;
      const barHeight = volume / maxVolume * Math.max(1, volumePaneHeight - 8);
      const x = xFor(index);
      const y = padding.top + pricePlotHeight + volumePaneHeight - barHeight;
      const color = candle.closeTicks >= candle.openTicks ? "var(--positive)" : "var(--negative)";
      return `<rect x="${(x - candleWidth / 2).toFixed(2)}" y="${y.toFixed(2)}" width="${candleWidth.toFixed(2)}" height="${barHeight.toFixed(2)}" fill="${color}" opacity="0.45"/>`;
    }).join("")
    : "";
  const volumePane = hasVolume
    ? `<g aria-label="Volume pane"><line x1="${padding.left}" x2="${width - padding.right}" y1="${(padding.top + pricePlotHeight).toFixed(2)}" y2="${(padding.top + pricePlotHeight).toFixed(2)}" stroke="var(--border-soft)"/>${volumeShapes}</g>`
    : "";
  const metricPane = hasMetricPane
    ? `<g aria-label="Metric pane"><line x1="${padding.left}" x2="${width - padding.right}" y1="${(padding.top + pricePlotHeight + volumePaneHeight + metricPaneHeight / 2).toFixed(2)}" y2="${(padding.top + pricePlotHeight + volumePaneHeight + metricPaneHeight / 2).toFixed(2)}" stroke="var(--border-soft)"/><text x="${padding.left - 4}" y="${(padding.top + pricePlotHeight + volumePaneHeight + 10).toFixed(2)}" text-anchor="end" class="chart-axis">+1</text><text x="${padding.left - 4}" y="${(padding.top + pricePlotHeight + volumePaneHeight + metricPaneHeight - 2).toFixed(2)}" text-anchor="end" class="chart-axis">-1</text></g>`
    : "";
  return `<svg class="chart-svg" viewBox="0 0 ${width} ${height}" role="img" aria-label="${mode} chart"><g>${gridShapes}${priceShapes}${markerShapes}${strategyShapes}${metricShape}${drawingShapes}</g>${volumePane}${metricPane}<text x="${padding.left}" y="${height - 6}" class="chart-axis">${escapeXml(new Date(candles[0].timeMs).toISOString())}</text><text x="${width - padding.right}" y="${height - 6}" text-anchor="end" class="chart-axis">${escapeXml(new Date(candles[candles.length - 1].timeMs).toISOString())}</text></svg>`;
}

const MAX_CANDLES_PER_BATCH = 4096;
const MAX_RENDER_CANDLES = 4096;
const MAX_MARKERS = 4096;
const MAX_DRAWINGS = 256;

function validCandle(candle: Candle): boolean {
  return Number.isSafeInteger(candle.timeMs)
    && candle.timeMs >= 0
    && Number.isSafeInteger(candle.sequence)
    && candle.sequence >= 0
    && [candle.openTicks, candle.highTicks, candle.lowTicks, candle.closeTicks]
      .every((value) => Number.isSafeInteger(value) && value > 0)
    && candle.highTicks >= Math.max(candle.openTicks, candle.closeTicks)
    && candle.lowTicks <= Math.min(candle.openTicks, candle.closeTicks)
    && (candle.volumeTicks === undefined || (Number.isSafeInteger(candle.volumeTicks) && candle.volumeTicks >= 0));
}

function validMarkerTime(timeMs: number): boolean {
  return Number.isSafeInteger(timeMs) && timeMs >= 0;
}

/** Bounded OHLCV storage with sequence-aware correction semantics. */
export class CandleSeries {
  readonly #maxCandles: number;
  readonly #candles = new Map<number, Candle>();
  #lastSequence = -1;
  #droppedBatches = 0;
  #requiresRecovery = false;

  constructor(maxCandles = 20_000) {
    if (!Number.isSafeInteger(maxCandles) || maxCandles < 1) throw new Error("maxCandles must be positive");
    this.#maxCandles = maxCandles;
  }

  apply(batch: CandleBatch): BatchResult {
    if (!Number.isSafeInteger(batch.sequence) || batch.sequence < 0 || batch.candles.length > MAX_CANDLES_PER_BATCH) {
      this.#droppedBatches += 1;
      return { accepted: 0, replaced: 0, rejected: batch.candles.length, dropped: true };
    }
    if (this.#lastSequence >= 0 && batch.sequence > this.#lastSequence + 1) {
      this.#droppedBatches += 1;
      this.#requiresRecovery = true;
      return { accepted: 0, replaced: 0, rejected: batch.candles.length, dropped: true };
    }
    if (this.#requiresRecovery) {
      this.#droppedBatches += 1;
      return { accepted: 0, replaced: 0, rejected: batch.candles.length, dropped: true };
    }
    let accepted = 0;
    let replaced = 0;
    let rejected = 0;
    for (const candle of batch.candles) {
      if (!validCandle(candle) || candle.sequence > batch.sequence) {
        rejected += 1;
        continue;
      }
      const previous = this.#candles.get(candle.timeMs);
      if (previous && previous.sequence > candle.sequence) {
        rejected += 1;
        continue;
      }
      this.#candles.set(candle.timeMs, candle);
      if (previous) replaced += 1;
      else accepted += 1;
      this.#lastSequence = Math.max(this.#lastSequence, candle.sequence);
    }
    this.#lastSequence = Math.max(this.#lastSequence, batch.sequence);
    while (this.#candles.size > this.#maxCandles) {
      const oldest = this.#candles.keys().next().value;
      if (oldest === undefined) break;
      this.#candles.delete(oldest);
    }
    return { accepted, replaced, rejected, dropped: false };
  }

  snapshot(): readonly Candle[] {
    return Object.freeze([...this.#candles.values()].sort((left, right) => left.timeMs - right.timeMs));
  }

  get lastSequence(): number { return this.#lastSequence; }
  get droppedBatches(): number { return this.#droppedBatches; }
  get requiresRecovery(): boolean { return this.#requiresRecovery; }

  /** Installs a provider snapshot boundary and permits subsequent deltas. */
  recoverAfterSnapshot(sequence: number, candles: readonly Candle[] = []): void {
    if (!Number.isSafeInteger(sequence) || sequence < -1 || candles.length > MAX_CANDLES_PER_BATCH || (sequence < 0 && candles.length > 0)) {
      throw new Error("invalid chart recovery snapshot");
    }
    this.#candles.clear();
    this.#lastSequence = sequence;
    this.#requiresRecovery = false;
    for (const candle of candles) {
      if (!validCandle(candle) || candle.sequence > sequence) throw new Error("invalid chart recovery candle");
      this.#candles.set(candle.timeMs, candle);
    }
  }
}

/** Complete renderer-neutral model for one instrument/timeframe chart. */
export class MarketChartModel {
  readonly candles: CandleSeries;
  readonly #news = new Map<string, NewsMarker>();
  readonly #strategies = new Map<string, StrategyMarker>();
  readonly #metrics = new Map<string, MetricOverlay>();
  readonly #drawings = new Map<string, ChartDrawing>();

  constructor(maxCandles = 20_000) {
    this.candles = new CandleSeries(maxCandles);
  }

  applyCandles(batch: CandleBatch): BatchResult { return this.candles.apply(batch); }

  upsertNews(marker: NewsMarker): void {
    if (!marker.id.trim() || !validMarkerTime(marker.timeMs) || !Number.isFinite(marker.relevance)) return;
    this.#news.set(marker.id, marker);
    this.trimMarkers(this.#news);
  }

  /** Replaces the bounded news-marker projection after a symbol/snapshot change. */
  replaceNews(markers: readonly NewsMarker[]): void {
    this.#news.clear();
    for (const marker of markers) this.upsertNews(marker);
  }

  upsertStrategy(marker: StrategyMarker): void {
    if (!marker.id.trim() || !validMarkerTime(marker.timeMs) || !marker.label.trim()) return;
    this.#strategies.set(marker.id, marker);
    this.trimMarkers(this.#strategies);
  }

  upsertMetric(overlay: MetricOverlay): void {
    if (!overlay.id.trim() || !validMarkerTime(overlay.timeMs) || !Number.isFinite(overlay.score)) return;
    this.#metrics.set(overlay.id, { ...overlay, score: Math.max(-1, Math.min(1, overlay.score)) });
    this.trimMarkers(this.#metrics);
  }

  upsertDrawing(drawing: ChartDrawing): void {
    if (!drawing.id.trim() || drawing.id.length > 128 || !validMarkerTime(drawing.startTimeMs)
      || !Number.isSafeInteger(drawing.startPriceTicks) || drawing.startPriceTicks <= 0
      || !/^#[0-9a-f]{6}$/i.test(drawing.color)) return;
    if (drawing.kind === "trendline" && (!validMarkerTime(drawing.endTimeMs ?? -1)
      || !Number.isSafeInteger(drawing.endPriceTicks) || (drawing.endPriceTicks ?? 0) <= 0)) return;
    this.#drawings.set(drawing.id, Object.freeze({ ...drawing }));
    while (this.#drawings.size > MAX_DRAWINGS) this.#drawings.delete(this.#drawings.keys().next().value!);
  }

  replaceDrawings(drawings: readonly ChartDrawing[]): void {
    this.#drawings.clear();
    for (const drawing of drawings.slice(0, MAX_DRAWINGS)) this.upsertDrawing(drawing);
  }

  removeDrawing(id: string): void { this.#drawings.delete(id); }

  removeMarker(id: string): void {
    this.#news.delete(id);
    this.#strategies.delete(id);
    this.#metrics.delete(id);
  }

  snapshot(): ChartSeriesSnapshot {
    return Object.freeze({
      candles: this.candles.snapshot(),
      news: Object.freeze([...this.#news.values()].sort((left, right) => left.timeMs - right.timeMs)),
      strategies: Object.freeze([...this.#strategies.values()].sort((left, right) => left.timeMs - right.timeMs)),
      metrics: Object.freeze([...this.#metrics.values()].sort((left, right) => left.timeMs - right.timeMs)),
      drawings: Object.freeze([...this.#drawings.values()].sort((left, right) => left.id.localeCompare(right.id))),
      lastSequence: this.candles.lastSequence,
      droppedBatches: this.candles.droppedBatches,
      requiresRecovery: this.candles.requiresRecovery,
    });
  }

  private trimMarkers<T extends { readonly timeMs: number }>(markers: Map<string, T>): void {
    while (markers.size > MAX_MARKERS) {
      const oldest = [...markers.entries()].sort((left, right) => left[1].timeMs - right[1].timeMs)[0]?.[0];
      if (oldest === undefined) break;
      markers.delete(oldest);
    }
  }
}
