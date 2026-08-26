import { readFile } from "node:fs/promises";
import { test } from "node:test";
import assert from "node:assert/strict";
import { transform } from "esbuild";

const source = await readFile(new URL("../src/charts/market-chart.ts", import.meta.url), "utf8");
const compiled = await transform(source, { loader: "ts", format: "esm", target: "es2022" });
const moduleUrl = `data:text/javascript;base64,${Buffer.from(compiled.code).toString("base64")}`;
const { renderChartSvg, resampleCandles } = await import(moduleUrl);

const valid = (timeMs, sequence, openTicks, closeTicks) => ({
  timeMs, sequence, openTicks, closeTicks,
  highTicks: Math.max(openTicks, closeTicks) + 2,
  lowTicks: Math.min(openTicks, closeTicks) - 1,
  volumeTicks: 10,
});

test("renderer filters malformed candles and emits finite, bounded SVG", () => {
  const candles = [valid(1_700_000_000_000, 0, 100, 105), { ...valid(1_700_000_060_000, 1, 105, 110), highTicks: Number.NaN }, valid(1_700_000_120_000, 2, 110, 106)];
  const svg = renderChartSvg({ candles, news: [], strategies: [], metrics: [], drawings: [], lastSequence: 2, droppedBatches: 0, requiresRecovery: false }, { width: 640, height: 220 });
  assert.equal((svg.match(/data-candle-direction=/g) ?? []).length, 2);
  assert.match(svg, /▲/);
  assert.match(svg, /▼/);
  assert.doesNotMatch(svg, /NaN|Infinity/);
  assert.ok(svg.length < 100_000);
});

test("resampling validates before aggregation and freezes the one-minute result", () => {
  const candles = [valid(1_700_000_000_000, 0, 100, 105), { ...valid(1_700_000_060_000, 1, 105, 110), lowTicks: Number.POSITIVE_INFINITY }];
  const result = resampleCandles(candles, "1m");
  assert.equal(result.length, 1);
  assert.ok(Object.isFrozen(result));
});

test("maximum chart window remains bounded", () => {
  const candles = Array.from({ length: 4096 }, (_, index) => valid(1_700_000_000_000 + index * 60_000, index, 100 + index, 101 + index));
  const started = performance.now();
  const svg = renderChartSvg({ candles, news: [], strategies: [], metrics: [], drawings: [], lastSequence: 4095, droppedBatches: 0, requiresRecovery: false }, { width: 640, height: 220 });
  const elapsed = performance.now() - started;
  assert.ok(svg.length < 2_000_000);
  // Keep this as a runaway-work guard, not a machine-specific FPS claim:
  // workstation frame-budget telemetry is exposed by the renderer itself.
  assert.ok(elapsed < 2_000, `maximum chart render took ${elapsed.toFixed(1)} ms`);
});
