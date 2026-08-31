# Volatility-scaled trend starter strategy

`cross_asset.volatility_scaled_trend.v1` combines normalized EMA trend,
normalized ATR, and relative spread. It waits for fresh, sufficiently warmed
evidence, refuses entries through a wide-spread guard, and scales an absolute
target quantity down when observed volatility exceeds its configured budget.
Neutral trend emits `Close`; incomplete, duplicate, stale, low-confidence, or
illiquid evidence emits an explicit `NoAction` with a stable rationale code.
Its manifest declares `missing_evidence: no_action`; legacy packages retain the
typed `skip` default and are not invoked with incomplete snapshots.

The strategy produces `StrategyProposal` values only. It cannot call a broker,
and manual/hybrid/autonomous consumers all use the normal coordinator, risk,
preview, and execution boundaries.

The Rust package manifest is discoverable and duplicate-checked. The current
runtime has no arbitrary native-library loader, so this immutable package is
instantiated through the small built-in adapter in `insider-runtime`. Set
`strategy.starter_enabled = true` in CFG to admit it to the live strategy
catalog. The default remains `false`; merely installing the package cannot
initiate autonomous behavior.
