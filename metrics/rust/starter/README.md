# Deterministic starter metrics

This package set supplies three conservative, price-scale-independent inputs:

- `trend.ema.normalized.v1`: bounded fast/slow EMA distance over canonical bars.
- `volatility.atr.normalized.v1`: average true range divided by bar close.
- `liquidity.spread.v1`: bid/ask spread divided by midpoint.

The runtime registers the Rust implementations in its metric catalog. The
manifests are also traversed by the same bounded, duplicate-rejecting package
discovery used for every metric package. Rust manifest `entrypoint` loading is
not dynamic yet; the runtime therefore uses an explicit built-in adapter for
these immutable IDs and verifies the same declared input contract in the SDK.

EMA and ATR retain bounded state per canonical `InstrumentId`. A repeated latest
`bar_index` replaces the prior observation (correction semantics), while an
older bar is rejected. Their public batch reference functions are used to prove
incremental/replay parity.
