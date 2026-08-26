# Changelog

All notable changes to InsiderTrader are documented here. The project follows
semantic-versioned release labels; unreleased work remains in the repository
until it has deterministic tests and the required evidence.

## [0.1.0] - 2026-08-26

### Added

- Rust workspace for canonical market data, features, metrics, strategies,
  portfolio/risk, execution, paper simulation, reconciliation, replay, news,
  context retrieval, LLM providers, autonomy, telemetry, and experiment/model
  registries.
- Authenticated, bounded Unix IPC bridge with idempotent commands, optimistic
  concurrency, owner-only sockets, connection request limits, and I/O deadlines.
- Tauri desktop workstation with dockable workspaces, charts, news, strategy and
  metric inspection, risk/order workflows, CFG generation, alerts, replay and
  research surfaces.
- CFG-first configuration with bounded parsing, atomic reload, secret-boundary
  rules, provider adapters, and a safe paper startup preflight.
- Pinned Rust/Python/Node/npm toolchains, reproducible lockfiles, GitHub CI,
  Dependabot updates, contributor/security documentation, and deterministic
  Rust/Python/UI verification.

### Safety boundary

Live trading is disabled until the G07/G15 release-certification evidence in
`docs/runbooks/release-certification.md` exists for the configured asset class.
The repository does not claim that paper tests substitute for a broker canary,
seven-day soak, disaster drills, reconciliation, or operational approvals.
