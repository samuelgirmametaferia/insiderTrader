# Contributing to InsiderTrader

InsiderTrader is a deterministic trading system with a Tauri workstation. Contributions
must preserve the boundary that metrics and strategies propose actions while portfolio,
risk, execution, broker, and reconciliation services remain authoritative.

## Development setup

Install the pinned Rust toolchain from `rust-toolchain.toml`, Python version from
`.python-version`, and Node/npm versions from `ui/.node-version` and `ui/package.json`.
Then hydrate dependencies without changing lockfiles:

```bash
cargo test --workspace
python3 -m pytest
cd ui && npm ci && npm test && npm run check && npm run build
```

The complete required gate is:

```bash
./scripts/check.sh
```

Run it before opening a pull request. It checks formatting, Clippy, Rust and Python
tests, schema/dependency/license/security/documentation contracts, UI tests and build,
and the CFG/runbook requirements.

## Configuration and secrets

Runtime behavior that may change operationally belongs in a bounded `.cfg` file. Start
with `config/example.cfg` and use the Configuration panel or an atomic engine reload;
do not hard-code deployment values in Rust or TypeScript. Credentials are never stored
in `.cfg`, UI local storage, fixtures, or commits. Use the deployment secret boundary
and reference secrets by name.

Provider failures must degrade without blocking charts, deterministic strategies, risk,
or manual order entry. New provider integrations require deterministic offline fixtures.

## Change requirements

Every change must include:

1. Tests that fail before the fix and pass after it, with bounded inputs and failure
   behavior covered.
2. An update to `PLAN.md` describing objectively checkable evidence and any remaining
   limitation.
3. Schema changes with regenerated artifacts and compatibility tests.
4. Journal/event changes with replay and restart coverage.
5. UI changes with accessibility labels, keyboard behavior, persistence boundaries, and
   a workstation contract test where applicable.

Never bypass risk or reconciliation in a UI action, LLM tool, strategy, or test helper.
Do not add live provider calls to deterministic tests. Do not mark an acceptance gate
complete without the packaged evidence required by `PLAN.md` and `AGENTS.md`.

## Pull requests

Describe the runtime path changed, configuration keys added or changed, migration and
rollback behavior, and verification commands with their results. Keep commits focused;
do not include credentials, generated build directories, local journals, or unrelated
formatting churn. A reviewer must be able to reconstruct the decision and verify that
manual, hybrid, autonomous, and replay semantics remain aligned.
