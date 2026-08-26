# InsiderTrader release certification runbook

This runbook produces the external evidence required by `PLAN.md` G15. It must be
executed against a packaged Linux release candidate in an isolated paper account.
No gate is marked passed from a development server, an unpinned worktree, or an
operator assertion without the artifacts below.

## 1. Freeze the release candidate

1. Record the 40-character source revision, Rust/Node/npm versions, binary hashes,
   `Cargo.lock`, `ui/package-lock.json`, schema bundle hash, and deployment CFG hash.
2. Run `npm ci --prefix ui` and `./scripts/check.sh` from a clean checkout. Store the
   complete log and exit code under the release evidence directory.
3. Build the packaged desktop artifact and calculate SHA-256 hashes. Verify the
   hashes again after copying to the test host; a mismatch invalidates the RC.

## 2. Seven-day paper soak

Run the RC continuously for seven calendar days with market/news ingestion, metrics,
strategies, UI sessions, research jobs, and configured LLM workloads enabled. Capture
hourly snapshots of RSS, queue depth, journal sequence, provider health, supervisor
state, risk state, orders, fills, positions, and account values. The signed report must
show zero duplicate orders, unreconciled final positions, silent data gaps, journal
corruption, critical alerts, or unbounded resource growth. Preserve raw telemetry and
the final state hash; do not summarize away anomalies.

## 3. Disaster drills

Execute each drill independently, recording UTC start/end, injected fault, TraceIds,
expected fail-closed behavior, recovery time, final journal/projection hashes, and
reconciliation result:

- kill the UI while paper execution remains active;
- terminate the engine, reboot the host, and recover from a damaged journal tail;
- simulate disk-full warning, network partition, IBKR disconnect, and clock anomaly;
- disable market, news, and LLM providers; corrupt a rebuildable cache;
- rebuild the read model, trigger a risk halt, and revoke a test credential.

Any unexpected order, state mutation without a journal event, stale-data admission,
or recovery outside the 30-second target invalidates the RC and requires a new run.

## 4. Broker and statement reconciliation

1. Export the paper broker statement and the InsiderTrader ledger for the identical
   UTC interval.
2. Compare every order intent, broker order, fill, fee, cash balance, position,
   average cost, and working-order state. Use asset-specific tolerances only when
   documented before the run; unexplained differences are failures.
3. Obtain account-permission and capability evidence for each asset class intended
   for live use. Do not enable an asset class based only on a paper fixture.

## 5. Canary and approval record

Before any live canary, attach the RC manifest, soak report, drill report,
reconciliation hash, capability fixtures, kill-switch evidence, risk-owner approval,
operations approval, and account-owner approval. Start in shadow mode, then paper,
then the minimum permitted live size for one certified asset class. Reconcile the
statement before expanding the canary. A failed canary immediately enters
`REDUCE_ONLY`/`HALTED` according to the incident runbook.

## 6. Evidence manifest

The release evidence directory must contain the source/config/schema/artifact hashes,
all command logs, hourly soak snapshots, telemetry exports, drill records, broker
statement and ledger comparison, approvals, and the signed G15 YAML manifest. Each
file is immutable after signing. Missing, unsigned, or post-hoc edited evidence keeps
G15 pending.
