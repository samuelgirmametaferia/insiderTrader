# InsiderTrader operator runbook

This runbook is for the Linux desktop/paper deployment described by `PLAN.md`.
Live trading remains disabled until the applicable G07/G15 certification evidence
exists. Commands below use deployment-owned paths; never place credentials in the
configuration file or journal.

## 1. Install and validate

1. Install the pinned Rust toolchain and the Node/npm versions declared in
   `rust-toolchain.toml`, `ui/.node-version`, and `ui/package.json`.
   On Debian/Ubuntu reference hosts, install the Tauri WebView dependencies before
   compiling the desktop shell:

   ```bash
   sudo apt-get install --yes protobuf-compiler libwebkit2gtk-4.1-dev \
     libjavascriptcoregtk-4.1-dev libgtk-3-dev libayatana-appindicator3-dev \
     librsvg2-dev libxdo-dev libssl-dev
   ```

   Record the package-manager output in release evidence; do not substitute a web-only
   build for the desktop compile.
2. Hydrate dependencies from lockfiles, then run the repository gate:

   ```bash
   npm ci --prefix ui
   ./scripts/check.sh
   ```

3. Do not proceed if any check fails. Record the exact revision and gate log in the
   release evidence directory.

## 2. Create a paper configuration

```bash
mkdir -p data
if test -e data/insidertrader.cfg; then
  echo "data/insidertrader.cfg already exists; back it up and change it through review" >&2
  exit 1
fi
install -m 0600 config/example.cfg data/insidertrader.cfg
```

Edit only deployment-approved values, or use the UI Configuration panel to merge
validated values. Keep `broker.mode = "paper"`; API keys belong in the secret
manager/environment. Validate the file through the desktop bridge before starting
an autonomous service. The parser enforces the 1 MiB file bound, unique keys,
typed scalars, and finite numeric values.

Before startup, verify the deployment-owned paths are regular files or new paths;
never point the journal or socket at a shared system location:

```bash
test -f data/insidertrader.cfg
test ! -e data/runtime.sock || test -S data/runtime.sock
test ! -e data/runtime.journal || test -f data/runtime.journal
```

## 3. Start and verify paper operation

```bash
insider-desktop-bridge serve \
  --config data/insidertrader.cfg \
  --journal data/runtime.journal \
  --socket data/runtime.sock \
  --account 1
```

Confirm in System Health that the journal, market feed, broker session, risk
engine, and reconciliation are healthy. Confirm the risk state is `RUNNING`, the
broker is Paper, and the selected strategy/model/prompt versions are immutable
IDs. Submit a small manual order only after preview, risk approval, explicit
`CONFIRM`, and fill reconciliation are visible. A UI crash must not be treated as
an execution-state transition.

## 4. Halt, reduce-only, and outage response

On unexpected behavior, stop opening exposure first: set risk to `REDUCE_ONLY`
or `CANCEL_ONLY` using an authorized command, then cancel working orders. For a
provider outage, leave deterministic metrics/strategies running only if their
freshness and health gates remain green; otherwise quarantine the affected worker.
LLM/news outages must never be bypassed by manually treating stale data as fresh.
Record the incident trace ID, risk transition event, provider health snapshot, and
last reconciled broker state before restarting anything.

## 5. Restart and reconciliation

1. Stop the UI independently; verify the execution service remains in its configured
   deployment mode.
2. For an engine restart, preserve the journal and use the same configuration and
   account identity. Never delete or edit journal segments.
3. Wait for journal replay and broker reconciliation to complete. Investigate every
   `Unknown` order, fill anomaly, position mismatch, or sequence gap before retrying.
4. Resume `RUNNING` only with named authorization after cash, positions, fills, and
   working orders match the broker statement. Keep the system halted/reduce-only
   when reconciliation is incomplete.

## 6. Backup and restore

Use the Backup & Restore panel or the authenticated backup command to create a
consistent projection backup. Verify its manifest record count, newest sequence,
and SHA-256 before copying it to encrypted operator storage. Restore into a new
destination only; never overwrite the authoritative journal. Run the read-only
integrity scan and compare the restored projection cursor to the source before
pointing a recovery instance at it.

## 7. Paper-to-live change control

Live mode requires a reviewed configuration change, secret-manager readiness,
broker account permission proof, asset-class certification, shadow/paper evidence,
minimum-size canary approval, kill-switch drill, statement reconciliation, and
risk/operations/account-owner sign-off. Change `broker.mode` only during an
approved maintenance window. If any prerequisite is absent, remain in Paper.

## 8. Evidence retention

Retain the configuration hash, schema hash, binary hash, journal/projection backup
manifest, test/gate logs, reconciliation report, approvals, and incident trace IDs
for the retention period defined by operations and compliance. Redact secrets and
provider tokens before sharing logs.

## 9. Credential rotation

1. Set the affected provider/account to `REDUCE_ONLY` (or `CANCEL_ONLY` if the
   provider cannot be trusted) and record the authorization event and TraceId.
2. Create a new credential in the deployment secret manager. Do not write the
   secret, token, or key into `.cfg`, UI storage, journals, traces, or shell history.
3. Update only the secret reference/environment binding, restart the affected
   provider component, and confirm the supervisor reports authenticated/healthy.
4. Revoke the old credential and verify that an intentional old-credential probe is
   rejected. Never test revocation by submitting a live order.
5. Reconcile orders, fills, positions, and account permissions, then restore
   `RUNNING` only with named authorization. Retain rotation timestamps, provider
   health snapshots, revocation evidence, and reconciliation hashes.
