## Summary

Describe the user-visible or runtime behavior changed and the owning gate from `PLAN.md`.

## Safety and configuration

- [ ] No broker, risk, reconciliation, or journal authority moved into the terminal/LLM.
- [ ] Operationally changeable values use `.cfg` (with validation and bounded defaults).
- [ ] Secrets are absent from source, fixtures, logs, and UI persistence.
- [ ] Replay, restart, idempotency, and failure behavior were considered.

## Verification

- [ ] `./scripts/check.sh` passes.
- [ ] Component tests include invalid/boundary inputs and failure behavior.
- [ ] Schema/generated artifacts and compatibility fixtures are updated if needed.
- [ ] `PLAN.md` records objective evidence and any remaining limitation.
- [ ] Live-trading certification is not claimed without the required external evidence.

## Operator impact

List new/changed CFG keys, migration or rollback behavior, runbook updates, and any
manual verification still required. Include screenshots or traces for UI changes when
they materially clarify the behavior.
