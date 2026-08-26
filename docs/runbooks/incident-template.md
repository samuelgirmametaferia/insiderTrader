# Incident record: `<short-condition>`

Do not edit or delete journal segments while investigating. Preserve the original
record with restricted permissions and redact secrets before distribution.

## Incident identity

- Incident ID:
- Opened at (UTC):
- Operator / approver:
- Deployment mode (`paper` / certified `live`):
- Affected account, instruments, providers, and services:
- Primary TraceId(s):

## Detection and immediate safety action

- Detection source and alert ID:
- Last known healthy timestamp:
- Risk state before action:
- Action taken (`REDUCE_ONLY`, `CANCEL_ONLY`, `HALTED`, or none):
- Authorization identity and event ID:
- Working orders cancelled or confirmed unknown:

## Diagnosis

- Market/news/LLM/provider health snapshots:
- Supervisor status and quarantined workers:
- Journal segment and projection integrity results:
- Clock/session/data-freshness findings:
- Suspected cause (facts only; link evidence):

## Recovery and reconciliation

1. Keep new exposure disabled until the cause is contained.
2. Restart only the affected component using the operator runbook; do not blind-retry
   unknown orders.
3. Record journal replay and broker reconciliation results:
   - cash difference:
   - position difference:
   - fill/order difference:
   - unexplained items and disposition:
4. Resume `RUNNING` only after named approval and an explicit reconciliation event.

## Verification and closure

- Regression/fault test or replay fixture added:
- Final risk, broker, reconciliation, and provider states:
- User/account-owner communication sent:
- Evidence paths and SHA-256 hashes:
- Closed at (UTC):
- Closure approver:
- Follow-up owner and due date:
