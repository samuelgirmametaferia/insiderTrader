# Security policy

InsiderTrader can submit broker orders. Treat every deployment as safety-critical:
paper mode is the default, live mode requires the certification gates in `AGENTS.md`,
and deterministic risk/execution/reconciliation services—not an LLM or the UI—are the
source of truth for order state.

## Reporting a vulnerability

Do not open a public issue for a vulnerability that could expose credentials, alter
journal history, bypass risk, submit an unauthorized order, or corrupt reconciliation.
Use the repository host's private security-advisory channel when available. Include:

- affected commit or release and deployment mode;
- reproducible steps or a minimal offline fixture;
- expected versus observed authorization, journal, or broker behavior;
- logs/traces with credentials and personal data removed;
- whether the issue is currently exploitable in paper or live mode.

If no private channel is available, contact the maintainers privately before disclosure.
Allow reasonable time for triage, mitigation, and a coordinated release.

## Supported safety expectations

Reports are especially important for:

- secret leakage through configuration, logs, IPC, UI persistence, or provider errors;
- malformed or oversized input that bypasses bounded parsing;
- duplicate, replayed, or ambiguous order/fill events;
- retry paths that change client order identity or resend unknown acknowledgements;
- point-in-time replay leakage;
- LLM output becoming an action without schema and policy validation;
- filesystem discovery, symlink, or worker-isolation escapes.

Never test against a live broker or another person's account without written
authorization. Use the paper exchange simulator and checked-in fixtures for reports.
