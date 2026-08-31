#!/usr/bin/env python3
"""Verify that the operator runbook retains mandatory safety procedures."""

from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
RUNBOOK = ROOT / "docs" / "runbooks" / "operator-guide.md"
INCIDENT = ROOT / "docs" / "runbooks" / "incident-template.md"
RELEASE = ROOT / "docs" / "runbooks" / "release-certification.md"


def main() -> int:
    text = RUNBOOK.read_text(encoding="utf-8")
    required = (
        "## 1. Install and validate",
        "## 2. Create a paper configuration",
        "## 3. Start and verify paper operation",
        "## 4. Halt, reduce-only, and outage response",
        "## 5. Restart and reconciliation",
        "## 6. Backup and restore",
        "## 7. Paper-to-live change control",
        "## 8. Evidence retention",
        "## 9. Credential rotation",
        "cargo build --locked -p insider-runtime -p insider-terminal",
        "broker.mode = \"paper\"",
        "data/insidertrader.cfg already exists",
        "--journal data/runtime.journal",
        "risk state is `RUNNING`",
        "reconciliation",
        "Live trading remains disabled",
        "Never test revocation by submitting a live order",
        "insider-terminal --socket data/runtime.sock",
    )
    missing = [marker for marker in required if marker not in text]
    if missing:
        raise SystemExit("operator runbook missing: " + ", ".join(missing))
    incident = INCIDENT.read_text(encoding="utf-8")
    incident_markers = ("## Incident identity", "## Detection and immediate safety action", "## Diagnosis", "## Recovery and reconciliation", "## Verification and closure", "journal", "reconciliation")
    missing_incident = [marker for marker in incident_markers if marker not in incident]
    if missing_incident:
        raise SystemExit("incident template missing: " + ", ".join(missing_incident))
    release = RELEASE.read_text(encoding="utf-8")
    release_markers = (
        "# InsiderTrader release certification runbook",
        "## 1. Freeze the release candidate",
        "## 2. Seven-day paper soak",
        "## 3. Disaster drills",
        "## 4. Broker and statement reconciliation",
        "## 5. Canary and approval record",
        "## 6. Evidence manifest",
        "cargo build --locked -p insider-runtime -p insider-terminal",
        "REDUCE_ONLY",
        "signed G15 YAML manifest",
    )
    missing_release = [marker for marker in release_markers if marker not in release]
    if missing_release:
        raise SystemExit("release certification runbook missing: " + ", ".join(missing_release))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
