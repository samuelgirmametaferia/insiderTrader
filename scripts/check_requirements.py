#!/usr/bin/env python3
"""Validate the normative requirement traceability table."""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REQUIRED_FIELDS = {
    "requirement_id",
    "agents_section",
    "normative_text",
    "owner_gate",
    "verification_id",
    "status",
    "evidence",
}
GATE_PATTERN = re.compile(r"G(?:0[0-9]|1[0-5])\Z")
STATUS_VALUES = {"planned", "implemented", "verified", "blocked"}
VERIFICATION_PATTERN = re.compile(r"verify-[a-z0-9][a-z0-9-]*\Z")


def main() -> int:
    table = ROOT / "evidence" / "requirements.csv"
    with table.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if set(reader.fieldnames or ()) != REQUIRED_FIELDS:
            print("requirements.csv has an invalid header", file=sys.stderr)
            return 1
        seen: set[str] = set()
        acceptance_ids = {
            match.group(1)
            for match in re.finditer(r"^- \[[ x]\] (A\d{3}) ", (ROOT / "AGENTS.md").read_text(encoding="utf-8"), re.MULTILINE)
        }
        row_count = 0
        for line_number, row in enumerate(reader, start=2):
            requirement_id = row["requirement_id"].strip()
            if not requirement_id:
                print(f"blank requirement_id at line {line_number}", file=sys.stderr)
                return 1
            row_count += 1
            if requirement_id in seen:
                print(f"duplicate requirement {requirement_id} at line {line_number}", file=sys.stderr)
                return 1
            seen.add(requirement_id)
            if GATE_PATTERN.fullmatch(row["owner_gate"].strip()) is None:
                print(f"invalid owner gate at line {line_number}", file=sys.stderr)
                return 1
            if not row["verification_id"].strip():
                print(f"missing verification at line {line_number}", file=sys.stderr)
                return 1
            if VERIFICATION_PATTERN.fullmatch(row["verification_id"].strip()) is None:
                print(f"invalid verification_id at line {line_number}", file=sys.stderr)
                return 1
            if row["status"].strip() not in STATUS_VALUES:
                print(f"invalid status at line {line_number}", file=sys.stderr)
                return 1
            if not row["agents_section"].strip() or not row["normative_text"].strip():
                print(f"missing normative traceability at line {line_number}", file=sys.stderr)
                return 1
            if not row["evidence"].strip():
                print(f"missing evidence reference at line {line_number}", file=sys.stderr)
                return 1
            if requirement_id.startswith("ACCEPT-"):
                acceptance_id = requirement_id.removeprefix("ACCEPT-")
                # The matrix must never claim a checked Appendix-B item is merely
                # planned, and a verified/implemented item must point at an
                # executable or testable artifact rather than AGENTS.md itself.
                checked = bool(re.search(rf"^- \[x\] {re.escape(acceptance_id)} ", (ROOT / "AGENTS.md").read_text(encoding="utf-8"), re.MULTILINE | re.IGNORECASE))
                status = row["status"].strip()
                if checked and status not in {"implemented", "verified"}:
                    print(f"checked acceptance {acceptance_id} is still marked {status}", file=sys.stderr)
                    return 1
                if status in {"implemented", "verified"} and Path(row["evidence"].strip()).name == "AGENTS.md":
                    print(f"acceptance {acceptance_id} claims completion without executable evidence", file=sys.stderr)
                    return 1
                if status in {"implemented", "verified"} and row["normative_text"].strip() in {f"{acceptance_id} acceptance requirement", "acceptance requirement"}:
                    print(f"placeholder normative text for {acceptance_id}", file=sys.stderr)
                    return 1
            evidence_path = ROOT / row["evidence"].strip()
            if not evidence_path.exists():
                print(f"evidence path does not exist at line {line_number}: {row['evidence']}", file=sys.stderr)
                return 1
        missing_acceptance = {f"ACCEPT-{acceptance_id}" for acceptance_id in acceptance_ids} - seen
        if missing_acceptance:
            print("missing acceptance traceability: " + ", ".join(sorted(missing_acceptance)), file=sys.stderr)
            return 1
        # Every acceptance checkbox must have exactly one matrix row; duplicate
        # Appendix-B IDs are rejected above, and stale matrix rows are rejected
        # here so removed requirements cannot remain falsely traceable.
        stale_acceptance = {rid.removeprefix("ACCEPT-") for rid in seen if rid.startswith("ACCEPT-")} - acceptance_ids
        if stale_acceptance:
            print("stale acceptance traceability: " + ", ".join(sorted(stale_acceptance)), file=sys.stderr)
            return 1
        if row_count == 0:
            print("requirements.csv must contain at least one requirement", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
