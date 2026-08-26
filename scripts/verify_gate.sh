#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
gate="${1:-}"
if [[ ! "$gate" =~ ^G(0[0-9]|1[0-5])$ ]]; then
  echo "usage: $0 G00..G15" >&2
  exit 2
fi

evidence_path="$project_root/evidence/gates/$gate.yaml"
if [[ ! -f "$evidence_path" ]]; then
  echo "missing gate evidence: $evidence_path" >&2
  exit 1
fi

PROJECT_ROOT="$project_root" GATE="$gate" EVIDENCE_PATH="$evidence_path" python3 - <<'PY'
import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path

root = Path(os.environ["PROJECT_ROOT"])
gate = os.environ["GATE"]
evidence_path = Path(os.environ["EVIDENCE_PATH"])

# Evidence is JSON-formatted YAML: JSON is a strict YAML 1.2 subset and lets
# this verifier run with the Python standard library in offline environments.
try:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as error:
    raise SystemExit(f"invalid evidence document (JSON/YAML subset required): {error}")

def fail(message: str) -> None:
    raise SystemExit(f"{gate}: {message}")

if not isinstance(evidence, dict):
    fail("top-level evidence must be an object")
if evidence.get("gate") != gate:
    fail("gate field does not match requested gate")
if evidence.get("status") != "passed":
    fail("status must be passed")
revision = evidence.get("source_revision")
if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
    fail("source_revision must be a 40-character lowercase commit hash")
try:
    dt.datetime.fromisoformat(str(evidence["completed_at"]).replace("Z", "+00:00"))
except (KeyError, TypeError, ValueError) as error:
    fail(f"completed_at must be RFC3339: {error}")

verification = evidence.get("verification")
if not isinstance(verification, dict):
    fail("verification must be an object")
for name in ("unit", "integration", "replay", "performance", "fault_injection"):
    section = verification.get(name)
    if not isinstance(section, dict) or not section.get("run_url"):
        fail(f"verification.{name}.run_url is required")
security = evidence.get("security_findings")
if security != {"critical_open": 0, "high_open": 0}:
    fail("critical_open and high_open must both be exactly zero")
approvals = evidence.get("approvals")
if not isinstance(approvals, list) or not approvals:
    fail("at least one approval is required")

artifacts = evidence.get("release_artifacts", [])
if not isinstance(artifacts, list):
    fail("release_artifacts must be an array")
for artifact in artifacts:
    if not isinstance(artifact, dict) or not isinstance(artifact.get("path"), str):
        fail("each release artifact requires a path and sha256")
    relative = Path(artifact["path"])
    if relative.is_absolute() or ".." in relative.parts:
        fail(f"artifact path escapes repository: {relative}")
    target = root / relative
    if not target.is_file():
        fail(f"artifact does not exist: {relative}")
    expected = artifact.get("sha256")
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
        fail(f"artifact hash is invalid: {relative}")
    actual = hashlib.sha256(target.read_bytes()).hexdigest()
    if actual != expected:
        fail(f"artifact hash mismatch: {relative}")

schema_hash = evidence.get("schema_bundle_sha256")
if schema_hash is not None:
    if not isinstance(schema_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", schema_hash):
        fail("schema_bundle_sha256 is invalid")
    digest = hashlib.sha256()
    schema_files = sorted(path for path in (root / "schemas").rglob("*") if path.is_file())
    for path in schema_files:
        digest.update(path.relative_to(root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
    if digest.hexdigest() != schema_hash:
        fail("schema_bundle_sha256 does not match schemas/")

try:
    actual_revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True, stderr=subprocess.DEVNULL
    ).strip()
except (OSError, subprocess.CalledProcessError):
    fail("cannot resolve git HEAD; evidence cannot be tied to a source revision")
if actual_revision != revision:
    fail(f"source_revision {revision} does not match current HEAD {actual_revision}")

print(f"{gate}: evidence verified for {revision}")
PY
