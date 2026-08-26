#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

command -v protoc >/dev/null || { echo "protoc is required" >&2; exit 1; }
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
protoc --descriptor_set_out="$tmp_dir/contracts.pb" --include_imports --proto_path=schemas/proto schemas/proto/*.proto
[[ -s "$tmp_dir/contracts.pb" ]]
sha256sum schemas/proto/*.proto schemas/json/*.json | sort | diff -u schemas/compatibility.lock -
for schema in schemas/json/*.json evidence/gate-evidence.schema.json; do
  python3 -m json.tool "$schema" >/dev/null
done
