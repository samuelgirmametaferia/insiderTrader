"""Bounded framed worker for out-of-process Python strategies."""

from __future__ import annotations

import argparse
import importlib
import json
import struct
import sys
from collections.abc import Callable, Mapping
from typing import Any

from .contracts import Action, Proposal, StrategyContext, validate_proposal
from insidertrader.worker_sandbox import apply as apply_sandbox

MAX_FRAME_BYTES = 1_048_576
MAX_METRICS = 4_096
MAX_EVIDENCE = 256


def _read_frame(stream: Any) -> bytes | None:
    header = stream.read(4)
    if header == b"":
        return None
    if len(header) != 4:
        raise ValueError("truncated frame header")
    length = struct.unpack("<I", header)[0]
    if length == 0 or length > MAX_FRAME_BYTES:
        raise ValueError("frame exceeds bound")
    payload = stream.read(length)
    if len(payload) != length:
        raise ValueError("truncated frame payload")
    return payload


def _write_frame(stream: Any, value: Mapping[str, object]) -> None:
    payload = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if not payload or len(payload) > MAX_FRAME_BYTES:
        raise ValueError("response exceeds bound")
    stream.write(struct.pack("<I", len(payload)) + payload)
    stream.flush()


def _load_entrypoint(spec: str) -> Callable[[StrategyContext], Proposal | Mapping[str, object]]:
    module_name, separator, attribute = spec.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("entrypoint must be module:callable")
    value = getattr(importlib.import_module(module_name), attribute)
    if not callable(value):
        raise ValueError("entrypoint is not callable")
    return value


def _coerce_action(value: object) -> Action:
    if not isinstance(value, Mapping):
        raise ValueError("action must be an object")
    kind = value.get("kind")
    if not isinstance(kind, str):
        raise ValueError("action kind is missing")
    return Action(kind=kind, value=value.get("value"))


def _coerce_proposal(value: Proposal | Mapping[str, object]) -> Proposal:
    if isinstance(value, Proposal):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("strategy returned neither Proposal nor mapping")
    action = _coerce_action(value.get("action"))
    evidence = value.get("evidence", ())
    if not isinstance(evidence, (list, tuple)) or len(evidence) > MAX_EVIDENCE:
        raise ValueError("evidence exceeds bound")
    return Proposal(
        proposal_id=int(value["proposal_id"]),
        strategy_id=str(value["strategy_id"]),
        instrument_id=int(value["instrument_id"]),
        action=action,
        confidence=float(value["confidence"]),
        horizon_ns=int(value["horizon_ns"]),
        ttl_ns=int(value["ttl_ns"]),
        generated_mono_ns=int(value["generated_mono_ns"]),
        evidence=tuple(str(item) for item in evidence),
    )


def run(entrypoint: str, strategy_id: str) -> int:
    if not strategy_id.strip():
        raise ValueError("strategy ID is empty")
    function = _load_entrypoint(entrypoint)
    while True:
        frame = _read_frame(sys.stdin.buffer)
        if frame is None:
            return 0
        try:
            request = json.loads(frame.decode("utf-8"))
            if not isinstance(request, Mapping):
                raise ValueError("request must be an object")
            metrics = request.get("metrics")
            if not isinstance(metrics, list) or len(metrics) > MAX_METRICS:
                raise ValueError("metrics are missing or exceed bound")
            context = StrategyContext(
                instrument_id=int(request["instrument_id"]),
                now_mono_ns=int(request["now_mono_ns"]),
                metrics=tuple(item for item in metrics if isinstance(item, Mapping)),
            )
            proposal = validate_proposal(_coerce_proposal(function(context)), context.now_mono_ns)
            if proposal.strategy_id != strategy_id or proposal.instrument_id != context.instrument_id:
                raise ValueError("proposal identity does not match worker context")
            _write_frame(sys.stdout.buffer, {"ok": True, "proposal": proposal.to_wire()})
        except Exception as error:  # noqa: BLE001 - classify worker failures in-band
            _write_frame(sys.stdout.buffer, {"ok": False, "error": str(error)[:512]})


def main() -> int:
    apply_sandbox()
    parser = argparse.ArgumentParser(description="InsiderTrader Python strategy worker")
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--strategy-id", required=True)
    args = parser.parse_args()
    return run(args.entrypoint, args.strategy_id)


if __name__ == "__main__":
    raise SystemExit(main())
