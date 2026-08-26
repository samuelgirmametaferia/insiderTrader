"""Bounded framed worker for out-of-process Python metrics.

The process owns no broker credentials and communicates only through stdin/stdout.
Each request and response is a length-prefixed UTF-8 JSON frame.  The Rust host
remains authoritative: this worker only computes a candidate output, and the
host revalidates identity, bounds, freshness, and declared inputs before use.
"""

from __future__ import annotations

import argparse
import importlib
import json
import struct
import sys
from collections.abc import Callable, Mapping
from typing import Any

from .contracts import MetricContext, MetricDescriptor, MetricOutput, validate_output
from insidertrader.worker_sandbox import apply as apply_sandbox

MAX_FRAME_BYTES = 1_048_576
MAX_FEATURES = 4_096


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
    stream.write(struct.pack("<I", len(payload)))
    stream.write(payload)
    stream.flush()


def _load_entrypoint(spec: str) -> Callable[[MetricContext], MetricOutput | Mapping[str, object]]:
    module_name, separator, attribute = spec.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("entrypoint must be module:callable")
    value = getattr(importlib.import_module(module_name), attribute)
    if not callable(value):
        raise ValueError("entrypoint is not callable")
    return value


def _coerce_output(value: MetricOutput | Mapping[str, object]) -> MetricOutput:
    if isinstance(value, MetricOutput):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("metric returned neither MetricOutput nor mapping")
    try:
        return MetricOutput(
            metric_id=str(value["metric_id"]),
            instrument_id=int(value["instrument_id"]),
            generated_mono_ns=int(value["generated_mono_ns"]),
            ttl_ns=int(value["ttl_ns"]),
            score=float(value["score"]),
            confidence=float(value["confidence"]),
            uncertainty=float(value["uncertainty"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("metric output fields are malformed") from error


def run(entrypoint: str, descriptor: MetricDescriptor) -> int:
    descriptor.validate()
    function = _load_entrypoint(entrypoint)
    input_stream = sys.stdin.buffer
    output_stream = sys.stdout.buffer
    while True:
        frame = _read_frame(input_stream)
        if frame is None:
            return 0
        try:
            request = json.loads(frame.decode("utf-8"))
            if not isinstance(request, Mapping):
                raise ValueError("request must be an object")
            features = request.get("features")
            if not isinstance(features, Mapping) or len(features) > MAX_FEATURES:
                raise ValueError("features are missing or exceed bound")
            undeclared = set(features) - set(descriptor.inputs)
            missing = set(descriptor.inputs) - set(features)
            if undeclared or missing:
                raise ValueError("feature set does not match declared metric inputs")
            context = MetricContext(
                instrument_id=int(request["instrument_id"]),
                now_mono_ns=int(request["now_mono_ns"]),
                features={str(key): float(value) for key, value in features.items()},
            )
            result = validate_output(_coerce_output(function(context)), descriptor)
            _write_frame(output_stream, {"ok": True, "metric": result.to_wire()})
        except Exception as error:  # noqa: BLE001 - worker must classify and continue
            _write_frame(output_stream, {"ok": False, "error": str(error)[:512]})


def main() -> int:
    apply_sandbox()
    parser = argparse.ArgumentParser(description="InsiderTrader Python metric worker")
    parser.add_argument("--entrypoint", required=True)
    parser.add_argument("--metric-id", required=True)
    parser.add_argument("--ttl-ns", required=True, type=int)
    parser.add_argument("--min-score", type=float)
    parser.add_argument("--max-score", type=float)
    parser.add_argument("--input", action="append", default=[])
    args = parser.parse_args()
    descriptor = MetricDescriptor(
        metric_id=args.metric_id,
        inputs=tuple(args.input),
        ttl_ns=args.ttl_ns,
        min_score=args.min_score,
        max_score=args.max_score,
    )
    return run(args.entrypoint, descriptor)


if __name__ == "__main__":
    raise SystemExit(main())
