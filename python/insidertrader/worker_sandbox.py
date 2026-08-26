"""Process-level safety limits for untrusted Python research workers."""

from __future__ import annotations

import os
import socket

MIN_MEMORY_BYTES = 64 * 1024 * 1024
MAX_MEMORY_BYTES = 8 * 1024 * 1024 * 1024
MAX_CPU_SECONDS = 86_400


def bounded_env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    """Read one bounded worker setting without silently accepting bad input."""
    raw = os.environ.get(name)
    value = default if raw is None else int(raw)
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def network_enabled() -> bool:
    """Return the exact opt-in network policy injected by the Rust host."""
    return os.environ.get("IT_PYTHON_ALLOW_NETWORK") == "true"


def apply() -> None:
    """Apply bounded resources and deny network access before loading plugins."""
    workdir = os.environ.get("IT_PYTHON_WORKDIR")
    if workdir:
        os.chdir(workdir)
    try:
        import resource

        # This is a process lifetime ceiling; per-evaluation deadlines are
        # enforced by the Rust host, so the default must not kill a healthy
        # long-running worker after only a few requests.
        cpu_seconds = bounded_env_int("IT_PYTHON_CPU_SECONDS", 3600, 1, MAX_CPU_SECONDS)
        memory_bytes = bounded_env_int(
            "IT_PYTHON_MEMORY_BYTES", 512 * 1024 * 1024, MIN_MEMORY_BYTES, MAX_MEMORY_BYTES
        )
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
        resource.setrlimit(resource.RLIMIT_FSIZE, (16 * 1024 * 1024, 16 * 1024 * 1024))
    except (ImportError, OSError):
        # Windows and constrained containers may not expose POSIX limits; the
        # Rust host still enforces frame/deadline bounds in that environment.
        pass

    if not network_enabled():
        def denied(*_args: object, **_kwargs: object) -> None:
            raise OSError("network access is disabled for Python workers")

        socket.socket = denied  # type: ignore[assignment]
        socket.create_connection = denied  # type: ignore[assignment]
