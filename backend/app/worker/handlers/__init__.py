"""Handler registry.

A handler is an async callable taking (JobContext, payload). Blocking work MUST go
through asyncio.to_thread: a CPU-bound handler on the event loop freezes the
heartbeat for EVERY job on that worker, which expires their leases and causes
spurious reaping -- the system's own worst failure mode.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

Handler = Callable[["JobContext", dict[str, Any]], Awaitable[dict[str, Any] | None]]
REGISTRY: dict[str, Handler] = {}


@dataclass
class JobContext:
    job_id: UUID
    attempt: int
    max_attempts: int
    correlation_id: str | None = None
    _cancelled: bool = False

    def cancelled(self) -> bool:
        """Refreshed from the heartbeat's RETURNING clause -- no extra query."""
        return self._cancelled


def handler(name: str) -> Callable[[Handler], Handler]:
    def deco(fn: Handler) -> Handler:
        if name in REGISTRY:
            raise RuntimeError(f"duplicate handler registration: {name!r}")
        REGISTRY[name] = fn
        return fn

    return deco


@handler("demo.echo")
async def echo(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    return {"echoed": payload, "attempt": ctx.attempt}


@handler("demo.sleep")
async def sleep(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    seconds = float(payload.get("seconds", 0.1))
    await asyncio.sleep(seconds)
    return {"slept": seconds}


@handler("demo.flaky")
async def flaky(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    import random

    if random.random() < float(payload.get("failure_rate", 0.3)):
        raise RuntimeError(f"flaky handler failed on attempt {ctx.attempt}")
    return {"ok": True, "attempt": ctx.attempt}


@handler("demo.always_fail")
async def always_fail(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    raise RuntimeError(payload.get("message", "this handler always fails"))


@handler("demo.cpu")
async def cpu(ctx: JobContext, payload: dict[str, Any]) -> dict[str, Any]:
    """Deliberately routed through a thread. The naive version blocks the event
    loop, freezes heartbeats for every job on this worker, and reproduces the
    design's own worst bug in front of whoever is watching."""

    def _burn(n: int) -> int:
        return sum(i * i for i in range(n))

    iterations = int(payload.get("iterations", 1_000_000))
    total = await asyncio.to_thread(_burn, iterations)
    return {"iterations": iterations, "checksum": total % 1_000_003}
