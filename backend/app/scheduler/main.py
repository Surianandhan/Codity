"""The scheduler process.

A separate process, not a FastAPI background task and not a leader-elected worker
role. An in-API task ties job liveness to HTTP traffic and double-fires on every API
replica; leader election is a distributed-systems problem this system does not need,
because correctness under N schedulers comes from the database -- SKIP LOCKED and
``ux_jobs_schedule_occurrence`` -- rather than from singleton-ness.

    uv run python -m app.scheduler.main
    uv run python -m app.scheduler.main --loops promoter,cron
    uv run python -m app.scheduler.main --once
"""

import argparse
import asyncio
import contextlib
import signal
from collections.abc import Mapping

import structlog

from app.config import get_settings
from app.db.session import get_sessionmaker
from app.main import configure_logging
from app.scheduler.loops import build_loops, run_all, run_once_all

log = structlog.get_logger()


def _select(available: Mapping[str, object], names: str) -> list[str]:
    if names.strip().lower() == "all":
        return list(available)
    chosen = [n.strip() for n in names.split(",") if n.strip()]
    unknown = [n for n in chosen if n not in available]
    if unknown:
        raise SystemExit(
            f"unknown loop(s): {', '.join(unknown)}. known: {', '.join(available)}"
        )
    return chosen


async def _run(args: argparse.Namespace) -> None:
    configure_logging()
    s = get_settings()
    all_loops = build_loops(
        sessionmaker=get_sessionmaker(),
        promoter_interval_ms=s.promoter_interval_ms,
        cron_interval_ms=s.cron_interval_ms,
        reaper_interval_ms=s.reaper_interval_ms,
        reaper_batch=s.reaper_batch,
    )
    selected = {name: all_loops[name] for name in _select(all_loops, args.loops)}

    if args.once:
        results = await run_once_all(selected)
        for name, counts in results.items():
            log.info("scheduler.once", loop=name, **counts)
        return

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    log.info("scheduler.started", loops=list(selected))
    try:
        await run_all(selected, stop)
    finally:
        # Nothing to drain: every tick is a single committed transaction, so a
        # scheduler can be killed at any instant without leaving state behind.
        log.info("scheduler.stopped")


def main() -> None:
    p = argparse.ArgumentParser(
        prog="codity-scheduler",
        description="Promoter, cron dispatcher, lease reaper, dead-worker sweep, retention.",
    )
    p.add_argument(
        "--loops",
        default="all",
        help="comma-separated subset of promoter,cron,reaper,dead_worker,retention (default: all)",
    )
    p.add_argument(
        "--once",
        action="store_true",
        help="run exactly one tick of each selected loop and exit",
    )
    asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    main()
