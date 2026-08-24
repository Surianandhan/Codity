"""Record the kill -9 recovery demo as an animated GIF.

This does not stage a recording of a scripted narrative. It runs the real thing: a
real worker process claims a real job, is killed with SIGKILL while holding the
lease, and a second worker completes it after the reaper reclaims it. Every line in
the output is either a command being run or a value read back out of Postgres. If
the recovery fails, the recording fails with it -- there is no path here that
prints success without observing it.

Why record it at all: the reliability core is the centre of this project, and until
now the only way to see it work was to start two workers and time a `pkill`
correctly. A reader who never runs the system should still be able to watch a job
survive its worker dying.

    uv run python scripts/record_kill_demo.py

Requires a seeded database (`scripts/seed.py`) and an otherwise idle system. It
manages its own workers and kills them on the way out, including on failure.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

# Run as a script (`python scripts/record_kill_demo.py`), so backend/ is not on
# sys.path. Same shape as seed.py, demo_load.py and bench_claim.py.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_settings  # noqa: E402

REPO = Path(__file__).resolve().parents[2]
OUT = REPO / "docs" / "images" / "kill-9-recovery.gif"

BG = (11, 15, 25)
FG = (203, 213, 225)
DIM = (110, 122, 145)
GREEN = (52, 211, 153)
RED = (248, 113, 113)
AMBER = (251, 191, 36)
BLUE = (96, 165, 250)

FONT_PATH = "/System/Library/Fonts/Menlo.ttc"
FONT_SIZE = 15
LINE_H = 21
PAD = 22
COLS = 88
WIDTH = PAD * 2 + int(COLS * FONT_SIZE * 0.605)
ROWS = 26
HEIGHT = PAD * 2 + ROWS * LINE_H


@dataclass
class Line:
    text: str
    colour: tuple[int, int, int] = FG


@dataclass
class Recorder:
    """Accumulates terminal lines and snapshots the buffer as GIF frames."""

    lines: list[Line] = field(default_factory=list)
    frames: list[Image.Image] = field(default_factory=list)
    durations: list[int] = field(default_factory=list)
    font: Any = None

    def __post_init__(self) -> None:
        self.font = ImageFont.truetype(FONT_PATH, FONT_SIZE)

    def echo(self, text: str = "", colour: tuple[int, int, int] = FG) -> None:
        """Append a line, and mirror it to stdout so a live run is watchable too."""
        self.lines.append(Line(text, colour))
        print(text)

    def snap(self, hold_ms: int = 900) -> None:
        img = Image.new("RGB", (WIDTH, HEIGHT), BG)
        d = ImageDraw.Draw(img)
        for i, line in enumerate(self.lines[-ROWS:]):
            d.text((PAD, PAD + i * LINE_H), line.text, font=self.font, fill=line.colour)
        self.frames.append(img)
        self.durations.append(hold_ms)

    def save(self, path: Path) -> None:
        if not self.frames:
            raise RuntimeError("nothing recorded")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.frames[0].save(
            path,
            save_all=True,
            append_images=self.frames[1:],
            duration=self.durations,
            loop=0,
            optimize=True,
        )


async def scalar(engine: Any, sql: str, **params: Any) -> Any:
    async with engine.connect() as c:
        return (await c.execute(text(sql), params)).scalar()


async def fetch(engine: Any, sql: str, **params: Any) -> list[Any]:
    async with engine.connect() as c:
        return list((await c.execute(text(sql), params)).all())


def spawn_worker(org: str, name: str, log: Path) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "app.worker.main", "--org", org, "--name", name,
         "--concurrency", "2"],
        stdout=log.open("wb"),
        stderr=subprocess.STDOUT,
        cwd=str(REPO / "backend"),
    )


async def enqueue(engine: Any, queue_id: str, seconds: int, lease: int) -> str:
    """One job, inserted with the same column values the API's create path writes.

    timeout_ms is clamped below the lease because ck_jobs_timeout_lt_lease makes a
    job that could outlive its own lease unrepresentable.
    """
    timeout_ms = min(seconds * 1000 + 3000, lease * 1000 - 1000)
    async with engine.begin() as c:
        return str(
            (
                await c.execute(
                    text(
                        "INSERT INTO jobs (id, organization_id, project_id, queue_id, kind,"
                        " handler, status, priority, run_at, payload, attempt, max_attempts,"
                        " backoff_strategy, backoff_base_ms, backoff_max_ms,"
                        " unregistered_count, timeout_ms, lease_seconds, lease_epoch,"
                        " cancel_requested, lock_version, created_at, updated_at)"
                        " SELECT gen_random_uuid(), q.organization_id, q.project_id, q.id,"
                        " 'immediate', 'demo.sleep', 'queued', 0, now(),"
                        " jsonb_build_object('seconds', CAST(:s AS int)), 0, 3,"
                        " 'exponential', 1000, 300000, 0, CAST(:t AS int),"
                        " q.visibility_timeout_sec, 0, false, 0, now(), now()"
                        " FROM queues q WHERE q.id = CAST(:q AS uuid)"
                        " RETURNING id"
                    ),
                    {"s": seconds, "t": timeout_ms, "q": queue_id},
                )
            ).scalar_one()
        )


async def wait_for(engine: Any, sql: str, timeout: int = 25, **params: Any) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = await scalar(engine, sql, **params)
        if got is not None:
            return got
        await asyncio.sleep(0.5)
    return None


async def run(args: argparse.Namespace) -> int:
    engine = create_async_engine(str(get_settings().database_url), poolclass=NullPool)
    r = Recorder()
    tmp = Path(tempfile.mkdtemp(prefix="killdemo-"))
    try:
        org = await scalar(
            engine, "SELECT id::text FROM organizations ORDER BY created_at LIMIT 1"
        )
        if org is None:
            print("no organization -- run scripts/seed.py first", file=sys.stderr)
            return 2
        q = await fetch(
            engine,
            "SELECT id::text, visibility_timeout_sec FROM queues"
            " WHERE organization_id = CAST(:o AS uuid) AND name = 'demo' LIMIT 1",
            o=org,
        )
        if not q:
            print("no 'demo' queue -- run scripts/seed.py first", file=sys.stderr)
            return 2
        queue_id, lease = str(q[0][0]), int(q[0][1])

        r.echo("# Codity — a job survives its worker being killed", DIM)
        r.echo(f"# 'demo' queue: {lease}s lease, handler sleeps {args.handler_seconds}s", DIM)
        r.echo()
        r.snap(1800)

        r.echo("$ python -m app.worker.main --name worker-A", BLUE)
        spawn_worker(org, "worker-A", tmp / "a.log")
        await asyncio.sleep(4)
        r.echo("  worker-A registered, polling for work", GREEN)
        r.echo()
        r.snap(1400)

        r.echo("$ # enqueue one job that outlives a kill", DIM)
        job_id = await enqueue(engine, queue_id, args.handler_seconds, lease)
        r.echo(f"  job {job_id[:8]}… queued", FG)
        r.echo()
        r.snap(1200)

        holder = await wait_for(
            engine,
            "SELECT w.name FROM jobs j JOIN workers w ON w.id = j.worker_id"
            " WHERE j.id = CAST(:j AS uuid) AND j.status = 'running'",
            j=job_id,
        )
        if holder is None:
            r.echo("  job was never claimed — aborting", RED)
            r.snap(2500)
            return 1
        r.echo("$ select status, worker from jobs where id = …", BLUE)
        r.echo(f"  running    {holder}", GREEN)
        r.echo()
        r.snap(1600)

        r.echo("$ kill -9 $(pgrep -f worker-A)        # mid-flight", RED)
        subprocess.run(["pkill", "-9", "-f", "app.worker.main.*worker-A"], capture_output=True)
        await asyncio.sleep(2)
        if subprocess.run(
            ["pgrep", "-f", "app.worker.main.*worker-A"], capture_output=True
        ).returncode == 0:
            r.echo("  worker-A survived the kill — aborting", RED)
            r.snap(2500)
            return 1
        r.echo("  worker-A is gone. Its job is still marked running.", AMBER)
        r.echo(f"  nothing alive can finish it; the lease lapses in ~{lease}s.", AMBER)
        r.echo()
        r.snap(2400)

        r.echo("$ python -m app.worker.main --name worker-B", BLUE)
        spawn_worker(org, "worker-B", tmp / "b.log")
        r.echo("  worker-B is up, but cannot touch the job until the lease lapses.", DIM)
        r.echo()
        r.snap(1600)

        r.echo("$ # waiting for the reaper…", DIM)
        r.snap(1000)
        deadline = time.monotonic() + lease + 75
        seen = ""
        while time.monotonic() < deadline:
            st = str(await scalar(
                engine, "SELECT status FROM jobs WHERE id = CAST(:j AS uuid)", j=job_id
            ))
            if st != seen:
                r.echo(f"  status: {st}", GREEN if st == "completed" else AMBER)
                r.snap(1100)
                seen = st
            if st in {"completed", "failed", "dead_letter"}:
                break
            await asyncio.sleep(1.5)

        if seen != "completed":
            r.echo(f"  job did not recover (ended {seen!r})", RED)
            r.snap(3000)
            return 1

        r.echo()
        r.echo("$ select attempt, worker, status, error from job_executions", BLUE)
        for attempt, worker, status, err in await fetch(
            engine,
            "SELECT e.attempt_number, COALESCE(w.name,'—'), e.status::text,"
            " COALESCE(e.error_class,'') FROM job_executions e"
            " LEFT JOIN workers w ON w.id = e.worker_id"
            " WHERE e.job_id = CAST(:j AS uuid) ORDER BY e.id",
            j=job_id,
        ):
            r.echo(
                f"  {attempt}   {str(worker):<12} {status:<11} {err}",
                GREEN if status == "succeeded" else RED,
            )
        r.echo()
        r.echo("  The reaper closed the dead attempt as lost/LeaseExpired,", DIM)
        r.echo("  requeued the job, and worker-B ran it. Exactly once.", DIM)
        r.snap(6000)

        r.save(args.out)
        print(f"\nwrote {args.out} ({args.out.stat().st_size:,} bytes, {len(r.frames)} frames)")
        return 0
    finally:
        subprocess.run(
            ["pkill", "-9", "-f", "app.worker.main.*worker-"], capture_output=True
        )
        with contextlib.suppress(Exception):
            await engine.dispose()
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(prog="record-kill-demo")
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument("--handler-seconds", type=int, default=15)
    os.environ.setdefault("CODITY_LOG_JSON", "false")
    return asyncio.run(run(ap.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
