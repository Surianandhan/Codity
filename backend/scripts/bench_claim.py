#!/usr/bin/env python
"""Measure claim throughput as claimer concurrency scales against a fixed backlog.

    uv run python scripts/bench_claim.py
    uv run python scripts/bench_claim.py --jobs 20000 --levels 1,2,4,8,16

docs/ARCHITECTURE.md calls the worker count "the throughput knob". This is the
measurement that says what that knob is actually worth, so the claim can be read as
a number instead of taken on faith.

What is measured: the claim path only -- ``lock_queue.sql``, then
``claim_jobs.sql``, then COMMIT, through ``app.services.claim.claim_jobs``, the
same function the worker calls. No job is executed. A benchmark that ran the
handlers would be measuring ``demo.sleep``, not the queue.

Four things make the number mean something rather than merely exist.

**The concurrency cap is provably not the bottleneck.** Claimed jobs are never
completed here, so every claim permanently consumes an in-flight slot, and
``ck_queues_max_concurrency_range`` caps ``max_concurrency`` at 1000 -- a 5000-job
backlog simply cannot be held in flight at once. So the backlog is drained in
segments of at most ``SEGMENT_MAX`` (< the cap), re-seeded between segments with
the clock stopped. Within a segment ``max_concurrency - in_flight`` therefore
always exceeds the work still queued, so ``claim_jobs.sql``'s headroom never
throttles a claim below what is available. Left at a realistic value instead, the
queue would hit its cap inside one batch and the benchmark would be timing
``GREATEST(..., 0) = 0``: measuring the cap, not the claim.

**Each claimer owns a private Postgres backend.** The engine uses ``NullPool`` and
each claimer checks out one connection and holds it for the whole level, so K
claimers are K genuinely concurrent backends. A shared pool would serialise them at
the pool rather than at the database and the curve would be an artifact of
SQLAlchemy. Connections are established, and the level's clock started, only once
every claimer is connected -- otherwise handshake cost would be charged to the
high-concurrency levels, which is the term being measured.

**Every level faces an identical backlog.** The queue is emptied and re-seeded
before each segment of each level, and ``VACUUM (ANALYZE)`` runs between them: each
level churns the whole backlog through ``jobs``, and without the vacuum the dead
tuples would accumulate in ix_jobs_claim so every later level scanned more corpses
than the one before it. Levels run in ascending concurrency, so that bias would
land entirely on the high end and manufacture exactly the flattening this exists to
test for.

**Disjointness is asserted, not assumed.** The union of claimed ids must be exactly
the seeded backlog with no id claimed twice, cross-checked against ``jobs`` and
``job_executions`` rather than trusted from the return values. A throughput number
from a run that double-claimed would be worse than no number at all, so the check
prints in the same block as the timing and a violation exits non-zero.

Known bias, stated rather than hidden: because nothing is ever completed, the
in-flight count that headroom aggregates grows from 0 to a full segment as a
segment drains. A real worker completes jobs and holds that count near its
concurrency, so these are floor figures. Every level pays it identically over an
identical backlog, so the shape of the curve -- the thing actually being asked
about -- is unaffected.
"""

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

# Run as a script (`python scripts/bench_claim.py`), so backend/ is not on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import insert, text  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine  # noqa: E402
from sqlalchemy.pool import NullPool  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.db.models.base import uuid7  # noqa: E402
from app.db.models.execution import Worker  # noqa: E402
from app.db.models.scheduling import Job, Queue  # noqa: E402
from app.db.models.tenancy import Organization, Project  # noqa: E402
from app.domain.enums import BackoffStrategy, JobKind, JobStatus, WorkerStatus  # noqa: E402
from app.services.claim import claim_jobs  # noqa: E402

DEFAULT_JOBS = 5_000
DEFAULT_LEVELS = "1,2,4,8"
DEFAULT_BATCH = 10  # app.config's claim_batch_size default: what a worker really asks for

# The schema ceiling: ck_queues_max_concurrency_range is BETWEEN 1 AND 1000.
MAX_CONCURRENCY = 1_000
# Jobs drained per timed segment. Strictly below the cap, so headroom
# (max_concurrency - in_flight) always exceeds the work still queued and the cap
# can never be what limits a claim.
SEGMENT_MAX = 900

LEASE_SECONDS = 300
# How long an idle claimer waits before re-polling. Small enough not to distort the
# tail of a segment, large enough not to spin a core while the last batches drain.
IDLE_SLEEP_S = 0.002
# Consecutive empty claims before a claimer concludes the segment is gone. Only
# reached when a run is already failing; the normal exit is the shared done event.
EMPTY_ROUNDS = 50


@dataclass(frozen=True)
class Scope:
    """The throwaway tenant this benchmark builds and then removes."""

    org_id: UUID
    project_id: UUID
    queue_id: UUID

    @property
    def timeout_ms(self) -> int:
        """A job may never outlive its own lease (ck_jobs_timeout_lt_lease)."""
        return LEASE_SECONDS * 1000 - 1000


class Gate:
    """Opens once every claimer has arrived.

    Used twice: to hold the clock until all connections exist, and to hold
    re-seeding until every claimer has left the segment just drained. Without the
    second use, a straggler still inside a claim would collide with the DELETE that
    starts the next segment -- and could claim rows the next segment's clock has
    not started counting.
    """

    def __init__(self, parties: int) -> None:
        self._parties = parties
        self._count = 0
        self._event = asyncio.Event()

    def arrive(self) -> None:
        self._count += 1
        if self._count >= self._parties:
            self._event.set()

    async def wait(self) -> None:
        await self._event.wait()


@dataclass
class Segment:
    """One timed drain of ``total`` jobs by every claimer in the level."""

    total: int
    parties: int
    go: asyncio.Event = field(default_factory=asyncio.Event)
    done: asyncio.Event = field(default_factory=asyncio.Event)
    parked: Gate = field(init=False)
    deadline: float = 0.0
    started_at: float = 0.0
    finished_at: float | None = None
    claimed: int = 0

    def __post_init__(self) -> None:
        self.parked = Gate(self.parties)

    def record(self, n: int, at: float) -> None:
        """Stop the clock the instant the last job is claimed.

        Letting claimers poll their way to an empty streak first would add trailing
        idle time to the wall clock -- a different amount at every level, which is
        precisely the term the comparison is trying to isolate.
        """
        self.claimed += n
        if self.claimed >= self.total and self.finished_at is None:
            self.finished_at = at
            self.done.set()

    @property
    def seconds(self) -> float:
        return (self.finished_at or time.perf_counter()) - self.started_at

    @property
    def drained(self) -> bool:
        return self.finished_at is not None


@dataclass
class LevelResult:
    workers: int
    claimed_ids: list[UUID]
    seconds: float
    latencies_ms: list[float]
    productive_calls: int
    empty_calls: int
    segments: int
    rows_matched: bool
    drained: bool

    @property
    def claimed(self) -> int:
        return len(self.claimed_ids)

    @property
    def unique(self) -> int:
        return len(set(self.claimed_ids))

    @property
    def duplicates(self) -> int:
        return self.claimed - self.unique

    @property
    def claims_per_sec(self) -> float:
        return self.claimed / self.seconds if self.seconds > 0 else 0.0

    def pct(self, q: float) -> float:
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        # Nearest-rank, no interpolation: every number printed is a latency that
        # actually happened rather than one between two that did.
        rank = max(1, min(len(ordered), int(q * len(ordered) + 0.5)))
        return ordered[rank - 1]

    def ok(self, expected: set[UUID]) -> bool:
        return (
            self.drained
            and self.duplicates == 0
            and self.rows_matched
            and set(self.claimed_ids) == expected
        )


# --- setup / teardown -------------------------------------------------------


async def create_scope(engine: AsyncEngine) -> Scope:
    """One throwaway org, project and queue."""
    suffix = uuid7().hex[:12]
    async with AsyncSession(engine) as s:
        org = Organization(id=uuid7(), name="Bench", slug=f"bench-{suffix}")
        s.add(org)
        await s.flush()
        project = Project(id=uuid7(), organization_id=org.id, name="Bench", slug=f"bench-{suffix}")
        s.add(project)
        await s.flush()
        queue = Queue(
            id=uuid7(),
            organization_id=org.id,
            project_id=project.id,
            name=f"bench-{suffix}",
            max_concurrency=MAX_CONCURRENCY,
            visibility_timeout_sec=LEASE_SECONDS,
            default_timeout_ms=LEASE_SECONDS * 1000 - 1000,
        )
        s.add(queue)
        await s.flush()
        scope = Scope(org_id=org.id, project_id=project.id, queue_id=queue.id)
        await s.commit()
    return scope


async def register_workers(engine: AsyncEngine, org_id: UUID, count: int) -> list[UUID]:
    """Real ``workers`` rows: job_executions.worker_id is a foreign key, so a claim
    against an invented worker id raises 23503 instead of claiming."""
    now = datetime.now(UTC)
    ids: list[UUID] = []
    async with AsyncSession(engine) as s:
        for i in range(count):
            worker = Worker(
                id=uuid7(),
                organization_id=org_id,
                name=f"bench-claimer-{i}",
                hostname="bench",
                pid=i,
                status=WorkerStatus.ACTIVE,
                concurrency=1,
                started_at=now,
                last_heartbeat_at=now,
            )
            s.add(worker)
            ids.append(worker.id)
        await s.commit()
    return ids


def split_backlog(jobs: int) -> list[int]:
    """The backlog as segment sizes, each below the concurrency cap and summing to
    exactly ``jobs``. Depends only on ``--jobs``, so every level drains the same
    segmentation."""
    count = max(1, -(-jobs // SEGMENT_MAX))
    base, extra = divmod(jobs, count)
    return [base + (1 if i < extra else 0) for i in range(count)]


async def seed_segment(engine: AsyncEngine, scope: Scope, count: int) -> set[UUID]:
    """Empty the queue and lay down ``count`` fresh queued jobs. Untimed."""
    now = datetime.now(UTC)
    ids = [uuid7() for _ in range(count)]
    rows = [
        {
            "id": job_id,
            "organization_id": scope.org_id,
            "project_id": scope.project_id,
            "queue_id": scope.queue_id,
            "kind": JobKind.IMMEDIATE,
            "handler": "bench.noop",
            "status": JobStatus.QUEUED,
            # Uniform priority and run_at, so the claim's ORDER BY falls through to
            # id and no level can get lucky on a cheaper sort.
            "priority": 0,
            "run_at": now,
            "payload": {},
            "attempt": 0,
            "max_attempts": 3,
            "backoff_strategy": BackoffStrategy.EXPONENTIAL,
            "backoff_base_ms": 1_000,
            "backoff_max_ms": 300_000,
            "timeout_ms": scope.timeout_ms,
            "lease_seconds": LEASE_SECONDS,
        }
        for job_id in ids
    ]
    async with AsyncSession(engine) as s:
        await s.execute(
            text("DELETE FROM job_executions WHERE queue_id = CAST(:q AS uuid)"),
            {"q": str(scope.queue_id)},
        )
        await s.execute(
            text("DELETE FROM jobs WHERE queue_id = CAST(:q AS uuid)"), {"q": str(scope.queue_id)}
        )
        await s.execute(insert(Job), rows)
        await s.commit()
    return set(ids)


async def vacuum(engine: AsyncEngine) -> None:
    """Reclaim the segment just deleted, so no level scans another level's corpses.
    VACUUM cannot run inside a transaction, hence AUTOCOMMIT."""
    autocommit = engine.execution_options(isolation_level="AUTOCOMMIT")
    async with autocommit.connect() as conn:
        await conn.execute(text("VACUUM (ANALYZE) jobs, job_executions"))


async def claimed_rows(engine: AsyncEngine, scope: Scope) -> tuple[int, int]:
    """The same fact read back from the database rather than from return values: a
    claim that reported an id it did not actually own would disagree here."""
    async with AsyncSession(engine) as s:
        jobs = await s.scalar(
            text(
                "SELECT count(*) FROM jobs WHERE queue_id = CAST(:q AS uuid)"
                " AND status = 'claimed'"
            ),
            {"q": str(scope.queue_id)},
        )
        executions = await s.scalar(
            text("SELECT count(*) FROM job_executions WHERE queue_id = CAST(:q AS uuid)"),
            {"q": str(scope.queue_id)},
        )
    return int(jobs or 0), int(executions or 0)


async def cleanup(engine: AsyncEngine, scope: Scope) -> None:
    """Remove everything this script created, children first."""
    params = {"o": str(scope.org_id)}
    async with AsyncSession(engine) as s:
        for sql in (
            "DELETE FROM job_executions WHERE organization_id = CAST(:o AS uuid)",
            "DELETE FROM jobs WHERE organization_id = CAST(:o AS uuid)",
            "DELETE FROM worker_heartbeats WHERE worker_id IN"
            " (SELECT id FROM workers WHERE organization_id = CAST(:o AS uuid))",
            "DELETE FROM workers WHERE organization_id = CAST(:o AS uuid)",
            "DELETE FROM queues WHERE organization_id = CAST(:o AS uuid)",
            "DELETE FROM projects WHERE organization_id = CAST(:o AS uuid)",
            "DELETE FROM organizations WHERE id = CAST(:o AS uuid)",
        ):
            await s.execute(text(sql), params)
        await s.commit()


# --- the measurement --------------------------------------------------------


async def claimer(
    engine: AsyncEngine,
    scope: Scope,
    worker_id: UUID,
    batch_size: int,
    ready: Gate,
    segments: list[Segment],
) -> tuple[list[UUID], list[float], int, int]:
    """One independent claimer: its own Postgres backend, its own claim loop, alive
    for every segment of the level.

    ``engine.connect()`` on a NullPool engine opens a brand-new backend, and holding
    it across the whole level means the loop reuses it between commits instead of
    paying a fresh handshake per claim -- which is what a pooled worker does, and
    without which this would largely be a benchmark of connection setup.
    """
    claimed: list[UUID] = []
    latencies: list[float] = []
    productive = empty = 0

    async with engine.connect() as conn:
        session = AsyncSession(bind=conn)
        try:
            # Force the backend into existence before the clock starts.
            await session.execute(text("SELECT 1"))
            await session.commit()
            ready.arrive()

            for segment in segments:
                await segment.go.wait()
                streak = 0
                while not segment.done.is_set() and time.perf_counter() < segment.deadline:
                    t0 = time.perf_counter()
                    batch = await claim_jobs(session, scope.queue_id, worker_id, batch_size)
                    await session.commit()
                    at = time.perf_counter()

                    if batch:
                        # Empty claims stay out of the latency sample: they are a
                        # different statement shape (headroom finds nothing to
                        # lock) and happen only once the segment is already gone,
                        # so mixing them in would drag the percentiles toward the
                        # drained state rather than the working one.
                        latencies.append((at - t0) * 1000.0)
                        productive += 1
                        streak = 0
                        claimed.extend(c.job_id for c in batch)
                        segment.record(len(batch), at)
                    else:
                        empty += 1
                        streak += 1
                        if streak >= EMPTY_ROUNDS:
                            break
                        await asyncio.sleep(IDLE_SLEEP_S)
                segment.parked.arrive()
        finally:
            await session.close()

    return claimed, latencies, productive, empty


async def run_level(
    engine: AsyncEngine,
    scope: Scope,
    workers: list[UUID],
    sizes: list[int],
    batch_size: int,
    timeout: float,
) -> tuple[LevelResult, set[UUID]]:
    ready = Gate(len(workers))
    segments = [Segment(total=n, parties=len(workers)) for n in sizes]
    tasks = [
        asyncio.create_task(claimer(engine, scope, w, batch_size, ready, segments))
        for w in workers
    ]

    expected: set[UUID] = set()
    rows_matched = True
    try:
        await ready.wait()
        for segment in segments:
            expected |= await seed_segment(engine, scope, segment.total)
            await vacuum(engine)
            segment.deadline = time.perf_counter() + timeout
            segment.started_at = time.perf_counter()
            segment.go.set()
            # Wait for every claimer to leave, not merely for the count to be hit:
            # re-seeding under a straggler would corrupt the next segment.
            await segment.parked.wait()
            if await claimed_rows(engine, scope) != (segment.total, segment.total):
                rows_matched = False
        results = await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            task.cancel()

    return (
        LevelResult(
            workers=len(workers),
            claimed_ids=[job_id for r in results for job_id in r[0]],
            seconds=sum(s.seconds for s in segments),
            latencies_ms=[ms for r in results for ms in r[1]],
            productive_calls=sum(r[2] for r in results),
            empty_calls=sum(r[3] for r in results),
            segments=len(segments),
            rows_matched=rows_matched,
            drained=all(s.drained for s in segments),
        ),
        expected,
    )


# --- reporting --------------------------------------------------------------


def _line(label: str, value: object) -> str:
    return f"  {label:<44}{value}"


def _warn(dsn: str, jobs: int, levels: list[int], sizes: list[int]) -> None:
    print()
    print("=" * 72)
    print("  BENCHMARK -- THIS WRITES TO A DATABASE")
    print("=" * 72)
    print(_line("database", dsn))
    print(_line("creates", "one throwaway organization, project and queue,"))
    print(_line("", f"plus {max(levels)} workers -- all deleted again on exit"))
    print(_line("inserts", f"{jobs} jobs per level x {len(levels)} levels"))
    print(_line("", f"in {len(sizes)} re-seeded segment(s) of <= {max(sizes)}"))
    print(_line("also runs", "VACUUM (ANALYZE) jobs, job_executions"))
    print(_line("does NOT touch", "any pre-existing org, queue or job"))
    print("=" * 72)
    print()


def _report(
    dsn: str,
    jobs: int,
    batch_size: int,
    sizes: list[int],
    results: list[tuple[LevelResult, bool]],
) -> None:
    print()
    print("=" * 72)
    print("  CLAIM THROUGHPUT")
    print("=" * 72)
    print(_line("database", dsn))
    print(_line("backlog per level", f"{jobs} queued jobs, re-seeded for every level"))
    print(_line("claim batch size", batch_size))
    print(_line("queue max_concurrency", f"{MAX_CONCURRENCY}  (the schema ceiling)"))
    print(
        _line(
            "drained in",
            f"{len(sizes)} timed segment(s) of <= {max(sizes)}, re-seeded off the clock",
        )
    )
    print(_line("", "so headroom always exceeds the work still queued:"))
    print(_line("", "the concurrency cap is never the bottleneck"))
    print(_line("timed", "lock_queue.sql + claim_jobs.sql + COMMIT"))
    print(_line("not timed", "handler execution -- no job is run"))
    print(_line("connections", "NullPool, one dedicated backend per claimer"))
    print()
    print(f"  {'workers':>7}  {'claimed':>7}  {'seconds':>7}  {'claims/s':>9}"
          f"  {'p50 ms':>7}  {'p95 ms':>7}  {'batches':>7}  {'disjoint':>8}")
    print(f"  {'-' * 7}  {'-' * 7}  {'-' * 7}  {'-' * 9}"
          f"  {'-' * 7}  {'-' * 7}  {'-' * 7}  {'-' * 8}")
    for r, ok in results:
        print(
            f"  {r.workers:>7}  {r.claimed:>7}  {r.seconds:>7.2f}  {r.claims_per_sec:>9.1f}"
            f"  {r.pct(0.50):>7.2f}  {r.pct(0.95):>7.2f}  {r.productive_calls:>7}"
            f"  {'PASS' if ok else 'FAIL':>8}"
        )
    print()

    base = results[0][0].claims_per_sec if results else 0.0
    if base > 0 and len(results) > 1:
        print("  throughput vs 1 claimer (linear scaling would be 2.00x at 2, 8.00x at 8):")
        for r, _ in results:
            bar = "#" * min(60, int(round(r.claims_per_sec / base * 8)))
            print(f"    {r.workers:>3}  {r.claims_per_sec / base:>5.2f}x  {bar}")
        print()

    for r, ok in results:
        if ok:
            continue
        print(f"  [FAIL] {r.workers} claimer(s):")
        if not r.drained:
            print(f"         a segment never drained -- {r.claimed}/{jobs} claimed before timeout")
        if r.duplicates:
            print(f"         {r.duplicates} job(s) claimed more than once"
                  " -- SKIP LOCKED did not hold")
        if r.unique != jobs and r.drained:
            print(f"         union of claimers is {r.unique} ids, expected {jobs}")
        if not r.rows_matched:
            print("         jobs/job_executions disagree with what the claimers reported")

    print("  Read the throughput figures only if every disjointness check says PASS.")
    print("  A claim path that double-claims can be made arbitrarily fast.")
    print()
    print("  Caveat, by construction: claimed jobs are never completed here, so the")
    print("  in-flight count claim_jobs.sql aggregates for headroom climbs from 0 to a")
    print("  full segment as that segment drains. A real worker completes jobs and holds")
    print("  that count near its concurrency, so these are floor figures. Every level")
    print("  pays it identically, so the shape of the curve is unaffected.")
    print("=" * 72)
    print()


# --- entrypoint -------------------------------------------------------------


async def main_async(args: argparse.Namespace) -> int:
    dsn = args.database_url or str(get_settings().database_url)
    levels = [int(x) for x in args.levels.split(",") if x.strip()]
    if not levels or any(n < 1 for n in levels):
        raise SystemExit(f"--levels must be positive integers, got {args.levels!r}")
    if args.jobs < 1:
        raise SystemExit("--jobs must be >= 1")
    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")

    sizes = split_backlog(args.jobs)
    _warn(dsn, args.jobs, levels, sizes)

    # NullPool: every connect() is a genuinely separate Postgres backend. A shared
    # pool would serialise the claimers at the pool, and the measurement would be of
    # SQLAlchemy rather than of the claim path.
    engine = create_async_engine(dsn, poolclass=NullPool)
    scope: Scope | None = None
    try:
        scope = await create_scope(engine)
        workers = await register_workers(engine, scope.org_id, max(levels))

        results: list[tuple[LevelResult, bool]] = []
        for n in levels:
            print(f"  level {n:>3} claimer(s) ... ", end="", flush=True)
            result, expected = await run_level(
                engine, scope, workers[:n], sizes, args.batch_size, args.timeout
            )
            ok = result.ok(expected)
            print(
                f"{result.claimed:>7} claimed in {result.seconds:>6.2f}s "
                f"({result.claims_per_sec:>8.1f}/s, p50 {result.pct(0.50):.2f}ms)"
                f"{'' if ok else '   ** DISJOINTNESS FAILED **'}"
            )
            results.append((result, ok))

        _report(dsn, args.jobs, args.batch_size, sizes, results)
        return 0 if all(ok for _, ok in results) else 1
    finally:
        if scope is not None:
            await cleanup(engine, scope)
            print(f"  cleaned up throwaway organization {scope.org_id}")
        await engine.dispose()


def main() -> None:
    p = argparse.ArgumentParser(
        prog="bench_claim",
        description="Measure claims/second as claimer concurrency scales against a fixed backlog.",
    )
    p.add_argument("--jobs", type=int, default=DEFAULT_JOBS, help="backlog size per level")
    p.add_argument("--levels", default=DEFAULT_LEVELS, help="comma-separated claimer counts")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH, help="jobs per claim call")
    p.add_argument("--timeout", type=float, default=120.0, help="seconds before a segment aborts")
    p.add_argument("--database-url", default=None, help="override the configured asyncpg DSN")
    raise SystemExit(asyncio.run(main_async(p.parse_args())))


if __name__ == "__main__":
    main()
