"""The five scheduler loops: promoter, cron dispatcher, lease reaper, dead-worker
sweep, retention sweep.

Two rules shape every loop in this file.

**Correctness never depends on running exactly one scheduler.** Every statement is
``FOR UPDATE SKIP LOCKED`` and every cron occurrence is protected by
``ux_jobs_schedule_occurrence``, so N schedulers partition the work rather than
collide. ``pg_try_advisory_xact_lock`` appears only to stop redundant ticks burning
CPU -- if it is never acquired, the system is still correct. The transaction-scoped
variant is used deliberately: a session-scoped lock leaks onto a pooled connection
and has to be released by hand on every error path, and a scheduler that crashes
mid-tick would wedge every other scheduler until its connection was recycled.

**Every tick writes its own row in ``system_state``.** The API is a different
process, so an in-process gauge is invisible to ``/system/status``. That table is
what turns the system's biggest silent failure -- a dead promoter, which stalls
every delayed job and every backoff retry while immediate jobs keep flowing -- into
a number on the dashboard. The tick is recorded in its own session, never the work
session: an aborted work transaction cannot also report that it aborted.
"""

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import structlog
from croniter import croniter  # type: ignore[import-untyped]
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.base import uuid7
from app.services.claim import load_sql

log = structlog.get_logger()

# Advisory-lock namespace. Two int4 arguments: (CLASS, per-loop ordinal), so these
# locks can never be confused with an application lock taken elsewhere.
ADVISORY_CLASS = 0x0C0D17

# Not in Settings: app/config.py is owned elsewhere. Overridable per instance.
DEAD_WORKER_INTERVAL_MS = 15_000
RETENTION_INTERVAL_MS = 60_000
# A worker heartbeats every lease_seconds / 6; the shortest sane lease is 10s, so
# 90s of silence is many missed beats on any queue, not one unlucky GC pause.
DEAD_WORKER_TIMEOUT_SEC = 90
# Batched so a single tick never takes a long lock or a long transaction.
RETENTION_BATCH = 5_000
JOB_RETENTION_DAYS = 30
STATS_RETENTION_DAYS = 30
WORKER_CORPSE_HOURS = 24
CRON_BATCH = 100
# Cap on repeated same-tick re-entry when a loop is saturated, so draining a
# backlog cannot starve the other loops in this process.
MAX_EAGER_ROUNDS = 20


@dataclass
class Tick:
    """What one tick did. ``saturated`` means the batch came back full, so the loop
    re-runs immediately instead of sleeping -- a 10k-job backlog drains in seconds
    rather than at one batch per interval."""

    counts: dict[str, int] = field(default_factory=dict)
    saturated: bool = False
    skipped: bool = False


_RECORD_TICK = text(
    """
    INSERT INTO system_state (name, last_run_at, last_error, updated_at)
    VALUES (:name, now(), CAST(:err AS text), now())
    ON CONFLICT (name) DO UPDATE
       SET last_run_at = now(),
           last_error  = EXCLUDED.last_error,
           updated_at  = now()
    """
)


class SchedulerLoop:
    """One loop: acquire the advisory lock, do a bounded unit of work, record the
    tick, sleep. Subclasses implement :meth:`tick` only."""

    name = "loop"
    ordinal = 0

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        interval_ms: int,
        batch: int = 200,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.interval = interval_ms / 1000
        self.batch = batch
        self._consecutive_failures = 0

    async def tick(self, session: AsyncSession) -> Tick:
        raise NotImplementedError

    async def run_once(self) -> Tick:
        """One transaction: try the lock, work, commit. A lock we do not get means
        another scheduler is already doing this work -- not an error."""
        async with self.sessionmaker() as session, session.begin():
            got = await session.scalar(
                text("SELECT pg_try_advisory_xact_lock(:cls, :ord)"),
                {"cls": ADVISORY_CLASS, "ord": self.ordinal},
            )
            if not got:
                return Tick(skipped=True)
            return await self.tick(session)

    async def record(self, error: str | None) -> None:
        """Own session, always. If the work transaction aborted, it cannot be the
        thing that reports the abort."""
        async with self.sessionmaker() as session, session.begin():
            await session.execute(_RECORD_TICK, {"name": self.name, "err": error})

    def _retry_delay(self) -> float:
        """Postgres is down or restarting. Back off to 30s rather than spinning a
        failing transaction every interval."""
        return float(min(30.0, self.interval * (2**self._consecutive_failures)))

    async def run(self, stop: asyncio.Event) -> None:
        log.info("scheduler.loop_started", loop=self.name, interval_s=self.interval)
        eager_rounds = 0
        while not stop.is_set():
            started = time.monotonic()
            try:
                result = await self.run_once()
                # Recorded even when the advisory lock was not acquired: the row
                # reports THIS process's liveness, and a scheduler that keeps losing
                # a race is alive. The work itself was done by whoever won it.
                await self.record(None)
                self._consecutive_failures = 0
            except Exception as exc:  # noqa: BLE001 - a loop must outlive one bad tick
                self._consecutive_failures += 1
                log.error(
                    "scheduler.tick_failed",
                    loop=self.name,
                    error_class=type(exc).__name__,
                    error=str(exc)[:500],
                    consecutive=self._consecutive_failures,
                )
                # Best effort: if the database is unreachable this fails too, and
                # the staleness of last_run_at is itself the signal.
                with contextlib.suppress(Exception):
                    await self.record(f"{type(exc).__name__}: {exc}"[:500])
                delay = self._retry_delay()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                continue

            if result.counts and any(result.counts.values()):
                log.info(
                    "scheduler.tick",
                    loop=self.name,
                    duration_ms=int((time.monotonic() - started) * 1000),
                    **result.counts,
                )

            if result.saturated and eager_rounds < MAX_EAGER_ROUNDS:
                eager_rounds += 1
                continue
            eager_rounds = 0
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.interval)
        log.info("scheduler.loop_stopped", loop=self.name)


class PromoterLoop(SchedulerLoop):
    """scheduled -> queued once run_at has arrived.

    The only thing that makes a delayed job, a cron occurrence, or a backoff retry
    eligible: the claim query has no run_at predicate, so 'queued' means "due now"
    and this loop is what establishes that invariant.
    """

    name = "promoter"
    ordinal = 1

    async def tick(self, session: AsyncSession) -> Tick:
        rows = (await session.execute(text(load_sql("promote_due")), {"batch": self.batch})).all()
        return Tick(counts={"promoted": len(rows)}, saturated=len(rows) >= self.batch)


class ReaperLoop(SchedulerLoop):
    """Reclaim expired leases: the recovery path for kill -9, SIGSTOP, and network
    partitions.

    Touches jobs and job_executions only. The workers table is swept by a separate
    loop in a separate transaction -- holding jobs locks while taking workers locks
    deadlocks against a heartbeat that took workers first, and the victim Postgres
    prefers is the lagging worker, which manufactures the very lease expiry this is
    trying to repair.
    """

    name = "reaper"
    ordinal = 2

    async def tick(self, session: AsyncSession) -> Tick:
        rows = (await session.execute(text(load_sql("reap_leases")), {"batch": self.batch})).all()
        dead_lettered = sum(1 for r in rows if r.status == "dead_letter")
        return Tick(
            counts={
                "reclaimed": len(rows) - dead_lettered,
                "dead_lettered": dead_lettered,
                "executions_closed": rows[0].executions_closed if rows else 0,
            },
            saturated=len(rows) >= self.batch,
        )


_SELECT_DUE_SCHEDULES = text(
    """
    SELECT s.id, s.organization_id, s.project_id, s.queue_id,
           s.cron, s.timezone, s.next_occurrence_at,
           s.catchup_policy, s.max_catchup_occurrences,
           s.handler, s.payload, s.priority, s.max_attempts, s.timeout_ms,
           q.is_paused, q.visibility_timeout_sec, q.default_timeout_ms,
           COALESCE(rp.strategy, 'exponential') AS backoff_strategy,
           COALESCE(rp.base_ms, 1000)           AS backoff_base_ms,
           COALESCE(rp.max_ms, 300000)          AS backoff_max_ms,
           now() AS db_now
      FROM job_schedules s
      JOIN queues q ON q.id = s.queue_id
      LEFT JOIN retry_policies rp ON rp.id = q.retry_policy_id
     WHERE s.is_active
       AND s.deleted_at IS NULL
       AND s.next_occurrence_at IS NOT NULL
       AND s.next_occurrence_at <= now()
     ORDER BY s.next_occurrence_at
     LIMIT CAST(:batch AS int)
       FOR UPDATE OF s SKIP LOCKED
    """
)

# Every NOT NULL column without a server_default is supplied explicitly: this is a
# raw-SQL INSERT, so nothing the ORM would have defaulted is applied.
#
# ON CONFLICT names the partial index's predicate as well as its columns --
# inference against a partial unique index requires it, and without the WHERE this
# statement raises 42P10 instead of suppressing the duplicate.
_INSERT_OCCURRENCE = text(
    """
    INSERT INTO jobs (
        id, organization_id, project_id, queue_id, schedule_id,
        kind, handler, status, priority,
        run_at, scheduled_for, payload,
        attempt, max_attempts, backoff_strategy, backoff_base_ms, backoff_max_ms,
        unregistered_count, timeout_ms, lease_seconds, lease_epoch,
        cancel_requested, lock_version, correlation_id, created_at, updated_at
    ) VALUES (
        CAST(:id AS uuid), CAST(:organization_id AS uuid), CAST(:project_id AS uuid),
        CAST(:queue_id AS uuid), CAST(:schedule_id AS uuid),
        'recurring', :handler, 'scheduled', :priority,
        CAST(:scheduled_for AS timestamptz), CAST(:scheduled_for AS timestamptz),
        CAST(:payload AS jsonb),
        0, :max_attempts, :backoff_strategy, :backoff_base_ms, :backoff_max_ms,
        0, :timeout_ms, :lease_seconds, 0,
        false, 0, :correlation_id, now(), now()
    )
    ON CONFLICT (schedule_id, scheduled_for) WHERE schedule_id IS NOT NULL
    DO NOTHING
    RETURNING id
    """
)

# Driven by the SELECTED row, never by the INSERT's RETURNING set. If a prior tick
# committed the job but crashed before advancing, or an operator used "run now", the
# insert is suppressed and RETURNING is empty -- advancing only on returned rows
# would freeze next_occurrence_at, so the schedule re-selects, re-conflicts, and
# does nothing forever. A permanently dead schedule, once per second.
_ADVANCE_SCHEDULE = text(
    """
    UPDATE job_schedules
       SET last_occurrence_at   = CAST(:last_occurrence_at AS timestamptz),
           next_occurrence_at   = CAST(:next_occurrence_at AS timestamptz),
           skipped_occurrences  = skipped_occurrences + CAST(:skipped AS int),
           updated_at           = now()
     WHERE id = CAST(:id AS uuid)
    """
)


def _zone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError):
        log.warning("scheduler.unknown_timezone", timezone=name)
        return ZoneInfo("UTC")


def next_occurrence(cron: str, tz: ZoneInfo, after: datetime) -> datetime:
    """The occurrence strictly after ``after``, computed in the schedule's own zone.

    croniter's timezone-aware iteration is what makes DST correct: a 02:30 daily job
    is 02:30 local on both sides of the transition, not a fixed UTC offset.
    """
    it = croniter(cron, after.astimezone(tz))
    nxt: datetime = it.get_next(datetime)
    return nxt.astimezone(UTC)


def plan_occurrences(
    cron: str,
    timezone: str,
    nominal: datetime,
    now: datetime,
    catchup_policy: str,
    max_catchup: int,
) -> tuple[list[datetime], datetime, int]:
    """Return (occurrences to materialise, new next_occurrence_at, missed count).

    Every occurrence is computed from the previous **nominal** instant, never from
    now(), so a slow tick cannot shift the schedule -- a per-minute job that ticks
    900ms late still fires at :00, not at :00.9 and then :01.9.
    """
    tz = _zone(timezone)
    fire = [nominal]
    missed = 0
    cursor = next_occurrence(cron, tz, nominal)

    if catchup_policy == "catchup":
        while cursor <= now and len(fire) < max_catchup:
            fire.append(cursor)
            cursor = next_occurrence(cron, tz, cursor)

    # Whatever remains behind now() after the policy has had its say is a missed
    # occurrence: advance past it rather than firing a backlog on restart.
    while cursor <= now:
        missed += 1
        cursor = next_occurrence(cron, tz, cursor)

    return fire, cursor, missed


class CronLoop(SchedulerLoop):
    """Materialise cron occurrences into jobs.

    Occurrences are computed by croniter in Python rather than in SQL: DST, named
    timezones, and catch-up policy are all things Postgres would have to be taught,
    and the schedule's own zone is stored per row.

    Exactly-once promotion under N schedulers is ``ux_jobs_schedule_occurrence``,
    not the advisory lock.
    """

    name = "cron"
    ordinal = 3

    async def tick(self, session: AsyncSession) -> Tick:
        rows = (await session.execute(_SELECT_DUE_SCHEDULES, {"batch": self.batch})).all()
        inserted = suppressed = skipped_paused = missed_total = 0

        for r in rows:
            now: datetime = r.db_now
            nominal: datetime = r.next_occurrence_at
            fire, next_at, missed = plan_occurrences(
                r.cron, r.timezone, nominal, now, r.catchup_policy, r.max_catchup_occurrences
            )
            missed_total += missed

            if r.is_paused:
                # Pause blocks admission. Materialising into a paused queue would let
                # a per-minute schedule silently accumulate a backlog that stampedes
                # the instant someone resumes it.
                skipped_paused += len(fire)
            else:
                # A job may never outlive its own lease (ck_jobs_timeout_lt_lease).
                timeout_ms = min(r.timeout_ms, r.visibility_timeout_sec * 1000 - 1)
                for occurrence in fire:
                    row = (
                        await session.execute(
                            _INSERT_OCCURRENCE,
                            {
                                "id": str(uuid7()),
                                "organization_id": str(r.organization_id),
                                "project_id": str(r.project_id),
                                "queue_id": str(r.queue_id),
                                "schedule_id": str(r.id),
                                "handler": r.handler,
                                "priority": r.priority,
                                "scheduled_for": occurrence,
                                "payload": json.dumps(r.payload or {}),
                                "max_attempts": r.max_attempts,
                                "backoff_strategy": r.backoff_strategy,
                                "backoff_base_ms": r.backoff_base_ms,
                                "backoff_max_ms": r.backoff_max_ms,
                                "timeout_ms": timeout_ms,
                                # lease_seconds is snapshotted from the queue: the
                                # queue owns lease length, and changing it later must
                                # not retroactively alter occurrences already created.
                                "lease_seconds": r.visibility_timeout_sec,
                                "correlation_id": f"schedule-{r.id}",
                            },
                        )
                    ).first()
                    if row is None:
                        suppressed += 1
                    else:
                        inserted += 1

            await session.execute(
                _ADVANCE_SCHEDULE,
                {
                    "id": str(r.id),
                    "last_occurrence_at": fire[-1],
                    "next_occurrence_at": next_at,
                    "skipped": len(fire) if r.is_paused else 0,
                },
            )

        return Tick(
            counts={
                "schedules": len(rows),
                "jobs_created": inserted,
                "occurrences_suppressed": suppressed,
                "paused_skipped": skipped_paused,
                "missed_skipped": missed_total,
            },
            saturated=len(rows) >= self.batch,
        )


# COALESCE, because a worker that registered and died before its first beat has a
# NULL last_heartbeat_at and would otherwise stay 'active' forever.
_SWEEP_DEAD_WORKERS = text(
    """
    UPDATE workers
       SET status     = 'dead',
           updated_at = now()
     WHERE status IN ('starting', 'active', 'draining')
       AND COALESCE(last_heartbeat_at, started_at, created_at)
           < now() - make_interval(secs => CAST(:timeout AS int))
    RETURNING id, name
    """
)


class DeadWorkerLoop(SchedulerLoop):
    """Mark silent workers dead.

    Its own loop and its own transaction, deliberately separate from the reaper.
    Combining them means holding jobs locks while taking workers locks, which
    deadlocks against a heartbeat that took workers first and then blocked on jobs.

    This loop does not touch the jobs those workers held: the reaper reclaims them
    from ``lease_expires_at``, which is the single authority on ownership. Marking a
    worker dead is a dashboard fact, not a recovery action -- and a worker that was
    merely slow revives itself on its next heartbeat.
    """

    name = "dead_worker"
    ordinal = 4

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        interval_ms: int = DEAD_WORKER_INTERVAL_MS,
        batch: int = 200,
        timeout_sec: int = DEAD_WORKER_TIMEOUT_SEC,
    ) -> None:
        super().__init__(sessionmaker, interval_ms, batch)
        self.timeout_sec = timeout_sec

    async def tick(self, session: AsyncSession) -> Tick:
        rows = (await session.execute(_SWEEP_DEAD_WORKERS, {"timeout": self.timeout_sec})).all()
        for r in rows:
            log.warning("scheduler.worker_declared_dead", worker=r.name, worker_id=str(r.id))
        return Tick(counts={"workers_dead": len(rows)})


# Retention. Each statement is bounded by LIMIT and SKIP LOCKED so a tick is short
# and never blocks the hot path.
_PURGE_LOGS = text(
    """
    WITH doomed AS (
        SELECT l.id
          FROM job_logs l
          JOIN jobs j   ON j.id = l.job_id
          JOIN queues q ON q.id = j.queue_id
         WHERE l.logged_at < now() - make_interval(days => q.log_retention_days)
         LIMIT CAST(:batch AS int)
           FOR UPDATE OF l SKIP LOCKED
    )
    DELETE FROM job_logs l USING doomed d WHERE l.id = d.id RETURNING l.id
    """
)

_PURGE_EXECUTIONS = text(
    """
    WITH doomed AS (
        SELECT e.id
          FROM job_executions e
          JOIN queues q ON q.id = e.queue_id
         WHERE e.finished_at IS NOT NULL
           AND e.finished_at < now() - make_interval(days => q.log_retention_days)
         LIMIT CAST(:batch AS int)
           FOR UPDATE OF e SKIP LOCKED
    )
    DELETE FROM job_executions e USING doomed d WHERE e.id = d.id RETURNING e.id
    """
)

# The guard is what stops retention destroying evidence. dead_letter_entries.job_id
# is ON DELETE RESTRICT precisely so an operator's workflow record outlives the
# sweep -- which also means the guard must exclude any job carrying an entry at all,
# not merely an unresolved one: a RESTRICT violation aborts the whole tick, so a
# single resolved entry would otherwise stop retention running ever again.
_PURGE_JOBS = text(
    """
    WITH doomed AS (
        SELECT j.id
          FROM jobs j
         WHERE j.finished_at IS NOT NULL
           AND j.finished_at < now() - make_interval(days => CAST(:days AS int))
         ORDER BY j.finished_at
         LIMIT CAST(:batch AS int)
           FOR UPDATE SKIP LOCKED
    )
    DELETE FROM jobs j USING doomed d
     WHERE j.id = d.id
       AND NOT EXISTS (
           SELECT 1 FROM dead_letter_entries q WHERE q.job_id = j.id
       )
    RETURNING j.id
    """
)

_PURGE_STATS = text(
    """
    DELETE FROM queue_stats_minute
     WHERE bucket_start < date_trunc('minute', now())
                          - make_interval(days => CAST(:days AS int))
    RETURNING queue_id
    """
)

_PURGE_HEARTBEATS = text(
    """
    WITH doomed AS (
        SELECT id FROM worker_heartbeats
         WHERE beat_at < now() - make_interval(hours => CAST(:hours AS int))
         LIMIT CAST(:batch AS int)
           FOR UPDATE SKIP LOCKED
    )
    DELETE FROM worker_heartbeats h USING doomed d WHERE h.id = d.id RETURNING h.id
    """
)

# Otherwise every worker restart leaves a corpse and the Workers screen fills with
# dead rows during the demo. Guarded on held jobs so a row is never removed from
# under a job still pointing at it.
_PURGE_WORKERS = text(
    """
    DELETE FROM workers w
     WHERE w.status IN ('dead', 'stopped')
       AND COALESCE(w.stopped_at, w.last_heartbeat_at, w.updated_at)
           < now() - make_interval(hours => CAST(:hours AS int))
       AND NOT EXISTS (SELECT 1 FROM jobs j WHERE j.worker_id = w.id)
    RETURNING w.id
    """
)


class RetentionLoop(SchedulerLoop):
    """Bounded deletes of the volume tables, driven by queues.log_retention_days.

    Monthly declarative partitioning is the next step at real volume and is
    documented rather than implemented at this scale -- at this size a batched
    DELETE is both simpler and cheaper than a partition-maintenance job.
    """

    name = "retention"
    ordinal = 5

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        interval_ms: int = RETENTION_INTERVAL_MS,
        batch: int = RETENTION_BATCH,
        job_retention_days: int = JOB_RETENTION_DAYS,
    ) -> None:
        super().__init__(sessionmaker, interval_ms, batch)
        self.job_retention_days = job_retention_days

    async def tick(self, session: AsyncSession) -> Tick:
        counts: dict[str, int] = {}
        # Children before parents: job_logs cascades from job_executions, and both
        # must go before the jobs they belong to.
        counts["logs"] = len(
            (await session.execute(_PURGE_LOGS, {"batch": self.batch})).all()
        )
        counts["executions"] = len(
            (await session.execute(_PURGE_EXECUTIONS, {"batch": self.batch})).all()
        )
        counts["jobs"] = len(
            (
                await session.execute(
                    _PURGE_JOBS, {"batch": self.batch, "days": self.job_retention_days}
                )
            ).all()
        )
        counts["stats_buckets"] = len(
            (await session.execute(_PURGE_STATS, {"days": STATS_RETENTION_DAYS})).all()
        )
        counts["heartbeats"] = len(
            (
                await session.execute(
                    _PURGE_HEARTBEATS, {"batch": self.batch, "hours": WORKER_CORPSE_HOURS}
                )
            ).all()
        )
        counts["workers"] = len(
            (await session.execute(_PURGE_WORKERS, {"hours": WORKER_CORPSE_HOURS})).all()
        )
        saturated = counts["logs"] >= self.batch or counts["executions"] >= self.batch
        return Tick(counts=counts, saturated=saturated)


def build_loops(
    sessionmaker: async_sessionmaker[AsyncSession],
    promoter_interval_ms: int,
    cron_interval_ms: int,
    reaper_interval_ms: int,
    reaper_batch: int,
) -> dict[str, SchedulerLoop]:
    """All five loops, keyed by the name they use in ``system_state``."""
    loops: list[SchedulerLoop] = [
        PromoterLoop(sessionmaker, promoter_interval_ms, batch=500),
        CronLoop(sessionmaker, cron_interval_ms, batch=CRON_BATCH),
        ReaperLoop(sessionmaker, reaper_interval_ms, batch=reaper_batch),
        DeadWorkerLoop(sessionmaker),
        RetentionLoop(sessionmaker),
    ]
    return {loop.name: loop for loop in loops}


async def run_all(loops: dict[str, SchedulerLoop], stop: asyncio.Event) -> None:
    await asyncio.gather(*(loop.run(stop) for loop in loops.values()))


async def run_once_all(loops: dict[str, SchedulerLoop]) -> dict[str, dict[str, Any]]:
    """One tick of each loop, in order. Used by --once and by tests that want a
    deterministic scheduler pass without a background task."""
    results: dict[str, dict[str, Any]] = {}
    for name, loop in loops.items():
        tick = await loop.run_once()
        await loop.record(None)
        results[name] = {"skipped": tick.skipped, **tick.counts}
    return results
