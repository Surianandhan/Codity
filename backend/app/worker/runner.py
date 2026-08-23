"""The claim -> execute -> complete loop, with the reliability core wired in.

What the worker guarantees, and where each guarantee lives:

* **No double execution.** ``claimed -> running`` is a guarded write (``start_job``);
  zero rows means the claim is no longer ours -- graceful shutdown released it, or
  the reaper took it -- and the handler is never invoked. Every subsequent write
  carries ``lease_epoch``, so a merely *slow* worker whose lease was stolen has its
  late completion rejected instead of committed over the live attempt.
* **A ``kill -9`` costs one attempt, not a job.** Leases are renewed by a heartbeat
  every ``lease_seconds / 6``; silence lets the reaper reclaim.
* **A SIGTERM costs nothing for work that never started.** Claims that have not
  reached ``running`` are released back to ``queued``; ``attempt`` increments at
  ``claimed -> running``, so nothing needs to be decremented and monotonicity holds.
* **Failures back off; exhausted budgets dead-letter.** Both decisions are made by
  Postgres inside ``fail_job.sql``. This module never computes a timestamp, so no
  worker's clock can move a retry.
* **A handler this build does not have is a bounded cost.** The job is released to
  ``scheduled`` with a backoff rather than to an immediately-claimable ``queued``,
  and ``unregistered_count`` -- read, not merely written -- dead-letters it once a
  rolling deploy has had ``UNREGISTERED_MAX_RELEASES`` chances to bring the handler.
* **The beat follows the leases held, not the queues polled.** Sizing it from the
  claimable queues lets an operator pausing a short-lease queue expire the lease of
  a job already running on it, which is a double *execution* that fencing cannot
  undo -- only a double *commit* is fencing's to prevent.

Cancel and drain ride the heartbeat's ``RETURNING`` clause -- no extra query on the
hot path. ``ctx.cancelled()`` is the cooperative half; a handler that ignores it is
``task.cancel()``ed one grace period later, which is the half that makes the API
endpoint mean something.
"""

import asyncio
import contextlib
import math
import os
import random
import signal
import socket
from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import Text, bindparam, select, text, update
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.base import uuid7
from app.db.models.execution import Worker
from app.db.models.scheduling import Job, Queue
from app.domain.enums import JobStatus, WorkerStatus
from app.domain.errors import PermanentError
from app.services.claim import ClaimedJob, claim_jobs, complete_job, start_job
from app.services.reliability import LeaseRenewal, heartbeat
from app.services.reliability import fail_job as fail_job_stmt
from app.worker.handlers import REGISTRY, JobContext
from app.worker.logsink import LogSink, attach_logger

log = structlog.get_logger()

# A heartbeat that fires faster than this buys nothing and costs a round trip per
# job-holding worker; the floor matters only for absurdly short leases in tests.
MIN_HEARTBEAT_INTERVAL = 0.5
# Used until the worker has seen its queues: the shortest lease the schema permits
# is 10s, so this is deliberately conservative rather than optimistic.
DEFAULT_LEASE_SECONDS = 30

# How many times one job may be claimed by a worker that does not know its handler
# before the job is dead-lettered, and the backoff between those releases. Both are
# READ by _RELEASE_UNREGISTERED below -- a counter nothing reads guards nothing.
# 5s then 10s gives a rolling deploy room to finish; the third release gives up.
UNREGISTERED_MAX_RELEASES = 3
UNREGISTERED_BACKOFF_SEC = 5
UNREGISTERED_BACKOFF_MAX_SEC = 300

# The open execution row for the attempt we are running. Pinned by lease_epoch, not
# by "most recent": a reaped-then-reclaimed job has more than one execution row and
# only the one carrying our epoch is ours to log against.
_OPEN_EXECUTION = text(
    "SELECT id FROM job_executions"
    " WHERE job_id = CAST(:jid AS uuid) AND lease_epoch = CAST(:ep AS bigint)"
    " AND finished_at IS NULL"
    " ORDER BY id DESC LIMIT 1"
)

# running -> cancelled. Fenced like every other write this worker makes. Not part of
# fail_job.sql because a cancellation is not a failed attempt: it consumes no retry
# budget, produces no DLQ entry, and is terminal on the first request.
_CANCEL = text(
    """
    WITH cancelled AS (
        UPDATE jobs
           SET status           = 'cancelled',
               finished_at      = now(),
               worker_id        = NULL,
               claimed_at       = NULL,
               lease_expires_at = NULL,
               lease_epoch      = lease_epoch + 1,
               last_error_class = 'Cancelled',
               last_error_message = CAST(:reason AS text),
               updated_at       = now()
         WHERE id          = CAST(:jid AS uuid)
           AND worker_id   = CAST(:wid AS uuid)
           AND lease_epoch = CAST(:ep AS bigint)
           AND status      = 'running'
     RETURNING id
    )
    UPDATE job_executions e
       SET status      = 'cancelled',
           finished_at = now(),
           duration_ms = CASE
                             WHEN e.started_at IS NOT NULL
                             THEN (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int
                         END,
           error_class   = 'Cancelled',
           error_message = CAST(:reason AS text)
      FROM cancelled c
     WHERE e.job_id = c.id
       AND e.finished_at IS NULL
    RETURNING e.id
    """
)

# fail_job.sql closes the execution as 'failed' because that is the common case. A
# timeout is a distinct fact the timeline shows differently, and the execution row
# carrying THIS attempt's epoch is the only one relabelled.
_MARK_TIMED_OUT = text(
    "UPDATE job_executions SET status = 'timed_out'"
    " WHERE job_id = CAST(:jid AS uuid) AND lease_epoch = CAST(:ep AS bigint)"
    " AND status = 'failed'"
)

# The handler is not in THIS worker's registry (mixed deployment / rolling upgrade).
# Release without consuming an attempt -- the job provably never ran -- but do three
# things the previous version of this block did not:
#
# 1. FENCE AND GATE THE EXECUTION CLOSE. The close is driven by the jobs UPDATE's
#    RETURNING set and carries the same lease_epoch, exactly like shutdown()'s
#    release/close pair. An ungated `WHERE job_id = :jid AND finished_at IS NULL`
#    closes whatever open row exists -- and after this worker stalls, is reaped, and
#    the job is re-claimed, that row is the LIVE one belonging to the worker now
#    running the job. Its successful work would render as a lost attempt with
#    error_class 'UnregisteredHandler', which is a fabricated failure on another
#    worker's row.
# 2. RELEASE TO 'scheduled', NOT 'queued'. The claim query has no run_at predicate --
#    that is what keeps ix_jobs_claim small -- so 'queued' means "due now" and a job
#    parked there is re-claimed on the next ~500ms poll. Same reasoning as
#    fail_job.sql's retry branch.
# 3. MAKE unregistered_count MEAN SOMETHING. The counter is read back here: the
#    release that pushes it to UNREGISTERED_MAX_RELEASES dead-letters the job instead
#    of re-queueing it. Without that terminating condition, a job whose handler no
#    deployed worker knows is an infinite claim/release loop across the whole fleet.
#    finished_at is set on that branch because ck_jobs_terminal_finished makes
#    terminal-status and finished_at equivalent in BOTH directions -- omitting it does
#    not lose a timestamp, it aborts the transaction.
_RELEASE_UNREGISTERED = text(
    """
    WITH released AS (
        UPDATE jobs j
           SET unregistered_count = j.unregistered_count + 1,
               status = CASE
                            WHEN j.unregistered_count + 1 >= CAST(:threshold AS int)
                            THEN 'dead_letter'::job_status
                            ELSE 'scheduled'::job_status
                        END,
               run_at = CASE
                            WHEN j.unregistered_count + 1 >= CAST(:threshold AS int)
                            THEN j.run_at
                            ELSE now() + make_interval(secs => LEAST(
                                     CAST(:backoff_sec AS numeric)
                                     * power(2::numeric, j.unregistered_count::numeric),
                                     CAST(:backoff_max_sec AS numeric)))
                        END,
               finished_at = CASE
                                 WHEN j.unregistered_count + 1 >= CAST(:threshold AS int)
                                 THEN now()
                             END,
               worker_id        = NULL,
               claimed_at       = NULL,
               lease_expires_at = NULL,
               lease_epoch      = j.lease_epoch + 1,
               last_error_class   = 'UnregisteredHandler',
               last_error_message = CAST(:message AS text),
               updated_at       = now()
         WHERE j.id          = CAST(:jid AS uuid)
           AND j.worker_id   = CAST(:wid AS uuid)
           AND j.lease_epoch = CAST(:ep AS bigint)
           AND j.status      = 'running'
     RETURNING j.id, j.organization_id, j.project_id, j.queue_id, j.correlation_id,
               j.payload, j.status, j.unregistered_count, j.run_at
    ),
    closed AS (
        UPDATE job_executions e
           SET status      = 'lost',
               finished_at = now(),
               duration_ms = CASE
                                 WHEN e.started_at IS NOT NULL
                                 THEN (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int
                             END,
               error_class   = 'UnregisteredHandler',
               error_message = CAST(:message AS text)
          FROM released r
         WHERE e.job_id      = r.id
           AND e.lease_epoch = CAST(:ep AS bigint)
           AND e.finished_at IS NULL
     RETURNING e.id
    ),
    dlq AS (
        INSERT INTO dead_letter_entries
            (organization_id, project_id, queue_id, job_id, correlation_id,
             error_class, error_message, error_fingerprint, payload_snapshot,
             dead_lettered_at)
        SELECT r.organization_id, r.project_id, r.queue_id, r.id, r.correlation_id,
               'UnregisteredHandler', CAST(:message AS text),
               md5('UnregisteredHandler'
                   || coalesce(substring(CAST(:message AS text) FOR 200), '')),
               r.payload, now()
          FROM released r
         WHERE r.status = 'dead_letter'
     RETURNING id
    )
    SELECT r.id                 AS job_id,
           r.status             AS status,
           r.unregistered_count AS unregistered_count,
           r.run_at             AS run_at,
           (SELECT count(*) FROM closed) AS executions_closed,
           (SELECT count(*) FROM dlq)    AS dlq_entries
      FROM released r
    """
)

# SIGTERM: hand back claims that never reached the handler. Free, because attempt
# increments at claimed -> running, so a released claim provably never executed and
# nothing has to be decremented -- the monotonicity rule is never even approached.
#
# `status = 'claimed'` is the guard, not just a filter: a task that reached start_job
# while shutdown was in flight owns a 'running' row and must NOT be yanked out from
# under a live handler. The two writes race deliberately, and exactly one wins --
# whichever loses sees zero rows and takes the other path.
#
# The ids bind is typed ARRAY(Text) and cast in SQL rather than passed as a '{a,b}'
# array literal: asyncpg binds parameters by type, so a literal string arrives as a
# str where a sequence is required and every release raises DataError -- silently
# stranding every unstarted claim until its lease expires, which is the one outcome
# graceful shutdown exists to avoid.
_RELEASE_UNSTARTED = text(
    "UPDATE jobs SET status='queued', worker_id=NULL, claimed_at=NULL,"
    " lease_expires_at=NULL, lease_epoch=lease_epoch+1, updated_at=now()"
    " WHERE id = ANY(CAST(:ids AS uuid[]))"
    " AND worker_id = CAST(:wid AS uuid) AND status='claimed'"
    " RETURNING id"
).bindparams(bindparam("ids", type_=ARRAY(Text)))

# Closes the job_executions row the claim opened, for exactly the jobs the release
# above actually won (never the full offered set -- a job that raced its own
# start_job to 'running' keeps its execution row open under the drain path below).
# Without this the row stays open with finished_at IS NULL, and the NEXT claim's
# execution INSERT hits ux_job_executions_open_one and raises 23505 forever: a
# released-but-never-run job becomes permanently unclaimable. This is the same
# class of bug the reaper's orphan-close CTE fixes for expired leases -- the
# release path needs its own copy because it is a different SQL statement.
_CLOSE_RELEASED_EXECUTIONS = text(
    "UPDATE job_executions SET status='lost', finished_at=now(),"
    " error_class='ReleasedBeforeStart'"
    " WHERE job_id = ANY(CAST(:ids AS uuid[])) AND finished_at IS NULL"
).bindparams(bindparam("ids", type_=ARRAY(Text)))


class WorkerRunner:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        organization_id: UUID,
        name: str | None = None,
        concurrency: int = 4,
        batch_size: int = 10,
        poll_interval_ms: int = 500,
        grace_seconds: int = 30,
        cancel_grace_seconds: int = 10,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.organization_id = organization_id
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.poll_interval = poll_interval_ms / 1000
        self.grace_seconds = grace_seconds
        self.cancel_grace_seconds = cancel_grace_seconds

        self.worker_id: UUID | None = None
        self.logsink = LogSink(sessionmaker)
        self._stopping = asyncio.Event()
        self._inflight: dict[UUID, asyncio.Task[None]] = {}
        # Claimed but not yet started: these can be released for free on shutdown,
        # because attempt only increments at claimed -> running.
        self._unstarted: dict[UUID, int] = {}
        # Cancel and drain, both learned from the heartbeat's RETURNING clause.
        self._contexts: dict[UUID, JobContext] = {}
        self._cancel_requested: set[UUID] = set()
        self._cancel_watch: dict[UUID, asyncio.Task[None]] = {}
        self._drain_requested = False
        # Two bounds on the beat, and the beat obeys the smaller of them.
        # _lease_seconds is the queue-side bound: it covers the gap between claiming
        # a job and the first heartbeat that sees it. _held_lease_seconds is the real
        # one -- the shortest lease this worker is ACTUALLY holding, refreshed from
        # heartbeat.sql's lease_expires_at on every beat, None when it holds nothing.
        self._lease_seconds = DEFAULT_LEASE_SECONDS
        self._held_lease_seconds: float | None = None
        self._heartbeat_stop = asyncio.Event()
        self._heartbeat_task: asyncio.Task[None] | None = None

    @property
    def heartbeat_interval(self) -> float:
        """``lease / 6``: five missed beats before a lease can expire.

        INVARIANT: **the interval is never wider than one sixth of the shortest lease
        this worker actually HOLDS.** It is not derived from the queues the worker
        might claim from, because those two sets diverge -- and every way they
        diverge is a reaping bug.

        The one that bites: queue A has ``visibility_timeout_sec`` 10, queue B has
        300, and the worker is running a job from A when an operator pauses A. A
        beat sized from the *unpaused* queues sees only B, widens to 50s, and the
        10s lease on the job in flight expires. The reaper then reclaims a job that
        is ACTIVELY RUNNING and a second worker executes it. ``lease_epoch`` fencing
        rejects the loser's COMMIT; it cannot un-send the emails the loser sent.
        (``jobs.lease_seconds`` is snapshotted at enqueue, so editing a queue's
        timeout splits the two sets the same way, without any pause involved.)
        """
        lease = float(self._lease_seconds)
        if self._held_lease_seconds is not None:
            lease = min(lease, self._held_lease_seconds)
        return max(MIN_HEARTBEAT_INTERVAL, lease / 6)

    # --- lifecycle ---
    async def register(self) -> UUID:
        async with self.sessionmaker() as s:
            existing = (
                await s.execute(
                    select(Worker).where(
                        Worker.organization_id == self.organization_id, Worker.name == self.name
                    )
                )
            ).scalar_one_or_none()
            now = datetime.now(UTC)
            if existing:
                existing.status = WorkerStatus.ACTIVE
                existing.started_at = now
                existing.last_heartbeat_at = now
                existing.drain_requested = False
                existing.pid = os.getpid()
                worker_id = existing.id
            else:
                w = Worker(
                    id=uuid7(),
                    organization_id=self.organization_id,
                    name=self.name,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    status=WorkerStatus.ACTIVE,
                    concurrency=self.concurrency,
                    started_at=now,
                    last_heartbeat_at=now,
                )
                s.add(w)
                await s.flush()
                worker_id = w.id
            await s.commit()
        self.worker_id = worker_id
        log.info("worker.registered", worker=self.name, worker_id=str(worker_id))
        return worker_id

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stopping.set)

    # --- main loop ---
    async def run(self) -> None:
        await self.register()
        self.install_signal_handlers()
        await self.logsink.start()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")
        try:
            while not self._stopping.is_set():
                if self._drain_requested:
                    # Drain: stop claiming, let what we hold finish. shutdown() waits.
                    log.info("worker.draining_stop_claiming", inflight=len(self._inflight))
                    break
                claimed = await self._claim_round()
                if not claimed:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self._stopping.wait(), timeout=self.poll_interval)
        finally:
            await self.shutdown()

    # --- heartbeat ---
    async def _heartbeat_loop(self) -> None:
        """Renew every lease this worker holds, and read the two control flags back.

        Runs until in-flight work has drained, NOT until the claim loop stops: a
        shutdown that stopped beating while a 20s handler finished would let its own
        lease expire and hand the job to the reaper it was trying to avoid.
        """
        while not self._heartbeat_stop.is_set():
            try:
                await self._heartbeat_once()
            except LookupError:
                # The workers row is gone (retention swept it, or the org went away).
                log.warning("worker.heartbeat_orphaned", worker=self.name)
                with contextlib.suppress(Exception):
                    await self.register()
            except Exception as exc:  # noqa: BLE001 - a lost beat must not kill the worker
                log.warning("worker.heartbeat_failed", worker=self.name, error=str(exc))
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._heartbeat_stop.wait(), timeout=self.heartbeat_interval
                )

    async def _heartbeat_once(self) -> None:
        assert self.worker_id is not None
        async with self.sessionmaker() as s:
            result = await heartbeat(s, self.worker_id)
            await s.commit()
        self._note_held_leases(result.leases)
        if result.drain_requested and not self._drain_requested:
            self._drain_requested = True
            log.info("worker.drain_requested", worker=self.name, inflight=len(self._inflight))
            await self._mark_draining()
        for job_id in result.cancelled_job_ids:
            self._observe_cancel(job_id)

    def _note_held_leases(self, leases: tuple[LeaseRenewal, ...]) -> None:
        """Re-derive the beat from the leases the database just renewed for us.

        Cleared when nothing is held: an idle worker has no lease to protect and
        falls back to the queue-side bound rather than pinning the beat forever at
        whatever the last job happened to need.

        The worker's clock appears here and nowhere else in this module, and it sizes
        an *interval* -- it never renews or expires anything, both of which stay
        entirely with the database's ``now()``. It is read AFTER the round trip
        rather than before so the elapsed time makes the estimate short, never long.
        """
        if not leases:
            self._held_lease_seconds = None
            return
        now = datetime.now(UTC)
        self._held_lease_seconds = min(
            (lease.lease_expires_at - now).total_seconds() for lease in leases
        )

    async def _mark_draining(self) -> None:
        assert self.worker_id is not None
        async with self.sessionmaker() as s:
            await s.execute(
                update(Worker)
                .where(Worker.id == self.worker_id)
                .values(status=WorkerStatus.DRAINING, updated_at=datetime.now(UTC))
            )
            await s.commit()

    # --- cooperative cancellation ---
    def _observe_cancel(self, job_id: UUID) -> None:
        if job_id in self._cancel_requested:
            return
        self._cancel_requested.add(job_id)
        ctx = self._contexts.get(job_id)
        if ctx is not None:
            ctx._cancelled = True
        log.info(
            "job.cancel_requested",
            job_id=str(job_id),
            grace_seconds=self.cancel_grace_seconds,
        )
        self._cancel_watch[job_id] = asyncio.create_task(
            self._enforce_cancel(job_id), name=f"cancel-{job_id}"
        )

    async def _enforce_cancel(self, job_id: UUID) -> None:
        """A handler that ignores ``ctx.cancelled()`` is not allowed to ignore cancel."""
        try:
            await asyncio.sleep(self.cancel_grace_seconds)
        except asyncio.CancelledError:
            return
        task = self._inflight.get(job_id)
        if task is not None and not task.done():
            log.warning(
                "job.cancel_forced", job_id=str(job_id), grace_seconds=self.cancel_grace_seconds
            )
            task.cancel()

    # --- claiming ---
    async def _queues(self) -> list[tuple[UUID, int]]:
        async with self.sessionmaker() as s:
            rows = (
                await s.execute(
                    select(Queue.id, Queue.priority, Queue.visibility_timeout_sec).where(
                        Queue.organization_id == self.organization_id,
                        Queue.is_paused.is_(False),
                    )
                )
            ).all()
        if rows:
            shortest = min(int(r[2]) for r in rows)
            # A newly claimed job can only come from an unpaused queue, so this bounds
            # the leases we are about to take on. It may only SHRINK while we hold
            # work: pausing a queue removes it from this list, and the lease on a job
            # already running did not get longer because an operator paused the queue
            # it came from. The beat that renews it must not slow down.
            self._lease_seconds = (
                min(self._lease_seconds, shortest) if self._inflight else shortest
            )
        return _weighted_order([(r[0], int(r[1])) for r in rows])

    async def _claim_round(self) -> int:
        free = self.concurrency - len(self._inflight)
        if free <= 0:
            await asyncio.sleep(0.01)
            return 0
        queues = await self._queues()
        if not queues:
            return 0
        # Cap each queue's share so one busy queue cannot consume the whole batch and
        # starve the rest of this worker's queues for a full polling cycle.
        share = math.ceil(self.batch_size / len(queues))
        total = 0
        for queue_id, _prio in queues:
            if self._stopping.is_set() or self._drain_requested or free <= 0:
                break
            async with self.sessionmaker() as s:
                assert self.worker_id is not None
                jobs = await claim_jobs(s, queue_id, self.worker_id, min(share, free))
                await s.commit()
            for cj in jobs:
                self._unstarted[cj.job_id] = cj.lease_epoch
                self._inflight[cj.job_id] = asyncio.create_task(self._execute(cj))
            free -= len(jobs)
            total += len(jobs)
        return total

    # --- execution ---
    async def _execute(self, cj: ClaimedJob) -> None:
        assert self.worker_id is not None
        execution_id: int | None = None
        try:
            async with self.sessionmaker() as s:
                # GUARDED: zero rows means shutdown released it or the reaper took
                # it. Another worker may already be running it -- do not invoke.
                if not await start_job(s, cj.job_id, self.worker_id, cj.lease_epoch):
                    await s.commit()
                    log.warning("job.start_lost", job_id=str(cj.job_id))
                    return
                await s.commit()
            self._unstarted.pop(cj.job_id, None)

            async with self.sessionmaker() as s:
                job = (await s.execute(select(Job).where(Job.id == cj.job_id))).scalar_one()
                handler_name, payload = job.handler, job.payload
                attempt, max_attempts = job.attempt, job.max_attempts
                timeout_s = job.timeout_ms / 1000
                correlation_id = job.correlation_id
                cancel_requested = job.cancel_requested
                execution_id = (
                    await s.execute(
                        _OPEN_EXECUTION, {"jid": str(cj.job_id), "ep": cj.lease_epoch}
                    )
                ).scalar()

            fn = REGISTRY.get(handler_name)
            if fn is None:
                await self._release_unregistered(cj, handler_name)
                return

            ctx = JobContext(
                job_id=cj.job_id,
                attempt=attempt,
                max_attempts=max_attempts,
                correlation_id=correlation_id,
            )
            if execution_id is not None:
                attach_logger(ctx, self.logsink.logger_for(execution_id, cj.job_id))
            self._contexts[cj.job_id] = ctx
            if cancel_requested or cj.job_id in self._cancel_requested:
                # The flag may have been set between claim and start; the heartbeat
                # would only tell us one beat later, and by then the handler is running.
                self._cancel_requested.add(cj.job_id)
                ctx._cancelled = True
                await self._mark_cancelled(cj, "cancelled before the handler was invoked")
                return

            structlog.contextvars.bind_contextvars(
                job_id=str(cj.job_id), attempt=attempt, correlation_id=correlation_id
            )
            self._emit(execution_id, cj.job_id, "info", "handler started", handler=handler_name)
            try:
                result = await asyncio.wait_for(fn(ctx, payload), timeout=timeout_s)
            finally:
                structlog.contextvars.unbind_contextvars("job_id", "attempt", "correlation_id")

            if ctx.cancelled():
                # It returned cooperatively, but it returned because it was asked to
                # stop -- recording that as 'completed' would make cancel a lie.
                await self._mark_cancelled(cj, "handler returned after cancellation")
                return

            async with self.sessionmaker() as s:
                ok = await complete_job(s, cj.job_id, self.worker_id, cj.lease_epoch, result)
                await s.commit()
            if ok:
                self._emit(execution_id, cj.job_id, "info", "handler completed")
                log.info("job.completed", job_id=str(cj.job_id))
            else:
                # Fenced out: our lease was stolen. Discard rather than commit a
                # stale outcome over the current attempt.
                log.warning("job.abandoned_stale_lease", job_id=str(cj.job_id))
        except asyncio.CancelledError:
            if cj.job_id not in self._cancel_requested:
                raise
            self._emit(execution_id, cj.job_id, "warning", "handler cancelled")
            with contextlib.suppress(Exception):
                await self._mark_cancelled(cj, "handler did not return within the grace period")
        except Exception as exc:  # noqa: BLE001 - the boundary must not leak
            self._emit(
                execution_id,
                cj.job_id,
                "error",
                str(exc)[:2000],
                error_class=type(exc).__name__,
            )
            await self._fail(cj, exc)
        finally:
            self._inflight.pop(cj.job_id, None)
            self._unstarted.pop(cj.job_id, None)
            self._contexts.pop(cj.job_id, None)
            self._cancel_requested.discard(cj.job_id)
            watch = self._cancel_watch.pop(cj.job_id, None)
            if watch is not None:
                watch.cancel()
            if execution_id is not None:
                self.logsink.release(execution_id)

    def _emit(
        self,
        execution_id: int | None,
        job_id: UUID,
        level: str,
        message: str,
        **context: object,
    ) -> None:
        if execution_id is not None:
            self.logsink.emit(execution_id, job_id, level, message, dict(context))

    async def _release_unregistered(self, cj: ClaimedJob, handler_name: str) -> None:
        """This worker does not know the handler (mixed deployment / rolling upgrade).

        Release without routing through ``fail_job``: a handler this worker happens not
        to have is not a failed attempt, so it consumes no retry budget and leaves no
        failure on the timeline. But release to a delayed ``scheduled``, not to an
        immediately-claimable ``queued``, and give up after
        ``UNREGISTERED_MAX_RELEASES``. See ``_RELEASE_UNREGISTERED`` for why each
        of those, and why the execution close is gated on this statement's own
        ``RETURNING`` set instead of on "the open row for this job".

        A ``None`` row means the fence rejected the write: the reaper already took the
        job back and another worker may own it. That is precisely the case where the
        old unfenced close corrupted the live attempt, so there is nothing to do here
        but say so.
        """
        assert self.worker_id is not None
        message = f"no handler registered for {handler_name!r}"
        async with self.sessionmaker() as s:
            row = (
                await s.execute(
                    _RELEASE_UNREGISTERED,
                    {
                        "jid": str(cj.job_id),
                        "wid": str(self.worker_id),
                        "ep": cj.lease_epoch,
                        "message": message,
                        "threshold": UNREGISTERED_MAX_RELEASES,
                        "backoff_sec": UNREGISTERED_BACKOFF_SEC,
                        "backoff_max_sec": UNREGISTERED_BACKOFF_MAX_SEC,
                    },
                )
            ).first()
            await s.commit()

        if row is None:
            log.warning(
                "job.unregistered_release_fenced_out",
                job_id=str(cj.job_id),
                handler=handler_name,
            )
        elif JobStatus(row.status) is JobStatus.DEAD_LETTER:
            log.error(
                "job.unregistered_dead_lettered",
                job_id=str(cj.job_id),
                handler=handler_name,
                unregistered_count=row.unregistered_count,
                dlq_entries=row.dlq_entries,
            )
        else:
            log.error(
                "job.unregistered_handler",
                job_id=str(cj.job_id),
                handler=handler_name,
                unregistered_count=row.unregistered_count,
                max_releases=UNREGISTERED_MAX_RELEASES,
                next_run_at=row.run_at.isoformat(),
            )

    async def _mark_cancelled(self, cj: ClaimedJob, reason: str) -> bool:
        """running -> cancelled, fenced. False means the lease was stolen first."""
        assert self.worker_id is not None
        async with self.sessionmaker() as s:
            row = (
                await s.execute(
                    _CANCEL,
                    {
                        "jid": str(cj.job_id),
                        "wid": str(self.worker_id),
                        "ep": cj.lease_epoch,
                        "reason": reason,
                    },
                )
            ).first()
            await s.commit()
        if row is None:
            log.warning("job.cancel_fenced_out", job_id=str(cj.job_id))
            return False
        log.info("job.cancelled", job_id=str(cj.job_id), reason=reason)
        return True

    async def _fail(self, cj: ClaimedJob, exc: BaseException) -> None:
        """Hand the failure to fail_job.sql: it decides retry-with-backoff vs
        dead-letter, closes the execution row, and writes the DLQ entry -- in one
        statement, with the backoff computed in the database."""
        assert self.worker_id is not None
        cls = type(exc).__name__
        message = str(exc)[:2000]
        # PermanentError skips the attempt budget: a 400 from a dependency will still
        # be a 400 in thirty seconds.
        retryable = not isinstance(exc, PermanentError)
        async with self.sessionmaker() as s:
            outcome = await fail_job_stmt(
                s,
                cj.job_id,
                self.worker_id,
                cj.lease_epoch,
                cls,
                message,
                retryable=retryable,
            )
            if outcome is not None and isinstance(exc, TimeoutError):
                await s.execute(_MARK_TIMED_OUT, {"jid": str(cj.job_id), "ep": cj.lease_epoch})
            await s.commit()

        if outcome is None:
            # The reaper already took the job back; another worker may own it now.
            log.warning("job.fail_fenced_out", job_id=str(cj.job_id), error_class=cls)
        elif outcome.will_retry:
            log.warning(
                "job.retry_scheduled",
                job_id=str(cj.job_id),
                error_class=cls,
                attempt=outcome.attempt,
                max_attempts=outcome.max_attempts,
                next_run_at=outcome.next_run_at.isoformat(),
            )
        else:
            log.error(
                "job.dead_lettered",
                job_id=str(cj.job_id),
                error_class=cls,
                attempt=outcome.attempt,
                max_attempts=outcome.max_attempts,
                retryable=retryable,
            )

    async def shutdown(self) -> None:
        assert self.worker_id is not None
        # Unstarted claims cost nothing to release: attempt has not moved.
        if self._unstarted:
            offered = [str(job_id) for job_id in self._unstarted]
            async with self.sessionmaker() as s:
                released = (
                    await s.execute(
                        _RELEASE_UNSTARTED, {"ids": offered, "wid": str(self.worker_id)}
                    )
                ).all()
                if released:
                    won_ids = [str(row.id) for row in released]
                    await s.execute(_CLOSE_RELEASED_EXECUTIONS, {"ids": won_ids})
                await s.commit()
            # offered != released is normal and not an error: the difference is the
            # claims that won the race to 'running' and are now being drained below.
            log.info(
                "worker.released_unstarted", released=len(released), offered=len(offered)
            )

        if self._inflight:
            log.info("worker.draining", inflight=len(self._inflight))
            # The heartbeat is still beating here: in-flight leases must be renewed
            # for the whole grace period or the reaper takes work we are finishing.
            _, pending = await asyncio.wait(
                list(self._inflight.values()), timeout=self.grace_seconds
            )
            if pending:
                log.warning("worker.drain_timeout", abandoned=len(pending))

        for watch in list(self._cancel_watch.values()):
            watch.cancel()
        self._cancel_watch.clear()

        self._heartbeat_stop.set()
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
            self._heartbeat_task = None
        # Stopped last, so a handler's final line still ships.
        await self.logsink.stop()

        async with self.sessionmaker() as s:
            await s.execute(
                update(Worker)
                .where(Worker.id == self.worker_id)
                .values(status=WorkerStatus.STOPPED, stopped_at=datetime.now(UTC))
            )
            await s.commit()
        log.info("worker.stopped", worker=self.name)


def _weighted_order(queues: list[tuple[UUID, int]]) -> list[tuple[UUID, int]]:
    """Weighted-random order by ``queues.priority`` (Efraimidis-Spirakis).

    Strict priority order would let a busy high-priority queue starve a low-priority
    one indefinitely: the worker would spend every cycle filling its batch from the
    top queue and never reach the bottom. Weighted-random makes a high-priority queue
    *usually* first rather than *always* first, so both "high priority wins on
    average" and "low priority still progresses" are true -- and both are testable.
    """
    keyed: list[tuple[float, UUID, int]] = []
    for queue_id, priority in queues:
        # priority is -100..100; shift to a strictly positive weight (1..201).
        weight = float(priority) + 101.0
        u = random.random() or 1e-12
        keyed.append((u ** (1.0 / weight), queue_id, priority))
    keyed.sort(key=lambda k: k[0], reverse=True)
    return [(queue_id, priority) for _, queue_id, priority in keyed]
