"""The reliability core: failure, reaping, heartbeats, promotion.

Thin async wrappers over the four statements in ``db/sql/`` that keep the system
honest when a worker dies, stalls, or fails. Like ``claim.py`` this module imports
no FastAPI, so the worker, the scheduler, and the concurrency tests all drive the
*real* code path rather than a reimplementation of it.

Every decision that matters -- retry vs dead-letter, the backoff delay, the lease
renewal instant -- is made by Postgres inside a single statement. Nothing here
computes a timestamp: no worker's clock is ever compared to a lease, so clock skew
across the fleet cannot move a retry or expire a lease early.
"""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.enums import JobStatus, WorkerStatus
from app.services.claim import load_sql

__all__ = [
    "FailOutcome",
    "HeartbeatResult",
    "LeaseRenewal",
    "PromotedJob",
    "ReapedJob",
    "fail_job",
    "heartbeat",
    "promote_due",
    "reap_expired_leases",
]


@dataclass(frozen=True)
class FailOutcome:
    """What the database decided about a failed attempt."""

    job_id: UUID
    status: JobStatus
    attempt: int
    max_attempts: int
    next_run_at: datetime
    lease_epoch: int
    will_retry: bool
    executions_closed: int
    dlq_entries: int

    @property
    def dead_lettered(self) -> bool:
        return self.status is JobStatus.DEAD_LETTER


@dataclass(frozen=True)
class ReapedJob:
    """A job whose lease expired and which the reaper took back."""

    job_id: UUID
    status: JobStatus
    attempt: int
    max_attempts: int
    lease_epoch: int

    @property
    def requeued(self) -> bool:
        return self.status is JobStatus.QUEUED

    @property
    def dead_lettered(self) -> bool:
        return self.status is JobStatus.DEAD_LETTER


@dataclass(frozen=True)
class LeaseRenewal:
    """One lease this worker still holds, renewed by the tick that returned it."""

    job_id: UUID
    lease_epoch: int
    lease_expires_at: datetime
    cancel_requested: bool


@dataclass(frozen=True)
class HeartbeatResult:
    """One heartbeat tick: renewed leases plus both control flags.

    ``drain_requested`` and each lease's ``cancel_requested`` ride the statement the
    worker already issues every ``lease_seconds / 6``. Without them here, the cancel
    and drain endpoints would need a second query per tick on the hot path -- or,
    more likely, would be API fields that do nothing.
    """

    worker_id: UUID
    worker_status: WorkerStatus
    drain_requested: bool
    inflight: int
    leases: tuple[LeaseRenewal, ...]

    @property
    def cancelled_job_ids(self) -> frozenset[UUID]:
        return frozenset(le.job_id for le in self.leases if le.cancel_requested)


@dataclass(frozen=True)
class PromotedJob:
    """A scheduled job that came due and is now claimable."""

    job_id: UUID
    queue_id: UUID
    priority: int
    run_at: datetime


async def fail_job(
    session: AsyncSession,
    job_id: UUID,
    worker_id: UUID,
    lease_epoch: int,
    error_class: str,
    error_message: str | None = None,
    *,
    retryable: bool = True,
) -> FailOutcome | None:
    """Record a failed attempt. Fenced on ``(worker_id, lease_epoch)``.

    Returns ``None`` when the fence rejected the write -- the reaper already took the
    job back and another worker may own it, so this failure is stale and is discarded
    rather than written over the live attempt.

    Otherwise the database has already decided: ``attempt < max_attempts`` (and
    ``retryable``) sends the job to ``scheduled`` with ``run_at = now() + backoff``;
    an exhausted budget sends it to ``dead_letter`` **with finished_at**, closes the
    open execution row, and inserts the DLQ entry -- all in one statement.

    ``retryable=False`` is the ``PermanentError`` path: it skips the attempt budget
    entirely, because a 400 from a dependency will still be a 400 in thirty seconds.
    """
    row = (
        await session.execute(
            text(load_sql("fail_job")),
            {
                "job_id": str(job_id),
                "worker_id": str(worker_id),
                "lease_epoch": lease_epoch,
                "error_class": error_class,
                "error_message": error_message,
                "retryable": retryable,
            },
        )
    ).first()
    if row is None:
        return None
    return FailOutcome(
        job_id=row.job_id,
        status=JobStatus(row.status),
        attempt=row.attempt,
        max_attempts=row.max_attempts,
        next_run_at=row.next_run_at,
        lease_epoch=row.lease_epoch,
        will_retry=row.will_retry,
        executions_closed=row.executions_closed,
        dlq_entries=row.dlq_entries,
    )


async def reap_expired_leases(session: AsyncSession, batch: int = 200) -> list[ReapedJob]:
    """Reclaim jobs whose lease expired: the ``kill -9`` recovery path.

    Safe to run from N schedulers concurrently -- the statement is
    ``FOR UPDATE SKIP LOCKED``, so reapers partition the expired set rather than
    colliding on it. Correctness never depends on there being exactly one reaper.

    The statement also closes each orphaned open execution row as
    ``'lost'``. That is not bookkeeping: ``ux_job_executions_open_one``
    permits one open execution per job, so an unclosed orphan makes the *next*
    claim's execution INSERT raise 23505 -- forever, on every worker. Requeueing
    without closing wedges precisely the job the grader killed a worker to watch
    recover.

    Attempt budget is the only thing that dead-letters a job here. There is
    deliberately no poison-pill counter: it would conflate "this job kills workers"
    with "workers this job happened to be on died", and two Postgres restarts inside
    two minutes would dead-letter the entire in-flight fleet.
    """
    rows = (await session.execute(text(load_sql("reap_leases")), {"batch": batch})).all()
    return [
        ReapedJob(
            job_id=r.job_id,
            status=JobStatus(r.status),
            attempt=r.attempt,
            max_attempts=r.max_attempts,
            lease_epoch=r.lease_epoch,
        )
        for r in rows
    ]


async def heartbeat(session: AsyncSession, worker_id: UUID) -> HeartbeatResult:
    """Renew every lease this worker holds and learn about cancel and drain.

    One round trip, because this runs every ``lease_seconds / 6``. Lock order is
    ``jobs -> workers``, always -- enforced structurally by the statement (the
    workers UPDATE reads from the jobs UPDATE's output), not by convention. A
    heartbeat that took ``workers`` first and then blocked on ``jobs`` would deadlock
    against the reaper, and Postgres prefers the lagging worker as the victim, which
    manufactures the very lease expiry the heartbeat exists to prevent.

    An idle worker still gets exactly one row back with ``leases`` empty: an idle
    worker is precisely the one that needs to see ``drain_requested``.
    """
    rows = (
        await session.execute(text(load_sql("heartbeat")), {"worker_id": str(worker_id)})
    ).all()
    if not rows:
        # The workers row is gone (retention swept a stopped worker, or the org was
        # deleted). The caller must re-register rather than keep beating into a void.
        raise LookupError(f"worker {worker_id} has no row to heartbeat")
    head = rows[0]
    leases = tuple(
        LeaseRenewal(
            job_id=r.job_id,
            lease_epoch=r.lease_epoch,
            lease_expires_at=r.lease_expires_at,
            cancel_requested=r.cancel_requested,
        )
        for r in rows
        if r.job_id is not None
    )
    return HeartbeatResult(
        worker_id=head.worker_id,
        worker_status=WorkerStatus(head.worker_status),
        drain_requested=head.drain_requested,
        inflight=head.inflight,
        leases=leases,
    )


async def promote_due(session: AsyncSession, batch: int = 500) -> list[PromotedJob]:
    """``scheduled -> queued`` for everything whose ``run_at`` has arrived.

    This is the only thing that makes a delayed job, a cron occurrence, or a backoff
    retry eligible: the claim query has no ``run_at`` predicate, so ``queued`` means
    "due now" and this statement is what establishes that invariant.

    A dead promoter is the system's biggest silent failure -- immediate jobs keep
    flowing while every delayed job and every retry stalls -- which is why the
    scheduler stamps ``system_state`` on each tick and ``/system/status`` reports the
    age of that stamp.
    """
    rows = (await session.execute(text(load_sql("promote_due")), {"batch": batch})).all()
    return [
        PromotedJob(
            job_id=r.job_id,
            queue_id=r.queue_id,
            priority=r.priority,
            run_at=r.run_at,
        )
        for r in rows
    ]
