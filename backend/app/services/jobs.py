import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.base import uuid7
from app.db.models.scheduling import Job, JobBatch, Queue
from app.domain.enums import JobKind, JobStatus
from app.domain.errors import NotFoundError, ValidationError

DEFAULT_MAX_ATTEMPTS = 3


async def get_queue(session: AsyncSession, queue_id: UUID, organization_id: UUID) -> Queue:
    q = (
        await session.execute(
            select(Queue).where(Queue.id == queue_id, Queue.organization_id == organization_id)
        )
    ).scalar_one_or_none()
    if q is None:
        # 404 rather than 403: a 403 confirms the resource exists.
        raise NotFoundError(f"queue {queue_id} not found")
    return q


def _resolve_schedule(kind: str, body: Any, now: datetime) -> tuple[JobStatus, datetime]:
    """Immediate/batch are due now and go straight to 'queued'. Everything else is
    'scheduled' and waits for the promoter -- which is why the claim query needs no
    run_at predicate and its partial index stays small."""
    match kind:
        case JobKind.IMMEDIATE | JobKind.BATCH:
            return JobStatus.QUEUED, now
        case JobKind.DELAYED:
            return JobStatus.SCHEDULED, now + timedelta(milliseconds=body.delay_ms)
        case JobKind.SCHEDULED:
            return JobStatus.SCHEDULED, body.run_at
        case JobKind.RECURRING:
            raise ValidationError("recurring jobs are created via POST /queues/{id}/schedules")
    raise ValidationError(f"unsupported job kind {kind!r}")


def _effective_timeout_ms(queue: Queue, body: Any) -> int:
    timeout_ms: int = body.timeout_ms or queue.default_timeout_ms
    # Defence in depth: the DB CHECK enforces this too, but a 422 beats a 500 --
    # and for a batch it beats aborting a transaction that already holds N inserts.
    if timeout_ms >= queue.visibility_timeout_sec * 1000:
        raise ValidationError(
            f"timeout_ms ({timeout_ms}) must be < the queue's lease "
            f"({queue.visibility_timeout_sec * 1000}ms); a job may not outlive its lease"
        )
    return timeout_ms


async def create_job(
    session: AsyncSession,
    queue: Queue,
    body: Any,
    correlation_id: str | None = None,
) -> Job:
    now = datetime.now(UTC)
    status, run_at = _resolve_schedule(body.kind, body, now)
    timeout_ms = _effective_timeout_ms(queue, body)

    job = Job(
        id=uuid7(),
        organization_id=queue.organization_id,
        project_id=queue.project_id,
        queue_id=queue.id,
        kind=body.kind,
        handler=body.handler,
        status=status,
        priority=body.priority if body.priority is not None else queue.default_priority,
        run_at=run_at,
        payload=body.payload,
        attempt=0,
        max_attempts=body.max_attempts or DEFAULT_MAX_ATTEMPTS,
        timeout_ms=timeout_ms,
        # Snapshotted: the queue owns lease length, and changing it later must not
        # retroactively alter jobs already in flight.
        lease_seconds=queue.visibility_timeout_sec,
        idempotency_key=body.idempotency_key,
        correlation_id=correlation_id,
    )
    session.add(job)
    await session.flush()
    return job


def _reject_oversized_items(queue: Queue, items: list[dict[str, Any]]) -> None:
    """Check every item against the queue's payload cap before anything is inserted.

    ``jobs.payload`` carries a 1 MiB CHECK as the database backstop, and a violation
    inside a batch does not fail one row -- it aborts the transaction holding all N.
    Naming the offending index in a 422 is the difference between a fixable error and
    a 500 for a request the client cannot debug.
    """
    for i, item in enumerate(items):
        size = len(json.dumps(item, separators=(",", ":"), default=str).encode())
        if size > queue.max_payload_bytes:
            raise ValidationError(
                f"items[{i}] is {size} bytes, over the queue's max_payload_bytes "
                f"({queue.max_payload_bytes})",
                [{"field": f"items.{i}", "issue": "payload_too_large"}],
            )


async def create_batch(
    session: AsyncSession,
    queue: Queue,
    body: Any,
    correlation_id: str | None = None,
) -> JobBatch:
    """One ``job_batches`` row and its N children, staged in a single flush.

    The transaction boundary belongs to the caller, so the batch and every child
    become visible together or not at all. ``total_jobs`` is written here, once, and
    never touched again: progress is a GROUP BY over ``ix_jobs_batch``, so there is no
    counter to increment on completion and none to reconcile after a crash.

    The children are ordinary immediate work -- ``queued``, ``run_at = now()``, the
    queue's lease snapshotted onto each -- distinguished only by ``batch_id``. The
    claim path therefore needs to know nothing about batches at all.
    """
    now = datetime.now(UTC)
    timeout_ms = _effective_timeout_ms(queue, body)
    _reject_oversized_items(queue, body.items)

    batch = JobBatch(
        id=uuid7(),
        organization_id=queue.organization_id,
        project_id=queue.project_id,
        queue_id=queue.id,
        handler=body.handler,
        total_jobs=len(body.items),
    )
    session.add(batch)
    # Flush the parent before the children reference it. `jobs.batch_id` is a plain
    # column with no relationship() behind it, so SQLAlchemy's unit of work has no
    # dependency to sort on and would otherwise emit the child INSERTs first --
    # fk_jobs_batch_id then fails on a batch row that does not exist yet. Two
    # flushes, still one transaction: the caller owns the commit.
    await session.flush()

    priority = body.priority if body.priority is not None else queue.default_priority
    max_attempts = body.max_attempts or DEFAULT_MAX_ATTEMPTS
    lease_seconds = queue.visibility_timeout_sec
    children = [
        Job(
            id=uuid7(),
            organization_id=queue.organization_id,
            project_id=queue.project_id,
            queue_id=queue.id,
            batch_id=batch.id,
            kind=JobKind.BATCH,
            handler=body.handler,
            status=JobStatus.QUEUED,
            priority=priority,
            run_at=now,
            # One item, one child. The batch itself stores no payload: rebuilding the
            # request from the children is what lets idempotency recompute rather than
            # store the item list.
            payload=item,
            attempt=0,
            max_attempts=max_attempts,
            timeout_ms=timeout_ms,
            lease_seconds=lease_seconds,
            # The key goes on the first child only. ux_jobs_live_idempotency is UNIQUE
            # over (queue_id, idempotency_key) across live rows, so N children carrying
            # it would collide with each other; one is all ``resolve`` needs to find the
            # batch again on a retry.
            idempotency_key=body.idempotency_key if i == 0 else None,
            correlation_id=correlation_id,
        )
        for i, item in enumerate(body.items)
    ]
    # add_all + one flush: SQLAlchemy's insertmanyvalues turns this into a handful of
    # multi-row INSERTs, not 1000 round trips.
    session.add_all(children)
    await session.flush()
    return batch
