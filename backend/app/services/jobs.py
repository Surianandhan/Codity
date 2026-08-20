from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.base import uuid7
from app.db.models.scheduling import Job, Queue
from app.domain.enums import JobKind, JobStatus
from app.domain.errors import NotFoundError, ValidationError


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


async def create_job(
    session: AsyncSession,
    queue: Queue,
    body: Any,
    correlation_id: str | None = None,
) -> Job:
    now = datetime.now(UTC)
    status, run_at = _resolve_schedule(body.kind, body, now)

    timeout_ms = body.timeout_ms or queue.default_timeout_ms
    # Defence in depth: the DB CHECK enforces this too, but a 422 beats a 500.
    if timeout_ms >= queue.visibility_timeout_sec * 1000:
        raise ValidationError(
            f"timeout_ms ({timeout_ms}) must be < the queue's lease "
            f"({queue.visibility_timeout_sec * 1000}ms); a job may not outlive its lease"
        )

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
        max_attempts=body.max_attempts or 3,
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
