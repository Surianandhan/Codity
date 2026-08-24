"""Job creation, the job explorer, and everything hanging off a single job.

Two rules shape this module:

* **Terminal states have zero out-edges.** Cancel of an already-finished job is a
  409, and manual retry never resurrects a terminal row -- it inserts a new job
  with ``replay_of_job_id`` pointing back. History stays immutable and the
  transition trigger stays simple.
* **Every list is keyset-paginated and every filter is declared.** An unknown
  query parameter is a 400 rather than silently-unfiltered data.
"""

from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Header, Query, Request
from sqlalchemy import ColumnElement, func, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import MemberDep, PrincipalDep, SessionDep
from app.api.pagination import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    Envelope,
    decode_int_cursor,
    decode_time_uuid_cursor,
    encode_int_cursor,
    encode_time_cursor,
    envelope,
    keyset_after_desc,
    paginate,
    reject_unknown_query_params,
    request_id,
)
from app.api.schemas import JobCreate, JobOut, ORMModel
from app.db.models.base import uuid7
from app.db.models.execution import JobExecution
from app.db.models.observability import JobLog
from app.db.models.scheduling import Job, JobBatch, Queue
from app.domain.enums import TERMINAL_STATUSES, ExecutionStatus, JobKind, JobStatus
from app.domain.errors import ConflictError, IllegalTransition, NotFoundError, QueuePaused
from app.services.idempotency import is_live_key_conflict, resolve
from app.services.jobs import create_batch, create_job, get_queue

router = APIRouter(tags=["jobs"])


# --- wire models ------------------------------------------------------------


class ExecutionOut(ORMModel):
    id: int
    job_id: UUID
    queue_id: UUID
    attempt_number: int
    worker_id: UUID | None
    lease_epoch: int
    status: ExecutionStatus
    claimed_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None
    queue_wait_ms: int | None
    next_retry_at: datetime | None
    error_class: str | None
    error_message: str | None
    result: dict[str, Any] | None
    log_line_count: int


class JobLogOut(ORMModel):
    id: int
    execution_id: int
    job_id: UUID
    seq: int
    level: str
    message: str
    context: dict[str, Any]
    logged_at: datetime


class BatchOut(ORMModel):
    id: UUID
    project_id: UUID
    queue_id: UUID
    name: str | None
    handler: str
    total_jobs: int
    created_at: datetime
    # GROUP BY status over ix_jobs_batch rather than maintained counters: nothing
    # to drift, nothing to reconcile.
    counts: dict[str, int]
    terminal_jobs: int
    pending: int
    progress: float


# --- helpers ----------------------------------------------------------------


async def _load_job(session: AsyncSession, job_id: UUID, org_id: UUID) -> Job:
    job = (
        await session.execute(
            select(Job).where(Job.id == job_id, Job.organization_id == org_id)
        )
    ).scalar_one_or_none()
    if job is None:
        # 404 rather than 403 on a cross-tenant read: a 403 confirms it exists.
        raise NotFoundError("job not found")
    return job


async def create_replay_job(
    session: AsyncSession,
    *,
    source: Job,
    queue: Queue,
    payload: dict[str, Any],
    correlation_id: str | None,
) -> Job:
    """Insert the replacement job for a manual retry or a DLQ replay.

    Shared with the DLQ router so the replay-insert shape has exactly one owner.

    The replacement is always ``kind='immediate'``. Copying the original kind
    would drag ``schedule_id``/``scheduled_for`` with it -- ``ck_jobs_occurrence``
    ties the two together and ``ux_jobs_schedule_occurrence`` already holds that
    cron instant -- so a replayed recurring job would either fail the check or
    collide with the occurrence it is replaying.
    """
    now = datetime.now(UTC)
    job = Job(
        id=uuid7(),
        organization_id=source.organization_id,
        project_id=source.project_id,
        queue_id=source.queue_id,
        replay_of_job_id=source.id,
        kind=JobKind.IMMEDIATE,
        handler=source.handler,
        status=JobStatus.QUEUED,
        priority=source.priority,
        run_at=now,
        payload=payload,
        attempt=0,
        max_attempts=source.max_attempts,
        backoff_strategy=source.backoff_strategy,
        backoff_base_ms=source.backoff_base_ms,
        backoff_max_ms=source.backoff_max_ms,
        # Re-clamped against the queue's *current* lease: the queue may have been
        # reconfigured since the original ran, and ck_jobs_timeout_lt_lease is not
        # negotiable.
        timeout_ms=min(source.timeout_ms, queue.visibility_timeout_sec * 1000 - 1),
        lease_seconds=queue.visibility_timeout_sec,
        # Deliberately not copied. The key belongs to the original request, and
        # reusing it would collide with any live sibling under
        # ux_jobs_live_idempotency.
        idempotency_key=None,
        correlation_id=correlation_id,
    )
    session.add(job)
    await session.flush()
    return job


# --- creation ---------------------------------------------------------------


async def _commit_or_key_conflict(session: AsyncSession, key: str | None) -> None:
    """Commit, translating a lost race on the live idempotency index into a 409.

    The advisory lock in ``resolve`` covers every caller that goes through this
    module; this covers everything else, including a second API replica that got
    past the lock in the window before the first request committed.
    """
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        if is_live_key_conflict(exc):
            raise ConflictError(
                f"Idempotency-Key {key!r} is already in use on this queue",
                [{"field": "Idempotency-Key", "issue": "in_progress"}],
            ) from exc
        raise


async def _batch_summary(
    session: AsyncSession, batch_id: UUID, organization_id: UUID
) -> dict[str, Any]:
    """A batch and its live progress, from one GROUP BY over ``ix_jobs_batch``.

    Nothing is incremented on completion and nothing is reconciled after a crash:
    the counts are derived from the child rows every time they are asked for, so
    they cannot drift from the thing they describe.
    """
    batch = (
        await session.execute(
            select(JobBatch).where(
                JobBatch.id == batch_id,
                JobBatch.organization_id == organization_id,
            )
        )
    ).scalar_one_or_none()
    if batch is None:
        raise NotFoundError("batch not found")

    rows = (
        await session.execute(
            select(Job.status, func.count())
            .where(Job.batch_id == batch_id, Job.organization_id == organization_id)
            .group_by(Job.status)
        )
    ).all()
    counts = {str(status): int(n) for status, n in rows}
    terminal = sum(n for status, n in counts.items() if JobStatus(status) in TERMINAL_STATUSES)

    return {
        "id": batch.id,
        "project_id": batch.project_id,
        "queue_id": batch.queue_id,
        "name": batch.name,
        "handler": batch.handler,
        "total_jobs": batch.total_jobs,
        "created_at": batch.created_at,
        "counts": counts,
        "terminal_jobs": terminal,
        "pending": batch.total_jobs - terminal,
        "progress": round(terminal / batch.total_jobs, 4) if batch.total_jobs else 0.0,
    }


@router.post(
    "/queues/{queue_id}/jobs",
    response_model=JobOut | BatchOut,
    status_code=201,
    summary="Create a job, or a batch of jobs",
    description=(
        "One endpoint, five kinds, discriminated on `kind`. Every kind except "
        "`batch` returns a `JobOut`. A `batch` creates one job per item and returns "
        "a `BatchOut` -- returning a single arbitrary child for a request that made "
        "a thousand of them would leave the caller with no id it can poll."
    ),
)
async def enqueue(
    queue_id: UUID,
    body: JobCreate,
    principal: MemberDep,
    session: SessionDep,
    request: Request,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> Job | dict[str, Any]:
    queue = await get_queue(session, queue_id, principal.organization_id)
    if queue.is_paused:
        # Pause blocks admission only; in-flight work is unaffected.
        raise QueuePaused(f"queue {queue.name!r} is paused")

    # The header wins over the body field: it is the transport-level retry key a
    # proxy or SDK sets, and it is what the client will resend verbatim.
    key = idempotency_key or body.idempotency_key
    if key is not None:
        body = body.model_copy(update={"idempotency_key": key})
        replayed = await resolve(session, queue, key, body)
        if replayed is not None:
            # Commit to release the advisory lock, then return the original 201.
            await session.commit()
            if replayed.batch_id is not None:
                # The key rides on the first child, but a replay must answer in the
                # same shape the original request did -- the batch, not one of its
                # thousand children.
                return await _batch_summary(
                    session, replayed.batch_id, principal.organization_id
                )
            return replayed

    if body.kind == JobKind.BATCH:
        batch = await create_batch(
            session, queue, body, correlation_id=request_id(request)
        )
        await _commit_or_key_conflict(session, key)
        return await _batch_summary(session, batch.id, principal.organization_id)

    job = await create_job(
        session, queue, body, correlation_id=request_id(request)
    )
    await _commit_or_key_conflict(session, key)
    await session.refresh(job)
    return job


# --- explorer ---------------------------------------------------------------

_JOB_FILTERS = frozenset(
    {
        "status",
        "kind",
        "queue_id",
        "handler",
        "created_after",
        "created_before",
        "run_after",
        "run_before",
        "limit",
        "cursor",
    }
)


def _job_filters(
    status: list[JobStatus] | None,
    kind: JobKind | None,
    queue_id: UUID | None,
    handler: str | None,
    created_after: datetime | None,
    created_before: datetime | None,
    run_after: datetime | None,
    run_before: datetime | None,
) -> list[ColumnElement[bool]]:
    where: list[ColumnElement[bool]] = []
    if status:
        where.append(Job.status.in_(status))
    if kind is not None:
        where.append(Job.kind == kind)
    if queue_id is not None:
        where.append(Job.queue_id == queue_id)
    if handler is not None:
        where.append(Job.handler == handler)
    if created_after is not None:
        where.append(Job.created_at >= created_after)
    if created_before is not None:
        where.append(Job.created_at < created_before)
    if run_after is not None:
        where.append(Job.run_at >= run_after)
    if run_before is not None:
        where.append(Job.run_at < run_before)
    return where


async def _job_page(
    session: AsyncSession,
    request: Request,
    scope: ColumnElement[bool],
    where: list[ColumnElement[bool]],
    limit: int,
    cursor: str | None,
) -> dict[str, Any]:
    stmt = select(Job).where(scope, *where)
    if cursor:
        ts, ident = decode_time_uuid_cursor(cursor)
        stmt = stmt.where(keyset_after_desc(Job.created_at, Job.id, ts, ident))
    # Matches ix_jobs_project_created / ix_jobs_queue_status_created exactly.
    stmt = stmt.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    data, page = paginate(rows, limit, lambda j: encode_time_cursor(j.created_at, j.id))
    return envelope(request, data, page)


@router.get("/projects/{project_id}/jobs", response_model=Envelope[JobOut])
async def list_project_jobs(
    project_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    status: Annotated[list[JobStatus] | None, Query()] = None,
    kind: JobKind | None = None,
    queue_id: UUID | None = None,
    handler: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    run_after: datetime | None = None,
    run_before: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, _JOB_FILTERS)
    where = _job_filters(
        status, kind, queue_id, handler, created_after, created_before, run_after, run_before
    )
    return await _job_page(
        session,
        request,
        (Job.project_id == project_id) & (Job.organization_id == principal.organization_id),
        where,
        limit,
        cursor,
    )


@router.get("/queues/{queue_id}/jobs", response_model=Envelope[JobOut])
async def list_queue_jobs(
    queue_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    status: Annotated[list[JobStatus] | None, Query()] = None,
    kind: JobKind | None = None,
    handler: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    run_after: datetime | None = None,
    run_before: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, _JOB_FILTERS - {"queue_id"})
    where = _job_filters(
        status, kind, None, handler, created_after, created_before, run_after, run_before
    )
    return await _job_page(
        session,
        request,
        (Job.queue_id == queue_id) & (Job.organization_id == principal.organization_id),
        where,
        limit,
        cursor,
    )


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: UUID, principal: PrincipalDep, session: SessionDep) -> Job:
    return await _load_job(session, job_id, principal.organization_id)


# --- lifecycle actions ------------------------------------------------------


@router.post("/jobs/{job_id}/cancel", response_model=JobOut)
async def cancel_job(job_id: UUID, principal: MemberDep, session: SessionDep) -> Job:
    """Cancel outright if the job has not been claimed; otherwise ask for it.

    The guarded UPDATE is the point: reading the status and then writing it would
    race a claimer, and ``scheduled|queued -> claimed`` can commit between the two.
    A rowcount of zero means the job moved, so we re-read and decide again --
    ``claimed``/``running`` becomes a cancel *request* the worker picks up from the
    RETURNING clause of the heartbeat it already issues.
    """
    org_id = principal.organization_id
    result = cast(
        CursorResult[Any],
        await session.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.organization_id == org_id,
                Job.status.in_([JobStatus.SCHEDULED, JobStatus.QUEUED]),
            )
            # finished_at is not optional: ck_jobs_terminal_finished makes terminal
            # status and finished_at the same fact, and omitting it aborts the txn.
            .values(
                status=JobStatus.CANCELLED,
                finished_at=func.now(),
                updated_at=func.now(),
                worker_id=None,
                claimed_at=None,
                lease_expires_at=None,
            )
        ),
    )
    if result.rowcount:
        await session.commit()
        return await _load_job(session, job_id, org_id)

    job = await _load_job(session, job_id, org_id)
    if job.status in TERMINAL_STATUSES:
        raise IllegalTransition(
            f"job is already {job.status}; terminal states have no outgoing transitions. "
            "Use POST /jobs/{job_id}/replay to run it again."
        )

    job.cancel_requested = True
    await session.commit()
    await session.refresh(job)
    return job


@router.post("/jobs/{job_id}/replay", response_model=JobOut, status_code=201)
async def replay_job(
    job_id: UUID, principal: MemberDep, session: SessionDep, request: Request
) -> Job:
    """Manual retry as a *new* job, never as a resurrection of the old one."""
    source = await _load_job(session, job_id, principal.organization_id)
    if source.status not in TERMINAL_STATUSES:
        raise ConflictError(
            f"job is {source.status} and has not finished; replay is for terminal jobs only"
        )
    queue = await get_queue(session, source.queue_id, principal.organization_id)
    if queue.is_paused:
        raise QueuePaused(f"queue {queue.name!r} is paused")

    job = await create_replay_job(
        session,
        source=source,
        queue=queue,
        payload=source.payload,
        correlation_id=request_id(request),
    )
    await session.commit()
    await session.refresh(job)
    return job


# --- attempt history and logs ----------------------------------------------


@router.get("/jobs/{job_id}/executions", response_model=Envelope[ExecutionOut])
async def list_executions(
    job_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"limit", "cursor"})
    await _load_job(session, job_id, principal.organization_id)

    # Newest attempt first, along ix_job_executions_job. The id is monotonic per
    # job, so it alone is a sufficient keyset -- there is deliberately no UNIQUE
    # (job_id, attempt_number), because a claim that died before starting leaves a
    # 'lost' row at attempt N and the next claim legitimately writes another.
    stmt = select(JobExecution).where(
        JobExecution.job_id == job_id,
        JobExecution.organization_id == principal.organization_id,
    )
    if cursor:
        stmt = stmt.where(JobExecution.id < decode_int_cursor(cursor))
    stmt = stmt.order_by(JobExecution.id.desc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    data, page = paginate(rows, limit, lambda e: encode_int_cursor(e.id))
    return envelope(request, data, page)


@router.get("/jobs/{job_id}/logs", response_model=Envelope[JobLogOut])
async def list_logs(
    job_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    execution_id: int | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"execution_id", "limit", "cursor"})
    # job_logs carries no organization_id of its own, so tenancy is established by
    # resolving the parent job first and filtering on its id.
    await _load_job(session, job_id, principal.organization_id)

    # Ascending: logs are read forwards, and a tail-follow poll just keeps passing
    # the last cursor back.
    stmt = select(JobLog).where(JobLog.job_id == job_id)
    if execution_id is not None:
        stmt = stmt.where(JobLog.execution_id == execution_id)
    if cursor:
        stmt = stmt.where(JobLog.id > decode_int_cursor(cursor))
    stmt = stmt.order_by(JobLog.id.asc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    data, page = paginate(rows, limit, lambda log: encode_int_cursor(log.id))
    return envelope(request, data, page)


# --- batches ----------------------------------------------------------------


@router.get("/batches/{batch_id}", response_model=BatchOut)
async def get_batch(
    batch_id: UUID, principal: PrincipalDep, session: SessionDep
) -> dict[str, Any]:
    return await _batch_summary(session, batch_id, principal.organization_id)
