"""Cron schedules -- the recurrence rules, not the jobs they emit.

A schedule is a rule; a job is one materialised occurrence of it. Keeping them in
separate tables is what lets one rule emit thousands of jobs, lets an occurrence
retain full job history after the rule changes, and lets
``ux_jobs_schedule_occurrence (schedule_id, scheduled_for)`` be the thing that
makes N concurrent schedulers safe.

Deletion is **soft**. ``jobs.schedule_id`` is ``ON DELETE RESTRICT`` precisely so
that deleting a rule cannot delete the record of what it already ran, and
``ux_job_schedules_queue_name`` is partial on ``deleted_at IS NULL`` so a deleted
schedule does not permanently reserve its name.
"""

from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from croniter import croniter  # type: ignore[import-untyped]
from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import MemberDep, PrincipalDep, SessionDep
from app.api.pagination import (
    DEFAULT_LIMIT,
    MAX_LIMIT,
    Envelope,
    decode_time_uuid_cursor,
    encode_time_cursor,
    envelope,
    keyset_after_desc,
    paginate,
    reject_unknown_query_params,
)
from app.api.schemas import ORMModel
from app.db.models.base import uuid7
from app.db.models.scheduling import JobSchedule, Queue
from app.domain.errors import ConflictError, NotFoundError, ValidationError
from app.services.jobs import get_queue

router = APIRouter(tags=["schedules"])


class ScheduleIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    cron: str = Field(min_length=1, max_length=200)
    timezone: str = "UTC"
    handler: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-100, le=100, description="higher runs first")
    max_attempts: int = Field(default=3, ge=1, le=50)
    timeout_ms: int | None = Field(default=None, ge=100)
    catchup_policy: Literal["skip", "catchup"] = "skip"
    max_catchup_occurrences: int = Field(default=10, ge=1, le=1000)
    is_active: bool = True


class ScheduleUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    cron: str | None = Field(default=None, min_length=1, max_length=200)
    timezone: str | None = None
    handler: str | None = Field(default=None, min_length=1, max_length=200)
    payload: dict[str, Any] | None = None
    priority: int | None = Field(default=None, ge=-100, le=100)
    max_attempts: int | None = Field(default=None, ge=1, le=50)
    timeout_ms: int | None = Field(default=None, ge=100)
    catchup_policy: Literal["skip", "catchup"] | None = None
    max_catchup_occurrences: int | None = Field(default=None, ge=1, le=1000)
    is_active: bool | None = None


class ScheduleOut(ORMModel):
    id: UUID
    project_id: UUID
    queue_id: UUID
    name: str
    cron: str
    timezone: str
    is_active: bool
    next_occurrence_at: datetime | None
    last_occurrence_at: datetime | None
    catchup_policy: str
    max_catchup_occurrences: int
    # Bumped instead of materialising an occurrence into a paused queue, so a
    # paused per-minute schedule cannot silently build a stampede for resume.
    skipped_occurrences: int
    handler: str
    payload: dict[str, Any]
    priority: int
    max_attempts: int
    timeout_ms: int
    created_at: datetime


def _validate_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError, KeyError) as exc:
        raise ValidationError(
            f"unknown timezone {name!r}",
            [{"field": "timezone", "issue": "unknown_timezone"}],
        ) from exc


def next_occurrence(cron: str, timezone: str, after: datetime) -> datetime:
    """The next firing instant, computed in the schedule's own timezone.

    Timezone-aware iteration is what makes DST correct: a '0 2 * * *' schedule must
    fire once on the day the clocks go forward, not zero times or twice.
    """
    tz = _validate_timezone(timezone)
    if not croniter.is_valid(cron):
        raise ValidationError(
            f"{cron!r} is not a valid cron expression",
            [{"field": "cron", "issue": "invalid_cron"}],
        )
    nxt: datetime = croniter(cron, after.astimezone(tz)).get_next(datetime)
    return nxt.astimezone(UTC)


def _check_timeout(timeout_ms: int, queue: Queue) -> int:
    """ck_jobs_timeout_lt_lease applies to every job this schedule will emit, so
    reject the rule now rather than failing silently at 03:00 every morning."""
    if timeout_ms >= queue.visibility_timeout_sec * 1000:
        raise ValidationError(
            f"timeout_ms ({timeout_ms}) must be < the queue's lease "
            f"({queue.visibility_timeout_sec * 1000}ms); a job may not outlive its lease",
            [{"field": "timeout_ms", "issue": "exceeds_lease"}],
        )
    return timeout_ms


async def _load_schedule(
    session: AsyncSession, schedule_id: UUID, org_id: UUID
) -> JobSchedule:
    schedule = (
        await session.execute(
            select(JobSchedule).where(
                JobSchedule.id == schedule_id,
                JobSchedule.organization_id == org_id,
                JobSchedule.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if schedule is None:
        raise NotFoundError("schedule not found")
    return schedule


@router.post("/queues/{queue_id}/schedules", response_model=ScheduleOut, status_code=201)
async def create_schedule(
    queue_id: UUID, body: ScheduleIn, principal: MemberDep, session: SessionDep
) -> JobSchedule:
    queue = await get_queue(session, queue_id, principal.organization_id)
    timeout_ms = _check_timeout(body.timeout_ms or queue.default_timeout_ms, queue)

    dup = (
        await session.execute(
            select(JobSchedule.id).where(
                JobSchedule.queue_id == queue_id,
                JobSchedule.name == body.name,
                JobSchedule.deleted_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if dup:
        raise ConflictError(f"a schedule named {body.name!r} already exists on this queue")

    schedule = JobSchedule(
        id=uuid7(),
        organization_id=queue.organization_id,
        project_id=queue.project_id,
        queue_id=queue.id,
        name=body.name,
        cron=body.cron,
        timezone=body.timezone,
        is_active=body.is_active,
        # Seeded here so the dispatcher's ix_job_schedules_due predicate has
        # something to select on the very first tick.
        next_occurrence_at=next_occurrence(body.cron, body.timezone, datetime.now(UTC)),
        catchup_policy=body.catchup_policy,
        max_catchup_occurrences=body.max_catchup_occurrences,
        handler=body.handler,
        payload=body.payload,
        priority=body.priority,
        max_attempts=body.max_attempts,
        timeout_ms=timeout_ms,
    )
    session.add(schedule)
    await session.commit()
    await session.refresh(schedule)
    return schedule


@router.get("/queues/{queue_id}/schedules", response_model=Envelope[ScheduleOut])
async def list_schedules(
    queue_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    is_active: bool | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"is_active", "limit", "cursor"})
    stmt = select(JobSchedule).where(
        JobSchedule.queue_id == queue_id,
        JobSchedule.organization_id == principal.organization_id,
        JobSchedule.deleted_at.is_(None),
    )
    if is_active is not None:
        stmt = stmt.where(JobSchedule.is_active.is_(is_active))
    if cursor:
        ts, ident = decode_time_uuid_cursor(cursor)
        stmt = stmt.where(keyset_after_desc(JobSchedule.created_at, JobSchedule.id, ts, ident))
    stmt = stmt.order_by(JobSchedule.created_at.desc(), JobSchedule.id.desc()).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    data, page = paginate(rows, limit, lambda s: encode_time_cursor(s.created_at, s.id))
    return envelope(request, data, page)


@router.get("/schedules/{schedule_id}", response_model=ScheduleOut)
async def get_schedule(
    schedule_id: UUID, principal: PrincipalDep, session: SessionDep
) -> JobSchedule:
    return await _load_schedule(session, schedule_id, principal.organization_id)


@router.patch("/schedules/{schedule_id}", response_model=ScheduleOut)
async def update_schedule(
    schedule_id: UUID, body: ScheduleUpdate, principal: MemberDep, session: SessionDep
) -> JobSchedule:
    schedule = await _load_schedule(session, schedule_id, principal.organization_id)
    changes = body.model_dump(exclude_unset=True)

    if "timeout_ms" in changes and changes["timeout_ms"] is not None:
        queue = await get_queue(session, schedule.queue_id, principal.organization_id)
        _check_timeout(changes["timeout_ms"], queue)

    for field, value in changes.items():
        if value is not None:
            setattr(schedule, field, value)

    # Recompute whenever the rule itself moved. Deriving from now() rather than
    # from the old next_occurrence_at is correct here: the operator has changed the
    # rule, so the previous nominal instant no longer means anything.
    if changes.get("cron") is not None or changes.get("timezone") is not None:
        schedule.next_occurrence_at = next_occurrence(
            schedule.cron, schedule.timezone, datetime.now(UTC)
        )

    await session.commit()
    await session.refresh(schedule)
    return schedule


@router.delete("/schedules/{schedule_id}", status_code=204)
async def delete_schedule(
    schedule_id: UUID, principal: MemberDep, session: SessionDep
) -> Response:
    schedule = await _load_schedule(session, schedule_id, principal.organization_id)
    # Soft: occurrences already materialised keep a live parent, which is exactly
    # what jobs.schedule_id ON DELETE RESTRICT requires. is_active is cleared too,
    # so the dispatcher's partial index stops seeing it immediately.
    schedule.deleted_at = datetime.now(UTC)
    schedule.is_active = False
    schedule.next_occurrence_at = None
    await session.commit()
    return Response(status_code=204)
