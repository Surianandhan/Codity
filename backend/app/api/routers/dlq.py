"""The dead letter queue: what broke, and what an operator did about it.

Replay never touches the dead job. Terminal states have zero out-edges, so a
replay inserts a *new* job carrying ``replay_of_job_id`` and resolves the entry
that pointed at the corpse. That keeps history immutable, keeps the transition
trigger simple, and means the DLQ screen can show "replayed as <job>" rather than
losing the evidence it was built to preserve.
"""

from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Query, Request
from sqlalchemy import ColumnElement, select, update
from sqlalchemy.engine import CursorResult
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
    request_id,
)
from app.api.routers.jobs import create_replay_job
from app.api.schemas import JobOut, ORMModel
from app.db.models.observability import DeadLetterEntry
from app.db.models.scheduling import Job
from app.domain.errors import ConflictError, NotFoundError, QueuePaused
from app.services.jobs import get_queue

router = APIRouter(tags=["dlq"])

RESOLUTION_REQUEUED = "requeued"
RESOLUTION_DISCARDED = "discarded"


class DlqEntryOut(ORMModel):
    id: UUID
    project_id: UUID
    queue_id: UUID
    job_id: UUID
    correlation_id: str | None
    error_class: str | None
    error_message: str | None
    # md5(error_class || error_message[:200]). Groups a poison-pill cluster into
    # one line on the DLQ screen instead of ten thousand.
    error_fingerprint: str | None
    payload_snapshot: dict[str, Any]
    dead_lettered_at: datetime
    resolution: str | None
    resolved_at: datetime | None
    resolved_by: UUID | None
    replay_job_id: UUID | None


class DlqReplayOut(ORMModel):
    entry: DlqEntryOut
    job: JobOut


_DLQ_FILTERS = frozenset(
    {
        "resolution",
        "queue_id",
        "error_class",
        "error_fingerprint",
        "dead_lettered_after",
        "dead_lettered_before",
        "limit",
        "cursor",
    }
)


@router.get("/projects/{project_id}/dlq", response_model=Envelope[DlqEntryOut])
async def list_dlq(
    project_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    resolution: Literal["open", "resolved", "all"] = "open",
    queue_id: UUID | None = None,
    error_class: str | None = None,
    error_fingerprint: str | None = None,
    dead_lettered_after: datetime | None = None,
    dead_lettered_before: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, _DLQ_FILTERS)

    where: list[ColumnElement[bool]] = [
        DeadLetterEntry.project_id == project_id,
        DeadLetterEntry.organization_id == principal.organization_id,
    ]
    # 'open' is the default and rides ix_dlq_open, whose partial predicate is
    # exactly `resolution IS NULL`.
    if resolution == "open":
        where.append(DeadLetterEntry.resolution.is_(None))
    elif resolution == "resolved":
        where.append(DeadLetterEntry.resolution.is_not(None))
    if queue_id is not None:
        where.append(DeadLetterEntry.queue_id == queue_id)
    if error_class is not None:
        where.append(DeadLetterEntry.error_class == error_class)
    if error_fingerprint is not None:
        where.append(DeadLetterEntry.error_fingerprint == error_fingerprint)
    if dead_lettered_after is not None:
        where.append(DeadLetterEntry.dead_lettered_at >= dead_lettered_after)
    if dead_lettered_before is not None:
        where.append(DeadLetterEntry.dead_lettered_at < dead_lettered_before)

    stmt = select(DeadLetterEntry).where(*where)
    if cursor:
        ts, ident = decode_time_uuid_cursor(cursor)
        stmt = stmt.where(
            keyset_after_desc(
                DeadLetterEntry.dead_lettered_at, DeadLetterEntry.id, ts, ident
            )
        )
    stmt = stmt.order_by(
        DeadLetterEntry.dead_lettered_at.desc(), DeadLetterEntry.id.desc()
    ).limit(limit + 1)

    rows = list((await session.execute(stmt)).scalars())
    data, page = paginate(rows, limit, lambda e: encode_time_cursor(e.dead_lettered_at, e.id))
    return envelope(request, data, page)


async def _resolve_entry(
    session: AsyncSession, entry_id: UUID, org_id: UUID, actor_id: UUID, resolution: str
) -> DeadLetterEntry:
    """Claim the entry with a guarded UPDATE.

    Read-then-write would let two operators both see an open entry and both replay
    it, producing two jobs for one failure. The conditional UPDATE takes the row
    lock, so the loser sees rowcount 0 once the winner commits and gets a 409.
    ``resolution`` and ``resolved_at`` move together or ck_dead_letter_entries_
    resolution_closed rejects the write.
    """
    result = cast(
        CursorResult[Any],
        await session.execute(
            update(DeadLetterEntry)
            .where(
                DeadLetterEntry.id == entry_id,
                DeadLetterEntry.organization_id == org_id,
                DeadLetterEntry.resolution.is_(None),
            )
            .values(
                resolution=resolution,
                resolved_at=datetime.now(UTC),
                resolved_by=actor_id,
            )
        ),
    )
    entry = (
        await session.execute(
            select(DeadLetterEntry).where(
                DeadLetterEntry.id == entry_id,
                DeadLetterEntry.organization_id == org_id,
            )
        )
    ).scalar_one_or_none()
    if entry is None:
        raise NotFoundError("dead letter entry not found")
    if not result.rowcount:
        raise ConflictError(
            f"dead letter entry is already resolved ({entry.resolution})"
        )
    return entry


@router.post("/dlq/{entry_id}/replay", response_model=DlqReplayOut, status_code=201)
async def replay_entry(
    entry_id: UUID, principal: MemberDep, session: SessionDep, request: Request
) -> dict[str, Any]:
    org_id = principal.organization_id
    entry = await _resolve_entry(session, entry_id, org_id, principal.user_id, RESOLUTION_REQUEUED)

    source = (
        await session.execute(
            select(Job).where(Job.id == entry.job_id, Job.organization_id == org_id)
        )
    ).scalar_one_or_none()
    if source is None:
        # dead_letter_entries.job_id is ON DELETE RESTRICT precisely so an
        # unresolved entry outlives the retention sweep; reaching here means the
        # job was purged after the entry was already resolved.
        raise NotFoundError("the dead-lettered job no longer exists")

    queue = await get_queue(session, entry.queue_id, org_id)
    if queue.is_paused:
        raise QueuePaused(f"queue {queue.name!r} is paused")

    job = await create_replay_job(
        session,
        source=source,
        queue=queue,
        # The snapshot, not jobs.payload: the snapshot is the payload as it was
        # when the job died and cannot be purged out from under the operator.
        payload=entry.payload_snapshot,
        correlation_id=request_id(request),
    )
    entry.replay_job_id = job.id
    await session.commit()
    await session.refresh(entry)
    await session.refresh(job)
    return {"entry": entry, "job": job}


@router.post("/dlq/{entry_id}/discard", response_model=DlqEntryOut)
async def discard_entry(
    entry_id: UUID, principal: MemberDep, session: SessionDep
) -> DeadLetterEntry:
    entry = await _resolve_entry(
        session, entry_id, principal.organization_id, principal.user_id, RESOLUTION_DISCARDED
    )
    await session.commit()
    await session.refresh(entry)
    return entry
