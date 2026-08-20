"""The worker fleet: who is alive, what they are holding, and how to drain them.

Liveness is derived, never stored as a boolean. A worker cannot write "I am dead",
so a ``is_live`` column would be permanently stale for exactly the workers that
matter. The only honest signal is the age of ``last_heartbeat_at`` measured
against ``now()`` **in Postgres** -- comparing against the API process's own clock
would make fleet health depend on NTP agreement between machines.
"""

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request
from sqlalchemy import func, select
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
    single_page,
)
from app.api.schemas import ORMModel
from app.db.models.execution import Worker, WorkerHeartbeat
from app.db.models.scheduling import Job
from app.domain.enums import INFLIGHT_STATUSES
from app.domain.errors import NotFoundError

router = APIRouter(tags=["workers"])

# A worker heartbeats every lease_seconds / 6 -- 50s on the default 300s queue
# lease. Three missed beats is the liveness cut-off: shorter and a single GC pause
# flags a healthy worker as dead, longer and the Workers screen lags the reaper
# that is already reclaiming its jobs.
LIVENESS_GRACE_SECONDS = 150

# The heartbeat history shown on a worker detail page. Bounded rather than paged:
# the question it answers ("is this worker beating steadily?") is answered by the
# recent tail, and older beats belong to the retention sweep.
HEARTBEAT_HISTORY = 60


class WorkerOut(ORMModel):
    id: UUID
    name: str
    hostname: str | None
    pid: int | None
    status: str
    concurrency: int
    last_heartbeat_at: datetime | None
    heartbeat_age_seconds: float | None
    is_live: bool
    drain_requested: bool
    started_at: datetime | None
    stopped_at: datetime | None
    created_at: datetime
    inflight: int


class HeartbeatOut(ORMModel):
    id: int
    beat_at: datetime
    inflight: int


class WorkerDetailOut(WorkerOut):
    heartbeats: list[HeartbeatOut]


async def _inflight_by_worker(
    session: AsyncSession, org_id: UUID, worker_ids: list[UUID]
) -> dict[UUID, int]:
    """One grouped query for the whole page rather than N+1 counts."""
    if not worker_ids:
        return {}
    rows = (
        await session.execute(
            select(Job.worker_id, func.count())
            .where(
                Job.organization_id == org_id,
                Job.worker_id.in_(worker_ids),
                Job.status.in_(INFLIGHT_STATUSES),
            )
            .group_by(Job.worker_id)
        )
    ).all()
    return {wid: int(n) for wid, n in rows if wid is not None}


def _shape(worker: Worker, age: float | None, inflight: int) -> dict[str, Any]:
    return {
        "id": worker.id,
        "name": worker.name,
        "hostname": worker.hostname,
        "pid": worker.pid,
        "status": worker.status,
        "concurrency": worker.concurrency,
        "last_heartbeat_at": worker.last_heartbeat_at,
        "heartbeat_age_seconds": age,
        "is_live": age is not None and age <= LIVENESS_GRACE_SECONDS,
        "drain_requested": worker.drain_requested,
        "started_at": worker.started_at,
        "stopped_at": worker.stopped_at,
        "created_at": worker.created_at,
        "inflight": inflight,
    }


def _age_column() -> Any:
    """Heartbeat age in seconds, computed by Postgres so every worker is measured
    against one clock."""
    return func.extract("epoch", func.now() - Worker.last_heartbeat_at)


@router.get("/orgs/{org_id}/workers", response_model=Envelope[WorkerOut])
async def list_workers(
    org_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    status: Annotated[list[str] | None, Query()] = None,
    live: bool | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
    cursor: str | None = None,
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"status", "live", "limit", "cursor"})
    if org_id != principal.organization_id:
        raise NotFoundError("organization not found")

    age = _age_column()
    stmt = select(Worker, age).where(Worker.organization_id == org_id)
    if status:
        stmt = stmt.where(Worker.status.in_(status))
    if live is not None:
        alive = Worker.last_heartbeat_at.is_not(None) & (age <= LIVENESS_GRACE_SECONDS)
        stmt = stmt.where(alive if live else ~alive)
    if cursor:
        ts, ident = decode_time_uuid_cursor(cursor)
        stmt = stmt.where(keyset_after_desc(Worker.created_at, Worker.id, ts, ident))
    stmt = stmt.order_by(Worker.created_at.desc(), Worker.id.desc()).limit(limit + 1)

    rows = [(w, None if a is None else float(a)) for w, a in (await session.execute(stmt)).all()]
    page_rows, page = paginate(
        rows, limit, lambda row: encode_time_cursor(row[0].created_at, row[0].id)
    )
    inflight = await _inflight_by_worker(session, org_id, [w.id for w, _ in page_rows])
    data = [_shape(w, a, inflight.get(w.id, 0)) for w, a in page_rows]
    return envelope(request, data, page)


async def _load_worker(
    session: AsyncSession, worker_id: UUID, org_id: UUID
) -> tuple[Worker, float | None]:
    row = (
        await session.execute(
            select(Worker, _age_column()).where(
                Worker.id == worker_id, Worker.organization_id == org_id
            )
        )
    ).first()
    if row is None:
        raise NotFoundError("worker not found")
    worker, age = row
    return worker, None if age is None else float(age)


@router.get("/workers/{worker_id}", response_model=WorkerDetailOut)
async def get_worker(
    worker_id: UUID, principal: PrincipalDep, session: SessionDep
) -> dict[str, Any]:
    org_id = principal.organization_id
    worker, age = await _load_worker(session, worker_id, org_id)
    inflight = await _inflight_by_worker(session, org_id, [worker.id])

    beats = list(
        (
            await session.execute(
                select(WorkerHeartbeat)
                .where(WorkerHeartbeat.worker_id == worker_id)
                .order_by(WorkerHeartbeat.beat_at.desc(), WorkerHeartbeat.id.desc())
                .limit(HEARTBEAT_HISTORY)
            )
        ).scalars()
    )
    shaped = _shape(worker, age, inflight.get(worker.id, 0))
    shaped["heartbeats"] = beats
    return shaped


@router.get("/workers/{worker_id}/heartbeats", response_model=Envelope[HeartbeatOut])
async def list_heartbeats(
    worker_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"limit"})
    await _load_worker(session, worker_id, principal.organization_id)
    beats = list(
        (
            await session.execute(
                select(WorkerHeartbeat)
                .where(WorkerHeartbeat.worker_id == worker_id)
                .order_by(WorkerHeartbeat.beat_at.desc(), WorkerHeartbeat.id.desc())
                .limit(limit)
            )
        ).scalars()
    )
    return single_page(request, beats, limit)


@router.post("/workers/{worker_id}/drain", response_model=WorkerOut)
async def drain_worker(
    worker_id: UUID, principal: MemberDep, session: SessionDep
) -> dict[str, Any]:
    """Ask a worker to stop claiming and finish what it holds.

    The worker learns about this from the RETURNING clause of the heartbeat it
    already issues every ``lease_seconds / 6`` -- no extra query on the hot path,
    and no field that validates but does nothing.
    """
    org_id = principal.organization_id
    worker, age = await _load_worker(session, worker_id, org_id)
    worker.drain_requested = True
    await session.commit()
    await session.refresh(worker)
    inflight = await _inflight_by_worker(session, org_id, [worker.id])
    return _shape(worker, age, inflight.get(worker.id, 0))
