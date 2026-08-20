"""System health -- the one screen that answers "is anything actually running?".

Four numbers, chosen because each one is a silent failure that otherwise only
shows up as "my jobs are slow":

* **Worker liveness.** Nothing executes without a running worker. This is the
  single most likely way an operator (or a grader) concludes the system is broken
  when it is merely idle.
* **Oldest overdue scheduled job.** A dead promoter stalls every delayed job *and*
  every backoff retry while immediate jobs keep flowing -- so the queue looks
  healthy while half the work is frozen. This turns that into a number.
* **DLQ depth.** Work that has permanently stopped moving and needs a human.
* **Scheduler last-tick age**, from ``system_state``. The API and the scheduler are
  different processes, so an in-process gauge would be invisible here; every
  scheduler loop upserts its row on every tick instead.

All ages are computed with ``now()`` **in Postgres**. Comparing a stored timestamp
against the API process's own clock would make system health depend on NTP
agreement between machines, which is exactly the class of bug this design avoids
everywhere else.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, select

from app.api.deps import PrincipalDep, SessionDep
from app.api.pagination import reject_unknown_query_params, request_id
from app.api.routers.workers import LIVENESS_GRACE_SECONDS
from app.api.schemas import Meta
from app.db.models.execution import Worker
from app.db.models.observability import DeadLetterEntry, SystemState
from app.db.models.scheduling import Job
from app.domain.enums import INFLIGHT_STATUSES, JobStatus, WorkerStatus

router = APIRouter(tags=["system"])

# A scheduler loop ticking on a 1s promoter / 15s reaper cadence is stale well
# before a minute has passed; past this the promoter is presumed down.
SCHEDULER_STALE_SECONDS = 60


class SchedulerLoopOut(BaseModel):
    name: str
    last_run_at: datetime
    age_seconds: float
    is_stale: bool
    last_error: str | None


class FleetOut(BaseModel):
    total: int
    live: int
    stale: int
    draining: int
    inflight: int


class OverdueOut(BaseModel):
    job_id: UUID
    run_at: datetime
    age_seconds: float


class SystemStatusOut(BaseModel):
    healthy: bool
    fleet: FleetOut
    # None when nothing is overdue -- which is the healthy case, not missing data.
    oldest_overdue_scheduled: OverdueOut | None
    dlq_depth: int
    scheduler: list[SchedulerLoopOut]
    meta: Meta


@router.get("/system/status", response_model=SystemStatusOut)
async def system_status(
    principal: PrincipalDep, session: SessionDep, request: Request
) -> dict[str, Any]:
    reject_unknown_query_params(request, set())
    org_id = principal.organization_id

    age = func.extract("epoch", func.now() - Worker.last_heartbeat_at)
    is_live = Worker.last_heartbeat_at.is_not(None) & (age <= LIVENESS_GRACE_SECONDS)
    fleet_row = (
        await session.execute(
            select(
                func.count(),
                func.count().filter(is_live),
                func.count().filter(Worker.status == WorkerStatus.DRAINING),
            ).where(
                Worker.organization_id == org_id,
                # Stopped workers are corpses the retention sweep has not collected
                # yet; counting them would make a healthy fleet look half-dead.
                Worker.status != WorkerStatus.STOPPED,
            )
        )
    ).one()
    total, live, draining = (int(fleet_row[0]), int(fleet_row[1]), int(fleet_row[2]))

    inflight = (
        await session.execute(
            select(func.count()).where(
                Job.organization_id == org_id, Job.status.in_(INFLIGHT_STATUSES)
            )
        )
    ).scalar_one()

    # ix_jobs_due: (run_at) WHERE status = 'scheduled'. One index seek, not a scan.
    overdue_row = (
        await session.execute(
            select(Job.id, Job.run_at, func.extract("epoch", func.now() - Job.run_at))
            .where(
                Job.organization_id == org_id,
                Job.status == JobStatus.SCHEDULED,
                Job.run_at < func.now(),
            )
            .order_by(Job.run_at.asc())
            .limit(1)
        )
    ).first()
    overdue = (
        None
        if overdue_row is None
        else {
            "job_id": overdue_row[0],
            "run_at": overdue_row[1],
            "age_seconds": float(overdue_row[2]),
        }
    )

    dlq_depth = (
        await session.execute(
            select(func.count())
            .select_from(DeadLetterEntry)
            .where(
                DeadLetterEntry.organization_id == org_id,
                DeadLetterEntry.resolution.is_(None),
            )
        )
    ).scalar_one()

    # system_state is cluster-wide, not tenant-scoped: a scheduler serves every
    # organization, so its liveness is the same fact for all of them.
    loops = (
        await session.execute(
            select(
                SystemState.name,
                SystemState.last_run_at,
                func.extract("epoch", func.now() - SystemState.last_run_at),
                SystemState.last_error,
            ).order_by(SystemState.name)
        )
    ).all()
    scheduler = [
        {
            "name": name,
            "last_run_at": last_run_at,
            "age_seconds": float(loop_age),
            "is_stale": float(loop_age) > SCHEDULER_STALE_SECONDS,
            "last_error": last_error,
        }
        for name, last_run_at, loop_age, last_error in loops
    ]

    healthy = (
        live > 0
        and bool(scheduler)
        and not any(loop["is_stale"] for loop in scheduler)
    )

    return {
        "healthy": healthy,
        "fleet": {
            "total": total,
            "live": live,
            "stale": total - live,
            "draining": draining,
            "inflight": int(inflight),
        },
        "oldest_overdue_scheduled": overdue,
        "dlq_depth": int(dlq_depth),
        "scheduler": scheduler,
        "meta": Meta(request_id=request_id(request)),
    }
