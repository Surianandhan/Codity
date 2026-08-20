from uuid import UUID

from fastapi import APIRouter, Request
from sqlalchemy import select

from app.api.deps import MemberDep, PrincipalDep, SessionDep
from app.api.schemas import JobCreate, JobOut
from app.db.models.scheduling import Job
from app.domain.errors import NotFoundError, QueuePaused
from app.services.jobs import create_job, get_queue

router = APIRouter(tags=["jobs"])


@router.post("/queues/{queue_id}/jobs", response_model=JobOut, status_code=201)
async def enqueue(
    queue_id: UUID,
    body: JobCreate,
    principal: MemberDep,
    session: SessionDep,
    request: Request,
) -> Job:
    queue = await get_queue(session, queue_id, principal.organization_id)
    if queue.is_paused:
        raise QueuePaused(f"queue {queue.name!r} is paused")
    job = await create_job(
        session, queue, body, correlation_id=getattr(request.state, "request_id", None)
    )
    await session.commit()
    await session.refresh(job)
    return job


@router.get("/jobs/{job_id}", response_model=JobOut)
async def get_job(job_id: UUID, principal: PrincipalDep, session: SessionDep) -> Job:
    job = (
        await session.execute(
            select(Job).where(Job.id == job_id, Job.organization_id == principal.organization_id)
        )
    ).scalar_one_or_none()
    if job is None:
        raise NotFoundError("job not found")
    return job


@router.get("/queues/{queue_id}/jobs", response_model=list[JobOut])
async def list_jobs(
    queue_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    limit: int = 50,
) -> list[Job]:
    return list(
        (
            await session.execute(
                select(Job)
                .where(Job.queue_id == queue_id, Job.organization_id == principal.organization_id)
                .order_by(Job.created_at.desc(), Job.id.desc())
                .limit(min(limit, 200))
            )
        ).scalars()
    )
