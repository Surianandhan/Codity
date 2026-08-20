from uuid import UUID

from fastapi import APIRouter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import MemberDep, PrincipalDep, SessionDep
from app.api.schemas import ProjectIn, ProjectOut, QueueIn, QueueOut
from app.db.models.base import uuid7
from app.db.models.scheduling import Queue
from app.db.models.tenancy import Project
from app.domain.errors import ConflictError, NotFoundError

router = APIRouter(tags=["projects"])


@router.post("/orgs/{org_id}/projects", response_model=ProjectOut, status_code=201)
async def create_project(
    org_id: UUID, body: ProjectIn, principal: MemberDep, session: SessionDep
) -> Project:
    if org_id != principal.organization_id:
        raise NotFoundError("organization not found")
    dup = (
        await session.execute(
            select(Project.id).where(Project.organization_id == org_id, Project.slug == body.slug)
        )
    ).scalar_one_or_none()
    if dup:
        raise ConflictError(f"project slug {body.slug!r} already exists in this organization")
    project = Project(id=uuid7(), organization_id=org_id, name=body.name, slug=body.slug)
    session.add(project)
    await session.commit()
    await session.refresh(project)
    return project


@router.get("/orgs/{org_id}/projects", response_model=list[ProjectOut])
async def list_projects(
    org_id: UUID, principal: PrincipalDep, session: SessionDep
) -> list[Project]:
    if org_id != principal.organization_id:
        raise NotFoundError("organization not found")
    return list(
        (
            await session.execute(
                select(Project)
                .where(Project.organization_id == org_id)
                .order_by(Project.created_at)
            )
        ).scalars()
    )


@router.post("/projects/{project_id}/queues", response_model=QueueOut, status_code=201)
async def create_queue(
    project_id: UUID, body: QueueIn, principal: MemberDep, session: SessionDep
) -> Queue:
    project = (
        await session.execute(
            select(Project).where(
                Project.id == project_id, Project.organization_id == principal.organization_id
            )
        )
    ).scalar_one_or_none()
    if project is None:
        raise NotFoundError("project not found")

    queue = Queue(
        id=uuid7(),
        organization_id=principal.organization_id,
        project_id=project_id,
        **body.model_dump(),
    )
    session.add(queue)
    await session.commit()
    await session.refresh(queue)
    return queue


@router.get("/projects/{project_id}/queues", response_model=list[QueueOut])
async def list_queues(
    project_id: UUID, principal: PrincipalDep, session: SessionDep
) -> list[Queue]:
    return list(
        (
            await session.execute(
                select(Queue)
                .where(Queue.project_id == project_id,
                       Queue.organization_id == principal.organization_id)
                .order_by(Queue.name)
            )
        ).scalars()
    )


@router.post("/queues/{queue_id}/pause", response_model=QueueOut)
async def pause_queue(queue_id: UUID, principal: MemberDep, session: SessionDep) -> Queue:
    return await _set_paused(session, queue_id, principal.organization_id, True)


@router.post("/queues/{queue_id}/resume", response_model=QueueOut)
async def resume_queue(queue_id: UUID, principal: MemberDep, session: SessionDep) -> Queue:
    return await _set_paused(session, queue_id, principal.organization_id, False)


async def _set_paused(
    session: AsyncSession, queue_id: UUID, org_id: UUID, paused: bool
) -> Queue:
    from datetime import UTC, datetime

    queue = (
        await session.execute(
            select(Queue).where(Queue.id == queue_id, Queue.organization_id == org_id)
        )
    ).scalar_one_or_none()
    if queue is None:
        raise NotFoundError("queue not found")
    # Both columns must move together or ck_queues_pause_consistency rejects the write.
    queue.is_paused = paused
    queue.paused_at = datetime.now(UTC) if paused else None
    await session.commit()
    await session.refresh(queue)
    return queue
