"""The claim/start/complete path. Imported by the worker AND by tests, and it never
imports FastAPI -- that separation is what lets concurrency tests drive the real
code rather than a reimplementation."""

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

SQL_DIR = Path(__file__).resolve().parent.parent / "db" / "sql"


@lru_cache
def load_sql(name: str) -> str:
    return (SQL_DIR / f"{name}.sql").read_text()


@dataclass(frozen=True)
class ClaimedJob:
    job_id: UUID
    organization_id: UUID
    queue_id: UUID
    attempt_number: int
    lease_epoch: int


async def claim_jobs(
    session: AsyncSession, queue_id: UUID, worker_id: UUID, batch_size: int
) -> list[ClaimedJob]:
    """Atomically claim up to batch_size jobs. Never exceeds the queue's
    max_concurrency, and never returns a job another worker also holds.

    Issued as TWO statements in one transaction, not one:

    FOR NO KEY UPDATE only forces a fresh re-check of the LOCKED ROW ITSELF for a
    transaction that blocked on it (Postgres's EvalPlanQual). It does not give the
    rest of a single statement a fresh snapshot. If the queue lock and the
    in-flight COUNT lived in the same statement, a claimer that waited behind
    another transaction's commit would still see the in-flight count as it stood
    when ITS statement started -- stale, understating in-flight jobs -- and
    max_concurrency would overshoot under real concurrency. This was only caught
    by a genuine concurrent stress test; nothing single-statement reveals it.

    Splitting into two statements fixes it: step 2 is a NEW statement, and READ
    COMMITTED gives every new statement a fresh snapshot -- taken only once the
    lock already held in step 1 makes that snapshot safe to act on.
    """
    locked = (
        await session.execute(text(load_sql("lock_queue")), {"queue_id": str(queue_id)})
    ).first()
    if locked is None:
        # Queue is paused or does not exist. Do not issue the second statement at
        # all -- there is nothing to compute headroom against.
        return []

    rows = (
        await session.execute(
            text(load_sql("claim_jobs")),
            {
                "queue_id": str(queue_id),
                "batch_size": batch_size,
                "worker_id": str(worker_id),
                "max_concurrency": locked.max_concurrency,
            },
        )
    ).all()
    return [
        ClaimedJob(
            job_id=r.job_id,
            organization_id=r.organization_id,
            queue_id=r.queue_id,
            attempt_number=r.attempt_number,
            lease_epoch=r.lease_epoch,
        )
        for r in rows
    ]


async def start_job(
    session: AsyncSession, job_id: UUID, worker_id: UUID, lease_epoch: int
) -> bool:
    """Claimed -> running. Returns False if the claim is no longer ours.

    A False here MUST abort the task before the handler runs: it means graceful
    shutdown released the job or the reaper took it, and another worker may already
    be executing it.
    """
    row = (
        await session.execute(
            text(load_sql("start_job")),
            {"job_id": str(job_id), "worker_id": str(worker_id), "lease_epoch": lease_epoch},
        )
    ).first()
    return row is not None


async def complete_job(
    session: AsyncSession,
    job_id: UUID,
    worker_id: UUID,
    lease_epoch: int,
    result: dict[str, Any] | None = None,
) -> bool:
    """Running -> completed, fenced on lease_epoch. False means the lease was stolen
    and the result must be discarded rather than committed over the live attempt."""
    row = (
        await session.execute(
            text(load_sql("complete_job")),
            {
                "job_id": str(job_id),
                "worker_id": str(worker_id),
                "lease_epoch": lease_epoch,
                "result": json.dumps(result) if result is not None else None,
            },
        )
    ).first()
    return row is not None
