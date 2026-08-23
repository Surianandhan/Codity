"""Regression tests for the execution-row lifecycle.

Before these, ``start_job`` updated only ``jobs``. The execution row opened at claim
time kept ``started_at`` NULL forever, which meant:

* every ``duration_ms`` in the product was NULL -- ``complete_job`` and ``fail_job``
  both derive it from ``e.started_at``;
* ``ExecutionStatus.RUNNING`` was unreachable, so an attempt could never be observed
  in progress and the job timeline jumped straight from ``claimed`` to a terminal
  state.

The existing e2e test selected ``duration_ms`` and never asserted on it, which is
exactly why this survived. These tests assert the value, not just the shape.
"""

import asyncio
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text

from app.services.claim import complete_job, start_job
from tests.test_concurrency import (
    Sessions,
    claim_one,
    register_worker,
    seed_jobs,
    seed_scope,
)

pytestmark = pytest.mark.concurrency


async def _read_execution(sessions: Sessions, job_id: UUID) -> dict[str, Any]:
    async with sessions() as s:
        row = (
            await s.execute(
                text(
                    "SELECT status::text AS status, started_at, duration_ms"
                    "  FROM job_executions WHERE job_id = CAST(:j AS uuid)"
                ),
                {"j": str(job_id)},
            )
        ).one()
    return {
        "status": row.status,
        "started_at": row.started_at,
        "duration_ms": row.duration_ms,
    }


async def test_start_job_marks_the_attempt_running(sessionmaker_: Sessions) -> None:
    """An in-progress attempt must be visible as 'running' with started_at set.

    This is what the dashboard timeline reads to show an attempt that has begun but
    not yet finished.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "timing-worker")
    epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)

    at_claim = await _read_execution(sessionmaker_, job_id)
    assert at_claim["status"] == "claimed"
    assert at_claim["started_at"] is None, "the attempt has not begun yet"

    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, epoch)
        await s.commit()

    in_flight = await _read_execution(sessionmaker_, job_id)
    assert in_flight["status"] == "running", "an attempt in progress must be observable"
    assert in_flight["started_at"] is not None


async def test_completed_attempt_records_a_duration(sessionmaker_: Sessions) -> None:
    """duration_ms is computed from e.started_at, so it is NULL unless start_job set it."""
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "duration-worker")
    epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)

    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, epoch)
        await s.commit()

    await asyncio.sleep(0.05)

    async with sessionmaker_() as s:
        assert await complete_job(s, job_id, worker_id, epoch, {"ok": True})
        await s.commit()

    done = await _read_execution(sessionmaker_, job_id)
    assert done["status"] == "succeeded"
    assert done["duration_ms"] is not None, "duration_ms must not be NULL"
    assert done["duration_ms"] >= 0


async def test_start_job_does_not_touch_a_stale_epochs_attempt(
    sessionmaker_: Sessions,
) -> None:
    """The execution UPDATE is fenced on lease_epoch, exactly like the jobs UPDATE.

    A worker holding a superseded epoch -- the state a reaped worker is left in --
    must move neither row.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "fence-worker")
    epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)

    async with sessionmaker_() as s:
        assert not await start_job(s, job_id, worker_id, epoch - 1)
        await s.commit()

    untouched = await _read_execution(sessionmaker_, job_id)
    assert untouched["status"] == "claimed", "a stale epoch must not start the attempt"
    assert untouched["started_at"] is None
