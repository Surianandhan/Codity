"""Pause semantics and cron dispatch under N schedulers.

The cron tests deliberately bypass ``SchedulerLoop.run_once`` and call
``CronLoop.tick`` on two independent sessions instead. ``run_once`` takes
``pg_try_advisory_xact_lock`` first, so the second scheduler would return
``skipped`` without ever touching a schedule -- and the property under test is that
correctness does **not** depend on that lock. The advisory lock exists only to stop
redundant ticks burning CPU; what makes N schedulers safe is
``ux_jobs_schedule_occurrence`` and ``FOR UPDATE SKIP LOCKED``, and those are what
these tests exercise.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app.db.models.base import uuid7
from app.db.models.scheduling import Job, JobSchedule
from app.domain.enums import JobKind, JobStatus
from app.scheduler.loops import CronLoop
from app.services.claim import claim_jobs, start_job
from tests.test_concurrency import (
    Scope,
    Sessions,
    count_rows,
    job_state,
    register_worker,
    seed_jobs,
    seed_scope,
)

pytestmark = pytest.mark.concurrency


async def set_paused(sessions: Sessions, queue_id: UUID, *, paused: bool) -> None:
    """``is_paused`` and ``paused_at`` are written together or the CHECK rejects it."""
    async with sessions() as s:
        await s.execute(
            text(
                "UPDATE queues SET is_paused = :paused,"
                " paused_at = CASE WHEN :paused THEN now() ELSE NULL END,"
                " updated_at = now()"
                " WHERE id = CAST(:q AS uuid)"
            ),
            {"paused": paused, "q": str(queue_id)},
        )
        await s.commit()


async def seed_schedule(
    sessions: Sessions,
    scope: Scope,
    *,
    cron: str = "* * * * *",
    due_at: datetime | None = None,
    name: str = "every-minute",
) -> tuple[UUID, datetime]:
    """A schedule whose next occurrence is already due. Returns (id, nominal instant).

    The nominal instant is truncated to the minute so it is a real occurrence of
    ``* * * * *`` rather than an arbitrary timestamp -- the drift rule computes the
    following occurrence from this value, not from ``now()``.
    """
    nominal = due_at or (
        datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=1)
    )
    async with sessions() as s:
        schedule = JobSchedule(
            id=uuid7(),
            organization_id=scope.org_id,
            project_id=scope.project_id,
            queue_id=scope.queue_id,
            name=name,
            cron=cron,
            timezone="UTC",
            is_active=True,
            next_occurrence_at=nominal,
            handler="demo.echo",
            payload={},
            timeout_ms=scope.timeout_ms,
        )
        s.add(schedule)
        await s.flush()
        schedule_id = schedule.id
        await s.commit()
    return schedule_id, nominal


async def schedule_state(sessions: Sessions, schedule_id: UUID) -> dict[str, object]:
    async with sessions() as s:
        row = (
            await s.execute(
                text(
                    "SELECT next_occurrence_at, last_occurrence_at, skipped_occurrences"
                    " FROM job_schedules WHERE id = CAST(:s AS uuid)"
                ),
                {"s": str(schedule_id)},
            )
        ).one()
    return dict(row._mapping)


# --- pause ------------------------------------------------------------------


async def test_pause_stops_claiming_resume_restarts(sessionmaker_: Sessions) -> None:
    """Pause blocks admission only: claims return zero, in-flight work is untouched.

    The mechanic is the ``AND NOT is_paused`` in the claim's ``q`` CTE. When it
    matches nothing, ``headroom`` is an empty set -- and ``COALESCE(max(n), 0)`` is
    what turns that into ``LIMIT 0``. A bare scalar subquery would yield NULL, and
    ``LIMIT NULL`` in Postgres means *no limit*, so a paused queue would drain
    itself at unlimited concurrency. That is the specific failure this test would
    catch.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=10)
    await seed_jobs(sessionmaker_, scope, 8)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "pauser")

    async with sessionmaker_() as s:
        before = await claim_jobs(s, scope.queue_id, worker_id, 2)
        await s.commit()
    assert len(before) == 2
    # One of them is genuinely mid-flight when the pause lands.
    async with sessionmaker_() as s:
        assert await start_job(s, before[0].job_id, worker_id, before[0].lease_epoch)
        await s.commit()

    await set_paused(sessionmaker_, scope.queue_id, paused=True)

    async with sessionmaker_() as s:
        during = await claim_jobs(s, scope.queue_id, worker_id, 5)
        await s.commit()
    assert during == [], "a paused queue admitted new work"

    # In-flight work is unaffected -- "paused, 2 still running".
    assert (await job_state(sessionmaker_, before[0].job_id))["status"] == JobStatus.RUNNING
    assert (await job_state(sessionmaker_, before[1].job_id))["status"] == JobStatus.CLAIMED
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM jobs WHERE queue_id = CAST(:q AS uuid) AND status = 'queued'",
            {"q": str(scope.queue_id)},
        )
        == 6
    ), "pausing moved queued jobs somewhere"

    await set_paused(sessionmaker_, scope.queue_id, paused=False)

    async with sessionmaker_() as s:
        after = await claim_jobs(s, scope.queue_id, worker_id, 5)
        await s.commit()
    assert len(after) == 5, "resume did not restart admission"


# --- cron -------------------------------------------------------------------


async def test_cron_two_schedulers_one_job_per_occurrence(sessionmaker_: Sessions) -> None:
    """Two schedulers tick the same due schedule: exactly one job for that instant.

    The two ticks overlap on purpose -- the second runs while the first still holds
    an uncommitted lock on the ``job_schedules`` row. Run them sequentially and the
    second sees an already-advanced ``next_occurrence_at`` and does nothing, which
    would prove only that the first tick worked.
    """
    scope = await seed_scope(sessionmaker_)
    schedule_id, nominal = await seed_schedule(sessionmaker_, scope)
    loop = CronLoop(sessionmaker_, 1_000, batch=10)

    async with sessionmaker_() as s1, sessionmaker_() as s2, s1.begin():
        first = await loop.tick(s1)
        # s1 holds the schedule row lock here, uncommitted.
        async with s2.begin():
            second = await loop.tick(s2)

    assert first.counts["jobs_created"] == 1
    assert second.counts["schedules"] == 0, "the second scheduler did not skip the locked row"

    occurrences = (
        "SELECT count(*) FROM jobs WHERE schedule_id = CAST(:s AS uuid)"
        " AND scheduled_for = CAST(:t AS timestamptz)"
    )
    assert (
        await count_rows(sessionmaker_, occurrences, {"s": str(schedule_id), "t": nominal})
        == 1
    ), "an occurrence was materialised twice"

    state = await schedule_state(sessionmaker_, schedule_id)
    assert isinstance(state["next_occurrence_at"], datetime)
    assert state["next_occurrence_at"] > nominal, "next_occurrence_at did not advance"
    assert state["last_occurrence_at"] == nominal

    # A second tick after both committed produces the *next* occurrence, not a
    # duplicate of this one.
    async with sessionmaker_() as s, s.begin():
        await loop.tick(s)
    assert (
        await count_rows(sessionmaker_, occurrences, {"s": str(schedule_id), "t": nominal})
        == 1
    )

    # And the guarantee itself, stated directly: the unique index is what makes N
    # schedulers safe, not the advisory lock and not the row lock.
    with pytest.raises(IntegrityError):
        async with sessionmaker_() as s:
            s.add(
                Job(
                    id=uuid7(),
                    organization_id=scope.org_id,
                    project_id=scope.project_id,
                    queue_id=scope.queue_id,
                    schedule_id=schedule_id,
                    kind=JobKind.RECURRING,
                    handler="demo.echo",
                    status=JobStatus.SCHEDULED,
                    run_at=nominal,
                    scheduled_for=nominal,
                    payload={},
                    timeout_ms=scope.timeout_ms,
                    lease_seconds=scope.lease_seconds,
                )
            )
            await s.commit()


async def test_cron_advances_when_insert_suppressed(sessionmaker_: Sessions) -> None:
    """A suppressed INSERT still advances ``next_occurrence_at``.

    The occurrence already exists -- a prior tick committed the job and crashed
    before advancing, or an operator used "run now" -- so ``ON CONFLICT DO NOTHING``
    returns no rows. Advancing only on returned rows would freeze
    ``next_occurrence_at`` at this instant: the schedule re-selects, re-conflicts,
    and does nothing forever, once per tick. A permanently dead schedule that looks
    perfectly healthy.
    """
    scope = await seed_scope(sessionmaker_)
    schedule_id, nominal = await seed_schedule(sessionmaker_, scope)

    # The occurrence is already there, exactly as a crashed tick would have left it.
    async with sessionmaker_() as s:
        s.add(
            Job(
                id=uuid7(),
                organization_id=scope.org_id,
                project_id=scope.project_id,
                queue_id=scope.queue_id,
                schedule_id=schedule_id,
                kind=JobKind.RECURRING,
                handler="demo.echo",
                status=JobStatus.SCHEDULED,
                run_at=nominal,
                scheduled_for=nominal,
                payload={},
                timeout_ms=scope.timeout_ms,
                lease_seconds=scope.lease_seconds,
            )
        )
        await s.commit()

    loop = CronLoop(sessionmaker_, 1_000, batch=10)
    async with sessionmaker_() as s, s.begin():
        tick = await loop.tick(s)

    assert tick.counts["schedules"] == 1
    assert tick.counts["jobs_created"] == 0
    assert tick.counts["occurrences_suppressed"] == 1

    state = await schedule_state(sessionmaker_, schedule_id)
    assert isinstance(state["next_occurrence_at"], datetime)
    assert state["next_occurrence_at"] > nominal, (
        "next_occurrence_at is frozen on the occurrence that was suppressed:"
        " this schedule will never fire again"
    )
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM jobs WHERE schedule_id = CAST(:s AS uuid)",
            {"s": str(schedule_id)},
        )
        == 1
    )


async def test_cron_does_not_materialise_into_a_paused_queue(
    sessionmaker_: Sessions,
) -> None:
    """A paused queue accumulates ``skipped_occurrences``, not a backlog.

    Materialising into a paused queue would let a per-minute schedule silently build
    a pile of jobs that stampedes the instant someone resumes it -- the opposite of
    what pausing is for.
    """
    scope = await seed_scope(sessionmaker_)
    schedule_id, nominal = await seed_schedule(sessionmaker_, scope)
    await set_paused(sessionmaker_, scope.queue_id, paused=True)

    loop = CronLoop(sessionmaker_, 1_000, batch=10)
    async with sessionmaker_() as s, s.begin():
        tick = await loop.tick(s)

    assert tick.counts["jobs_created"] == 0
    assert tick.counts["paused_skipped"] == 1
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM jobs WHERE schedule_id = CAST(:s AS uuid)",
            {"s": str(schedule_id)},
        )
        == 0
    )

    state = await schedule_state(sessionmaker_, schedule_id)
    assert state["skipped_occurrences"] == 1
    assert isinstance(state["next_occurrence_at"], datetime)
    assert state["next_occurrence_at"] > nominal, "a paused queue froze its schedule"
