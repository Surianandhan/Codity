"""Worker lifecycle defects that only a *second* worker can expose.

Each test here reproduces a specific interleaving that the single-worker happy path
never reaches, and each one fails against the shipped behaviour it replaces:

* the unregistered-handler release closed ``job_executions`` with an UPDATE fenced on
  nothing at all -- so a stale worker waking up after being reaped closed the LIVE
  execution row of the worker that had since taken the job;
* the same release parked the job in ``queued`` (re-claimable on the next ~500ms
  poll) and incremented an ``unregistered_count`` that nothing ever read, so the
  claim/release loop it was supposed to bound was unbounded;
* the heartbeat interval was sized from the queues the worker might claim FROM
  rather than the leases it actually HOLDS, so pausing a short-lease queue slowed the
  beat below the lease of a job already running on it.

Helpers come from ``test_concurrency`` for the same reason that module states: these
drive the real SQL through the real service wrappers, on genuinely independent
committed sessions.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy import text

from app.db.models.base import uuid7
from app.db.models.scheduling import Queue
from app.domain.enums import JobStatus
from app.services.claim import ClaimedJob, claim_jobs, start_job
from app.services.reliability import promote_due, reap_expired_leases
from app.worker.runner import UNREGISTERED_MAX_RELEASES, WorkerRunner
from tests.test_concurrency import (
    Scope,
    Sessions,
    claim_one,
    count_rows,
    expire_lease,
    job_state,
    register_worker,
    seed_jobs,
    seed_scope,
)

pytestmark = pytest.mark.concurrency

MISSING_HANDLER = "shipped.in.the.next.deploy"


# --- helpers ----------------------------------------------------------------


async def _executions(sessions: Sessions, job_id: UUID) -> list[dict[str, Any]]:
    """Every attempt row for a job, oldest first."""
    async with sessions() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT id, status::text AS status, lease_epoch, finished_at,"
                    "       error_class"
                    "  FROM job_executions WHERE job_id = CAST(:j AS uuid) ORDER BY id"
                ),
                {"j": str(job_id)},
            )
        ).all()
    return [dict(r._mapping) for r in rows]


async def _unregistered_count(sessions: Sessions, job_id: UUID) -> int:
    return await count_rows(
        sessions,
        "SELECT unregistered_count FROM jobs WHERE id = CAST(:j AS uuid)",
        {"j": str(job_id)},
    )


async def _claimed(sessions: Sessions, scope: Scope, worker_id: UUID, job_id: UUID) -> ClaimedJob:
    """Claim ``job_id`` and hand back the ``ClaimedJob`` the runner would be holding."""
    epoch = await claim_one(sessions, scope, worker_id, job_id)
    return ClaimedJob(
        job_id=job_id,
        organization_id=scope.org_id,
        queue_id=scope.queue_id,
        attempt_number=1,
        lease_epoch=epoch,
    )


async def _promote(sessions: Sessions, job_id: UUID) -> None:
    """Age a scheduled job's backoff away and run the real promoter over it.

    The claim query has no ``run_at`` predicate, so this -- not the passage of the
    poll interval -- is the only thing that makes a released job claimable again.
    """
    async with sessions() as s:
        await s.execute(
            text(
                "UPDATE jobs SET run_at = now() - interval '1 second'"
                " WHERE id = CAST(:j AS uuid)"
            ),
            {"j": str(job_id)},
        )
        await s.commit()
    async with sessions() as s:
        await promote_due(s)
        await s.commit()


# --- FIX 1: the release must not touch another worker's live attempt --------


async def test_unregistered_release_cannot_close_a_live_execution_row(
    sessionmaker_: Sessions,
) -> None:
    """A stale worker's unregistered-handler release must not close worker B's row.

    The interleaving, all of which is reachable on any rolling deploy:

    1. worker A claims job J and starts it, finds the handler missing, then stalls
       (GC pause, SIGSTOP, a slow session) before it can release;
    2. J's lease expires, the reaper requeues it and closes A's execution row;
    3. worker B claims J, starts it, and opens a NEW execution row;
    4. A wakes and runs the release.

    The ``jobs`` UPDATE in step 4 is fenced on ``lease_epoch`` and matches nothing,
    which is correct. The execution close used to be a separate, unfenced, ungated
    statement whose only predicates were ``job_id`` and ``finished_at IS NULL`` -- and
    the row matching those is **B's**. B's successful work then rendered on the
    timeline as a lost attempt with ``error_class = 'UnregisteredHandler'``: a failure
    that never happened, attributed to a worker that never saw it.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1, handler=MISSING_HANDLER)

    runner_a = WorkerRunner(sessionmaker_, scope.org_id, name="stale-a", concurrency=1)
    worker_a = await runner_a.register()
    worker_b = await register_worker(sessionmaker_, scope.org_id, "live-b")

    # 1. A claims and starts, then stalls holding this ClaimedJob.
    cj_a = await _claimed(sessionmaker_, scope, worker_a, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_a, cj_a.lease_epoch)
        await s.commit()

    # 2. The lease expires and the reaper takes J back, closing A's attempt row.
    await expire_lease(sessionmaker_, job_id)
    async with sessionmaker_() as s:
        reaped = await reap_expired_leases(s)
        await s.commit()
    assert [r.job_id for r in reaped] == [job_id]

    # 3. B claims and starts it. This is the live attempt.
    epoch_b = await claim_one(sessionmaker_, scope, worker_b, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_b, epoch_b)
        await s.commit()

    # 4. A finally runs the block it was stalled in front of.
    await runner_a._release_unregistered(cj_a, MISSING_HANDLER)

    rows = await _executions(sessionmaker_, job_id)
    assert len(rows) == 2, "expected A's reaped attempt and B's live attempt"
    stale, live = rows
    assert stale["lease_epoch"] == cj_a.lease_epoch
    assert stale["status"] == "lost"
    assert stale["error_class"] == "LeaseExpired", "the reaper's verdict was overwritten"

    assert live["lease_epoch"] == epoch_b
    assert live["finished_at"] is None, (
        "a stale worker closed the LIVE execution row of the worker now running the"
        " job -- the execution UPDATE ran on job_id alone, unfenced on lease_epoch and"
        " ungated by whether the jobs UPDATE matched anything"
    )
    assert live["status"] == "running"
    assert live["error_class"] is None

    # And the job itself is untouched: still B's, still running, still on B's epoch.
    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.RUNNING
    assert state["worker_id"] == worker_b
    assert state["lease_epoch"] == epoch_b
    assert state["finished_at"] is None
    assert await _unregistered_count(sessionmaker_, job_id) == 0, (
        "a fenced-out release counted itself against the job it no longer owns"
    )


async def test_unregistered_release_still_closes_its_own_attempt(
    sessionmaker_: Sessions,
) -> None:
    """Fencing the close must not stop it closing the row it legitimately owns.

    ``ux_job_executions_open_one`` permits one open execution per job, so a release
    that leaves its own row open makes the NEXT claim's INSERT raise 23505 -- forever.
    This is the guarantee the fence in the test above must not cost.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1, handler=MISSING_HANDLER)

    runner = WorkerRunner(sessionmaker_, scope.org_id, name="releaser", concurrency=1)
    worker_id = await runner.register()
    cj = await _claimed(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, cj.lease_epoch)
        await s.commit()

    await runner._release_unregistered(cj, MISSING_HANDLER)

    (row,) = await _executions(sessionmaker_, job_id)
    assert row["finished_at"] is not None, "the release left its own execution row open"
    assert row["status"] == "lost"
    assert row["error_class"] == "UnregisteredHandler"

    # The next claim can therefore open a fresh row.
    await _promote(sessionmaker_, job_id)
    successor = await register_worker(sessionmaker_, scope.org_id, "successor")
    async with sessionmaker_() as s:
        again = await claim_jobs(s, scope.queue_id, successor, 10)
        await s.commit()
    assert [c.job_id for c in again] == [job_id]


# --- FIX 3: unregistered_count has to mean something ------------------------


async def test_unregistered_release_parks_the_job_instead_of_requeueing_it(
    sessionmaker_: Sessions,
) -> None:
    """The release must not hand the job straight back to the claim poll.

    ``queued`` means "due now" -- the claim query has no ``run_at`` predicate, which
    is what keeps ``ix_jobs_claim`` small. A job released to ``queued`` because no
    worker knows its handler is re-claimed within one poll interval, by this worker or
    another, and the fleet spins on it at claim-poll rate. ``scheduled`` with a future
    ``run_at`` is the same answer ``fail_job.sql`` gives a backoff retry, for the same
    reason.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1, handler=MISSING_HANDLER)

    runner = WorkerRunner(sessionmaker_, scope.org_id, name="rolling-deploy", concurrency=1)
    worker_id = await runner.register()
    cj = await _claimed(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, cj.lease_epoch)
        await s.commit()

    await runner._release_unregistered(cj, MISSING_HANDLER)

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.SCHEDULED, "released straight back into the claim set"
    assert state["worker_id"] is None
    assert state["lease_expires_at"] is None
    assert state["lease_epoch"] == cj.lease_epoch + 1, "the fence did not advance"
    assert state["finished_at"] is None, "'scheduled' is not terminal"
    run_at = state["run_at"]
    assert isinstance(run_at, datetime)
    assert run_at > datetime.now(UTC), "the release carries no backoff"
    assert await _unregistered_count(sessionmaker_, job_id) == 1

    # The decisive part: the very next poll finds nothing to claim.
    other = await register_worker(sessionmaker_, scope.org_id, "next-poll")
    async with sessionmaker_() as s:
        immediately = await claim_jobs(s, scope.queue_id, other, 10)
        await s.commit()
    assert immediately == [], "the job was claimable again on the next poll"


async def test_unregistered_count_dead_letters_the_job(sessionmaker_: Sessions) -> None:
    """The counter is READ: past the threshold the job is dead-lettered, not requeued.

    A counter that is written and never read bounds nothing. Under the old behaviour
    this loop had no terminating condition at all -- a job whose handler no deployed
    worker knows would be claimed, released and re-claimed forever, by every worker in
    the fleet, and the only evidence would be a column nobody queried.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5)
    (job_id,) = await seed_jobs(
        sessionmaker_, scope, 1, handler=MISSING_HANDLER, max_attempts=50
    )
    runner = WorkerRunner(sessionmaker_, scope.org_id, name="fleet", concurrency=1)
    worker_id = await runner.register()

    for release in range(1, UNREGISTERED_MAX_RELEASES + 1):
        if release > 1:
            await _promote(sessionmaker_, job_id)
        cj = await _claimed(sessionmaker_, scope, worker_id, job_id)
        async with sessionmaker_() as s:
            assert await start_job(s, job_id, worker_id, cj.lease_epoch)
            await s.commit()
        await runner._release_unregistered(cj, MISSING_HANDLER)

        state = await job_state(sessionmaker_, job_id)
        assert await _unregistered_count(sessionmaker_, job_id) == release
        if release < UNREGISTERED_MAX_RELEASES:
            assert state["status"] == JobStatus.SCHEDULED
        else:
            assert state["status"] == JobStatus.DEAD_LETTER, (
                f"still going round after {release} releases"
            )

    state = await job_state(sessionmaker_, job_id)
    assert state["finished_at"] is not None, (
        "ck_jobs_terminal_finished makes terminal and finished_at equivalent in both"
        " directions -- a dead-letter without it aborts the whole transaction"
    )
    assert state["worker_id"] is None

    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM dead_letter_entries WHERE job_id = CAST(:j AS uuid)"
            " AND error_class = 'UnregisteredHandler'",
            {"j": str(job_id)},
        )
        == 1
    ), "the job died with no operator-facing evidence of why"

    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE job_id = CAST(:j AS uuid)"
            " AND finished_at IS NULL",
            {"j": str(job_id)},
        )
        == 0
    )

    # And it is genuinely out of the claim set, not merely flagged.
    await _promote(sessionmaker_, job_id)
    other = await register_worker(sessionmaker_, scope.org_id, "after-dlq")
    async with sessionmaker_() as s:
        nothing = await claim_jobs(s, scope.queue_id, other, 10)
        await s.commit()
    assert nothing == []


# --- FIX 2: the beat follows the lease we hold, not the queues we poll ------


async def test_beat_follows_the_held_lease_not_the_claimable_queues(
    sessionmaker_: Sessions,
) -> None:
    """Pausing the short-lease queue must not slow the beat below its own lease.

    Queue A's visibility timeout is 10s and queue B's is 300s. The worker is holding a
    job from A when an operator pauses A. ``_queues()`` then returns only B, and an
    interval derived from *claimable* queues becomes 50s -- five times the lease of the
    job in flight. The lease expires, the reaper reclaims a job that is actively
    running, and a second worker executes it. ``lease_epoch`` prevents the double
    commit; nothing prevents the double execution.

    ``self._lease_seconds = 300`` below is exactly the state ``_queues()`` leaves
    behind once A is paused; the assertion is that the beat ignores it in favour of
    the lease ``heartbeat.sql`` just renewed.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=5, lease_seconds=10)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)

    runner = WorkerRunner(sessionmaker_, scope.org_id, name="beat", concurrency=1)
    worker_id = await runner.register()
    await claim_one(sessionmaker_, scope, worker_id, job_id)

    runner._lease_seconds = 300
    await runner._heartbeat_once()

    assert runner.heartbeat_interval <= 10 / 6 + 0.5, (
        "the beat was sized from the queues this worker may claim from rather than"
        f" the 10s lease it is holding (interval={runner.heartbeat_interval}s)"
    )

    # An idle worker has no lease to protect and falls back to the queue-side bound.
    async with sessionmaker_() as s:
        await s.execute(
            text(
                "UPDATE jobs SET status='completed', finished_at=now(), worker_id=NULL,"
                " claimed_at=NULL, lease_expires_at=NULL WHERE id = CAST(:j AS uuid)"
            ),
            {"j": str(job_id)},
        )
        await s.execute(
            text(
                "UPDATE job_executions SET status='lost', finished_at=now()"
                " WHERE job_id = CAST(:j AS uuid) AND finished_at IS NULL"
            ),
            {"j": str(job_id)},
        )
        await s.commit()
    await runner._heartbeat_once()
    assert runner._held_lease_seconds is None
    assert runner.heartbeat_interval == pytest.approx(300 / 6)


async def test_pausing_a_queue_cannot_widen_the_beat_mid_flight(
    sessionmaker_: Sessions,
) -> None:
    """The queue-side bound may only shrink while this worker holds work.

    It is the bound that covers the gap between claiming a job and the first beat that
    sees it, so it has to stay valid for jobs already in flight. A queue vanishing
    from the claimable set (paused, or its timeout edited) says nothing about the
    lease of a job already running on it.
    """
    scope = await seed_scope(
        sessionmaker_, max_concurrency=5, lease_seconds=10, queue_name="fast"
    )
    async with sessionmaker_() as s:
        s.add(
            Queue(
                id=uuid7(),
                organization_id=scope.org_id,
                project_id=scope.project_id,
                name="slow",
                max_concurrency=5,
                visibility_timeout_sec=300,
                default_timeout_ms=60_000,
            )
        )
        await s.commit()

    runner = WorkerRunner(sessionmaker_, scope.org_id, name="ratchet", concurrency=1)
    await runner.register()

    await runner._queues()
    assert runner._lease_seconds == 10, "the shortest claimable lease sets the bound"

    # A job from the fast queue is in flight when the operator pauses that queue.
    holder = asyncio.create_task(asyncio.sleep(60))
    runner._inflight[uuid7()] = holder
    try:
        async with sessionmaker_() as s:
            await s.execute(
                text(
                    "UPDATE queues SET is_paused = true, paused_at = now()"
                    " WHERE id = CAST(:q AS uuid)"
                ),
                {"q": str(scope.queue_id)},
            )
            await s.commit()

        await runner._queues()
        assert runner._lease_seconds == 10, "pausing a queue widened the beat on a live lease"
        assert runner.heartbeat_interval == pytest.approx(10 / 6)
    finally:
        holder.cancel()
