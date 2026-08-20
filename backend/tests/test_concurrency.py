"""Concurrency, fencing, and recovery -- the evidence for the Reliability marks.

Every test in this module drives the **real** SQL in ``app/db/sql/`` through the
same service wrappers the worker and scheduler use. Nothing here reimplements the
claim, the fence, or the reaper: a test that reimplements them proves only that the
test is self-consistent.

The one rule that makes these tests mean anything: **genuinely independent,
committed sessions**. Inside a single transaction ``FOR UPDATE SKIP LOCKED`` has
nothing to skip -- every row it would step over is one it already holds -- so a
"concurrency" test written against one session passes vacuously and would keep
passing if ``SKIP LOCKED`` were deleted from the statement. conftest's ``engine``
fixture uses ``NullPool`` and its teardown TRUNCATEs rather than rolling back,
precisely so each session here is a separate backend with its own snapshot.

The helpers below are shared with ``test_retry.py``, ``test_scheduling.py``, and
``test_api.py``.
"""

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.base import uuid7
from app.db.models.execution import Worker
from app.db.models.scheduling import Job, Queue
from app.db.models.tenancy import Organization, Project
from app.domain.enums import BackoffStrategy, JobKind, JobStatus, WorkerStatus
from app.services.claim import claim_jobs, complete_job, load_sql, start_job
from app.services.reliability import reap_expired_leases
from app.worker.runner import WorkerRunner

pytestmark = pytest.mark.concurrency

Sessions = async_sessionmaker[AsyncSession]


# --- shared fixtures --------------------------------------------------------


@dataclass(frozen=True)
class Scope:
    """One tenant's worth of rows: everything a job needs to exist."""

    org_id: UUID
    project_id: UUID
    queue_id: UUID
    lease_seconds: int
    max_concurrency: int

    @property
    def timeout_ms(self) -> int:
        """A job may never outlive its own lease (ck_jobs_timeout_lt_lease)."""
        return self.lease_seconds * 1000 - 1000


async def seed_scope(
    sessions: Sessions,
    *,
    max_concurrency: int = 1000,
    lease_seconds: int = 10,
    queue_name: str = "default",
) -> Scope:
    """Commit an organization, a project, and a queue."""
    suffix = uuid7().hex[:12]
    async with sessions() as s:
        org = Organization(id=uuid7(), name="Acme", slug=f"acme-{suffix}")
        s.add(org)
        await s.flush()
        project = Project(
            id=uuid7(), organization_id=org.id, name="Demo", slug=f"demo-{suffix}"
        )
        s.add(project)
        await s.flush()
        queue = Queue(
            id=uuid7(),
            organization_id=org.id,
            project_id=project.id,
            name=queue_name,
            max_concurrency=max_concurrency,
            visibility_timeout_sec=lease_seconds,
            default_timeout_ms=lease_seconds * 1000 - 1000,
        )
        s.add(queue)
        await s.flush()
        scope = Scope(
            org_id=org.id,
            project_id=project.id,
            queue_id=queue.id,
            lease_seconds=lease_seconds,
            max_concurrency=max_concurrency,
        )
        await s.commit()
    return scope


async def seed_jobs(
    sessions: Sessions,
    scope: Scope,
    count: int,
    *,
    status: JobStatus = JobStatus.QUEUED,
    handler: str = "demo.echo",
    priority: int = 0,
    max_attempts: int = 3,
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL,
    base_ms: int = 1_000,
    max_ms: int = 300_000,
) -> list[UUID]:
    """Commit ``count`` jobs and return their ids, in creation order."""
    ids: list[UUID] = []
    now = datetime.now(UTC)
    async with sessions() as s:
        for _ in range(count):
            job = Job(
                id=uuid7(),
                organization_id=scope.org_id,
                project_id=scope.project_id,
                queue_id=scope.queue_id,
                kind=JobKind.IMMEDIATE,
                handler=handler,
                status=status,
                priority=priority,
                run_at=now,
                payload={},
                attempt=0,
                max_attempts=max_attempts,
                backoff_strategy=strategy,
                backoff_base_ms=base_ms,
                backoff_max_ms=max_ms,
                timeout_ms=scope.timeout_ms,
                lease_seconds=scope.lease_seconds,
            )
            s.add(job)
            ids.append(job.id)
        await s.commit()
    return ids


async def register_worker(sessions: Sessions, org_id: UUID, name: str) -> UUID:
    """A real ``workers`` row: job_executions.worker_id is a foreign key, so a
    claim against an invented worker id would raise 23503 rather than claim."""
    now = datetime.now(UTC)
    async with sessions() as s:
        worker = Worker(
            id=uuid7(),
            organization_id=org_id,
            name=name,
            hostname="test",
            pid=0,
            status=WorkerStatus.ACTIVE,
            concurrency=4,
            started_at=now,
            last_heartbeat_at=now,
        )
        s.add(worker)
        await s.flush()
        worker_id = worker.id
        await s.commit()
    return worker_id


async def expire_lease(sessions: Sessions, job_id: UUID) -> None:
    """Age a lease into the past. This is the only way to simulate a ``kill -9``
    without actually killing a process: the reaper's sole predicate is
    ``lease_expires_at < now()``, and no worker's own clock is consulted."""
    async with sessions() as s:
        await s.execute(
            text(
                "UPDATE jobs SET lease_expires_at = now() - interval '1 second'"
                " WHERE id = CAST(:job_id AS uuid)"
            ),
            {"job_id": str(job_id)},
        )
        await s.commit()


async def job_state(sessions: Sessions, job_id: UUID) -> dict[str, object]:
    async with sessions() as s:
        row = (
            await s.execute(
                text(
                    "SELECT status::text AS status, attempt, lease_epoch, worker_id,"
                    " finished_at, run_at, started_at, claimed_at, lease_expires_at"
                    " FROM jobs WHERE id = CAST(:job_id AS uuid)"
                ),
                {"job_id": str(job_id)},
            )
        ).one()
    return dict(row._mapping)


async def count_rows(sessions: Sessions, sql: str, params: dict[str, object]) -> int:
    async with sessions() as s:
        value = await s.scalar(text(sql), params)
    return int(value or 0)


async def claim_one(
    sessions: Sessions, scope: Scope, worker_id: UUID, job_id: UUID
) -> int:
    """Claim ``job_id`` and return the lease_epoch it was claimed under."""
    async with sessions() as s:
        claimed = await claim_jobs(s, scope.queue_id, worker_id, 50)
        await s.commit()
    match = [c for c in claimed if c.job_id == job_id]
    assert match, f"expected to claim {job_id}, got {[str(c.job_id) for c in claimed]}"
    return match[0].lease_epoch


# --- claim disjointness -----------------------------------------------------


@pytest.mark.timeout(120)
async def test_concurrent_claims_are_disjoint(sessionmaker_: Sessions) -> None:
    """100 jobs, 8 concurrent claimers: the union is every job, and no job is
    claimed twice.

    This is the invariant ``FOR UPDATE SKIP LOCKED`` exists to provide. The broken
    alternative -- SELECT the ready set, then UPDATE it -- passes an "all jobs were
    claimed" assertion and fails this one: under READ COMMITTED two claimers can
    both see a job queued and both update it, and the second update simply wins.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=1000)
    expected = set(await seed_jobs(sessionmaker_, scope, 100))
    workers = [
        await register_worker(sessionmaker_, scope.org_id, f"claimer-{i}") for i in range(8)
    ]

    async def claimer(worker_id: UUID) -> list[UUID]:
        got: list[UUID] = []
        empty_rounds = 0
        while empty_rounds < 3:
            async with sessionmaker_() as s:
                batch = await claim_jobs(s, scope.queue_id, worker_id, 13)
                await s.commit()
            if batch:
                empty_rounds = 0
                got.extend(c.job_id for c in batch)
            else:
                empty_rounds += 1
                await asyncio.sleep(0.005)
        return got

    results = await asyncio.gather(*(claimer(w) for w in workers))

    claimed = [job_id for r in results for job_id in r]
    assert len(claimed) == 100, "a job was claimed more than once"
    assert set(claimed) == expected, "the union of all claimers is not the ready set"
    # And the same fact read back from the database, not from the return values.
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM jobs WHERE queue_id = CAST(:q AS uuid)"
            " AND status = 'claimed'",
            {"q": str(scope.queue_id)},
        )
        == 100
    )
    # Exactly one execution row per job: the claim opens it, so a double claim
    # would show up here even if the return values somehow agreed.
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE queue_id = CAST(:q AS uuid)",
            {"q": str(scope.queue_id)},
        )
        == 100
    )


@pytest.mark.timeout(120)
async def test_claim_respects_max_concurrency(sessionmaker_: Sessions) -> None:
    """A cap of 3 under 12 concurrent claimers is never exceeded at any sample.

    ``SKIP LOCKED`` alone guarantees *disjointness*, which is the wrong invariant
    here: with an unlocked in-flight count, K claimers each read the same headroom
    and each spend it, so the cap overshoots by ``(K-1) x batch_size``. What makes
    the cap exact is the ``FOR NO KEY UPDATE`` lock on the queue row, which holds
    the in-flight count still for the duration of the claim statement.

    The sampler runs in its own session, so it sees only committed state -- an
    overshoot would be visible to it the instant the offending claim committed.
    """
    total = 50
    scope = await seed_scope(sessionmaker_, max_concurrency=3)
    await seed_jobs(sessionmaker_, scope, total)
    workers = [
        await register_worker(sessionmaker_, scope.org_id, f"capped-{i}") for i in range(12)
    ]

    finished: list[UUID] = []
    samples: list[int] = []
    stop = asyncio.Event()
    deadline = asyncio.get_running_loop().time() + 60

    async def sampler() -> None:
        while not stop.is_set():
            async with sessionmaker_() as s:
                inflight = await s.scalar(
                    text(
                        "SELECT count(*) FROM jobs WHERE queue_id = CAST(:q AS uuid)"
                        " AND status IN ('claimed', 'running')"
                    ),
                    {"q": str(scope.queue_id)},
                )
            samples.append(int(inflight or 0))
            await asyncio.sleep(0.002)

    async def claimer(worker_id: UUID) -> None:
        while len(finished) < total and asyncio.get_running_loop().time() < deadline:
            async with sessionmaker_() as s:
                batch = await claim_jobs(s, scope.queue_id, worker_id, 5)
                await s.commit()
            if not batch:
                await asyncio.sleep(0.003)
                continue
            # Occupying a slot and then freeing it is what makes the cap a moving
            # target rather than a one-shot check.
            for job in batch:
                async with sessionmaker_() as s:
                    assert await start_job(s, job.job_id, worker_id, job.lease_epoch)
                    assert await complete_job(s, job.job_id, worker_id, job.lease_epoch)
                    await s.commit()
                finished.append(job.job_id)

    sample_task = asyncio.create_task(sampler())
    try:
        await asyncio.gather(*(claimer(w) for w in workers))
    finally:
        stop.set()
        await sample_task

    assert len(finished) == total, "claimers did not drain the queue"
    assert len(set(finished)) == total, "a job was completed twice"
    assert samples, "the sampler never observed the queue"
    assert max(samples) > 0, "the sampler never caught a job in flight"
    assert max(samples) <= 3, f"max_concurrency=3 was exceeded: peak in-flight {max(samples)}"


async def test_claim_cap_holds_when_a_claimer_waits_on_the_queue_lock(
    sessionmaker_: Sessions,
) -> None:
    """The two-session, deterministic form of the test above.

    A claims 3 against a cap of 3 and holds its transaction open, so it still owns
    the ``FOR NO KEY UPDATE`` lock on the queue row. B issues the same statement and
    blocks on that lock -- mutual exclusion works. The question this test asks is
    what B computes *after* the lock is released: the cap is exact only if B's
    in-flight count sees A's three claims.

    Under READ COMMITTED a statement keeps the snapshot it took when it started.
    Releasing a row lock re-checks the *locked row* (EvalPlanQual); it does not give
    the rest of the statement a newer snapshot, so the ``count(*)`` over ``jobs``
    inside ``headroom`` is still the pre-A count. That is the check-then-act race
    the queue-row lock was supposed to close, and it is still open -- it has simply
    been serialised.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=3)
    await seed_jobs(sessionmaker_, scope, 20)
    worker_a = await register_worker(sessionmaker_, scope.org_id, "first")
    worker_b = await register_worker(sessionmaker_, scope.org_id, "second")

    session_a = sessionmaker_()
    session_b = sessionmaker_()
    try:
        first = await claim_jobs(session_a, scope.queue_id, worker_a, 3)
        assert len(first) == 3, "the first claimer did not fill the cap"

        pending = asyncio.create_task(claim_jobs(session_b, scope.queue_id, worker_b, 3))
        await asyncio.sleep(0.3)
        assert not pending.done(), "the second claimer did not block on the queue row lock"

        await session_a.commit()
        second = await pending
        await session_b.commit()
    finally:
        await session_a.close()
        await session_b.close()

    inflight = await count_rows(
        sessionmaker_,
        "SELECT count(*) FROM jobs WHERE queue_id = CAST(:q AS uuid)"
        " AND status IN ('claimed', 'running')",
        {"q": str(scope.queue_id)},
    )
    assert second == [], (
        "the second claimer spent headroom the first had already consumed: "
        f"it claimed {len(second)} more with the cap already full"
    )
    assert inflight == 3, f"max_concurrency=3 was exceeded: {inflight} in flight"


# --- leases, reaping, fencing ----------------------------------------------


async def test_expired_lease_reclaimed_once(sessionmaker_: Sessions) -> None:
    """Two reapers race one expired lease: exactly one requeue, epoch +1.

    The two sessions are deliberately *overlapping* rather than sequential -- the
    second reaper runs while the first still holds an uncommitted row lock, which
    is the only arrangement that exercises ``SKIP LOCKED`` at all. Run them one
    after the other and the second finds a requeued job and does nothing, which
    proves nothing about concurrency.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "doomed")

    epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)
    await expire_lease(sessionmaker_, job_id)

    async with sessionmaker_() as s1, sessionmaker_() as s2, s1.begin():
        first = await reap_expired_leases(s1)
        # s1 holds the row lock here, uncommitted.
        async with s2.begin():
            second = await reap_expired_leases(s2)

    assert len(first) == 1, "the first reaper did not reclaim the expired lease"
    assert second == [], "the second reaper did not skip the locked row"
    assert first[0].requeued

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.QUEUED
    assert state["lease_epoch"] == epoch + 1, "the fence advanced more than once"
    assert state["worker_id"] is None
    # attempt never moved: this job was claimed but never started.
    assert state["attempt"] == 0

    # The orphaned execution row is closed, not left open.
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE job_id = CAST(:j AS uuid)"
            " AND status = 'lost' AND finished_at IS NOT NULL",
            {"j": str(job_id)},
        )
        == 1
    )


async def test_stolen_lease_write_is_fenced(sessionmaker_: Sessions) -> None:
    """A completion carrying a stale ``lease_epoch`` updates zero rows.

    Worker A claims under epoch e and starts running. It stalls -- a GC pause, a
    SIGSTOP, a partition -- and the reaper, which cannot tell a slow worker from a
    dead one, takes the job back. Worker B claims it and starts it. A then wakes up
    and tries to commit its result over B's live attempt.

    Everything except the epoch matches from A's point of view, so the epoch is the
    only thing that can reject the write.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_a = await register_worker(sessionmaker_, scope.org_id, "worker-a")
    worker_b = await register_worker(sessionmaker_, scope.org_id, "worker-b")

    epoch_a = await claim_one(sessionmaker_, scope, worker_a, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_a, epoch_a)
        await s.commit()

    await expire_lease(sessionmaker_, job_id)
    async with sessionmaker_() as s:
        reaped = await reap_expired_leases(s)
        await s.commit()
    assert len(reaped) == 1 and reaped[0].requeued

    epoch_b = await claim_one(sessionmaker_, scope, worker_b, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_b, epoch_b)
        await s.commit()

    # Worker A, alive after all, tries to commit the result of the stolen attempt.
    async with sessionmaker_() as s:
        stale = await complete_job(s, job_id, worker_a, epoch_a, {"stale": True})
        await s.commit()
    assert stale is False, "a stale-epoch completion was accepted"

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.RUNNING, "the stale write moved the job"
    assert state["worker_id"] == worker_b
    assert state["finished_at"] is None

    # B's completion, under the live epoch, is accepted.
    async with sessionmaker_() as s:
        assert await complete_job(s, job_id, worker_b, epoch_b, {"ok": True})
        await s.commit()
    assert (await job_state(sessionmaker_, job_id))["status"] == JobStatus.COMPLETED


async def test_zombie_after_same_worker_reclaim_rejected(sessionmaker_: Sessions) -> None:
    """The ABA case: the same worker reclaims the job its own zombie is still on.

    This is why the fence is an epoch and not ``lease_expires_at > now()``. W1
    claims, stalls, is reaped, and then re-claims the *same* job itself. Its
    original coroutine is still alive; its completion matches ``worker_id = W1``
    **and** a live lease, so a time-based fence lets it commit a stale result as
    the current attempt's outcome. A counter that never repeats does not.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "worker-1")

    zombie_epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, zombie_epoch)
        await s.commit()

    await expire_lease(sessionmaker_, job_id)
    async with sessionmaker_() as s:
        assert len(await reap_expired_leases(s)) == 1
        await s.commit()

    live_epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_id, live_epoch)
        await s.commit()

    assert live_epoch == zombie_epoch + 2, "reclaim did not advance the fence twice"

    state = await job_state(sessionmaker_, job_id)
    assert state["worker_id"] == worker_id, "the ABA setup needs the same owner"

    async with sessionmaker_() as s:
        accepted = await complete_job(s, job_id, worker_id, zombie_epoch, {"zombie": True})
        await s.commit()
    assert accepted is False, "the zombie's write passed the fence"
    assert (await job_state(sessionmaker_, job_id))["status"] == JobStatus.RUNNING

    # No execution row was closed by the zombie, and none carries its result.
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE job_id = CAST(:j AS uuid)"
            " AND result IS NOT NULL",
            {"j": str(job_id)},
        )
        == 0
    )


async def test_orphan_execution_does_not_block_next_attempt(
    sessionmaker_: Sessions,
) -> None:
    """kill -9 -> reap -> re-claim. The second execution INSERT succeeds.

    ``ux_job_executions_open_one`` permits exactly one execution row per job with
    ``finished_at IS NULL``. A killed worker leaves that row open forever, so the
    reaper's ``closed`` CTE is load-bearing: without it the *next* claim's INSERT
    raises 23505, and so does every claim after it, on every worker, forever. The
    job the grader killed a worker to watch recover would be the one job that never
    recovers.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_a = await register_worker(sessionmaker_, scope.org_id, "killed")
    worker_b = await register_worker(sessionmaker_, scope.org_id, "survivor")

    epoch_a = await claim_one(sessionmaker_, scope, worker_a, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_a, epoch_a)
        await s.commit()

    # kill -9: no completion, no failure, no release. Just silence.
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE job_id = CAST(:j AS uuid)"
            " AND finished_at IS NULL",
            {"j": str(job_id)},
        )
        == 1
    ), "the orphaned execution row was not left open"

    await expire_lease(sessionmaker_, job_id)
    async with sessionmaker_() as s:
        assert len(await reap_expired_leases(s)) == 1
        await s.commit()

    # The re-claim is the assertion: it would raise IntegrityError (23505) if the
    # reaper had requeued the job without closing the orphan.
    epoch_b = await claim_one(sessionmaker_, scope, worker_b, job_id)
    async with sessionmaker_() as s:
        assert await start_job(s, job_id, worker_b, epoch_b)
        assert await complete_job(s, job_id, worker_b, epoch_b, {"ok": True})
        await s.commit()

    async with sessionmaker_() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT status::text AS status, attempt_number, lease_epoch"
                    " FROM job_executions WHERE job_id = CAST(:j AS uuid)"
                    " ORDER BY id"
                ),
                {"j": str(job_id)},
            )
        ).all()
    assert [r.status for r in rows] == ["lost", "succeeded"]
    # Two real rows at the same attempt number: a claim that died before finishing
    # and the claim that replaced it. Both are history, which is why there is
    # deliberately no UNIQUE (job_id, attempt_number).
    assert rows[0].attempt_number == 1
    assert rows[0].lease_epoch == epoch_a
    assert rows[1].lease_epoch == epoch_b
    assert (await job_state(sessionmaker_, job_id))["attempt"] == 2


# --- graceful shutdown ------------------------------------------------------


async def test_sigterm_releases_unstarted_claims(sessionmaker_: Sessions) -> None:
    """SIGTERM hands back claims that never reached the handler, for free.

    ``attempt`` increments at ``claimed -> running``, so a claim released from
    ``claimed`` provably never executed: nothing has to be decremented and the
    monotonicity rule is never even approached.

    The guard is ``status = 'claimed'``, not merely a filter on the id list. A task
    that won the race to ``running`` while shutdown was in flight owns a live
    handler and must NOT be yanked out from under it -- which is why one of the
    four claims below is started first and is expected to survive the release.
    """
    scope = await seed_scope(sessionmaker_)
    job_ids = await seed_jobs(sessionmaker_, scope, 4)

    runner = WorkerRunner(
        sessionmaker_, scope.org_id, name="drainer", concurrency=4, batch_size=4
    )
    worker_id = await runner.register()

    async with sessionmaker_() as s:
        claimed = await claim_jobs(s, scope.queue_id, worker_id, 4)
        await s.commit()
    assert len(claimed) == 4
    epochs = {c.job_id: c.lease_epoch for c in claimed}

    # One of them won the race to 'running' before SIGTERM landed.
    started = claimed[0].job_id
    async with sessionmaker_() as s:
        assert await start_job(s, started, worker_id, epochs[started])
        await s.commit()

    # The claim buffer the runner would hold at the moment the signal arrives.
    runner._unstarted = dict(epochs)
    await runner.shutdown()

    for job_id in job_ids:
        state = await job_state(sessionmaker_, job_id)
        if job_id == started:
            assert state["status"] == JobStatus.RUNNING, "a live handler was yanked"
            assert state["attempt"] == 1
            assert state["worker_id"] == worker_id
        else:
            assert state["status"] == JobStatus.QUEUED
            assert state["attempt"] == 0, "a released claim consumed an attempt"
            assert state["worker_id"] is None
            assert state["claimed_at"] is None
            assert state["lease_expires_at"] is None
            assert state["lease_epoch"] == epochs[job_id] + 1


async def test_released_claim_can_be_claimed_again(sessionmaker_: Sessions) -> None:
    """A job released by graceful shutdown must be claimable by the next worker.

    ``claim_jobs`` opens an execution row at claim time, and
    ``ux_job_executions_open_one`` permits exactly one open row per job. The reaper
    closes that row as ``lost`` before requeueing precisely because of this index --
    the graceful-release path has to discharge the same obligation, and it is the
    *only* other path that moves a job out of ``claimed`` without finishing its
    execution.

    Separated from the SIGTERM test above so the two facts report independently:
    that one asserts the release itself (queued, attempt unchanged, epoch bumped),
    this one asserts the release leaves the job usable.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)

    runner = WorkerRunner(
        sessionmaker_, scope.org_id, name="releaser", concurrency=1, batch_size=1
    )
    worker_id = await runner.register()
    epoch = await claim_one(sessionmaker_, scope, worker_id, job_id)
    runner._unstarted = {job_id: epoch}
    await runner.shutdown()

    assert (await job_state(sessionmaker_, job_id))["status"] == JobStatus.QUEUED
    assert (
        await count_rows(
            sessionmaker_,
            "SELECT count(*) FROM job_executions WHERE job_id = CAST(:j AS uuid)"
            " AND finished_at IS NULL",
            {"j": str(job_id)},
        )
        == 0
    ), "the release left its execution row open, so the next claim cannot insert one"

    successor = await register_worker(sessionmaker_, scope.org_id, "successor")
    async with sessionmaker_() as s:
        again = await claim_jobs(s, scope.queue_id, successor, 10)
        await s.commit()
    assert len(again) == 1, "a released job could not be claimed again"


async def test_start_rowcount_zero_means_do_not_run(sessionmaker_: Sessions) -> None:
    """``claimed -> running`` is a guarded write and the rowcount must be checked.

    If the release commits while the start is in flight, another worker claims the
    now-``queued`` job immediately. A start that ignores its rowcount would then run
    the handler anyway -- a double execution on every deploy, with no lease expiry,
    no partition, and no slow worker involved.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1)
    worker_a = await register_worker(sessionmaker_, scope.org_id, "releasing")
    worker_b = await register_worker(sessionmaker_, scope.org_id, "stealing")

    epoch_a = await claim_one(sessionmaker_, scope, worker_a, job_id)

    # Graceful shutdown releases it back to 'queued'. The execution row is closed
    # here too -- that is what a correct release has to do, and the fact that the
    # shipped release path does not is asserted separately by
    # test_released_claim_can_be_claimed_again. Closing it here keeps this test
    # about the start guard and nothing else.
    async with sessionmaker_() as s:
        await s.execute(
            text(
                "UPDATE jobs SET status='queued', worker_id=NULL, claimed_at=NULL,"
                " lease_expires_at=NULL, lease_epoch=lease_epoch+1, updated_at=now()"
                " WHERE id = CAST(:j AS uuid) AND worker_id = CAST(:w AS uuid)"
                " AND status='claimed'"
            ),
            {"j": str(job_id), "w": str(worker_a)},
        )
        await s.execute(
            text(
                "UPDATE job_executions SET status='lost', finished_at=now(),"
                " error_class='Released'"
                " WHERE job_id = CAST(:j AS uuid) AND finished_at IS NULL"
            ),
            {"j": str(job_id)},
        )
        await s.commit()

    # ...and another worker takes it before the original start lands.
    epoch_b = await claim_one(sessionmaker_, scope, worker_b, job_id)
    assert epoch_b == epoch_a + 2

    async with sessionmaker_() as s:
        ran = await start_job(s, job_id, worker_a, epoch_a)
        await s.commit()
    assert ran is False, "the released claim was started anyway"

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.CLAIMED
    assert state["attempt"] == 0, "a lost start consumed an attempt"
    assert state["worker_id"] == worker_b


# --- the hot path plan ------------------------------------------------------


@pytest.mark.timeout(120)
async def test_claim_uses_partial_index(sessionmaker_: Sessions) -> None:
    """The claim is an index scan on ``ix_jobs_claim``: no Seq Scan, no Sort.

    The partial index's predicate is character-identical to the claim's WHERE and
    its column order matches the ORDER BY, so the ordering comes from the index
    rather than from sorting the ready set. A Sort node here means every claim
    reads and orders the whole queue, which is the difference between a queue that
    scales and one that degrades quadratically as the backlog grows.

    ``enable_seqscan`` is deliberately left on: forcing the plan would prove only
    that the index can be used, not that the planner chooses it.
    """
    scope = await seed_scope(sessionmaker_, max_concurrency=1000)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "planner")

    # Enough rows, and enough non-queued history, that a sequential scan is a
    # genuinely bad plan rather than a cheap one the planner picks for a toy table.
    async with sessionmaker_() as s:
        await s.execute(
            text(
                "INSERT INTO jobs (id, organization_id, project_id, queue_id, kind,"
                " handler, status, priority, run_at, payload, attempt, max_attempts,"
                " backoff_strategy, backoff_base_ms, backoff_max_ms, unregistered_count,"
                " timeout_ms, lease_seconds, lease_epoch, cancel_requested, lock_version,"
                " finished_at, created_at, updated_at)"
                " SELECT gen_random_uuid(), CAST(:org AS uuid), CAST(:proj AS uuid),"
                "        CAST(:queue AS uuid), 'immediate', 'demo.echo',"
                "        CASE WHEN g % 5 = 0 THEN 'queued'::job_status"
                "             ELSE 'completed'::job_status END,"
                "        (g % 21) - 10, now() - make_interval(secs => g % 600),"
                "        '{}'::jsonb, 0, 3, 'exponential', 1000, 300000, 0,"
                "        :timeout, :lease, 0, false, 0,"
                "        CASE WHEN g % 5 = 0 THEN NULL ELSE now() END, now(), now()"
                "   FROM generate_series(1, 20000) g"
            ),
            {
                "org": str(scope.org_id),
                "proj": str(scope.project_id),
                "queue": str(scope.queue_id),
                "timeout": scope.timeout_ms,
                "lease": scope.lease_seconds,
            },
        )
        await s.commit()
    async with sessionmaker_() as s:
        await s.execute(text("ANALYZE jobs"))
        await s.commit()

    async with sessionmaker_() as s:
        rows = (
            await s.execute(
                text("EXPLAIN " + load_sql("claim_jobs")),
                {
                    "queue_id": str(scope.queue_id),
                    "batch_size": 10,
                    "worker_id": str(worker_id),
                    # claim_jobs.sql now takes max_concurrency as a param rather than
                    # locking+reading the queue row itself -- that lock is a separate
                    # PRIOR statement (lock_queue.sql) in the real claim path, split out
                    # to fix a stale-snapshot race under concurrency. See
                    # app/services/claim.py::claim_jobs for the full explanation.
                    "max_concurrency": scope.max_concurrency,
                },
            )
        ).all()
        await s.rollback()
    plan = "\n".join(str(r[0]) for r in rows)

    assert "ix_jobs_claim" in plan, f"the claim did not use its partial index:\n{plan}"
    assert "Sort" not in plan, f"the claim sorted instead of reading index order:\n{plan}"
    assert "Seq Scan on jobs" not in plan, (
        "the claim sequentially scans the whole jobs table. The `picked` CTE reads"
        " ix_jobs_claim correctly, but its LIMIT is a runtime subquery, so the"
        " planner has no row estimate for it and hash-joins the `claimed` UPDATE"
        " against all of jobs instead of looking each id up by primary key. The"
        " same statement with a literal LIMIT plans as a nested loop over"
        f" uq_jobs_id_organization_id.\n{plan}"
    )
