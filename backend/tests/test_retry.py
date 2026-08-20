"""Retry, backoff, and the dead letter queue.

``fail_job.sql`` makes every decision that matters here -- retry or dead-letter,
how long to back off, which execution row to close -- inside one statement, with
the delay computed by Postgres so no worker's clock is ever consulted. These tests
drive that statement through ``app.services.reliability.fail_job``, the same entry
point the worker uses.

The backoff assertions are exact rather than "within some bound". ``random()`` is
seeded with ``setseed`` on the same connection immediately before the statement
runs, and ``now()`` is transaction_timestamp, so both halves of ``now() + capped *
random()`` are known and the expected ``run_at`` is a single number. A bounds check
(``0 <= delay <= cap``) would pass just as happily against a formula that had the
strategies swapped.
"""

from uuid import UUID

import pytest
from sqlalchemy import text

from app.domain.backoff import RetryPolicy, base_delay_ms
from app.domain.enums import BackoffStrategy, JobStatus
from app.services.claim import claim_jobs, start_job
from app.services.reliability import fail_job, promote_due
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

# Any value works; a fixed one keeps a failure reproducible.
SEED = 0.4242


async def take_attempt(sessions: Sessions, scope: Scope, worker_id: UUID, job_id: UUID) -> int:
    """``queued -> claimed -> running``. Returns the lease_epoch of this attempt."""
    async with sessions() as s:
        claimed = await claim_jobs(s, scope.queue_id, worker_id, 50)
        await s.commit()
    match = [c for c in claimed if c.job_id == job_id]
    assert match, f"{job_id} was not claimable"
    epoch = match[0].lease_epoch
    async with sessions() as s:
        assert await start_job(s, job_id, worker_id, epoch)
        await s.commit()
    return epoch


async def make_due_and_promote(sessions: Sessions, job_id: UUID) -> None:
    """Drag a backoff retry's ``run_at`` into the past and run the promoter.

    Nothing else makes a ``scheduled`` job claimable: the claim query has no
    ``run_at`` predicate, so ``queued`` means "due now" and only the promoter
    establishes that.
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
        promoted = await promote_due(s)
        await s.commit()
    assert any(p.job_id == job_id for p in promoted), "the promoter did not pick the retry up"


async def fail_with_known_jitter(
    sessions: Sessions, job_id: UUID, worker_id: UUID, epoch: int
) -> tuple[float, float]:
    """Fail an attempt with ``random()`` pinned, and return (delay_ms, jitter).

    Everything happens in one transaction on one connection, which is what makes
    the arithmetic exact: ``now()`` is the transaction timestamp, so the ``now()``
    read here and the ``now()`` inside ``fail_job.sql`` are the same instant, and
    ``setseed`` makes the statement's single ``random()`` call reproduce the value
    sampled just before it.
    """
    async with sessions() as s:
        started_at = await s.scalar(text("SELECT now()"))
        await s.execute(text("SELECT setseed(:seed)"), {"seed": SEED})
        jitter = float(await s.scalar(text("SELECT random()")) or 0.0)
        await s.execute(text("SELECT setseed(:seed)"), {"seed": SEED})
        outcome = await fail_job(s, job_id, worker_id, epoch, "Boom", "dependency is down")
        await s.commit()
    assert outcome is not None, "the fence rejected a failure it should have accepted"
    assert started_at is not None
    delay_ms = (outcome.next_run_at - started_at).total_seconds() * 1000
    return delay_ms, jitter


@pytest.mark.parametrize(
    "strategy",
    [BackoffStrategy.FIXED, BackoffStrategy.LINEAR, BackoffStrategy.EXPONENTIAL],
)
@pytest.mark.timeout(120)
async def test_backoff_sequence_per_strategy(
    sessionmaker_: Sessions, strategy: BackoffStrategy
) -> None:
    """Five consecutive attempts land on the strategy's curve, capped, then jittered.

    ``backoff_max_ms`` is set low enough that the cap binds partway through each
    sequence, so this also asserts the ``LEAST(...)`` rather than only the growth.

    The expected value comes from ``app.domain.backoff``, an independent Python
    implementation of the same formulas -- so a drift between the SQL and the
    documented curve is a test failure rather than a silent behaviour change.
    """
    base_ms, max_ms, attempts = 1_000, 2_500, 5
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(
        sessionmaker_,
        scope,
        1,
        max_attempts=attempts + 1,
        strategy=strategy,
        base_ms=base_ms,
        max_ms=max_ms,
    )
    worker_id = await register_worker(sessionmaker_, scope.org_id, f"retrier-{strategy}")
    policy = RetryPolicy(strategy=strategy, base_ms=base_ms, max_ms=max_ms)

    observed: list[float] = []
    for attempt in range(1, attempts + 1):
        if attempt > 1:
            await make_due_and_promote(sessionmaker_, job_id)
        epoch = await take_attempt(sessionmaker_, scope, worker_id, job_id)
        assert (await job_state(sessionmaker_, job_id))["attempt"] == attempt

        delay_ms, jitter = await fail_with_known_jitter(
            sessionmaker_, job_id, worker_id, epoch
        )
        expected = base_delay_ms(policy, attempt) * jitter
        assert abs(delay_ms - expected) < 1.0, (
            f"{strategy} attempt {attempt}: run_at is {delay_ms:.3f}ms out,"
            f" expected {expected:.3f}ms"
        )
        observed.append(delay_ms)
        assert (await job_state(sessionmaker_, job_id))["status"] == JobStatus.SCHEDULED

    # The cap is the reason the last two entries stop growing; full jitter is why
    # every entry is a fraction of it rather than the value itself.
    assert all(0 <= d <= max_ms for d in observed), observed
    caps = [base_delay_ms(policy, n) for n in range(1, attempts + 1)]
    if strategy is BackoffStrategy.FIXED:
        assert caps == [base_ms] * attempts, "fixed backoff is not flat"
    else:
        assert caps[0] < caps[-1], f"{strategy} does not grow"
        assert caps[-1] == max_ms, f"{strategy} never reached the cap in {attempts} attempts"


async def test_retry_goes_to_scheduled_not_queued(sessionmaker_: Sessions) -> None:
    """A retry with budget left parks in ``scheduled`` and is not claimable.

    This is the whole reason backoff exists at all. The claim query deliberately has
    no ``run_at`` predicate -- that is what keeps ``ix_jobs_claim`` down to the ready
    set -- so a retry parked in ``queued`` with a future ``run_at`` would be claimed
    by the very next poll, burning every attempt in milliseconds and hammering the
    dependency that is already down.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1, max_attempts=3)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "backoff")

    epoch = await take_attempt(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        outcome = await fail_job(s, job_id, worker_id, epoch, "Boom", "down")
        await s.commit()
    assert outcome is not None and outcome.will_retry

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.SCHEDULED
    assert state["finished_at"] is None, "a retry is not terminal"
    assert state["worker_id"] is None
    assert state["lease_epoch"] == epoch + 1, "the fence did not advance on release"

    # Not claimable: the claim predicate is status='queued', full stop.
    async with sessionmaker_() as s:
        assert await claim_jobs(s, scope.queue_id, worker_id, 10) == []
        await s.commit()

    # And not promotable either, until run_at arrives.
    async with sessionmaker_() as s:
        await s.execute(
            text("UPDATE jobs SET run_at = now() + interval '1 hour' WHERE id = CAST(:j AS uuid)"),
            {"j": str(job_id)},
        )
        await s.commit()
    async with sessionmaker_() as s:
        assert await promote_due(s) == []
        await s.commit()

    # Once it is due, the promoter -- and only the promoter -- makes it claimable.
    await make_due_and_promote(sessionmaker_, job_id)
    assert (await job_state(sessionmaker_, job_id))["status"] == JobStatus.QUEUED
    async with sessionmaker_() as s:
        assert len(await claim_jobs(s, scope.queue_id, worker_id, 10)) == 1
        await s.commit()


@pytest.mark.timeout(120)
async def test_exhausted_job_dead_letters_once(sessionmaker_: Sessions) -> None:
    """``max_attempts=2`` and a handler that always fails: one DLQ row, finished_at set.

    ``finished_at`` is the load-bearing detail. ``ck_jobs_terminal_finished`` makes
    terminal status and ``finished_at`` equivalent in both directions, so omitting it
    on the dead-letter branch would not merely lose a timestamp -- it would abort the
    whole transaction, and the first exhausted job would poison every subsequent
    ``fail_job`` call.
    """
    scope = await seed_scope(sessionmaker_)
    (job_id,) = await seed_jobs(sessionmaker_, scope, 1, max_attempts=2, base_ms=1, max_ms=1)
    worker_id = await register_worker(sessionmaker_, scope.org_id, "poison")

    epoch = await take_attempt(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        first = await fail_job(s, job_id, worker_id, epoch, "Boom", "attempt 1")
        await s.commit()
    assert first is not None and first.will_retry

    await make_due_and_promote(sessionmaker_, job_id)
    epoch = await take_attempt(sessionmaker_, scope, worker_id, job_id)
    async with sessionmaker_() as s:
        last = await fail_job(s, job_id, worker_id, epoch, "Boom", "attempt 2")
        await s.commit()
    assert last is not None
    assert last.dead_lettered and not last.will_retry
    assert last.dlq_entries == 1
    assert last.executions_closed == 1

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == JobStatus.DEAD_LETTER
    assert state["finished_at"] is not None, "a terminal job without finished_at"
    assert state["attempt"] == 2

    dlq_count = "SELECT count(*) FROM dead_letter_entries WHERE job_id = CAST(:j AS uuid)"
    assert await count_rows(sessionmaker_, dlq_count, {"j": str(job_id)}) == 1

    # Exactly two attempts of history, both closed, neither still open.
    async with sessionmaker_() as s:
        rows = (
            await s.execute(
                text(
                    "SELECT status::text AS status, attempt_number FROM job_executions"
                    " WHERE job_id = CAST(:j AS uuid) ORDER BY id"
                ),
                {"j": str(job_id)},
            )
        ).all()
    assert [(r.status, r.attempt_number) for r in rows] == [("failed", 1), ("failed", 2)]

    # "Once" means once. A duplicate failure from the same attempt -- a retried
    # network call on the worker's side -- is fenced out and writes no second entry.
    async with sessionmaker_() as s:
        replayed = await fail_job(s, job_id, worker_id, epoch, "Boom", "attempt 2 again")
        await s.commit()
    assert replayed is None, "a terminal job accepted a second failure"
    assert await count_rows(sessionmaker_, dlq_count, {"j": str(job_id)}) == 1
