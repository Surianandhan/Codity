"""HTTP contract: tenant isolation, idempotency, cancel, the DLQ, keyset paging,
and the one error envelope.

Everything below goes through the real ASGI app -- routers, dependencies, error
handlers and middleware included -- because that is the only layer at which the
claims here are even expressible. A service-level test cannot tell a 404 from a
403, cannot see the error envelope at all, and cannot observe that ``201`` is
replayed rather than re-issued.

Two conventions carried over from the rest of the suite:

* **State claims are asserted against the database, contract claims against the
  response.** A test that only reads back its own request's echo would pass
  against a handler that wrote nothing.
* **Cross-tenant misses are 404, never 403.** A 403 is an existence oracle: it
  tells org B that org A's job id is real. ADR-013 calls tenant leakage the worst
  bug this system could ship, and this module is the only place it is tested.

Only ``test_idempotency_in_progress_is_409`` carries the ``concurrency`` marker.
The advisory lock in ``app.services.idempotency.resolve`` is
``pg_try_advisory_xact_lock`` -- transaction-scoped -- so the in-flight branch is
unreachable without a genuinely separate committed session holding the lock. The
rest of this module is single-session HTTP contract coverage and deliberately
stays out of ``make test-concurrency``, which selects the reliability suite.
"""

from typing import Any
from uuid import UUID, uuid4

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.main import create_app
from app.services.claim import start_job
from app.services.idempotency import _advisory_key
from app.services.reliability import fail_job
from tests.test_concurrency import (
    Scope,
    Sessions,
    claim_one,
    job_state,
    register_worker,
)

LEASE_SECONDS = 30
TIMEOUT_MS = 10_000


@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test/api/v1"
    ) as c:
        yield c


# --- helpers ----------------------------------------------------------------


async def _bootstrap(
    client: AsyncClient, email: str = "dev@example.com", org: str = "Acme"
) -> tuple[dict[str, str], Scope]:
    """One tenant, created the way a client creates one: register, then build a
    project and a queue with the returned token."""
    r = await client.post(
        "/auth/register",
        json={"email": email, "password": "demo-password", "organization_name": org},
    )
    assert r.status_code == 201, r.text
    headers = {"Authorization": f"Bearer {r.json()['access_token']}"}

    me = (await client.get("/auth/me", headers=headers)).json()
    org_id = UUID(me["organization_id"])

    slug = org.lower()
    r = await client.post(
        f"/orgs/{org_id}/projects", json={"name": org, "slug": slug}, headers=headers
    )
    assert r.status_code == 201, r.text
    project_id = UUID(r.json()["id"])

    r = await client.post(
        f"/projects/{project_id}/queues",
        json={
            "name": "default",
            "max_concurrency": 100,
            "visibility_timeout_sec": LEASE_SECONDS,
            "default_timeout_ms": TIMEOUT_MS,
        },
        headers=headers,
    )
    assert r.status_code == 201, r.text
    scope = Scope(
        org_id=org_id,
        project_id=project_id,
        queue_id=UUID(r.json()["id"]),
        lease_seconds=LEASE_SECONDS,
        max_concurrency=100,
    )
    return headers, scope


async def _scalar(engine: AsyncEngine, sql: str, params: dict[str, Any] | None = None) -> Any:
    async with engine.connect() as c:
        return await c.scalar(text(sql), params or {})


async def _enqueue(
    client: AsyncClient, headers: dict[str, str], scope: Scope, **body: Any
) -> Any:
    payload = {"kind": "immediate", "handler": "demo.echo", **body}
    return await client.post(f"/queues/{scope.queue_id}/jobs", json=payload, headers=headers)


async def _enqueued_id(
    client: AsyncClient, headers: dict[str, str], scope: Scope, **body: Any
) -> UUID:
    r = await _enqueue(client, headers, scope, **body)
    assert r.status_code == 201, r.text
    return UUID(r.json()["id"])


async def _run(sessions: Sessions, scope: Scope, job_id: UUID, worker: str) -> tuple[UUID, int]:
    """``queued -> claimed -> running`` through the real claim and start statements."""
    worker_id = await register_worker(sessions, scope.org_id, worker)
    epoch = await claim_one(sessions, scope, worker_id, job_id)
    async with sessions() as s:
        assert await start_job(s, job_id, worker_id, epoch), "start_job lost the fence"
        await s.commit()
    return worker_id, epoch


async def _dead_letter(
    client: AsyncClient,
    headers: dict[str, str],
    scope: Scope,
    sessions: Sessions,
    worker: str = "dlq-worker",
) -> UUID:
    """Drive one job all the way to ``dead_letter`` and its DLQ entry.

    ``max_attempts=1`` exhausts the budget on the first failure, so ``fail_job``
    takes the dead-letter branch of its single statement -- the same branch the
    worker takes in production. Nothing here inserts a DLQ row by hand.
    """
    job_id = await _enqueued_id(
        client, headers, scope, handler="demo.boom", payload={"n": 1}, max_attempts=1
    )
    worker_id, epoch = await _run(sessions, scope, job_id, worker)
    async with sessions() as s:
        outcome = await fail_job(s, job_id, worker_id, epoch, "BoomError", "it broke")
        await s.commit()
    assert outcome is not None and outcome.dead_lettered, outcome
    return job_id


async def _open_entry(client: AsyncClient, headers: dict[str, str], scope: Scope) -> str:
    r = await client.get(f"/projects/{scope.project_id}/dlq", headers=headers)
    assert r.status_code == 200, r.text
    entries = r.json()["data"]
    assert len(entries) == 1, entries
    return str(entries[0]["id"])


async def _expect_404(
    client: AsyncClient, method: str, path: str, headers: dict[str, str], **kwargs: Any
) -> None:
    r = await client.request(method, path, headers=headers, **kwargs)
    assert r.status_code == 404, (
        f"{method} {path} answered {r.status_code} across a tenant boundary; it must be "
        "404 -- a 403 confirms the resource exists, which is the leak itself"
    )
    assert r.json()["error"]["code"] == "not_found", r.text


# --- cross-org isolation ----------------------------------------------------


@pytest.mark.timeout(120)
async def test_cross_org_reads_are_404_not_403(client, sessionmaker_) -> None:
    """Org B asking for org A's resources by id learns nothing, including whether
    they exist.

    Every id below is real and readable by its owner, which is what makes the
    assertion meaningful: a handler that forgot its ``organization_id`` predicate
    would answer 200 here, and one that checked ownership *after* loading would
    answer 403 and confirm the id.
    """
    a_headers, a = await _bootstrap(client, "a@example.com", "Acme")
    b_headers, _ = await _bootstrap(client, "b@example.com", "Globex")

    job_id = await _enqueued_id(client, a_headers, a)
    batch = await _enqueue(client, a_headers, a, kind="batch", items=[{"i": 0}, {"i": 1}])
    assert batch.status_code == 201, batch.text
    batch_id = batch.json()["id"]

    # Sanity: the owner really can read all of it, so a 404 below is about tenancy.
    for path in (
        f"/jobs/{job_id}",
        f"/batches/{batch_id}",
        f"/queues/{a.queue_id}/stats",
        f"/projects/{a.project_id}/metrics/summary",
        f"/orgs/{a.org_id}/projects",
    ):
        assert (await client.get(path, headers=a_headers)).status_code == 200, path

    for path in (
        f"/jobs/{job_id}",
        f"/batches/{batch_id}",
        f"/queues/{a.queue_id}/stats",
        f"/projects/{a.project_id}/metrics/summary",
        f"/orgs/{a.org_id}/projects",
    ):
        await _expect_404(client, "GET", path, b_headers)


@pytest.mark.timeout(120)
async def test_cross_org_writes_are_404_and_change_nothing(client, engine, sessionmaker_) -> None:
    """A read leak exposes data; a write leak corrupts another tenant's queue.

    The database is checked after each attempt because a 404 is not by itself
    proof that nothing happened -- a handler could write and then fail to read
    back.
    """
    a_headers, a = await _bootstrap(client, "a@example.com", "Acme")
    b_headers, _ = await _bootstrap(client, "b@example.com", "Globex")

    dead_id = await _dead_letter(client, a_headers, a, sessionmaker_)
    entry_id = await _open_entry(client, a_headers, a)
    # Enqueued *after* the dead-letter seeding: that seeding claims the whole
    # queue, and a job created before it would already be 'claimed' here.
    job_id = await _enqueued_id(client, a_headers, a)

    before = await _scalar(engine, "SELECT count(*) FROM jobs")

    # Enqueue onto another tenant's queue.
    r = await client.post(
        f"/queues/{a.queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo"},
        headers=b_headers,
    )
    assert r.status_code == 404, r.text
    assert r.json()["error"]["code"] == "not_found", r.text

    await _expect_404(client, "POST", f"/jobs/{job_id}/cancel", b_headers)
    await _expect_404(client, "POST", f"/dlq/{entry_id}/replay", b_headers)
    await _expect_404(client, "POST", f"/dlq/{entry_id}/discard", b_headers)

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == before, (
        "a cross-tenant write created or removed a job"
    )
    assert (await job_state(sessionmaker_, job_id))["status"] == "queued"
    assert (await job_state(sessionmaker_, dead_id))["status"] == "dead_letter"
    # A leaked cancel would not change the status of a queued job it could not
    # find -- it would set the flag. Check the flag too.
    assert (
        await _scalar(
            engine,
            "SELECT cancel_requested FROM jobs WHERE id = CAST(:j AS uuid)",
            {"j": str(job_id)},
        )
        is False
    ), "another tenant requested cancellation of this job"
    assert (
        await _scalar(engine, "SELECT count(*) FROM dead_letter_entries WHERE resolution IS NULL")
        == 1
    ), "another tenant resolved the entry"


@pytest.mark.timeout(120)
async def test_cross_org_lists_return_none_of_another_tenant(client, sessionmaker_) -> None:
    """The list routes are org-scoped rather than 404-ing, so they need their own
    assertion: a missing ``organization_id`` predicate is invisible to the
    by-id tests above and returns a full page of someone else's jobs."""
    a_headers, a = await _bootstrap(client, "a@example.com", "Acme")
    b_headers, b = await _bootstrap(client, "b@example.com", "Globex")

    await _enqueued_id(client, a_headers, a)
    await _dead_letter(client, a_headers, a, sessionmaker_)

    for path in (
        f"/projects/{a.project_id}/jobs",
        f"/queues/{a.queue_id}/jobs",
        f"/projects/{a.project_id}/dlq",
    ):
        r = await client.get(path, headers=b_headers)
        assert r.status_code == 200, r.text
        assert r.json()["data"] == [], f"{path} leaked another tenant's rows"

    # And B's own listing of B's project is empty for the ordinary reason.
    r = await client.get(f"/projects/{b.project_id}/jobs", headers=b_headers)
    assert r.status_code == 200 and r.json()["data"] == []


# --- idempotency ------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_same_key_same_body_replays_the_original_job(client, engine) -> None:
    """A retried POST returns the original job and its original 201.

    ``200`` on the replay would be wrong in a way clients notice: an SDK that
    treats 201 as "created" and 200 as "already exists" would take a different
    branch for a request that is, by definition, the same request.
    """
    headers, scope = await _bootstrap(client)
    keyed = {**headers, "Idempotency-Key": "order-4711"}
    body = {"kind": "immediate", "handler": "demo.echo", "payload": {"order": 4711}}

    first = await client.post(f"/queues/{scope.queue_id}/jobs", json=body, headers=keyed)
    assert first.status_code == 201, first.text

    again = await client.post(f"/queues/{scope.queue_id}/jobs", json=body, headers=keyed)
    assert again.status_code == 201, again.text
    assert again.json() == first.json(), "a replay must reproduce the original response"

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 1, (
        "the retry enqueued a second job"
    )
    assert (
        await _scalar(engine, "SELECT idempotency_key FROM jobs") == "order-4711"
    ), "the key was not persisted, so nothing would have been found to replay"


@pytest.mark.timeout(120)
async def test_same_key_different_body_is_422_key_reuse(client, engine) -> None:
    """Silently replaying the first job here would be the dangerous answer: the
    caller is told 201 and given an id for work it never asked for."""
    headers, scope = await _bootstrap(client)
    keyed = {**headers, "Idempotency-Key": "order-4711"}

    first = await client.post(
        f"/queues/{scope.queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo", "payload": {"order": 4711}},
        headers=keyed,
    )
    assert first.status_code == 201, first.text

    clash = await client.post(
        f"/queues/{scope.queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo", "payload": {"order": 9999}},
        headers=keyed,
    )
    assert clash.status_code == 422, clash.text
    assert clash.json()["error"]["code"] == "idempotency_key_reuse", clash.text

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 1
    assert (
        await _scalar(engine, "SELECT count(*) FROM jobs WHERE payload->>'order' = '9999'") == 0
    )
    # The rejection does not burn the key: the original request still replays.
    replay = await client.post(
        f"/queues/{scope.queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo", "payload": {"order": 4711}},
        headers=keyed,
    )
    assert replay.status_code == 201, replay.text
    assert replay.json()["id"] == first.json()["id"]


@pytest.mark.concurrency
@pytest.mark.timeout(120)
async def test_idempotency_in_progress_is_409(client, engine) -> None:
    """The in-flight branch, made deterministic rather than raced.

    ``resolve`` guards the key with ``pg_try_advisory_xact_lock``, which is
    transaction-scoped and non-blocking. Racing two real requests to reproduce
    this would be flaky; holding the *same* lock from an independent committed
    session is exactly the state the second request would observe, and it is
    reproducible. The final POST after the lock is released is what proves the
    409 was the lock and not a permanent rejection.
    """
    headers, scope = await _bootstrap(client)
    key = "order-4711"
    keyed = {**headers, "Idempotency-Key": key}
    body = {"kind": "immediate", "handler": "demo.echo", "payload": {"order": 4711}}

    async with engine.begin() as conn:
        held = await conn.scalar(
            text("SELECT pg_try_advisory_xact_lock(CAST(:k AS bigint))"),
            {"k": _advisory_key(scope.queue_id, key)},
        )
        assert held, "the probe session failed to take the lock; the test proves nothing"

        blocked = await client.post(
            f"/queues/{scope.queue_id}/jobs", json=body, headers=keyed
        )

    assert blocked.status_code == 409, blocked.text
    assert blocked.json()["error"]["code"] == "idempotency_in_progress", blocked.text
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 0, (
        "a duplicate in-flight request enqueued a job anyway"
    )

    after = await client.post(f"/queues/{scope.queue_id}/jobs", json=body, headers=keyed)
    assert after.status_code == 201, after.text
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 1


# --- cancel -----------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_cancel_before_start_is_terminal(client, engine, sessionmaker_) -> None:
    """A job no worker has touched is cancelled outright, and stays cancelled.

    ``finished_at`` is not decoration: ``ck_jobs_terminal_finished`` makes
    terminal status and ``finished_at`` the same fact, so a cancel that set only
    the status would abort its own transaction.
    """
    headers, scope = await _bootstrap(client)
    job_id = await _enqueued_id(client, headers, scope)

    r = await client.post(f"/jobs/{job_id}/cancel", headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "cancelled"
    assert r.json()["finished_at"] is not None

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == "cancelled"
    assert state["finished_at"] is not None
    assert state["worker_id"] is None and state["claimed_at"] is None

    assert await _scalar(engine, "SELECT count(*) FROM job_executions") == 0, (
        "cancelling unstarted work must not fabricate an attempt"
    )

    # Terminal states have zero out-edges.
    again = await client.post(f"/jobs/{job_id}/cancel", headers=headers)
    assert again.status_code == 409, again.text
    assert again.json()["error"]["code"] == "illegal_transition", again.text


@pytest.mark.timeout(120)
async def test_cancel_of_a_running_job_is_a_request_not_a_transition(
    client, engine, sessionmaker_
) -> None:
    """A running job is asked to stop; it is not teleported to terminal.

    Flipping a ``running`` row to ``cancelled`` from the API would strand the
    worker that still owns the lease: it would go on to complete the job and its
    fenced write would land on a terminal row. The cancel is a *flag* the worker
    reads from the heartbeat it already issues.
    """
    headers, scope = await _bootstrap(client)
    job_id = await _enqueued_id(client, headers, scope)
    worker_id, _ = await _run(sessionmaker_, scope, job_id, "cancel-worker")

    r = await client.post(f"/jobs/{job_id}/cancel", headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "running", "a running job was moved to a terminal state"
    assert r.json()["finished_at"] is None

    row = await _scalar(
        engine,
        "SELECT cancel_requested FROM jobs WHERE id = CAST(:j AS uuid)",
        {"j": str(job_id)},
    )
    assert row is True, "cancel_requested was never set, so the worker never learns"

    state = await job_state(sessionmaker_, job_id)
    assert state["status"] == "running"
    assert state["finished_at"] is None
    assert state["worker_id"] == worker_id, "the lease was taken from its owner"


# --- dead letter queue ------------------------------------------------------


@pytest.mark.timeout(120)
async def test_dlq_lists_the_open_entry(client, sessionmaker_) -> None:
    """The entry carries the evidence, including the payload as it was when the
    job died -- ``payload_snapshot``, not a live read of ``jobs.payload``, which
    the retention sweep may remove."""
    headers, scope = await _bootstrap(client)
    job_id = await _dead_letter(client, headers, scope, sessionmaker_)

    r = await client.get(f"/projects/{scope.project_id}/dlq", headers=headers)
    assert r.status_code == 200, r.text
    (entry,) = r.json()["data"]
    assert entry["job_id"] == str(job_id)
    assert entry["queue_id"] == str(scope.queue_id)
    assert entry["error_class"] == "BoomError"
    assert entry["payload_snapshot"] == {"n": 1}
    assert entry["error_fingerprint"], "no fingerprint, so a poison pill cannot be grouped"
    assert entry["resolution"] is None and entry["resolved_at"] is None

    resolved = await client.get(
        f"/projects/{scope.project_id}/dlq", params={"resolution": "resolved"}, headers=headers
    )
    assert resolved.status_code == 200 and resolved.json()["data"] == []


@pytest.mark.timeout(120)
async def test_dlq_replay_creates_a_new_job_and_leaves_the_corpse_terminal(
    client, engine, sessionmaker_
) -> None:
    """Replay inserts; it never resurrects.

    Terminal states have zero out-edges, so the only correct answer is a new job
    carrying ``replay_of_job_id``. Flipping the dead row back to ``queued`` would
    satisfy a "the job runs again" assertion and destroy the failure history the
    DLQ exists to preserve -- hence the checks on the *original* row.
    """
    headers, scope = await _bootstrap(client)
    job_id = await _dead_letter(client, headers, scope, sessionmaker_)
    entry_id = await _open_entry(client, headers, scope)

    r = await client.post(f"/dlq/{entry_id}/replay", headers=headers)
    assert r.status_code == 201, r.text
    entry, new_job = r.json()["entry"], r.json()["job"]

    assert new_job["id"] != str(job_id), "the DLQ replayed the dead job in place"
    assert new_job["status"] == "queued"
    assert new_job["attempt"] == 0
    assert new_job["payload"] == {"n": 1}, "the replacement did not carry the snapshot"
    assert entry["resolution"] == "requeued"
    assert entry["resolved_at"] is not None
    assert entry["replay_job_id"] == new_job["id"]

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 2, "no new job was inserted"
    assert (
        await _scalar(
            engine,
            "SELECT replay_of_job_id FROM jobs WHERE id = CAST(:n AS uuid)",
            {"n": new_job["id"]},
        )
        == job_id
    ), "the replacement does not point back at what it replaces"

    original = await job_state(sessionmaker_, job_id)
    assert original["status"] == "dead_letter", "the dead job was resurrected"
    assert original["finished_at"] is not None

    # The entry is claimed by a guarded UPDATE, so a second operator gets a 409
    # rather than a second replacement job.
    twice = await client.post(f"/dlq/{entry_id}/replay", headers=headers)
    assert twice.status_code == 409, twice.text
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 2


@pytest.mark.timeout(120)
async def test_dlq_discard_resolves_the_entry_exactly_once(
    client, engine, sessionmaker_
) -> None:
    headers, scope = await _bootstrap(client)
    job_id = await _dead_letter(client, headers, scope, sessionmaker_)
    entry_id = await _open_entry(client, headers, scope)

    r = await client.post(f"/dlq/{entry_id}/discard", headers=headers)
    assert r.status_code == 200, r.text
    assert r.json()["resolution"] == "discarded"
    assert r.json()["resolved_at"] is not None
    assert r.json()["replay_job_id"] is None, "a discard must not enqueue anything"

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 1
    assert (await job_state(sessionmaker_, job_id))["status"] == "dead_letter"

    again = await client.post(f"/dlq/{entry_id}/discard", headers=headers)
    assert again.status_code == 409, again.text
    assert again.json()["error"]["code"] == "conflict", again.text

    assert (await client.get(f"/projects/{scope.project_id}/dlq", headers=headers)).json()[
        "data"
    ] == [], "a discarded entry is still open"
    resolved = await client.get(
        f"/projects/{scope.project_id}/dlq", params={"resolution": "resolved"}, headers=headers
    )
    assert len(resolved.json()["data"]) == 1


# --- keyset pagination ------------------------------------------------------


@pytest.mark.timeout(120)
async def test_keyset_walk_returns_every_job_once_under_concurrent_inserts(
    client, engine
) -> None:
    """The property offset paging cannot provide.

    Two things are stacked here on purpose:

    * The ten jobs are the children of one batch, written in **one transaction**,
      so ``created_at`` is ``transaction_timestamp()`` and identical across all
      ten. A cursor of ``created_at`` alone would return nothing after the first
      page; the ``(created_at, id)`` tiebreak is the only reason the walk
      completes at all.
    * A new job is enqueued **between** every page fetch. Under ``OFFSET`` each
      insert shifts the window and one already-returned row comes back on the
      next page. A keyset cursor is anchored to a row, so the walk is unaffected.
    """
    headers, scope = await _bootstrap(client)
    r = await _enqueue(
        client, headers, scope, kind="batch", items=[{"i": i} for i in range(10)]
    )
    assert r.status_code == 201, r.text

    async with engine.connect() as c:
        rows = (
            await c.execute(
                text("SELECT id, created_at FROM jobs WHERE batch_id = CAST(:b AS uuid)"),
                {"b": r.json()["id"]},
            )
        ).all()
    expected = {str(row.id) for row in rows}
    assert len(expected) == 10
    assert len({row.created_at for row in rows}) == 1, (
        "the children no longer share a created_at, so this test no longer exercises "
        "the keyset tiebreak"
    )

    seen: list[str] = []
    cursor: str | None = None
    for page_no in range(10):  # a bound, not an expectation: 10 rows at 3/page is 4
        params: dict[str, Any] = {"limit": 3}
        if cursor is not None:
            params["cursor"] = cursor
        page = await client.get(
            f"/projects/{scope.project_id}/jobs", params=params, headers=headers
        )
        assert page.status_code == 200, page.text
        body = page.json()
        seen.extend(job["id"] for job in body["data"])

        # The write that breaks offset paging: it lands newer than the cursor.
        await _enqueued_id(client, headers, scope, payload={"inserted_after_page": page_no})

        if not body["page"]["has_more"]:
            assert body["page"]["next_cursor"] is None
            break
        cursor = body["page"]["next_cursor"]
        assert cursor, "has_more without a cursor leaves the client unable to continue"
    else:
        pytest.fail("the page walk did not terminate")

    assert len(seen) == len(set(seen)), f"a row was returned on two pages: {seen}"
    assert set(seen) == expected, "the walk skipped rows that existed when it started"


# --- error envelope ---------------------------------------------------------


@pytest.mark.timeout(120)
async def test_one_error_envelope_for_domain_and_framework_errors(client) -> None:
    """Domain errors and FastAPI's own errors answer in the same shape.

    Without the handlers in ``app/api/errors.py`` FastAPI emits a bare
    ``{"detail": ...}`` for its built-in 404/405/422 while domain errors use the
    envelope -- two shapes for one API, and a client that must branch on which
    layer failed. The ``detail`` assertion is the regression check.
    """
    headers, scope = await _bootstrap(client)
    rid = "req-envelope-0001"
    h = {**headers, "X-Request-ID": rid}

    cases = [
        # A domain 404 raised by the router.
        (404, "not_found", await client.get(f"/jobs/{uuid4()}", headers=h)),
        # FastAPI's own body validation: `handler` is required.
        (
            422,
            "validation_error",
            await client.post(
                f"/queues/{scope.queue_id}/jobs", json={"kind": "immediate"}, headers=h
            ),
        ),
        # Starlette's router: the path exists, the method does not.
        (405, "method_not_allowed", await client.delete(f"/jobs/{uuid4()}", headers=h)),
    ]

    for status, code, response in cases:
        assert response.status_code == status, response.text
        body = response.json()
        assert "detail" not in body, f"FastAPI's bare shape leaked: {body}"
        assert set(body) == {"error"}, body
        error = body["error"]
        assert set(error) == {"code", "message", "details", "request_id"}, error
        assert error["code"] == code, error
        assert isinstance(error["message"], str) and error["message"]
        assert isinstance(error["details"], list)
        # The envelope's request_id is the real correlation id, not a placeholder.
        assert error["request_id"] == rid, error
        assert response.headers["X-Request-ID"] == rid

    validation = cases[1][2].json()["error"]["details"]
    assert validation, "a validation failure with no details tells the client nothing"
    assert all({"field", "issue"} <= set(d) for d in validation), validation


@pytest.mark.timeout(120)
async def test_unknown_query_param_is_400(client) -> None:
    """A typo'd filter is rejected rather than ignored.

    Ignoring it returns *unfiltered* data under a request that asked to filter,
    which the caller has no way to detect. 400 rather than 422 because the
    request is well-formed -- it just asks for something the endpoint does not
    offer.
    """
    headers, scope = await _bootstrap(client)

    r = await client.get(
        f"/projects/{scope.project_id}/jobs", params={"statuss": "queued"}, headers=headers
    )
    assert r.status_code == 400, r.text
    error = r.json()["error"]
    assert error["code"] == "unknown_query_param", error
    assert [d["field"] for d in error["details"]] == ["statuss"], error

    # The declared filter on the same route is accepted.
    ok = await client.get(
        f"/projects/{scope.project_id}/jobs", params={"status": "queued"}, headers=headers
    )
    assert ok.status_code == 200, ok.text

    # `queue_id` is a legal filter on the project route and an illegal one on the
    # queue route, where honouring it would silently list another queue.
    crossed = await client.get(
        f"/queues/{scope.queue_id}/jobs",
        params={"queue_id": str(scope.queue_id)},
        headers=headers,
    )
    assert crossed.status_code == 400, crossed.text
    assert crossed.json()["error"]["code"] == "unknown_query_param"
