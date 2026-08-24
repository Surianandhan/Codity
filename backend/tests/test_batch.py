"""Batch creation: one request, N children, one ``job_batches`` row, one transaction.

``batch`` is the only job kind whose request maps to more than one row, and it is the
only one whose failure mode is *silent*: a service that reads ``handler`` and
``payload`` but never ``items`` returns a perfectly well-formed ``201`` while writing
a single job with ``batch_id = NULL``. Every test here therefore asserts against the
**database**, not against the response body alone -- an assertion on the response is
exactly what that bug would have satisfied.

The claim/complete steps drive the real SQL through the same service wrappers the
worker uses (via ``test_concurrency``'s helpers), so the progress numbers in
``GET /batches/{id}`` are aggregated over rows a real claim actually moved.
"""

from typing import Any
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from app.main import create_app
from app.services.claim import complete_job, start_job
from tests.test_concurrency import (
    Scope,
    Sessions,
    claim_one,
    job_state,
    register_worker,
)

# The Pydantic cap on BatchJobIn.items, and the job_batches CHECK. Exercising the
# boundary rather than a token 3 is the point: 1000 children must go in as one
# statement batch inside one transaction.
MAX_BATCH = 1000

LEASE_SECONDS = 30


@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test/api/v1"
    ) as c:
        yield c


async def _bootstrap(client: AsyncClient) -> tuple[dict[str, str], Scope]:
    """Register through the API -- a batch POST needs a real JWT, so the tenant has
    to be created the way a client would create it rather than seeded directly."""
    r = await client.post(
        "/auth/register",
        json={
            "email": "batch@example.com",
            "password": "demo-password",
            "organization_name": "Acme",
        },
    )
    assert r.status_code == 201, r.text
    headers = {"Authorization": f"Bearer {r.json()['access_token']}"}

    org_id = UUID((await client.get("/auth/me", headers=headers)).json()["organization_id"])

    r = await client.post(
        f"/orgs/{org_id}/projects", json={"name": "Demo", "slug": "demo"}, headers=headers
    )
    assert r.status_code == 201, r.text
    project_id = UUID(r.json()["id"])

    r = await client.post(
        f"/projects/{project_id}/queues",
        json={
            "name": "default",
            "max_concurrency": 100,
            "visibility_timeout_sec": LEASE_SECONDS,
            "default_timeout_ms": 10_000,
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


async def _post_batch(
    client: AsyncClient,
    headers: dict[str, str],
    scope: Scope,
    items: list[dict[str, Any]],
    **extra: Any,
):
    return await client.post(
        f"/queues/{scope.queue_id}/jobs",
        json={"kind": "batch", "handler": "demo.echo", "items": items, **extra},
        headers=headers,
    )


async def _children(engine: AsyncEngine, batch_id: str) -> list[Any]:
    """Every child of a batch, in insertion order (uuid7 ids are time-ordered)."""
    async with engine.connect() as c:
        return (
            await c.execute(
                text(
                    "SELECT id, batch_id, kind, status::text AS status, payload,"
                    " lease_seconds, timeout_ms, priority, idempotency_key"
                    " FROM jobs WHERE batch_id = CAST(:b AS uuid) ORDER BY id"
                ),
                {"b": batch_id},
            )
        ).all()


async def _scalar(engine: AsyncEngine, sql: str, params: dict[str, Any] | None = None) -> Any:
    async with engine.connect() as c:
        return await c.scalar(text(sql), params or {})


async def _finish(sessions: Sessions, job_id: UUID, worker_id: UUID, epoch: int) -> None:
    """claimed -> running -> completed through the real fenced statements."""
    async with sessions() as s:
        assert await start_job(s, job_id, worker_id, epoch), "start_job lost the fence"
        await s.commit()
    async with sessions() as s:
        assert await complete_job(s, job_id, worker_id, epoch, {"ok": True}), (
            "complete_job lost the fence"
        )
        await s.commit()


# --- creation ---------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_batch_creates_one_job_per_item(client, engine) -> None:
    """1000 items -> 1000 children sharing one batch_id, and one job_batches row.

    This is the test the shipped code fails: it created exactly one job, with a NULL
    batch_id and no batch row at all.
    """
    headers, scope = await _bootstrap(client)
    items = [{"i": i, "sku": f"sku-{i:04d}"} for i in range(MAX_BATCH)]

    r = await _post_batch(client, headers, scope, items)
    assert r.status_code == 201, r.text
    body = r.json()
    batch_id = body["id"]

    assert body["total_jobs"] == MAX_BATCH, body
    assert body["queue_id"] == str(scope.queue_id)

    rows = await _children(engine, batch_id)
    assert len(rows) == MAX_BATCH, f"expected {MAX_BATCH} children, got {len(rows)}"
    assert {row.status for row in rows} == {"queued"}, "children are ordinary immediate work"
    assert {row.kind for row in rows} == {"batch"}
    # One item per child, in request order, and nothing merged or dropped.
    assert [row.payload for row in rows] == items
    # Snapshotted from the queue exactly as the single-job path does it.
    assert {row.lease_seconds for row in rows} == {LEASE_SECONDS}
    assert {row.timeout_ms for row in rows} == {10_000}

    assert await _scalar(engine, "SELECT count(*) FROM jobs") == MAX_BATCH, (
        "the batch must not create work outside itself"
    )
    batches = await _scalar(engine, "SELECT count(*) FROM job_batches")
    assert batches == 1, f"expected exactly one job_batches row, got {batches}"
    assert (
        await _scalar(
            engine,
            "SELECT total_jobs FROM job_batches WHERE id = CAST(:b AS uuid)",
            {"b": batch_id},
        )
        == MAX_BATCH
    )


@pytest.mark.timeout(60)
async def test_batch_response_is_a_batch_not_a_single_job(client) -> None:
    """The response shape distinguishes the two branches of the union.

    Returning one arbitrary child for a request that created 1000 of them is not a
    smaller answer, it is a wrong one -- the caller has no id it can poll.
    """
    headers, scope = await _bootstrap(client)

    batch = (await _post_batch(client, headers, scope, [{"i": 0}, {"i": 1}])).json()
    assert {"id", "total_jobs", "counts", "progress"} <= set(batch), batch
    assert "attempt" not in batch, "a batch response must not masquerade as one job"
    assert "run_at" not in batch

    single = await client.post(
        f"/queues/{scope.queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo"},
        headers=headers,
    )
    assert single.status_code == 201, single.text
    # Every other kind is untouched: still a JobOut, field for field.
    assert single.json()["attempt"] == 0
    assert single.json()["status"] == "queued"
    assert "total_jobs" not in single.json()


@pytest.mark.timeout(60)
async def test_batch_is_one_transaction(client, engine) -> None:
    """A child that cannot be written takes the whole request with it.

    ``timeout_ms >= lease`` violates ck_jobs_timeout_lt_lease. The batch must be
    rejected before anything is inserted -- a half-written batch would report a
    total_jobs its children can never reach.
    """
    headers, scope = await _bootstrap(client)

    r = await _post_batch(
        client,
        headers,
        scope,
        [{"i": i} for i in range(3)],
        timeout_ms=LEASE_SECONDS * 1000,
    )
    assert r.status_code == 422, r.text
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 0
    assert await _scalar(engine, "SELECT count(*) FROM job_batches") == 0


# --- progress ---------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_batch_progress_matches_reality(client, engine, sessionmaker_) -> None:
    """GET /batches/{id} aggregates the children that actually moved.

    Nothing is incremented anywhere: the numbers come from one GROUP BY over
    ix_jobs_batch, so they cannot drift from the rows they describe.
    """
    headers, scope = await _bootstrap(client)
    r = await _post_batch(client, headers, scope, [{"i": i} for i in range(5)])
    assert r.status_code == 201, r.text
    batch_id = r.json()["id"]

    fresh = (await client.get(f"/batches/{batch_id}", headers=headers)).json()
    assert fresh["counts"] == {"queued": 5}
    assert fresh["terminal_jobs"] == 0
    assert fresh["pending"] == 5
    assert fresh["progress"] == 0.0

    children = [row.id for row in await _children(engine, batch_id)]
    worker_id = await register_worker(sessionmaker_, scope.org_id, "batch-worker")

    # One real claim takes the whole queue; finish two of the five.
    epoch = await claim_one(sessionmaker_, scope, worker_id, children[0])
    await _finish(sessionmaker_, children[0], worker_id, epoch)
    second = await job_state(sessionmaker_, children[1])
    await _finish(sessionmaker_, children[1], worker_id, int(second["lease_epoch"]))

    got = (await client.get(f"/batches/{batch_id}", headers=headers)).json()
    assert got["total_jobs"] == 5
    assert got["counts"] == {"completed": 2, "claimed": 3}, got
    assert got["terminal_jobs"] == 2
    assert got["pending"] == 3
    assert got["progress"] == 0.4


# --- idempotency ------------------------------------------------------------


@pytest.mark.timeout(120)
async def test_same_key_different_items_is_not_a_replay(client, engine) -> None:
    """Two different batches under one Idempotency-Key must not replay each other.

    With ``items`` outside the request fingerprint this is the worst failure the
    endpoint can produce: the second request is answered ``201`` with the *first*
    batch's ids, and a thousand jobs the caller believes were enqueued never exist.
    """
    headers, scope = await _bootstrap(client)
    keyed = {**headers, "Idempotency-Key": "nightly-export-2026-08-24"}

    first = await _post_batch(client, keyed, scope, [{"i": 1}, {"i": 2}])
    assert first.status_code == 201, first.text

    other = await _post_batch(client, keyed, scope, [{"i": 3}, {"i": 4}])
    assert other.status_code == 422, (
        f"a different item list under the same key must be rejected, got {other.status_code}: "
        f"{other.text}"
    )
    assert other.json()["error"]["code"] == "idempotency_key_reuse"

    # And nothing from the rejected request landed.
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 2
    assert await _scalar(engine, "SELECT count(*) FROM job_batches") == 1
    payloads = await _scalar(
        engine, "SELECT count(*) FROM jobs WHERE payload->>'i' IN ('3', '4')"
    )
    assert payloads == 0


@pytest.mark.timeout(120)
async def test_same_key_same_items_replays_the_original_batch(client, engine) -> None:
    headers, scope = await _bootstrap(client)
    keyed = {**headers, "Idempotency-Key": "nightly-export-2026-08-24"}
    items = [{"i": 1}, {"i": 2}, {"i": 3}]

    first = await _post_batch(client, keyed, scope, items)
    assert first.status_code == 201, first.text

    again = await _post_batch(client, keyed, scope, items)
    assert again.status_code == 201, again.text
    assert again.json()["id"] == first.json()["id"], "a replay must return the original batch"

    assert await _scalar(engine, "SELECT count(*) FROM job_batches") == 1
    assert await _scalar(engine, "SELECT count(*) FROM jobs") == 3, (
        "a retried request must not double-enqueue the children"
    )
