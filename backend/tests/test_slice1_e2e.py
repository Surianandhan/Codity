"""Slice 1 acceptance: a job posted over HTTP is claimed by a real worker and
reaches 'completed', with exactly one execution row."""

import asyncio
from uuid import UUID

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from app.main import create_app
from app.worker.runner import WorkerRunner


@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test/api/v1"
    ) as c:
        yield c


async def _bootstrap(client: AsyncClient) -> tuple[str, UUID, UUID]:
    r = await client.post(
        "/auth/register",
        json={
            "email": "dev@example.com",
            "password": "demo-password",
            "organization_name": "Acme",
        },
    )
    assert r.status_code == 201, r.text
    token = r.json()["access_token"]
    h = {"Authorization": f"Bearer {token}"}

    me = (await client.get("/auth/me", headers=h)).json()
    org_id = UUID(me["organization_id"])

    r = await client.post(
        f"/orgs/{org_id}/projects", json={"name": "Demo", "slug": "demo"}, headers=h
    )
    assert r.status_code == 201, r.text
    project_id = UUID(r.json()["id"])

    r = await client.post(
        f"/projects/{project_id}/queues",
        json={"name": "default", "max_concurrency": 5, "visibility_timeout_sec": 30,
              "default_timeout_ms": 10_000},
        headers=h,
    )
    assert r.status_code == 201, r.text
    return token, org_id, UUID(r.json()["id"])


async def _drain(runner: WorkerRunner, seconds: float = 5.0) -> None:
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(seconds)
    runner._stopping.set()
    await asyncio.wait_for(task, timeout=15)


@pytest.mark.timeout(60)
async def test_posted_job_is_claimed_and_completed(client, sessionmaker_, engine):
    token, org_id, queue_id = await _bootstrap(client)
    h = {"Authorization": f"Bearer {token}"}

    r = await client.post(
        f"/queues/{queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo", "payload": {"hello": "world"}},
        headers=h,
    )
    assert r.status_code == 201, r.text
    job = r.json()
    job_id = job["id"]
    assert job["status"] == "queued"
    assert job["attempt"] == 0

    runner = WorkerRunner(sessionmaker_, org_id, name="test-worker-1", concurrency=2)
    await _drain(runner, seconds=3.0)

    got = (await client.get(f"/jobs/{job_id}", headers=h)).json()
    assert got["status"] == "completed", got
    assert got["attempt"] == 1, "attempt must increment exactly once, at claimed->running"
    assert got["finished_at"] is not None

    async with engine.connect() as c:
        rows = (
            await c.execute(
                text(
                    "select status, attempt_number, duration_ms, result"
                    " from job_executions where job_id = :j"
                ),
                {"j": job_id},
            )
        ).all()
    assert len(rows) == 1, f"expected exactly one execution row, got {rows}"
    assert rows[0].status == "succeeded"
    assert rows[0].attempt_number == 1
    assert rows[0].result == {"echoed": {"hello": "world"}, "attempt": 1}


@pytest.mark.timeout(60)
async def test_paused_queue_is_not_claimed(client, sessionmaker_):
    token, org_id, queue_id = await _bootstrap(client)
    h = {"Authorization": f"Bearer {token}"}

    r = await client.post(
        f"/queues/{queue_id}/jobs",
        json={"kind": "immediate", "handler": "demo.echo"},
        headers=h,
    )
    job_id = r.json()["id"]
    assert (await client.post(f"/queues/{queue_id}/pause", headers=h)).status_code == 200

    runner = WorkerRunner(sessionmaker_, org_id, name="test-worker-2", concurrency=2)
    await _drain(runner, seconds=2.0)

    got = (await client.get(f"/jobs/{job_id}", headers=h)).json()
    assert got["status"] == "queued", "a paused queue must not be drained"

    await client.post(f"/queues/{queue_id}/resume", headers=h)
    runner2 = WorkerRunner(sessionmaker_, org_id, name="test-worker-3", concurrency=2)
    await _drain(runner2, seconds=3.0)
    got = (await client.get(f"/jobs/{job_id}", headers=h)).json()
    assert got["status"] == "completed", "resume must restart claiming"
