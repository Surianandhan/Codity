import os
import subprocess

# Must be set before app.config is imported anywhere: get_settings() is lru_cached.
os.environ.setdefault("CODITY_DATABASE_URL", "postgresql+asyncpg://localhost/codity_test")
os.environ.setdefault("CODITY_JWT_SECRET", "test-secret-not-for-production")

import pytest  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

DB_URL = os.environ["CODITY_DATABASE_URL"]

# Tables truncated between tests. Ordered so FK dependents go first.
# TRUNCATE ... CASCADE reaches tables that REFERENCE these, so job_batches,
# job_schedules, job_logs, dead_letter_entries and queue_stats_minute are all
# cleared via their FKs to jobs/queues. system_state references nothing, so it is
# listed explicitly -- without it, scheduler-tick rows survive between tests and
# any assertion about /system/status staleness becomes order-dependent.
TABLES = [
    "job_executions", "worker_heartbeats", "jobs", "workers", "queues",
    "retry_policies", "projects", "organization_members", "refresh_tokens",
    "users", "organizations", "system_state",
]


@pytest.fixture(scope="session", autouse=True)
def _migrate() -> None:
    subprocess.run(
        ["uv", "run", "alembic", "upgrade", "head"],
        env={**os.environ},
        capture_output=True,
        check=True,
    )


@pytest.fixture
async def engine():
    # NullPool: the concurrency tests need genuinely independent connections, or
    # SKIP LOCKED semantics cannot be observed at all.
    from sqlalchemy.pool import NullPool

    eng = create_async_engine(DB_URL, poolclass=NullPool)
    yield eng
    await eng.dispose()


@pytest.fixture
async def sessionmaker_(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def _clean(engine):
    """Real commits with truncation teardown.

    A connection-bound-transaction-rollback fixture would be faster, but every
    concurrency test here needs committed state visible to other sessions -- inside
    one transaction, SKIP LOCKED has nothing to skip and the tests pass vacuously.
    """
    yield
    async with engine.begin() as c:
        await c.execute(text(f"TRUNCATE {', '.join(TABLES)} RESTART IDENTITY CASCADE"))


@pytest.fixture(autouse=True)
async def _reset_app_engine():
    """app.db.session caches a module-level engine. pytest-asyncio gives each test a
    fresh event loop, so a cached engine holds connections bound to a dead loop and
    the next test dies with 'Event loop is closed'. Reset it around every test."""
    import app.db.session as sess

    sess._engine = None
    sess._sessionmaker = None
    yield
    if sess._engine is not None:
        await sess._engine.dispose()
    sess._engine = None
    sess._sessionmaker = None
