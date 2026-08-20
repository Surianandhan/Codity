"""Migration chain integrity.

These run against a real PostgreSQL 15 (no Docker on this machine), because the
defects they catch -- native enum lifecycle, non-IMMUTABLE functions in CHECK
constraints, FK ordering -- do not reproduce against SQLite or a mocked dialect.
"""

import os
import subprocess

import pytest
from sqlalchemy import create_engine, text

TEST_DB = os.environ.get("CODITY_TEST_DB", "codity_test")
SYNC_URL = f"postgresql+psycopg://localhost/{TEST_DB}"
ENV = {**os.environ, "CODITY_DATABASE_URL": f"postgresql+asyncpg://localhost/{TEST_DB}"}


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "alembic", *args], env=ENV, capture_output=True, text=True, check=False
    )


def _counts() -> tuple[int, int]:
    with create_engine(SYNC_URL).connect() as c:
        tables = c.execute(
            text(
                "select count(*) from pg_tables "
                "where schemaname='public' and tablename <> 'alembic_version'"
            )
        ).scalar_one()
        types = c.execute(
            text("select count(*) from pg_type where typname in ('job_status','execution_status')")
        ).scalar_one()
    return int(tables), int(types)


@pytest.fixture(autouse=True)
def _clean() -> None:
    _alembic("downgrade", "base")


def test_upgrade_creates_schema() -> None:
    r = _alembic("upgrade", "head")
    assert r.returncode == 0, r.stderr
    tables, _ = _counts()
    assert tables >= 8, f"expected the core tables, got {tables}"


def test_upgrade_downgrade_roundtrip() -> None:
    """Downgrade must drop native enum types too.

    Alembic autogenerate does not emit DROP TYPE, so without an explicit drop the
    type survives `downgrade base` and the *second* `upgrade head` fails with
    DuplicateObjectError. Running the cycle twice is what catches it -- a single
    upgrade/downgrade pass looks clean.
    """
    for cycle in range(2):
        up = _alembic("upgrade", "head")
        assert up.returncode == 0, f"cycle {cycle} upgrade failed: {up.stderr}"
        tables, _ = _counts()
        assert tables >= 8, f"cycle {cycle}: expected tables after upgrade, got {tables}"

        down = _alembic("downgrade", "base")
        assert down.returncode == 0, f"cycle {cycle} downgrade failed: {down.stderr}"
        tables, types = _counts()
        assert tables == 0, f"cycle {cycle}: {tables} tables left after downgrade"
        assert types == 0, f"cycle {cycle}: {types} enum types leaked after downgrade"


def test_enum_labels_are_lowercase_values() -> None:
    """SQLAlchemy's Enum() defaults to member NAMES (uppercase). Every CHECK
    constraint and every hand-written SQL predicate uses the lowercase values, so a
    regression here breaks the schema at CREATE time."""
    from app.domain.enums import JobStatus

    assert _alembic("upgrade", "head").returncode == 0
    with create_engine(SYNC_URL).connect() as c:
        labels = set(
            c.execute(
                text(
                    "select enumlabel from pg_enum e "
                    "join pg_type t on t.oid = e.enumtypid where t.typname = 'job_status'"
                )
            )
            .scalars()
            .all()
        )
    assert labels == {s.value for s in JobStatus}, f"label drift: {labels}"
