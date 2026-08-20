from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base, TimestampMixin, uuid7
from app.domain.enums import ExecutionStatus, WorkerStatus

execution_status_enum = Enum(
    ExecutionStatus,
    name="execution_status",
    native_enum=True,
    create_type=True,
    values_callable=lambda cls: [m.value for m in cls],
)

_OPEN = ", ".join(f"'{s}'" for s in ("claimed", "running"))
_WORKER_STATES = ", ".join(f"'{s.value}'" for s in WorkerStatus)


class Worker(Base, TimestampMixin):
    __tablename__ = "workers"
    __table_args__ = (
        UniqueConstraint("organization_id", "name", name="uq_workers_org_name"),
        CheckConstraint(f"status IN ({_WORKER_STATES})", name="status_valid"),
        Index("ix_workers_liveness", "organization_id", "last_heartbeat_at"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    hostname: Mapped[str | None] = mapped_column(String(255))
    pid: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=WorkerStatus.STARTING)
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False, default=4)
    # Denormalised for the hot liveness check; worker_heartbeats keeps the history.
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Read by the worker from the heartbeat's RETURNING clause -- no extra query.
    drain_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkerHeartbeat(Base):
    __tablename__ = "worker_heartbeats"
    __table_args__ = (Index("ix_worker_heartbeats_worker_time", "worker_id", text("beat_at DESC")),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    worker_id: Mapped[UUID] = mapped_column(
        ForeignKey("workers.id", ondelete="CASCADE"), nullable=False
    )
    beat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    inflight: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class JobExecution(Base):
    """One row per claim attempt. Opened at CLAIM time so a worker that dies before
    starting still leaves evidence, which the reaper closes as 'lost'.

    There is deliberately no UNIQUE (job_id, attempt_number): a claim that dies
    before starting leaves a 'lost' row at attempt N, and the next claim legitimately
    produces a second row at attempt N. Both are real history.
    """

    __tablename__ = "job_executions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["job_id", "organization_id"],
            ["jobs.id", "jobs.organization_id"],
            ondelete="CASCADE",
            name="fk_job_executions_job",
        ),
        # status and finished_at must agree, in both directions.
        CheckConstraint(
            f"(status IN ({_OPEN})) = (finished_at IS NULL)", name="open_iff_unfinished"
        ),
        # At most one open execution per job. The reaper closes orphans, so this
        # never wedges a job -- without that close, the next claim's insert would
        # raise 23505 forever and the kill -9 demo would produce a stuck job.
        Index(
            "ux_job_executions_open_one",
            "job_id",
            unique=True,
            postgresql_where=text("finished_at IS NULL"),
        ),
        Index("ix_job_executions_job", "job_id", text("attempt_number DESC"), text("id DESC")),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    job_id: Mapped[UUID] = mapped_column(nullable=False)
    organization_id: Mapped[UUID] = mapped_column(nullable=False)
    queue_id: Mapped[UUID] = mapped_column(nullable=False)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[UUID | None] = mapped_column(ForeignKey("workers.id", ondelete="SET NULL"))
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_enum, nullable=False)

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    queue_wait_ms: Mapped[int | None] = mapped_column(Integer)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_class: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    # server_default, not just default=: the claim statement is raw SQL and never
    # goes through the ORM, so a Python-side default is not applied.
    log_line_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
