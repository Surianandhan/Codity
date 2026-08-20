"""Operator-facing tables: the dead letter queue, execution logs, scheduler
liveness, and the per-minute rollup behind the throughput chart.

None of these sit on the hot claim path. They exist so that a failure is visible
and replayable rather than merely recorded.
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
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

from app.db.models.base import Base

_RESOLUTIONS = ", ".join(f"'{r}'" for r in ("requeued", "discarded", "resolved_manually"))
_LOG_LEVELS = ", ".join(f"'{level}'" for level in ("debug", "info", "warning", "error", "critical"))


class DeadLetterEntry(Base):
    """Operator workflow for a permanently failed job: what broke, the payload as it
    was, and what a human did about it.

    Kept off the hot ``jobs`` row deliberately -- the payload snapshot and resolution
    trail are read once by an operator and never by the claim query.
    """

    __tablename__ = "dead_letter_entries"
    __table_args__ = (
        # Composite, so a cross-tenant DLQ row is unrepresentable rather than merely
        # unlikely. RESTRICT, not CASCADE: an unresolved entry is the operator's
        # evidence and must outlive the jobs retention sweep, which is why that sweep
        # carries a NOT EXISTS guard.
        ForeignKeyConstraint(
            ["job_id", "organization_id"],
            ["jobs.id", "jobs.organization_id"],
            ondelete="RESTRICT",
            name="fk_dead_letter_entries_job",
        ),
        CheckConstraint(
            f"resolution IS NULL OR resolution IN ({_RESOLUTIONS})", name="resolution_valid"
        ),
        # Resolution and its timestamp are written together, or not at all -- the same
        # shape as queues.is_paused/paused_at and jobs.status/finished_at.
        CheckConstraint("(resolution IS NULL) = (resolved_at IS NULL)", name="resolution_closed"),
        # The DLQ screen: open entries for a project, newest first.
        Index(
            "ix_dlq_open",
            "project_id",
            text("dead_lettered_at DESC"),
            postgresql_where=text("resolution IS NULL"),
        ),
    )

    # server_default, not just default=: fail_job.sql and reap_leases.sql insert here
    # with raw SQL and never pass through the ORM.
    id: Mapped[UUID] = mapped_column(primary_key=True, server_default=text("gen_random_uuid()"))
    organization_id: Mapped[UUID] = mapped_column(nullable=False)
    project_id: Mapped[UUID] = mapped_column(nullable=False)
    queue_id: Mapped[UUID] = mapped_column(nullable=False)
    job_id: Mapped[UUID] = mapped_column(nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(64))

    error_class: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)
    # md5(error_class || substring(error_message for 200)). Groups a poison-pill
    # cluster into one row on the DLQ screen.
    error_fingerprint: Mapped[str | None] = mapped_column(String(32))
    # The payload as it was when the job died. jobs.payload can be purged; this cannot.
    payload_snapshot: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    dead_lettered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )

    resolution: Mapped[str | None] = mapped_column(String(20))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    resolved_by: Mapped[UUID | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))
    # Replay inserts a *new* job rather than resurrecting a terminal one; this points
    # at it, and jobs.replay_of_job_id points back.
    replay_job_id: Mapped[UUID | None] = mapped_column(ForeignKey("jobs.id", ondelete="SET NULL"))


class JobLog(Base):
    """One line emitted by a handler, owned end to end by the worker's log sink.

    ``seq`` is a per-execution monotonic counter held in the sink. The UNIQUE on
    (execution_id, seq) is what makes the batched flush idempotent: a retried flush
    lands as ON CONFLICT DO NOTHING instead of duplicating the tail of the batch.
    """

    __tablename__ = "job_logs"
    __table_args__ = (
        # Doubles as ix_job_logs_exec_seq -- ordered log reads use this index too.
        UniqueConstraint("execution_id", "seq", name="uq_job_logs_execution_id_seq"),
        CheckConstraint(f"level IN ({_LOG_LEVELS})", name="level_valid"),
        CheckConstraint("seq >= 0", name="seq_non_negative"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    execution_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("job_executions.id", ondelete="CASCADE"), nullable=False
    )
    # Denormalised so the job-detail log view never has to join through executions.
    job_id: Mapped[UUID] = mapped_column(ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    seq: Mapped[int] = mapped_column(Integer, nullable=False)
    level: Mapped[str] = mapped_column(
        String(10), nullable=False, default="info", server_default=text("'info'")
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    context: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    logged_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class SystemState(Base):
    """Scheduler liveness, one row per loop ('promoter', 'cron', 'reaper',
    'dead_worker', 'retention').

    The API and the scheduler are different processes, so an in-process gauge is
    invisible to /system/status. Every loop upserts its row on every tick, which turns
    the system's biggest silent failure -- a dead promoter, stalling every delayed job
    and every backoff retry while immediate jobs keep flowing -- into a number on the
    dashboard.
    """

    __tablename__ = "system_state"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    last_error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class QueueStatsMinute(Base):
    """Per-queue, per-minute rollup behind the throughput chart.

    Pre-aggregated because the chart's alternative is a COUNT over job history on
    every poll. Duration percentiles are deliberately not stored -- sum and max give
    mean and worst case; percentiles need a second rollup pass and are documented as
    such.
    """

    __tablename__ = "queue_stats_minute"
    __table_args__ = (
        ForeignKeyConstraint(
            ["queue_id"], ["queues.id"], ondelete="CASCADE", name="fk_queue_stats_minute_queue"
        ),
        CheckConstraint("date_trunc('minute', bucket_start) = bucket_start", name="bucket_aligned"),
        # Retention sweep: delete whole buckets older than the window.
        Index("ix_queue_stats_minute_bucket", text("bucket_start DESC")),
    )

    queue_id: Mapped[UUID] = mapped_column(primary_key=True)
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)

    # Every counter carries a server_default: the rollup is a raw-SQL upsert that
    # touches only the columns it increments.
    enqueued: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    completed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    failed: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    dead_lettered: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    retried: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
    # bigint: a minute of long jobs overflows int4 milliseconds at ~24 days of summed
    # duration, which a 64-concurrency fleet reaches inside a single bucket.
    sum_duration_ms: Mapped[int] = mapped_column(
        BigInteger, nullable=False, server_default=text("0")
    )
    max_duration_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("0"))
