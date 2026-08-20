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
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.models.base import Base, TimestampMixin, uuid7
from app.domain.enums import (
    INFLIGHT_STATUSES,
    LIVE_STATUSES,
    TERMINAL_STATUSES,
    BackoffStrategy,
    JobKind,
    JobStatus,
)

# Native PG enums: they are what let the partial indexes below stay small and
# let the DB reject an unknown status at cast time rather than at read time.
# values_callable is required: without it SQLAlchemy uses the enum MEMBER NAMES
# (uppercase) as the Postgres labels, which would not match the lowercase values
# used by every CHECK constraint and every hand-written SQL predicate.
job_status_enum = Enum(
    JobStatus,
    name="job_status",
    native_enum=True,
    create_type=True,
    values_callable=lambda cls: [m.value for m in cls],
)

_STRATEGIES = ", ".join(f"'{s.value}'" for s in BackoffStrategy)
_KINDS = ", ".join(f"'{k.value}'" for k in JobKind)
# Derived from the canonical sets so SQL predicates can never drift from Python.
_TERMINAL = ", ".join(f"'{s.value}'" for s in sorted(TERMINAL_STATUSES))
_INFLIGHT = ", ".join(f"'{s.value}'" for s in sorted(INFLIGHT_STATUSES))
_LIVE = ", ".join(f"'{s.value}'" for s in sorted(LIVE_STATUSES))
_CATCHUP_POLICIES = ", ".join(f"'{p}'" for p in ("skip", "catchup"))


class RetryPolicy(Base, TimestampMixin):
    """Named, reusable policy. Snapshotted onto each job at enqueue so that editing a
    policy never retroactively changes the backoff of jobs already mid-retry."""

    __tablename__ = "retry_policies"
    __table_args__ = (
        UniqueConstraint("organization_id", "name", name="uq_retry_policies_org_name"),
        CheckConstraint(f"strategy IN ({_STRATEGIES})", name="strategy_valid"),
        CheckConstraint("max_attempts BETWEEN 1 AND 50", name="max_attempts_range"),
        CheckConstraint("base_ms BETWEEN 1 AND 3600000", name="base_ms_range"),
        CheckConstraint("max_ms >= base_ms", name="max_ms_gte_base"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    strategy: Mapped[str] = mapped_column(
        String(20), nullable=False, default=BackoffStrategy.EXPONENTIAL
    )
    base_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    max_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=300_000)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)


class Queue(Base, TimestampMixin):
    __tablename__ = "queues"
    __table_args__ = (
        ForeignKeyConstraint(
            ["project_id", "organization_id"],
            ["projects.id", "projects.organization_id"],
            ondelete="CASCADE",
            name="fk_queues_project",
        ),
        UniqueConstraint("project_id", "name", name="uq_queues_project_name"),
        # Anchors for the composite FK from jobs.
        UniqueConstraint("id", "project_id", name="uq_queues_id_project_id"),
        UniqueConstraint("id", "organization_id", name="uq_queues_id_organization_id"),
        CheckConstraint("max_concurrency BETWEEN 1 AND 1000", name="max_concurrency_range"),
        CheckConstraint("priority BETWEEN -100 AND 100", name="priority_range"),
        CheckConstraint("default_priority BETWEEN -100 AND 100", name="default_priority_range"),
        CheckConstraint("visibility_timeout_sec BETWEEN 10 AND 3600", name="visibility_range"),
        CheckConstraint("max_payload_bytes BETWEEN 1024 AND 1048576", name="payload_range"),
        # is_paused and paused_at must always be written together.
        CheckConstraint("is_paused = (paused_at IS NOT NULL)", name="pause_consistency"),
        # A job may never be allowed to outlive its own lease.
        CheckConstraint(
            "default_timeout_ms < visibility_timeout_sec * 1000", name="timeout_lt_lease"
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(nullable=False)
    project_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)

    max_concurrency: Mapped[int] = mapped_column(Integer, nullable=False, default=10)
    # Polling weight across queues. Distinct from default_priority, which is the
    # default priority of jobs created *on* this queue.
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    default_priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    visibility_timeout_sec: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    default_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=60_000)
    max_payload_bytes: Mapped[int] = mapped_column(Integer, nullable=False, default=65_536)
    retry_policy_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("retry_policies.id", ondelete="RESTRICT")
    )
    dlq_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_paused: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    paused_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    log_retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=7)


class Job(Base, TimestampMixin):
    __tablename__ = "jobs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["queue_id", "project_id"],
            ["queues.id", "queues.project_id"],
            ondelete="CASCADE",
            name="fk_jobs_queue",
        ),
        # Anchor so job_executions/job_logs can carry a composite FK and a
        # cross-tenant child row becomes unrepresentable.
        UniqueConstraint("id", "organization_id", name="uq_jobs_id_organization_id"),
        CheckConstraint(f"kind IN ({_KINDS})", name="kind_valid"),
        CheckConstraint(f"backoff_strategy IN ({_STRATEGIES})", name="backoff_strategy_valid"),
        CheckConstraint("priority BETWEEN -100 AND 100", name="priority_range"),
        CheckConstraint("attempt >= 0", name="attempt_non_negative"),
        CheckConstraint("max_attempts BETWEEN 1 AND 50", name="max_attempts_range"),
        CheckConstraint("timeout_ms < lease_seconds * 1000", name="timeout_lt_lease"),
        CheckConstraint("pg_column_size(payload) <= 1048576", name="payload_size"),
        # A cron occurrence and its schedule are inseparable.
        CheckConstraint(
            "(schedule_id IS NULL) = (scheduled_for IS NULL)", name="schedule_occurrence"
        ),
        CheckConstraint(
            "kind <> 'recurring' OR schedule_id IS NOT NULL", name="recurring_needs_schedule"
        ),
        # In-flight implies a lease exists.
        CheckConstraint(
            f"status NOT IN ({_INFLIGHT})"
            " OR (lease_expires_at IS NOT NULL AND claimed_at IS NOT NULL)",
            name="lease_present",
        ),
        # Terminal implies finished_at, and vice versa. This is what forces every
        # terminal transition -- including the DLQ path -- to set finished_at.
        CheckConstraint(
            f"(status IN ({_TERMINAL})) = (finished_at IS NOT NULL)", name="terminal_finished"
        ),
        # --- Hot path ---
        # Claim. Partial predicate is character-identical to the claim query's WHERE.
        Index(
            "ix_jobs_claim",
            "queue_id",
            text("priority DESC"),
            text("run_at ASC"),
            "id",
            postgresql_where=text("status = 'queued'"),
        ),
        # Promoter: scheduled jobs that have come due.
        Index("ix_jobs_due", "run_at", postgresql_where=text("status = 'scheduled'")),
        # Reaper: expired leases.
        Index(
            "ix_jobs_lease",
            "lease_expires_at",
            postgresql_where=text(f"status IN ({_INFLIGHT})"),
        ),
        # Queue depth: partial, so it spans the live set only and never job history.
        Index("ix_jobs_depth", "queue_id", "status", postgresql_where=text(f"status IN ({_LIVE})")),
        # --- Listing ---
        Index("ix_jobs_project_created", "project_id", text("created_at DESC"), text("id DESC")),
        Index(
            "ix_jobs_queue_status_created",
            "queue_id",
            "status",
            text("created_at DESC"),
            text("id DESC"),
        ),
        Index("ix_jobs_batch", "batch_id", "status", postgresql_where=text("batch_id IS NOT NULL")),
        # Idempotency, scoped to live statuses: a completed key is reusable, and DLQ
        # replay of a keyed job does not collide.
        Index(
            "ux_jobs_live_idempotency",
            "queue_id",
            "idempotency_key",
            unique=True,
            postgresql_where=text(f"idempotency_key IS NOT NULL AND status IN ({_LIVE})"),
        ),
        # Exactly one job per cron occurrence. This is what makes N schedulers safe.
        Index(
            "ux_jobs_schedule_occurrence",
            "schedule_id",
            "scheduled_for",
            unique=True,
            postgresql_where=text("schedule_id IS NOT NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID] = mapped_column(nullable=False)
    queue_id: Mapped[UUID] = mapped_column(nullable=False)
    batch_id: Mapped[UUID | None] = mapped_column(ForeignKey("job_batches.id", ondelete="SET NULL"))
    # RESTRICT, not CASCADE: deleting a schedule must not delete the history of
    # what it already ran. Schedule deletion is soft (job_schedules.deleted_at).
    schedule_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("job_schedules.id", ondelete="RESTRICT")
    )
    replay_of_job_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("jobs.id", ondelete="SET NULL")
    )

    kind: Mapped[str] = mapped_column(String(20), nullable=False)
    handler: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[JobStatus] = mapped_column(job_status_enum, nullable=False)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)

    # The single eligibility timestamp. scheduled_for is the nominal cron instant only.
    run_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )

    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    backoff_strategy: Mapped[str] = mapped_column(
        String(20), nullable=False, default=BackoffStrategy.EXPONENTIAL
    )
    backoff_base_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=1000)
    backoff_max_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=300_000)
    unregistered_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=60_000)
    # Snapshotted from queues.visibility_timeout_sec at enqueue.
    lease_seconds: Mapped[int] = mapped_column(Integer, nullable=False)

    worker_id: Mapped[UUID | None] = mapped_column()
    # The fencing token. Bumped on every ownership change; never by the trigger.
    lease_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    idempotency_key: Mapped[str | None] = mapped_column(String(255))
    correlation_id: Mapped[str | None] = mapped_column(String(64))
    last_error_class: Mapped[str | None] = mapped_column(String(200))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    # ORM optimistic locking only. Never the fence: the trigger bumps it on heartbeats.
    lock_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class JobBatch(Base, TimestampMixin):
    """A group of jobs submitted in one request.

    ``total_jobs`` is written once, at creation, inside the same transaction as the
    children. Progress is a GROUP BY status over ix_jobs_batch rather than a
    maintained counter, so there is nothing to drift and nothing to reconcile.

    There is deliberately no ``fail_fast``: honouring it would require a per-batch
    check on the hot claim path, and a validated-but-inert field is worse than an
    absent one.
    """

    __tablename__ = "job_batches"
    __table_args__ = (
        ForeignKeyConstraint(
            ["queue_id", "project_id"],
            ["queues.id", "queues.project_id"],
            ondelete="CASCADE",
            name="fk_job_batches_queue",
        ),
        CheckConstraint("total_jobs BETWEEN 1 AND 1000", name="total_jobs_range"),
        Index("ix_job_batches_project_created", "project_id", text("created_at DESC")),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID] = mapped_column(nullable=False)
    queue_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str | None] = mapped_column(String(200))
    handler: Mapped[str] = mapped_column(String(200), nullable=False)
    # Set at creation, in the same transaction as the children.
    total_jobs: Mapped[int] = mapped_column(Integer, nullable=False)


class JobSchedule(Base, TimestampMixin):
    """A recurrence rule. Not a job.

    One schedule materialises many jobs; jobs.schedule_id + jobs.scheduled_for links
    them, and ux_jobs_schedule_occurrence is what makes N schedulers safe.

    The columns from ``handler`` down are the job template the cron dispatcher copies
    onto each occurrence -- a schedule has to know what to enqueue.
    """

    __tablename__ = "job_schedules"
    __table_args__ = (
        ForeignKeyConstraint(
            ["queue_id", "project_id"],
            ["queues.id", "queues.project_id"],
            ondelete="CASCADE",
            name="fk_job_schedules_queue",
        ),
        CheckConstraint(f"catchup_policy IN ({_CATCHUP_POLICIES})", name="catchup_policy_valid"),
        CheckConstraint("max_catchup_occurrences BETWEEN 1 AND 1000", name="max_catchup_range"),
        CheckConstraint("priority BETWEEN -100 AND 100", name="priority_range"),
        CheckConstraint("max_attempts BETWEEN 1 AND 50", name="max_attempts_range"),
        # Deletion is soft, so uniqueness must be too -- otherwise a deleted schedule
        # permanently reserves its name.
        Index(
            "ux_job_schedules_queue_name",
            "queue_id",
            "name",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        # The cron dispatcher's exact predicate.
        Index(
            "ix_job_schedules_due",
            "next_occurrence_at",
            postgresql_where=text("is_active AND deleted_at IS NULL"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid7)
    organization_id: Mapped[UUID] = mapped_column(
        ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[UUID] = mapped_column(nullable=False)
    queue_id: Mapped[UUID] = mapped_column(nullable=False)
    name: Mapped[str] = mapped_column(String(200), nullable=False)

    cron: Mapped[str] = mapped_column(String(200), nullable=False)
    # Stored per schedule; croniter's timezone-aware iteration handles DST.
    timezone: Mapped[str] = mapped_column(
        String(64), nullable=False, default="UTC", server_default=text("'UTC'")
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    # Advanced from the nominal scheduled_for, never from now(), so a slow tick can
    # never shift the schedule.
    next_occurrence_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_occurrence_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # 'skip' (default) advances past a backlog after downtime; 'catchup' fires up to
    # max_catchup_occurrences.
    catchup_policy: Mapped[str] = mapped_column(
        String(20), nullable=False, default="skip", server_default=text("'skip'")
    )
    max_catchup_occurrences: Mapped[int] = mapped_column(
        Integer, nullable=False, default=10, server_default=text("10")
    )
    # Incremented instead of materialising an occurrence into a paused queue, so a
    # paused per-minute schedule cannot silently accumulate a stampede.
    skipped_occurrences: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default=text("0")
    )
    # Soft delete: occurrences already materialised keep a live parent, which is what
    # jobs.schedule_id ON DELETE RESTRICT requires.
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # --- Job template ---
    handler: Mapped[str] = mapped_column(String(200), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, default=dict, server_default=text("'{}'::jsonb")
    )
    priority: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=0, server_default=text("0")
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer, nullable=False, default=3, server_default=text("3")
    )
    timeout_ms: Mapped[int] = mapped_column(
        Integer, nullable=False, default=60_000, server_default=text("60000")
    )
