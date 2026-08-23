"""Dashboard numbers: stat cards, the throughput series, and queue depth.

Two rules keep these endpoints cheap enough to poll:

* **Depth comes from ``ix_jobs_depth``**, a partial index over the four live
  statuses only. A ``GROUP BY status`` without that predicate degrades into a scan
  of all job history, which grows without bound while the answer -- how much work
  is outstanding right now -- does not.
* **Everything else is aggregated from ``jobs`` and ``job_executions``
  directly.** There used to be a ``queue_stats_minute`` rollup here, but nothing in
  the running system ever wrote to it -- only ``scripts/seed.py`` did -- so every
  window counter was frozen at whatever the seed left behind and every live job was
  invisible. Reinstating a rollup writer would mean a second source of truth that
  can drift, a backfill, and its own tests; aggregating the source tables is correct
  by construction, and at this system's volume the scans are bounded by retention.

Empty buckets are gap-filled with ``generate_series`` in SQL. Returning only the
minutes that happened to have rows makes the client reconstruct the time axis, and
a flat-line outage renders as a *gap* rather than as zero -- which is the one
reading the chart exists to make obvious.

Each metric is bucketed by the timestamp that actually dates the event it counts;
the choice is commented at each branch, because getting it wrong is invisible in
the response and wrong in the chart.
"""

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import BaseModel
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PrincipalDep, SessionDep
from app.api.pagination import reject_unknown_query_params, request_id
from app.api.schemas import Meta
from app.db.models.observability import DeadLetterEntry
from app.db.models.scheduling import Job, Queue
from app.db.models.tenancy import Project
from app.domain.enums import INFLIGHT_STATUSES, LIVE_STATUSES, JobStatus
from app.domain.errors import NotFoundError, ValidationError
from app.services.jobs import get_queue

router = APIRouter(tags=["metrics"])

WindowLiteral = Literal["15m", "1h", "6h", "24h", "7d"]
BucketLiteral = Literal["1m", "5m", "15m", "1h"]

_WINDOW_SECONDS: dict[str, int] = {"15m": 900, "1h": 3600, "6h": 21600, "24h": 86400, "7d": 604800}
_BUCKET_SECONDS: dict[str, int] = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600}

# A chart with more points than a screen has pixels is a denial-of-service dressed
# as a feature.
MAX_BUCKETS = 1500


class ThroughputPoint(BaseModel):
    bucket_start: datetime
    enqueued: int
    completed: int
    failed: int
    dead_lettered: int
    retried: int
    mean_duration_ms: float | None
    max_duration_ms: int


class ThroughputOut(BaseModel):
    """Not keyset-paginated: the series is bounded by (window / bucket), so there
    is nothing to page through."""

    data: list[ThroughputPoint]
    window: WindowLiteral
    bucket: BucketLiteral
    queue_id: UUID | None
    meta: Meta


class MetricsSummaryOut(BaseModel):
    project_id: UUID
    window: WindowLiteral
    # Live statuses only, from ix_jobs_depth.
    depth: dict[str, int]
    enqueued: int
    completed: int
    failed: int
    dead_lettered: int
    retried: int
    mean_duration_ms: float | None
    max_duration_ms: int
    success_rate: float | None
    dlq_open: int
    # The promoter's health as a number: how far behind the oldest job whose
    # run_at has passed but which is still 'scheduled'.
    oldest_overdue_seconds: float | None


class QueueStatsOut(BaseModel):
    queue_id: UUID
    name: str
    is_paused: bool
    max_concurrency: int
    depth: dict[str, int]
    inflight: int
    headroom: int
    oldest_queued_age_seconds: float | None
    window: WindowLiteral
    enqueued: int
    completed: int
    failed: int
    dead_lettered: int
    retried: int
    mean_duration_ms: float | None
    max_duration_ms: int


def _resolve_window(window: str, bucket: str) -> tuple[int, int]:
    window_seconds = _WINDOW_SECONDS[window]
    bucket_seconds = _BUCKET_SECONDS[bucket]
    if window_seconds // bucket_seconds > MAX_BUCKETS:
        raise ValidationError(
            f"window {window} at bucket {bucket} would produce "
            f"{window_seconds // bucket_seconds} points; use a coarser bucket",
            [{"field": "bucket", "issue": "too_many_buckets"}],
        )
    return window_seconds, bucket_seconds


async def _assert_project(session: AsyncSession, project_id: UUID, org_id: UUID) -> None:
    found = (
        await session.execute(
            select(Project.id).where(Project.id == project_id, Project.organization_id == org_id)
        )
    ).scalar_one_or_none()
    if found is None:
        raise NotFoundError("project not found")


async def _depth(
    session: AsyncSession, org_id: UUID, *, project_id: UUID | None, queue_id: UUID | None
) -> dict[str, int]:
    """Depth by live status.

    The ``status IN (...)`` list is character-identical to ix_jobs_depth's partial
    predicate, so the index spans the live set only and this never touches job
    history -- which is the whole point, since history grows without bound while
    "how much work is outstanding right now" does not.

    The queue-scoped form deliberately omits ``organization_id``: ix_jobs_depth is
    ``(queue_id, status)``, and an extra predicate on a non-indexed column forces a
    heap recheck on every row. It is safe to omit because the caller has already
    resolved the queue for this tenant and ``fk_jobs_queue (queue_id, project_id)``
    makes a job on another tenant's queue unrepresentable.
    """
    stmt = select(Job.status, func.count()).where(Job.status.in_(LIVE_STATUSES))
    if queue_id is not None:
        stmt = stmt.where(Job.queue_id == queue_id)
    else:
        stmt = stmt.where(Job.organization_id == org_id)
    if project_id is not None:
        stmt = stmt.where(Job.project_id == project_id)
    rows = (await session.execute(stmt.group_by(Job.status))).all()
    counts = {status.value: 0 for status in sorted(LIVE_STATUSES)}
    for status, n in rows:
        counts[JobStatus(status).value] = int(n)
    return counts


def _job_scope(project_id: UUID | None, queue_id: UUID | None) -> str:
    """Tenant predicate on ``jobs``, built from constants only -- never from input.

    Emitted as literal SQL rather than as ``(:project_id IS NULL OR ...)`` because a
    parameterised OR is not sargable: the planner cannot know at plan time which
    branch survives, so it declines the index and scans. Every value stays a bound
    parameter; only the *presence* of a clause varies, and only across the three
    call sites in this module.

    ``organization_id`` is always required even when a queue or project already
    pins the tenant. It is redundant given fk_jobs_queue and the caller's own
    ownership check, and it stays anyway -- a redundant tenant predicate on a
    reporting query costs a filter, and dropping one is how a cross-tenant read
    gets introduced later by someone reading this as permission to relax it.
    """
    clauses = ["j.organization_id = CAST(:org_id AS uuid)"]
    if project_id is not None:
        clauses.append("j.project_id = CAST(:project_id AS uuid)")
    if queue_id is not None:
        clauses.append("j.queue_id = CAST(:queue_id AS uuid)")
    return "\n       AND ".join(clauses)


def _scope_params(
    org_id: UUID, project_id: UUID | None, queue_id: UUID | None
) -> dict[str, str]:
    """Exactly the parameters `_job_scope` emitted -- no more, no fewer."""
    params = {"org_id": str(org_id)}
    if project_id is not None:
        params["project_id"] = str(project_id)
    if queue_id is not None:
        params["queue_id"] = str(queue_id)
    return params


# Window totals. Three independent scans, one per timestamp column, because a job's
# enqueue, its outcome and its attempts are three different events at three
# different instants -- OR-ing them into one WHERE would both mis-date the counters
# and defeat every index.
_ROLLUP_TEMPLATE = """
WITH win AS (
    SELECT now() - make_interval(secs => :window_seconds) AS lo
),
enq AS (
    -- Enqueues bucket by created_at: a job is enqueued when it enters the system,
    -- and created_at is the only timestamp a job that has not yet run even has.
    -- Project scope rides ix_jobs_project_created (project_id, created_at DESC) --
    -- leading equality plus a range on the second column. Queue scope rides
    -- ix_jobs_queue_status_created on queue_id, with created_at as a filter.
    SELECT count(*)::bigint AS enqueued
      FROM jobs j CROSS JOIN win w
     WHERE {scope}
       AND j.created_at >= w.lo
),
outcome AS (
    -- Terminal outcomes bucket by finished_at, NOT created_at: a job completed when
    -- it finished. A three-day-old job that succeeds now belongs to this window, and
    -- dating it by creation would file today's throughput under a bucket that has
    -- already scrolled off the chart.
    -- ck_jobs_terminal_finished makes status-terminal and finished_at-non-NULL
    -- equivalent in both directions, so the status filter alone is total here.
    -- 'cancelled' is deliberately absent: an operator withdrawing work is not a
    -- failure, and counting it would drag success_rate down for a non-event.
    -- Rides ix_jobs_queue_status_created (queue_id, status) on the queue-scoped
    -- path; on the project-scoped path only project_id is indexed and finished_at
    -- is a filter -- there is no index on finished_at, and adding one is a
    -- migration, which is out of scope for this change.
    SELECT count(*) FILTER (WHERE j.status = 'completed')::bigint   AS completed,
           count(*) FILTER (WHERE j.status = 'failed')::bigint      AS failed,
           count(*) FILTER (WHERE j.status = 'dead_letter')::bigint AS dead_lettered
      FROM jobs j CROSS JOIN win w
     WHERE {scope}
       AND j.status IN ('completed', 'failed', 'dead_letter')
       AND j.finished_at >= w.lo
),
attempt AS (
    -- Durations and retries come from job_executions, the only table that records
    -- per-attempt timing, bucketed by the execution's own finished_at: an attempt
    -- becomes measurable exactly when it closes.
    -- attempt_number is (jobs.attempt + 1) at claim, so > 1 is by definition a
    -- re-execution -- a retry -- and needs no join back to the retry decision.
    -- AVG/MAX ignore NULL duration_ms rather than treating it as zero. Executions
    -- that were claimed but never started (reaped as 'lost') and every row written
    -- before start_job began setting job_executions.started_at have NULL here; a
    -- COALESCE to 0 would report those as instantaneous work, which is a lie.
    -- The join is jobs -> job_executions on job_id, ix_job_executions_job's
    -- leading column.
    SELECT count(*) FILTER (WHERE e.attempt_number > 1)::bigint AS retried,
           avg(e.duration_ms)::float8                          AS mean_duration_ms,
           max(e.duration_ms)                                  AS max_duration_ms
      FROM job_executions e
      JOIN jobs j ON j.id = e.job_id
     CROSS JOIN win w
     WHERE {scope}
       AND e.finished_at >= w.lo
)
SELECT e.enqueued, o.completed, o.failed, o.dead_lettered,
       a.retried, a.mean_duration_ms, a.max_duration_ms
  FROM enq e CROSS JOIN outcome o CROSS JOIN attempt a
"""


async def _rollup(
    session: AsyncSession,
    org_id: UUID,
    window_seconds: int,
    *,
    project_id: UUID | None = None,
    queue_id: UUID | None = None,
) -> dict[str, Any]:
    sql = _ROLLUP_TEMPLATE.format(scope=_job_scope(project_id, queue_id))
    params: dict[str, Any] = dict(_scope_params(org_id, project_id, queue_id))
    params["window_seconds"] = window_seconds
    row = (await session.execute(text(sql), params)).one()

    completed = int(row.completed)
    failed = int(row.failed)
    dead = int(row.dead_lettered)
    finished = completed + failed + dead
    return {
        "enqueued": int(row.enqueued),
        "completed": completed,
        "failed": failed,
        "dead_lettered": dead,
        "retried": int(row.retried),
        # None, not 0.0: an empty window has no mean, and 0ms is a claim about
        # speed that no row supports.
        "mean_duration_ms": (
            float(row.mean_duration_ms) if row.mean_duration_ms is not None else None
        ),
        # max_duration_ms is typed `int` on all three response models (and on the
        # frontend), so NULL has to land somewhere -- 0 is the only value the
        # contract admits. mean_duration_ms is `float | None` and carries the
        # "nothing measurable in this window" signal honestly; read the two together.
        "max_duration_ms": int(row.max_duration_ms) if row.max_duration_ms is not None else 0,
        # None, not 0.0: "no jobs finished" and "every job failed" are opposite
        # facts and must not render as the same number.
        "success_rate": (completed / finished) if finished else None,
    }


# Same three event streams as the rollup, UNION ALL'd into one per-minute stream and
# then gap-filled. date_trunc('minute', ...) is the finest bucket the API offers, and
# every coarser bucket (5m/15m/1h) is a whole number of minutes, so a truncated event
# always falls inside exactly one bucket.
_THROUGHPUT_TEMPLATE = """
WITH bounds AS (
    SELECT h.hi,
           h.hi - make_interval(secs => :window_seconds - :bucket_seconds) AS lo
      FROM (
          SELECT to_timestamp(
                     floor(extract(epoch FROM now()) / :bucket_seconds)::bigint
                     * :bucket_seconds
                 ) AS hi
      ) h
),
buckets AS (
    SELECT gs AS bucket_start
      FROM bounds b,
           generate_series(b.lo, b.hi, make_interval(secs => :bucket_seconds)) AS gs
),
events AS (
    -- Enqueues: dated by created_at. See _ROLLUP_TEMPLATE for why each branch
    -- picks the timestamp it does; the two queries must agree or the chart and the
    -- stat cards will disagree on the same window.
    SELECT date_trunc('minute', j.created_at) AS bucket_start,
           1 AS enqueued, 0 AS completed, 0 AS failed, 0 AS dead_lettered,
           0 AS retried, NULL::int AS duration_ms
      FROM jobs j
     WHERE {scope}
       AND j.created_at >= (SELECT lo FROM bounds)
    UNION ALL
    -- Terminal outcomes: dated by finished_at.
    SELECT date_trunc('minute', j.finished_at),
           0,
           CASE WHEN j.status = 'completed'   THEN 1 ELSE 0 END,
           CASE WHEN j.status = 'failed'      THEN 1 ELSE 0 END,
           CASE WHEN j.status = 'dead_letter' THEN 1 ELSE 0 END,
           0,
           NULL::int
      FROM jobs j
     WHERE {scope}
       AND j.status IN ('completed', 'failed', 'dead_letter')
       AND j.finished_at >= (SELECT lo FROM bounds)
    UNION ALL
    -- Attempts: dated by the execution's finished_at.
    SELECT date_trunc('minute', e.finished_at),
           0, 0, 0, 0,
           CASE WHEN e.attempt_number > 1 THEN 1 ELSE 0 END,
           e.duration_ms
      FROM job_executions e
      JOIN jobs j ON j.id = e.job_id
     WHERE {scope}
       AND e.finished_at >= (SELECT lo FROM bounds)
)
SELECT b.bucket_start,
       COALESCE(SUM(ev.enqueued), 0)::bigint      AS enqueued,
       COALESCE(SUM(ev.completed), 0)::bigint     AS completed,
       COALESCE(SUM(ev.failed), 0)::bigint        AS failed,
       COALESCE(SUM(ev.dead_lettered), 0)::bigint AS dead_lettered,
       COALESCE(SUM(ev.retried), 0)::bigint       AS retried,
       -- No COALESCE: an empty bucket has no mean, and the response model says so.
       AVG(ev.duration_ms)::float8                AS mean_duration_ms,
       COALESCE(MAX(ev.duration_ms), 0)::int      AS max_duration_ms
  FROM buckets b
  LEFT JOIN events ev
         ON ev.bucket_start >= b.bucket_start
        AND ev.bucket_start <  b.bucket_start + make_interval(secs => :bucket_seconds)
 GROUP BY b.bucket_start
 ORDER BY b.bucket_start
"""


@router.get("/projects/{project_id}/metrics/summary", response_model=MetricsSummaryOut)
async def metrics_summary(
    project_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    window: WindowLiteral = "1h",
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"window"})
    org_id = principal.organization_id
    await _assert_project(session, project_id, org_id)

    window_seconds = _WINDOW_SECONDS[window]
    rollup = await _rollup(session, org_id, window_seconds, project_id=project_id)

    dlq_open = (
        await session.execute(
            select(func.count())
            .select_from(DeadLetterEntry)
            .where(
                DeadLetterEntry.organization_id == org_id,
                DeadLetterEntry.project_id == project_id,
                DeadLetterEntry.resolution.is_(None),
            )
        )
    ).scalar_one()

    # Rides ix_jobs_due: (run_at) WHERE status = 'scheduled'.
    overdue = (
        await session.execute(
            select(func.extract("epoch", func.now() - func.min(Job.run_at))).where(
                Job.organization_id == org_id,
                Job.project_id == project_id,
                Job.status == JobStatus.SCHEDULED,
                Job.run_at < func.now(),
            )
        )
    ).scalar_one()

    return {
        "project_id": project_id,
        "window": window,
        "depth": await _depth(session, org_id, project_id=project_id, queue_id=None),
        "dlq_open": int(dlq_open),
        "oldest_overdue_seconds": float(overdue) if overdue is not None else None,
        **rollup,
    }


@router.get("/projects/{project_id}/metrics/throughput", response_model=ThroughputOut)
async def metrics_throughput(
    project_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    queue_id: UUID | None = None,
    window: WindowLiteral = "1h",
    bucket: BucketLiteral = "1m",
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"queue_id", "window", "bucket"})
    org_id = principal.organization_id
    await _assert_project(session, project_id, org_id)
    window_seconds, bucket_seconds = _resolve_window(window, bucket)

    sql = _THROUGHPUT_TEMPLATE.format(scope=_job_scope(project_id, queue_id))
    params: dict[str, Any] = dict(_scope_params(org_id, project_id, queue_id))
    params["window_seconds"] = window_seconds
    params["bucket_seconds"] = bucket_seconds
    rows = (await session.execute(text(sql), params)).all()

    data = [
        {
            "bucket_start": r.bucket_start,
            "enqueued": int(r.enqueued),
            "completed": int(r.completed),
            "failed": int(r.failed),
            "dead_lettered": int(r.dead_lettered),
            "retried": int(r.retried),
            "mean_duration_ms": (
                float(r.mean_duration_ms) if r.mean_duration_ms is not None else None
            ),
            "max_duration_ms": int(r.max_duration_ms),
        }
        for r in rows
    ]
    return {
        "data": data,
        "window": window,
        "bucket": bucket,
        "queue_id": queue_id,
        "meta": Meta(request_id=request_id(request)),
    }


@router.get("/queues/{queue_id}/stats", response_model=QueueStatsOut)
async def queue_stats(
    queue_id: UUID,
    principal: PrincipalDep,
    session: SessionDep,
    request: Request,
    window: WindowLiteral = "1h",
) -> dict[str, Any]:
    reject_unknown_query_params(request, {"window"})
    org_id = principal.organization_id
    queue: Queue = await get_queue(session, queue_id, org_id)

    depth = await _depth(session, org_id, project_id=None, queue_id=queue_id)
    inflight = sum(depth[s.value] for s in INFLIGHT_STATUSES)

    oldest = (
        await session.execute(
            select(func.extract("epoch", func.now() - func.min(Job.run_at))).where(
                Job.organization_id == org_id,
                Job.queue_id == queue_id,
                Job.status == JobStatus.QUEUED,
            )
        )
    ).scalar_one()

    rollup = await _rollup(session, org_id, _WINDOW_SECONDS[window], queue_id=queue_id)
    # QueueStatsOut has no success_rate field; the rollup computes it for the
    # summary endpoint, so drop it rather than let response_model silently eat it.
    rollup.pop("success_rate", None)
    return {
        "queue_id": queue.id,
        "name": queue.name,
        "is_paused": queue.is_paused,
        "max_concurrency": queue.max_concurrency,
        "depth": depth,
        "inflight": inflight,
        # What a claim would be allowed to take right now, by the same arithmetic
        # the claim statement does under the queue row lock.
        "headroom": max(queue.max_concurrency - inflight, 0),
        "oldest_queued_age_seconds": float(oldest) if oldest is not None else None,
        "window": window,
        **rollup,
    }
