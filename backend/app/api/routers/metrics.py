"""Dashboard numbers: stat cards, the throughput series, and queue depth.

Two rules keep these endpoints cheap enough to poll:

* **Depth comes from ``ix_jobs_depth``**, a partial index over the four live
  statuses only. A ``GROUP BY status`` without that predicate degrades into a scan
  of all job history, which grows without bound while the answer -- how much work
  is outstanding right now -- does not.
* **Throughput comes from ``queue_stats_minute``**, not from counting job rows. A
  chart that recounts history on every 30s poll is a self-inflicted load test.

Empty buckets are gap-filled with ``generate_series`` in SQL. Returning only the
minutes that happened to have rows makes the client reconstruct the time axis, and
a flat-line outage renders as a *gap* rather than as zero -- which is the one
reading the chart exists to make obvious.
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


_ROLLUP_SQL = """
SELECT COALESCE(SUM(s.enqueued), 0)::bigint       AS enqueued,
       COALESCE(SUM(s.completed), 0)::bigint      AS completed,
       COALESCE(SUM(s.failed), 0)::bigint         AS failed,
       COALESCE(SUM(s.dead_lettered), 0)::bigint  AS dead_lettered,
       COALESCE(SUM(s.retried), 0)::bigint        AS retried,
       COALESCE(SUM(s.sum_duration_ms), 0)::bigint AS sum_duration_ms,
       COALESCE(MAX(s.max_duration_ms), 0)::int    AS max_duration_ms
  FROM queue_stats_minute s
  JOIN queues q ON q.id = s.queue_id
 WHERE q.organization_id = CAST(:org_id AS uuid)
   AND (CAST(:project_id AS uuid) IS NULL OR q.project_id = CAST(:project_id AS uuid))
   AND (CAST(:queue_id   AS uuid) IS NULL OR s.queue_id   = CAST(:queue_id   AS uuid))
   AND s.bucket_start >= date_trunc('minute', now()) - make_interval(secs => :window_seconds)
"""


async def _rollup(
    session: AsyncSession,
    org_id: UUID,
    window_seconds: int,
    *,
    project_id: UUID | None = None,
    queue_id: UUID | None = None,
) -> dict[str, Any]:
    row = (
        await session.execute(
            text(_ROLLUP_SQL),
            {
                "org_id": str(org_id),
                "project_id": str(project_id) if project_id else None,
                "queue_id": str(queue_id) if queue_id else None,
                "window_seconds": window_seconds,
            },
        )
    ).one()
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
        "mean_duration_ms": (int(row.sum_duration_ms) / completed) if completed else None,
        "max_duration_ms": int(row.max_duration_ms),
        "success_rate": (completed / finished) if finished else None,
    }


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


_THROUGHPUT_SQL = """
WITH bounds AS (
    SELECT to_timestamp(
               floor(extract(epoch FROM now()) / :bucket_seconds)::bigint * :bucket_seconds
           ) AS hi
),
buckets AS (
    SELECT gs AS bucket_start
      FROM bounds b,
           generate_series(
               b.hi - make_interval(secs => :window_seconds - :bucket_seconds),
               b.hi,
               make_interval(secs => :bucket_seconds)
           ) AS gs
),
scoped AS (
    SELECT s.*
      FROM queue_stats_minute s
      JOIN queues q ON q.id = s.queue_id
     WHERE q.organization_id = CAST(:org_id AS uuid)
       AND q.project_id = CAST(:project_id AS uuid)
       AND (CAST(:queue_id AS uuid) IS NULL OR s.queue_id = CAST(:queue_id AS uuid))
)
SELECT b.bucket_start,
       COALESCE(SUM(s.enqueued), 0)::bigint        AS enqueued,
       COALESCE(SUM(s.completed), 0)::bigint       AS completed,
       COALESCE(SUM(s.failed), 0)::bigint          AS failed,
       COALESCE(SUM(s.dead_lettered), 0)::bigint   AS dead_lettered,
       COALESCE(SUM(s.retried), 0)::bigint         AS retried,
       COALESCE(SUM(s.sum_duration_ms), 0)::bigint AS sum_duration_ms,
       COALESCE(MAX(s.max_duration_ms), 0)::int    AS max_duration_ms
  FROM buckets b
  LEFT JOIN scoped s
         ON s.bucket_start >= b.bucket_start
        AND s.bucket_start <  b.bucket_start + make_interval(secs => :bucket_seconds)
 GROUP BY b.bucket_start
 ORDER BY b.bucket_start
"""


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

    rows = (
        await session.execute(
            text(_THROUGHPUT_SQL),
            {
                "org_id": str(org_id),
                "project_id": str(project_id),
                "queue_id": str(queue_id) if queue_id else None,
                "window_seconds": window_seconds,
                "bucket_seconds": bucket_seconds,
            },
        )
    ).all()

    data = [
        {
            "bucket_start": r.bucket_start,
            "enqueued": int(r.enqueued),
            "completed": int(r.completed),
            "failed": int(r.failed),
            "dead_lettered": int(r.dead_lettered),
            "retried": int(r.retried),
            "mean_duration_ms": (
                int(r.sum_duration_ms) / int(r.completed) if int(r.completed) else None
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
