"""Request-level idempotency for job creation.

This is not the same problem as the handler contract in the reliability design.
That one is *execution*-level: a handler may be invoked more than once for one
job, so its side effects must be keyed on ``job_id``. This one is *request*-level:
a client that retries ``POST /queues/{id}/jobs`` after a timeout must not create a
second job. Two problems, two mechanisms, and conflating them is how a system ends
up with neither.

Semantics, per the plan:

* same key + same body  -> replay the original ``201`` and the original job id
* same key + different body -> ``422 idempotency_key_reuse``
* same key, first request still in flight -> ``409 idempotency_in_progress``

**Where the record lives.** The plan's dedicated ``idempotency_keys`` table is not
in the schema this module ships against, and migrations are outside this module's
remit, so the job row *is* the idempotency record:

* Uniqueness is ``ux_jobs_live_idempotency`` -- UNIQUE (queue_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL AND status IN (live statuses). Scoping it to
  live statuses is deliberate in the plan: a completed key becomes reusable, and a
  DLQ replay of a keyed job does not collide with the corpse it replaces.
* The record is committed in the same transaction as the job insert, because it is
  the same row. There is no window in which a key is reserved but the job is not.
* The request fingerprint is *recomputed* from the persisted job rather than
  stored, so a replay with a different body is still rejected.

The one fidelity gap that recomputation costs, stated rather than hidden: a
``delayed`` job stores ``run_at = created_at + delay_ms`` and not ``delay_ms``, and
``created_at`` is a server clock while ``run_at`` is an application clock, so the
delay is not exactly reconstructible. Timing is therefore excluded from the
fingerprint for ``delayed`` (and included exactly for ``scheduled``, which stores
the client's instant verbatim). Restoring full fidelity means adding the
``idempotency_keys`` table and hashing the raw request body into it.
"""

import hashlib
import json
from datetime import UTC
from typing import Any
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.scheduling import Job, Queue
from app.domain.enums import LIVE_STATUSES, BackoffStrategy, JobKind
from app.domain.errors import IdempotencyInProgress, IdempotencyKeyReuse

# The unique index that carries the guarantee. Named here so the IntegrityError
# translation below cannot drift from the DDL by a rename.
LIVE_IDEMPOTENCY_INDEX = "ux_jobs_live_idempotency"

# Defaults applied by app.services.jobs.create_job. Mirrored here so the
# fingerprint compares *effective* values -- otherwise omitting a field that
# defaults to the same value would read as a different body.
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_STRATEGY = BackoffStrategy.EXPONENTIAL


def _digest(parts: dict[str, Any]) -> str:
    canonical = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def request_fingerprint(queue: Queue, body: Any) -> str:
    """Fingerprint of the job a request would produce, in effective terms."""
    parts: dict[str, Any] = {
        "kind": str(body.kind),
        "handler": body.handler,
        "payload": body.payload,
        "priority": body.priority if body.priority is not None else queue.default_priority,
        "max_attempts": body.max_attempts or DEFAULT_MAX_ATTEMPTS,
        "timeout_ms": body.timeout_ms or queue.default_timeout_ms,
        "backoff_strategy": str(body.backoff_strategy or DEFAULT_BACKOFF_STRATEGY),
    }
    if body.kind == JobKind.SCHEDULED:
        parts["run_at"] = body.run_at.astimezone(UTC).isoformat()
    if body.kind == JobKind.BATCH:
        # Without this, `items` is invisible to the fingerprint and two entirely
        # different batches sharing one key would replay each other -- the caller
        # would get the first batch's id back and never learn the second was dropped.
        parts["items"] = body.items
        # body.payload is the unused JobBase default for a batch; the written row's
        # payload is items[0]. Neutralise it on both sides so they can agree.
        parts["payload"] = {}
    return _digest(parts)


def job_fingerprint(job: Job, items: list[Any] | None = None) -> str:
    """The same fingerprint, recomputed from the row that was actually written.

    ``items`` is supplied only for a batch. The item list is not stored anywhere as
    a unit -- it lives spread across the children, one payload each -- so the caller
    reconstructs it and passes it in. Recomputing beats storing a second copy that
    could fall out of step with the rows it claims to describe.
    """
    parts: dict[str, Any] = {
        "kind": str(job.kind),
        "handler": job.handler,
        "payload": job.payload,
        "priority": job.priority,
        "max_attempts": job.max_attempts,
        "timeout_ms": job.timeout_ms,
        "backoff_strategy": str(job.backoff_strategy),
    }
    if job.kind == JobKind.SCHEDULED:
        parts["run_at"] = job.run_at.astimezone(UTC).isoformat()
    if job.kind == JobKind.BATCH:
        parts["items"] = items or []
        parts["payload"] = {}
    return _digest(parts)


def _advisory_key(queue_id: UUID, key: str) -> int:
    """A stable signed bigint for (queue_id, idempotency_key).

    Postgres advisory locks are cluster-wide, so this serialises duplicate
    requests across every API replica, not just within one process.
    """
    digest = hashlib.sha256(f"{queue_id}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big", signed=True)


async def resolve(session: AsyncSession, queue: Queue, key: str, body: Any) -> Job | None:
    """Decide what to do with an ``Idempotency-Key``.

    Returns the original job when this is a replay, or ``None`` when the caller
    should go ahead and insert. Raises on reuse and on an in-flight duplicate.

    The advisory lock is a *transaction*-scoped ``try`` lock, which is what turns
    the in-flight case into an immediate 409 instead of a blocked request. Without
    it the second insert would simply wait on the unique index until the first
    transaction commits -- correct, but it holds a connection hostage for as long
    as the first request runs. The lock is released by the same COMMIT that makes
    the job visible, so there is no window where the lock is gone but the row is
    not yet there.
    """
    acquired = await session.scalar(
        text("SELECT pg_try_advisory_xact_lock(CAST(:k AS bigint))"),
        {"k": _advisory_key(queue.id, key)},
    )
    if not acquired:
        raise IdempotencyInProgress(
            f"a request with Idempotency-Key {key!r} is already in progress on this queue",
            [{"field": "Idempotency-Key", "issue": "in_progress"}],
        )

    existing = (
        await session.execute(
            select(Job).where(
                Job.queue_id == queue.id,
                Job.idempotency_key == key,
                Job.status.in_(LIVE_STATUSES),
            )
        )
    ).scalar_one_or_none()
    if existing is None:
        return None

    items: list[Any] | None = None
    if existing.kind == JobKind.BATCH and existing.batch_id is not None:
        # Rebuild the submitted item list from the children. uuid7 primary keys are
        # time-ordered and the children were inserted in request order, so ORDER BY id
        # returns the items exactly as they arrived -- which is what makes comparing
        # two batch bodies meaningful rather than order-dependent noise.
        items = list(
            (
                await session.execute(
                    select(Job.payload).where(Job.batch_id == existing.batch_id).order_by(Job.id)
                )
            )
            .scalars()
            .all()
        )

    if job_fingerprint(existing, items) != request_fingerprint(queue, body):
        raise IdempotencyKeyReuse(
            f"Idempotency-Key {key!r} was already used on this queue with a different request body",
            [{"field": "Idempotency-Key", "issue": "reused_with_different_body"}],
        )
    return existing


def is_live_key_conflict(exc: IntegrityError) -> bool:
    """True when a failed insert lost the race on the live idempotency index.

    Belt and braces behind the advisory lock: the lock covers every caller that
    goes through this module, and the index covers everything else.
    """
    return LIVE_IDEMPOTENCY_INDEX in str(getattr(exc, "orig", exc))
