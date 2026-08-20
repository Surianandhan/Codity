"""The ``job_logs`` pipeline, owned end to end by one component.

"Maintain execution logs" is a core requirement and the dashboard ships a log viewer.
Without a named owner this becomes a naive per-line ``INSERT`` on the handler's own
await path -- one round trip per line, inside the job's timeout budget -- or an empty
table. So handler output goes into an ``asyncio.Queue`` and a single flush task ships
it: every ``flush_interval_ms`` (200 by default) or every ``batch_lines`` (100),
whichever comes first.

Three properties are deliberate:

**``seq`` is assigned in the sink, not by the database.** It is a per-execution
monotonic counter, which is what makes ``uq_job_logs_execution_id_seq`` meaningful:
a flush that fails halfway and is retried lands as ``ON CONFLICT DO NOTHING``
instead of duplicating the tail of the batch. A serial id could not do that.

**``log_line_count`` is bumped in the same statement as the insert**, driven off the
INSERT's own ``RETURNING`` -- so the counter counts rows that actually landed, not
rows that were offered, and never drifts from ``count(*)`` over ``job_logs``.

**Truncation is a cap on shipping, not on logging.** A runaway handler in a loop can
emit millions of lines; past ``max_lines`` per execution the sink stops shipping and
emits exactly one ``logs truncated`` line, so the evidence that output was dropped is
itself in the log rather than inferred from a suspiciously round line count.
"""

import asyncio
import contextlib
import json
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import structlog
from sqlalchemy import Text, bindparam, text
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

log = structlog.get_logger()

__all__ = [
    "LEVELS",
    "MAX_LINES_PER_EXECUTION",
    "TRUNCATION_MESSAGE",
    "JobLogger",
    "LogLine",
    "LogSink",
    "attach_logger",
    "current_job_logger",
]

DEFAULT_FLUSH_INTERVAL_MS = 200
DEFAULT_BATCH_LINES = 100
MAX_LINES_PER_EXECUTION = 500
DEFAULT_QUEUE_MAXSIZE = 10_000
TRUNCATION_MESSAGE = "logs truncated"

# Mirrors ck_job_logs_level_valid. An unknown level would abort the whole batch, so
# it is coerced here rather than discovered at flush time.
LEVELS = frozenset({"debug", "info", "warning", "error", "critical"})


@dataclass(frozen=True, slots=True)
class LogLine:
    execution_id: int
    job_id: UUID
    seq: int
    level: str
    message: str
    context: dict[str, Any]
    logged_at: datetime


@dataclass(slots=True)
class _ExecState:
    """Per-execution sink state: the seq counter and whether the cap was hit."""

    next_seq: int = 0
    truncated: bool = False


# One statement per flush. unnest() of parallel arrays rather than executemany:
# executemany would be N round trips for the counter bump alone, and the counter must
# be driven off the insert's RETURNING to stay honest about conflicts.
_FLUSH = text(
    """
    WITH lines AS (
        SELECT CAST(t.execution_id AS bigint)      AS execution_id,
               CAST(t.job_id AS uuid)              AS job_id,
               CAST(t.seq AS integer)              AS seq,
               t.level                             AS level,
               t.message                           AS message,
               CAST(t.context AS jsonb)            AS context,
               CAST(t.logged_at AS timestamptz)    AS logged_at
          FROM unnest(:execution_ids, :job_ids, :seqs, :levels,
                      :messages, :contexts, :logged_ats)
            AS t(execution_id, job_id, seq, level, message, context, logged_at)
    ),
    ins AS (
        INSERT INTO job_logs (execution_id, job_id, seq, level, message, context, logged_at)
        SELECT execution_id, job_id, seq, level, message, context, logged_at
          FROM lines
        -- Idempotent by construction: a retried flush re-offers lines that already
        -- landed and they are dropped here instead of duplicating the tail.
        ON CONFLICT (execution_id, seq) DO NOTHING
        RETURNING execution_id
    ),
    counted AS (
        SELECT execution_id, count(*)::int AS n
          FROM ins
         GROUP BY execution_id
    )
    UPDATE job_executions e
       SET log_line_count = e.log_line_count + c.n
      FROM counted c
     WHERE e.id = c.execution_id
    RETURNING e.id AS execution_id, e.log_line_count AS log_line_count
    """
).bindparams(
    bindparam("execution_ids", type_=ARRAY(Text)),
    bindparam("job_ids", type_=ARRAY(Text)),
    bindparam("seqs", type_=ARRAY(Text)),
    bindparam("levels", type_=ARRAY(Text)),
    bindparam("messages", type_=ARRAY(Text)),
    bindparam("contexts", type_=ARRAY(Text)),
    bindparam("logged_ats", type_=ARRAY(Text)),
)

_CURRENT_LOGGER: ContextVar["JobLogger | None"] = ContextVar("codity_job_logger", default=None)


def current_job_logger() -> "JobLogger | None":
    """The logger bound to the currently executing handler task, if any.

    ``ctx.logger`` is the documented way in; this exists for helper functions deep in
    a handler's call stack that were never handed the context.
    """
    return _CURRENT_LOGGER.get()


def attach_logger(ctx: object, logger: "JobLogger") -> None:
    """Bind a per-execution logger onto the JobContext handed to the handler.

    Set dynamically because ``JobContext`` is the handler registry's type, and the
    sink is the only thing that may construct a logger for an execution id.
    """
    cast(Any, ctx).logger = logger
    _CURRENT_LOGGER.set(logger)


class JobLogger:
    """The handle a handler writes through. Never touches the database itself."""

    __slots__ = ("_execution_id", "_job_id", "_sink")

    def __init__(self, sink: "LogSink", execution_id: int, job_id: UUID) -> None:
        self._sink = sink
        self._execution_id = execution_id
        self._job_id = job_id

    @property
    def execution_id(self) -> int:
        return self._execution_id

    def log(self, level: str, message: str, **context: Any) -> None:
        self._sink.emit(self._execution_id, self._job_id, level, message, context)

    def debug(self, message: str, **context: Any) -> None:
        self.log("debug", message, **context)

    def info(self, message: str, **context: Any) -> None:
        self.log("info", message, **context)

    def warning(self, message: str, **context: Any) -> None:
        self.log("warning", message, **context)

    def error(self, message: str, **context: Any) -> None:
        self.log("error", message, **context)

    def critical(self, message: str, **context: Any) -> None:
        self.log("critical", message, **context)


class _Stop:
    """Sentinel pushed by stop(); ordering through the queue is what guarantees every
    line emitted before stop() is shipped by the final flush."""


@dataclass(slots=True)
class SinkStats:
    shipped: int = 0
    dropped_backpressure: int = 0
    dropped_truncated: int = 0
    failed_batches: int = 0
    flushes: int = 0


class LogSink:
    """Owns the queue, the seq counters, and the flush task.

    One instance per worker process. Handlers never see it; they see ``JobLogger``.
    """

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        *,
        flush_interval_ms: int = DEFAULT_FLUSH_INTERVAL_MS,
        batch_lines: int = DEFAULT_BATCH_LINES,
        max_lines: int = MAX_LINES_PER_EXECUTION,
        queue_maxsize: int = DEFAULT_QUEUE_MAXSIZE,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.flush_interval = flush_interval_ms / 1000
        self.batch_lines = batch_lines
        self.max_lines = max_lines
        self.stats = SinkStats()

        self._queue: asyncio.Queue[LogLine | _Stop] = asyncio.Queue(maxsize=queue_maxsize)
        self._state: dict[int, _ExecState] = {}
        self._task: asyncio.Task[None] | None = None

    # --- lifecycle ---
    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="logsink")

    async def stop(self, timeout: float = 10.0) -> None:
        """Ship what is queued, then stop. Called after in-flight work has drained,
        so a job's last line is never lost to shutdown."""
        task, self._task = self._task, None
        if task is None:
            return
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_Stop())
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except TimeoutError:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._state.clear()
        log.info(
            "logsink.stopped",
            shipped=self.stats.shipped,
            dropped_backpressure=self.stats.dropped_backpressure,
            dropped_truncated=self.stats.dropped_truncated,
            failed_batches=self.stats.failed_batches,
        )

    # --- producer side ---
    def logger_for(self, execution_id: int, job_id: UUID) -> JobLogger:
        self._state.setdefault(execution_id, _ExecState())
        return JobLogger(self, execution_id, job_id)

    def release(self, execution_id: int) -> None:
        """Forget an execution's counter once it is finished. Without this the sink
        is a memory leak proportional to jobs-ever-run on a long-lived worker."""
        self._state.pop(execution_id, None)

    def emit(
        self,
        execution_id: int,
        job_id: UUID,
        level: str,
        message: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        state = self._state.setdefault(execution_id, _ExecState())
        if state.truncated:
            self.stats.dropped_truncated += 1
            return

        if state.next_seq >= self.max_lines:
            state.truncated = True
            self.stats.dropped_truncated += 1
            self._offer(
                LogLine(
                    execution_id=execution_id,
                    job_id=job_id,
                    seq=state.next_seq,
                    level="warning",
                    message=TRUNCATION_MESSAGE,
                    context={"limit": self.max_lines},
                    logged_at=datetime.now(UTC),
                )
            )
            state.next_seq += 1
            return

        line = LogLine(
            execution_id=execution_id,
            job_id=job_id,
            seq=state.next_seq,
            level=level if level in LEVELS else "info",
            message=message,
            context=context or {},
            logged_at=datetime.now(UTC),
        )
        state.next_seq += 1
        self._offer(line)

    def _offer(self, line: LogLine) -> None:
        # Never block the handler on the sink: a slow database would otherwise show up
        # as job latency and, past lease_seconds, as spurious reaping.
        try:
            self._queue.put_nowait(line)
        except asyncio.QueueFull:
            self.stats.dropped_backpressure += 1

    # --- consumer side ---
    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        pending: list[LogLine] = []
        deadline: float | None = None
        stopping = False

        while not stopping:
            timeout = None if deadline is None else max(0.0, deadline - loop.time())
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout)
            except TimeoutError:
                await self._flush(pending)
                pending, deadline = [], None
                continue
            except asyncio.CancelledError:
                break

            if isinstance(item, _Stop):
                stopping = True
                # Anything already queued was emitted before stop() and must ship.
                while not self._queue.empty():
                    queued = self._queue.get_nowait()
                    if isinstance(queued, LogLine):
                        pending.append(queued)
                break

            if not pending:
                deadline = loop.time() + self.flush_interval
            pending.append(item)
            if len(pending) >= self.batch_lines:
                await self._flush(pending)
                pending, deadline = [], None

        if pending:
            await self._flush(pending)

    async def _flush(self, batch: list[LogLine]) -> None:
        if not batch:
            return
        self.stats.flushes += 1
        params = {
            "execution_ids": [str(line.execution_id) for line in batch],
            "job_ids": [str(line.job_id) for line in batch],
            "seqs": [str(line.seq) for line in batch],
            "levels": [line.level for line in batch],
            "messages": [line.message for line in batch],
            "contexts": [json.dumps(line.context) for line in batch],
            "logged_ats": [line.logged_at.isoformat() for line in batch],
        }
        # One retry, then drop. The retry is safe precisely because of the ON CONFLICT:
        # a batch that half-landed before the connection dropped re-lands as a no-op.
        for attempt in (1, 2):
            try:
                async with self.sessionmaker() as session:
                    await session.execute(_FLUSH, params)
                    await session.commit()
                self.stats.shipped += len(batch)
                return
            except Exception as exc:  # noqa: BLE001 - the sink must never kill a worker
                if attempt == 2:
                    self.stats.failed_batches += 1
                    log.warning("logsink.flush_failed", lines=len(batch), error=str(exc))
                    return
                await asyncio.sleep(0.05)
