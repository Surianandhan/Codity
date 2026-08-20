"""The claim -> execute -> complete loop.

Slice 1 scope: claim, guarded start, execute, complete, and mark a failure terminal.
Retry/backoff, the DLQ, heartbeats and the lease reaper arrive in Slice 2.
"""

import asyncio
import contextlib
import os
import signal
import socket
from datetime import UTC, datetime
from uuid import UUID

import structlog
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.db.models.base import uuid7
from app.db.models.execution import Worker
from app.db.models.scheduling import Job, Queue
from app.domain.enums import JobStatus, WorkerStatus
from app.services.claim import ClaimedJob, claim_jobs, complete_job, start_job
from app.worker.handlers import REGISTRY, JobContext

log = structlog.get_logger()


class WorkerRunner:
    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        organization_id: UUID,
        name: str | None = None,
        concurrency: int = 4,
        batch_size: int = 10,
        poll_interval_ms: int = 500,
        grace_seconds: int = 30,
    ) -> None:
        self.sessionmaker = sessionmaker
        self.organization_id = organization_id
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.poll_interval = poll_interval_ms / 1000
        self.grace_seconds = grace_seconds

        self.worker_id: UUID | None = None
        self._stopping = asyncio.Event()
        self._inflight: dict[UUID, asyncio.Task[None]] = {}
        # Claimed but not yet started: these can be released for free on shutdown,
        # because attempt only increments at claimed -> running.
        self._unstarted: dict[UUID, int] = {}

    # --- lifecycle ---
    async def register(self) -> UUID:
        async with self.sessionmaker() as s:
            existing = (
                await s.execute(
                    select(Worker).where(
                        Worker.organization_id == self.organization_id, Worker.name == self.name
                    )
                )
            ).scalar_one_or_none()
            now = datetime.now(UTC)
            if existing:
                existing.status = WorkerStatus.ACTIVE
                existing.started_at = now
                existing.last_heartbeat_at = now
                existing.drain_requested = False
                existing.pid = os.getpid()
                worker_id = existing.id
            else:
                w = Worker(
                    id=uuid7(),
                    organization_id=self.organization_id,
                    name=self.name,
                    hostname=socket.gethostname(),
                    pid=os.getpid(),
                    status=WorkerStatus.ACTIVE,
                    concurrency=self.concurrency,
                    started_at=now,
                    last_heartbeat_at=now,
                )
                s.add(w)
                await s.flush()
                worker_id = w.id
            await s.commit()
        self.worker_id = worker_id
        log.info("worker.registered", worker=self.name, worker_id=str(worker_id))
        return worker_id

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self._stopping.set)

    # --- main loop ---
    async def run(self) -> None:
        await self.register()
        self.install_signal_handlers()
        try:
            while not self._stopping.is_set():
                claimed = await self._claim_round()
                if not claimed:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(
                            self._stopping.wait(), timeout=self.poll_interval
                        )
        finally:
            await self.shutdown()

    async def _queues(self) -> list[tuple[UUID, int]]:
        async with self.sessionmaker() as s:
            rows = (
                await s.execute(
                    select(Queue.id, Queue.priority)
                    .where(
                        Queue.organization_id == self.organization_id,
                        Queue.is_paused.is_(False),
                    )
                    .order_by(Queue.priority.desc())
                )
            ).all()
        return [(r[0], r[1]) for r in rows]

    async def _claim_round(self) -> int:
        free = self.concurrency - len(self._inflight)
        if free <= 0:
            await asyncio.sleep(0.01)
            return 0
        total = 0
        for queue_id, _prio in await self._queues():
            if self._stopping.is_set() or free <= 0:
                break
            async with self.sessionmaker() as s:
                assert self.worker_id is not None
                jobs = await claim_jobs(s, queue_id, self.worker_id, min(self.batch_size, free))
                await s.commit()
            for cj in jobs:
                self._unstarted[cj.job_id] = cj.lease_epoch
                self._inflight[cj.job_id] = asyncio.create_task(self._execute(cj))
            free -= len(jobs)
            total += len(jobs)
        return total

    async def _execute(self, cj: ClaimedJob) -> None:
        assert self.worker_id is not None
        try:
            async with self.sessionmaker() as s:
                # GUARDED: zero rows means shutdown released it or the reaper took
                # it. Another worker may already be running it -- do not invoke.
                if not await start_job(s, cj.job_id, self.worker_id, cj.lease_epoch):
                    await s.commit()
                    log.warning("job.start_lost", job_id=str(cj.job_id))
                    return
                await s.commit()
            self._unstarted.pop(cj.job_id, None)

            async with self.sessionmaker() as s:
                job = (await s.execute(select(Job).where(Job.id == cj.job_id))).scalar_one()
                handler_name, payload = job.handler, job.payload
                attempt, max_attempts = job.attempt, job.max_attempts
                timeout_s = job.timeout_ms / 1000
                correlation_id = job.correlation_id

            fn = REGISTRY.get(handler_name)
            if fn is None:
                await self._release_unregistered(cj, handler_name)
                return

            ctx = JobContext(
                job_id=cj.job_id,
                attempt=attempt,
                max_attempts=max_attempts,
                correlation_id=correlation_id,
            )
            structlog.contextvars.bind_contextvars(
                job_id=str(cj.job_id), attempt=attempt, correlation_id=correlation_id
            )
            try:
                result = await asyncio.wait_for(fn(ctx, payload), timeout=timeout_s)
            finally:
                structlog.contextvars.unbind_contextvars(
                    "job_id", "attempt", "correlation_id"
                )

            async with self.sessionmaker() as s:
                ok = await complete_job(s, cj.job_id, self.worker_id, cj.lease_epoch, result)
                await s.commit()
            if ok:
                log.info("job.completed", job_id=str(cj.job_id))
            else:
                # Fenced out: our lease was stolen. Discard rather than commit a
                # stale outcome over the current attempt.
                log.warning("job.abandoned_stale_lease", job_id=str(cj.job_id))
        except Exception as exc:  # noqa: BLE001 - the boundary must not leak
            await self._fail(cj, exc)
        finally:
            self._inflight.pop(cj.job_id, None)
            self._unstarted.pop(cj.job_id, None)

    async def _release_unregistered(self, cj: ClaimedJob, handler_name: str) -> None:
        """This worker does not know the handler (mixed deployment / rolling upgrade).
        Release WITHOUT consuming an attempt, but count it: without the counter this
        is an infinite claim/release loop across the fleet at claim-poll rate."""
        assert self.worker_id is not None
        async with self.sessionmaker() as s:
            await s.execute(
                text(
                    "UPDATE jobs SET status='queued', worker_id=NULL, claimed_at=NULL,"
                    " lease_expires_at=NULL, lease_epoch=lease_epoch+1,"
                    " unregistered_count=unregistered_count+1, updated_at=now()"
                    " WHERE id=CAST(:jid AS uuid) AND worker_id=CAST(:wid AS uuid)"
                    " AND lease_epoch=:ep AND status='running'"
                ),
                {"jid": str(cj.job_id), "wid": str(self.worker_id), "ep": cj.lease_epoch},
            )
            await s.execute(
                text(
                    "UPDATE job_executions SET status='lost', finished_at=now(),"
                    " error_class='UnregisteredHandler'"
                    " WHERE job_id=CAST(:jid AS uuid) AND finished_at IS NULL"
                ),
                {"jid": str(cj.job_id)},
            )
            await s.commit()
        log.error("job.unregistered_handler", job_id=str(cj.job_id), handler=handler_name)

    async def _fail(self, cj: ClaimedJob, exc: BaseException) -> None:
        """Slice 1: terminal failure. Retry/backoff and the DLQ land in Slice 2."""
        assert self.worker_id is not None
        cls = type(exc).__name__
        async with self.sessionmaker() as s:
            await s.execute(
                update(Job)
                .where(
                    Job.id == cj.job_id,
                    Job.worker_id == self.worker_id,
                    Job.lease_epoch == cj.lease_epoch,
                    Job.status == JobStatus.RUNNING,
                )
                # finished_at is mandatory on any terminal status --
                # ck_jobs_terminal_finished rejects the write otherwise.
                .values(
                    status=JobStatus.FAILED,
                    finished_at=datetime.now(UTC),
                    worker_id=None,
                    claimed_at=None,
                    lease_expires_at=None,
                    lease_epoch=Job.lease_epoch + 1,
                    last_error_class=cls,
                    last_error_message=str(exc)[:2000],
                    updated_at=datetime.now(UTC),
                )
            )
            await s.execute(
                text(
                    "UPDATE job_executions SET status='failed', finished_at=now(),"
                    " duration_ms=(EXTRACT(EPOCH FROM now()-started_at)*1000)::int,"
                    " error_class=:cls, error_message=:msg"
                    " WHERE job_id=CAST(:jid AS uuid) AND finished_at IS NULL"
                ),
                {"cls": cls, "msg": str(exc)[:2000], "jid": str(cj.job_id)},
            )
            await s.commit()
        log.warning("job.failed", job_id=str(cj.job_id), error_class=cls)

    async def shutdown(self) -> None:
        assert self.worker_id is not None
        # Unstarted claims cost nothing to release: attempt has not moved.
        if self._unstarted:
            async with self.sessionmaker() as s:
                await s.execute(
                    text(
                        "UPDATE jobs SET status='queued', worker_id=NULL, claimed_at=NULL,"
                        " lease_expires_at=NULL, lease_epoch=lease_epoch+1, updated_at=now()"
                        " WHERE id = ANY(CAST(:ids AS uuid[]))"
                        " AND worker_id=CAST(:wid AS uuid) AND status='claimed'"
                    ),
                    {
                        "ids": "{" + ",".join(str(i) for i in self._unstarted) + "}",
                        "wid": str(self.worker_id),
                    },
                )
                await s.commit()
            log.info("worker.released_unstarted", count=len(self._unstarted))

        if self._inflight:
            log.info("worker.draining", inflight=len(self._inflight))
            _, pending = await asyncio.wait(
                list(self._inflight.values()), timeout=self.grace_seconds
            )
            if pending:
                log.warning("worker.drain_timeout", abandoned=len(pending))

        async with self.sessionmaker() as s:
            await s.execute(
                update(Worker)
                .where(Worker.id == self.worker_id)
                .values(status=WorkerStatus.STOPPED, stopped_at=datetime.now(UTC))
            )
            await s.commit()
        log.info("worker.stopped", worker=self.name)
