-- Mark a running job completed. Fenced on (worker_id, lease_epoch).
--
-- Params: :job_id, :worker_id, :lease_epoch, :result
--
-- Zero rows means the lease was stolen; the worker discards its result rather than
-- committing a stale outcome over the current attempt.
--
-- Fencing uses lease_epoch, not `lease_expires_at > now()`, because the time
-- predicate has an ABA hole: a worker that stalls, is reaped, and then re-claims
-- the SAME job would let its original zombie coroutine pass both the worker_id and
-- the live-lease check. An epoch never repeats.
-- The final SELECT reads from `done`, NOT from the execution UPDATE. The caller
-- treats "no row" as "my lease was stolen, discard the result". Returning the
-- execution row's id instead conflates two different things: a data-modifying CTE
-- always runs, so a job that WAS legitimately completed but whose execution row is
-- already closed (a reaper orphan-close, say) would report a stolen lease, and the
-- worker would log job.abandoned_stale_lease for work that actually succeeded.
-- The question this statement answers is "did I complete the job" -- only `done`
-- knows that. fail_job.sql already uses this shape.
WITH done AS (
    UPDATE jobs
       SET status      = 'completed',
           finished_at = now(),
           updated_at  = now()
     WHERE id          = CAST(:job_id AS uuid)
       AND worker_id   = CAST(:worker_id AS uuid)
       AND lease_epoch = CAST(:lease_epoch AS bigint)
       AND status      = 'running'
 RETURNING id, started_at
),
exec AS (
    UPDATE job_executions e
       SET status      = 'succeeded',
           finished_at = now(),
           duration_ms = (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int,
           result      = CAST(:result AS jsonb)
      FROM done d
     WHERE e.job_id = d.id
       AND e.finished_at IS NULL
 RETURNING e.job_id, e.duration_ms
)
SELECT d.id AS job_id,
       (SELECT x.duration_ms FROM exec x LIMIT 1) AS duration_ms
  FROM done d;
