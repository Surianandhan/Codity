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
)
UPDATE job_executions e
   SET status      = 'succeeded',
       finished_at = now(),
       duration_ms = (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int,
       result      = CAST(:result AS jsonb)
  FROM done d
 WHERE e.job_id = d.id
   AND e.finished_at IS NULL
RETURNING e.job_id, e.duration_ms;
