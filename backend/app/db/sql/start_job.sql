-- Transition a claimed job to running. GUARDED: the caller MUST check rowcount.
--
-- Params: :job_id, :worker_id, :lease_epoch
--
-- Zero rows means the claim is no longer ours -- the graceful-shutdown path
-- released it, or the reaper took it. The executor must then CANCEL the task
-- rather than invoke the handler. Skipping that check is a double-execution bug on
-- every deploy: the release commits while this UPDATE is in flight, another worker
-- claims the now-queued job, and this handler runs to completion anyway.
--
-- attempt increments HERE, not at claim. That is what makes a graceful release
-- from 'claimed' free: the job provably never executed, so nothing needs to be
-- decremented and the monotonicity rule is never violated.
--
-- The `exec` CTE moves the ATTEMPT row to 'running' too. Without it, the execution
-- row opened at claim time keeps started_at NULL forever -- which makes every
-- duration_ms in the product NULL (complete_job and fail_job both derive it from
-- e.started_at) and makes ExecutionStatus.RUNNING unreachable, so an attempt could
-- never be observed in progress. It is fenced on lease_epoch for the same reason
-- the jobs UPDATE is: a stale worker must not touch the live attempt.
-- ck_job_executions_open_iff_unfinished permits 'running' with finished_at NULL.
WITH started AS (
    UPDATE jobs
       SET status     = 'running',
           started_at = now(),
           attempt    = attempt + 1,
           updated_at = now()
     WHERE id          = CAST(:job_id AS uuid)
       AND worker_id   = CAST(:worker_id AS uuid)
       AND lease_epoch = CAST(:lease_epoch AS bigint)
       AND status      = 'claimed'
 RETURNING id, attempt, lease_epoch
),
exec AS (
    UPDATE job_executions e
       SET status     = 'running',
           started_at = now()
      FROM started s
     WHERE e.job_id      = s.id
       AND e.lease_epoch = s.lease_epoch
       AND e.finished_at IS NULL
 RETURNING e.id
)
SELECT s.id, s.attempt FROM started s;
