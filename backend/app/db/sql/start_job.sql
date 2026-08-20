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
UPDATE jobs
   SET status     = 'running',
       started_at = now(),
       attempt    = attempt + 1,
       updated_at = now()
 WHERE id          = CAST(:job_id AS uuid)
   AND worker_id   = CAST(:worker_id AS uuid)
   AND lease_epoch = CAST(:lease_epoch AS bigint)
   AND status      = 'claimed'
RETURNING id, attempt;
