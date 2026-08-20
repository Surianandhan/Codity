-- Promoter: scheduled -> queued once run_at has arrived.
--
-- Params: :batch
--
-- This is the only thing that makes a delayed job, a cron occurrence, or a backoff
-- retry eligible. The claim query deliberately has NO run_at predicate -- that is what
-- keeps ix_jobs_claim to the ready set alone -- so 'queued' means "due now" and this
-- statement is what establishes that invariant. A dead promoter is the system's biggest
-- silent failure: immediate jobs keep flowing while every delayed job and every retry
-- stalls, which is why the scheduler upserts system_state on each tick and
-- /system/status reports the age.
--
-- FOR UPDATE SKIP LOCKED so N schedulers partition the due set rather than serialising
-- on it, and so a promoter never blocks behind a claim holding the same row.
--
-- No pause check here: pausing blocks ADMISSION, and admission is the claim. A promoted
-- job simply waits in 'queued' until the queue resumes, which is what lets the UI say
-- "paused - 4 still running" without a backlog appearing from nowhere on resume.
WITH due AS (
    SELECT id
      FROM jobs
     WHERE status = 'scheduled'
       AND run_at <= now()
     ORDER BY run_at
     LIMIT CAST(:batch AS int)
     FOR UPDATE SKIP LOCKED
)
UPDATE jobs j
   SET status     = 'queued',
       updated_at = now()
  FROM due d
 WHERE j.id = d.id
RETURNING j.id AS job_id, j.queue_id, j.priority, j.run_at;
