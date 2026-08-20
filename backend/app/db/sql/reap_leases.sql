-- Reclaim jobs whose lease expired: the recovery path for `kill -9`, SIGSTOP, a
-- network partition, or a worker that simply stopped heartbeating.
--
-- Params: :batch
--
-- FOR UPDATE SKIP LOCKED, so N schedulers partition the expired set instead of
-- colliding. Correctness never depends on there being exactly one reaper.
--
-- THE `closed` CTE IS LOAD-BEARING -- deleting it wedges the flagship demo.
-- ux_job_executions_open_one permits exactly one execution row per job with
-- finished_at IS NULL. A kill -9 leaves that row open forever. If the reaper requeues
-- the job without closing it, the NEXT claim's execution INSERT raises 23505 -- and so
-- does every claim after that, on every worker, forever. The job becomes permanently
-- unexecutable, and it is precisely the job the grader killed a worker to watch
-- recover. Closing it as 'lost' is also what puts "claimed, then the worker died" on
-- the job timeline instead of leaving a silent gap.
--
-- There is deliberately NO poison-pill counter. A rule like
-- `consecutive_lease_expiries >= 2` conflates "this job kills workers" with "workers
-- this job happened to be on died": two Postgres restarts inside two minutes would
-- dead-letter the entire in-flight fleet, bypassing max_attempts entirely. The attempt
-- budget is the only thing that dead-letters a job.
--
-- Lock order is jobs -> workers, always, and the dead-worker sweep runs in its OWN
-- transaction. Holding jobs locks while taking workers locks deadlocks against a
-- heartbeat that took workers first and then blocked on jobs -- and the victim is
-- preferentially the lagging worker, manufacturing the very lease expiry this is
-- trying to repair. This statement therefore touches jobs and job_executions only.
WITH expired AS (
    SELECT id
      FROM jobs
     WHERE status IN ('claimed', 'running')
       AND lease_expires_at < now()
     ORDER BY lease_expires_at
     LIMIT CAST(:batch AS int)
     FOR UPDATE SKIP LOCKED
),
closed AS (
    UPDATE job_executions e
       SET status        = 'lost',
           finished_at   = now(),
           duration_ms   = CASE
                               WHEN e.started_at IS NOT NULL
                               THEN (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int
                           END,
           error_class   = 'LeaseExpired',
           error_message = 'worker lease expired'
      FROM expired x
     WHERE e.job_id = x.id
       AND e.finished_at IS NULL
 RETURNING e.id
),
reclaimed AS (
    UPDATE jobs j
       SET status      = CASE WHEN j.attempt >= j.max_attempts THEN 'dead_letter'::job_status
                              ELSE 'queued'::job_status END,
           -- ck_jobs_terminal_finished again: the dead-letter branch MUST set this or
           -- the whole reap transaction aborts and no lease is ever reclaimed.
           finished_at = CASE WHEN j.attempt >= j.max_attempts THEN now() ELSE NULL END,
           -- The fence. Bumped on every ownership change, so the presumed-dead
           -- worker's late completion carries a stale epoch and is rejected even if it
           -- turns out to be alive. This is what makes reaping safe against a merely
           -- SLOW worker, which is indistinguishable from a dead one.
           worker_id        = NULL,
           claimed_at       = NULL,
           lease_expires_at = NULL,
           lease_epoch      = j.lease_epoch + 1,
           last_error_class   = 'LeaseExpired',
           last_error_message = 'worker lease expired',
           updated_at         = now()
      FROM expired x
     WHERE j.id = x.id
 RETURNING j.id, j.organization_id, j.project_id, j.queue_id, j.correlation_id,
           j.payload, j.status, j.attempt, j.max_attempts, j.lease_epoch
),
dlq AS (
    INSERT INTO dead_letter_entries
        (organization_id, project_id, queue_id, job_id, correlation_id,
         error_class, error_message, error_fingerprint, payload_snapshot, dead_lettered_at)
    SELECT r.organization_id, r.project_id, r.queue_id, r.id, r.correlation_id,
           'LeaseExpired', 'worker lease expired', md5('LeaseExpired'), r.payload, now()
      FROM reclaimed r
     WHERE r.status = 'dead_letter'
 RETURNING id
)
SELECT r.id           AS job_id,
       r.status       AS status,
       r.attempt      AS attempt,
       r.max_attempts AS max_attempts,
       r.lease_epoch  AS lease_epoch,
       (SELECT count(*) FROM closed) AS executions_closed,
       (SELECT count(*) FROM dlq)    AS dlq_entries
  FROM reclaimed r;
