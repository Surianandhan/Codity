-- Record a failed attempt: retry with backoff, or dead-letter. Fenced on
-- (worker_id, lease_epoch).
--
-- Params: :job_id, :worker_id, :lease_epoch, :error_class, :error_message, :retryable
--
-- Zero rows returned means the lease was stolen between the handler raising and this
-- statement running -- the reaper already requeued the job and another worker may own
-- it. The failure is discarded rather than written over the live attempt.
--
-- THE SINGLE MOST IMPORTANT LINE IN THIS FILE is `finished_at = now()` on the
-- dead-letter branch. ck_jobs_terminal_finished makes terminal-status and finished_at
-- equivalent in both directions, so omitting it does not merely lose a timestamp -- it
-- aborts the whole transaction. No job would ever reach the DLQ, and the first
-- exhausted job would poison every subsequent fail_job call.
--
-- A retry goes to 'scheduled' with a future run_at, NOT to 'queued'. The claim query
-- has no run_at predicate (that is what keeps ix_jobs_claim small), so a queued job is
-- by definition due; a backoff retry parked in 'queued' would be re-claimed on the very
-- next poll, burning every attempt in milliseconds and deleting backoff entirely.
WITH locked AS (
    SELECT *
      FROM jobs
     WHERE id          = CAST(:job_id AS uuid)
       AND worker_id   = CAST(:worker_id AS uuid)
       AND lease_epoch = CAST(:lease_epoch AS bigint)
       AND status      = 'running'
       FOR UPDATE
),
decision AS (
    SELECT l.*,
           -- A PermanentError arrives as :retryable = false and skips the budget
           -- entirely: a 400 from a dependency will still be a 400 in 30 seconds.
           (CAST(:retryable AS boolean) AND l.attempt < l.max_attempts) AS will_retry,
           -- Backoff, computed in the DATABASE, not the worker: the worker's clock is
           -- never compared to anything, so clock skew across the fleet cannot move a
           -- retry. numeric arithmetic throughout -- 2^49 overflows bigint but not
           -- numeric, and max_attempts allows 50.
           make_interval(secs =>
               (LEAST(
                    CASE l.backoff_strategy
                        WHEN 'fixed'  THEN l.backoff_base_ms::numeric
                        WHEN 'linear' THEN l.backoff_base_ms::numeric * l.attempt
                        ELSE               l.backoff_base_ms::numeric
                                           * power(2::numeric, (l.attempt - 1)::numeric)
                    END,
                    l.backoff_max_ms::numeric
               )
               -- FULL jitter: uniform over [0, capped], not the capped value and not
               -- +/-10%. When a shared dependency dies, every in-flight job fails at
               -- nearly the same instant; deterministic backoff reproduces the
               -- stampede at every tier and narrow jitter only smears it.
               * random()) / 1000.0
           ) AS backoff
      FROM locked l
),
upd AS (
    UPDATE jobs j
       SET status           = CASE WHEN d.will_retry THEN 'scheduled'::job_status
                                   ELSE 'dead_letter'::job_status END,
           run_at           = CASE WHEN d.will_retry THEN now() + d.backoff
                                   ELSE j.run_at END,
           -- Required by ck_jobs_terminal_finished on the dead-letter branch, and
           -- required to be NULL on the retry branch by the same constraint.
           finished_at      = CASE WHEN d.will_retry THEN NULL ELSE now() END,
           -- Ownership is released, so the fence advances. Any write still in flight
           -- from this attempt now carries a stale epoch and is rejected.
           worker_id        = NULL,
           claimed_at       = NULL,
           lease_expires_at = NULL,
           lease_epoch      = j.lease_epoch + 1,
           last_error_class   = CAST(:error_class AS text),
           last_error_message = CAST(:error_message AS text),
           updated_at       = now()
      FROM decision d
     WHERE j.id = d.id
 RETURNING j.id, j.organization_id, j.project_id, j.queue_id, j.correlation_id,
           j.payload, j.status, j.attempt, j.max_attempts, j.run_at, j.lease_epoch,
           d.will_retry
),
closed AS (
    UPDATE job_executions e
       SET status      = 'failed',
           finished_at = now(),
           duration_ms = CASE
                             WHEN e.started_at IS NOT NULL
                             THEN (EXTRACT(EPOCH FROM now() - e.started_at) * 1000)::int
                         END,
           error_class   = CAST(:error_class AS text),
           error_message = CAST(:error_message AS text),
           -- Surfaced by the job timeline as "next attempt in 4.2s".
           next_retry_at = CASE WHEN u.will_retry THEN u.run_at ELSE NULL END
      FROM upd u
     WHERE e.job_id = u.id
       AND e.finished_at IS NULL
 RETURNING e.id
),
dlq AS (
    -- Only on the exhausted branch. The payload is snapshotted here because
    -- jobs.payload is subject to the retention sweep and an open DLQ entry is not.
    INSERT INTO dead_letter_entries
        (organization_id, project_id, queue_id, job_id, correlation_id,
         error_class, error_message, error_fingerprint, payload_snapshot, dead_lettered_at)
    SELECT u.organization_id, u.project_id, u.queue_id, u.id, u.correlation_id,
           CAST(:error_class AS text), CAST(:error_message AS text),
           -- Fingerprint groups a poison-pill cluster into one row on the DLQ screen.
           md5(coalesce(CAST(:error_class AS text), '')
               || coalesce(substring(CAST(:error_message AS text) FOR 200), '')),
           u.payload, now()
      FROM upd u
     WHERE NOT u.will_retry
 RETURNING id
)
SELECT u.id           AS job_id,
       u.status       AS status,
       u.attempt      AS attempt,
       u.max_attempts AS max_attempts,
       u.run_at       AS next_run_at,
       u.lease_epoch  AS lease_epoch,
       u.will_retry   AS will_retry,
       (SELECT count(*) FROM closed) AS executions_closed,
       (SELECT count(*) FROM dlq)    AS dlq_entries
  FROM upd u;
