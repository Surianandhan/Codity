-- Atomically claim up to :batch_size jobs from one queue for one worker.
--
-- Params: :queue_id, :batch_size, :worker_id
--
-- Why FOR NO KEY UPDATE on the queue row and not FOR UPDATE:
--   Inserting a job takes FOR KEY SHARE on the referenced queues row (the FK).
--   FOR UPDATE conflicts with FOR KEY SHARE, so every claim would block every
--   enqueue on that queue. FOR NO KEY UPDATE still conflicts with itself -- so
--   claimers stay mutually exclusive, which is the invariant that matters -- but
--   lets producers through.
--
-- Why the queue row is locked at all: it makes the in-flight count stable for the
-- duration of the statement, which makes max_concurrency an EXACT cap rather than
-- a check-then-act race. Cost: claims serialise per queue. Different queues never
-- contend.
--
-- Why SKIP LOCKED on the jobs: plain FOR UPDATE would make worker B block on the
-- row worker A is claiming and then re-read it after A commits -- waiting for work
-- it can never have. SKIP LOCKED steps over locked rows, so K workers partition the
-- ready set with no coordination.
--
-- Pause and existence are checked by lock_queue.sql BEFORE this statement runs;
-- app/services/claim.py returns [] without issuing this statement at all if that
-- lock finds no row. headroom therefore always yields exactly one row here.
-- The queue row's FOR NO KEY UPDATE lock is acquired by lock_queue.sql as a
-- PRIOR statement in the same transaction. This statement must be issued only
-- after that lock is held -- see app/services/claim.py::claim_jobs. Splitting it
-- out is required for correctness, not style: see lock_queue.sql's header.
WITH headroom AS (
    SELECT GREATEST(
               LEAST(
                   CAST(:batch_size AS int),
                   CAST(:max_concurrency AS int)
                   - (SELECT count(*)::int
                        FROM jobs
                       WHERE queue_id = :queue_id
                         AND status IN ('claimed', 'running'))
               ),
               0
           )::int AS n
),
picked AS (
    SELECT j.id
      FROM jobs j
     WHERE j.queue_id = :queue_id
       AND j.status = 'queued'
     ORDER BY j.priority DESC, j.run_at ASC, j.id ASC
     LIMIT (SELECT COALESCE(max(n), 0) FROM headroom)
     FOR UPDATE SKIP LOCKED
),
-- Driving the UPDATE from `id = ANY(ARRAY(SELECT id FROM picked))` rather than a
-- join to the `picked` CTE is what lets the planner use ix_jobs_claim's underlying
-- primary key for the UPDATE's own row lookups. A `FROM picked p WHERE j.id = p.id`
-- join instead makes jobs (not picked) the join's driving side for row
-- identification, and Postgres has no cheaper way to drive that than a Hash Join
-- built on a full scan of jobs -- exactly the Seq Scan this design exists to avoid.
claimed AS (
    UPDATE jobs j
       SET status           = 'claimed',
           worker_id        = CAST(:worker_id AS uuid),
           lease_epoch      = j.lease_epoch + 1,
           claimed_at       = now(),
           lease_expires_at = now() + make_interval(secs => j.lease_seconds),
           updated_at       = now()
     WHERE j.id = ANY(ARRAY(SELECT id FROM picked))
 RETURNING j.id, j.organization_id, j.queue_id, j.handler, j.payload,
           j.attempt, j.max_attempts, j.timeout_ms, j.lease_epoch,
           j.claimed_at, j.run_at, j.correlation_id
)
-- The execution row is opened at CLAIM time, not at start. A worker that dies
-- between claim and start therefore leaves evidence (the reaper closes it as
-- 'lost'), which is what makes the "worker died before starting" case visible in
-- the UI instead of invisible.
INSERT INTO job_executions
    (job_id, organization_id, queue_id, attempt_number, worker_id,
     lease_epoch, status, claimed_at, queue_wait_ms)
SELECT c.id, c.organization_id, c.queue_id, c.attempt + 1, CAST(:worker_id AS uuid),
       c.lease_epoch, 'claimed', c.claimed_at,
       (EXTRACT(EPOCH FROM c.claimed_at - c.run_at) * 1000)::int
  FROM claimed c
RETURNING job_id, organization_id, queue_id, attempt_number, lease_epoch;
