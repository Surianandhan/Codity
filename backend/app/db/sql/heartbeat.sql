-- One statement per heartbeat tick: renew every lease this worker holds, refresh the
-- worker row, append a heartbeat history row, and report back both control flags.
--
-- Params: :worker_id
--
-- Called every lease_seconds / 6, so it is on the hot path and must stay ONE round
-- trip. The RETURNING clause is why: jobs.cancel_requested and workers.drain_requested
-- both ride the heartbeat the worker already issues. POST /jobs/{id}/cancel and
-- POST /workers/{id}/drain set flags in the database; without them being returned here
-- the worker would need a second query per tick to notice, and both endpoints would be
-- API fields that do nothing -- worse than not offering them at all.
--
-- LOCK ORDER IS jobs -> workers, ALWAYS. That is not decoration: the reaper takes jobs
-- locks, and a heartbeat that took workers first and then blocked on jobs deadlocks
-- against it. Postgres would pick a victim, and the victim it prefers is the lagging
-- worker -- which manufactures the very lease expiry the heartbeat exists to prevent.
-- The order is enforced structurally, not by comment: the workers UPDATE reads FROM
-- `counted`, which reads FROM `held`, so its input cannot be produced until the jobs
-- UPDATE has run to completion.
--
-- Renewal is fenced by worker_id alone and needs no epoch: the reaper NULLs worker_id
-- when it takes a job away, so a lease this worker no longer owns is not matched and
-- silently stops being renewed. A worker that has been reaped learns of it from
-- start_job/complete_job/fail_job returning zero rows.
WITH held AS (
    UPDATE jobs j
       SET lease_expires_at = now() + make_interval(secs => j.lease_seconds),
           updated_at       = now()
     WHERE j.worker_id = CAST(:worker_id AS uuid)
       AND j.status IN ('claimed', 'running')
 RETURNING j.id, j.lease_epoch, j.lease_expires_at, j.cancel_requested
),
counted AS (
    SELECT count(*)::int AS inflight FROM held
),
touched AS (
    UPDATE workers wk
       SET last_heartbeat_at = now(),
           -- A worker the dead-worker sweep gave up on but which is in fact alive
           -- revives itself here rather than staying a corpse on the dashboard.
           -- 'stopped' is deliberate and is not overwritten.
           status     = CASE WHEN wk.status = 'dead' THEN 'active' ELSE wk.status END,
           updated_at = now()
      FROM counted c
     WHERE wk.id = CAST(:worker_id AS uuid)
 RETURNING wk.id, wk.drain_requested, wk.status
),
beat AS (
    -- Append-only history. The denormalised workers.last_heartbeat_at above serves the
    -- hot liveness check; this table is what gives the Workers screen a real trace and
    -- gives the reaper tests evidence of exactly when a worker went quiet.
    --
    -- It selects FROM touched, not from :worker_id directly, and that is deliberate:
    -- worker_heartbeats.worker_id is an FK, so an unconditional INSERT for a worker row
    -- that no longer exists (retention swept it, or the org was deleted) raises 23503
    -- and ABORTS the caller's whole transaction -- on every subsequent tick, forever.
    -- Driving it from the UPDATE's output makes that case return zero rows instead, and
    -- the wrapper turns zero rows into "re-register".
    INSERT INTO worker_heartbeats (worker_id, beat_at, inflight)
    SELECT t.id, now(), c.inflight
      FROM touched t
     CROSS JOIN counted c
 RETURNING id
)
-- Anchored on the worker row, LEFT JOINed to the leases: a worker holding nothing must
-- still get exactly one row back, because an idle worker is precisely the one that
-- needs to see drain_requested.
SELECT t.id               AS worker_id,
       t.drain_requested  AS drain_requested,
       t.status           AS worker_status,
       c.inflight         AS inflight,
       h.id               AS job_id,
       h.lease_epoch      AS lease_epoch,
       h.lease_expires_at AS lease_expires_at,
       h.cancel_requested AS cancel_requested
  FROM touched t
 CROSS JOIN counted c
  LEFT JOIN held h ON true;
