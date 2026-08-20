-- Step 1 of claim, run as its own statement. FOR NO KEY UPDATE only guarantees a
-- fresh re-check of THIS row for a transaction that blocked on it; it does NOT
-- give the rest of a single statement a fresh snapshot. If the headroom count and
-- the picked CTE ran in the SAME statement as this lock, a claimer that waited
-- behind another transaction's commit would still see the in-flight count as it
-- was when its statement STARTED -- stale, understating in-flight jobs, and the
-- concurrency cap overshoots. Splitting into two statements in one transaction
-- fixes it: step 2 is a new statement, and READ COMMITTED gives every new
-- statement a fresh snapshot, taken only once the lock (already held) makes that
-- snapshot safe to act on.
SELECT id, max_concurrency
  FROM queues
 WHERE id = :queue_id
   AND NOT is_paused
 FOR NO KEY UPDATE;
