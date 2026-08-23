# Design Decisions

ADR-lite. Each entry is **Context / Options / Decision / Consequences / Revisit if**. They were
written as the decisions were made, not reconstructed afterwards, which is why several record the
alternative that was tried and rejected rather than only the winner.

Vocabulary: [`SCHEMA_NAMES.md`](SCHEMA_NAMES.md). Structure: [`DATABASE.md`](DATABASE.md). Topology:
[`ARCHITECTURE.md`](ARCHITECTURE.md).

| # | Decision | Chosen |
|---|---|---|
| [001](#adr-001--queue-substrate) | Queue substrate | Postgres + `SKIP LOCKED` |
| [002](#adr-002--the-claim-statement) | Claim | `FOR UPDATE SKIP LOCKED`, one statement |
| [003](#adr-003--exact-queue-concurrency-caps) | Concurrency cap | `FOR NO KEY UPDATE` on the queue row |
| [004](#adr-004--fencing-token) | Fencing | `lease_epoch` counter |
| [005](#adr-005--delivery-semantics) | Delivery | At-least-once, stated |
| [006](#adr-006--retry-destination) | Retry destination | `scheduled` + promoter |
| [007](#adr-007--where-attempt-is-incremented) | Attempt increment | At `claimed → running` |
| [008](#adr-008--full-jitter) | Jitter | Full jitter |
| [009](#adr-009--manual-retry-is-a-new-job) | Manual retry | Replay as a new job |
| [010](#adr-010--keyset-pagination) | Pagination | Keyset |
| [011](#adr-011--primary-key-types) | Primary keys | UUIDv7 + `bigint` children |
| [012](#adr-012--the-scheduler-is-its-own-process) | Scheduler | Separate process, N-safe by constraint |
| [013](#adr-013--tenant-isolation-without-rls) | Tenant isolation | Hand-filtered + composite FKs |
| [014](#adr-014--two-roles-not-four) | RBAC | Two roles |
| [015](#adr-015--polling-not-websockets) | Live updates | Polling |
| [016](#adr-016--inline-mermaid-diagrams) | Diagrams | Inline Mermaid |
| [017](#adr-017--read-committed) | Isolation level | `READ COMMITTED` |

---

## ADR-001 — Queue substrate

**Context.** The system needs a durable work queue with fair distribution across N workers,
visibility timeouts, priorities, and delayed delivery.

**Options.**
1. Redis + Celery / RQ.
2. RabbitMQ or SQS.
3. PostgreSQL as the queue, via `SELECT … FOR UPDATE SKIP LOCKED`.

**Decision.** PostgreSQL. Option 3.

**Consequences.**
- One infrastructure dependency instead of two. Nothing to install, nothing to keep in sync, one
  thing to back up.
- **Job state and job payload commit atomically.** With a broker, "job accepted" (Postgres) and "job
  enqueued" (broker) are two commits, and every dual-write failure mode follows: a job in the table
  that no worker will ever see, or a message referencing a row that was rolled back. Here there is
  one transaction.
- `SKIP LOCKED` — the mechanism the reliability requirements are actually about — sits at the centre
  of the design instead of being hidden inside a broker.
- Throughput ceiling is lower than a purpose-built broker's. At this scale that ceiling is nowhere
  near.
- Redis is not installed on the target machine anyway, so a broker-based design would have been
  undemonstrable.

**Revisit if.** Sustained enqueue rate exceeds roughly 5k/s per queue, or claim latency at p99
exceeds the poll interval with `queue_id` sharding (ADR-003) already applied.

---

## ADR-002 — The claim statement

**Context.** K workers poll the same queue. No two may ever execute the same job.

**Options.**
1. `SELECT … WHERE status='queued'`, then `UPDATE … SET status='claimed'` in a second statement.
2. `SELECT … FOR UPDATE` (blocking) in one statement.
3. `SELECT … FOR UPDATE SKIP LOCKED` in one statement, with the `UPDATE` and the `job_executions`
   insert in the same statement as CTEs.

**Decision.** Option 3. One statement, in `app/db/sql/claim_jobs.sql`.

**Why not option 1.** Under `READ COMMITTED`, workers A and B can both `SELECT` job J, both see
`status='queued'`, and both `UPDATE` it. The second `UPDATE` does not fail — it overwrites. Two
workers execute J, and nothing in the database notices. Taking the row lock *inside the same
statement* as the read is what removes the window.

**Why not option 2.** Plain `FOR UPDATE` makes worker B **block** on the row worker A is claiming,
then re-read it after A commits — B waits for work it provably cannot have, and claim latency across
the fleet serialises on the slowest claimer. `SKIP LOCKED` steps over locked rows and takes the next
available ones, so K workers partition the ready set with no coordination and no blocking.

**Consequences.**
- Claim, transition, and execution-row insert are one round trip and one transaction.
- The statement is raw SQL in a `.sql` file, so it can be pasted into `psql` and `EXPLAIN`ed.
- `COALESCE(max(n), 0)` guards the `LIMIT`: if the queue is paused or missing, the headroom CTE is
  empty, a bare scalar subquery yields `NULL`, and **`LIMIT NULL` in Postgres means no limit** — a
  paused queue would drain itself completely. `max()` over an empty set is `NULL`, coalesced to `0`,
  and `LIMIT 0` claims nothing.
- Evidence: `test_concurrent_claims_are_disjoint` — 100 jobs, 8 concurrent sessions, union of claimed
  ids is 100 and the pairwise intersection is empty.

**Revisit if.** The claim's `EXPLAIN` stops using `ix_jobs_claim`, or per-queue claim contention
shows up in `pg_stat_activity` as a wait bottleneck.

---

## ADR-003 — Exact queue concurrency caps

**Context.** `queues.max_concurrency` must cap how many jobs from a queue run at once, across the
whole fleet.

**Options.**
1. Count in-flight jobs, then claim up to the difference (no lock).
2. Maintain an `active_count` counter column, incremented at claim and decremented at completion.
3. A `queue_slots` table with one row per slot, claimed with `SKIP LOCKED`.
4. Lock the `queues` row with `FOR UPDATE`, compute headroom inside the claim statement.
5. Lock the `queues` row with **`FOR NO KEY UPDATE`**, compute headroom inside the claim statement.

**Decision.** Option 5.

**Why not option 1 — this is the one that looks fine and is not.** It is check-then-act. With
`max_concurrency=10` and 8 in flight, workers A and B each read `8` in their own snapshot, each
compute a budget of 2, and each claim 2 *disjoint* rows. `SKIP LOCKED` guarantees disjointness, which
is the wrong invariant here — twelve jobs now run against a cap of ten. With K workers the overshoot
is `(K-1) × batch_size`, and it grows exactly when the system is busiest.

**Why not options 2 and 3.** A counter drifts the moment a worker dies between decrementing and
committing, and drift needs a reconciliation sweep that itself needs testing. A slots table is an
extra table, an extra index, and an extra thing to leak.

**Why `FOR NO KEY UPDATE` and not `FOR UPDATE`.** This is the subtle half. Inserting a job takes a
**`FOR KEY SHARE`** lock on the referenced `queues` row, via the foreign key. `FOR UPDATE` conflicts
with `FOR KEY SHARE` — so every claim would block every enqueue on that queue for the duration of
the claim, turning a throughput optimisation into a producer stall that only appears under load.
`FOR NO KEY UPDATE` still conflicts **with itself**, so claimers remain mutually exclusive (the
invariant that matters), but it does **not** conflict with `FOR KEY SHARE`, so producers are never
blocked.

**Consequences.**
- The cap is **exact**, not approximate, and trivially testable.
- Claim throughput *per queue* is serialised — the honest cost. The lock is held for a single short
  statement; different queues never contend; horizontal scale comes from more queues plus a larger
  `batch_size`.
- Evidence: `test_claim_respects_max_concurrency` — cap 3, 12 concurrent claimers, 50 jobs,
  in-flight never exceeds 3 at any sample.

**Revisit if.** A single queue needs more claim throughput than one serialised statement provides.
The documented next step is hashing `queue_id` into N shard rows so claims spread across shards while
each shard keeps its exact cap.

---

## ADR-004 — Fencing token

**Context.** A worker that is merely *slow* — a GC pause, a `SIGSTOP`, a network partition — stops
heartbeating while its threads keep running. The reaper cannot distinguish this from death, reclaims
the job, and a second worker starts it. Both are now executing, and the first one is about to write
its result.

**Options.**
1. Trust `lease_expires_at > now()` in the completion predicate.
2. Use the existing `lock_version` optimistic-locking column.
3. A dedicated `lease_epoch bigint`, bumped on **every** ownership change.

**Decision.** Option 3.

**Why not option 1 — the ABA hole.** Worker W1 claims job J, stalls, is reaped, and then **re-claims
J itself** while its original zombie coroutine is still alive. The zombie's completion matches
`worker_id = W1` *and* a live `lease_expires_at`. It commits a stale result as the current attempt's
outcome, and every predicate passes. An epoch counter does not repeat, so the zombie's `lease_epoch`
can never match again.

**Why not option 2.** `lock_version` is trigger-owned and bumped on *every* update, heartbeats
included. A heartbeat from the legitimate owner would invalidate the owner's own completion. It can
serve ORM optimistic locking; it can never be the fence.

**Decision detail.** Every worker write carries the epoch it claimed under:

```sql
WHERE id = $1 AND worker_id = $2 AND lease_epoch = $3 AND status = 'running'
```

**Zero rows updated means the lease was stolen.** The worker discards its result, logs an
`abandoned` event, and does not retry the write.

**Consequences.**
- Stale writes are impossible at the row level, under any interleaving.
- **Residual risk, stated plainly: fencing protects the database row. It cannot un-send an email the
  zombie already sent.** That is the handler's contract — ADR-005.
- Evidence: `test_stolen_lease_write_is_fenced` and
  `test_zombie_after_same_worker_reclaim_rejected` (the ABA case specifically).

**Revisit if.** Never, for this mechanism. If lease ownership ever becomes shared (multiple runners
per job), the fence needs to become per-runner rather than per-job.

---

## ADR-005 — Delivery semantics

**Context.** What does the system promise a handler author?

**Options.**
1. Claim exactly-once semantics and hope.
2. Claim at-least-once, and give handlers a written contract.
3. Implement a two-phase commit protocol with handlers.

**Decision.** Option 2. **This system is at-least-once, not exactly-once.**

**Why.** Exactly-once *delivery* across a process boundary is impossible. The worker cannot
atomically commit "job done" to Postgres and "side effect performed" to a third-party API — they are
different systems with different transactions. Every protocol has a window where one succeeded and
the other did not. A system that claims exactly-once is a system whose author has not found the
window yet.

**What is actually guaranteed:**
- At most one worker holds a valid lease on a job at any instant.
- At most one execution can record a result for a given `lease_epoch`.
- A handler is invoked **at least** once, and may be invoked more than once under partition.

**The handler contract.** Handlers receive `job_id` and `attempt` on `JobContext` and **must be
idempotent**: key external writes on `job_id`, or make the operation naturally idempotent (upsert,
not insert). This is documented, and the seeded demo handlers are written this way.

Note that this is *execution-level* idempotency, and it is a different problem from the
`Idempotency-Key` header, which is *request-level* — it stops one HTTP retry creating two jobs. Both
exist; they solve different failures.

**Consequences.** Honest, testable guarantees. The failure matrix in ARCHITECTURE-adjacent docs lists
the residual risk for each failure rather than claiming none.

**Revisit if.** A use case genuinely requires effectively-once. The path is transactional outbox on
the handler's side, not a change here.

---

## ADR-006 — Retry destination

**Context.** A failed attempt with attempt budget remaining has to become eligible again, later.

**Options.**
1. `status = 'queued'` with a future `run_at`, and add `run_at <= now()` to the claim predicate.
2. `status = 'scheduled'` with a future `run_at`, promoted by the scheduler when due.

**Decision.** Option 2.

**Why not option 1 — it silently deletes backoff.** The claim query has **no `run_at` predicate** —
that is what keeps `ix_jobs_claim` narrow and its partial index small. A `queued` job is by
definition due. Put a backoff retry into `queued` and the very next poll claims it ~500ms later:
five attempts burn in about 20ms, the backoff table becomes decorative, and the system hammers the
dependency that is already down at maximum rate. The bug is invisible in a unit test and obvious in
production.

Adding `run_at <= now()` to the claim would fix it, at the cost of widening the hot index and making
every claim scan rows it cannot have.

**Consequences.**
- One promotion path for delayed jobs, cron occurrences, and retries — the same code, tested once.
- A dead promoter stalls all three. That is why `system_state.last_run_at` is on the dashboard
  (ARCHITECTURE §5); it is the system's most dangerous silent failure.
- Evidence: `test_retry_goes_to_scheduled_not_queued` — a failed attempt with budget left lands in
  `scheduled` with `finished_at` NULL and the fence advanced; a claim then returns nothing (the
  predicate is `status='queued'`, full stop), and with `run_at` pushed an hour out the promoter
  returns nothing either. Both halves are asserted, which is what makes it a test of the
  *destination* rather than of the wording of a status.

**Revisit if.** Promotion latency becomes the dominant term in end-to-end retry time. `LISTEN/NOTIFY`
would replace the promoter's poll.

---

## ADR-007 — Where `attempt` is incremented

**Context.** `attempt` drives backoff and the dead-letter decision, so where it increments changes
what a graceful shutdown has to do.

**Options.**
1. Increment at claim, and decrement when a claimed-but-unstarted job is released.
2. Increment at the `claimed → running` transition.

**Decision.** Option 2.

**Why.** A job released from `claimed` **provably never executed** — the handler is invoked only
after the guarded start returns a row. So there is nothing to decrement, graceful release costs
nothing, and `attempt` is **only ever incremented**, by exactly one statement, on exactly one edge.
Nothing in the schema enforces that monotonicity — there is no transition trigger and no CHECK on
`attempt`'s direction; it holds because no other statement writes the column. Option 1 needs a
compensating write on a shutdown path —
the one path least likely to run to completion, since it executes precisely when the process is
being killed.

**Consequences.**
- The start statement must be a guarded write **whose rowcount is checked**:

  ```sql
  UPDATE jobs SET status='running', started_at=now(), attempt=attempt+1, updated_at=now()
   WHERE id=$1 AND worker_id=$2 AND lease_epoch=$3 AND status='claimed'
  RETURNING id;
  ```

  If this returns zero rows the shutdown path already released the job, and the executor **must
  cancel the task before invoking the handler**. Skipping that check is a double-execution bug on
  every deploy: the release commits while the start is in flight, another worker claims the
  now-`queued` job immediately, and the original handler runs to completion anyway. No lease expiry,
  no partition, no slow worker involved — just a rolling restart.
- Evidence: `test_sigterm_releases_unstarted_claims`, `test_start_rowcount_zero_means_do_not_run`.

**Revisit if.** Never; the alternative is strictly worse.

---

## ADR-008 — Full jitter

**Context.** When a shared dependency fails, every in-flight job fails at nearly the same instant and
schedules its retry from nearly the same instant.

**Options.**
1. Deterministic backoff, no jitter.
2. Narrow jitter, e.g. ±10%.
3. Full jitter: `delay = random() × capped_delay`.

**Decision.** Option 3, applied to `fixed` (`base`), `linear` (`base × n`), and `exponential`
(`base × 2^(n-1)`), each capped at `backoff_max_ms` first.

**Why.** Deterministic backoff reproduces the stampede at every tier — the thundering herd arrives
again at t+1s, t+2s, t+4s, each time in lockstep, each time knocking over a dependency that was
halfway up. Narrow jitter merely smears the spike; the herd is still a herd. Full jitter spreads
retries uniformly across the whole window, which is what actually lets a recovering dependency come
back.

**Consequences.**
- Retry timing is non-deterministic, so tests assert **bounds**, not equality:
  `test_backoff_sequence_per_strategy` checks each delay lies within `(0, capped]` and that the cap
  holds.
- Mean delay is half the deterministic value, which is the intended trade: the tail matters, the mean
  does not.

**Revisit if.** A queue needs a strict minimum inter-attempt gap (rate-limited third-party APIs).
Decorrelated jitter with a floor would be the replacement.

---

## ADR-009 — Manual retry is a new job

**Context.** "Retry failed jobs" is a core requirement, and the DLQ needs a replay action.

**Options.**
1. Mutate the terminal job back to `queued` and reset its counters.
2. Insert a new job with `replay_of_job_id` pointing at the original.

**Decision.** Option 2.

**Why.** Option 1 gives terminal states out-edges — `dead_letter → queued` and `completed → queued`
would have to become legal — and once those edges exist, nothing distinguishes a deliberate replay
from a bug that resurrects a completed job. That matters more here than it would elsewhere, because
the state machine is enforced only by each statement's `WHERE` clause naming its source status
([`DATABASE.md`](DATABASE.md) §5): there is no trigger holding the diagram, so an edge that becomes
writable is an edge anything can take. It also destroys history: the
attempt history, the error, the timings and the DLQ record are all overwritten by the replay, so the
one artefact an operator needs to understand *why* it failed is gone.

**Consequences.**
- Terminal states have **zero out-edges**, so the diagram in [`DATABASE.md`](DATABASE.md) §5 stays
  small enough to read in one sitting.
- The UI timeline can show the original and the replay as separate, linked runs.
- `ux_jobs_live_idempotency` is scoped to *live* statuses, so replaying a dead-lettered job that
  carried an `Idempotency-Key` does not collide with its own ancestor. **This is not covered by a
  test** — see [`TESTING.md`](TESTING.md) §3. The index is right; the assertion is missing.
- Job count grows with replays. That is what a history table is for.

**Revisit if.** Never.

---

## ADR-010 — Keyset pagination

**Context.** The job explorer pages through a table that is being inserted into constantly.

**Options.**
1. `LIMIT`/`OFFSET` with a `total` count.
2. Keyset on the `(created_at, id)` tuple, cursor encoded base64.

**Decision.** Option 2.

**Why.** `jobs` is insert-heavy by design. With offset pages, rows inserted between page 1 and page 2
shift the window: the reader **skips** rows and **sees duplicates**, and neither is visible as an
error. Deep offsets also make the database scan and discard everything it skips. Keyset reads from
the cursor forward, so a concurrent insert cannot displace a page.

**Consequences.**
- `total` is deliberately absent from cursor pages — an exact count over a live table costs a scan
  and is stale before it is rendered. Counts come from `GET /queues/{id}/stats`, which reads the
  partial `ix_jobs_depth`.
- The cursor tuple must match index order exactly, which is why `ix_jobs_project_created` and
  `ix_jobs_queue_status_created` are `(… created_at DESC, id DESC)`.
- No page-number UI. Infinite scroll / "load more" instead.
- **No test covers this.** A cursor walked to exhaustion under concurrent inserts returning each
  job exactly once is the property that matters, and it is unasserted — see
  [`TESTING.md`](TESTING.md) §3.

**Revisit if.** A user-facing requirement genuinely needs jump-to-page.

---

## ADR-011 — Primary key types

**Context.** Identifier choice affects index locality, enumerability, and payload size on the
fastest-growing tables.

**Options.**
1. `bigint` identity everywhere.
2. UUIDv4 everywhere.
3. UUIDv7 for entities, `bigint` identity for append-only children.

**Decision.** Option 3.

**Why.** UUIDv4 is random, so every insert dirties a different index leaf — write amplification and
poor cache locality on exactly the table under the most insert pressure. UUIDv7 is time-ordered, so
inserts stay on the rightmost page, while keeping the properties that matter externally:
non-enumerable (a `bigint` job id leaks volume and lets a client walk the id space) and
client-generable before the round trip. For `job_executions`, `job_logs`, `worker_heartbeats` and
`queue_stats_minute` none of that applies — they are never referenced from outside — so 8 bytes
beats 16 on the rows there are most of.

**Consequences.**
- Ids are generated in the application (`uuid-utils`), not by the database, so a client can construct
  a job id before it POSTs.
- Mixed id types across tables. The per-table mapping is pinned in
  [`SCHEMA_NAMES.md` §10](SCHEMA_NAMES.md) so it is never a guess.

**Revisit if.** Postgres ships a native `uuidv7()` — then generation can move server-side, though
client-generability would be lost.

---

## ADR-012 — The scheduler is its own process

**Context.** Something must promote due jobs, dispatch cron occurrences, reap expired leases, sweep
dead workers, and enforce retention.

**Options.**
1. FastAPI background task inside the API process.
2. A leader-elected role inside the worker fleet.
3. A separate `scheduler` process, made safe under N replicas by database constraints.

**Decision.** Option 3.

**Why not option 1.** It ties job liveness to HTTP traffic — an idle API stops promoting delayed
jobs and stops reaping leases, so the system degrades exactly when nobody is watching. And it
double-fires on every API replica, so scaling the API multiplies the scheduler.

**Why not option 2.** Leader election is a distributed-systems problem this system does not need to
solve, with its own failure modes (split brain, fencing of the *leader*, lease renewal) that are
strictly harder than the problem being solved.

**Why option 3 is safe under N.** Correctness comes from the database, not from singleton-ness:
- Cron double-promotion is impossible — `ux_jobs_schedule_occurrence` on `(schedule_id,
  scheduled_for)`.
- Reaper and promoter overlap is harmless — every statement is `FOR UPDATE SKIP LOCKED`, so two
  schedulers partition the work rather than collide.

`pg_try_advisory_lock` is used **only** to stop redundant ticks burning CPU. If it is never acquired,
the system is still correct — which is the test of whether a lock is load-bearing.

**Consequences.**
- A third process to run and to document. The README says loudly that **nothing executes without a
  worker**, and the scheduler's staleness is on the dashboard via `system_state`.
- Evidence: `test_cron_two_schedulers_one_job_per_occurrence`,
  `test_expired_lease_reclaimed_once`.
- **Lock ordering is fixed at `jobs → workers`**, and the dead-worker sweep runs in its **own
  transaction**, separate from lease reclaim. Combining them — holding `jobs` locks while taking
  `workers` locks — deadlocks against a heartbeat that took `workers` first and then blocked on
  `jobs`. The deadlock victim is preferentially the lagging worker, which manufactures the very
  lease expiry the design is trying to avoid.

**Revisit if.** Tick frequency needs to exceed what polling can serve; `LISTEN/NOTIFY` is the seam.

---

## ADR-013 — Tenant isolation without RLS

**Context.** Multi-tenancy is a hard requirement. A cross-tenant read is the worst bug this system
could ship.

**Options.**
1. Filter by `organization_id` in each query by hand, over composite FKs that make cross-tenant rows
   unrepresentable.
2. Option 1 **plus** a `TenantSession` that injects the predicate automatically via SQLAlchemy's
   `with_loader_criteria`.
3. Option 2 **plus** Postgres Row-Level Security.

**Decision — and what actually shipped is option 1, not option 2.** This ADR originally recorded
option 2 as the decision. It is worth correcting in place rather than quietly, because the gap
between the two is the difference between a control and a convention.

**What is real.** The structural half, and it is the stronger half:

- `jobs` declares `UNIQUE (id, organization_id)` (`uq_jobs_id_organization_id`).
- `job_executions` references **`(job_id, organization_id)`** rather than `job_id` alone
  (`fk_job_executions_job`).

A child row whose tenant disagrees with its parent's is therefore **not representable**. The
database rejects it regardless of what the application does, and this holds for the raw-SQL hot path
exactly as it holds for the ORM. A forged cross-tenant row is impossible, not merely unlikely.

**What is not real: the loader hook.** `TenantSession` and `with_loader_criteria` are **zero hits in
the codebase**. Every tenant-scoped read filters by hand — each router writes
`.where(… .organization_id == principal.organization_id)` explicitly — which is precisely the option
this ADR once dismissed as "a policy, not a control". That dismissal was correct, and the honest
position is that the query half of this design is a convention that relies on nobody forgetting, on
a codebase that will grow.

**Why it matters, stated exactly.** The composite FKs stop a cross-tenant row being *written*. They
do nothing to stop a router that omits its predicate from *reading* across tenants, and nothing
would catch it: there is no loader hook, no RLS, and no test asserting that every tenant-scoped
query emits an `organization_id` predicate. For a system whose worst possible bug is a cross-tenant
read, that is the weakest link in the design, and it is one file of SQLAlchemy event wiring away
from being closed. The loader hook is the next thing to build here, ahead of RLS.

**Why not option 3 — a real trade-off, not an oversight.** RLS is a *second* enforcement layer, and
adding it before the *first* behavioural layer exists gets the order wrong. It also costs roughly a
day, and every fixture, migration or admin script that forgets to `SET` the GUC turns into an opaque
"my test returns zero rows and no error" debugging session. Spending that day on the reliability
core was the better trade; spending the next hour on the loader hook is a better trade still.

**The documented hardening step**, so this is a decision with a migration path rather than a gap:

```sql
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;

CREATE POLICY jobs_tenant_isolation ON jobs
    USING      (organization_id = current_setting('app.current_org', true)::uuid)
    WITH CHECK (organization_id = current_setting('app.current_org', true)::uuid);

-- Set once per checked-out connection, inside the transaction:
SELECT set_config('app.current_org', $1, true);
```

Repeat per tenant-scoped table. The `true` third argument to `current_setting` returns `NULL` rather
than raising when the GUC is unset, so an unset connection sees zero rows instead of an error —
which is exactly the opaque failure described above, and why the rollout needs a session-level
assertion that the GUC is set.

**Consequences.** Enforcement is asymmetric, and the asymmetry should be understood rather than
averaged: **structurally strong** (the FK graph, which no code path can bypass) and
**behaviourally weak** (hand-written predicates, unenforced and untested). A raw-SQL statement that
bypasses the ORM must carry its own `organization_id` predicate — the hot-path `.sql` files do, and
they are reviewed as SQL for exactly this reason. The same is spelled out from the schema's side in
[`DATABASE.md`](DATABASE.md) §8.

**Revisit if.** A compliance requirement demands defence in depth at the database layer, or raw-SQL
surface grows beyond the reviewed hot path.

---

## ADR-014 — Two roles, not four

**Context.** The spec lists RBAC as a **bonus**.

**Options.** Two roles (`owner`, `member`); or the full `owner/admin/operator/viewer` ladder.

**Decision.** Two roles, via a single `require_member` dependency on mutating routes. Reads require
authentication plus org membership.

**Why.** A four-tier ladder puts a *bonus* on the critical path of every endpoint: every route needs
a permission decision, every route needs a test per tier, and the matrix is 4 × routes. That is
budget taken directly from the reliability core.

**The deferred design**, ready to implement: `viewer` = read-only; `operator` = viewer + pause/resume,
cancel, replay; `admin` = operator + queue and schedule CRUD, member invite; `owner` = admin +
billing, member removal, org delete. It slots into the same dependency as a `require_role(min_role)`
comparison against an ordered enum — no schema change beyond widening the `role` CHECK.

**Consequences.**
- **Cross-tenant access returns 404, not 403.** A 403 confirms the resource exists, which is an
  enumeration oracle across tenants. This is how the routers behave; **no test asserts it**
  ([`TESTING.md`](TESTING.md) §3).
- There is **no last-owner guard** — no constraint trigger, no application check. Nothing can
  currently demote or remove the last owner because the membership mutation endpoints are not built
  ([`API.md`](API.md) §1), so the hole is closed by absence rather than by a control. The guard
  lands with the endpoint that needs it, and it has to land in the same change.

**Revisit if.** Real multi-user orgs need finer separation of duty.

---

## ADR-015 — Polling, not WebSockets

**Context.** The dashboard must show job progress that changes second by second.

**Options.** WebSockets / SSE push; or TanStack Query polling with per-view intervals.

**Decision.** Polling.

**Why.** WebSockets are listed as a **bonus** in the spec. The connection lifecycle — reconnect with
backoff, auth on upgrade, resubscribe after reconnect, server-side fanout, and the "did I miss an
event while disconnected" reconciliation — is a substantial amount of code whose failure modes are
subtle and whose marks are zero. Polling is correct by construction: every poll is a fresh read of
the truth, and a missed poll self-heals.

**The cost is bounded and stated:**

| Query | Interval |
|---|---|
| Job detail (non-terminal) | 2s — **stops on terminal status** |
| Job list | 5s |
| Queue health | 10s |
| Workers | 10s |
| Throughput chart | 30s |
| Anything on a hidden tab | paused |

Terminal-stop and hidden-tab-pause are what keep an open dashboard from being a load generator.

**Consequences.** Up to one interval of staleness. The polling hooks are the seam: swapping the
transport later touches the hooks and nothing else.

**Revisit if.** Interval load becomes material, or sub-second latency is required.

---

## ADR-016 — Inline Mermaid diagrams

**Context.** The architecture diagram and ER diagram are required deliverables.

**Options.** Exported images; separate `.mmd` files plus a drift checker; inline fenced Mermaid in
the Markdown.

**Decision.** Inline fenced Mermaid.

**Why.** GitHub renders it natively, so a reviewer sees the picture without cloning. It is **plain
text in the diff**, so a schema change and its diagram change are visible in the same review. A
parallel `.mmd` file is a second copy of the same truth, which means it can drift, which means
building a drift checker — a tool to protect against a problem created by the file layout.

**Consequences.** No binary assets in the repo. The diagram cannot be styled beyond what Mermaid
offers, which is fine.

**Revisit if.** A diagram outgrows Mermaid's expressiveness.

---

## ADR-017 — `READ COMMITTED`

**Context.** Postgres offers `READ COMMITTED`, `REPEATABLE READ`, and `SERIALIZABLE`.

**Options.** Raise the isolation level for the claim path, or keep the default.

**Decision.** Keep `READ COMMITTED`, the default, everywhere.

**Why.** The invariant the claim needs is **per-row mutual exclusion**, and `FOR UPDATE SKIP LOCKED`
provides exactly that regardless of isolation level. `REPEATABLE READ` would add serialisation
failures (`40001`) on the hottest statement in the system, which means a retry loop, which means
retry-loop bugs — for no additional safety, because the row lock already does the work.

The corollary is that `READ COMMITTED` is *not* sufficient for a `SELECT`-then-`UPDATE` claim
(ADR-002); the mitigation is the single-statement lock, not a higher isolation level.

**Consequences.** No serialisation-failure retry logic anywhere in the codebase. Any future
read-modify-write that spans statements must take an explicit row lock, and the reviewer's question
for such a patch is "where is the `FOR UPDATE`".

**Revisit if.** A new cross-row invariant appears that a row lock cannot express.
