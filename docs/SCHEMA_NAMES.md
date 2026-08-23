# Canonical Names

This file is the single source of truth for every name that appears in more than one place: the
database, `app/domain/enums.py`, the OpenAPI schema, and the React client. It was written **before**
the code it governs.

Every name drift that has to be reconciled late in a project like this — `status` vs `state`, `dead`
vs `dead_letter`, `run_at` vs `scheduled_at` vs `next_run_at`, `is_paused` vs `paused`, which
direction priority sorts, whether an id is a `uuid` or a `bigint` — is a symptom of not having this
file. Nothing here is negotiable in a pull request; changing a name here is a schema change.

`backend/app/domain/enums.py` is the executable copy. **Import from it. Never redefine a literal.**

---

## 1. `job_status` — native Postgres `ENUM`

The lifecycle state of a job row. Eight values, no others.

| Value | Meaning |
|---|---|
| `scheduled` | Not yet eligible. Waiting for `run_at`. Covers delayed jobs, cron occurrences, **and backoff retries**. |
| `queued` | Eligible **now**. This is the only status the claim query looks at. |
| `claimed` | A worker holds a lease but has not started the handler. `attempt` not yet incremented. |
| `running` | Handler is executing. `attempt` has been incremented. |
| `completed` | Terminal. Handler returned. |
| `failed` | Terminal. Non-retryable failure that is not routed to the DLQ. |
| `dead_letter` | Terminal. Attempt budget exhausted, or a `PermanentError`. A `dead_letter_entries` row exists. |
| `cancelled` | Terminal. Cancelled before or during execution. |

Terminal set: `completed`, `failed`, `dead_letter`, `cancelled`. Terminal statuses have **zero
out-edges** — see `docs/DATABASE.md`. Terminal statuses **require** `finished_at`; the
`ck_jobs_terminal_finished` CHECK aborts the transaction if it is omitted.

Not used anywhere: `pending`, `waiting`, `active`, `success`, `error`, `dead`, `retrying`.

## 2. `execution_status` — native Postgres `ENUM`

The outcome of one *attempt*, recorded on `job_executions`. Seven values.

| Value | Meaning |
|---|---|
| `claimed` | Row opened at claim time. Open (`finished_at IS NULL`). |
| `running` | Handler started. Open. |
| `succeeded` | Handler returned. Closed. |
| `failed` | Handler raised. Closed. |
| `timed_out` | Exceeded `jobs.timeout_ms`. Closed. Counts as a failed attempt. |
| `cancelled` | Cancelled mid-flight. Closed. |
| `lost` | The reaper closed an orphan whose worker never came back. Closed. |

`succeeded`, not `completed` — the job says `completed`, the attempt says `succeeded`, and the
difference is load-bearing: a job can complete after several attempts that did not succeed.

## 3. Timestamps — `run_at` is the only eligibility clock

| Column | Table | Meaning |
|---|---|---|
| `run_at` | `jobs` | **The single eligibility timestamp.** The promoter moves `scheduled → queued` when `run_at <= now()`. Backoff writes a future `run_at`. |
| `scheduled_for` | `jobs` | **Cron occurrence instant only.** The nominal time the recurrence rule fired. `NULL` unless `schedule_id` is set (`ck_jobs_schedule_occurrence` enforces the biconditional). Never used for eligibility. |
| `next_occurrence_at` | `job_schedules` | The next instant the dispatcher should materialise. Advanced from the nominal `scheduled_for`, never from `now()`. |
| `claimed_at` / `started_at` / `finished_at` | `jobs`, `job_executions` | Lifecycle instants. |
| `lease_expires_at` | `jobs` | When the reaper may reclaim. |
| `last_heartbeat_at` | `workers` | Denormalised liveness; the hot check. |

Not used anywhere: `scheduled_at`, `next_run_at`, `execute_at`, `eta`, `available_at`.

All timestamps are `timestamptz`. There are no naive timestamps in the schema, in Python, or on the
wire. **All time originates from `now()` inside Postgres** — no process compares its own clock to a
lease, so clock skew between nodes cannot cause a false reclaim.

## 4. Queue pause — `is_paused` **and** `paused_at`, always together

```sql
CONSTRAINT ck_queues_pause_consistency CHECK (is_paused = (paused_at IS NOT NULL))
```

Pause: `is_paused = true, paused_at = now()`. Resume: `is_paused = false, paused_at = NULL`.
Writing one without the other is rejected by the CHECK. There is no `paused`, `enabled`, or
`active` boolean.

Pause blocks **admission only**: in-flight work finishes, and the cron dispatcher does not
materialise occurrences into a paused queue.

## 5. Priority — smallint, `-100..100`, **HIGHER RUNS FIRST**

`ORDER BY priority DESC, run_at ASC, id ASC`. Default `0`.

Two distinct columns, and they are not interchangeable:

| Column | Meaning |
|---|---|
| `queues.priority` | **Inter-queue** weight. How often a worker polls this queue relative to its others. |
| `queues.default_priority` | The default `jobs.priority` for jobs *created on* this queue. |
| `jobs.priority` | **Intra-queue** ordering within the claim. |

The direction is stated in the API field description and in the OpenAPI schema, because
"priority 1" meaning "most important" is the other half of the industry and a silent disagreement
here inverts the whole queue.

## 6. Fencing — `lease_epoch`

`jobs.lease_epoch bigint NOT NULL DEFAULT 0`. Incremented on **every** ownership change: claim,
reclaim, retry, graceful release. Every write a worker makes carries the epoch it claimed under;
zero rows updated means the lease was stolen and the result is discarded.

- `lease_epoch` is **the fence**. Nothing else is.
- `lock_version` exists on `jobs` as a **reserved column for future ORM optimistic locking**. It is
  set to `0` at insert and **never incremented** — no trigger owns it (the schema has no triggers)
  and no statement bumps it. It is inert today, and it could **never** be the fence regardless.
- `lease_expires_at > now()` is **not** a fence — it has an ABA hole (see `docs/DESIGN_DECISIONS.md`,
  ADR-004).

## 7. `attempt` — monotonic, incremented at `claimed → running`

`jobs.attempt` starts at `0` and is incremented by exactly one in the guarded `start_job` statement.
It is **never** decremented — `start_job.sql` is the only statement that writes the column, and it
only ever adds one. Monotonicity is a property of having a single writer, not of a database
constraint: there is no trigger rejecting `NEW.attempt < OLD.attempt`, because there are no triggers
in this schema at all.

A job released from `claimed` provably never executed, so nothing needs decrementing. This is what
makes graceful shutdown free.

`job_executions.attempt_number` is the attempt this row represents — written at claim time as
`jobs.attempt + 1`, i.e. the attempt this claim is *about to* become.

## 8. Retry destination — `scheduled`, not `queued`

A failed attempt with budget remaining goes to `status = 'scheduled'` with a future `run_at`. The
claim predicate is `status = 'queued'` **only** — it has no `run_at` term, which is what keeps
`ix_jobs_claim` small. A queued job is by definition due.

## 9. Roles

Two roles: `owner`, `member`. Stored on `organization_members.role`. The
`owner/admin/operator/viewer` ladder is designed in `docs/DESIGN_DECISIONS.md` (ADR-014) and
deliberately not built.

## 10. Identifier type, per table

**UUIDv7, application-generated.** Time-ordered so inserts stay on the rightmost index page,
non-enumerable, and generable by the client before the round trip:

`organizations`, `users`, `projects`, `queues`, `retry_policies`, `jobs`, `job_schedules`,
`job_batches`, `workers`, `dead_letter_entries`.

**`bigint GENERATED ALWAYS AS IDENTITY`** — append-only, high-volume, never referenced externally,
and 8 bytes beats 16:

`job_executions`, `job_logs`, `worker_heartbeats`, `queue_stats_minute`.

**Natural / composite keys:**

| Table | Key |
|---|---|
| `organization_members` | `(organization_id, user_id)` |
| `refresh_tokens` | `jti` (uuid) |
| `system_state` | `name` (text: `promoter`, `cron`, `reaper`, `retention`) |

There is no `worker_queue_assignments` table and no `idempotency_keys` table. Queue subscription is a
worker CLI argument, and the job row is itself the idempotency record — see
[`DATABASE.md`](DATABASE.md) §3.

## 11. Wire vocabulary

| Concept | On the wire |
|---|---|
| Job lifecycle field | `status` (never `state`) |
| List payload | `{ "data": [...], "page": {...}, "meta": {...} }` (never `items`, never `results`) |
| Page cursor | `page.next_cursor`, opaque base64 of `(created_at, id)` |
| Error | `{ "error": { "code", "message", "details", "request_id" } }` |
| Correlation | `X-Request-ID` header ↔ `meta.request_id` ↔ `jobs.correlation_id` |
| Idempotency | `Idempotency-Key` request header |

Filter query parameter names match column names exactly: `status`, `kind`, `queue_id`, `handler`,
`created_after`, `created_before`, `run_after`, `run_before`. Unknown query parameters are a
`400 unknown_query_param`, so a typo'd filter fails loudly instead of silently returning everything.
