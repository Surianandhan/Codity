# Grader Walkthrough — about 10 minutes

> ## Read this first
>
> **Nothing executes without a running worker.**
>
> The API accepts jobs and returns `201`. Those jobs sit in `queued` and stay there until a `worker`
> process claims them. That is the design — the API never executes work — but if you POST a job with
> no worker running, the dashboard shows a job that never moves, and the system looks broken when it
> is behaving exactly as specified.
>
> Three backend processes, three terminals: **`make api`**, **`make worker`**, **`make scheduler`**.
> The dashboard is a fourth terminal and is optional — every step below can also be driven from
> Swagger.
>
> The `scheduler` matters too: without it, *immediate* jobs still flow perfectly while every
> *delayed* job, every *cron* occurrence, and **every backoff retry** silently stalls. That is the
> most confusing possible failure, which is why its staleness is reported at
> `GET /api/v1/system/status`.

Setup is in [`../README.md`](../README.md) — five commands, no Docker. This document assumes
`make setup && make migrate && make seed` has been run.

Each step names the rubric row it is evidence for.

---

### 1 · Start the system — 90s
> **System Architecture**

Three backend processes, plus the dashboard:

```bash
make api
```

```bash
make worker
```

```bash
make scheduler
```

```bash
cd frontend && npm install && npm run dev
```

Three independent process types, all stateless, all speaking only to Postgres — no broker, no Redis,
no shared memory, no RPC between them. Kill any one and the other two keep working. The topology and
why the scheduler is its own process rather than an API background task are in
[`ARCHITECTURE.md`](ARCHITECTURE.md).

---

### 2 · Enqueue a job from Swagger and watch it run — 60s
> **Backend Engineering · API Design**

Open <http://localhost:8000/docs>, `POST /api/v1/queues/{queue_id}/jobs`, `kind: "immediate"`,
`handler: "demo.echo"`.

One endpoint covers all five job kinds via a Pydantic discriminated union on `kind` — not five
near-duplicate endpoints each re-implementing validation, auth and idempotency.

The response is `201` with `status: "queued"`. Within a second the worker terminal logs the claim,
and `GET /jobs/{job_id}` returns `completed`. Note `attempt: 1` and `lease_epoch: 1`.

---

### 3 · Open the job timeline — 45s
> **Frontend & UX · Database Design**

<http://localhost:5173> → the job you just created.

The timeline is rendered from `job_executions`, one group per attempt. It exists because the
execution row is created at **claim** time rather than start time — which is what makes step 5
visible instead of invisible. Keep this tab open.

---

### 4 · Generate load — 60s
> **Reliability & Concurrency**

```bash
make demo
```

500 jobs across 3 queues with a handler that fails about 20% of the time. Watch the throughput chart
move and the queue depth drain on the project page, and watch failed jobs retry with **visibly
increasing gaps** — that is full-jitter exponential backoff, and the reason retries go to `scheduled`
rather than straight back to `queued` ([ADR-006](DESIGN_DECISIONS.md)). Some jobs exhaust their
attempts and land in the DLQ.

Every number on that page is aggregated from `jobs` and `job_executions` at request time — there is
no rollup table in front of them, so nothing on the dashboard can be stale in a way the source
tables are not. Empty minutes are gap-filled in SQL, so an outage renders as a **gap** in the line
rather than as a zero, which is the one reading the chart exists to make obvious.

`make demo` prints an invariant block at the end:

```
duplicate first-attempt executions = 0
terminal == enqueued
```

---

### 5 · The crash-recovery demo — 3 min
> **Reliability & Concurrency · System Architecture** — the centrepiece

Start a second worker:

```bash
make worker2
```

Re-run `make demo`, and while jobs are in flight, send `SIGKILL` to worker-1:

```bash
pkill -9 -f "worker.main --org .* --name worker-1"
```

No cleanup runs. No shutdown hook fires. Worker-1 simply stops existing, holding leases on in-flight
jobs.

Within one lease period, on the job timeline:

```
attempt 1 · claimed by worker-1 → lease expired (lost)
attempt 2 · claimed by worker-2 → running → completed
```

What happened, and why each piece is required:

| Mechanism | Without it |
|---|---|
| The reaper finds `lease_expires_at < now()` and requeues | Jobs held by a dead worker are lost forever |
| It **closes the orphaned execution row as `lost`** in the same statement | `ux_job_executions_open_one` makes the *next* claim raise `23505`, permanently — this very demo wedges the job |
| `lease_epoch` is bumped on reclaim | A merely-slow worker (not dead) could still commit a stale result later |
| `attempt` increments at `claimed → running`, not at claim | The attempt budget would be consumed by claims that never executed |

Then re-run the invariant check: every job reached a terminal state exactly once, and completions ==
enqueued − dead-lettered. **No job ran twice, and no job was lost.**

The fence is `lease_epoch`, not `lease_expires_at > now()` — the time-based version has an ABA hole
described in [ADR-004](DESIGN_DECISIONS.md).

---

### 6 · Pause a queue — 45s
> **Backend Engineering · Database Design**

On the project page, the queue table carries a **Pause** / **Resume** button per row. Click it on a
busy queue while `make demo` is still draining. Or open the queue itself — click its row — for the
**Pause admission** / **Resume admission** button in the queue screen's header. Either is
`POST /api/v1/queues/{id}/pause` and `/resume` underneath.

While paused with work still running, the queue screen's header shows a badge reading
**"paused — N still running"**, where N is the live in-flight count; once the last one finishes the
badge reads just "paused". That badge *is* the point of the step: claims stop immediately, but
**in-flight jobs finish**. The **config** tab on the same screen shows `is_paused`, `inflight`,
`headroom` and `max_concurrency` as the server computes them.

One thing to know before you deep-link: the queue screen is reached by clicking through from a
project, because there is no `GET /queues/{id}` for it to recover its project from a bare queue id
([`API.md`](API.md) §1). Pasting a `/queues/<uuid>` URL cold still works — it just omits the
back-link rather than faking one.

Pause blocks admission only, and the cron dispatcher will not materialise occurrences into a paused
queue — it increments `skipped_occurrences` instead. Otherwise a paused queue with a per-minute
schedule quietly accumulates a backlog that stampedes on resume. That behaviour is pinned by
`test_cron_does_not_materialise_into_a_paused_queue`.

`is_paused` and `paused_at` are written together, enforced by
`CHECK (is_paused = (paused_at IS NOT NULL))`. The database will not hold a half-paused queue.

---

### 7 · Replay from the dead letter queue — 45s
> **Backend Engineering · Frontend & UX**

Pick the **Dead letter** saved filter on the job explorer (`?status=dead_letter`), then **click a
row** to open the job and hit **Replay** there. Replay lives on the job detail screen, one job at a
time — the explorer lists dead-lettered jobs but carries no replay button and no multi-select, so
bulk replay is API-only today (`POST /api/v1/dlq/{entry_id}/replay`). The button is disabled unless
the job is terminal, and the server answers `409` for a non-terminal job regardless.

Replay inserts a **new** job with `replay_of_job_id` pointing at the original — it does not mutate
the terminal job. Terminal states have zero out-edges, so history stays immutable and the original
failure remains inspectable ([ADR-009](DESIGN_DECISIONS.md)). The replayed job should succeed even
if the original carried an `Idempotency-Key`, because `ux_jobs_live_idempotency` is scoped to *live*
statuses — worth trying by hand, since no test asserts it ([`TESTING.md`](TESTING.md) §3).

The DLQ is a saved filter on the job explorer rather than a sixth route — same data, one fewer screen
to maintain.

---

### 8 · Exact concurrency caps — 45s
> **Reliability & Concurrency · Database Design**

Use the **`bulk`** queue, which `scripts/seed.py` ships with `max_concurrency = 3` for exactly this
step — there is no `PATCH /queues/{id}`, so queue configuration is set at seed time or in `psql`,
not from the API. Push load at it and sample the in-flight count on the queue screen (or the
`inflight` field of `GET /api/v1/queues/{id}/stats`): it never exceeds 3, not "usually 3".

This is exact rather than approximate because the claim statement takes a **`FOR NO KEY UPDATE`**
lock on the `queues` row before computing headroom. The obvious alternative — count in flight, then
claim the difference — is check-then-act: K workers each read the same headroom and each spend it,
overshooting by `(K-1) × batch_size`. And it must be `FOR NO KEY UPDATE` rather than `FOR UPDATE`,
because `FOR UPDATE` conflicts with the `FOR KEY SHARE` lock every job insert takes on that same
queue row — which would block every enqueue behind every claim ([ADR-003](DESIGN_DECISIONS.md)).

---

### 9 · Cron under two schedulers — 60s
> **System Architecture · Database Design**

Create a `* * * * *` schedule, then start a **second** scheduler process. Watch the top of the
minute:

```sql
SELECT scheduled_for, count(*) FROM jobs WHERE schedule_id = '…' GROUP BY 1;
```

Every count is `1`. Two schedulers, one job per occurrence — guaranteed by
`UNIQUE (schedule_id, scheduled_for)`, not by leader election and not by an advisory lock. The
advisory lock in the scheduler only avoids redundant CPU; remove it and the system is still correct.

---

### 10 · Tests and static checks — 90s
> **Testing · Documentation**

```bash
make check
```

```bash
make test-concurrency
```

`make check` is ruff + mypy (strict) + the full suite. The concurrency tests run against real
committed sessions with one connection per simulated worker — `SKIP LOCKED` semantics do not exist
inside a single transaction, so a concurrency test sharing one session passes for the wrong reason.
Named tests and what each one protects: [`TESTING.md`](TESTING.md).

---

## Where to look next

| Question | Document |
|---|---|
| How do the processes fit together? | [`ARCHITECTURE.md`](ARCHITECTURE.md) |
| Why is the schema shaped like this? | [`DATABASE.md`](DATABASE.md) |
| Why this choice and not the obvious one? | [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) |
| What does the API promise? | [`API.md`](API.md) |
| What does each name mean? | [`SCHEMA_NAMES.md`](SCHEMA_NAMES.md) |
| What is tested, and what is deliberately not? | [`TESTING.md`](TESTING.md) |

The most concentrated evidence of engineering judgement is
[`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) — every entry names the option that was rejected and the
specific failure that rejecting it avoids. If you read one file, read that one.
