# API

Base path `/api/v1`. JSON in, JSON out, `application/json` throughout.

Interactive docs are generated from the code and served by the running API:

- Swagger UI — <http://localhost:8000/docs>
- OpenAPI schema — <http://localhost:8000/api/v1/openapi.json>

**`/docs` is the authoritative list of what is live in your checkout.** This file is the contract:
the shapes, the guarantees, and the reasoning. Field and value names are pinned in
[`SCHEMA_NAMES.md`](SCHEMA_NAMES.md) — in particular `status` (never `state`), `data` (never
`items`), `run_at`, `is_paused`, and priority sorting **higher first**.

---

## 1. Endpoints

### Auth

| Method | Path | Purpose |
|---|---|---|
| POST | `/auth/register` | Create user + organization, returns tokens |
| POST | `/auth/login` | Access + refresh token, both in the JSON body |
| GET | `/auth/me` | Current principal |

Those three are the whole auth surface. **There is no `/auth/refresh` and no `/auth/logout`** — see
§2 and the deferred list in [`../README.md`](../README.md).

### Organizations and projects

| Method | Path | Purpose |
|---|---|---|
| GET / POST | `/orgs/{org_id}/projects` | List and create projects |

That is the entire org/project surface. There is no route on `/orgs/{org_id}` itself, no membership
endpoint, no retry-policy endpoint, and no `GET`/`PATCH`/`DELETE` on a single project. The
`organizations`, `organization_members` and `retry_policies` tables exist and are populated by
`scripts/seed.py`; the CRUD over them is deferred, not hidden — see [`../README.md`](../README.md).

### Queues

| Method | Path | Purpose |
|---|---|---|
| GET / POST | `/projects/{project_id}/queues` | List and create queues in a project |
| POST | `/queues/{queue_id}/pause` | Sets `is_paused=true, paused_at=now()` |
| POST | `/queues/{queue_id}/resume` | Sets `is_paused=false, paused_at=NULL` |
| GET | `/queues/{queue_id}/stats` | Configuration, depth by live status, window counters |

**There is no `GET /queues/{queue_id}`.** `/stats` is the single-queue read: it returns the queue's
configuration alongside its depth, and the dashboard's queue screen is built on it. The consequence
is visible in the UI — a queue's project is not recoverable from the queue id alone, so the
dashboard hands `project_id` over in router state on the way in and omits the back-link on a cold
deep link rather than faking it. There is no `PATCH` or `DELETE` on a queue either; pause and resume
are the only mutations.

Pause blocks **admission only**. In-flight jobs finish, and the cron dispatcher does not materialise
occurrences into a paused queue (it increments `skipped_occurrences` instead) — otherwise a paused
queue with a per-minute schedule silently accumulates a backlog that stampedes on resume. The UI
says "paused — 4 still running".

### Jobs

| Method | Path | Purpose |
|---|---|---|
| POST | `/queues/{queue_id}/jobs` | Create a job — all five kinds |
| GET | `/projects/{project_id}/jobs` | Job explorer: filter, sort, keyset page |
| GET | `/queues/{queue_id}/jobs` | Same, scoped to one queue |
| GET | `/jobs/{job_id}` | Job detail |
| POST | `/jobs/{job_id}/cancel` | Sets `cancel_requested`, or cancels outright if not started |
| POST | `/jobs/{job_id}/replay` | `201` + a **new** job with `replay_of_job_id` set |
| GET | `/jobs/{job_id}/executions` | Attempt history |
| GET | `/jobs/{job_id}/logs` | Execution logs, ascending keyset |

### Scheduling and batches

| Method | Path | Purpose |
|---|---|---|
| GET / POST | `/queues/{queue_id}/schedules` | Cron schedules |
| GET / PATCH / DELETE | `/schedules/{schedule_id}` | Schedule (delete is soft) |
| GET | `/batches/{batch_id}` | Batch aggregate progress |

### Fleet, DLQ, metrics

| Method | Path | Purpose |
|---|---|---|
| GET | `/orgs/{org_id}/workers` | Worker fleet + liveness |
| GET | `/workers/{worker_id}` | Worker detail |
| GET | `/workers/{worker_id}/heartbeats` | Heartbeat history, keyset paged |
| POST | `/workers/{worker_id}/drain` | Sets `drain_requested` |
| GET | `/projects/{project_id}/dlq` | Dead letter entries |
| POST | `/dlq/{entry_id}/replay` | Replay + resolve the entry |
| POST | `/dlq/{entry_id}/discard` | Resolve without replay |
| GET | `/projects/{project_id}/metrics/summary` | Stat cards |
| GET | `/projects/{project_id}/metrics/throughput` | Time series (`queue_id`, `window`, `bucket`) |
| GET | `/system/status` | Fleet health + scheduler staleness |

### Unversioned health

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | Process alive. No I/O. |
| GET | `/readyz` | Database reachable — it opens a connection and runs `select 1`. It does **not** check that migrations are current; a schema-version probe is not implemented. |

The DLQ has no dedicated *screen* in the dashboard — it is a saved filter on the job explorer
(`?status=dead_letter`). Replay from the UI is **one job at a time, from the job detail screen**:
the explorer lists dead-lettered jobs but carries no replay button and no multi-select, so bulk
replay is an API-only operation today. The DLQ does have its own endpoints, because the operator
workflow (resolution, who replayed, error fingerprint grouping) is real data that does not live on
`jobs`.

---

## 2. Authentication

JWT bearer, `Authorization: Bearer <access_token>`.

| Token | TTL | Storage |
|---|---|---|
| Access | 15 minutes | Client memory; sent as a bearer header |
| Refresh | 30 days | Returned in the login/register JSON body; the client stores it |

- Passwords are hashed with **Argon2id**, and rehashed on successful login when the parameters have
  since been strengthened.
- `users.token_version` is claimed as `ver` in the access token and **re-checked against the
  database on every request**, alongside org membership — so a bumped version invalidates every
  previously issued access token without a per-token blocklist. The check is live; there is not yet
  a password-change endpoint that bumps it.

**Refresh-token rotation is not implemented.** This is the one place where the schema is ahead of
the code, and it is worth stating exactly rather than leaving it to be discovered:

- `refresh_tokens` is created on login with a `jti`, and the column `replaced_by_jti` exists on the
  table for rotation to write into. **Nothing ever writes it**, because there is no endpoint that
  rotates.
- There is therefore **no reuse detection** and no chain revocation.
- There is **no httpOnly cookie anywhere in the codebase** — `set_cookie` is never called. The
  refresh token travels in the JSON body like the access token.
- With no `/auth/refresh`, an expired access token means logging in again; with no `/auth/logout`,
  a refresh token is only invalidated by its own expiry.

The intended design — rotate on every use, record `replaced_by_jti`, treat presentation of an
already-rotated token as theft and revoke the whole chain — is recorded in the deferred list in
[`../README.md`](../README.md). It is a design, not shipped behaviour.

**Authorization.** Two roles, `owner` and `member` ([ADR-014](DESIGN_DECISIONS.md)). Mutating routes
require membership via a single `require_member` dependency; reads require authentication plus org
membership.

**Cross-tenant requests return `404`, not `403`.** A 403 confirms the resource exists, which turns
the id space into an enumeration oracle across tenants. To a caller from the wrong org, the resource
simply does not exist.

---

## 3. Creating a job

One endpoint for all five kinds, a Pydantic **discriminated union on `kind`**. Five endpoints would
duplicate validation, auth, and idempotency five times, and the dashboard's single create dialog maps
to one schema.

| `kind` | Extra fields | Created as |
|---|---|---|
| `immediate` | — | `queued`, `run_at = now()` |
| `delayed` | `delay_ms` (> 0) | `scheduled`, `run_at = now() + delay_ms` |
| `scheduled` | `run_at` (aware datetime) | `scheduled` |
| `recurring` | `cron`, `timezone`, `catchup_policy` | `scheduled` + a `job_schedules` row |
| `batch` | `items` (1–1000) | `queued` children + a `job_batches` row |

Common fields: `handler`, `payload`, `priority` (−100..100, **higher runs first**), `max_attempts`,
`timeout_ms`, `backoff_strategy`, `idempotency_key`. Anything omitted is taken from the queue's
configuration and **snapshotted onto the job row**, so later edits to the queue or its retry policy
never retroactively change a job already mid-retry.

```bash
curl -sS -X POST http://localhost:8000/api/v1/queues/$QUEUE_ID/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Idempotency-Key: order-4417-confirm" \
  -H 'Content-Type: application/json' \
  -d '{"kind":"immediate","handler":"demo.echo","payload":{"msg":"hello"},"priority":10}'
```

```json
{
  "id": "0192f3c1-8b2a-7c31-9f4e-2b6d5a1c0e77",
  "queue_id": "0192f3c1-1111-7000-8000-000000000001",
  "project_id": "0192f3c1-0000-7000-8000-000000000001",
  "kind": "immediate",
  "handler": "demo.echo",
  "status": "queued",
  "priority": 10,
  "run_at": "2026-08-20T09:14:02.117Z",
  "payload": { "msg": "hello" },
  "attempt": 0,
  "max_attempts": 3,
  "worker_id": null,
  "lease_epoch": 0,
  "claimed_at": null,
  "started_at": null,
  "finished_at": null,
  "last_error_class": null,
  "last_error_message": null,
  "created_at": "2026-08-20T09:14:02.117Z"
}
```

`201 Created`. The job is `queued`, not running — **a queued job stays queued forever unless a worker
process is running.** See the README.

There is **no `fail_fast` field on batches.** It cannot be honoured without a per-batch check on the
hot claim path, and a field that validates but does nothing is worse than an absent one.

**Payload limits.** Pydantic enforces `queues.max_payload_bytes` (default 64 KiB, per-queue
override), with a 1 MiB CHECK on `jobs.payload` as the database backstop.

---

## 4. Idempotency

`Idempotency-Key` is accepted on `POST /queues/{queue_id}/jobs`.

**There is no `idempotency_keys` table. The job row *is* the idempotency record.** That is the whole
mechanism, and it is worth stating precisely because the usual design stores a key, a body hash and
a serialised response in a table of its own:

- **Uniqueness** is the partial index `ux_jobs_live_idempotency` — `UNIQUE (queue_id,
  idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN (live statuses)`.
- **Atomicity is free**, and this is the part the separate table exists to buy. The key and the job
  commit together because they are the *same row*. There is no window in which a key is reserved but
  the job is not, so there is no crash to survive between two writes.
- **The request fingerprint is recomputed, not stored.** A replay is compared against a fingerprint
  derived from the persisted job — effective `kind`, `handler`, `payload`, `priority`,
  `max_attempts`, `timeout_ms`, `backoff_strategy` — rather than against a hash written at insert
  time. Effective values are used so that omitting a field that defaults to the same value does not
  read as a different body.
- **No response is stored.** A replay re-serialises the original job row, which is what the original
  `201` contained anyway.
- A transaction-scoped `pg_try_advisory_xact_lock` on `(queue_id, key)` serialises duplicate
  requests across every API replica, which is what turns the in-flight case into an immediate `409`
  instead of a request blocked on the unique index until the first transaction commits.

**The one cost of recomputation, stated rather than hidden:** a `delayed` job stores
`run_at = created_at + delay_ms` rather than `delay_ms` itself, and `created_at` is a server clock
while `run_at` is an application clock — so the delay is not exactly reconstructible. Timing is
therefore excluded from the fingerprint for `delayed`, and included verbatim for `scheduled`, which
stores the client's instant as given. Restoring full fidelity means adding the `idempotency_keys`
table and hashing the raw request body into it. The module's own docstring in
`app/services/idempotency.py` says the same thing.

| Situation | Result |
|---|---|
| Same key, same body | The original `201` and the original job id, replayed |
| Same key, different body | `422 idempotency_key_reuse` |
| Same key, first request still in flight | `409 idempotency_in_progress` |
| Key reused after the original job reached a terminal state | Accepted — a new job |

The last row is deliberate: `ux_jobs_live_idempotency` is scoped to live statuses, so a completed key
is reusable and a DLQ replay of a keyed job does not collide with its own ancestor.

This is **request-level** idempotency — it stops one HTTP retry creating two jobs. **Execution-level**
idempotency is the handler contract in [ADR-005](DESIGN_DECISIONS.md): delivery is at-least-once, so
handlers must be idempotent. Two different problems, both real.

---

## 5. Pagination

**Keyset, not offset** ([ADR-010](DESIGN_DECISIONS.md)). Every list endpoint returns the same
envelope:

```json
{
  "data": [],
  "page": { "limit": 25, "has_more": true, "next_cursor": "eyJ0IjoiMjAyNi0wOC0yMFQwOToxNDowMloiLCJpIjoiMDE5MmYzYzEifQ" },
  "meta": { "request_id": "01JD8ZQ4T7WVXK3M0PB9E6RSNC" }
}
```

- `next_cursor` is an **opaque** base64 encoding of the `(created_at, id)` tuple. Do not parse it;
  pass it back as `?cursor=`.
- `limit` defaults to 25, maximum 100.
- Iterate until `has_more` is `false`.
- **`total` is deliberately absent.** An exact count over a table under constant insert costs a scan
  and is stale before it renders. Counts come from `GET /queues/{queue_id}/stats`, which reads the
  partial `ix_jobs_depth` index and returns depth by live status.

```bash
curl -sS -H "Authorization: Bearer $TOKEN" \
  "http://localhost:8000/api/v1/projects/$PROJECT_ID/jobs?status=dead_letter&limit=50&cursor=$CURSOR"
```

**Filters:** `status` (repeatable), `kind`, `queue_id`, `handler`, `created_after`, `created_before`,
`run_after`, `run_before`. Names match column names exactly.

**Unknown query parameters return `400 unknown_query_param`.** A typo'd filter that is silently
ignored returns *everything* and looks like a working request — the failure mode is a user acting on
a wrong result, which is worse than an error.

---

## 6. Errors

One envelope for every failure, including FastAPI's own 404, 405 and 422 — handled explicitly in
`app/api/errors.py`, because otherwise the framework leaks a bare `{"detail": …}` for its built-in
errors while domain errors use the envelope, and the API has two shapes.

```json
{
  "error": {
    "code": "queue_paused",
    "message": "Queue 'emails' is paused",
    "details": [{ "field": "queue_id", "issue": "paused" }],
    "request_id": "01JD8ZQ4T7WVXK3M0PB9E6RSNC"
  }
}
```

`code` is a stable machine-readable string — clients branch on `code`, never on `message`.

| Domain error | `code` | HTTP |
|---|---|---|
| `ValidationError` | `validation_error` | 422 |
| `NotFoundError`, cross-tenant access | `not_found` | 404 |
| `PermissionDenied` | `permission_denied` | 403 |
| unauthenticated / bad token | `unauthenticated` | 401 |
| `ConflictError` | `conflict` | 409 |
| `IllegalTransition` | `illegal_transition` | 409 |
| `QueuePaused` | `queue_paused` | 409 |
| `IdempotencyKeyReuse` | `idempotency_key_reuse` | 422 |
| `IdempotencyInProgress` | `idempotency_in_progress` | 409 |
| unknown query parameter | `unknown_query_param` | 400 |
| unhandled | `internal_error` | 500 |

A `500` is logged with its `request_id` and **never leaks the exception** to the client. Quote the
`request_id` in a bug report — it is the same value returned in the `X-Request-ID` response header
and persisted as `jobs.correlation_id`, so one `grep` follows the request from the POST through every
retry on every worker.

---

## 7. Correlation

Every response carries `X-Request-ID`, mirrored in `meta.request_id` (and in `error.request_id` on
failures). For job creation the same value is persisted as `jobs.correlation_id` and re-bound by the
worker when it claims the job, so API logs, scheduler logs and worker logs across every attempt share
one key.
