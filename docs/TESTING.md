# Testing

```bash
make test
```

```bash
make test-concurrency
```

```bash
make check
```

`pytest` + `pytest-asyncio` + `httpx.AsyncClient`, against a **real local PostgreSQL** database
(`codity_test`), with `alembic upgrade head` run once per session.

---

## 1. No testcontainers

Docker is not installed on the target machine, so the test suite cannot depend on it. It also should
not: the behaviour under test is `FOR UPDATE SKIP LOCKED`, `FOR NO KEY UPDATE` lock conflicts,
partial unique indexes, and CHECK constraints. None of that survives being mocked, and none of it is
reproducible against SQLite. The tests run against the same PostgreSQL 15 the application runs
against, or they prove nothing.

`make setup` creates `codity_test`. Point somewhere else with
`CODITY_DATABASE_URL=postgresql+asyncpg://…/other_db`.

---

## 2. Two fixtures, because concurrency tests need real sessions

| Fixture | Mechanism | Used by |
|---|---|---|
| `db` | A connection-bound transaction, rolled back after each test | ~90% of tests. Fast and fully isolated. |
| `db_committed` | Real commits, truncation teardown | Every test that spawns concurrent sessions |

The second fixture is not a convenience. **`SKIP LOCKED` semantics do not exist inside a single
transaction** — a transaction cannot skip its own locks, and two "workers" sharing one session are
one worker. Any concurrency test written against the rollback fixture passes for the wrong reason and
tests nothing. Tests requiring it are marked `@pytest.mark.concurrency` and can be run alone with
`make test-concurrency`.

---

## 3. Critical tests

These are the evidence for the reliability design. **They are not cut under time pressure.**

### Claiming and concurrency

| Test | Arrange → Act → Assert |
|---|---|
| `test_concurrent_claims_are_disjoint` | 100 queued jobs, 8 concurrent sessions → all claim → union == 100, pairwise intersection empty |
| `test_claim_respects_max_concurrency` | cap 3, 12 concurrent claimers, 50 jobs → claim → in-flight never exceeds 3 at any sample |
| `test_pause_stops_claiming_resume_restarts` | pause mid-drain → claims return 0; resume → claims resume |
| `test_claim_uses_partial_index` | `EXPLAIN` the real claim → references `ix_jobs_claim`, no `Seq Scan`, no `Sort` |

### Leases, fencing, recovery

| Test | Arrange → Act → Assert |
|---|---|
| `test_expired_lease_reclaimed_once` | claim, expire lease, two concurrent reapers → exactly one requeue, `lease_epoch` +1 |
| `test_stolen_lease_write_is_fenced` | claim at epoch e, reap, re-claim → complete with epoch e → rowcount 0, job untouched |
| `test_zombie_after_same_worker_reclaim_rejected` | W1 claims, is reaped, W1 re-claims (epoch e+2), zombie completes with e → rejected — **the ABA case** |
| `test_orphan_execution_does_not_block_next_attempt` | claim, lose the worker, reap, claim again → the second execution insert succeeds |

`test_orphan_execution_does_not_block_next_attempt` guards the `closed` CTE in `reap_leases.sql`.
`ux_job_executions_open_one` permits one open execution per job; if an abrupt worker death leaves an
orphaned open row and the reaper does not close it, the *next* claim's execution insert raises
`23505` — and so does every claim after that. The job becomes permanently unexecutable, and it is
precisely the flagship crash-recovery demo that wedges it.

### Retries and the DLQ

| Test | Arrange → Act → Assert |
|---|---|
| `test_backoff_sequence_per_strategy` | attempts 1..5 per strategy → `run_at` deltas within jitter bounds and capped at `backoff_max_ms` |
| `test_retry_goes_to_scheduled_not_queued` | fail with budget remaining → status `scheduled`, not claimable until `run_at` |
| `test_exhausted_job_dead_letters_once` | `max_attempts=2`, always-fail handler → exactly one `dead_letter_entries` row, `finished_at` set |
| `test_dlq_replay_of_keyed_job_succeeds` | dead-letter a job carrying an `Idempotency-Key`, replay → new job, no unique violation |

Backoff is **full jitter**, so these assert bounds, never equality. A test that asserts an exact delay
against a jittered schedule is a flaky test that will be deleted by whoever inherits it.

### Shutdown

| Test | Arrange → Act → Assert |
|---|---|
| `test_sigterm_releases_unstarted_claims` | SIGTERM with buffered claims → jobs return to `queued`, `attempt` unchanged, nothing lost |
| `test_start_rowcount_zero_cancels_task` | release the job between claim and start → **the handler is never invoked** |

The second is the deploy-time double-execution bug: the release commits while the start is in flight,
another worker claims the now-`queued` job, and the original handler runs to completion anyway. No
lease expiry and no partition required — just a rolling restart.

### Scheduling

| Test | Arrange → Act → Assert |
|---|---|
| `test_cron_two_schedulers_one_job_per_occurrence` | 2 schedulers, one due schedule → exactly one job for that `scheduled_for` |
| `test_cron_advances_when_insert_suppressed` | pre-insert the occurrence, tick → `next_occurrence_at` still advances |

The second guards a schedule that dies silently. If `next_occurrence_at` is advanced only when the
insert returns a row, a suppressed insert (a prior tick that committed the job then crashed, or an
operator's "run now") freezes the cursor: the schedule re-selects, re-conflicts, and does nothing —
forever, once per second, with no error anywhere.

### API and tenancy

| Test | Arrange → Act → Assert |
|---|---|
| `test_idempotency_replay_returns_original` | same `Idempotency-Key` twice → one job row, same id, `201` both times |
| `test_cross_org_returns_404` | a user in org A requests org B's job → **404**, not 403 |
| `test_migrations_upgrade_then_downgrade` | empty DB → `upgrade head` → `downgrade base` → clean |
| `test_state_machine_matches_enum` | the DB trigger's edge set == `LEGAL_TRANSITIONS` in `app/domain/enums.py` |
| `test_layering_rules` | AST-walk every module under `app/`: `domain/` imports no `fastapi`/`app.db`/`app.api`; `services/` and `repositories/` import no `fastapi` |

`test_layering_rules` is what makes the layering rule in `ARCHITECTURE.md` §3 real. An architecture
rule nobody checks is a comment, and the specific thing it protects is the worker's ability to import
`services/claim.py` without pulling in the web framework.

---

## 4. Integration tier

Roughly 20 tests: one 422-shape assertion and one 404 assertion per router, plus the full idempotency
and cross-org matrices. They exist to catch envelope drift — a router that returns FastAPI's bare
`{"detail": …}` instead of the error envelope, or `state` where the contract says `status`.

---

## 5. No coverage gate

`make cov` prints a coverage report. There is deliberately **no `--cov-fail-under`**.

Coverage percentage earns zero marks and does not measure whether the reliability invariants hold. A
gate added late in a build creates pressure to write filler serializer tests at exactly the moment
the concurrency tests are at risk — optimising the number instead of the property. The tests in §3
are the ones that matter, and they are named individually so their absence is visible.

---

## 6. Writing a new concurrency test

1. Use `db_committed`, and mark it `@pytest.mark.concurrency`.
2. Open **separate sessions** — one per simulated worker. Two coroutines sharing a session are one
   worker with extra steps.
3. Drive them with `asyncio.gather` so the statements genuinely interleave.
4. Assert a **property** (disjointness, a cap, a rowcount of zero), never a timing.
5. Let the fixture handle teardown; a test that writes outside the fixture must clean up after
   itself, or it will poison the next run.
