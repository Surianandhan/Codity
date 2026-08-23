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

**34 tests across seven files.** Nine of those are regression tests in the strict sense — written
after a defect was found, and **verified to fail against the code they replace**: two of the three
in `tests/test_execution_timing.py`, and all six in `tests/test_worker_lifecycle.py`. That is what
separates a regression test from one written to match whatever the code already does, and it is the
distinction §3 leans on. 34 is a small number, and it is deliberately spent: it buys the
reliability invariants in §3 — disjoint claims, an exact concurrency cap, lease fencing including
the ABA case, orphaned-execution recovery, the shutdown race, and cron under two schedulers — rather
than breadth across routers. Every one of them runs against genuinely independent **committed**
sessions (§2), which is the difference between testing `SKIP LOCKED` and testing nothing. What that
choice costs is listed honestly at the end of §3.

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

## 2. One mode: real commits, cleaned between tests

There is no rollback fixture and no two-fixture split. `backend/tests/conftest.py` defines five
fixtures, and every test in the suite gets the same treatment:

| Fixture | Scope | What it does |
|---|---|---|
| `_migrate` | session, autouse | `alembic upgrade head` once against `codity_test` |
| `engine` | function | An async engine on **`NullPool`**, so every session gets its own connection |
| `sessionmaker_` | function | `async_sessionmaker(engine, expire_on_commit=False)` |
| `_clean` | function, **autouse** | Teardown: every table is emptied, identities restarted, cascading |
| `_reset_app_engine` | function, autouse | Clears the module-level engine cache in `app.db.session` |

**This is the decision that makes the concurrency tests mean anything**, and it is why the faster
option was rejected rather than overlooked. A connection-bound transaction rolled back after each
test is the standard pytest pattern and it is measurably quicker — but **`SKIP LOCKED` semantics do
not exist inside a single transaction.** A transaction cannot skip its own locks, two "workers"
sharing one session are one worker with extra steps, and every concurrency assertion in §3 would
pass *vacuously* while testing nothing at all. `NullPool` plus committed writes plus a teardown that
empties the tables is what buys genuinely independent sessions. The cost is teardown time, paid once
per test, and it is the right trade for a suite whose entire point is what happens *between*
connections.

`_reset_app_engine` exists for a narrower reason, worth knowing before you debug it: `app.db.session`
caches an engine at module level, pytest-asyncio gives each test a fresh event loop, and a cached
engine holds connections bound to a dead loop — so the *next* test dies with `Event loop is closed`.

Concurrency tests are marked `@pytest.mark.concurrency`, applied at **module level** in
`test_concurrency.py`, `test_retry.py`, `test_scheduling.py`, `test_execution_timing.py` and
`test_worker_lifecycle.py`. So `make test-concurrency` (`pytest -m concurrency`) selects **29 of the
34 tests** — most of the suite, not a corner of it. The five it deselects are the three migration
tests and the two API end-to-end tests.

---

## 3. Critical tests

These are the evidence for the reliability design. **They are not cut under time pressure.**

### Claiming and concurrency

| Test | Arrange → Act → Assert |
|---|---|
| `test_concurrent_claims_are_disjoint` | 100 queued jobs, 8 concurrent sessions → all claim → union == 100, pairwise intersection empty |
| `test_claim_respects_max_concurrency` | cap 3, 12 concurrent claimers, 50 jobs → claim → in-flight never exceeds 3 at any sample |
| `test_claim_cap_holds_when_a_claimer_waits_on_the_queue_lock` | the two-session deterministic form: A fills the cap and holds its transaction open, B blocks on the queue-row lock → after release, B claims **zero**, in-flight is still 3 |
| `test_pause_stops_claiming_resume_restarts` | pause mid-drain → claims return 0; resume → claims resume |
| `test_claim_uses_partial_index` | `EXPLAIN` the real claim → references `ix_jobs_claim`, no `Seq Scan`, no `Sort` |

`test_claim_cap_holds_when_a_claimer_waits_on_the_queue_lock` is the one that found a real bug.
Locking the queue row does *not* give the rest of a single statement a fresh snapshot: under
`READ COMMITTED` a statement keeps the snapshot it took when it started, and releasing a row lock
re-checks only the *locked row*. A claimer that waited behind another transaction's commit therefore
computed its headroom from a pre-lock count — the exact check-then-act race the lock exists to
close, merely serialised. The fix is why the lock is `lock_queue.sql`, issued as its **own
statement** before `claim_jobs.sql` in the same transaction: a new statement gets a new snapshot,
taken once the lock already makes it safe to act on.

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

`test_backoff_sequence_per_strategy` is parametrised over `fixed`, `linear` and `exponential`, so it
is three of the 28. Backoff is **full jitter**, so these assert bounds, never equality. A test that
asserts an exact delay against a jittered schedule is a flaky test that will be deleted by whoever
inherits it.

**Not tested:** that replaying a dead-lettered job which carried an `Idempotency-Key` succeeds
without a unique violation. `ux_jobs_live_idempotency` is scoped to live statuses precisely so that
it does, and [ADR-009](DESIGN_DECISIONS.md) rests on it, but no test asserts it. It is the most
obvious gap in this tier.

### Shutdown

| Test | Arrange → Act → Assert |
|---|---|
| `test_sigterm_releases_unstarted_claims` | SIGTERM with buffered claims → jobs return to `queued`, `attempt` unchanged, nothing lost |
| `test_released_claim_can_be_claimed_again` | graceful release → the execution row is **closed**, and the next worker can claim the job |
| `test_start_rowcount_zero_means_do_not_run` | release the job between claim and start → **the handler is never invoked**, and no attempt is consumed |

The second is the deploy-time double-execution bug: the release commits while the start is in flight,
another worker claims the now-`queued` job, and the original handler runs to completion anyway. No
lease expiry and no partition required — just a rolling restart.

### Scheduling

| Test | Arrange → Act → Assert |
|---|---|
| `test_cron_two_schedulers_one_job_per_occurrence` | 2 schedulers, one due schedule → exactly one job for that `scheduled_for` |
| `test_cron_advances_when_insert_suppressed` | pre-insert the occurrence, tick → `next_occurrence_at` still advances |
| `test_cron_does_not_materialise_into_a_paused_queue` | tick a due schedule on a paused queue → no job, `skipped_occurrences` increments |

The second guards a schedule that dies silently. If `next_occurrence_at` is advanced only when the
insert returns a row, a suppressed insert (a prior tick that committed the job then crashed, or an
operator's "run now") freezes the cursor: the schedule re-selects, re-conflicts, and does nothing —
forever, once per second, with no error anywhere.

### Execution timing — `tests/test_execution_timing.py`

The newest file, and the only one written **after** the bug it describes was found rather than
before.

| Test | Arrange → Act → Assert |
|---|---|
| `test_start_job_marks_the_attempt_running` | claim → the execution row is `claimed` with `started_at` NULL; start → it is `running` with `started_at` set |
| `test_completed_attempt_records_a_duration` | claim, start, wait, complete → `duration_ms` is **not NULL** and ≥ 0 |
| `test_start_job_does_not_touch_a_stale_epochs_attempt` | start with a superseded `lease_epoch` → neither the job row nor the execution row moves |

**Two of these three were verified to fail against the pre-fix code, and that is what makes them
regression tests rather than decoration.** `start_job` updated only `jobs`; the execution row opened
at claim time kept `started_at` NULL forever. Both `complete_job` and `fail_job` derive `duration_ms`
from `e.started_at`, so **every duration in the product was NULL**, and `ExecutionStatus.RUNNING`
was unreachable — an attempt could never be observed in progress, and the job timeline jumped
straight from `claimed` to a terminal state.

The reason it survived is worth naming: the existing end-to-end test *selected* `duration_ms` and
never asserted on it. A test that reads a column without checking its value proves the query
compiles and nothing else. These assert the value.

The third test does not reproduce a past bug — it pins the fence. The new execution `UPDATE` had to
be guarded on `lease_epoch` exactly as the `jobs` `UPDATE` is, or the fix would have opened a fresh
hole beside the one it closed.

### Worker lifecycle — `tests/test_worker_lifecycle.py`

Six tests, each reproducing an interleaving only a **second** worker can reach, and each one written
against a defect it reproduces rather than against current behaviour.

| Test | Arrange → Act → Assert |
|---|---|
| `test_unregistered_release_cannot_close_a_live_execution_row` | a stale worker wakes after being reaped and releases → it must not close the *successor's* live execution row |
| `test_unregistered_release_still_closes_its_own_attempt` | the same release → its own attempt row is closed, so the next claim can open one |
| `test_unregistered_release_parks_the_job_instead_of_requeueing_it` | release a job whose handler is unregistered → parked, not returned to `queued` for the next poll to re-claim |
| `test_unregistered_count_dead_letters_the_job` | repeated unregistered releases → bounded by `UNREGISTERED_MAX_RELEASES`, then dead-lettered |
| `test_beat_follows_the_held_lease_not_the_claimable_queues` | heartbeat interval sized from leases **held**, not queues subscribed to |
| `test_pausing_a_queue_cannot_widen_the_beat_mid_flight` | pause a short-lease queue with a job running on it → the beat does not slow below that job's lease |

The first three are one bug each in the unregistered-handler release path: an execution `UPDATE`
fenced on nothing, so a reaped worker could close a live row belonging to its successor; a release
into `queued` that the next ~500ms poll re-claimed, making the claim/release loop unbounded; and an
`unregistered_count` that nothing read, so the bound it existed to enforce did not exist. The last
two are one bug in heartbeat sizing: the interval came from the queues a worker might claim *from*
rather than the leases it actually *holds*, so pausing a short-lease queue starved the heartbeat of
a job already running on it — a self-inflicted lease expiry.

Like every other concurrency test here, these drive the real SQL through the real service wrappers
on independent committed sessions; the helpers are imported from `test_concurrency.py` rather than
re-implemented.

### API end-to-end and migrations

| Test | Arrange → Act → Assert |
|---|---|
| `test_posted_job_is_claimed_and_completed` | `POST` a job over HTTP, run a real worker against it → `completed` |
| `test_paused_queue_is_not_claimed` | pause, `POST`, run the worker → the job stays `queued` |
| `test_upgrade_creates_schema` | `upgrade head` on an empty database → the expected tables exist |
| `test_upgrade_downgrade_roundtrip` | empty DB → `upgrade head` → `downgrade base` → clean |
| `test_enum_labels_are_lowercase_values` | every Postgres enum label matches the Python `StrEnum` value |

**What is not tested here, stated rather than implied.** There is no test asserting that a
cross-tenant request returns `404` rather than `403`, no test asserting the error envelope's shape
per router, and no test walking the AST to enforce the layering rule in
[`ARCHITECTURE.md`](ARCHITECTURE.md) §3. `LEGAL_TRANSITIONS` in `app/domain/enums.py` is likewise
asserted by nothing — there is no status-transition trigger for it to be compared against, because
the schema has no triggers at all ([`DATABASE.md`](DATABASE.md) §5). Each of those behaviours is
implemented; none of them is pinned. The reliability core got the test budget, and this is where the
bill came due.

---

## 4. No coverage gate

`make cov` prints a coverage report. There is deliberately **no `--cov-fail-under`**.

Coverage percentage earns zero marks and does not measure whether the reliability invariants hold. A
gate added late in a build creates pressure to write filler serializer tests at exactly the moment
the concurrency tests are at risk — optimising the number instead of the property. The tests in §3
are the ones that matter, and they are named individually so their absence is visible.

---

## 5. Writing a new concurrency test

1. Take `sessionmaker_`. The module-level `pytestmark = pytest.mark.concurrency` already covers the
   four files that have one; a new file needs its own.
2. Open **separate sessions** — one per simulated worker. Two coroutines sharing a session are one
   worker with extra steps.
3. Drive them with `asyncio.gather` so the statements genuinely interleave.
4. Assert a **property** (disjointness, a cap, a rowcount of zero), never a timing.
5. Let `_clean` handle teardown — it is autouse and empties every table after each test, so a test
   that writes outside the fixtures still gets cleaned up. What it cannot clean is anything written
   to a table not in that list; add it there rather than tidying by hand.
