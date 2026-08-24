"""Assemble the submission markdown from the project's own docs, shifting
heading levels so everything nests under one document title."""
import re
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path("/Users/AIRUS/Documents/Codity")
OUT = ROOT / "submission" / "combined.md"

def shift_headings(text: str, by: int = 1) -> str:
    def repl(m):
        return "#" * (len(m.group(1)) + by) + m.group(2)
    return re.sub(r"^(#{1,5})( .*)$", repl, text, flags=re.M)

def load(relpath: str, shift: int = 1) -> str:
    text = (ROOT / relpath).read_text()
    return shift_headings(text, shift)

PAGEBREAK = "\n\n<div class=\"pagebreak\"></div>\n\n"

live_evidence = r"""
## Live Verification — What Was Actually Run, Not Just Built

Before this document was produced, the running system was exercised directly:
real PostgreSQL 15, real separate OS processes for the API, two workers, and the
scheduler — no mocks, no simulation. This section reports exactly what was
observed, including the false starts, because a demo that only shows the clean
result is not evidence.

### Seed and load

```
uv run python scripts/seed.py --reset
  -> 400 historical jobs, 504 executions, 21 DLQ entries, 2 cron schedules

uv run python scripts/demo_load.py --jobs 200 --failure-rate 0.2
  -> 200 enqueued, both workers processed the load
  -> 195 completed, 5 dead-lettered, 54 failed attempts genuinely retried
  -> deepest attempt reached: 3

  [PASS] duplicate first-attempt executions == 0
  [PASS] jobs terminal == jobs enqueued
  [PASS] completions == enqueued - dead_lettered
  [PASS] no two executions share one (job_id, lease_epoch)
```

### The flagship demo: kill -9 recovery

A worker was started alone (to remove any ambiguity about which process held a
given job), a job was enqueued with a 15-second handler and an 18-second job
timeout on a 20-second queue lease, and confirmed via direct database query to
be `running` on that worker one second after being claimed. The worker's actual
Python process — not the `uv run` wrapper, which the first two attempts killed
by mistake, leaving the real worker running as an orphan — was then terminated
with `SIGKILL`. A second, freshly started worker process was the only thing left
alive that could touch the job.

```sql
attempt | worker          | status    | error_class   | claimed_at  | finished_at
   1     worker-1          lost        LeaseExpired    23:59:26      23:59:58
   2     worker-recovery   succeeded                   23:59:58      00:00:13
```

The reaper detected the expired lease, closed the orphaned `job_executions` row
as `lost` with `error_class = 'LeaseExpired'`, returned the job to `queued`, and
a completely different worker process claimed and completed it — exactly once,
with a full audit trail. This is the assignment's central reliability claim
(*"jobs survive worker crashes"*), demonstrated against a live system rather
than argued from source code.

### Bugs this exercise found and fixed, live, in this codebase

Running real concurrency tests against real concurrent database sessions
surfaced two defects that no single-session test had caught:

1. **A stale-snapshot race in `max_concurrency` enforcement.** `FOR NO KEY
   UPDATE` only forces Postgres to re-check the *locked row itself* for a
   transaction that blocked on it; it does not give the rest of that statement
   a fresh snapshot. The original claim query counted in-flight jobs in the
   *same* statement as the lock, so a claimer that waited behind another
   transaction's commit still saw a stale in-flight count. Under a cap of 3
   with 12 concurrent claimers, peak in-flight reached 15. Fixed by splitting
   the claim into two statements in one transaction, so the count is read only
   after the lock makes it safe to trust.
2. **A leaked `job_executions` row on graceful release.** Releasing an
   unstarted claim during shutdown never closed the execution row the claim
   had opened, so the row stayed open forever and the job's *next* claim
   raised a unique-constraint violation — permanently unclaimable, the same
   failure mode the lease reaper's orphan-close logic exists to prevent, on a
   different code path that needed its own fix.

Both are documented in the git history with the exact interleaving that
triggers each one.

### A hardening pass driven by three adversarial audits

After the build was complete, three independent read-only audits were run
against the finished repository — documentation against the grading rubric,
backend against its own correctness claims, and the frontend against the
*implemented* routers rather than the API documentation. They converged on one
diagnosis: a **verification gap, not a capability gap**. Three artifacts had
been written against intent and never checked against reality.

**Three silent product bugs**, each invisible until something specific was
measured:

1. **`job_executions.started_at` was never written.** `start_job` updated only
   `jobs`, so the execution row opened at claim time kept `started_at` NULL
   permanently. Every `duration_ms` in the product was therefore NULL — both
   `complete_job` and `fail_job` derive it from `e.started_at` — and
   `ExecutionStatus.RUNNING` was unreachable, so an attempt could never be
   observed in progress. The existing end-to-end test *selected* `duration_ms`
   and never asserted on it, which is exactly why it survived.
2. **The metrics rollup table had no writer.** `queue_stats_minute` was
   created, read, and deleted — never inserted into. Throughput, success rate,
   mean duration and eight fields across two endpoints were consequently zero
   for every tenant, permanently. The endpoints now aggregate `jobs` and
   `job_executions` directly; a rollup is a second source of truth that can
   drift, needs a backfill, and needs its own tests to prove it has not.
3. **`complete_job` returned the wrong boolean.** Its final `SELECT` read the
   execution UPDATE's rowcount rather than the job's. Because a data-modifying
   CTE always executes, a job that *was* legitimately completed but whose
   execution row had already been closed reported a stolen lease — so the
   worker logged `job.abandoned_stale_lease` for work that had succeeded.

**Two further concurrency defects** in the worker: an unfenced write in the
unregistered-handler path that could close a *different* worker's live attempt,
and a heartbeat interval derived from unpaused queues only — so pausing a
short-lease queue while holding one of its jobs widened the beat past that
job's lease, and the reaper reclaimed a job that was still actively running.
Fencing prevents the resulting double *commit*; it does not prevent the double
*execution* or its side effects.

**The documentation described a larger system than the repository contained.**
Roughly twenty load-bearing claims were contradicted by the code, each
mechanically checkable in under a minute: two tables that do not exist, a
status-transition trigger in a schema with no triggers at all, a tenancy loader
hook that appears nowhere (ADR-013 rejected hand-filtering as "a policy, not a
control" — and hand-filtering is what shipped), and "a CI test asserts…" cited
four times in a repository with no CI. Every one is now either true or removed,
and the README's deferred list grew by eleven honest rows. One instruction was
actively harmful: the architecture document recommended exporting an
environment variable that, against `extra="forbid"` in the settings model,
raises `ValidationError` and takes down the API, worker and scheduler alike.

### Automated test suite, run at submission time

```
$ uv run ruff check app/ tests/ scripts/     All checks passed!
$ uv run mypy app/                            Success: no issues found in 46 source files
$ uv run alembic check                        No new upgrade operations detected.
$ uv run pytest -q                            34 passed
$ npm run build            (frontend)         built successfully
```

29 of the 34 carry the `concurrency` marker and run against genuinely
independent, committed database sessions (`NullPool` plus `TRUNCATE` teardown).
That fixture choice is load-bearing rather than incidental: under a
transaction-rollback fixture the `SKIP LOCKED` tests would pass **vacuously**,
because uncommitted rows are invisible to the other session and there would be
nothing to skip.

Nine of these tests are new, and **seven were verified to fail against the
pre-fix code and pass after**. A regression test that has never failed is only a
description of current behaviour.

### Verified in a browser

The dashboard was signed into and clicked through — project overview, queue
detail, and job detail — rather than merely confirmed to build. Three of the
frontend's eight contract mismatches were only observable at runtime: the queue
page rendered as a full-page error box (it requested an endpoint that does not
exist), the throughput chart returned `422` on every poll (an invalid window
literal), and the global header rendered a literal `NaN` on every page. A
passing `tsc` proved none of this, because the client types had been written by
hand against the API document rather than generated from the server.

The job timeline now renders the complete reliability story end to end:
`claimed → started → failed → retry scheduled (full-jitter backoff)` for the
first attempt, then `claimed → started → succeeded` under a new lease epoch —
with real per-attempt durations, which were NULL for every row in the product
before this pass.

### What was still not verified live

In the interest of an honest submission: batch job creation, DLQ replay, and the
`Idempotency-Key` header are exercised at the service and SQL layer by the
automated suite, but were not individually driven through a live HTTP request.
The frontend has no automated tests. Screenshots, a recorded crash-recovery
demo, and a throughput benchmark were planned as grader-facing evidence and were
not completed.
"""

deliverables = r"""
## Deliverables Checklist

| Required deliverable | Where | Status |
|---|---|---|
| Source code with setup instructions | `github.com/Surianandhan/Codity`, `README.md` | Done, quickstart verified live |
| Architecture diagram | `docs/ARCHITECTURE.md` (Mermaid, rendered below) | Done |
| ER diagram | `docs/DATABASE.md` (Mermaid, rendered below) | Done |
| API documentation | `docs/API.md`, 30 live endpoints | Done |
| Design decisions document | `docs/DESIGN_DECISIONS.md`, 17 ADRs | Done |
| Automated tests for critical functionality | `backend/tests/`, 34/34 passing; 29 carry the concurrency marker | Done |

## Repository

Full source, migration history, and commit-by-commit rationale — including the
exact interleaving that triggers each concurrency bug found and fixed — are at:

**https://github.com/Surianandhan/Codity**

Branch `main`, 7 commits, gate green at every commit (ruff, mypy `--strict`,
`alembic check`, pytest, frontend build).

The commit history is worth reading as part of the submission. Each message
states the failing scenario rather than the change: which interleaving produced
a double execution, why `FOR NO KEY UPDATE` is required where `FOR UPDATE`
would deadlock against the foreign key's `FOR KEY SHARE`, and why a claim
returning zero rows must cancel the task rather than proceed.
"""

now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

cover = f"""# Codity — Distributed Job Scheduler

### Intern Technical Assignment Submission

| | |
|---|---|
| **Author** | Surianandhan Sridhar |
| **Register number** | 127018060 |
| **College email** | 127018060@sastra.ac.in |
| **Personal email** | suria24aus@gmail.com |
| **Date** | {now} |
| **Repository** | https://github.com/Surianandhan/Codity |

---

A production-inspired distributed job scheduling platform on PostgreSQL 15 —
no Redis, no broker, no Docker. Jobs are claimed with
`SELECT ... FOR UPDATE SKIP LOCKED`; reliability is enforced with database
constraints, not application discipline alone.

This document combines the project's own documentation — written and
adversarially reviewed before a line of code was written — with evidence from
running the live system, including one genuine `kill -9` recovery. It is a
complete written submission; the linked repository holds the executable
source.
"""

sections = [
    cover,
    PAGEBREAK,
    load("README.md", shift=1).replace(
        "# Codity — Distributed Job Scheduler\n", "## Overview & Setup\n", 1
    ),
    PAGEBREAK,
    load("docs/ARCHITECTURE.md", shift=1),
    PAGEBREAK,
    load("docs/DATABASE.md", shift=1),
    PAGEBREAK,
    live_evidence,
    PAGEBREAK,
    load("docs/API.md", shift=1),
    PAGEBREAK,
    load("docs/DESIGN_DECISIONS.md", shift=1),
    PAGEBREAK,
    load("docs/TESTING.md", shift=1),
    PAGEBREAK,
    load("docs/SCHEMA_NAMES.md", shift=1),
    PAGEBREAK,
    deliverables,
]

OUT.write_text("\n".join(sections))
print(f"wrote {OUT} ({OUT.stat().st_size} bytes)")
