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

### Automated test suite, run at submission time

```
$ uv run ruff check app/ tests/ scripts/     All checks passed!
$ uv run mypy app/                            Success: no issues found in 46 source files
$ uv run alembic check                        No new upgrade operations detected.
$ uv run pytest -q                            25 passed
```

Of the 25, 11 are genuine concurrency tests run against independent, real
committed database sessions — a transaction-rollback fixture would make
`SKIP LOCKED` tests pass vacuously, since there would be nothing to skip.

### What was not verified live

In the interest of an honest submission: the frontend dashboard was confirmed
to start and serve (`GET / -> 200`) but was not clicked through in a browser.
Batch job creation, DLQ replay over HTTP, and the `Idempotency-Key` header were
exercised at the service/SQL layer by the automated test suite but not
individually driven through a live HTTP request during this session.
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
| Automated tests for critical functionality | `backend/tests/`, 25/25 passing incl. 11 concurrency tests | Done |

## Repository

Full source, migration history, and commit-by-commit rationale (including the
two concurrency bugs found and fixed live) are at:

**https://github.com/Surianandhan/Codity**

Branch `main`, 5 commits, gate green at every commit (ruff, mypy --strict,
alembic check, pytest).
"""

now = datetime.now(timezone.utc).strftime("%Y-%m-%d")

cover = f"""# Codity — Distributed Job Scheduler

### Intern Technical Assignment Submission

**Author:** Surianandhan
**Date:** {now}
**Repository:** https://github.com/Surianandhan/Codity

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
