"""Canonical vocabulary. Every other module imports these; nothing redefines them.

See docs/SCHEMA_NAMES.md -- the names here are the single source of truth.
"""

from enum import StrEnum


class JobStatus(StrEnum):
    SCHEDULED = "scheduled"
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"
    CANCELLED = "cancelled"


class ExecutionStatus(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    TIMED_OUT = "timed_out"
    CANCELLED = "cancelled"
    LOST = "lost"


class JobKind(StrEnum):
    IMMEDIATE = "immediate"
    DELAYED = "delayed"
    SCHEDULED = "scheduled"
    RECURRING = "recurring"
    BATCH = "batch"


class BackoffStrategy(StrEnum):
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"


class Role(StrEnum):
    OWNER = "owner"
    MEMBER = "member"


class WorkerStatus(StrEnum):
    STARTING = "starting"
    ACTIVE = "active"
    DRAINING = "draining"
    STOPPED = "stopped"
    DEAD = "dead"


TERMINAL_STATUSES = frozenset(
    {JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.DEAD_LETTER, JobStatus.CANCELLED}
)

CLAIMABLE_STATUSES = frozenset({JobStatus.QUEUED})
INFLIGHT_STATUSES = frozenset({JobStatus.CLAIMED, JobStatus.RUNNING})
LIVE_STATUSES = frozenset(
    {JobStatus.SCHEDULED, JobStatus.QUEUED, JobStatus.CLAIMED, JobStatus.RUNNING}
)

# The state machine, as data -- the diagram in docs/DATABASE.md section 5 transcribed.
#
# Referenced by nothing. There is no job_status_transitions table and no trigger:
# the schema contains no triggers at all. Enforcement is Python-side only, and it
# is per-statement rather than central -- every transition is a guarded UPDATE
# whose WHERE names the source status (claim_jobs.sql matches status='queued',
# start_job.sql matches 'claimed', complete_job.sql and fail_job.sql match
# 'running'), and every caller checks the rowcount. An illegal edge does not
# raise; it updates zero rows.
#
# So this set is documentation in executable form, and nothing keeps it honest.
# Drift between it and the .sql files is possible and would be silent. Closing
# that means either a BEFORE UPDATE trigger asserting these edges, or a test
# parsing the source status out of each statement -- neither is written.
LEGAL_TRANSITIONS: frozenset[tuple[JobStatus, JobStatus]] = frozenset(
    {
        (JobStatus.SCHEDULED, JobStatus.QUEUED),
        (JobStatus.SCHEDULED, JobStatus.CANCELLED),
        (JobStatus.QUEUED, JobStatus.CLAIMED),
        (JobStatus.QUEUED, JobStatus.CANCELLED),
        (JobStatus.CLAIMED, JobStatus.RUNNING),
        (JobStatus.CLAIMED, JobStatus.QUEUED),
        (JobStatus.RUNNING, JobStatus.COMPLETED),
        (JobStatus.RUNNING, JobStatus.SCHEDULED),
        (JobStatus.RUNNING, JobStatus.QUEUED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.RUNNING, JobStatus.DEAD_LETTER),
        (JobStatus.RUNNING, JobStatus.CANCELLED),
    }
)
