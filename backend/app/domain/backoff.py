"""Retry backoff. Kept pure and dependency-free so it is testable without a database.

The same formulas are implemented in SQL in db/sql/fail_job.sql -- the worker computes
nothing, the database does. This module exists for validation, for the seed script, and
so tests can assert the SQL against an independent implementation.
"""

import random
from dataclasses import dataclass

from app.domain.enums import BackoffStrategy


@dataclass(frozen=True)
class RetryPolicy:
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    base_ms: int = 1_000
    max_ms: int = 300_000
    max_attempts: int = 3


def base_delay_ms(policy: RetryPolicy, attempt: int) -> int:
    """Delay before `attempt`, before jitter and before the cap.

    attempt is 1-based: attempt=1 is the delay before the *second* try.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    match policy.strategy:
        case BackoffStrategy.FIXED:
            raw = policy.base_ms
        case BackoffStrategy.LINEAR:
            raw = policy.base_ms * attempt
        case BackoffStrategy.EXPONENTIAL:
            raw = policy.base_ms * (2 ** (attempt - 1))
    return min(raw, policy.max_ms)


def next_delay_ms(policy: RetryPolicy, attempt: int, rng: random.Random | None = None) -> int:
    """Capped delay with full jitter applied: uniform over [0, capped].

    Full jitter rather than none or +/-10%: when a shared dependency fails, every in-flight
    job fails at nearly the same instant. Deterministic backoff reproduces that stampede at
    every tier; narrow jitter only smears it. Full jitter decorrelates the fleet.
    """
    r = rng or random
    return int(base_delay_ms(policy, attempt) * r.random())
