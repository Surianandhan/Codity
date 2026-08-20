"""Wire models. The five job kinds are one discriminated union so validation, auth
and idempotency are written once rather than five times."""

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, EmailStr, Field, field_validator

from app.domain.enums import BackoffStrategy, JobKind, JobStatus


class ORMModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)


# --- auth ---
class RegisterIn(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    full_name: str | None = None
    organization_name: str = Field(min_length=1, max_length=200)


class LoginIn(BaseModel):
    email: EmailStr
    password: str


class TokenOut(BaseModel):
    access_token: str
    refresh_token: str
    token_type: Literal["bearer"] = "bearer"


class MeOut(ORMModel):
    id: UUID
    email: str
    full_name: str | None
    organization_id: UUID
    role: str


# --- projects / queues ---
class ProjectIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    slug: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9-]*$")


class ProjectOut(ORMModel):
    id: UUID
    organization_id: UUID
    name: str
    slug: str
    created_at: datetime


class QueueIn(BaseModel):
    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    max_concurrency: int = Field(default=10, ge=1, le=1000)
    priority: int = Field(default=0, ge=-100, le=100, description="polling weight; higher first")
    default_priority: int = Field(default=0, ge=-100, le=100)
    visibility_timeout_sec: int = Field(default=300, ge=10, le=3600)
    default_timeout_ms: int = Field(default=60_000, ge=100)
    max_payload_bytes: int = Field(default=65_536, ge=1024, le=1_048_576)
    dlq_enabled: bool = True

    @field_validator("default_timeout_ms")
    @classmethod
    def _timeout_lt_lease(cls, v: int, info: Any) -> int:
        lease = (info.data or {}).get("visibility_timeout_sec", 300)
        if v >= lease * 1000:
            raise ValueError(
                f"default_timeout_ms ({v}) must be < visibility_timeout_sec*1000 ({lease * 1000}): "
                "a job may never outlive its own lease"
            )
        return v


class QueueOut(ORMModel):
    id: UUID
    project_id: UUID
    name: str
    max_concurrency: int
    priority: int
    default_priority: int
    visibility_timeout_sec: int
    default_timeout_ms: int
    dlq_enabled: bool
    is_paused: bool
    paused_at: datetime | None
    created_at: datetime


# --- jobs ---
class JobBase(BaseModel):
    handler: str = Field(min_length=1, max_length=200)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int | None = Field(default=None, ge=-100, le=100, description="higher runs first")
    max_attempts: int | None = Field(default=None, ge=1, le=50)
    timeout_ms: int | None = Field(default=None, ge=100)
    backoff_strategy: BackoffStrategy | None = None
    idempotency_key: str | None = Field(default=None, max_length=255)


class ImmediateJobIn(JobBase):
    kind: Literal[JobKind.IMMEDIATE] = JobKind.IMMEDIATE


class DelayedJobIn(JobBase):
    kind: Literal[JobKind.DELAYED] = JobKind.DELAYED
    delay_ms: int = Field(gt=0, le=86_400_000 * 30)


class ScheduledJobIn(JobBase):
    kind: Literal[JobKind.SCHEDULED] = JobKind.SCHEDULED
    run_at: AwareDatetime


class RecurringJobIn(JobBase):
    kind: Literal[JobKind.RECURRING] = JobKind.RECURRING
    cron: str = Field(min_length=1, max_length=200)
    timezone: str = "UTC"
    catchup_policy: Literal["skip", "catchup"] = "skip"


class BatchJobIn(JobBase):
    kind: Literal[JobKind.BATCH] = JobKind.BATCH
    items: list[dict[str, Any]] = Field(min_length=1, max_length=1000)


JobCreate = Annotated[
    ImmediateJobIn | DelayedJobIn | ScheduledJobIn | RecurringJobIn | BatchJobIn,
    Field(discriminator="kind"),
]


class JobOut(ORMModel):
    id: UUID
    queue_id: UUID
    project_id: UUID
    kind: str
    handler: str
    status: JobStatus
    priority: int
    run_at: datetime
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    worker_id: UUID | None
    lease_epoch: int
    claimed_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    last_error_class: str | None
    last_error_message: str | None
    created_at: datetime


class Page(BaseModel):
    limit: int
    has_more: bool
    next_cursor: str | None = None


class Meta(BaseModel):
    request_id: str


class JobPage(BaseModel):
    data: list[JobOut]
    page: Page
    meta: Meta
