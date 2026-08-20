from functools import lru_cache

from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="CODITY_", extra="forbid", case_sensitive=False
    )

    database_url: PostgresDsn = Field(
        default=PostgresDsn("postgresql+asyncpg://localhost/codity"),
        description="asyncpg DSN",
    )
    db_pool_size: int = 10
    db_max_overflow: int = 5

    jwt_secret: str = Field(default="dev-only-change-me", min_length=8)
    jwt_algorithm: str = "HS256"
    access_token_ttl_minutes: int = 15
    refresh_token_ttl_days: int = 30

    log_level: str = "INFO"
    log_json: bool = True

    cors_origins: list[str] = ["http://localhost:5173"]

    # Worker
    worker_name: str | None = None
    worker_concurrency: int = 4
    claim_batch_size: int = 10
    poll_interval_ms: int = 500
    shutdown_grace_seconds: int = 30

    # Scheduler
    reaper_interval_ms: int = 15_000
    promoter_interval_ms: int = 1_000
    cron_interval_ms: int = 1_000
    reaper_batch: int = 200

    @property
    def sync_database_url(self) -> str:
        """Alembic runs sync; strip the asyncpg driver."""
        return str(self.database_url).replace("+asyncpg", "")


@lru_cache
def get_settings() -> Settings:
    return Settings()
