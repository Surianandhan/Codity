"""log_line_count server default

Revision ID: f8ef4aba8de3
Revises: eafcffd66e0b
Create Date: 2026-08-20 08:45:25.178746
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = 'f8ef4aba8de3'
down_revision: str | None = 'eafcffd66e0b'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # The claim statement is raw SQL and bypasses the ORM, so a Python-side
    # default=0 is never applied and the INSERT sends NULL.
    op.alter_column("job_executions", "log_line_count", server_default=sa.text("0"))


def downgrade() -> None:
    op.alter_column("job_executions", "log_line_count", server_default=None)
