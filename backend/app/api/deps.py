from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models.tenancy import OrganizationMember, User
from app.db.session import get_session
from app.domain.enums import Role
from app.domain.errors import PermissionDenied
from app.services.security import decode_token

SessionDep = Annotated[AsyncSession, Depends(get_session)]


class Principal(BaseModel):
    user_id: UUID
    organization_id: UUID
    role: Role


async def current_principal(
    session: SessionDep, authorization: Annotated[str | None, Header()] = None
) -> Principal:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise PermissionDenied("missing bearer token")
    claims = decode_token(authorization.split(" ", 1)[1])

    user_id = UUID(claims["sub"])
    org_id = UUID(claims["org"])

    # Re-resolve against the DB rather than trusting the role claim: a revoked
    # membership or a bumped token_version must take effect immediately.
    row = (
        await session.execute(
            select(OrganizationMember.role, User.token_version)
            .join(User, User.id == OrganizationMember.user_id)
            .where(
                OrganizationMember.user_id == user_id,
                OrganizationMember.organization_id == org_id,
                User.is_active.is_(True),
            )
        )
    ).first()
    if row is None:
        raise PermissionDenied("not a member of this organization")
    role, token_version = row
    if token_version != claims.get("ver"):
        raise PermissionDenied("token has been revoked")
    return Principal(user_id=user_id, organization_id=org_id, role=Role(role))


PrincipalDep = Annotated[Principal, Depends(current_principal)]


async def require_member(principal: PrincipalDep) -> Principal:
    """Both roles may mutate; the split exists so the owner-only routes (delete
    project, transfer ownership) have a distinct dependency."""
    return principal


async def require_owner(principal: PrincipalDep) -> Principal:
    if principal.role is not Role.OWNER:
        raise PermissionDenied("owner role required")
    return principal


MemberDep = Annotated[Principal, Depends(require_member)]
OwnerDep = Annotated[Principal, Depends(require_owner)]
