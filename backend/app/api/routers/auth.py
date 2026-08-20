from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, status
from sqlalchemy import select

from app.api.deps import PrincipalDep, SessionDep
from app.api.schemas import LoginIn, MeOut, RegisterIn, TokenOut
from app.db.models.base import uuid7
from app.db.models.tenancy import Organization, OrganizationMember, RefreshToken, User
from app.domain.enums import Role
from app.domain.errors import ConflictError, PermissionDenied
from app.services.security import (
    create_access_token,
    create_refresh_token,
    hash_password,
    verify_password,
)

router = APIRouter(prefix="/auth", tags=["auth"])


def _slugify(name: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in name.lower()).strip("-")[:100] or "org"


async def _issue(session: SessionDep, user: User, org_id: UUID, role: str) -> TokenOut:  # type: ignore[name-defined]
    access = create_access_token(user.id, org_id, role, user.token_version)
    refresh, jti, expires = create_refresh_token(user.id)
    session.add(RefreshToken(jti=jti, user_id=user.id, expires_at=expires))
    await session.commit()
    return TokenOut(access_token=access, refresh_token=refresh)


@router.post("/register", response_model=TokenOut, status_code=status.HTTP_201_CREATED)
async def register(body: RegisterIn, session: SessionDep) -> TokenOut:
    existing = (
        await session.execute(select(User.id).where(User.email == body.email))
    ).scalar_one_or_none()
    if existing:
        raise ConflictError("that email is already registered")

    user = User(
        id=uuid7(),
        email=body.email,
        password_hash=hash_password(body.password),
        full_name=body.full_name,
    )
    org = Organization(
        id=uuid7(), name=body.organization_name, slug=_slugify(body.organization_name)
    )
    session.add_all([user, org])
    await session.flush()
    session.add(
        OrganizationMember(id=uuid7(), organization_id=org.id, user_id=user.id, role=Role.OWNER)
    )
    return await _issue(session, user, org.id, Role.OWNER)


@router.post("/login", response_model=TokenOut)
async def login(body: LoginIn, session: SessionDep) -> TokenOut:
    user = (
        await session.execute(select(User).where(User.email == body.email))
    ).scalar_one_or_none()
    # Same error for unknown-user and wrong-password: never confirm an email exists.
    if user is None or not verify_password(body.password, user.password_hash):
        raise PermissionDenied("invalid credentials")
    if not user.is_active:
        raise PermissionDenied("account is disabled")

    membership = (
        await session.execute(
            select(OrganizationMember).where(OrganizationMember.user_id == user.id)
        )
    ).scalars().first()
    if membership is None:
        raise PermissionDenied("user belongs to no organization")

    user.last_login_at = datetime.now(UTC)
    return await _issue(session, user, membership.organization_id, membership.role)


@router.get("/me", response_model=MeOut)
async def me(principal: PrincipalDep, session: SessionDep) -> MeOut:
    user = (
        await session.execute(select(User).where(User.id == principal.user_id))
    ).scalar_one()
    return MeOut(
        id=user.id,
        email=user.email,
        full_name=user.full_name,
        organization_id=principal.organization_id,
        role=principal.role,
    )
