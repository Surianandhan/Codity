"""Password hashing and JWT minting. No FastAPI imports -- the worker and the
seed script use this too."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from jose import JWTError, jwt

from app.config import get_settings
from app.domain.errors import PermissionDenied

_hasher = PasswordHasher()


def hash_password(raw: str) -> str:
    return _hasher.hash(raw)


def verify_password(raw: str, hashed: str) -> bool:
    try:
        _hasher.verify(hashed, raw)
    except VerifyMismatchError:
        return False
    return True


def needs_rehash(hashed: str) -> bool:
    """Argon2 parameters get stronger over time; rehash on successful login."""
    return _hasher.check_needs_rehash(hashed)


def create_access_token(user_id: UUID, org_id: UUID, role: str, token_version: int) -> str:
    s = get_settings()
    now = datetime.now(UTC)
    payload = {
        "sub": str(user_id),
        "org": str(org_id),
        "role": role,
        "ver": token_version,
        "iat": now,
        "exp": now + timedelta(minutes=s.access_token_ttl_minutes),
        "typ": "access",
    }
    return str(jwt.encode(payload, s.jwt_secret, algorithm=s.jwt_algorithm))


def create_refresh_token(user_id: UUID) -> tuple[str, str, datetime]:
    """Returns (token, jti, expires_at). The jti is stored so rotation can detect reuse."""
    s = get_settings()
    now = datetime.now(UTC)
    jti = uuid4().hex
    expires = now + timedelta(days=s.refresh_token_ttl_days)
    token = str(
        jwt.encode(
            {"sub": str(user_id), "jti": jti, "iat": now, "exp": expires, "typ": "refresh"},
            s.jwt_secret,
            algorithm=s.jwt_algorithm,
        )
    )
    return token, jti, expires


def decode_token(token: str, expected_type: str = "access") -> dict[str, Any]:
    s = get_settings()
    try:
        claims: dict[str, Any] = jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm])
    except JWTError as exc:
        raise PermissionDenied("invalid or expired token") from exc
    if claims.get("typ") != expected_type:
        raise PermissionDenied(f"expected a {expected_type} token")
    return claims
