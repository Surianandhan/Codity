"""Keyset pagination, the single response envelope, and strict query handling.

Offset paging is the wrong tool here. ``jobs`` is insert-heavy, so an offset page
skips and duplicates rows the moment a concurrent insert lands ahead of the
cursor, and a deep offset scans everything it skips. A keyset cursor is a
``(created_at, id)`` tuple matching the DESC listing indexes, so a page walk is
stable under concurrent writes and every page costs one index seek.

``total`` and ``prev_cursor`` are deliberately absent: a total is a full count on
every page, and counts are served by ``/queues/{id}/stats`` instead.
"""

import base64
import binascii
import json
from collections.abc import Callable, Iterable, Sequence
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import Request
from pydantic import BaseModel
from sqlalchemy import ColumnElement, SQLColumnExpression, and_, or_

from app.api.schemas import Meta, Page
from app.domain.errors import DomainError, ValidationError

# Page sizes. The cap exists so a client cannot turn a keyset endpoint back into
# a full-table read by asking for one enormous page.
DEFAULT_LIMIT = 25
MAX_LIMIT = 200


class UnknownQueryParam(DomainError):
    """400, not 422: the request is well-formed, it just asks for something this
    endpoint does not offer. Silently ignoring a typo'd filter is worse -- the
    caller gets unfiltered data and believes it was filtered."""

    code = "unknown_query_param"
    http_status = 400


class InvalidCursor(ValidationError):
    code = "invalid_cursor"


class Envelope[T](BaseModel):
    """{data, page, meta} -- the one list shape for the whole API."""

    data: list[T]
    page: Page
    meta: Meta


def request_id(request: Request) -> str:
    return str(getattr(request.state, "request_id", "-"))


def reject_unknown_query_params(request: Request, allowed: Iterable[str]) -> None:
    known = set(allowed)
    unknown = sorted({k for k in request.query_params if k not in known})
    if unknown:
        raise UnknownQueryParam(
            f"unknown query parameter(s): {', '.join(unknown)}",
            [{"field": k, "issue": "unknown_query_param"} for k in unknown],
        )


# --- cursor codec -----------------------------------------------------------
# Opaque by construction: base64url of a compact JSON object. Clients must treat
# it as a token, which is what lets the sort key change without breaking them.


def _encode(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode(cursor: str) -> dict[str, Any]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        obj = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except (binascii.Error, ValueError, UnicodeDecodeError) as exc:
        raise InvalidCursor("cursor is not a valid pagination cursor") from exc
    if not isinstance(obj, dict):
        raise InvalidCursor("cursor is not a valid pagination cursor")
    return dict(obj)


def encode_time_cursor(ts: datetime, ident: UUID | int) -> str:
    return _encode({"t": ts.astimezone(UTC).isoformat(), "i": str(ident)})


def decode_time_uuid_cursor(cursor: str) -> tuple[datetime, UUID]:
    obj = _decode(cursor)
    try:
        return datetime.fromisoformat(str(obj["t"])), UUID(str(obj["i"]))
    except (KeyError, ValueError) as exc:
        raise InvalidCursor("cursor is not a valid pagination cursor") from exc


def encode_int_cursor(value: int) -> str:
    return _encode({"n": value})


def decode_int_cursor(cursor: str) -> int:
    obj = _decode(cursor)
    try:
        return int(obj["n"])
    except (KeyError, TypeError, ValueError) as exc:
        raise InvalidCursor("cursor is not a valid pagination cursor") from exc


# --- keyset predicates ------------------------------------------------------
# Written as OR/AND rather than a row-wise ``(a, b) < (c, d)`` so the bind
# parameter types are the column types and the planner keeps using the
# (created_at DESC, id DESC) index.


def keyset_after_desc(
    ts_col: SQLColumnExpression[Any], id_col: SQLColumnExpression[Any], ts: datetime, ident: Any
) -> ColumnElement[bool]:
    """Rows strictly after the cursor in DESC (newest-first) order."""
    return or_(ts_col < ts, and_(ts_col == ts, id_col < ident))


# --- page assembly ----------------------------------------------------------


def paginate[R](
    rows: Sequence[R], limit: int, cursor_of: Callable[[R], str]
) -> tuple[list[R], Page]:
    """Split an over-fetched result (``limit + 1`` rows) into a page.

    ``has_more`` comes from the extra row rather than a COUNT, so knowing whether
    a next page exists costs one row instead of one full scan.
    """
    has_more = len(rows) > limit
    data = list(rows[:limit])
    next_cursor = cursor_of(data[-1]) if has_more and data else None
    return data, Page(limit=limit, has_more=has_more, next_cursor=next_cursor)


def envelope(request: Request, data: Sequence[Any], page: Page) -> dict[str, Any]:
    return {"data": list(data), "page": page, "meta": Meta(request_id=request_id(request))}


def single_page(request: Request, data: Sequence[Any], limit: int) -> dict[str, Any]:
    """Envelope for a bounded, non-paginated collection (a fixed-length time
    series, a worker's recent heartbeats). Same shape, no cursor to follow."""
    return envelope(request, data, Page(limit=limit, has_more=False, next_cursor=None))
