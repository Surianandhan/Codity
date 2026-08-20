"""One error envelope for every failure path, including FastAPI's own 404/405/422.

Without the handlers below, FastAPI leaks a bare {"detail": ...} for its built-in
errors while domain errors use the envelope -- two shapes for one API.
"""

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.domain.errors import DomainError


def envelope(
    code: str, message: str, request_id: str, details: list[Any] | None = None
) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "details": details or [],
            "request_id": request_id,
        }
    }


def _rid(request: Request) -> str:
    return getattr(request.state, "request_id", "-")


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(DomainError)
    async def _domain(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status,
            content=envelope(exc.code, exc.message, _rid(request), exc.details),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [
            {"field": ".".join(str(p) for p in e["loc"][1:]), "issue": e["msg"]}
            for e in exc.errors()
        ]
        return JSONResponse(
            status_code=422,
            content=envelope(
                "validation_error", "request validation failed", _rid(request), details
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        codes = {401: "unauthenticated", 403: "permission_denied", 404: "not_found",
                 405: "method_not_allowed", 429: "rate_limited"}
        return JSONResponse(
            status_code=exc.status_code,
            content=envelope(
                codes.get(exc.status_code, "error"), str(exc.detail), _rid(request)
            ),
            headers=getattr(exc, "headers", None),
        )
