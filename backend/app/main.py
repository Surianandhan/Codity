import logging

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text

from app.api.errors import register_error_handlers
from app.api.middleware.request_context import RequestContextMiddleware
from app.api.routers import auth, dlq, jobs, metrics, projects, schedules, system, workers
from app.config import get_settings
from app.db.session import get_engine


def configure_logging() -> None:
    s = get_settings()
    logging.basicConfig(format="%(message)s", level=getattr(logging, s.log_level.upper(), 20))
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer()
            if s.log_json
            else structlog.dev.ConsoleRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, s.log_level.upper(), 20)
        ),
    )


def create_app() -> FastAPI:
    configure_logging()
    s = get_settings()
    app = FastAPI(
        title="Codity Job Scheduler",
        version="0.1.0",
        description="Distributed job scheduling on PostgreSQL.",
        openapi_url="/api/v1/openapi.json",
        docs_url="/docs",
    )
    app.add_middleware(RequestContextMiddleware)
    # Registered explicitly so the API works when hit directly on :8000 and not
    # only through the Vite dev proxy.
    app.add_middleware(
        CORSMiddleware,
        allow_origins=s.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=["X-Request-ID"],
    )
    register_error_handlers(app)

    routers = (
        auth.router,
        projects.router,
        jobs.router,
        schedules.router,
        workers.router,
        dlq.router,
        metrics.router,
        system.router,
    )
    for r in routers:
        app.include_router(r, prefix="/api/v1")

    @app.get("/healthz", tags=["system"])
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", tags=["system"])
    async def readyz() -> dict[str, str]:
        async with get_engine().connect() as c:
            await c.execute(text("select 1"))
        return {"status": "ready"}

    return app


app = create_app()
