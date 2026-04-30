from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .api.routes import router
from .control_plane import reconcile_orphaned_local_runs
from .settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sentry / OpenTelemetry initialisation (no-op when DSN is absent)
# ---------------------------------------------------------------------------
_SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
if _SENTRY_DSN:
    try:
        import sentry_sdk
        from sentry_sdk.integrations.fastapi import FastApiIntegration
        from sentry_sdk.integrations.starlette import StarletteIntegration
        sentry_sdk.init(
            dsn=_SENTRY_DSN,
            traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
            environment=os.environ.get("SENTRY_ENVIRONMENT", "production"),
            integrations=[StarletteIntegration(), FastApiIntegration()],
        )
        logger.info("Sentry SDK initialised.")
    except Exception:
        logger.warning("sentry-sdk not installed or init failed — continuing without Sentry.")


def _run_startup_reconciliation(app: FastAPI) -> None:
    app.state.control_plane_startup_error = None
    try:
        reconcile_orphaned_local_runs()
    except Exception as exc:
        # Keep the API available for read-only/diagnostic routes even when the
        # Databricks-backed control plane cannot be reached during startup.
        app.state.control_plane_startup_error = str(exc)
        logger.exception("Control-plane startup reconciliation failed.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _run_startup_reconciliation(app)
    yield


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.state.control_plane_startup_error = None

app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-API-Key"],
)

app.include_router(router, prefix=settings.api_prefix)
