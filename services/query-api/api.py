"""
api.py
FastAPI Application Entry Point -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Responsibilities:
    - Create the FastAPI application instance
    - Manage DB client lifecycle via @asynccontextmanager lifespan
    - Register /events and /analytics routers
    - Expose GET /health for operational readiness checks
    - Expose GET / root redirect to /docs

Run:
    uvicorn api:app --host 127.0.0.1 --port 8000 --reload

Environment variables (all optional, override defaults):
    MONGO_URI       mongodb://localhost:27017/
    MONGO_DB        disaster_db
    MONGO_COL       disaster_events
    POSTGRES_DSN    postgresql://postgres:postgres@localhost:5432/disaster_dw
    API_HOST        127.0.0.1
    API_PORT        8000
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

from config import settings
from models.schemas import HealthResponse, ServiceStatus
from mongo_client import MongoReadClient
from postgres_client import PostgresReadClient
from routers import analytics, events, live, map as map_router, realtime

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


# ── Lifespan: open / close DB connections ─────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs once at startup (before yield) and once at shutdown (after yield).
    Stores shared client instances on app.state so dependency functions
    in dependencies.py can retrieve them via request.app.state.
    """
    # ── Startup ───────────────────────────────────────────────────────────────
    log.info("[STARTUP] Connecting to MongoDB ...")
    mongo = MongoReadClient(settings.mongo)
    try:
        mongo.connect()
        log.info("[STARTUP] MongoDB OK")
        try:
            mongo.ensure_live_indexes()
        except Exception as exc:
            log.warning("[STARTUP] Live index creation skipped: %s", exc)
    except (ImportError, ConnectionError) as exc:
        log.warning("[STARTUP] MongoDB unavailable: %s", exc)
        # App starts in degraded mode; /events endpoints will return 503

    log.info("[STARTUP] Connecting to PostgreSQL ...")
    postgres = PostgresReadClient(settings.postgres)
    try:
        postgres.connect()
        log.info("[STARTUP] PostgreSQL OK")
        try:
            postgres.ensure_map_indexes()
        except Exception as exc:
            log.warning("[STARTUP] Map index creation skipped: %s", exc)
    except (ImportError, ConnectionError) as exc:
        log.warning("[STARTUP] PostgreSQL unavailable: %s", exc)
        # App starts in degraded mode; /analytics endpoints will return 503

    app.state.mongo    = mongo
    app.state.postgres = postgres

    yield   # ── Application running ──────────────────────────────────────────

    # ── Shutdown ──────────────────────────────────────────────────────────────
    log.info("[SHUTDOWN] Closing MongoDB connection ...")
    mongo.close()
    log.info("[SHUTDOWN] Closing PostgreSQL connection pool ...")
    postgres.close()
    log.info("[SHUTDOWN] Clean shutdown complete.")


# ── FastAPI app ────────────────────────────────────────────────────────────────

app = FastAPI(
    title       = "Disaster Monitoring Query API",
    description = (
        "Read-only REST API for the Distributed NoSQL-Based Disaster Monitoring "
        "and Analytics System (Module 6).\n\n"
        "**MongoDB endpoints** (`/events/*`) query individual processed disaster "
        "events from the operational store.\n\n"
        "**PostgreSQL endpoints** (`/analytics/*`) query pre-aggregated star-schema "
        "data for trend and location analytics."
    ),
    version     = "1.0.0",
    lifespan    = lifespan,
    docs_url    = "/docs",
    redoc_url   = "/redoc",
)

# Allow all origins in development (tighten for production)
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_methods     = ["GET", "POST"],
    allow_headers     = ["*"],
)


# ── Routers ───────────────────────────────────────────────────────────────────

app.include_router(events.router)
app.include_router(analytics.router)
app.include_router(map_router.router)
app.include_router(realtime.router)
app.include_router(live.router)


# ── Root ──────────────────────────────────────────────────────────────────────

@app.get("/", include_in_schema=False)
def root():
    """Redirect bare root to the interactive API docs."""
    return RedirectResponse(url="/docs")


# ── Health ────────────────────────────────────────────────────────────────────

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Service Health",
    description=(
        "Checks connectivity to MongoDB and PostgreSQL. "
        "Returns overall status ('ok' / 'degraded') plus per-service detail."
    ),
    tags=["Health"],
)
def health():
    mongo_ok    = app.state.mongo.is_connected()    if app.state.mongo    else False
    postgres_ok = app.state.postgres.is_connected() if app.state.postgres else False

    overall = "ok" if (mongo_ok and postgres_ok) else "degraded"

    return HealthResponse(
        status     = overall,
        mongodb    = ServiceStatus(status="ok"    if mongo_ok    else "error",
                                   detail=None    if mongo_ok    else "MongoDB ping failed"),
        postgresql = ServiceStatus(status="ok"    if postgres_ok else "error",
                                   detail=None    if postgres_ok else "PostgreSQL ping failed"),
    )


# ── Dev entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api:app",
        host    = settings.host,
        port    = settings.port,
        reload  = True,
        log_level = "info",
    )
