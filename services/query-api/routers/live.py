"""
routers/live.py
Live Event Endpoints -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Endpoints:
    GET  /live/snapshot   -- all currently active live events (initial map load)
    GET  /live/stream     -- SSE stream; pushes new live events as they are generated
    POST /live/start      -- start the live_generator.py subprocess
    POST /live/stop       -- stop the live_generator.py subprocess
    GET  /live/status     -- generator running state

Architecture:
    live_generator.py inserts one synthetic event into MongoDB `live_events`
    every N seconds (default 30).  A TTL index expires documents after 8 hours,
    keeping the collection bounded at ~960 events — small enough for the frontend
    to render without clustering.

    /live/snapshot  →  full collection dump (for initial page load)
    /live/stream    →  polls `processed_at > last_seen` every second via SSE
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone
from typing import AsyncIterator

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

log = logging.getLogger(__name__)

router = APIRouter(prefix="/live", tags=["Live Events"])

# ── Generator process state ────────────────────────────────────────────────────

_gen_process: subprocess.Popen | None = None

_GENERATOR_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "realtime-service", "live_generator.py",
    )
)


# ── SSE helpers ────────────────────────────────────────────────────────────────

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, default=str)}\n\n"


async def _live_event_generator(request: Request) -> AsyncIterator[str]:
    """
    Poll live_events every second for documents newer than `last_seen`.
    Yields SSE frames until the client disconnects.
    """
    mongo = request.app.state.mongo
    last_seen: datetime = datetime.now(tz=timezone.utc)

    yield _sse({"type": "connected", "timestamp": last_seen.isoformat()})

    while True:
        if await request.is_disconnected():
            log.info("[LIVE-SSE] Client disconnected")
            break

        try:
            new_events = mongo.get_live_since(last_seen)
        except Exception as exc:
            log.warning("[LIVE-SSE] MongoDB poll error: %s", exc)
            await asyncio.sleep(2)
            continue

        for ev in new_events:
            yield _sse({"type": "event", "data": ev})
            ts = ev.get("processed_at")
            if ts:
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts > last_seen:
                    last_seen = ts

        await asyncio.sleep(1)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get(
    "/snapshot",
    summary="Live event snapshot",
    description=(
        "Returns all disaster events currently in the live window (last 8 hours). "
        "The collection is bounded by a MongoDB TTL index, so the response is "
        "always ≤ ~960 events — safe for direct frontend rendering without clustering. "
        "Call this once on page load, then subscribe to GET /live/stream for updates."
    ),
)
def get_live_snapshot(request: Request):
    mongo = request.app.state.mongo
    try:
        events = mongo.get_live_snapshot()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}")
    return {"count": len(events), "events": events}


@router.get(
    "/stream",
    summary="Live event SSE stream",
    description=(
        "Server-Sent Events stream for live disaster events. "
        "Connect with `new EventSource('/live/stream')`. "
        "Each frame is `{type: 'event', data: {...}}`. "
        "An initial `{type: 'connected'}` frame confirms the connection. "
        "Polls MongoDB every second — only new events are delivered."
    ),
)
async def stream_live(request: Request):
    return StreamingResponse(
        _live_event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control":    "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@router.post(
    "/start",
    summary="Start live generator",
    description=(
        "Launches live_generator.py as a background subprocess. "
        "`rate` controls seconds between events (default 30 → ~960 events in the "
        "8-hour live window).  Smaller values produce a denser map."
    ),
)
async def start_live_generator(rate: float = 10.0):
    global _gen_process

    if _gen_process and _gen_process.poll() is None:
        return {"status": "already_running", "pid": _gen_process.pid, "rate": rate}

    if not os.path.exists(_GENERATOR_PATH):
        raise HTTPException(
            status_code=500,
            detail=f"live_generator.py not found at {_GENERATOR_PATH}",
        )

    cmd = [sys.executable, _GENERATOR_PATH, "--rate", str(rate)]
    _gen_process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log.info("[LIVE] Generator started  pid=%d  rate=%.1fs", _gen_process.pid, rate)
    return {"status": "started", "pid": _gen_process.pid, "rate": rate}


@router.post(
    "/stop",
    summary="Stop live generator",
    description="Terminates the live_generator.py subprocess if it is running.",
)
async def stop_live_generator():
    global _gen_process

    if _gen_process is None or _gen_process.poll() is not None:
        return {"status": "not_running"}

    _gen_process.terminate()
    try:
        _gen_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        _gen_process.kill()

    pid = _gen_process.pid
    _gen_process = None
    log.info("[LIVE] Generator stopped  pid=%d", pid)
    return {"status": "stopped", "pid": pid}


@router.get(
    "/status",
    summary="Live generator status",
    description="Returns whether the live event generator is currently running.",
)
async def live_generator_status():
    running = _gen_process is not None and _gen_process.poll() is None
    return {
        "generator_running": running,
        "pid":               _gen_process.pid if running else None,
    }
