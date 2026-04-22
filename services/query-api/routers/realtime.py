"""
realtime.py
Real-Time Streaming Router -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Endpoints:
    GET  /stream/events   -- SSE stream; pushes new MongoDB events as they arrive
    POST /stream/start    -- start the event_streamer simulation process
    POST /stream/stop     -- stop the event_streamer simulation process
    GET  /stream/status   -- simulation process status + last-event timestamp
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

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

log = logging.getLogger(__name__)

router = APIRouter(prefix="/stream", tags=["Real-Time Stream"])

# ── Simulation process state ───────────────────────────────────────────────────
_sim_process: subprocess.Popen | None = None

_STREAMER_PATH = os.path.normpath(
    os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "..", "realtime-service", "event_streamer.py",
    )
)


# ── SSE helpers ────────────────────────────────────────────────────────────────

def _to_sse(data: dict) -> str:
    """Encode a dict as a single SSE data frame."""
    payload = json.dumps(data, default=str)
    return f"data: {payload}\n\n"


async def _event_generator(request: Request) -> AsyncIterator[str]:
    """
    Poll MongoDB every second for documents inserted after `last_seen`.
    Yields SSE frames to the connected client until the client disconnects.
    """
    mongo = request.app.state.mongo
    last_seen: datetime = datetime.now(tz=timezone.utc)

    # Send a 'connected' heartbeat so the frontend knows the stream is live
    yield _to_sse({"type": "connected", "timestamp": last_seen.isoformat()})

    while True:
        if await request.is_disconnected():
            log.info("[SSE] Client disconnected")
            break

        try:
            new_events = mongo.get_new_since(last_seen)
        except Exception as exc:
            log.warning("[SSE] MongoDB poll error: %s", exc)
            await asyncio.sleep(2)
            continue

        for ev in new_events:
            yield _to_sse({"type": "event", "data": ev})
            # Advance cursor so we don't re-send the same document
            ts = ev.get("processed_at")
            if ts:
                if isinstance(ts, str):
                    ts = datetime.fromisoformat(ts)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts > last_seen:
                    last_seen = ts

        await asyncio.sleep(1)


# ── Endpoints ──────────────────────────────────────────────────────────────────

@router.get(
    "/events",
    summary="Live event stream (SSE)",
    description=(
        "Server-Sent Events stream. Connect with `EventSource('/stream/events')`. "
        "Pushes new disaster events from MongoDB as they are written by the pipeline. "
        "Each frame is a JSON object with `type: 'event'` and a `data` payload."
    ),
)
async def stream_events(request: Request):
    return StreamingResponse(
        _event_generator(request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # disable nginx buffering
        },
    )


@router.post(
    "/start",
    summary="Start simulation streamer",
    description=(
        "Launches `event_streamer.py` as a background process. "
        "The streamer reads local JSON data files and inserts events into MongoDB "
        "one-by-one with a configurable delay, simulating a realistic real-time feed."
    ),
)
async def start_simulation(
    delay: float = 0.5,
    scale: int   = 1,
):
    global _sim_process

    if _sim_process and _sim_process.poll() is None:
        return {"status": "already_running", "pid": _sim_process.pid}

    if not os.path.exists(_STREAMER_PATH):
        return {
            "status": "error",
            "detail": f"event_streamer.py not found at {_STREAMER_PATH}",
        }

    cmd = [sys.executable, _STREAMER_PATH, "--delay", str(delay), "--scale", str(scale)]
    _sim_process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    log.info("[STREAM] Simulation started  pid=%d  delay=%.2fs  scale=%d",
             _sim_process.pid, delay, scale)
    return {"status": "started", "pid": _sim_process.pid, "delay": delay, "scale": scale}


@router.post(
    "/stop",
    summary="Stop simulation streamer",
    description="Terminates the background event_streamer.py process if it is running.",
)
async def stop_simulation():
    global _sim_process

    if _sim_process is None or _sim_process.poll() is not None:
        return {"status": "not_running"}

    _sim_process.terminate()
    try:
        _sim_process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        _sim_process.kill()

    pid = _sim_process.pid
    _sim_process = None
    log.info("[STREAM] Simulation stopped  pid=%d", pid)
    return {"status": "stopped", "pid": pid}


@router.get(
    "/status",
    summary="Simulation status",
    description="Returns whether the event_streamer.py simulation is currently running.",
)
async def simulation_status():
    global _sim_process

    running = _sim_process is not None and _sim_process.poll() is None
    return {
        "simulation_running": running,
        "pid": _sim_process.pid if running else None,
    }
