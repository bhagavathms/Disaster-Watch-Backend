"""
event_streamer.py
Real-Time Event Streamer -- Simulation Mode
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Reads the four local JSON data files produced by Module 1 and inserts
events into MongoDB one-by-one with a configurable delay between each
insertion, simulating a live disaster event feed.

The streamer tags every document it inserts with:
    processed_at  = datetime.utcnow()   (set at insert time, not data time)

This means the SSE endpoint in Module 6 (/stream/events) will see each
event arrive in real-time as it is inserted, giving the frontend a
realistic drip-feed rather than a bulk dump.

Usage:
    python event_streamer.py                        # 0.5s delay, all events
    python event_streamer.py --delay 1.0            # 1 second between events
    python event_streamer.py --delay 0.1 --scale 2  # fast, double the events
    python event_streamer.py --dry-run              # print events, no MongoDB
    python event_streamer.py --mongo-uri mongodb://localhost:28020/
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("EventStreamer")

# ── Defaults ───────────────────────────────────────────────────────────────────
_DEFAULT_MONGO_URI  = "mongodb://localhost:28020/"
_DEFAULT_DATABASE   = "disaster_db"
_DEFAULT_COLLECTION = "disaster_events"

_DATA_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data")
)

_FILE_MAP = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}

# Severity thresholds mirror kafka-consumer/config.py
_THRESHOLDS = {
    "earthquake": {"min": 1.5,  "medium": 3.0,   "high": 5.0,    "critical": 7.0},
    "fire":       {"min": 5.0,  "medium": 100.0,  "high": 2000.0, "critical": 7000.0},
    "flood":      {"min": 5.0,  "medium": 50.0,   "high": 200.0,  "critical": 800.0},
    "storm":      {"min": 35.0, "medium": 62.0,   "high": 120.0,  "critical": 200.0},
}


# ── Classification ─────────────────────────────────────────────────────────────

def _classify(etype: str, severity_raw: float) -> str | None:
    t = _THRESHOLDS.get(etype, {})
    if severity_raw < t.get("min", 0):
        return None
    if severity_raw >= t.get("critical", float("inf")):
        return "CRITICAL"
    if severity_raw >= t.get("high", float("inf")):
        return "HIGH"
    if severity_raw >= t.get("medium", float("inf")):
        return "MEDIUM"
    return "LOW"


# ── Data loading ───────────────────────────────────────────────────────────────

def _load_events(data_dir: str, scale: int) -> list[dict]:
    """
    Load all four JSON files, classify severity, filter below-threshold events,
    sort by original timestamp, and optionally replicate via scale multiplier.
    """
    all_events: list[dict] = []

    for etype, fname in _FILE_MAP.items():
        path = os.path.join(data_dir, fname)
        try:
            with open(path, encoding="utf-8") as fh:
                raw = json.load(fh)
        except FileNotFoundError:
            log.warning("Data file not found: %s — skipping", path)
            continue

        for ev in raw:
            level = _classify(etype, ev.get("severity_raw", 0.0))
            if level is None:
                continue
            all_events.append({
                "event_id":       ev["event_id"],
                "event_type":     ev["event_type"],
                "severity_level": level,
                "severity_raw":   ev.get("severity_raw"),
                "latitude":       ev["latitude"],
                "longitude":      ev["longitude"],
                "event_time":     ev["timestamp"],
                "source":         ev["source"],
            })

    # Scale: duplicate events with new IDs if scale > 1
    if scale > 1:
        base = list(all_events)
        for _ in range(scale - 1):
            for ev in base:
                copy = dict(ev)
                copy["event_id"] = str(uuid.uuid4())
                all_events.append(copy)

    # Sort by event_time so the stream plays back in chronological order
    all_events.sort(key=lambda e: e["event_time"])
    return all_events


# ── MongoDB document builder ───────────────────────────────────────────────────

def _build_doc(ev: dict) -> dict:
    """Convert a flat event dict into the MongoDB document format used by Module 4."""
    now = datetime.now(tz=timezone.utc)

    try:
        event_time = datetime.strptime(ev["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, KeyError):
        event_time = now

    return {
        "_id":            ev["event_id"],
        "event_type":     ev["event_type"],
        "severity_level": ev["severity_level"],
        "severity_raw":   ev.get("severity_raw"),
        "location": {
            "type": "Point",
            "coordinates": [ev["longitude"], ev["latitude"]],
        },
        "event_time":    event_time,
        "processed_at":  now,          # set at insert time — drives SSE polling
        "source":        ev["source"],
    }


# ── Main loop ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    events = _load_events(args.data_dir or _DATA_DIR, args.scale)
    if not events:
        log.error("No events loaded. Run Module 1 first: python main.py")
        sys.exit(1)

    log.info("Loaded %d events — delay=%.2fs  dry_run=%s",
             len(events), args.delay, args.dry_run)

    if args.dry_run:
        for i, ev in enumerate(events[:10]):
            log.info("[DRY-RUN] %d  %s", i + 1, ev)
        log.info("... (dry-run: no MongoDB writes)")
        return

    # ── Connect to MongoDB ─────────────────────────────────────────────────────
    try:
        from pymongo import MongoClient, ASCENDING
        from pymongo.errors import DuplicateKeyError
    except ImportError:
        log.error("pymongo not installed.  Run: pip install pymongo>=4.0.0")
        sys.exit(1)

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)
    try:
        client.admin.command("ping")
    except Exception as exc:
        log.error("Cannot reach MongoDB at %s: %s", args.mongo_uri, exc)
        sys.exit(1)

    col = client[_DEFAULT_DATABASE][_DEFAULT_COLLECTION]
    log.info("[MONGO] Connected to %s", args.mongo_uri)

    # ── Stream events ──────────────────────────────────────────────────────────
    inserted = 0
    skipped  = 0

    log.info("Streaming %d events into MongoDB (Ctrl-C to stop) ...", len(events))

    try:
        for i, ev in enumerate(events):
            doc = _build_doc(ev)
            try:
                col.insert_one(doc)
                inserted += 1
                if inserted % 50 == 0 or inserted <= 5:
                    log.info(
                        "[%d/%d] inserted  type=%-12s severity=%-8s event_id=%s",
                        inserted, len(events),
                        ev["event_type"], ev["severity_level"], ev["event_id"][:8],
                    )
            except DuplicateKeyError:
                skipped += 1

            time.sleep(args.delay)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")

    log.info("Done. inserted=%d  skipped(dup)=%d", inserted, skipped)
    client.close()


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Real-time event streamer: inserts events into MongoDB with delay",
    )
    p.add_argument(
        "--mongo-uri",
        default=_DEFAULT_MONGO_URI,
        metavar="URI",
        help=f"MongoDB URI (default: {_DEFAULT_MONGO_URI})",
    )
    p.add_argument(
        "--delay",
        type=float,
        default=0.5,
        metavar="SECONDS",
        help="Seconds to wait between each event insertion (default: 0.5)",
    )
    p.add_argument(
        "--scale",
        type=int,
        default=1,
        metavar="N",
        help="Multiply the event set N times (default: 1)",
    )
    p.add_argument(
        "--data-dir",
        default=None,
        metavar="PATH",
        help="Path to the data/ directory (default: <project-root>/data/)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print first 10 events without writing to MongoDB",
    )
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
