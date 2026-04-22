"""
live_generator.py
Live Disaster Event Generator
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Generates synthetic disaster events continuously and inserts them into
a dedicated MongoDB `live_events` collection.  A TTL index on `event_time`
automatically expires documents after 8 hours, so the collection never
grows beyond ~960 events at the default 30-second rate — well within
the 1000-event frontend budget.

Rate:      --rate N  (seconds between events, default 30)
Steady state at 30 s:  28 800 s / 30 s = 960 live events in the window

Geographic distributions are weighted hotspot clusters that mirror real
disaster-prone regions.  Severity is drawn from a realistic probability
distribution (LOW 40 % / MEDIUM 35 % / HIGH 18 % / CRITICAL 7 %).

Usage:
    python live_generator.py                         # 1 event / 30 s
    python live_generator.py --rate 10               # 1 event / 10 s (~2880 window)
    python live_generator.py --dry-run               # print without MongoDB writes
    python live_generator.py --mongo-uri mongodb://localhost:28020/
"""

from __future__ import annotations

import argparse
import logging
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
log = logging.getLogger("LiveGenerator")

_DEFAULT_MONGO_URI  = "mongodb://localhost:28020/"
_DEFAULT_DATABASE   = "disaster_db"
_LIVE_COLLECTION    = "live_events"
_TTL_SECONDS        = 8 * 3600   # 8 hours

# ── Geographic hotspot clusters ────────────────────────────────────────────────
# Each tuple: (lat_center, lon_center, lat_std, lon_std, weight)
# Weight controls how likely this cluster is chosen relative to others.

_EARTHQUAKE_CLUSTERS = [
    # Ring of Fire
    (36.0,   138.0,  5.0,  6.0,  18),   # Japan / NE Asia
    (12.0,   125.0,  4.0,  5.0,  10),   # Philippines
    (-3.0,   122.0,  5.0, 10.0,  12),   # Indonesia
    (-30.0,  -71.0,  8.0,  2.0,  12),   # Chile / Andes
    (55.0,  -155.0,  4.0, 15.0,   8),   # Alaska / Aleutians
    (13.0,   -89.0,  3.0,  4.0,   6),   # Central America
    (-40.0,  175.0,  4.0,  4.0,   5),   # New Zealand
    # Alpine-Himalayan Belt
    (30.0,    85.0,  3.0, 10.0,   8),   # Himalayas / Nepal / Tibet
    (38.0,    36.0,  3.0,  8.0,   7),   # Turkey / Anatolia
    (39.0,    16.0,  3.0,  6.0,   5),   # Italy / Greece
    (33.0,    57.0,  4.0,  6.0,   5),   # Iran
    (-15.0,  -75.0,  5.0,  4.0,   4),   # Peru
]

_FIRE_CLUSTERS = [
    (37.0,  -120.0,  2.0,  3.0,  10),   # California / western USA
    (40.0,    15.0,  3.0,  8.0,   8),   # Mediterranean (Iberia, Italy, Greece)
    (-8.0,   -60.0,  5.0,  8.0,  12),   # Amazon basin
    (15.0,   100.0,  5.0, 12.0,  10),   # SE Asia (Thailand, Myanmar)
    (-25.0,  135.0,  6.0, 12.0,  10),   # Australia interior
    (58.0,    80.0,  3.0, 10.0,   6),   # W Siberia (taiga)
    (-5.0,    25.0,  8.0, 12.0,  10),   # Central Africa
    (-22.0,   28.0,  5.0,  8.0,   8),   # Southern Africa (savanna)
    (50.0,   -110.0, 3.0,  5.0,   6),   # Canadian prairies / Rockies
]

_FLOOD_CLUSTERS = [
    (23.0,   90.0,  3.0,  4.0, 14),    # Bangladesh / Brahmaputra delta
    (30.0,  117.0,  3.0,  4.0, 10),    # Yangtze / Eastern China
    (36.0,  -90.0,  5.0,  3.0,  8),    # Mississippi / Midwest USA
    (12.0,  105.0,  3.0,  4.0,  8),    # Mekong delta / Vietnam
    (52.0,    8.0,  3.0,  5.0,  6),    # Rhine / European lowlands
    (26.0,   83.0,  3.0,  6.0, 10),    # Ganges / Northern India
    ( 6.0,    7.0,  4.0,  6.0,  8),    # Niger delta / West Africa
    (-5.0,  -60.0,  5.0,  8.0,  6),    # Amazon floodplain
    (18.0,   96.0,  3.0,  3.0,  6),    # Irrawaddy delta / Myanmar
    (15.0,   36.0,  4.0,  5.0,  6),    # Nile / Sudan-Ethiopia
]

_STORM_CLUSTERS = [
    (18.0,  -75.0,  5.0, 12.0, 14),    # Atlantic hurricane (Caribbean)
    (25.0,  -90.0,  4.0,  6.0,  8),    # Gulf of Mexico
    (18.0,  132.0,  6.0, 12.0, 16),    # NW Pacific typhoon belt
    (15.0,   87.0,  4.0,  5.0, 10),    # Bay of Bengal cyclones
    (-15.0,  65.0,  8.0, 12.0,  8),    # South Indian Ocean
    (-15.0,-155.0,  5.0, 10.0,  6),    # South Pacific (Fiji / Vanuatu)
    (22.0,  115.0,  4.0,  6.0,  8),    # South China Sea
]

_CLUSTERS = {
    "earthquake": _EARTHQUAKE_CLUSTERS,
    "fire":       _FIRE_CLUSTERS,
    "flood":      _FLOOD_CLUSTERS,
    "storm":      _STORM_CLUSTERS,
}

# Type selection weights — earthquakes and storms are more frequent in real data
_TYPE_WEIGHTS = [
    ("earthquake", 30),
    ("storm",      27),
    ("flood",      25),
    ("fire",       18),
]
_TYPES, _TYPE_W = zip(*_TYPE_WEIGHTS)

# Severity distribution — realistic global breakdown
_SEVERITY_CHOICES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
_SEVERITY_WEIGHTS = [40, 35, 18, 7]

_SOURCES = {
    "earthquake": ["USGS", "EMSC", "JMA", "GeoNet"],
    "fire":       ["NASA-FIRMS", "EFFIS", "NIFC", "Copernicus"],
    "flood":      ["USGS", "Copernicus-EMS", "Dartmouth-FO", "GDACS"],
    "storm":      ["NHC", "JTWC", "IMD", "BOM", "Meteo-France"],
}


# ── Event generation ───────────────────────────────────────────────────────────

def _sample_location(event_type: str) -> tuple[float, float]:
    """Draw (lat, lon) from the weighted cluster distribution for event_type."""
    clusters = _CLUSTERS[event_type]
    weights  = [c[4] for c in clusters]
    cluster  = random.choices(clusters, weights=weights, k=1)[0]
    lat_c, lon_c, lat_std, lon_std, _ = cluster

    lat = random.gauss(lat_c, lat_std)
    lon = random.gauss(lon_c, lon_std)

    # Clamp to valid WGS-84 range
    lat = max(-85.0, min(85.0,  lat))
    lon = max(-180.0, min(180.0, lon))
    return round(lat, 5), round(lon, 5)


def _generate_event(processed_at=None) -> dict:
    """Produce one synthetic live event dict."""
    event_type  = random.choices(_TYPES, weights=_TYPE_W, k=1)[0]
    severity    = random.choices(_SEVERITY_CHOICES, weights=_SEVERITY_WEIGHTS, k=1)[0]
    lat, lon    = _sample_location(event_type)
    source      = random.choice(_SOURCES[event_type])
    ts          = processed_at or datetime.now(tz=timezone.utc)
    event_id    = str(uuid.uuid4())

    return {
        "_id":            event_id,
        "event_id":       event_id,
        "event_type":     event_type,
        "severity_level": severity,
        "location": {
            "type":        "Point",
            "coordinates": [lon, lat],
        },
        "latitude":      lat,
        "longitude":     lon,
        "event_time":    ts,
        "processed_at":  ts,
        "source":        source,
        "is_live":       True,
    }


# ── Main loop ──────────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    if args.dry_run:
        log.info("[DRY-RUN] Generating sample events (no MongoDB writes)")
        for i in range(5):
            ev = _generate_event()
            log.info("[%d] %s", i + 1, {k: v for k, v in ev.items() if k != "location"})
        return

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

    col = client[_DEFAULT_DATABASE][_LIVE_COLLECTION]

    # Ensure TTL index — expires documents 8 hours after event_time
    col.create_index(
        [("event_time", ASCENDING)],
        expireAfterSeconds=_TTL_SECONDS,
        name="ttl_event_time",
        background=True,
    )
    # Ensure processed_at index so SSE polling stays fast
    col.create_index(
        [("processed_at", ASCENDING)],
        name="idx_processed_at",
        background=True,
    )

    # ── Seed: pre-populate with N historical events ───────────────────────────
    seed_count = random.randint(10, 20)
    log.info("[LIVE] Seeding %d initial events spread over the last 8 hours ...", seed_count)
    now = datetime.now(tz=timezone.utc)
    for i in range(seed_count):
        # Spread seed events evenly across the last 8 hours
        offset_seconds = int((i / seed_count) * _TTL_SECONDS)
        seed_ts = datetime.fromtimestamp(now.timestamp() - _TTL_SECONDS + offset_seconds, tz=timezone.utc)
        ev = _generate_event(processed_at=seed_ts)
        try:
            col.insert_one(ev)
        except DuplicateKeyError:
            pass
    log.info("[LIVE] Seed complete.")

    log.info(
        "[LIVE] Generator running  rate=1 event/%ds  TTL=%dh  collection=%s",
        args.rate, _TTL_SECONDS // 3600, _LIVE_COLLECTION,
    )

    inserted = 0
    try:
        while True:
            ev = _generate_event()
            try:
                col.insert_one(ev)
                inserted += 1
                log.info(
                    "[%d] %s  severity=%-8s  lat=%.3f  lon=%.3f  source=%s",
                    inserted, ev["event_type"], ev["severity_level"],
                    ev["latitude"], ev["longitude"], ev["source"],
                )
            except DuplicateKeyError:
                pass

            time.sleep(args.rate)

    except KeyboardInterrupt:
        log.info("Stopped.  Total inserted: %d", inserted)
    finally:
        client.close()


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Live disaster event generator — inserts synthetic events into MongoDB",
    )
    p.add_argument(
        "--mongo-uri", default=_DEFAULT_MONGO_URI, metavar="URI",
        help=f"MongoDB connection URI (default: {_DEFAULT_MONGO_URI})",
    )
    p.add_argument(
        "--rate", type=float, default=10.0, metavar="SECONDS",
        help="Seconds between events (default: 10 → ~2880 events in 8-h TTL window)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print sample events without writing to MongoDB",
    )
    return p.parse_args()


if __name__ == "__main__":
    run(_parse_args())
