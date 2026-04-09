"""
transform.py
Transformation Layer -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Converts flat MongoDB extraction records into star-schema compatible rows.

For each raw event this module produces a TransformedEvent dataclass that
carries all data needed to populate the four star-schema tables:

    dim_event_type   <- event_type_name
    dim_location     <- latitude, longitude, country, region
    dim_time         <- date, day, month, year, hour
    fact_disaster_events <- event_id, severity_level, source
                            + FK references to the three dims above

Geographic classification:
    latitude/longitude are mapped to (country, region) using bounding-box
    lookups covering the known disaster-prone regions in the dataset.
    Coordinates that match no box fall back to a hemisphere label.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

log = logging.getLogger(__name__)


# ── Geographic bounding-box classifier ───────────────────────────────────────
# Each entry: (lat_min, lat_max, lon_min, lon_max, country, region)
# Ordered from most-specific to least-specific so the first match wins.

_REGION_BOXES: list[tuple] = [
    # ── East Asia / Pacific ──────────────────────────────────────────────────
    (30.0,  45.0,  129.0,  146.0, "Japan",          "East Asia"),
    (18.0,  31.0,  127.0,  144.0, "Japan",          "Western Pacific"),
    (10.0,  25.0,  140.0,  160.0, "Micronesia",     "Western Pacific"),
    # ── Southeast Asia ───────────────────────────────────────────────────────
    (-12.0,  6.0,  113.0,  129.0, "Indonesia",      "Southeast Asia"),
    (  5.0, 20.0,  118.0,  132.0, "Philippines",    "Southeast Asia"),
    (  7.0, 14.0,  102.0,  109.0, "Vietnam",        "Southeast Asia"),
    # ── South Asia ───────────────────────────────────────────────────────────
    (20.0,  27.0,   87.0,   93.0, "Bangladesh",     "South Asia"),
    (24.0,  35.0,   63.0,   76.0, "Pakistan",       "South Asia"),
    ( 7.0,  22.0,   80.0,   93.0, "India",          "South Asia"),
    # ── Central Asia / Tibet ─────────────────────────────────────────────────
    (28.0,  40.0,   72.0,  102.0, "China",          "Central Asia"),
    # ── Middle East ──────────────────────────────────────────────────────────
    (34.0,  43.0,   25.0,   44.0, "Turkey",         "Middle East"),
    # ── South America ────────────────────────────────────────────────────────
    (-46.0, -18.0, -80.0,  -62.0, "Chile",          "South America"),
    (-16.0,  6.0,  -66.0,  -43.0, "Brazil",         "South America"),
    (-14.0,  2.0,  -80.0,  -68.0, "Peru",           "South America"),
    # ── North America ────────────────────────────────────────────────────────
    (55.0,  68.0, -160.0, -143.0, "USA",            "North America - Alaska"),
    (32.0,  44.0, -125.0, -113.0, "USA",            "North America - West Coast"),
    (40.0,  50.0, -118.0, -100.0, "USA",            "North America - Mountain"),
    (26.0,  35.0,  -97.0,  -79.0, "USA",            "North America - South"),
    (35.0,  50.0,  -97.0,  -73.0, "USA",            "North America - East"),
    # ── Caribbean ────────────────────────────────────────────────────────────
    (12.0,  26.0,  -84.0,  -58.0, "Caribbean",      "Caribbean"),
    # ── Europe ───────────────────────────────────────────────────────────────
    (34.0,  42.0,   18.0,   28.0, "Greece",         "Southern Europe"),
    (45.0,  52.0,   14.0,   24.0, "Central Europe", "Central Europe"),
    (46.0,  55.0,   -2.0,    8.0, "Western Europe", "Western Europe"),
    (50.0,  60.0,   -5.0,    2.0, "UK",             "Western Europe"),
    # ── Africa ───────────────────────────────────────────────────────────────
    ( 3.0,  12.0,    1.0,   11.0, "Nigeria",        "West Africa"),
    ( 9.0,  22.0,    7.0,   24.0, "Chad / Niger",   "West Africa"),
    ( 3.0,  15.0,   25.0,   40.0, "East Africa",    "East Africa"),
    # ── Oceania / Australia ───────────────────────────────────────────────────
    (-44.0, -10.0,  112.0,  155.0, "Australia",     "Oceania"),
    # ── Siberia / Russia ─────────────────────────────────────────────────────
    (49.0,  72.0,   79.0,  125.0, "Russia",         "Siberia"),
]


def _classify_location(lat: float, lon: float) -> tuple[str, str]:
    """
    Return (country, region) for the given WGS-84 coordinates.
    Falls back to a broad hemisphere label if no bounding box matches.
    """
    for lat_lo, lat_hi, lon_lo, lon_hi, country, region in _REGION_BOXES:
        if lat_lo <= lat <= lat_hi and lon_lo <= lon <= lon_hi:
            return country, region

    # Hemisphere fallback
    ns = "North" if lat >= 0 else "South"
    ew = "East"  if lon >= 0 else "West"
    return "Unknown", f"{ns} {ew} Hemisphere"


# ── Transformed event dataclass ───────────────────────────────────────────────

@dataclass
class TransformedEvent:
    """
    Holds all columns needed to populate the star schema for one event.

    The three dimension sub-groups map directly to their respective tables:
        (event_type_name)           -> dim_event_type
        (latitude, longitude,
         country, region)           -> dim_location
        (date, day, month,
         year, hour)                -> dim_time
        (event_id, severity_level,
         source)                    -> fact_disaster_events (non-FK columns)
    """
    # dim_event_type
    event_type_name: str

    # dim_location
    latitude:   float
    longitude:  float
    country:    str
    region:     str

    # dim_time
    date:   date
    day:    int
    month:  int
    year:   int
    hour:   int

    # fact (non-FK payload)
    event_id:       str
    severity_level: str
    source:         str


# ── Public transform function ─────────────────────────────────────────────────

def transform(raw_events: list[dict]) -> list[TransformedEvent]:
    """
    Convert a list of flat MongoDB extraction records into TransformedEvent
    objects ready for loading into the PostgreSQL star schema.

    Records with missing or unparseable fields are skipped with a warning.

    Args:
        raw_events: Output of extract.extract() -- flat dicts from MongoDB.

    Returns:
        List of TransformedEvent objects (one per successfully processed event).
    """
    results: list[TransformedEvent] = []
    skipped = 0

    for rec in raw_events:
        try:
            results.append(_transform_one(rec))
        except Exception as exc:
            skipped += 1
            log.warning(
                "[TRANSFORM] Skipped  event_id=%s  reason=%s",
                rec.get("event_id", "?"), exc,
            )

    log.info(
        "[TRANSFORM] Complete  transformed=%d  skipped=%d",
        len(results), skipped,
    )
    return results


def _transform_one(rec: dict) -> TransformedEvent:
    """Transform a single raw extraction record into a TransformedEvent."""
    lat = float(rec["latitude"])
    lon = float(rec["longitude"])
    country, region = _classify_location(lat, lon)

    # event_time may arrive as a timezone-aware datetime (from MongoDB BSON)
    # or as a string if the record was injected by a test harness.
    et = rec["event_time"]
    if isinstance(et, str):
        et = datetime.strptime(et, "%Y-%m-%dT%H:%M:%SZ")

    return TransformedEvent(
        # dim_event_type
        event_type_name = str(rec["event_type"]),

        # dim_location
        latitude  = lat,
        longitude = lon,
        country   = country,
        region    = region,

        # dim_time
        date  = et.date(),
        day   = et.day,
        month = et.month,
        year  = et.year,
        hour  = et.hour,

        # fact payload
        event_id       = str(rec["event_id"]),
        severity_level = str(rec["severity_level"]),
        source         = str(rec.get("source", "")),
    )
