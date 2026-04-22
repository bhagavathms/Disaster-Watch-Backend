"""
normalizer.py — Source Schema → Unified Kafka Schema Converter
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Converts raw source-specific records (produced by generate_data.py) into the
unified Kafka event schema consumed by the Kafka producer → consumer → MongoDB
→ ETL pipeline.

Unified Kafka schema (7 required fields):
  event_id      str   — UUID v4
  event_type    str   — earthquake | fire | flood | storm
  severity_raw  float — type-specific physical unit (see below)
  latitude      float — WGS-84
  longitude     float — WGS-84
  timestamp     str   — ISO-8601 UTC  (YYYY-MM-DDTHH:MM:SSZ)
  source        str   — originating agency / sensor

Severity units after normalization:
  earthquake → Richter magnitude    (from USGS properties.mag)
  fire       → FRP in MW            (from NASA FIRMS frp)
  flood      → water height in cm   (from USGS HWM height_above_gnd_ft × 30.48)
  storm      → wind speed in km/h   (from IBTrACS wmo_wind × 1.852)
"""

from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Maps USGS network code → readable source label
_NET_TO_SOURCE: dict[str, str] = {
    "us": "USGS",
    "eu": "EMSC",
    "jp": "JMA",
    "es": "IGN",
    "nz": "GeoNet",
}


# ── Per-type normalizers ──────────────────────────────────────────────────────

def normalize_earthquake(raw: dict[str, Any]) -> dict[str, Any]:
    """USGS GeoJSON Feature → unified schema."""
    props  = raw["properties"]
    coords = raw["geometry"]["coordinates"]  # [lon, lat, depth]
    epoch_ms = props["time"]

    # epoch ms → ISO-8601 UTC string
    from datetime import datetime, timezone
    ts = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    source = _NET_TO_SOURCE.get(props.get("net", "us"), "USGS")

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "earthquake",
        "severity_raw": props["mag"],
        "latitude":    coords[1],
        "longitude":   coords[0],
        "timestamp":   ts,
        "source":      source,
    }


def normalize_fire(raw: dict[str, Any]) -> dict[str, Any]:
    """NASA FIRMS MODIS record → unified schema."""
    acq_date = raw["acq_date"]             # YYYY-MM-DD
    acq_time = raw["acq_time"].zfill(4)   # HHMM
    ts = f"{acq_date}T{acq_time[:2]}:{acq_time[2:]}:00Z"

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "fire",
        "severity_raw": raw["frp"],
        "latitude":    raw["latitude"],
        "longitude":   raw["longitude"],
        "timestamp":   ts,
        "source":      "NASA-FIRMS",
    }


def normalize_flood(raw: dict[str, Any]) -> dict[str, Any]:
    """USGS HWM record → unified schema (height_above_gnd_ft → cm)."""
    severity_cm = round(raw["height_above_gnd_ft"] * 30.48, 2)

    peak_date = raw["peak_date"]
    if not peak_date.endswith("Z"):
        peak_date = peak_date + "Z"

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "flood",
        "severity_raw": severity_cm,
        "latitude":    raw["site_latitude"],
        "longitude":   raw["site_longitude"],
        "timestamp":   peak_date,
        "source":      "USGS-HWM",
    }


def normalize_storm(raw: dict[str, Any]) -> dict[str, Any]:
    """IBTrACS record → unified schema (wmo_wind knots → km/h)."""
    wmo_wind    = raw["wmo_wind"] or 0.0
    severity_kmh = round(wmo_wind * 1.852, 1)

    iso_time = raw["iso_time"]  # "YYYY-MM-DD HH:MM:SS"
    ts = iso_time.replace(" ", "T") + "Z"

    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "storm",
        "severity_raw": severity_kmh,
        "latitude":    raw["lat"],
        "longitude":   raw["lon"],
        "timestamp":   ts,
        "source":      "IBTrACS",
    }


_NORMALIZERS = {
    "earthquake": normalize_earthquake,
    "fire":       normalize_fire,
    "flood":      normalize_flood,
    "storm":      normalize_storm,
}


# ── Public API ────────────────────────────────────────────────────────────────

def normalize_all(
    raw_datasets: dict[str, list[dict[str, Any]]],
) -> dict[str, list[dict[str, Any]]]:
    """
    Normalize all raw source records into the unified Kafka schema.

    Args:
        raw_datasets: {event_type: [raw_record, ...]} as returned by
                      generate_data.generate_all_events()

    Returns:
        {event_type: [unified_record, ...]} ready for writer_service.write_all()
    """
    unified: dict[str, list[dict[str, Any]]] = {}
    for event_type, records in raw_datasets.items():
        fn = _NORMALIZERS[event_type]
        normalized = []
        for rec in records:
            try:
                normalized.append(fn(rec))
            except Exception as exc:
                log.warning("Skipping malformed %s record: %s", event_type, exc)
        unified[event_type] = normalized
        log.info("Normalized %d %s records", len(normalized), event_type)
    return unified


def normalize_from_files(raw_dir: str | Path) -> dict[str, list[dict[str, Any]]]:
    """
    Load raw JSON files from disk and normalize them.
    Expects files named: earthquakes.json, fires.json, floods.json, storms.json
    under raw_dir.

    Args:
        raw_dir: Path to directory containing raw source JSON files.

    Returns:
        Unified schema datasets dict.
    """
    raw_dir = Path(raw_dir)
    file_map = {
        "earthquake": "earthquakes.json",
        "fire":       "fires.json",
        "flood":      "floods.json",
        "storm":      "storms.json",
    }
    raw_datasets: dict[str, list[dict[str, Any]]] = {}
    for event_type, filename in file_map.items():
        path = raw_dir / filename
        if not path.exists():
            log.warning("Raw file not found, skipping: %s", path)
            raw_datasets[event_type] = []
            continue
        with path.open(encoding="utf-8") as fh:
            raw_datasets[event_type] = json.load(fh)
        log.info("Loaded %d raw %s records from %s", len(raw_datasets[event_type]), event_type, path)

    return normalize_all(raw_datasets)
