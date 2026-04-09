"""
adapters/usgs_earthquake.py
USGS Earthquake GeoJSON → Canonical Disaster Event

severity_raw = properties.mag  (fits consumer thresholds: 1.5/3.0/5.0/7.0)
magnitude    = properties.mag  (direct physical value)
area_km2     = Wells & Coppersmith (1994): 10^(-3.49 + 0.91 × mag)
"""

from __future__ import annotations

import json
import math
import os
import uuid
from datetime import datetime, timezone
from typing import Any


def _epoch_ms_to_iso(t: int) -> str:
    return datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _area_km2(mag: float) -> float:
    """Wells & Coppersmith (1994) rupture area estimate."""
    return round(10 ** (-3.49 + 0.91 * mag), 4)


def transform(geojson_path: str) -> list[dict]:
    """
    Read USGS GeoJSON FeatureCollection and return canonical event list.

    Filters:
      - Only features with geometry.type == "Point"
      - mag must be numeric
    """
    with open(geojson_path, encoding="utf-8") as f:
        data = json.load(f)

    out: list[dict] = []

    for feat in data.get("features", []):
        props = feat.get("properties", {})
        geom  = feat.get("geometry", {})

        if geom.get("type") != "Point":
            continue

        coords = geom.get("coordinates", [])
        if len(coords) < 2:
            continue

        mag = props.get("mag")
        if mag is None or not isinstance(mag, (int, float)):
            continue

        lon      = float(coords[0])
        lat      = float(coords[1])
        depth_km = float(coords[2]) if len(coords) > 2 else 0.0
        t_ms     = props.get("time", 0)
        mag      = float(mag)

        record: dict[str, Any] = {
            # ── Canonical fields ──────────────────────────────────────────
            "event_id":    feat.get("id", str(uuid.uuid4())),
            "event_type":  "earthquake",
            "severity_raw": mag,                  # consumer threshold: 1.5/3.0/5.0/7.0
            "latitude":    lat,
            "longitude":   lon,
            "timestamp":   _epoch_ms_to_iso(t_ms),
            "source":      "USGS_EQ",
            # ── Computed ──────────────────────────────────────────────────
            "magnitude":   mag,
            "area_km2":    _area_km2(mag),
            # ── Earthquake-specific extended ──────────────────────────────
            "depth_km":       depth_km,
            "alert_level":    props.get("alert"),
            "tsunami":        bool(props.get("tsunami", 0)),
            "significance":   props.get("sig"),
            "place":          props.get("place", ""),
            "review_status":  props.get("status", "automatic"),
            "network":        props.get("net", ""),
            "magnitude_type": props.get("magType", ""),
            "mmi":            props.get("mmi"),
            "felt_count":     props.get("felt"),
        }

        out.append(record)

    return out
