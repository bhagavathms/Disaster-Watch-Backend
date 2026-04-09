"""
adapters/nasa_firms_viirs.py
NASA FIRMS VIIRS CSV → Canonical Fire Events

Same clustering logic as MODIS adapter.
Key differences from MODIS:
  - confidence field is string: "l" / "n" / "h"  (filter out "l")
  - brightness field is bright_ti4  (Band I-4, fire channel)
  - background field is bright_ti5  (Band I-5)
  - Higher spatial resolution: scan/track values smaller (~0.32–0.75 km)

severity_raw = total_frp_mw  (consumer thresholds: 5 / 20 / 50 / 200 MW)
magnitude    = log10(total_frp_mw + 1)
area_km2     = sum(scan × track) per cluster
"""

from __future__ import annotations

import csv
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

_CLUSTER_RESOLUTION = 0.5

_CONF_SCORE = {"l": 25, "n": 60, "h": 90}


def _parse_date(date_str: str, acq_time: int) -> str:
    """DD-MM-YYYY + HHMM integer → ISO UTC string."""
    try:
        day, month, year = date_str.split("-")
        h = acq_time // 100
        m = acq_time % 100
        dt = datetime(int(year), int(month), int(day), h, m, tzinfo=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return "2024-01-01T00:00:00Z"


def _grid_key(lat: float, lon: float, date_str: str) -> tuple:
    cell_lat = math.floor(lat / _CLUSTER_RESOLUTION) * _CLUSTER_RESOLUTION
    cell_lon = math.floor(lon / _CLUSTER_RESOLUTION) * _CLUSTER_RESOLUTION
    return (cell_lat, cell_lon, date_str)


def transform(csv_path: str) -> list[dict]:
    """
    Read VIIRS CSV, cluster pixels, return canonical fire event list.
    """
    clusters: dict[tuple, list[dict]] = defaultdict(list)

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                conf_str   = str(row.get("confidence", "n")).strip().lower()
                fire_type  = int(row["type"])
                frp        = float(row["frp"])
                lat        = float(row["latitude"])
                lon        = float(row["longitude"])
            except (ValueError, KeyError):
                continue

            # ── Pre-filters ───────────────────────────────────────────────
            if conf_str == "l":
                continue          # low confidence VIIRS detection
            if fire_type != 0:
                continue

            conf_pct = _CONF_SCORE.get(conf_str, 60)
            key      = _grid_key(lat, lon, row["acq_date"])
            clusters[key].append({
                "lat":     lat,
                "lon":     lon,
                "frp":     frp,
                "scan":    float(row.get("scan", 0.5)),
                "track":   float(row.get("track", 0.5)),
                "date":    row["acq_date"],
                "time":    int(row.get("acq_time", 0)),
                "sat":     row.get("satellite", "N"),
                "conf":    conf_pct,
                "bright":  float(row.get("bright_ti4", 0)),
                "bg_temp": float(row.get("bright_ti5", 0)),
                "dn":      row.get("daynight", "D"),
            })

    out: list[dict] = []

    for key, pixels in clusters.items():
        total_frp = round(sum(p["frp"] for p in pixels), 2)

        if total_frp < 5.0:
            continue

        pixel_areas = [p["scan"] * p["track"] for p in pixels]
        area_km2    = round(sum(pixel_areas), 4)
        magnitude   = round(math.log10(total_frp + 1), 4)

        lat_c = sum(p["lat"] * p["frp"] for p in pixels) / total_frp
        lon_c = sum(p["lon"] * p["frp"] for p in pixels) / total_frp

        first   = sorted(pixels, key=lambda p: p["time"])[0]
        ts      = _parse_date(first["date"], first["time"])
        sat     = first["sat"]
        dn      = first["dn"]

        avg_conf    = round(sum(p["conf"] for p in pixels) / len(pixels), 1)
        mean_bright = round(sum(p["bright"] for p in pixels) / len(pixels), 1)
        mean_bg     = round(sum(p["bg_temp"] for p in pixels) / len(pixels), 1)

        event_id = f"FIRMS-VIIRS-{key[0]:.2f}-{key[1]:.2f}-{key[2]}"

        record: dict[str, Any] = {
            # ── Canonical ─────────────────────────────────────────────────
            "event_id":    event_id,
            "event_type":  "fire",
            "severity_raw": total_frp,
            "latitude":    round(lat_c, 6),
            "longitude":   round(lon_c, 6),
            "timestamp":   ts,
            "source":      "NASA_FIRMS_VIIRS",
            # ── Computed ──────────────────────────────────────────────────
            "magnitude":   magnitude,
            "area_km2":    area_km2,
            # ── Fire-specific extended ────────────────────────────────────
            "frp_mw":            total_frp,
            "pixel_count":       len(pixels),
            "confidence_pct":    avg_conf,
            "brightness_k":      mean_bright,
            "background_temp_k": mean_bg,
            "satellite":         sat,
            "daynight":          dn,
        }

        out.append(record)

    return out
