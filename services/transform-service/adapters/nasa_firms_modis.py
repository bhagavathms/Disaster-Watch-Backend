"""
adapters/nasa_firms_modis.py
NASA FIRMS MODIS CSV → Canonical Fire Events

Each CSV row is a satellite pixel.  Pixels that are spatially and temporally
close together belong to the same fire.  This adapter:
  1. Filters out unreliable detections (confidence < 30, type != 0)
  2. Clusters nearby pixels into single fire events (0.5° grid cell + same date)
  3. Aggregates per cluster: sum FRP, sum pixel area, compute centroid
  4. Emits one canonical record per cluster

severity_raw = total_frp_mw  (consumer thresholds: 5 / 20 / 50 / 200 MW)
magnitude    = log10(total_frp_mw + 1)
area_km2     = sum(scan × track) per cluster
"""

from __future__ import annotations

import csv
import math
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

# Grid resolution for pixel clustering (degrees)
_CLUSTER_RESOLUTION = 0.5


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
    """Bucket lat/lon into a coarse grid cell for clustering."""
    cell_lat = math.floor(lat / _CLUSTER_RESOLUTION) * _CLUSTER_RESOLUTION
    cell_lon = math.floor(lon / _CLUSTER_RESOLUTION) * _CLUSTER_RESOLUTION
    return (cell_lat, cell_lon, date_str)


def transform(csv_path: str) -> list[dict]:
    """
    Read MODIS CSV, cluster pixels, return canonical fire event list.
    """
    # ── Cluster pixels ────────────────────────────────────────────────────────
    clusters: dict[tuple, list[dict]] = defaultdict(list)

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                confidence = int(row["confidence"])
                fire_type  = int(row["type"])
                frp        = float(row["frp"])
                lat        = float(row["latitude"])
                lon        = float(row["longitude"])
            except (ValueError, KeyError):
                continue

            # ── Pre-filters ───────────────────────────────────────────────
            if confidence < 30:
                continue          # unreliable detection
            if fire_type != 0:
                continue          # not a vegetation fire

            key = _grid_key(lat, lon, row["acq_date"])
            clusters[key].append({
                "lat":     lat,
                "lon":     lon,
                "frp":     frp,
                "scan":    float(row.get("scan", 1.0)),
                "track":   float(row.get("track", 1.0)),
                "date":    row["acq_date"],
                "time":    int(row.get("acq_time", 0)),
                "sat":     row.get("satellite", ""),
                "conf":    confidence,
                "bright":  float(row.get("brightness", 0)),
                "bg_temp": float(row.get("bright_t31", 0)),
                "dn":      row.get("daynight", "D"),
            })

    # ── Aggregate clusters → canonical events ─────────────────────────────────
    out: list[dict] = []

    for key, pixels in clusters.items():
        total_frp   = round(sum(p["frp"] for p in pixels), 2)

        # Drop clusters below min threshold (same as consumer fire min_threshold)
        if total_frp < 5.0:
            continue

        pixel_areas = [p["scan"] * p["track"] for p in pixels]
        area_km2    = round(sum(pixel_areas), 4)
        magnitude   = round(math.log10(total_frp + 1), 4)

        # FRP-weighted centroid
        lat_c = sum(p["lat"] * p["frp"] for p in pixels) / total_frp
        lon_c = sum(p["lon"] * p["frp"] for p in pixels) / total_frp

        # Representative timestamp (first detection in cluster)
        first   = sorted(pixels, key=lambda p: p["time"])[0]
        ts      = _parse_date(first["date"], first["time"])
        sat     = first["sat"]
        dn      = first["dn"]

        avg_conf    = round(sum(p["conf"] for p in pixels) / len(pixels), 1)
        mean_bright = round(sum(p["bright"] for p in pixels) / len(pixels), 1)
        mean_bg     = round(sum(p["bg_temp"] for p in pixels) / len(pixels), 1)

        event_id = f"FIRMS-MODIS-{key[0]:.2f}-{key[1]:.2f}-{key[2]}"

        record: dict[str, Any] = {
            # ── Canonical ─────────────────────────────────────────────────
            "event_id":    event_id,
            "event_type":  "fire",
            "severity_raw": total_frp,     # consumer thresholds: 5/20/50/200 MW
            "latitude":    round(lat_c, 6),
            "longitude":   round(lon_c, 6),
            "timestamp":   ts,
            "source":      "NASA_FIRMS_MODIS",
            # ── Computed ──────────────────────────────────────────────────
            "magnitude":   magnitude,
            "area_km2":    area_km2,
            # ── Fire-specific extended ────────────────────────────────────
            "frp_mw":          total_frp,
            "pixel_count":     len(pixels),
            "confidence_pct":  avg_conf,
            "brightness_k":    mean_bright,
            "background_temp_k": mean_bg,
            "satellite":       sat,
            "daynight":        dn,
        }

        out.append(record)

    return out
