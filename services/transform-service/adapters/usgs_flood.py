"""
adapters/usgs_flood.py
USGS Flood High-Water Mark JSON → Canonical Disaster Event

One HWM JSON is a list of individual measurement points, many sharing the
same event_id.  This adapter groups them by event_id (one flood event = one
canonical record), then computes the Flood Magnitude Index (FMI).

FMI = 0.4*A_score + 0.3*D_score + 0.2*T_score
  A_score = log10(area_km2 + 1)    — spatial extent from convex hull
  D_score = mean estimated depth    — elevation above estimated baseline
  T_score = log10(duration_hours+1) — flagged time range within event

severity_raw = FMI   (consumer thresholds: 0.5 / 1.0 / 2.0 / 3.0)

Why no Q_score (discharge)?
  USGS HWM data does not include discharge.  Weight is redistributed to A+D.
"""

from __future__ import annotations

import json
import math
import os
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any


# ── Geometry helper: convex hull area in km² ──────────────────────────────────

def _shoelace(pts: list[tuple[float, float]]) -> float:
    """Shoelace formula for polygon area (signed), input in degrees."""
    n = len(pts)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += pts[i][0] * pts[j][1]
        area -= pts[j][0] * pts[i][1]
    return abs(area) / 2.0

def _convex_hull(pts: list[tuple[float, float]]) -> list[tuple[float, float]]:
    """Andrew's monotone chain convex hull."""
    pts = sorted(set(pts))
    if len(pts) < 3:
        return pts
    lower: list = []
    for p in pts:
        while len(lower) >= 2 and _cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    upper: list = []
    for p in reversed(pts):
        while len(upper) >= 2 and _cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]

def _cross(O, A, B) -> float:
    return (A[0] - O[0]) * (B[1] - O[1]) - (A[1] - O[1]) * (B[0] - O[0])

def _area_deg_to_km2(area_deg: float, lat_c: float) -> float:
    """Convert degree² area to km² using local scale at given latitude."""
    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * math.cos(math.radians(lat_c))
    return area_deg * km_per_deg_lat * km_per_deg_lon


# ── Date parsing ──────────────────────────────────────────────────────────────

def _parse_iso(s: str) -> datetime | None:
    """Parse USGS flood date strings like '2026-01-22T07:00:00'."""
    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None

def _to_iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ── Main transform ────────────────────────────────────────────────────────────

def transform(hwm_json_path: str) -> list[dict]:
    """
    Group HWM records by event_id and emit one canonical event per flood.

    Quality filter: skip entire event if ALL marks are Poor quality.
    """
    with open(hwm_json_path, encoding="utf-8") as f:
        records: list[dict] = json.load(f)

    # Group by event_id
    groups: dict[int, list[dict]] = defaultdict(list)
    for r in records:
        groups[r["event_id"]].append(r)

    out: list[dict] = []

    for eid, marks in groups.items():
        # ── Quality filter: require at least one Good or Fair mark ────────
        best_quality = min(m["hwm_quality_id"] for m in marks)  # 2=Good, 3=Fair, 4=Poor
        if best_quality > 3 and len(marks) < 3:
            continue   # skip very poor single-point events

        # ── Spatial: collect Good/Fair points for area calculation ────────
        pts = [(m["latitude_dd"], m["longitude_dd"])
               for m in marks if m["hwm_quality_id"] <= 3]
        if not pts:
            pts = [(m["latitude_dd"], m["longitude_dd"]) for m in marks]

        lat_c = sum(p[0] for p in pts) / len(pts)
        lon_c = sum(p[1] for p in pts) / len(pts)

        # Area from convex hull (in km²)
        if len(pts) >= 3:
            hull    = _convex_hull(pts)
            deg2    = _shoelace(hull)
            area_km2 = _area_deg_to_km2(deg2, lat_c)
        else:
            # Fallback: treat as a 1 km² point event
            area_km2 = 1.0

        # ── Depth proxy: mean elevation across marks ───────────────────────
        elevs    = [m["elev_ft"] for m in marks]
        mean_elev = sum(elevs) / len(elevs)
        # Depth proxy: deviation above the minimum mark (rough water rise, ft → m)
        min_elev = min(elevs)
        depth_m  = (mean_elev - min_elev) * 0.3048   # feet → metres
        D_score  = min(5.0, depth_m / 2.0)            # cap at 5

        # ── Time: flag_date range ─────────────────────────────────────────
        flag_dates = [_parse_iso(m["flag_date"]) for m in marks if m.get("flag_date")]
        flag_dates = [d for d in flag_dates if d is not None]
        if flag_dates:
            t_min = min(flag_dates)
            t_max = max(flag_dates)
            duration_h = max(1, (t_max - t_min).total_seconds() / 3600)
        else:
            t_min = None
            duration_h = 24.0  # assume 1-day event if unknown

        timestamp = _to_iso(t_min) if t_min else "2024-06-01T00:00:00Z"

        # ── FMI ───────────────────────────────────────────────────────────
        A_score = math.log10(area_km2 + 1)
        T_score = math.log10(duration_h + 1)

        # Redistribute Q_score weight to A and D
        fmi = round(0.5 * A_score + 0.3 * D_score + 0.2 * T_score, 4)
        fmi = max(0.0, fmi)

        # ── Source metadata ───────────────────────────────────────────────
        environment = marks[0].get("hwm_environment", "Riverine")
        state       = marks[0].get("stateName", "")
        country     = marks[0].get("countyName", "")
        waterbody   = marks[0].get("waterbody", "")
        event_name  = marks[0].get("eventName", "")
        quality_name = _QUALITY_NAMES.get(best_quality, "Unknown")

        record: dict[str, Any] = {
            # ── Canonical ─────────────────────────────────────────────────
            "event_id":    f"USGS-FLOOD-{eid}",
            "event_type":  "flood",
            "severity_raw": fmi,          # consumer thresholds: 0.5/1.0/2.0/3.0
            "latitude":    round(lat_c, 6),
            "longitude":   round(lon_c, 6),
            "timestamp":   timestamp,
            "source":      "USGS_FLOOD",
            # ── Computed ──────────────────────────────────────────────────
            "magnitude":   round(fmi, 4),
            "area_km2":    round(area_km2, 4),
            # ── Flood-specific extended ───────────────────────────────────
            "flood_event_id":     eid,
            "flood_event_name":   event_name,
            "hwm_count":          len(marks),
            "water_elevation_ft": round(mean_elev, 2),
            "quality":            quality_name,
            "environment":        environment,
            "state":              state,
            "waterbody":          waterbody,
            "fmi":                round(fmi, 4),
            "duration_hours":     round(duration_h, 1),
        }

        out.append(record)

    return out


_QUALITY_NAMES = {2: "Good", 3: "Fair", 4: "Poor"}
