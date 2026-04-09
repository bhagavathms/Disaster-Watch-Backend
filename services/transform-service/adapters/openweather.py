"""
adapters/openweather.py
OpenWeather JSON Array → Canonical Storm Events

Storm detection gate: a record is ONLY emitted if at least one of:
  - rain.1h  >= 2.5 mm   (moderate rain)
  - wind.speed >= 10 m/s  (wind storm onset)
  - weather[0].id in 200–299  (thunderstorm code)

severity_raw = wind_speed_kmh  (consumer thresholds: 35/62/120/200 km/h)
magnitude    = Beaufort scale (0–12) derived from wind.speed
area_km2     = None (single-point measurement; cannot determine storm area)
"""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from typing import Any


def _epoch_to_iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _beaufort(speed_ms: float) -> int:
    """Convert m/s wind speed to Beaufort scale (0–12)."""
    thresholds = [0.3, 1.6, 3.4, 5.5, 8.0, 10.8, 13.9, 17.2, 20.8, 24.5, 28.5, 32.7]
    for b, t in enumerate(thresholds):
        if speed_ms < t:
            return b
    return 12


def _is_storm(record: dict) -> bool:
    """Return True if this weather snapshot meets the storm detection threshold."""
    weather_codes = [w.get("id", 0) for w in record.get("weather", [])]
    rain_1h   = record.get("rain", {}).get("1h", 0.0) or 0.0
    wind_ms   = record.get("wind", {}).get("speed", 0.0) or 0.0
    has_thunder = any(200 <= code <= 299 for code in weather_codes)

    return rain_1h >= 2.5 or wind_ms >= 10.0 or has_thunder


def transform(json_path: str) -> list[dict]:
    """
    Read OpenWeather snapshot array, apply storm gate, return canonical events.
    """
    with open(json_path, encoding="utf-8") as f:
        snapshots: list[dict] = json.load(f)

    out: list[dict] = []

    for snap in snapshots:
        if not _is_storm(snap):
            continue   # not a storm event — discard entirely

        coord    = snap.get("coord", {})
        lat      = float(coord.get("lat", 0))
        lon      = float(coord.get("lon", 0))
        dt_epoch = snap.get("dt", 0)
        city_id  = snap.get("id", 0)
        city     = snap.get("name", "")

        wind     = snap.get("wind", {})
        wind_ms  = float(wind.get("speed", 0))
        gust_ms  = wind.get("gust")
        wind_deg = wind.get("deg", 0)

        main_    = snap.get("main", {})
        pressure = main_.get("pressure", 1013)
        humidity = main_.get("humidity", 0)

        rain    = snap.get("rain", {})
        rain_1h = rain.get("1h", 0.0) or 0.0
        rain_3h = rain.get("3h", 0.0) or 0.0

        vis      = snap.get("visibility", None)
        clouds   = snap.get("clouds", {}).get("all", 0)

        weather_list = snap.get("weather", [{}])
        cond_main = weather_list[0].get("main", "") if weather_list else ""
        cond_id   = weather_list[0].get("id", 0)   if weather_list else 0
        cond_desc = weather_list[0].get("description", "") if weather_list else ""

        sys_      = snap.get("sys", {})
        country   = sys_.get("country", "")

        # ── severity_raw = wind speed in km/h ─────────────────────────────
        wind_kmh  = round(wind_ms * 3.6, 2)    # m/s → km/h
        beaufort  = _beaufort(wind_ms)

        # ── No meaningful single-point area of effect ─────────────────────
        area_km2  = None

        event_id  = f"OWM-{city_id}-{dt_epoch}"

        record: dict[str, Any] = {
            # ── Canonical ─────────────────────────────────────────────────
            "event_id":    event_id,
            "event_type":  "storm",
            "severity_raw": wind_kmh,   # consumer thresholds: 35/62/120/200 km/h
            "latitude":    lat,
            "longitude":   lon,
            "timestamp":   _epoch_to_iso(dt_epoch),
            "source":      "OPENWEATHER",
            # ── Computed ──────────────────────────────────────────────────
            "magnitude":   float(beaufort),
            "area_km2":    area_km2,
            # ── Storm-specific extended ───────────────────────────────────
            "rain_1h_mm":      rain_1h,
            "rain_3h_mm":      rain_3h,
            "wind_speed_ms":   wind_ms,
            "wind_speed_kmh":  wind_kmh,
            "wind_gust_ms":    gust_ms,
            "wind_direction":  wind_deg,
            "pressure_hpa":    pressure,
            "humidity_pct":    humidity,
            "visibility_m":    vis,
            "cloud_cover_pct": clouds,
            "condition_main":  cond_main,
            "condition_id":    cond_id,
            "condition_desc":  cond_desc,
            "city_name":       city,
            "country_code":    country,
            "beaufort_scale":  beaufort,
        }

        out.append(record)

    return out
