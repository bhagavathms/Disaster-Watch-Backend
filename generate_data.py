"""
generate_data.py — Task A + B
Synthetic disaster event generator for Step 1 of the
Distributed NoSQL-Based Disaster Monitoring and Analytics System.

Unified event schema (every record):
  event_id      : str   — UUID v4
  event_type    : str   — earthquake | fire | flood | storm
  severity_raw  : float — intentionally inconsistent units per type
  latitude      : float — WGS-84
  longitude     : float — WGS-84
  timestamp     : str   — ISO-8601 UTC
  source        : str   — originating agency / sensor

Design goals:
  - Non-uniform severity distributions (log-normal, exponential, Gaussian)
  - Geographically clustered around real disaster-prone regions
  - Intentional noise: sensor nulls, extreme outliers, micro-events
  - Aftershock clusters injected for earthquakes
  - Timestamps spread over a 90-day window
"""

import math
import random
import uuid
from datetime import datetime, timedelta
from typing import Any

# ── Reproducibility ───────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

# ── Time window: last 90 days of 2024 ─────────────────────────────────────────
_END   = datetime(2024, 12, 31, 23, 59, 59)
_START = _END - timedelta(days=90)

# ── Geographic clusters ────────────────────────────────────────────────────────
# Format: (centre_lat, centre_lon, std_dev_degrees)
# Multiple clusters per type to create realistic spatial heterogeneity.
_CLUSTERS: dict[str, list[tuple[float, float, float]]] = {
    "earthquake": [
        ( 36.0,  138.0, 4.0),   # Japan (Pacific Ring of Fire)
        ( -5.0,  120.0, 5.0),   # Indonesia / Sulawesi
        (-30.0,  -70.0, 6.0),   # Chile / Andes subduction
        ( 37.5, -119.5, 3.0),   # California (San Andreas)
        ( 39.0,   35.0, 4.0),   # Turkey / North Anatolian Fault
        ( 62.0, -150.0, 5.0),   # Alaska (Aleutian arc)
    ],
    "fire": [
        ( 37.5, -119.5, 3.0),   # California (wildfire corridor)
        (-30.0,  145.0, 8.0),   # Australia (eastern bushfire belt)
        ( -5.0,  -55.0, 10.0),  # Amazon Basin (deforestation fires)
        ( 38.0,   22.0,  5.0),  # Mediterranean (Greece / Turkey)
        ( 60.0,  100.0, 10.0),  # Siberia (boreal forest)
    ],
    "flood": [
        ( 23.7,   90.4, 2.0),   # Bangladesh (Brahmaputra / Ganges delta)
        ( 32.0,  -91.0, 3.0),   # Mississippi Valley
        ( 52.0,    5.0, 2.0),   # Netherlands / Rhine delta
        ( 10.5,  105.5, 2.5),   # Mekong Delta (Vietnam)
        (  8.0,    6.0, 3.0),   # Nigeria (Niger delta)
        ( 30.0,   70.0, 4.0),   # Pakistan (Indus river plain)
    ],
    "storm": [
        ( 24.0,  -88.0, 5.0),   # Gulf of Mexico (hurricane track)
        ( 14.0,   87.0, 5.0),   # Bay of Bengal (cyclone basin)
        ( 12.0,  125.0, 4.0),   # Philippine Sea (typhoon corridor)
        ( 18.0,  -70.0, 4.0),   # Caribbean (tropical storm belt)
        ( 25.0,  135.0, 6.0),   # Western Pacific (typhoon alley)
    ],
}

# ── Low-level helpers ─────────────────────────────────────────────────────────

def _ts() -> str:
    """Random ISO-8601 timestamp within the 90-day window."""
    delta_s = random.randint(0, int((_END - _START).total_seconds()))
    return (_START + timedelta(seconds=delta_s)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _gauss_clamp(mu: float, sigma: float, lo: float, hi: float) -> float:
    """Gaussian sample clamped to [lo, hi]."""
    return max(lo, min(hi, random.gauss(mu, sigma)))


def _cluster_coord(event_type: str) -> tuple[float, float]:
    """
    Pick a random cluster for the event type and sample a coordinate
    from a Gaussian centred on that cluster.
    """
    lat_c, lon_c, std = random.choice(_CLUSTERS[event_type])
    lat = round(_gauss_clamp(lat_c, std, -85.0,  85.0), 6)
    lon = round(_gauss_clamp(lon_c, std, -180.0, 180.0), 6)
    return lat, lon


def _make_event(
    event_type: str,
    severity: float,
    lat: float,
    lon: float,
    source: str,
) -> dict[str, Any]:
    """Construct a schema-compliant event record."""
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "severity_raw": round(severity, 2),
        "latitude":    lat,
        "longitude":   lon,
        "timestamp":   _ts(),
        "source":      source,
    }


# ── Per-type generators ───────────────────────────────────────────────────────

def _earthquakes(n: int) -> list[dict[str, Any]]:
    """
    severity_raw = Richter magnitude, range 1.0 – 9.5
    Distribution: log-normal (many M2–M4, rare M7+).

    Edge cases injected:
      ~5 %  catastrophic mainshocks  (M 8.0 – 9.5)
      ~8 %  micro-quakes             (M 1.0 – 1.4) — below most reporting thresholds
      ~1 %  sensor null              (0.0)
      ~10 % intraplate / scattered   (random global position)
      4 random mainshocks get 3–8 aftershocks each (realistic seismic sequence)
    """
    SOURCES = ["USGS", "EMSC", "JMA", "IGN", "GeoNet"]
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            mag = 0.0                                                    # sensor null
        elif roll < 0.06:
            mag = round(random.uniform(8.0, 9.5), 1)                    # catastrophic
        elif roll < 0.14:
            mag = round(random.uniform(1.0, 1.4), 1)                    # micro-quake
        else:
            # Log-normal: median ≈ 3.5, long right tail towards M7+
            raw = random.lognormvariate(math.log(3.5), 0.55)
            mag = round(max(1.0, min(9.5, raw)), 1)

        lat, lon = _cluster_coord("earthquake")

        # ~10 % scattered globally (intraplate seismicity)
        if random.random() < 0.10:
            lat = round(random.uniform(-70.0, 70.0), 6)
            lon = round(random.uniform(-180.0, 180.0), 6)

        out.append(_make_event("earthquake", mag, lat, lon, random.choice(SOURCES)))

    # ── Aftershock injection ──────────────────────────────────────────────────
    # Pick 4 events as "mainshocks" and append a realistic aftershock sequence.
    # Aftershocks: lower magnitude, same region, same monitoring agency.
    mainshock_indices = random.sample(range(len(out)), min(4, len(out)))
    for idx in mainshock_indices:
        ms = out[idx]
        n_aftershocks = random.randint(3, 8)
        for _ in range(n_aftershocks):
            decay    = random.uniform(1.0, 3.5)          # Båth's law approximation
            as_mag   = round(max(0.5, ms["severity_raw"] - decay), 1)
            as_lat   = round(_gauss_clamp(ms["latitude"],  0.25, -85.0,  85.0), 6)
            as_lon   = round(_gauss_clamp(ms["longitude"], 0.25, -180.0, 180.0), 6)
            out.append(_make_event("earthquake", as_mag, as_lat, as_lon, ms["source"]))

    return out


def _fires(n: int) -> list[dict[str, Any]]:
    """
    severity_raw = fire intensity index (arbitrary units), range 0 – 10 000
    Higher values → larger burned area / higher radiative power.
    Distribution: exponential (many small fires, occasional mega-fire).

    Edge cases injected:
      ~6 %  mega-fire spike           (7 000 – 10 000) — fire-storm conditions
      ~10 % tiny brush fires          (1.0 – 4.9)      — below suppression threshold
      ~1 %  sensor null               (0.0)
    """
    SOURCES = ["NASA_FIRMS", "Copernicus_EMS", "CAL_FIRE", "NIFC", "EFFIS"]
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            intensity = 0.0                                              # sensor null
        elif roll < 0.07:
            intensity = round(random.uniform(7000.0, 10000.0), 2)      # mega-fire
        elif roll < 0.17:
            intensity = round(random.uniform(1.0, 4.9), 2)             # brush fire
        else:
            # Exponential: mean ≈ 300 (right-skewed, most fires 50–600)
            raw = random.expovariate(1.0 / 300.0)
            intensity = round(max(1.0, min(10000.0, raw)), 2)

        lat, lon = _cluster_coord("fire")
        out.append(_make_event("fire", intensity, lat, lon, random.choice(SOURCES)))

    return out


def _floods(n: int) -> list[dict[str, Any]]:
    """
    severity_raw = water-level rise in cm, range 0 – 2 000 cm
    Distribution: right-skewed (moderate flooding most common, extreme rare).

    Edge cases injected:
      ~15 % extreme flood             (800 – 2 000 cm) — inundation / displacement
      ~12 % marginal / noise          (0.1 – 4.9 cm)   — below warning threshold
      ~1 %  sensor null               (0.0)
      ~8 %  flash-flood outside zones (random global coord)
    """
    SOURCES = ["GFO", "Dartmouth_FO", "CEMS", "ECMWF", "GLOFAS"]
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            level = 0.0                                                  # sensor null
        elif roll < 0.16:
            level = round(random.uniform(800.0, 2000.0), 2)            # extreme
        elif roll < 0.28:
            level = round(random.uniform(0.1, 4.9), 2)                 # marginal
        else:
            level = round(random.uniform(5.0, 800.0), 2)               # typical

        lat, lon = _cluster_coord("flood")

        # ~8 % flash floods outside known flood plains
        if random.random() < 0.08:
            lat = round(random.uniform(-60.0, 60.0), 6)
            lon = round(random.uniform(-180.0, 180.0), 6)

        out.append(_make_event("flood", level, lat, lon, random.choice(SOURCES)))

    return out


def _storms(n: int) -> list[dict[str, Any]]:
    """
    severity_raw = maximum sustained wind speed in km/h, range 10 – 300
    Distribution: Gaussian centred ~90 km/h with two outlier bands.

    Edge cases injected:
      ~1 %  sensor null               (0.0)            — anemometer failure
      ~7 %  Category-5 equivalent     (200 – 300 km/h) — extreme tropical cyclone
      ~10 % tropical depression       (10 – 34 km/h)   — below tropical storm threshold
    """
    SOURCES = ["NHC", "JTWC", "IMD", "ECMWF", "WMO"]
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            wind = 0.0                                                   # sensor null
        elif roll < 0.08:
            wind = round(_gauss_clamp(250.0, 20.0, 200.0, 300.0), 1)  # Cat-5
        elif roll < 0.18:
            wind = round(_gauss_clamp(22.0,   5.0,  10.0,  34.0), 1)  # depression
        else:
            # Moderate storms: Cat-1 to Cat-3 range
            wind = round(_gauss_clamp(90.0, 40.0, 35.0, 200.0), 1)

        lat, lon = _cluster_coord("storm")
        out.append(_make_event("storm", wind, lat, lon, random.choice(SOURCES)))

    # Guarantee at least one sensor-null regardless of random outcome
    if not any(e["severity_raw"] == 0.0 for e in out):
        out[0]["severity_raw"] = 0.0

    return out


# ── Public API ────────────────────────────────────────────────────────────────

def generate_all_events(
    n_earthquakes: int = 100,
    n_fires:       int = 105,
    n_floods:      int = 105,
    n_storms:      int = 95,
) -> dict[str, list[dict[str, Any]]]:
    """
    Generate all synthetic events, segmented by event_type (Task B).

    Args:
        n_earthquakes: Base count before aftershock injection (final count will be higher).
        n_fires:       Number of fire events.
        n_floods:      Number of flood events.
        n_storms:      Number of storm events.

    Returns:
        {
          "earthquake": [...],
          "fire":       [...],
          "flood":      [...],
          "storm":      [...],
        }
    """
    return {
        "earthquake": _earthquakes(n_earthquakes),
        "fire":       _fires(n_fires),
        "flood":      _floods(n_floods),
        "storm":      _storms(n_storms),
    }
