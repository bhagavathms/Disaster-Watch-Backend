"""
generate_data.py — Raw Source Schema Generator
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Generates synthetic disaster events in the native schemas of real-world
data providers:
  Earthquake → USGS GeoJSON Feature  (USGS Earthquake Catalog API format)
  Fire       → NASA FIRMS MODIS      (active fire data, CSV-to-JSON)
  Flood      → USGS High Water Marks (USGS STN Flood Event Data API)
  Storm      → IBTrACS               (NOAA IBTrACS Best Track format)

Output of generate_all_events() is passed to normalizer.py, which
converts each source schema into the unified Kafka event schema:
  {event_id, event_type, severity_raw, latitude, longitude, timestamp, source}

Design goals:
  - Non-uniform severity distributions (log-normal, exponential, Gaussian)
  - Geographically clustered around real, land/ocean disaster-prone regions
  - Timestamps spread across 1980-01-01 → 2025-12-31 (45-year window)
  - Real-world seasonal monthly patterns (monsoon, fire season, cyclone season)
  - Lognormal inter-annual variability per event type
  - Source-specific fidelity: field names, units, and metadata match real APIs
"""

import calendar
import math
import random
import uuid
from datetime import datetime, timezone
from typing import Any

# ── Reproducibility ───────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)

# ── Seasonal monthly weights (source: EMDAT, NASA FIRMS, NOAA IBTrACS) ────────
_MONTHLY_WEIGHTS: dict[str, list[float]] = {
    "earthquake": [1.00, 1.00, 1.00, 1.00, 1.00, 1.00,
                   1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
    "fire":       [0.85, 0.80, 0.70, 0.65, 0.75, 1.00,
                   1.20, 1.35, 1.25, 1.10, 0.95, 0.90],
    "flood":      [0.55, 0.55, 0.60, 0.70, 0.85, 1.25,
                   1.55, 1.60, 1.35, 0.90, 0.65, 0.55],
    "storm":      [0.70, 0.80, 0.75, 0.55, 0.55, 0.75,
                   0.95, 1.30, 1.55, 1.35, 1.00, 0.75],
}

# ── Year-to-year variability (lognormal noise, type-specific sigma) ────────────
_YEARS: list[int] = list(range(1980, 2026))
_YEAR_SIGMA: dict[str, float] = {
    "earthquake": 0.15,
    "fire":       0.28,
    "flood":      0.22,
    "storm":      0.22,
}
_rng = random.Random(SEED + 99)
_YEAR_WEIGHTS: dict[str, list[float]] = {}
for _etype, _sigma in _YEAR_SIGMA.items():
    _raw  = [_rng.lognormvariate(0.0, _sigma) for _ in _YEARS]
    _mean = sum(_raw) / len(_raw)
    _YEAR_WEIGHTS[_etype] = [w / _mean for w in _raw]


def _ts(event_type: str) -> str:
    """Return a realistic ISO-8601 UTC timestamp for the given event type."""
    year  = random.choices(_YEARS, weights=_YEAR_WEIGHTS[event_type], k=1)[0]
    month = random.choices(range(1, 13), weights=_MONTHLY_WEIGHTS[event_type], k=1)[0]
    _, days_in_month = calendar.monthrange(year, month)
    day    = random.randint(1, days_in_month)
    hour   = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return datetime(year, month, day, hour, minute, second).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ts_to_epoch_ms(ts_str: str) -> int:
    """Convert 'YYYY-MM-DDTHH:MM:SSZ' to Unix epoch milliseconds."""
    dt = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ── Geographic hotspot zones ──────────────────────────────────────────────────
# Format: (centre_lat, centre_lon, std_dev_degrees, weight)
_CLUSTERS: dict[str, list[tuple[float, float, float, int]]] = {

    "earthquake": [
        ( 37.0,  141.5, 5.0, 20),  # Japan
        ( -3.0,  118.0, 7.0, 19),  # Indonesia
        (-20.0,  -70.0, 8.0, 18),  # Chile/Peru
        ( 55.0, -153.0, 7.0, 16),  # Alaska/Aleutians
        (  9.0,  126.0, 4.0, 15),  # Philippines
        ( -6.0,  147.0, 4.5, 14),  # Papua New Guinea
        (-18.0,  168.0, 4.5, 12),  # Vanuatu/Tonga
        ( 28.5,   84.5, 4.5, 17),  # Nepal/Himalayas
        ( 36.5,   70.5, 3.5, 14),  # Hindu Kush
        ( 38.5,   38.0, 4.0, 15),  # Turkey
        ( 32.5,   48.0, 4.5, 14),  # Iran/Zagros
        ( 13.0,  -87.5, 3.5, 13),  # Central America
        ( 15.5,  -62.0, 4.5, 11),  # Caribbean
        ( 37.0, -119.5, 3.0, 12),  # California
        ( 46.0, -123.5, 3.5,  9),  # Cascadia
        ( 36.5,   22.5, 3.5, 10),  # Hellenic Arc
        (-38.5,  176.0, 5.0, 13),  # New Zealand
        (  3.0,   36.5, 7.0,  9),  # East African Rift
        ( 64.5,  -18.5, 4.0,  5),  # Iceland
        ( 42.5,   44.5, 3.5,  8),  # Caucasus
    ],

    "fire": [
        ( -5.0,   23.0, 8.0, 20),  # Central African savanna
        (-16.0,   31.0, 7.0, 18),  # Southern African savanna
        ( 10.0,   -2.0, 6.5, 16),  # West African Sahel
        (  4.0,   37.0, 5.0, 12),  # East African savanna
        (-19.0,   45.0, 4.0,  8),  # Madagascar
        (-13.0,  -50.0, 7.0, 17),  # Brazilian Cerrado
        ( -9.0,  -57.0, 5.5, 14),  # Amazon deforestation arc
        (-16.0,  -62.0, 4.0,  7),  # Bolivian Chiquitania
        ( 60.0,  110.0,12.0, 13),  # Siberia/Russian Far East
        (  0.5,  113.0, 4.5, 13),  # Borneo/Sumatra peat fires
        ( 19.0,  100.0, 4.0, 11),  # SE Asia mainland
        ( 21.0,   82.0, 3.5, 12),  # Central India dry forests
        ( 30.0,   75.0, 2.5,  8),  # North India crop residue
        (-23.0,  133.0, 9.0, 12),  # Australian interior
        (-35.0,  148.0, 3.5,  9),  # SE Australia
        ( 39.0, -121.0, 3.5, 10),  # California/Oregon
        ( 47.0, -114.0, 4.0,  7),  # Northern Rockies
        ( 54.0, -122.0, 5.0,  7),  # Canadian boreal
        ( 38.0,   16.0, 5.5,  7),  # Mediterranean basin
    ],

    "flood": [
        ( 23.5,   90.0, 2.5, 20),  # Bangladesh delta
        ( 26.5,   92.5, 2.0, 13),  # Assam/Brahmaputra
        ( 27.0,   68.5, 3.5, 17),  # Pakistan/Indus
        ( 25.0,   85.0, 2.5, 14),  # Ganges plain
        ( 16.5,   81.0, 3.0, 14),  # Indian east coast deltas
        ( 11.0,   76.0, 2.0,  9),  # Kerala/Karnataka
        ( 11.5,  105.0, 2.5, 15),  # Mekong delta
        ( 17.0,   95.5, 2.5, 12),  # Irrawaddy delta
        ( 14.5,  100.5, 2.5, 11),  # Chao Phraya
        ( 30.0,  112.0, 4.0, 18),  # Yangtze basin
        ( 34.5,  113.5, 3.5, 12),  # Yellow River
        ( 13.0,    4.0, 5.0, 13),  # Niger inland delta
        ( -3.0,   23.0, 5.0, 11),  # Congo basin
        ( 15.0,   32.5, 3.0, 10),  # Nile/Blue Nile
        (-16.0,   35.0, 3.0,  8),  # Mozambique/Zambezi
        (  7.5,   -4.5, 4.0, 10),  # West Africa coastal
        ( -4.0,  -63.0, 6.0, 14),  # Amazon várzea
        (-27.0,  -59.0, 5.0, 10),  # La Plata/Parana
        ( 35.0,  -90.0, 4.5, 10),  # Mississippi Valley
        ( 30.0,  -88.0, 3.5,  9),  # US Gulf Coast
        ( 48.5,   13.0, 4.0,  9),  # Rhine/Danube/Elbe
    ],

    "storm": [
        ( 14.0,  138.0, 7.0, 20),  # Philippine Sea genesis
        ( 15.0,  118.0, 4.5, 17),  # South China Sea
        ( 31.0,  133.0, 5.0, 13),  # Japan/Korea recurvature
        ( 18.0,  112.0, 3.5, 10),  # Northern SCS/Vietnam
        ( 14.0,   87.0, 4.5, 17),  # Bay of Bengal
        ( 15.0,   63.0, 4.0,  9),  # Arabian Sea
        ( 13.5,  -43.0, 6.5, 15),  # Atlantic MDR
        ( 20.0,  -77.0, 4.0, 14),  # Caribbean
        ( 24.0,  -89.0, 4.0, 13),  # Gulf of Mexico
        ( 30.0,  -84.0, 3.5, 11),  # US Gulf/Florida coast
        ( 34.5,  -74.5, 3.5,  9),  # US Atlantic seaboard
        ( 16.0, -101.0, 4.0, 12),  # Mexico Pacific coast
        ( 13.5,  -91.0, 3.0,  8),  # Central American Pacific
        (-17.0,   57.0, 5.5, 12),  # SW Indian Ocean
        (-20.0,  118.0, 4.0, 10),  # NW Australian coast
        (-17.0,  152.0, 4.5,  9),  # Queensland/Coral Sea
        (-15.0,  170.0, 5.5,  9),  # South Pacific
    ],
}

_INTRAPLATE_ZONES: list[tuple[float, float]] = [
    ( 36.6,  -89.6),  # New Madrid Seismic Zone
    ( 20.0,   78.0),  # Deccan Plateau
    ( 39.0,  116.0),  # North China Craton
    ( 33.5,   36.5),  # Dead Sea Transform
    ( 45.7,   26.7),  # Vrancea, Romania
    ( 42.5,    1.5),  # Pyrenees
    ( 36.6,    3.0),  # Tell Atlas, Algeria
    ( 52.0,    1.0),  # UK/North Sea
    ( 45.5,   80.5),  # Dzungarian Gate
    (-31.0,  116.5),  # SW Australia
    ( 47.5,   -0.5),  # Armorican Massif
    (-26.0,   27.5),  # Witwatersrand
]


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _gauss_clamp(mu: float, sigma: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, random.gauss(mu, sigma)))


def _cluster_coord(event_type: str) -> tuple[float, float]:
    zones   = _CLUSTERS[event_type]
    weights = [z[3] for z in zones]
    lat_c, lon_c, std, _ = random.choices(zones, weights=weights, k=1)[0]
    lat = round(_gauss_clamp(lat_c, std, -85.0,   85.0), 6)
    lon = round(_gauss_clamp(lon_c, std, -180.0, 180.0), 6)
    return lat, lon


# ── USGS GeoJSON helpers ──────────────────────────────────────────────────────

_USGS_NET_MAP = {
    "USGS": "us", "EMSC": "eu", "JMA": "jp", "IGN": "es", "GeoNet": "nz",
}
_EQ_SOURCES = list(_USGS_NET_MAP.keys())

_PLACE_DIRS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW",
               "NNE", "ENE", "ESE", "SSE", "SSW", "WSW", "WNW", "NNW"]


def _random_place() -> str:
    km = random.randint(1, 120)
    return f"{km}km {random.choice(_PLACE_DIRS)} of Seismic Zone"


def _make_earthquake_raw(mag: float, lat: float, lon: float, ts_str: str, source: str) -> dict[str, Any]:
    """Build a USGS GeoJSON Feature record."""
    epoch_ms = _ts_to_epoch_ms(ts_str)
    net      = _USGS_NET_MAP.get(source, "us")
    eid      = f"{net}{uuid.uuid4().hex[:8]}"
    depth    = round(random.uniform(2.0, 700.0), 1)

    nst    = random.randint(5, 200) if mag > 0 else None
    gap    = random.randint(10, 350) if mag > 0 else None
    dmin   = round(random.uniform(0.01, 50.0), 3) if mag > 0 else None
    mag_nst = random.randint(5, 100) if mag > 0 else None
    sig    = max(0, int((mag - 1.0) * 100 + random.gauss(0, 50))) if mag > 0 else 0

    return {
        "type": "Feature",
        "id": eid,
        "properties": {
            "mag":     mag,
            "magType": random.choice(["ml", "mb", "mww", "mwb", "md"]),
            "time":    epoch_ms,
            "place":   _random_place(),
            "net":     net,
            "updated": epoch_ms + random.randint(60_000, 3_600_000),
            "status":  random.choice(["reviewed", "automatic"]),
            "tsunami": 1 if mag >= 7.5 and random.random() < 0.30 else 0,
            "sig":     sig,
            "nst":     nst,
            "dmin":    dmin,
            "rms":     round(random.uniform(0.2, 2.0), 3),
            "gap":     gap,
            "magNst":  mag_nst,
        },
        "geometry": {
            "type":        "Point",
            "coordinates": [lon, lat, depth],
        },
    }


# ── NASA FIRMS MODIS helpers ──────────────────────────────────────────────────

def _make_fire_raw(frp: float, lat: float, lon: float, ts_str: str) -> dict[str, Any]:
    """Build a NASA FIRMS MODIS active fire record."""
    dt   = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
    hour = dt.hour

    brightness = round(300.0 + frp * 0.04 + random.gauss(0, 15), 1)
    brightness = max(280.0, min(500.0, brightness))

    return {
        "latitude":   lat,
        "longitude":  lon,
        "brightness": brightness,
        "scan":       round(random.uniform(0.8, 2.5), 1),
        "track":      round(random.uniform(0.8, 2.5), 1),
        "acq_date":   dt.strftime("%Y-%m-%d"),
        "acq_time":   dt.strftime("%H%M"),
        "satellite":  random.choice(["Terra", "Aqua", "NOAA-20", "Suomi NPP"]),
        "instrument": random.choice(["MODIS", "VIIRS"]),
        "confidence": random.randint(50, 100) if frp > 0 else random.randint(0, 30),
        "version":    "6.1NRT",
        "bright_t31": round(270.0 + random.gauss(0, 12), 1),
        "frp":        frp,
        "daynight":   "D" if 6 <= hour < 18 else "N",
    }


# ── USGS HWM helpers ──────────────────────────────────────────────────────────

_FLOOD_EVENT_TYPES = [
    "Monsoon", "Spring Snowmelt", "Tropical Cyclone", "Extratropical Storm",
    "Flash Flood", "River Overflow", "Dam Release", "Storm Surge",
]
_HWM_ENVIRONMENTS = ["Riverine", "Coastal", "Lacustrine", "Estuarine"]
_HWM_DESCRIPTIONS = [
    "River gauge station", "Bridge reference mark",
    "Flood control structure", "Riverside monitoring point",
    "Culvert inlet", "Levee crown",
]


def _make_flood_raw(severity_cm: float, lat: float, lon: float, ts_str: str, hwm_id: int) -> dict[str, Any]:
    """Build a USGS High Water Mark record."""
    height_ft = round(severity_cm / 30.48, 2) if severity_cm > 0 else 0.0
    year = ts_str[:4]
    peak_date = ts_str.replace("Z", "")  # IBTrACS-style: no trailing Z

    return {
        "hwm_id":              hwm_id,
        "site_latitude":       lat,
        "site_longitude":      lon,
        "height_above_gnd_ft": height_ft,
        "hwm_environment":     random.choice(_HWM_ENVIRONMENTS),
        "peak_date":           peak_date,
        "flood_event":         f"{random.choice(_FLOOD_EVENT_TYPES)} {year}",
        "country":             "Unknown",
        "state_name":          None,
        "county":              None,
        "site_description":    random.choice(_HWM_DESCRIPTIONS),
        "hwm_quality_id":      random.randint(1, 4),
        "approval_status_id":  random.randint(1, 2),
    }


# ── IBTrACS helpers ───────────────────────────────────────────────────────────

_STORM_NAMES = [
    "ALICE", "BOB", "CAROL", "DAVID", "EVE", "FRANK", "GRACE", "HENRY",
    "IDA",   "JOSE", "KATE", "LEE",  "MARIE", "NORBERT", "ODILE", "POLO",
    "ROSA",  "SERGIO", "TARA", "WILLA", "XINA", "YORK",  "ZELDA",
    "ANA",   "BINA", "DOVI", "ELOISE", "FABIAN", "GINA", "HAROLD",
    "IRMA",  "KENNETH", "LOTA", "NIRAN", "OSCAR", "PAM",  "RAKEL",
]


def _get_basin(lat: float, lon: float) -> str:
    if lat >= 0:
        if lon >= 100:
            return "WP"
        if 40 <= lon < 100:
            return "NI"
        if lon < -100:
            return "EP"
        return "NA"
    else:
        if lon >= 90:
            return "SP"
        return "SI"


def _generate_sid(lat: float, lon: float, ts_str: str) -> str:
    dt  = datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%SZ")
    doy = dt.timetuple().tm_yday
    hem = "N" if lat >= 0 else "S"
    return f"{dt.year}{doy:03d}{hem}{abs(int(lat * 10)):03d}{abs(int(lon)):03d}"


def _make_storm_raw(wind_kmh: float, lat: float, lon: float, ts_str: str) -> dict[str, Any]:
    """Build an IBTrACS Best Track record."""
    wmo_wind = round(wind_kmh / 1.852, 1) if wind_kmh > 0 else 0.0  # km/h → knots
    wmo_pres = (
        max(870, round(1015 - (wmo_wind ** 1.3) * 0.018))
        if wmo_wind >= 10 else None
    )

    if wmo_wind < 34:
        nature = "TD"
    elif wmo_wind < 64:
        nature = "TS"
    else:
        nature = "TY" if _get_basin(lat, lon) == "WP" else "HU"

    iso_time = ts_str.replace("T", " ").replace("Z", "")

    return {
        "sid":         _generate_sid(lat, lon, ts_str),
        "season":      int(ts_str[:4]),
        "number":      random.randint(1, 30),
        "basin":       _get_basin(lat, lon),
        "subbasin":    "MM",
        "name":        random.choice(_STORM_NAMES),
        "iso_time":    iso_time,
        "nature":      nature,
        "lat":         lat,
        "lon":         lon,
        "wmo_wind":    wmo_wind,
        "wmo_pres":    wmo_pres,
        "track_type":  "main",
        "dist2land":   round(random.uniform(0, 2000), 0),
        "landfall":    1 if random.random() < 0.15 else 0,
    }


# ── Per-type generators ───────────────────────────────────────────────────────

def _earthquakes(n: int) -> list[dict[str, Any]]:
    """
    severity (mag) range 1.0–9.5, log-normal distribution.
    ~5 % catastrophic (M8+), ~8 % micro (M1.0–1.4), ~1 % sensor null.
    ~10 % intraplate events. 4 random mainshocks get aftershock sequences.
    """
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            mag = 0.0
        elif roll < 0.06:
            mag = round(random.uniform(8.0, 9.5), 1)
        elif roll < 0.14:
            mag = round(random.uniform(1.0, 1.4), 1)
        else:
            raw = random.lognormvariate(math.log(3.5), 0.55)
            mag = round(max(1.0, min(9.5, raw)), 1)

        source = random.choice(_EQ_SOURCES)
        lat, lon = _cluster_coord("earthquake")

        if random.random() < 0.10:
            base_lat, base_lon = random.choice(_INTRAPLATE_ZONES)
            lat = round(_gauss_clamp(base_lat, 1.0, -85.0,   85.0), 6)
            lon = round(_gauss_clamp(base_lon, 1.0, -180.0, 180.0), 6)
            if mag > 6.0:
                mag = round(random.uniform(3.5, 6.0), 1)

        ts = _ts("earthquake")
        out.append(_make_earthquake_raw(mag, lat, lon, ts, source))

    # Aftershock injection
    mainshock_indices = random.sample(range(len(out)), min(4, len(out)))
    for idx in mainshock_indices:
        ms     = out[idx]
        ms_mag = ms["properties"]["mag"]
        ms_lat = ms["geometry"]["coordinates"][1]
        ms_lon = ms["geometry"]["coordinates"][0]
        ms_src = next(k for k, v in _USGS_NET_MAP.items() if v == ms["properties"]["net"])
        ms_ts  = datetime.fromtimestamp(ms["properties"]["time"] / 1000, tz=timezone.utc)

        for _ in range(random.randint(3, 8)):
            decay  = random.uniform(1.0, 3.5)
            as_mag = round(max(0.5, ms_mag - decay), 1)
            as_lat = round(_gauss_clamp(ms_lat, 0.25, -85.0,   85.0), 6)
            as_lon = round(_gauss_clamp(ms_lon, 0.25, -180.0, 180.0), 6)
            # Aftershock occurs 1 minute–72 hours after mainshock
            offset_s = random.randint(60, 259_200)
            from datetime import timedelta
            as_dt  = ms_ts + timedelta(seconds=offset_s)
            as_ts  = as_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            out.append(_make_earthquake_raw(as_mag, as_lat, as_lon, as_ts, ms_src))

    return out


def _fires(n: int) -> list[dict[str, Any]]:
    """
    FRP (Fire Radiative Power) in MW — NASA FIRMS unit.
    Distribution: exponential (many small, rare mega-fires).
    ~6 % extreme (FRP 7000–10000 MW), ~10 % tiny (<5 MW), ~1 % sensor null.
    """
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            frp = 0.0
        elif roll < 0.07:
            frp = round(random.uniform(7000.0, 10000.0), 2)
        elif roll < 0.17:
            frp = round(random.uniform(1.0, 4.9), 2)
        else:
            raw = random.expovariate(1.0 / 300.0)
            frp = round(max(1.0, min(10000.0, raw)), 2)

        lat, lon = _cluster_coord("fire")
        ts = _ts("fire")
        out.append(_make_fire_raw(frp, lat, lon, ts))

    return out


def _floods(n: int) -> list[dict[str, Any]]:
    """
    Water level height stored as height_above_gnd_ft (USGS HWM unit).
    Normalizer converts to cm (×30.48) for unified severity_raw.
    ~15 % extreme flood (>26 ft / 800 cm), ~12 % marginal, ~1 % sensor null.
    """
    out: list[dict[str, Any]] = []
    hwm_id_counter = 100_000

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            level_cm = 0.0
        elif roll < 0.16:
            level_cm = round(random.uniform(800.0, 2000.0), 2)
        elif roll < 0.28:
            level_cm = round(random.uniform(0.1, 4.9), 2)
        else:
            level_cm = round(random.uniform(5.0, 800.0), 2)

        lat, lon = _cluster_coord("flood")
        ts = _ts("flood")
        out.append(_make_flood_raw(level_cm, lat, lon, ts, hwm_id_counter))
        hwm_id_counter += 1

    return out


def _storms(n: int) -> list[dict[str, Any]]:
    """
    Wind speed in km/h → stored as wmo_wind in knots (IBTrACS unit).
    Normalizer converts back to km/h for unified severity_raw.
    ~7 % Category-5 (200–300 km/h), ~10 % tropical depression, ~1 % null.
    """
    out: list[dict[str, Any]] = []

    for _ in range(n):
        roll = random.random()
        if roll < 0.01:
            wind_kmh = 0.0
        elif roll < 0.08:
            wind_kmh = round(_gauss_clamp(250.0, 20.0, 200.0, 300.0), 1)
        elif roll < 0.18:
            wind_kmh = round(_gauss_clamp(22.0, 5.0, 10.0, 34.0), 1)
        else:
            wind_kmh = round(_gauss_clamp(90.0, 40.0, 35.0, 200.0), 1)

        lat, lon = _cluster_coord("storm")
        ts = _ts("storm")
        out.append(_make_storm_raw(wind_kmh, lat, lon, ts))

    if not any(e["wmo_wind"] == 0.0 for e in out):
        out[0]["wmo_wind"] = 0.0

    return out


# ── Public API ────────────────────────────────────────────────────────────────

def generate_all_events(
    n_earthquakes: int = 100,
    n_fires:       int = 105,
    n_floods:      int = 105,
    n_storms:      int = 95,
) -> dict[str, list[dict[str, Any]]]:
    """
    Generate raw source-schema records for all disaster types.

    Returns a dict of raw records (NOT unified Kafka schema).
    Pass the result to normalizer.normalize_all() to get the unified schema.

    Args:
        n_earthquakes: Base count before aftershock injection.
        n_fires:       Number of fire events.
        n_floods:      Number of flood events.
        n_storms:      Number of storm events.

    Returns:
        {
          "earthquake": [USGS GeoJSON Feature, ...],
          "fire":       [NASA FIRMS MODIS record, ...],
          "flood":      [USGS HWM record, ...],
          "storm":      [IBTrACS record, ...],
        }
    """
    return {
        "earthquake": _earthquakes(n_earthquakes),
        "fire":       _fires(n_fires),
        "flood":      _floods(n_floods),
        "storm":      _storms(n_storms),
    }
