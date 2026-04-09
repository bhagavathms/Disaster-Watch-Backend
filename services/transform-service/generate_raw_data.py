"""
generate_raw_data.py
Synthetic Raw Data Generator -- Transform Service
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Generates realistic synthetic data in EXACT real-source schemas:
  USGS Earthquake  → data/raw/usgs_earthquakes.geojson   (GeoJSON FeatureCollection)
  USGS Flood (HWM) → data/raw/usgs_floods.json           (HWM JSON array)
  NASA FIRMS MODIS → data/raw/nasa_firms_modis.csv        (CSV)
  NASA FIRMS VIIRS → data/raw/nasa_firms_viirs.csv        (CSV)
  OpenWeather      → data/raw/openweather_storms.json     (JSON array)

All values are statistically plausible and geographically realistic.
No actual API calls are made.

Usage:
    python generate_raw_data.py
    python generate_raw_data.py --seed 42 --out-dir ../../data/raw
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Synthetic raw data generator")
    p.add_argument("--seed", type=int, default=2024, help="Random seed (default: 2024)")
    p.add_argument("--out-dir", default=None,
                   help="Output directory (default: ../../data/raw relative to this file)")
    return p.parse_args()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def _rand_dt(rng: random.Random, start: datetime, end: datetime) -> datetime:
    delta = (end - start).total_seconds()
    return start + timedelta(seconds=rng.uniform(0, delta))

def _epoch_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)

def _epoch_s(dt: datetime) -> int:
    return int(dt.timestamp())


# ── Geographic zones ──────────────────────────────────────────────────────────

_EQ_ZONES = [
    # (lat_min, lat_max, lon_min, lon_max, weight, region_hint)
    (30.0,  45.0, 130.0, 145.0, 15, "Japan"),
    (-8.0,   5.0,  95.0, 140.0, 14, "Indonesia"),
    (-50.0, -5.0, -75.0, -65.0, 10, "Chile/Peru"),
    (36.0,  42.0,  26.0,  45.0,  8, "Turkey"),
    (25.0,  38.0,  60.0,  75.0,  8, "Pakistan/Afghanistan"),
    ( 5.0,  20.0, 118.0, 127.0,  8, "Philippines"),
    (35.0,  47.0,  12.0,  28.0,  6, "Greece/Italy"),
    (25.0,  40.0,  44.0,  63.0,  7, "Iran"),
    (15.0,  32.0,-117.0, -85.0,  7, "Mexico/C.America"),
    (-47.0,-34.0, 165.0, 178.0,  5, "New Zealand"),
    (27.0,  35.0,  84.0,  95.0,  5, "Nepal/Tibet"),
    (35.0,  60.0,  55.0,  80.0,  3, "Central Asia"),
]

_FIRE_ZONES = [
    (-40.0,-10.0, 113.0, 154.0, 15, "AUS", "Australia"),
    ( 32.0, 47.0,-124.0,-114.0, 14, "US",  "North America"),
    (-15.0, -3.0, -70.0, -47.0, 12, "BR",  "Amazon"),
    ( -5.0, 15.0,  95.0, 140.0, 10, "ID",  "Southeast Asia"),
    ( 50.0, 70.0,  70.0, 140.0,  9, "RU",  "Siberia"),
    ( 36.0, 45.0,  -9.0,  28.0,  8, "EU",  "Southern Europe"),
    (-30.0, 15.0, -20.0,  45.0, 12, "AF",  "Sub-Saharan Africa"),
]

_FLOOD_EVENTS = [
    # (lat_c, lon_c, radius_deg, state_abbr, country, waterbodies)
    (24.0,  89.5, 1.0, "BD", "Bangladesh",  ["Brahmaputra", "Jamuna", "Padma"]),
    (30.5, 114.5, 1.5, "CN", "China",       ["Yangtze", "Han River"]),
    (25.5,  85.0, 1.0, "IN", "India",       ["Ganges", "Kosi River"]),
    (30.2,  71.0, 0.8, "PK", "Pakistan",    ["Indus", "Chenab"]),
    (33.5, -91.5, 1.2, "US", "USA",         ["Mississippi", "Arkansas River"]),
    (30.0, -90.0, 0.8, "US", "USA",         ["Mississippi", "Pearl River"]),
    (35.0,-106.0, 0.7, "US", "USA",         ["Rio Grande", "Pecos River"]),
    (47.5,   8.5, 0.6, "DE", "Germany",     ["Rhine", "Neckar"]),
    (48.2,  17.0, 0.7, "SK", "Slovakia",    ["Danube", "Morava"]),
    (22.5, 114.0, 0.5, "CN", "China",       ["Pearl River", "East River"]),
    (-23.0,-46.5, 0.8, "BR", "Brazil",      ["Tiete", "Paraiba do Sul"]),
    (13.5,   2.5, 0.9, "NG", "Nigeria",     ["Niger", "Benue"]),
    (48.4,-115.5, 0.5, "US", "USA",         ["Kootenai River", "Bull Lake"]),
    (45.8,-121.5, 0.6, "US", "USA",         ["Columbia River", "Willamette"]),
    ( 1.5, 103.5, 0.4, "SG", "Malaysia",    ["Johor River", "Sungai Muar"]),
]

_STORM_ZONES = [
    # (lat_min, lat_max, lon_min, lon_max, weight, country_code, basin)
    (10.0, 30.0, 120.0, 160.0, 20, "PH", "Western Pacific"),
    (15.0, 35.0, -95.0, -60.0, 15, "US", "North Atlantic"),
    (10.0, 25.0,  80.0,  95.0, 14, "IN", "Bay of Bengal"),
    (10.0, 25.0,  55.0,  75.0,  8, "IN", "Arabian Sea"),
    (-20.0,-10.0,155.0, 175.0,  6, "AU", "South Pacific"),
    (35.0, 50.0,  -5.0,  25.0,  7, "IT", "Mediterranean"),
    (25.0, 40.0, 120.0, 140.0, 10, "JP", "East China Sea"),
    ( 5.0, 20.0,  90.0, 115.0,  8, "TH", "South China Sea"),
    (15.0, 30.0, -85.0, -65.0,  7, "MX", "Gulf of Mexico"),
    (20.0, 35.0, 140.0, 160.0,  5, "JP", "Northwest Pacific"),
]

# ── Date range ────────────────────────────────────────────────────────────────
_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
_END   = datetime(2025, 12, 31, 23, 59, 59, tzinfo=timezone.utc)


# ══════════════════════════════════════════════════════════════════════════════
# 1.  USGS EARTHQUAKE  (GeoJSON FeatureCollection)
# ══════════════════════════════════════════════════════════════════════════════

_MAG_TYPES = ["mb", "mww", "ml", "mw", "md", "mwr"]
_NETWORKS  = ["us", "ci", "nc", "ak", "uu", "hv", "se", "nn"]
_STATUSES  = ["reviewed", "reviewed", "reviewed", "automatic"]

def _eq_mag(rng: random.Random) -> float:
    """Gutenberg-Richter-like: small quakes most common."""
    # Power-law: inverse transform sampling
    # P(M>m) ∝ 10^(-b*(m-mmin)), b≈1
    mmin, mmax = 2.0, 8.5
    u = rng.random()
    mag = mmin - math.log10(1 - u * (1 - 10**(-(mmax-mmin)))) / 1.0
    return round(min(mmax, max(mmin, mag)), 1)

def _eq_alert(mag: float, rng: random.Random) -> str | None:
    if mag >= 7.5: return "red"   if rng.random() < 0.7 else "orange"
    if mag >= 6.5: return "orange" if rng.random() < 0.5 else "yellow"
    if mag >= 5.5: return "yellow" if rng.random() < 0.3 else "green"
    if mag >= 4.5: return "green"  if rng.random() < 0.2 else None
    return None

def _eq_sig(mag: float, rng: random.Random) -> int:
    base = int(mag * mag * 15)
    return min(1000, max(0, base + rng.randint(-30, 30)))

def generate_earthquakes(rng: random.Random, n: int = 130) -> dict:
    features = []
    zone_weights = [z[4] for z in _EQ_ZONES]

    for _ in range(n):
        zone = rng.choices(_EQ_ZONES, weights=zone_weights, k=1)[0]
        lat  = round(rng.uniform(zone[0], zone[1]), 4)
        lon  = round(rng.uniform(zone[2], zone[3]), 4)
        dep  = round(rng.choices(
            [rng.uniform(0, 35),  rng.uniform(35, 70), rng.uniform(70, 300)],
            weights=[50, 30, 20]
        )[0], 3)

        mag     = _eq_mag(rng)
        mag_type = rng.choice(_MAG_TYPES)
        dt      = _rand_dt(rng, _START, _END)
        t_ms    = _epoch_ms(dt)
        net     = rng.choice(_NETWORKS)
        code    = f"{net}{rng.randint(10000, 99999)}"
        eid     = f"us{rng.randint(6000000000, 7999999999):010d}"
        sig     = _eq_sig(mag, rng)
        alert   = _eq_alert(mag, rng)
        tsunami = 1 if (mag >= 7.0 and dep < 60 and rng.random() < 0.3) else 0
        nst     = rng.randint(5, 150) if mag >= 3.0 else None
        gap     = round(rng.uniform(10, 300), 1)
        rms_val = round(rng.uniform(0.3, 1.8), 2)
        dmin    = round(rng.uniform(0.1, 15), 3)
        felt    = int(rng.lognormvariate(2, 1.5)) if mag >= 3.0 else None
        cdi     = round(min(10, max(1, mag * 0.8 + rng.uniform(-1, 1))), 1) if felt else None
        mmi_val = round(min(10, max(1, mag * 1.1 - dep * 0.01 + rng.uniform(-0.5, 0.5))), 3) \
                  if mag >= 5.0 else None

        title   = f"M {mag} - {zone[5]}"
        place   = f"{rng.randint(5, 200)} km from {zone[5]}"

        features.append({
            "type": "Feature",
            "properties": {
                "mag":     mag,
                "place":   place,
                "time":    t_ms,
                "updated": t_ms + rng.randint(60_000, 3_600_000),
                "tz":      None,
                "url":     f"https://earthquake.usgs.gov/earthquakes/eventpage/{eid}",
                "detail":  f"https://earthquake.usgs.gov/fdsnws/event/1/query?eventid={eid}&format=geojson",
                "felt":    felt,
                "cdi":     cdi,
                "mmi":     mmi_val,
                "alert":   alert,
                "status":  rng.choice(_STATUSES),
                "tsunami": tsunami,
                "sig":     sig,
                "net":     net,
                "code":    code,
                "ids":     f",{eid},",
                "sources": f",{net},",
                "types":   ",origin,phase-data,",
                "nst":     nst,
                "dmin":    dmin,
                "rms":     rms_val,
                "gap":     gap,
                "magType": mag_type,
                "type":    "earthquake",
                "title":   title,
            },
            "geometry": {"type": "Point", "coordinates": [lon, lat, dep]},
            "id": eid,
        })

    return {
        "type": "FeatureCollection",
        "metadata": {
            "generated": _epoch_ms(datetime.now(timezone.utc)),
            "title":     "Synthetic USGS Earthquakes",
            "status":    200,
            "count":     len(features),
        },
        "features": features,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 2.  USGS FLOOD — High-Water Mark
# ══════════════════════════════════════════════════════════════════════════════

_QUALITY_NAMES = {
    2: "Good: +/- 0.10 ft",
    3: "Fair: +/- 0.20 ft",
    4: "Poor: +/- 0.40 ft",
}
_QUALITY_UNCERTAINTY = {2: 0.1, 3: 0.2, 4: 0.4}
_HWM_ENVIRONMENTS = ["Riverine", "Riverine", "Riverine", "Coastal"]
_VERTICAL_DATUM  = "NAVD88"
_VERTICAL_METHOD = "RT-GNSS"
_HORIZ_DATUM     = "WGS84 (from Digital Map)"
_HORIZ_METHOD    = "RT-GNSS"

def generate_floods(rng: random.Random) -> list[dict]:
    records = []
    hwm_id  = 49000
    event_id = 300

    for flood_zone in rng.choices(_FLOOD_EVENTS, k=len(_FLOOD_EVENTS)):
        event_id += rng.randint(1, 5)
        event_name = f"Synthetic {flood_zone[4]} Flood Event"
        lat_c, lon_c = flood_zone[0], flood_zone[1]
        radius       = flood_zone[2]
        state        = flood_zone[3]
        country      = flood_zone[4]
        waterbodies  = flood_zone[5]

        # baseline elevation at this site (ft), realistic for river valleys
        base_elev = round(rng.uniform(50, 3500), 1)
        # flood adds 1–80 ft of water
        flood_add = round(rng.lognormvariate(2.5, 0.8), 2)
        peak_elev = base_elev + flood_add

        flag_dt   = _rand_dt(rng, _START, _END)
        survey_dt = flag_dt + timedelta(days=rng.randint(1, 45))
        n_marks   = rng.randint(3, 9)
        environment = rng.choice(_HWM_ENVIRONMENTS)
        surveyor_name = rng.choice(["Alex Smith", "Maria Garcia", "James Chen",
                                    "Sarah Johnson", "David Kim"])
        surveyor_id = rng.randint(1000, 9999)
        waterbody  = rng.choice(waterbodies)

        for i in range(n_marks):
            hwm_id += 1
            offset_lat = rng.uniform(-radius * 0.3, radius * 0.3)
            offset_lon = rng.uniform(-radius * 0.3, radius * 0.3)
            pt_lat = round(lat_c + offset_lat, 8)
            pt_lon = round(lon_c + offset_lon, 8)

            q_id   = rng.choices([2, 3, 4], weights=[50, 35, 15])[0]
            elev   = round(peak_elev + rng.uniform(-2, 2), 3)
            marker = rng.choice(["Nail and HWM tag", "Not marked", "PK nail",
                                  "Screw and HWM tag"])

            records.append({
                "latitude":              pt_lat,
                "longitude":             pt_lon,
                "eventName":             event_name,
                "hwmTypeName":           "Debris",
                "hwmQualityName":        _QUALITY_NAMES[q_id],
                "verticalDatumName":     _VERTICAL_DATUM,
                "verticalMethodName":    _VERTICAL_METHOD,
                "approvalMember":        "",
                "markerName":            marker,
                "horizontalMethodName":  _HORIZ_METHOD,
                "horizontalDatumName":   _HORIZ_DATUM,
                "flagMemberName":        surveyor_name,
                "surveyMemberName":      surveyor_name,
                "site_no":               f"{state}SYN{hwm_id:05d}",
                "siteDescription":       f"{waterbody} near {country}",
                "sitePriorityName":      "",
                "networkNames":          "",
                "stateName":             state,
                "countyName":            country,
                "sitePermHousing":       "No",
                "site_latitude":         pt_lat,
                "site_longitude":        pt_lon,
                "hwm_id":                hwm_id,
                "waterbody":             waterbody,
                "site_id":               hwm_id - 1000,
                "event_id":              event_id,
                "hwm_type_id":           2,
                "hwm_quality_id":        q_id,
                "hwm_locationdescription": f"Near {waterbody}",
                "latitude_dd":           pt_lat,
                "longitude_dd":          pt_lon,
                "survey_date":           survey_dt.strftime("%Y-%m-%dT07:00:00"),
                "elev_ft":               elev,
                "vdatum_id":             2,
                "vcollect_method_id":    6,
                "bank":                  rng.choice(["Left", "Right", "N/A"]),
                "marker_id":             6,
                "hcollect_method_id":    6,
                "hwm_environment":       environment,
                "flag_date":             flag_dt.strftime("%Y-%m-%dT07:00:00"),
                "stillwater":            rng.choice([0, 0, 0, 1]),
                "hdatum_id":             4,
                "flag_member_id":        surveyor_id,
                "survey_member_id":      surveyor_id,
                "uncertainty":           0.2,
                "hwm_uncertainty":       _QUALITY_UNCERTAINTY[q_id],
                "hwm_label":             f"SYN{i+1:03d}",
                "files":                 [],
            })

    return records


# ══════════════════════════════════════════════════════════════════════════════
# 3.  NASA FIRMS — MODIS
# ══════════════════════════════════════════════════════════════════════════════

_MODIS_SATELLITES = ["Terra", "Aqua"]

def _frp_sample(rng: random.Random) -> float:
    """Right-skewed: most fires low FRP, few extreme."""
    return round(max(0.5, rng.lognormvariate(2.5, 1.4)), 2)

def generate_firms_modis(rng: random.Random, n_clusters: int = 80) -> list[dict]:
    rows = []
    zone_weights = [z[4] for z in _FIRE_ZONES]
    acq_date = _rand_dt(rng, _START, _END)

    for _ in range(n_clusters):
        zone     = rng.choices(_FIRE_ZONES, weights=zone_weights, k=1)[0]
        c_lat    = rng.uniform(zone[0], zone[1])
        c_lon    = rng.uniform(zone[2], zone[3])
        n_pixels = rng.randint(2, 8)
        acq_date = _rand_dt(rng, _START, _END)
        date_str = acq_date.strftime("%d-%m-%Y")
        acq_time = acq_date.hour * 100 + acq_date.minute
        sat      = rng.choice(_MODIS_SATELLITES)
        version  = "6.2"

        for _ in range(n_pixels):
            lat  = round(c_lat + rng.uniform(-0.3, 0.3), 4)
            lon  = round(c_lon + rng.uniform(-0.3, 0.3), 4)
            frp  = _frp_sample(rng)
            scan = round(rng.uniform(0.8, 4.0), 1)
            trk  = round(rng.uniform(0.8, 2.0), 1)
            conf = rng.randint(20, 100)
            bright   = round(rng.uniform(295, 500), 1)
            bright31 = round(rng.uniform(270, 310), 1)
            dn       = "D" if 600 <= acq_time <= 1800 else "N"
            rows.append({
                "latitude":   lat,
                "longitude":  lon,
                "brightness": bright,
                "scan":       scan,
                "track":      trk,
                "acq_date":   date_str,
                "acq_time":   acq_time,
                "satellite":  sat,
                "instrument": "MODIS",
                "confidence": conf,
                "version":    version,
                "bright_t31": bright31,
                "frp":        frp,
                "daynight":   dn,
                "type":       0,
            })

    return rows


# ══════════════════════════════════════════════════════════════════════════════
# 4.  NASA FIRMS — VIIRS
# ══════════════════════════════════════════════════════════════════════════════

_VIIRS_SATS = ["N", "1", "2"]
_VIIRS_CONF = ["l", "n", "n", "n", "h", "h"]

def generate_firms_viirs(rng: random.Random, n_clusters: int = 80) -> list[dict]:
    rows = []
    zone_weights = [z[4] for z in _FIRE_ZONES]

    for _ in range(n_clusters):
        zone     = rng.choices(_FIRE_ZONES, weights=zone_weights, k=1)[0]
        c_lat    = rng.uniform(zone[0], zone[1])
        c_lon    = rng.uniform(zone[2], zone[3])
        n_pixels = rng.randint(2, 8)
        acq_date = _rand_dt(rng, _START, _END)
        date_str = acq_date.strftime("%d-%m-%Y")
        acq_time = acq_date.hour * 100 + acq_date.minute
        sat      = rng.choice(_VIIRS_SATS)

        for _ in range(n_pixels):
            lat  = round(c_lat + rng.uniform(-0.15, 0.15), 5)
            lon  = round(c_lon + rng.uniform(-0.15, 0.15), 5)
            frp  = _frp_sample(rng)
            scan = round(rng.uniform(0.32, 0.75), 2)
            trk  = round(rng.uniform(0.36, 0.76), 2)
            conf = rng.choice(_VIIRS_CONF)
            ti4  = round(rng.uniform(300, 500), 2)
            ti5  = round(rng.uniform(260, 310), 2)
            dn   = "D" if 600 <= acq_time <= 1800 else "N"
            rows.append({
                "latitude":   lat,
                "longitude":  lon,
                "bright_ti4": ti4,
                "scan":       scan,
                "track":      trk,
                "acq_date":   date_str,
                "acq_time":   acq_time,
                "satellite":  sat,
                "instrument": "VIIRS",
                "confidence": conf,
                "version":    2,
                "bright_ti5": ti5,
                "frp":        frp,
                "daynight":   dn,
                "type":       0,
            })

    return rows


# ══════════════════════════════════════════════════════════════════════════════
# 5.  OPENWEATHER — Storm snapshots
# ══════════════════════════════════════════════════════════════════════════════

_STORM_CONDITIONS = [
    (200, "Thunderstorm", "thunderstorm with light rain"),
    (201, "Thunderstorm", "thunderstorm with rain"),
    (202, "Thunderstorm", "thunderstorm with heavy rain"),
    (211, "Thunderstorm", "thunderstorm"),
    (212, "Thunderstorm", "heavy thunderstorm"),
    (502, "Rain",         "heavy intensity rain"),
    (503, "Rain",         "very heavy rain"),
    (504, "Rain",         "extreme rain"),
    (521, "Rain",         "shower rain"),
    (531, "Rain",         "ragged shower rain"),
]

_CITY_NAMES = [
    ("Manila", "PH"), ("Tokyo", "JP"), ("Miami", "US"), ("Mumbai", "IN"),
    ("Dhaka", "BD"), ("Bangkok", "TH"), ("Taipei", "TW"), ("Osaka", "JP"),
    ("Houston", "US"), ("New Orleans", "US"), ("Kolkata", "IN"),
    ("Colombo", "LK"), ("Guangzhou", "CN"), ("Haikou", "CN"),
    ("Puerto Rico", "PR"), ("Havana", "CU"), ("Cancun", "MX"),
    ("Darwin", "AU"), ("Guam", "GU"), ("Ho Chi Minh", "VN"),
    ("Yangon", "MM"), ("Chennai", "IN"), ("Chittagong", "BD"),
    ("Kaohsiung", "TW"), ("Okinawa", "JP"), ("Naha", "JP"),
    ("Suva", "FJ"), ("Port Moresby", "PG"), ("Karachi", "PK"),
]

def _beaufort(speed_ms: float) -> int:
    thresholds = [0.3,1.6,3.4,5.5,8.0,10.8,13.9,17.2,20.8,24.5,28.5,32.7]
    for b, t in enumerate(thresholds):
        if speed_ms < t:
            return b
    return 12

def generate_storms(rng: random.Random, n: int = 130) -> list[dict]:
    records = []
    zone_weights = [z[4] for z in _STORM_ZONES]
    city_id_start = 1_600_000

    for i in range(n):
        zone       = rng.choices(_STORM_ZONES, weights=zone_weights, k=1)[0]
        lat        = round(rng.uniform(zone[0], zone[1]), 2)
        lon        = round(rng.uniform(zone[2], zone[3]), 2)
        dt         = _rand_dt(rng, _START, _END)
        city_name, country = rng.choice(_CITY_NAMES)
        city_id    = city_id_start + i

        # Storm-condition values — all exceed storm threshold
        wind_ms    = round(rng.uniform(10.0, 80.0), 2)
        gust_ms    = round(wind_ms + rng.uniform(1, 20), 2)
        rain_1h    = round(rng.lognormvariate(2.0, 0.9), 2)
        rain_3h    = round(rain_1h * rng.uniform(1.5, 3.5), 2)
        pressure   = rng.randint(930, 1005)
        humidity   = rng.randint(70, 100)
        temp_k     = round(rng.uniform(290, 310), 2)
        visibility = rng.randint(500, 8000)
        clouds     = rng.randint(70, 100)
        condition  = rng.choice(_STORM_CONDITIONS)

        sunrise_dt = dt.replace(hour=6, minute=0, second=0)
        sunset_dt  = dt.replace(hour=18, minute=30, second=0)

        records.append({
            "coord":   {"lon": lon, "lat": lat},
            "weather": [{"id": condition[0], "main": condition[1],
                         "description": condition[2], "icon": "10d"}],
            "base":    "stations",
            "main":    {
                "temp":       temp_k,
                "feels_like": round(temp_k - rng.uniform(0, 3), 2),
                "temp_min":   round(temp_k - rng.uniform(0, 2), 2),
                "temp_max":   round(temp_k + rng.uniform(0, 2), 2),
                "pressure":   pressure,
                "humidity":   humidity,
                "sea_level":  pressure,
                "grnd_level": pressure - rng.randint(0, 30),
            },
            "visibility": visibility,
            "wind": {
                "speed": wind_ms,
                "deg":   rng.randint(0, 360),
                "gust":  gust_ms,
            },
            "rain":   {"1h": rain_1h, "3h": rain_3h},
            "clouds": {"all": clouds},
            "dt":     _epoch_s(dt),
            "sys":    {
                "type":    2,
                "id":      rng.randint(1000, 9999),
                "country": country,
                "sunrise": _epoch_s(sunrise_dt),
                "sunset":  _epoch_s(sunset_dt),
            },
            "timezone": rng.choice([-18000, -14400, 0, 19800, 28800, 32400]),
            "id":       city_id,
            "name":     city_name,
            "cod":      200,
        })

    return records


# ── Writers ───────────────────────────────────────────────────────────────────

def _write_json(path: str, obj) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    print(f"  Written: {path}  ({os.path.getsize(path):,} bytes)")

def _write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Written: {path}  ({os.path.getsize(path):,} bytes)  rows={len(rows)}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = _parse_args()
    rng  = random.Random(args.seed)

    if args.out_dir:
        out_dir = os.path.abspath(args.out_dir)
    else:
        out_dir = os.path.normpath(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         "..", "..", "data", "raw")
        )
    os.makedirs(out_dir, exist_ok=True)

    print(f"\nGenerating synthetic raw data → {out_dir}\n")

    print("[1/5] USGS Earthquakes (GeoJSON FeatureCollection) ...")
    eq_data = generate_earthquakes(rng, n=130)
    _write_json(os.path.join(out_dir, "usgs_earthquakes.geojson"), eq_data)
    print(f"       {eq_data['metadata']['count']} earthquake features")

    print("\n[2/5] USGS Floods — High-Water Marks ...")
    flood_data = generate_floods(rng)
    _write_json(os.path.join(out_dir, "usgs_floods.json"), flood_data)
    events_seen = len({r["event_id"] for r in flood_data})
    print(f"       {len(flood_data)} HWM records across {events_seen} flood events")

    print("\n[3/5] NASA FIRMS MODIS ...")
    modis_data = generate_firms_modis(rng, n_clusters=80)
    _write_csv(os.path.join(out_dir, "nasa_firms_modis.csv"), modis_data)

    print("\n[4/5] NASA FIRMS VIIRS ...")
    viirs_data = generate_firms_viirs(rng, n_clusters=80)
    _write_csv(os.path.join(out_dir, "nasa_firms_viirs.csv"), viirs_data)

    print("\n[5/5] OpenWeather Storm Snapshots ...")
    storm_data = generate_storms(rng, n=130)
    _write_json(os.path.join(out_dir, "openweather_storms.json"), storm_data)
    print(f"       {len(storm_data)} storm snapshots")

    print("\nRaw data generation complete.\n")


if __name__ == "__main__":
    main()
