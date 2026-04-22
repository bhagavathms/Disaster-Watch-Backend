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
    latitude/longitude → (country, region) via the `reverse_geocoder` library,
    which performs a k-d tree nearest-neighbour lookup against a global city
    database (no internet required, fully offline).

    Install:  pip install reverse_geocoder
    Fallback: if not installed, falls back to broad hemisphere labels.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime

log = logging.getLogger(__name__)

# ── Reverse geocoder setup ────────────────────────────────────────────────────

try:
    import reverse_geocoder as _rg
    _RG_AVAILABLE = True
    log.info("[TRANSFORM] reverse_geocoder loaded — using accurate country lookup")
except ImportError:
    _RG_AVAILABLE = False
    log.warning(
        "[TRANSFORM] reverse_geocoder not installed. Country/region will be approximate. "
        "Run:  pip install reverse_geocoder"
    )

# ISO 3166-1 alpha-2 → full country name
_CC_TO_COUNTRY: dict[str, str] = {
    "AF": "Afghanistan",     "AL": "Albania",          "DZ": "Algeria",
    "AO": "Angola",          "AR": "Argentina",        "AM": "Armenia",
    "AU": "Australia",       "AZ": "Azerbaijan",       "BD": "Bangladesh",
    "BO": "Bolivia",         "BA": "Bosnia-Herzegovina","BR": "Brazil",
    "BN": "Brunei",          "BF": "Burkina Faso",     "BI": "Burundi",
    "KH": "Cambodia",        "CM": "Cameroon",         "CA": "Canada",
    "CF": "Central African Republic", "TD": "Chad",   "CL": "Chile",
    "CN": "China",           "CO": "Colombia",         "CD": "DR Congo",
    "CG": "Republic of Congo", "CR": "Costa Rica",    "CI": "Ivory Coast",
    "CU": "Cuba",            "CY": "Cyprus",           "DJ": "Djibouti",
    "DO": "Dominican Republic", "EC": "Ecuador",       "EG": "Egypt",
    "SV": "El Salvador",     "ER": "Eritrea",          "ET": "Ethiopia",
    "FJ": "Fiji",            "FR": "France",           "GA": "Gabon",
    "GE": "Georgia",         "DE": "Germany",          "GH": "Ghana",
    "GR": "Greece",          "GT": "Guatemala",        "GN": "Guinea",
    "GW": "Guinea-Bissau",   "HT": "Haiti",            "HN": "Honduras",
    "HU": "Hungary",         "IS": "Iceland",          "IN": "India",
    "ID": "Indonesia",       "IR": "Iran",             "IQ": "Iraq",
    "IT": "Italy",           "JM": "Jamaica",          "JP": "Japan",
    "JO": "Jordan",          "KZ": "Kazakhstan",       "KE": "Kenya",
    "KG": "Kyrgyzstan",      "LA": "Laos",             "LB": "Lebanon",
    "LR": "Liberia",         "LY": "Libya",            "MG": "Madagascar",
    "MW": "Malawi",          "MY": "Malaysia",         "ML": "Mali",
    "MR": "Mauritania",      "MX": "Mexico",           "MN": "Mongolia",
    "MA": "Morocco",         "MZ": "Mozambique",       "MM": "Myanmar",
    "NA": "Namibia",         "NP": "Nepal",            "NZ": "New Zealand",
    "NI": "Nicaragua",       "NE": "Niger",            "NG": "Nigeria",
    "MK": "North Macedonia", "NO": "Norway",           "OM": "Oman",
    "PK": "Pakistan",        "PA": "Panama",           "PG": "Papua New Guinea",
    "PY": "Paraguay",        "PE": "Peru",             "PH": "Philippines",
    "PL": "Poland",          "PT": "Portugal",         "RO": "Romania",
    "RU": "Russia",          "RW": "Rwanda",           "SA": "Saudi Arabia",
    "SN": "Senegal",         "SL": "Sierra Leone",     "SB": "Solomon Islands",
    "SO": "Somalia",         "ZA": "South Africa",     "SS": "South Sudan",
    "ES": "Spain",           "LK": "Sri Lanka",        "SD": "Sudan",
    "SR": "Suriname",        "SY": "Syria",            "TJ": "Tajikistan",
    "TZ": "Tanzania",        "TH": "Thailand",         "TL": "Timor-Leste",
    "TG": "Togo",            "TO": "Tonga",            "TT": "Trinidad and Tobago",
    "TN": "Tunisia",         "TR": "Turkey",           "TM": "Turkmenistan",
    "UG": "Uganda",          "UA": "Ukraine",          "AE": "United Arab Emirates",
    "GB": "United Kingdom",  "US": "United States",    "UY": "Uruguay",
    "UZ": "Uzbekistan",      "VU": "Vanuatu",          "VE": "Venezuela",
    "VN": "Vietnam",         "YE": "Yemen",            "ZM": "Zambia",
    "ZW": "Zimbabwe",        "TW": "Taiwan",           "KR": "South Korea",
    "KP": "North Korea",     "FM": "Micronesia",       "PW": "Palau",
    "WS": "Samoa",           "KI": "Kiribati",         "PF": "French Polynesia",
    "NC": "New Caledonia",   "GU": "Guam",
}

# ISO alpha-2 → broad sub-continental region (for dim_location.region)
_CC_TO_REGION: dict[str, str] = {
    # East Asia
    "JP": "East Asia",  "CN": "East Asia",  "KR": "East Asia",
    "KP": "East Asia",  "MN": "East Asia",  "TW": "East Asia",
    # Southeast Asia
    "ID": "Southeast Asia",  "PH": "Southeast Asia",  "VN": "Southeast Asia",
    "TH": "Southeast Asia",  "MY": "Southeast Asia",  "MM": "Southeast Asia",
    "KH": "Southeast Asia",  "LA": "Southeast Asia",  "TL": "Southeast Asia",
    "BN": "Southeast Asia",  "SG": "Southeast Asia",
    # South Asia
    "IN": "South Asia",  "BD": "South Asia",  "PK": "South Asia",
    "NP": "South Asia",  "LK": "South Asia",  "AF": "South Asia",
    # Central Asia
    "KZ": "Central Asia",  "UZ": "Central Asia",  "TM": "Central Asia",
    "KG": "Central Asia",  "TJ": "Central Asia",
    # Middle East
    "TR": "Middle East",  "IR": "Middle East",  "IQ": "Middle East",
    "SY": "Middle East",  "LB": "Middle East",  "JO": "Middle East",
    "SA": "Middle East",  "YE": "Middle East",  "OM": "Middle East",
    "AE": "Middle East",  "IL": "Middle East",  "CY": "Middle East",
    # Caucasus
    "GE": "Caucasus",  "AM": "Caucasus",  "AZ": "Caucasus",
    # Europe
    "GR": "Southern Europe",  "IT": "Southern Europe",  "ES": "Southern Europe",
    "PT": "Southern Europe",  "MK": "Southern Europe",  "AL": "Southern Europe",
    "BA": "Southern Europe",
    "DE": "Central Europe",  "PL": "Central Europe",  "HU": "Central Europe",
    "RO": "Central Europe",  "UA": "Eastern Europe",
    "GB": "Western Europe",  "FR": "Western Europe",  "NO": "Northern Europe",
    "IS": "Northern Europe",
    # Russia/Siberia
    "RU": "Siberia / Russia",
    # North America
    "US": "North America",  "CA": "North America",  "MX": "Central America",
    "GT": "Central America",  "SV": "Central America",  "HN": "Central America",
    "NI": "Central America",  "CR": "Central America",  "PA": "Central America",
    # Caribbean
    "CU": "Caribbean",  "JM": "Caribbean",  "HT": "Caribbean",
    "DO": "Caribbean",  "TT": "Caribbean",
    # South America
    "CO": "South America",  "VE": "South America",  "EC": "South America",
    "PE": "South America",  "BR": "South America",  "BO": "South America",
    "CL": "South America",  "AR": "South America",  "PY": "South America",
    "UY": "South America",  "SR": "South America",
    # Africa
    "NG": "West Africa",  "GH": "West Africa",  "CI": "West Africa",
    "SN": "West Africa",  "ML": "West Africa",  "BF": "West Africa",
    "GN": "West Africa",  "SL": "West Africa",  "LR": "West Africa",
    "MR": "West Africa",  "NE": "West Africa",  "TD": "West Africa",
    "CM": "Central Africa",  "CF": "Central Africa",  "GA": "Central Africa",
    "CG": "Central Africa",  "CD": "Central Africa",  "AO": "Central Africa",
    "ET": "East Africa",  "KE": "East Africa",  "TZ": "East Africa",
    "UG": "East Africa",  "RW": "East Africa",  "BI": "East Africa",
    "SO": "East Africa",  "DJ": "East Africa",  "ER": "East Africa",
    "SD": "East Africa",  "SS": "East Africa",
    "ZA": "Southern Africa",  "MZ": "Southern Africa",  "ZW": "Southern Africa",
    "ZM": "Southern Africa",  "MW": "Southern Africa",  "NA": "Southern Africa",
    "DZ": "North Africa",  "LY": "North Africa",  "EG": "North Africa",
    "MA": "North Africa",  "TN": "North Africa",
    "MG": "Indian Ocean",
    # Oceania
    "AU": "Oceania",  "NZ": "Oceania",  "PG": "Oceania",  "FJ": "Oceania",
    "VU": "Oceania",  "SB": "Oceania",  "TO": "Oceania",  "WS": "Oceania",
    "FM": "Oceania",  "PW": "Oceania",  "KI": "Oceania",
    "PF": "Oceania",  "NC": "Oceania",  "GU": "Oceania",
}


def _classify_location(lat: float, lon: float) -> tuple[str, str]:
    """
    Return (country, region) for the given WGS-84 coordinates.

    Primary:  reverse_geocoder k-d tree lookup (accurate, offline, fast).
    Fallback: hemisphere label when reverse_geocoder is not installed.
    """
    if _RG_AVAILABLE:
        try:
            hits = _rg.search((lat, lon), verbose=False)
            if hits:
                cc      = hits[0].get("cc", "")
                country = _CC_TO_COUNTRY.get(cc, cc or "Unknown")
                region  = _CC_TO_REGION.get(cc, _hemisphere(lat, lon))
                return country, region
        except Exception as exc:
            log.debug("[TRANSFORM] reverse_geocoder error at (%.3f, %.3f): %s", lat, lon, exc)

    # Offline fallback
    return "Unknown", _hemisphere(lat, lon)


def _hemisphere(lat: float, lon: float) -> str:
    ns = "North" if lat >= 0 else "South"
    ew = "East"  if lon >= 0 else "West"
    return f"{ns} {ew} Hemisphere"


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

    et = rec["event_time"]
    if isinstance(et, str):
        et = datetime.strptime(et, "%Y-%m-%dT%H:%M:%SZ")

    return TransformedEvent(
        event_type_name = str(rec["event_type"]),
        latitude        = lat,
        longitude       = lon,
        country         = country,
        region          = region,
        date            = et.date(),
        day             = et.day,
        month           = et.month,
        year            = et.year,
        hour            = et.hour,
        event_id        = str(rec["event_id"]),
        severity_level  = str(rec["severity_level"]),
        source          = str(rec.get("source", "")),
    )
