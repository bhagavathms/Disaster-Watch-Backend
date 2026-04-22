"""
routers/map.py
Map Endpoint -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

GET /events/map

Returns either grid-aggregated clusters or raw individual events depending
on the Leaflet zoom level supplied by the frontend.

  zoom <= 9  -> "clustered" mode: GROUP BY spatial grid cell + event_type
  zoom >= 10 -> "events" mode:    up to 1000 raw events in the viewport bbox
"""

from __future__ import annotations

from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from dependencies import get_postgres
from postgres_client import PostgresReadClient

router = APIRouter(tags=["Map  (PostgreSQL)"])

_MAX_CLUSTERED_ZOOM = 9
_DEFAULT_START_DATE = "1980-01-01"   # matches full historical data range


# ── GET /events/map ───────────────────────────────────────────────────────────

@router.get(
    "/events/map",
    summary="Map events (clustered or raw)",
    description=(
        "Returns aggregated grid clusters at low zoom levels (<= 9) "
        "and individual events at high zoom levels (>= 10). "
        "Always bounded."
    ),
)
def get_map_events(
    min_lat:    float           = Query(..., ge=-90,  le=90, description="South boundary"),
    max_lat:    float           = Query(..., ge=-90,  le=90, description="North boundary"),
    min_lon:    float           = Query(..., ge=-180, le=180, description="West boundary"),
    max_lon:    float           = Query(..., ge=-180, le=180, description="East boundary"),
    zoom:       int             = Query(..., ge=2,    le=20, description="Leaflet zoom level"),
    start_date: Optional[str]   = Query(None, description="YYYY-MM-DD (default: 1980-01-01, full history)"),
    end_date:   Optional[str]   = Query(None, description="YYYY-MM-DD (default: today)"),
    pg: PostgresReadClient      = Depends(get_postgres),
):
    if min_lat >= max_lat:
        raise HTTPException(422, "min_lat must be less than max_lat")
    if min_lon >= max_lon:
        raise HTTPException(422, "min_lon must be less than max_lon")

    today = date.today()
    sd = _parse_date(start_date, date.fromisoformat(_DEFAULT_START_DATE))
    ed = _parse_date(end_date,   today)
    if sd > ed:
        raise HTTPException(422, "start_date must be on or before end_date")

    try:
        if zoom <= _MAX_CLUSTERED_ZOOM:
            result = pg.get_map_clustered(
                min_lat=min_lat, max_lat=max_lat,
                min_lon=min_lon, max_lon=max_lon,
                zoom=zoom,
                start_date=str(sd),
                end_date=str(ed),
            )
            return {
                "mode":        "clustered",
                "total_count": result["total_count"],
                "data":        result["clusters"],
            }
        else:
            result = pg.get_map_events(
                min_lat=min_lat, max_lat=max_lat,
                min_lon=min_lon, max_lon=max_lon,
                start_date=str(sd),
                end_date=str(ed),
            )
            return {
                "mode":        "events",
                "total_count": result["total_count"],
                "data":        result["events"],
            }
    except Exception as exc:
        raise HTTPException(503, f"PostgreSQL unavailable: {exc}")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_date(value: Optional[str], default: date) -> date:
    if value is None:
        return default
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise HTTPException(422, f"Invalid date format '{value}'. Use YYYY-MM-DD.")
