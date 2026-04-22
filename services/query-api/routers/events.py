"""
routers/events.py
MongoDB Event Endpoints -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Three read-only endpoints backed by the MongoDB operational store:

    GET /events/recent         -- latest N events
    GET /events/search         -- filtered + paginated events
    GET /events/by-location    -- geospatial bounding-box query

Read-path rationale (MongoDB vs PostgreSQL):
    MongoDB is used here because these endpoints query *individual* processed
    events, not aggregates.  MongoDB's document model, 2dsphere geospatial
    index, and low-latency point reads make it the right tool for
    operational queries with variable predicates.
"""

from __future__ import annotations

import math
from datetime import datetime
from enum import Enum
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from dependencies import get_mongo, get_postgres
from models.schemas import (
    BoundingBox,
    EventResponse,
    LocationEventsResponse,
    RecentEventsResponse,
    SearchEventsResponse,
)
from mongo_client import MongoReadClient
from postgres_client import PostgresReadClient

router = APIRouter(prefix="/events", tags=["Events  (MongoDB)"])


# ── Shared enums (reused in analytics router too) ─────────────────────────────

class EventType(str, Enum):
    earthquake = "earthquake"
    fire       = "fire"
    flood      = "flood"
    storm      = "storm"


class SeverityLevel(str, Enum):
    LOW      = "LOW"
    MEDIUM   = "MEDIUM"
    HIGH     = "HIGH"
    CRITICAL = "CRITICAL"


# ── GET /events/recent ────────────────────────────────────────────────────────

@router.get(
    "/recent",
    response_model=RecentEventsResponse,
    summary="Recent Events",
    description=(
        "Returns the most recent disaster events from the MongoDB operational store, "
        "sorted by `event_time` descending (newest first)."
    ),
)
def get_recent_events(
    limit:      int                  = Query(20,  ge=1, le=100,
                                            description="Number of events to return"),
    event_type: Optional[EventType]  = Query(None,
                                            description="Filter to a single event type"),
    mongo: MongoReadClient           = Depends(get_mongo),
):
    """
    **Example requests:**

    - `GET /events/recent` — last 20 events across all types
    - `GET /events/recent?limit=5&event_type=earthquake` — last 5 earthquakes
    """
    try:
        docs = mongo.get_recent(
            limit      = limit,
            event_type = event_type.value if event_type else None,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}")

    return RecentEventsResponse(
        count = len(docs),
        data  = [EventResponse(**d) for d in docs],
    )


# ── GET /events/search ────────────────────────────────────────────────────────

@router.get(
    "/search",
    summary="Search Events",
    description=(
        "Filter events by date range, type (multi-value), country, and lat/lng proximity. "
        "Backed by PostgreSQL so country and region fields are always present. "
        "Returns {events, total}."
    ),
)
def search_events(
    start_date:   Optional[str]         = Query(None, description="YYYY-MM-DD"),
    end_date:     Optional[str]         = Query(None, description="YYYY-MM-DD"),
    type:         Optional[List[str]]   = Query(None, description="Repeat for multiple: ?type=earthquake&type=fire"),
    country:      Optional[str]         = Query(None, description="Partial match, e.g. Indonesia"),
    region:       Optional[str]         = Query(None, description="Exact match, e.g. Americas"),
    lat:          Optional[float]       = Query(None, description="Proximity centre latitude"),
    lng:          Optional[float]       = Query(None, description="Proximity centre longitude"),
    proximity_km: float                 = Query(500.0, description="Radius in km when lat/lng provided"),
    limit:        int                   = Query(20, ge=1, le=10000),
    offset:       int                   = Query(0,  ge=0, description="Skip first N results for pagination"),
    pg: PostgresReadClient              = Depends(get_postgres),
):
    try:
        return pg.search_events(
            start_date   = start_date,
            end_date     = end_date,
            event_types  = type,
            country      = country,
            region       = region,
            lat          = lat,
            lng          = lng,
            proximity_km = proximity_km,
            limit        = limit,
            offset       = offset,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}")


# ── GET /events/by-location ───────────────────────────────────────────────────

@router.get(
    "/by-location",
    response_model=LocationEventsResponse,
    summary="Events by Geographic Bounding Box",
    description=(
        "Return events whose coordinates fall inside a rectangular bounding box. "
        "Uses the 2dsphere geospatial index created by Module 4."
    ),
)
def events_by_location(
    min_lat: float = Query(..., ge=-90,  le=90,  description="South boundary (latitude)"),
    max_lat: float = Query(..., ge=-90,  le=90,  description="North boundary (latitude)"),
    min_lon: float = Query(..., ge=-180, le=180, description="West boundary (longitude)"),
    max_lon: float = Query(..., ge=-180, le=180, description="East boundary (longitude)"),
    limit:   int   = Query(100, ge=1, le=500,    description="Maximum results to return"),
    mongo: MongoReadClient = Depends(get_mongo),
):
    """
    **Example requests:**

    - Japan bounding box: `?min_lat=30&max_lat=46&min_lon=129&max_lon=146`
    - Chile corridor:     `?min_lat=-46&max_lat=-18&min_lon=-80&max_lon=-62`
    - Bay of Bengal:      `?min_lat=6&max_lat=23&min_lon=80&max_lon=93`
    """
    if min_lat >= max_lat:
        raise HTTPException(
            status_code=422,
            detail="min_lat must be strictly less than max_lat",
        )
    if min_lon >= max_lon:
        raise HTTPException(
            status_code=422,
            detail="min_lon must be strictly less than max_lon",
        )

    try:
        docs = mongo.by_location(min_lat, max_lat, min_lon, max_lon, limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}")

    return LocationEventsResponse(
        bounding_box = BoundingBox(
            min_lat=min_lat, max_lat=max_lat,
            min_lon=min_lon, max_lon=max_lon,
        ),
        count = len(docs),
        data  = [EventResponse(**d) for d in docs],
    )
