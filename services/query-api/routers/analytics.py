"""
routers/analytics.py
PostgreSQL Analytics Endpoints -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Three read-only endpoints backed by the PostgreSQL data warehouse:

    GET /analytics/event-counts      -- total events per disaster type
    GET /analytics/monthly-trends    -- monthly event volume (optional filters)
    GET /analytics/locations/top     -- highest-activity regions

Read-path rationale (PostgreSQL vs MongoDB):
    PostgreSQL is used here because these endpoints run *aggregations*
    across the entire dataset.  The star schema (fact_disaster_events +
    dim_event_type + dim_location + dim_time) is purpose-built for
    GROUP BY / COUNT queries that would be expensive in MongoDB.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from dependencies import get_postgres
from models.schemas import (
    EventCountsResponse,
    EventCountItem,
    MonthlyTrendsResponse,
    MonthlyTrendItem,
    TopLocationsResponse,
    TopLocationItem,
)
from postgres_client import PostgresReadClient
from routers.events import EventType   # reuse shared enum

router = APIRouter(prefix="/analytics", tags=["Analytics  (PostgreSQL)"])


# ── GET /analytics/event-counts ───────────────────────────────────────────────

@router.get(
    "/event-counts",
    response_model=EventCountsResponse,
    summary="Event Counts by Type",
    description=(
        "Returns the total number of disaster events for each event type, "
        "aggregated from the PostgreSQL star schema.  Results are ordered "
        "by count descending."
    ),
)
def get_event_counts(
    postgres: PostgresReadClient = Depends(get_postgres),
):
    """
    **Example request:**

    - `GET /analytics/event-counts`

    Queries `fact_disaster_events JOIN dim_event_type GROUP BY event_type_name`.
    """
    try:
        rows = postgres.get_event_counts()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}")

    items = [EventCountItem(event_type=r["event_type"], count=r["count"]) for r in rows]
    return EventCountsResponse(
        total_events=sum(i.count for i in items),
        data=items,
    )


# ── GET /analytics/monthly-trends ─────────────────────────────────────────────

@router.get(
    "/monthly-trends",
    response_model=MonthlyTrendsResponse,
    summary="Monthly Event Trends",
    description=(
        "Returns event counts grouped by year and month, ordered oldest-first. "
        "Optionally filter to a single event type or calendar year.  "
        "Joins fact_disaster_events with dim_time and (when filtered) dim_event_type."
    ),
)
def get_monthly_trends(
    event_type: Optional[EventType] = Query(
        None, description="Limit to one disaster type"
    ),
    year: Optional[int] = Query(
        None, ge=1980, le=2100, description="Limit to a single calendar year"
    ),
    start_date: Optional[str] = Query(
        None, description="Start of date range (YYYY-MM-DD)"
    ),
    end_date: Optional[str] = Query(
        None, description="End of date range (YYYY-MM-DD)"
    ),
    country: Optional[str] = Query(
        None, description="Filter by country (partial match, e.g. Indonesia)"
    ),
    postgres: PostgresReadClient = Depends(get_postgres),
):
    """
    **Example requests:**

    - `GET /analytics/monthly-trends` — all types, all years
    - `GET /analytics/monthly-trends?event_type=earthquake&year=2024`
    - `GET /analytics/monthly-trends?start_date=2010-01-01&end_date=2020-12-31`
    - `GET /analytics/monthly-trends?country=Japan&event_type=earthquake`
    """
    try:
        rows = postgres.get_monthly_trends(
            event_type=event_type.value if event_type else None,
            year=year,
            start_date=start_date,
            end_date=end_date,
            country=country,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}")

    _MONTH_NAMES = {
        1: "January", 2: "February", 3: "March",    4: "April",
        5: "May",     6: "June",     7: "July",      8: "August",
        9: "September", 10: "October", 11: "November", 12: "December",
    }

    items = [
        MonthlyTrendItem(
            year=r["year"],
            month=r["month"],
            month_name=_MONTH_NAMES.get(r["month"], str(r["month"])),
            count=r["count"],
        )
        for r in rows
    ]

    active_filters: dict = {}
    if event_type:
        active_filters["event_type"] = event_type.value
    if year is not None:
        active_filters["year"] = year
    if start_date:
        active_filters["start_date"] = start_date
    if end_date:
        active_filters["end_date"] = end_date
    if country:
        active_filters["country"] = country

    return MonthlyTrendsResponse(filters=active_filters, data=items)


# ── GET /analytics/locations/top ──────────────────────────────────────────────

@router.get(
    "/locations/top",
    response_model=TopLocationsResponse,
    summary="Top Locations by Event Count",
    description=(
        "Returns the regions with the highest number of disaster events. "
        "Groups by country + region from dim_location.  "
        "Optionally filter to a single event type."
    ),
)
def get_top_locations(
    limit: int = Query(
        10, ge=1, le=100,
        description="Number of top regions to return",
    ),
    event_type: Optional[EventType] = Query(
        None, description="Limit to one disaster type"
    ),
    postgres: PostgresReadClient = Depends(get_postgres),
):
    """
    **Example requests:**

    - `GET /analytics/locations/top` — top 10 regions across all types
    - `GET /analytics/locations/top?limit=5&event_type=flood`
    - `GET /analytics/locations/top?limit=20`
    """
    try:
        rows = postgres.get_top_locations(
            limit=limit,
            event_type=event_type.value if event_type else None,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"PostgreSQL unavailable: {exc}")

    items = [
        TopLocationItem(
            country=r["country"],
            region=r["region"],
            count=r["count"],
        )
        for r in rows
    ]

    return TopLocationsResponse(
        limit=limit,
        event_type_filter=event_type.value if event_type else None,
        data=items,
    )
