"""
models/schemas.py
Pydantic Response Models -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

All models used as FastAPI response_model= values, which means:
  - They control what fields are serialized in the HTTP response
  - They appear verbatim in the /docs OpenAPI schema
  - FastAPI validates that the data returned by each route matches the model

MongoDB event fields map directly to EventResponse.
PostgreSQL analytical result rows map to the analytics response models.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# ── Shared enums / constants ──────────────────────────────────────────────────

VALID_EVENT_TYPES  = {"earthquake", "fire", "flood", "storm"}
VALID_SEVERITY     = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}


# ── MongoDB event models ──────────────────────────────────────────────────────

class EventResponse(BaseModel):
    """A single processed disaster event as returned by the MongoDB store."""
    event_id:       str
    event_type:     str
    severity_level: str
    latitude:       float
    longitude:      float
    event_time:     datetime
    processed_at:   datetime
    source:         str

    model_config = {"from_attributes": True}


class RecentEventsResponse(BaseModel):
    """Response for GET /events/recent."""
    count: int      = Field(description="Number of events returned")
    data:  list[EventResponse]


class SearchEventsResponse(BaseModel):
    """Response for GET /events/search (paginated)."""
    total:     int  = Field(description="Total matching events across all pages")
    page:      int
    page_size: int
    pages:     int  = Field(description="Total number of pages")
    data:      list[EventResponse]


class BoundingBox(BaseModel):
    """Geographic bounding box used in /events/by-location."""
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float


class LocationEventsResponse(BaseModel):
    """Response for GET /events/by-location."""
    bounding_box: BoundingBox
    count:        int = Field(description="Number of events found within the box")
    data:         list[EventResponse]


# ── PostgreSQL analytics models ───────────────────────────────────────────────

class EventCountItem(BaseModel):
    """One row from GET /analytics/event-counts."""
    event_type: str
    count:      int


class EventCountsResponse(BaseModel):
    """Response for GET /analytics/event-counts."""
    total_events: int = Field(description="Sum of all event type counts")
    data:         list[EventCountItem]


class MonthlyTrendItem(BaseModel):
    """One month's row from GET /analytics/monthly-trends."""
    year:       int
    month:      int
    month_name: str
    count:      int


class MonthlyTrendsResponse(BaseModel):
    """Response for GET /analytics/monthly-trends."""
    filters: dict = Field(description="Active query filters (event_type, year)")
    data:    list[MonthlyTrendItem]


class TopLocationItem(BaseModel):
    """One region row from GET /analytics/locations/top."""
    country: Optional[str]
    region:  Optional[str]
    count:   int


class TopLocationsResponse(BaseModel):
    """Response for GET /analytics/locations/top."""
    limit:              int
    event_type_filter:  Optional[str]
    data:               list[TopLocationItem]


# ── System / health models ────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    """Connectivity status of a single backing service."""
    status: str             = Field(description="'ok' or 'error'")
    detail: Optional[str]   = Field(default=None)


class HealthResponse(BaseModel):
    """Response for GET /health."""
    status:     str           = Field(description="'ok' or 'degraded'")
    mongodb:    ServiceStatus
    postgresql: ServiceStatus
    version:    str           = "1.0.0"
