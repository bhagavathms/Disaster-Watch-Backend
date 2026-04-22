"""
mongo_client.py
MongoDB Read-Only Client -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Provides three query operations against disaster_db.disaster_events:
    get_recent()     -- latest N events, optional type filter
    search()         -- filtered + paginated events
    by_location()    -- geospatial bounding-box query (uses 2dsphere index)

PyMongo's MongoClient is internally thread-safe and manages its own
connection pool, so a single instance is safe to share across FastAPI's
thread pool.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from config import MongoSettings

log = logging.getLogger(__name__)


class MongoReadClient:
    """
    Read-only client for disaster_db.disaster_events.

    MongoDB document format (written by Module 4):
        _id            : str  UUID (= event_id)
        event_type     : str
        severity_level : str
        location       : {"type": "Point", "coordinates": [lon, lat]}
        event_time     : datetime (UTC-aware)
        processed_at   : datetime (UTC-aware)
        source         : str

    Every public method normalises raw MongoDB documents before returning:
        _id                     -> event_id (str)
        location.coordinates    -> longitude / latitude (float)
    """

    _LIVE_COLLECTION = "live_events"

    def __init__(self, settings: MongoSettings) -> None:
        self._settings  = settings
        self._client    = None
        self._col       = None
        self._live_col  = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open connection and verify with ping. Raises ConnectionError on failure."""
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError("pymongo not installed.  Run: pip install pymongo>=4.0.0")

        self._client = MongoClient(
            self._settings.uri, serverSelectionTimeoutMS=5_000
        )
        try:
            self._client.admin.command("ping")
        except Exception as exc:
            self._client = None
            raise ConnectionError(
                f"Cannot reach MongoDB at {self._settings.uri}. "
                f"Is mongod running?  Error: {exc}"
            ) from exc

        self._col      = self._client[self._settings.database][self._settings.collection]
        self._live_col = self._client[self._settings.database][self._LIVE_COLLECTION]
        log.info(
            "[MONGO] Connected  %s / %s.%s",
            self._settings.uri, self._settings.database, self._settings.collection,
        )

    def close(self) -> None:
        if self._client:
            self._client.close()
            log.info("[MONGO] Connection closed")

    def is_connected(self) -> bool:
        """Lightweight connectivity check (used by /health endpoint)."""
        if self._client is None:
            return False
        try:
            self._client.admin.command("ping")
            return True
        except Exception:
            return False

    # ── Query operations ───────────────────────────────────────────────────────

    def get_recent(
        self,
        limit:      int,
        event_type: Optional[str] = None,
    ) -> list[dict]:
        """
        Return the most recent `limit` events, sorted by event_time descending.
        Optionally filter to a single event_type.
        """
        from pymongo import DESCENDING

        query: dict = {}
        if event_type:
            query["event_type"] = event_type

        docs = (
            self._col
            .find(query)
            .sort("event_time", DESCENDING)
            .limit(limit)
        )
        return [self._normalise(d) for d in docs]

    def search(
        self,
        event_type:     Optional[str]      = None,
        severity_level: Optional[str]      = None,
        date_from:      Optional[datetime] = None,
        date_to:        Optional[datetime] = None,
        page:           int                = 1,
        page_size:      int                = 20,
    ) -> tuple[list[dict], int]:
        """
        Filtered and paginated event search.

        Returns (records, total_count) where total_count reflects the full
        filtered result set (not just the current page) so callers can
        compute total pages.
        """
        from pymongo import DESCENDING

        query: dict = {}

        if event_type:
            query["event_type"] = event_type

        if severity_level:
            query["severity_level"] = severity_level

        if date_from or date_to:
            # Ensure timezone-aware for correct BSON Date comparison
            ts_filter: dict = {}
            if date_from:
                df = date_from.replace(tzinfo=timezone.utc) if date_from.tzinfo is None else date_from
                ts_filter["$gte"] = df
            if date_to:
                dt = date_to.replace(tzinfo=timezone.utc) if date_to.tzinfo is None else date_to
                ts_filter["$lte"] = dt
            query["event_time"] = ts_filter

        total = self._col.count_documents(query)
        skip  = (page - 1) * page_size
        docs  = (
            self._col
            .find(query)
            .sort("event_time", DESCENDING)
            .skip(skip)
            .limit(page_size)
        )
        return [self._normalise(d) for d in docs], total

    def by_location(
        self,
        min_lat: float,
        max_lat: float,
        min_lon: float,
        max_lon: float,
        limit:   int = 100,
    ) -> list[dict]:
        """
        Return events whose location falls within the given bounding box.

        Uses a $geoWithin + GeoJSON Polygon query against the 2dsphere
        index created by Module 4 on the 'location' field.

        Coordinates are in WGS-84.  GeoJSON convention: [longitude, latitude].
        The polygon ring must be closed (first == last vertex).
        """
        query = {
            "location": {
                "$geoWithin": {
                    "$geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [min_lon, min_lat],
                            [max_lon, min_lat],
                            [max_lon, max_lat],
                            [min_lon, max_lat],
                            [min_lon, min_lat],   # close the ring
                        ]],
                    }
                }
            }
        }
        docs = self._col.find(query).limit(limit)
        return [self._normalise(d) for d in docs]

    def get_new_since(self, since: datetime) -> list[dict]:
        """
        Return events inserted into MongoDB after `since` (by processed_at).
        Used by the SSE streaming endpoint to poll for new events.
        """
        from pymongo import ASCENDING

        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        docs = (
            self._col
            .find({"processed_at": {"$gt": since}})
            .sort("processed_at", ASCENDING)
        )
        return [self._normalise(d) for d in docs]

    # ── Live events ───────────────────────────────────────────────────────────

    def get_live_snapshot(self) -> list[dict]:
        """
        Return all documents currently in the live_events collection.
        TTL ensures only the last 8 hours of generated events are present,
        so this is always a small, frontend-safe payload (≤ ~960 events).
        """
        from pymongo import DESCENDING
        docs = self._live_col.find({}).sort("event_time", DESCENDING)
        return [self._normalise_live(d) for d in docs]

    def get_live_since(self, since: datetime) -> list[dict]:
        """
        Return live events inserted after `since` (by processed_at).
        Used by the SSE stream to deliver only new events each poll cycle.
        """
        from pymongo import ASCENDING
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        docs = (
            self._live_col
            .find({"processed_at": {"$gt": since}})
            .sort("processed_at", ASCENDING)
        )
        return [self._normalise_live(d) for d in docs]

    def ensure_live_indexes(self) -> None:
        """
        Create TTL + polling indexes on live_events.
        Safe to call repeatedly (create_index is idempotent for same keyPattern).
        The TTL index is also created by live_generator.py, but calling it here
        guarantees the collection is ready even before the generator is started.
        """
        from pymongo import ASCENDING
        self._live_col.create_index(
            [("event_time", ASCENDING)],
            expireAfterSeconds=8 * 3600,
            name="ttl_event_time",
            background=True,
        )
        self._live_col.create_index(
            [("processed_at", ASCENDING)],
            name="idx_processed_at",
            background=True,
        )
        log.info("[MONGO] Live event indexes verified (live_events)")

    # ── Internal normalisation ─────────────────────────────────────────────────

    @staticmethod
    def _normalise_live(doc: dict) -> dict:
        """
        Normalise a live_events document.  The generator stores lat/lon both
        as top-level fields AND inside the GeoJSON `location` sub-document,
        so we just strip internal Mongo fields and ensure all types are clean.
        """
        out = dict(doc)
        out["event_id"] = str(out.pop("_id"))
        out.pop("location", None)   # GeoJSON blob not needed by frontend
        out.pop("is_live", None)    # internal flag
        out["latitude"]  = float(out.get("latitude",  0.0))
        out["longitude"] = float(out.get("longitude", 0.0))
        # Serialise datetimes so JSON encoder doesn't choke
        for key in ("event_time", "processed_at"):
            val = out.get(key)
            if isinstance(val, datetime):
                out[key] = val.strftime("%Y-%m-%dT%H:%M:%SZ")
        return out

    @staticmethod
    def _normalise(doc: dict) -> dict:
        """
        Convert a raw MongoDB document to a flat API-ready dict.

        Transformations:
            _id                      -> event_id (str)
            location.coordinates[0]  -> longitude (float)
            location.coordinates[1]  -> latitude  (float)
        All other fields are passed through unchanged.
        """
        out             = dict(doc)
        out["event_id"] = str(out.pop("_id"))
        coords          = out.pop("location", {}).get("coordinates", [0.0, 0.0])
        out["longitude"] = float(coords[0])
        out["latitude"]  = float(coords[1])
        return out
