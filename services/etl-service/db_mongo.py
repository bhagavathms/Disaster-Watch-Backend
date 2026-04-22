"""
db_mongo.py
MongoDB Extraction Client -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Connects to disaster_db.disaster_events (written by Module 4) and
returns flat Python dicts ready for the transform step.

No writes are performed here; this client is read-only.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger(__name__)

_DEFAULT_URI        = "mongodb://localhost:28020/"  # mongos router
_DEFAULT_DATABASE   = "disaster_db"
_DEFAULT_COLLECTION = "disaster_events"


class MongoExtractClient:
    """
    Read-only MongoDB client used by the ETL extraction layer.

    MongoDB document format (written by Module 4):
        _id            : str  (= event_id UUID)
        event_type     : str
        severity_level : str
        location       : {"type": "Point", "coordinates": [lon, lat]}
        event_time     : datetime (UTC, timezone-aware)
        processed_at   : datetime (UTC, timezone-aware)
        source         : str

    After normalisation each record becomes:
        event_id, event_type, severity_level,
        latitude, longitude, event_time, processed_at, source
    """

    def __init__(
        self,
        uri:        str = _DEFAULT_URI,
        database:   str = _DEFAULT_DATABASE,
        collection: str = _DEFAULT_COLLECTION,
    ) -> None:
        self.uri        = uri
        self.database   = database
        self.collection = collection
        self._client    = None
        self._col       = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open connection, verify with ping, log document count."""
        try:
            from pymongo import MongoClient
        except ImportError:
            raise ImportError(
                "pymongo is not installed.  Run: pip install pymongo>=4.0.0"
            )

        self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5_000)
        try:
            self._client.admin.command("ping")
        except Exception as exc:
            raise ConnectionError(
                f"Cannot reach MongoDB at {self.uri}. "
                f"Is mongod running?  Original error: {exc}"
            ) from exc

        self._col   = self._client[self.database][self.collection]
        total       = self._col.count_documents({})
        log.info(
            "[MONGO] Connected  uri=%s  db=%s  collection=%s  total_docs=%d",
            self.uri, self.database, self.collection, total,
        )

    def close(self) -> None:
        if self._client:
            self._client.close()
            log.info("[MONGO] Connection closed")

    # ── Extraction ─────────────────────────────────────────────────────────────

    def fetch_all(self) -> list[dict]:
        """Return every document as a normalised flat dict."""
        return self._fetch({})

    def fetch_since(self, since: datetime) -> list[dict]:
        """
        Return documents whose event_time >= since.
        Useful for incremental ETL runs (--since flag).
        """
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return self._fetch({"event_time": {"$gte": since}})

    def fetch_loaded_ids(self) -> set[str]:
        """
        Return the set of event_ids already in the collection.
        Used as a secondary deduplication check against PostgreSQL.
        """
        return {str(doc["_id"]) for doc in self._col.find({}, {"_id": 1})}

    # ── Internal ───────────────────────────────────────────────────────────────

    def _fetch(self, query: dict) -> list[dict]:
        raw     = list(self._col.find(query))
        records = [self._normalise(doc) for doc in raw]
        log.info("[MONGO] Fetched %d documents (query=%s)", len(records), query or "all")
        return records

    @staticmethod
    def _normalise(doc: dict) -> dict:
        """
        Convert a MongoDB document into a flat extraction dict.

        Key transformations:
            _id                          → event_id (str)
            location.coordinates[0]      → longitude (float)
            location.coordinates[1]      → latitude  (float)
            event_time / processed_at    → kept as datetime objects
        """
        out             = dict(doc)
        out["event_id"] = str(out.pop("_id"))
        coords          = out.pop("location", {}).get("coordinates", [0.0, 0.0])
        out["longitude"] = float(coords[0])
        out["latitude"]  = float(coords[1])
        return out
