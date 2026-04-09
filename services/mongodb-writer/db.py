"""
db.py
MongoDB Client — Module 4
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Responsibilities:
  - Connect to MongoDB (standalone, replica set, or sharded cluster — URI only)
  - Create indexes on first run
  - Convert ProcessedEvent dicts to MongoDB documents
  - Perform idempotent upserts (single + bulk)

Does NOT:
  - Manage sharding or replica sets (infrastructure concern)
  - Transform event data (done by processor.py in Module 3)
  - Expose any query API (read path comes in Module 5)

Document schema stored in MongoDB:
  {
    "_id":            "<event_id UUID>",   ← natural deduplication key
    "event_type":     "earthquake",
    "severity_level": "HIGH",
    "location": {                          ← GeoJSON Point (2dsphere indexed)
      "type": "Point",
      "coordinates": [longitude, latitude] ← GeoJSON order: lon first, then lat
    },
    "event_time":   ISODate(...),          ← original event timestamp
    "processed_at": ISODate(...),          ← when Module 3 processed it
    "source":       "GeoNet"
  }

Indexes created automatically on connect():
  1. { location: "2dsphere" }                       — geospatial queries
  2. { event_time: -1 }                             — time-range queries
  3. { event_type: 1, severity_level: 1, event_time: -1 } — compound filter
  4. { source: 1 }                                  — source-based filtering
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from config import MongoConfig

log = logging.getLogger(__name__)


class MongoDBClient:
    """
    Thin, stateful wrapper around PyMongo.

    Lifecycle:
        client = MongoDBClient(config)
        client.connect()           # raises on connection failure
        client.upsert_many(docs)   # idempotent batch write
        client.close()

    Upsert semantics:
        Every write uses UpdateOne(filter={"_id": event_id}, update={"$set": doc}, upsert=True).
        Re-sending the same event_id is a no-op — the document is updated in place.
        This makes the service safe to restart mid-stream without creating duplicates.

    Batch writes:
        upsert_many() uses bulk_write(ordered=False) — failures in one document
        do not block the rest of the batch. Each failed upsert is logged and counted.
    """

    def __init__(self, config: MongoConfig) -> None:
        self.cfg    = config
        self._client = None   # pymongo.MongoClient; set by connect()
        self._db     = None   # pymongo.Database
        self._col    = None   # pymongo.Collection
        self._log    = logging.getLogger(self.__class__.__name__)

    # ── Public API ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Open a MongoDB connection and verify it with a ping.
        Raises ConnectionError if the server is unreachable.
        """
        try:
            from pymongo import MongoClient
            from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
        except ImportError:
            raise ImportError(
                "pymongo is not installed. Run: pip install pymongo"
            )

        self._log.info("Connecting to MongoDB at %s ...", self.cfg.uri)
        try:
            self._client = MongoClient(
                self.cfg.uri,
                serverSelectionTimeoutMS=5_000,    # fail fast on bad URI
                w=self.cfg.write_concern,
            )
            # Force the connection (lazy by default)
            self._client.admin.command("ping")
            self._db  = self._client[self.cfg.database]
            self._col = self._db[self.cfg.collection]
            self._log.info(
                "Connected: db=%s  collection=%s",
                self.cfg.database, self.cfg.collection,
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as exc:
            raise ConnectionError(
                f"Cannot reach MongoDB at {self.cfg.uri}.\n"
                f"  1. Is mongod running?  See README.md -> 'Starting MongoDB Locally'\n"
                f"  2. Check the URI with --mongo-uri flag.\n"
                f"  3. Use --dry-run to test document format without MongoDB.\n"
                f"  Original error: {exc}"
            ) from exc

    def create_indexes(self) -> None:
        """
        Create all recommended indexes on the disaster_events collection.

        Safe to call repeatedly — MongoDB is idempotent on existing indexes.
        Should be called once on service startup before any writes.

        Index design rationale:
          2dsphere  : required for all $geoNear / $geoWithin / $near queries
          event_time: most dashboard queries filter by time window
          compound  : covers "all HIGH earthquakes in last 7 days" type queries
          source    : supports data provenance / audit queries
        """
        from pymongo import ASCENDING, DESCENDING, GEOSPHERE

        self._log.info("Creating indexes on %s.%s ...",
                       self.cfg.database, self.cfg.collection)

        indexes = [
            # 1 — Geospatial (required for $geoNear, $near, $geoWithin)
            ([("location", GEOSPHERE)],
             "idx_location_2dsphere"),

            # 2 — Time-descending (most recent events first)
            ([("event_time", DESCENDING)],
             "idx_event_time_desc"),

            # 3 — Type + Severity + Time compound (most common Module 5 query)
            ([("event_type", ASCENDING),
              ("severity_level", ASCENDING),
              ("event_time", DESCENDING)],
             "idx_type_severity_time"),

            # 4 — Source filtering (provenance / audit)
            ([("source", ASCENDING)],
             "idx_source"),
        ]

        created = []
        for keys, name in indexes:
            self._col.create_index(keys, name=name, background=True)
            created.append(name)

        self._log.info("Indexes ready: %s", created)

    def upsert_event(self, event: dict[str, Any]) -> str:
        """
        Write a single processed event to MongoDB.

        Args:
            event: ProcessedEvent.to_dict() output from Module 3.

        Returns:
            The MongoDB _id of the upserted document (= event_id).
        """
        from pymongo import UpdateOne

        doc    = self._to_document(event)
        result = self._col.update_one(
            filter={"_id": doc["_id"]},
            update={"$set": doc},
            upsert=True,
        )
        action = "inserted" if result.upserted_id else "updated"
        self._log.debug("%-8s  _id=%s", action, doc["_id"][:8])
        return doc["_id"]

    def upsert_many(self, events: list[dict[str, Any]]) -> dict[str, int]:
        """
        Bulk upsert a batch of processed events.

        Uses ordered=False so failures in individual documents do not block
        the rest of the batch. All failures are logged and counted.

        Args:
            events: List of ProcessedEvent.to_dict() dicts.

        Returns:
            {"inserted": n, "updated": n, "errors": n}
        """
        if not events:
            return {"inserted": 0, "updated": 0, "errors": 0}

        from pymongo import UpdateOne
        from pymongo.errors import BulkWriteError

        ops  = []
        docs = []
        for event in events:
            doc = self._to_document(event)
            docs.append(doc)
            ops.append(
                UpdateOne(
                    filter={"_id": doc["_id"]},
                    update={"$set": doc},
                    upsert=True,
                )
            )

        try:
            result = self._col.bulk_write(ops, ordered=False)
            summary = {
                "inserted": result.upserted_count,
                "updated":  result.modified_count,
                "errors":   0,
            }
        except BulkWriteError as exc:
            errors  = len(exc.details.get("writeErrors", []))
            result  = exc.details.get("nUpserted", 0)
            summary = {
                "inserted": result,
                "updated":  exc.details.get("nModified", 0),
                "errors":   errors,
            }
            self._log.error(
                "Bulk write partial failure: %d errors out of %d ops",
                errors, len(ops),
            )

        self._log.debug(
            "Batch of %d: inserted=%d  updated=%d  errors=%d",
            len(events), summary["inserted"], summary["updated"], summary["errors"],
        )
        return summary

    def collection_stats(self) -> dict[str, Any]:
        """Return a summary of what is currently in the collection."""
        pipeline = [
            {"$group": {
                "_id":   "$event_type",
                "count": {"$sum": 1},
                "levels": {"$addToSet": "$severity_level"},
            }},
            {"$sort": {"_id": 1}},
        ]
        rows    = list(self._col.aggregate(pipeline))
        total   = self._col.count_documents({})
        return {"total": total, "by_type": rows}

    def close(self) -> None:
        """Close the MongoDB connection."""
        if self._client is not None:
            self._client.close()
            self._log.info("MongoDB connection closed.")

    # ── Private helpers ───────────────────────────────────────────────────────

    @staticmethod
    def _to_document(event: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a ProcessedEvent dict (from Module 3) into a MongoDB document.

        Transformations applied:
          event_id  → _id           (MongoDB primary key = natural dedup)
          lat + lon → location      (GeoJSON Point for 2dsphere index)
          str dates → datetime      (BSON Date for proper time queries)

        GeoJSON coordinate order:
          MongoDB requires [longitude, latitude] — the opposite of
          the (lat, lon) convention used in many APIs. This is a
          GeoJSON standard (RFC 7946), not a MongoDB quirk.

        Args:
            event: dict with keys matching ProcessedEvent.to_dict() output.

        Returns:
            MongoDB-ready document dict.
        """
        doc = dict(event)   # shallow copy — don't mutate the caller's dict

        # event_id → _id (primary key)
        doc["_id"] = doc.pop("event_id")

        # lat + lon → GeoJSON Point
        lat = doc.pop("latitude")
        lon = doc.pop("longitude")
        doc["location"] = {
            "type":        "Point",
            "coordinates": [lon, lat],   # GeoJSON: [longitude, latitude]
        }

        # ISO-8601 strings → timezone-aware datetime (BSON Date)
        for field in ("event_time", "processed_at"):
            raw_ts = doc.get(field)
            if isinstance(raw_ts, str):
                doc[field] = datetime.strptime(
                    raw_ts, "%Y-%m-%dT%H:%M:%SZ"
                ).replace(tzinfo=timezone.utc)

        return doc
