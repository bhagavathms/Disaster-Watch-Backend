"""
extract.py
Extraction Layer -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Responsibilities:
  - Pull processed disaster events from MongoDB
  - Optionally filter by a "since" date (incremental mode)
  - Optionally exclude event_ids already loaded into PostgreSQL (deduplication)
  - Return a flat list of dicts ready for the transform step

This layer owns NO business logic -- it only decides WHICH records to hand
off to the transform layer.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from db_mongo import MongoExtractClient

log = logging.getLogger(__name__)


def extract(
    client:      MongoExtractClient,
    since:       Optional[datetime] = None,
    exclude_ids: Optional[set[str]] = None,
) -> list[dict]:
    """
    Extract processed events from MongoDB and return them as flat dicts.

    Args:
        client:
            A connected MongoExtractClient.
        since:
            If provided, only events with event_time >= since are fetched
            (incremental / delta mode).  Pass None for a full load.
        exclude_ids:
            Set of event_ids that are already present in PostgreSQL.
            Matched records are filtered out before returning, so the
            transform and load layers never see already-loaded data.

    Returns:
        List of flat extraction dicts with keys:
            event_id, event_type, severity_level,
            latitude, longitude, event_time, processed_at, source

    Example:
        {
            "event_id":       "047b5a02-f449-4b22-bcb4-683282dced0b",
            "event_type":     "earthquake",
            "severity_level": "CRITICAL",
            "latitude":       -30.727968,
            "longitude":      -65.885635,
            "event_time":     datetime(2024, 10, 12, 0, 6, 47, tzinfo=utc),
            "processed_at":   datetime(2026, 2, 24, 10, 50, 41, tzinfo=utc),
            "source":         "USGS",
        }
    """
    # ── Fetch from MongoDB ─────────────────────────────────────────────────────
    if since is not None:
        log.info("[EXTRACT] Incremental mode  since=%s", since.date())
        raw = client.fetch_since(since)
    else:
        log.info("[EXTRACT] Full load mode")
        raw = client.fetch_all()

    total_fetched = len(raw)

    # ── Apply deduplication filter ─────────────────────────────────────────────
    if exclude_ids:
        before = len(raw)
        raw    = [r for r in raw if r["event_id"] not in exclude_ids]
        skipped = before - len(raw)
        if skipped:
            log.info(
                "[EXTRACT] Dedup: skipped %d event(s) already in PostgreSQL",
                skipped,
            )

    log.info(
        "[EXTRACT] Done  fetched=%d  after_dedup=%d  ready_for_transform=%d",
        total_fetched,
        total_fetched - (total_fetched - len(raw)),
        len(raw),
    )
    return raw
