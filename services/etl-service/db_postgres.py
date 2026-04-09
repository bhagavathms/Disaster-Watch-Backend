"""
db_postgres.py
PostgreSQL Connection + Star-Schema DDL -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Defines the star schema (4 tables) and a thin connection wrapper.
All CREATE TABLE statements use IF NOT EXISTS so they are safe to
re-run on every ETL invocation.

Star Schema:
    dim_event_type        -- normalised disaster type lookup
    dim_location          -- geographic attributes (lat, lon, country, region)
    dim_time              -- date/time decomposition
    fact_disaster_events  -- one row per processed event (central fact table)
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# ── Data-Definition SQL ───────────────────────────────────────────────────────

_DDL = """
-- ============================================================
-- DIMENSION: disaster event type
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_event_type (
    event_type_id   SERIAL      PRIMARY KEY,
    event_type_name VARCHAR(50) UNIQUE NOT NULL
);

-- ============================================================
-- DIMENSION: geographic location
-- One row per unique (latitude, longitude) pair.
-- country / region are approximate labels derived from
-- coordinate bounding-box classification.
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL           PRIMARY KEY,
    country     VARCHAR(100),
    region      VARCHAR(100),
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    UNIQUE (latitude, longitude)
);

-- ============================================================
-- DIMENSION: time (date + hour granularity)
-- Two events on the same calendar date at the same hour
-- share a single time_id -- good for hourly aggregations.
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_time (
    time_id SERIAL   PRIMARY KEY,
    date    DATE     NOT NULL,
    day     SMALLINT NOT NULL,
    month   SMALLINT NOT NULL,
    year    SMALLINT NOT NULL,
    hour    SMALLINT NOT NULL,
    UNIQUE (date, hour)
);

-- ============================================================
-- FACT TABLE: one row per processed disaster event
-- event_id carries the UNIQUE constraint for deduplication:
-- re-running the ETL against the same MongoDB data is safe.
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_disaster_events (
    fact_id        SERIAL      PRIMARY KEY,
    event_id       VARCHAR(36) UNIQUE NOT NULL,
    event_type_id  INTEGER     NOT NULL REFERENCES dim_event_type(event_type_id),
    location_id    INTEGER     NOT NULL REFERENCES dim_location(location_id),
    time_id        INTEGER     NOT NULL REFERENCES dim_time(time_id),
    severity_level VARCHAR(10) NOT NULL,
    source         VARCHAR(50),
    count_metric   SMALLINT    NOT NULL DEFAULT 1
);
"""


# ── PostgresClient ────────────────────────────────────────────────────────────

class PostgresClient:
    """
    Thin psycopg2 wrapper that manages the connection lifecycle
    and exposes helpers for DML/DDL execution.

    autocommit is disabled; callers must explicitly commit() or rollback().
    """

    def __init__(
        self,
        dsn: str = "postgresql://postgres:postgres@localhost:5432/disaster_dw",
    ) -> None:
        self.dsn   = dsn
        self._conn = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """Open connection; raises ConnectionError with actionable message on failure."""
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is not installed.  Run: pip install psycopg2-binary"
            )

        try:
            self._conn              = psycopg2.connect(self.dsn)
            self._conn.autocommit   = False
            log.info("[POSTGRES] Connected  dsn=%s", self.dsn)
        except Exception as exc:
            raise ConnectionError(
                f"Cannot reach PostgreSQL ({self.dsn}).\n"
                f"  1. Ensure PostgreSQL is running.\n"
                f"  2. Create the database: createdb disaster_dw\n"
                f"  Original error: {exc}"
            ) from exc

    def create_tables(self) -> None:
        """
        Execute the full star-schema DDL.
        Safe to call on every run (all statements use IF NOT EXISTS).
        """
        with self._conn.cursor() as cur:
            cur.execute(_DDL)
        self._conn.commit()
        log.info("[POSTGRES] Star-schema tables created (or already exist)")

    # ── Transaction helpers ────────────────────────────────────────────────────

    def cursor(self):
        return self._conn.cursor()

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def close(self) -> None:
        if self._conn and not self._conn.closed:
            self._conn.close()
            log.info("[POSTGRES] Connection closed")
