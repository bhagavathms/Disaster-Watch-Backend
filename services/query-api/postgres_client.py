"""
postgres_client.py
PostgreSQL Read-Only Client -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Provides three analytical query operations against the disaster_dw star schema:
    get_event_counts()    -- total events per disaster type
    get_monthly_trends()  -- monthly event counts (optional type/year filter)
    get_top_locations()   -- regions ranked by event count

Uses a ThreadedConnectionPool so multiple FastAPI thread-pool workers can
each hold their own connection without blocking each other.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Optional

from config import PostgresSettings

log = logging.getLogger(__name__)

_MONTH_NAMES = {
    1: "January",  2: "February", 3: "March",    4: "April",
    5: "May",       6: "June",     7: "July",     8: "August",
    9: "September", 10: "October", 11: "November", 12: "December",
}


class PostgresReadClient:
    """
    Read-only PostgreSQL client backed by a ThreadedConnectionPool.

    Star-schema tables queried:
        dim_event_type       -- event_type_id, event_type_name
        dim_location         -- location_id, country, region, lat, lon
        dim_time             -- time_id, date, day, month, year, hour
        fact_disaster_events -- fact_id, event_id, FKs, severity_level, source
    """

    def __init__(self, settings: PostgresSettings) -> None:
        self._settings = settings
        self._pool     = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Create the connection pool.  Pool size: 1 min, 10 max connections.
        Raises ConnectionError if PostgreSQL is unreachable.
        """
        try:
            import psycopg2
            from psycopg2 import pool as pg_pool
        except ImportError:
            raise ImportError(
                "psycopg2 not installed.  Run: pip install psycopg2-binary"
            )
        try:
            self._pool = pg_pool.ThreadedConnectionPool(
                1, 10, self._settings.dsn
            )
            log.info("[POSTGRES] Connection pool ready  dsn=%s", self._settings.dsn)
        except Exception as exc:
            self._pool = None
            raise ConnectionError(
                f"Cannot reach PostgreSQL ({self._settings.dsn}).\n"
                f"  1. Ensure PostgreSQL is running.\n"
                f"  2. Create the database: createdb disaster_dw\n"
                f"  3. Run the ETL service first: python etl_runner.py --simulate\n"
                f"  Original error: {exc}"
            ) from exc

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            log.info("[POSTGRES] Connection pool closed")

    def is_connected(self) -> bool:
        """Lightweight connectivity check (used by /health endpoint)."""
        if self._pool is None:
            return False
        try:
            conn = self._pool.getconn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            self._pool.putconn(conn)
            return True
        except Exception:
            return False

    # ── Context manager ────────────────────────────────────────────────────────

    @contextmanager
    def _conn(self):
        """Borrow a connection from the pool and return it when done."""
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    # ── Analytical queries ─────────────────────────────────────────────────────

    def get_event_counts(self) -> list[dict]:
        """
        Total event count grouped by disaster type.

        SQL:
            SELECT event_type_name, COUNT(*) AS count
            FROM fact_disaster_events
            JOIN dim_event_type ...
            GROUP BY event_type_name
            ORDER BY count DESC
        """
        sql = """
            SELECT
                et.event_type_name  AS event_type,
                COUNT(*)            AS count
            FROM fact_disaster_events f
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            GROUP BY et.event_type_name
            ORDER BY count DESC
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return [{"event_type": r[0], "count": r[1]} for r in rows]

    def get_monthly_trends(
        self,
        event_type: Optional[str] = None,
        year:       Optional[int] = None,
    ) -> list[dict]:
        """
        Monthly event counts, optionally filtered by event type and/or year.

        Returns rows ordered chronologically (earliest month first) so
        callers can plot a time series directly.
        """
        params:         list   = []
        where_clauses:  list   = []

        if event_type:
            where_clauses.append("et.event_type_name = %s")
            params.append(event_type)
        if year:
            where_clauses.append("t.year = %s")
            params.append(year)

        where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        sql = f"""
            SELECT
                t.year,
                t.month,
                COUNT(*) AS count
            FROM fact_disaster_events f
            JOIN dim_time       t  ON f.time_id       = t.time_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            {where}
            GROUP BY t.year, t.month
            ORDER BY t.year ASC, t.month ASC
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {
                "year":       r[0],
                "month":      r[1],
                "month_name": _MONTH_NAMES.get(r[1], "Unknown"),
                "count":      r[2],
            }
            for r in rows
        ]

    def get_top_locations(
        self,
        limit:      int            = 10,
        event_type: Optional[str]  = None,
    ) -> list[dict]:
        """
        Geographic regions (country + region) ranked by total event count.

        Optionally scoped to a single disaster type.
        """
        params: list = []
        where  = ""
        if event_type:
            where = "WHERE et.event_type_name = %s"
            params.append(event_type)

        sql = f"""
            SELECT
                l.country,
                l.region,
                COUNT(*) AS count
            FROM fact_disaster_events f
            JOIN dim_location    l  ON f.location_id   = l.location_id
            JOIN dim_event_type  et ON f.event_type_id = et.event_type_id
            {where}
            GROUP BY l.country, l.region
            ORDER BY count DESC
            LIMIT %s
        """
        params.append(limit)

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        return [
            {"country": r[0], "region": r[1], "count": r[2]}
            for r in rows
        ]
