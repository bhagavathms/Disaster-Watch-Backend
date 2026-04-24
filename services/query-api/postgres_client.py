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

try:
    import h3
    _H3_AVAILABLE = True
except ImportError:
    _H3_AVAILABLE = False
    logging.warning("h3 not installed. Map clustering will fall back to square grid.")

# Square grid fallback cell sizes (degrees) when h3 is not installed
_SQUARE_CELL: dict[int, float] = {
    2: 20.0, 3: 12.0,
    4:  6.0, 5:  3.0,
    6:  1.5, 7:  0.75,
    8:  0.3, 9:  0.1,
}

# SQL pre-aggregation cell size (degrees) — fine enough to preserve spatial
# accuracy before H3 re-clusters, coarse enough to keep SQL rows manageable.
_SQL_PRE_CELL: dict[int, float] = {
    2: 5.0,  3: 2.0,
    4: 1.0,  5: 0.5,
    6: 0.2,  7: 0.1,
    8: 0.05, 9: 0.02,
}

# H3 resolution per Leaflet zoom level.
# res 2 ≈ 86 000 km²  res 3 ≈ 12 000 km²  res 4 ≈ 1 700 km²  res 5 ≈ 250 km²
_H3_RES: dict[int, int] = {
    2: 2,  3: 2,  4: 2,   # ~86 000 km² — country-scale cells at world view
    5: 3,  6: 3,           # ~12 000 km²
    7: 3,  8: 4,           # zoom 7 coarsened to res 3 to avoid mid-zoom fragmentation
    9: 4,                  # ~1 700 km² — res 5 was too fine, caused too many 1-event clusters
}

# Minimum event count for a cluster to be returned (filters noise at low zoom).
_MIN_COUNT: dict[int, int] = {2: 100, 3: 50, 4: 20, 5: 5}   # zoom 6+ → 0

# Maximum clusters returned per event_type per zoom level.
# Prevents flooding the frontend with tiny clusters at mid zoom levels.
_MAX_PER_TYPE: dict[int, int] = {
    2: 500, 3: 300,
    4: 200, 5: 100,
    6:  80, 7: 100,
    8: 150, 9: 200,        # caps added — no cap caused thousands of clusters at fine resolution
}


def _h3_cluster(rows: list, zoom: int) -> list[dict]:
    """
    Apply H3 hexagonal clustering to pre-aggregated (lat, lng, event_type, count) rows.

    Groups by (H3 cell, event_type) using weighted centroids so the returned
    lat/lng is the true centre of mass of events in each hexagon.
    """
    res = _H3_RES.get(zoom, 5)
    acc: dict[tuple, dict] = {}

    for lat, lng, event_type, count in rows:
        cell_id = h3.latlng_to_cell(float(lat), float(lng), res)
        key = (cell_id, event_type)
        if key not in acc:
            acc[key] = {"lat_sum": 0.0, "lng_sum": 0.0, "total": 0}
        acc[key]["lat_sum"] += float(lat) * count
        acc[key]["lng_sum"] += float(lng) * count
        acc[key]["total"]   += count

    clusters = [
        {
            "lat":        v["lat_sum"] / v["total"],
            "lng":        v["lng_sum"] / v["total"],
            "event_type": event_type,
            "count":      v["total"],
        }
        for (_, event_type), v in acc.items()
    ]

    # Filter tiny clusters at low zoom (cosmetic noise reduction)
    min_count = _MIN_COUNT.get(zoom, 0)
    if min_count:
        clusters = [c for c in clusters if c["count"] >= min_count]

    # Cap per event_type so every disaster category's top hotspots are visible
    limit = _MAX_PER_TYPE.get(zoom)
    if limit is None:
        return clusters

    by_type: dict[str, list] = {}
    for c in clusters:
        by_type.setdefault(c["event_type"], []).append(c)

    result: list[dict] = []
    for type_clusters in by_type.values():
        type_clusters.sort(key=lambda c: -c["count"])
        result.extend(type_clusters[:limit])

    return result

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
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        country:    Optional[str] = None,
    ) -> list[dict]:
        """
        Monthly event counts, optionally filtered by event type, year,
        date range (YYYY-MM-DD), and/or country (ILIKE).

        Returns rows ordered chronologically (earliest month first).
        """
        where_clauses: list[str] = []
        params:        dict      = {}

        if event_type:
            where_clauses.append("et.event_type_name = %(event_type)s")
            params["event_type"] = event_type
        if year:
            where_clauses.append("t.year = %(year)s")
            params["year"] = year
        if start_date:
            where_clauses.append("t.date >= %(start_date)s")
            params["start_date"] = start_date
        if end_date:
            where_clauses.append("t.date <= %(end_date)s")
            params["end_date"] = end_date
        if country:
            where_clauses.append("lower(l.country) = lower(%(country)s)")
            params["country"] = country

        needs_location  = country is not None
        needs_event_type = event_type is not None

        location_join   = "JOIN dim_location   l  ON f.location_id   = l.location_id" if needs_location else ""
        event_type_join = "JOIN dim_event_type et ON f.event_type_id = et.event_type_id" if needs_event_type else ""

        where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        sql = f"""
            SELECT
                t.year,
                t.month,
                COUNT(*) AS count
            FROM fact_disaster_events f
            JOIN dim_time       t  ON f.time_id       = t.time_id
            {event_type_join}
            {location_join}
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

    def search_events(
        self,
        start_date:     Optional[str]       = None,
        end_date:       Optional[str]       = None,
        event_types:    Optional[list[str]] = None,
        country:        Optional[str]       = None,
        region:         Optional[str]       = None,
        lat:            Optional[float]     = None,
        lng:            Optional[float]     = None,
        proximity_km:   float               = 500.0,
        limit:          int                 = 20,
        offset:         int                 = 0,
    ) -> dict:
        """
        Flexible event search backed by the PostgreSQL star schema.
        Supports date range, multi-type, country (ILIKE), and lat/lng
        bounding-box proximity filters.  Returns {events, total}.
        """
        where:   list[str] = []
        params:  dict      = {"limit": limit, "offset": offset}

        if start_date:
            where.append("t.date >= %(start_date)s")
            params["start_date"] = start_date
        if end_date:
            where.append("t.date <= %(end_date)s")
            params["end_date"] = end_date

        if event_types:
            where.append("et.event_type_name = ANY(%(event_types)s)")
            params["event_types"] = list(event_types)

        if country:
            where.append("lower(l.country) = lower(%(country)s)")
            params["country"] = country

        if region:
            where.append("l.region ILIKE %(region)s")
            params["region"] = f"%{region}%"

        if lat is not None and lng is not None:
            # ±4.5° ≈ 500 km bounding box
            deg = proximity_km / 111.0
            where.append(
                "l.latitude  BETWEEN %(prox_min_lat)s AND %(prox_max_lat)s"
                " AND l.longitude BETWEEN %(prox_min_lng)s AND %(prox_max_lng)s"
            )
            params.update(
                prox_min_lat=lat - deg, prox_max_lat=lat + deg,
                prox_min_lng=lng - deg, prox_max_lng=lng + deg,
            )

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        count_sql = f"""
            SELECT COUNT(*)
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            {where_sql}
        """
        events_sql = f"""
            SELECT
                f.event_id,
                et.event_type_name      AS event_type,
                f.severity_level,
                l.latitude,
                l.longitude,
                l.country,
                l.region,
                (t.date + (t.hour * INTERVAL '1 hour'))::TEXT AS event_time,
                f.source
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            {where_sql}
            ORDER BY f.time_id DESC
            LIMIT %(limit)s
            OFFSET %(offset)s
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total: int = cur.fetchone()[0]
                cur.execute(events_sql, params)
                rows = cur.fetchall()

        events = [
            {
                "event_id":       r[0],
                "event_type":     r[1],
                "severity_level": r[2],
                "latitude":       float(r[3]),
                "longitude":      float(r[4]),
                "country":        r[5],
                "region":         r[6],
                "event_time":     r[7],
                "source":         r[8],
            }
            for r in rows
        ]
        return {"events": events, "total": total}

    def get_event_summary(
        self,
        start_date:   Optional[str]       = None,
        end_date:     Optional[str]       = None,
        event_types:  Optional[list[str]] = None,
        country:      Optional[str]       = None,
    ) -> dict:
        """
        Fast GROUP BY summary: per-type and per-severity counts for the given
        filters.  Replaces fetching a 2000-event analytics sample on the frontend.
        """
        where:  list[str] = []
        params: dict      = {}

        if start_date:
            where.append("t.date >= %(start_date)s");  params["start_date"] = start_date
        if end_date:
            where.append("t.date <= %(end_date)s");    params["end_date"]   = end_date
        if event_types:
            where.append("et.event_type_name = ANY(%(event_types)s)")
            params["event_types"] = list(event_types)
        if country:
            where.append("lower(l.country) = lower(%(country)s)")
            params["country"] = country

        where_sql = ("WHERE " + " AND ".join(where)) if where else ""

        sql = f"""
            SELECT et.event_type_name, f.severity_level, COUNT(*) AS cnt
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            {where_sql}
            GROUP BY et.event_type_name, f.severity_level
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()

        by_type: dict[str, int] = {}
        by_sev:  dict[str, int] = {}
        total = 0
        for event_type, severity, cnt in rows:
            by_type[event_type] = by_type.get(event_type, 0) + cnt
            by_sev[severity]    = by_sev.get(severity,    0) + cnt
            total += cnt

        return {"by_type": by_type, "by_severity": by_sev, "total": total}

    def ensure_map_indexes(self) -> None:
        """
        Create B-tree indexes that make /events/map queries fast at scale.
        Safe to call repeatedly (IF NOT EXISTS).
        """
        stmts = [
            "CREATE INDEX IF NOT EXISTS idx_dim_location_lat_lon "
            "ON dim_location (latitude, longitude)",
            "CREATE INDEX IF NOT EXISTS idx_dim_time_date "
            "ON dim_time (date)",
        ]
        with self._conn() as conn:
            with conn.cursor() as cur:
                for stmt in stmts:
                    cur.execute(stmt)
            conn.commit()
        log.info("[POSTGRES] Map indexes verified")


    def get_map_clustered(
        self,
        min_lat:    float,
        max_lat:    float,
        min_lon:    float,
        max_lon:    float,
        zoom:       int,
        start_date: str,
        end_date:   str,
    ) -> dict:
        if _H3_AVAILABLE:
            cell = _SQL_PRE_CELL.get(zoom, 0.1)
        else:
            cell = _SQUARE_CELL.get(zoom, 1.0)

        count_sql = """
            SELECT COUNT(*)
            FROM fact_disaster_events f
            JOIN dim_location l ON f.location_id = l.location_id
            JOIN dim_time     t ON f.time_id      = t.time_id
            WHERE l.latitude  BETWEEN %(min_lat)s  AND %(max_lat)s
              AND l.longitude BETWEEN %(min_lon)s  AND %(max_lon)s
              AND t.date      BETWEEN %(start_date)s AND %(end_date)s
        """
        cluster_sql = """
            SELECT
                AVG(l.latitude)::FLOAT   AS lat,
                AVG(l.longitude)::FLOAT  AS lng,
                et.event_type_name       AS event_type,
                COUNT(*)                 AS count
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            WHERE l.latitude  BETWEEN %(min_lat)s  AND %(max_lat)s
              AND l.longitude BETWEEN %(min_lon)s  AND %(max_lon)s
              AND t.date      BETWEEN %(start_date)s AND %(end_date)s
            GROUP BY
                ROUND((l.latitude  / %(cell)s)::NUMERIC),
                ROUND((l.longitude / %(cell)s)::NUMERIC),
                et.event_type_name
            ORDER BY count DESC
        """
        params = dict(
            cell=cell,
            min_lat=min_lat, max_lat=max_lat,
            min_lon=min_lon, max_lon=max_lon,
            start_date=start_date, end_date=end_date,
        )
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_count: int = cur.fetchone()[0]
                cur.execute(cluster_sql, params)
                rows = cur.fetchall()

        if _H3_AVAILABLE:
            clusters = _h3_cluster(rows, zoom)
        else:
            clusters = [
                {"lat": r[0], "lng": r[1], "event_type": r[2], "count": r[3]}
                for r in rows
            ]

        return {"total_count": total_count, "clusters": clusters}

    def get_map_events(
        self,
        min_lat:    float,
        max_lat:    float,
        min_lon:    float,
        max_lon:    float,
        start_date: str,
        end_date:   str,
    ) -> list[dict]:
        """
        Return up to 1000 individual events within the viewport bbox.
        Used at zoom ≥ 10 where the viewport covers a small geographic area.

        event_time is reconstructed from dim_time.date + hour offset since
        the star schema stores date and hour as separate columns.
        """
        count_sql = """
            SELECT COUNT(*)
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            WHERE l.latitude  BETWEEN %(min_lat)s  AND %(max_lat)s
              AND l.longitude BETWEEN %(min_lon)s  AND %(max_lon)s
              AND t.date      BETWEEN %(start_date)s AND %(end_date)s
        """
        events_sql = """
            SELECT
                f.event_id,
                et.event_type_name      AS event_type,
                f.severity_level,
                l.latitude,
                l.longitude,
                (t.date + (t.hour * INTERVAL '1 hour'))::TEXT AS event_time
            FROM fact_disaster_events f
            JOIN dim_location   l  ON f.location_id   = l.location_id
            JOIN dim_event_type et ON f.event_type_id = et.event_type_id
            JOIN dim_time       t  ON f.time_id       = t.time_id
            WHERE l.latitude  BETWEEN %(min_lat)s  AND %(max_lat)s
              AND l.longitude BETWEEN %(min_lon)s  AND %(max_lon)s
              AND t.date      BETWEEN %(start_date)s AND %(end_date)s
            ORDER BY t.date DESC, t.hour DESC
            LIMIT 1000
        """
        params = dict(
            min_lat=min_lat, max_lat=max_lat,
            min_lon=min_lon, max_lon=max_lon,
            start_date=start_date, end_date=end_date,
        )
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(count_sql, params)
                total_count: int = cur.fetchone()[0]
                cur.execute(events_sql, params)
                rows = cur.fetchall()
        return {
            "total_count": total_count,
            "events": [
                {
                    "event_id":       r[0],
                    "event_type":     r[1],
                    "severity_level": r[2],
                    "latitude":       float(r[3]),
                    "longitude":      float(r[4]),
                    "event_time":     r[5],
                }
                for r in rows
            ],
        }
