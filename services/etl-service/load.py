"""
load.py
Load Layer -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Inserts TransformedEvent records into the PostgreSQL star schema.

Deduplication strategy:
    - Dimension tables: INSERT ... ON CONFLICT DO NOTHING + SELECT fallback
      (get-or-create pattern; safe to repeat across ETL runs)
    - Fact table: INSERT ... ON CONFLICT (event_id) DO NOTHING
      (event_id carries a UNIQUE constraint; re-runs are idempotent)

Transaction strategy:
    - Each event's four inserts (3 dims + 1 fact) are wrapped in a
      savepoint so a single bad event does not roll back the whole batch.
    - A single COMMIT is issued at the end of the full batch.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from transform import TransformedEvent

log = logging.getLogger(__name__)


# ── Load statistics ───────────────────────────────────────────────────────────

@dataclass
class LoadStats:
    """Running totals produced by one ETL load run."""
    total_events:        int = 0
    facts_inserted:      int = 0   # new rows in fact table
    facts_skipped:       int = 0   # event_id already existed (dedup)
    errors:              int = 0   # events that raised an exception

    def print_report(self) -> None:
        sep = "-" * 52
        rate = (
            f"{100 * self.facts_inserted / self.total_events:.1f}%"
            if self.total_events else "n/a"
        )
        print(f"\n{sep}")
        print("  MODULE 5 -- ETL LOAD REPORT")
        print(sep)
        print(f"  Events received         : {self.total_events}")
        print(f"  Fact rows inserted      : {self.facts_inserted}")
        print(f"  Fact rows skipped (dup) : {self.facts_skipped}")
        print(f"  Errors                  : {self.errors}")
        print(f"  Load rate               : {rate}")
        print(sep)


# ── Dimension: get-or-create helpers ─────────────────────────────────────────
# Pattern for each dimension:
#   1. Attempt INSERT ... ON CONFLICT DO NOTHING RETURNING <pk>
#   2. If the row already existed, fetchone() returns None  →  SELECT to get PK
# This avoids any need for advisory locks or xmax tricks.

def _get_or_create_event_type(cur, name: str) -> int:
    cur.execute(
        """
        INSERT INTO dim_event_type (event_type_name)
        VALUES (%s)
        ON CONFLICT (event_type_name) DO NOTHING
        RETURNING event_type_id
        """,
        (name,),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT event_type_id FROM dim_event_type WHERE event_type_name = %s",
        (name,),
    )
    return cur.fetchone()[0]


def _get_or_create_location(
    cur, lat: float, lon: float, country: str, region: str
) -> int:
    cur.execute(
        """
        INSERT INTO dim_location (latitude, longitude, country, region)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (latitude, longitude) DO NOTHING
        RETURNING location_id
        """,
        (lat, lon, country, region),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT location_id FROM dim_location WHERE latitude = %s AND longitude = %s",
        (lat, lon),
    )
    return cur.fetchone()[0]


def _get_or_create_time(cur, ev: TransformedEvent) -> int:
    cur.execute(
        """
        INSERT INTO dim_time (date, day, month, year, hour)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date, hour) DO NOTHING
        RETURNING time_id
        """,
        (ev.date, ev.day, ev.month, ev.year, ev.hour),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT time_id FROM dim_time WHERE date = %s AND hour = %s",
        (ev.date, ev.hour),
    )
    return cur.fetchone()[0]


def _insert_fact(
    cur,
    ev:            TransformedEvent,
    event_type_id: int,
    location_id:   int,
    time_id:       int,
    stats:         LoadStats,
) -> None:
    """
    Insert a fact row.  ON CONFLICT (event_id) DO NOTHING means that if the
    same event_id is submitted again (re-run scenario), it is silently skipped.
    cur.rowcount == 1 only when a new row was actually inserted.
    """
    cur.execute(
        """
        INSERT INTO fact_disaster_events
            (event_id, event_type_id, location_id, time_id,
             severity_level, source, count_metric)
        VALUES (%s, %s, %s, %s, %s, %s, 1)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            ev.event_id, event_type_id, location_id, time_id,
            ev.severity_level, ev.source,
        ),
    )
    if cur.rowcount == 1:
        stats.facts_inserted += 1
    else:
        stats.facts_skipped += 1


# ── Public load function ──────────────────────────────────────────────────────

def load(conn, events: list[TransformedEvent]) -> LoadStats:
    """
    Load all TransformedEvent records into the PostgreSQL star schema.

    For each event (in order):
        1. get_or_create dim_event_type  -> event_type_id
        2. get_or_create dim_location    -> location_id
        3. get_or_create dim_time        -> time_id
        4. insert into fact_disaster_events (ON CONFLICT event_id DO NOTHING)

    Per-event errors use savepoints so one bad record does not abort the
    rest of the batch.  A single COMMIT covers the entire successful batch.

    Args:
        conn:   An open psycopg2 connection (autocommit=False).
        events: Output of transform.transform().

    Returns:
        LoadStats with insert/skip/error counts.
    """
    stats = LoadStats(total_events=len(events))

    with conn.cursor() as cur:
        for i, ev in enumerate(events):
            savepoint = f"sp_{i}"
            try:
                cur.execute(f"SAVEPOINT {savepoint}")

                et_id  = _get_or_create_event_type(cur, ev.event_type_name)
                loc_id = _get_or_create_location(
                    cur, ev.latitude, ev.longitude, ev.country, ev.region
                )
                t_id   = _get_or_create_time(cur, ev)
                _insert_fact(cur, ev, et_id, loc_id, t_id, stats)

                cur.execute(f"RELEASE SAVEPOINT {savepoint}")

            except Exception as exc:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                stats.errors += 1
                log.error(
                    "[LOAD] Error  event_id=%s  error=%s", ev.event_id, exc
                )

    conn.commit()
    log.info(
        "[LOAD] Committed  inserted=%d  skipped=%d  errors=%d",
        stats.facts_inserted, stats.facts_skipped, stats.errors,
    )
    return stats
