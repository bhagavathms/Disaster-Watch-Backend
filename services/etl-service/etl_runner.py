"""
etl_runner.py
ETL Orchestrator -- Module 5 (ETL Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Batch ETL job that moves processed disaster events from MongoDB
into a PostgreSQL star-schema data warehouse.

Pipeline:
    [MongoDB]  disaster_db.disaster_events
        |
        | extract.py  -- pulls flat docs, applies dedup filter
        v
    [Python]   list[dict]  (raw extraction records)
        |
        | transform.py  -- classifies location, decomposes timestamp,
        |                  builds TransformedEvent dataclass
        v
    [Python]   list[TransformedEvent]
        |
        | load.py  -- upserts dim tables, inserts fact rows
        v
    [PostgreSQL]  disaster_dw  (dim_event_type, dim_location,
                                 dim_time, fact_disaster_events)

Usage:
    python etl_runner.py --simulate --dry-run        # zero external deps
    python etl_runner.py --simulate                  # JSON files -> PostgreSQL
    python etl_runner.py                             # full ETL (MongoDB + PG)
    python etl_runner.py --dry-run                   # MongoDB extract, no PG
    python etl_runner.py --since 2024-11-01          # incremental from date
    python etl_runner.py --create-tables             # DDL only, then exit
    python etl_runner.py --mongo-uri mongodb://...   # custom Mongo URI
    python etl_runner.py --pg-dsn postgresql://...   # custom Postgres DSN
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

# ── Logging (console + file) ──────────────────────────────────────────────────
_LOG_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "logs")
)
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  [%(levelname)s]  %(name)s: %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            os.path.join(_LOG_DIR, "etl_service.log"), encoding="utf-8"
        ),
    ],
)
log = logging.getLogger("ETLRunner")

# ── Local imports ──────────────────────────────────────────────────────────────
from db_mongo   import MongoExtractClient
from db_postgres import PostgresClient
from extract    import extract
from transform  import transform
from load       import load

# ── Defaults ──────────────────────────────────────────────────────────────────
_DEFAULT_MONGO_URI = "mongodb://localhost:28020/"  # mongos router
_DEFAULT_PG_DSN    = "postgresql://postgres:postgres@localhost:5432/disaster_dw"


# ── CLI ────────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ETL: MongoDB disaster_events -> PostgreSQL star schema",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--mongo-uri",
        default=_DEFAULT_MONGO_URI,
        metavar="URI",
        help=f"MongoDB connection URI  (default: {_DEFAULT_MONGO_URI})",
    )
    p.add_argument(
        "--pg-dsn",
        default=_DEFAULT_PG_DSN,
        metavar="DSN",
        help=f"PostgreSQL DSN  (default: {_DEFAULT_PG_DSN})",
    )
    p.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="Incremental mode: only extract events on or after this date",
    )
    p.add_argument(
        "--create-tables",
        action="store_true",
        help="Create (or verify) PostgreSQL star-schema tables, then exit",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Extract and transform only. Print a sample of transformed records. "
            "No data is written to PostgreSQL."
        ),
    )
    p.add_argument(
        "--simulate",
        action="store_true",
        help=(
            "Read from local JSON data files instead of MongoDB. "
            "No Kafka or MongoDB required. Combine with --dry-run to run "
            "with zero external service dependencies."
        ),
    )
    p.add_argument(
        "--data-dir",
        default=None,
        metavar="PATH",
        help="Path to the data/ directory (used with --simulate). "
             "Defaults to <project-root>/data/",
    )
    return p.parse_args()


# ── Main ETL flow ─────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    since: datetime | None = None
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            log.error("--since must be YYYY-MM-DD format.  Got: %s", args.since)
            sys.exit(1)

    _print_banner(args)

    # ── Simulate mode: read JSON files, skip MongoDB ──────────────────────────
    if args.simulate:
        raw_events = _simulate_extract(args, since)
        if not raw_events:
            log.info("No events loaded from simulation files.")
            return
        transformed = transform(raw_events)
        if not transformed:
            log.warning("All events skipped during transform.")
            return
        if args.dry_run:
            _print_dry_run(transformed)
            return
        # Simulate + write to PostgreSQL
        pg = PostgresClient(dsn=args.pg_dsn)
        try:
            pg.connect()
            pg.create_tables()
            exclude_ids = _fetch_pg_event_ids(pg)
            transformed = [e for e in transformed if e.event_id not in exclude_ids]
            stats = load(pg._conn, transformed)
            stats.print_report()
        except (ImportError, ConnectionError) as exc:
            log.error("%s", exc)
            sys.exit(1)
        finally:
            pg.close()
        return

    # ── Live mode: MongoDB + PostgreSQL ───────────────────────────────────────
    mongo = MongoExtractClient(uri=args.mongo_uri)
    try:
        mongo.connect()
    except (ImportError, ConnectionError) as exc:
        log.error("%s", exc)
        sys.exit(1)

    pg: PostgresClient | None = None
    if not args.dry_run:
        pg = PostgresClient(dsn=args.pg_dsn)
        try:
            pg.connect()
            pg.create_tables()
        except (ImportError, ConnectionError) as exc:
            log.error("%s", exc)
            mongo.close()
            sys.exit(1)

    if args.create_tables:
        log.info("--create-tables complete. Exiting.")
        if pg:
            pg.close()
        mongo.close()
        return

    try:
        # ── Step 1: Extract ───────────────────────────────────────────────────
        exclude_ids: set[str] = set()
        if pg is not None:
            exclude_ids = _fetch_pg_event_ids(pg)

        raw_events = extract(mongo, since=since, exclude_ids=exclude_ids)

        if not raw_events:
            log.info("No new events to process. ETL complete with nothing to do.")
            return

        # ── Step 2: Transform ─────────────────────────────────────────────────
        transformed = transform(raw_events)

        if not transformed:
            log.warning("All events were skipped during transform. Check the data.")
            return

        # ── Step 3: Dry-run output ────────────────────────────────────────────
        if args.dry_run:
            _print_dry_run(transformed)
            return

        # ── Step 4: Load ──────────────────────────────────────────────────────
        stats = load(pg._conn, transformed)
        stats.print_report()

    finally:
        mongo.close()
        if pg:
            pg.close()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _simulate_extract(args: argparse.Namespace, since: datetime | None) -> list[dict]:
    """
    Load events from local JSON data files (Module 1 output) and
    convert them into extraction-layer dicts that match what MongoExtractClient
    would return from a live database.

    Severity classification is applied inline so that only events that would
    have passed Module 3's threshold filter reach the transform layer.
    Records below the minimum threshold are filtered out, matching the live
    pipeline behaviour.
    """
    import json

    data_dir = args.data_dir or os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..", "data"
        )
    )
    log.info("[SIMULATE] Loading from data_dir=%s", data_dir)

    # Severity thresholds (mirrors kafka-consumer/config.py SEVERITY_THRESHOLDS)
    _THRESHOLDS = {
        "earthquake": {"min": 1.5, "medium": 3.0, "high": 5.0, "critical": 7.0},
        "fire":       {"min": 5.0, "medium": 100.0, "high": 2000.0, "critical": 7000.0},
        "flood":      {"min": 5.0, "medium": 50.0,  "high": 200.0,  "critical": 800.0},
        "storm":      {"min": 35.0, "medium": 62.0,  "high": 120.0,  "critical": 200.0},
    }
    _FILE_MAP = {
        "earthquake": "earthquakes.json",
        "fire":       "fires.json",
        "flood":      "floods.json",
        "storm":      "storms.json",
    }

    def _classify(etype: str, sev: float) -> str | None:
        t = _THRESHOLDS.get(etype, {})
        if sev < t.get("min", 0):      return None
        if sev >= t.get("critical", float("inf")): return "CRITICAL"
        if sev >= t.get("high", float("inf")):     return "HIGH"
        if sev >= t.get("medium", float("inf")):   return "MEDIUM"
        return "LOW"

    records: list[dict] = []
    for etype, fname in _FILE_MAP.items():
        path = os.path.join(data_dir, fname)
        try:
            with open(path, encoding="utf-8") as fh:
                events = json.load(fh)
        except FileNotFoundError:
            log.warning("[SIMULATE] File not found: %s", path)
            continue

        for e in events:
            level = _classify(etype, e.get("severity_raw", 0.0))
            if level is None:
                continue  # below threshold — same as Module 3 drop

            # Apply --since filter if set
            if since is not None:
                from datetime import datetime, timezone
                ts = datetime.strptime(e["timestamp"], "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                if ts < since:
                    continue

            records.append({
                "event_id":       e["event_id"],
                "event_type":     e["event_type"],
                "severity_level": level,
                "latitude":       e["latitude"],
                "longitude":      e["longitude"],
                "event_time":     e["timestamp"],
                "source":         e["source"],
            })

    log.info("[SIMULATE] Loaded %d events (after threshold filter)", len(records))
    return records


def _fetch_pg_event_ids(pg: PostgresClient) -> set[str]:
    """
    Return all event_ids already stored in fact_disaster_events.
    This is the primary deduplication gate: events already in PostgreSQL
    are excluded from the current ETL run before transformation even starts.
    Returns an empty set on the first run (table exists but is empty).
    """
    try:
        with pg.cursor() as cur:
            cur.execute("SELECT event_id FROM fact_disaster_events")
            ids = {row[0] for row in cur.fetchall()}
        log.info("[DEDUP] %d event_id(s) already loaded in PostgreSQL", len(ids))
        return ids
    except Exception:
        # Table not yet populated (first run) or other transient error
        pg.rollback()
        return set()


def _print_dry_run(transformed) -> None:
    """Print a sample of transformed records without touching PostgreSQL."""
    sep = "=" * 62
    print(f"\n{sep}")
    print("  [DRY RUN] Transformed records ready for load")
    print(f"  Total: {len(transformed)}  |  Showing first 5")
    print(sep)

    for ev in transformed[:5]:
        print(f"\n  event_id        : {ev.event_id}")
        print(f"  --- dim_event_type ---")
        print(f"  event_type_name : {ev.event_type_name}")
        print(f"  --- dim_location ---")
        print(f"  latitude        : {ev.latitude}")
        print(f"  longitude       : {ev.longitude}")
        print(f"  country         : {ev.country}")
        print(f"  region          : {ev.region}")
        print(f"  --- dim_time ---")
        print(f"  date            : {ev.date}")
        print(f"  day / month / year / hour : "
              f"{ev.day} / {ev.month} / {ev.year} / {ev.hour:02d}:00")
        print(f"  --- fact_disaster_events ---")
        print(f"  severity_level  : {ev.severity_level}")
        print(f"  source          : {ev.source}")

    if len(transformed) > 5:
        print(f"\n  ... and {len(transformed) - 5} more event(s)")
    print(f"\n  No data was written to PostgreSQL (dry-run mode)")
    print(sep)


def _print_banner(args: argparse.Namespace) -> None:
    sep = "=" * 62
    print(f"\n{sep}")
    print("  Disaster Monitoring System -- Module 5")
    print("  ETL Service: MongoDB -> PostgreSQL Data Warehouse")
    print(sep)
    if args.simulate:
        print(f"  Source       : local JSON files (simulate mode)")
    else:
        print(f"  MongoDB URI  : {args.mongo_uri}")
    if not args.dry_run:
        print(f"  PostgreSQL   : {args.pg_dsn}")
    print(f"  Since filter : {args.since or 'none (full load)'}")
    print(f"  Simulate     : {args.simulate}")
    print(f"  Dry run      : {args.dry_run}")
    print(sep + "\n")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    run(_parse_args())
