"""
verify_pipeline.py
Pipeline Health-Check Script
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Checks every component that has been built so far — without needing
Kafka, MongoDB, or any external service to be running.

Run from the project root:
    python verify_pipeline.py

A green PASS on every check means the pipeline foundation is healthy
and ready for the next module.
"""

import json
import os
import sys

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")
PRODUCER_DIR  = os.path.join(PROJECT_ROOT, "services", "kafka-producer")
CONSUMER_DIR  = os.path.join(PROJECT_ROOT, "services", "kafka-consumer")
WRITER_DIR    = os.path.join(PROJECT_ROOT, "services", "mongodb-writer")
ETL_DIR       = os.path.join(PROJECT_ROOT, "services", "etl-service")
QUERY_API_DIR = os.path.join(PROJECT_ROOT, "services", "query-api")

EXPECTED_FILES = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}

REQUIRED_FIELDS = {
    "event_id", "event_type", "severity_raw",
    "latitude", "longitude", "timestamp", "source",
}

VALID_EVENT_TYPES = {"earthquake", "fire", "flood", "storm"}

# Minimum expected events per type (sanity threshold, not exact count)
MIN_EVENTS_PER_TYPE = 50

# ── Helpers ───────────────────────────────────────────────────────────────────

_passed = 0
_failed = 0


def check(label: str, condition: bool, detail: str = "") -> bool:
    global _passed, _failed
    status = "  PASS" if condition else "  FAIL"
    line   = f"{status}  {label}"
    if detail:
        line += f"\n         {detail}"
    print(line)
    if condition:
        _passed += 1
    else:
        _failed += 1
    return condition


def section(title: str) -> None:
    print(f"\n{'=' * 56}")
    print(f"  {title}")
    print("=" * 56)


# ── Check 1: Module 1 — Data files ───────────────────────────────────────────

def check_data_files() -> dict[str, list[dict]]:
    """Verify all four JSON files exist, are valid JSON, and are non-empty."""
    section("Module 1 — Dataset Files (data/)")

    check(
        "data/ directory exists",
        os.path.isdir(DATA_DIR),
        f"Expected at: {DATA_DIR}",
    )

    datasets: dict[str, list[dict]] = {}

    for event_type, filename in EXPECTED_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        exists = os.path.isfile(path)
        check(f"File exists: {filename}", exists, f"Path: {path}")

        if not exists:
            continue

        # Parse JSON
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            check(f"Valid JSON: {filename}", True)
        except json.JSONDecodeError as exc:
            check(f"Valid JSON: {filename}", False, str(exc))
            continue

        # Is it a list?
        is_list = isinstance(data, list)
        check(
            f"Top-level is a list: {filename}",
            is_list,
            f"Got {type(data).__name__}",
        )
        if not is_list:
            continue

        # Minimum event count
        check(
            f"Event count >= {MIN_EVENTS_PER_TYPE}: {filename}",
            len(data) >= MIN_EVENTS_PER_TYPE,
            f"Found {len(data)} events",
        )

        datasets[event_type] = data

    return datasets


# ── Check 2: Module 1 — Schema validation ────────────────────────────────────

def check_schema(datasets: dict[str, list[dict]]) -> None:
    """Spot-check every record in every file against the unified schema."""
    section("Module 1 — Schema Validation")

    for event_type, records in datasets.items():
        filename = EXPECTED_FILES[event_type]
        missing_fields, wrong_types, bad_coords, wrong_type_values = [], [], [], []

        for rec in records:
            eid = rec.get("event_id", "<unknown>")

            # Required fields present?
            missing = REQUIRED_FIELDS - rec.keys()
            if missing:
                missing_fields.append((eid, missing))

            # event_type value correct?
            if rec.get("event_type") not in VALID_EVENT_TYPES:
                wrong_type_values.append((eid, rec.get("event_type")))

            # severity_raw is numeric?
            if not isinstance(rec.get("severity_raw"), (int, float)):
                wrong_types.append((eid, type(rec.get("severity_raw")).__name__))

            # Coordinate ranges
            lat = rec.get("latitude",  999)
            lon = rec.get("longitude", 999)
            if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                bad_coords.append((eid, lat, lon))

        check(
            f"All required fields present: {filename}",
            len(missing_fields) == 0,
            f"{len(missing_fields)} records with missing fields" if missing_fields else "",
        )
        check(
            f"All event_type values valid: {filename}",
            len(wrong_type_values) == 0,
            f"{len(wrong_type_values)} records with bad event_type" if wrong_type_values else "",
        )
        check(
            f"severity_raw is numeric: {filename}",
            len(wrong_types) == 0,
            f"{len(wrong_types)} records with non-numeric severity" if wrong_types else "",
        )
        check(
            f"Coordinates in valid range: {filename}",
            len(bad_coords) == 0,
            f"{len(bad_coords)} records with out-of-range coords" if bad_coords else "",
        )


# ── Check 3: Module 1 — Distribution sanity ──────────────────────────────────

def check_distributions(datasets: dict[str, list[dict]]) -> None:
    """
    Verify the data is genuinely non-uniform.
    Each type should have: nulls (noise), outliers, and a spread.
    """
    section("Module 1 — Data Distribution Sanity")

    expected_ranges = {
        # (min_possible, max_possible, expected_unit)
        # min is 0.0 for all types because sensor-null records are intentional noise
        "earthquake": (0.0,  9.5,   "Richter"),
        "fire":       (0.0,  10000, "intensity index"),
        "flood":      (0.0,  2000,  "cm rise"),
        "storm":      (0.0,  300,   "km/h wind"),
    }

    for event_type, records in datasets.items():
        lo, hi, unit = expected_ranges[event_type]
        sevs  = [r["severity_raw"] for r in records]
        nulls = sum(1 for s in sevs if s == 0.0)
        mn    = min(sevs)
        mx    = max(sevs)
        mean  = sum(sevs) / len(sevs)

        check(
            f"severity_raw in [{lo}, {hi}] ({unit}): {EXPECTED_FILES[event_type]}",
            lo <= mn and mx <= hi,
            f"actual range [{mn:.2f}, {mx:.2f}]  mean={mean:.2f}  nulls={nulls}",
        )
        check(
            f"Non-trivial spread (max > 2x mean): {EXPECTED_FILES[event_type]}",
            mx > 2 * mean if mean > 0 else True,
            f"max={mx:.2f}  mean={mean:.2f}  -> ratio={mx/mean:.1f}x" if mean > 0 else "mean=0",
        )
        check(
            f"Contains noise (at least 1 null/zero): {EXPECTED_FILES[event_type]}",
            nulls >= 1,
            f"Found {nulls} zero-severity records",
        )


# ── Check 4: Module 1 — Timestamps ───────────────────────────────────────────

def check_timestamps(datasets: dict[str, list[dict]]) -> None:
    """Verify timestamps parse and span multiple days."""
    from datetime import datetime
    section("Module 1 — Timestamps")

    all_timestamps = []
    for event_type, records in datasets.items():
        bad = []
        for rec in records:
            ts = rec.get("timestamp", "")
            try:
                dt = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
                all_timestamps.append(dt)
            except (ValueError, TypeError):
                bad.append(ts)

        check(
            f"All timestamps are valid ISO-8601: {EXPECTED_FILES[event_type]}",
            len(bad) == 0,
            f"{len(bad)} unparseable timestamps" if bad else "",
        )

    if all_timestamps:
        span_days = (max(all_timestamps) - min(all_timestamps)).days
        check(
            "Timestamps span at least 30 days across all types",
            span_days >= 30,
            f"Actual span: {span_days} days  "
            f"({min(all_timestamps).date()} to {max(all_timestamps).date()})",
        )


# ── Check 5: Module 2 — Producer files ───────────────────────────────────────

def check_producer_files() -> None:
    """Verify the kafka-producer microservice files are present."""
    section("Module 2 — Kafka Producer Service Files")

    for filename in ["producer.py", "config.py", "requirements.txt", "README.md"]:
        path = os.path.join(PRODUCER_DIR, filename)
        check(
            f"File exists: services/kafka-producer/{filename}",
            os.path.isfile(path),
        )


# ── Check 6: Module 2 — Producer importable ──────────────────────────────────

def check_producer_imports() -> None:
    """Verify config.py imports without errors (no Kafka needed)."""
    section("Module 2 — Producer Configuration")

    sys.path.insert(0, PRODUCER_DIR)
    try:
        import config as cfg
        check("config.py imports successfully", True)

        has_enum  = hasattr(cfg, "ReplaySpeed")
        has_class = hasattr(cfg, "ProducerConfig")
        check("ReplaySpeed enum defined",   has_enum)
        check("ProducerConfig class defined", has_class)

        if has_enum:
            speeds = {s.name for s in cfg.ReplaySpeed}
            check(
                "ReplaySpeed has FAST / NORMAL / SLOW",
                speeds == {"FAST", "NORMAL", "SLOW"},
                f"Found: {speeds}",
            )

        if has_class:
            c = cfg.ProducerConfig()
            check(
                "Default broker is localhost:9092",
                c.bootstrap_servers == ["localhost:9092"],
                f"Got: {c.bootstrap_servers}",
            )
            check(
                "Default topic_map has 4 topics",
                len(c.topic_map) == 4,
                f"Got: {c.topic_map}",
            )
            check(
                "data_dir resolves to an existing directory",
                os.path.isdir(c.data_dir),
                f"Resolved to: {c.data_dir}",
            )

    except ImportError as exc:
        check("config.py imports successfully", False, str(exc))
    finally:
        sys.path.pop(0)
        # Remove cached modules so the consumer check imports its own config.py
        for mod in ["config", "producer"]:
            sys.modules.pop(mod, None)


# ── Check 7: Module 3 — Consumer processor files ─────────────────────────────

def check_consumer_files() -> None:
    """Verify the kafka-consumer microservice files are present."""
    section("Module 3 — Kafka Consumer Processor Files")

    for filename in ["consumer.py", "processor.py", "config.py",
                     "requirements.txt", "README.md"]:
        path = os.path.join(CONSUMER_DIR, filename)
        check(
            f"File exists: services/kafka-consumer/{filename}",
            os.path.isfile(path),
        )


def check_consumer_logic() -> None:
    """Import processor.py and run it against a handful of synthetic events."""
    section("Module 3 — Processor Logic (no Kafka needed)")

    sys.path.insert(0, CONSUMER_DIR)
    try:
        from processor import EventProcessor, ProcessedEvent
        from config import SEVERITY_THRESHOLDS, ConsumerConfig

        check("processor.py imports successfully", True)
        check("config.py (consumer) imports successfully", True)

        # Verify threshold table completeness
        check(
            "SEVERITY_THRESHOLDS covers all 4 event types",
            set(SEVERITY_THRESHOLDS.keys()) == {"earthquake", "fire", "flood", "storm"},
        )
        for etype, thresholds in SEVERITY_THRESHOLDS.items():
            keys = set(thresholds.keys())
            check(
                f"Threshold keys complete: {etype}",
                keys == {"min_threshold", "medium", "high", "critical"},
                f"Got: {keys}",
            )

        # Run processor against synthetic probe events
        proc = EventProcessor()

        probes = [
            # (event_type, severity_raw, expected_level)
            ("earthquake", 0.0,    None),        # sensor null -> drop
            ("earthquake", 1.2,    None),        # micro-quake -> drop
            ("earthquake", 2.5,    "LOW"),
            ("earthquake", 3.8,    "MEDIUM"),
            ("earthquake", 6.0,    "HIGH"),
            ("earthquake", 8.5,    "CRITICAL"),
            ("fire",        0.0,    None),
            ("fire",        50.0,   "LOW"),
            ("fire",       500.0,   "MEDIUM"),
            ("fire",      3000.0,   "HIGH"),
            ("fire",      8000.0,   "CRITICAL"),
            ("flood",       2.0,    None),
            ("flood",      20.0,    "LOW"),
            ("flood",     100.0,    "MEDIUM"),
            ("flood",     400.0,    "HIGH"),
            ("flood",    1000.0,    "CRITICAL"),
            ("storm",      10.0,    None),
            ("storm",      45.0,    "LOW"),
            ("storm",      90.0,    "MEDIUM"),
            ("storm",     150.0,    "HIGH"),
            ("storm",     250.0,    "CRITICAL"),
        ]

        all_ok = True
        for etype, sev, expected in probes:
            raw = {
                "event_id":    "test-00000000-0000-0000-0000",
                "event_type":  etype,
                "severity_raw": sev,
                "latitude":    0.0,
                "longitude":   0.0,
                "timestamp":   "2024-11-01T00:00:00Z",
                "source":      "TEST",
            }
            result = proc.process(raw)
            got    = result.severity_level if result else None
            if got != expected:
                all_ok = False
                check(
                    f"Classify {etype} sev={sev} -> {expected}",
                    False,
                    f"Got: {got}",
                )

        check(
            f"All {len(probes)} probe classifications correct",
            all_ok,
        )

        # Verify ProcessedEvent schema
        test_raw = {
            "event_id":    "abc-123",
            "event_type":  "flood",
            "severity_raw": 300.0,
            "latitude":    10.5,
            "longitude":   90.0,
            "timestamp":   "2024-11-01T08:00:00Z",
            "source":      "CEMS",
        }
        proc2   = EventProcessor()
        outcome = proc2.process(test_raw)
        check("ProcessedEvent is not None for valid event", outcome is not None)
        if outcome:
            d = outcome.to_dict()
            check(
                "ProcessedEvent has all 8 output fields",
                set(d.keys()) == {
                    "event_id", "event_type", "severity_level",
                    "latitude", "longitude", "event_time",
                    "processed_at", "source",
                },
                f"Got keys: {set(d.keys())}",
            )
            check("severity_raw removed from output", "severity_raw" not in d)
            check("timestamp renamed to event_time",   "event_time" in d)
            check("processed_at field injected",       "processed_at" in d)
            check(
                "severity_level is HIGH for flood 300cm",
                d["severity_level"] == "HIGH",
                f"Got: {d['severity_level']}",
            )

    except ImportError as exc:
        check("processor.py imports successfully", False, str(exc))
    finally:
        sys.path.pop(0)
        for mod in ["config", "processor"]:
            sys.modules.pop(mod, None)


# ── Check 8: Module 4 — MongoDB Writer files ──────────────────────────────────

def check_writer_files() -> None:
    """Verify the mongodb-writer microservice files are present."""
    section("Module 4 -- MongoDB Writer Service Files")

    for filename in ["writer.py", "db.py", "config.py", "requirements.txt", "README.md"]:
        path = os.path.join(WRITER_DIR, filename)
        check(
            f"File exists: services/mongodb-writer/{filename}",
            os.path.isfile(path),
        )


# ── Check 9: Module 4 — MongoDB Writer config ─────────────────────────────────

def check_writer_config() -> None:
    """Verify config.py imports and defaults are correct (no MongoDB needed)."""
    section("Module 4 -- MongoDB Writer Configuration")

    sys.path.insert(0, WRITER_DIR)
    try:
        from config import WriterConfig, MongoConfig, KafkaConfig
        check("config.py (writer) imports successfully", True)

        # MongoConfig defaults
        mc = MongoConfig()
        check(
            "MongoConfig.database = 'disaster_db'",
            mc.database == "disaster_db",
            f"Got: {mc.database}",
        )
        check(
            "MongoConfig.collection = 'disaster_events'",
            mc.collection == "disaster_events",
            f"Got: {mc.collection}",
        )
        check(
            "MongoConfig.batch_size = 50",
            mc.batch_size == 50,
            f"Got: {mc.batch_size}",
        )

        # KafkaConfig — separate group from Module 3
        kc = KafkaConfig()
        check(
            "KafkaConfig.group_id = 'disaster-storage-group'",
            kc.group_id == "disaster-storage-group",
            f"Got: {kc.group_id}",
        )
        check(
            "KafkaConfig has 4 topics",
            len(kc.topics) == 4,
            f"Got: {kc.topics}",
        )
        check(
            "KafkaConfig.enable_auto_commit = False",
            kc.enable_auto_commit is False,
            f"Got: {kc.enable_auto_commit}",
        )

        # WriterConfig wrapper
        wc = WriterConfig()
        check("WriterConfig wraps MongoConfig",    isinstance(wc.mongo, MongoConfig))
        check("WriterConfig wraps KafkaConfig",    isinstance(wc.kafka, KafkaConfig))
        check("WriterConfig.dry_run defaults False", wc.dry_run is False)
        check(
            "WriterConfig.data_dir resolves to existing directory",
            os.path.isdir(wc.data_dir),
            f"Resolved to: {wc.data_dir}",
        )

    except ImportError as exc:
        check("config.py (writer) imports successfully", False, str(exc))
    finally:
        if WRITER_DIR in sys.path:
            sys.path.remove(WRITER_DIR)
        for mod in ["config"]:
            sys.modules.pop(mod, None)


# ── Check 10: Module 4 — Document transformation (_to_document) ───────────────

def check_writer_document_transform() -> None:
    """Import db.py and verify _to_document() produces correct MongoDB structure."""
    section("Module 4 -- Document Transformation (_to_document)")

    sys.path.insert(0, WRITER_DIR)
    try:
        from db import MongoDBClient
        check("db.py imports successfully", True)

        sample = {
            "event_id":       "abc-123",
            "event_type":     "earthquake",
            "severity_level": "HIGH",
            "latitude":       -6.251812,
            "longitude":      119.820412,
            "event_time":     "2024-10-03T18:52:09Z",
            "processed_at":   "2026-02-24T10:50:41Z",
            "source":         "GeoNet",
        }
        doc = MongoDBClient._to_document(sample)

        # event_id -> _id
        check(
            "event_id promoted to _id",
            doc.get("_id") == "abc-123",
            f"Got _id: {doc.get('_id')}",
        )
        check("event_id key removed", "event_id" not in doc)

        # GeoJSON location
        loc = doc.get("location", {})
        check(
            "location.type = 'Point'",
            loc.get("type") == "Point",
            f"Got: {loc.get('type')}",
        )
        coords = loc.get("coordinates", [])
        check(
            "location.coordinates = [lon, lat] (GeoJSON order)",
            coords == [119.820412, -6.251812],
            f"Got: {coords}",
        )
        check("latitude key removed",  "latitude"  not in doc)
        check("longitude key removed", "longitude" not in doc)

        # Datetime conversion
        from datetime import datetime
        check(
            "event_time converted to datetime object",
            isinstance(doc.get("event_time"), datetime),
            f"Got type: {type(doc.get('event_time')).__name__}",
        )
        check(
            "processed_at converted to datetime object",
            isinstance(doc.get("processed_at"), datetime),
            f"Got type: {type(doc.get('processed_at')).__name__}",
        )
        if isinstance(doc.get("event_time"), datetime):
            check(
                "event_time is UTC-aware (has tzinfo)",
                doc["event_time"].tzinfo is not None,
                f"tzinfo: {doc['event_time'].tzinfo}",
            )

    except ImportError as exc:
        check("db.py imports successfully", False, str(exc))
    finally:
        if WRITER_DIR in sys.path:
            sys.path.remove(WRITER_DIR)
        for mod in ["config", "db"]:
            sys.modules.pop(mod, None)


# ── Check 11: Module 5 — ETL Service files ──────────────────────────────────

def check_etl_files() -> None:
    """Verify the etl-service microservice files are present."""
    section("Module 5 — ETL Service Files")

    for filename in ["etl_runner.py", "extract.py", "transform.py", "load.py",
                     "db_mongo.py", "db_postgres.py", "requirements.txt", "README.md"]:
        path = os.path.join(ETL_DIR, filename)
        check(
            f"File exists: services/etl-service/{filename}",
            os.path.isfile(path),
        )


# ── Check 12: Module 5 — ETL imports & configuration ────────────────────────

def check_etl_imports() -> None:
    """Verify key ETL classes/functions import and have expected structure."""
    section("Module 5 — ETL Imports & Configuration")

    sys.path.insert(0, ETL_DIR)
    try:
        import dataclasses

        # TransformedEvent dataclass
        from transform import TransformedEvent
        check("TransformedEvent imports successfully", True)

        expected_fields = {
            "event_type_name", "latitude", "longitude", "country", "region",
            "date", "day", "month", "year", "hour",
            "event_id", "severity_level", "source",
        }
        actual_fields = {f.name for f in dataclasses.fields(TransformedEvent)}
        check(
            "TransformedEvent has all 13 expected fields",
            expected_fields == actual_fields,
            f"Missing: {expected_fields - actual_fields}, Extra: {actual_fields - expected_fields}"
            if expected_fields != actual_fields else "",
        )

        # LoadStats dataclass
        from load import LoadStats
        check("LoadStats imports successfully", True)

        ls_fields = {f.name for f in dataclasses.fields(LoadStats)}
        expected_ls = {"total_events", "facts_inserted", "facts_skipped", "errors"}
        check(
            "LoadStats has all 4 expected fields",
            expected_ls == ls_fields,
            f"Missing: {expected_ls - ls_fields}" if expected_ls != ls_fields else "",
        )
        check(
            "LoadStats has print_report method",
            hasattr(LoadStats, "print_report") and callable(getattr(LoadStats, "print_report")),
        )

        # MongoExtractClient
        from db_mongo import MongoExtractClient
        check("MongoExtractClient imports successfully", True)

        mongo_methods = {"connect", "close", "fetch_all", "fetch_since", "fetch_loaded_ids"}
        missing_methods = {m for m in mongo_methods if not hasattr(MongoExtractClient, m)}
        check(
            "MongoExtractClient has all 5 expected methods",
            len(missing_methods) == 0,
            f"Missing: {missing_methods}" if missing_methods else "",
        )

        # PostgresClient + DDL
        from db_postgres import PostgresClient, _DDL
        check("PostgresClient and _DDL import successfully", True)

        ddl_tables = ["dim_event_type", "dim_location", "dim_time", "fact_disaster_events"]
        missing_tables = [t for t in ddl_tables if t not in _DDL]
        check(
            "_DDL contains all 4 star-schema table names",
            len(missing_tables) == 0,
            f"Missing tables in DDL: {missing_tables}" if missing_tables else "",
        )

        # ETL runner CLI flags (read as text, do NOT import)
        runner_path = os.path.join(ETL_DIR, "etl_runner.py")
        with open(runner_path, encoding="utf-8") as fh:
            runner_src = fh.read()
        check(
            "etl_runner.py has --simulate and --dry-run flags",
            "--simulate" in runner_src and "--dry-run" in runner_src,
        )

    except ImportError as exc:
        check("ETL imports successful", False, str(exc))
    finally:
        sys.path.pop(0)
        for mod in ["transform", "load", "db_mongo", "db_postgres", "extract", "config"]:
            sys.modules.pop(mod, None)


# ── Check 13: Module 5 — Transform & location logic ─────────────────────────

def check_etl_logic() -> None:
    """Run transform functions against sample data to verify correctness."""
    section("Module 5 — Transform & Location Logic")

    sys.path.insert(0, ETL_DIR)
    try:
        from transform import transform, _classify_location, TransformedEvent

        # ── transform() with a sample event ──────────────────────────────────
        sample_raw = {
            "event_id":       "test-etl-001",
            "event_type":     "earthquake",
            "severity_level": "HIGH",
            "latitude":       35.6,
            "longitude":      139.7,
            "event_time":     "2024-11-15T08:30:00Z",
            "source":         "USGS",
        }
        result = transform([sample_raw])
        check("transform() returns a list", isinstance(result, list))
        check(
            "transform() returns TransformedEvent instances",
            len(result) == 1 and isinstance(result[0], TransformedEvent),
            f"Got {len(result)} results, type: {type(result[0]).__name__}" if result else "Empty list",
        )

        if result:
            te = result[0]
            check(
                "TransformedEvent.event_type_name matches input",
                te.event_type_name == "earthquake",
                f"Got: {te.event_type_name}",
            )

            # Date decomposition
            check("Date decomposition: day = 15",   te.day == 15,   f"Got: {te.day}")
            check("Date decomposition: month = 11", te.month == 11, f"Got: {te.month}")
            check("Date decomposition: year = 2024", te.year == 2024, f"Got: {te.year}")
            check("Date decomposition: hour = 8",   te.hour == 8,   f"Got: {te.hour}")

        # ── _classify_location() probes ──────────────────────────────────────
        location_probes = [
            # (lat, lon, expected_country, label)
            (35.6,  139.7, "Japan",       "Tokyo area"),
            (34.0, -118.0, "USA",         "Los Angeles"),
            (-6.2,  119.8, "Indonesia",   "Sulawesi"),
            (23.0,   90.0, "Bangladesh",  "Dhaka area"),
            (-33.0, -71.0, "Chile",       "Santiago area"),
            (37.0,   22.0, "Greece",      "Peloponnese"),
            (-30.0, 135.0, "Australia",   "South Australia"),
            (60.0,  100.0, "Russia",      "Siberia"),
        ]

        for lat, lon, expected_country, label in location_probes:
            country, region = _classify_location(lat, lon)
            check(
                f"_classify_location({lat}, {lon}) -> {expected_country} ({label})",
                country == expected_country,
                f"Got: country={country}, region={region}",
            )

    except ImportError as exc:
        check("ETL transform imports successful", False, str(exc))
    finally:
        sys.path.pop(0)
        for mod in ["transform", "load", "db_mongo", "db_postgres", "extract", "config"]:
            sys.modules.pop(mod, None)


# ── Check 14: Module 6 — Query API files ────────────────────────────────────

def check_query_api_files() -> None:
    """Verify the query-api microservice files are present."""
    section("Module 6 — Query API Service Files")

    files = [
        "api.py", "config.py", "mongo_client.py", "postgres_client.py",
        "dependencies.py", "requirements.txt", "README.md",
        os.path.join("models", "__init__.py"),
        os.path.join("models", "schemas.py"),
        os.path.join("routers", "__init__.py"),
        os.path.join("routers", "events.py"),
        os.path.join("routers", "analytics.py"),
    ]
    for filename in files:
        path = os.path.join(QUERY_API_DIR, filename)
        display = f"services/query-api/{filename.replace(os.sep, '/')}"
        check(f"File exists: {display}", os.path.isfile(path))


# ── Check 15: Module 6 — Query API configuration & schemas ──────────────────

def check_query_api_config() -> None:
    """Verify config defaults and Pydantic schema models."""
    section("Module 6 — Query API Configuration & Schemas")

    sys.path.insert(0, QUERY_API_DIR)
    try:
        # Config classes
        from config import MongoSettings, PostgresSettings, Settings, settings
        check("Config classes import successfully", True)

        check(
            "settings.mongo.uri default",
            settings.mongo.uri == "mongodb://localhost:27017/",
            f"Got: {settings.mongo.uri}",
        )
        check(
            "settings.mongo.database = 'disaster_db'",
            settings.mongo.database == "disaster_db",
            f"Got: {settings.mongo.database}",
        )
        check(
            "settings.mongo.collection = 'disaster_events'",
            settings.mongo.collection == "disaster_events",
            f"Got: {settings.mongo.collection}",
        )
        check(
            "settings.postgres.dsn contains 'disaster_dw'",
            "disaster_dw" in settings.postgres.dsn,
            f"Got: {settings.postgres.dsn}",
        )
        check(
            "settings.host = '127.0.0.1'",
            settings.host == "127.0.0.1",
            f"Got: {settings.host}",
        )
        check(
            "settings.port = 8000",
            settings.port == 8000,
            f"Got: {settings.port}",
        )

        # Pydantic schema models
        from models.schemas import (
            EventResponse, HealthResponse, RecentEventsResponse,
            SearchEventsResponse, LocationEventsResponse,
            EventCountsResponse, MonthlyTrendsResponse, TopLocationsResponse,
            ServiceStatus,
        )
        check("All 9 Pydantic schema models import successfully", True)

        # EventResponse fields
        er_fields = set(EventResponse.model_fields.keys())
        expected_er = {
            "event_id", "event_type", "severity_level",
            "latitude", "longitude", "event_time", "processed_at", "source",
        }
        check(
            "EventResponse has all 8 expected fields",
            expected_er == er_fields,
            f"Missing: {expected_er - er_fields}" if expected_er != er_fields else "",
        )

        # HealthResponse fields
        hr_fields = set(HealthResponse.model_fields.keys())
        expected_hr = {"status", "mongodb", "postgresql", "version"}
        check(
            "HealthResponse has all 4 expected fields",
            expected_hr <= hr_fields,
            f"Missing: {expected_hr - hr_fields}" if not expected_hr <= hr_fields else "",
        )

    except ImportError as exc:
        check("Query API config/schema imports successful", False, str(exc))
    finally:
        sys.path.pop(0)
        for mod in ["config", "models", "models.schemas"]:
            sys.modules.pop(mod, None)


# ── Check 16: Module 6 — Clients, dependencies & router endpoints ───────────

def check_query_api_clients_and_routes() -> None:
    """Verify client classes, dependency functions, and FastAPI route paths."""
    section("Module 6 — Clients, Dependencies & Router Endpoints")

    sys.path.insert(0, QUERY_API_DIR)
    try:
        # MongoReadClient
        from mongo_client import MongoReadClient
        check("MongoReadClient imports successfully", True)

        mongo_methods = {"get_recent", "search", "by_location"}
        missing = {m for m in mongo_methods if not hasattr(MongoReadClient, m)}
        check(
            "MongoReadClient has get_recent, search, by_location",
            len(missing) == 0,
            f"Missing: {missing}" if missing else "",
        )
        check("MongoReadClient has is_connected", hasattr(MongoReadClient, "is_connected"))

        # PostgresReadClient
        from postgres_client import PostgresReadClient
        check("PostgresReadClient imports successfully", True)

        pg_methods = {"get_event_counts", "get_monthly_trends", "get_top_locations"}
        missing = {m for m in pg_methods if not hasattr(PostgresReadClient, m)}
        check(
            "PostgresReadClient has get_event_counts, get_monthly_trends, get_top_locations",
            len(missing) == 0,
            f"Missing: {missing}" if missing else "",
        )
        check("PostgresReadClient has is_connected", hasattr(PostgresReadClient, "is_connected"))

        # Dependencies
        from dependencies import get_mongo, get_postgres
        check("Dependency functions import successfully", True)
        check(
            "get_mongo and get_postgres are callable",
            callable(get_mongo) and callable(get_postgres),
        )

        # FastAPI app (requires fastapi installed)
        try:
            from fastapi import FastAPI
            from api import app
            check("FastAPI app imports successfully", True)

            check(
                "app is a FastAPI instance",
                isinstance(app, FastAPI),
                f"Got type: {type(app).__name__}",
            )
            check(
                'app.title = "Disaster Monitoring Query API"',
                app.title == "Disaster Monitoring Query API",
                f"Got: {app.title}",
            )

            # Collect all route paths
            route_paths = set()
            for route in app.routes:
                if hasattr(route, "path"):
                    route_paths.add(route.path)

            expected_routes = {
                "/events/recent", "/events/search", "/events/by-location",
                "/analytics/event-counts", "/analytics/monthly-trends",
                "/analytics/locations/top", "/health",
            }
            missing_routes = expected_routes - route_paths
            check(
                "All 7 expected route paths registered",
                len(missing_routes) == 0,
                f"Missing routes: {missing_routes}" if missing_routes else
                f"Found: {sorted(expected_routes)}",
            )

        except ImportError as exc:
            check("FastAPI app imports (requires fastapi installed)", False, str(exc))

    except ImportError as exc:
        check("Query API client imports successful", False, str(exc))
    finally:
        sys.path.pop(0)
        for mod in [
            "config", "models", "models.schemas",
            "mongo_client", "postgres_client", "dependencies",
            "routers", "routers.events", "routers.analytics", "api",
        ]:
            sys.modules.pop(mod, None)


# ── Check 17: Log files ───────────────────────────────────────────────────────

def check_log_files() -> None:
    """Verify log files are being created."""
    section("Logging -- Persistent Log Files")

    log_dir = os.path.join(PROJECT_ROOT, "logs")
    check(
        "logs/ directory exists",
        os.path.isdir(log_dir),
        f"Expected at: {log_dir}  -- run main.py or producer.py to create it",
    )

    for logfile in [
        "writer_service.log",
        "kafka_producer.log",
        "kafka_consumer.log",
        "mongodb_writer.log",
        "etl_service.log",
    ]:
        path   = os.path.join(log_dir, logfile)
        exists = os.path.isfile(path)
        size   = os.path.getsize(path) if exists else 0
        check(
            f"Log file exists and non-empty: {logfile}",
            exists and size > 0,
            f"Path: {path}  size: {size} bytes",
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "=" * 56)
    print("  Disaster Monitoring System")
    print("  Pipeline Health Check")
    print("=" * 56)

    datasets = check_data_files()

    if datasets:
        check_schema(datasets)
        check_distributions(datasets)
        check_timestamps(datasets)
    else:
        print("\n  [SKIPPED] Cannot check schema/distributions — no data loaded.")
        print("  Run: python main.py  to regenerate datasets.\n")

    check_producer_files()
    check_producer_imports()
    check_consumer_files()
    check_consumer_logic()
    check_writer_files()
    check_writer_config()
    check_writer_document_transform()
    check_etl_files()
    check_etl_imports()
    check_etl_logic()
    check_query_api_files()
    check_query_api_config()
    check_query_api_clients_and_routes()
    check_log_files()

    # ── Summary ───────────────────────────────────────────────────────────────
    total = _passed + _failed
    print(f"\n{'=' * 56}")
    print(f"  Results: {_passed} passed,  {_failed} failed  (out of {total} checks)")
    print("=" * 56)

    if _failed == 0:
        print("  All checks passed. Pipeline is healthy.\n")
    else:
        print(f"  {_failed} check(s) failed. See details above.\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
