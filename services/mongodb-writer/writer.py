"""
writer.py
MongoDB Writer Microservice — Module 4
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Responsibilities:
  1. Subscribe to raw Kafka topics (own consumer group: disaster-storage-group)
  2. Process each event using EventProcessor (imported from Module 3)
  3. Accumulate processed events into batches
  4. Flush batches to MongoDB via MongoDBClient (db.py)
  5. Commit Kafka offsets only AFTER a successful MongoDB write

Consumer group independence:
  This service uses group_id = "disaster-storage-group", separate from
  Module 3's "disaster-processor-group". Kafka delivers all events to both
  groups independently — there is no shared state between the two services.

Two operating modes:
  --simulate    Read from local JSON files. No Kafka needed. Still writes to MongoDB
                unless --dry-run is also set.
  (default)     Connect to Kafka, consume the live stream.

Flags:
  --dry-run     Convert events to MongoDB document format and print them.
                No MongoDB connection required. Combine with --simulate to
                test the full pipeline without any external services.

Usage:
    python writer.py --simulate --dry-run      # zero dependencies, full trace
    python writer.py --simulate                # needs MongoDB, no Kafka
    python writer.py                           # needs Kafka + MongoDB
    python writer.py --from-beginning          # re-process from Kafka offset 0
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

# ── Import EventProcessor from Module 3 ──────────────────────────────────────
# Resolves to services/kafka-consumer/ relative to this file's location.
# This avoids duplicating processing logic and ensures schema consistency.
#
# Import order matters here:
#   1. Temporarily add kafka-consumer dir to the front of sys.path
#   2. Import processor (which also loads kafka-consumer's config internally)
#   3. Remove kafka-consumer dir from sys.path and clear stale module cache
#   4. Import local config and db (mongodb-writer's own modules)
#
# This prevents kafka-consumer/config.py from shadowing mongodb-writer/config.py.
_CONSUMER_DIR = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "kafka-consumer")
)
sys.path.insert(0, _CONSUMER_DIR)

try:
    from processor import EventProcessor, ProcessedEvent
except ImportError as _e:
    print(
        f"[ERROR] Cannot import processor.py from {_CONSUMER_DIR}.\n"
        f"        Make sure services/kafka-consumer/processor.py exists.\n"
        f"        Original error: {_e}"
    )
    sys.exit(1)
finally:
    # Remove consumer dir and its cached modules so local config/db take priority
    if _CONSUMER_DIR in sys.path:
        sys.path.remove(_CONSUMER_DIR)
    for _mod in ["config", "processor", "consumer"]:
        sys.modules.pop(_mod, None)

from config import WriterConfig, MongoConfig, KafkaConfig
from db import MongoDBClient

# ── Logging (console + file) ──────────────────────────────────────────────────
_LOG_FORMAT = "%(asctime)s  [%(levelname)s]  %(name)s: %(message)s"
_LOG_DIR    = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "logs")
)
os.makedirs(_LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=_LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)
_fh = logging.FileHandler(
    os.path.join(_LOG_DIR, "mongodb_writer.log"), encoding="utf-8"
)
_fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(_fh)

# Source file map (used in simulate mode)
_FILE_MAP: dict[str, str] = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}


# ── Writer service ────────────────────────────────────────────────────────────

class DisasterEventWriter:
    """
    End-to-end pipeline: raw event → processed event → MongoDB document.

    Batch strategy:
        Events are accumulated in an in-memory buffer.
        When the buffer reaches cfg.mongo.batch_size, a bulk_write() is issued.
        On shutdown (KeyboardInterrupt or end of simulate), any remaining
        buffered events are flushed before closing.

        Kafka offsets are committed only AFTER a successful flush() — so no
        event is considered "done" until it is safely in MongoDB.

    Duplicate handling:
        Every write is an upsert keyed by event_id (= MongoDB _id).
        Restarting from --from-beginning is safe: existing documents are
        overwritten in place (same data, updated processed_at), not duplicated.
    """

    def __init__(self, config: WriterConfig) -> None:
        self.cfg       = config
        self._log      = logging.getLogger(self.__class__.__name__)
        self._mongo    = MongoDBClient(config.mongo)
        self._processor = EventProcessor()
        self._consumer = None       # KafkaConsumer; set by connect()
        self._buffer:  list[dict[str, Any]] = []
        self._stats    = {"received": 0, "stored": 0, "dropped": 0, "errors": 0}

    # ── Public API ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Connect to both MongoDB and Kafka.
        MongoDB is connected first — if it fails, there's no point starting Kafka.
        """
        # MongoDB first
        try:
            self._mongo.connect()
            self._mongo.create_indexes()
        except ConnectionError as exc:
            self._log.error("%s", exc)
            sys.exit(1)

        # Kafka second
        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable
        except ImportError:
            self._log.error(
                "kafka-python not installed. Run: pip install -r requirements.txt"
            )
            sys.exit(1)

        self._log.info(
            "Connecting to Kafka at %s ...", self.cfg.kafka.bootstrap_servers
        )
        try:
            self._consumer = KafkaConsumer(
                *self.cfg.kafka.topics,
                bootstrap_servers   = self.cfg.kafka.bootstrap_servers,
                group_id            = self.cfg.kafka.group_id,
                auto_offset_reset   = self.cfg.kafka.auto_offset_reset,
                enable_auto_commit  = self.cfg.kafka.enable_auto_commit,
                value_deserializer  = lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms = -1,
            )
            self._log.info(
                "Subscribed to %d topics | group=%s",
                len(self.cfg.kafka.topics), self.cfg.kafka.group_id,
            )
        except NoBrokersAvailable:
            self._log.error(
                "No Kafka brokers at %s. "
                "Use --simulate to run without Kafka.",
                self.cfg.kafka.bootstrap_servers,
            )
            sys.exit(1)

    def start(self) -> None:
        """
        Main streaming loop. Blocks until Ctrl+C.

        Flow per message:
          1. Deserialize (handled by value_deserializer)
          2. Process through EventProcessor (validate → filter → classify → enrich)
          3. If passed: append to buffer
          4. If buffer full: flush to MongoDB + commit Kafka offset
          5. If dropped: count, continue
        """
        if self._consumer is None:
            raise RuntimeError("Call connect() before start().")

        self._log.info(
            "Listening on %s ... (Ctrl+C to stop)", self.cfg.kafka.topics
        )
        try:
            for message in self._consumer:
                self._stats["received"] += 1
                raw    = message.value
                result = self._processor.process(raw)

                if result is not None:
                    self._buffer.append(result.to_dict())
                    if len(self._buffer) >= self.cfg.mongo.batch_size:
                        self._flush_and_commit()
                else:
                    self._stats["dropped"] += 1

        except KeyboardInterrupt:
            self._log.info("Writer stopped by user.")
        finally:
            self._flush_and_commit()   # drain remaining buffer
            self._print_stats()
            self.close()

    def simulate(self) -> None:
        """
        Run the pipeline against local JSON files.
        If --dry-run: print documents only, no MongoDB writes.
        Otherwise:    connect to MongoDB and write.

        This mode does NOT need Kafka.
        """
        if not self.cfg.dry_run:
            try:
                self._mongo.connect()
                self._mongo.create_indexes()
            except ConnectionError as exc:
                self._log.error("%s", exc)
                sys.exit(1)

        self._log.info(
            "[SIMULATE]  dry_run=%s  data_dir=%s",
            self.cfg.dry_run, self.cfg.data_dir,
        )

        all_events: list[dict] = []
        for event_type, filename in _FILE_MAP.items():
            path = os.path.join(self.cfg.data_dir, filename)
            if not os.path.isfile(path):
                self._log.warning("[SIMULATE] Not found: %s — skipping", path)
                continue
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            all_events.extend(data)
            self._log.info("[SIMULATE] Loaded %4d events from %s", len(data), filename)

        all_events.sort(key=lambda e: e.get("timestamp", ""))
        self._log.info("[SIMULATE] Total: %d events to process\n", len(all_events))

        # Track examples per severity level for the summary
        examples: dict[str, dict | None] = {
            "LOW": None, "MEDIUM": None, "HIGH": None, "CRITICAL": None
        }

        for raw in all_events:
            self._stats["received"] += 1
            result = self._processor.process(raw)

            if result is None:
                self._stats["dropped"] += 1
                continue

            event_dict = result.to_dict()
            doc        = MongoDBClient._to_document(event_dict)

            lvl = result.severity_level
            if examples[lvl] is None:
                examples[lvl] = {"processed": event_dict, "document": doc}

            if self.cfg.dry_run:
                self._stats["stored"] += 1
            else:
                self._buffer.append(event_dict)
                if len(self._buffer) >= self.cfg.mongo.batch_size:
                    self._flush_buffer()

        # Drain remaining buffer
        if not self.cfg.dry_run:
            self._flush_buffer()

        # ── Print sample documents ─────────────────────────────────────────
        self._print_example_documents(examples)
        self._print_stats()

        if not self.cfg.dry_run:
            self._print_collection_stats()

    def close(self) -> None:
        """Close Kafka consumer and MongoDB connection."""
        if self._consumer is not None:
            self._consumer.close()
            self._log.info("Kafka consumer closed.")
        self._mongo.close()

    # ── Private helpers ───────────────────────────────────────────────────────

    def _flush_and_commit(self) -> None:
        """Flush buffer to MongoDB, then commit Kafka offsets."""
        if not self._buffer:
            return
        self._flush_buffer()
        if self._consumer is not None:
            self._consumer.commit()     # ack only AFTER successful MongoDB write

    def _flush_buffer(self) -> None:
        """Write the current buffer to MongoDB and reset it."""
        if not self._buffer:
            return
        n = len(self._buffer)
        try:
            summary = self._mongo.upsert_many(self._buffer)
            self._stats["stored"]  += summary["inserted"] + summary["updated"]
            self._stats["errors"]  += summary["errors"]
            self._log.info(
                "Flushed batch of %d → MongoDB  "
                "(inserted=%d  updated=%d  errors=%d)",
                n, summary["inserted"], summary["updated"], summary["errors"],
            )
        except Exception as exc:
            self._log.error("MongoDB batch write failed: %s", exc)
            self._stats["errors"] += n
        finally:
            self._buffer.clear()

    def _print_example_documents(
        self, examples: dict[str, dict | None]
    ) -> None:
        """Print one representative document per severity level."""
        print("\n" + "=" * 70)
        print("  [MODULE 4] Sample MongoDB Documents per Severity Level")
        print("=" * 70)

        for level, ex in examples.items():
            if ex is None:
                continue

            raw_event = ex["processed"]
            mongo_doc = ex["document"]

            print(f"\n  -- {level} --")
            print(f"  Processed event (Module 3 output):")
            print(f"    {json.dumps(raw_event)}")
            print(f"  MongoDB document (_id=event_id, GeoJSON location, datetime):")
            # Render datetime fields as strings for display
            display_doc = {
                k: (v.isoformat() if hasattr(v, "isoformat") else v)
                for k, v in mongo_doc.items()
            }
            print(f"    {json.dumps(display_doc, indent=None)}")

    def _print_stats(self) -> None:
        s   = self._stats
        sep = "-" * 56
        print(f"\n{sep}")
        print("  MODULE 4 -- STORAGE REPORT")
        print(sep)
        print(f"  Events received from stream : {s['received']}")
        print(f"  Events dropped (low sev)   : {s['dropped']}")
        print(f"  Events written to MongoDB  : {s['stored']}")
        print(f"  Write errors               : {s['errors']}")
        if s["received"] > 0:
            pct = s["stored"] / s["received"] * 100
            print(f"  Storage rate               : {pct:.1f}%")
        print(sep + "\n")

    def _print_collection_stats(self) -> None:
        """Query MongoDB and print current collection state."""
        try:
            stats = self._mongo.collection_stats()
            print(f"  MongoDB collection state after write:")
            print(f"    Total documents: {stats['total']}")
            for row in stats["by_type"]:
                print(
                    f"    {row['_id']:<12}  count={row['count']:>4}  "
                    f"levels={sorted(row['levels'])}"
                )
            print()
        except Exception as exc:
            self._log.warning("Could not retrieve collection stats: %s", exc)


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="writer.py",
        description=(
            "MongoDB Writer Microservice — "
            "Disaster Event Durable Storage (Module 4)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python writer.py --simulate --dry-run     Full trace, no external services
  python writer.py --simulate               JSON -> MongoDB (no Kafka)
  python writer.py                          Kafka -> MongoDB (live stream)
  python writer.py --from-beginning         Re-process from Kafka offset 0
  python writer.py --broker localhost:9093  Custom Kafka broker
  python writer.py --mongo-uri "mongodb://localhost:28020/"
        """,
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Read from local JSON files instead of Kafka",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print MongoDB documents without writing (no MongoDB needed)",
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap server (default: localhost:9092)",
    )
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:28020/",
        metavar="URI",
        help="MongoDB connection URI (default: mongodb://localhost:28020/)",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help='Reset Kafka consumer group to "earliest" offset',
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="MongoDB bulk-write batch size (default: 50)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        metavar="PATH",
        help="Override data/ directory (simulate mode only)",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()

    mongo_cfg = MongoConfig(
        uri        = args.mongo_uri,
        batch_size = args.batch_size,
    )
    kafka_cfg = KafkaConfig(
        bootstrap_servers = [args.broker],
    )
    if args.from_beginning:
        kafka_cfg.auto_offset_reset = "earliest"

    config = WriterConfig(
        mongo   = mongo_cfg,
        kafka   = kafka_cfg,
        dry_run = args.dry_run,
    )
    if args.data_dir:
        config.data_dir = os.path.abspath(args.data_dir)

    writer = DisasterEventWriter(config)

    print("\n" + "=" * 62)
    print("  Disaster Monitoring System -- Module 4")
    print("  MongoDB Writer Service")
    print("=" * 62)
    print(f"  MongoDB URI    : {config.mongo.uri}")
    print(f"  Database       : {config.mongo.database}")
    print(f"  Collection     : {config.mongo.collection}")
    print(f"  Kafka broker   : {config.kafka.bootstrap_servers}")
    print(f"  Consumer group : {config.kafka.group_id}")
    print(f"  Batch size     : {config.mongo.batch_size}")
    print(f"  Simulate       : {args.simulate}")
    print(f"  Dry run        : {args.dry_run}")
    print("=" * 62 + "\n")

    try:
        if args.simulate:
            writer.simulate()
        else:
            writer.connect()
            writer.start()
    finally:
        writer.close()


if __name__ == "__main__":
    main()
