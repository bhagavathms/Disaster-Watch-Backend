"""
producer.py
Kafka Producer Microservice — Module 2
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Responsibilities:
  1. Load disaster events from /data/*.json (produced by Module 1)
  2. Merge all event types into a single time-ordered stream
  3. Validate each record against the unified event schema
  4. Publish each event to its dedicated Kafka topic as a JSON message
  5. Simulate real-time streaming with configurable inter-message delays

Intentional constraints (enforced by design):
  - NO Kafka consumers
  - NO database connections
  - NO event transformations
  - NO cloud services

Usage:
    python producer.py                        # NORMAL speed (0.5 s delay)
    python producer.py --speed FAST           # 0.1 s delay
    python producer.py --speed SLOW           # 2.0 s delay
    python producer.py --dry-run              # validate data, no Kafka needed
    python producer.py --create-topics        # create topics then stream
    python producer.py --speed FAST --broker localhost:9093
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

from config import ProducerConfig, ReplaySpeed, NORMAL_CONFIG

# ── Logging ───────────────────────────────────────────────────────────────────
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
# Also write every log line to a persistent file
_fh = logging.FileHandler(os.path.join(_LOG_DIR, "kafka_producer.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(_fh)

# ── Schema constants ──────────────────────────────────────────────────────────
_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "event_id", "event_type", "severity_raw",
    "latitude", "longitude", "timestamp", "source",
})
_VALID_EVENT_TYPES: frozenset[str] = frozenset({
    "earthquake", "fire", "flood", "storm",
})

# Maps event_type → source JSON filename (mirrors Module 1 output)
_FILE_MAP: dict[str, str] = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}


# ── Producer ──────────────────────────────────────────────────────────────────

class DisasterEventProducer:
    """
    Streams disaster events from local JSON files to Kafka topics.

    Lifecycle:
        producer = DisasterEventProducer(config)

        # Option A — real Kafka
        producer.connect()
        producer.stream()

        # Option B — no Kafka required
        producer.dry_run()

        # Always call in a finally block:
        producer.close()

    The `stream()` method:
      - Loads all four JSON files from config.data_dir
      - Merges them into a single list sorted by timestamp (ascending)
      - Publishes each event to its topic with config.replay_speed delay
      - Logs progress every 25 events and on completion
    """

    def __init__(self, config: ProducerConfig = NORMAL_CONFIG) -> None:
        self.cfg  = config
        self._log = logging.getLogger(self.__class__.__name__)
        self._kafka = None          # KafkaProducer; set by connect()

    # ── Public API ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Establish connection to the Kafka broker.
        Exits with a clear diagnostic if the broker is unreachable.
        """
        try:
            from kafka import KafkaProducer
            from kafka.errors import NoBrokersAvailable
        except ImportError:
            self._log.error(
                "kafka-python is not installed. Run: pip install kafka-python"
            )
            sys.exit(1)

        self._log.info("Connecting to Kafka at %s ...", self.cfg.bootstrap_servers)
        try:
            self._kafka = KafkaProducer(
                bootstrap_servers=self.cfg.bootstrap_servers,
                # Serialize message value as UTF-8 JSON bytes
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                # Use event_id as the partition key (consistent routing)
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=self.cfg.acks,
                retries=self.cfg.retries,
                request_timeout_ms=self.cfg.request_timeout_ms,
            )
            self._log.info("Kafka connection established.")
        except NoBrokersAvailable:
            self._log.error(
                "No Kafka brokers available at %s.\n"
                "  1. Is Kafka running?  See README.md -> 'Starting Kafka Locally'\n"
                "  2. Check the broker address with --broker flag.\n"
                "  3. To test without Kafka, use --dry-run.",
                self.cfg.bootstrap_servers,
            )
            sys.exit(1)

    def create_topics(self) -> None:
        """
        Create the four disaster topics on the broker if they do not exist.
        Safe to call on an already-configured cluster (idempotent).
        """
        try:
            from kafka.admin import KafkaAdminClient, NewTopic
            from kafka.errors import TopicAlreadyExistsError
        except ImportError:
            self._log.error("kafka-python is not installed.")
            sys.exit(1)

        self._log.info("Checking / creating Kafka topics ...")
        admin = KafkaAdminClient(bootstrap_servers=self.cfg.bootstrap_servers)
        try:
            existing = set(admin.list_topics())
            to_create = [
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
                for topic in self.cfg.topic_map.values()
                if topic not in existing
            ]
            if to_create:
                admin.create_topics(to_create)
                self._log.info(
                    "Topics created: %s", [t.name for t in to_create]
                )
            else:
                self._log.info("All topics already exist — no action needed.")
        finally:
            admin.close()

    def stream(self) -> None:
        """
        Main streaming loop.

        Loads, sorts, and publishes all events in timestamp order with
        per-message delay defined by config.replay_speed.
        Handles KeyboardInterrupt gracefully (flushes buffer before exit).
        """
        if self._kafka is None:
            raise RuntimeError("Call connect() before stream().")

        events = self._load_all_events()
        if not events:
            self._log.warning(
                "No valid events found in '%s'. Nothing to stream.", self.cfg.data_dir
            )
            return

        delay = self.cfg.replay_speed.value
        total = len(events)

        self._log.info(
            "Starting stream: %d events | speed=%s (%.1fs delay) | topics=%s",
            total,
            self.cfg.replay_speed.name,
            delay,
            list(self.cfg.topic_map.values()),
        )

        sent = 0
        try:
            for event in events:
                topic = self.cfg.topic_map[event["event_type"]]
                self._publish(topic, event)
                sent += 1
                self._log_progress(sent, total, event)
                time.sleep(delay)

        except KeyboardInterrupt:
            self._log.warning(
                "Stream interrupted after %d / %d events.", sent, total
            )
        finally:
            # Flush ensures all in-flight messages reach the broker
            # before we close the connection.
            self._kafka.flush()
            self._log.info("Kafka buffer flushed.")

        if sent == total:
            self._log.info("Stream complete. All %d events published.", total)

    def dry_run(self) -> None:
        """
        Validate and print events without connecting to Kafka.
        No broker required — useful for testing data integrity.
        Shows the first 5 messages per topic in the exact format they
        would be sent to Kafka.
        """
        events = self._load_all_events()
        total  = len(events)
        self._log.info("[DRY RUN] %d valid events loaded. No Kafka connection.", total)
        print()

        shown: dict[str, int] = {}
        for event in events:
            etype = event["event_type"]
            shown[etype] = shown.get(etype, 0) + 1
            if shown[etype] <= 5:
                topic = self.cfg.topic_map[etype]
                key   = event["event_id"]
                value = json.dumps(event, indent=None)
                print(
                    f"  TOPIC={topic:<22}  "
                    f"KEY={key[:8]}...  "
                    f"VALUE={value}"
                )

        print()
        print(f"  [DRY RUN] Summary — total events that would be published: {total}")
        print()
        header = f"  {'EVENT TYPE':<14} {'TOTAL':>7}  {'SHOWN':>6}  {'TOPIC'}"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for etype in _VALID_EVENT_TYPES:
            cnt   = shown.get(etype, 0)
            topic = self.cfg.topic_map.get(etype, "N/A")
            print(f"  {etype:<14} {cnt:>7}  {min(5, cnt):>6}  {topic}")
        print()

    def close(self) -> None:
        """Close the Kafka producer connection cleanly."""
        if self._kafka is not None:
            self._kafka.close()
            self._log.info("Kafka producer closed.")

    # ── Private: data loading ─────────────────────────────────────────────────

    def _load_all_events(self) -> list[dict]:
        """
        Read all four JSON dataset files, validate each record, merge into
        a single list, and sort by ISO-8601 timestamp ascending.

        Skips:
          - Files that do not exist (with a warning)
          - Records missing required fields
          - Records with an unknown event_type

        Returns a single sorted list ready for publishing.
        """
        all_events: list[dict] = []

        for event_type, filename in _FILE_MAP.items():
            path = os.path.join(self.cfg.data_dir, filename)

            if not os.path.isfile(path):
                self._log.warning(
                    "Dataset file not found, skipping: %s\n"
                    "  Run Module 1 (main.py at project root) to generate data.",
                    path,
                )
                continue

            with open(path, encoding="utf-8") as fh:
                try:
                    raw: list[dict] = json.load(fh)
                except json.JSONDecodeError as exc:
                    self._log.error("Failed to parse %s: %s", path, exc)
                    continue

            valid, skipped = 0, 0
            for record in raw:
                if self._is_valid(record):
                    all_events.append(record)
                    valid += 1
                else:
                    skipped += 1
                    self._log.debug("Skipped invalid record in %s: %s", filename, record)

            self._log.info(
                "Loaded %-20s  valid=%d  skipped=%d",
                filename, valid, skipped,
            )
        # ISO-8601 strings are lexicographically sortable — no datetime parsing needed.
        all_events.sort(key=lambda e: e["timestamp"])
        self._log.info(
            "Merged and sorted %d events by timestamp. "
            "Range: %s  ->  %s",
            len(all_events),
            all_events[0]["timestamp"] if all_events else "N/A",
            all_events[-1]["timestamp"] if all_events else "N/A",
        )
        return all_events

    # ── Private: publishing ───────────────────────────────────────────────────

    def _publish(self, topic: str, event: dict) -> None:
        """
        Send one event to a Kafka topic.

        Message anatomy:
          topic  : disaster-type-specific topic (e.g. "earthquakes-topic")
          key    : event_id (UUID string)
                   → guarantees all events with the same ID go to the
                     same partition (important for exactly-once semantics later)
          value  : full event dict serialized as UTF-8 JSON

        The send() call is non-blocking; the buffer is flushed at stream end.
        """
        self._kafka.send(
            topic,
            key=event["event_id"],
            value=event,
        )

    def _log_progress(self, sent: int, total: int, event: dict) -> None:
        """Log a progress line every 25 messages and at the very end."""
        if sent % 25 == 0 or sent == total:
            pct = sent / total * 100
            self._log.info(
                "[%4d / %4d  %5.1f%%]  type=%-12s  sev=%-8.2f  ts=%s",
                sent, total, pct,
                event["event_type"],
                event["severity_raw"],
                event["timestamp"],
            )

    # ── Private: validation ───────────────────────────────────────────────────

    @staticmethod
    def _is_valid(record: dict) -> bool:
        """
        Lightweight pre-publish schema check.
        Returns False (never raises) so callers can silently skip bad records.

        Checks:
          - All required fields are present
          - event_type is one of the four valid values
          - latitude in [-90, 90], longitude in [-180, 180]

        Note: severity_raw == 0.0 is intentional noise; it passes here
        and is handled by the downstream consumer/transformer (Module 3).
        """
        if not _REQUIRED_FIELDS.issubset(record.keys()):
            return False
        if record.get("event_type") not in _VALID_EVENT_TYPES:
            return False
        lat = record.get("latitude", 999)
        lon = record.get("longitude", 999)
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            return False
        return True


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="producer.py",
        description="Kafka Producer — Disaster Event Streaming Simulator (Module 2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python producer.py                          Normal speed (0.5s delay)
  python producer.py --speed FAST             Fast speed   (0.1s delay)
  python producer.py --speed SLOW             Slow speed   (2.0s delay)
  python producer.py --dry-run                Validate data, no Kafka needed
  python producer.py --create-topics          Auto-create topics then stream
  python producer.py --speed FAST --dry-run   Fast dry-run
  python producer.py --broker localhost:9093  Custom broker port
  python producer.py --data-dir /path/to/data Override data directory
        """,
    )
    parser.add_argument(
        "--speed",
        choices=["FAST", "NORMAL", "SLOW"],
        default="NORMAL",
        metavar="SPEED",
        help="Replay speed: FAST=0.1s | NORMAL=0.5s | SLOW=2.0s  (default: NORMAL)",
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap server address (default: localhost:9092)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        metavar="PATH",
        help="Override path to the data/ directory (default: auto-resolved from project root)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and print messages without connecting to Kafka",
    )
    parser.add_argument(
        "--create-topics",
        action="store_true",
        help="Create Kafka topics before streaming (safe to run on existing topics)",
    )
    return parser


def main() -> None:
    args   = _build_parser().parse_args()
    config = ProducerConfig(
        bootstrap_servers=[args.broker],
        replay_speed=ReplaySpeed[args.speed],
    )
    if args.data_dir:
        config.data_dir = os.path.abspath(args.data_dir)

    # ── Banner ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("  Disaster Monitoring System -- Module 2")
    print("  Kafka Producer Service")
    print("=" * 62)
    print(f"  Broker        : {config.bootstrap_servers}")
    print(f"  Replay speed  : {config.replay_speed.name}  "
          f"({config.replay_speed.value}s delay)")
    print(f"  Data dir      : {config.data_dir}")
    print(f"  Dry run       : {args.dry_run}")
    print(f"  Create topics : {args.create_topics}")
    print("=" * 62 + "\n")

    producer = DisasterEventProducer(config)
    try:
        if args.dry_run:
            # No broker connection needed
            producer.dry_run()
        else:
            producer.connect()
            if args.create_topics:
                producer.create_topics()
            producer.stream()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
