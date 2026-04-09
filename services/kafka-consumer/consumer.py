"""
consumer.py
Kafka Consumer Processor Microservice — Module 3
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Subscribes to all four Kafka disaster topics, runs every message through
EventProcessor, and logs the enriched output.

Intentional constraints:
  - NO database writes  (Module 4 adds MongoDB)
  - NO re-publishing to Kafka
  - NO transformations beyond what processor.py defines

Two operating modes:
  --simulate   Read from local JSON files, no Kafka required.
               Use this to verify processing logic before Kafka is running.

  (default)    Connect to Kafka and process the live stream.

Usage:
    python consumer.py                           # live Kafka stream
    python consumer.py --simulate                # test without Kafka
    python consumer.py --speed FAST --simulate   # simulate, log every event
    python consumer.py --from-beginning          # re-process from offset 0
    python consumer.py --broker localhost:9093   # custom broker
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

from config import ConsumerConfig
from processor import EventProcessor, ProcessedEvent

# ── Logging (console + persistent file) ──────────────────────────────────────
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
    os.path.join(_LOG_DIR, "kafka_consumer.log"), encoding="utf-8"
)
_fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(_fh)

# Map event_type → source JSON filename (used only in simulate mode)
_FILE_MAP: dict[str, str] = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}


# ── Consumer ──────────────────────────────────────────────────────────────────

class DisasterEventConsumer:
    """
    Subscribes to Kafka topics and routes each message through EventProcessor.

    Consumer group design:
        group_id = "disaster-processor-group"
        - All instances sharing this group_id are co-ordinated by Kafka.
        - Kafka assigns each partition to exactly one member.
        - For our local setup (1 partition/topic, 1 instance), this consumer
          handles all four topics.
        - Module 4's MongoDB writer will run in the same group.

    Offset management:
        enable_auto_commit = False
        - We commit offsets manually after each message.
        - Prevents re-processing the same message on restart (at-least-once
          delivery is acceptable here; MongoDB upsert on event_id handles dupes).

    Lifecycle:
        consumer = DisasterEventConsumer(config, processor)
        consumer.connect()          # establishes Kafka connection
        consumer.start()            # blocks until Ctrl+C
        # close() called automatically in start()'s finally block
    """

    def __init__(self, config: ConsumerConfig, processor: EventProcessor) -> None:
        self.cfg       = config
        self.processor = processor
        self._log      = logging.getLogger(self.__class__.__name__)
        self._consumer = None   # KafkaConsumer; set by connect()

    # ── Public API ────────────────────────────────────────────────────────────

    def connect(self) -> None:
        """
        Establish Kafka broker connection and subscribe to topics.
        Exits with a clear message if the broker is unreachable.
        """
        try:
            from kafka import KafkaConsumer
            from kafka.errors import NoBrokersAvailable
        except ImportError:
            self._log.error(
                "kafka-python is not installed. Run: pip install -r requirements.txt"
            )
            sys.exit(1)

        self._log.info("Connecting to Kafka at %s ...", self.cfg.bootstrap_servers)
        try:
            self._consumer = KafkaConsumer(
                *self.cfg.topics,
                bootstrap_servers   = self.cfg.bootstrap_servers,
                group_id            = self.cfg.group_id,
                auto_offset_reset   = self.cfg.auto_offset_reset,
                enable_auto_commit  = self.cfg.enable_auto_commit,
                # Deserialize message value bytes → Python dict automatically
                value_deserializer  = lambda b: json.loads(b.decode("utf-8")),
                # Block indefinitely waiting for the next message
                consumer_timeout_ms = -1,
            )
            self._log.info(
                "Subscribed to %d topics | group=%s | offset_reset=%s",
                len(self.cfg.topics), self.cfg.group_id, self.cfg.auto_offset_reset,
            )
        except NoBrokersAvailable:
            self._log.error(
                "No Kafka brokers available at %s.\n"
                "  1. Is Kafka running? See README.md\n"
                "  2. Try --broker with a different address\n"
                "  3. Use --simulate to test without Kafka",
                self.cfg.bootstrap_servers,
            )
            sys.exit(1)

    def start(self) -> None:
        """
        Main consumption loop. Blocks until Ctrl+C.

        For each message from Kafka:
          1. Deserialize (handled by value_deserializer)
          2. Run through EventProcessor (validate → filter → classify → enrich)
          3. Log result (or note the drop reason)
          4. Commit offset (manual — we decide when we're done with a message)
        """
        if self._consumer is None:
            raise RuntimeError("Call connect() before start().")

        self._log.info(
            "Listening for events on %s ... (Ctrl+C to stop)", self.cfg.topics
        )
        n_consumed = 0

        try:
            for message in self._consumer:
                n_consumed += 1
                raw = message.value     # already a dict (deserialized above)

                try:
                    result = self.processor.process(raw)
                except Exception as exc:
                    self._log.error(
                        "Unexpected processor error | topic=%s offset=%d: %s",
                        message.topic, message.offset, exc,
                    )
                    result = None

                if result is not None:
                    self._log_processed(result)

                # Manual commit — acknowledge this message is done
                self._consumer.commit()

        except KeyboardInterrupt:
            self._log.info(
                "Consumer stopped by user. Messages consumed: %d", n_consumed
            )
        finally:
            self.processor.stats.print_report()
            self.close()

    def simulate(self) -> None:
        """
        Run the full processing pipeline against local JSON files.
        No Kafka connection required — great for testing Module 3 logic.

        Prints:
          - Sample input → output pairs for each severity level
          - A full ProcessingStats report at the end
        """
        self._log.info("[SIMULATE] Loading events from: %s", self.cfg.data_dir)

        all_events: list[dict] = []
        for event_type, filename in _FILE_MAP.items():
            path = os.path.join(self.cfg.data_dir, filename)
            if not os.path.isfile(path):
                self._log.warning("[SIMULATE] File not found: %s — skipping", path)
                continue
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
            all_events.extend(data)
            self._log.info("[SIMULATE] Loaded %4d events from %s", len(data), filename)

        # Process in timestamp order (matches live Kafka stream order)
        all_events.sort(key=lambda e: e.get("timestamp", ""))
        self._log.info("[SIMULATE] Total to process: %d events\n", len(all_events))

        # Collect one representative input→output pair per outcome category
        examples: dict[str, dict | None] = {
            "LOW": None, "MEDIUM": None, "HIGH": None,
            "CRITICAL": None, "DROPPED": None,
        }

        for raw in all_events:
            result = self.processor.process(raw)
            if result is not None:
                lvl = result.severity_level
                if examples[lvl] is None:
                    examples[lvl] = {"input": raw, "output": result.to_dict()}
            else:
                if examples["DROPPED"] is None:
                    examples["DROPPED"] = {"input": raw}

        # ── Print sample transformations ──────────────────────────────────────
        print("\n" + "=" * 68)
        print("  [SIMULATE] Sample transformations per outcome")
        print("=" * 68)

        for label, ex in examples.items():
            if ex is None:
                continue
            print(f"\n  [{label}]")
            print(f"  INPUT  : {json.dumps(ex['input'])}")
            if "output" in ex:
                print(f"  OUTPUT : {json.dumps(ex['output'])}")
            else:
                sev = ex["input"].get("severity_raw")
                etype = ex["input"].get("event_type", "?")
                print(f"  REASON : severity_raw={sev} below min_threshold for {etype}")

        self.processor.stats.print_report()

    def close(self) -> None:
        """Close the Kafka consumer and release resources."""
        if self._consumer is not None:
            self._consumer.close()
            self._log.info("Kafka consumer closed.")

    # ── Private helpers ───────────────────────────────────────────────────────

    def _log_processed(self, event: ProcessedEvent) -> None:
        """
        Logging strategy:
          DEBUG  → every event (captured in kafka_consumer.log)
          INFO   → every log_every_n-th event (console + file, progress view)
          WARNING→ every CRITICAL event (always visible regardless of N)
        """
        count = self.processor.stats.processed

        # Always write the full JSON to the log file (DEBUG level)
        self._log.debug("PROCESSED: %s", json.dumps(event.to_dict()))

        # Progress summary every N events
        if count % self.cfg.log_every_n == 0:
            self._log.info(
                "[%4d]  %-12s  %-8s  lat=%7.2f  lon=%8.2f  ts=%s",
                count,
                event.event_type,
                event.severity_level,
                event.latitude,
                event.longitude,
                event.event_time,
            )

        # CRITICAL events always visible in console
        if event.severity_level == "CRITICAL":
            self._log.warning(
                "CRITICAL  %-12s  id=%s  sev=%s  ts=%s  src=%s",
                event.event_type,
                event.event_id[:8],
                event.severity_level,
                event.event_time,
                event.source,
            )


# ── CLI ───────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="consumer.py",
        description=(
            "Kafka Consumer Processor — "
            "Disaster Event Stream Processing (Module 3)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python consumer.py                         Connect to Kafka, process stream
  python consumer.py --simulate              Test against local JSON files
  python consumer.py --from-beginning        Re-process from offset 0
  python consumer.py --log-every 1           Log every single event
  python consumer.py --broker localhost:9093 Custom broker port
  python consumer.py --simulate --log-every 1
        """,
    )
    parser.add_argument(
        "--broker",
        default="localhost:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap server (default: localhost:9092)",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Process local JSON files without connecting to Kafka",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help='Reset consumer group offset to "earliest" (re-process all messages)',
    )
    parser.add_argument(
        "--log-every",
        type=int,
        default=10,
        metavar="N",
        help="Log a summary line every N processed events (default: 10)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        metavar="PATH",
        help="Override path to data/ directory (simulate mode only)",
    )
    return parser


def main() -> None:
    args   = _build_parser().parse_args()
    config = ConsumerConfig(
        bootstrap_servers = [args.broker],
        log_every_n       = args.log_every,
    )
    if args.from_beginning:
        config.auto_offset_reset = "earliest"
    if args.data_dir:
        config.data_dir = os.path.abspath(args.data_dir)

    processor = EventProcessor()
    consumer  = DisasterEventConsumer(config, processor)

    print("\n" + "=" * 62)
    print("  Disaster Monitoring System -- Module 3")
    print("  Kafka Consumer Processor")
    print("=" * 62)
    print(f"  Broker      : {config.bootstrap_servers}")
    print(f"  Topics      : {config.topics}")
    print(f"  Group ID    : {config.group_id}")
    print(f"  Offset reset: {config.auto_offset_reset}")
    print(f"  Log every   : {config.log_every_n} events")
    print(f"  Simulate    : {args.simulate}")
    print("=" * 62 + "\n")

    if args.simulate:
        consumer.simulate()
    else:
        consumer.connect()
        consumer.start()


if __name__ == "__main__":
    main()
