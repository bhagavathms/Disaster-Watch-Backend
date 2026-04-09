"""
writer_service.py — Task C
Dataset Writer Microservice for Step 1 of the
Distributed NoSQL-Based Disaster Monitoring and Analytics System.

Responsibilities:
  - Accept segmented synthetic datasets ({event_type: [events]})
  - Validate every record against the unified event schema
  - Write each dataset to its own JSON file under `output_dir/`

Output layout:
  data/
    earthquakes.json
    fires.json
    floods.json
    storms.json

Intentionally minimal: no Kafka, no database, no HTTP server.
This is the terminal I/O sink for Step 1; later pipeline stages will
read these files or replace this service with a Kafka producer.
"""

import json
import logging
from pathlib import Path
from typing import Any

# ── Logging ───────────────────────────────────────────────────────────────────
_LOG_FORMAT = "%(asctime)s  [%(levelname)s]  %(name)s: %(message)s"
_LOG_DIR    = Path(__file__).parent / "logs"
_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format=_LOG_FORMAT,
    datefmt="%Y-%m-%d %H:%M:%S",
)
# Also write every log line to a persistent file
_fh = logging.FileHandler(_LOG_DIR / "writer_service.log", encoding="utf-8")
_fh.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt="%Y-%m-%d %H:%M:%S"))
logging.getLogger().addHandler(_fh)

# ── Schema constants (single source of truth for this service) ────────────────
_REQUIRED_FIELDS: dict[str, type | tuple] = {
    "event_id":    str,
    "event_type":  str,
    "severity_raw": (int, float),
    "latitude":    (int, float),
    "longitude":   (int, float),
    "timestamp":   str,
    "source":      str,
}

_VALID_EVENT_TYPES = {"earthquake", "fire", "flood", "storm"}

# Maps logical event_type → output filename
_FILE_MAP: dict[str, str] = {
    "earthquake": "earthquakes.json",
    "fire":       "fires.json",
    "flood":      "floods.json",
    "storm":      "storms.json",
}


# ── Service class ─────────────────────────────────────────────────────────────

class DatasetWriterService:
    """
    Validates and persists one JSON array per disaster type.

    Usage:
        writer = DatasetWriterService(output_dir="data")
        writer.print_summary(datasets)
        paths = writer.write_all(datasets)

    `datasets` is the dict returned by generate_all_events():
        { "earthquake": [...], "fire": [...], "flood": [...], "storm": [...] }
    """

    def __init__(self, output_dir: str = "data") -> None:
        self.output_dir = Path(output_dir)
        self._log = logging.getLogger(self.__class__.__name__)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._log.info("Output directory ready: %s", self.output_dir.resolve())

    # ── Public API ────────────────────────────────────────────────────────────

    def write_dataset(
        self,
        event_type: str,
        events: list[dict[str, Any]],
    ) -> Path:
        """
        Validate and write a single event-type dataset to its JSON file.

        Args:
            event_type: One of "earthquake" | "fire" | "flood" | "storm".
            events:     List of event dicts to write.

        Returns:
            Path to the written file.

        Raises:
            ValueError: Unknown event_type or schema violation.
            TypeError:  Field has wrong Python type.
        """
        if event_type not in _FILE_MAP:
            raise ValueError(
                f"Unknown event_type '{event_type}'. "
                f"Must be one of: {set(_FILE_MAP)}"
            )

        validated = [self._validate(e) for e in events]
        out_path  = self.output_dir / _FILE_MAP[event_type]

        with out_path.open("w", encoding="utf-8") as fh:
            json.dump(validated, fh, indent=2, ensure_ascii=False)

        self._log.info(
            "%-12s -> %-20s  (%d events written)",
            event_type, out_path.name, len(validated),
        )
        return out_path

    def write_all(
        self,
        datasets: dict[str, list[dict[str, Any]]],
    ) -> dict[str, Path]:
        """
        Write all segmented datasets in one call.

        Args:
            datasets: { event_type: [event, ...] }

        Returns:
            { event_type: Path } mapping each type to its output file.
        """
        results: dict[str, Path] = {}
        for event_type, events in datasets.items():
            results[event_type] = self.write_dataset(event_type, events)

        total = sum(len(v) for v in datasets.values())
        self._log.info("All datasets written — total events across all types: %d", total)
        return results

    def print_summary(self, datasets: dict[str, list[dict[str, Any]]]) -> None:
        """
        Print a concise statistical table to stdout before writing.
        Useful for a quick sanity check on the generated distributions.
        """
        sep = "-" * 64
        print(f"\n{sep}")
        print(f"  {'EVENT TYPE':<14} {'COUNT':>6}  {'MIN sev':>10}  {'MAX sev':>10}  {'MEAN sev':>10}")
        print(sep)
        grand_total = 0
        for etype, events in datasets.items():
            sevs = [e["severity_raw"] for e in events]
            nulls = sum(1 for s in sevs if s == 0.0)
            print(
                f"  {etype:<14} {len(events):>6}  "
                f"{min(sevs):>10.2f}  {max(sevs):>10.2f}  "
                f"{sum(sevs) / len(sevs):>10.2f}"
                f"  ({nulls} nulls)"
            )
            grand_total += len(events)
        print(sep)
        print(f"  {'TOTAL':<14} {grand_total:>6}")
        print(sep + "\n")

    # ── Private ───────────────────────────────────────────────────────────────

    @staticmethod
    def _validate(event: dict[str, Any]) -> dict[str, Any]:
        """
        Lightweight schema validation. Checks:
          - All required fields are present
          - Each field has the correct Python type
          - event_type is one of the four valid values
          - latitude in [-90, 90], longitude in [-180, 180]

        Does NOT reject 0.0 severity — that is intentional noise for
        downstream filtering to handle.

        Raises ValueError / TypeError on invalid records.
        """
        eid = event.get("event_id", "<unknown>")

        for field, expected_type in _REQUIRED_FIELDS.items():
            if field not in event:
                raise ValueError(f"[{eid}] Missing required field: '{field}'")
            if not isinstance(event[field], expected_type):
                raise TypeError(
                    f"[{eid}] Field '{field}' expected {expected_type}, "
                    f"got {type(event[field]).__name__} = {event[field]!r}"
                )

        if event["event_type"] not in _VALID_EVENT_TYPES:
            raise ValueError(
                f"[{eid}] Invalid event_type: '{event['event_type']}'. "
                f"Must be one of {_VALID_EVENT_TYPES}"
            )

        lat, lon = event["latitude"], event["longitude"]
        if not (-90.0 <= lat <= 90.0):
            raise ValueError(f"[{eid}] latitude out of range: {lat}")
        if not (-180.0 <= lon <= 180.0):
            raise ValueError(f"[{eid}] longitude out of range: {lon}")

        return event
