"""
processor.py
Stream Processing Logic — Module 3
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Processing pipeline (one message at a time):

    raw dict (from Kafka)
        │
        ▼
    [1] _is_valid()              — schema gate: all fields present, types correct
        │ fails → return None (drop, count as dropped_invalid)
        ▼
    [2] _classify_severity()     — per-type threshold lookup
        │ None → return None (drop, count as dropped_below_threshold)
        ▼
    [3] Build ProcessedEvent     — rename fields, add processed_at, attach level
        │
        ▼
    ProcessedEvent               — ready for Module 4 MongoDB storage

Schema transformation:
    Input  (raw Kafka message): event_id, event_type, severity_raw,
                                latitude, longitude, timestamp, source
    Output (ProcessedEvent)   : event_id, event_type, severity_level,
                                latitude, longitude, event_time,
                                processed_at, source

    Changes:
      severity_raw  → removed  (replaced by severity_level)
      timestamp     → renamed to event_time
      processed_at  → NEW field (UTC ISO-8601, this service's processing time)
      severity_level→ NEW field (LOW | MEDIUM | HIGH | CRITICAL)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from config import SEVERITY_THRESHOLDS

# ── Schema constants ──────────────────────────────────────────────────────────
_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "event_id", "event_type", "severity_raw",
    "latitude", "longitude", "timestamp", "source",
})
_VALID_EVENT_TYPES: frozenset[str] = frozenset({
    "earthquake", "fire", "flood", "storm",
})


# ── Output schema ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ProcessedEvent:
    """
    Immutable, schema-validated processed event.

    This is the contract between Module 3 (this service) and
    Module 4 (MongoDB storage). The fields below are exactly what
    will be written to the MongoDB collection.

    `frozen=True` prevents accidental mutation after creation.
    """
    event_id:       str
    event_type:     str
    severity_level: str   # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    latitude:       float
    longitude:      float
    event_time:     str   # original event timestamp (renamed from "timestamp")
    processed_at:   str   # UTC time this service processed the event
    source:         str

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict (for JSON logging and MongoDB insertion)."""
        return {
            "event_id":       self.event_id,
            "event_type":     self.event_type,
            "severity_level": self.severity_level,
            "latitude":       self.latitude,
            "longitude":      self.longitude,
            "event_time":     self.event_time,
            "processed_at":   self.processed_at,
            "source":         self.source,
        }


# ── Processing statistics ─────────────────────────────────────────────────────

@dataclass
class ProcessingStats:
    """Running counters updated by EventProcessor.process()."""
    received:                int = 0
    processed:               int = 0
    dropped_invalid:         int = 0
    dropped_below_threshold: int = 0

    # Fine-grained breakdowns (populated during processing)
    by_type:  dict[str, int] = field(default_factory=dict)
    by_level: dict[str, int] = field(default_factory=dict)

    @property
    def total_dropped(self) -> int:
        return self.dropped_invalid + self.dropped_below_threshold

    @property
    def drop_rate(self) -> float:
        return self.total_dropped / self.received * 100 if self.received > 0 else 0.0

    @property
    def pass_rate(self) -> float:
        return self.processed / self.received * 100 if self.received > 0 else 0.0

    def print_report(self) -> None:
        """Print a summary table to stdout."""
        sep = "-" * 58
        print(f"\n{sep}")
        print("  PROCESSING REPORT")
        print(sep)
        print(f"  Received                : {self.received}")
        print(f"  Processed (passed)      : {self.processed}  ({self.pass_rate:.1f}%)")
        print(f"  Dropped — invalid       : {self.dropped_invalid}")
        print(f"  Dropped — below threshold: {self.dropped_below_threshold}")
        print(f"  Total dropped           : {self.total_dropped}  ({self.drop_rate:.1f}%)")

        if self.by_type:
            print(f"\n  Processed by event type:")
            for etype in ("earthquake", "fire", "flood", "storm"):
                cnt = self.by_type.get(etype, 0)
                print(f"    {etype:<12}  {cnt:>5}")

        if self.by_level:
            print(f"\n  Processed by severity level:")
            for level in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
                cnt = self.by_level.get(level, 0)
                bar = "#" * (cnt // max(1, self.processed // 30))   # tiny bar chart
                print(f"    {level:<8}  {cnt:>5}  {bar}")

        print(sep + "\n")


# ── Processor ─────────────────────────────────────────────────────────────────

class EventProcessor:
    """
    Stateless per-event transformation engine.

    The processor is intentionally decoupled from Kafka — it receives
    a plain dict and returns either a ProcessedEvent or None. This makes
    it independently testable and reusable by Module 4.

    Usage:
        processor = EventProcessor()

        result = processor.process(raw_dict)
        if result is not None:
            # pass to MongoDB writer (Module 4)
            db.insert(result.to_dict())
    """

    def __init__(self) -> None:
        self._log  = logging.getLogger(self.__class__.__name__)
        self.stats = ProcessingStats()

    # ── Public API ────────────────────────────────────────────────────────────

    def process(self, raw: dict[str, Any]) -> ProcessedEvent | None:
        """
        Transform one raw event. Returns None if the event should be dropped.

        Steps:
            1. Schema validation  — all fields present, types correct
            2. Severity classification — per-type threshold lookup
            3. Filter decision    — drop if below minimum threshold
            4. Enrichment         — rename fields, add processed_at

        Args:
            raw: Deserialized Kafka message value (dict from JSON).

        Returns:
            ProcessedEvent on success, None if dropped.
        """
        self.stats.received += 1

        # ── Step 1: Validate ─────────────────────────────────────────────────
        if not self._is_valid(raw):
            self.stats.dropped_invalid += 1
            return None

        event_type   = raw["event_type"]
        severity_raw = float(raw["severity_raw"])

        # ── Step 2 & 3: Classify + filter ────────────────────────────────────
        severity_level = self._classify_severity(event_type, severity_raw)

        if severity_level is None:
            self._log.debug(
                "DROP  %-12s  id=%s  severity_raw=%.2f  (below threshold)",
                event_type, raw["event_id"][:8], severity_raw,
            )
            self.stats.dropped_below_threshold += 1
            return None

        # ── Step 4: Build ProcessedEvent ──────────────────────────────────────
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        result = ProcessedEvent(
            event_id       = raw["event_id"],
            event_type     = event_type,
            severity_level = severity_level,
            latitude       = float(raw["latitude"]),
            longitude      = float(raw["longitude"]),
            event_time     = raw["timestamp"],     # field renamed
            processed_at   = now,                  # new field
            source         = raw["source"],
        )

        # Update statistics
        self.stats.processed += 1
        self.stats.by_type[event_type]    = self.stats.by_type.get(event_type, 0) + 1
        self.stats.by_level[severity_level] = self.stats.by_level.get(severity_level, 0) + 1

        return result

    # ── Private helpers ───────────────────────────────────────────────────────

    def _is_valid(self, raw: dict[str, Any]) -> bool:
        """
        Schema validation gate.

        An event is dropped here if:
          - Any required field is absent
          - event_type is not one of the four known values
          - severity_raw is not a numeric type
        """
        missing = _REQUIRED_FIELDS - raw.keys()
        if missing:
            self._log.warning(
                "DROP  <unknown>  missing fields=%s", sorted(missing)
            )
            return False

        if raw["event_type"] not in _VALID_EVENT_TYPES:
            self._log.warning(
                "DROP  id=%s  unknown event_type=%r",
                str(raw.get("event_id", "?"))[:8], raw["event_type"],
            )
            return False

        if not isinstance(raw["severity_raw"], (int, float)):
            self._log.warning(
                "DROP  id=%s  non-numeric severity_raw=%r",
                str(raw.get("event_id", "?"))[:8], raw["severity_raw"],
            )
            return False

        return True

    def _classify_severity(
        self,
        event_type: str,
        severity_raw: float,
    ) -> str | None:
        """
        Map severity_raw to a standard label using per-type thresholds.

        Returns:
            "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
                if the event passes the minimum threshold.
            None
                if severity_raw < min_threshold (sensor null or negligible event).

        Lookup order (first match wins):
            CRITICAL  ← severity_raw >= thresholds["critical"]
            HIGH      ← severity_raw >= thresholds["high"]
            MEDIUM    ← severity_raw >= thresholds["medium"]
            LOW       ← severity_raw >= thresholds["min_threshold"]
            DROP      ← severity_raw <  thresholds["min_threshold"]
        """
        t = SEVERITY_THRESHOLDS[event_type]

        if severity_raw < t["min_threshold"]:
            return None           # sensor null or negligible event → drop

        if severity_raw >= t["critical"]:  return "CRITICAL"
        if severity_raw >= t["high"]:      return "HIGH"
        if severity_raw >= t["medium"]:    return "MEDIUM"
        return "LOW"
