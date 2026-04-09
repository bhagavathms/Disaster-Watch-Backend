"""
config.py
Configuration for the Kafka Consumer Processor Microservice — Module 3
Distributed NoSQL-Based Disaster Monitoring and Analytics System

All severity thresholds and consumer knobs live here.
No external config files, no environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# ── Path resolution ───────────────────────────────────────────────────────────
_SERVICE_DIR      = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DATA_DIR = os.path.normpath(
    os.path.join(_SERVICE_DIR, "..", "..", "data")
)

# ── Severity thresholds (single source of truth) ──────────────────────────────
#
# Each dict defines the filter + classification rules for one event type.
#
# severity_raw < min_threshold        → DROP  (sensor null or too minor to act on)
# min_threshold <= raw < medium       → LOW
# medium        <= raw < high         → MEDIUM
# high          <= raw < critical     → HIGH
# critical      <= raw                → CRITICAL
#
# Basis:
#   earthquake → Richter magnitude scale (standard seismological categories)
#   storm      → Saffir-Simpson / Beaufort scale (35 km/h = tropical storm onset)
#   flood      → Water level rise in cm (50 cm ≈ significant property damage threshold)
#   fire       → Arbitrary intensity index; 7000+ aligns with "mega-fire" classification

SEVERITY_THRESHOLDS: dict[str, dict[str, float]] = {
    "earthquake": {
        "min_threshold": 1.5,    # < 1.5  → DROP  (imperceptible / sensor null)
        "medium":        3.0,    # 3.0–4.9 → MEDIUM (felt widely, minor damage)
        "high":          5.0,    # 5.0–6.9 → HIGH   (strong, significant damage)
        "critical":      7.0,    # ≥ 7.0  → CRITICAL (major earthquake)
    },
    "fire": {
        # severity_raw = total Fire Radiative Power (FRP) in MW
        # aggregated across all satellite pixels in one fire cluster (NASA FIRMS).
        "min_threshold":  5.0,   # < 5 MW   → DROP  (sub-threshold / false alarm)
        "medium":        20.0,   # 20–49 MW → MEDIUM (active wildfire)
        "high":          50.0,   # 50–199 MW → HIGH  (large wildfire)
        "critical":     200.0,   # ≥ 200 MW → CRITICAL (mega-fire)
    },
    "flood": {
        # severity_raw = Flood Magnitude Index (FMI) — dimensionless composite
        # FMI = 0.5*log10(area_km2+1) + 0.3*depth_score + 0.2*log10(duration_h+1)
        # Computed from USGS High-Water Mark (HWM) data grouped by flood event_id.
        "min_threshold": 0.5,   # < 0.5  → DROP  (minor / data quality too low)
        "medium":        1.0,   # 1.0–1.9 → MEDIUM (Moderate flood)
        "high":          2.0,   # 2.0–2.9 → HIGH   (Severe flood)
        "critical":      3.0,   # ≥ 3.0  → CRITICAL (Extreme flood)
    },
    "storm": {
        "min_threshold":  35.0,  # < 35 km/h → DROP  (tropical depression / null)
        "medium":         62.0,  # 62–119 km/h  → MEDIUM (tropical storm to Cat-1)
        "high":          120.0,  # 120–199 km/h → HIGH   (Cat-2 to Cat-3)
        "critical":      200.0,  # ≥ 200 km/h  → CRITICAL (Cat-4 to Cat-5)
    },
}


# ── Consumer configuration ────────────────────────────────────────────────────

@dataclass
class ConsumerConfig:
    """
    Configuration for the Kafka Consumer Processor.

    Attributes:
        bootstrap_servers  : Kafka broker list.
        topics             : Topics to subscribe to (all four disaster types).
        group_id           : Consumer group ID.
                             Consumers sharing this ID form a group —
                             Kafka distributes partitions between them.
                             Module 4 will join this same group.
        auto_offset_reset  : "earliest" → start from first message
                             if no committed offset exists for this group.
        enable_auto_commit : False → we commit offsets manually after each
                             message (prevents data loss on crash).
        log_every_n        : Print a progress line every N processed events.
        data_dir           : Path to data/ (used only in --simulate mode).
    """

    # Kafka cluster
    bootstrap_servers: list[str] = field(
        default_factory=lambda: ["localhost:9092"]
    )

    # Topics — must match what the producer publishes to
    topics: list[str] = field(
        default_factory=lambda: [
            "earthquakes-topic",
            "fires-topic",
            "floods-topic",
            "storms-topic",
        ]
    )

    # Consumer group
    group_id:           str  = "disaster-processor-group"
    auto_offset_reset:  str  = "earliest"
    enable_auto_commit: bool = False

    # Behaviour
    log_every_n: int = 10
    data_dir:    str = _DEFAULT_DATA_DIR
