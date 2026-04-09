"""
config.py
Configuration for the Kafka Producer Microservice — Module 2
Distributed NoSQL-Based Disaster Monitoring and Analytics System

All tuneable knobs live here. No environment variables, no external config
files — intentionally flat for a local university project setup.

To change behaviour, either:
  (a) Modify the defaults in ProducerConfig, or
  (b) Pass a custom ProducerConfig instance to DisasterEventProducer, or
  (c) Use the CLI flags in producer.py (--speed, --broker, --data-dir).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import Enum


# ── Replay speed ──────────────────────────────────────────────────────────────

class ReplaySpeed(Enum):
    """
    Simulated inter-message delay.

    FAST   — stress-test / throughput demo  (0.1 s between messages)
    NORMAL — default demo mode              (0.5 s between messages)
    SLOW   — educational / step-through     (2.0 s between messages)
    """
    FAST   = 0.1
    NORMAL = 0.5
    SLOW   = 2.0


# ── Path resolution ───────────────────────────────────────────────────────────
# Resolves data/ relative to the project root, regardless of the CWD from
# which the service is launched.
#
# Layout assumed:
#   <project-root>/
#     data/                   ← JSON datasets
#     services/
#       kafka-producer/       ← this file lives here
#
_SERVICE_DIR      = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DATA_DIR = os.path.normpath(
    os.path.join(_SERVICE_DIR, "..", "..", "data")
)


# ── ProducerConfig ────────────────────────────────────────────────────────────

@dataclass
class ProducerConfig:
    """
    Single source of truth for all producer knobs.

    Attributes:
        bootstrap_servers : Kafka broker(s). Default: local single-node.
        replay_speed      : Enum controlling inter-message sleep duration.
        data_dir          : Absolute or relative path to the data/ directory.
        topic_map         : Maps event_type → Kafka topic name.
                            Must stay in sync with consumer subscriptions.
        acks              : Kafka producer acks level.
                            "1"  = leader ACK only (fast, safe for local use).
                            "all"= full ISR ACK    (durable, slower).
        retries           : Retry count on transient send failures.
        request_timeout_ms: Per-request Kafka timeout in milliseconds.
    """

    # Kafka cluster
    bootstrap_servers: list[str] = field(
        default_factory=lambda: ["localhost:9092"]
    )

    # Streaming behaviour
    replay_speed: ReplaySpeed = ReplaySpeed.NORMAL

    # Data source
    data_dir: str = _DEFAULT_DATA_DIR

    # Topic routing — one topic per disaster type
    # These names MUST match what the consumer(s) subscribe to in Module 3.
    topic_map: dict[str, str] = field(
        default_factory=lambda: {
            "earthquake": "earthquakes-topic",
            "fire":       "fires-topic",
            "flood":      "floods-topic",
            "storm":      "storms-topic",
        }
    )

    # Kafka producer tuning
    acks:                str = "1"
    retries:             int = 3
    request_timeout_ms:  int = 10_000


# ── Convenience presets ───────────────────────────────────────────────────────
# Import whichever preset matches your current run mode:
#   from config import FAST_CONFIG
#   producer = DisasterEventProducer(FAST_CONFIG)

FAST_CONFIG   = ProducerConfig(replay_speed=ReplaySpeed.FAST)
NORMAL_CONFIG = ProducerConfig(replay_speed=ReplaySpeed.NORMAL)
SLOW_CONFIG   = ProducerConfig(replay_speed=ReplaySpeed.SLOW)
