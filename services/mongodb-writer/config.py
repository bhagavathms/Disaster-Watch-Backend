"""
config.py
Configuration for the MongoDB Writer Microservice — Module 4
Distributed NoSQL-Based Disaster Monitoring and Analytics System

Separates concerns cleanly:
  MongoConfig  — everything the MongoDB client needs
  KafkaConfig  — everything the Kafka consumer needs (own group, raw topics)

Why a separate KafkaConfig here?
  This service subscribes to the RAW Kafka topics in its OWN consumer group
  ("disaster-storage-group"), independent of Module 3 ("disaster-processor-group").
  Kafka delivers every message to every consumer group — both services see all events.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# ── Path resolution ───────────────────────────────────────────────────────────
_SERVICE_DIR      = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DATA_DIR = os.path.normpath(
    os.path.join(_SERVICE_DIR, "..", "..", "data")
)

# Path to Module 3's processor.py — imported at runtime by writer.py
CONSUMER_SERVICE_DIR = os.path.normpath(
    os.path.join(_SERVICE_DIR, "..", "kafka-consumer")
)


# ── MongoDB configuration ─────────────────────────────────────────────────────

@dataclass
class MongoConfig:
    """
    Everything the MongoDBClient needs to connect and write.

    URI variants:
      Standalone (local dev):
        "mongodb://localhost:27017/"

      Replica set (production-like local):
        "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"

      Sharded cluster (advanced local):
        "mongodb://localhost:27020/"    (mongos router)

    The application code is identical for all three — only this URI changes.
    Sharding and replication are infrastructure concerns, not application concerns.

    write_concern:
      "majority" → confirmed by majority of replica-set members before ACK.
      1          → confirmed by primary only (faster, acceptable for local dev).

    batch_size:
      Number of processed events to accumulate before a single bulk_write().
      Larger = fewer round-trips = better throughput.
      Smaller = lower per-event latency.
      50 is a sensible default for a streaming workload.
    """

    uri:           str = "mongodb://localhost:27017/"
    database:      str = "disaster_db"
    collection:    str = "disaster_events"
    write_concern: str = "1"           # use "majority" with a real replica set
    batch_size:    int = 50


# ── Kafka consumer configuration (for this service's own consumer group) ──────

@dataclass
class KafkaConfig:
    """
    Kafka consumer settings for the MongoDB writer's independent subscription.

    group_id "disaster-storage-group" is intentionally different from
    Module 3's "disaster-processor-group".  Kafka sends every message to
    every group, so both services receive the full event stream without
    interfering with each other.

    enable_auto_commit = False → we commit offsets only after a successful
    MongoDB bulk_write(), preventing data loss if the service crashes mid-batch.
    """

    bootstrap_servers:  list[str] = field(
        default_factory=lambda: ["localhost:9092"]
    )
    topics: list[str] = field(
        default_factory=lambda: [
            "earthquakes-topic",
            "fires-topic",
            "floods-topic",
            "storms-topic",
        ]
    )
    group_id:           str  = "disaster-storage-group"   # != Module 3's group
    auto_offset_reset:  str  = "earliest"
    enable_auto_commit: bool = False


# ── Combined writer config (convenience wrapper) ──────────────────────────────

@dataclass
class WriterConfig:
    """
    Single object passed to DisasterEventWriter.
    Groups MongoDB + Kafka + operational settings.
    """
    mongo: MongoConfig  = field(default_factory=MongoConfig)
    kafka: KafkaConfig  = field(default_factory=KafkaConfig)
    data_dir: str       = _DEFAULT_DATA_DIR
    dry_run:  bool      = False   # if True, documents are printed, not written
