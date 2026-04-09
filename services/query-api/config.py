"""
config.py
Settings -- Module 6 (Query API Service)
Distributed NoSQL-Based Disaster Monitoring and Analytics System

All values can be overridden via environment variables, so the service
works identically in local dev and any deployed environment without
changing source code.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class MongoSettings:
    """Connection settings for the MongoDB operational store."""
    uri:        str = "mongodb://localhost:27017/"
    database:   str = "disaster_db"
    collection: str = "disaster_events"


@dataclass
class PostgresSettings:
    """Connection settings for the PostgreSQL data warehouse."""
    dsn: str = "postgresql://postgres:postgres@localhost:5432/disaster_dw"


@dataclass
class Settings:
    """Top-level application settings."""
    mongo:    MongoSettings    = field(default_factory=MongoSettings)
    postgres: PostgresSettings = field(default_factory=PostgresSettings)
    host:     str  = "127.0.0.1"
    port:     int  = 8000
    debug:    bool = False


# ── Module-level singleton (override via env vars) ────────────────────────────
settings = Settings(
    mongo = MongoSettings(
        uri        = os.getenv("MONGO_URI",        "mongodb://localhost:27017/"),
        database   = os.getenv("MONGO_DATABASE",   "disaster_db"),
        collection = os.getenv("MONGO_COLLECTION", "disaster_events"),
    ),
    postgres = PostgresSettings(
        dsn = os.getenv(
            "POSTGRES_DSN",
            "postgresql://postgres:postgres@localhost:5432/disaster_dw",
        ),
    ),
    host  = os.getenv("API_HOST",  "127.0.0.1"),
    port  = int(os.getenv("API_PORT",  "8000")),
    debug = os.getenv("API_DEBUG", "false").lower() == "true",
)
