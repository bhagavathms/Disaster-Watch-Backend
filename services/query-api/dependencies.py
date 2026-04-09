"""
dependencies.py
FastAPI Dependency Injection -- Module 6 (Query API Service)

Provides get_mongo() and get_postgres() as FastAPI dependency functions.
Clients are stored on app.state (set during lifespan startup in api.py)
so they are created once and reused across all requests.
"""

from __future__ import annotations

from fastapi import Request

from mongo_client    import MongoReadClient
from postgres_client import PostgresReadClient


def get_mongo(request: Request) -> MongoReadClient:
    """Inject the shared MongoDB read client."""
    return request.app.state.mongo


def get_postgres(request: Request) -> PostgresReadClient:
    """Inject the shared PostgreSQL read client."""
    return request.app.state.postgres
