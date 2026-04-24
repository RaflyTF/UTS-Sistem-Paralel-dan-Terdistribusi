"""
conftest.py — Shared pytest fixtures for the Log Aggregator test suite.

Key design:
  - Each test function gets an *isolated* SQLite DB in a tmp_path subdirectory,
    so tests are fully independent and can run in parallel.
  - app_client fixture triggers the FastAPI lifespan (startup + shutdown) via
    httpx ASGITransport, giving a realistic in-process test environment.
  - The FastAPI `app` object is exposed so tests can call
    app.state.queue_manager.flush() to drain the async queue before assertions.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
import sys
from typing import Any, AsyncGenerator, Tuple, cast

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.main import create_app


@asynccontextmanager
async def _lifespan(app: Any) -> AsyncGenerator[None, None]:
    """
    Run FastAPI lifespan without relying on httpx's ASGITransport(lifespan=...).
    This keeps compatibility with older httpx versions.
    """
    if hasattr(app.router, "lifespan_context"):
        async with app.router.lifespan_context(app):
            yield
    else:
        await app.router.startup()
        try:
            yield
        finally:
            await app.router.shutdown()


@pytest_asyncio.fixture
async def app_and_client(
    tmp_path: Path,
) -> AsyncGenerator[Tuple, None]:
    """
    Yield (app, AsyncClient) with a fresh, isolated SQLite DB per test.

    Usage in tests:
        async def test_something(app_and_client):
            app, client = app_and_client
            await client.post("/publish", json={...})
            await app.state.queue_manager.flush()
            resp = await client.get("/events?topic=...")
    """
    db_path = tmp_path / "test_dedup.db"
    app = create_app(db_path=db_path)

    async with _lifespan(app):
        async with AsyncClient(
            transport=ASGITransport(app=cast(Any, app)),
            base_url="http://testserver",
        ) as client:
            yield app, client


# ── Shared event factories ────────────────────────────────────────────────────

def make_valid_event(
    topic: str = "test.topic",
    event_id: str = "evt-001",
    source: str = "test-service",
    timestamp: str = "2024-05-07T10:00:00Z",
    payload: dict | None = None,
) -> dict:
    return {
        "topic":     topic,
        "event_id":  event_id,
        "timestamp": timestamp,
        "source":    source,
        "payload":   payload or {"key": "value"},
    }
