"""
main.py — FastAPI application entry point.

Uses a factory pattern (create_app) so tests can inject a custom SQLite path
without module-level side effects.

Startup sequence (lifespan):
  1. Record start_time for uptime tracking
  2. Initialise DedupStore (SQLite, creates file if absent)
  3. Start QueueManager + consumer asyncio task

Shutdown sequence (lifespan teardown):
  4. Stop consumer loop gracefully
"""

from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator, Optional

from fastapi import FastAPI

from .dedup_store import DedupStore
from .queue_manager import QueueManager
from .router import router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

_DEFAULT_DB = Path(
    os.environ.get("DEDUP_DB_PATH", "/app/data/dedup_store.db")
)


def create_app(db_path: Optional[Path] = None) -> FastAPI:
    """
    Application factory.

    Args:
        db_path: Override the SQLite path (used in tests for isolation).
                 Defaults to DEDUP_DB_PATH env var or /app/data/dedup_store.db.
    """
    resolved_db = db_path if db_path is not None else _DEFAULT_DB

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        # ── Startup ──────────────────────────────────────────────────────
        logger.info("=== UTS Log Aggregator starting up ===")
        logger.info("DB path: %s", resolved_db)

        app.state.start_time = time.time()
        app.state.dedup_store = DedupStore(resolved_db)
        app.state.queue_manager = QueueManager(app.state.dedup_store)
        await app.state.queue_manager.start()

        logger.info("=== Aggregator ready — listening on /publish /events /stats ===")
        yield

        # ── Shutdown ─────────────────────────────────────────────────────
        logger.info("=== Aggregator shutting down ===")
        await app.state.queue_manager.stop()
        logger.info("=== Shutdown complete ===")

    application = FastAPI(
        title="UTS Pub-Sub Log Aggregator",
        description=(
            "Idempotent log aggregator with SQLite-backed deduplication. "
            "Implements Pub-Sub pattern with at-least-once delivery semantics."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )
    application.include_router(router)
    return application


# Module-level app instance used by uvicorn and Docker CMD
app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info",
    )
