"""
queue_manager.py — Internal asyncio pipeline: Publisher → Queue → Consumer.

Architecture (Tanenbaum & Van Steen Ch. 2 — Pub-Sub pattern):
  ┌───────────┐   enqueue()   ┌──────────────┐   _process_event()   ┌─────────────┐
  │  FastAPI  │ ────────────▶ │ asyncio.Queue │ ──────────────────▶ │  DedupStore │
  │ /publish  │               │  (in-memory)  │                      │  (SQLite)   │
  └───────────┘               └──────────────┘                      └─────────────┘

Idempotency (Ch. 7):
  _process_event() calls dedup_store.mark_processed() which returns:
    True  → unique event → increment unique_processed_count, log INFO
    False → duplicate    → increment duplicate_dropped_count, log WARNING

Stats counters:
  received_count          — events accepted into the queue (this session)
  unique_processed_count  — unique events committed to SQLite (persistent)
  duplicate_dropped_count — duplicates detected and discarded (this session)

  On startup, unique_processed_count is initialised from SQLite row count so
  the /stats endpoint reflects persisted data across restarts.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional

from .dedup_store import DedupStore
from .models import Event

logger = logging.getLogger(__name__)


class QueueManager:
    """
    Manages the event pipeline.

    Thread-safety note:
      The asyncio.Queue and all counter increments happen on the single asyncio
      event loop thread. SQLite calls are dispatched to the default ThreadPoolExecutor
      via run_in_executor to keep the event loop non-blocking.
    """

    def __init__(self, dedup_store: DedupStore, max_queue_size: int = 50_000) -> None:
        self.dedup_store = dedup_store
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._consumer_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

        # In-memory counters (reset per session)
        self.received_count: int = 0
        self.duplicate_dropped_count: int = 0

        # Initialised from SQLite so persists across restarts
        self.unique_processed_count: int = dedup_store.count_unique()

    # ── Lifecycle ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._consumer_task = asyncio.create_task(
            self._consumer_loop(), name="consumer-loop"
        )
        logger.info(
            "QueueManager started — unique_processed loaded from DB: %d",
            self.unique_processed_count,
        )

    async def stop(self) -> None:
        self._running = False
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        logger.info("QueueManager stopped")

    # ── Enqueue ─────────────────────────────────────────────────────────────

    async def enqueue(self, event: Event) -> None:
        """
        Non-blocking enqueue. Raises asyncio.QueueFull if queue is at capacity,
        which the router translates to HTTP 503 for back-pressure signalling.
        """
        self.queue.put_nowait(event)
        self.received_count += 1

    # ── Consumer loop ────────────────────────────────────────────────────────

    async def _consumer_loop(self) -> None:
        logger.info("Consumer loop waiting for events …")
        while self._running:
            try:
                event = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                try:
                    await self._process_event(event)
                finally:
                    self.queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                logger.exception("Unexpected error in consumer loop: %s", exc)

    async def _process_event(self, event: Event) -> None:
        """
        Core idempotency logic:
          1. Call mark_processed() on dedup_store (atomic SQLite INSERT).
          2. On success  → unique event, increment counter, log INFO.
          3. On failure  → duplicate, increment drop counter, log WARNING.

        The SQLite PRIMARY KEY (topic, event_id) guarantees that even if
        two identical events reach this method concurrently (via the thread
        pool), only one INSERT succeeds and the other gets IntegrityError.
        """
        loop = asyncio.get_running_loop()
        success: bool = await loop.run_in_executor(
            None,
            self.dedup_store.mark_processed,
            event.topic,
            event.event_id,
            event.source,
            event.timestamp,
            event.payload,
        )

        if success:
            self.unique_processed_count += 1
            logger.info(
                "[PROCESSED]  topic=%-30s  event_id=%s  source=%s",
                event.topic,
                event.event_id,
                event.source,
            )
        else:
            self.duplicate_dropped_count += 1
            logger.warning(
                "[DUPLICATE]  topic=%-30s  event_id=%s  source=%s  — dropped",
                event.topic,
                event.event_id,
                event.source,
            )

    # ── Testing helpers ──────────────────────────────────────────────────────

    async def flush(self, timeout: float = 10.0) -> None:
        """
        Wait until all currently queued events have been processed.
        Used in tests and integration scenarios.
        """
        try:
            await asyncio.wait_for(self.queue.join(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("flush() timed out after %.1fs", timeout)

    # ── Stats snapshot ───────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        return {
            "received":          self.received_count,
            "unique_processed":  self.unique_processed_count,
            "duplicate_dropped": self.duplicate_dropped_count,
        }
