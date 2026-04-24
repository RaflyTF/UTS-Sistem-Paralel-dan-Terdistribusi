"""
publisher.py — Standalone publisher service for Docker Compose (bonus).

Behaviour:
  1. Poll GET /health until the aggregator is ready (with exponential backoff).
  2. Generate TOTAL_EVENTS unique events distributed across 5 topics.
  3. Add DUPLICATE_RATIO * TOTAL_EVENTS duplicate events (same event_id).
  4. Shuffle the full list (simulate real-world out-of-order + duplicate delivery).
  5. Send events in batches of BATCH_SIZE to POST /publish.
  6. Print a final summary matching the /stats endpoint.

Environment variables (set via Docker Compose):
  AGGREGATOR_URL   — default: http://localhost:8080
  TOTAL_EVENTS     — default: 5000
  DUPLICATE_RATIO  — default: 0.25  (25% >= required 20%)
  BATCH_SIZE       — default: 100
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("publisher")

AGGREGATOR_URL: str = os.environ.get("AGGREGATOR_URL", "http://localhost:8080")
TOTAL_EVENTS:   int = int(os.environ.get("TOTAL_EVENTS", "5000"))
DUPLICATE_RATIO: float = float(os.environ.get("DUPLICATE_RATIO", "0.25"))
BATCH_SIZE:     int = int(os.environ.get("BATCH_SIZE", "100"))

TOPICS = [
    "auth.user.login",
    "payment.order.created",
    "infra.server.health",
    "api.request.received",
    "db.query.executed",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_event(topic: str, source: str, idx: int) -> Dict[str, Any]:
    """Generate a unique event with a collision-resistant event_id."""
    ts_ms = int(time.time() * 1000)
    event_id = f"{source}-{ts_ms}-{uuid.uuid4()}"
    return {
        "topic":     topic,
        "event_id":  event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    source,
        "payload":   {"index": idx, "data": f"log-entry-{idx}"},
    }


def build_event_list() -> List[Dict[str, Any]]:
    """
    Build a list of TOTAL_EVENTS unique events plus DUPLICATE_RATIO * TOTAL_EVENTS
    duplicates (same event_id re-sent) — simulating at-least-once delivery.
    """
    unique_events: List[Dict[str, Any]] = []
    for i in range(TOTAL_EVENTS):
        topic  = TOPICS[i % len(TOPICS)]
        source = f"service-{(i % 10):02d}"
        unique_events.append(make_event(topic, source, i))

    n_dups = math.ceil(TOTAL_EVENTS * DUPLICATE_RATIO)
    dup_events = [
        dict(e) | {"timestamp": datetime.now(timezone.utc).isoformat()}
        for e in random.choices(unique_events, k=n_dups)
    ]

    combined = unique_events + dup_events
    random.shuffle(combined)

    logger.info(
        "Generated %d unique + %d duplicate = %d total events across %d topics",
        len(unique_events),
        len(dup_events),
        len(combined),
        len(TOPICS),
    )
    return combined


async def wait_for_aggregator(client: httpx.AsyncClient, max_retries: int = 30) -> None:
    """Poll /health with exponential backoff until the aggregator is ready."""
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(f"{AGGREGATOR_URL}/health", timeout=3.0)
            if resp.status_code == 200:
                logger.info("Aggregator ready after %d attempt(s)", attempt)
                return
        except (httpx.ConnectError, httpx.ReadTimeout):
            pass
        logger.info("Waiting for aggregator … attempt %d/%d (%.1fs)", attempt, max_retries, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 1.5, 10.0)
    raise RuntimeError(f"Aggregator at {AGGREGATOR_URL} did not become ready in time.")


async def publish_batches(
    client: httpx.AsyncClient,
    events: List[Dict[str, Any]],
) -> None:
    """Send events in batches; retry failed batches once with backoff."""
    total = len(events)
    sent  = 0
    start = time.time()

    for i in range(0, total, BATCH_SIZE):
        batch = events[i : i + BATCH_SIZE]
        payload = {"events": batch}

        for attempt in (1, 2):
            try:
                resp = await client.post(
                    f"{AGGREGATOR_URL}/publish",
                    content=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    timeout=30.0,
                )
                if resp.status_code in (200, 202):
                    sent += len(batch)
                    break
                logger.warning(
                    "Batch %d–%d: HTTP %d (attempt %d)",
                    i, i + len(batch), resp.status_code, attempt,
                )
            except httpx.RequestError as exc:
                logger.warning("Batch %d–%d: request error %s (attempt %d)", i, i + len(batch), exc, attempt)
            await asyncio.sleep(0.5 * attempt)

        if sent % 1000 == 0 or i + BATCH_SIZE >= total:
            elapsed = time.time() - start
            pct = 100.0 * sent / total
            logger.info(
                "Progress: %d/%d events sent (%.1f%%)  — %.1f ev/s",
                sent, total, pct,
                sent / elapsed if elapsed > 0 else 0,
            )


async def print_final_stats(client: httpx.AsyncClient) -> None:
    """Wait a moment for the consumer to drain, then fetch /stats."""
    await asyncio.sleep(2.0)
    try:
        resp = await client.get(f"{AGGREGATOR_URL}/stats", timeout=10.0)
        if resp.status_code == 200:
            stats = resp.json()
            logger.info("=== FINAL STATS FROM AGGREGATOR ===")
            logger.info("  received          : %s", stats.get("received"))
            logger.info("  unique_processed  : %s", stats.get("unique_processed"))
            logger.info("  duplicate_dropped : %s", stats.get("duplicate_dropped"))
            logger.info("  topics            : %s", stats.get("topics"))
            logger.info("  uptime_seconds    : %s", stats.get("uptime_seconds"))
        else:
            logger.warning("Could not fetch stats: HTTP %d", resp.status_code)
    except httpx.RequestError as exc:
        logger.warning("Stats fetch failed: %s", exc)


async def main() -> None:
    logger.info("Publisher starting — target: %s", AGGREGATOR_URL)
    logger.info(
        "Config: TOTAL_EVENTS=%d  DUPLICATE_RATIO=%.0f%%  BATCH_SIZE=%d",
        TOTAL_EVENTS, DUPLICATE_RATIO * 100, BATCH_SIZE,
    )

    async with httpx.AsyncClient() as client:
        await wait_for_aggregator(client)
        events = build_event_list()
        await publish_batches(client, events)
        await print_final_stats(client)

    logger.info("Publisher finished.")


if __name__ == "__main__":
    asyncio.run(main())
