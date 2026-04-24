"""
router.py — FastAPI route handlers for the Log Aggregator.

Endpoints:
  POST /publish        — accept single event, array, or batch wrapper
  GET  /events?topic=  — list unique processed events for a topic
  GET  /stats          — system-wide metrics
  GET  /health         — liveness probe (used by Docker healthcheck)
  GET  /               — API info / welcome

Error handling:
  - 400 / 422 for malformed JSON or schema violations
  - 503 if the internal queue is full (back-pressure)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Union

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from pydantic import ValidationError

from .models import BatchPublishRequest, Event, StatsResponse

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _qm(request: Request):  # type: ignore[return]
    """Extract the QueueManager from app state."""
    return request.app.state.queue_manager


# ── Routes ───────────────────────────────────────────────────────────────────

@router.get("/", include_in_schema=False)
async def root() -> Dict[str, Any]:
    return {
        "service": "UTS Pub-Sub Log Aggregator",
        "version": "1.0.0",
        "endpoints": [
            "POST /publish",
            "GET  /events?topic=<topic>",
            "GET  /stats",
            "GET  /health",
        ],
    }


@router.get("/health")
async def health() -> Dict[str, str]:
    """Liveness probe — returns 200 when the service is ready."""
    return {"status": "ok"}


@router.post("/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish_events(request: Request) -> Dict[str, Any]:
    """
    Accept events for asynchronous processing.

    Body formats accepted:
      1. Single event object  { "topic": …, "event_id": …, … }
      2. Array of events      [ { … }, { … } ]
      3. Batch wrapper        { "events": [ { … }, { … } ] }

    All events are validated via Pydantic before being enqueued.
    Returns HTTP 202 Accepted immediately; processing is async.

    Returns HTTP 503 if the internal queue is at capacity (back-pressure).
    """
    try:
        body: Any = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid JSON body: {exc}",
        ) from exc

    # ── Parse into list[Event] ────────────────────────────────────────────
    events: List[Event] = []
    try:
        if isinstance(body, list):
            # Format 2: array of raw event dicts
            events = [Event(**item) for item in body]
        elif isinstance(body, dict) and "events" in body:
            # Format 3: batch wrapper
            batch = BatchPublishRequest(**body)
            events = batch.events
        elif isinstance(body, dict):
            # Format 1: single event
            events = [Event(**body)]
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Body must be a JSON object (single event), array of events, "
                       "or {'events': [...]}",
            )
    except ValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=jsonable_encoder(exc.errors()),
        ) from exc

    if not events:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No events provided.",
        )

    # ── Enqueue ───────────────────────────────────────────────────────────
    qm = _qm(request)
    enqueued = 0
    for event in events:
        try:
            await qm.enqueue(event)
            enqueued += 1
        except asyncio.QueueFull:
            # Return partial success count + 503 guidance
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Queue is full. Enqueued {enqueued}/{len(events)} events. "
                       "Retry with exponential back-off.",
            )

    logger.info("[ACCEPTED] %d event(s) enqueued for processing", enqueued)
    return {
        "accepted": enqueued,
        "message":  f"Accepted {enqueued} event(s) for async processing.",
    }


@router.get("/events")
async def get_events(topic: str, request: Request) -> Dict[str, Any]:
    """
    Return the list of unique processed events for the given topic.

    Only events that have passed deduplication and been committed to SQLite
    are returned. Duplicates are never included.

    Query params:
      topic (required) — exact topic name, e.g. 'auth.user.login'
    """
    if not topic.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="'topic' query parameter must not be blank.",
        )

    qm = _qm(request)
    loop = asyncio.get_running_loop()
    events = await loop.run_in_executor(
        None, qm.dedup_store.get_events_by_topic, topic
    )
    return {"topic": topic, "count": len(events), "events": events}


@router.get("/stats", response_model=StatsResponse)
async def get_stats(request: Request) -> StatsResponse:
    """
    Return aggregated system metrics.

    Fields:
      received          — events accepted (this session, resets on restart)
      unique_processed  — unique events in SQLite (persistent across restarts)
      duplicate_dropped — duplicates discarded (this session)
      topics            — list of topics with at least one processed event
      uptime_seconds    — seconds since service started
    """
    qm = _qm(request)
    snap = qm.snapshot()

    loop = asyncio.get_running_loop()
    topics = await loop.run_in_executor(None, qm.dedup_store.get_all_topics)
    uptime = time.time() - request.app.state.start_time

    return StatsResponse(
        received=snap["received"],
        unique_processed=snap["unique_processed"],
        duplicate_dropped=snap["duplicate_dropped"],
        topics=topics,
        uptime_seconds=round(uptime, 3),
    )
