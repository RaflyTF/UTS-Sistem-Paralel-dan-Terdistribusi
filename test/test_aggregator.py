"""
test_aggregator.py — Unit & integration tests for the Pub-Sub Log Aggregator.

Test coverage (10 tests):
  1.  test_schema_valid_event              — valid event accepted (HTTP 202)
  2.  test_schema_invalid_timestamp        — bad timestamp → HTTP 422
  3.  test_schema_missing_required_field   — missing topic/event_id → HTTP 422
  4.  test_dedup_duplicate_dropped         — same event twice → only once in /events
  5.  test_dedup_store_persistence         — reinitialise DedupStore with same file → still deduplicates
  6.  test_dedup_different_topics          — same event_id on different topics → both processed
  7.  test_events_endpoint_consistency     — GET /events matches number of unique events
  8.  test_stats_consistency               — GET /stats counters are self-consistent
  9.  test_batch_publish                   — batch of N events all accepted and processed
  10. test_stress_batch_performance        — 1 000 events (250 dups) complete within time budget
"""

from __future__ import annotations

import time
import uuid
from typing import Tuple

import pytest

from conftest import make_valid_event
from src.dedup_store import DedupStore


# ── Helpers ───────────────────────────────────────────────────────────────────

def unique_id() -> str:
    return str(uuid.uuid4())


# ── Test 1: Schema — valid event ──────────────────────────────────────────────

async def test_schema_valid_event(app_and_client: Tuple) -> None:
    """A correctly formed event should be accepted with HTTP 202."""
    app, client = app_and_client
    event = make_valid_event(event_id=unique_id())
    resp = await client.post("/publish", json=event)

    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["accepted"] == 1


# ── Test 2: Schema — invalid timestamp ───────────────────────────────────────

async def test_schema_invalid_timestamp(app_and_client: Tuple) -> None:
    """An event with a non-ISO-8601 timestamp should be rejected with HTTP 422."""
    app, client = app_and_client
    event = make_valid_event(event_id=unique_id(), timestamp="not-a-timestamp")
    resp = await client.post("/publish", json=event)

    assert resp.status_code == 422, resp.text


# ── Test 3: Schema — missing required field ───────────────────────────────────

async def test_schema_missing_required_field(app_and_client: Tuple) -> None:
    """An event missing 'topic' or 'event_id' should be rejected with HTTP 422."""
    app, client = app_and_client

    # Missing 'topic'
    no_topic = {
        "event_id":  unique_id(),
        "timestamp": "2024-05-07T10:00:00Z",
        "source":    "svc",
        "payload":   {},
    }
    resp = await client.post("/publish", json=no_topic)
    assert resp.status_code == 422, resp.text

    # Missing 'event_id'
    no_event_id = {
        "topic":     "auth.user.login",
        "timestamp": "2024-05-07T10:00:00Z",
        "source":    "svc",
        "payload":   {},
    }
    resp2 = await client.post("/publish", json=no_event_id)
    assert resp2.status_code == 422, resp2.text


# ── Test 4: Dedup — duplicate event dropped ───────────────────────────────────

async def test_dedup_duplicate_dropped(app_and_client: Tuple) -> None:
    """
    Sending the same event (same topic + event_id) three times must result in
    exactly ONE entry in /events and duplicate_dropped >= 2 in /stats.
    """
    app, client = app_and_client
    eid = unique_id()
    event = make_valid_event(topic="dedup.test", event_id=eid)

    for _ in range(3):
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 202

    # Drain the consumer queue before asserting
    await app.state.queue_manager.flush()

    # /events must contain exactly 1 entry
    evts_resp = await client.get("/events", params={"topic": "dedup.test"})
    assert evts_resp.status_code == 200
    body = evts_resp.json()
    assert body["count"] == 1, f"Expected 1 unique event, got {body['count']}"
    assert body["events"][0]["event_id"] == eid

    # /stats must show >= 2 duplicates dropped
    stats_resp = await client.get("/stats")
    stats = stats_resp.json()
    assert stats["duplicate_dropped"] >= 2
    assert stats["unique_processed"] >= 1


# ── Test 5: Dedup store persistence (simulate restart) ────────────────────────

async def test_dedup_store_persistence(tmp_path) -> None:
    """
    After reinitialising DedupStore with the same SQLite file (simulating a
    container restart), previously processed events must not be inserted again.
    This validates that dedup survives restarts (Tanenbaum & Van Steen Ch. 6).
    """
    db_path = tmp_path / "persist_test.db"

    # First 'session': mark an event as processed
    store_a = DedupStore(db_path)
    inserted = store_a.mark_processed(
        topic="persist.test",
        event_id="stable-id-001",
        source="svc",
        event_ts="2024-05-07T10:00:00Z",
        payload={"x": 1},
    )
    assert inserted is True

    # Simulate restart: create a brand-new DedupStore instance on the same file
    store_b = DedupStore(db_path)

    # The same event must now be detected as a duplicate
    reinserted = store_b.mark_processed(
        topic="persist.test",
        event_id="stable-id-001",
        source="svc",
        event_ts="2024-05-07T10:00:00Z",
        payload={"x": 1},
    )
    assert reinserted is False, "Expected duplicate detection after simulated restart"

    # count_unique must be 1
    assert store_b.count_unique() == 1


# ── Test 6: Same event_id on different topics ─────────────────────────────────

async def test_dedup_different_topics(app_and_client: Tuple) -> None:
    """
    The same event_id on two *different* topics must be treated as two
    independent events (composite key is topic + event_id).
    """
    app, client = app_and_client
    shared_id = unique_id()

    for topic in ("topic.alpha", "topic.beta"):
        event = make_valid_event(topic=topic, event_id=shared_id)
        resp = await client.post("/publish", json=event)
        assert resp.status_code == 202

    await app.state.queue_manager.flush()

    for topic in ("topic.alpha", "topic.beta"):
        resp = await client.get("/events", params={"topic": topic})
        body = resp.json()
        assert body["count"] == 1, f"topic={topic} expected 1 event, got {body['count']}"


# ── Test 7: GET /events consistency ───────────────────────────────────────────

async def test_events_endpoint_consistency(app_and_client: Tuple) -> None:
    """
    After publishing N unique events to one topic, GET /events must return
    exactly N results, each with the correct event_id.
    """
    app, client = app_and_client
    topic = "events.consistency.test"
    n = 20
    ids = [unique_id() for _ in range(n)]

    batch = [make_valid_event(topic=topic, event_id=eid) for eid in ids]
    resp = await client.post("/publish", json={"events": batch})
    assert resp.status_code == 202
    assert resp.json()["accepted"] == n

    await app.state.queue_manager.flush()

    resp2 = await client.get("/events", params={"topic": topic})
    body = resp2.json()
    assert body["count"] == n
    returned_ids = {e["event_id"] for e in body["events"]}
    assert returned_ids == set(ids)


# ── Test 8: GET /stats consistency ────────────────────────────────────────────

async def test_stats_consistency(app_and_client: Tuple) -> None:
    """
    After publishing a known mix of unique + duplicate events, /stats must
    satisfy: unique_processed + duplicate_dropped == received.
    """
    app, client = app_and_client
    topic = "stats.test"

    # 5 unique events
    uniq_ids = [unique_id() for _ in range(5)]
    for eid in uniq_ids:
        await client.post("/publish", json=make_valid_event(topic=topic, event_id=eid))

    # 3 duplicates (resend first 3 unique events)
    for eid in uniq_ids[:3]:
        await client.post("/publish", json=make_valid_event(topic=topic, event_id=eid))

    await app.state.queue_manager.flush()

    stats = (await client.get("/stats")).json()

    assert stats["received"] == 8
    assert stats["unique_processed"] == 5
    assert stats["duplicate_dropped"] == 3
    assert stats["unique_processed"] + stats["duplicate_dropped"] == stats["received"]
    assert topic in stats["topics"]


# ── Test 9: Batch publish ─────────────────────────────────────────────────────

async def test_batch_publish(app_and_client: Tuple) -> None:
    """
    A batch of 50 events sent in a single POST must all be accepted and
    processed as unique events.
    """
    app, client = app_and_client
    topic = "batch.test"
    n = 50
    batch = [make_valid_event(topic=topic, event_id=unique_id()) for _ in range(n)]

    resp = await client.post("/publish", json={"events": batch})
    assert resp.status_code == 202
    assert resp.json()["accepted"] == n

    await app.state.queue_manager.flush()

    resp2 = await client.get("/events", params={"topic": topic})
    assert resp2.json()["count"] == n


# ── Test 10: Stress — 1 000 events (250 duplicates) within time budget ────────

async def test_stress_batch_performance(app_and_client: Tuple) -> None:
    """
    Publish 750 unique + 250 duplicate events (total 1 000) and assert the
    entire round-trip (enqueue + process + persist) completes within 8 seconds.

    This validates the '>= 5 000 event' scalability requirement at a reduced
    scale suitable for in-process unit testing.
    """
    app, client = app_and_client
    topic = "stress.test"

    unique_ids = [unique_id() for _ in range(750)]
    import random
    dup_ids = random.choices(unique_ids, k=250)
    all_ids = unique_ids + dup_ids
    random.shuffle(all_ids)

    batch = [make_valid_event(topic=topic, event_id=eid) for eid in all_ids]

    t_start = time.perf_counter()
    resp = await client.post("/publish", json={"events": batch})
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 1000

    await app.state.queue_manager.flush(timeout=15.0)
    elapsed = time.perf_counter() - t_start

    resp2 = await client.get("/events", params={"topic": topic})
    assert resp2.json()["count"] == 750  # only unique events

    stats = (await client.get("/stats")).json()
    assert stats["unique_processed"] == 750
    assert stats["duplicate_dropped"] == 250

    assert elapsed < 8.0, (
        f"Stress test took {elapsed:.2f}s — expected < 8.0s. "
        "Consumer may be too slow or SQLite is under contention."
    )
