"""
dedup_store.py — Persistent deduplication store backed by SQLite.

Design rationale:
  - PRIMARY KEY (topic, event_id) enforces uniqueness at the DB level atomically.
  - threading.Lock serialises all DB access so asyncio's run_in_executor thread pool
    never causes concurrent-write corruption.
  - Each connection is short-lived (context manager) to avoid 'check_same_thread'
    issues in multi-threaded executor pools.
  - SQLite WAL journal mode enables concurrent reads with single-writer semantics.

Relationship to Tanenbaum & Van Steen Ch. 7 (Consistency & Replication):
  Idempotency is guaranteed because mark_processed() is atomic — a duplicate INSERT
  always raises IntegrityError which we catch and return False, ensuring each
  (topic, event_id) pair is stored exactly once regardless of retry count.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)


class DedupStore:
    """
    Thread-safe, restart-persistent deduplication store.

    Primary key: (topic, event_id) — composite key allows the same event_id
    on different topics to be treated as independent events (supports
    multi-service environments where UUIDs may not be globally scoped).
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._init_db()
        logger.info("DedupStore initialised — path=%s", self.db_path)

    # ── Initialisation ──────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS processed_events (
                    topic          TEXT NOT NULL,
                    event_id       TEXT NOT NULL,
                    source         TEXT NOT NULL,
                    event_ts       TEXT NOT NULL,
                    payload        TEXT NOT NULL DEFAULT '{}',
                    processed_at   TEXT NOT NULL,
                    PRIMARY KEY (topic, event_id)
                );

                CREATE INDEX IF NOT EXISTS idx_topic
                    ON processed_events (topic);
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    # ── Public API ──────────────────────────────────────────────────────────

    def mark_processed(
        self,
        topic: str,
        event_id: str,
        source: str,
        event_ts: str,
        payload: dict,
    ) -> bool:
        """
        Atomically attempt to record the event as processed.

        Returns:
            True  — event was newly inserted (unique event, process it).
            False — IntegrityError on PRIMARY KEY (duplicate, discard it).

        This is the single point of idempotency enforcement.
        The SQLite PRIMARY KEY constraint guarantees that even under concurrent
        execution (multiple run_in_executor threads), only one INSERT succeeds.
        """
        processed_at = datetime.now(timezone.utc).isoformat()
        payload_json = json.dumps(payload, ensure_ascii=False)

        with self._lock:
            with self._connect() as conn:
                try:
                    conn.execute(
                        """
                        INSERT INTO processed_events
                            (topic, event_id, source, event_ts, payload, processed_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (topic, event_id, source, event_ts, payload_json, processed_at),
                    )
                    conn.commit()
                    return True
                except sqlite3.IntegrityError:
                    # PRIMARY KEY violation → duplicate event
                    return False

    def is_processed(self, topic: str, event_id: str) -> bool:
        """Read-only duplicate check (used for early-exit optimisation)."""
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
                    (topic, event_id),
                ).fetchone()
        return row is not None

    def get_events_by_topic(self, topic: str) -> List[dict]:
        """Return all unique processed events for a given topic, ordered by ingestion time."""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT topic, event_id, source, event_ts, payload, processed_at
                      FROM processed_events
                     WHERE topic = ?
                     ORDER BY processed_at ASC
                    """,
                    (topic,),
                ).fetchall()
        return [
            {
                "topic":        row["topic"],
                "event_id":     row["event_id"],
                "source":       row["source"],
                "timestamp":    row["event_ts"],
                "payload":      json.loads(row["payload"]),
                "processed_at": row["processed_at"],
            }
            for row in rows
        ]

    def get_all_topics(self) -> List[str]:
        """Return sorted list of distinct topics in the store."""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
                ).fetchall()
        return [row["topic"] for row in rows]

    def count_unique(self) -> int:
        """Return total number of unique processed events (used on startup to restore counter)."""
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) AS cnt FROM processed_events"
                ).fetchone()
        return row["cnt"]
