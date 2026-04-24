"""
models.py — Pydantic data models for the Pub-Sub Log Aggregator.

Event JSON schema (minimal):
  {
    "topic":     string   — routing key, e.g. "auth.user.login"
    "event_id":  string   — globally unique, e.g. UUID v4 prefixed with source+timestamp
    "timestamp": string   — ISO 8601, e.g. "2024-05-07T10:30:00Z"
    "source":    string   — originating service / host
    "payload":   object   — arbitrary event data
  }
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from pydantic import BaseModel, Field, field_validator, model_validator


class Event(BaseModel):
    """Single event unit published to the aggregator."""

    topic: str = Field(..., min_length=1, description="Routing topic, e.g. 'auth.user.login'")
    event_id: str = Field(..., min_length=1, description="Unique event identifier (UUID v4 recommended)")
    timestamp: str = Field(..., description="ISO 8601 timestamp of when the event occurred")
    source: str = Field(..., min_length=1, description="Service or host that produced this event")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Arbitrary event data")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        normalised = v.replace("Z", "+00:00")
        try:
            datetime.fromisoformat(normalised)
        except ValueError as exc:
            raise ValueError(
                f"timestamp must be ISO 8601 (e.g. '2024-05-07T10:30:00Z'), got: {v!r}"
            ) from exc
        return v

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("event_id must not be blank or whitespace-only")
        return stripped

    @field_validator("topic")
    @classmethod
    def validate_topic(cls, v: str) -> str:
        stripped = v.strip()
        if not stripped:
            raise ValueError("topic must not be blank or whitespace-only")
        return stripped


class BatchPublishRequest(BaseModel):
    """Batch wrapper for multiple events in a single POST /publish call."""

    events: List[Event] = Field(..., min_length=1, description="List of events (at least 1)")


class PublishResponse(BaseModel):
    accepted: int
    message: str


class StatsResponse(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
