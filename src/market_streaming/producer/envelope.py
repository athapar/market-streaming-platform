"""
Kafka message envelope.

Bronze stores raw_payload as STRING (spec §2.8) so the producer should not
mutate Polygon's JSON. We emit one Kafka message per Polygon event with:
    key   = symbol               (preserves per-symbol ordering within a partition)
    value = original event JSON  (verbatim, what Polygon sent)

Channel scope: minute aggregates (AM), trades (T), and quotes (Q). The Stocks
Advanced plan provides real-time access to all three channels. The routing table
maps each Polygon event type to a Kafka topic. Bronze ingests all topics via
subscribePattern and stores raw JSON — schema differences between AM/T/Q are
handled in the Silver layer.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from typing import Any

EVENT_TYPE_MINUTE_AGG = "AM"
EVENT_TYPE_SECOND_AGG = "A"
EVENT_TYPE_TRADE = "T"
EVENT_TYPE_QUOTE = "Q"
EVENT_TYPE_STATUS = "status"

TOPIC_AGGREGATES = "market.aggregates"
TOPIC_TRADES = "market.trades"
TOPIC_QUOTES = "market.quotes"

ACTIVE_TOPICS: tuple[str, ...] = (TOPIC_AGGREGATES, TOPIC_TRADES, TOPIC_QUOTES)

_EVENT_TO_TOPIC: dict[str, str] = {
    EVENT_TYPE_MINUTE_AGG: TOPIC_AGGREGATES,
    EVENT_TYPE_SECOND_AGG: TOPIC_AGGREGATES,
    EVENT_TYPE_TRADE: TOPIC_TRADES,
    EVENT_TYPE_QUOTE: TOPIC_QUOTES,
}


@dataclass(frozen=True)
class Envelope:
    topic: str
    key: str
    value: str
    trace_id: str = ""

    def to_spillover_record(self) -> dict[str, str]:
        return {"topic": self.topic, "key": self.key, "value": self.value,
                "trace_id": self.trace_id}

    @classmethod
    def from_spillover_record(cls, record: dict[str, str]) -> "Envelope":
        return cls(topic=record["topic"], key=record["key"], value=record["value"],
                   trace_id=record.get("trace_id", ""))


def route_event(event: dict[str, Any]) -> Envelope | None:
    """
    Convert a single Polygon event dict to an Envelope, or None if the event
    type isn't routable (status frames, unknown types, missing symbol).
    """
    topic = _EVENT_TO_TOPIC.get(event.get("ev", ""))
    if topic is None:
        return None
    symbol = event.get("sym")
    if not symbol:
        return None
    trace_id = uuid.uuid4().hex[:12]
    return Envelope(
        topic=topic, key=symbol,
        value=json.dumps(event, separators=(",", ":")),
        trace_id=trace_id,
    )
