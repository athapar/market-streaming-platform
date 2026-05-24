"""
Kafka message envelope.

Bronze stores raw_payload as STRING (spec §2.8) so the producer should not
mutate Polygon's JSON. We emit one Kafka message per Polygon event with:
    key   = symbol               (preserves per-symbol ordering within a partition)
    value = original event JSON  (verbatim, what Polygon sent)

Current channel scope: minute aggregates (AM) only. The Stocks Starter plan
includes delayed AM events but not T (trades) or Q (quotes); see
docs/phase1_provisioning.md. The routing table below is structured so that
upgrading the plan and adding T/Q is a one-line change in route_event plus a
channel config update — no architectural refactor needed.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

EVENT_TYPE_MINUTE_AGG = "AM"
EVENT_TYPE_SECOND_AGG = "A"
EVENT_TYPE_TRADE = "T"  # not yet enabled; needs plan upgrade
EVENT_TYPE_QUOTE = "Q"  # not yet enabled; needs plan upgrade
EVENT_TYPE_STATUS = "status"

TOPIC_AGGREGATES = "market.aggregates"
TOPIC_TRADES = "market.trades"  # reserved; unused until plan upgrade
TOPIC_QUOTES = "market.quotes"  # reserved; unused until plan upgrade

# Active topics this producer publishes to. Topic-creation and Bronze ingestion
# read this list, so adding T/Q later means appending entries here.
ACTIVE_TOPICS: tuple[str, ...] = (TOPIC_AGGREGATES,)

_EVENT_TO_TOPIC: dict[str, str] = {
    EVENT_TYPE_MINUTE_AGG: TOPIC_AGGREGATES,
    EVENT_TYPE_SECOND_AGG: TOPIC_AGGREGATES,
    # EVENT_TYPE_TRADE: TOPIC_TRADES,
    # EVENT_TYPE_QUOTE: TOPIC_QUOTES,
}


@dataclass(frozen=True)
class Envelope:
    topic: str
    key: str
    value: str

    def to_spillover_record(self) -> dict[str, str]:
        return {"topic": self.topic, "key": self.key, "value": self.value}

    @classmethod
    def from_spillover_record(cls, record: dict[str, str]) -> "Envelope":
        return cls(topic=record["topic"], key=record["key"], value=record["value"])


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
    return Envelope(topic=topic, key=symbol, value=json.dumps(event, separators=(",", ":")))
