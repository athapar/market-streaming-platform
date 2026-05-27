"""Tests for event routing and envelope serialization."""
import json

from market_streaming.producer.envelope import (
    TOPIC_AGGREGATES,
    TOPIC_QUOTES,
    TOPIC_TRADES,
    Envelope,
    route_event,
)

# -- route_event --------------------------------------------------------

def test_route_am_event():
    event = {"ev": "AM", "sym": "AAPL", "o": 150.0, "c": 151.0}
    env = route_event(event)
    assert env is not None
    assert env.topic == TOPIC_AGGREGATES
    assert env.key == "AAPL"
    assert json.loads(env.value) == event


def test_route_trade_event():
    event = {"ev": "T", "sym": "MSFT", "p": 400.0, "s": 100}
    env = route_event(event)
    assert env is not None
    assert env.topic == TOPIC_TRADES
    assert env.key == "MSFT"


def test_route_quote_event():
    event = {"ev": "Q", "sym": "NVDA", "bp": 900.0, "ap": 900.05}
    env = route_event(event)
    assert env is not None
    assert env.topic == TOPIC_QUOTES
    assert env.key == "NVDA"


def test_route_second_agg_goes_to_aggregates_topic():
    event = {"ev": "A", "sym": "SPY", "o": 500.0}
    env = route_event(event)
    assert env is not None
    assert env.topic == TOPIC_AGGREGATES


def test_route_status_event_returns_none():
    event = {"ev": "status", "status": "connected"}
    assert route_event(event) is None


def test_route_unknown_event_type_returns_none():
    event = {"ev": "UNKNOWN", "sym": "AAPL"}
    assert route_event(event) is None


def test_route_missing_symbol_returns_none():
    event = {"ev": "AM"}
    assert route_event(event) is None


def test_route_empty_symbol_returns_none():
    event = {"ev": "T", "sym": ""}
    assert route_event(event) is None


def test_route_preserves_raw_json_verbatim():
    event = {"ev": "T", "sym": "AAPL", "p": 150.123456789, "extra": [1, 2, 3]}
    env = route_event(event)
    parsed = json.loads(env.value)
    assert parsed["p"] == 150.123456789
    assert parsed["extra"] == [1, 2, 3]


def test_route_generates_trace_id():
    event = {"ev": "AM", "sym": "AAPL", "o": 150.0}
    env = route_event(event)
    assert len(env.trace_id) == 12
    assert env.trace_id.isalnum()


def test_route_generates_unique_trace_ids():
    event = {"ev": "AM", "sym": "AAPL", "o": 150.0}
    ids = {route_event(event).trace_id for _ in range(100)}
    assert len(ids) == 100


# -- Envelope serialization ---------------------------------------------

def test_envelope_spillover_roundtrip():
    env = Envelope(topic="market.trades", key="AAPL", value='{"a":1}',
                   trace_id="abc123def456")
    record = env.to_spillover_record()
    restored = Envelope.from_spillover_record(record)
    assert restored == env


def test_envelope_spillover_missing_trace_id():
    record = {"topic": "market.trades", "key": "AAPL", "value": '{"a":1}'}
    restored = Envelope.from_spillover_record(record)
    assert restored.trace_id == ""
    assert restored.topic == "market.trades"
