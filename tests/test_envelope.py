import json

from market_streaming.producer.envelope import (
    ACTIVE_TOPICS,
    TOPIC_AGGREGATES,
    Envelope,
    route_event,
)


def test_route_minute_aggregate():
    event = {
        "ev": "AM", "sym": "AAPL", "o": 150.10, "h": 150.25, "l": 150.05,
        "c": 150.20, "v": 1234, "vw": 150.18, "s": 1700000000000, "e": 1700000060000,
    }
    env = route_event(event)
    assert env is not None
    assert env.topic == TOPIC_AGGREGATES
    assert env.key == "AAPL"
    assert json.loads(env.value) == event


def test_route_second_aggregate_also_routed():
    env = route_event({"ev": "A", "sym": "MSFT", "o": 350.1, "s": 1700000000000})
    assert env is not None
    assert env.topic == TOPIC_AGGREGATES
    assert env.key == "MSFT"


def test_route_trade_currently_dropped():
    # Stocks Starter plan doesn't include T/Q; enabling later means adding the
    # mapping in envelope._EVENT_TO_TOPIC. Until then trades are not routed.
    assert route_event({"ev": "T", "sym": "AAPL", "p": 150.0, "s": 100}) is None


def test_route_status_dropped():
    assert route_event({"ev": "status", "status": "connected"}) is None


def test_route_unknown_event_dropped():
    assert route_event({"ev": "XX", "sym": "AAPL"}) is None


def test_route_missing_symbol_dropped():
    assert route_event({"ev": "AM"}) is None


def test_envelope_spillover_roundtrip():
    env = Envelope(topic=TOPIC_AGGREGATES, key="AAPL", value='{"ev":"AM","sym":"AAPL"}')
    restored = Envelope.from_spillover_record(env.to_spillover_record())
    assert restored == env


def test_active_topics_contains_aggregates():
    assert TOPIC_AGGREGATES in ACTIVE_TOPICS
