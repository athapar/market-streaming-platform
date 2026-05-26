"""Tests for producer metrics (internal counters + Prometheus integration)."""
from market_streaming.producer.metrics import Metrics


def test_initial_counters_are_zero():
    m = Metrics()
    assert m.polygon_events_received == 0
    assert m.kafka_produced == 0
    assert m.kafka_acked == 0
    assert m.kafka_failed == 0
    assert m.spillover_written == 0
    assert m.reconnects == 0


def test_mark_polygon_event_increments():
    m = Metrics()
    m.mark_polygon_event("AM")
    m.mark_polygon_event("T")
    m.mark_polygon_event("Q")
    assert m.polygon_events_received == 3
    assert m.last_polygon_event_ts is not None


def test_mark_kafka_produced_increments():
    m = Metrics()
    m.mark_kafka_produced()
    m.mark_kafka_produced()
    assert m.kafka_produced == 2


def test_mark_kafka_ack_increments():
    m = Metrics()
    m.mark_kafka_ack()
    assert m.kafka_acked == 1
    assert m.last_kafka_ack_ts is not None


def test_mark_kafka_fail_increments():
    m = Metrics()
    m.mark_kafka_fail()
    m.mark_kafka_fail()
    assert m.kafka_failed == 2


def test_mark_spillover_increments():
    m = Metrics()
    m.mark_spillover()
    assert m.spillover_written == 1


def test_mark_reconnect_increments():
    m = Metrics()
    m.mark_reconnect()
    m.mark_reconnect()
    assert m.reconnects == 2


def test_snapshot_returns_all_fields():
    m = Metrics()
    m.mark_polygon_event("AM")
    m.mark_kafka_ack()
    snap = m.snapshot()

    assert "polygon_events_received" in snap
    assert "kafka_acked" in snap
    assert "last_polygon_event_age_s" in snap
    assert "last_kafka_ack_age_s" in snap
    assert snap["polygon_events_received"] == 1
    assert snap["kafka_acked"] == 1


def test_snapshot_age_is_none_before_first_event():
    m = Metrics()
    snap = m.snapshot()
    assert snap["last_polygon_event_age_s"] is None
    assert snap["last_kafka_ack_age_s"] is None


def test_observe_produce_latency_does_not_raise():
    m = Metrics()
    m.observe_produce_latency(0.005)
    m.observe_produce_latency(0.001)
