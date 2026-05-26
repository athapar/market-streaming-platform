"""
Counters, Prometheus metrics, and periodic heartbeat output.

Dual-track observability:
- Internal counters + stdout heartbeat for terminal monitoring
- Prometheus counters/histograms/gauges scraped by Grafana for dashboards

The Prometheus HTTP server starts on --metrics-port (default 9090) when the
producer runs in live mode (not --dry-run).
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# ---------------------------------------------------------------------------
# Prometheus metrics (module-level singletons, registered on import)
# ---------------------------------------------------------------------------

PROM_EVENTS_RECEIVED = Counter(
    "polygon_events_received_total",
    "Total events received from Polygon WebSocket",
    ["event_type"],
)

PROM_KAFKA_DELIVERY = Counter(
    "kafka_delivery_total",
    "Kafka delivery outcomes",
    ["status"],
)

PROM_KAFKA_PRODUCE_LATENCY = Histogram(
    "kafka_produce_latency_seconds",
    "Time from event receipt to Kafka produce() call",
    buckets=(0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

PROM_IN_FLIGHT = Gauge(
    "kafka_in_flight_messages",
    "Messages produced but not yet acknowledged by the broker",
)

PROM_LAST_EVENT_AGE = Gauge(
    "polygon_last_event_age_seconds",
    "Seconds since the last Polygon event was received (staleness detector)",
)

PROM_SPILLOVER = Counter(
    "spillover_events_total",
    "Events written to NDJSON spillover due to Kafka unavailability",
)

PROM_RECONNECTS = Counter(
    "websocket_reconnects_total",
    "WebSocket reconnection attempts after disconnect",
)

PROM_UPTIME = Gauge(
    "producer_uptime_seconds",
    "Seconds since the producer started",
)


def start_metrics_server(port: int = 9090) -> None:
    """Start the Prometheus HTTP metrics endpoint."""
    start_http_server(port)


# ---------------------------------------------------------------------------
# Internal metrics (stdout heartbeat + final snapshot)
# ---------------------------------------------------------------------------

@dataclass
class Metrics:
    polygon_events_received: int = 0
    polygon_status_frames: int = 0
    kafka_produced: int = 0
    kafka_acked: int = 0
    kafka_failed: int = 0
    spillover_written: int = 0
    reconnects: int = 0
    last_polygon_event_ts: float | None = field(default=None)
    last_kafka_ack_ts: float | None = field(default=None)
    _start_ts: float = field(default_factory=time.monotonic)

    def mark_polygon_event(self, event_type: str = "unknown") -> None:
        self.polygon_events_received += 1
        self.last_polygon_event_ts = time.monotonic()
        PROM_EVENTS_RECEIVED.labels(event_type=event_type).inc()

    def mark_kafka_produced(self) -> None:
        self.kafka_produced += 1
        PROM_IN_FLIGHT.inc()

    def mark_kafka_ack(self) -> None:
        self.kafka_acked += 1
        self.last_kafka_ack_ts = time.monotonic()
        PROM_KAFKA_DELIVERY.labels(status="ack").inc()
        PROM_IN_FLIGHT.dec()

    def mark_kafka_fail(self) -> None:
        self.kafka_failed += 1
        PROM_KAFKA_DELIVERY.labels(status="fail").inc()
        PROM_IN_FLIGHT.dec()

    def mark_spillover(self) -> None:
        self.spillover_written += 1
        PROM_SPILLOVER.inc()

    def mark_reconnect(self) -> None:
        self.reconnects += 1
        PROM_RECONNECTS.inc()

    def observe_produce_latency(self, seconds: float) -> None:
        PROM_KAFKA_PRODUCE_LATENCY.observe(seconds)

    def snapshot(self) -> dict[str, float | int | None]:
        now = time.monotonic()
        PROM_UPTIME.set(now - self._start_ts)
        if self.last_polygon_event_ts is not None:
            PROM_LAST_EVENT_AGE.set(now - self.last_polygon_event_ts)
        return {
            "polygon_events_received": self.polygon_events_received,
            "polygon_status_frames": self.polygon_status_frames,
            "kafka_produced": self.kafka_produced,
            "kafka_acked": self.kafka_acked,
            "kafka_failed": self.kafka_failed,
            "spillover_written": self.spillover_written,
            "reconnects": self.reconnects,
            "last_polygon_event_age_s": (
                round(now - self.last_polygon_event_ts, 2)
                if self.last_polygon_event_ts is not None
                else None
            ),
            "last_kafka_ack_age_s": (
                round(now - self.last_kafka_ack_ts, 2)
                if self.last_kafka_ack_ts is not None
                else None
            ),
        }


async def heartbeat_loop(metrics: Metrics, interval_s: float = 10.0) -> None:
    while True:
        await asyncio.sleep(interval_s)
        snap = metrics.snapshot()
        parts = [f"{k}={v}" for k, v in snap.items()]
        print("[heartbeat] " + " ".join(parts), flush=True)
