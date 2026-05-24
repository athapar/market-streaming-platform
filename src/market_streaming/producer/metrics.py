"""
Counters and periodic heartbeat output.

The heartbeat is intentionally stdout-only; for a Phase 1 deliverable we want
the run signal in front of you while watching a terminal, not in a metrics
backend. Wire Prometheus later if the producer graduates.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field


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

    def mark_polygon_event(self) -> None:
        self.polygon_events_received += 1
        self.last_polygon_event_ts = time.monotonic()

    def mark_kafka_ack(self) -> None:
        self.kafka_acked += 1
        self.last_kafka_ack_ts = time.monotonic()

    def snapshot(self) -> dict[str, float | int | None]:
        now = time.monotonic()
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
