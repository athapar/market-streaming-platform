"""
Confluent Cloud Kafka producer wrapper.

Config rationale:
- acks=all + enable.idempotence=true: tolerate broker leader failover without
  losing or duplicating messages on the broker side. This is what makes the
  Bronze layer's exactly-once guarantee possible end-to-end alongside Delta's
  atomic commits and Spark checkpointing.
- compression.type=lz4: cheap CPU, halves bandwidth on JSON payloads.
- linger.ms=50: small batching window — under steady state we get good
  throughput; under sparse load we never wait more than 50ms to flush.

Delivery failures (broker unreachable past retry deadline, queue full, etc.)
write the envelope to spillover. The replay script picks them up later.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, Protocol

from confluent_kafka import KafkaError, Producer

from market_streaming.observability.alerts import alert_kafka_failures
from market_streaming.producer.envelope import Envelope
from market_streaming.producer.metrics import Metrics

log = logging.getLogger(__name__)


class SpilloverSink(Protocol):
    def write(self, envelope: Envelope) -> object: ...


def build_producer_config(
    bootstrap_servers: str,
    sasl_username: str,
    sasl_password: str,
    client_id: str = "polygon-producer",
) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": sasl_username,
        "sasl.password": sasl_password,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
        "message.send.max.retries": 10,
        "retry.backoff.ms": 200,
    }


class KafkaSink:
    def __init__(
        self,
        producer: Producer,
        spillover: SpilloverSink,
        metrics: Metrics,
    ) -> None:
        self._producer = producer
        self._spillover = spillover
        self._metrics = metrics

    def _make_callback(self, envelope: Envelope) -> Callable[[KafkaError | None, object], None]:
        def cb(err: KafkaError | None, _msg: object) -> None:
            if err is None:
                self._metrics.mark_kafka_ack()
                return
            self._metrics.mark_kafka_fail()
            log.warning("kafka delivery failed: %s; spilling envelope", err)
            self._spillover.write(envelope)
            self._metrics.mark_spillover()
            if self._metrics.kafka_failed > 0 and self._metrics.kafka_failed % 100 == 0:
                alert_kafka_failures(self._metrics.kafka_failed, str(err))
        return cb

    def publish(self, envelope: Envelope) -> None:
        t0 = time.monotonic()
        headers = {"trace_id": envelope.trace_id.encode()} if envelope.trace_id else None
        try:
            self._producer.produce(
                topic=envelope.topic,
                key=envelope.key,
                value=envelope.value,
                headers=headers,
                on_delivery=self._make_callback(envelope),
            )
            self._metrics.mark_kafka_produced()
            self._metrics.observe_produce_latency(time.monotonic() - t0)
        except BufferError:
            log.warning("local producer queue full; spilling envelope")
            self._spillover.write(envelope)
            self._metrics.mark_spillover()
        self._producer.poll(0)

    def flush(self, timeout_s: float = 10.0) -> int:
        return self._producer.flush(timeout_s)


class DryRunSink:
    """Stand-in for KafkaSink that prints instead of producing. Used by
    --dry-run so the Polygon WS side can be validated without Confluent."""

    def __init__(self, metrics: Metrics, sample_every: int = 100) -> None:
        self._metrics = metrics
        self._sample_every = sample_every

    def publish(self, envelope: Envelope) -> None:
        self._metrics.mark_kafka_produced()
        self._metrics.mark_kafka_ack()
        if self._metrics.kafka_produced % self._sample_every == 1:
            print(f"[dry-run sample] topic={envelope.topic} key={envelope.key} value={envelope.value[:200]}")

    def flush(self, timeout_s: float = 10.0) -> int:
        return 0
