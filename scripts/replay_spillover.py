"""
Replay spillover NDJSON files into Kafka.

Reads every .ndjson file under data/spillover/, re-publishes each envelope via
the same Kafka producer config the live producer uses, then renames the file
with a .replayed suffix on success.

Re-runs are safe: a .replayed file is not picked up again. A crash mid-file
means the file stays .ndjson and the whole file replays from line 1 on the
next invocation; downstream Silver dedup handles the resulting duplicates.
"""
from __future__ import annotations

import sys

from confluent_kafka import Producer

from market_streaming.config import SPILLOVER_DIR, require_env
from market_streaming.producer.envelope import Envelope
from market_streaming.producer.kafka_sink import build_producer_config
from market_streaming.producer.spillover import replay_all


def _publish_factory(producer: Producer):
    failures = {"count": 0}

    def cb(err, _msg):
        if err is not None:
            failures["count"] += 1
            print(f"replay delivery failed: {err}", file=sys.stderr)

    def publish(envelope: Envelope) -> None:
        try:
            producer.produce(
                topic=envelope.topic,
                key=envelope.key,
                value=envelope.value,
                on_delivery=cb,
            )
        except BufferError:
            producer.flush(5.0)
            producer.produce(
                topic=envelope.topic,
                key=envelope.key,
                value=envelope.value,
                on_delivery=cb,
            )
        producer.poll(0)

    return publish, failures


def main() -> int:
    producer = Producer(
        build_producer_config(
            bootstrap_servers=require_env("KAFKA_BOOTSTRAP_SERVERS"),
            sasl_username=require_env("KAFKA_SASL_USERNAME"),
            sasl_password=require_env("KAFKA_SASL_PASSWORD"),
            client_id="polygon-producer-replay",
        )
    )
    publish, failures = _publish_factory(producer)
    counts = replay_all(SPILLOVER_DIR, publish)
    producer.flush(30.0)
    print(f"replayed {counts['envelopes']} envelopes across {counts['files']} files; "
          f"{failures['count']} delivery failures")
    return 0 if failures["count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
