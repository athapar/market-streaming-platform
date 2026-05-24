"""
Create market.trades and market.quotes topics on Confluent Cloud.

Idempotent: if a topic already exists, the Admin API returns a TopicExists
error which we treat as success.

Defaults match what the Bronze streaming job expects:
  - 6 partitions (over our 5-symbol universe this leaves room for SPY/QQQ-heavy
    partitions to spread out across a 2-3 executor cluster later)
  - 7-day retention (Confluent Free tier limit; long enough for any reasonable
    bug-fix replay window)
"""
from __future__ import annotations

import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError, KafkaException

from market_streaming.config import require_env
from market_streaming.producer.envelope import ACTIVE_TOPICS

RETENTION_7D_MS = str(7 * 24 * 60 * 60 * 1000)


def _admin_config() -> dict[str, str]:
    return {
        "bootstrap.servers": require_env("KAFKA_BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": require_env("KAFKA_SASL_USERNAME"),
        "sasl.password": require_env("KAFKA_SASL_PASSWORD"),
    }


def create_topics(partitions: int, retention_ms: str) -> int:
    admin = AdminClient(_admin_config())
    topics = [
        NewTopic(
            topic=name,
            num_partitions=partitions,
            replication_factor=3,
            config={"retention.ms": retention_ms},
        )
        for name in ACTIVE_TOPICS
    ]
    futures = admin.create_topics(topics)
    exit_code = 0
    for name, fut in futures.items():
        try:
            fut.result()
            print(f"created topic: {name}")
        except KafkaException as exc:
            err = exc.args[0]
            if isinstance(err, KafkaError) and err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"topic already exists (ok): {name}")
            else:
                print(f"failed to create topic {name}: {exc}", file=sys.stderr)
                exit_code = 1
    return exit_code


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--partitions", type=int, default=6)
    p.add_argument("--retention-ms", default=RETENTION_7D_MS)
    args = p.parse_args()
    return create_topics(args.partitions, args.retention_ms)


if __name__ == "__main__":
    raise SystemExit(main())
