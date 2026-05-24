"""
Bronze layer: Kafka -> Delta, raw events, append-only.

Design constraints (spec §2.8, §5.5):
- Never parse Polygon's JSON. raw_payload stays STRING so a schema change at
  the source can't break the write. event_type is extracted with json path
  *only* so the table can be partitioned/filtered by it; we never assume more.
- Append-only. Dedup happens in Silver. Bronze is the audit log used to debug
  what actually arrived, and the replay source if Silver/Gold need rebuilding.
- Exactly-once-from-Kafka requires the checkpoint location + an atomic Delta
  commit. Both are wired here; do not change without understanding the
  trade-off.

Pure functions so they're unit-testable without a Databricks cluster. The
notebook is a thin wrapper.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


BRONZE_COLUMNS: tuple[str, ...] = (
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "raw_payload",
    "ingest_timestamp",
    "event_type",
    "ingest_date",
)


def kafka_jaas_config(username: str, password: str) -> str:
    return (
        'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="{username}" password="{password}";'
    )


def enrich_bronze(raw_kafka_df: "DataFrame") -> "DataFrame":
    """
    Project the raw Kafka source DataFrame into the Bronze schema.

    Input columns (Spark Kafka source defaults): key, value, topic, partition,
    offset, timestamp, timestampType. We keep only what's needed for audit and
    add ingest_timestamp/event_type/ingest_date for downstream filtering and
    partitioning.

    event_type is the only thing we extract from the payload — and only via
    json path, never a full parse. If Polygon changes the payload shape,
    event_type may become NULL but the row still lands.
    """
    from pyspark.sql.functions import col, current_timestamp, get_json_object, to_date

    return (
        raw_kafka_df.selectExpr(
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
            "CAST(value AS STRING) AS raw_payload",
        )
        .withColumn("ingest_timestamp", current_timestamp())
        .withColumn("event_type", get_json_object(col("raw_payload"), "$.ev"))
        .withColumn("ingest_date", to_date(col("ingest_timestamp")))
    )


def read_kafka_stream(
    spark: "SparkSession",
    bootstrap_servers: str,
    sasl_username: str,
    sasl_password: str,
    subscribe_pattern: str,
    starting_offsets: str = "latest",
) -> "DataFrame":
    """
    Build the streaming Kafka source.

    subscribePattern (not subscribe) lets us add new market.* topics later
    without re-editing this job. starting_offsets defaults to latest so first
    deploys don't accidentally replay a week of history; for a deliberate
    backfill, delete the checkpoint and set starting_offsets="earliest".
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", kafka_jaas_config(sasl_username, sasl_password))
        .option("subscribePattern", subscribe_pattern)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def write_bronze_stream(
    enriched_df: "DataFrame",
    checkpoint_path: str,
    target_table: str,
    trigger_seconds: int = 10,
) -> "StreamingQuery":
    """
    Append-only write to a pre-created Delta table. Output mode is `append`
    (the only legal mode for an unbounded source without aggregation), and
    mergeSchema is OFF: we want the writer to fail loudly if someone forgets
    to update the DDL when adding a column, rather than silently evolve.
    """
    return (
        enriched_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "false")
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .toTable(target_table)
    )


def build_bronze_stream(
    spark: "SparkSession",
    bootstrap_servers: str,
    sasl_username: str,
    sasl_password: str,
    subscribe_pattern: str,
    checkpoint_path: str,
    target_table: str,
    starting_offsets: str = "latest",
    trigger_seconds: int = 10,
) -> "StreamingQuery":
    raw = read_kafka_stream(
        spark, bootstrap_servers, sasl_username, sasl_password,
        subscribe_pattern, starting_offsets,
    )
    enriched = enrich_bronze(raw)
    return write_bronze_stream(enriched, checkpoint_path, target_table, trigger_seconds)


def bronze_ddl(target_table: str) -> str:
    """
    DDL for the Bronze table. Pre-created with explicit partitioning so the
    streaming writer doesn't have to guess. Partitioned by (ingest_date,
    kafka_topic):
      - ingest_date keeps small Bronze daily slices on disk for cheap pruning
        when replaying or running diagnostics on a specific day.
      - kafka_topic is cheap separation now (we only have market.aggregates)
        and free future-proofing for when T/Q topics come online.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset LONG,
  kafka_timestamp TIMESTAMP,
  raw_payload STRING,
  ingest_timestamp TIMESTAMP,
  event_type STRING,
  ingest_date DATE
)
USING DELTA
PARTITIONED BY (ingest_date, kafka_topic)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""".strip()
