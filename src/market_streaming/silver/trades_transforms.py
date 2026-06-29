"""
Silver layer for trades: Bronze Delta → typed, deduped, FIGI-joined Delta.

Polygon T.* (trade) events are tick-level: one event per executed trade. The
natural dedup key is (symbol, trade_id) — trade_id is globally unique per
executed trade. Duplicates arise from producer retries or Kafka exactly-once
boundary conditions; the MERGE absorbs them idempotently.

Timestamps: Polygon's realtime endpoint sends SIP and exchange timestamps as
epoch NANOSECONDS. The DELAYED endpoint (wss://delayed.polygon.io) sends them
as epoch MILLISECONDS instead — the docs do not call this out. Parse with
_epoch_to_timestamp() which auto-detects by magnitude (ns ≳ 10^15, ms ≲ 10^15).

Same architectural pattern as the AM Silver (foreachBatch + MERGE), different
schema and merge key.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from market_streaming.transform_utils import build_merge_condition, dedup_keep_latest

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


TRADES_PAYLOAD_SCHEMA = StructType([
    StructField("ev",  StringType(),  True),   # "T"
    StructField("sym", StringType(),  True),   # ticker
    StructField("x",   IntegerType(), True),   # exchange ID
    StructField("i",   StringType(),  True),   # trade ID (unique per trade)
    StructField("z",   IntegerType(), True),   # tape (1=A, 2=B, 3=C)
    StructField("p",   DoubleType(),  True),   # price
    StructField("s",   IntegerType(), True),   # size (shares)
    StructField("t",   LongType(),    True),   # SIP timestamp (epoch ns or ms — auto-detected)
    StructField("y",   LongType(),    True),   # exchange timestamp (epoch ns or ms)
    StructField("q",   LongType(),    True),   # sequence number
])


def _epoch_to_timestamp(col):
    """Cast an epoch-integer column to TimestampType, auto-detecting ns vs ms.

    Polygon's realtime feed uses nanoseconds; the delayed feed uses
    milliseconds for the same fields. A clean magnitude threshold separates
    them: any sensible recent date is ~1e12 in ms and ~1e18 in ns, so 1e15
    splits them with 3 orders of magnitude of headroom on either side.
    """
    return (
        F.when(col > F.lit(10**15), col / F.lit(1e9))
         .otherwise(col / F.lit(1e3))
         .cast(TimestampType())
    )

SILVER_TRADES_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "trade_id",
    "trade_price",
    "trade_size",
    "exchange_id",
    "tape",
    "sip_timestamp",
    "exchange_timestamp",
    "sequence_number",
    "trade_date",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "bronze_ingest_timestamp",
    "silver_timestamp",
]

MERGE_KEYS: list[str] = ["symbol", "trade_id"]

# Partition column — added to the MERGE condition for partition pruning (OPT-3).
# A given (symbol, trade_id) always carries the same trade_date, so it never
# alters match semantics.
PARTITION_COL: str = "trade_date"


def parse_trades(df: "DataFrame") -> "DataFrame":
    """Parse Bronze raw_payload JSON → typed Silver trades columns."""
    return (
        df
        .withColumn("_j", F.from_json(F.col("raw_payload"), TRADES_PAYLOAD_SCHEMA))
        .withColumn("symbol",             F.col("_j.sym"))
        .withColumn("trade_id",           F.col("_j.i"))
        .withColumn("trade_price",        F.col("_j.p"))
        .withColumn("trade_size",         F.col("_j.s"))
        .withColumn("exchange_id",        F.col("_j.x"))
        .withColumn("tape",              F.col("_j.z"))
        .withColumn("sip_timestamp",      _epoch_to_timestamp(F.col("_j.t")))
        .withColumn("exchange_timestamp", _epoch_to_timestamp(F.col("_j.y")))
        .withColumn("sequence_number",    F.col("_j.q"))
        .withColumn("trade_date",         F.to_date(F.col("sip_timestamp")))
        .withColumn("bronze_ingest_timestamp", F.col("ingest_timestamp"))
        .withColumn("silver_timestamp",   F.current_timestamp())
        .drop("_j", "raw_payload", "ingest_date", "ingest_timestamp",
              "kafka_timestamp", "event_type")
        .filter(F.col("symbol").isNotNull() & F.col("trade_id").isNotNull())
    )


def join_security_master(df: "DataFrame", seed_df: "DataFrame") -> "DataFrame":
    """Broadcast-join symbol → composite_figi from the static seed Parquet."""
    figi_map = seed_df.select(
        F.col("symbol").alias("_seed_symbol"),
        F.col("composite_figi"),
    )
    return (
        df
        .join(F.broadcast(figi_map), df["symbol"] == figi_map["_seed_symbol"], "left")
        .drop("_seed_symbol")
    )


def silver_trades_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi          STRING,
  symbol                  STRING        NOT NULL,
  trade_id                STRING        NOT NULL,
  trade_price             DOUBLE        NOT NULL,
  trade_size              INTEGER       NOT NULL,
  exchange_id             INTEGER,
  tape                    INTEGER,
  sip_timestamp           TIMESTAMP     NOT NULL,
  exchange_timestamp      TIMESTAMP,
  sequence_number         BIGINT,
  trade_date              DATE,
  kafka_topic             STRING,
  kafka_partition         INTEGER,
  kafka_offset            BIGINT,
  bronze_ingest_timestamp TIMESTAMP,
  silver_timestamp        TIMESTAMP
)
USING DELTA
PARTITIONED BY (trade_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'           = 'true',
  'delta.autoOptimize.optimizeWrite'     = 'true',
  'delta.autoOptimize.autoCompact'       = 'true'
)
""".strip()


def merge_trades_batch(
    batch_df: "DataFrame",
    batch_id: int,
    target_table: str,
    metrics_table: str | None = None,
) -> None:
    """foreachBatch handler: dedup within batch then MERGE into Silver trades."""
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession

    def _do_merge(tracker=None):
        # OPT-4 struct-max dedup. No persist() — cache is rejected on Databricks
        # serverless ([NOT_SUPPORTED_WITH_SERVERLESS]); OPT-2's win here is just
        # dropping the redundant isEmpty() action.
        deduped = dedup_keep_latest(batch_df, MERGE_KEYS)

        rows_out = deduped.count()
        if rows_out == 0:
            if tracker:
                tracker.record(rows_in=0, rows_out=0)
            return

        if tracker:
            tracker.record(rows_in=batch_df.count(), rows_out=rows_out)

        dt = DeltaTable.forName(spark, target_table)
        # OPT-3: lead with the partition column so Delta prunes partitions.
        (
            dt.alias("tgt")
            .merge(
                deduped.alias("src"),
                build_merge_condition(MERGE_KEYS, PARTITION_COL),
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    if metrics_table:
        from market_streaming.observability.pipeline_metrics import track_batch
        with track_batch(spark, metrics_table, "silver_trades", batch_id) as tracker:
            _do_merge(tracker)
    else:
        _do_merge()


def build_silver_trades_stream(
    spark: "SparkSession",
    bronze_table: str,
    seed_path: str,
    target_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 30,
    metrics_table: str | None = None,
) -> "StreamingQuery":
    """Wire Bronze Delta (trades only) → parse → FIGI join → MERGE into Silver."""
    seed_df = spark.read.parquet(seed_path)

    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreDeletes", "true")
        .table(bronze_table)
    )

    silver_stream = (
        bronze_stream
        .filter(F.col("kafka_topic") == "market.trades")
        .transform(parse_trades)
        .transform(lambda df: join_security_master(df, seed_df))
        .select(SILVER_TRADES_COLUMNS)
    )

    writer = (
        silver_stream.writeStream
        .format("delta")
        .foreachBatch(
            lambda df, bid: merge_trades_batch(df, bid, target_table, metrics_table)
        )
        .option("checkpointLocation", checkpoint_path)
    )

    if trigger_type == "availableNow":
        writer = writer.trigger(availableNow=True)
    elif trigger_type == "once":
        writer = writer.trigger(once=True)
    else:
        writer = writer.trigger(processingTime=f"{trigger_seconds} seconds")

    return writer.start()
