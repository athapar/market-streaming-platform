"""
Silver layer for quotes: Bronze Delta → typed, deduped, FIGI-joined Delta.

Polygon Q.* (quote) events are NBBO updates: one event per best-bid/best-offer
change. The natural dedup key is (symbol, sip_timestamp, sequence_number) —
the combination is unique per NBBO update for a given symbol.

Timestamps: Polygon's realtime endpoint sends SIP and exchange timestamps as
epoch NANOSECONDS. The DELAYED endpoint (wss://delayed.polygon.io) sends them
as epoch MILLISECONDS instead. _epoch_to_timestamp() auto-detects by magnitude
(ns ≳ 10^15, ms ≲ 10^15).

Quote volume is significantly higher than trades. For 20 liquid symbols,
expect 10-50M+ quote updates per day. The Gold layer pre-aggregates into
per-minute stats to keep Snowflake volume manageable.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark import StorageLevel
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


QUOTES_PAYLOAD_SCHEMA = StructType([
    StructField("ev",  StringType(),  True),   # "Q"
    StructField("sym", StringType(),  True),   # ticker
    StructField("bx",  IntegerType(), True),   # bid exchange ID
    StructField("bp",  DoubleType(),  True),   # bid price
    StructField("bs",  IntegerType(), True),   # bid size (lots)
    StructField("ax",  IntegerType(), True),   # ask exchange ID
    StructField("ap",  DoubleType(),  True),   # ask price
    StructField("as",  IntegerType(), True),   # ask size (lots)
    StructField("t",   LongType(),    True),   # SIP timestamp (epoch ns or ms — auto-detected)
    StructField("y",   LongType(),    True),   # exchange timestamp (epoch ns or ms)
    StructField("q",   LongType(),    True),   # sequence number
    StructField("z",   IntegerType(), True),   # tape (1=A, 2=B, 3=C)
])


def _epoch_to_timestamp(col):
    """Cast an epoch-integer column to TimestampType, auto-detecting ns vs ms.

    Polygon's realtime feed uses nanoseconds; the delayed feed uses
    milliseconds. Magnitude threshold: ms timestamps are ~1e12, ns are ~1e18,
    so 1e15 is a clean separator with orders of magnitude of headroom.
    """
    return (
        F.when(col > F.lit(10**15), col / F.lit(1e9))
         .otherwise(col / F.lit(1e3))
         .cast(TimestampType())
    )

SILVER_QUOTES_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "bid_price",
    "bid_size",
    "bid_exchange_id",
    "ask_price",
    "ask_size",
    "ask_exchange_id",
    "sip_timestamp",
    "exchange_timestamp",
    "sequence_number",
    "tape",
    "quote_date",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "bronze_ingest_timestamp",
    "silver_timestamp",
]

MERGE_KEYS: list[str] = ["symbol", "sip_timestamp", "sequence_number"]

# Partition column — added to the MERGE condition for partition pruning (OPT-3).
# quote_date is derived from sip_timestamp, so it never alters match semantics.
PARTITION_COL: str = "quote_date"


def parse_quotes(df: "DataFrame") -> "DataFrame":
    """Parse Bronze raw_payload JSON → typed Silver quotes columns."""
    return (
        df
        .withColumn("_j", F.from_json(F.col("raw_payload"), QUOTES_PAYLOAD_SCHEMA))
        .withColumn("symbol",             F.col("_j.sym"))
        .withColumn("bid_price",          F.col("_j.bp"))
        .withColumn("bid_size",           F.col("_j.bs"))
        .withColumn("bid_exchange_id",    F.col("_j.bx"))
        .withColumn("ask_price",          F.col("_j.ap"))
        .withColumn("ask_size",           F.col("_j.as"))
        .withColumn("ask_exchange_id",    F.col("_j.ax"))
        .withColumn("sip_timestamp",      _epoch_to_timestamp(F.col("_j.t")))
        .withColumn("exchange_timestamp", _epoch_to_timestamp(F.col("_j.y")))
        .withColumn("sequence_number",    F.col("_j.q"))
        .withColumn("tape",              F.col("_j.z"))
        .withColumn("quote_date",         F.to_date(F.col("sip_timestamp")))
        .withColumn("bronze_ingest_timestamp", F.col("ingest_timestamp"))
        .withColumn("silver_timestamp",   F.current_timestamp())
        .drop("_j", "raw_payload", "ingest_date", "ingest_timestamp",
              "kafka_timestamp", "event_type")
        .filter(
            F.col("symbol").isNotNull()
            & F.col("sip_timestamp").isNotNull()
            & F.col("bid_price").isNotNull()
            & F.col("ask_price").isNotNull()
        )
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


def silver_quotes_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi          STRING,
  symbol                  STRING        NOT NULL,
  bid_price               DOUBLE,
  bid_size                INTEGER,
  bid_exchange_id         INTEGER,
  ask_price               DOUBLE,
  ask_size                INTEGER,
  ask_exchange_id         INTEGER,
  sip_timestamp           TIMESTAMP     NOT NULL,
  exchange_timestamp      TIMESTAMP,
  sequence_number         BIGINT        NOT NULL,
  tape                    INTEGER,
  quote_date              DATE,
  kafka_topic             STRING,
  kafka_partition         INTEGER,
  kafka_offset            BIGINT,
  bronze_ingest_timestamp TIMESTAMP,
  silver_timestamp        TIMESTAMP
)
USING DELTA
PARTITIONED BY (quote_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'           = 'true',
  'delta.autoOptimize.optimizeWrite'     = 'true',
  'delta.autoOptimize.autoCompact'       = 'true'
)
""".strip()


def merge_quotes_batch(
    batch_df: "DataFrame",
    batch_id: int,
    target_table: str,
    metrics_table: str | None = None,
) -> None:
    """foreachBatch handler: dedup within batch then MERGE into Silver quotes."""
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession

    def _do_merge(tracker=None):
        # OPT-4 struct-max dedup + OPT-2 cache once (drops the redundant
        # isEmpty()/count() actions).
        deduped = dedup_keep_latest(batch_df, MERGE_KEYS).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        try:
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
        finally:
            deduped.unpersist()

    if metrics_table:
        from market_streaming.observability.pipeline_metrics import track_batch
        with track_batch(spark, metrics_table, "silver_quotes", batch_id) as tracker:
            _do_merge(tracker)
    else:
        _do_merge()


def build_silver_quotes_stream(
    spark: "SparkSession",
    bronze_table: str,
    seed_path: str,
    target_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 30,
    metrics_table: str | None = None,
) -> "StreamingQuery":
    """Wire Bronze Delta (quotes only) → parse → FIGI join → MERGE into Silver."""
    seed_df = spark.read.parquet(seed_path)

    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreDeletes", "true")
        .table(bronze_table)
    )

    silver_stream = (
        bronze_stream
        .filter(F.col("kafka_topic") == "market.quotes")
        .transform(parse_quotes)
        .transform(lambda df: join_security_master(df, seed_df))
        .select(SILVER_QUOTES_COLUMNS)
    )

    writer = (
        silver_stream.writeStream
        .format("delta")
        .foreachBatch(
            lambda df, bid: merge_quotes_batch(df, bid, target_table, metrics_table)
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
