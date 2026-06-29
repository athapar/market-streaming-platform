"""
Silver layer: Bronze Delta → typed, deduped, FIGI-joined Delta.

Design constraints (spec §2.9, §5.5):
- Parse Polygon AM JSON here, not in Bronze. Silver owns schema; Bronze is
  the raw audit log.
- Natural dedup key: (symbol, window_start). One AM bar per ticker per minute.
  Duplicates arise from the producer's --dup-rate flag, network retries, or
  Kafka producer retries. MERGE handles them idempotently.
- FIGI join is a broadcast of a tiny seed (5 rows). Symbol not in seed gets
  NULL composite_figi — row still lands. No silent data loss.
- foreachBatch + MERGE rather than streaming MERGE because Delta's streaming
  MERGE requires Photon on some runtimes. foreachBatch is portable and makes
  the dedup logic explicit and testable.
- CDF enabled so Gold can use CHANGES FEED instead of full re-scan.
- Bronze is multi-topic (market.aggregates + market.trades + market.quotes);
  this stream filters to market.aggregates BEFORE parsing so the AM schema
  isn't accidentally applied to T/Q payloads (which would silently produce
  garbage rows — e.g. trade size `s` interpreted as window_start_ms).

Pure functions (no SparkSession at import time) so they're unit-testable
without a Databricks cluster. The notebook is a thin wrapper.
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


# ---------------------------------------------------------------------------
# Polygon AM (minute-aggregate) payload schema
# Polygon docs: https://polygon.io/docs/stocks/ws_stocks_am
# ---------------------------------------------------------------------------
AM_PAYLOAD_SCHEMA = StructType([
    StructField("ev",  StringType(),  True),   # "AM"
    StructField("sym", StringType(),  True),   # ticker, e.g. "AAPL"
    StructField("o",   DoubleType(),  True),   # open
    StructField("h",   DoubleType(),  True),   # high
    StructField("l",   DoubleType(),  True),   # low
    StructField("c",   DoubleType(),  True),   # close
    StructField("v",   LongType(),    True),   # volume (shares)
    StructField("vw",  DoubleType(),  True),   # VWAP
    StructField("n",   IntegerType(), True),   # number of trades
    StructField("s",   LongType(),    True),   # window start epoch ms
    StructField("e",   LongType(),    True),   # window end epoch ms
])

SILVER_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "event_type",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "vwap",
    "trade_count",
    "window_start",
    "window_end",
    "event_date",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "bronze_ingest_timestamp",
    "silver_timestamp",
]

# Natural key for dedup MERGE: one AM bar per symbol per minute
MERGE_KEYS: list[str] = ["symbol", "window_start"]

# Partition column of the Silver table — added to the MERGE condition so Delta
# can prune partitions instead of scanning the whole target (OPT-3). It is
# functionally derived from window_start, so it never changes match semantics.
PARTITION_COL: str = "event_date"


def parse_silver(df: "DataFrame") -> "DataFrame":
    """Parse Bronze raw_payload JSON → typed Silver columns.

    Rows where symbol or window_start cannot be parsed are dropped — Bronze
    holds the raw bytes; Silver only carries structurally valid AM events.
    Non-AM event_types (if ever present) are kept so the table is a full
    record of the market.* topic family; Gold filters to AM.
    """
    return (
        df
        .withColumn("_j", F.from_json(F.col("raw_payload"), AM_PAYLOAD_SCHEMA))
        .withColumn("symbol",      F.col("_j.sym"))
        .withColumn("event_type",  F.col("_j.ev"))
        .withColumn("open_price",  F.col("_j.o"))
        .withColumn("high_price",  F.col("_j.h"))
        .withColumn("low_price",   F.col("_j.l"))
        .withColumn("close_price", F.col("_j.c"))
        .withColumn("volume",      F.col("_j.v"))
        .withColumn("vwap",        F.col("_j.vw"))
        .withColumn("trade_count", F.col("_j.n"))
        # Polygon sends epoch ms; cast to Timestamp (seconds)
        .withColumn("window_start",
            (F.col("_j.s") / 1000).cast(TimestampType()))
        .withColumn("window_end",
            (F.col("_j.e") / 1000).cast(TimestampType()))
        .withColumn("event_date",  F.to_date(F.col("window_start")))
        # Carry lineage columns from Bronze
        .withColumn("bronze_ingest_timestamp", F.col("ingest_timestamp"))
        .withColumn("silver_timestamp", F.current_timestamp())
        # Drop Bronze-specific columns not needed in Silver.
        # event_type is NOT dropped — withColumn above already overwrote
        # Bronze's extracted copy with the fully-parsed _j.ev value.
        .drop("_j", "raw_payload", "ingest_date", "ingest_timestamp",
              "kafka_timestamp")
        .filter(F.col("symbol").isNotNull() & F.col("window_start").isNotNull())
    )


def join_security_master(df: "DataFrame", seed_df: "DataFrame") -> "DataFrame":
    """Broadcast-join symbol → composite_figi from the static seed Parquet.

    The seed script writes `ticker AS symbol`, so the join key in the Parquet
    is `symbol`. We alias it to `_seed_symbol` before joining to avoid column
    ambiguity with the `symbol` column already present in df.

    The seed is tiny (5 rows for this universe). Symbols absent from the seed
    land with NULL composite_figi — no data dropped, downstream Gold filters
    can handle or alert on NULLs.
    """
    figi_map = seed_df.select(
        F.col("symbol").alias("_seed_symbol"),
        F.col("composite_figi"),
    )
    return (
        df
        .join(F.broadcast(figi_map), df["symbol"] == figi_map["_seed_symbol"], "left")
        .drop("_seed_symbol")
    )


def silver_ddl(table: str) -> str:
    """Idempotent CREATE TABLE DDL for the Silver table.

    Partitioned by event_date only (not symbol) — date pruning is the most
    common access pattern. CDF enabled for Gold to consume via CHANGES FEED.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi          STRING,
  symbol                  STRING        NOT NULL,
  event_type              STRING,
  open_price              DOUBLE,
  high_price              DOUBLE,
  low_price               DOUBLE,
  close_price             DOUBLE,
  volume                  BIGINT,
  vwap                    DOUBLE,
  trade_count             INTEGER,
  window_start            TIMESTAMP     NOT NULL,
  window_end              TIMESTAMP,
  event_date              DATE,
  kafka_topic             STRING,
  kafka_partition         INTEGER,
  kafka_offset            BIGINT,
  bronze_ingest_timestamp TIMESTAMP,
  silver_timestamp        TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'           = 'true',
  'delta.autoOptimize.optimizeWrite'     = 'true',
  'delta.autoOptimize.autoCompact'       = 'true'
)
""".strip()


def merge_silver_batch(
    batch_df: "DataFrame",
    batch_id: int,
    target_table: str,
    metrics_table: str | None = None,
) -> None:
    """foreachBatch handler: dedup within the micro-batch then MERGE into Silver.

    Two-step approach:
    1. Within-batch dedup via row_number — picks highest kafka_offset for each
       (symbol, window_start) pair. This handles duplicates that arrived in the
       *same* micro-batch (producer retries, synthetic --dup-rate).
    2. MERGE into Delta — handles duplicates that arrived in *different* batches
       (late Bronze commits, stream restarts). whenMatchedUpdateAll so a late
       arrival with corrected prices still propagates.
    """
    from delta.tables import DeltaTable

    spark = batch_df.sparkSession

    def _do_merge(tracker=None):
        # OPT-4: struct-max dedup (no window sort). We deliberately do NOT
        # persist() the result — PERSIST/cache is rejected on Databricks
        # serverless compute ([NOT_SUPPORTED_WITH_SERVERLESS]). The micro-batch
        # source is checkpoint-backed, so recomputing it for the count + MERGE
        # is acceptable; OPT-2's win here is dropping the extra isEmpty() action.
        deduped = dedup_keep_latest(batch_df, MERGE_KEYS)

        rows_out = deduped.count()
        if rows_out == 0:
            if tracker:
                tracker.record(rows_in=0, rows_out=0)
            return

        if tracker:
            # Pre-dedup input count — only computed when metrics are on.
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
        with track_batch(spark, metrics_table, "silver", batch_id) as tracker:
            _do_merge(tracker)
    else:
        _do_merge()


def build_silver_stream(
    spark: "SparkSession",
    bronze_table: str,
    seed_path: str,
    target_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 30,
    metrics_table: str | None = None,
) -> "StreamingQuery":
    """Wire Bronze Delta stream → parse → FIGI join → MERGE into Silver.

    Reads Bronze as a Delta stream (not Kafka directly) so Silver's checkpoint
    is independent of Bronze's and each layer can be rebuilt individually.

    trigger_type defaults to "availableNow" for Databricks Free Edition
    (Serverless). Switch to "processingTime" on a Classic cluster.
    """
    from pyspark.sql import functions as F  # noqa: F401  (needed inside lambda)

    seed_df = spark.read.parquet(seed_path)

    bronze_stream = (
        spark.readStream
        .format("delta")
        .option("ignoreDeletes", "true")   # Bronze is append-only; defensive
        .table(bronze_table)
    )

    silver_stream = (
        bronze_stream
        # Bronze holds all market.* topics; restrict to AM bars before parsing
        # so the AM schema isn't applied to T/Q payloads.
        .filter(F.col("kafka_topic") == "market.aggregates")
        .transform(parse_silver)
        # Defence-in-depth: parse_silver tolerates non-AM events, but with the
        # topic filter above we should only see AM. Drop any stragglers whose
        # event_type doesn't match (e.g. status frames mis-routed).
        .filter(F.col("event_type") == "AM")
        .transform(lambda df: join_security_master(df, seed_df))
        .select(SILVER_COLUMNS)
    )

    writer = (
        silver_stream.writeStream
        .format("delta")
        .foreachBatch(
            lambda df, bid: merge_silver_batch(df, bid, target_table, metrics_table)
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
