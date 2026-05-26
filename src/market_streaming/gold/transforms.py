"""
Gold layer: Silver CDF → gold_minute_bars + gold_daily_rollup.

Two tables written in a single foreachBatch pass from the Silver CDF stream:

gold_minute_bars
  One row per (composite_figi, window_start). Clean, serving-ready projection
  of Silver with internal lineage columns stripped. Downstream Snowflake sync
  reads this for per-minute OHLCV queries.

gold_daily_rollup
  One row per (composite_figi, event_date). Full-day OHLCV, volume-weighted
  VWAP, bar count, first/last bar timestamps. This is the reconciliation join
  point with the batch pipeline's daily closing prices.

  Daily rollup is recomputed from the full Silver snapshot for every affected
  date in each batch — not accumulated incrementally. This means late-arriving
  Silver bars (corrected prices, deferred commits) automatically produce the
  correct daily aggregate without any special handling.

Both tables are MERGE targets keyed on their natural primary key, so Gold
can be fully rebuilt: delete checkpoints, drop tables, re-run with
startingVersion=0.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


# ---------------------------------------------------------------------------
# Column lists
# ---------------------------------------------------------------------------

GOLD_MINUTE_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "event_type",
    "window_start",
    "window_end",
    "event_date",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "volume",
    "vwap",
    "trade_count",
    "silver_timestamp",
]

MINUTE_MERGE_KEYS: list[str] = ["composite_figi", "window_start"]
DAILY_MERGE_KEYS:  list[str] = ["composite_figi", "event_date"]


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def minute_bars_ddl(table: str) -> str:
    """Idempotent CREATE TABLE for gold_minute_bars."""
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi   STRING    NOT NULL,
  symbol           STRING    NOT NULL,
  event_type       STRING,
  window_start     TIMESTAMP NOT NULL,
  window_end       TIMESTAMP,
  event_date       DATE,
  open_price       DOUBLE,
  high_price       DOUBLE,
  low_price        DOUBLE,
  close_price      DOUBLE,
  volume           BIGINT,
  vwap             DOUBLE,
  trade_count      INTEGER,
  silver_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed'       = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
""".strip()


def daily_rollup_ddl(table: str) -> str:
    """Idempotent CREATE TABLE for gold_daily_rollup.

    open_price  = open of the earliest minute bar for the day
    close_price = close of the latest minute bar for the day
    vwap        = volume-weighted average of per-minute VWAPs
    total_trades = sum of trade_count (NULL when Polygon doesn't send 'n')
    bar_count   = number of distinct minute bars received
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi  STRING NOT NULL,
  symbol          STRING NOT NULL,
  event_date      DATE   NOT NULL,
  open_price      DOUBLE,
  high_price      DOUBLE,
  low_price       DOUBLE,
  close_price     DOUBLE,
  volume          BIGINT,
  vwap            DOUBLE,
  total_trades    BIGINT,
  bar_count       BIGINT,
  first_bar_start TIMESTAMP,
  last_bar_start  TIMESTAMP,
  updated_at      TIMESTAMP
)
USING DELTA
PARTITIONED BY (event_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
""".strip()


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

def aggregate_daily(silver_df: "DataFrame") -> "DataFrame":
    """Compute daily OHLCV rollup from a Silver (or Silver-shaped) DataFrame.

    Uses min_by/max_by to get the open of the first bar and close of the last
    bar by window_start, without requiring a window function + groupBy.
    VWAP is volume-weighted: SUM(vwap * volume) / SUM(volume).
    """
    return (
        silver_df
        .groupBy("composite_figi", "symbol", "event_date")
        .agg(
            F.min_by("open_price",  "window_start").alias("open_price"),
            F.max("high_price")                    .alias("high_price"),
            F.min("low_price")                     .alias("low_price"),
            F.max_by("close_price", "window_start").alias("close_price"),
            F.sum("volume")                        .alias("volume"),
            (F.sum(F.col("vwap") * F.col("volume")) / F.sum("volume"))
                                                   .alias("vwap"),
            F.sum("trade_count")                   .alias("total_trades"),
            F.count("*")                           .alias("bar_count"),
            F.min("window_start")                  .alias("first_bar_start"),
            F.max("window_start")                  .alias("last_bar_start"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )


# ---------------------------------------------------------------------------
# MERGE helpers
# ---------------------------------------------------------------------------

def _merge(spark: "SparkSession", df: "DataFrame", table: str, keys: list[str]) -> None:
    from delta.tables import DeltaTable

    condition = " AND ".join(f"tgt.{k} = src.{k}" for k in keys)
    (
        DeltaTable.forName(spark, table).alias("tgt")
        .merge(df.alias("src"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


# ---------------------------------------------------------------------------
# foreachBatch handler
# ---------------------------------------------------------------------------

def write_gold_batch(
    batch_df: "DataFrame",
    batch_id: int,
    silver_table: str,
    minute_table: str,
    daily_table: str,
) -> None:
    """Process one CDF micro-batch into both Gold tables.

    Steps:
    1. Filter CDF change types — keep inserts and post-update images only.
       Pre-update images and deletes are not relevant (Silver doesn't delete).
    2. Write minute bars — select Gold columns, MERGE on (composite_figi,
       window_start). Handles same-batch and cross-batch duplicates.
    3. Recompute daily rollup for affected dates — re-reads the full Silver
       snapshot for those dates so late-arriving bars are folded in correctly.
    """
    spark = batch_df.sparkSession

    # Step 1: net-new / updated rows from CDF
    net_new = (
        batch_df
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )

    if net_new.isEmpty():
        return

    # Step 2: minute bars
    minute_rows = net_new.select(GOLD_MINUTE_COLUMNS)
    _merge(spark, minute_rows, minute_table, MINUTE_MERGE_KEYS)

    # Step 3: daily rollup — full recompute for affected event_dates
    affected_dates = net_new.select("event_date").distinct()
    silver_snapshot = (
        spark.read.format("delta").table(silver_table)
        .join(F.broadcast(affected_dates), "event_date")
    )
    daily_rows = aggregate_daily(silver_snapshot)
    _merge(spark, daily_rows, daily_table, DAILY_MERGE_KEYS)


# ---------------------------------------------------------------------------
# Stream entry point
# ---------------------------------------------------------------------------

def build_gold_stream(
    spark: "SparkSession",
    silver_table: str,
    minute_table: str,
    daily_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 60,
    starting_version: int = 0,
) -> "StreamingQuery":
    """Read Silver CDF stream → write_gold_batch → both Gold tables.

    starting_version is only used on the very first run (no checkpoint).
    Default 0 means rebuild Gold from all of Silver's history — safe and
    correct. Subsequent runs resume from the checkpoint automatically.
    """
    silver_cdf = (
        spark.readStream
        .format("delta")
        .option("readChangeData", "true")
        .option("startingVersion", starting_version)
        .table(silver_table)
    )

    writer = (
        silver_cdf.writeStream
        .format("delta")
        .foreachBatch(
            lambda df, bid: write_gold_batch(
                df, bid, silver_table, minute_table, daily_table
            )
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
