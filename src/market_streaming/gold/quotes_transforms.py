"""
Gold layer for quotes: Silver quotes CDF → gold_quote_stats (pre-aggregated).

Raw quotes stay in Silver Delta for deep analysis. Gold pre-aggregates into
one row per (composite_figi, minute) with spread statistics, quote counts,
and order imbalance. This reduces Snowflake volume by ~1000x while preserving
the metrics that dbt and the dashboard actually need.

Aggregation approach: for each affected minute in the micro-batch, re-read
ALL Silver quotes for that minute and recompute the stats. This is the same
"full recompute for affected windows" pattern used by gold_daily_rollup for
AM bars — correct under late arrivals without incremental bookkeeping.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


GOLD_QUOTE_STATS_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "window_start",
    "quote_date",
    "quote_count",
    "avg_bid_price",
    "avg_ask_price",
    "avg_spread_dollars",
    "avg_spread_bps",
    "min_spread_bps",
    "max_spread_bps",
    "avg_mid_price",
    "avg_bid_size",
    "avg_ask_size",
    "bid_size_total",
    "ask_size_total",
    "order_imbalance",
    "updated_at",
]

MERGE_KEYS: list[str] = ["composite_figi", "window_start"]


def gold_quote_stats_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi    STRING        NOT NULL,
  symbol            STRING        NOT NULL,
  window_start      TIMESTAMP     NOT NULL,
  quote_date        DATE          NOT NULL,
  quote_count       BIGINT,
  avg_bid_price     DOUBLE,
  avg_ask_price     DOUBLE,
  avg_spread_dollars DOUBLE,
  avg_spread_bps    DOUBLE,
  min_spread_bps    DOUBLE,
  max_spread_bps    DOUBLE,
  avg_mid_price     DOUBLE,
  avg_bid_size      DOUBLE,
  avg_ask_size      DOUBLE,
  bid_size_total    BIGINT,
  ask_size_total    BIGINT,
  order_imbalance   DOUBLE,
  updated_at        TIMESTAMP
)
USING DELTA
PARTITIONED BY (quote_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
""".strip()


def aggregate_quote_stats(quotes_df: "DataFrame") -> "DataFrame":
    """Aggregate raw quotes into per-(symbol, minute) spread statistics.

    Filters to valid quotes where ask > bid > 0 (crossed/locked quotes excluded).
    """
    valid = quotes_df.filter(
        (F.col("ask_price") > F.col("bid_price"))
        & (F.col("bid_price") > 0)
    )

    with_derived = (
        valid
        .withColumn("mid_price",
            (F.col("ask_price") + F.col("bid_price")) / 2)
        .withColumn("spread_dollars",
            F.col("ask_price") - F.col("bid_price"))
        .withColumn("spread_bps",
            (F.col("ask_price") - F.col("bid_price"))
            / ((F.col("ask_price") + F.col("bid_price")) / 2) * 10000)
        .withColumn("window_start",
            F.date_trunc("minute", F.col("sip_timestamp")))
    )

    return (
        with_derived
        .groupBy("composite_figi", "symbol", "window_start", "quote_date")
        .agg(
            F.count("*")                    .alias("quote_count"),
            F.avg("bid_price")              .alias("avg_bid_price"),
            F.avg("ask_price")              .alias("avg_ask_price"),
            F.avg("spread_dollars")         .alias("avg_spread_dollars"),
            F.avg("spread_bps")             .alias("avg_spread_bps"),
            F.min("spread_bps")             .alias("min_spread_bps"),
            F.max("spread_bps")             .alias("max_spread_bps"),
            F.avg("mid_price")              .alias("avg_mid_price"),
            F.avg("bid_size")               .alias("avg_bid_size"),
            F.avg("ask_size")               .alias("avg_ask_size"),
            F.sum("bid_size")               .alias("bid_size_total"),
            F.sum("ask_size")               .alias("ask_size_total"),
            # order imbalance: (bid_size - ask_size) / (bid_size + ask_size)
            ((F.sum("bid_size") - F.sum("ask_size"))
             / (F.sum("bid_size") + F.sum("ask_size")))
                                            .alias("order_imbalance"),
        )
        .withColumn("updated_at", F.current_timestamp())
    )


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


def write_gold_quotes_batch(
    batch_df: "DataFrame",
    batch_id: int,
    silver_table: str,
    target_table: str,
    metrics_table: str | None = None,
) -> None:
    """Process one CDF micro-batch: re-aggregate affected minutes from Silver."""
    spark = batch_df.sparkSession

    def _do_write(tracker=None):
        net_new = (
            batch_df
            .filter(F.col("_change_type").isin("insert", "update_postimage"))
            .drop("_change_type", "_commit_version", "_commit_timestamp")
            .filter(F.col("composite_figi").isNotNull())
        )

        if net_new.isEmpty():
            if tracker:
                tracker.record(rows_in=0, rows_out=0)
            return

        rows_in = net_new.count()

        # Identify affected (quote_date, minute) windows
        affected_windows = (
            net_new
            .withColumn("window_start",
                F.date_trunc("minute", F.col("sip_timestamp")))
            .select("quote_date", "window_start")
            .distinct()
        )

        # Re-read full Silver for those windows and recompute stats
        silver_snapshot = (
            spark.read.format("delta").table(silver_table)
            .join(F.broadcast(affected_windows), ["quote_date", "window_start"])
        )

        # Need window_start on silver for the join — add it
        silver_with_window = silver_snapshot.withColumn(
            "window_start", F.date_trunc("minute", F.col("sip_timestamp"))
        )

        stats = aggregate_quote_stats(silver_with_window)
        rows_out = stats.count()

        _merge(spark, stats, target_table, MERGE_KEYS)

        if tracker:
            tracker.record(rows_in=rows_in, rows_out=rows_out)

    if metrics_table:
        from market_streaming.observability.pipeline_metrics import track_batch
        with track_batch(spark, metrics_table, "gold_quotes", batch_id) as tracker:
            _do_write(tracker)
    else:
        _do_write()


def build_gold_quotes_stream(
    spark: "SparkSession",
    silver_table: str,
    target_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 60,
    starting_version: int = 0,
    metrics_table: str | None = None,
) -> "StreamingQuery":
    """Read Silver quotes CDF → aggregate → write Gold quote stats."""
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
            lambda df, bid: write_gold_quotes_batch(
                df, bid, silver_table, target_table, metrics_table
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
