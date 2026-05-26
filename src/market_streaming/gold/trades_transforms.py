"""
Gold layer for trades: Silver trades CDF → gold_trades serving table.

Unlike AM bars, trades have no daily rollup — they are point events, not
windowed aggregates. Gold trades is a clean projection of Silver with lineage
columns stripped, ready for Snowflake sync and dbt analytics.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery


GOLD_TRADES_COLUMNS: list[str] = [
    "composite_figi",
    "symbol",
    "trade_id",
    "trade_price",
    "trade_size",
    "exchange_id",
    "tape",
    "sip_timestamp",
    "trade_date",
    "silver_timestamp",
]

MERGE_KEYS: list[str] = ["composite_figi", "trade_id"]


def gold_trades_ddl(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  composite_figi    STRING        NOT NULL,
  symbol            STRING        NOT NULL,
  trade_id          STRING        NOT NULL,
  trade_price       DOUBLE        NOT NULL,
  trade_size        INTEGER       NOT NULL,
  exchange_id       INTEGER,
  tape              INTEGER,
  sip_timestamp     TIMESTAMP     NOT NULL,
  trade_date        DATE,
  silver_timestamp  TIMESTAMP
)
USING DELTA
PARTITIONED BY (trade_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
)
""".strip()


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


def write_gold_trades_batch(
    batch_df: "DataFrame",
    batch_id: int,
    target_table: str,
    metrics_table: str | None = None,
) -> None:
    """Process one CDF micro-batch into Gold trades."""
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
        gold_rows = net_new.select(GOLD_TRADES_COLUMNS)
        _merge(spark, gold_rows, target_table, MERGE_KEYS)

        if tracker:
            tracker.record(rows_in=rows_in, rows_out=rows_in)

    if metrics_table:
        from market_streaming.observability.pipeline_metrics import track_batch
        with track_batch(spark, metrics_table, "gold_trades", batch_id) as tracker:
            _do_write(tracker)
    else:
        _do_write()


def build_gold_trades_stream(
    spark: "SparkSession",
    silver_table: str,
    target_table: str,
    checkpoint_path: str,
    trigger_type: str = "availableNow",
    trigger_seconds: int = 60,
    starting_version: int = 0,
    metrics_table: str | None = None,
) -> "StreamingQuery":
    """Read Silver trades CDF → write Gold trades."""
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
            lambda df, bid: write_gold_trades_batch(
                df, bid, target_table, metrics_table
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
