# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Ingest — Silver CDF → minute_bars + daily_rollup
# MAGIC
# MAGIC Reads Silver via Change Data Feed (CDF) so only net-new rows are
# MAGIC processed each run. Writes two Gold tables in a single foreachBatch:
# MAGIC
# MAGIC - **gold_minute_bars**: serving-ready OHLCV per (composite_figi, minute).
# MAGIC   Stripped of Kafka/Bronze lineage columns. Synced to Snowflake.
# MAGIC - **gold_daily_rollup**: one row per (composite_figi, date). Full-day
# MAGIC   OHLCV, volume-weighted VWAP, bar count. This is the reconciliation
# MAGIC   join point with the batch pipeline's daily closing prices.
# MAGIC
# MAGIC **Pre-requisite:** Silver must have CDF enabled (it does — silver_ddl sets
# MAGIC `delta.enableChangeDataFeed = true`).
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Attach to cluster.
# MAGIC 2. Install package cell.
# MAGIC 3. Configuration — defaults work for the standard layout.
# MAGIC 4. DDL — creates both Gold tables (idempotent).
# MAGIC 5. Start stream.
# MAGIC 6. Monitor cells ad-hoc.

# COMMAND ----------

# MAGIC %md ## Install package

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import sys

repo_root = "/Workspace/Users/armaant.08@gmail.com/market-streaming-platform"
src_path  = f"{repo_root}/src"

if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(sys.path[0])

# COMMAND ----------

from market_streaming.gold.transforms import (
    aggregate_daily, build_gold_stream, daily_rollup_ddl, minute_bars_ddl,
)
print("import ok")

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text(    "target_catalog",        "main",                                        "Target catalog")
dbutils.widgets.text(    "target_schema",          "market_streaming",                            "Target schema")
dbutils.widgets.text(    "silver_table",           "main.market_streaming.silver_market_events",  "Silver table")
dbutils.widgets.text(    "minute_table_name",      "gold_minute_bars",                            "Minute bars table")
dbutils.widgets.text(    "daily_table_name",       "gold_daily_rollup",                           "Daily rollup table")
dbutils.widgets.text(    "checkpoint_path",        "dbfs:/checkpoints/market_streaming/gold",     "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",           "availableNow",
                         ["availableNow", "processingTime", "once"],                              "Trigger type")
dbutils.widgets.text(    "trigger_seconds",        "60",                                          "Trigger seconds (processingTime only)")
dbutils.widgets.text(    "starting_version",       "0",                                           "Silver CDF starting version")
dbutils.widgets.text(    "max_files_per_trigger",  "200",                                         "Max Delta files per micro-batch (0 = unbounded)")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
silver_table     = dbutils.widgets.get("silver_table")
minute_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('minute_table_name')}"
daily_table      = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('daily_table_name')}"
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/gold"
trigger_type     = dbutils.widgets.get("trigger_type")
trigger_seconds  = int(dbutils.widgets.get("trigger_seconds"))
starting_version = int(dbutils.widgets.get("starting_version"))
# 0 / blank → unbounded (None); otherwise cap files per micro-batch.
max_files_per_trigger = int(dbutils.widgets.get("max_files_per_trigger") or 0) or None

print(f"silver_table    = {silver_table}")
print(f"minute_table    = {minute_table}")
print(f"daily_table     = {daily_table}")
print(f"checkpoint_path = {checkpoint_path}")
print(f"trigger_type    = {trigger_type}")
print(f"starting_version= {starting_version}")
print(f"max_files/trig  = {max_files_per_trigger}")

# COMMAND ----------

# MAGIC %md ## DDL (idempotent)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(minute_bars_ddl(minute_table))
spark.sql(daily_rollup_ddl(daily_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {minute_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start streaming query
# MAGIC
# MAGIC - `availableNow`: processes all Silver CDF changes not yet in Gold, then
# MAGIC   stops. Re-run to catch up after new Silver rows arrive.
# MAGIC - `processingTime`: runs continuously (Classic cluster only).

# COMMAND ----------

query = build_gold_stream(
    spark=spark,
    silver_table=silver_table,
    minute_table=minute_table,
    daily_table=daily_table,
    checkpoint_path=checkpoint_path,
    trigger_type=trigger_type,
    trigger_seconds=trigger_seconds,
    starting_version=starting_version,
    max_files_per_trigger=max_files_per_trigger,
)

print(f"query id     = {query.id}")
print(f"query run id = {query.runId}")
print(f"status       = {query.status}")

if trigger_type in ("availableNow", "once"):
    query.awaitTermination()
    print("batch complete")

# COMMAND ----------

# MAGIC %md ## Monitor — minute bars

# COMMAND ----------

spark.sql(f"""
SELECT
  event_date,
  COUNT(*)                    AS bars,
  COUNT(DISTINCT composite_figi) AS symbols,
  MIN(window_start)           AS first_bar,
  MAX(window_start)           AS last_bar
FROM {minute_table}
GROUP BY event_date
ORDER BY event_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Monitor — daily rollup

# COMMAND ----------

spark.sql(f"""
SELECT
  composite_figi, symbol, event_date,
  open_price, high_price, low_price, close_price,
  volume, ROUND(vwap, 4) AS vwap,
  bar_count, total_trades,
  first_bar_start, last_bar_start,
  updated_at
FROM {daily_table}
ORDER BY event_date DESC, symbol
""").display()

# COMMAND ----------

# MAGIC %md ## Duplicate check — both tables should return zero rows

# COMMAND ----------

spark.sql(f"""
SELECT 'minute_bars' AS tbl, composite_figi, window_start, COUNT(*) AS n
FROM {minute_table}
GROUP BY composite_figi, window_start HAVING n > 1
UNION ALL
SELECT 'daily_rollup', composite_figi, CAST(event_date AS STRING), COUNT(*)
FROM {daily_table}
GROUP BY composite_figi, event_date HAVING COUNT(*) > 1
""").display()
