# Databricks notebook source
# MAGIC %md
# MAGIC # Quotes Gold Ingest — Silver Quotes CDF → Gold Quote Stats
# MAGIC
# MAGIC Reads Silver quotes via Change Data Feed (CDF) and pre-aggregates into
# MAGIC `gold_quote_stats`: one row per (composite_figi, minute) with spread
# MAGIC statistics, quote counts, and order imbalance.
# MAGIC
# MAGIC **Why pre-aggregate?** Raw quotes for 20 symbols can be 10-50M+ rows/day.
# MAGIC Syncing that to Snowflake via `executemany()` is infeasible. Pre-aggregation
# MAGIC reduces volume by ~1000x while preserving the metrics that dbt and the
# MAGIC dashboard need. Raw quotes remain in Silver Delta for deep analysis.
# MAGIC
# MAGIC The aggregation re-reads the full Silver partition for affected minutes
# MAGIC (same pattern as `gold_daily_rollup` for AM bars), ensuring late-arriving
# MAGIC quotes produce correct stats without incremental bookkeeping.

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

from market_streaming.gold.quotes_transforms import (
    build_gold_quotes_stream, gold_quote_stats_ddl,
)
print("import ok")

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text(    "target_catalog",        "main",                                     "Target catalog")
dbutils.widgets.text(    "target_schema",          "market_streaming",                         "Target schema")
dbutils.widgets.text(    "silver_table",           "main.market_streaming.silver_quotes",      "Silver quotes table")
dbutils.widgets.text(    "target_table_name",      "gold_quote_stats",                         "Gold quote stats table")
dbutils.widgets.text(    "checkpoint_path",        "dbfs:/checkpoints/market_streaming/gold_quotes", "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",           "availableNow",
                         ["availableNow", "processingTime", "once"],                           "Trigger type")
dbutils.widgets.text(    "trigger_seconds",        "60",                                       "Trigger seconds (processingTime only)")
dbutils.widgets.text(    "starting_version",       "0",                                        "Silver CDF starting version")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
silver_table     = dbutils.widgets.get("silver_table")
target_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/gold_quotes"
trigger_type     = dbutils.widgets.get("trigger_type")
trigger_seconds  = int(dbutils.widgets.get("trigger_seconds"))
starting_version = int(dbutils.widgets.get("starting_version"))

print(f"silver_table    = {silver_table}")
print(f"target_table    = {target_table}")
print(f"checkpoint_path = {checkpoint_path}")
print(f"trigger_type    = {trigger_type}")
print(f"starting_version= {starting_version}")

# COMMAND ----------

# MAGIC %md ## DDL (idempotent)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(gold_quote_stats_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------

# MAGIC %md ## Start streaming query

# COMMAND ----------

query = build_gold_quotes_stream(
    spark=spark,
    silver_table=silver_table,
    target_table=target_table,
    checkpoint_path=checkpoint_path,
    trigger_type=trigger_type,
    trigger_seconds=trigger_seconds,
    starting_version=starting_version,
)

print(f"query id     = {query.id}")
print(f"query run id = {query.runId}")
print(f"status       = {query.status}")

if trigger_type in ("availableNow", "once"):
    query.awaitTermination()
    print("batch complete")

# COMMAND ----------

# MAGIC %md ## Monitor — quote stats by date

# COMMAND ----------

spark.sql(f"""
SELECT
  quote_date,
  COUNT(*)                        AS minute_windows,
  SUM(quote_count)                AS total_quotes,
  COUNT(DISTINCT symbol)          AS symbols,
  ROUND(AVG(avg_spread_bps), 2)   AS avg_spread_bps,
  ROUND(MIN(min_spread_bps), 2)   AS tightest_spread_bps,
  ROUND(AVG(order_imbalance), 4)  AS avg_order_imbalance
FROM {target_table}
GROUP BY quote_date
ORDER BY quote_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Spread by symbol (latest date)

# COMMAND ----------

spark.sql(f"""
SELECT
  symbol,
  COUNT(*)                        AS minute_windows,
  SUM(quote_count)                AS total_quotes,
  ROUND(AVG(avg_spread_bps), 2)   AS avg_spread_bps,
  ROUND(MIN(min_spread_bps), 2)   AS min_spread_bps,
  ROUND(MAX(max_spread_bps), 2)   AS max_spread_bps,
  ROUND(AVG(order_imbalance), 4)  AS avg_imbalance
FROM {target_table}
WHERE quote_date = (SELECT MAX(quote_date) FROM {target_table})
GROUP BY symbol
ORDER BY avg_spread_bps
""").display()
