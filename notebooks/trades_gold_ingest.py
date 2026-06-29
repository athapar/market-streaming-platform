# Databricks notebook source
# MAGIC %md
# MAGIC # Trades Gold Ingest — Silver Trades CDF → Gold Trades
# MAGIC
# MAGIC Reads Silver trades via Change Data Feed (CDF) so only net-new rows are
# MAGIC processed each run. Writes a single Gold table (`gold_trades`) — a
# MAGIC serving-ready projection with lineage columns stripped.
# MAGIC
# MAGIC Unlike AM bars, trades have no daily rollup. Aggregation into daily
# MAGIC summaries, trade size distributions, and microstructure metrics is handled
# MAGIC by dbt models downstream.
# MAGIC
# MAGIC **Pre-requisite:** Silver trades must have CDF enabled (it does via DDL).
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Attach to cluster.
# MAGIC 2. Install package cell.
# MAGIC 3. Configuration — defaults work for standard layout.
# MAGIC 4. DDL — creates gold_trades table (idempotent).
# MAGIC 5. Start stream.

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

from market_streaming.gold.trades_transforms import (
    build_gold_trades_stream, gold_trades_ddl,
)
print("import ok")

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text(    "target_catalog",        "main",                                    "Target catalog")
dbutils.widgets.text(    "target_schema",          "market_streaming",                        "Target schema")
dbutils.widgets.text(    "silver_table",           "main.market_streaming.silver_trades",     "Silver trades table")
dbutils.widgets.text(    "target_table_name",      "gold_trades",                             "Gold trades table")
dbutils.widgets.text(    "checkpoint_path",        "dbfs:/checkpoints/market_streaming/gold_trades", "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",           "availableNow",
                         ["availableNow", "processingTime", "once"],                          "Trigger type")
dbutils.widgets.text(    "trigger_seconds",        "60",                                      "Trigger seconds (processingTime only)")
dbutils.widgets.text(    "starting_version",       "0",                                       "Silver CDF starting version")
dbutils.widgets.text(    "max_files_per_trigger",  "200",                                     "Max Delta files per micro-batch (0 = unbounded)")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
silver_table     = dbutils.widgets.get("silver_table")
target_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/gold_trades"
trigger_type     = dbutils.widgets.get("trigger_type")
trigger_seconds  = int(dbutils.widgets.get("trigger_seconds"))
starting_version = int(dbutils.widgets.get("starting_version"))
# 0 / blank → unbounded (None); otherwise cap files per micro-batch so the
# large trades CDF backlog drains in chunks instead of OOM-ing the driver.
max_files_per_trigger = int(dbutils.widgets.get("max_files_per_trigger") or 0) or None

print(f"silver_table    = {silver_table}")
print(f"target_table    = {target_table}")
print(f"checkpoint_path = {checkpoint_path}")
print(f"trigger_type    = {trigger_type}")
print(f"starting_version= {starting_version}")
print(f"max_files/trig  = {max_files_per_trigger}")

# COMMAND ----------

# MAGIC %md ## DDL (idempotent)

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(gold_trades_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------

# MAGIC %md ## Start streaming query

# COMMAND ----------

query = build_gold_trades_stream(
    spark=spark,
    silver_table=silver_table,
    target_table=target_table,
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

# MAGIC %md ## Monitor — trade counts by date

# COMMAND ----------

spark.sql(f"""
SELECT
  trade_date,
  COUNT(*)                     AS trades,
  COUNT(DISTINCT symbol)       AS symbols,
  MIN(sip_timestamp)           AS first_trade,
  MAX(sip_timestamp)           AS last_trade,
  ROUND(SUM(trade_price * trade_size), 2) AS total_dollar_volume
FROM {target_table}
GROUP BY trade_date
ORDER BY trade_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Duplicate check — should return zero rows

# COMMAND ----------

spark.sql(f"""
SELECT composite_figi, trade_id, COUNT(*) AS n
FROM {target_table}
GROUP BY composite_figi, trade_id
HAVING n > 1
ORDER BY n DESC
LIMIT 20
""").display()
