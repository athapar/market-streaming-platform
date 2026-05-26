# Databricks notebook source
# MAGIC %md
# MAGIC # Quotes Silver Ingest — Bronze Delta → Typed, Deduped, FIGI-Joined Quotes
# MAGIC
# MAGIC Reads the Bronze Delta table (filtered to `kafka_topic = 'market.quotes'`),
# MAGIC parses Polygon Q.* JSON, deduplicates on `(symbol, sip_timestamp,
# MAGIC sequence_number)`, attaches `composite_figi`, and MERGE-writes into
# MAGIC `silver_quotes`.
# MAGIC
# MAGIC **Note on volume:** Quote data is significantly higher volume than trades
# MAGIC or aggregates. For 20 liquid symbols, expect 10-50M+ rows per day in
# MAGIC Silver. The Gold layer pre-aggregates into per-minute stats to keep
# MAGIC Snowflake volume manageable.
# MAGIC
# MAGIC **Pre-requisite:** Bronze must have quote data (producer run with `--channels AM,T,Q`).

# COMMAND ----------

# MAGIC %md ## Install package

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import sys

repo_root = "/Workspace/Users/armaant.08@gmail.com/market-streaming-pipeline"
src_path = f"{repo_root}/src"

if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(sys.path[0])

# COMMAND ----------

from market_streaming.silver.quotes_transforms import *
print("import worked")

# COMMAND ----------

# MAGIC %md ## Seed — verify security_master.parquet

# COMMAND ----------

seed_dbfs_path = "/Workspace/Users/armaant.08@gmail.com/security_master_current.parquet"

try:
    _seed_check = spark.read.parquet(seed_dbfs_path)
    print(f"seed rows: {_seed_check.count()}")
    _seed_check.select("symbol", "composite_figi").show()
except Exception as e:
    raise RuntimeError(
        f"Security master seed not found at {seed_dbfs_path}.\n"
        "Upload from data/seed/security_master_current.parquet."
    ) from e

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("target_catalog",    "main",                              "Target catalog")
dbutils.widgets.text("target_schema",     "market_streaming",                  "Target schema")
dbutils.widgets.text("target_table_name", "silver_quotes",                     "Target table")
dbutils.widgets.text("bronze_table",      "main.market_streaming.bronze_market_events", "Bronze table")
dbutils.widgets.text("seed_path",         seed_dbfs_path,                      "Seed parquet path")
dbutils.widgets.text(    "checkpoint_path",  "dbfs:/checkpoints/market_streaming/silver_quotes", "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",   "availableNow",
                         ["availableNow", "processingTime", "once"],           "Trigger type")
dbutils.widgets.text(    "trigger_seconds", "30",                              "Trigger seconds (processingTime only)")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
target_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
bronze_table     = dbutils.widgets.get("bronze_table")
seed_path        = dbutils.widgets.get("seed_path")
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/silver_quotes"
trigger_type     = dbutils.widgets.get("trigger_type")
trigger_seconds  = int(dbutils.widgets.get("trigger_seconds"))

print(f"bronze_table    = {bronze_table}")
print(f"target_table    = {target_table}")
print(f"seed_path       = {seed_path}")
print(f"checkpoint_path = {checkpoint_path}")
print(f"trigger_type    = {trigger_type}")

# COMMAND ----------

# MAGIC %md ## DDL (idempotent)

# COMMAND ----------

from market_streaming.silver.quotes_transforms import silver_quotes_ddl

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(silver_quotes_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------

# MAGIC %md ## Start streaming query

# COMMAND ----------

from market_streaming.silver.quotes_transforms import build_silver_quotes_stream

query = build_silver_quotes_stream(
    spark=spark,
    bronze_table=bronze_table,
    seed_path=seed_path,
    target_table=target_table,
    checkpoint_path=checkpoint_path,
    trigger_type=trigger_type,
    trigger_seconds=trigger_seconds,
)

print(f"query id     = {query.id}")
print(f"query run id = {query.runId}")
print(f"status       = {query.status}")

if trigger_type in ("availableNow", "once"):
    query.awaitTermination()
    print("batch complete")

# COMMAND ----------

# MAGIC %md ## Monitor — quote counts by date

# COMMAND ----------

spark.sql(f"""
SELECT
  quote_date,
  COUNT(*)                                          AS quote_rows,
  COUNT(DISTINCT symbol)                            AS symbols,
  MIN(sip_timestamp)                                AS first_quote,
  MAX(sip_timestamp)                                AS last_quote,
  ROUND(AVG(ask_price - bid_price), 6)              AS avg_spread_dollars,
  ROUND(AVG((ask_price - bid_price) / ((ask_price + bid_price) / 2) * 10000), 2) AS avg_spread_bps
FROM {target_table}
WHERE ask_price > bid_price AND bid_price > 0
GROUP BY quote_date
ORDER BY quote_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Crossed/locked quote check

# COMMAND ----------

spark.sql(f"""
SELECT
  quote_date,
  COUNT(*) AS crossed_or_locked,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {target_table}), 4) AS pct_of_total
FROM {target_table}
WHERE ask_price <= bid_price
GROUP BY quote_date
ORDER BY quote_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Sample recent quotes

# COMMAND ----------

spark.sql(f"""
SELECT
  composite_figi, symbol,
  bid_price, bid_size, ask_price, ask_size,
  ROUND(ask_price - bid_price, 4) AS spread,
  sip_timestamp, quote_date,
  kafka_offset, silver_timestamp
FROM {target_table}
ORDER BY silver_timestamp DESC
LIMIT 20
""").display()
