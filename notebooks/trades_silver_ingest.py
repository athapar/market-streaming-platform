# Databricks notebook source
# MAGIC %md
# MAGIC # Trades Silver Ingest — Bronze Delta → Typed, Deduped, FIGI-Joined Trades
# MAGIC
# MAGIC Reads the Bronze Delta table (filtered to `kafka_topic = 'market.trades'`),
# MAGIC parses Polygon T.* JSON, deduplicates on `(symbol, trade_id)`, attaches
# MAGIC `composite_figi` from the security-master seed, and MERGE-writes into
# MAGIC `silver_trades`.
# MAGIC
# MAGIC **Pre-requisite:** Bronze must have trade data (producer run with `--channels AM,T,Q`).
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Attach to a running cluster.
# MAGIC 2. Install package cell.
# MAGIC 3. Seed — verify security_master.parquet is uploaded.
# MAGIC 4. Configuration — widget defaults work for standard setup.
# MAGIC 5. DDL — creates silver_trades table (idempotent).
# MAGIC 6. Start stream.

# COMMAND ----------

# MAGIC %md ## Install package

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import sys

repo_root = "/Workspace/Users/armaant.08@gmail.com/market-streaming-platform"
src_path = f"{repo_root}/src"

if src_path not in sys.path:
    sys.path.insert(0, src_path)

print(sys.path[0])

# COMMAND ----------

from market_streaming.silver.trades_transforms import *
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
dbutils.widgets.text("target_table_name", "silver_trades",                     "Target table")
dbutils.widgets.text("bronze_table",      "main.market_streaming.bronze_market_events", "Bronze table")
dbutils.widgets.text("seed_path",         seed_dbfs_path,                      "Seed parquet path")
dbutils.widgets.text(    "checkpoint_path",  "dbfs:/checkpoints/market_streaming/silver_trades", "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",   "availableNow",
                         ["availableNow", "processingTime", "once"],           "Trigger type")
dbutils.widgets.text(    "trigger_seconds", "30",                              "Trigger seconds (processingTime only)")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
target_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
bronze_table     = dbutils.widgets.get("bronze_table")
seed_path        = dbutils.widgets.get("seed_path")
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/silver_trades"
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

from market_streaming.silver.trades_transforms import silver_trades_ddl

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(silver_trades_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------

# MAGIC %md ## Start streaming query

# COMMAND ----------

from market_streaming.silver.trades_transforms import build_silver_trades_stream

query = build_silver_trades_stream(
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

# MAGIC %md ## Monitor — trade counts by date

# COMMAND ----------

spark.sql(f"""
SELECT
  trade_date,
  COUNT(*)                                          AS trade_rows,
  COUNT(DISTINCT symbol)                            AS symbols,
  COUNT(DISTINCT composite_figi)                    AS distinct_figis,
  SUM(CASE WHEN composite_figi IS NULL THEN 1 END)  AS null_figi_rows,
  MIN(sip_timestamp)                                AS first_trade,
  MAX(sip_timestamp)                                AS last_trade,
  SUM(trade_price * trade_size)                     AS total_dollar_volume
FROM {target_table}
GROUP BY trade_date
ORDER BY trade_date DESC
""").display()

# COMMAND ----------

# MAGIC %md ## Duplicate check — should return zero rows

# COMMAND ----------

spark.sql(f"""
SELECT symbol, trade_id, COUNT(*) AS occurrences
FROM {target_table}
GROUP BY symbol, trade_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md ## Sample recent trades

# COMMAND ----------

spark.sql(f"""
SELECT
  composite_figi, symbol, trade_id,
  trade_price, trade_size, exchange_id,
  sip_timestamp, trade_date,
  kafka_offset, silver_timestamp
FROM {target_table}
ORDER BY silver_timestamp DESC
LIMIT 20
""").display()
