# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Ingest — Bronze Delta → Typed, Deduped, FIGI-Joined Delta
# MAGIC
# MAGIC Reads the Bronze Delta table as a stream, parses Polygon AM JSON,
# MAGIC deduplicates on `(symbol, window_start)`, attaches `composite_figi`
# MAGIC from the security-master seed, and MERGE-writes into Silver.
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Attach to a running cluster.
# MAGIC 2. `Install package` — editable install so transforms are importable.
# MAGIC 3. `Seed` — upload your local security_master.parquet to DBFS once.
# MAGIC 4. `Configuration` — widget defaults work for the standard setup.
# MAGIC 5. `DDL` — idempotent CREATE TABLE.
# MAGIC 6. `Start stream` — launches the streaming query.
# MAGIC
# MAGIC Silver reads Bronze Delta directly (not Kafka), so no Kafka credentials
# MAGIC are needed here. The two streams have independent checkpoints and can
# MAGIC be restarted independently.

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

from market_streaming.bronze.transforms import *
print("import worked")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed — upload security_master.parquet to DBFS (one-time)
# MAGIC
# MAGIC Silver needs the security-master Parquet to join `symbol → composite_figi`.
# MAGIC Run the cell below once; it's a no-op if the file already exists.
# MAGIC
# MAGIC **Upload steps:**
# MAGIC 1. From your local machine, find the seed at `data/seeds/security_master.parquet`.
# MAGIC 2. In the Databricks sidebar go to **Catalog → DBFS → Upload** and drop
# MAGIC    the file into `dbfs:/seeds/market_streaming/`.
# MAGIC 3. Or use the Databricks CLI:
# MAGIC    ```
# MAGIC    databricks fs cp data/seeds/security_master.parquet \
# MAGIC        dbfs:/seeds/market_streaming/security_master.parquet
# MAGIC    ```
# MAGIC 4. Run the cell below to confirm.

# COMMAND ----------

seed_dbfs_path = "/Workspace/Users/armaant.08@gmail.com/security_master_current.parquet"

try:
    files = dbutils.fs.ls(seed_dbfs_path)
    print(f"seed found: {seed_dbfs_path}")
    # Quick sanity check: print row count
    _seed_check = spark.read.parquet(seed_dbfs_path)
    print(f"seed rows  : {_seed_check.count()}")
    _seed_check.select("symbol", "composite_figi").show()
except Exception as e:
    raise RuntimeError(
        f"Security master seed not found at {seed_dbfs_path}.\n"
        "Upload it from your local data/seeds/security_master.parquet — "
        "see the cell above for instructions."
    ) from e

# COMMAND ----------

# MAGIC %md ## Configuration

# COMMAND ----------

dbutils.widgets.text("target_catalog",    "main",                              "Target catalog")
dbutils.widgets.text("target_schema",     "market_streaming",                  "Target schema")
dbutils.widgets.text("target_table_name", "silver_market_events",              "Target table")
dbutils.widgets.text("bronze_table",      "main.market_streaming.bronze_market_events", "Bronze table")
dbutils.widgets.text("seed_path",         seed_dbfs_path,                      "Seed parquet path")
dbutils.widgets.text(    "checkpoint_path",  "dbfs:/checkpoints/market_streaming/silver", "Checkpoint path")
dbutils.widgets.dropdown("trigger_type",   "availableNow",
                         ["availableNow", "processingTime", "once"],           "Trigger type")
dbutils.widgets.text(    "trigger_seconds", "30",                              "Trigger seconds (processingTime only)")

target_catalog   = dbutils.widgets.get("target_catalog")
target_schema    = dbutils.widgets.get("target_schema")
target_table     = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
bronze_table     = dbutils.widgets.get("bronze_table")
seed_path        = dbutils.widgets.get("seed_path")
checkpoint_path  = dbutils.widgets.get("checkpoint_path")
checkpoint_path = "/Volumes/main/market_streaming/checkpoints/silver"
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

from market_streaming.silver.transforms import silver_ddl

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(silver_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------

# MAGIC %md ## Start streaming query

# COMMAND ----------

from market_streaming.silver.transforms import build_silver_stream

query = build_silver_stream(
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

# MAGIC %md
# MAGIC ## Monitor progress
# MAGIC
# MAGIC Run these cells ad-hoc while the stream is running.

# COMMAND ----------

# Row count and dedup coverage
spark.sql(f"""
SELECT
  event_date,
  COUNT(*)                                          AS silver_rows,
  COUNT(DISTINCT composite_figi)                    AS distinct_figis,
  SUM(CASE WHEN composite_figi IS NULL THEN 1 END)  AS null_figi_rows,
  MIN(window_start)                                 AS earliest_bar,
  MAX(window_start)                                 AS latest_bar
FROM {target_table}
GROUP BY event_date
ORDER BY event_date DESC
""").display()

# COMMAND ----------

# Duplicate check — should always return zero rows
spark.sql(f"""
SELECT symbol, window_start, COUNT(*) AS occurrences
FROM {target_table}
GROUP BY symbol, window_start
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
""").display()

# COMMAND ----------

# Sample recent Silver bars
spark.sql(f"""
SELECT
  composite_figi, symbol, event_type,
  window_start, open_price, high_price, low_price, close_price,
  volume, vwap, trade_count,
  kafka_offset, silver_timestamp
FROM {target_table}
ORDER BY silver_timestamp DESC
LIMIT 20
""").display()

# COMMAND ----------

# query.awaitTermination()
