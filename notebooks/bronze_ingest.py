# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingest — Kafka -> Delta
# MAGIC
# MAGIC Streams from Confluent Cloud (`market.*` topics) into
# MAGIC `bronze_market_events`, append-only, exactly-once via checkpoint +
# MAGIC Delta atomic commits.
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Attach to a running cluster.
# MAGIC 2. Cell `Install package` — pip-installs the repo as editable so
# MAGIC    `market_streaming.bronze.transforms` is importable.
# MAGIC 3. Cell `Configuration` — reads secrets and widget values.
# MAGIC 4. Cell `DDL` — creates the table if it doesn't exist (idempotent).
# MAGIC 5. Cell `Start stream` — kicks off the streaming query.
# MAGIC
# MAGIC **Trigger modes:**
# MAGIC - `availableNow` (default): reads all Kafka messages currently available,
# MAGIC   commits them to Delta, then the query stops. Re-run the cell to catch up
# MAGIC   again. Use this on Databricks Free Edition (Serverless compute).
# MAGIC - `processingTime`: continuous micro-batch — query runs indefinitely.
# MAGIC   Requires a Classic (non-Serverless) cluster.

# COMMAND ----------

# MAGIC %md ## Install package

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

# MAGIC %md ## Configuration

# COMMAND ----------
dbutils.widgets.text(    "target_catalog",    "main",                              "Target catalog")
dbutils.widgets.text(    "target_schema",     "market_streaming",                  "Target schema")
dbutils.widgets.text(    "target_table_name", "bronze_market_events",              "Target table")
dbutils.widgets.text(    "subscribe_pattern", "market\\..*",                       "Kafka topic pattern")
dbutils.widgets.text(    "checkpoint_path",   "dbfs:/checkpoints/market_streaming/bronze", "Checkpoint path")
dbutils.widgets.dropdown("starting_offsets",  "latest", ["latest", "earliest"],    "Starting offsets")
dbutils.widgets.dropdown("trigger_type",      "availableNow",
                         ["availableNow", "processingTime", "once"],               "Trigger type")
dbutils.widgets.text(    "trigger_seconds",   "10",                                "Trigger seconds (processingTime only)")
dbutils.widgets.text(    "secret_scope",      "market-streaming",                  "Secret scope")


target_catalog    = dbutils.widgets.get("target_catalog")
target_schema     = dbutils.widgets.get("target_schema")
target_table      = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
subscribe_pattern = dbutils.widgets.get("subscribe_pattern")
checkpoint_path   = dbutils.widgets.get("checkpoint_path")
starting_offsets  = dbutils.widgets.get("starting_offsets")
trigger_type      = dbutils.widgets.get("trigger_type")
trigger_seconds   = int(dbutils.widgets.get("trigger_seconds"))
scope             = dbutils.widgets.get("secret_scope")

kafka_bootstrap = dbutils.secrets.get(scope=scope, key="kafka-bootstrap-servers")
kafka_username  = dbutils.secrets.get(scope=scope, key="kafka-sasl-username")
kafka_password  = dbutils.secrets.get(scope=scope, key="kafka-sasl-password")

print(f"target_table      = {target_table}")
print(f"subscribe_pattern = {subscribe_pattern}")
print(f"checkpoint_path   = {checkpoint_path}")
print(f"starting_offsets  = {starting_offsets}")
print(f"trigger_type      = {trigger_type}")

# COMMAND ----------

# MAGIC %md ## DDL (idempotent)

# COMMAND ----------

from market_streaming.bronze.transforms import bronze_ddl

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(bronze_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Start streaming query
# MAGIC
# MAGIC - **availableNow**: cell runs to completion when Kafka is drained. Re-run
# MAGIC   to pick up new messages. `query.awaitTermination()` is called automatically.
# MAGIC - **processingTime**: cell returns immediately; query keeps running in the
# MAGIC   background. Stop it with `query.stop()`.

# COMMAND ----------

from market_streaming.bronze.transforms import build_bronze_stream

query = build_bronze_stream(
    spark=spark,
    bootstrap_servers=kafka_bootstrap,
    sasl_username=kafka_username,
    sasl_password=kafka_password,
    subscribe_pattern=subscribe_pattern,
    checkpoint_path=checkpoint_path,
    target_table=target_table,
    starting_offsets=starting_offsets,
    trigger_type=trigger_type,
    trigger_seconds=trigger_seconds,
)

print(f"query id     = {query.id}")
print(f"query run id = {query.runId}")
print(f"status       = {query.status}")

# For availableNow: block until the batch completes so the cell shows done.
# For processingTime: comment this out and monitor via query.recentProgress.
if trigger_type in ("availableNow", "once"):
    query.awaitTermination()
    print("batch complete")

# COMMAND ----------
# MAGIC %md ## Verify rows landed

# COMMAND ----------
spark.sql(f"""
SELECT
  kafka_topic, kafka_partition,
  COUNT(*)           AS rows,
  MIN(kafka_offset)  AS min_offset,
  MAX(kafka_offset)  AS max_offset,
  MAX(ingest_timestamp) AS last_ingest
FROM {target_table}
GROUP BY kafka_topic, kafka_partition
ORDER BY kafka_topic, kafka_partition
""").display()


