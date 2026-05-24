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
# MAGIC See `docs/phase2_databricks.md` for workspace, Repos, and secrets setup.

# COMMAND ----------
# MAGIC %md ## Install package

# COMMAND ----------
# MAGIC %pip install -e /Workspace/Repos/${workspace_user}/market-streaming-pipeline
# MAGIC %restart_python

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------
dbutils.widgets.text("target_catalog", "main", "Target catalog")
dbutils.widgets.text("target_schema", "market_streaming", "Target schema")
dbutils.widgets.text("target_table_name", "bronze_market_events", "Target table")
dbutils.widgets.text("subscribe_pattern", "market\\..*", "Kafka topic pattern")
dbutils.widgets.text("checkpoint_path", "dbfs:/checkpoints/market_streaming/bronze", "Checkpoint path")
dbutils.widgets.dropdown("starting_offsets", "latest", ["latest", "earliest"], "Starting offsets")
dbutils.widgets.text("trigger_seconds", "10", "Trigger interval (seconds)")
dbutils.widgets.text("secret_scope", "market-streaming", "Secret scope")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = f"{target_catalog}.{target_schema}.{dbutils.widgets.get('target_table_name')}"
subscribe_pattern = dbutils.widgets.get("subscribe_pattern")
checkpoint_path = dbutils.widgets.get("checkpoint_path")
starting_offsets = dbutils.widgets.get("starting_offsets")
trigger_seconds = int(dbutils.widgets.get("trigger_seconds"))
scope = dbutils.widgets.get("secret_scope")

kafka_bootstrap = dbutils.secrets.get(scope=scope, key="kafka-bootstrap-servers")
kafka_username = dbutils.secrets.get(scope=scope, key="kafka-sasl-username")
kafka_password = dbutils.secrets.get(scope=scope, key="kafka-sasl-password")

print(f"target_table       = {target_table}")
print(f"subscribe_pattern  = {subscribe_pattern}")
print(f"checkpoint_path    = {checkpoint_path}")
print(f"starting_offsets   = {starting_offsets}")
print(f"trigger_seconds    = {trigger_seconds}")

# COMMAND ----------
# MAGIC %md ## DDL (idempotent)

# COMMAND ----------
from market_streaming.bronze.transforms import bronze_ddl

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(bronze_ddl(target_table))
display(spark.sql(f"DESCRIBE TABLE EXTENDED {target_table}"))

# COMMAND ----------
# MAGIC %md ## Start streaming query

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
    trigger_seconds=trigger_seconds,
)

print(f"query id     = {query.id}")
print(f"query run id = {query.runId}")
print(f"status       = {query.status}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Operational notes
# MAGIC - `query.awaitTermination()` blocks the notebook; in Databricks Jobs
# MAGIC   the cluster keeps it alive. Interactively, you can leave this cell
# MAGIC   commented out and inspect `query.recentProgress` instead.
# MAGIC - To stop cleanly: `query.stop()`.
# MAGIC - To restart from scratch: stop the query, delete the checkpoint
# MAGIC   directory, drop the table, re-run.

# COMMAND ----------
# query.awaitTermination()
