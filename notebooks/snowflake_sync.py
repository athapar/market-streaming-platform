# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Sync — Gold Delta → Snowflake
# MAGIC
# MAGIC Reads the two Gold Delta tables and writes them into Snowflake using
# MAGIC `snowflake-connector-python` + `write_pandas`.
# MAGIC
# MAGIC **Run order:**
# MAGIC 1. Install package.
# MAGIC 2. Add Snowflake secrets to the secret scope (one-time — see cell below).
# MAGIC 3. Run DDL cell once to create Snowflake objects.
# MAGIC 4. Run sync cells after each Gold ingest run.
# MAGIC
# MAGIC **Snowflake objects created:**
# MAGIC - Database  : `MARKET_STREAMING`
# MAGIC - Schema    : `MARKET_STREAMING.GOLD`     — streaming Gold tables
# MAGIC - Schema    : `MARKET_STREAMING.RECON`    — batch BigQuery bridge (for dbt recon)
# MAGIC - Tables    : `GOLD_MINUTE_BARS`, `GOLD_DAILY_ROLLUP`, `BATCH_DAILY_PRICES`

# COMMAND ----------
# MAGIC %md ## Install package

# COMMAND ----------
# MAGIC %restart_python

# COMMAND ----------
import sys

repo_root = "/Workspace/Users/armaant.08@gmail.com/market-streaming-pipeline"
if f"{repo_root}/src" not in sys.path:
    sys.path.insert(0, f"{repo_root}/src")

# COMMAND ----------
# MAGIC %pip install snowflake-connector-python[pandas]

# COMMAND ----------
from market_streaming.sync.snowflake_writer import (
    SNOWFLAKE_DDL, build_connection, execute_sql, sync_table,
)
print("import ok")

# COMMAND ----------
# MAGIC %md
# MAGIC ## One-time: add Snowflake secrets to the scope
# MAGIC
# MAGIC Run from your **local terminal** (Databricks CLI):
# MAGIC ```bash
# MAGIC databricks secrets put-secret market-streaming snowflake-account   --string-value "abc12345.us-east-1"
# MAGIC databricks secrets put-secret market-streaming snowflake-user      --string-value "STREAMING_USER"
# MAGIC databricks secrets put-secret market-streaming snowflake-password  --string-value "..."
# MAGIC databricks secrets put-secret market-streaming snowflake-warehouse --string-value "COMPUTE_WH"
# MAGIC databricks secrets put-secret market-streaming snowflake-role      --string-value "SYSADMIN"
# MAGIC ```
# MAGIC Your Snowflake account identifier is on the Snowflake login page, format:
# MAGIC `orgname-accountname` (e.g. `myorg-ab12345`) — use that, not the full URL.

# COMMAND ----------
# MAGIC %md ## Configuration

# COMMAND ----------
dbutils.widgets.text("target_catalog",    "main",                                       "Target catalog")
dbutils.widgets.text("target_schema",     "market_streaming",                           "Target schema")
dbutils.widgets.text("minute_table",      "main.market_streaming.gold_minute_bars",     "Gold minute bars table")
dbutils.widgets.text("daily_table",       "main.market_streaming.gold_daily_rollup",    "Gold daily rollup table")
dbutils.widgets.text("secret_scope",      "market-streaming",                           "Secret scope")
dbutils.widgets.text("sf_database",       "MARKET_STREAMING",                           "Snowflake database")
dbutils.widgets.text("sf_schema",         "GOLD",                                       "Snowflake schema")

scope         = dbutils.widgets.get("secret_scope")
minute_table  = dbutils.widgets.get("minute_table")
daily_table   = dbutils.widgets.get("daily_table")
sf_database   = dbutils.widgets.get("sf_database")
sf_schema     = dbutils.widgets.get("sf_schema")

sf_account    = dbutils.secrets.get(scope=scope, key="snowflake-account")
sf_user       = dbutils.secrets.get(scope=scope, key="snowflake-user")
sf_password   = dbutils.secrets.get(scope=scope, key="snowflake-password")
sf_warehouse  = dbutils.secrets.get(scope=scope, key="snowflake-warehouse")
sf_role       = dbutils.secrets.get(scope=scope, key="snowflake-role")

print(f"Snowflake account : [redacted — set]")
print(f"minute_table      : {minute_table}")
print(f"daily_table       : {daily_table}")

# COMMAND ----------
# MAGIC %md ## One-time DDL — create Snowflake objects

# COMMAND ----------
# Run this cell once. It is idempotent (CREATE IF NOT EXISTS throughout).
conn = build_connection(
    account=sf_account, user=sf_user, password=sf_password,
    warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
    role=sf_role,
)

for statement in SNOWFLAKE_DDL.strip().split(";"):
    stmt = statement.strip()
    if stmt:
        execute_sql(conn, stmt)
        print(f"OK: {stmt[:60]}...")

conn.close()
print("DDL complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Sync gold_minute_bars → Snowflake
# MAGIC
# MAGIC Full replace: truncates `GOLD_MINUTE_BARS` then inserts current Gold snapshot.
# MAGIC Re-run any time after a Gold ingest run.

# COMMAND ----------
conn = build_connection(
    account=sf_account, user=sf_user, password=sf_password,
    warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
    role=sf_role,
)

try:
    minute_df = spark.read.format("delta").table(minute_table)
    n = sync_table(minute_df, conn, "GOLD_MINUTE_BARS", mode="replace")
    print(f"GOLD_MINUTE_BARS : {n:,} rows written")
finally:
    conn.close()

# COMMAND ----------
# MAGIC %md ## Sync gold_daily_rollup → Snowflake

# COMMAND ----------
conn = build_connection(
    account=sf_account, user=sf_user, password=sf_password,
    warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
    role=sf_role,
)

try:
    daily_df = spark.read.format("delta").table(daily_table)
    n = sync_table(daily_df, conn, "GOLD_DAILY_ROLLUP", mode="replace")
    print(f"GOLD_DAILY_ROLLUP: {n:,} rows written")
finally:
    conn.close()

# COMMAND ----------
# MAGIC %md ## Verify row counts match Delta

# COMMAND ----------
conn = build_connection(
    account=sf_account, user=sf_user, password=sf_password,
    warehouse=sf_warehouse, database=sf_database, schema=sf_schema,
    role=sf_role,
)

try:
    delta_minute = spark.read.format("delta").table(minute_table).count()
    delta_daily  = spark.read.format("delta").table(daily_table).count()

    sf_minute = execute_sql(conn, "SELECT COUNT(*) FROM GOLD_MINUTE_BARS")[0][0]
    sf_daily  = execute_sql(conn, "SELECT COUNT(*) FROM GOLD_DAILY_ROLLUP")[0][0]

    print(f"gold_minute_bars  — Delta: {delta_minute:,}  Snowflake: {sf_minute:,}  match={delta_minute == sf_minute}")
    print(f"gold_daily_rollup — Delta: {delta_daily:,}   Snowflake: {sf_daily:,}   match={delta_daily == sf_daily}")
finally:
    conn.close()
