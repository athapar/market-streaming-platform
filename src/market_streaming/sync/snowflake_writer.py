"""
Snowflake sync utilities — write Spark DataFrames into Snowflake tables.

Uses snowflake-connector-python + write_pandas rather than the Spark-Snowflake
Maven connector because:
  - Databricks Serverless cannot attach Maven jars at the cluster level.
  - Data volume is trivial: 5 symbols × O(days) × O(390 bars/day).

At larger scale swap write_pandas for the Spark connector:
  spark.write.format("net.snowflake.spark.snowflake") ...
but the interface below stays the same — just replace sync_table's body.

Snowflake naming convention: tables and columns are created UPPERCASE by
default. write_pandas normalises column names to uppercase automatically when
auto_create_table=False; the Snowflake DDL uses matching uppercase names.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    import snowflake.connector


def build_connection(
    account: str,
    user: str,
    password: str,
    warehouse: str,
    database: str,
    schema: str,
    role: str | None = None,
) -> "snowflake.connector.SnowflakeConnection":
    """Open and return a Snowflake connection."""
    import snowflake.connector

    params: dict = dict(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    if role:
        params["role"] = role
    return snowflake.connector.connect(**params)


def sync_table(
    df: "DataFrame",
    conn: "snowflake.connector.SnowflakeConnection",
    sf_table: str,
    mode: str = "replace",
) -> int:
    """Write a Spark DataFrame to a Snowflake table and return row count written.

    mode="replace"  truncate-then-insert (default). Always produces a correct
                    snapshot. Safe to re-run; idempotent.
    mode="append"   insert-only, no truncation. Use when the caller has already
                    filtered to only net-new rows.

    Column names are uppercased to match Snowflake's default case folding.
    The Snowflake table must already exist (auto_create_table=False) so DDL is
    explicit and version-controlled.
    """
    from snowflake.connector.pandas_tools import write_pandas

    pdf = df.toPandas()
    pdf.columns = [c.upper() for c in pdf.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=pdf,
        table_name=sf_table.upper(),
        overwrite=(mode == "replace"),
        auto_create_table=False,
    )
    if not success:
        raise RuntimeError(f"write_pandas reported failure for table {sf_table}")
    return nrows


def execute_sql(conn: "snowflake.connector.SnowflakeConnection", sql: str) -> list:
    """Execute a single SQL statement and return all result rows."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Snowflake DDL (mirrors Gold Delta schema)
# ---------------------------------------------------------------------------

SNOWFLAKE_DDL = """
CREATE DATABASE IF NOT EXISTS MARKET_STREAMING;

CREATE SCHEMA IF NOT EXISTS MARKET_STREAMING.GOLD;

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.GOLD.GOLD_MINUTE_BARS (
  COMPOSITE_FIGI   VARCHAR     NOT NULL,
  SYMBOL           VARCHAR     NOT NULL,
  EVENT_TYPE       VARCHAR,
  WINDOW_START     TIMESTAMP_NTZ NOT NULL,
  WINDOW_END       TIMESTAMP_NTZ,
  EVENT_DATE       DATE,
  OPEN_PRICE       FLOAT,
  HIGH_PRICE       FLOAT,
  LOW_PRICE        FLOAT,
  CLOSE_PRICE      FLOAT,
  VOLUME           NUMBER,
  VWAP             FLOAT,
  TRADE_COUNT      NUMBER,
  SILVER_TIMESTAMP TIMESTAMP_NTZ,
  PRIMARY KEY (COMPOSITE_FIGI, WINDOW_START)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.GOLD.GOLD_DAILY_ROLLUP (
  COMPOSITE_FIGI   VARCHAR NOT NULL,
  SYMBOL           VARCHAR NOT NULL,
  EVENT_DATE       DATE    NOT NULL,
  OPEN_PRICE       FLOAT,
  HIGH_PRICE       FLOAT,
  LOW_PRICE        FLOAT,
  CLOSE_PRICE      FLOAT,
  VOLUME           NUMBER,
  VWAP             FLOAT,
  TOTAL_TRADES     NUMBER,
  BAR_COUNT        NUMBER,
  FIRST_BAR_START  TIMESTAMP_NTZ,
  LAST_BAR_START   TIMESTAMP_NTZ,
  UPDATED_AT       TIMESTAMP_NTZ,
  PRIMARY KEY (COMPOSITE_FIGI, EVENT_DATE)
);

CREATE SCHEMA IF NOT EXISTS MARKET_STREAMING.RECON;

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.BATCH_DAILY_PRICES (
  COMPOSITE_FIGI   VARCHAR NOT NULL,
  SYMBOL           VARCHAR NOT NULL,
  PRICE_DATE       DATE    NOT NULL,
  OPEN_PRICE       FLOAT,
  HIGH_PRICE       FLOAT,
  LOW_PRICE        FLOAT,
  CLOSE_PRICE      FLOAT,
  VOLUME           NUMBER,
  VWAP             FLOAT,
  SOURCE           VARCHAR DEFAULT 'batch_bigquery',
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI, PRICE_DATE)
);
"""
