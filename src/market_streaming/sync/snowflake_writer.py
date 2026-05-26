"""
Snowflake sync utilities — write Spark DataFrames into Snowflake tables.

Why not write_pandas / Spark-Snowflake connector:
  - write_pandas stages data as Parquet via Arrow. Arrow serialises all
    timestamps as int64 (microseconds or nanoseconds since epoch). Snowflake
    reads that raw int64 as NUMBER(38,0) and rejects it when the target column
    is TIMESTAMP_NTZ — regardless of how the pandas dtype is set beforehand.
    Two patch attempts confirmed this is a version-sensitive Arrow/connector
    compatibility issue, not a simple dtype coercion.
  - Spark-Snowflake Maven connector cannot be attached on Databricks Serverless.

Solution: collect() the Spark DataFrame to Python Row objects, convert each
field to the native Python type Snowflake connector expects (datetime.datetime
for TIMESTAMP_NTZ, datetime.date for DATE), and insert via cursor.executemany().
For 5 symbols × O(400 bars/day) this is fast enough (<5 s). The interface is
identical so swapping to write_pandas or the Spark connector later is one line.
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


def _coerce(val, dtype):
    """Convert a single Spark Row value to a type Snowflake connector accepts.

    Spark Row.collect() returns:
      TimestampType → datetime.datetime (may be UTC-aware in Spark Connect)
      DateType      → datetime.date
      All others    → native Python scalar

    Snowflake TIMESTAMP_NTZ requires a timezone-naive datetime.datetime.
    Passing a tz-aware datetime causes a silent wrong-value insert; passing
    an int causes the NUMBER(38,0) rejection we've been seeing.
    """
    import datetime
    from pyspark.sql.types import DateType, TimestampType

    if val is None:
        return None

    if isinstance(dtype, TimestampType):
        if isinstance(val, datetime.datetime):
            # Strip timezone info — Snowflake TIMESTAMP_NTZ is always naive
            return val.replace(tzinfo=None)
        if isinstance(val, (int, float)):
            # Fallback: epoch microseconds (Spark Connect sometimes returns these)
            return datetime.datetime(1970, 1, 1) + datetime.timedelta(microseconds=int(val))
        # Last resort: string parse
        return datetime.datetime.fromisoformat(str(val))

    if isinstance(dtype, DateType):
        if isinstance(val, datetime.date):
            return val
        return datetime.date.fromisoformat(str(val))

    return val


def sync_table(
    df: "DataFrame",
    conn: "snowflake.connector.SnowflakeConnection",
    sf_table: str,
    mode: str = "replace",
) -> int:
    """Write a Spark DataFrame to a Snowflake table and return row count written.

    mode="replace"  TRUNCATE then INSERT (default). Idempotent, always correct.
    mode="append"   INSERT only.

    Uses cursor.executemany() with native Python types — bypasses Arrow/Parquet
    staging so there is no timestamp-as-int64 ambiguity.
    """
    schema = df.schema
    col_names = [f.name.upper() for f in schema.fields]
    dtypes    = [f.dataType for f in schema.fields]

    if mode == "replace":
        cur = conn.cursor()
        try:
            cur.execute(f"TRUNCATE TABLE IF EXISTS {sf_table.upper()}")
        finally:
            cur.close()

    rows = df.collect()
    if not rows:
        return 0

    data = [
        tuple(_coerce(row[i], dtypes[i]) for i in range(len(schema.fields)))
        for row in rows
    ]

    placeholders = ", ".join(["%s"] * len(col_names))
    cols_clause  = ", ".join(col_names)
    insert_sql   = (
        f"INSERT INTO {sf_table.upper()} ({cols_clause}) VALUES ({placeholders})"
    )

    cur = conn.cursor()
    try:
        cur.executemany(insert_sql, data)
        return len(data)
    finally:
        cur.close()


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
  COMPOSITE_FIGI   VARCHAR       NOT NULL,
  SYMBOL           VARCHAR       NOT NULL,
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
  COMPOSITE_FIGI  VARCHAR       NOT NULL,
  SYMBOL          VARCHAR       NOT NULL,
  EVENT_DATE      DATE          NOT NULL,
  OPEN_PRICE      FLOAT,
  HIGH_PRICE      FLOAT,
  LOW_PRICE       FLOAT,
  CLOSE_PRICE     FLOAT,
  VOLUME          NUMBER,
  VWAP            FLOAT,
  TOTAL_TRADES    NUMBER,
  BAR_COUNT       NUMBER,
  FIRST_BAR_START TIMESTAMP_NTZ,
  LAST_BAR_START  TIMESTAMP_NTZ,
  UPDATED_AT      TIMESTAMP_NTZ,
  PRIMARY KEY (COMPOSITE_FIGI, EVENT_DATE)
);

CREATE SCHEMA IF NOT EXISTS MARKET_STREAMING.RECON;

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.BATCH_DAILY_PRICES (
  COMPOSITE_FIGI  VARCHAR       NOT NULL,
  SYMBOL          VARCHAR       NOT NULL,
  PRICE_DATE      DATE          NOT NULL,
  OPEN_PRICE      FLOAT,
  HIGH_PRICE      FLOAT,
  LOW_PRICE       FLOAT,
  CLOSE_PRICE     FLOAT,
  VOLUME          NUMBER,
  VWAP            FLOAT,
  SOURCE          VARCHAR       DEFAULT 'batch_bigquery',
  LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI, PRICE_DATE)
);
"""
