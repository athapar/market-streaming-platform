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
    import snowflake.connector
    from pyspark.sql import DataFrame


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


def apply_ddl(conn: "snowflake.connector.SnowflakeConnection") -> None:
    """Run the full SNOWFLAKE_DDL idempotently (CREATE … IF NOT EXISTS throughout).

    Safe and cheap to call at the start of every bridge / sync script:
    statements are no-ops once the objects exist. Adding a new table to
    SNOWFLAKE_DDL means every bridge auto-bootstraps it on next run — no
    separate provisioning step.
    """
    for stmt in SNOWFLAKE_DDL.strip().split(";"):
        s = stmt.strip()
        if s:
            execute_sql(conn, s)


# ---------------------------------------------------------------------------
# Snowflake DDL (mirrors Gold Delta schema)
# ---------------------------------------------------------------------------

SNOWFLAKE_DDL = """
CREATE DATABASE IF NOT EXISTS MARKET_STREAMING;

CREATE SCHEMA IF NOT EXISTS MARKET_STREAMING.GOLD;
CREATE SCHEMA IF NOT EXISTS MARKET_STREAMING.OPS;

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

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.COMPANY_OVERVIEW (
  COMPOSITE_FIGI       VARCHAR       NOT NULL,
  TICKER               VARCHAR       NOT NULL,
  COMPANY_NAME         VARCHAR,
  SIC_CODE             VARCHAR,
  SIC_DESCRIPTION      VARCHAR,
  MARKET_CAP           FLOAT,
  SHARES_OUTSTANDING   FLOAT,
  TOTAL_EMPLOYEES      NUMBER,
  LIST_DATE            DATE,
  LOADED_AT            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.FUNDAMENTALS_VALUATION (
  COMPOSITE_FIGI         VARCHAR       NOT NULL,
  TICKER                 VARCHAR       NOT NULL,
  CLOSE_PRICE            FLOAT,
  PRICE_AS_OF            DATE,
  FINANCIALS_AS_OF       DATE,
  FILING_DATE            DATE,
  QUARTERS_INCLUDED      NUMBER,
  MARKET_CAP             FLOAT,
  SHARES_OUTSTANDING     FLOAT,
  PE_RATIO               FLOAT,
  PB_RATIO               FLOAT,
  PS_RATIO               FLOAT,
  EV_EBIT                FLOAT,
  PRICE_TO_FCF           FLOAT,
  GROSS_MARGIN           FLOAT,
  OPERATING_MARGIN       FLOAT,
  NET_MARGIN             FLOAT,
  ROE                    FLOAT,
  ROA                    FLOAT,
  CURRENT_RATIO          FLOAT,
  DEBT_TO_EQUITY         FLOAT,
  TTM_REVENUE            FLOAT,
  TTM_NET_INCOME         FLOAT,
  TTM_OPERATING_INCOME   FLOAT,
  TTM_FREE_CASH_FLOW     FLOAT,
  BOOK_VALUE             FLOAT,
  TOTAL_ASSETS           FLOAT,
  TOTAL_LIABILITIES      FLOAT,
  LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.FUNDAMENTALS_FACTOR_SCORES (
  COMPOSITE_FIGI         VARCHAR       NOT NULL,
  TICKER                 VARCHAR       NOT NULL,
  VALUE_SCORE            FLOAT,
  GROWTH_SCORE           FLOAT,
  QUALITY_SCORE          FLOAT,
  FACTOR_CLASSIFICATION  VARCHAR,
  PE_RATIO               FLOAT,
  PB_RATIO               FLOAT,
  OPERATING_MARGIN       FLOAT,
  ROE                    FLOAT,
  DEBT_TO_EQUITY         FLOAT,
  FCF_CONVERSION         FLOAT,
  LOADED_AT              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.DIVIDEND_YIELD (
  COMPOSITE_FIGI            VARCHAR       NOT NULL,
  TICKER                    VARCHAR       NOT NULL,
  EX_DIVIDEND_DATE          DATE          NOT NULL,
  CASH_AMOUNT               FLOAT,
  TTM_DIVIDENDS_PER_SHARE   FLOAT,
  CLOSE_PRICE               FLOAT,
  TTM_DIVIDEND_YIELD        FLOAT,
  LOADED_AT                 TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI, EX_DIVIDEND_DATE)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.RECON.BATCH_DAILY_RETURNS (
  COMPOSITE_FIGI   VARCHAR       NOT NULL,
  TICKER           VARCHAR       NOT NULL,
  PRICE_DATE       DATE          NOT NULL,
  CLOSE_PRICE      FLOAT,
  DAILY_RETURN     FLOAT,
  VOLATILITY_20D   FLOAT,
  VOLATILITY_60D   FLOAT,
  LOADED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (COMPOSITE_FIGI, PRICE_DATE)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.GOLD.GOLD_TRADES (
  COMPOSITE_FIGI   VARCHAR       NOT NULL,
  SYMBOL           VARCHAR       NOT NULL,
  TRADE_ID         VARCHAR       NOT NULL,
  TRADE_PRICE      FLOAT         NOT NULL,
  TRADE_SIZE       NUMBER        NOT NULL,
  EXCHANGE_ID      NUMBER,
  TAPE             NUMBER,
  SIP_TIMESTAMP    TIMESTAMP_NTZ NOT NULL,
  TRADE_DATE       DATE,
  SILVER_TIMESTAMP TIMESTAMP_NTZ,
  PRIMARY KEY (COMPOSITE_FIGI, TRADE_ID)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.GOLD.GOLD_QUOTE_STATS (
  COMPOSITE_FIGI     VARCHAR       NOT NULL,
  SYMBOL             VARCHAR       NOT NULL,
  WINDOW_START       TIMESTAMP_NTZ NOT NULL,
  QUOTE_DATE         DATE          NOT NULL,
  QUOTE_COUNT        NUMBER,
  AVG_BID_PRICE      FLOAT,
  AVG_ASK_PRICE      FLOAT,
  AVG_SPREAD_DOLLARS FLOAT,
  AVG_SPREAD_BPS     FLOAT,
  MIN_SPREAD_BPS     FLOAT,
  MAX_SPREAD_BPS     FLOAT,
  AVG_MID_PRICE      FLOAT,
  AVG_BID_SIZE       FLOAT,
  AVG_ASK_SIZE       FLOAT,
  BID_SIZE_TOTAL     NUMBER,
  ASK_SIZE_TOTAL     NUMBER,
  ORDER_IMBALANCE    FLOAT,
  UPDATED_AT         TIMESTAMP_NTZ,
  PRIMARY KEY (COMPOSITE_FIGI, WINDOW_START)
);

CREATE TABLE IF NOT EXISTS MARKET_STREAMING.OPS.PIPELINE_BATCH_METRICS (
  LAYER          VARCHAR       NOT NULL,
  BATCH_ID       NUMBER        NOT NULL,
  ROWS_IN        NUMBER        NOT NULL,
  ROWS_OUT       NUMBER        NOT NULL,
  DURATION_MS    NUMBER        NOT NULL,
  STARTED_AT     TIMESTAMP_NTZ NOT NULL,
  COMPLETED_AT   TIMESTAMP_NTZ NOT NULL,
  STATUS         VARCHAR       NOT NULL,
  ERROR_MESSAGE  VARCHAR
);
"""
