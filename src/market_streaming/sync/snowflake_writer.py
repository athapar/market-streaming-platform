"""
Snowflake sync utilities — write Spark DataFrames into Snowflake tables.

Why not write_pandas:
  write_pandas stages data as Parquet via Arrow. Arrow serialises all
  timestamps as int64 (microseconds or nanoseconds since epoch). Snowflake
  reads that raw int64 as NUMBER(38,0) and rejects it when the target column
  is TIMESTAMP_NTZ — regardless of how the pandas dtype is set beforehand.
  Two patch attempts confirmed this is a version-sensitive Arrow/connector
  compatibility issue, not a simple dtype coercion.

Why not the Spark-Snowflake Maven connector:
  Can't be attached on Databricks Serverless.

What sync_table does instead — two paths, auto-selected by row count:

  1. SMALL TABLES (< 50K rows): collect() to driver, build native Python
     tuples (datetime/date for timestamps), cursor.executemany().
     Correct, simple, fast enough at this scale.

  2. LARGE TABLES (>= 50K rows): Spark writes Parquet to a UC Volume staging
     directory, PUT files to a Snowflake user stage, COPY INTO with
     MATCH_BY_COLUMN_NAME. Snowflake's PARQUET copy path handles TIMESTAMP_NTZ
     natively (different code path from write_pandas — the Arrow/int64 bug
     does not apply here). Empirically ~50-100× faster than executemany on
     multi-million-row tables; required for GOLD_TRADES at session-scale
     volume (~7M+ rows).

The bulk_threshold and stage_volume are parameters of sync_table so callers
can tune per table; defaults are conservative.
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


DEFAULT_BULK_THRESHOLD = 50_000          # row count above which bulk path is used
DEFAULT_STAGE_VOLUME   = "/Volumes/main/market_streaming/checkpoints/sync_stage"


def sync_table(
    df: "DataFrame",
    conn: "snowflake.connector.SnowflakeConnection",
    sf_table: str,
    mode: str = "replace",
    bulk_threshold: int = DEFAULT_BULK_THRESHOLD,
    stage_volume: str = DEFAULT_STAGE_VOLUME,
) -> int:
    """Write a Spark DataFrame to a Snowflake table and return row count written.

    mode="replace"  TRUNCATE then load (default). Idempotent.
    mode="append"   Load only, no truncate.

    Auto-routes between two backends based on row count:

      rows < bulk_threshold     cursor.executemany() with native Python types.
                                Correctness-first path; handles TIMESTAMP_NTZ via
                                _coerce. Fast enough for snapshot tables.

      rows >= bulk_threshold    Spark writes Parquet to UC Volume → PUT files
                                to a Snowflake user stage → COPY INTO. Bypasses
                                Arrow entirely; the PARQUET copy path handles
                                TIMESTAMP_NTZ natively. Empirically ~50–100×
                                faster on multi-million-row tables.

    Set bulk_threshold=0 to force the bulk path; set very large to force
    executemany. Default 50K is a conservative crossover point.
    """
    if mode == "replace":
        execute_sql(conn, f"TRUNCATE TABLE IF EXISTS {sf_table.upper()}")

    row_count = df.count()
    if row_count == 0:
        return 0

    if row_count >= bulk_threshold:
        return _bulk_load_via_copy(df, conn, sf_table, row_count, stage_volume)
    return _row_load_via_executemany(df, conn, sf_table)


def _row_load_via_executemany(
    df: "DataFrame",
    conn: "snowflake.connector.SnowflakeConnection",
    sf_table: str,
) -> int:
    """Driver-side row-by-row INSERT. Suitable for <~50K rows."""
    schema    = df.schema
    col_names = [f.name.upper() for f in schema.fields]
    dtypes    = [f.dataType for f in schema.fields]

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


def _bulk_load_via_copy(
    df: "DataFrame",
    conn: "snowflake.connector.SnowflakeConnection",
    sf_table: str,
    row_count: int,
    stage_volume: str,
) -> int:
    """Bulk load: Spark → UC-Volume Parquet → PUT → COPY INTO.

    Snowflake's PARQUET copy path handles TIMESTAMP_NTZ correctly — different
    code path from `write_pandas`, so the Arrow/int64 issue does not apply.

    Steps:
      1. Rename columns to UPPER_CASE (match Snowflake target schema)
      2. Spark writes Parquet to a UC Volume staging directory
      3. PUT each part file to a per-run Snowflake user stage
      4. COPY INTO with MATCH_BY_COLUMN_NAME to map fields by name
      5. REMOVE the stage; delete the local staging directory
    """
    import os
    import shutil
    import time
    import uuid

    run_id   = f"{sf_table.lower()}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
    spark_out = f"{stage_volume}/{run_id}"
    os.makedirs(stage_volume, exist_ok=True)

    # Rename to match Snowflake target column case
    col_names = [f.name.upper() for f in df.schema.fields]
    df_upper  = df.toDF(*col_names)

    # coalesce(4) is a good trade-off: a handful of ~150-200MB files for a
    # 7M-row table, parallelisable across PUT calls without bottlenecking on
    # a single executor for very large datasets.
    n_partitions = max(1, min(8, (row_count // 1_000_000) + 1))
    print(f"  [bulk] writing {row_count:,} rows to Parquet in {n_partitions} part(s) at {spark_out}")
    (
        df_upper
        .coalesce(n_partitions)
        .write.mode("overwrite")
        .parquet(spark_out)
    )

    # Spark writes hidden _SUCCESS / _committed_* metadata files alongside
    # the actual part-*.parquet files; we only PUT the parquet files.
    parquet_files = sorted(
        f for f in os.listdir(spark_out)
        if f.endswith(".parquet") and not f.startswith(".") and not f.startswith("_")
    )
    if not parquet_files:
        raise RuntimeError(f"no parquet output files in {spark_out}")

    stage_name = f"@~/sf_bulk_{run_id}"
    cur = conn.cursor()
    try:
        for fname in parquet_files:
            local_uri = f"file://{spark_out}/{fname}"
            cur.execute(
                f"PUT '{local_uri}' {stage_name} "
                f"AUTO_COMPRESS=TRUE OVERWRITE=TRUE PARALLEL=4"
            )
        print(f"  [bulk] PUT complete ({len(parquet_files)} file(s)) → {stage_name}")

        cur.execute(f"""
            COPY INTO {sf_table.upper()}
            FROM {stage_name}
            FILE_FORMAT = (TYPE = PARQUET)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT
        """)
        # Each COPY result row: (file, status, rows_parsed, rows_loaded, ...)
        results = cur.fetchall()
        rows_loaded = sum(
            int(r[3]) for r in results
            if str(r[1]).upper() in ("LOADED", "PARTIALLY_LOADED")
        )
        print(f"  [bulk] COPY INTO loaded {rows_loaded:,} rows into {sf_table.upper()}")

        # Best-effort cleanup of the Snowflake stage; do not fail the sync on it
        try:
            cur.execute(f"REMOVE {stage_name}")
        except Exception as e:
            print(f"  [bulk] warning: stage cleanup failed ({e!r}) — orphan files in {stage_name}")

        return rows_loaded
    finally:
        cur.close()
        # Best-effort cleanup of the local UC Volume staging directory
        try:
            shutil.rmtree(spark_out, ignore_errors=True)
        except Exception:
            pass


def execute_sql(conn: "snowflake.connector.SnowflakeConnection", sql: str) -> list:
    """Execute a single SQL statement and return all result rows (empty list for DDL)."""
    cur = conn.cursor()
    try:
        cur.execute(sql)
        try:
            return cur.fetchall()
        except Exception:
            # DDL statements (CREATE, TRUNCATE, etc.) may not have a result set
            return []
    finally:
        cur.close()


def apply_ddl(
    conn: "snowflake.connector.SnowflakeConnection",
    verbose: bool = True,
) -> None:
    """Run the full SNOWFLAKE_DDL idempotently (CREATE … IF NOT EXISTS throughout).

    Safe and cheap to call at the start of every bridge / sync script:
    statements are no-ops once the objects exist. Adding a new table to
    SNOWFLAKE_DDL means every bridge auto-bootstraps it on next run.

    After applying DDL we explicitly USE the connection's intended database
    and schema. Snowflake does not always preserve session context across
    a long sequence of CREATE statements, and write_pandas resolves
    unqualified table names against the *current* session schema — so we
    pin it back to the value passed into build_connection().
    """
    import sys

    target_db     = getattr(conn, "database", None) or "MARKET_STREAMING"
    target_schema = getattr(conn, "schema",   None) or "RECON"

    for stmt in SNOWFLAKE_DDL.strip().split(";"):
        s = stmt.strip()
        if not s:
            continue
        first_line = s.splitlines()[0][:80]
        try:
            execute_sql(conn, s)
            if verbose:
                print(f"  [DDL]  {first_line}")
        except Exception as e:
            print(f"  [DDL FAIL] {first_line}", file=sys.stderr)
            print(f"  [DDL FAIL] {e}", file=sys.stderr)
            raise

    # Pin session back to the bridge's target database/schema so unqualified
    # table references (write_pandas, TRUNCATE …) resolve correctly.
    execute_sql(conn, f"USE DATABASE {target_db}")
    execute_sql(conn, f"USE SCHEMA   {target_db}.{target_schema}")
    if verbose:
        print(f"  [DDL]  session pinned to {target_db}.{target_schema}")


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
