"""
Export the batch pipeline's daily price mart from BigQuery and load it into
Snowflake MARKET_STREAMING.RECON.BATCH_DAILY_PRICES.

This is the recon bridge: the batch pipeline produces daily OHLCV in BigQuery;
the streaming pipeline produces daily OHLCV in Snowflake. dbt's recon models
join on (composite_figi, date) and compute the delta.

Run once per trading day after the batch pipeline completes (typically
after market close + batch job runtime, ~7–8 PM ET).

The BQ mart queried is fct_daily_ohlcv (or equivalent) from the batch
pipeline. Adjust BQ_MART_TABLE in your .env if the table name differs.

Usage:
    python scripts/bq_to_snowflake_batch.py [--date YYYY-MM-DD] [--dry-run]
    python scripts/bq_to_snowflake_batch.py --backfill [--dry-run]

    --date      specific trading date to sync (default: today)
    --backfill  load ALL history from BigQuery (truncate + full reload)
    --dry-run   query BQ and print rows but skip Snowflake write
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone

import pandas as pd
from google.cloud import bigquery

from market_streaming.config import load_symbols, require_env, optional_env
from market_streaming.sync.snowflake_writer import apply_ddl, connect_from_env, execute_sql


# ---------------------------------------------------------------------------
# BQ query — pulls from the batch pipeline's daily fact table
# ---------------------------------------------------------------------------

def _mart_table() -> str:
    return optional_env("BQ_MART_TABLE") or "fact_daily_prices"


def build_bq_query(project: str, dataset: str, symbols: list[str], price_date: date) -> str:
    symbol_list = ", ".join(f"'{s}'" for s in symbols)
    date_str    = price_date.isoformat()
    return f"""
        SELECT
            composite_figi,
            ticker                  AS symbol,
            price_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            vwap,
            'batch_bigquery'        AS source
        FROM `{project}.{dataset}.{_mart_table()}`
        WHERE price_date = '{date_str}'
          AND ticker IN ({symbol_list})
          AND composite_figi IS NOT NULL
    """


def build_bq_backfill_query(project: str, dataset: str, symbols: list[str]) -> str:
    symbol_list = ", ".join(f"'{s}'" for s in symbols)
    return f"""
        SELECT
            composite_figi,
            ticker                  AS symbol,
            price_date,
            open_price,
            high_price,
            low_price,
            close_price,
            volume,
            vwap,
            'batch_bigquery'        AS source
        FROM `{project}.{dataset}.{_mart_table()}`
        WHERE ticker IN ({symbol_list})
          AND composite_figi IS NOT NULL
        ORDER BY price_date
    """


def _bq_client_and_symbols():
    project = require_env("GOOGLE_CLOUD_PROJECT")
    dataset = require_env("BQ_DATASET_ID")
    require_env("GOOGLE_APPLICATION_CREDENTIALS")
    symbols = load_symbols()
    if not symbols:
        raise RuntimeError("symbols.txt is empty")
    client = bigquery.Client(project=project)
    return client, project, dataset, symbols


def fetch_batch_prices(price_date: date) -> pd.DataFrame:
    client, project, dataset, symbols = _bq_client_and_symbols()
    query = build_bq_query(project, dataset, symbols, price_date)
    df = client.query(query).result().to_dataframe()
    print(f"BQ returned {len(df)} rows for {price_date}")
    return df


def fetch_all_batch_prices() -> pd.DataFrame:
    client, project, dataset, symbols = _bq_client_and_symbols()
    query = build_bq_backfill_query(project, dataset, symbols)
    print(f"fetching full history from {project}.{dataset}.{_mart_table()} ...")
    df = client.query(query).result().to_dataframe()
    date_range = f"{df['price_date'].min()} to {df['price_date'].max()}" if len(df) else "empty"
    print(f"BQ returned {len(df):,} rows ({date_range})")
    return df


# ---------------------------------------------------------------------------
# Snowflake load
# ---------------------------------------------------------------------------

def _sf_conn():
    return connect_from_env(database="MARKET_STREAMING", schema="RECON")


def load_to_snowflake(df: pd.DataFrame, price_date: date) -> int:
    """DELETE existing rows for price_date then INSERT fresh from BQ."""
    from snowflake.connector.pandas_tools import write_pandas

    conn = _sf_conn()
    try:
        apply_ddl(conn)
        execute_sql(
            conn,
            f"DELETE FROM BATCH_DAILY_PRICES WHERE PRICE_DATE = '{price_date.isoformat()}'"
        )

        df.columns = [c.upper() for c in df.columns]
        _, _, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="BATCH_DAILY_PRICES",
            database="MARKET_STREAMING",
            schema="RECON",
            overwrite=False,
            auto_create_table=False,
        )
        print(f"Snowflake BATCH_DAILY_PRICES: {nrows} rows inserted for {price_date}")
        return nrows
    finally:
        conn.close()


def backfill_to_snowflake(df: pd.DataFrame) -> int:
    """TRUNCATE then bulk load entire history from BQ."""
    from snowflake.connector.pandas_tools import write_pandas

    conn = _sf_conn()
    try:
        apply_ddl(conn)
        execute_sql(conn, "TRUNCATE TABLE IF EXISTS BATCH_DAILY_PRICES")
        print("truncated BATCH_DAILY_PRICES")

        df.columns = [c.upper() for c in df.columns]
        _, _, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name="BATCH_DAILY_PRICES",
            database="MARKET_STREAMING",
            schema="RECON",
            overwrite=False,
            auto_create_table=False,
        )
        print(f"Snowflake BATCH_DAILY_PRICES: {nrows:,} rows loaded (full backfill)")
        return nrows
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Trading date to sync (YYYY-MM-DD, default: today)",
    )
    p.add_argument(
        "--backfill",
        action="store_true",
        help="Load ALL history from BigQuery (truncate + full reload)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch from BQ and print summary; skip Snowflake write",
    )
    args = p.parse_args()

    if args.backfill:
        print("backfill mode: loading full history from BigQuery ...")
        df = fetch_all_batch_prices()

        if df.empty:
            print("no batch rows found in BigQuery")
            return 0

        symbols = df["symbol"].nunique()
        dates = df["price_date"].nunique()
        print(f"  {len(df):,} rows — {symbols} symbols × {dates:,} trading days")
        print(f"  date range: {df['price_date'].min()} to {df['price_date'].max()}")

        if args.dry_run:
            print("dry-run: skipping Snowflake write")
            return 0

        backfill_to_snowflake(df)
        return 0

    price_date = date.fromisoformat(args.date)
    print(f"syncing batch prices for {price_date} ...")

    df = fetch_batch_prices(price_date)

    if df.empty:
        print(f"no batch rows for {price_date} — market closed or batch not yet run")
        return 0

    print(df[["composite_figi", "symbol", "price_date", "close_price", "volume"]].to_string(index=False))

    if args.dry_run:
        print("dry-run: skipping Snowflake write")
        return 0

    load_to_snowflake(df, price_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
