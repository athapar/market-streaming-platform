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

    --date      specific trading date to sync (default: today)
    --dry-run   query BQ and print rows but skip Snowflake write
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timezone

import pandas as pd
from google.cloud import bigquery

from market_streaming.config import load_symbols, require_env, optional_env
from market_streaming.sync.snowflake_writer import apply_ddl, build_connection, execute_sql


# ---------------------------------------------------------------------------
# BQ query — pulls from the batch pipeline's daily fact table
# ---------------------------------------------------------------------------

def build_bq_query(project: str, dataset: str, symbols: list[str], price_date: date) -> str:
    symbol_list = ", ".join(f"'{s}'" for s in symbols)
    date_str    = price_date.isoformat()
    # Default to the batch's split-adjusted daily fact (fact_daily_prices).
    # Override with BQ_MART_TABLE if the table name differs in your project.
    mart_table  = optional_env("BQ_MART_TABLE") or "fact_daily_prices"
    # composite_figi is already a column on fact_daily_prices (the batch dbt
    # model attaches it via int_security_master_historical), so no SCD2 join
    # is needed here. The previous SCD2 join silently dropped tickers that
    # Polygon's reference endpoint returns no composite_figi for (ACN, BK,
    # LIN, MDT). Reading the column directly preserves the full universe.
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
        FROM `{project}.{dataset}.{mart_table}`
        WHERE price_date = '{date_str}'
          AND ticker IN ({symbol_list})
          AND composite_figi IS NOT NULL
    """


def fetch_batch_prices(price_date: date) -> pd.DataFrame:
    project = require_env("GOOGLE_CLOUD_PROJECT")
    dataset = require_env("BQ_DATASET_ID")
    require_env("GOOGLE_APPLICATION_CREDENTIALS")

    symbols = load_symbols()
    if not symbols:
        raise RuntimeError("symbols.txt is empty")

    client = bigquery.Client(project=project)
    query  = build_bq_query(project, dataset, symbols, price_date)
    df     = client.query(query).result().to_dataframe()
    print(f"BQ returned {len(df)} rows for {price_date}")
    return df


# ---------------------------------------------------------------------------
# Snowflake load
# ---------------------------------------------------------------------------

def load_to_snowflake(df: pd.DataFrame, price_date: date) -> int:
    """DELETE existing rows for price_date then INSERT fresh from BQ."""
    from snowflake.connector.pandas_tools import write_pandas

    conn = build_connection(
        account   = require_env("SNOWFLAKE_ACCOUNT"),
        user      = require_env("SNOWFLAKE_USER"),
        password  = require_env("SNOWFLAKE_PASSWORD"),
        warehouse = require_env("SNOWFLAKE_WAREHOUSE"),
        database  = "MARKET_STREAMING",
        schema    = "RECON",
        role      = optional_env("SNOWFLAKE_ROLE"),
    )

    try:
        apply_ddl(conn)
        # Delete the date's rows first so the load is idempotent
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
        "--dry-run",
        action="store_true",
        help="Fetch from BQ and print rows; skip Snowflake write",
    )
    args = p.parse_args()

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
