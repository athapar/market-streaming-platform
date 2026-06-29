"""
Bridge the batch pipeline's dividend yield mart from BigQuery into Snowflake.

BQ source: mart_dividend_yield  (one row per composite_figi, ex_dividend_date)
SF target: MARKET_STREAMING.RECON.DIVIDEND_YIELD

Full TRUNCATE + INSERT — the mart recomputes TTM yields from full dividend
history each batch run, so a partial reload would be inconsistent. Dataset is
tiny (~8K rows for 104 symbols × 20 years), so the full replace is cheap.

Usage:
    python scripts/bq_to_snowflake_dividends.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd
from google.cloud import bigquery

from market_streaming.config import require_env
from market_streaming.sync.snowflake_writer import apply_ddl, connect_from_env, execute_sql


SF_TABLE = "DIVIDEND_YIELD"


def build_query(project: str, dataset: str) -> str:
    return f"""
        SELECT
            composite_figi,
            ticker,
            ex_dividend_date,
            cash_amount,
            ttm_dividends_per_share,
            close_price,
            ttm_dividend_yield
        FROM `{project}.{dataset}.mart_dividend_yield`
        WHERE composite_figi IS NOT NULL
    """


def fetch() -> pd.DataFrame:
    project = require_env("GOOGLE_CLOUD_PROJECT")
    dataset = require_env("BQ_DATASET_ID")
    require_env("GOOGLE_APPLICATION_CREDENTIALS")

    client = bigquery.Client(project=project)
    df = client.query(build_query(project, dataset)).result().to_dataframe()
    print(f"BQ mart_dividend_yield: {len(df):,} rows")
    return df


def load(df: pd.DataFrame) -> int:
    from snowflake.connector.pandas_tools import write_pandas

    conn = connect_from_env(database="MARKET_STREAMING", schema="RECON")
    try:
        apply_ddl(conn)
        execute_sql(conn, f"TRUNCATE TABLE IF EXISTS {SF_TABLE}")
        if df.empty:
            print(f"Snowflake {SF_TABLE}: 0 rows (BQ returned empty)")
            return 0

        df = df.copy()
        df.columns = [c.upper() for c in df.columns]
        _, _, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=SF_TABLE,
            database="MARKET_STREAMING",
            schema="RECON",
            overwrite=False,
            auto_create_table=False,
        )
        print(f"Snowflake {SF_TABLE}: {nrows:,} rows inserted")
        return nrows
    finally:
        conn.close()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="skip Snowflake write")
    args = p.parse_args()

    df = fetch()
    if df.empty:
        print("BQ returned no dividend rows — check upstream mart")
        return 0

    print(df.head(10).to_string(index=False))

    if args.dry_run:
        print("\ndry-run: skipping Snowflake write")
        return 0

    load(df)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
