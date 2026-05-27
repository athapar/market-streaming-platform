"""
Bridge fundamentals data from the batch BigQuery pipeline into Snowflake.

Three tables are synced (TRUNCATE + INSERT — all are snapshot tables, one row
per composite_figi):

    BQ source                                   Snowflake target
    -----------------------------------------   ------------------------------------
    stg_company_overview                  -->   RECON.COMPANY_OVERVIEW
    mart_fundamentals_valuation           -->   RECON.FUNDAMENTALS_VALUATION
    mart_fundamentals_factor_scores       -->   RECON.FUNDAMENTALS_FACTOR_SCORES

Run after the batch pipeline's dbt build completes (fundamentals refresh
quarterly; safe to run more often — full replace).

Usage:
    python scripts/bq_to_snowflake_fundamentals.py [--dry-run] [--tables ...]

    --dry-run    fetch from BQ and print row counts; skip Snowflake write
    --tables     comma-separated subset: overview,valuation,factors (default: all)
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Callable

import pandas as pd
from google.cloud import bigquery

from market_streaming.config import optional_env, require_env
from market_streaming.sync.snowflake_writer import apply_ddl, build_connection, execute_sql


# ---------------------------------------------------------------------------
# BQ queries
# ---------------------------------------------------------------------------

def query_company_overview(project: str, dataset: str) -> str:
    return f"""
        SELECT
            composite_figi,
            ticker,
            company_name,
            sic_code,
            sic_description,
            market_cap,
            shares_outstanding,
            total_employees,
            list_date
        FROM `{project}.{dataset}.stg_company_overview`
        WHERE composite_figi IS NOT NULL
    """


def query_valuation(project: str, dataset: str) -> str:
    return f"""
        SELECT
            composite_figi,
            ticker,
            close_price,
            price_as_of,
            financials_as_of,
            filing_date,
            quarters_included,
            market_cap,
            shares_outstanding,
            pe_ratio,
            pb_ratio,
            ps_ratio,
            ev_ebit,
            price_to_fcf,
            gross_margin,
            operating_margin,
            net_margin,
            roe,
            roa,
            current_ratio,
            debt_to_equity,
            ttm_revenue,
            ttm_net_income,
            ttm_operating_income,
            ttm_free_cash_flow,
            book_value,
            total_assets,
            total_liabilities
        FROM `{project}.{dataset}.mart_fundamentals_valuation`
    """


def query_factor_scores(project: str, dataset: str) -> str:
    return f"""
        SELECT
            composite_figi,
            ticker,
            value_score,
            growth_score,
            quality_score,
            factor_classification,
            pe_ratio,
            pb_ratio,
            operating_margin,
            roe,
            debt_to_equity,
            fcf_conversion
        FROM `{project}.{dataset}.mart_fundamentals_factor_scores`
    """


# ---------------------------------------------------------------------------
# Bridge spec — one entry per table
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Bridge:
    key: str                                  # CLI alias
    sf_table: str                             # target table name
    build_query: Callable[[str, str], str]    # BQ query builder


BRIDGES: list[Bridge] = [
    Bridge("overview",  "COMPANY_OVERVIEW",            query_company_overview),
    Bridge("valuation", "FUNDAMENTALS_VALUATION",      query_valuation),
    Bridge("factors",   "FUNDAMENTALS_FACTOR_SCORES",  query_factor_scores),
]


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def fetch(bq_client: bigquery.Client, bridge: Bridge, project: str, dataset: str) -> pd.DataFrame:
    df = bq_client.query(bridge.build_query(project, dataset)).result().to_dataframe()
    print(f"[BQ]  {bridge.sf_table:<32} {len(df):>6,} rows")
    return df


def load(conn, bridge: Bridge, df: pd.DataFrame) -> int:
    """TRUNCATE + INSERT for snapshot tables — full replace is idempotent."""
    from snowflake.connector.pandas_tools import write_pandas

    execute_sql(conn, f"TRUNCATE TABLE IF EXISTS {bridge.sf_table}")
    if df.empty:
        print(f"[SF]  {bridge.sf_table:<32} (empty — nothing to insert)")
        return 0

    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    _, _, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=bridge.sf_table,
        overwrite=False,
        auto_create_table=False,
    )
    print(f"[SF]  {bridge.sf_table:<32} {nrows:>6,} rows inserted")
    return nrows


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dry-run", action="store_true", help="skip Snowflake write")
    p.add_argument(
        "--tables",
        default=",".join(b.key for b in BRIDGES),
        help=f"comma-separated subset: {','.join(b.key for b in BRIDGES)}",
    )
    args = p.parse_args()

    wanted = {t.strip() for t in args.tables.split(",") if t.strip()}
    selected = [b for b in BRIDGES if b.key in wanted]
    if not selected:
        print(f"no tables selected (got: {args.tables})", file=sys.stderr)
        return 1

    project = require_env("GOOGLE_CLOUD_PROJECT")
    dataset = require_env("BQ_DATASET_ID")
    require_env("GOOGLE_APPLICATION_CREDENTIALS")
    bq_client = bigquery.Client(project=project)

    frames = [(b, fetch(bq_client, b, project, dataset)) for b in selected]

    if args.dry_run:
        print("\ndry-run: skipping Snowflake write")
        return 0

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
        for bridge, df in frames:
            load(conn, bridge, df)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
