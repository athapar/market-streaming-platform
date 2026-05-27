"""Snowflake connection helper for the Streamlit dashboard.

Credential resolution order (first hit wins):
  1. st.secrets["snowflake"]   — Streamlit Community Cloud, configured in the
                                  app's Settings → Secrets UI
  2. os.environ                 — local dev via .env, CI, etc.

Both surfaces support the same keys: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER,
SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE (optional).
"""
from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def _secret(key: str, *, required: bool = True) -> str | None:
    """Resolve a credential from st.secrets first, then env vars."""
    # st.secrets raises StreamlitSecretNotFoundError outside Streamlit Cloud
    # when no secrets.toml exists; treat that as 'not present' and fall through.
    try:
        section = st.secrets.get("snowflake", {})
        if key in section:
            return str(section[key])
    except Exception:
        pass

    val = os.environ.get(key)
    if val is None and required:
        raise RuntimeError(
            f"missing credential {key!r}: set it as an env var locally or "
            f"under [snowflake] in Streamlit Cloud's Secrets UI"
        )
    return val


@lru_cache(maxsize=1)
def _get_connection_params() -> dict:
    return dict(
        account   = _secret("SNOWFLAKE_ACCOUNT"),
        user      = _secret("SNOWFLAKE_USER"),
        password  = _secret("SNOWFLAKE_PASSWORD"),
        warehouse = _secret("SNOWFLAKE_WAREHOUSE"),
        database  = "MARKET_STREAMING",
        role      = _secret("SNOWFLAKE_ROLE", required=False),
    )


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(**_get_connection_params())


def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame with sensible dtypes.

    Snowflake's NUMBER / DECIMAL types come back as Python Decimal objects
    via cursor.fetchall(), which pandas stores as `object` dtype. That
    breaks numeric pandas operations (`.nsmallest`, `.nlargest`, `.mean`
    over the column, comparisons, plotting colour scales). Auto-coerce
    object columns to numeric when possible — leaves real string columns
    untouched.
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=cols)

        # Coerce Decimal -> float on object columns where every non-null
        # value is numeric. `errors='coerce'` returns NaN for non-numeric
        # entries, so we only swap the column back in if no real strings
        # got nulled (i.e. the converted series is numeric for all rows
        # that were originally non-null).
        for col in df.select_dtypes(include="object").columns:
            converted = pd.to_numeric(df[col], errors="coerce")
            original_non_null = df[col].notna()
            converted_non_null = converted.notna()
            if original_non_null.equals(converted_non_null):
                df[col] = converted

        return df
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Schema resolution
# ---------------------------------------------------------------------------
#
# dbt's default generate_schema_name prepends `target.schema` to any custom
# schema name, so +schema: observability + target.schema: DBT_DEV produces
# the physical Snowflake schema DBT_DEV_OBSERVABILITY. We keep that
# convention (it makes per-environment isolation cheap — DBT_DEV_*, DBT_PROD_*
# etc. coexist in the same Snowflake account) and resolve dashboard-side.
#
# Override at deploy time by setting DBT_TARGET (env var) or [snowflake]
# DBT_TARGET in Streamlit Cloud secrets. Default is DBT_DEV.

DATABASE     = "MARKET_STREAMING"
_DBT_TARGET  = _secret("DBT_TARGET", required=False) or "DBT_DEV"


def fqn(custom_schema: str, table: str) -> str:
    """Fully-qualified Snowflake table name for a dbt-built mart.

        fqn('observability', 'mart_ops__pipeline_health')
          -> MARKET_STREAMING.DBT_DEV_OBSERVABILITY.MART_OPS__PIPELINE_HEALTH

    Use this for any table dbt produces. For raw Snowflake-sync targets
    (GOLD, RECON, OPS — synced from Databricks, not built by dbt) just
    write the literal `MARKET_STREAMING.<SCHEMA>.<TABLE>` since they do
    not carry the DBT_TARGET prefix.
    """
    return f"{DATABASE}.{_DBT_TARGET}_{custom_schema.upper()}.{table.upper()}"
