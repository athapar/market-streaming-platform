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
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or {})
        cols = [desc[0].lower() for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()
