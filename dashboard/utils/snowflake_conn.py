"""Snowflake connection helper for the Streamlit dashboard."""
from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


@lru_cache(maxsize=1)
def _get_connection_params() -> dict:
    return dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database="MARKET_STREAMING",
        role=os.getenv("SNOWFLAKE_ROLE"),
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
