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


# ---------------------------------------------------------------------------
# Compact-layout CSS — call at the top of every page after st.set_page_config
# ---------------------------------------------------------------------------

_COMPACT_CSS = """
<style>
    /* ── Dark Bloomberg-style terminal theme ── */

    /* Main background */
    .stApp, [data-testid="stAppViewContainer"] {
        background-color: #0a0e17 !important;
    }
    .block-container {
        /* top padding must clear Streamlit's fixed header (~3rem tall).
           Earlier we used 0.5rem which hid the page title behind the toolbar. */
        padding: 3rem 1rem 1rem 1rem !important;
        max-width: 100% !important;
        background-color: #0a0e17 !important;
    }

    /* Also tighten the header itself so the gap above content doesn't grow */
    [data-testid="stHeader"] {
        background-color: rgba(10, 14, 23, 0.6) !important;
        backdrop-filter: blur(8px);
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #0d1117 !important;
        border-right: 1px solid #1a2332 !important;
    }
    [data-testid="stSidebar"] * { color: #8b949e !important; }
    [data-testid="stSidebar"] a:hover { color: #58a6ff !important; }
    [data-testid="stSidebarNavLink"][aria-selected="true"] {
        background-color: #161b22 !important;
        border-left: 3px solid #1f6feb !important;
    }

    /*
     * config.toml sets textColor=#e6edf3 (bright white) — Streamlit applies
     * this as the default to ALL text including headings. We only override
     * DOWNWARD: dim body text to #c9d1d9, dim captions/labels further.
     * Headings keep the config.toml default without any CSS interference.
     *
     * IMPORTANT: do NOT set color on generic selectors like p, span, div
     * because Streamlit wraps heading text inside <p> tags.
     */

    /* Dim body text in non-heading contexts only */
    [data-testid="stWidgetLabel"] * { color: #8b949e !important; }
    [data-testid="stCaptionContainer"] * { color: #8b949e !important; }

    /* Tighter header spacing */
    h1 { padding: 0 !important; margin: 0 0 0.4rem 0 !important; font-size: 1.6rem !important; }
    h2 { padding: 0 !important; margin: 0.6rem 0 0.2rem 0 !important; font-size: 1.15rem !important; }
    h3 { padding: 0 !important; margin: 0.5rem 0 0.2rem 0 !important; font-size: 1.0rem  !important; }

    /* Custom heading classes — used by the heading() helper.
       Selector ratchets up specificity (html body + descendant chain)
       to beat any Streamlit emotion class that might set color.
       The `*` descendant rule covers cases where the text is wrapped
       in nested spans by markdown processing. */
    html body div.ms-heading-1,
    html body div.ms-heading-1 *,
    [data-testid="stMarkdownContainer"] div.ms-heading-1,
    [data-testid="stMarkdownContainer"] div.ms-heading-1 * {
        color: #e6edf3 !important;
        font-size: 1.6rem !important;
        font-weight: 600 !important;
        margin: 0 0 0.4rem 0 !important;
        line-height: 1.2 !important;
    }
    html body div.ms-heading-2,
    html body div.ms-heading-2 *,
    [data-testid="stMarkdownContainer"] div.ms-heading-2,
    [data-testid="stMarkdownContainer"] div.ms-heading-2 * {
        color: #e6edf3 !important;
        font-size: 1.25rem !important;
        font-weight: 600 !important;
        margin: 0.6rem 0 0.2rem 0 !important;
        line-height: 1.2 !important;
    }
    html body div.ms-heading-3,
    html body div.ms-heading-3 *,
    [data-testid="stMarkdownContainer"] div.ms-heading-3,
    [data-testid="stMarkdownContainer"] div.ms-heading-3 * {
        color: #e6edf3 !important;
        font-size: 1.0rem !important;
        font-weight: 600 !important;
        margin: 0.5rem 0 0.2rem 0 !important;
        line-height: 1.2 !important;
    }

    /* Metric tiles — dark card style */
    [data-testid="stMetric"] {
        background-color: #161b22 !important;
        border: 1px solid #1a2332 !important;
        border-radius: 6px !important;
        padding: 0.4rem 0.6rem !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.15rem !important;
        line-height: 1.2 !important;
        color: #58a6ff !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.78rem !important;
        color: #8b949e !important;
    }

    /* Dividers */
    hr { margin: 0.35rem 0 !important; border-color: #1a2332 !important; }

    /* Plotly charts: kill default vertical margin */
    div[data-testid="stPlotlyChart"] { margin: 0 !important; padding: 0 !important; }

    /* DataFrames — dark tables */
    .stDataFrame { margin-top: 0.2rem !important; }
    .stDataFrame table { background-color: #0d1117 !important; color: #c9d1d9 !important; }
    .stDataFrame th { background-color: #161b22 !important; color: #8b949e !important; }

    /* Column gutter */
    div[data-testid="column"] > div { gap: 0.3rem !important; padding-top: 0 !important; }

    /* Caption */
    [data-testid="stCaptionContainer"] { margin-top: -0.2rem !important; margin-bottom: 0.3rem !important; }

    /* Select boxes / inputs */
    [data-testid="stSelectbox"] label { color: #8b949e !important; }
    .stSelectbox > div > div { background-color: #161b22 !important; border-color: #1a2332 !important; }
    .stMultiSelect > div > div { background-color: #161b22 !important; border-color: #1a2332 !important; }

    /* Info/warning boxes */
    .stAlert { background-color: #161b22 !important; border-color: #1a2332 !important; }
</style>
"""


def heading(text: str, level: int = 1) -> None:
    """Render a heading using a CSS class defined in compact_layout()'s
    style block. We do NOT use inline `style` attributes because Streamlit's
    HTML sanitizer in `unsafe_allow_html=True` strips them out — that's
    why earlier attempts with inline color were invisible. Class attributes
    are preserved by the sanitizer.
    """
    lvl = level if level in (1, 2, 3) else 3
    st.markdown(
        f'<div class="ms-heading-{lvl}">{text}</div>',
        unsafe_allow_html=True,
    )


def compact_layout() -> None:
    """Inject CSS that compacts headers, metrics, charts, and gutters.

    Call once at the top of each page, immediately after st.set_page_config.
    Idempotent — Streamlit will render the <style> block once per session.
    """
    st.markdown(_COMPACT_CSS, unsafe_allow_html=True)
