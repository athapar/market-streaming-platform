"""
Market Streaming Pipeline — Analytics Dashboard

Multi-page Streamlit app for pipeline observability, market analytics,
and data quality monitoring. Reads from Snowflake dbt marts.

Run: streamlit run dashboard/app.py
"""
import streamlit as st

from utils.snowflake_conn import compact_layout
from utils.theme import dark_chart  # noqa: F401 — registers dark plotly template

st.set_page_config(
    page_title="Market Streaming Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)
compact_layout()

st.title("Market Streaming Pipeline")
st.markdown(
    """
    Real-time market data analytics platform ingesting **104 symbols** via
    Polygon.io WebSocket, streaming through Kafka and Spark Structured Streaming
    into a Delta Lake medallion architecture, with Snowflake serving layer
    and dbt-powered analytics. Fundamentals, dividends, and daily returns are
    bridged from a companion BigQuery batch pipeline.

    ---

    **Navigate** using the sidebar to explore:

    *Streaming pipeline*
    - **Pipeline Health** — throughput, latency percentiles, session coverage
    - **Market Overview** — daily returns heatmap, top movers, volume leaders
    - **Risk Analytics** — rolling beta, volatility, Sharpe, correlation matrix
    - **Data Quality** — completeness, validity scores, anomaly detection
    - **Microstructure** — spread, trade flow, order imbalance, U-curve

    *Cross-pipeline (streaming × batch)*
    - **Fundamentals** — TTM valuation ratios re-priced with live streaming close
    - **Dividends** — TTM yield + sector breakdown + live-yield estimate
    - **Reconciliation** — streaming vs batch agreement on prices and returns
    """
)

st.sidebar.success("Select a page above.")
