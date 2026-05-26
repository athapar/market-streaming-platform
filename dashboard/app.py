"""
Market Streaming Pipeline — Analytics Dashboard

Multi-page Streamlit app for pipeline observability, market analytics,
and data quality monitoring. Reads from Snowflake dbt marts.

Run: streamlit run dashboard/app.py
"""
import streamlit as st

st.set_page_config(
    page_title="Market Streaming Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("Market Streaming Pipeline")
st.markdown(
    """
    Real-time market data analytics platform ingesting **104 symbols** via
    Polygon.io WebSocket, streaming through Kafka and Spark Structured Streaming
    into a Delta Lake medallion architecture, with Snowflake serving layer
    and dbt-powered analytics.

    ---

    **Navigate** using the sidebar to explore:
    - **Pipeline Health** — throughput, latency percentiles, session coverage
    - **Market Overview** — daily returns heatmap, top movers, volume leaders
    - **Risk Analytics** — rolling beta, volatility, Sharpe, correlation matrix
    - **Data Quality** — completeness, validity scores, anomaly detection
    """
)

st.sidebar.success("Select a page above.")
