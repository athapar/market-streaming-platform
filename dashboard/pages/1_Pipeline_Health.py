"""Pipeline Health — throughput, latency, coverage over time."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from utils.snowflake_conn import fqn, query

st.set_page_config(page_title="Pipeline Health", layout="wide")
st.title("Pipeline Health")


@st.cache_data(ttl=300)
def load_health():
    return query(f"""
        SELECT * FROM {fqn('observability', 'mart_ops__pipeline_health')}
        ORDER BY event_date
    """)


@st.cache_data(ttl=300)
def load_quality_summary():
    return query(f"""
        SELECT
            event_date,
            COUNT(*)                                    AS symbols,
            ROUND(AVG(quality_score), 1)                AS avg_quality_score,
            ROUND(AVG(completeness_pct), 1)             AS avg_completeness,
            ROUND(AVG(validity_pct), 1)                 AS avg_validity,
            SUM(total_invalid_bars)                      AS total_invalid_bars
        FROM {fqn('observability', 'mart_ops__data_quality')}
        GROUP BY event_date
        ORDER BY event_date
    """)


df = load_health()

if df.empty:
    st.warning("No pipeline health data yet. Run the pipeline for at least one market day.")
    st.stop()

# --- KPIs ---
latest = df.iloc[-1]
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Active Symbols", int(latest["symbols_active"]))
col2.metric("Bars Today", f"{int(latest['total_bars']):,}")
col3.metric("Coverage", f"{latest['coverage_pct']:.1f}%")
col4.metric("p50 Latency", f"{latest['p50_latency_s']:.1f}s")
col5.metric("p99 Latency", f"{latest['p99_latency_s']:.1f}s")

st.divider()

# --- Latency over time ---
st.subheader("Processing Latency (seconds)")
fig_latency = go.Figure()
for col, name, dash in [
    ("p50_latency_s", "p50", "solid"),
    ("p95_latency_s", "p95", "dash"),
    ("p99_latency_s", "p99", "dot"),
]:
    fig_latency.add_trace(go.Scatter(
        x=df["event_date"], y=df[col],
        name=name, mode="lines+markers",
        line=dict(dash=dash),
    ))
fig_latency.update_layout(
    yaxis_title="Seconds",
    xaxis_title="Date",
    height=350,
    margin=dict(t=20),
)
st.plotly_chart(fig_latency, use_container_width=True)

# --- Throughput and Coverage ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Daily Bar Count")
    fig_bars = px.bar(df, x="event_date", y="total_bars", color_discrete_sequence=["#636EFA"])
    fig_bars.update_layout(height=300, margin=dict(t=20), xaxis_title="Date", yaxis_title="Bars")
    st.plotly_chart(fig_bars, use_container_width=True)

with col_right:
    st.subheader("Session Coverage (%)")
    fig_cov = px.line(df, x="event_date", y="coverage_pct", markers=True)
    fig_cov.update_layout(height=300, margin=dict(t=20), xaxis_title="Date", yaxis_title="%")
    fig_cov.add_hline(y=100, line_dash="dash", line_color="green", annotation_text="Full Session")
    st.plotly_chart(fig_cov, use_container_width=True)

# --- Dollar Volume ---
st.subheader("Total Dollar Volume Processed")
fig_dv = px.area(df, x="event_date", y="total_dollar_volume")
fig_dv.update_layout(height=300, margin=dict(t=20), yaxis_title="USD", xaxis_title="Date")
st.plotly_chart(fig_dv, use_container_width=True)

# --- Data Quality Trend ---
dq = load_quality_summary()
if not dq.empty:
    st.subheader("Data Quality Score (daily average)")
    fig_dq = px.line(dq, x="event_date", y="avg_quality_score", markers=True,
                     color_discrete_sequence=["#00CC96"])
    fig_dq.update_layout(height=300, margin=dict(t=20), yaxis_title="Score (0-100)", xaxis_title="Date")
    fig_dq.add_hline(y=90, line_dash="dash", line_color="orange", annotation_text="Target")
    st.plotly_chart(fig_dq, use_container_width=True)
