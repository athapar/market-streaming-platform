"""Pipeline Health — throughput, batch ingest lag, coverage over time."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.snowflake_conn import compact_layout, heading, fqn, query
from utils.theme import CYAN, GREEN, ORANGE, dark_chart

st.set_page_config(page_title="Pipeline Health", layout="wide")
compact_layout()
heading("Pipeline Health")


@st.cache_data(ttl=300)
def load_health():
    return query(f"""
        SELECT * FROM {fqn('observability', 'mart_ops__pipeline_health')}
        ORDER BY event_date
    """)


@st.cache_data(ttl=200)
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

# --- KPIs (row 1) ---
latest = df.iloc[-1]
mode = latest.get("pipeline_mode", "batch")
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Active Symbols", int(latest["symbols_active"]))
k2.metric("Bars Today",     f"{int(latest['total_bars']):,}")
k3.metric("Coverage",       f"{latest['coverage_pct']:.1f}%")
k4.metric("p50 Batch Lag",  f"{latest['p50_batch_lag_s']:.0f}s")
k5.metric("p99 Batch Lag",  f"{latest['p99_batch_lag_s']:.0f}s")

st.caption(
    f"Ingest tier: **{mode}**. Batch lag = market-minute → Silver-write gap for the "
    "`availableNow` Spark batch — it reflects when the batch was kicked off, not "
    "producer→Kafka latency. The real-time producer path (sub-50 ms p99) is tracked "
    "in the Prometheus/Grafana `kafka_produce_latency_seconds` histogram."
)


def _tight(fig, height=225):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=14, b=14, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


# --- Row 2: latency + coverage + dollar volume (3 cols) ---
c1, c2, c3 = st.columns(3)

with c1:
    heading("Batch Ingest Lag (s)", 3)
    fig_latency = go.Figure()
    for col, name, dash in [
        ("p50_batch_lag_s", "p50", "solid"),
        ("p95_batch_lag_s", "p95", "dash"),
        ("p99_batch_lag_s", "p99", "dot"),
    ]:
        fig_latency.add_trace(go.Scatter(
            x=df["event_date"], y=df[col],
            name=name, mode="lines+markers",
            line=dict(dash=dash),
        ))
    fig_latency.update_layout(yaxis_title="seconds", xaxis_title=None)
    st.plotly_chart(_tight(fig_latency), use_container_width=True)

with c2:
    heading("Session Coverage (%)", 3)
    fig_cov = px.line(df, x="event_date", y="coverage_pct", markers=True)
    fig_cov.update_layout(yaxis_title="%", xaxis_title=None)
    fig_cov.add_hline(y=100, line_dash="dash", line_color=GREEN,
                      annotation_text="full")
    st.plotly_chart(_tight(fig_cov), use_container_width=True)

with c3:
    heading("Dollar Volume (USD)", 3)
    fig_dv = px.area(df, x="event_date", y="total_dollar_volume",
                     color_discrete_sequence=[CYAN])
    fig_dv.update_layout(yaxis_title=None, xaxis_title=None)
    st.plotly_chart(_tight(fig_dv), use_container_width=True)

# --- Row 3: bar count + quality (2 cols) ---
c4, c5 = st.columns(2)

with c4:
    heading("Daily Bar Count", 3)
    fig_bars = px.bar(df, x="event_date", y="total_bars",
                      color_discrete_sequence=[CYAN])
    fig_bars.update_layout(yaxis_title="bars", xaxis_title=None)
    st.plotly_chart(_tight(fig_bars), use_container_width=True)

with c5:
    dq = load_quality_summary()
    if not dq.empty:
        heading("Data Quality Score (daily avg)", 3)
        fig_dq = px.line(dq, x="event_date", y="avg_quality_score", markers=True,
                         color_discrete_sequence=[GREEN])
        fig_dq.update_layout(yaxis_title="score (0-100)", xaxis_title=None)
        fig_dq.add_hline(y=90, line_dash="dash", line_color=ORANGE,
                         annotation_text="target")
        st.plotly_chart(_tight(fig_dq), use_container_width=True)
    else:
        st.info("Data Quality mart not yet populated.")
