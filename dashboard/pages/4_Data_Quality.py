"""Data Quality — completeness, validity, anomaly detection."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.snowflake_conn import fqn, query

st.set_page_config(page_title="Data Quality", layout="wide")
st.title("Data Quality")


@st.cache_data(ttl=300)
def load_quality():
    return query(f"""
        SELECT * FROM {fqn('observability', 'mart_ops__data_quality')}
        ORDER BY event_date DESC, symbol
    """)


@st.cache_data(ttl=300)
def load_recon():
    return query(f"""
        SELECT
            price_date, recon_status,
            COUNT(*) as cnt
        FROM {fqn('marts', 'mart_recon__daily_delta')}
        GROUP BY price_date, recon_status
        ORDER BY price_date
    """)


@st.cache_data(ttl=300)
def load_unusual():
    return query(f"""
        SELECT symbol, event_date, activity_classification,
               daily_simple_return, volume_zscore, vol_zscore,
               total_volume, realized_vol_ann_pct
        FROM {fqn('analytics', 'mart_analytics__unusual_activity')}
        WHERE is_unusual = TRUE
        ORDER BY event_date DESC, volume_zscore DESC
        LIMIT 100
    """)


dq = load_quality()

if dq.empty:
    st.warning("No data quality data yet.")
    st.stop()

dates = sorted(dq["event_date"].unique(), reverse=True)
selected_date = st.selectbox("Date", dates, index=0)

day_dq = dq[dq["event_date"] == selected_date]

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Avg Quality Score", f"{day_dq['quality_score'].mean():.1f}")
col2.metric("Avg Completeness", f"{day_dq['completeness_pct'].mean():.1f}%")
col3.metric("Avg Validity", f"{day_dq['validity_pct'].mean():.1f}%")
col4.metric("Total Invalid Bars", int(day_dq["total_invalid_bars"].sum()))

st.divider()

# --- Quality Score Distribution ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Quality Score Distribution")
    fig_hist = px.histogram(day_dq, x="quality_score", nbins=20, color_discrete_sequence=["#636EFA"])
    fig_hist.update_layout(height=300, margin=dict(t=20), xaxis_title="Quality Score", yaxis_title="Symbols")
    st.plotly_chart(fig_hist, use_container_width=True)

with col_right:
    st.subheader("Completeness by Symbol")
    sorted_dq = day_dq.sort_values("completeness_pct")
    fig_comp = px.bar(
        sorted_dq, x="completeness_pct", y="symbol",
        orientation="h",
        color="completeness_pct",
        color_continuous_scale="RdYlGn",
    )
    fig_comp.update_layout(height=max(300, len(sorted_dq) * 12), margin=dict(t=20, l=60))
    st.plotly_chart(fig_comp, use_container_width=True)

# --- Worst Quality Symbols ---
st.subheader("Lowest Quality Scores")
worst = day_dq.nsmallest(10, "quality_score")[
    ["symbol", "quality_score", "completeness_pct", "validity_pct",
     "bar_count", "total_invalid_bars", "avg_latency_s"]
].reset_index(drop=True)
worst.columns = ["Symbol", "Quality", "Completeness %", "Validity %",
                  "Bars", "Invalid Bars", "Avg Latency (s)"]
st.dataframe(worst, use_container_width=True, hide_index=True)

# --- Recon Status Breakdown ---
st.divider()
st.subheader("Reconciliation Status (Streaming vs Batch)")
recon = load_recon()

if not recon.empty:
    fig_recon = px.bar(
        recon, x="price_date", y="cnt", color="recon_status",
        color_discrete_map={
            "OK": "#00CC96", "PARTIAL_SESSION": "#FFA15A",
            "MISSING_STREAMING": "#EF553B", "MISSING_BATCH": "#AB63FA",
            "CLOSE_MISMATCH": "#FF6692", "VWAP_MISMATCH": "#B6E880",
            "CLOSE_AND_VWAP_MISMATCH": "#FF97FF",
        },
    )
    fig_recon.update_layout(
        height=350, margin=dict(t=20),
        xaxis_title="Date", yaxis_title="Symbol-Days",
        barmode="stack",
    )
    st.plotly_chart(fig_recon, use_container_width=True)
else:
    st.info("No recon data available yet.")

# --- Unusual Activity Log ---
st.divider()
st.subheader("Recent Unusual Activity Events")
unusual = load_unusual()

if not unusual.empty:
    unusual_display = unusual.copy()
    unusual_display["daily_simple_return"] = (unusual_display["daily_simple_return"] * 100).round(2)
    unusual_display.columns = [
        "Symbol", "Date", "Classification", "Return %",
        "Vol Z-Score", "Volatility Z-Score", "Volume", "Realized Vol %"
    ]
    st.dataframe(unusual_display, use_container_width=True, hide_index=True)
else:
    st.info("No unusual activity detected.")
