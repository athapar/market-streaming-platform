"""Data Quality — completeness, validity, anomaly detection."""
import streamlit as st
import plotly.express as px

from utils.snowflake_conn import compact_layout, heading, fqn, query, CACHE_TTL
from utils.theme import CYAN, GREEN, dark_chart  # noqa: F401

st.set_page_config(page_title="Data Quality", layout="wide")
compact_layout()
heading("Data Quality")


@st.cache_data(ttl=CACHE_TTL)
def load_quality():
    return query(f"""
        SELECT * FROM {fqn('observability', 'mart_ops__data_quality')}
        ORDER BY event_date DESC, symbol
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_recon():
    return query(f"""
        SELECT
            price_date, recon_status,
            COUNT(*) as cnt
        FROM {fqn('marts', 'mart_recon__daily_delta')}
        WHERE price_date >= TO_DATE('05/26/2026', 'MM/DD/YYYY')
        GROUP BY price_date, recon_status
        ORDER BY price_date
    """)


@st.cache_data(ttl=CACHE_TTL)
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
date_col, _ = st.columns([1, 5])
with date_col:
    selected_date = st.selectbox("Date", dates, index=0, label_visibility="collapsed")
day_dq = dq[dq["event_date"] == selected_date]

# --- KPIs ---
k1, k2, k3, k4 = st.columns(4)
k1.metric("Avg Quality",       f"{day_dq['quality_score'].mean():.1f}")
k2.metric("Avg Completeness",  f"{day_dq['completeness_pct'].mean():.1f}%")
k3.metric("Avg Validity",      f"{day_dq['validity_pct'].mean():.1f}%")
k4.metric("Total Invalid Bars", int(day_dq["total_invalid_bars"].sum()))


def _tight(fig, height=200):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


# --- Row 1: histogram + recon stack + completeness (3 cols) ---
c1, c2, c3 = st.columns([1, 1, 1])

with c1:
    heading("Quality Score Distribution", 3)
    fig_hist = px.histogram(day_dq, x="quality_score", nbins=20,
                            color_discrete_sequence=["#636EFA"])
    fig_hist.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_hist), use_container_width=True)

with c2:
    recon = load_recon()
    heading("Recon Status Stack", 3)
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
        fig_recon.update_layout(xaxis_title=None, yaxis_title=None, barmode="stack")
        st.plotly_chart(_tight(fig_recon), use_container_width=True)
    else:
        st.info("Recon mart empty.")

with c3:
    heading("Lowest Quality (top 10)", 3)
    worst = day_dq.nsmallest(10, "quality_score")[
        ["symbol", "quality_score", "completeness_pct", "validity_pct"]
    ].reset_index(drop=True)
    worst.columns = ["Sym", "Qual", "Compl %", "Val %"]
    st.dataframe(worst, use_container_width=True, hide_index=True, height=280)

# --- Row 2: completeness bars (full width since long list) ---
heading("Completeness by Symbol", 3)
sorted_dq = day_dq.sort_values("completeness_pct")
fig_comp = px.bar(
    sorted_dq, x="completeness_pct", y="symbol", orientation="h",
    color="completeness_pct", color_continuous_scale="RdYlGn",
)
fig_comp.update_layout(
    height=max(280, len(sorted_dq) * 11),
    margin=dict(t=24, b=24, l=60, r=8),
    font=dict(size=9),
    xaxis_title="%", yaxis_title=None,
)
st.plotly_chart(fig_comp, use_container_width=True)

# --- Row 3: unusual activity table ---
unusual = load_unusual()
if not unusual.empty:
    heading("Recent Unusual Activity (top 100)", 3)
    u = unusual.copy()
    u["daily_simple_return"] = (u["daily_simple_return"] * 100).round(2)
    u.columns = ["Sym", "Date", "Class", "Return %",
                 "Vol Z", "Volatility Z", "Volume", "Realized Vol %"]
    st.dataframe(u, use_container_width=True, hide_index=True, height=300)
else:
    st.info("No unusual activity detected.")
