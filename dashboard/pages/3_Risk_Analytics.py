"""Risk Analytics — rolling beta, volatility, Sharpe, correlation matrix."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.snowflake_conn import compact_layout, heading, fqn, query, CACHE_TTL
from utils.theme import CYAN, GRAY, BG_CARD, dark_chart  # noqa: F401

st.set_page_config(page_title="Risk Analytics", layout="wide")
compact_layout()
heading("Risk Analytics")


@st.cache_data(ttl=CACHE_TTL)
def load_risk():
    return query(f"""
        SELECT symbol, price_date, rolling_beta, rolling_correlation,
               rolling_vol_ann_pct, rolling_sharpe, rolling_alpha_ann_pct,
               window_size
        FROM {fqn('analytics', 'mart_analytics__rolling_risk')}
        ORDER BY symbol, price_date
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_correlations():
    return query(f"""
        SELECT symbol_a, symbol_b, correlation
        FROM {fqn('analytics', 'mart_analytics__correlation_matrix')}
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_volume_profile():
    return query(f"""
        SELECT symbol, bucket_id, bucket_time,
               avg_volume_per_bucket, relative_volume,
               avg_trades_per_bar, return_stddev
        FROM {fqn('analytics', 'mart_analytics__volume_profile')}
        ORDER BY symbol, bucket_id
    """)


risk_df = load_risk()
if risk_df.empty:
    st.warning("No risk data yet. Run `dbt run` after accumulating >= 10 trading days.")
    st.stop()

symbols = sorted(risk_df["symbol"].unique())
default_pick = [s for s in ["AAPL", "NVDA", "MSFT", "SPY"] if s in symbols][:4]
selected = st.multiselect("Symbols", symbols, default=default_pick or symbols[:4],
                          label_visibility="collapsed")

if not selected:
    st.info("Select at least one symbol.")
    st.stop()

filtered = risk_df[risk_df["symbol"].isin(selected)]


def _tight(fig, height=235):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


# --- Row 1: Beta + Vol + Sharpe (3 cols) ---
c1, c2, c3 = st.columns(3)

with c1:
    heading("Rolling 20-D Beta vs SPY", 3)
    fig_beta = px.line(filtered, x="price_date", y="rolling_beta", color="symbol")
    fig_beta.add_hline(y=1.0, line_dash="dash", line_color="gray",
                       annotation_text="mkt (1.0)")
    fig_beta.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_beta), use_container_width=True)

with c2:
    heading("Annualised Volatility (%)", 3)
    fig_vol = px.line(filtered, x="price_date", y="rolling_vol_ann_pct", color="symbol")
    fig_vol.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_vol), use_container_width=True)

with c3:
    heading("Rolling Sharpe", 3)
    fig_sharpe = px.line(filtered, x="price_date", y="rolling_sharpe", color="symbol")
    fig_sharpe.add_hline(y=0, line_dash="dash", line_color="gray")
    fig_sharpe.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_sharpe), use_container_width=True)

# --- Row 2: Beta vs Vol scatter (1/2) + Correlation heatmap (1/2) ---
c4, c5 = st.columns([1, 1])

with c4:
    heading("Beta vs Vol — Latest", 3)
    latest_date = risk_df["price_date"].max()
    latest_risk = risk_df[risk_df["price_date"] == latest_date].copy()
    fig_scatter = px.scatter(
        latest_risk, x="rolling_beta", y="rolling_vol_ann_pct",
        text="symbol", size="rolling_vol_ann_pct",
        color="rolling_sharpe", color_continuous_scale="RdYlGn",
        hover_data={"rolling_sharpe": ":.2f", "rolling_correlation": ":.2f"},
    )
    fig_scatter.update_traces(textposition="top center", textfont_size=8)
    fig_scatter.update_layout(xaxis_title="Î²", yaxis_title="Vol (ann %)")
    st.plotly_chart(_tight(fig_scatter, height=420), use_container_width=True)

with c5:
    corr_df = load_correlations()
    if not corr_df.empty:
        heading("20-D Correlation Matrix", 3)
        mirror = corr_df.rename(columns={"symbol_a": "symbol_b", "symbol_b": "symbol_a"})
        full = pd.concat([corr_df, mirror]).drop_duplicates(subset=["symbol_a", "symbol_b"])
        pivot = full.pivot(index="symbol_a", columns="symbol_b", values="correlation").fillna(1.0)
        order = pivot.mean().sort_values(ascending=False).index
        pivot = pivot.loc[order, order]
        fig_corr = px.imshow(pivot, color_continuous_scale="RdBu_r",
                             zmin=-1, zmax=1, aspect="auto")
        st.plotly_chart(_tight(fig_corr, height=420), use_container_width=True)
    else:
        st.info("Correlation matrix not yet computed.")

# --- Row 3: Volume Profile (full width but compact) ---
vp_df = load_volume_profile()
if not vp_df.empty:
    c6, c7 = st.columns([1, 4])
    with c6:
        heading("Intraday Volume Profile", 3)
        vp_symbol = st.selectbox("Symbol", symbols, index=0, label_visibility="collapsed")
    with c7:
        vp_filtered = vp_df[vp_df["symbol"] == vp_symbol]
        fig_vp = go.Figure()
        fig_vp.add_trace(go.Bar(
            x=vp_filtered["bucket_id"].astype(str),
            y=vp_filtered["avg_volume_per_bucket"],
            marker_color=vp_filtered["relative_volume"].apply(
                lambda x: "#EF553B" if x > 1.5 else ("#636EFA" if x > 0.8 else "#B6B6B6")
            ),
            hovertemplate="Bucket: %{x}<br>Avg Volume: %{y:,.0f}<extra></extra>",
        ))
        fig_vp.update_layout(xaxis_title="time of day (HHMM)", yaxis_title=None)
        st.plotly_chart(_tight(fig_vp, height=240), use_container_width=True)
