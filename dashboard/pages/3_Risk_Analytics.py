"""Risk Analytics — rolling beta, volatility, Sharpe, correlation matrix."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import numpy as np

from utils.snowflake_conn import query

st.set_page_config(page_title="Risk Analytics", layout="wide")
st.title("Risk Analytics")


@st.cache_data(ttl=300)
def load_risk():
    return query("""
        SELECT symbol, price_date, rolling_beta, rolling_correlation,
               rolling_vol_ann_pct, rolling_sharpe, rolling_alpha_ann_pct,
               window_size
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__ROLLING_RISK
        ORDER BY symbol, price_date
    """)


@st.cache_data(ttl=300)
def load_correlations():
    return query("""
        SELECT symbol_a, symbol_b, correlation
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__CORRELATION_MATRIX
    """)


@st.cache_data(ttl=300)
def load_volume_profile():
    return query("""
        SELECT symbol, bucket_id, bucket_time,
               avg_volume_per_bucket, relative_volume,
               avg_trades_per_bar, return_stddev
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__VOLUME_PROFILE
        ORDER BY symbol, bucket_id
    """)


risk_df = load_risk()

if risk_df.empty:
    st.warning("No risk data yet. Run `dbt run` after accumulating >= 10 trading days.")
    st.stop()

symbols = sorted(risk_df["symbol"].unique())
selected = st.multiselect("Symbols", symbols, default=["AAPL", "NVDA", "MSFT", "SPY"][:len(symbols)])

if not selected:
    st.info("Select at least one symbol.")
    st.stop()

filtered = risk_df[risk_df["symbol"].isin(selected)]

# --- Rolling Beta ---
st.subheader("Rolling 20-Day Beta vs SPY")
fig_beta = px.line(filtered, x="price_date", y="rolling_beta", color="symbol")
fig_beta.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="Market (1.0)")
fig_beta.update_layout(height=350, margin=dict(t=20), xaxis_title="Date", yaxis_title="Beta")
st.plotly_chart(fig_beta, use_container_width=True)

# --- Rolling Volatility and Sharpe ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Rolling Annualized Volatility (%)")
    fig_vol = px.line(filtered, x="price_date", y="rolling_vol_ann_pct", color="symbol")
    fig_vol.update_layout(height=300, margin=dict(t=20), xaxis_title="Date", yaxis_title="Vol %")
    st.plotly_chart(fig_vol, use_container_width=True)

with col2:
    st.subheader("Rolling Sharpe Ratio")
    fig_sharpe = px.line(filtered, x="price_date", y="rolling_sharpe", color="symbol")
    fig_sharpe.add_hline(y=0, line_dash="dash", line_color="gray")
    fig_sharpe.update_layout(height=300, margin=dict(t=20), xaxis_title="Date", yaxis_title="Sharpe")
    st.plotly_chart(fig_sharpe, use_container_width=True)

# --- Beta vs Volatility Scatter (latest) ---
st.subheader("Beta vs Volatility (Latest)")
latest_date = risk_df["price_date"].max()
latest_risk = risk_df[risk_df["price_date"] == latest_date].copy()

fig_scatter = px.scatter(
    latest_risk, x="rolling_beta", y="rolling_vol_ann_pct",
    text="symbol", size="rolling_vol_ann_pct",
    color="rolling_sharpe", color_continuous_scale="RdYlGn",
    hover_data={"rolling_sharpe": ":.2f", "rolling_correlation": ":.2f"},
)
fig_scatter.update_traces(textposition="top center", textfont_size=9)
fig_scatter.update_layout(
    height=450, margin=dict(t=20),
    xaxis_title="Rolling Beta", yaxis_title="Rolling Vol (ann. %)",
)
st.plotly_chart(fig_scatter, use_container_width=True)

# --- Correlation Heatmap ---
st.subheader("Trailing 20-Day Correlation Matrix")
corr_df = load_correlations()

if not corr_df.empty:
    # Build symmetric matrix
    mirror = corr_df.rename(columns={"symbol_a": "symbol_b", "symbol_b": "symbol_a"})
    full = pd.concat([corr_df, mirror]).drop_duplicates(subset=["symbol_a", "symbol_b"])
    pivot = full.pivot(index="symbol_a", columns="symbol_b", values="correlation").fillna(1.0)

    # sort by mean correlation for visual clustering
    order = pivot.mean().sort_values(ascending=False).index
    pivot = pivot.loc[order, order]

    fig_corr = px.imshow(
        pivot, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
        aspect="auto",
    )
    fig_corr.update_layout(height=600, margin=dict(t=20))
    st.plotly_chart(fig_corr, use_container_width=True)
else:
    st.info("Correlation matrix not yet computed.")

# --- Volume Profile ---
st.subheader("Intraday Volume Profile")
vp_df = load_volume_profile()

if not vp_df.empty:
    vp_symbol = st.selectbox("Symbol for Volume Profile", symbols, index=0)
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
    fig_vp.update_layout(
        height=350, margin=dict(t=20),
        xaxis_title="Time of Day (HHMM)",
        yaxis_title="Average Volume per 30-min Bucket",
    )
    st.plotly_chart(fig_vp, use_container_width=True)
