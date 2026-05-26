"""Microstructure Analytics — spread, trade flow, order imbalance."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.snowflake_conn import query

st.set_page_config(page_title="Microstructure", layout="wide")
st.title("Microstructure Analytics")


@st.cache_data(ttl=300)
def load_micro_daily():
    return query("""
        SELECT symbol, trade_date,
               trade_count, total_dollar_volume, avg_trade_size, median_trade_size,
               buy_volume_pct, trade_imbalance,
               block_trade_count, block_dollar_volume,
               total_quotes, avg_spread_bps, min_spread_bps, max_spread_bps,
               avg_quote_imbalance, quote_to_trade_ratio
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__MICROSTRUCTURE_DAILY
        ORDER BY trade_date DESC, symbol
    """)


@st.cache_data(ttl=300)
def load_spread_profile():
    return query("""
        SELECT symbol, bucket_id,
               avg_spread_bps, avg_min_spread_bps,
               avg_trades_per_bucket, avg_dollar_volume_per_bucket,
               avg_imbalance, relative_spread, avg_buy_pct
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__SPREAD_PROFILE
        ORDER BY symbol, bucket_id
    """)


@st.cache_data(ttl=300)
def load_trade_sizes():
    return query("""
        SELECT symbol, trade_date, size_class,
               trade_count, total_dollar_volume, pct_of_trades, pct_of_dollar_volume
        FROM MARKET_STREAMING.ANALYTICS.MART_ANALYTICS__TRADE_SIZE_DISTRIBUTION
        ORDER BY trade_date DESC, symbol, size_class
    """)


df = load_micro_daily()

if df.empty:
    st.warning("No microstructure data yet. Run the trades/quotes pipeline and `dbt run`.")
    st.stop()

dates = sorted(df["trade_date"].unique(), reverse=True)
selected_date = st.selectbox("Trading Date", dates, index=0)
day_df = df[df["trade_date"] == selected_date]

# --- KPIs ---
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Symbols", len(day_df))
col2.metric("Total Trades", f"{day_df['trade_count'].sum():,.0f}")
col3.metric("Dollar Volume", f"${day_df['total_dollar_volume'].sum():,.0f}")

spread_df = day_df[day_df["avg_spread_bps"].notna()]
if not spread_df.empty:
    col4.metric("Avg Spread", f"{spread_df['avg_spread_bps'].mean():.1f} bps")
    col5.metric("Symbols w/ Quotes", len(spread_df))
else:
    col4.metric("Avg Spread", "N/A")
    col5.metric("Symbols w/ Quotes", "0")

st.divider()

# --- Spread by Symbol ---
if not spread_df.empty:
    st.subheader("Bid-Ask Spread by Symbol")
    spread_sorted = spread_df.sort_values("avg_spread_bps")
    fig_spread = px.bar(
        spread_sorted, x="symbol", y="avg_spread_bps",
        color="avg_spread_bps", color_continuous_scale="YlOrRd",
        hover_data={"min_spread_bps": ":.2f", "max_spread_bps": ":.2f",
                     "total_quotes": ":,.0f"},
    )
    fig_spread.update_layout(height=350, margin=dict(t=20),
                              xaxis_title="Symbol", yaxis_title="Avg Spread (bps)")
    st.plotly_chart(fig_spread, use_container_width=True)

# --- Trade Imbalance ---
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Buy/Sell Imbalance (Tick Rule)")
    imb_df = day_df.sort_values("trade_imbalance")
    fig_imb = px.bar(
        imb_df, x="symbol", y="trade_imbalance",
        color="trade_imbalance", color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
    )
    fig_imb.update_layout(height=350, margin=dict(t=20),
                           xaxis_title="Symbol", yaxis_title="Imbalance")
    st.plotly_chart(fig_imb, use_container_width=True)

with col_right:
    st.subheader("Block Trades")
    block_df = day_df[day_df["block_trade_count"] > 0].sort_values(
        "block_dollar_volume", ascending=False
    ).head(20)
    if not block_df.empty:
        fig_block = px.bar(
            block_df, x="symbol", y="block_dollar_volume",
            color="block_trade_count", color_continuous_scale="Blues",
        )
        fig_block.update_layout(height=350, margin=dict(t=20),
                                 xaxis_title="Symbol", yaxis_title="Block $ Volume")
        st.plotly_chart(fig_block, use_container_width=True)
    else:
        st.info("No block trades detected.")

# --- Intraday Spread Profile ---
st.divider()
st.subheader("Intraday Spread Profile (U-Curve)")
sp_df = load_spread_profile()

if not sp_df.empty:
    sp_symbols = sorted(sp_df["symbol"].unique())
    sp_selected = st.selectbox("Symbol", sp_symbols, index=0)
    sp_filtered = sp_df[sp_df["symbol"] == sp_selected]

    fig_ucurve = go.Figure()
    fig_ucurve.add_trace(go.Bar(
        x=sp_filtered["bucket_id"].astype(str),
        y=sp_filtered["avg_spread_bps"],
        name="Avg Spread (bps)",
        marker_color=sp_filtered["relative_spread"].apply(
            lambda x: "#EF553B" if x > 1.3 else ("#636EFA" if x > 0.8 else "#B6B6B6")
        ),
    ))
    fig_ucurve.update_layout(
        height=350, margin=dict(t=20),
        xaxis_title="Time of Day (HHMM)",
        yaxis_title="Average Spread (bps)",
    )
    st.plotly_chart(fig_ucurve, use_container_width=True)
else:
    st.info("No spread profile data yet.")

# --- Trade Size Distribution ---
st.divider()
st.subheader("Trade Size Distribution")
ts_df = load_trade_sizes()

if not ts_df.empty:
    ts_day = ts_df[ts_df["trade_date"] == selected_date]
    if not ts_day.empty:
        fig_size = px.sunburst(
            ts_day, path=["size_class", "symbol"],
            values="total_dollar_volume",
            color="size_class",
            color_discrete_map={
                "ODD_LOT": "#FFA15A", "ROUND_LOT": "#636EFA", "BLOCK": "#EF553B"
            },
        )
        fig_size.update_layout(height=450, margin=dict(t=20))
        st.plotly_chart(fig_size, use_container_width=True)
else:
    st.info("No trade size data yet.")
