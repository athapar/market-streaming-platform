"""Microstructure Analytics — spread, trade flow, order imbalance."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.snowflake_conn import compact_layout, heading, fqn, query, CACHE_TTL
from utils.theme import CYAN, RED, dark_chart  # noqa: F401

st.set_page_config(page_title="Microstructure", layout="wide")
compact_layout()
heading("Microstructure Analytics")


@st.cache_data(ttl=CACHE_TTL)
def load_micro_daily():
    return query(f"""
        SELECT symbol, trade_date,
               trade_count, total_dollar_volume, avg_trade_size, median_trade_size,
               buy_volume_pct, trade_imbalance,
               block_trade_count, block_dollar_volume,
               total_quotes, avg_spread_bps, min_spread_bps, max_spread_bps,
               avg_quote_imbalance, quote_to_trade_ratio
        FROM {fqn('analytics', 'mart_analytics__microstructure_daily')}
        ORDER BY trade_date DESC, symbol
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_spread_profile():
    return query(f"""
        SELECT symbol, bucket_id,
               avg_spread_bps, avg_min_spread_bps,
               avg_trades_per_bucket, avg_dollar_volume_per_bucket,
               avg_imbalance, relative_spread, avg_buy_pct
        FROM {fqn('analytics', 'mart_analytics__spread_profile')}
        ORDER BY symbol, bucket_id
    """)


@st.cache_data(ttl=CACHE_TTL)
def load_trade_sizes():
    return query(f"""
        SELECT symbol, trade_date, size_class,
               trade_count, total_dollar_volume, pct_of_trades, pct_of_dollar_volume
        FROM {fqn('analytics', 'mart_analytics__trade_size_distribution')}
        ORDER BY trade_date DESC, symbol, size_class
    """)


df = load_micro_daily()
if df.empty:
    st.warning("No microstructure data yet. Run the trades/quotes pipeline and `dbt run`.")
    st.stop()

dates = sorted(df["trade_date"].unique(), reverse=True)
date_col, _ = st.columns([1, 5])
with date_col:
    selected_date = st.selectbox("Trading Date", dates, index=0, label_visibility="collapsed")
day_df = df[df["trade_date"] == selected_date]

# --- KPIs ---
spread_df = day_df[day_df["avg_spread_bps"].notna()]
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Active Symbols", len(day_df),
          help="Symbols with trading activity this day (of a 104-name universe; "
               "one name is inactive).")
k2.metric("Total Trades",  f"{day_df['trade_count'].sum():,.0f}")
k3.metric("$-Volume",      f"${day_df['total_dollar_volume'].sum() / 1e9:,.2f}B")
k4.metric("Avg Spread",    f"{spread_df['avg_spread_bps'].mean():.2f} bps" if not spread_df.empty else "N/A")
k5.metric("Quoted Symbols", len(spread_df),
          help="High-liquidity names subscribed to NBBO quote data (20 of the "
               "104-symbol universe). Spread metrics are computed only for these.")


def _tight(fig, height=250):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


# --- Row 1: spread, imbalance, block (3 cols) ---
c1, c2, c3 = st.columns(3)

with c1:
    heading("Bid-Ask Spread by Symbol", 3)
    if not spread_df.empty:
        ss = spread_df.sort_values("avg_spread_bps")
        fig_sp = px.bar(ss, x="symbol", y="avg_spread_bps",
                        color="avg_spread_bps", color_continuous_scale="YlOrRd")
        fig_sp.update_layout(xaxis_title=None, yaxis_title="bps")
        st.plotly_chart(_tight(fig_sp), use_container_width=True)
    else:
        st.info("No quote data for this date.")

with c2:
    heading("Trade Imbalance (tick rule)", 3)
    imb_df = day_df.sort_values("trade_imbalance")
    fig_imb = px.bar(imb_df, x="symbol", y="trade_imbalance",
                     color="trade_imbalance", color_continuous_scale="RdYlGn",
                     color_continuous_midpoint=0)
    fig_imb.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_imb), use_container_width=True)

with c3:
    heading("Block Trades ($)", 3)
    block_df = day_df[day_df["block_trade_count"] > 0].sort_values(
        "block_dollar_volume", ascending=False).head(20)
    if not block_df.empty:
        fig_block = px.bar(block_df, x="symbol", y="block_dollar_volume",
                           color="block_trade_count", color_continuous_scale="Blues")
        fig_block.update_layout(xaxis_title=None, yaxis_title=None)
        st.plotly_chart(_tight(fig_block), use_container_width=True)
    else:
        st.info("No block trades.")

# --- Row 2: spread U-curve (1/2) + trade size sunburst (1/2) ---
c4, c5 = st.columns(2)

with c4:
    sp_df = load_spread_profile()
    heading("Intraday Spread U-Curve", 3)
    if not sp_df.empty:
        sp_symbols = sorted(sp_df["symbol"].unique())
        sel_col, _ = st.columns([1, 3])
        with sel_col:
            sp_selected = st.selectbox("Symbol", sp_symbols, index=0,
                                       key="ucurve_sym", label_visibility="collapsed")
        sp_filtered = sp_df[sp_df["symbol"] == sp_selected]
        fig_ucurve = go.Figure()
        fig_ucurve.add_trace(go.Bar(
            x=sp_filtered["bucket_id"].astype(str),
            y=sp_filtered["avg_spread_bps"],
            marker_color=sp_filtered["relative_spread"].apply(
                lambda x: "#EF553B" if x > 1.3 else ("#636EFA" if x > 0.8 else "#B6B6B6")
            ),
            hovertemplate="Bucket: %{x}<br>Spread: %{y:.2f} bps<extra></extra>",
        ))
        fig_ucurve.update_layout(xaxis_title="time of day (HHMM)", yaxis_title="bps")
        st.plotly_chart(_tight(fig_ucurve, height=300), use_container_width=True)
    else:
        st.info("No spread profile data.")

with c5:
    ts_df = load_trade_sizes()
    heading("Trade Size Distribution ($-volume)", 3)
    if not ts_df.empty:
        ts_day = ts_df[ts_df["trade_date"] == selected_date]
        if not ts_day.empty:
            fig_size = px.sunburst(
                ts_day, path=["size_class", "symbol"], values="total_dollar_volume",
                color="size_class",
                color_discrete_map={
                    "ODD_LOT": "#FFA15A", "ROUND_LOT": "#636EFA", "BLOCK": "#EF553B"
                },
            )
            st.plotly_chart(_tight(fig_size, height=340), use_container_width=True)
    else:
        st.info("No trade size data.")
