"""Market Overview — returns heatmap, top movers, volume leaders."""
import streamlit as st
import plotly.express as px
import pandas as pd

from utils.snowflake_conn import fqn, query

st.set_page_config(page_title="Market Overview", layout="wide")
st.title("Market Overview")


@st.cache_data(ttl=300)
def load_daily_stats():
    return query(f"""
        SELECT
            symbol, event_date,
            daily_simple_return, close_price,
            total_volume, total_dollar_volume,
            realized_vol_ann_pct, volume_zscore,
            bar_count
        FROM {fqn('analytics', 'mart_analytics__daily_stats')}
        ORDER BY event_date DESC, symbol
    """)


df = load_daily_stats()

if df.empty:
    st.warning("No analytics data yet. Run `dbt run` after ingesting market data.")
    st.stop()

dates = sorted(df["event_date"].unique(), reverse=True)
selected_date = st.selectbox("Trading Date", dates, index=0)

day_df = df[df["event_date"] == selected_date].copy()
day_df["return_pct"] = day_df["daily_simple_return"] * 100

# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)
col1.metric("Symbols Tracked", len(day_df))
col2.metric("Avg Return", f"{day_df['return_pct'].mean():.2f}%")
col3.metric("Advancers / Decliners",
            f"{(day_df['return_pct'] > 0).sum()} / {(day_df['return_pct'] < 0).sum()}")
col4.metric("Avg Realized Vol",
            f"{day_df['realized_vol_ann_pct'].mean():.1f}%")

st.divider()

# --- Returns Heatmap (treemap) ---
st.subheader("Daily Returns Treemap")
tree_df = day_df[day_df["total_dollar_volume"].notna()].copy()
tree_df["abs_dv"] = tree_df["total_dollar_volume"].abs().clip(lower=1)

fig_tree = px.treemap(
    tree_df,
    path=["symbol"],
    values="abs_dv",
    color="return_pct",
    color_continuous_scale="RdYlGn",
    color_continuous_midpoint=0,
    hover_data={"return_pct": ":.2f", "close_price": ":.2f", "total_volume": ":,.0f"},
)
fig_tree.update_layout(height=500, margin=dict(t=30, l=0, r=0, b=0))
st.plotly_chart(fig_tree, use_container_width=True)

# --- Top Movers ---
col_up, col_down = st.columns(2)

with col_up:
    st.subheader("Top Gainers")
    top_up = day_df.nlargest(10, "return_pct")[
        ["symbol", "return_pct", "close_price", "total_volume"]
    ].reset_index(drop=True)
    top_up.columns = ["Symbol", "Return %", "Close", "Volume"]
    st.dataframe(top_up, use_container_width=True, hide_index=True)

with col_down:
    st.subheader("Top Losers")
    top_dn = day_df.nsmallest(10, "return_pct")[
        ["symbol", "return_pct", "close_price", "total_volume"]
    ].reset_index(drop=True)
    top_dn.columns = ["Symbol", "Return %", "Close", "Volume"]
    st.dataframe(top_dn, use_container_width=True, hide_index=True)

# --- Volume Leaders ---
st.subheader("Dollar Volume Leaders")
vol_df = day_df.nlargest(20, "total_dollar_volume")
fig_vol = px.bar(
    vol_df, x="symbol", y="total_dollar_volume",
    color="return_pct", color_continuous_scale="RdYlGn",
    color_continuous_midpoint=0,
)
fig_vol.update_layout(
    height=350, margin=dict(t=20),
    xaxis_title="Symbol", yaxis_title="Dollar Volume (USD)",
)
st.plotly_chart(fig_vol, use_container_width=True)

# --- Unusual Activity ---
st.subheader("Unusual Activity Flags")
unusual = day_df[day_df["volume_zscore"].abs() > 2.0][
    ["symbol", "return_pct", "volume_zscore", "realized_vol_ann_pct", "total_volume"]
].sort_values("volume_zscore", ascending=False).reset_index(drop=True)
unusual.columns = ["Symbol", "Return %", "Volume Z-Score", "Realized Vol %", "Volume"]

if unusual.empty:
    st.info("No unusual activity detected for this date.")
else:
    st.dataframe(unusual, use_container_width=True, hide_index=True)
