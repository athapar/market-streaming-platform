"""Market Overview — returns heatmap, top movers, volume leaders."""
import streamlit as st
import plotly.express as px

from utils.snowflake_conn import compact_layout, heading, fqn, query, CACHE_TTL
from utils.theme import dark_chart  # noqa: F401 — registers dark template on import

st.set_page_config(page_title="Market Overview", layout="wide")
compact_layout()
heading("Market Overview")


@st.cache_data(ttl=CACHE_TTL)
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
date_col, _ = st.columns([1, 5])
with date_col:
    selected_date = st.selectbox("Trading Date", dates, index=0, label_visibility="collapsed")

day_df = df[df["event_date"] == selected_date].copy()
day_df["return_pct"] = day_df["daily_simple_return"] * 100

# --- KPIs (4 metric tiles) ---
k1, k2, k3, k4 = st.columns(4)
k1.metric("Symbols",          len(day_df))
k2.metric("Avg Return",       f"{day_df['return_pct'].mean():.2f}%")
k3.metric("Up / Down",        f"{(day_df['return_pct'] > 0).sum()} / {(day_df['return_pct'] < 0).sum()}")
k4.metric("Avg Realized Vol", f"{day_df['realized_vol_ann_pct'].mean():.1f}%")


def _tight(fig, height=270):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
    )
    return fig


# --- Row: treemap (2/3) + volume leaders (1/3) ---
c_tree, c_vol = st.columns([2, 1])

with c_tree:
    heading("Daily Returns Treemap (sized by $-volume)", 3)
    tree_df = day_df[day_df["total_dollar_volume"].notna()].copy()
    tree_df["abs_dv"] = tree_df["total_dollar_volume"].abs().clip(lower=1)
    fig_tree = px.treemap(
        tree_df, path=["symbol"], values="abs_dv",
        color="return_pct", color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        hover_data={"return_pct": ":.2f", "close_price": ":.2f", "total_volume": ":,.0f"},
    )
    st.plotly_chart(_tight(fig_tree, height=420), use_container_width=True)

with c_vol:
    heading("Top 15 by $-Volume", 3)
    vol_df = day_df.nlargest(15, "total_dollar_volume")
    fig_vol = px.bar(
        vol_df, y="symbol", x="total_dollar_volume", orientation="h",
        color="return_pct", color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
    )
    fig_vol.update_layout(yaxis=dict(autorange="reversed"),
                          xaxis_title=None, yaxis_title=None)
    st.plotly_chart(_tight(fig_vol, height=420), use_container_width=True)

# --- Row: gainers + losers + unusual (3 tables) ---
g, l, u = st.columns(3)

with g:
    heading("Top Gainers", 3)
    tg = day_df.nlargest(10, "return_pct")[
        ["symbol", "return_pct", "close_price"]].reset_index(drop=True)
    tg.columns = ["Sym", "Return %", "Close"]
    st.dataframe(tg, use_container_width=True, hide_index=True, height=370)

with l:
    heading("Top Losers", 3)
    tl = day_df.nsmallest(10, "return_pct")[
        ["symbol", "return_pct", "close_price"]].reset_index(drop=True)
    tl.columns = ["Sym", "Return %", "Close"]
    st.dataframe(tl, use_container_width=True, hide_index=True, height=370)

with u:
    heading("Unusual Activity (|Z| > 2)", 3)
    unusual = day_df[day_df["volume_zscore"].abs() > 2.0][
        ["symbol", "return_pct", "volume_zscore"]
    ].sort_values("volume_zscore", ascending=False).reset_index(drop=True)
    unusual.columns = ["Sym", "Return %", "Vol Z"]
    if unusual.empty:
        st.info("No unusual activity for this date.")
    else:
        st.dataframe(unusual, use_container_width=True, hide_index=True, height=370)
