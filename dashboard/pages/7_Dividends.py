"""Dividends — TTM yield time series and current-yield ranking."""
import streamlit as st
import plotly.express as px
import pandas as pd

from utils.snowflake_conn import fqn, query

st.set_page_config(page_title="Dividends", layout="wide")
st.title("Dividends & Yield")
st.caption(
    "Trailing twelve-month dividend yield per ex-dividend event. "
    "On the latest event per security, `live_yield_estimate` recomputes the "
    "yield using the most recent streaming close — a near-real-time "
    "alternative to the ex-date yield."
)


@st.cache_data(ttl=300)
def load_dividends():
    return query(f"""
        SELECT
            composite_figi, ticker, ex_dividend_date,
            cash_amount, ttm_dividends_per_share,
            batch_close_price, ttm_dividend_yield,
            live_yield_estimate, is_latest_event,
            sic_code, sic_description, market_cap
        FROM {fqn('fundamentals', 'mart_fundamentals__dividend_yield')}
        ORDER BY ticker, ex_dividend_date
    """)


df = load_dividends()

if df.empty:
    st.warning(
        "No dividend data yet. Run "
        "`python scripts/bq_to_snowflake_dividends.py` then `dbt run`."
    )
    st.stop()

latest = df[df["is_latest_event"]].copy()
# Convert to percent for display
latest["ttm_yield_pct"]    = latest["ttm_dividend_yield"]   * 100
latest["live_yield_pct"]   = latest["live_yield_estimate"]  * 100

# --- KPIs ---
total_securities = latest["ticker"].nunique()
paying = latest[latest["ttm_yield_pct"] > 0]
median_yield = paying["ttm_yield_pct"].median()
top_yield = paying["ttm_yield_pct"].max() if not paying.empty else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Dividend Payers", len(paying))
col2.metric("Median TTM Yield", f"{median_yield:.2f}%" if pd.notna(median_yield) else "—")
col3.metric("Top TTM Yield", f"{top_yield:.2f}%" if pd.notna(top_yield) else "—")
col4.metric("Total Ex-Div Events", f"{len(df):,}")

st.divider()

# --- Top yielding ---
st.subheader("Top Yielders (Latest Ex-Div Events)")
top_n = st.slider("How many to show", min_value=10, max_value=40, value=20, step=5)
top_y = paying.nlargest(top_n, "ttm_yield_pct").copy()

fig_top = px.bar(
    top_y, x="ticker", y="ttm_yield_pct",
    color="ttm_yield_pct", color_continuous_scale="Greens",
    hover_data={
        "ticker": False,
        "ttm_yield_pct": ":.2f",
        "live_yield_pct": ":.2f",
        "cash_amount": ":.4f",
        "ttm_dividends_per_share": ":.4f",
        "batch_close_price": ":.2f",
    },
)
fig_top.update_layout(
    height=400, margin=dict(t=20),
    xaxis_title="", yaxis_title="TTM Dividend Yield (%)",
)
st.plotly_chart(fig_top, use_container_width=True)

# --- TTM (batch) vs live yield delta ---
delta_df = paying.dropna(subset=["live_yield_pct"]).copy()
delta_df["yield_delta_bps"] = (delta_df["live_yield_pct"] - delta_df["ttm_yield_pct"]) * 100  # already pct → bps
if not delta_df.empty:
    st.subheader("Live vs Ex-Date Yield Delta (basis points)")
    st.caption(
        "Positive = current streaming close is lower than the ex-date close, "
        "raising the yield. Negative = price has risen since ex-date."
    )
    fig_delta = px.bar(
        delta_df.sort_values("yield_delta_bps"),
        x="ticker", y="yield_delta_bps",
        color="yield_delta_bps", color_continuous_scale="RdBu",
        color_continuous_midpoint=0,
        hover_data={"ttm_yield_pct": ":.2f", "live_yield_pct": ":.2f"},
    )
    fig_delta.update_layout(height=350, margin=dict(t=20),
                            xaxis_title="", yaxis_title="Yield Δ (bps)")
    st.plotly_chart(fig_delta, use_container_width=True)

st.divider()

# --- Sector view ---
sec_df = latest.dropna(subset=["sic_description"])
if not sec_df.empty:
    sec_summary = (
        sec_df.groupby("sic_description")
        .agg(
            securities  = ("ticker", "count"),
            avg_yield   = ("ttm_yield_pct", "mean"),
            med_yield   = ("ttm_yield_pct", "median"),
            total_cap   = ("market_cap", "sum"),
        )
        .reset_index()
        .sort_values("avg_yield", ascending=False)
        .head(15)
    )

    st.subheader("Yield by Sector (Top 15)")
    fig_sec = px.bar(
        sec_summary, x="sic_description", y="avg_yield",
        color="avg_yield", color_continuous_scale="Greens",
        hover_data={"securities": True, "med_yield": ":.2f",
                    "total_cap": ":,.0f"},
        labels={"sic_description": "Sector (SIC)", "avg_yield": "Avg TTM Yield (%)"},
    )
    fig_sec.update_layout(height=400, margin=dict(t=20, b=140),
                          xaxis_tickangle=-30)
    st.plotly_chart(fig_sec, use_container_width=True)

st.divider()

# --- Per-security yield trend ---
st.subheader("Yield Trend Over Time")
symbols = sorted(df["ticker"].unique())
default_pick = [s for s in ["MSFT", "JNJ", "PG", "T", "VZ", "XOM"] if s in symbols][:4]
selected = st.multiselect("Symbols", symbols, default=default_pick or symbols[:4])

if selected:
    trend = df[df["ticker"].isin(selected)].copy()
    trend["ttm_yield_pct"] = trend["ttm_dividend_yield"] * 100
    fig_trend = px.line(
        trend, x="ex_dividend_date", y="ttm_yield_pct",
        color="ticker", markers=True,
        hover_data={"cash_amount": ":.4f", "batch_close_price": ":.2f"},
    )
    fig_trend.update_layout(
        height=400, margin=dict(t=20),
        xaxis_title="Ex-Dividend Date", yaxis_title="TTM Yield (%)",
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with st.expander("Latest yield ranking — full table"):
    table = latest[[
        "ticker", "ex_dividend_date", "cash_amount",
        "ttm_dividends_per_share", "batch_close_price",
        "ttm_yield_pct", "live_yield_pct", "sic_description",
    ]].sort_values("ttm_yield_pct", ascending=False).reset_index(drop=True)
    table.columns = [
        "Ticker", "Ex-Div Date", "Cash / Share",
        "TTM Cash / Share", "Batch Close",
        "TTM Yield %", "Live Yield %", "Sector",
    ]
    st.dataframe(table, use_container_width=True, hide_index=True)
