"""Dividends — TTM yield time series and current-yield ranking."""
import streamlit as st
import plotly.express as px
import pandas as pd

from utils.snowflake_conn import compact_layout, fqn, query
from utils.theme import dark_chart  # noqa: F401

st.set_page_config(page_title="Dividends", layout="wide")
compact_layout()
st.title("Dividends & Yield")
st.caption(
    "TTM dividend yield per ex-dividend event. `live_yield_estimate` on the "
    "latest event per security uses the most recent streaming close."
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
    st.warning("No dividend data. Run `bq_to_snowflake_dividends.py` then `dbt run`.")
    st.stop()

latest = df[df["is_latest_event"]].copy()
latest["ttm_yield_pct"]  = latest["ttm_dividend_yield"]  * 100
latest["live_yield_pct"] = latest["live_yield_estimate"] * 100

paying = latest[latest["ttm_yield_pct"] > 0]
median_yield = paying["ttm_yield_pct"].median()
top_yield = paying["ttm_yield_pct"].max() if not paying.empty else 0

# --- KPIs ---
k1, k2, k3, k4 = st.columns(4)
k1.metric("Payers",             len(paying))
k2.metric("Median TTM Yield",   f"{median_yield:.2f}%" if pd.notna(median_yield) else "—")
k3.metric("Top TTM Yield",      f"{top_yield:.2f}%" if pd.notna(top_yield) else "—")
k4.metric("Ex-Div Events",      f"{len(df):,}")


def _tight(fig, height=270):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


# --- Row 1: top yielders + live/ex-date delta (2 cols) ---
c1, c2 = st.columns(2)

with c1:
    st.subheader("Top Yielders (latest ex-div events)")
    top_y = paying.nlargest(20, "ttm_yield_pct")
    fig_top = px.bar(
        top_y, x="ticker", y="ttm_yield_pct",
        color="ttm_yield_pct", color_continuous_scale="Greens",
        hover_data={"ticker": False, "ttm_yield_pct": ":.2f",
                    "live_yield_pct": ":.2f", "cash_amount": ":.4f"},
    )
    fig_top.update_layout(xaxis_title=None, yaxis_title="TTM Yield %")
    st.plotly_chart(_tight(fig_top), use_container_width=True)

with c2:
    delta_df = paying.dropna(subset=["live_yield_pct"]).copy()
    delta_df["yield_delta_bps"] = (delta_df["live_yield_pct"] - delta_df["ttm_yield_pct"]) * 100
    if not delta_df.empty:
        st.subheader("Live vs Ex-Date Yield Δ (bps)")
        fig_delta = px.bar(
            delta_df.sort_values("yield_delta_bps"),
            x="ticker", y="yield_delta_bps",
            color="yield_delta_bps", color_continuous_scale="RdBu",
            color_continuous_midpoint=0,
            hover_data={"ttm_yield_pct": ":.2f", "live_yield_pct": ":.2f"},
        )
        fig_delta.update_layout(xaxis_title=None, yaxis_title="bps")
        st.plotly_chart(_tight(fig_delta), use_container_width=True)

# --- Row 2: sector yields + per-symbol trend (2 cols) ---
c3, c4 = st.columns(2)

with c3:
    st.subheader("Avg TTM Yield by Sector (top 15)")
    sec_df = latest.dropna(subset=["sic_description"])
    if not sec_df.empty:
        sec_summary = (
            sec_df.groupby("sic_description")
            .agg(
                securities=("ticker", "count"),
                avg_yield=("ttm_yield_pct", "mean"),
                med_yield=("ttm_yield_pct", "median"),
                total_cap=("market_cap", "sum"),
            )
            .reset_index()
            .sort_values("avg_yield", ascending=False)
            .head(15)
        )
        fig_sec = px.bar(
            sec_summary, x="avg_yield", y="sic_description", orientation="h",
            color="avg_yield", color_continuous_scale="Greens",
            hover_data={"securities": True, "med_yield": ":.2f", "total_cap": ":,.0f"},
        )
        fig_sec.update_layout(yaxis={"categoryorder": "total ascending"},
                              xaxis_title="avg yield %", yaxis_title=None)
        st.plotly_chart(_tight(fig_sec, height=340), use_container_width=True)

with c4:
    st.subheader("Yield Trend Over Time")
    symbols = sorted(df["ticker"].unique())
    default_pick = [s for s in ["MSFT", "JNJ", "PG", "T", "VZ", "XOM"] if s in symbols][:4]
    selected = st.multiselect("Symbols", symbols, default=default_pick or symbols[:4],
                              label_visibility="collapsed")
    if selected:
        trend = df[df["ticker"].isin(selected)].copy()
        trend["ttm_yield_pct"] = trend["ttm_dividend_yield"] * 100
        fig_trend = px.line(
            trend, x="ex_dividend_date", y="ttm_yield_pct",
            color="ticker", markers=True,
            hover_data={"cash_amount": ":.4f", "batch_close_price": ":.2f"},
        )
        fig_trend.update_layout(xaxis_title=None, yaxis_title="TTM Yield %")
        st.plotly_chart(_tight(fig_trend, height=320), use_container_width=True)

# --- Expander: full table ---
with st.expander("Latest yield ranking — full table"):
    table = latest[[
        "ticker", "ex_dividend_date", "cash_amount",
        "ttm_dividends_per_share", "batch_close_price",
        "ttm_yield_pct", "live_yield_pct", "sic_description",
    ]].sort_values("ttm_yield_pct", ascending=False).reset_index(drop=True)
    table.columns = ["Ticker", "Ex-Div", "$/Share", "TTM $/Share", "Batch Close",
                     "TTM Yield %", "Live Yield %", "Sector"]
    st.dataframe(table, use_container_width=True, hide_index=True)
