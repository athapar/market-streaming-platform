"""Fundamentals — live-priced valuation ratios + factor scoring."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.snowflake_conn import fqn, query

st.set_page_config(page_title="Fundamentals", layout="wide")
st.title("Fundamentals")
st.caption(
    "Batch-computed TTM fundamentals × live streaming close. "
    "P/E, P/B, P/S, market cap rescaled by `price_scale = live_close / batch_close`. "
    "Margins and balance-sheet ratios pass through."
)


@st.cache_data(ttl=300)
def load_valuation():
    return query(f"""
        SELECT
            composite_figi, ticker,
            live_close, live_close_date, batch_close, batch_close_date,
            price_scale, close_delta_pct, pricing_status,
            live_pe_ratio, live_pb_ratio, live_ps_ratio, live_price_to_fcf, live_market_cap,
            batch_pe_ratio, batch_pb_ratio, batch_ps_ratio, batch_ev_ebit,
            gross_margin, operating_margin, net_margin,
            roe, roa, current_ratio, debt_to_equity,
            ttm_revenue, ttm_net_income, ttm_free_cash_flow,
            book_value, total_assets, total_liabilities, shares_outstanding,
            financials_as_of, filing_date
        FROM {fqn('fundamentals', 'mart_fundamentals__valuation_live')}
        ORDER BY ticker
    """)


@st.cache_data(ttl=300)
def load_factor_scores():
    return query(f"""
        SELECT
            composite_figi, ticker, factor_classification,
            value_score, growth_score, quality_score,
            pe_ratio, pb_ratio, operating_margin, roe, debt_to_equity, fcf_conversion,
            sic_code, sic_description, market_cap
        FROM {fqn('fundamentals', 'mart_fundamentals__factor_scores')}
        ORDER BY ticker
    """)


val = load_valuation()

if val.empty:
    st.warning(
        "No fundamentals data yet. Run "
        "`python scripts/bq_to_snowflake_fundamentals.py` then `dbt run` "
        "for the warehouse project."
    )
    st.stop()

# --- KPIs ---
total = len(val)
ok = (val["pricing_status"] == "OK").sum()
stale = (val["pricing_status"] == "STALE_STREAMING").sum()
missing = (val["pricing_status"] == "MISSING_STREAMING").sum()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Securities", total)
col2.metric("OK Pricing", ok)
col3.metric("Stale", stale)
col4.metric("Missing", missing)
col5.metric(
    "Median P/E (live)",
    f"{val['live_pe_ratio'].dropna().median():.1f}",
)

st.divider()

# --- Live vs Batch close movement (top movers) ---
st.subheader("Movement Since Batch Snapshot")
mover_cols = ["ticker", "batch_close", "live_close", "close_delta_pct", "pricing_status"]
movers = val.dropna(subset=["close_delta_pct"]).copy()
movers["abs_delta"] = movers["close_delta_pct"].abs()

if not movers.empty:
    top_movers = movers.nlargest(20, "abs_delta")[mover_cols].sort_values(
        "close_delta_pct", ascending=False
    ).reset_index(drop=True)
    top_movers.columns = ["Ticker", "Batch Close", "Live Close", "Δ Close %", "Status"]

    fig_movers = px.bar(
        top_movers, x="Ticker", y="Δ Close %",
        color="Δ Close %", color_continuous_scale="RdYlGn", color_continuous_midpoint=0,
        hover_data={"Batch Close": ":.2f", "Live Close": ":.2f"},
    )
    fig_movers.update_layout(height=380, margin=dict(t=20), xaxis_title="", yaxis_title="Δ %")
    st.plotly_chart(fig_movers, use_container_width=True)
else:
    st.info("No live pricing available — streaming pipeline hasn't run yet today.")

st.divider()

# --- Valuation scatter: P/E vs ROE ---
st.subheader("Valuation Quadrant — P/E vs ROE")
plot_df = val.dropna(subset=["live_pe_ratio", "roe", "live_market_cap"]).copy()
plot_df = plot_df[(plot_df["live_pe_ratio"] > 0) & (plot_df["live_pe_ratio"] < 200)]

if not plot_df.empty:
    fig_quad = px.scatter(
        plot_df,
        x="live_pe_ratio", y="roe",
        size="live_market_cap", text="ticker",
        color="operating_margin", color_continuous_scale="Viridis",
        hover_data={
            "ticker": False,
            "live_pe_ratio": ":.1f", "roe": ":.2%",
            "live_market_cap": ":,.0f", "operating_margin": ":.2%",
        },
        labels={"live_pe_ratio": "P/E (live)", "roe": "ROE", "operating_margin": "Op Margin"},
    )
    fig_quad.update_traces(textposition="top center", textfont_size=8)
    fig_quad.update_layout(height=500, margin=dict(t=20))
    st.plotly_chart(fig_quad, use_container_width=True)

st.divider()

# --- Factor Scores ---
fs = load_factor_scores()

if not fs.empty:
    st.subheader("Factor Classification — Value / Growth / Quality")

    col_dist, col_scatter = st.columns([1, 2])

    with col_dist:
        st.markdown("**Classification distribution**")
        class_counts = fs["factor_classification"].value_counts().reset_index()
        class_counts.columns = ["Classification", "Count"]
        fig_class = px.pie(
            class_counts, names="Classification", values="Count",
            color="Classification",
            color_discrete_map={
                "VALUE":   "#636EFA",
                "GROWTH":  "#EF553B",
                "QUALITY": "#00CC96",
                "BLEND":   "#B6B6B6",
            },
            hole=0.4,
        )
        fig_class.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_class, use_container_width=True)

    with col_scatter:
        st.markdown("**Value vs Growth (size = quality)**")
        fig_vg = px.scatter(
            fs, x="value_score", y="growth_score",
            size="quality_score", text="ticker",
            color="factor_classification",
            color_discrete_map={
                "VALUE":   "#636EFA",
                "GROWTH":  "#EF553B",
                "QUALITY": "#00CC96",
                "BLEND":   "#B6B6B6",
            },
            hover_data={"ticker": False, "value_score": ":.2f",
                        "growth_score": ":.2f", "quality_score": ":.2f"},
        )
        fig_vg.update_traces(textposition="top center", textfont_size=8)
        fig_vg.add_hline(y=0.5, line_dash="dot", line_color="gray", opacity=0.4)
        fig_vg.add_vline(x=0.5, line_dash="dot", line_color="gray", opacity=0.4)
        fig_vg.update_layout(height=400, margin=dict(t=20))
        st.plotly_chart(fig_vg, use_container_width=True)

    # --- Sector breakdown ---
    sec_df = fs.dropna(subset=["sic_description", "market_cap"])
    if not sec_df.empty:
        st.markdown("**Sector mix by market cap**")
        sec_agg = (
            sec_df.groupby("sic_description")
            .agg(market_cap=("market_cap", "sum"),
                 securities=("ticker", "count"),
                 avg_quality=("quality_score", "mean"))
            .reset_index()
            .sort_values("market_cap", ascending=False)
            .head(20)
        )
        fig_sec = px.bar(
            sec_agg, x="market_cap", y="sic_description",
            orientation="h", color="avg_quality",
            color_continuous_scale="Viridis",
            hover_data={"securities": True, "market_cap": ":,.0f", "avg_quality": ":.2f"},
            labels={"sic_description": "", "market_cap": "Aggregate Market Cap (USD)"},
        )
        fig_sec.update_layout(height=500, margin=dict(t=20, l=200),
                              yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig_sec, use_container_width=True)

st.divider()

# --- Full valuation table ---
with st.expander("Full valuation table"):
    table = val[[
        "ticker", "live_close", "close_delta_pct", "pricing_status",
        "live_pe_ratio", "live_pb_ratio", "live_ps_ratio",
        "operating_margin", "net_margin", "roe", "roa",
        "current_ratio", "debt_to_equity",
        "live_market_cap", "ttm_revenue", "ttm_net_income",
        "financials_as_of",
    ]].copy()
    for col in ("operating_margin", "net_margin", "roe", "roa"):
        table[col] = (table[col] * 100).round(2)
    st.dataframe(table, use_container_width=True, hide_index=True)
