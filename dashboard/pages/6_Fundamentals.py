"""Fundamentals — live-priced valuation ratios + factor scoring."""
import streamlit as st
import plotly.express as px

from utils.snowflake_conn import compact_layout, heading, fqn, query
from utils.theme import dark_chart  # noqa: F401

st.set_page_config(page_title="Fundamentals", layout="wide")
compact_layout()
heading("Fundamentals")
st.caption(
    "Batch TTM fundamentals × live streaming close. Price-derived ratios "
    "(P/E, P/B, P/S, market cap) rescaled by `live_close / batch_close`; "
    "margins and balance-sheet ratios pass through."
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
    st.warning("No fundamentals data. Run `bq_to_snowflake_fundamentals.py` then `dbt run`.")
    st.stop()

# --- KPIs ---
total = len(val)
ok = (val["pricing_status"] == "OK").sum()
stale = (val["pricing_status"] == "STALE_STREAMING").sum()
missing = (val["pricing_status"] == "MISSING_STREAMING").sum()
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Securities",     total)
k2.metric("OK Pricing",     ok)
k3.metric("Stale",          stale)
k4.metric("Missing",        missing)
k5.metric("Median P/E live", f"{val['live_pe_ratio'].dropna().median():.1f}")


def _tight(fig, height=290):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=10)),
    )
    return fig


fs = load_factor_scores()
movers = val.dropna(subset=["close_delta_pct"]).copy()

# --- Row 1: factor breakdown + movers + P/E vs ROE scatter (3 cols) ---
c1, c2, c3 = st.columns([0.8, 1.2, 1.4])

with c1:
    heading("Factor Classification", 3)
    if not fs.empty:
        class_counts = (
            fs["factor_classification"].value_counts()
            .reset_index()
            .rename(columns={"index": "Classification", "factor_classification": "Classification",
                              "count": "Count"})
        )
        if "Count" not in class_counts.columns:
            class_counts.columns = ["Classification", "Count"]
        fig_class = px.bar(
            class_counts, x="Count", y="Classification",
            orientation="h",
            color="Classification",
            color_discrete_map={"VALUE": "#58a6ff", "GROWTH": "#f85149",
                                "QUALITY": "#3fb950", "BLEND": "#8b949e"},
        )
        fig_class.update_layout(
            showlegend=False, xaxis_title=None, yaxis_title=None,
        )
        st.plotly_chart(_tight(fig_class), use_container_width=True)

with c2:
    heading("Top Movers Since Batch Snapshot", 3)
    if not movers.empty:
        movers["abs_delta"] = movers["close_delta_pct"].abs()
        top = movers.nlargest(20, "abs_delta").sort_values("close_delta_pct", ascending=False)
        fig_movers = px.bar(
            top, x="ticker", y="close_delta_pct",
            color="close_delta_pct", color_continuous_scale="RdYlGn",
            color_continuous_midpoint=0,
            hover_data={"batch_close": ":.2f", "live_close": ":.2f"},
        )
        fig_movers.update_layout(xaxis_title=None, yaxis_title="Δ %")
        st.plotly_chart(_tight(fig_movers), use_container_width=True)
    else:
        st.info("No live pricing.")

with c3:
    heading("Valuation Quadrant — P/E vs ROE", 3)
    plot_df = val.dropna(subset=["live_pe_ratio", "roe", "live_market_cap"]).copy()
    plot_df = plot_df[(plot_df["live_pe_ratio"] > 0) & (plot_df["live_pe_ratio"] < 200)]
    if not plot_df.empty:
        fig_quad = px.scatter(
            plot_df, x="live_pe_ratio", y="roe",
            size="live_market_cap", text="ticker",
            color="operating_margin", color_continuous_scale="Viridis",
            hover_data={"ticker": False, "live_pe_ratio": ":.1f", "roe": ":.2%",
                        "live_market_cap": ":,.0f", "operating_margin": ":.2%"},
            labels={"live_pe_ratio": "P/E", "roe": "ROE"},
        )
        fig_quad.update_traces(textposition="top center", textfont_size=7)
        st.plotly_chart(_tight(fig_quad), use_container_width=True)

# --- Row 2: value vs growth scatter (1/2) + sector mix (1/2) ---
c4, c5 = st.columns(2)

with c4:
    heading("Value vs Growth (size = quality)", 3)
    if not fs.empty:
        fig_vg = px.scatter(
            fs, x="value_score", y="growth_score",
            size="quality_score", text="ticker",
            color="factor_classification",
            color_discrete_map={"VALUE": "#636EFA", "GROWTH": "#EF553B",
                                "QUALITY": "#00CC96", "BLEND": "#B6B6B6"},
            hover_data={"ticker": False, "value_score": ":.2f",
                        "growth_score": ":.2f", "quality_score": ":.2f"},
        )
        fig_vg.update_traces(textposition="top center", textfont_size=7)
        fig_vg.add_hline(y=0.5, line_dash="dot", line_color="gray", opacity=0.4)
        fig_vg.add_vline(x=0.5, line_dash="dot", line_color="gray", opacity=0.4)
        st.plotly_chart(_tight(fig_vg, height=340), use_container_width=True)

with c5:
    heading("Sector Mix by Market Cap", 3)
    if not fs.empty:
        sec_df = fs.dropna(subset=["sic_description", "market_cap"])
        if not sec_df.empty:
            sec_agg = (
                sec_df.groupby("sic_description")
                .agg(market_cap=("market_cap", "sum"),
                     securities=("ticker", "count"),
                     avg_quality=("quality_score", "mean"))
                .reset_index()
                .sort_values("market_cap", ascending=False)
                .head(15)
            )
            fig_sec = px.bar(
                sec_agg, x="market_cap", y="sic_description",
                orientation="h", color="avg_quality",
                color_continuous_scale="Viridis",
                hover_data={"securities": True, "market_cap": ":,.0f", "avg_quality": ":.2f"},
            )
            fig_sec.update_layout(yaxis={"categoryorder": "total ascending"},
                                  xaxis_title="$ market cap", yaxis_title=None)
            st.plotly_chart(_tight(fig_sec, height=340), use_container_width=True)

# --- Expander: full table ---
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
