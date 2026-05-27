"""Reconciliation — streaming vs batch agreement on prices and returns."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.snowflake_conn import query

st.set_page_config(page_title="Reconciliation", layout="wide")
st.title("Streaming vs Batch Reconciliation")
st.caption(
    "Two independent reconciliations: daily OHLCV (price-level) and daily "
    "return (close-to-close). Both join on `(composite_figi, price_date)`. "
    "`PARTIAL_SESSION` and `RETURN_MISMATCH` rows usually reflect a streaming "
    "session that didn't cover the full trading day."
)


_STATUS_COLORS = {
    "OK":                        "#00CC96",
    "PARTIAL_SESSION":           "#FFA15A",
    "CLOSE_MISMATCH":            "#FF6692",
    "VWAP_MISMATCH":             "#B6E880",
    "CLOSE_AND_VWAP_MISMATCH":   "#FF97FF",
    "RETURN_MISMATCH":           "#FF6692",
    "MISSING_STREAMING":         "#EF553B",
    "MISSING_BATCH":             "#AB63FA",
}


@st.cache_data(ttl=300)
def load_price_recon():
    return query("""
        SELECT
            symbol, price_date, session_coverage, recon_status,
            s_close, b_close, close_delta_pct,
            s_vwap, b_vwap, vwap_pct_delta,
            s_volume, b_volume, volume_pct_delta,
            bar_count, first_bar_start, last_bar_start
        FROM MARKET_STREAMING.MARTS.MART_RECON__DAILY_DELTA
        ORDER BY price_date DESC, symbol
    """)


@st.cache_data(ttl=300)
def load_returns_recon():
    return query("""
        SELECT
            symbol, price_date, recon_status,
            streaming_close, streaming_return,
            batch_close, batch_return,
            return_delta, return_delta_bps,
            batch_volatility_20d, batch_volatility_60d
        FROM MARKET_STREAMING.MARTS.MART_RECON__RETURNS_DELTA
        ORDER BY price_date DESC, symbol
    """)


pdf = load_price_recon()
rdf = load_returns_recon()

if pdf.empty and rdf.empty:
    st.warning("No reconciliation data yet. Run the bridge scripts and `dbt run`.")
    st.stop()

# --- KPIs (latest date) ---
all_dates = sorted(set(pdf["price_date"].tolist() + rdf["price_date"].tolist()),
                   reverse=True)
selected_date = st.selectbox("Trading Date", all_dates, index=0)

day_p = pdf[pdf["price_date"] == selected_date]
day_r = rdf[rdf["price_date"] == selected_date]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Symbol-Days (Price)", len(day_p))
col2.metric(
    "Price OK",
    f"{(day_p['recon_status'] == 'OK').sum()} / {len(day_p)}" if len(day_p) else "—",
)
col3.metric("Symbol-Days (Returns)", len(day_r))
col4.metric(
    "Returns OK",
    f"{(day_r['recon_status'] == 'OK').sum()} / {len(day_r)}" if len(day_r) else "—",
)
if len(day_r):
    rd_full = day_r.dropna(subset=["return_delta_bps"])
    if not rd_full.empty:
        col5.metric("Median |Δ return|", f"{rd_full['return_delta_bps'].abs().median():.2f} bps")
    else:
        col5.metric("Median |Δ return|", "—")
else:
    col5.metric("Median |Δ return|", "—")

st.divider()

# --- Status stacked bar over time ---
st.subheader("Recon Status by Date")

col_left, col_right = st.columns(2)

with col_left:
    st.markdown("**Daily price recon**")
    if not pdf.empty:
        p_agg = (pdf.groupby(["price_date", "recon_status"])
                    .size().reset_index(name="cnt"))
        fig_p = px.bar(
            p_agg, x="price_date", y="cnt", color="recon_status",
            color_discrete_map=_STATUS_COLORS,
        )
        fig_p.update_layout(height=350, margin=dict(t=20),
                            xaxis_title="", yaxis_title="Symbol-Days",
                            barmode="stack", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_p, use_container_width=True)

with col_right:
    st.markdown("**Daily return recon**")
    if not rdf.empty:
        r_agg = (rdf.groupby(["price_date", "recon_status"])
                    .size().reset_index(name="cnt"))
        fig_r = px.bar(
            r_agg, x="price_date", y="cnt", color="recon_status",
            color_discrete_map=_STATUS_COLORS,
        )
        fig_r.update_layout(height=350, margin=dict(t=20),
                            xaxis_title="", yaxis_title="Symbol-Days",
                            barmode="stack", legend=dict(orientation="h", y=-0.2))
        st.plotly_chart(fig_r, use_container_width=True)

st.divider()

# --- Close-price delta distribution ---
st.subheader("Close-Price Δ Distribution (Latest Date)")
if not day_p.empty:
    full = day_p.dropna(subset=["close_delta_pct"])
    if not full.empty:
        fig_hist = px.histogram(
            full, x="close_delta_pct", nbins=40,
            color="session_coverage",
            color_discrete_map={
                "full_session":      "#636EFA",
                "partial_session":   "#FFA15A",
                "missing_streaming": "#EF553B",
                "missing_batch":     "#AB63FA",
            },
        )
        fig_hist.update_layout(
            height=350, margin=dict(t=20),
            xaxis_title="Δ Close (%)",
            yaxis_title="Symbols",
            barmode="overlay",
        )
        fig_hist.update_traces(opacity=0.7)
        fig_hist.add_vline(x=0.5, line_dash="dot", line_color="#FF6692",
                            annotation_text="+0.5% mismatch")
        fig_hist.add_vline(x=-0.5, line_dash="dot", line_color="#FF6692",
                            annotation_text="-0.5%")
        st.plotly_chart(fig_hist, use_container_width=True)

# --- Return delta scatter ---
if not day_r.empty:
    st.subheader("Streaming vs Batch Daily Return — Latest Date")
    both = day_r.dropna(subset=["streaming_return", "batch_return"])
    if not both.empty:
        fig_scatter = px.scatter(
            both, x="batch_return", y="streaming_return",
            color="recon_status", color_discrete_map=_STATUS_COLORS,
            hover_data={"symbol": True, "return_delta_bps": ":.2f"},
            text="symbol",
        )
        # 45-degree reference line
        bounds = [
            min(both["batch_return"].min(), both["streaming_return"].min()),
            max(both["batch_return"].max(), both["streaming_return"].max()),
        ]
        fig_scatter.add_trace(go.Scatter(
            x=bounds, y=bounds, mode="lines",
            line=dict(dash="dash", color="gray"), showlegend=False,
            hoverinfo="skip",
        ))
        fig_scatter.update_traces(textposition="top center", textfont_size=8)
        fig_scatter.update_layout(
            height=500, margin=dict(t=20),
            xaxis_title="Batch Daily Return", yaxis_title="Streaming Daily Return",
            xaxis_tickformat=".2%", yaxis_tickformat=".2%",
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

st.divider()

# --- Mismatch tables ---
st.subheader("Mismatches Requiring Attention")
mismatch_p = pdf[~pdf["recon_status"].isin(["OK", "PARTIAL_SESSION"])].copy()
mismatch_r = rdf[~rdf["recon_status"].isin(["OK"])].copy()

col_pl, col_pr = st.columns(2)
with col_pl:
    st.markdown("**Price mismatches** (excluding PARTIAL_SESSION)")
    if mismatch_p.empty:
        st.success("No price mismatches.")
    else:
        show_p = mismatch_p[[
            "price_date", "symbol", "recon_status",
            "s_close", "b_close", "close_delta_pct",
            "vwap_pct_delta", "volume_pct_delta",
        ]].head(50)
        st.dataframe(show_p, use_container_width=True, hide_index=True)

with col_pr:
    st.markdown("**Return mismatches**")
    if mismatch_r.empty:
        st.success("No return mismatches.")
    else:
        show_r = mismatch_r[[
            "price_date", "symbol", "recon_status",
            "streaming_return", "batch_return", "return_delta_bps",
        ]].head(50)
        st.dataframe(show_r, use_container_width=True, hide_index=True)
