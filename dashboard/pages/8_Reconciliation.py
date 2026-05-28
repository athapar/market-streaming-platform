"""Reconciliation — streaming vs batch agreement on prices and returns."""
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.snowflake_conn import compact_layout, fqn, query
from utils.theme import dark_chart  # noqa: F401

st.set_page_config(page_title="Reconciliation", layout="wide")
compact_layout()
st.title("Streaming vs Batch Reconciliation")
st.caption(
    "Two independent reconciliations joined on `(composite_figi, price_date)`. "
    "`PARTIAL_SESSION` / `RETURN_MISMATCH` typically reflect a sub-full-day "
    "streaming session."
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
    return query(f"""
        SELECT
            symbol, price_date, session_coverage, recon_status,
            s_close, b_close, close_pct_delta,
            s_vwap, b_vwap, vwap_pct_delta,
            s_volume, b_volume, volume_pct_delta,
            bar_count, first_bar_start, last_bar_start
        FROM {fqn('marts', 'mart_recon__daily_delta')}
        ORDER BY price_date DESC, symbol
    """)


@st.cache_data(ttl=300)
def load_returns_recon():
    return query(f"""
        SELECT
            symbol, price_date, recon_status,
            streaming_close, streaming_return,
            batch_close, batch_return,
            return_delta, return_delta_bps,
            batch_volatility_20d, batch_volatility_60d
        FROM {fqn('marts', 'mart_recon__returns_delta')}
        ORDER BY price_date DESC, symbol
    """)


pdf = load_price_recon()
rdf = load_returns_recon()

if pdf.empty and rdf.empty:
    st.warning("No reconciliation data yet. Run the bridge scripts and `dbt run`.")
    st.stop()

all_dates = sorted(set(pdf["price_date"].tolist() + rdf["price_date"].tolist()),
                   reverse=True)
date_col, _ = st.columns([1, 5])
with date_col:
    selected_date = st.selectbox("Trading Date", all_dates, index=0, label_visibility="collapsed")

day_p = pdf[pdf["price_date"] == selected_date]
day_r = rdf[rdf["price_date"] == selected_date]

# --- KPIs ---
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Price Recon",   len(day_p))
k2.metric("Price OK",      f"{(day_p['recon_status'] == 'OK').sum()} / {len(day_p)}" if len(day_p) else "—")
k3.metric("Return Recon",  len(day_r))
k4.metric("Returns OK",    f"{(day_r['recon_status'] == 'OK').sum()} / {len(day_r)}" if len(day_r) else "—")
rd_full = day_r.dropna(subset=["return_delta_bps"]) if len(day_r) else None
k5.metric("Median |Δ return|",
          f"{rd_full['return_delta_bps'].abs().median():.2f} bps"
          if rd_full is not None and not rd_full.empty else "—")


def _tight(fig, height=250):
    dark_chart(fig, height)
    fig.update_layout(
        margin=dict(t=24, b=24, l=8, r=8),
        legend=dict(orientation="h", y=1.08, x=0, font=dict(size=9)),
    )
    return fig


# --- Row 1: price recon stack + return recon stack + Δ histogram (3 cols) ---
c1, c2, c3 = st.columns(3)

with c1:
    st.subheader("Daily Price Recon Status")
    if not pdf.empty:
        p_agg = (pdf.groupby(["price_date", "recon_status"])
                    .size().reset_index(name="cnt"))
        fig_p = px.bar(p_agg, x="price_date", y="cnt", color="recon_status",
                       color_discrete_map=_STATUS_COLORS)
        fig_p.update_layout(xaxis_title=None, yaxis_title=None, barmode="stack")
        st.plotly_chart(_tight(fig_p), use_container_width=True)

with c2:
    st.subheader("Daily Return Recon Status")
    if not rdf.empty:
        r_agg = (rdf.groupby(["price_date", "recon_status"])
                    .size().reset_index(name="cnt"))
        fig_r = px.bar(r_agg, x="price_date", y="cnt", color="recon_status",
                       color_discrete_map=_STATUS_COLORS)
        fig_r.update_layout(xaxis_title=None, yaxis_title=None, barmode="stack")
        st.plotly_chart(_tight(fig_r), use_container_width=True)

with c3:
    st.subheader(f"Δ Close % Distribution ({selected_date})")
    if not day_p.empty:
        full = day_p.dropna(subset=["close_pct_delta"])
        if not full.empty:
            fig_hist = px.histogram(
                full, x="close_pct_delta", nbins=40,
                color="session_coverage",
                color_discrete_map={
                    "full_session":      "#636EFA",
                    "partial_session":   "#FFA15A",
                    "missing_streaming": "#EF553B",
                    "missing_batch":     "#AB63FA",
                },
            )
            fig_hist.update_layout(xaxis_title="Δ %", yaxis_title=None, barmode="overlay")
            fig_hist.update_traces(opacity=0.7)
            fig_hist.add_vline(x=0.5,  line_dash="dot", line_color="#FF6692")
            fig_hist.add_vline(x=-0.5, line_dash="dot", line_color="#FF6692")
            st.plotly_chart(_tight(fig_hist), use_container_width=True)

# --- Row 2: scatter (1/2) + mismatch tables stacked (1/2) ---
c4, c5 = st.columns(2)

with c4:
    st.subheader(f"Streaming vs Batch Daily Return ({selected_date})")
    if not day_r.empty:
        both = day_r.dropna(subset=["streaming_return", "batch_return"])
        if not both.empty:
            fig_scatter = px.scatter(
                both, x="batch_return", y="streaming_return",
                color="recon_status", color_discrete_map=_STATUS_COLORS,
                hover_data={"symbol": True, "return_delta_bps": ":.2f"},
                text="symbol",
            )
            bounds = [
                min(both["batch_return"].min(), both["streaming_return"].min()),
                max(both["batch_return"].max(), both["streaming_return"].max()),
            ]
            fig_scatter.add_trace(go.Scatter(
                x=bounds, y=bounds, mode="lines",
                line=dict(dash="dash", color="gray"), showlegend=False, hoverinfo="skip",
            ))
            fig_scatter.update_traces(textposition="top center", textfont_size=7)
            fig_scatter.update_layout(
                xaxis_title="batch", yaxis_title="streaming",
                xaxis_tickformat=".2%", yaxis_tickformat=".2%",
            )
            st.plotly_chart(_tight(fig_scatter, height=380), use_container_width=True)

with c5:
    st.subheader("Mismatches Requiring Attention")
    mismatch_p = pdf[~pdf["recon_status"].isin(["OK", "PARTIAL_SESSION"])].copy()
    mismatch_r = rdf[~rdf["recon_status"].isin(["OK"])].copy()

    st.markdown("**Price mismatches** (excl. PARTIAL_SESSION)")
    if mismatch_p.empty:
        st.success("None.")
    else:
        show_p = mismatch_p[[
            "price_date", "symbol", "recon_status", "close_pct_delta", "vwap_pct_delta"
        ]].head(20)
        st.dataframe(show_p, use_container_width=True, hide_index=True, height=170)

    st.markdown("**Return mismatches**")
    if mismatch_r.empty:
        st.success("None.")
    else:
        show_r = mismatch_r[[
            "price_date", "symbol", "recon_status",
            "streaming_return", "batch_return", "return_delta_bps",
        ]].head(20)
        st.dataframe(show_r, use_container_width=True, hide_index=True, height=170)
