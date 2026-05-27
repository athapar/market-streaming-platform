{{
  config(
    materialized = 'table',
    description  = 'Live-priced valuation: batch TTM fundamentals × latest streaming close.'
  )
}}

/*
  mart_fundamentals__valuation_live
  ───────────────────────────────────────────────────────────────────────────
  One row per composite_figi: the most recent batch fundamentals snapshot
  re-priced against the latest streaming close.

  How to read each column
  ───────────────────────
  live_*                  Computed from latest streaming close (Gold daily rollup).
  batch_*                 As of the batch pipeline's last daily close (batch_close_date).
  *_delta_pct             100 × (live − batch) / batch.  Movement since the batch snapshot.
  price_scale             live_close / batch_close.  Common multiplier for all price-derived ratios.
  pricing_status          OK / STALE_STREAMING / MISSING_STREAMING — see CASE below.

  STALE_STREAMING fires when the most recent streaming close lags the batch
  close, which happens overnight before the next session captures any bars.
  In that window the live values equal the batch values by construction.
*/

with priced as (
    select * from {{ ref('int_fundamentals__live_priced') }}
)

select
    composite_figi,
    ticker,

    -- prices
    live_close,
    live_close_date,
    batch_close,
    batch_close_date,
    price_scale,

    -- live (rescaled) ratios
    case when price_scale is null then batch_pe_ratio
         else batch_pe_ratio       * price_scale end as live_pe_ratio,
    case when price_scale is null then batch_pb_ratio
         else batch_pb_ratio       * price_scale end as live_pb_ratio,
    case when price_scale is null then batch_ps_ratio
         else batch_ps_ratio       * price_scale end as live_ps_ratio,
    case when price_scale is null then batch_price_to_fcf
         else batch_price_to_fcf   * price_scale end as live_price_to_fcf,
    case when price_scale is null then batch_market_cap
         else batch_market_cap     * price_scale end as live_market_cap,

    -- batch ratios for reference / delta
    batch_pe_ratio,
    batch_pb_ratio,
    batch_ps_ratio,
    batch_ev_ebit,
    batch_price_to_fcf,
    batch_market_cap,

    -- % change since batch snapshot
    case
        when batch_close is null or batch_close = 0 then null
        else round((live_close - batch_close) / batch_close * 100, 4)
    end                                                as close_delta_pct,

    -- price-independent fundamentals
    gross_margin,
    operating_margin,
    net_margin,
    roe,
    roa,
    current_ratio,
    debt_to_equity,

    -- absolute TTM fundamentals
    ttm_revenue,
    ttm_net_income,
    ttm_free_cash_flow,
    book_value,
    total_assets,
    total_liabilities,
    shares_outstanding,

    -- session lineage
    live_session_volume,
    live_bar_count,
    financials_as_of,
    filing_date,
    batch_loaded_at,

    -- pricing status
    case
        when live_close is null then 'MISSING_STREAMING'
        when live_close_date < batch_close_date then 'STALE_STREAMING'
        else 'OK'
    end                                                as pricing_status,

    current_timestamp()                                as recon_run_at

from priced
