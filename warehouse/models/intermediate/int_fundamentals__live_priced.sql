{{
  config(
    materialized = 'view',
    description  = 'Batch TTM fundamentals joined to the latest streaming close. Live-priced valuation.'
  )
}}

/*
  Joins each security's batch TTM fundamentals snapshot to its most recent
  streaming close, then rescales price-derived ratios from the batch price to
  the live price.

  Price-scaling identities (since every price-derived ratio is linear in price):
      live_pe  = batch_pe  × (live_close / batch_close)
      live_pb  = batch_pb  × (live_close / batch_close)
      live_ps  = batch_ps  × (live_close / batch_close)
      live_p_fcf = batch_p_fcf × (live_close / batch_close)
      live_market_cap = market_cap × (live_close / batch_close)

  Profitability and balance-sheet ratios (margins, ROE, ROA, debt/equity,
  current ratio) do not depend on price and pass through unchanged.

  ev_ebit involves debt + cash terms, not just market_cap, so a clean live
  recompute requires the underlying balance-sheet items. We pass the batch
  value through and expose batch_close so downstream models can recompute
  if needed.
*/

with latest_streaming as (
    select
        composite_figi,
        symbol,
        close_price                                as live_close,
        price_date                                 as live_close_date,
        volume                                     as live_session_volume,
        bar_count                                  as live_bar_count
    from {{ ref('stg_streaming__daily_rollup') }}
    qualify row_number() over (
        partition by composite_figi
        order by price_date desc
    ) = 1
),

batch_fund as (
    select * from {{ ref('stg_batch__fundamentals_valuation') }}
)

select
    b.composite_figi,
    b.ticker,

    -- prices
    s.live_close,
    s.live_close_date,
    b.batch_close_price                            as batch_close,
    b.batch_price_as_of                            as batch_close_date,

    -- live / batch price scaling factor (null when streaming is missing)
    case
        when s.live_close is null or b.batch_close_price is null then null
        when b.batch_close_price = 0 then null
        else s.live_close / b.batch_close_price
    end                                            as price_scale,

    -- batch ratios (passed through)
    b.batch_pe_ratio,
    b.batch_pb_ratio,
    b.batch_ps_ratio,
    b.batch_ev_ebit,
    b.batch_price_to_fcf,

    -- price-independent fundamentals (passed through)
    b.gross_margin,
    b.operating_margin,
    b.net_margin,
    b.roe,
    b.roa,
    b.current_ratio,
    b.debt_to_equity,

    -- snapshot context
    b.financials_as_of,
    b.filing_date,
    b.market_cap                                   as batch_market_cap,
    b.shares_outstanding,
    b.ttm_revenue,
    b.ttm_net_income,
    b.ttm_free_cash_flow,
    b.book_value,
    b.total_assets,
    b.total_liabilities,

    -- session context
    s.live_session_volume,
    s.live_bar_count,

    b.batch_loaded_at

from batch_fund b
left join latest_streaming s
    on b.composite_figi = s.composite_figi
