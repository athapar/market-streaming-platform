{{
  config(
    materialized = 'table',
    description  = 'TTM dividend yield time series enriched with sector context.'
  )
}}

/*
  Thin pass-through of the batch dividend yield mart, enriched with sector and
  market-cap context from COMPANY_OVERVIEW. The TTM yield computation is done
  upstream in BigQuery (see batch repo mart_dividend_yield); the value of
  bridging it here is sector-level slicing in the dashboard and joinability
  with streaming price data.

  current_yield_estimate uses the latest live close (from the live-priced
  fundamentals mart) when available, so dashboards can show a near-real-time
  yield estimate alongside the latest ex-div TTM yield.
*/

with yield_events as (
    select * from {{ ref('stg_batch__dividend_yield') }}
),

overview as (
    select
        composite_figi,
        sic_code,
        sic_description,
        market_cap
    from {{ ref('stg_batch__company_overview') }}
),

latest_live as (
    select
        composite_figi,
        live_close,
        live_close_date
    from {{ ref('mart_fundamentals__valuation_live') }}
),

latest_event_per_security as (
    select composite_figi, max(ex_dividend_date) as latest_ex_dividend_date
    from yield_events
    group by composite_figi
)

select
    y.composite_figi,
    y.ticker,
    y.ex_dividend_date,
    y.cash_amount,
    y.ttm_dividends_per_share,
    y.batch_close_price,
    y.ttm_dividend_yield,

    -- live yield estimate (only meaningful on the latest event per security)
    case
        when le.latest_ex_dividend_date = y.ex_dividend_date and l.live_close is not null
        then round(y.ttm_dividends_per_share / nullif(l.live_close, 0), 6)
    end                                                as live_yield_estimate,

    case
        when le.latest_ex_dividend_date = y.ex_dividend_date then true
        else false
    end                                                as is_latest_event,

    o.sic_code,
    o.sic_description,
    o.market_cap

from yield_events y
left join overview                  o  using (composite_figi)
left join latest_event_per_security le using (composite_figi)
left join latest_live               l  on l.composite_figi = y.composite_figi
