{{
  config(
    materialized = 'table',
    description  = 'Value / Growth / Quality factor scores joined with company sector for slicing.'
  )
}}

/*
  Pass-through of the batch factor scores enriched with sector / market-cap
  context for dashboard slicing. The scoring itself is computed upstream in
  BigQuery (cross-sectional percentile ranks); this mart joins in
  COMPANY_OVERVIEW so the dashboard can group by sector without a second hop.
*/

with scores as (
    select * from {{ ref('stg_batch__fundamentals_factor_scores') }}
),

overview as (
    select
        composite_figi,
        sic_code,
        sic_description,
        market_cap,
        shares_outstanding
    from {{ ref('stg_batch__company_overview') }}
)

select
    s.composite_figi,
    s.ticker,
    s.factor_classification,

    -- factor scores (0–1, higher = stronger signal)
    s.value_score,
    s.growth_score,
    s.quality_score,

    -- underlying metrics
    s.pe_ratio,
    s.pb_ratio,
    s.operating_margin,
    s.roe,
    s.debt_to_equity,
    s.fcf_conversion,

    -- sector / size context
    o.sic_code,
    o.sic_description,
    o.market_cap,
    o.shares_outstanding

from scores s
left join overview o using (composite_figi)
