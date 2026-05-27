{{
  config(
    materialized = 'view',
    description  = 'Cross-sectional value / growth / quality factor scores per security.'
  )
}}

select
    composite_figi,
    ticker,
    value_score,
    growth_score,
    quality_score,
    factor_classification,
    pe_ratio,
    pb_ratio,
    operating_margin,
    roe,
    debt_to_equity,
    fcf_conversion
from {{ source('recon', 'FUNDAMENTALS_FACTOR_SCORES') }}
