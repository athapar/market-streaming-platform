{{
  config(
    materialized = 'view',
    description  = 'Reference data per security — sector (SIC), market cap, shares outstanding, list date.'
  )
}}

select
    composite_figi,
    ticker,
    company_name,
    sic_code,
    sic_description,
    market_cap,
    shares_outstanding,
    total_employees,
    list_date,
    loaded_at                                      as batch_loaded_at
from {{ source('recon', 'COMPANY_OVERVIEW') }}
