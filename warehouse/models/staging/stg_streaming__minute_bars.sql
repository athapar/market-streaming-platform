{{
  config(
    materialized = 'view',
    description  = 'Staging view over GOLD.GOLD_MINUTE_BARS. Normalises column names.'
  )
}}

select
    composite_figi,
    symbol,
    event_type,
    window_start,
    window_end,
    event_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    trade_count,
    silver_timestamp
from {{ source('gold', 'GOLD_MINUTE_BARS') }}
-- exclude pre-production test runs (see var docs in dbt_project.yml)
where event_date >= '{{ var("first_session_date") }}'
