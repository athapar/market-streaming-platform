{{
  config(
    materialized = 'view',
    description  = 'Staging view over GOLD.GOLD_TRADES. Normalises column names.'
  )
}}

select
    composite_figi,
    symbol,
    trade_id,
    trade_price,
    trade_size,
    exchange_id,
    tape,
    sip_timestamp,
    trade_date,
    silver_timestamp
from {{ source('gold', 'GOLD_TRADES') }}
-- exclude pre-production test runs (see var docs in dbt_project.yml)
where trade_date >= '{{ var("first_session_date") }}'
