{{
  config(
    materialized = 'view',
    description  = 'Staging view over GOLD.GOLD_DAILY_ROLLUP. Normalises column names to snake_case.'
  )
}}

select
    composite_figi,
    symbol,
    event_date                                    as price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    total_trades,
    bar_count,
    first_bar_start,
    last_bar_start,
    updated_at                                    as streaming_updated_at
from {{ source('gold', 'GOLD_DAILY_ROLLUP') }}
-- exclude pre-production test runs (see var docs in dbt_project.yml)
where event_date >= '{{ var("first_session_date") }}'
