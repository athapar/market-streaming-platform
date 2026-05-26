{{
  config(
    materialized = 'view',
    description  = 'Staging view over RECON.BATCH_DAILY_PRICES. Sourced from the batch BigQuery pipeline.'
  )
}}

select
    composite_figi,
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    vwap,
    source                                        as batch_source,
    loaded_at                                     as batch_loaded_at
from {{ source('recon', 'BATCH_DAILY_PRICES') }}
