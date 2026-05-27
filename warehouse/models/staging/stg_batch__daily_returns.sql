{{
  config(
    materialized = 'view',
    description  = 'Batch daily returns + rolling 20/60-day volatility on split-adjusted prices.'
  )
}}

select
    composite_figi,
    ticker,
    price_date,
    close_price                                    as batch_close_price,
    daily_return                                   as batch_daily_return,
    volatility_20d                                 as batch_volatility_20d,
    volatility_60d                                 as batch_volatility_60d
from {{ source('recon', 'BATCH_DAILY_RETURNS') }}
