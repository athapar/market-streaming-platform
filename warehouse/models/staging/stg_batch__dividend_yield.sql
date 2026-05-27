{{
  config(
    materialized = 'view',
    description  = 'TTM dividend yield time series per (composite_figi, ex_dividend_date).'
  )
}}

select
    composite_figi,
    ticker,
    ex_dividend_date,
    cash_amount,
    ttm_dividends_per_share,
    close_price                                    as batch_close_price,
    ttm_dividend_yield
from {{ source('recon', 'DIVIDEND_YIELD') }}
