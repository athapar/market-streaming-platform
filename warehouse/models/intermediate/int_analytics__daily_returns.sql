{{
  config(
    materialized = 'view',
    description  = 'Daily log returns from Gold daily rollup, used for rolling risk metrics.'
  )
}}

with daily as (
    select * from {{ ref('stg_streaming__daily_rollup') }}
),

with_lag as (
    select
        *,
        lag(close_price) over (
            partition by composite_figi order by price_date
        ) as prev_close
    from daily
)

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
    total_trades,
    bar_count,
    first_bar_start,
    last_bar_start,

    close_price * volume                              as dollar_volume,

    case
        when prev_close > 0
        then ln(close_price / prev_close)
    end                                               as log_return,

    case
        when prev_close > 0
        then (close_price - prev_close) / prev_close
    end                                               as simple_return,

    (high_price - low_price) / nullif(open_price, 0)  as intraday_range

from with_lag
