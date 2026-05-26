{{
  config(
    materialized = 'view',
    description  = 'Minute-level log returns and dollar volume derived from Gold minute bars.'
  )
}}

with bars as (
    select * from {{ ref('stg_streaming__minute_bars') }}
),

with_lag as (
    select
        *,
        lag(close_price) over (
            partition by symbol order by window_start
        ) as prev_close,
        lag(event_date) over (
            partition by symbol order by window_start
        ) as prev_event_date
    from bars
)

select
    composite_figi,
    symbol,
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

    close_price * volume                                     as dollar_volume,
    (high_price - low_price)                                 as bar_range,
    case
        when high_price > 0
        then (high_price - low_price) / high_price * 100
    end                                                      as bar_range_pct,

    case
        when prev_close > 0 and event_date = prev_event_date
        then ln(close_price / prev_close)
    end                                                      as log_return,

    case
        when prev_close > 0 and event_date = prev_event_date
        then (close_price - prev_close) / prev_close
    end                                                      as simple_return,

    silver_timestamp
from with_lag
