{{
  config(
    materialized = 'view',
    description  = 'Daily log returns from Gold daily rollup, used for rolling risk metrics.'
  )
}}

with daily as (
    select * from {{ ref('stg_streaming__daily_rollup') }}
),

-- Pin the close to the regular-session 16:00 bar instead of the rollup's
-- last-bar (post-market-skewed) close. See int_streaming__session_close: the
-- rollup close error compounds across day-over-day returns, so we anchor the
-- return basis to the official session boundary. coalesce keeps the rollup
-- close as a fallback if a session close is ever missing.
joined as (
    select
        d.*,
        coalesce(sc.session_close, d.close_price) as session_close_price
    from daily d
    left join {{ ref('int_streaming__session_close') }} sc
        on  d.composite_figi = sc.composite_figi
        and d.price_date     = sc.price_date
),

with_lag as (
    select
        *,
        lag(session_close_price) over (
            partition by composite_figi order by price_date
        ) as prev_close
    from joined
)

select
    composite_figi,
    symbol,
    price_date,
    open_price,
    high_price,
    low_price,
    session_close_price                               as close_price,
    volume,
    vwap,
    total_trades,
    bar_count,
    first_bar_start,
    last_bar_start,

    session_close_price * volume                      as dollar_volume,

    case
        when prev_close > 0
        then ln(session_close_price / prev_close)
    end                                               as log_return,

    case
        when prev_close > 0
        then (session_close_price - prev_close) / prev_close
    end                                               as simple_return,

    (high_price - low_price) / nullif(open_price, 0)  as intraday_range

from with_lag
