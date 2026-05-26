{{
  config(
    materialized = 'table',
    description  = 'Daily per-symbol statistics: realized volatility, volume profile, price dynamics.'
  )
}}

with minute_returns as (
    select * from {{ ref('int_analytics__minute_returns') }}
),

daily as (
    select * from {{ ref('int_analytics__daily_returns') }}
),

minute_aggs as (
    select
        symbol,
        composite_figi,
        event_date,

        count(*)                                              as bar_count,
        count(log_return)                                     as return_count,

        -- realized volatility from intraday minute returns (annualized)
        case
            when count(log_return) > 1
            then sqrt(sum(power(log_return, 2)) * 252 * 390) * 100
        end                                                   as realized_vol_ann_pct,

        -- intraday stats
        sum(volume)                                           as total_volume,
        sum(dollar_volume)                                    as total_dollar_volume,
        sum(trade_count)                                      as total_trades,
        avg(bar_range_pct)                                    as avg_bar_range_pct,
        max(bar_range_pct)                                    as max_bar_range_pct,

        -- VWAP deviation: how far did minute closes deviate from VWAP on average
        avg(abs(close_price - vwap) / nullif(vwap, 0)) * 100  as avg_vwap_deviation_pct,

        -- session timing
        min(window_start)                                     as session_open,
        max(window_start)                                     as session_close,
        datediff('minute', min(window_start), max(window_start)) as session_minutes

    from minute_returns
    group by symbol, composite_figi, event_date
),

combined as (
    select
        m.composite_figi,
        m.symbol,
        m.event_date,

        d.open_price,
        d.high_price,
        d.low_price,
        d.close_price,
        d.log_return                       as daily_log_return,
        d.simple_return                    as daily_simple_return,
        d.intraday_range,

        m.bar_count,
        m.return_count,
        m.realized_vol_ann_pct,
        m.total_volume,
        m.total_dollar_volume,
        m.total_trades,
        m.avg_bar_range_pct,
        m.max_bar_range_pct,
        m.avg_vwap_deviation_pct,
        m.session_open,
        m.session_close,
        m.session_minutes,

        -- Garman-Klass volatility estimator (annualized, uses OHLC)
        case
            when d.open_price > 0 and d.close_price > 0
            then sqrt(
                (0.5 * power(ln(d.high_price / d.low_price), 2)
                 - (2 * ln(2) - 1) * power(ln(d.close_price / d.open_price), 2))
                * 252
            ) * 100
        end                                as garman_klass_vol_ann_pct,

        -- rolling averages (20-day)
        avg(m.total_volume) over (
            partition by m.symbol order by m.event_date
            rows between 19 preceding and current row
        )                                  as volume_20d_avg,

        avg(m.total_dollar_volume) over (
            partition by m.symbol order by m.event_date
            rows between 19 preceding and current row
        )                                  as dollar_volume_20d_avg,

        avg(m.realized_vol_ann_pct) over (
            partition by m.symbol order by m.event_date
            rows between 19 preceding and current row
        )                                  as realized_vol_20d_avg

    from minute_aggs m
    left join daily d
        on  m.composite_figi = d.composite_figi
        and m.event_date     = d.price_date
)

select
    *,
    -- volume z-score vs 20-day average
    case
        when volume_20d_avg > 0
        then (total_volume - volume_20d_avg) / nullif(
            stddev(total_volume) over (
                partition by symbol order by event_date
                rows between 19 preceding and current row
            ), 0)
    end as volume_zscore,

    current_timestamp() as computed_at

from combined
