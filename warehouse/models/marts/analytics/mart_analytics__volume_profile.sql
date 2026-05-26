{{
  config(
    materialized = 'table',
    description  = 'Intraday volume profile: average volume and activity by 30-minute bucket per symbol.'
  )
}}

/*
  Aggregates minute bars into 30-minute time-of-day buckets.
  Shows the classic U-shaped intraday volume curve for each symbol.
  Useful for identifying optimal execution windows and detecting
  anomalous activity patterns.
*/

with bars as (
    select
        symbol,
        composite_figi,
        event_date,
        window_start,
        volume,
        dollar_volume,
        trade_count,
        close_price,
        vwap,
        log_return
    from {{ ref('int_analytics__minute_returns') }}
),

bucketed as (
    select
        *,
        -- 30-minute bucket: 09:30, 10:00, 10:30, ..., 15:30
        dateadd(
            'minute',
            floor(extract(minute from window_start) / 30) * 30,
            date_trunc('hour', window_start)
        )                                                    as time_bucket,
        extract(hour from window_start) * 100
            + floor(extract(minute from window_start) / 30) * 30
                                                             as bucket_id
    from bars
),

profiles as (
    select
        symbol,
        composite_figi,
        bucket_id,
        min(time_bucket)::time                           as bucket_time,

        count(distinct event_date)                       as trading_days,
        count(*)                                         as total_bars,

        -- volume
        avg(volume)                                      as avg_volume_per_bar,
        sum(volume) / count(distinct event_date)         as avg_volume_per_bucket,
        avg(dollar_volume)                               as avg_dollar_volume_per_bar,

        -- activity
        avg(trade_count)                                 as avg_trades_per_bar,

        -- volatility within bucket
        avg(abs(log_return))                             as avg_abs_return,
        stddev(log_return)                               as return_stddev,

        -- price dynamics
        avg(abs(close_price - vwap) / nullif(vwap, 0)) * 100
                                                         as avg_vwap_deviation_pct

    from bucketed
    where log_return is not null
    group by symbol, composite_figi, bucket_id
)

select
    symbol,
    composite_figi,
    bucket_id,
    bucket_time,
    trading_days,
    total_bars,
    avg_volume_per_bar,
    avg_volume_per_bucket,
    avg_dollar_volume_per_bar,
    avg_trades_per_bar,
    avg_abs_return,
    return_stddev,
    avg_vwap_deviation_pct,

    -- relative volume: this bucket's avg vs the symbol's overall avg
    avg_volume_per_bucket / nullif(
        avg(avg_volume_per_bucket) over (partition by symbol), 0
    )                                                    as relative_volume,

    current_timestamp()                                  as computed_at

from profiles
