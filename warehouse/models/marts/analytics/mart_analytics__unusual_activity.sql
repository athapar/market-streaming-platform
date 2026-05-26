{{
  config(
    materialized = 'table',
    description  = 'Flags unusual volume and volatility days using z-scores vs rolling 20-day baseline.'
  )
}}

/*
  Detects unusual market activity per (symbol, date).
  Flags are based on z-scores against a 20-day rolling window:
    - VOLUME_SPIKE:     volume z-score > 2.0
    - VOLATILITY_SPIKE: realized vol z-score > 2.0
    - LARGE_MOVE:       |daily return| > 2 * 20-day rolling vol
    - RANGE_EXPANSION:  intraday range > 2 * 20-day avg range

  These are observational flags, not trading signals.
*/

with daily as (
    select * from {{ ref('mart_analytics__daily_stats') }}
),

with_zscores as (
    select
        composite_figi,
        symbol,
        event_date,
        close_price,
        daily_log_return,
        daily_simple_return,
        total_volume,
        total_dollar_volume,
        realized_vol_ann_pct,
        intraday_range,
        bar_count,
        volume_zscore,

        -- volatility z-score
        case
            when realized_vol_20d_avg > 0
            then (realized_vol_ann_pct - realized_vol_20d_avg) / nullif(
                stddev(realized_vol_ann_pct) over (
                    partition by symbol order by event_date
                    rows between 19 preceding and current row
                ), 0)
        end                                                as vol_zscore,

        -- range z-score
        case
            when avg(intraday_range) over (
                    partition by symbol order by event_date
                    rows between 19 preceding and current row
                 ) > 0
            then (intraday_range - avg(intraday_range) over (
                    partition by symbol order by event_date
                    rows between 19 preceding and current row
                 )) / nullif(stddev(intraday_range) over (
                    partition by symbol order by event_date
                    rows between 19 preceding and current row
                 ), 0)
        end                                                as range_zscore,

        -- return magnitude vs rolling vol
        case
            when realized_vol_20d_avg > 0
            then abs(daily_log_return) / (realized_vol_20d_avg / 100 / sqrt(252))
        end                                                as return_vol_ratio,

        volume_20d_avg,
        realized_vol_20d_avg

    from daily
    where bar_count >= 10
),

flagged as (
    select
        *,
        case when volume_zscore > 2.0      then true else false end as is_volume_spike,
        case when vol_zscore > 2.0         then true else false end as is_volatility_spike,
        case when return_vol_ratio > 2.0   then true else false end as is_large_move,
        case when range_zscore > 2.0       then true else false end as is_range_expansion
    from with_zscores
)

select
    composite_figi,
    symbol,
    event_date,
    close_price,
    daily_simple_return,
    total_volume,
    total_dollar_volume,
    realized_vol_ann_pct,
    intraday_range,
    bar_count,

    volume_zscore,
    vol_zscore,
    range_zscore,
    return_vol_ratio,

    is_volume_spike,
    is_volatility_spike,
    is_large_move,
    is_range_expansion,

    -- overall flag
    (is_volume_spike or is_volatility_spike or is_large_move or is_range_expansion)
        as is_unusual,

    -- classification
    case
        when is_volume_spike and is_volatility_spike then 'VOLUME_AND_VOL_SPIKE'
        when is_large_move and is_volume_spike       then 'LARGE_MOVE_HIGH_VOLUME'
        when is_large_move                           then 'LARGE_MOVE'
        when is_volume_spike                         then 'VOLUME_SPIKE'
        when is_volatility_spike                     then 'VOLATILITY_SPIKE'
        when is_range_expansion                      then 'RANGE_EXPANSION'
        else                                              'NORMAL'
    end                                               as activity_classification,

    volume_20d_avg,
    realized_vol_20d_avg,
    current_timestamp()                               as computed_at

from flagged
