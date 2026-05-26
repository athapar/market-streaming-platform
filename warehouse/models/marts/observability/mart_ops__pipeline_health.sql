{{
  config(
    materialized = 'table',
    description  = 'Pipeline health metrics derived from Gold data: latency, throughput, coverage per day.'
  )
}}

/*
  Computes operational health metrics from the existing Gold tables.
  No additional instrumentation needed — everything is derived from
  columns already in the data (silver_timestamp, window_start, bar_count).

  Metrics:
    - processing_latency_*: silver_timestamp - window_start (how fast bars are processed)
    - coverage_pct: bars received / expected bars (symbols × 390 minutes)
    - symbols_active: distinct symbols seen per day
    - freshness: time since last bar was processed
*/

with minute_bars as (
    select
        event_date,
        symbol,
        window_start,
        silver_timestamp,
        volume,
        trade_count,
        close_price
    from {{ source('gold', 'GOLD_MINUTE_BARS') }}
),

daily_rollup as (
    select
        event_date,
        symbol,
        bar_count,
        first_bar_start,
        last_bar_start,
        updated_at
    from {{ source('gold', 'GOLD_DAILY_ROLLUP') }}
),

-- per-day latency from minute bars
latency_stats as (
    select
        event_date,
        count(*)                                                      as total_bars,
        count(distinct symbol)                                        as symbols_active,

        -- processing latency: how long from market event to Silver write
        avg(datediff('second', window_start, silver_timestamp))       as avg_latency_s,
        median(datediff('second', window_start, silver_timestamp))    as p50_latency_s,
        percentile_cont(0.95) within group (
            order by datediff('second', window_start, silver_timestamp)
        )                                                             as p95_latency_s,
        percentile_cont(0.99) within group (
            order by datediff('second', window_start, silver_timestamp)
        )                                                             as p99_latency_s,
        max(datediff('second', window_start, silver_timestamp))       as max_latency_s,

        -- throughput
        sum(volume)                                                   as total_volume,
        sum(trade_count)                                              as total_trades,
        sum(close_price * volume)                                     as total_dollar_volume,

        -- timing
        min(window_start)                                             as first_bar_at,
        max(window_start)                                             as last_bar_at,
        max(silver_timestamp)                                         as last_processed_at

    from minute_bars
    group by event_date
),

-- expected bars = symbols_active × 390 market minutes
coverage as (
    select
        l.*,
        l.symbols_active * 390                                        as expected_bars,
        round(l.total_bars * 100.0 / nullif(l.symbols_active * 390, 0), 2)
                                                                      as coverage_pct,
        datediff('minute', l.first_bar_at, l.last_bar_at)              as session_duration_min
    from latency_stats l
)

select
    event_date,
    symbols_active,
    total_bars,
    expected_bars,
    coverage_pct,
    session_duration_min,

    round(avg_latency_s, 2)  as avg_latency_s,
    round(p50_latency_s, 2)  as p50_latency_s,
    round(p95_latency_s, 2)  as p95_latency_s,
    round(p99_latency_s, 2)  as p99_latency_s,
    round(max_latency_s, 2)  as max_latency_s,

    total_volume,
    total_trades,
    round(total_dollar_volume, 2) as total_dollar_volume,

    -- throughput: bars per minute of session
    case
        when session_duration_min > 0
        then round(total_bars * 1.0 / session_duration_min, 2)
    end                       as bars_per_minute,

    first_bar_at,
    last_bar_at,
    last_processed_at,
    current_timestamp()       as computed_at

from coverage
