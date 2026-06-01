{{
  config(
    materialized = 'table',
    description  = 'Pipeline health metrics derived from Gold data: batch ingest lag, throughput, regular-session coverage per day.'
  )
}}

/*
  Computes operational health metrics from the existing Gold tables.
  No additional instrumentation needed — everything is derived from
  columns already in the data (silver_timestamp, window_start, bar_count).

  Metrics:
    - batch_lag_*: silver_timestamp - window_start. This is NOT producer→Kafka
      latency. The Spark stream runs with trigger(availableNow=True) (batch
      mode), so this measures the wall-clock gap between a bar's market minute
      and when the operator kicked off the batch that ingested it. For the
      real-time producer→Kafka path (sub-50ms p99) see the Prometheus/Grafana
      kafka_produce_latency_seconds histogram. pipeline_mode flags which tier
      a given day ran in.
    - coverage_pct: regular-session bars received / expected (symbols × 391).
      391 = one bar per minute 09:30–16:00 ET inclusive. We filter to the
      regular session because pre/post-market bars would push coverage >100%
      against a regular-hours baseline (an apples-to-oranges recon flag, not
      an achievement). Capped at 100%.
    - symbols_active: distinct symbols seen per day (regular session).
    - freshness: time since last bar was processed.

  window_start is stored in UTC (Polygon epoch ms cast straight to a timestamp
  with no TZ conversion), so we convert to America/New_York to apply the
  session filter — convert_timezone handles EST/EDT automatically.
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
    -- regular session only: 09:30–16:00 ET inclusive
    where time(convert_timezone('UTC', 'America/New_York', window_start))
              between '09:30:00' and '16:00:00'
),

-- per-day lag + throughput from regular-session minute bars
daily_stats as (
    select
        event_date,
        count(*)                                                      as total_bars,
        count(distinct symbol)                                        as symbols_active,

        -- batch ingest lag: market minute -> Silver write (see header note)
        avg(datediff('second', window_start, silver_timestamp))       as avg_batch_lag_s,
        median(datediff('second', window_start, silver_timestamp))    as p50_batch_lag_s,
        percentile_cont(0.95) within group (
            order by datediff('second', window_start, silver_timestamp)
        )                                                             as p95_batch_lag_s,
        percentile_cont(0.99) within group (
            order by datediff('second', window_start, silver_timestamp)
        )                                                             as p99_batch_lag_s,
        max(datediff('second', window_start, silver_timestamp))       as max_batch_lag_s,

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

-- expected bars = symbols_active × 391 regular-session minutes
coverage as (
    select
        d.*,
        d.symbols_active * 391                                        as expected_bars,
        least(
            round(d.total_bars * 100.0 / nullif(d.symbols_active * 391, 0), 2),
            100
        )                                                             as coverage_pct,
        datediff('minute', d.first_bar_at, d.last_bar_at)             as session_duration_min
    from daily_stats d
)

select
    event_date,
    symbols_active,
    total_bars,
    expected_bars,
    coverage_pct,
    session_duration_min,

    -- batch ingest lag (NOT producer latency — see model header)
    round(avg_batch_lag_s, 2)  as avg_batch_lag_s,
    round(p50_batch_lag_s, 2)  as p50_batch_lag_s,
    round(p95_batch_lag_s, 2)  as p95_batch_lag_s,
    round(p99_batch_lag_s, 2)  as p99_batch_lag_s,
    round(max_batch_lag_s, 2)  as max_batch_lag_s,

    -- which tier this day ran in: a median lag over 2 min means the bars were
    -- swept up by an availableNow batch, not a continuous (processingTime) stream
    case
        when p50_batch_lag_s > 120 then 'batch'
        else 'streaming'
    end                        as pipeline_mode,

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
