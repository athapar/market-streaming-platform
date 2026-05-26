{{
  config(
    materialized = 'table',
    description  = 'Data quality scorecard: completeness, validity, and freshness per symbol per day.'
  )
}}

/*
  Per (symbol, date) data quality checks computed from Gold.
  Each row gets a quality score (0-100) based on:
    - completeness: bar_count / 390 expected bars
    - validity: no nulls in critical fields, prices > 0, volume >= 0
    - consistency: close within OHLC range, low <= high
    - freshness: data was processed within expected latency window
*/

with bars as (
    select
        symbol,
        composite_figi,
        event_date,
        window_start,
        silver_timestamp,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        vwap,
        trade_count
    from {{ source('gold', 'GOLD_MINUTE_BARS') }}
),

checks as (
    select
        symbol,
        composite_figi,
        event_date,

        count(*)                                          as bar_count,
        round(count(*) * 100.0 / 390, 2)                 as completeness_pct,

        -- validity: count of bars failing basic checks
        count(case when close_price is null or close_price <= 0 then 1 end)
                                                          as null_or_negative_close,
        count(case when volume is null or volume < 0 then 1 end)
                                                          as invalid_volume,
        count(case when high_price < low_price then 1 end)
                                                          as high_lt_low,
        count(case when close_price > high_price or close_price < low_price then 1 end)
                                                          as close_outside_range,
        count(case when open_price > high_price or open_price < low_price then 1 end)
                                                          as open_outside_range,
        count(case when vwap is null or vwap <= 0 then 1 end)
                                                          as invalid_vwap,

        -- latency
        avg(datediff('second', window_start, silver_timestamp)) as avg_latency_s,
        max(datediff('second', window_start, silver_timestamp)) as max_latency_s,

        -- session window
        min(window_start)                                 as first_bar,
        max(window_start)                                 as last_bar

    from bars
    group by symbol, composite_figi, event_date
),

scored as (
    select
        *,
        -- total invalid bars
        (null_or_negative_close + invalid_volume + high_lt_low
         + close_outside_range + open_outside_range + invalid_vwap)
                                                          as total_invalid_bars,

        -- validity pct (bars passing all checks / total bars)
        round(
            (bar_count - (null_or_negative_close + invalid_volume + high_lt_low
             + close_outside_range + open_outside_range))
            * 100.0 / nullif(bar_count, 0), 2
        )                                                 as validity_pct,

        -- overall quality score: completeness 40%, validity 40%, freshness 20%
        round(
            least(completeness_pct, 100) * 0.40
            + (case
                when (null_or_negative_close + invalid_volume + high_lt_low
                      + close_outside_range + open_outside_range) = 0
                then 100.0
                else greatest(0,
                    100.0 - (null_or_negative_close + invalid_volume + high_lt_low
                             + close_outside_range + open_outside_range)
                    * 100.0 / nullif(bar_count, 0))
               end) * 0.40
            + (case when avg_latency_s < 300 then 100
                    when avg_latency_s < 900 then 50
                    else 0 end) * 0.20
        , 2)                                              as quality_score
    from checks
)

select
    composite_figi,
    symbol,
    event_date,
    bar_count,
    completeness_pct,
    validity_pct,
    quality_score,

    null_or_negative_close,
    invalid_volume,
    high_lt_low,
    close_outside_range,
    open_outside_range,
    invalid_vwap,
    total_invalid_bars,

    round(avg_latency_s, 2) as avg_latency_s,
    round(max_latency_s, 2) as max_latency_s,

    first_bar,
    last_bar,
    current_timestamp()     as computed_at

from scored
