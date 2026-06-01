{{
  config(
    materialized = 'table',
    description  = 'Data quality scorecard: completeness, validity, and freshness per symbol per day.'
  )
}}

/*
  Per (symbol, date) data quality checks computed from Gold.
  Each row gets a quality score (0-100) based on:
    - completeness: bar_count / 390 expected bars (50%)
    - validity: no nulls in critical fields, prices > 0, volume >= 0,
                close within OHLC range, low <= high (50%)

  Note: batch ingest lag (window_start → silver_timestamp) is reported as an
  observability column but deliberately excluded from the quality score. The
  Spark stream runs with trigger(availableNow=True), so a multi-minute lag just
  reflects when the batch was kicked off, not a data defect — scoring it would
  penalize batch-tier days for working as designed. Genuine staleness is caught
  upstream by the ingest freshness SLA / Grafana alerting, not here.
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

        -- batch ingest lag (observability only — NOT scored; see header note)
        avg(datediff('second', window_start, silver_timestamp)) as avg_batch_lag_s,
        max(datediff('second', window_start, silver_timestamp)) as max_batch_lag_s,

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

        -- overall quality score: completeness 50%, validity 50%
        -- (batch lag deliberately excluded — see header note)
        round(
            least(completeness_pct, 100) * 0.50
            + (case
                when (null_or_negative_close + invalid_volume + high_lt_low
                      + close_outside_range + open_outside_range) = 0
                then 100.0
                else greatest(0,
                    100.0 - (null_or_negative_close + invalid_volume + high_lt_low
                             + close_outside_range + open_outside_range)
                    * 100.0 / nullif(bar_count, 0))
               end) * 0.50
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

    round(avg_batch_lag_s, 2) as avg_batch_lag_s,
    round(max_batch_lag_s, 2) as max_batch_lag_s,

    first_bar,
    last_bar,
    current_timestamp()     as computed_at

from scored
