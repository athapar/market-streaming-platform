{{
  config(
    materialized = 'table',
    description  = 'Data quality scorecard: completeness, validity, and freshness per symbol per day.'
  )
}}

/*
  Per (symbol, date) data quality checks computed from Gold.
  Each row gets a quality score (0-100) based on:
    - completeness: bars present / bars expected *within the captured window*
                    (first_bar → last_bar). This is the data-integrity metric —
                    it catches dropped/missing minutes, NOT how long the producer
                    ran. (50%)
    - validity: no nulls in critical fields, prices > 0, volume >= 0,
                close within OHLC range, low <= high (50%)

  completeness_pct vs session_coverage_pct — read this before trusting either:
    The producer is started manually and captures a *partial* session window on
    most days (e.g. 09:22–11:38 ET), by design — this is a portfolio pipeline,
    not a 6.5h/day production system. Two different things were being conflated:

      completeness_pct  = bar_count / minutes_in_captured_window  → "did we drop
                          any bars while we were capturing?" (≈100% in practice)
      session_coverage_pct = bar_count / 391 full-session minutes → "how much of
                          the 09:30–16:00 ET session did we capture?" (low on
                          partial-session days — expected, NOT a defect)

    Only completeness (integrity) feeds the quality score. session_coverage is
    reported alongside so a partial run is not penalised for being short.

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
    -- exclude pre-production test runs (see var docs in dbt_project.yml)
    where event_date >= '{{ var("first_session_date") }}'
),

checks as (
    select
        symbol,
        composite_figi,
        event_date,

        count(*)                                          as bar_count,

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

derived as (
    select
        *,
        -- total invalid bars
        (null_or_negative_close + invalid_volume + high_lt_low
         + close_outside_range + open_outside_range + invalid_vwap)
                                                          as total_invalid_bars,

        -- minutes spanned by the captured window (1 bar/min, inclusive)
        datediff('minute', first_bar, last_bar) + 1       as expected_bars_in_window,

        -- in-window completeness (the integrity metric): bars present / bars
        -- expected within the captured window. ~100% unless minutes were dropped.
        least(round(
            bar_count * 100.0
            / nullif(datediff('minute', first_bar, last_bar) + 1, 0), 2
        ), 100)                                           as completeness_pct,

        -- session coverage: fraction of the full 09:30–16:00 ET session captured.
        -- Low on partial-session days *by design* — reported, not scored.
        least(round(bar_count * 100.0 / 391, 2), 100)     as session_coverage_pct,

        -- validity pct (bars passing all checks / total bars)
        round(
            (bar_count - (null_or_negative_close + invalid_volume + high_lt_low
             + close_outside_range + open_outside_range))
            * 100.0 / nullif(bar_count, 0), 2
        )                                                 as validity_pct
    from checks
),

scored as (
    select
        *,
        -- overall quality score: in-window completeness 50%, validity 50%.
        -- (session coverage and batch lag deliberately excluded — see header.)
        round(completeness_pct * 0.50 + validity_pct * 0.50, 2) as quality_score
    from derived
)

select
    composite_figi,
    symbol,
    event_date,
    bar_count,
    expected_bars_in_window,
    completeness_pct,
    session_coverage_pct,
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
