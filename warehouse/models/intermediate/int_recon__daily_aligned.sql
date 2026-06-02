{{
  config(
    materialized = 'view',
    description  = 'Full-outer-join of streaming and batch daily prices on (composite_figi, price_date).'
  )
}}

/*
  Aligns the streaming Gold daily rollup with the batch BigQuery daily prices.

  A FULL OUTER JOIN on (composite_figi, price_date) surfaces three cases:
    1. Row exists in both sources            — nominal case, compute delta.
    2. Row in streaming only                 — batch hasn't run yet for that date,
                                               or symbol missing from batch pipeline.
    3. Row in batch only                     — streaming was not running that day
                                               (e.g. session not captured).

  Note on expected VWAP / volume delta:
    The streaming pipeline captures bars from when the producer started to when
    it last ran. A partial session (e.g. 9:22–11:38 AM ET) captures less volume
    than the full trading day and computes a session-VWAP over that window.
    The batch pipeline covers the full 9:30–16:00 ET session.
    Close price will also differ unless the last captured bar coincides with 4 PM.
    These differences are expected and flagged as PARTIAL_SESSION, not errors.
*/

with streaming as (
    select * from {{ ref('stg_streaming__daily_rollup') }}
),

batch as (
    select * from {{ ref('stg_batch__daily_prices') }}
)

select
    -- identity (COALESCE handles the outer-join missing-side case)
    coalesce(s.composite_figi, b.composite_figi) as composite_figi,
    coalesce(s.symbol,         b.symbol)          as symbol,
    coalesce(s.price_date,     b.price_date)       as price_date,

    -- streaming columns
    s.open_price                                   as s_open,
    s.high_price                                   as s_high,
    s.low_price                                    as s_low,
    s.close_price                                  as s_close,
    s.volume                                       as s_volume,
    s.vwap                                         as s_vwap,
    s.total_trades                                 as s_total_trades,
    s.bar_count,
    s.first_bar_start,
    s.last_bar_start,
    s.streaming_updated_at,

    -- batch columns
    b.open_price                                   as b_open,
    b.high_price                                   as b_high,
    b.low_price                                    as b_low,
    b.close_price                                  as b_close,
    b.volume                                       as b_volume,
    b.vwap                                         as b_vwap,
    b.batch_source,
    b.batch_loaded_at,

    -- session coverage flag: streaming captures a partial or full session
    case
        when s.composite_figi is null then 'missing_streaming'
        when b.composite_figi is null then 'missing_batch'
        -- if last bar is before 19:45 UTC (≈ 15:45 ET) session is partial
        when s.last_bar_start < dateadd(hour, -0.25, dateadd(hour, 20, s.price_date::timestamp))
            then 'partial_session'
        else 'full_session'
    end                                            as session_coverage

from streaming s
full outer join batch b
    on  s.composite_figi = b.composite_figi
    and s.price_date     = b.price_date
-- Anchor to the streaming window. Batch prices go back ~20 years; there is
-- nothing to reconcile before streaming existed, and those batch-only rows
-- would otherwise flood the mart (and the dashboard) with MISSING_STREAMING.
where coalesce(s.price_date, b.price_date) >= '{{ var("first_session_date") }}'
