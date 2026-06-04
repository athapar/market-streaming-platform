{{
  config(
    materialized = 'view',
    description  = 'Regular-session (09:30-16:00 ET) daily close per (composite_figi, price_date), derived from the minute bars.'
  )
}}

/*
  Why this model exists
  ─────────────────────
  GOLD_DAILY_ROLLUP.close_price is the close of the *last bar of the day* — and
  the producer captures a few post-16:00 ET bars (sessions bleed to ~16:04). So
  the rollup "close" is frequently a post-market print, ~5-9 bps off the official
  16:00 regular-session close (measured across the captured sessions).

  That error is small per day, but it is corrosive for *returns*: a day-over-day
  return divides today's close by the prior day's close, so a post-market-skewed
  close pollutes BOTH the day it occurs on and the next day's return — the error
  carries over. Against batch's official/auction close that compounding shows up
  as a wall of RETURN_MISMATCH rows.

  This model pins the streaming close to the last bar inside the regular session
  (09:30-16:00 ET inclusive), so day-over-day returns and the close recon compare
  like-for-like against the batch official close. Consumed by
  int_analytics__daily_returns and int_recon__daily_aligned.
*/

with bars as (
    select
        composite_figi,
        symbol,
        event_date as price_date,
        window_start,
        close_price
    from {{ ref('stg_streaming__minute_bars') }}
    -- regular session only: 09:30-16:00 ET inclusive
    where time(convert_timezone('UTC', 'America/New_York', window_start))
              between '09:30:00' and '16:00:00'
)

select
    composite_figi,
    symbol,
    price_date,
    close_price as session_close
from bars
-- last bar at or before 16:00 ET = the regular-session close
qualify row_number() over (
    partition by composite_figi, price_date
    order by window_start desc
) = 1
