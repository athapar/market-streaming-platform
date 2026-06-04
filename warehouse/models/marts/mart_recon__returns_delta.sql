{{
  config(
    materialized = 'table',
    description  = 'Reconciliation: streaming-computed vs batch daily returns per (figi, date).'
  )
}}

/*
  mart_recon__returns_delta
  ───────────────────────────────────────────────────────────────────────────
  Independent close-to-close return computations:
    streaming_return = (gold_today_close − gold_yesterday_close) / gold_yesterday_close
    batch_return     = (batch_today_close − batch_yesterday_close) / batch_yesterday_close

  Both should agree to within rounding error (~1e-6) when the streaming session
  captured the full day and the batch close is the same as the SIP official
  close. They will diverge when:
    - streaming captured a partial session and its "close" is mid-day
    - batch hasn't run yet for the date (batch null)
    - streaming didn't run that day (streaming null)

  recon_status taxonomy
  ─────────────────────
  OK                  Both present, |Δreturn| ≤ 50 bps (5e-3).
  RETURN_MISMATCH     Both present, |Δreturn| > 50 bps.
  MISSING_STREAMING   Batch only.
  MISSING_BATCH       Streaming only.

  Tolerance rationale (50 bps)
  ────────────────────────────
  Streaming and batch use *independent* close-price sources: streaming's close
  is the last regular-session minute-bar print (~16:00 continuous trade), batch's
  is the official consolidated/closing-auction price. A daily return divides two
  closes, so it compounds the per-day source difference across BOTH days. At a 5
  bps tolerance the median |Δreturn| (~4 bps) sat right on the line and flagged
  ~half the universe on close-source noise, not error. 50 bps separates genuine
  per-symbol divergences (the tail) from that structural cross-source noise.
  (The streaming close is also pinned to the regular-session 16:00 bar upstream —
  see int_streaming__session_close — which removes the post-market-skew component
  of that noise.)
*/

with streaming as (
    select
        composite_figi,
        symbol,
        price_date,
        close_price                                as streaming_close,
        simple_return                              as streaming_return
    from {{ ref('int_analytics__daily_returns') }}
    where simple_return is not null
),

batch as (
    select
        composite_figi,
        ticker                                     as symbol,
        price_date,
        batch_close_price                          as batch_close,
        batch_daily_return                         as batch_return,
        batch_volatility_20d,
        batch_volatility_60d
    from {{ ref('stg_batch__daily_returns') }}
)

select
    coalesce(s.composite_figi, b.composite_figi)   as composite_figi,
    coalesce(s.symbol,         b.symbol)            as symbol,
    coalesce(s.price_date,     b.price_date)        as price_date,

    s.streaming_close,
    s.streaming_return,
    b.batch_close,
    b.batch_return,
    b.batch_volatility_20d,
    b.batch_volatility_60d,

    -- absolute delta
    case
        when s.streaming_return is null or b.batch_return is null then null
        else s.streaming_return - b.batch_return
    end                                            as return_delta,

    -- in basis points for readability
    case
        when s.streaming_return is null or b.batch_return is null then null
        else round((s.streaming_return - b.batch_return) * 10000, 4)
    end                                            as return_delta_bps,

    case
        when s.composite_figi is null then 'MISSING_STREAMING'
        when b.composite_figi is null then 'MISSING_BATCH'
        when abs(s.streaming_return - b.batch_return) > 0.0050 then 'RETURN_MISMATCH'
        else 'OK'
    end                                            as recon_status,

    current_timestamp()                            as recon_run_at

from streaming s
full outer join batch b
    on  s.composite_figi = b.composite_figi
    and s.price_date     = b.price_date
-- Anchor to the streaming window (see note in int_recon__daily_aligned):
-- batch returns span ~20 years, but reconciliation only applies once streaming
-- data exists.
where coalesce(s.price_date, b.price_date) >= '{{ var("first_session_date") }}'
