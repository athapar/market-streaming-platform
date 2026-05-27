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
  OK                  Both present, |Δreturn| ≤ 5 bps (5e-4).
  RETURN_MISMATCH     Both present, |Δreturn| > 5 bps. Typically a partial
                      streaming session — see session_coverage in the daily
                      delta mart for context.
  MISSING_STREAMING   Batch only.
  MISSING_BATCH       Streaming only.
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
        when abs(s.streaming_return - b.batch_return) > 0.0005 then 'RETURN_MISMATCH'
        else 'OK'
    end                                            as recon_status,

    current_timestamp()                            as recon_run_at

from streaming s
full outer join batch b
    on  s.composite_figi = b.composite_figi
    and s.price_date     = b.price_date
