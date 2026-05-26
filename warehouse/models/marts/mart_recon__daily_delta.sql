{{
  config(
    materialized = 'table',
    description  = 'Reconciliation mart: streaming vs batch daily OHLCV deltas with recon status.'
  )
}}

/*
  mart_recon__daily_delta
  ─────────────────────────────────────────────────────────────────────────────
  One row per (composite_figi, price_date).

  recon_status values
  ───────────────────
  OK                 Both sources agree within tolerance on close and VWAP.
  CLOSE_MISMATCH     |s_close − b_close| / b_close > 0.5 % (full session only).
  VWAP_MISMATCH      |s_vwap  − b_vwap|  / b_vwap  > 1.0 % (full session only).
  PARTIAL_SESSION    Streaming captured < full day; price deltas expected, not
                     flagged as errors.  Volume delta is still computed for info.
  MISSING_STREAMING  No streaming data for this (symbol, date). Batch only.
  MISSING_BATCH      Batch pipeline did not produce data for this (symbol, date).

  Tolerance rationale
  ───────────────────
  0.5 % close tolerance: sub-cent rounding on a $400 stock is ~$0.002 (0.0005 %).
  We give 10× headroom for micro-lot VWAP differences.
  1.0 % VWAP tolerance: session VWAP can differ from batch VWAP by a few cents
  due to lot-size weighting differences between the two sources.
  On a PARTIAL_SESSION day these thresholds are not applied — deltas are computed
  for information only.
*/

with aligned as (
    select * from {{ ref('int_recon__daily_aligned') }}
),

deltas as (
    select
        composite_figi,
        symbol,
        price_date,
        session_coverage,
        bar_count,
        first_bar_start,
        last_bar_start,
        streaming_updated_at,
        batch_loaded_at,

        -- raw prices
        s_open,    b_open,
        s_high,    b_high,
        s_low,     b_low,
        s_close,   b_close,
        s_vwap,    b_vwap,
        s_volume,  b_volume,
        s_total_trades,

        -- absolute deltas (streaming − batch)
        s_close  - b_close                         as close_delta,
        s_vwap   - b_vwap                          as vwap_delta,
        s_volume - b_volume                        as volume_delta,

        -- relative deltas (% of batch value)
        case
            when b_close  is null or b_close  = 0 then null
            else round((s_close  - b_close)  / b_close  * 100, 4)
        end                                        as close_pct_delta,

        case
            when b_vwap   is null or b_vwap   = 0 then null
            else round((s_vwap   - b_vwap)   / b_vwap   * 100, 4)
        end                                        as vwap_pct_delta,

        case
            when b_volume is null or b_volume = 0 then null
            else round((s_volume - b_volume) / b_volume * 100, 4)
        end                                        as volume_pct_delta

    from aligned
)

select
    composite_figi,
    symbol,
    price_date,
    session_coverage,
    bar_count,
    first_bar_start,
    last_bar_start,

    s_open,    b_open,
    s_high,    b_high,
    s_low,     b_low,
    s_close,   b_close,
    s_vwap,    b_vwap,
    s_volume,  b_volume,
    s_total_trades,

    close_delta,
    vwap_delta,
    volume_delta,
    close_pct_delta,
    vwap_pct_delta,
    volume_pct_delta,

    -- ── recon_status ──────────────────────────────────────────────────────
    case
        when session_coverage = 'missing_streaming' then 'MISSING_STREAMING'
        when session_coverage = 'missing_batch'     then 'MISSING_BATCH'
        when session_coverage = 'partial_session'   then 'PARTIAL_SESSION'
        -- full session: apply tolerances
        when abs(close_pct_delta) > 0.5
         and abs(vwap_pct_delta)  > 1.0             then 'CLOSE_AND_VWAP_MISMATCH'
        when abs(close_pct_delta) > 0.5             then 'CLOSE_MISMATCH'
        when abs(vwap_pct_delta)  > 1.0             then 'VWAP_MISMATCH'
        else                                             'OK'
    end                                             as recon_status,

    streaming_updated_at,
    batch_loaded_at,
    current_timestamp()                             as recon_run_at

from deltas
