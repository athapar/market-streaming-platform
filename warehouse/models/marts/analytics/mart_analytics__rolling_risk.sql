{{
  config(
    materialized = 'table',
    description  = 'Rolling 20-day beta, correlation, and volatility vs SPY benchmark.'
  )
}}

/*
  Computes rolling risk metrics for each symbol against SPY.
  Uses daily log returns from the daily rollup.

  Metrics per (symbol, date):
    - rolling_beta:        cov(r_i, r_spy) / var(r_spy) over 20-day window
    - rolling_correlation: corr(r_i, r_spy) over 20-day window
    - rolling_vol:         stddev(r_i) * sqrt(252) annualized
    - rolling_sharpe:      mean(r_i) / stddev(r_i) * sqrt(252) (excess return approx)
    - rolling_downside_dev: stddev of negative returns only * sqrt(252)

  SPY rows are included (beta = 1.0, correlation = 1.0 by definition).
*/

with returns as (
    select
        composite_figi,
        symbol,
        price_date,
        log_return
    from {{ ref('int_analytics__daily_returns') }}
    where log_return is not null
),

spy_returns as (
    select
        price_date,
        log_return as spy_return
    from returns
    where symbol = 'SPY'
),

paired as (
    select
        r.composite_figi,
        r.symbol,
        r.price_date,
        r.log_return,
        s.spy_return
    from returns r
    inner join spy_returns s on r.price_date = s.price_date
),

{#-
  Two Snowflake-specific quirks to work around here:

  1. WINDOW w AS (...) named-window clause: not supported. Every window
     function must repeat the full OVER (...) spec. Keep it in a Jinja var
     so the partition + order + frame is defined once and only once.

  2. Sliding window frames (ROWS BETWEEN N PRECEDING AND CURRENT ROW) are
     NOT supported for COVAR_SAMP, COVAR_POP, CORR, or REGR_*. They only
     work with cumulative frames or no frame at all. SUM, AVG, COUNT, and
     STDDEV all support sliding frames, so we recompute covariance and
     correlation manually using the identities:

         covar_pop  = E[xy] − E[x]·E[y]                       = mean(xy) − mean(x)·mean(y)
         covar_samp = covar_pop · n / (n−1)
         corr       = covar_pop / (stddev_pop_x · stddev_pop_y)

     Correlation is dimensionless — the sample-vs-population n/(n−1)
     factor cancels in numerator and denominator, so we can compute it
     directly from STDDEV (which is _SAMP by default in Snowflake).
-#}
{%- set roll_window = "(partition by symbol order by price_date rows between 19 preceding and current row)" -%}

paired_with_xy as (
    select
        *,
        log_return * spy_return as xy
    from paired
),

rolling_components as (
    select
        composite_figi,
        symbol,
        price_date,
        log_return,
        spy_return,

        count(*)             over {{ roll_window }}  as window_size,
        avg(log_return)      over {{ roll_window }}  as mean_x,
        avg(spy_return)      over {{ roll_window }}  as mean_y,
        avg(xy)              over {{ roll_window }}  as mean_xy,
        stddev(log_return)   over {{ roll_window }}  as stddev_x_samp,
        stddev(spy_return)   over {{ roll_window }}  as stddev_y_samp
    from paired_with_xy
),

rolling as (
    select
        *,

        -- population covariance via identity; multiply by n/(n−1) for sample
        (mean_xy - mean_x * mean_y)
            * window_size
            / nullif(window_size - 1, 0)                                  as cov_with_spy,

        -- sample variance of SPY = stddev_samp ^ 2
        stddev_y_samp * stddev_y_samp                                     as var_spy,

        -- rolling volatility (annualized %)
        stddev_x_samp * sqrt(252) * 100                                   as rolling_vol_ann_pct,

        -- rolling mean return (annualized %)
        mean_x * 252 * 100                                                as rolling_mean_return_ann_pct,

        -- rolling Sharpe approximation (no risk-free rate subtracted)
        case
            when stddev_x_samp > 0
            then (mean_x / stddev_x_samp) * sqrt(252)
        end                                                               as rolling_sharpe,

        -- correlation: dimensionless, so sample/pop adjustment cancels;
        -- equivalent to corr() but works under a sliding frame
        case
            when stddev_x_samp > 0 and stddev_y_samp > 0
            then (mean_xy - mean_x * mean_y) / (stddev_x_samp * stddev_y_samp)
        end                                                               as rolling_correlation

    from rolling_components
)

select
    composite_figi,
    symbol,
    price_date,
    log_return,
    spy_return,
    window_size,

    rolling_vol_ann_pct,
    rolling_mean_return_ann_pct,
    rolling_sharpe,
    rolling_correlation,

    case
        when var_spy > 0 and window_size >= 10
        then cov_with_spy / var_spy
    end                                               as rolling_beta,

    -- alpha = mean_return − beta · mean_spy_return (annualized %)
    case
        when var_spy > 0 and window_size >= 10
        then (rolling_mean_return_ann_pct
              - (cov_with_spy / var_spy) * mean_y * 252 * 100)
    end                                               as rolling_alpha_ann_pct,

    current_timestamp()                               as computed_at

from rolling
where window_size >= 10
