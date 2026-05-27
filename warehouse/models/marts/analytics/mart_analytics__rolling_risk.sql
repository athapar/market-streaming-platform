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
  Snowflake doesn't accept the standard-SQL `WINDOW w AS (...)` named-window
  clause — every window function must repeat the full OVER (...) spec.
  Keep the spec in a Jinja var so it's defined once and only once.
-#}
{%- set roll_window = "(partition by symbol order by price_date rows between 19 preceding and current row)" -%}

rolling as (
    select
        composite_figi,
        symbol,
        price_date,
        log_return,
        spy_return,

        count(*)                              over {{ roll_window }}  as window_size,

        -- rolling volatility (annualized)
        stddev(log_return)                    over {{ roll_window }} * sqrt(252) * 100
                                                                       as rolling_vol_ann_pct,

        -- rolling mean return (annualized)
        avg(log_return)                       over {{ roll_window }} * 252 * 100
                                                                       as rolling_mean_return_ann_pct,

        -- rolling Sharpe approximation (no risk-free rate subtracted)
        case
            when stddev(log_return)           over {{ roll_window }} > 0
            then (avg(log_return)             over {{ roll_window }}
                  / stddev(log_return)        over {{ roll_window }}) * sqrt(252)
        end                                                            as rolling_sharpe,

        -- beta components
        covar_samp(log_return, spy_return)    over {{ roll_window }}   as cov_with_spy,
        var_samp(spy_return)                  over {{ roll_window }}   as var_spy,

        -- correlation
        corr(log_return, spy_return)          over {{ roll_window }}   as rolling_correlation

    from paired
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

    -- alpha = mean_return - beta * mean_spy_return (annualized)
    case
        when var_spy > 0 and window_size >= 10
        then (rolling_mean_return_ann_pct
              - (cov_with_spy / var_spy)
                * avg(spy_return) over {{ roll_window }} * 252 * 100)
    end                                               as rolling_alpha_ann_pct,

    current_timestamp()                               as computed_at

from rolling
where window_size >= 10
