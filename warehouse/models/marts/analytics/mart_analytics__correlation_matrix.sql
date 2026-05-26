{{
  config(
    materialized = 'table',
    description  = 'Trailing 20-day pairwise return correlations across the tracked universe.'
  )
}}

/*
  Cross-sectional correlation matrix from daily log returns.
  Computes corr(symbol_a, symbol_b) over trailing 20-day windows.

  Filtered to the most recent date with sufficient data to keep the table
  compact. The dashboard uses this for a heatmap visualization.
*/

with returns as (
    select
        symbol,
        price_date,
        log_return
    from {{ ref('int_analytics__daily_returns') }}
    where log_return is not null
),

-- get latest 20 trading days
recent_dates as (
    select distinct price_date
    from returns
    order by price_date desc
    limit 20
),

recent_returns as (
    select r.*
    from returns r
    inner join recent_dates d on r.price_date = d.price_date
),

-- cross join symbols, compute correlation
pairs as (
    select
        a.symbol as symbol_a,
        b.symbol as symbol_b,
        corr(a.log_return, b.log_return) as correlation,
        count(*)                          as overlap_days
    from recent_returns a
    inner join recent_returns b
        on a.price_date = b.price_date
    where a.symbol <= b.symbol
    group by a.symbol, b.symbol
    having count(*) >= 10
)

select
    symbol_a,
    symbol_b,
    round(correlation, 4) as correlation,
    overlap_days,
    current_timestamp()   as computed_at
from pairs
