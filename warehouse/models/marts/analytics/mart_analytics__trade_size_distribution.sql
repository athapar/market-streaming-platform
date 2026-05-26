{{
  config(
    materialized = 'table',
    description  = 'Daily trade size distribution: odd lot / round lot / block classification per symbol.'
  )
}}

/*
  Classifies trades into size buckets and computes concentration metrics.
  Shows how trading activity is distributed between retail-sized odd lots,
  institutional round lots, and block trades.

  Size classifications:
    - ODD_LOT:    < 100 shares (retail-indicative)
    - ROUND_LOT:  100-9,999 shares
    - BLOCK:      >= 10,000 shares or >= $500K notional
*/

with trades as (
    select * from {{ ref('int_microstructure__trade_flow') }}
),

classified as (
    select
        *,
        case
            when is_block_trade then 'BLOCK'
            when trade_size < 100 then 'ODD_LOT'
            else 'ROUND_LOT'
        end as size_class
    from trades
),

daily_buckets as (
    select
        composite_figi,
        symbol,
        trade_date,
        size_class,

        count(*)                                as trade_count,
        sum(trade_size)                         as total_shares,
        sum(dollar_volume)                      as total_dollar_volume,
        avg(trade_size)                         as avg_size,
        avg(dollar_volume)                      as avg_dollar_volume

    from classified
    group by composite_figi, symbol, trade_date, size_class
),

daily_totals as (
    select
        composite_figi,
        symbol,
        trade_date,
        sum(trade_count)        as total_trades,
        sum(total_shares)       as total_shares,
        sum(total_dollar_volume) as total_dollar_volume
    from daily_buckets
    group by composite_figi, symbol, trade_date
)

select
    b.composite_figi,
    b.symbol,
    b.trade_date,
    b.size_class,

    b.trade_count,
    b.total_shares,
    b.total_dollar_volume,
    round(b.avg_size, 1)                                        as avg_size,
    round(b.avg_dollar_volume, 2)                               as avg_dollar_volume,

    -- percentage of daily totals
    round(b.trade_count * 100.0 / nullif(t.total_trades, 0), 2)
                                                                as pct_of_trades,
    round(b.total_shares * 100.0 / nullif(t.total_shares, 0), 2)
                                                                as pct_of_shares,
    round(b.total_dollar_volume * 100.0 / nullif(t.total_dollar_volume, 0), 2)
                                                                as pct_of_dollar_volume,

    current_timestamp()                                         as computed_at

from daily_buckets b
join daily_totals t
    on  b.composite_figi = t.composite_figi
    and b.trade_date     = t.trade_date
