{{
  config(
    materialized = 'table',
    description  = 'Intraday spread and trade activity profile by 30-minute bucket per symbol.'
  )
}}

/*
  Shows the classic U-shaped intraday spread pattern: wider at open/close,
  tighter midday. Combines quote-derived spread stats with trade activity
  for each 30-minute time-of-day window.
*/

with quote_stats as (
    select
        symbol,
        composite_figi,
        quote_date,
        window_start,
        quote_count,
        avg_spread_bps,
        min_spread_bps,
        max_spread_bps,
        order_imbalance,
        avg_bid_size,
        avg_ask_size,

        extract(hour from window_start) * 100
            + floor(extract(minute from window_start) / 30) * 30
                                                             as bucket_id
    from {{ ref('stg_streaming__quote_stats') }}
),

trade_stats as (
    select
        symbol,
        trade_date,
        extract(hour from sip_timestamp) * 100
            + floor(extract(minute from sip_timestamp) / 30) * 30
                                                             as bucket_id,
        count(*)                                             as trade_count,
        sum(dollar_volume)                                   as dollar_volume,
        avg(trade_size)                                      as avg_trade_size,
        sum(case when trade_direction = 'BUY' then 1 else 0 end)
            * 100.0 / nullif(count(*), 0)                    as buy_pct
    from {{ ref('int_microstructure__trade_flow') }}
    group by symbol, trade_date, bucket_id
),

combined as (
    select
        q.symbol,
        q.composite_figi,
        q.bucket_id,
        count(distinct q.quote_date)                         as trading_days,

        -- spread stats
        round(avg(q.avg_spread_bps), 2)                      as avg_spread_bps,
        round(avg(q.min_spread_bps), 2)                      as avg_min_spread_bps,
        round(avg(q.max_spread_bps), 2)                      as avg_max_spread_bps,
        round(avg(q.quote_count), 0)                         as avg_quotes_per_bucket,

        -- order imbalance
        round(avg(q.order_imbalance), 4)                     as avg_imbalance,

        -- size
        round(avg(q.avg_bid_size), 0)                        as avg_bid_size,
        round(avg(q.avg_ask_size), 0)                        as avg_ask_size

    from quote_stats q
    group by q.symbol, q.composite_figi, q.bucket_id
),

with_trades as (
    select
        c.*,

        round(avg(t.trade_count), 0)                         as avg_trades_per_bucket,
        round(avg(t.dollar_volume), 2)                       as avg_dollar_volume_per_bucket,
        round(avg(t.avg_trade_size), 0)                      as avg_trade_size,
        round(avg(t.buy_pct), 2)                             as avg_buy_pct

    from combined c
    left join trade_stats t
        on  c.symbol    = t.symbol
        and c.bucket_id = t.bucket_id
    group by
        c.symbol, c.composite_figi, c.bucket_id, c.trading_days,
        c.avg_spread_bps, c.avg_min_spread_bps, c.avg_max_spread_bps,
        c.avg_quotes_per_bucket, c.avg_imbalance,
        c.avg_bid_size, c.avg_ask_size
)

select
    symbol,
    composite_figi,
    bucket_id,
    trading_days,

    avg_spread_bps,
    avg_min_spread_bps,
    avg_max_spread_bps,
    avg_quotes_per_bucket,
    avg_imbalance,
    avg_bid_size,
    avg_ask_size,

    avg_trades_per_bucket,
    avg_dollar_volume_per_bucket,
    avg_trade_size,
    avg_buy_pct,

    -- relative spread: this bucket's avg vs the symbol's overall avg
    avg_spread_bps / nullif(
        avg(avg_spread_bps) over (partition by symbol), 0
    )                                                        as relative_spread,

    current_timestamp()                                      as computed_at

from with_trades
