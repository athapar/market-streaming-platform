{{
  config(
    materialized = 'table',
    description  = 'Daily microstructure summary: spread, trade flow, order imbalance per symbol.'
  )
}}

/*
  Combines trade-level metrics (from Gold trades via tick-rule classification)
  with quote-level metrics (from pre-aggregated Gold quote stats) into a single
  daily summary per symbol.

  Symbols with quote data get spread metrics; symbols without (those outside
  the 20-symbol quote subscription) still get trade flow metrics.
*/

with trades as (
    select * from {{ ref('int_microstructure__trade_flow') }}
),

trade_daily as (
    select
        composite_figi,
        symbol,
        trade_date,

        count(*)                                              as trade_count,
        sum(dollar_volume)                                    as total_dollar_volume,
        sum(trade_size)                                       as total_shares,
        avg(trade_size)                                       as avg_trade_size,
        percentile_cont(0.5) within group (order by trade_size)
                                                              as median_trade_size,

        -- trade direction (tick rule)
        sum(case when trade_direction = 'BUY'  then dollar_volume else 0 end)
                                                              as buy_dollar_volume,
        sum(case when trade_direction = 'SELL' then dollar_volume else 0 end)
                                                              as sell_dollar_volume,
        round(
            sum(case when trade_direction = 'BUY' then dollar_volume else 0 end)
            * 100.0 / nullif(sum(dollar_volume), 0), 2
        )                                                     as buy_volume_pct,

        -- order imbalance: (buy - sell) / (buy + sell)
        round(
            (sum(case when trade_direction = 'BUY'  then dollar_volume else 0 end)
           - sum(case when trade_direction = 'SELL' then dollar_volume else 0 end))
          / nullif(sum(dollar_volume), 0), 4
        )                                                     as trade_imbalance,

        -- block trades
        count(case when is_block_trade then 1 end)            as block_trade_count,
        sum(case when is_block_trade then dollar_volume end)  as block_dollar_volume,

        -- price range from trades
        min(trade_price)                                      as min_trade_price,
        max(trade_price)                                      as max_trade_price,
        min(sip_timestamp)                                    as first_trade,
        max(sip_timestamp)                                    as last_trade

    from trades
    group by composite_figi, symbol, trade_date
),

quote_daily as (
    select
        composite_figi,
        symbol,
        quote_date                                            as trade_date,

        sum(quote_count)                                      as total_quotes,
        round(avg(avg_spread_bps), 2)                         as avg_spread_bps,
        round(min(min_spread_bps), 2)                         as min_spread_bps,
        round(max(max_spread_bps), 2)                         as max_spread_bps,
        round(avg(avg_spread_dollars), 6)                     as avg_spread_dollars,
        round(avg(avg_mid_price), 4)                          as avg_mid_price,
        round(avg(order_imbalance), 4)                        as avg_quote_imbalance

    from {{ ref('stg_streaming__quote_stats') }}
    group by composite_figi, symbol, quote_date
)

select
    t.composite_figi,
    t.symbol,
    t.trade_date,

    -- trade metrics
    t.trade_count,
    t.total_dollar_volume,
    t.total_shares,
    t.avg_trade_size,
    t.median_trade_size,
    t.buy_volume_pct,
    t.trade_imbalance,
    t.block_trade_count,
    t.block_dollar_volume,
    t.min_trade_price,
    t.max_trade_price,
    t.first_trade,
    t.last_trade,

    -- quote metrics (null for symbols without quote subscription)
    q.total_quotes,
    q.avg_spread_bps,
    q.min_spread_bps,
    q.max_spread_bps,
    q.avg_spread_dollars,
    q.avg_mid_price,
    q.avg_quote_imbalance,

    -- derived
    case
        when q.total_quotes > 0 and t.trade_count > 0
        then round(q.total_quotes * 1.0 / t.trade_count, 2)
    end                                                       as quote_to_trade_ratio,

    current_timestamp()                                       as computed_at

from trade_daily t
left join quote_daily q
    on  t.composite_figi = q.composite_figi
    and t.trade_date     = q.trade_date
