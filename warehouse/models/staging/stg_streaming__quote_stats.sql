{{
  config(
    materialized = 'view',
    description  = 'Staging view over GOLD.GOLD_QUOTE_STATS. Pre-aggregated per-minute spread statistics.'
  )
}}

select
    composite_figi,
    symbol,
    window_start,
    quote_date,
    quote_count,
    avg_bid_price,
    avg_ask_price,
    avg_spread_dollars,
    avg_spread_bps,
    min_spread_bps,
    max_spread_bps,
    avg_mid_price,
    avg_bid_size,
    avg_ask_size,
    bid_size_total,
    ask_size_total,
    order_imbalance,
    updated_at
from {{ source('gold', 'GOLD_QUOTE_STATS') }}
