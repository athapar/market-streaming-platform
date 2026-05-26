{{
  config(
    materialized = 'view',
    description  = 'Per-trade enrichment: dollar volume, tick-rule trade direction, block trade flag.'
  )
}}

/*
  Trade direction inference via the tick rule (Lee & Ready 1991):
    - Price > previous price → BUY (uptick)
    - Price < previous price → SELL (downtick)
    - Price = previous price → carry forward previous direction (zero-tick)

  This is the standard method for classifying trade aggressor side when
  working with consolidated tape data without individual order attribution.
*/

with trades as (
    select * from {{ ref('stg_streaming__trades') }}
),

with_prev as (
    select
        *,
        trade_price * trade_size                                     as dollar_volume,
        lag(trade_price) over (
            partition by symbol order by sip_timestamp, trade_id
        )                                                            as prev_price
    from trades
),

with_raw_direction as (
    select
        *,
        case
            when trade_price > prev_price then 'BUY'
            when trade_price < prev_price then 'SELL'
            else null
        end                                                          as raw_direction
    from with_prev
),

-- fill forward null directions (zero-tick rule)
with_direction as (
    select
        *,
        coalesce(
            raw_direction,
            last_value(raw_direction ignore nulls) over (
                partition by symbol order by sip_timestamp, trade_id
                rows between unbounded preceding and current row
            ),
            'BUY'
        )                                                            as trade_direction,

        case
            when trade_size >= 10000
              or trade_price * trade_size >= 500000
            then true
            else false
        end                                                          as is_block_trade

    from with_raw_direction
)

select
    composite_figi,
    symbol,
    trade_id,
    trade_price,
    trade_size,
    dollar_volume,
    exchange_id,
    tape,
    sip_timestamp,
    trade_date,
    trade_direction,
    is_block_trade,
    silver_timestamp
from with_direction
