{{
  config(
    materialized = 'view',
    description  = 'Batch TTM fundamentals + valuation ratios snapshot per security.'
  )
}}

select
    composite_figi,
    ticker,
    close_price                                    as batch_close_price,
    price_as_of                                    as batch_price_as_of,
    financials_as_of,
    filing_date,
    quarters_included,
    market_cap,
    shares_outstanding,

    pe_ratio                                       as batch_pe_ratio,
    pb_ratio                                       as batch_pb_ratio,
    ps_ratio                                       as batch_ps_ratio,
    ev_ebit                                        as batch_ev_ebit,
    price_to_fcf                                   as batch_price_to_fcf,

    gross_margin,
    operating_margin,
    net_margin,
    roe,
    roa,
    current_ratio,
    debt_to_equity,

    ttm_revenue,
    ttm_net_income,
    ttm_operating_income,
    ttm_free_cash_flow,
    book_value,
    total_assets,
    total_liabilities,
    loaded_at                                      as batch_loaded_at
from {{ source('recon', 'FUNDAMENTALS_VALUATION') }}
