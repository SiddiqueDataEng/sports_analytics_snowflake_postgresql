-- =============================================================================
-- stg_orders
-- Cleansed order headers with derived date parts and discount flag.
-- Source: RAW.RAW_ORDERS
-- =============================================================================

with source as (
    select * from {{ source('raw', 'RAW_ORDERS') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by trim(order_id)
            order by _load_ts desc
        ) as rn
    from source
    where trim(order_id) is not null
),

transformed as (
    select
        trim(order_id)                                          as order_id,
        try_to_date(order_date, 'YYYY-MM-DD')                   as order_date,
        year(try_to_date(order_date, 'YYYY-MM-DD'))             as order_year,
        month(try_to_date(order_date, 'YYYY-MM-DD'))            as order_month,
        quarter(try_to_date(order_date, 'YYYY-MM-DD'))          as order_quarter,
        trim(cust_id)                                           as cust_id,
        trim(store_id)                                          as store_id,
        upper(trim(channel))                                    as channel,
        upper(trim(order_status))                               as order_status,
        nullif(trim(promo_code), '')                            as promo_code,
        try_to_decimal(total_amt,    12, 2)                     as total_amt,
        try_to_decimal(discount_amt, 12, 2)                     as discount_amt,
        try_to_decimal(net_amt,      12, 2)                     as net_amt,
        case
            when try_to_decimal(discount_amt, 12, 2) > 0 then true
            else false
        end                                                     as is_discounted,
        _src_file,
        _load_ts
    from deduped
    where rn = 1
)

select * from transformed
