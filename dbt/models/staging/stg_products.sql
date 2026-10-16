-- =============================================================================
-- stg_products
-- Cleansed product catalogue with computed gross margin.
-- Source: RAW.RAW_PRODUCTS
-- =============================================================================

with source as (
    select * from {{ source('raw', 'RAW_PRODUCTS') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by trim(prd_id)
            order by _load_ts desc
        ) as rn
    from source
    where trim(prd_id) is not null
),

transformed as (
    select
        trim(prd_id)                                as prd_id,
        trim(prd_name)                              as prd_name,
        upper(trim(prd_category))                   as prd_category,
        upper(trim(prd_subcategory))                as prd_subcategory,
        trim(brand)                                 as brand,
        try_to_decimal(unit_price, 10, 2)           as unit_price,
        try_to_decimal(unit_cost,  10, 2)           as unit_cost,
        case
            when try_to_decimal(unit_price, 10, 2) > 0
            then round(
                (try_to_decimal(unit_price, 10, 2) - try_to_decimal(unit_cost, 10, 2))
                / try_to_decimal(unit_price, 10, 2) * 100,
                2
            )
            else null
        end                                         as gross_margin_pct,
        upper(trim(sku))                            as sku,
        trim(supplier_id)                           as supplier_id,
        case upper(trim(is_active)) when 'Y' then true else false end as is_active,
        try_to_date(launch_date,    'YYYY-MM-DD')   as launch_date,
        try_to_date(discontinue_dt, 'YYYY-MM-DD')   as discontinue_dt,
        current_timestamp()                         as _load_ts
    from deduped
    where rn = 1
)

select * from transformed
