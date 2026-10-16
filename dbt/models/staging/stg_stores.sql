-- =============================================================================
-- stg_stores
-- Cleansed store master data.
-- Source: RAW.RAW_STORES
-- =============================================================================

with source as (
    select * from {{ source('raw', 'RAW_STORES') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by trim(store_id)
            order by _load_ts desc
        ) as rn
    from source
    where trim(store_id) is not null
),

transformed as (
    select
        trim(store_id)                          as store_id,
        trim(store_name)                        as store_name,
        upper(trim(store_type))                 as store_type,
        upper(trim(region))                     as region,
        trim(address)                           as address,
        initcap(trim(city))                     as city,
        upper(trim(state))                      as state,
        trim(zip_code)                          as zip_code,
        upper(trim(country))                    as country,
        try_to_date(open_date, 'YYYY-MM-DD')    as open_date,
        trim(manager_name)                      as manager_name,
        current_timestamp()                     as _load_ts
    from deduped
    where rn = 1
)

select * from transformed
