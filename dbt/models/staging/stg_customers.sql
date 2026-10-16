-- =============================================================================
-- stg_customers
-- Cleansed, typed, and deduplicated customer records.
-- Source: RAW.RAW_CUSTOMERS
-- Materialised as: view (in STAGING schema)
-- =============================================================================

with source as (
    select * from {{ source('raw', 'RAW_CUSTOMERS') }}
),

deduped as (
    select
        *,
        row_number() over (
            partition by trim(cust_id)
            order by _load_ts desc
        ) as rn
    from source
    where trim(cust_id) is not null
),

transformed as (
    select
        trim(cust_id)                                               as cust_id,
        initcap(trim(first_name))                                   as first_name,
        initcap(trim(last_name))                                    as last_name,
        initcap(trim(first_name)) || ' ' || initcap(trim(last_name)) as full_name,
        lower(trim(email))                                          as email,
        regexp_replace(trim(phone), '[^0-9\\-\\+\\(\\) ]', '')     as phone,
        try_to_date(date_of_birth, 'YYYY-MM-DD')                    as date_of_birth,
        upper(substr(trim(gender), 1, 1))                           as gender,
        trim(address_line1)                                         as address_line1,
        trim(address_line2)                                         as address_line2,
        initcap(trim(city))                                         as city,
        upper(trim(state))                                          as state,
        trim(zip_code)                                              as zip_code,
        upper(trim(country))                                        as country,
        upper(trim(loyalty_tier))                                   as loyalty_tier,
        try_to_date(signup_date, 'YYYY-MM-DD')                      as signup_date,
        case
            upper(trim(is_active))
            when 'Y' then true
            when '1' then true
            else false
        end                                                         as is_active,
        current_date()                                              as eff_start_dt,
        null::date                                                  as eff_end_dt,
        true                                                        as is_current,
        _src_file,
        _load_ts
    from deduped
    where rn = 1
)

select * from transformed
