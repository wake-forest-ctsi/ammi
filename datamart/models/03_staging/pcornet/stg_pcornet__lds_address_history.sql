
with source as (

    select * from {{ source('pcornet', 'lds_address_history') }}

),

renamed as (

    select
        addressid,
        patid,
        address_use,
        address_type,
        address_preferred,
        address_city,
        address_state,
        address_zip5,
        address_zip9,
        address_county,
        address_period_start,
        address_period_end

    from source

)

select * from renamed

