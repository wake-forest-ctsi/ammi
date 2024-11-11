
with source as (

    select * from {{ source('pcornet', 'private_address_history') }}

),

renamed as (

    select
        addressid,
        patid,
        address_use,
        address_type,
        address_preferred,
        address_street,
        address_detail,
        address_county,
        address_city,
        address_state,
        address_zip5,
        address_zip9,
        address_period_start,
        address_period_end,
        raw_address_text

    from source

)

select * from renamed

