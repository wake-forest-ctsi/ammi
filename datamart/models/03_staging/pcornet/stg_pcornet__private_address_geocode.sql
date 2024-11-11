
with source as (

    select * from {{ source('pcornet', 'private_address_geocode') }}

),

renamed as (

    select
        geocodeid,
        addressid,
        geocode_state,
        geocode_county,
        geocode_longitude,
        geocode_latitude,
        geocode_block,
        geocode_tract,
        geocode_group,
        geocode_zcta,
        geocode_custom,
        geocode_custom_text,
        shapefile,
        geo_accuracy,
        geo_prov_ref,
        assignment_date

    from source

)

select * from renamed

