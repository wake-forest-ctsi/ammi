
with source as (

    select * from {{ source('pcornet', 'provider') }}

),

renamed as (

    select
        providerid,
        provider_sex,
        provider_specialty_primary,
        provider_npi,
        provider_npi_flag,
        raw_provider_specialty_primary

    from source

)

select * from renamed

