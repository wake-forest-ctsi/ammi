
with source as (

    select * from {{ source('pcornet', 'birth_relationship') }}

),

renamed as (

    select
        birthid,
        patid,
        encounterid,
        pregnancyid,
        motherid,
        mother_encounterid

    from source

)

select * from renamed

