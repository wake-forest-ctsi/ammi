
with source as (

    select * from {{ source('pcornet', 'death') }}

),

renamed as (

    select
        patid,
        death_date,
        death_date_impute,
        death_source,
        death_match_confidence

    from source

)

select * from renamed

