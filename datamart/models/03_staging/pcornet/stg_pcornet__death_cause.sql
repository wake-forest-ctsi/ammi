
with source as (

    select * from {{ source('pcornet', 'death_cause') }}

),

renamed as (

    select
        patid,
        death_cause,
        death_cause_code,
        death_cause_type,
        death_cause_source,
        death_cause_confidence

    from source

)

select * from renamed

