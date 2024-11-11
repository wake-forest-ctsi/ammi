
with source as (

    select * from {{ source('pcornet', 'condition') }}

),

renamed as (

    select
        conditionid,
        patid,
        encounterid,
        report_date,
        resolve_date,
        onset_date,
        condition_status,
        condition,
        condition_type,
        condition_source,
        raw_condition_status,
        raw_condition,
        raw_condition_type,
        raw_condition_source

    from source

)

select * from renamed

