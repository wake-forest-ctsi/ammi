
with source as (

    select * from {{ source('pcornet', 'vital') }}

),

renamed as (

    select
        vitalid,
        patid,
        encounterid,
        measure_date,
        measure_time,
        vital_source,
        ht,
        wt,
        diastolic,
        systolic,
        original_bmi,
        bp_position,
        smoking,
        tobacco,
        tobacco_type,
        raw_vital_source,
        raw_ht,
        raw_wt,
        raw_diastolic,
        raw_systolic,
        raw_bp_position,
        raw_smoking,
        raw_tobacco,
        raw_tobacco_type

    from source

)

select * from renamed

