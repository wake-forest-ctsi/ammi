with cohort as (
    select
        distinct mother_patid
    from {{ ref('int_cohort') }}
),

lab_raw as (
    select
        patid,
        result_num,
        result_unit,
        raw_unit,
        norm_range_low,
        norm_range_high,
        {{ add_time_to_date_macro('specimen_date', 'specimen_time') }} as specimen_date
    from {{ ref('lab_result_cm') }}
    where lab_loinc = '1920-8'
),

lab_normalized as (
    select
        patid,
        result_num,
        specimen_date
    from lab_raw
),

renamed as (
    select
        lab_normalized.patid,
        lab_normalized.result_num,
        lab_normalized.specimen_date,
        'U/L' as result_unit
    from cohort
    inner join lab_normalized on cohort.mother_patid = lab_normalized.patid
)

select * from renamed