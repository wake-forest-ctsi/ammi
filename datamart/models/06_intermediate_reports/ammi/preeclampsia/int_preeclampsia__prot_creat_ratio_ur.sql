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
        norm_range_high,
        norm_range_low,
        {{ add_time_to_date_macro('specimen_date', 'specimen_time') }} as specimen_date
    from {{ ref('lab_result_cm') }}
    where lab_loinc = '2890-2' and raw_unit is not NULL and raw_unit not like 'see%'
),

lab_normalized as (
    select
        patid,
        case when result_unit like 'g%' then result_num*1000
             else result_num end as result_num,
        specimen_date,
        'mg/g' as result_unit
    from lab_raw
),

renamed as (
    select
        lab_normalized.patid,
        lab_normalized.result_num,
        lab_normalized.specimen_date,
        result_unit
    from cohort
    inner join lab_normalized on cohort.mother_patid = lab_normalized.patid
)

select * from renamed