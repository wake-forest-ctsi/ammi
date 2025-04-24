{% macro vital_features_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select
        patid,
        {{ add_time_to_date_macro("measure_date", "measure_time") }} as measure_date,
        systolic,
        diastolic,
        wt,
        original_bmi
    from {{ ref('vital') }}
),

height as (
    select 
        birthid,
        mother_height
    from {{ ref('int_mother_height') }}
),

renamed as (
    select
        cohort.birthid,
        avg(b.systolic) as 'sbp_value_mean',
        avg(b.diastolic) as 'dbp_value_mean',
        max(b.systolic) as 'sbp_value_max',
        max(b.diastolic) as 'dbp_value_max',
        min(b.systolic) as 'sbp_vale_min',
        min(b.diastolic) as 'dbp_value_min',
        avg(b.systolic - b.diastolic) as 'pulse_pressure_mean',
        max(b.systolic - b.diastolic) as 'pulse_pressure_max',
        min(b.systolic - b.diastolic) as 'pulse_pressure_min',
        avg(b.wt) as 'wt_mean',
        max(b.wt) as 'wt_max',
        min(b.wt) as 'wt_min',
        avg(b.original_bmi) as 'original_bmi_mean',
        max(b.original_bmi) as 'original_bmi_max',
        min(b.original_bmi) as 'original_bmi_min',
        avg(b.wt/square(c.mother_height)*705) as 'computed_bmi_mean',
        max(b.wt/square(c.mother_height)*705) as 'computed_bmi_max',
        min(b.wt/square(c.mother_height)*705) as 'computed_bmi_min'
    from cohort
    left join vital b on cohort.mother_patid = b.patid
     and b.measure_date between {{ date1 }} and {{ date2 }}
    left join height c on cohort.birthid = c.birthid
    group by cohort.birthid
)

select * from renamed

{% endmacro %}