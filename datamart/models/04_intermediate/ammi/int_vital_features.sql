-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_vital_features') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select
        patid,
        measure_date,
        systolic,
        diastolic,
        wt,
        original_bmi
    from {{ ref('stg_pcornet__vital') }}
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
        avg(b.systolic) as 'mean_sbp_value',
        avg(b.diastolic) as 'mean_dbp_value',
        max(b.systolic) as 'max_sbp_value',
        max(b.diastolic) as 'max_dbp_value',
        min(b.systolic) as 'min_sbp_vale',
        min(b.diastolic) as 'min_dbp_value',
        avg(b.systolic - b.diastolic) as 'mean_pulse_pressure',
        max(b.systolic - b.diastolic) as 'max_pulse_pressure',
        min(b.systolic - b.diastolic) as 'min_pulse_pressure',
        avg(b.wt) as 'mean_wt',
        max(b.wt) as 'max_wt',
        min(b.wt) as 'min_wt',
        avg(b.original_bmi) as 'mean_original_bmi',
        max(b.original_bmi) as 'max_original_bmi',
        min(b.original_bmi) as 'min_original_bmi',
        avg(b.wt/square(c.mother_height)*705) as 'mean_computed_bmi',
        max(b.wt/square(c.mother_height)*705) as 'max_computed_bmi',
        min(b.wt/square(c.mother_height)*705) as 'min_computed_bmi'
    from cohort
    left join vital b on cohort.mother_patid = b.patid
     and b.measure_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    left join height c on cohort.birthid = c.birthid
    group by cohort.birthid
)

select * from renamed