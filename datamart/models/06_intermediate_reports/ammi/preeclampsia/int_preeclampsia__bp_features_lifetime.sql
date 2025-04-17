-- it sounds like unc's bp feature is anything before 20 wk of pregnancy

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
        diastolic
    from {{ ref('stg_pcornet__vital') }}
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
        min(b.systolic - b.diastolic) as 'pulse_pressure_min'
    from cohort
    left join vital b on cohort.mother_patid = b.patid
     and b.measure_date < dateadd(week, 20, cohort.estimated_pregnancy_date)
    group by cohort.birthid
)

select * from renamed