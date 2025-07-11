with cohort as (
    select
        birthid,
        mother_patid,
        estimated_preg_start_date
    from {{ ref('int_cohort') }}
),

vital as (
    select
        patid,
        {{ add_time_to_date_macro("measure_date", "measure_time") }} measure_date,
        systolic,
        diastolic
    from {{ ref('vital') }}
    where systolic is not null and diastolic is not null
),

bp_week as (
    select
        a.birthid,
        datediff(week, a.estimated_preg_start_date, b.measure_date) as preg_weeks,
        systolic,
        diastolic
    from cohort a
    left join vital b on a.mother_patid = b.patid
     and b.measure_date between a.estimated_preg_start_date and dateadd(week, 20, a.estimated_preg_start_date)
),

bp_groupby_week as (
    select
        birthid,
        preg_weeks,
        avg(systolic) as systolic,
        avg(diastolic) as diastolic
    from bp_week
    group by birthid, preg_weeks
)

select * from bp_groupby_week