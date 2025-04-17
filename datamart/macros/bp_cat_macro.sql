{% macro bp_cat_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select 
        patid,
        measure_date,
        case when (systolic >= 160) or (diastolic >= 110) then 2
             when (systolic >= 140) or (diastolic >= 90) then 1
             else 0 end as bp_cat
    from {{ ref('vital') }}
    where systolic is not null and diastolic is not null
),

-- get the 4 hour apart criteria
vital_4h as (
    select
        cohort.birthid,
        vital.bp_cat
    from cohort
    left join vital on cohort.mother_patid = vital.patid
     and vital.measure_date between {{ date1 }} and {{ date2 }}
    where bp_cat > 0
    group by cohort.birthid, vital.bp_cat
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

bp_cat as (
    select
        birthid,
        max(bp_cat) as bp_cat
    from vital_4h
    group by birthid
),

renamed as (
    select
        cohort.birthid,
        (case when bp_cat is null then 0 else bp_cat end) as bp_cat
    from cohort
    left join bp_cat on cohort.birthid = bp_cat.birthid
)

select * from renamed

{% endmacro %}