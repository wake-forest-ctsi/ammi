{% macro bp_cat_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select 
        patid,
        {{ add_time_to_date_macro("measure_date", "measure_time") }} measure_date,
        case when (systolic >= 160) or (diastolic >= 110) then 2
             when (systolic >= 140) or (diastolic >= 90) then 1
             else 0 end as bp_cat
    from {{ ref('vital') }}
    where systolic is not null and diastolic is not null
),

-- select birthid with bp_cat >= 1
vital_4h_bp_cat_1 as(
    select
        cohort.birthid
    from cohort
    left join vital on cohort.mother_patid = vital.patid
     and vital.measure_date between {{ date1 }} and {{ date2 }}
     and bp_cat >= 1
    group by cohort.birthid
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

-- select birthid with bp_cat == 2
vital_4h_bp_cat_2 as (
    select
        cohort.birthid
    from cohort
    left join vital on cohort.mother_patid = vital.patid
     and vital.measure_date between {{ date1 }} and {{ date2 }}
     and bp_cat = 2
    group by cohort.birthid
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

bp_cat as (
    select
        birthid,
        case when birthid in (select birthid from vital_4h_bp_cat_2) then 2
             when birthid in (select birthid from vital_4h_bp_cat_1) then 1
             else 0 end as bp_cat
    from cohort
),


-- mark patients without bp at all
no_bp as (
    select
        cohort.birthid,
        min(case when bp_cat is null then 1 else 0 end) as nobp
    from cohort
    left join vital on cohort.mother_patid = vital.patid
     and vital.measure_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
),

renamed as (
    select
        cohort.birthid,
        bp_cat,
        nobp
    from cohort
    left join bp_cat on cohort.birthid = bp_cat.birthid
    left join no_bp on cohort.birthid = no_bp.birthid
)

select * from renamed

{% endmacro %}