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

vital_within_time as (
    select
        cohort.birthid,
        vital.measure_date,
        vital.bp_cat
    from cohort
    left join vital on cohort.mother_patid = vital.patid
     and vital.measure_date between {{ date1 }} and {{ date2 }}
),

-- select birthid with bp_cat >= 1
vital_4h_bp_cat_1 as(
    select
        birthid
    from vital_within_time
    where bp_cat >= 1
    group by birthid
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

-- select birthid with bp_cat == 2
vital_4h_bp_cat_2 as (
    select
        birthid
    from vital_within_time 
    where bp_cat = 2
    group by birthid
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

bp_cat as (
    select
        cohort.birthid,
        case when v2.birthid is not null then 2
             when v1.birthid is not null then 1
             else 0 end as bp_cat
    from cohort
    left join vital_4h_bp_cat_1 v1 on cohort.birthid = v1.birthid
    left join vital_4h_bp_cat_2 v2 on cohort.birthid = v2.birthid
),


-- mark patients without bp at all
no_bp as (
    select
        cohort.birthid,
        min(case when bp_cat is null then 1 else 0 end) as nobp
    from cohort
    left join vital_within_time on cohort.birthid = vital_within_time.birthid
    group by cohort.birthid
),

renamed as (
    select
        bp_cat.birthid,
        bp_cat,
        nobp
    from bp_cat
    left join no_bp on bp_cat.birthid = no_bp.birthid
)

select * from renamed

{% endmacro %}