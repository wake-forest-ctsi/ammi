-- helper macro to get the bp_cat

{% macro bp_cat_macro(cohort_table, date1, date2) %}

with cohort as (
    select
        *
    from {{ cohort_table }}
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

-- join to cohort
vital_4h as (
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
    from vital_4h
    where bp_cat >= 1
    group by birthid
    having datediff(hour, min(measure_date), max(measure_date)) >= 4
),

-- select birthid with bp_cat == 2
vital_4h_bp_cat_2 as (
    select
        birthid
    from vital_4h
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
        case when count(measure_date) = 0 then 1 else 0 end as nobp -- this is more clear
    from cohort
    left join vital_4h on cohort.birthid = vital_4h.birthid
    group by cohort.birthid
),

renamed as (
    select
        cohort.birthid,
        coalesce(bp_cat, 0) as bp_cat,
        nobp
    from cohort
    left join bp_cat on cohort.birthid = bp_cat.birthid
    left join no_bp on cohort.birthid = no_bp.birthid
)

select * from renamed

{% endmacro %}