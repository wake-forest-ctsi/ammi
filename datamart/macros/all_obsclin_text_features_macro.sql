{% macro all_obsclin_text_features_macro(cohort_table, min_count, date1, date2) %}

with cohort as (
    select
        *
    from {{ cohort_table }}
),

obsclin_text as (
    select
        patid,
        obsclin_code,
        {{ add_time_to_date_macro('obsclin_start_date', 'obsclin_start_time') }} obsclin_start_date,
        raw_obsclin_name as obsclin_name,
        obsclin_result_text as result
    from {{ ref('obs_clin') }}
    where obsclin_result_modifier = 'TX' 
),

obsclin_selected as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        obsclin_code,
        obsclin_start_date,
        obsclin_name,
        result
    from cohort
    left join obsclin_text on cohort.mother_patid = obsclin_text.patid 
     and obsclin_text.obsclin_start_date between {{ date1 }} and {{ date2 }}
),

obsclin_count as (
    select
        obsclin_code
    from obsclin_selected
    group by obsclin_code
    having count(distinct birthid) >= {{ min_count }}
),

renamed as (
    select
        obsclin_selected.birthid,
        obsclin_selected.mother_patid,
        obsclin_selected.baby_birth_date,
        obsclin_selected.obsclin_code,
        obsclin_selected.obsclin_start_date,
        obsclin_selected.obsclin_name,
        obsclin_selected.result
    from obsclin_selected
    inner join obsclin_count on obsclin_selected.obsclin_code = obsclin_count.obsclin_code
)

select * from renamed

{% endmacro %}