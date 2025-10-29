{% macro all_obsclin_numerical_features_macro(cohort_table, min_count, date1, date2) %}

with cohort as (
    select
        *
    from {{ cohort_table }}
),

obsclin_numerical as (
    select
        patid,
        obsclin_code,
        obsclin_result_unit as result_unit,
        {{ add_time_to_date_macro('obsclin_start_date', 'obsclin_start_time') }} obsclin_start_date,
        raw_obsclin_name as obsclin_name,
        obsclin_result_num as result
    from {{ ref('obs_clin') }}
    where lower(obsclin_result_modifier) = 'eq' 
      and obsclin_code is not null
      and lower(obsclin_code) != 'unknown'
      -- keep the raw unit is null since it can be from some questionary  
),

obsclin_selected as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        obsclin_code + '--' + isnull(cast(result_unit as varchar(20)), 'null') as obsclin_code,
        obsclin_name,
        obsclin_start_date,
        result
    from cohort
    inner join obsclin_numerical on cohort.mother_patid = obsclin_numerical.patid 
     and obsclin_numerical.obsclin_start_date between {{ date1 }} and {{ date2 }}
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
        obsclin_selected.obsclin_name,
        obsclin_selected.obsclin_start_date,
        obsclin_selected.result
    from obsclin_selected
    inner join obsclin_count on obsclin_selected.obsclin_code = obsclin_count.obsclin_code
)

select * from renamed

{% endmacro %}