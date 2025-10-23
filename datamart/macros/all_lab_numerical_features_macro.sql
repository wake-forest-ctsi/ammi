{% macro all_lab_numerical_features_macro(min_count, date1, date2, cohort_filter='') %}

-- it only outputs loinc code observed with patients > min_count

with cohort as (
    select
        {{ dbt_utils.star(from=ref('int_cohort'), relation_alias='cohort') }}
    from {{ ref('int_cohort') }} cohort
    {% if cohort_filter | length > 0 %}
    {{ cohort_filter }}
    {% endif %}
),

lab_numerical as (
    select
        patid,
        lab_loinc,
        result_unit,
        {{ add_time_to_date_macro('specimen_date', 'specimen_time') }} specimen_date,
        raw_lab_name as lab_name,
        result_num as result
    from {{ ref('lab_result_cm') }}
    where result_modifier = 'EQ' 
      and lab_loinc is not null
      and raw_unit is not null      -- ignore raw_unit is null  
),

lab_selected as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        cast(lab_loinc as varchar(20)) + '--' + isnull(cast(result_unit as varchar(20)), 'null') as lab_loinc,
        lab_name,
        specimen_date,
        result
    from cohort
    left join lab_numerical on cohort.mother_patid = lab_numerical.patid 
     and lab_numerical.specimen_date between {{ date1 }} and {{ date2 }}
),

lab_count as (
    select
        lab_loinc
    from lab_selected
    group by lab_loinc
    having count(distinct birthid) >= {{ min_count }}
),

renamed as (
    select
        lab_selected.birthid,
        lab_selected.mother_patid,
        lab_selected.baby_birth_date,
        lab_selected.lab_loinc,
        lab_selected.lab_name,
        lab_selected.specimen_date,
        lab_selected.result
    from lab_selected
    inner join lab_count on lab_selected.lab_loinc = lab_count.lab_loinc
)

select * from renamed

{% endmacro %}