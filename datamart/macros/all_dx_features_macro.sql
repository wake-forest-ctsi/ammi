{% macro all_dx_features_macro(min_count, date1, date2, cohort_filter='') %}

-- by default truncating the dx to 5 char, except keepting all chars for O, T, Z codes
-- it only outputs dx observed with patients > min_count

with cohort as (
    select
        {{ dbt_utils.star(from=ref('int_cohort'), relation_alias='cohort') }}
    from {{ ref('int_cohort') }} cohort
    {% if cohort_filter | length > 0 %}
    {{ cohort_filter }}
    {% endif %}
),

diagnosis as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        case when left(dx, 1) in ('O', 'T', 'Z') then replace(dx, '.', '_')
             else replace(left(dx, 5), '.', '_') end as dx,
        dx_date
    from cohort
    left join {{ ref('diagnosis') }} diagnosis
      on cohort.mother_patid = diagnosis.patid and dx_date between {{ date1 }} and {{ date2 }}
),

diagnosis_count as (
    select
        dx
    from diagnosis
    group by dx
    having count(distinct birthid) >= {{ min_count }}
),

renamed as (
    select
        diagnosis.birthid,
        diagnosis.mother_patid,
        diagnosis.baby_birth_date,
        diagnosis.dx,
        diagnosis.dx_date
    from diagnosis
    inner join diagnosis_count on diagnosis.dx = diagnosis_count.dx
)

select * from renamed

{% endmacro %}