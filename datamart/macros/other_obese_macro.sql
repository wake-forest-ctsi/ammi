{% macro other_obese_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

other_obese as (
    select
        patid,
        dx,
        dx_date
    from {{ ref('diagnosis') }}
    where dx like 'E66%' or dx like 'O99.21%'
),

renamed as (
    select
        cohort.birthid,
        max(case when dx = 'E66.01' then 1 else 0 end) as 'morbid',
        max(case when dx in ('E66.09', 'E66.8', 'E66.9') then 1 else 0 end) as 'obese',
        max(case when dx = 'E66.3' then 1 else 0 end) as 'overweight',
        max(case when dx like 'O99%' then 1 else 0 end) as 'O99_obese'
    from cohort
    left join other_obese on cohort.mother_patid = other_obese.patid
     and dx_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)

select * from renamed

{% endmacro %}