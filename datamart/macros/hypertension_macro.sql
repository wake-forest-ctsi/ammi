{# assume a bp_cat_table is computed #}
{% macro hyptertension_macro(bp_cat_table, date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

bp_cat as (
    select
        birthid,
        bp_cat,
        nobp
    from {{ ref(bp_cat_table) }}
),

diagnosis as (
    select
        patid,
        dx_date
    from {{ ref('diagnosis') }}
    where dx like 'I1%'
),

renamed as (
    select
        cohort.birthid,
        case when max(bp_cat.bp_cat) > 0 then 1
             when max(case when diagnosis.dx_date is not null then 1 else 0 end) > 0 then 1
             else 0 end as chronic_hyptertension,
        min(nobp) as nobp
    from cohort
    left join bp_cat on cohort.birthid = bp_cat.birthid
    left join diagnosis on cohort.mother_patid = diagnosis.patid
     and diagnosis.dx_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)

select * from renamed

{% endmacro %}