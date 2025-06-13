{% set date1 = 'cohort.estimated_preg_start_date' %}
{% set date2 = 'dateadd(week, 20, cohort.estimated_preg_start_date)' %}

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
    from {{ ref('int_preeclampsia__chronic_bp_cat') }}
),

diagnosis as (
    select
        patid,
        dx_date
    from {{ ref('diagnosis') }}
    where dx like 'O10%'           -- use O10% insetad of I1%
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