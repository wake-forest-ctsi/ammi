-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_chronic_hypertension') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

chronic_bp_cat as (
    select
        birthid,
        bp_cat
    from {{ ref('int_chronic_bp_cat') }}
),

diagnosis as (
    select
        patid,
        dx_date
    from {{ ref('stg_pcornet__diagnosis') }}
    where dx like 'I1%'
),

renamed as (
    select
        cohort.birthid,
        case when max(chronic_bp_cat.bp_cat) > 0 then 1
             when max(case when diagnosis.dx_date is not null then 1 else 0 end) > 0 then 1
             else 0 end as chronic_hyptertension
    from cohort
    left join chronic_bp_cat on cohort.birthid = chronic_bp_cat.birthid
    left join diagnosis on cohort.mother_patid = diagnosis.patid
     and diagnosis.dx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid
)

select * from renamed