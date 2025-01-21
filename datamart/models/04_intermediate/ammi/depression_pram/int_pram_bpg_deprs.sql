with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

diagnosis as (
    select
        patid,
        dx_date
    from {{ ref('stg_pcornet__diagnosis') }}
    where dx like 'F32%' or dx like 'F33%' or dx like 'F34.1%'
),

renamed as (
    select
        a.birthid,
        max(case when dx_date is not null then 2 else 1 end) as bpg_deprs
    from cohort a
    left join diagnosis b on a.mother_patid = b.patid
     and dx_date < a.estimated_pregnancy_date
    group by a.birthid
)

select * from renamed

