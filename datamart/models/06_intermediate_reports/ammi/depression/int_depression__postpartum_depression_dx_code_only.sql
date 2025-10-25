-- time range is fixed to between baby_birth_date and 1 year after that

with cohort as (
    select
        birthid,
        mother_patid,
        baby_birth_date
    from {{ ref('int_depression__cohort') }}
),

diagnosis as (
    select
        patid,
        dx_date
    from {{ ref('diagnosis') }}
    -- where dx like 'F53.0%' or dx like 'F32%' or dx like 'F33%' or dx like 'F34.1%'
    where dx = 'F53.0'
),

diagnosis_delete as (
    select
        patid,
        dx_date
    from {{ ref('diagnosis') }}
    where dx like 'F32%' or dx like 'F33%' or dx like 'F34.1%'
),

renamed as (
    select
        a.birthid,
        min(b.dx_date) as earliest_ppd_diagnosis_date,
        max(b.dx_date) as latest_ppd_diagnosis_date,
        min(c.dx_date) as earliest_ppd_diagnosis_date_delete,
        max(c.dx_date) as latest_ppd_diagnosis_date_delete
    from cohort a
    inner join diagnosis b on a.mother_patid = b.patid
     and b.dx_date between a.baby_birth_date and dateadd(year, 1.0, a.baby_birth_date)
    inner join diagnosis_delete c on a.mother_patid = c.patid
     and c.dx_date between a.baby_birth_date and dateadd(year, 1.0, a.baby_birth_date)
    group by a.birthid
)

select * from renamed