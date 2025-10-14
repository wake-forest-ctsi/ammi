with cohort as (
    select
        birthid,
        mother_patid,
        baby_birth_date
    from {{ ref('int_cohort') }}
),

diagnosis as (
    select
        patid,
        dx_date,
        dx
    from {{ ref('diagnosis')}}
    where dx like 'O86.0%' or dx like 'T81.4%'
),

renamed as (
    select
        birthid,
        max(case when dx is not null then 1 else 0 end) as SSI_diagnosis
    from cohort
    left join diagnosis on cohort.mother_patid = diagnosis.patid
     and datediff(day, cohort.baby_birth_date, dx_date) <= 30
     and datediff(day, cohort.baby_birth_date, dx_date) >= 0
    group by birthid
)

select * from renamed