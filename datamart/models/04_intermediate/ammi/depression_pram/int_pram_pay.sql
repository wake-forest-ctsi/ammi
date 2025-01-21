with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

encounter as (
    select
        patid,
        admit_date,
        case 
            when payer_type_primary like '2%' then 1
            when payer_type_primary like '5%' then 2
            when payer_type_primary = '81' then 3
            when payer_type_primary = '311' then 5
            when payer_type_primary like '1%' or payer_type_primary like '3%' then 6
            when payer_type_primary like '9%' then 8
            else 8 end as pay
    from {{ ref('stg_pcornet__encounter') }}
    where payer_type_primary != 'NI'
),

ins as (
    select
        a.birthid,
        max(case when b.pay is null then null when b.pay = 1 then 1 else 0 end) as 'insmed',
        max(case when b.pay is null then null when b.pay = 2 then 1 else 0 end) as 'inswork'
    from cohort a
    left join encounter b on a.mother_patid = b.patid
     and b.admit_date between dateadd(year, -2, a.estimated_pregnancy_date) and a.estimated_pregnancy_date
    group by a.birthid
),

hi as (
    select
        a.birthid,
        max(case when b.pay is null then null when b.pay = 1 then 1 else 0 end) as 'hi_medic',
        max(case when b.pay is null then null when b.pay = 2 then 1 else 0 end) as 'hi_work'
    from cohort a
    left join encounter b on a.mother_patid = b.patid
     and b.admit_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    group by a.birthid  
),

pay as (
    select
        a.birthid,
        min(b.pay) as pay
    from cohort a
    left join encounter b on a.mother_patid = b.patid
     and b.admit_date between a.estimated_pregnancy_date and a.baby_birth_date
    group by a.birthid
)

select
    a.birthid,
    b.insmed + 1 as insmed,
    b.inswork + 1 as inswork,
    c.hi_medic + 1 as hi_medic,
    c.hi_work + 1 as hi_work,
    d.pay
from cohort a
left join ins b on a.birthid = b.birthid
left join hi c on a.birthid = c.birthid
left join pay d on a.birthid = d.birthid