with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select
        patid,
        measure_date,
        wt
    from {{ ref('stg_pcornet__vital') }}
    where wt is not null and measure_date is not null
),

first_trimester_wt as (
    select
        a.birthid,
        avg(b.wt) as first_trimester_wt
    from cohort a
    left join vital b on a.mother_patid = b.patid
     and b.measure_date between dateadd(month, -1, a.estimated_pregnancy_date) and dateadd(week, 13, a.estimated_pregnancy_date)
    group by a.birthid
),

last_wt as (
    select
        a.birthid,
        avg(b.wt) as last_wt
    from cohort a
    left join vital b on a.mother_patid = b.patid
     and b.measure_date between dateadd(day, -30, a.baby_birth_date) and a.baby_birth_date
    group by a.birthid
),

renamed as (
    select
        a.birthid,
        b.first_trimester_wt as mat_prwt,
        c.last_wt - b.first_trimester_wt as pgwt_gn,
        b.first_trimester_wt/square(d.mother_height)*705 as mom_bmi

    from cohort a
    left join first_trimester_wt b on a.birthid = b.birthid
    left join last_wt c on a.birthid = c.birthid
    left join {{ ref('int_mother_height') }} d on a.birthid = d.birthid
)

select 
  *,
  case 
    when mom_bmi is null then null
    when mom_bmi <= 18.5 then 1
    when mom_bmi <= 24.9 then 2
    when mom_bmi <= 29.9 then 3
    else 4 end as 'mom_bmig_bc'
from renamed