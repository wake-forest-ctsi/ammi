with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

dx as (
    select
        patid,
        dx,
        dx_date
    from {{ ref('stg_pcornet__diagnosis') }}
    where dx like 'Z34%' or dx like 'O%'
),

renamed as (
    select
        a.birthid,
        min(datediff(week, a.estimated_pregnancy_date, b.dx_date)) as pnc_wks
    from cohort a
    left join dx b on a.mother_patid = b.patid
     and b.dx_date between a.estimated_pregnancy_date and a.baby_birth_date
    group by a.birthid
)

select * from renamed