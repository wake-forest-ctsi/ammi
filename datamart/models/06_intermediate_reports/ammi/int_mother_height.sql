-- for height, fix it to -1 and +1 year from baby_birth_date

with cohort as (
    select
        birthid,
        mother_patid,
        baby_birth_date
    from {{ ref('int_cohort') }}
),

obs_clin as (
    select
        patid,
        obsclin_result_num as height,
        obsclin_start_date
    from {{ ref('stg_pcornet__obs_clin') }}
    where obsclin_type = 'LC' and obsclin_code = '3137-7'
),

renamed as (
    select 
        a.birthid,
        avg(b.height) as mother_height
    from cohort a
    left join obs_clin b on a.mother_patid = b.patid
     and obsclin_start_date between dateadd(year, -1, a.baby_birth_date) and dateadd(year, 1, a.baby_birth_date)
    group by a.birthid
)

select * from renamed