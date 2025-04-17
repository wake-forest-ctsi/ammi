with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

edinburgh_total as (
    select
        patid,
        obsclin_start_date,
        obsclin_result_num
    from {{ ref('stg_pcornet__obs_clin') }}
    where obsclin_code = '99046-5'
),

edinburge_depression_total as (
    select
        patid,
        obsclin_start_date,
        obsclin_result_num
    from {{ ref('stg_pcornet__obs_clin') }}
    where obsclin_code = '71354-5'        
),

renamed as (
    select
        a.birthid,
        avg(b.obsclin_result_num) as edinburgh_total_avg,
        max(b.obsclin_result_num) as edinburgh_total_max,
        avg(c.obsclin_result_num) as edinburgh_depression_total_avg,
        max(c.obsclin_result_num) as edinburgh_depression_total_max,
        greatest(max(b.obsclin_result_num), max(c.obsclin_result_num)) as edinburgh_max
    from cohort a
    left join edinburgh_total b on a.mother_patid = b.patid
     and b.obsclin_start_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    left join edinburge_depression_total c on a.mother_patid = c.patid
     and c.obsclin_start_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    group by a.birthid
)

select * from renamed