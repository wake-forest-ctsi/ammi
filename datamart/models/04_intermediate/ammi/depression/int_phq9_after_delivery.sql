with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

phq9_total as (
    select
        patid,
        obsclin_start_date,
        obsclin_result_num
    from {{ ref('stg_pcornet__obs_clin') }}
    where obsclin_code = '21012959'
),

renamed as (
    select
        a.birthid,
        avg(b.obsclin_result_num) as phq9_total_avg,
        max(b.obsclin_result_num) as phq9_total_max
    from cohort a
    left join phq9_total b on a.mother_patid = b.patid
     and b.obsclin_start_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    group by a.birthid
)

select * from renamed