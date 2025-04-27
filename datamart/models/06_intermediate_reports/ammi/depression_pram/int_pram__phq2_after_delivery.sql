with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

phq2_Q1 as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
        case when obsclin_result_text = 'Not at all' then 0
             when obsclin_result_text = 'Several days' then 1
             when obsclin_result_text = 'More than half the days' then 2
             when obsclin_result_text = 'Nearly every day' then 3
             when obsclin_result_text = 'No information' then NULL
             else -99 end as phq2_q1
    from {{ ref('obs_clin') }}
    where obsclin_code = '21012948'
),

phq2_Q2 as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
        case when obsclin_result_text = 'Not at all' then 0
             when obsclin_result_text = 'Several days' then 1
             when obsclin_result_text = 'More than half the days' then 2
             when obsclin_result_text = 'Nearly every day' then 3
             when obsclin_result_text = 'No information' then NULL
             else -99 end as phq2_q2
    from {{ ref('obs_clin') }}
    where obsclin_code = '21012949'
),

renamed as (
    select
        a.birthid,
        max(b.phq2_q1) as phq2_q1_max,
        max(c.phq2_q2) as phq2_q2_max
    from cohort a
    left join phq2_q1 b on a.mother_patid = b.patid
     and b.obsclin_start_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    left join phq2_q2 c on a.mother_patid = c.patid
     and c.obsclin_start_date between a.baby_birth_date and dateadd(year, 1, a.baby_birth_date)
    group by a.birthid
)

select * from renamed