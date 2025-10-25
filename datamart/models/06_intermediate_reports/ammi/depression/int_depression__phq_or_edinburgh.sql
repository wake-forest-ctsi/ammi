-- see excel file for details

{% set features = 
      ('21012948', '21012949', '21012950', '21012951', '21012953',
       '21012954', '21012955', '21012956', '21012958',
       '99046-5', '71354-5', '21012959') %}

with cohort as (
    select
        *
    from {{ ref('int_depression__cohort') }}
),

phq as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
        obsclin_code,
        case
            when obsclin_code in ('99046-5', '71354-5', '21012959') then
                    obsclin_result_num
            else
                case when obsclin_result_text = 'Not at all' then 0
                     when obsclin_result_text = 'Several days' then 1
                     when obsclin_result_text = 'More than half the days' then 2
                     when obsclin_result_text = 'Nearly every day' then 3
                     else null end
        end as phq_value
    from {{ ref('obs_clin') }}
    where obsclin_code is not null
      and obsclin_code in {{ features }} 
),

phq_stats as (
    select
        cohort.birthid,
        'phq_or_edinburgh_max_' + obsclin_code as 'feature_name',
        -- count(phq_value) as value_counts,
        -- min(phq_value) as value_min,
        max(phq_value) as 'value'
        -- avg(phq_value) as value_mean
    from cohort
    inner join phq on cohort.mother_patid = phq.patid
     and obsclin_start_date between dateadd(year, -2, cohort.baby_birth_date) and cohort.baby_birth_date
    group by cohort.birthid, obsclin_code
)

select * from phq_stats
