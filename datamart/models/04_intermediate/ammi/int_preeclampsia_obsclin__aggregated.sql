{{ config(materialized='view', ) }}

select 
    int_preeclampsia_study_dates.birth_id
    ,obs_clin.obsclin_type
    ,obs_clin.obsclin_code
    --,obs_clin.obsclin_result_unit
    ,min(obs_clin.obsclin_result_num) as min
    ,max(obs_clin.obsclin_result_num) as max
    ,cast(avg(obs_clin.obsclin_result_num) as numeric (15,8)) as mean
from 
    {{ ref('obs_clin') }}
    inner join {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates on int_preeclampsia_study_dates.mother_id = obs_clin.patid
        and obs_clin.obsclin_start_date between int_preeclampsia_study_dates.estimated_pregnancy_start_date and int_preeclampsia_study_dates.study_window_end_date
where 
    1 = 1
    and (obs_clin.obsclin_type in ('LC') and obs_clin.obsclin_code in ('8310-5','8867-4','20564-1','8478-0','9279-1'))
    and obsclin_result_modifier = 'EQ'
group by
    int_preeclampsia_study_dates.birth_id
    ,obs_clin.obsclin_type
    ,obs_clin.obsclin_code
    ,obs_clin.obsclin_result_unit