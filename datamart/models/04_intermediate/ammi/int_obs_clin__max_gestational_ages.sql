select 
  obs_clin.encounterid
  , min(obs_clin.obsclin_result_num) as "gest_age_in_days"
from {{ ref('obs_clin') }}
where 
    obs_clin.obsclin_type = 'SM' 
    and obs_clin.obsclin_code = '444135009'
group by 
    obs_clin.encounterid