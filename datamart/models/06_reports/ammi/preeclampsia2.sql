select
    {{ dbt_utils.star(ref('birth_relationship'), except=["patid"], relation_alias='birth_relationship') }}
    , {{ dbt_utils.star(ref('demographic'), except=["patid"], relation_alias='demographic') }}
    , {{ dbt_utils.star(ref('obs_clin'), except=["encounterid"], relation_alias='obs_clin') }}
    , DATEADD(DAY, - obs_clin.obsclin_result_num, demographic__baby.birth_date) AS "preg_start_date"
from
    {{ ref('birth_relationship') }}
    inner join {{ ('demographic') }} demographic__baby on demographic__baby.patid = birth_relationship.patid
        and demographic__baby.birth_date is not null
    inner join {{ ('demographic') }} demographic__mother on demographic__mother.patid = birth_relationship.motherid
    inner join {{ (ref('obs_clin'))}} on obs_clin.encounterid = birth_relationship.mother_encounterid
        and obs_clin.obsclin_type = 'SM' AND obs_clin.obsclin_code = '444135009'