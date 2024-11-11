select
    birth_relationship.birthid
    ,birth_relationship.pregnancyid
    , DATEADD(DAY, - obs_clin.obsclin_result_num, demographic__baby.birth_date) AS "preg_start_date"
    , demographic__baby.birth_date AS "preg_end_date"
    , encounter__mother_pregnancy.admit_date AS "delivery_admission_date"
    , encounter__mother_pregnancy.discharge_date AS "delivery_discharge_date",
  DATEADD(DAY, -c.gest_age_in_days + 140, b.delivery_date) AS "cutoff_time" -- default cut off time is 20 week
from
    {{ ref('birth_relationship') }}
    inner join {{ ('encounter') }} encounter__mother_pregnancy on encounter__mother_pregnancy = birth_relationship.mother_encounterid
    inner join {{ ('demographic') }} demographic__baby on demographic__baby.patid = birth_relationship.patid
        and demographic__baby.birth_date is not null
    inner join {{ (ref('obs_clin'))}} on obs_clin.encounterid = birth_relationship.mother_encounterid
        and obs_clin.obsclin_type = 'SM' AND obs_clin.obsclin_code = '444135009'