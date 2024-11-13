{{ config(materialized='view', ) }}


select 
    int_preeclampsia_study_dates.birth_id
    ,avg(vital.systolic) as sbp_mean
    ,avg(vital.diastolic) as dbp_mean
    ,max(vital.systolic) as sbp_max
    ,max(vital.diastolic) as dbp_max
    ,min(vital.systolic) as sbp_min
    ,min(vital.diastolic) as dbp_min
    ,avg(vital.systolic - vital.diastolic) as pulse_pressure_mean
    ,max(vital.systolic - vital.diastolic) as pulse_pressure_max
    ,min(vital.systolic - vital.diastolic) as pulse_pressure_min
from 
    {{ ref('vital') }}
    inner join {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates on int_preeclampsia_study_dates.mother_id = vital.patid
        and vital.measure_date < int_preeclampsia_study_dates.feature_window_end_date
group by
    int_preeclampsia_study_dates.birth_id
