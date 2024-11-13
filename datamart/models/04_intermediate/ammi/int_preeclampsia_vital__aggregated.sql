{{ config(materialized='view', ) }}

select 
    int_preeclampsia_study_dates.birth_id
    ,max(vital.WT) as weight_max
    ,min(vital.WT) as weight_min
    ,avg(vital.WT) as weight_mean
    ,max(vital.original_bmi) as bmi_max
    ,min(vital.original_bmi) as bmi_min
    ,avg(vital.original_bmi) as bmi_mean
    ,max(vital.HT) as ht_max
    ,min(vital.HT) as ht_min
    ,avg(vital.HT) as ht_mean
from 
    {{ ref('vital') }}
    inner join {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates on int_preeclampsia_study_dates.mother_id = vital.patid
        and vital.measure_date between int_preeclampsia_study_dates.estimated_pregnancy_start_date and int_preeclampsia_study_dates.study_window_end_date
group by
    int_preeclampsia_study_dates.birth_id