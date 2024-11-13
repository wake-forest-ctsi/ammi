{{ config(materialized='view', ) }}

select 
    int_preeclampsia_study_dates.birth_id
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%aspirin%' THEN 1 ELSE 0 END) AS "med_rx_aspirin"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%nifedipine%' THEN 1 ELSE 0 END) AS "med_rx_nifedipine"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%insulin%' THEN 1 ELSE 0 END) AS "med_rx_insulin"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%amlodipine%' THEN 1 ELSE 0 END) AS "med_rx_amlodipine"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%glucagon%' THEN 1 ELSE 0 END) AS "med_rx_glucagon"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%glucose%' THEN 1 ELSE 0 END) AS "med_rx_glucose"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%methyldopa%' THEN 1 ELSE 0 END) AS "med_rx_methyldopa"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%promethazine%' THEN 1 ELSE 0 END) AS "med_rx_promethazine"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '% ethyl %' THEN 1 ELSE 0 END) AS "med_rx_ethyl"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%famotidine%' THEN 1 ELSE 0 END) AS "med_rx_famotidine"
    ,MAX(CASE WHEN LOWER(prescribing.raw_rx_med_name) LIKE '%ondansetron%' THEN 1 ELSE 0 END) AS "med_rx_ondansetron"
from 
    {{ ref('prescribing') }}
    inner join {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates on int_preeclampsia_study_dates.mother_id = prescribing.patid
        and prescribing.rx_order_date between int_preeclampsia_study_dates.estimated_pregnancy_start_date and int_preeclampsia_study_dates.study_window_end_date
group by
    int_preeclampsia_study_dates.birth_id