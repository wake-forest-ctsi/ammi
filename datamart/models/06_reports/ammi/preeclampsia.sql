/*
    The grain of this table is the birth_id.
    pregnancy_id
    birth_id
    mother_id
    mother_delivery_encounter_id
    baby_id
    baby_delivery_encounter_id
    delivery_date
    gestation_age_in_days_at_delivery
    pregnancy_start_date
    pregnancy_end_date
    mother_delivery_admission_time
    mother_delivery_discharge_time
    study_period_begin_date
    mother_is_hispanic
    mother_is_black
    mother_age_in_years_at_delivery
    bp_time_period ???
    max_bp_cat
    mother_max_weight
    mother_min_weight
    mother_mean_weight
    mother_median_weight
    mother_max_bmi
    mother_min_bmi
    mother_mean_bmi
    mother_median_bmi
    mother_max_height
    mother_min_height
    mother_median_height
*/

select
    int_preeclampsia_study_dates.*
    , {{ dbt_utils.star(from=ref('int_preeclampsia_obsclin__repivoted'), except=['birth_id']) }}
    , {{ dbt_utils.star(from=ref('int_preeclampsia_vital__aggregated'), except=['birth_id']) }}
    , {{ dbt_utils.star(from=ref('int_preeclampsia_prescribing__aggregated'), except=['birth_id']) }}
from
    {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates
    inner join {{ ref('int_preeclampsia_obsclin__repivoted') }} int_preeclampsia_obsclin__repivoted on int_preeclampsia_obsclin__repivoted.birth_id = int_preeclampsia_study_dates.birth_id
    inner join {{ ref('int_preeclampsia_vital__aggregated') }} int_preeclampsia_vital__aggregated on int_preeclampsia_vital__aggregated.birth_id = int_preeclampsia_study_dates.birth_id
    inner join {{ ref('int_preeclampsia_prescribing__aggregated') }} int_preeclampsia_prescribing__aggregated on int_preeclampsia_prescribing__aggregated.birth_id = int_preeclampsia_study_dates.birth_id