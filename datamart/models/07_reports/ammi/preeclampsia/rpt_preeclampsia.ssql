-- final data mart for preeclampsia, tried to reproduce kibria's table

select
    a.birthid as preg_deid,
    a.mother_patid as mom_deid,
    b.preeclampsia as cp_anypree_sf,
    c.chronic_hyptertension as chtn_any,
    d.sbp_value_mean as mean_sbp_value,
    d.sbp_value_median as med_sbp_value,
    d.dbp_value_median as med_dbp_value,
    d.dbp_value_mean as mean_dbp_value,
    d.sbp_value_max as max_sbp_value,
    d.dbp_value_max as max_dbp_value,
    d.wt_max as max_WEIGHT,
    d.wt_median as median_WEIGHT,
    d.wt_mean as mean_WEIGHT,
    d.wt_min as min_WEIGHT,
    d.pulse_pressure_max as max_pulse_pressure,
    coalesce(d.original_bmi_max, d.computed_bmi_max) as max_BMI,
    coalesce(d.original_bmi_mean, d.computed_bmi_mean) as mean_BMI,
    coalesce(d.original_bmi_min, d.computed_bmi_min) as min_BMI,
    coalesce(d.original_bmi_median, d.computed_bmi_median) as median_BMI,
    d.pulse_pressure_mean as mean_pulse_pressure,
    e.med_rx_nifedipine as med_NIFEdipine,
    e.med_rx_insulin as med_insulin,
    coalesce(f."LC_8478-0_max", d.computed_map_value_max) as max_MAP,
    e.med_rx_glucagon as med_glucagon,
    null as med_needle,
    h.mom_race_num,
    null as med_lancets,
    coalesce(f."LC_8478-0_mean", d.computed_map_value_mean) as mean_MAP,
    g.mother_age as preg_mom_age_at_del,
    coalesce(f."LC_8478-0_median", d.computed_map_value_median) as median_MAP,
    f."LC_8867-4_median" as median_PULSE,
    f."LC_8867-4_max" as max_PULSE,
    f."LC_8867-4_mean" as mean_PULSE,
    e.med_rx_glucose as med_glucose,
    f."LC_8310-5_min" as min_TEMPERATURE
from {{ ref('int_cohort') }} a
left join {{ ref('int_preeclampsia__severe') }} b on a.birthid = b.birthid
left join {{ ref('int_preeclampsia__chronic_hypertension') }} c on a.birthid = c.birthid
left join {{ ref('int_preeclampsia__vital_features') }} d on a.birthid = d.birthid
left join {{ ref('int_preeclampsia__rx') }} e on a.birthid = e.birthid
left join {{ ref('int_preeclampsia__obs_clin_features') }} f on a.birthid = f.birthid
left join {{ ref('int_mother_age_at_birth') }} g on a.birthid = g.birthid
left join {{ ref('int_preeclampsia__mom_race_num') }} h on a.birthid = h.birthid